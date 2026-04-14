from flask import Blueprint, request, session, jsonify, current_app, url_for, Response
from functools import wraps
from datetime import datetime
import io
import os
import shutil

from werkzeug.utils import secure_filename

from .db import connect, placeholders

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

messages_bp = Blueprint("messages", __name__)

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]


# ======================
# HELPERS
# ======================
def db():
    database_url = current_app.config["DATABASE_URL"]
    return connect(database_url)


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if "user" not in session:
            return jsonify({"ok": False, "error": "Login required"}), 401
        return f(*args, **kwargs)
    return wrapped


def is_admin():
    return session.get("role") == "ADMIN"


def has_module_access(module: str, need_edit: bool = False) -> bool:
    fn = current_app.config.get("HAS_MODULE_ACCESS_FUNC")
    if callable(fn):
        return fn(module, need_edit)
    return False


def require_module(module: str, need_edit: bool = False):
    def deco(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if not has_module_access(module, need_edit=need_edit):
                return jsonify({
                    "ok": False,
                    "error": f"No permission for {module}{' (edit)' if need_edit else ''}"
                }), 403
            return f(*args, **kwargs)
        return wrapped
    return deco


# ======================
# GOOGLE DRIVE HELPERS
# OAuth version using company Drive connection
# ======================
def _ensure_tmp_dir():
    os.makedirs("/tmp/erp_google", exist_ok=True)
    return "/tmp/erp_google"


def oauth_token_file():
    token_path = (
        current_app.config.get("GOOGLE_OAUTH_TOKEN_FILE")
        or os.environ.get("GOOGLE_OAUTH_TOKEN_FILE")
        or "/tmp/google-oauth-token.json"
    ).strip()

    parent = os.path.dirname(token_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    return token_path


def oauth_token_secret_file():
    return "/etc/secrets/google-oauth-token.json"


def clear_oauth_token_file():
    token_path = oauth_token_file()
    try:
        if os.path.exists(token_path):
            os.remove(token_path)
    except Exception:
        pass


def restore_token_from_secret_if_needed():
    token_path = oauth_token_file()
    secret_path = oauth_token_secret_file()

    if os.path.exists(token_path):
        return token_path

    db_token = get_saved_oauth_token_from_db()
    if db_token:
        try:
            with open(token_path, "w", encoding="utf-8") as f:
                f.write(db_token)
            return token_path
        except Exception as e:
            raise ValueError(f"Could not restore Google token from database: {str(e)}")

    if os.path.exists(secret_path):
        try:
            shutil.copyfile(secret_path, token_path)
            return token_path
        except Exception as e:
            raise ValueError(f"Could not restore Google token from secrets: {str(e)}")

    raise ValueError("Google Drive is not connected yet. Please connect Google Drive first.")


def save_runtime_token(token_json: str):
    token_path = oauth_token_file()
    with open(token_path, "w", encoding="utf-8") as f:
        f.write(token_json)

    save_oauth_token_to_db(token_json, session.get("user", "system"))

def get_saved_oauth_token_from_db():
    conn = db()
    row = conn.execute("""
        SELECT token_json
        FROM google_oauth_tokens
        WHERE service_name=%s
        LIMIT 1
    """, ("google_drive",)).fetchone()
    conn.close()
    return row["token_json"] if row and row["token_json"] else None


def save_oauth_token_to_db(token_json: str, updated_by: str):
    conn = db()
    existing = conn.execute("""
        SELECT id
        FROM google_oauth_tokens
        WHERE service_name=%s
        LIMIT 1
    """, ("google_drive",)).fetchone()

    if existing:
        conn.execute("""
            UPDATE google_oauth_tokens
            SET token_json=%s, updated_at=%s, updated_by=%s
            WHERE service_name=%s
        """, (token_json, now_iso(), updated_by, "google_drive"))
    else:
        conn.execute("""
            INSERT INTO google_oauth_tokens (service_name, token_json, updated_at, updated_by)
            VALUES (%s, %s, %s, %s)
        """, ("google_drive", token_json, now_iso(), updated_by))

    conn.commit()
    conn.close()


def get_oauth_drive_service():
    token_path = restore_token_from_secret_if_needed()

    try:
        creds = Credentials.from_authorized_user_file(token_path, DRIVE_SCOPES)
    except Exception:
        clear_oauth_token_file()
        raise ValueError("Google Drive token file is invalid. Please reconnect Google Drive.")

    try:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_runtime_token(creds.to_json())
        elif not creds.valid:
            clear_oauth_token_file()
            raise ValueError("Google Drive connection is invalid. Please reconnect Google Drive.")
    except ValueError:
        raise
    except Exception:
        clear_oauth_token_file()
        raise ValueError("Google Drive connection expired or was revoked. Please reconnect Google Drive.")

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def get_general_drive_root_folder_id():
    root_id = current_app.config.get("GOOGLE_DRIVE_ROOT_FOLDER_ID") or os.environ.get("GOOGLE_DRIVE_ROOT_FOLDER_ID")
    if not root_id or root_id == "PASTE_YOUR_ROOT_FOLDER_ID_HERE":
        raise RuntimeError("GOOGLE_DRIVE_ROOT_FOLDER_ID is not configured")
    return root_id.strip()


def get_messages_root_drive_folder_id():
    root_id = (
        current_app.config.get("GOOGLE_MESSAGES_DRIVE_ROOT_FOLDER_ID")
        or os.environ.get("GOOGLE_MESSAGES_DRIVE_ROOT_FOLDER_ID")
    )
    if root_id:
        return root_id.strip()
    return None


def find_child_folder(service, parent_id: str, folder_name: str):
    safe_name = folder_name.replace("'", "\\'")
    query = (
        f"name = '{safe_name}' and "
        f"mimeType = 'application/vnd.google-apps.folder' and "
        f"'{parent_id}' in parents and trashed = false"
    )

    result = service.files().list(
        q=query,
        fields="files(id,name)",
        pageSize=10,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()

    files = result.get("files", [])
    return files[0] if files else None


def ensure_drive_folder(service, parent_id: str, folder_name: str):
    found = find_child_folder(service, parent_id, folder_name)
    if found:
        return found["id"]

    created = service.files().create(
        body={
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        },
        fields="id,name",
        supportsAllDrives=True,
    ).execute()

    return created["id"]


def ensure_messages_root_folder(service):
    explicit_messages_root = get_messages_root_drive_folder_id()
    if explicit_messages_root:
        return explicit_messages_root

    general_root = get_general_drive_root_folder_id()
    return ensure_drive_folder(service, general_root, "Messages")


def get_conversation_google_emails(conn, conversation_id: int):
    rows = conn.execute("""
        SELECT DISTINCT LOWER(TRIM(u.google_email)) AS google_email
        FROM message_conversation_members m
        JOIN users u ON u.id = m.user_id
        WHERE m.conversation_id=%s
          AND m.active=1
          AND u.active=1
          AND u.google_email IS NOT NULL
          AND TRIM(u.google_email) <> ''
    """, (conversation_id,)).fetchall()

    emails = []
    for r in rows:
        email = (r["google_email"] or "").strip().lower()
        if email:
            emails.append(email)
    return emails


def sync_conversation_folder_shares(service, folder_id: str, allowed_emails):
    allowed = set((e or "").strip().lower() for e in allowed_emails if (e or "").strip())

    current = service.permissions().list(
        fileId=folder_id,
        fields="permissions(id,emailAddress,role)",
        supportsAllDrives=True,
    ).execute().get("permissions", [])

    current_map = {}
    for p in current:
        email = (p.get("emailAddress") or "").strip().lower()
        if email:
            current_map[email] = p

    for email, perm in list(current_map.items()):
        role = perm.get("role")
        if role in ("owner", "organizer", "fileOrganizer"):
            continue

        if email not in allowed:
            try:
                service.permissions().delete(
                    fileId=folder_id,
                    permissionId=perm["id"],
                    supportsAllDrives=True,
                ).execute()
            except Exception:
                pass

    for email in allowed:
        if email in current_map:
            continue
        try:
            service.permissions().create(
                fileId=folder_id,
                body={
                    "type": "user",
                    "role": "reader",
                    "emailAddress": email,
                },
                sendNotificationEmail=False,
                fields="id",
                supportsAllDrives=True,
            ).execute()
        except Exception:
            pass


def ensure_conversation_drive_folder(service, conn, conversation_id: int):
    root_folder_id = ensure_messages_root_folder(service)
    folder_id = ensure_drive_folder(service, root_folder_id, f"conversation_{conversation_id}")

    allowed_emails = get_conversation_google_emails(conn, conversation_id)
    sync_conversation_folder_shares(service, folder_id, allowed_emails)

    return folder_id


def upload_message_attachment_to_drive(conn, file_storage, conversation_id: int):
    if not file_storage or not getattr(file_storage, "filename", ""):
        return None

    service = get_oauth_drive_service()
    parent_drive_id = ensure_conversation_drive_folder(service, conn, conversation_id)

    safe_name = secure_filename(file_storage.filename) or "attachment"
    file_bytes = file_storage.read()

    media = MediaIoBaseUpload(
        io.BytesIO(file_bytes),
        mimetype=file_storage.mimetype or "application/octet-stream",
        resumable=False,
    )

    uploaded = service.files().create(
        body={
            "name": safe_name,
            "parents": [parent_drive_id],
        },
        media_body=media,
        fields="id,name,mimeType,size",
        supportsAllDrives=True,
    ).execute()

    return {
        "attachment_drive_id": uploaded.get("id"),
        "attachment_name": uploaded.get("name") or safe_name,
        "attachment_mime": uploaded.get("mimeType") or (file_storage.mimetype or "application/octet-stream"),
        "attachment_size": int(uploaded.get("size") or 0),
    }


def download_drive_file(drive_file_id: str):
    service = get_oauth_drive_service()

    meta = service.files().get(
        fileId=drive_file_id,
        fields="id,name,mimeType",
        supportsAllDrives=True,
    ).execute()

    request_obj = service.files().get_media(fileId=drive_file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request_obj)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    return {
        "bytes": fh.read(),
        "name": meta.get("name") or "attachment",
        "mime": meta.get("mimeType") or "application/octet-stream",
    }


def delete_drive_file_safe(drive_file_id: str):
    if not drive_file_id:
        return

    try:
        service = get_oauth_drive_service()
        service.files().delete(fileId=drive_file_id, supportsAllDrives=True).execute()
    except Exception:
        pass


# ======================
# MESSAGE HELPERS
# ======================
def get_active_user_brief_rows(conn):
    return conn.execute("""
        SELECT id, username, role, active, full_name, google_email
        FROM users
        WHERE active=1
        ORDER BY role DESC, COALESCE(full_name, username) ASC, username ASC
    """).fetchall()


def message_can_access_conversation(conn, conversation_id: int, user_id: int) -> bool:
    row = conn.execute("""
        SELECT 1
        FROM message_conversation_members
        WHERE conversation_id=%s AND user_id=%s AND active=1
        LIMIT 1
    """, (conversation_id, user_id)).fetchone()
    return row is not None


def ensure_direct_conversation(conn, user_a: int, user_b: int):
    low, high = sorted([int(user_a), int(user_b)])

    row = conn.execute("""
        SELECT mc.id
        FROM message_conversations mc
        JOIN message_conversation_members m1
          ON m1.conversation_id = mc.id AND m1.user_id=%s AND m1.active=1
        JOIN message_conversation_members m2
          ON m2.conversation_id = mc.id AND m2.user_id=%s AND m2.active=1
        WHERE mc.conversation_type='DIRECT' AND mc.active=1
        GROUP BY mc.id
        ORDER BY mc.id ASC
        LIMIT 1
    """, (low, high)).fetchone()

    if row:
        return int(row["id"])

    now = now_iso()

    row = conn.execute("""
        INSERT INTO message_conversations (conversation_type, title, created_at, created_by, active)
        VALUES ('DIRECT', '', %s, %s, 1)
        RETURNING id
    """, (now, session["user"])).fetchone()

    conversation_id = row["id"]

    for uid in (low, high):
        conn.execute("""
            INSERT INTO message_conversation_members (
                conversation_id, user_id, joined_at, active, last_read_message_id
            ) VALUES (%s,%s,%s,%s,NULL)
        """, (conversation_id, uid, now, 1))

    return int(conversation_id)


def get_conversation_last_message(conn, conversation_id: int):
    return conn.execute("""
        SELECT mm.id, mm.message_text, mm.created_at, mm.sender_user_id,
               mm.attachment_name, mm.attachment_mime, mm.attachment_drive_id, mm.deleted_at,
               u.username AS sender_username, u.full_name AS sender_full_name
        FROM message_messages mm
        LEFT JOIN users u ON u.id = mm.sender_user_id
        WHERE mm.conversation_id=%s AND mm.deleted_at IS NULL
        ORDER BY mm.id DESC
        LIMIT 1
    """, (conversation_id,)).fetchone()


def get_conversation_unread_count(conn, conversation_id: int, user_id: int) -> int:
    member = conn.execute("""
        SELECT last_read_message_id
        FROM message_conversation_members
        WHERE conversation_id=%s AND user_id=%s AND active=1
        LIMIT 1
    """, (conversation_id, user_id)).fetchone()

    last_read = int(member["last_read_message_id"] or 0) if member else 0

    row = conn.execute("""
        SELECT COUNT(*) AS c
        FROM message_messages
        WHERE conversation_id=%s AND deleted_at IS NULL AND id>%s AND sender_user_id<>%s
    """, (conversation_id, last_read, user_id)).fetchone()

    return int(row["c"] or 0)


def get_direct_other_user(conn, conversation_id: int, current_user_id: int):
    return conn.execute("""
        SELECT u.id, u.username, u.full_name, u.role, u.google_email
        FROM message_conversation_members m
        JOIN users u ON u.id = m.user_id
        WHERE m.conversation_id=%s
          AND m.active=1
          AND u.id<>%s
        LIMIT 1
    """, (conversation_id, current_user_id)).fetchone()


def get_bulk_conversation_metadata(conn, conversation_rows, user_id: int):
    conversation_ids = [int(r["id"]) for r in conversation_rows]
    if not conversation_ids:
        return {}, {}, {}

    in_clause = placeholders(len(conversation_ids))

    last_rows = conn.execute(f"""
        SELECT DISTINCT ON (mm.conversation_id)
               mm.conversation_id,
               mm.id,
               mm.message_text,
               mm.created_at,
               mm.sender_user_id,
               mm.attachment_name,
               mm.attachment_mime,
               mm.attachment_drive_id,
               mm.deleted_at,
               u.username AS sender_username,
               u.full_name AS sender_full_name
        FROM message_messages mm
        LEFT JOIN users u ON u.id = mm.sender_user_id
        WHERE mm.conversation_id IN ({in_clause})
          AND mm.deleted_at IS NULL
        ORDER BY mm.conversation_id, mm.id DESC
    """, tuple(conversation_ids)).fetchall()
    last_messages = {int(r["conversation_id"]): dict(r) for r in last_rows}

    unread_rows = conn.execute(f"""
        SELECT m.conversation_id, COUNT(mm.id) AS unread_count
        FROM message_conversation_members m
        LEFT JOIN message_messages mm
          ON mm.conversation_id = m.conversation_id
         AND mm.deleted_at IS NULL
         AND mm.id > COALESCE(m.last_read_message_id, 0)
         AND mm.sender_user_id <> %s
        WHERE m.user_id=%s
          AND m.active=1
          AND m.conversation_id IN ({in_clause})
        GROUP BY m.conversation_id
    """, (user_id, user_id, *conversation_ids)).fetchall()
    unread_counts = {int(r["conversation_id"]): int(r["unread_count"] or 0) for r in unread_rows}

    direct_ids = [
        int(r["id"])
        for r in conversation_rows
        if (r["conversation_type"] or "").upper() == "DIRECT"
    ]
    direct_users = {}
    if direct_ids:
        direct_in_clause = placeholders(len(direct_ids))
        other_rows = conn.execute(f"""
            SELECT m.conversation_id, u.id, u.username, u.full_name, u.role, u.google_email
            FROM message_conversation_members m
            JOIN users u ON u.id = m.user_id
            WHERE m.conversation_id IN ({direct_in_clause})
              AND m.active=1
              AND u.id<>%s
        """, (*direct_ids, user_id)).fetchall()
        direct_users = {int(r["conversation_id"]): dict(r) for r in other_rows}

    return last_messages, unread_counts, direct_users


def build_sidebar_conversation_payloads(conn, conversation_rows, user_id: int):
    last_messages, unread_counts, direct_users = get_bulk_conversation_metadata(conn, conversation_rows, user_id)
    payloads = []

    for conversation_row in conversation_rows:
        convo = dict(conversation_row)
        convo_id = int(convo["id"])
        last_msg = last_messages.get(convo_id)
        unread_count = unread_counts.get(convo_id, 0)

        payload = {
            "id": convo["id"],
            "conversation_type": convo["conversation_type"],
            "title": convo.get("title") or "",
            "created_at": convo["created_at"],
            "created_by": convo["created_by"],
            "active": convo["active"],
            "unread_count": unread_count,
            "last_message": dict(last_msg) if last_msg else None,
            "last_message_text": "",
            "last_message_at": "",
            "display_name": "",
            "other_user_name": None,
        }

        if last_msg:
            payload["last_message_text"] = last_msg["message_text"] or last_msg["attachment_name"] or "Attachment"
            payload["last_message_at"] = last_msg["created_at"] or ""

        if (convo["conversation_type"] or "").upper() == "DIRECT":
            other = direct_users.get(convo_id)
            display_name = (other["full_name"] or other["username"]) if other else "Direct Chat"
            payload["display_name"] = display_name
            payload["other_user_name"] = display_name
            payload["title"] = display_name
        else:
            payload["display_name"] = convo.get("title") or "Group Chat"

        payloads.append(payload)

    return payloads


def serialize_sidebar_conversation(conn, conversation_row, user_id: int):
    return build_sidebar_conversation_payloads(conn, [conversation_row], user_id)[0]


def serialize_conversation_detail(conn, conversation_row, user_id: int):
    convo = serialize_sidebar_conversation(conn, conversation_row, user_id)

    members = conn.execute("""
        SELECT u.id, u.username, u.full_name, u.role, u.google_email
        FROM message_conversation_members m
        JOIN users u ON u.id = m.user_id
        WHERE m.conversation_id=%s AND m.active=1
        ORDER BY u.role DESC, COALESCE(u.full_name, u.username) ASC
    """, (convo["id"],)).fetchall()

    convo["members"] = [dict(r) for r in members]
    convo["member_count"] = len(convo["members"])
    return convo


def mark_conversation_read(conn, conversation_id: int, user_id: int):
    last_msg = conn.execute("""
        SELECT id
        FROM message_messages
        WHERE conversation_id=%s AND deleted_at IS NULL
        ORDER BY id DESC
        LIMIT 1
    """, (conversation_id,)).fetchone()

    last_id = int(last_msg["id"]) if last_msg else None

    conn.execute("""
        UPDATE message_conversation_members
        SET last_read_message_id=%s
        WHERE conversation_id=%s AND user_id=%s
    """, (last_id, conversation_id, user_id))


def can_delete_message(message_row, current_user_id: int) -> bool:
    if int(current_user_id) == int(message_row["sender_user_id"] or 0):
        created_at = datetime.fromisoformat(message_row["created_at"])
        age_seconds = (datetime.now() - created_at).total_seconds()
        return age_seconds <= 900
    return is_admin()


def enrich_message_row(row):
    item = dict(row)
    attachment_drive_id = item.get("attachment_drive_id")
    attachment_url = item.get("attachment_url")

    if attachment_drive_id:
        item["attachment_url"] = url_for("messages.api_messages_attachment_download", message_id=item["id"])
    elif attachment_url:
        item["attachment_url"] = attachment_url
    else:
        item["attachment_url"] = None

    return item


# ==========================
# MESSAGES API
# ==========================
@messages_bp.route("/api/messages/users", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_users():
    conn = db()
    rows = get_active_user_brief_rows(conn)
    me_id = int(session.get("uid"))
    out = [dict(r) for r in rows if int(r["id"]) != me_id]
    conn.close()
    return jsonify({"ok": True, "data": out})


@messages_bp.route("/api/messages/unread-count", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_unread_count():
    conn = db()
    uid = int(session.get("uid"))

    row = conn.execute("""
        SELECT COUNT(mm.id) AS unread_count
        FROM message_conversation_members m
        JOIN message_conversations mc
          ON mc.id = m.conversation_id
        JOIN message_messages mm
          ON mm.conversation_id = mc.id
         AND mm.deleted_at IS NULL
         AND mm.id > COALESCE(m.last_read_message_id, 0)
         AND mm.sender_user_id <> %s
        WHERE mc.active=1
          AND m.user_id=%s
          AND m.active=1
          AND NOT EXISTS (
              SELECT 1
              FROM message_conversation_deleted d
              WHERE d.conversation_id = mc.id
                AND d.user_id = %s
                AND COALESCE(d.active,1)=1
          )
    """, (uid, uid, uid)).fetchone()

    conn.close()
    return jsonify({"ok": True, "data": {"unread_count": int(row["unread_count"] or 0)}})


@messages_bp.route("/api/messages/conversations", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_conversations():
    conn = db()
    uid = int(session.get("uid"))

    rows = conn.execute("""
        SELECT mc.*
        FROM message_conversations mc
        JOIN message_conversation_members m
          ON m.conversation_id = mc.id
        WHERE mc.active=1
          AND m.user_id=%s
          AND m.active=1
          AND NOT EXISTS (
              SELECT 1
              FROM message_conversation_deleted d
              WHERE d.conversation_id = mc.id
                AND d.user_id = %s
                AND COALESCE(d.active,1)=1
          )
        ORDER BY mc.id DESC
    """, (uid, uid)).fetchall()

    data = build_sidebar_conversation_payloads(conn, rows, uid)
    data.sort(key=lambda x: (x["last_message_at"] or x["created_at"] or ""), reverse=True)

    conn.close()
    return jsonify({"ok": True, "data": data})


@messages_bp.route("/api/messages/direct", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_create_direct():
    data = request.json or {}
    target_user_id = data.get("user_id")

    try:
        target_user_id = int(target_user_id)
    except Exception:
        return jsonify({"ok": False, "error": "Valid user_id is required"}), 400

    uid = int(session.get("uid"))
    if target_user_id == uid:
        return jsonify({"ok": False, "error": "You cannot message yourself here"}), 400

    conn = db()
    target = conn.execute("""
        SELECT id, active, username, full_name
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (target_user_id,)).fetchone()

    if not target or int(target["active"] or 0) != 1:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    conversation_id = ensure_direct_conversation(conn, uid, target_user_id)

    conn.execute("""
        UPDATE message_conversation_deleted
        SET active=0
        WHERE conversation_id=%s AND user_id=%s
    """, (conversation_id, uid))

    conn.commit()

    row = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=%s
    """, (conversation_id,)).fetchone()

    payload = serialize_conversation_detail(conn, row, uid)
    payload["other_user_name"] = target["full_name"] or target["username"]
    payload["title"] = target["full_name"] or target["username"]

    conn.close()
    return jsonify({"ok": True, "data": payload})


@messages_bp.route("/api/messages/groups", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_create_group():
    data = request.json or {}
    title = (data.get("title") or "").strip()
    member_user_ids = data.get("member_user_ids") or []

    if not title:
        return jsonify({"ok": False, "error": "Group title is required"}), 400

    clean_ids = set()
    for x in member_user_ids:
        try:
            clean_ids.add(int(x))
        except Exception:
            continue

    clean_ids.add(int(session.get("uid")))

    conn = db()

    if not clean_ids:
        conn.close()
        return jsonify({"ok": False, "error": "No members selected"}), 400

    in_clause = placeholders(len(clean_ids))
    existing = conn.execute(f"""
        SELECT id
        FROM users
        WHERE active=1
          AND id IN ({in_clause})
    """, tuple(clean_ids)).fetchall()

    existing_ids = {int(r["id"]) for r in existing}

    if int(session.get("uid")) not in existing_ids:
        conn.close()
        return jsonify({"ok": False, "error": "Current user missing"}), 400

    now = now_iso()

    row = conn.execute("""
        INSERT INTO message_conversations (
            conversation_type,
            title,
            created_at,
            created_by,
            active
        ) VALUES ('GROUP', %s, %s, %s, 1)
        RETURNING id
    """, (title, now, session["user"])).fetchone()

    cid = row["id"]

    for member_uid in sorted(existing_ids):
        conn.execute("""
            INSERT INTO message_conversation_members (
                conversation_id,
                user_id,
                joined_at,
                active,
                last_read_message_id
            ) VALUES (%s,%s,%s,%s,NULL)
        """, (cid, member_uid, now, 1))

    conn.commit()

    row = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=%s
    """, (cid,)).fetchone()

    payload = serialize_conversation_detail(conn, row, int(session.get("uid")))
    conn.close()

    return jsonify({"ok": True, "data": payload})


@messages_bp.route("/api/messages/groups/<int:conversation_id>/add-members", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_group_add_members(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    convo = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=%s AND active=1
        LIMIT 1
    """, (conversation_id,)).fetchone()

    if not convo:
        conn.close()
        return jsonify({"ok": False, "error": "Conversation not found"}), 404

    if (convo["conversation_type"] or "").upper() != "GROUP":
        conn.close()
        return jsonify({"ok": False, "error": "This is not a group"}), 400

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    data = request.json or {}
    member_user_ids = data.get("member_user_ids") or []

    clean_ids = set()
    for x in member_user_ids:
        try:
            clean_ids.add(int(x))
        except Exception:
            continue

    if not clean_ids:
        conn.close()
        return jsonify({"ok": False, "error": "No members selected"}), 400

    in_clause = placeholders(len(clean_ids))
    rows = conn.execute(f"""
        SELECT id
        FROM users
        WHERE active=1
          AND id IN ({in_clause})
    """, tuple(clean_ids)).fetchall()

    valid_ids = {int(r["id"]) for r in rows}
    now = now_iso()

    for member_uid in valid_ids:
        exists = conn.execute("""
            SELECT id
            FROM message_conversation_members
            WHERE conversation_id=%s AND user_id=%s
            LIMIT 1
        """, (conversation_id, member_uid)).fetchone()

        if exists:
            conn.execute("""
                UPDATE message_conversation_members
                SET active=1
                WHERE conversation_id=%s AND user_id=%s
            """, (conversation_id, member_uid))
        else:
            conn.execute("""
                INSERT INTO message_conversation_members (
                    conversation_id,
                    user_id,
                joined_at,
                active,
                last_read_message_id
                ) VALUES (%s,%s,%s,%s,NULL)
            """, (conversation_id, member_uid, now, 1))

        conn.execute("""
            UPDATE message_conversation_deleted
            SET active=0
            WHERE conversation_id=%s AND user_id=%s
        """, (conversation_id, member_uid))

    # refresh Drive folder sharing if folder already exists
    try:
        service = get_oauth_drive_service()
        ensure_conversation_drive_folder(service, conn, conversation_id)
    except Exception:
        pass

    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@messages_bp.route("/api/messages/conversations/<int:conversation_id>", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_conversation_get(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    row = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=%s
          AND active=1
    """, (conversation_id,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Conversation not found"}), 404

    payload = serialize_conversation_detail(conn, row, uid)
    conn.close()
    return jsonify({"ok": True, "data": payload})


@messages_bp.route("/api/messages/conversations/<int:conversation_id>/members", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_conversation_members(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    rows = conn.execute("""
        SELECT u.id, u.username, u.full_name, u.role, u.google_email
        FROM message_conversation_members m
        JOIN users u ON u.id = m.user_id
        WHERE m.conversation_id=%s
          AND m.active=1
        ORDER BY COALESCE(u.full_name, u.username) ASC
    """, (conversation_id,)).fetchall()

    conn.close()
    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


@messages_bp.route("/api/messages/conversations/<int:conversation_id>/messages", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_list(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    rows = conn.execute("""
        SELECT
            mm.*,
            u.username AS sender_username,
            u.full_name AS sender_full_name
        FROM message_messages mm
        JOIN users u
          ON u.id = mm.sender_user_id
        WHERE mm.conversation_id=%s
        ORDER BY mm.id ASC
        LIMIT 500
    """, (conversation_id,)).fetchall()

    mark_conversation_read(conn, conversation_id, uid)
    conn.commit()

    data = []
    for r in rows:
        item = enrich_message_row(r)
        item["can_delete"] = False if item.get("deleted_at") else can_delete_message(r, uid)
        data.append(item)

    conn.close()
    return jsonify({"ok": True, "data": data})


@messages_bp.route("/api/messages/conversations/<int:conversation_id>/read", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_mark_read(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    mark_conversation_read(conn, conversation_id, uid)
    conn.commit()
    conn.close()

    return jsonify({"ok": True})


@messages_bp.route("/api/messages/conversations/<int:conversation_id>/messages", methods=["POST"])
@login_required
@require_module("MESSAGES", need_edit=True)
def api_messages_send(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    message_text = ""
    attachment_name = None
    attachment_mime = None
    attachment_drive_id = None
    reply_to = None

    if request.content_type and request.content_type.startswith("multipart/form-data"):
        message_text = (request.form.get("message_text") or "").strip()
        reply_to = request.form.get("reply_to")
        file = request.files.get("file")

        if file and getattr(file, "filename", ""):
            uploaded = upload_message_attachment_to_drive(conn, file, conversation_id)
            if uploaded:
                attachment_name = uploaded["attachment_name"]
                attachment_mime = uploaded["attachment_mime"]
                attachment_drive_id = uploaded["attachment_drive_id"]
    else:
        data = request.json or {}
        message_text = (data.get("message_text") or "").strip()
        reply_to = data.get("reply_to")

    if reply_to in ("", None):
        reply_to = None
    else:
        try:
            reply_to = int(reply_to)
        except Exception:
            reply_to = None

    if not message_text and not attachment_drive_id:
        conn.close()
        return jsonify({"ok": False, "error": "Message or attachment is required"}), 400

    now = now_iso()

    row = conn.execute("""
        INSERT INTO message_messages (
            conversation_id,
            sender_user_id,
            message_text,
            reply_to,
            attachment_name,
            attachment_url,
            attachment_mime,
            attachment_drive_id,
            created_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """, (
        conversation_id,
        uid,
        message_text,
        reply_to,
        attachment_name,
        None,
        attachment_mime,
        attachment_drive_id,
        now
    )).fetchone()

    message_id = row["id"]

    conn.execute("""
        UPDATE message_conversation_members
        SET last_read_message_id=%s
        WHERE conversation_id=%s AND user_id=%s
    """, (message_id, conversation_id, uid))

    conn.commit()

    row = conn.execute("""
        SELECT
            mm.*,
            u.username AS sender_username,
            u.full_name AS sender_full_name
        FROM message_messages mm
        JOIN users u
          ON u.id = mm.sender_user_id
        WHERE mm.id=%s
    """, (message_id,)).fetchone()

    payload = enrich_message_row(row)
    payload["can_delete"] = can_delete_message(row, uid)

    conn.close()
    return jsonify({"ok": True, "data": payload})


@messages_bp.route("/api/messages/attachments/<int:message_id>/download", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_attachment_download(message_id):
    conn = db()
    uid = int(session.get("uid"))

    row = conn.execute("""
        SELECT mm.id, mm.conversation_id, mm.attachment_name, mm.attachment_mime, mm.attachment_drive_id
        FROM message_messages mm
        WHERE mm.id=%s
        LIMIT 1
    """, (message_id,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Attachment not found"}), 404

    if not row["attachment_drive_id"]:
        conn.close()
        return jsonify({"ok": False, "error": "No secure attachment stored for this message"}), 404

    if not message_can_access_conversation(conn, int(row["conversation_id"]), uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    drive_file_id = row["attachment_drive_id"]
    download_name = row["attachment_name"] or "attachment"
    download_mime = row["attachment_mime"] or "application/octet-stream"
    conn.close()

    try:
        file_data = download_drive_file(drive_file_id)
        final_name = file_data.get("name") or download_name
        final_mime = file_data.get("mime") or download_mime

        response = Response(file_data["bytes"], mimetype=final_mime)
        response.headers["Content-Disposition"] = f'inline; filename="{final_name}"'
        return response
    except Exception as e:
        return jsonify({"ok": False, "error": f"Could not open attachment: {str(e)}"}), 500


@messages_bp.route("/api/messages/messages/<int:message_id>", methods=["DELETE"])
@login_required
@require_module("MESSAGES", need_edit=True)
def api_messages_delete(message_id):
    conn = db()

    row = conn.execute("""
        SELECT *
        FROM message_messages
        WHERE id=%s
        LIMIT 1
    """, (message_id,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Message not found"}), 404

    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, int(row["conversation_id"]), uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    if row["deleted_at"]:
        conn.close()
        return jsonify({"ok": True})

    if not can_delete_message(row, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Delete window expired"}), 400

    if row["attachment_drive_id"]:
        delete_drive_file_safe(row["attachment_drive_id"])

    conn.execute("""
        UPDATE message_messages
        SET deleted_at=%s,
            deleted_by=%s,
            message_text='This message was deleted',
            attachment_name=NULL,
            attachment_url=NULL,
            attachment_mime=NULL,
            attachment_drive_id=NULL
        WHERE id=%s
    """, (now_iso(), session["user"], message_id))

    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@messages_bp.route("/api/messages/conversations/<int:conversation_id>/delete", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_delete_conversation(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    exists = conn.execute("""
        SELECT id
        FROM message_conversation_deleted
        WHERE conversation_id=%s AND user_id=%s
        LIMIT 1
    """, (conversation_id, uid)).fetchone()

    if exists:
        conn.execute("""
            UPDATE message_conversation_deleted
            SET deleted_at=%s, deleted_by=%s, active=1
            WHERE conversation_id=%s AND user_id=%s
        """, (now_iso(), session["user"], conversation_id, uid))
    else:
        conn.execute("""
            INSERT INTO message_conversation_deleted (
                conversation_id,
                user_id,
                deleted_at,
                deleted_by,
                active
            ) VALUES (%s,%s,%s,%s,1)
        """, (conversation_id, uid, now_iso(), session["user"]))

    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@messages_bp.route("/api/messages/trash", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_trash():
    conn = db()
    uid = int(session.get("uid"))

    rows = conn.execute("""
        SELECT mc.*, d.deleted_at AS user_deleted_at
        FROM message_conversation_deleted d
        JOIN message_conversations mc ON mc.id = d.conversation_id
        WHERE d.user_id=%s
          AND COALESCE(d.active,1)=1
        ORDER BY d.deleted_at DESC, mc.id DESC
    """, (uid,)).fetchall()

    data = build_sidebar_conversation_payloads(conn, rows, uid)
    deleted_at_map = {int(r["id"]): r["user_deleted_at"] for r in rows}
    for item in data:
        item["user_deleted_at"] = deleted_at_map.get(int(item["id"]))

    conn.close()
    return jsonify({"ok": True, "data": data})


@messages_bp.route("/api/messages/conversations/<int:conversation_id>/restore", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_restore_conversation(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    conn.execute("""
        UPDATE message_conversation_deleted
        SET active=0
        WHERE conversation_id=%s AND user_id=%s
    """, (conversation_id, uid))

    conn.commit()
    conn.close()

    return jsonify({"ok": True})


@messages_bp.route("/api/messages/conversations/<int:conversation_id>/permanent-delete", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_permanent_delete_conversation(conversation_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()

    rows = conn.execute("""
        SELECT attachment_drive_id
        FROM message_messages
        WHERE conversation_id=%s
          AND attachment_drive_id IS NOT NULL
    """, (conversation_id,)).fetchall()

    for r in rows:
        if r["attachment_drive_id"]:
            delete_drive_file_safe(r["attachment_drive_id"])

    conn.execute("DELETE FROM message_conversation_deleted WHERE conversation_id=%s", (conversation_id,))
    conn.execute("DELETE FROM message_conversation_members WHERE conversation_id=%s", (conversation_id,))
    conn.execute("DELETE FROM message_messages WHERE conversation_id=%s", (conversation_id,))
    conn.execute("DELETE FROM message_conversations WHERE id=%s", (conversation_id,))
    conn.commit()
    conn.close()

    return jsonify({"ok": True})


@messages_bp.route("/api/messages/groups/<int:conversation_id>/leave", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_leave_group(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    convo = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=%s AND active=1
        LIMIT 1
    """, (conversation_id,)).fetchone()

    if not convo:
        conn.close()
        return jsonify({"ok": False, "error": "Conversation not found"}), 404

    if (convo["conversation_type"] or "").upper() != "GROUP":
        conn.close()
        return jsonify({"ok": False, "error": "This is not a group"}), 400

    conn.execute("""
        UPDATE message_conversation_members
        SET active=0
        WHERE conversation_id=%s AND user_id=%s
    """, (conversation_id, uid))

    exists = conn.execute("""
        SELECT id
        FROM message_conversation_deleted
        WHERE conversation_id=%s AND user_id=%s
        LIMIT 1
    """, (conversation_id, uid)).fetchone()

    if exists:
        conn.execute("""
            UPDATE message_conversation_deleted
            SET deleted_at=%s, deleted_by=%s, active=1
            WHERE conversation_id=%s AND user_id=%s
        """, (now_iso(), session["user"], conversation_id, uid))
    else:
        conn.execute("""
            INSERT INTO message_conversation_deleted (
                conversation_id,
                user_id,
                deleted_at,
                deleted_by,
                active
            ) VALUES (%s,%s,%s,%s,1)
        """, (conversation_id, uid, now_iso(), session["user"]))

    conn.commit()
    conn.close()
    return jsonify({"ok": True})
