from flask import Blueprint, request, session, jsonify, current_app, url_for, Response, redirect
from functools import wraps
from datetime import datetime
import io
import os
import json

from werkzeug.utils import secure_filename

from .db_compat import sqlite3

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
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
    conn = sqlite3.connect(database_url)
    try:
        conn.row_factory = sqlite3.Row
    except Exception:
        pass
    return conn


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
# Per-user OAuth for message attachments
# ======================
def _ensure_tmp_dir():
    os.makedirs("/tmp/erp_google", exist_ok=True)
    return "/tmp/erp_google"


def _looks_like_json(text: str) -> bool:
    if not text:
        return False
    s = text.strip()
    return s.startswith("{") and s.endswith("}")


def _materialize_json_or_path(value, tmp_filename, fallback_secret_path=None, required=False):
    tmp_dir = _ensure_tmp_dir()
    out_path = os.path.join(tmp_dir, tmp_filename)

    if value:
        value = str(value).strip()

        if os.path.exists(value):
            return value

        if _looks_like_json(value):
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(value)
            return out_path

    if fallback_secret_path and os.path.exists(fallback_secret_path):
        with open(fallback_secret_path, "r", encoding="utf-8") as src:
            with open(out_path, "w", encoding="utf-8") as dst:
                dst.write(src.read())
        return out_path

    if required:
        raise ValueError(f"Required Google secret not found: {tmp_filename}")

    return out_path


def oauth_client_file():
    cfg = current_app.config.get("GOOGLE_OAUTH_CLIENT_FILE") or os.environ.get("GOOGLE_OAUTH_CLIENT_FILE")
    return _materialize_json_or_path(
        cfg,
        "google-oauth-client.json",
        fallback_secret_path="/etc/secrets/google-oauth-client.json",
        required=True,
    )


def get_messages_oauth_redirect_uri():
    forced = (
        current_app.config.get("GOOGLE_MESSAGES_OAUTH_REDIRECT_URI")
        or os.environ.get("GOOGLE_MESSAGES_OAUTH_REDIRECT_URI")
        or ""
    ).strip()

    if forced:
        return forced

    proto = request.headers.get("X-Forwarded-Proto", request.scheme)
    host = request.headers.get("X-Forwarded-Host", request.host)
    return f"{proto}://{host}/messages/google-drive/callback"


def get_user_message_token_row(conn, user_id: int):
    return conn.execute("""
        SELECT *
        FROM user_google_oauth_tokens
        WHERE user_id=? AND provider='GOOGLE_DRIVE_MESSAGES'
        LIMIT 1
    """, (user_id,)).fetchone()


def save_user_message_token(conn, user_id: int, token_json: str, google_account_email: str = None):
    now = now_iso()
    conn.execute("""
        INSERT INTO user_google_oauth_tokens (
            user_id,
            provider,
            token_json,
            google_account_email,
            created_at,
            updated_at
        ) VALUES (?, 'GOOGLE_DRIVE_MESSAGES', ?, ?, ?, ?)
        ON CONFLICT(user_id)
        DO UPDATE SET
            provider=excluded.provider,
            token_json=excluded.token_json,
            google_account_email=excluded.google_account_email,
            updated_at=excluded.updated_at
    """, (
        user_id,
        token_json,
        (google_account_email or "").strip().lower() or None,
        now,
        now,
    ))


def delete_user_message_token(conn, user_id: int):
    conn.execute("""
        DELETE FROM user_google_oauth_tokens
        WHERE user_id=? AND provider='GOOGLE_DRIVE_MESSAGES'
    """, (user_id,))


def _credentials_from_token_json(token_json: str):
    try:
        payload = json.loads(token_json)
    except Exception as e:
        raise ValueError(f"Stored Google Drive token is invalid JSON: {str(e)}")

    try:
        return Credentials.from_authorized_user_info(payload, DRIVE_SCOPES)
    except Exception as e:
        raise ValueError(f"Stored Google Drive token is invalid: {str(e)}")


def get_connected_google_account_email(service):
    try:
        about = service.about().get(fields="user(emailAddress)").execute()
        user = about.get("user") or {}
        return (user.get("emailAddress") or "").strip().lower() or None
    except Exception:
        return None


def get_user_oauth_drive_service(conn, user_id: int, fail_message: str = None):
    token_row = get_user_message_token_row(conn, user_id)
    if not token_row:
        raise ValueError(fail_message or "Connect your Google Drive for messages first.")

    creds = _credentials_from_token_json(token_row["token_json"])

    try:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            service = build("drive", "v3", credentials=creds, cache_discovery=False)
            google_account_email = get_connected_google_account_email(service)
            save_user_message_token(conn, user_id, creds.to_json(), google_account_email)
            conn.commit()
            return service

        if not creds.valid:
            raise ValueError("Your Google Drive connection is invalid. Please reconnect it from Messages.")

        return build("drive", "v3", credentials=creds, cache_discovery=False)

    except ValueError:
        raise
    except Exception:
        raise ValueError("Your Google Drive connection expired or was revoked. Please reconnect it from Messages.")


def get_connected_conversation_user_ids(conn, conversation_id: int):
    rows = conn.execute("""
        SELECT DISTINCT m.user_id
        FROM message_conversation_members m
        JOIN user_google_oauth_tokens t
          ON t.user_id = m.user_id
         AND t.provider = 'GOOGLE_DRIVE_MESSAGES'
        WHERE m.conversation_id=?
          AND m.active=1
        ORDER BY m.user_id ASC
    """, (conversation_id,)).fetchall()
    return [int(r["user_id"]) for r in rows]


def get_drive_candidates_for_conversation(conn, conversation_id: int, preferred_user_id=None, attachment_owner_user_id=None):
    ordered = []

    def add(uid):
        if uid in (None, "", 0):
            return
        try:
            uid = int(uid)
        except Exception:
            return
        if uid not in ordered:
            ordered.append(uid)

    add(preferred_user_id)
    add(attachment_owner_user_id)

    for uid in get_connected_conversation_user_ids(conn, conversation_id):
        add(uid)

    return ordered


def get_working_drive_service_for_candidates(conn, candidate_user_ids):
    last_error = None

    for candidate_user_id in candidate_user_ids:
        try:
            service = get_user_oauth_drive_service(conn, candidate_user_id)
            return candidate_user_id, service
        except Exception as e:
            last_error = e

    if last_error:
        raise last_error

    raise ValueError("No connected Google Drive account is available for this conversation.")


@messages_bp.route("/api/messages/google-drive/status", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_google_drive_status():
    conn = db()
    uid = int(session.get("uid"))
    token_row = get_user_message_token_row(conn, uid)
    user_row = conn.execute("""
        SELECT google_email
        FROM users
        WHERE id=?
        LIMIT 1
    """, (uid,)).fetchone()
    conn.close()

    return jsonify({
        "ok": True,
        "data": {
            "connected": token_row is not None,
            "google_account_email": (token_row["google_account_email"] if token_row else None),
            "saved_google_email": (user_row["google_email"] if user_row else None),
        }
    })


@messages_bp.route("/messages/google-drive/connect", methods=["GET"])
@login_required
@require_module("MESSAGES")
def messages_google_drive_connect():
    try:
        flow = Flow.from_client_secrets_file(
            oauth_client_file(),
            scopes=DRIVE_SCOPES,
        )
        flow.redirect_uri = get_messages_oauth_redirect_uri()

        authorization_url, state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",
            code_challenge_method="S256",
        )

        session["messages_google_drive_oauth_state"] = state
        session["messages_google_drive_code_verifier"] = flow.code_verifier

        return redirect(authorization_url)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Google Drive connect failed: {str(e)}"}), 500


@messages_bp.route("/messages/google-drive/callback", methods=["GET"])
@login_required
@require_module("MESSAGES")
def messages_google_drive_callback():
    state = session.get("messages_google_drive_oauth_state")
    code_verifier = session.get("messages_google_drive_code_verifier")

    if not state:
        return jsonify({"ok": False, "error": "Missing OAuth state. Please connect again."}), 400

    if not code_verifier:
        return jsonify({"ok": False, "error": "Missing OAuth code verifier. Please connect again."}), 400

    conn = db()

    try:
        flow = Flow.from_client_secrets_file(
            oauth_client_file(),
            scopes=DRIVE_SCOPES,
            state=state,
        )
        flow.redirect_uri = get_messages_oauth_redirect_uri()
        flow.code_verifier = code_verifier
        flow.fetch_token(authorization_response=request.url)

        creds = flow.credentials
        service = build("drive", "v3", credentials=creds, cache_discovery=False)
        google_account_email = get_connected_google_account_email(service)

        save_user_message_token(conn, int(session.get("uid")), creds.to_json(), google_account_email)
        conn.commit()

        session.pop("messages_google_drive_oauth_state", None)
        session.pop("messages_google_drive_code_verifier", None)

        return redirect("/messages")
    except Exception as e:
        conn.rollback()
        session.pop("messages_google_drive_oauth_state", None)
        session.pop("messages_google_drive_code_verifier", None)
        return jsonify({"ok": False, "error": f"Google Drive callback failed: {str(e)}"}), 500
    finally:
        conn.close()


@messages_bp.route("/api/messages/google-drive/disconnect", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_google_drive_disconnect():
    conn = db()
    delete_user_message_token(conn, int(session.get("uid")))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


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
        WHERE m.conversation_id=?
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


def refresh_conversation_folder_sharing(conn, conversation_id: int, preferred_user_id=None, attachment_owner_user_id=None):
    candidate_user_ids = get_drive_candidates_for_conversation(
        conn,
        conversation_id,
        preferred_user_id=preferred_user_id,
        attachment_owner_user_id=attachment_owner_user_id,
    )
    last_error = None

    for candidate_user_id in candidate_user_ids:
        try:
            service = get_user_oauth_drive_service(conn, candidate_user_id)
            ensure_conversation_drive_folder(service, conn, conversation_id)
            return
        except Exception as e:
            last_error = e

    if last_error:
        raise last_error

    raise ValueError("No connected Google Drive account is available for this conversation.")


def upload_message_attachment_to_drive(conn, file_storage, conversation_id: int, acting_user_id: int):
    if not file_storage or not getattr(file_storage, "filename", ""):
        return None

    service = get_user_oauth_drive_service(
        conn,
        acting_user_id,
        fail_message="Connect your Google Drive in Messages before sending attachments.",
    )
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


def download_drive_file(conn, drive_file_id: str, conversation_id: int, preferred_user_id=None, attachment_owner_user_id=None):
    candidate_user_ids = get_drive_candidates_for_conversation(
        conn,
        conversation_id,
        preferred_user_id=preferred_user_id,
        attachment_owner_user_id=attachment_owner_user_id,
    )
    last_error = None

    for candidate_user_id in candidate_user_ids:
        try:
            service = get_user_oauth_drive_service(conn, candidate_user_id)

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
        except Exception as e:
            last_error = e

    if last_error:
        raise last_error

    raise ValueError("No connected Google Drive account can open this attachment.")


def delete_drive_file_safe(conn, drive_file_id: str, conversation_id: int = None, preferred_user_id=None, attachment_owner_user_id=None):
    if not drive_file_id:
        return

    candidate_user_ids = []

    if conversation_id is not None:
        candidate_user_ids = get_drive_candidates_for_conversation(
            conn,
            conversation_id,
            preferred_user_id=preferred_user_id,
            attachment_owner_user_id=attachment_owner_user_id,
        )
    else:
        for uid in (preferred_user_id, attachment_owner_user_id):
            if uid:
                candidate_user_ids.append(int(uid))

    for candidate_user_id in candidate_user_ids:
        try:
            service = get_user_oauth_drive_service(conn, candidate_user_id)
            service.files().delete(fileId=drive_file_id, supportsAllDrives=True).execute()
            return
        except Exception:
            continue


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
        WHERE conversation_id=? AND user_id=? AND active=1
        LIMIT 1
    """, (conversation_id, user_id)).fetchone()
    return row is not None


def ensure_direct_conversation(conn, user_a: int, user_b: int):
    low, high = sorted([int(user_a), int(user_b)])

    row = conn.execute("""
        SELECT mc.id
        FROM message_conversations mc
        JOIN message_conversation_members m1
          ON m1.conversation_id = mc.id AND m1.user_id=? AND m1.active=1
        JOIN message_conversation_members m2
          ON m2.conversation_id = mc.id AND m2.user_id=? AND m2.active=1
        WHERE mc.conversation_type='DIRECT' AND mc.active=1
        GROUP BY mc.id
        ORDER BY mc.id ASC
        LIMIT 1
    """, (low, high)).fetchone()

    if row:
        return int(row["id"])

    now = now_iso()

    conn.execute("""
        INSERT INTO message_conversations (conversation_type, title, created_at, created_by, active)
        VALUES ('DIRECT', '', ?, ?, 1)
    """, (now, session["user"]))

    conversation_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

    for uid in (low, high):
        conn.execute("""
            INSERT INTO message_conversation_members (
                conversation_id, user_id, joined_at, active, last_read_message_id
            ) VALUES (?,?,?,?,NULL)
        """, (conversation_id, uid, now, 1))

    return int(conversation_id)


def get_conversation_last_message(conn, conversation_id: int):
    return conn.execute("""
        SELECT mm.id, mm.message_text, mm.created_at, mm.sender_user_id,
               mm.attachment_name, mm.attachment_mime, mm.attachment_drive_id, mm.deleted_at,
               u.username AS sender_username, u.full_name AS sender_full_name
        FROM message_messages mm
        LEFT JOIN users u ON u.id = mm.sender_user_id
        WHERE mm.conversation_id=? AND mm.deleted_at IS NULL
        ORDER BY mm.id DESC
        LIMIT 1
    """, (conversation_id,)).fetchone()


def get_conversation_unread_count(conn, conversation_id: int, user_id: int) -> int:
    member = conn.execute("""
        SELECT last_read_message_id
        FROM message_conversation_members
        WHERE conversation_id=? AND user_id=? AND active=1
        LIMIT 1
    """, (conversation_id, user_id)).fetchone()

    last_read = int(member["last_read_message_id"] or 0) if member else 0

    row = conn.execute("""
        SELECT COUNT(*) AS c
        FROM message_messages
        WHERE conversation_id=? AND deleted_at IS NULL AND id>? AND sender_user_id<>?
    """, (conversation_id, last_read, user_id)).fetchone()

    return int(row["c"] or 0)


def get_direct_other_user(conn, conversation_id: int, current_user_id: int):
    return conn.execute("""
        SELECT u.id, u.username, u.full_name, u.role, u.google_email
        FROM message_conversation_members m
        JOIN users u ON u.id = m.user_id
        WHERE m.conversation_id=?
          AND m.active=1
          AND u.id<>?
        LIMIT 1
    """, (conversation_id, current_user_id)).fetchone()


def serialize_sidebar_conversation(conn, conversation_row, user_id: int):
    convo = dict(conversation_row)

    last_msg = get_conversation_last_message(conn, convo["id"])
    unread_count = get_conversation_unread_count(conn, convo["id"], user_id)

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
        other = get_direct_other_user(conn, convo["id"], user_id)
        display_name = (other["full_name"] or other["username"]) if other else "Direct Chat"
        payload["display_name"] = display_name
        payload["other_user_name"] = display_name
        payload["title"] = display_name
    else:
        payload["display_name"] = convo.get("title") or "Group Chat"

    return payload


def serialize_conversation_detail(conn, conversation_row, user_id: int):
    convo = serialize_sidebar_conversation(conn, conversation_row, user_id)

    members = conn.execute("""
        SELECT u.id, u.username, u.full_name, u.role, u.google_email
        FROM message_conversation_members m
        JOIN users u ON u.id = m.user_id
        WHERE m.conversation_id=? AND m.active=1
        ORDER BY u.role DESC, COALESCE(u.full_name, u.username) ASC
    """, (convo["id"],)).fetchall()

    convo["members"] = [dict(r) for r in members]
    convo["member_count"] = len(convo["members"])
    return convo


def mark_conversation_read(conn, conversation_id: int, user_id: int):
    last_msg = conn.execute("""
        SELECT id
        FROM message_messages
        WHERE conversation_id=? AND deleted_at IS NULL
        ORDER BY id DESC
        LIMIT 1
    """, (conversation_id,)).fetchone()

    last_id = int(last_msg["id"]) if last_msg else None

    conn.execute("""
        UPDATE message_conversation_members
        SET last_read_message_id=?
        WHERE conversation_id=? AND user_id=?
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

    rows = conn.execute("""
        SELECT mc.id
        FROM message_conversations mc
        JOIN message_conversation_members m
          ON m.conversation_id = mc.id
        WHERE mc.active=1
          AND m.user_id=?
          AND m.active=1
          AND NOT EXISTS (
              SELECT 1
              FROM message_conversation_deleted d
              WHERE d.conversation_id = mc.id
                AND d.user_id = ?
                AND COALESCE(d.active,1)=1
          )
    """, (uid, uid)).fetchall()

    total = 0
    for r in rows:
        total += get_conversation_unread_count(conn, int(r["id"]), uid)

    conn.close()
    return jsonify({"ok": True, "data": {"unread_count": total}})


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
          AND m.user_id=?
          AND m.active=1
          AND NOT EXISTS (
              SELECT 1
              FROM message_conversation_deleted d
              WHERE d.conversation_id = mc.id
                AND d.user_id = ?
                AND COALESCE(d.active,1)=1
          )
        ORDER BY mc.id DESC
    """, (uid, uid)).fetchall()

    data = [serialize_sidebar_conversation(conn, r, uid) for r in rows]
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
        WHERE id=?
        LIMIT 1
    """, (target_user_id,)).fetchone()

    if not target or int(target["active"] or 0) != 1:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    conversation_id = ensure_direct_conversation(conn, uid, target_user_id)

    conn.execute("""
        UPDATE message_conversation_deleted
        SET active=0
        WHERE conversation_id=? AND user_id=?
    """, (conversation_id, uid))

    conn.commit()

    row = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=?
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

    placeholders = ",".join("?" for _ in clean_ids)
    existing = conn.execute(f"""
        SELECT id
        FROM users
        WHERE active=1
          AND id IN ({placeholders})
    """, tuple(clean_ids)).fetchall()

    existing_ids = {int(r["id"]) for r in existing}

    if int(session.get("uid")) not in existing_ids:
        conn.close()
        return jsonify({"ok": False, "error": "Current user missing"}), 400

    now = now_iso()

    conn.execute("""
        INSERT INTO message_conversations (
            conversation_type,
            title,
            created_at,
            created_by,
            active
        ) VALUES ('GROUP', ?, ?, ?, 1)
    """, (title, now, session["user"]))

    cid = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

    for member_uid in sorted(existing_ids):
        conn.execute("""
            INSERT INTO message_conversation_members (
                conversation_id,
                user_id,
                joined_at,
                active,
                last_read_message_id
            ) VALUES (?,?,?,?,NULL)
        """, (cid, member_uid, now, 1))

    conn.commit()

    row = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=?
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
        WHERE id=? AND active=1
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

    placeholders = ",".join("?" for _ in clean_ids)
    rows = conn.execute(f"""
        SELECT id
        FROM users
        WHERE active=1
          AND id IN ({placeholders})
    """, tuple(clean_ids)).fetchall()

    valid_ids = {int(r["id"]) for r in rows}
    now = now_iso()

    for member_uid in valid_ids:
        exists = conn.execute("""
            SELECT id
            FROM message_conversation_members
            WHERE conversation_id=? AND user_id=?
            LIMIT 1
        """, (conversation_id, member_uid)).fetchone()

        if exists:
            conn.execute("""
                UPDATE message_conversation_members
                SET active=1
                WHERE conversation_id=? AND user_id=?
            """, (conversation_id, member_uid))
        else:
            conn.execute("""
                INSERT INTO message_conversation_members (
                    conversation_id,
                    user_id,
                    joined_at,
                    active,
                    last_read_message_id
                ) VALUES (?,?,?,?,NULL)
            """, (conversation_id, member_uid, now, 1))

        conn.execute("""
            UPDATE message_conversation_deleted
            SET active=0
            WHERE conversation_id=? AND user_id=?
        """, (conversation_id, member_uid))

    try:
        refresh_conversation_folder_sharing(conn, conversation_id, preferred_user_id=uid)
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
        WHERE id=?
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
        WHERE m.conversation_id=?
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
        WHERE mm.conversation_id=?
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
            try:
                uploaded = upload_message_attachment_to_drive(conn, file, conversation_id, uid)
                if uploaded:
                    attachment_name = uploaded["attachment_name"]
                    attachment_mime = uploaded["attachment_mime"]
                    attachment_drive_id = uploaded["attachment_drive_id"]
            except ValueError as e:
                conn.close()
                return jsonify({"ok": False, "error": str(e)}), 400
            except Exception as e:
                conn.close()
                return jsonify({"ok": False, "error": f"Attachment upload failed: {str(e)}"}), 500
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

    conn.execute("""
        INSERT INTO message_messages (
            conversation_id,
            sender_user_id,
            message_text,
            reply_to,
            attachment_name,
            attachment_url,
            attachment_mime,
            attachment_drive_id,
            attachment_owner_user_id,
            created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        conversation_id,
        uid,
        message_text,
        reply_to,
        attachment_name,
        None,
        attachment_mime,
        attachment_drive_id,
        uid if attachment_drive_id else None,
        now
    ))

    message_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

    conn.execute("""
        UPDATE message_conversation_members
        SET last_read_message_id=?
        WHERE conversation_id=? AND user_id=?
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
        WHERE mm.id=?
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
        SELECT mm.id,
               mm.conversation_id,
               mm.attachment_name,
               mm.attachment_mime,
               mm.attachment_drive_id,
               mm.attachment_owner_user_id
        FROM message_messages mm
        WHERE mm.id=?
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
    try:
        file_data = download_drive_file(
            conn,
            drive_file_id,
            int(row["conversation_id"]),
            preferred_user_id=uid,
            attachment_owner_user_id=row["attachment_owner_user_id"],
        )
        final_name = file_data.get("name") or download_name
        final_mime = file_data.get("mime") or download_mime

        response = Response(file_data["bytes"], mimetype=final_mime)
        response.headers["Content-Disposition"] = f'inline; filename="{final_name}"'
        conn.close()
        return response
    except Exception as e:
        conn.close()
        return jsonify({"ok": False, "error": f"Could not open attachment: {str(e)}"}), 500


@messages_bp.route("/api/messages/messages/<int:message_id>", methods=["DELETE"])
@login_required
@require_module("MESSAGES", need_edit=True)
def api_messages_delete(message_id):
    conn = db()

    row = conn.execute("""
        SELECT *
        FROM message_messages
        WHERE id=?
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
        delete_drive_file_safe(
            conn,
            row["attachment_drive_id"],
            conversation_id=int(row["conversation_id"]),
            preferred_user_id=uid,
            attachment_owner_user_id=row["attachment_owner_user_id"],
        )

    conn.execute("""
        UPDATE message_messages
        SET deleted_at=?,
            deleted_by=?,
            message_text='This message was deleted',
            attachment_name=NULL,
            attachment_url=NULL,
            attachment_mime=NULL,
            attachment_drive_id=NULL,
            attachment_owner_user_id=NULL
        WHERE id=?
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
        WHERE conversation_id=? AND user_id=?
        LIMIT 1
    """, (conversation_id, uid)).fetchone()

    if exists:
        conn.execute("""
            UPDATE message_conversation_deleted
            SET deleted_at=?, deleted_by=?, active=1
            WHERE conversation_id=? AND user_id=?
        """, (now_iso(), session["user"], conversation_id, uid))
    else:
        conn.execute("""
            INSERT INTO message_conversation_deleted (
                conversation_id,
                user_id,
                deleted_at,
                deleted_by,
                active
            ) VALUES (?,?,?,?,1)
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
        WHERE d.user_id=?
          AND COALESCE(d.active,1)=1
        ORDER BY d.deleted_at DESC, mc.id DESC
    """, (uid,)).fetchall()

    data = []
    for r in rows:
        item = serialize_sidebar_conversation(conn, r, uid)
        item["user_deleted_at"] = r["user_deleted_at"]
        data.append(item)

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
        WHERE conversation_id=? AND user_id=?
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
        SELECT attachment_drive_id, attachment_owner_user_id
        FROM message_messages
        WHERE conversation_id=?
          AND attachment_drive_id IS NOT NULL
    """, (conversation_id,)).fetchall()

    for r in rows:
        if r["attachment_drive_id"]:
            delete_drive_file_safe(
                conn,
                r["attachment_drive_id"],
                conversation_id=conversation_id,
                attachment_owner_user_id=r["attachment_owner_user_id"],
            )

    conn.execute("DELETE FROM message_conversation_deleted WHERE conversation_id=?", (conversation_id,))
    conn.execute("DELETE FROM message_conversation_members WHERE conversation_id=?", (conversation_id,))
    conn.execute("DELETE FROM message_messages WHERE conversation_id=?", (conversation_id,))
    conn.execute("DELETE FROM message_conversations WHERE id=?", (conversation_id,))
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
        WHERE id=? AND active=1
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
        WHERE conversation_id=? AND user_id=?
    """, (conversation_id, uid))

    exists = conn.execute("""
        SELECT id
        FROM message_conversation_deleted
        WHERE conversation_id=? AND user_id=?
        LIMIT 1
    """, (conversation_id, uid)).fetchone()

    if exists:
        conn.execute("""
            UPDATE message_conversation_deleted
            SET deleted_at=?, deleted_by=?, active=1
            WHERE conversation_id=? AND user_id=?
        """, (now_iso(), session["user"], conversation_id, uid))
    else:
        conn.execute("""
            INSERT INTO message_conversation_deleted (
                conversation_id,
                user_id,
                deleted_at,
                deleted_by,
                active
            ) VALUES (?,?,?,?,1)
        """, (conversation_id, uid, now_iso(), session["user"]))

    try:
        refresh_conversation_folder_sharing(conn, conversation_id)
    except Exception:
        pass

    conn.commit()
    conn.close()
    return jsonify({"ok": True})
