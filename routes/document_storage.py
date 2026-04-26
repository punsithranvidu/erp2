from flask import Blueprint, request, session, jsonify, current_app, redirect
import os
import io
import json
import shutil
import httplib2
from functools import wraps
from datetime import datetime

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

from .db import connect

from google.oauth2.credentials import Credentials
from google_auth_httplib2 import AuthorizedHttp
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError


document_storage_bp = Blueprint("document_storage", __name__)
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]
DRIVE_HTTP_TIMEOUT_SECONDS = 8


def db():
    return connect(current_app.config["DATABASE_URL"])


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if "user" not in session:
            return jsonify({"ok": False, "error": "Login required"}), 401
        return f(*args, **kwargs)
    return wrapped


def has_module_access(module: str, need_edit: bool = False):
    func = current_app.config["HAS_MODULE_ACCESS_FUNC"]
    return func(module, need_edit=need_edit)


def require_module(module: str, need_edit: bool = False):
    def deco(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if not has_module_access(module, need_edit=need_edit):
                return jsonify({"ok": False, "error": f"No permission for {module}"}), 403
            return f(*args, **kwargs)
        return wrapped
    return deco


def is_admin():
    return session.get("role") == "ADMIN"


def get_me_user_id():
    return session.get("uid")


def _ensure_tmp_dir():
    os.makedirs("/tmp/erp_google", exist_ok=True)
    return "/tmp/erp_google"


def _looks_like_json(text: str) -> bool:
    if not text:
        return False
    s = text.strip()
    return s.startswith("{") and s.endswith("}")


def _materialize_json_or_path(value, tmp_filename, fallback_secret_path=None, required=False):
    """
    Accepts:
    - real file path
    - raw JSON content in env/config
    - fallback /etc/secrets file
    Returns a real file path usable by Google libraries.
    """
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
        shutil.copyfile(fallback_secret_path, out_path)
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


def oauth_token_file():
    """
    Writable runtime token file.
    We still use /tmp, but we also restore from /etc/secrets if available.
    """
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

    raise ValueError("Google Drive is not connected yet. Please click Connect / Refresh Google Drive first.")


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
        raise ValueError("Google Drive token file is invalid. Please click Connect / Refresh Google Drive again.")

    try:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_runtime_token(creds.to_json())

        elif not creds.valid:
            clear_oauth_token_file()
            raise ValueError("Google Drive connection is invalid. Please click Connect / Refresh Google Drive again.")

    except ValueError:
        raise
    except Exception:
        clear_oauth_token_file()
        raise ValueError("Google Drive connection expired or was revoked. Please click Connect / Refresh Google Drive again.")

    http = AuthorizedHttp(creds, http=httplib2.Http(timeout=DRIVE_HTTP_TIMEOUT_SECONDS))
    return build("drive", "v3", http=http, cache_discovery=False)


def get_redirect_uri():
    forced = (
        current_app.config.get("GOOGLE_OAUTH_REDIRECT_URI")
        or os.environ.get("GOOGLE_OAUTH_REDIRECT_URI")
        or ""
    ).strip()

    if forced:
        return forced

    proto = request.headers.get("X-Forwarded-Proto", request.scheme)
    host = request.headers.get("X-Forwarded-Host", request.host)
    return f"{proto}://{host}/google-drive/callback"


def get_root_drive_folder_id():
    root_id = current_app.config.get("GOOGLE_DRIVE_ROOT_FOLDER_ID") or os.environ.get("GOOGLE_DRIVE_ROOT_FOLDER_ID")
    if not root_id or root_id == "PASTE_YOUR_ROOT_FOLDER_ID_HERE":
        raise RuntimeError("GOOGLE_DRIVE_ROOT_FOLDER_ID is not configured")
    return root_id.strip()


def drive_create_folder(name: str, parent_drive_id: str):
    service = get_oauth_drive_service()
    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_drive_id],
    }
    return service.files().create(
        body=meta,
        fields="id,name,mimeType,webViewLink",
        supportsAllDrives=True,
    ).execute()


def drive_upload_file(file_storage, parent_drive_id: str, display_name: str = ""):
    service = get_oauth_drive_service()
    filename = (display_name or file_storage.filename or "document").strip()
    meta = {
        "name": filename,
        "parents": [parent_drive_id],
    }
    file_bytes = file_storage.read()
    media = MediaIoBaseUpload(
        io.BytesIO(file_bytes),
        mimetype=file_storage.mimetype or "application/octet-stream",
        resumable=False,
    )
    return service.files().create(
        body=meta,
        media_body=media,
        fields="id,name,mimeType,webViewLink",
        supportsAllDrives=True,
    ).execute()


def drive_rename_item(drive_id: str, new_name: str):
    service = get_oauth_drive_service()
    return service.files().update(
        fileId=drive_id,
        body={"name": new_name},
        fields="id,name,mimeType,webViewLink",
        supportsAllDrives=True,
    ).execute()


def drive_delete_item_safe(drive_id: str):
    if not drive_id:
        return {"ok": True, "deleted_in_drive": False, "warning": ""}

    try:
        service = get_oauth_drive_service()
        service.files().delete(fileId=drive_id, supportsAllDrives=True).execute()
        return {"ok": True, "deleted_in_drive": True, "warning": ""}
    except HttpError as e:
        return {
            "ok": True,
            "deleted_in_drive": False,
            "warning": f"Drive delete skipped: {str(e)}"
        }
    except Exception as e:
        return {
            "ok": True,
            "deleted_in_drive": False,
            "warning": f"Drive delete skipped: {str(e)}"
        }


def get_parent_drive_id(conn, parent_id):
    if parent_id in (None, "", "ROOT", "null"):
        return get_root_drive_folder_id()

    row = conn.execute(
        """
        SELECT id, drive_id, item_type, is_active, deleted_at
        FROM doc_items
        WHERE id=%s LIMIT 1
        """,
        (int(parent_id),),
    ).fetchone()

    if not row:
        raise ValueError("Parent folder not found")
    if int(row["is_active"]) != 1 or row["deleted_at"]:
        raise ValueError("Parent folder is inactive")
    if row["item_type"] != "FOLDER":
        raise ValueError("Parent must be a folder")
    if not row["drive_id"]:
        raise ValueError("Parent folder has no Drive ID")
    return row["drive_id"]


def drive_list_children(parent_drive_id: str):
    service = get_oauth_drive_service()
    out = []
    page_token = None

    while True:
        res = service.files().list(
            q=f"'{parent_drive_id}' in parents and trashed = false",
            fields="nextPageToken, files(id,name,mimeType,webViewLink,parents)",
            pageToken=page_token,
            pageSize=1000,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()

        out.extend(res.get("files", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break

    out.sort(key=lambda x: (0 if x.get("mimeType") == "application/vnd.google-apps.folder" else 1, (x.get("name") or "").lower()))
    return out


def drive_walk_sync_tree(root_drive_id: str):
    scanned = []
    skipped_messages_folders = 0
    ignored_root_drive_ids = set()

    def walk(parent_drive_id: str):
        nonlocal skipped_messages_folders

        for item in drive_list_children(parent_drive_id):
            name = (item.get("name") or "").strip()

            if parent_drive_id == root_drive_id and name.lower() == "messages":
                skipped_messages_folders += 1
                ignored_root_drive_ids.add(item.get("id"))
                continue

            scanned.append({
                "drive_id": item.get("id"),
                "parent_drive_id": parent_drive_id,
                "name": name,
                "mime_type": item.get("mimeType"),
                "web_view_link": item.get("webViewLink"),
                "item_type": "FOLDER" if item.get("mimeType") == "application/vnd.google-apps.folder" else "DOCUMENT",
            })

            if item.get("mimeType") == "application/vnd.google-apps.folder":
                walk(item.get("id"))

    walk(root_drive_id)
    return scanned, skipped_messages_folders, ignored_root_drive_ids


def get_item_row(conn, item_id):
    return conn.execute(
        """
        SELECT *
        FROM doc_items
        WHERE id=%s LIMIT 1
        """,
        (int(item_id),),
    ).fetchone()


def get_item_ancestors(conn, item_row):
    ancestors = []
    parent_id = item_row["parent_id"]

    while parent_id is not None:
        parent = conn.execute(
            """
            SELECT *
            FROM doc_items
            WHERE id=%s LIMIT 1
            """,
            (int(parent_id),),
        ).fetchone()

        if not parent:
            break

        ancestors.append(parent)
        parent_id = parent["parent_id"]

    return ancestors


def has_direct_permission(conn, item_id, uid, username, role, need_edit=False):
    if role == "ADMIN":
        return True

    if need_edit:
        row = conn.execute(
            """
            SELECT di.id
            FROM doc_items di
            LEFT JOIN doc_item_permissions dp
              ON dp.item_id = di.id AND dp.user_id = %s
            WHERE di.id = %s
              AND di.is_active = 1
              AND di.deleted_at IS NULL
              AND COALESCE(di.admin_locked, 0) = 0
              AND (
                  di.created_by = %s
                  OR COALESCE(dp.can_edit, 0) = 1
              )
            LIMIT 1
            """,
            (uid, item_id, username),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT di.id
            FROM doc_items di
            LEFT JOIN doc_item_permissions dp
              ON dp.item_id = di.id AND dp.user_id = %s
            WHERE di.id = %s
              AND di.is_active = 1
              AND di.deleted_at IS NULL
              AND COALESCE(di.admin_locked, 0) = 0
              AND (
                  di.created_by = %s
                  OR COALESCE(dp.can_access, 0) = 1
              )
            LIMIT 1
            """,
            (uid, item_id, username),
        ).fetchone()

    return row is not None


def has_item_access_or_inherited(conn, item_row, uid, username, role, need_edit=False):
    if role == "ADMIN":
        return True

    if not item_row:
        return False

    if item_row["deleted_at"] or int(item_row["is_active"] or 0) != 1:
        return False

    if int(item_row["admin_locked"] or 0) == 1:
        return False

    if has_direct_permission(conn, item_row["id"], uid, username, role, need_edit=need_edit):
        return True

    ancestors = get_item_ancestors(conn, item_row)
    for parent in ancestors:
        if parent["deleted_at"] or int(parent["is_active"] or 0) != 1:
            continue
        if int(parent["admin_locked"] or 0) == 1:
            continue
        if has_direct_permission(conn, parent["id"], uid, username, role, need_edit=need_edit):
            return True

    return False


def can_view_item(conn, item_id, uid, username, role):
    if role == "ADMIN":
        return True

    item = get_item_row(conn, item_id)
    return has_item_access_or_inherited(conn, item, uid, username, role, need_edit=False)


def can_edit_item(conn, item_id, uid, username, role):
    if role == "ADMIN":
        return True

    item = get_item_row(conn, item_id)
    return has_item_access_or_inherited(conn, item, uid, username, role, need_edit=True)


def save_item_permissions(conn, item_id, creator_username, permissions):
    conn.execute("DELETE FROM doc_item_permissions WHERE item_id=%s", (item_id,))

    seen_users = set()

    def safe_insert(user_id, can_access, can_edit):
        key = (item_id, user_id)
        if key in seen_users:
            return
        seen_users.add(key)

        conn.execute(
            """
            INSERT INTO doc_item_permissions (item_id, user_id, can_access, can_edit)
            VALUES (%s,%s,%s,%s)
            """,
            (item_id, user_id, can_access, can_edit),
        )

    creator = conn.execute(
        "SELECT id FROM users WHERE username=%s LIMIT 1",
        (creator_username,)
    ).fetchone()

    if creator:
        safe_insert(creator["id"], 1, 1)

    admins = conn.execute(
        "SELECT id FROM users WHERE role='ADMIN' AND active=1"
    ).fetchall()

    for a in admins:
        safe_insert(a["id"], 1, 1)

    for p in (permissions or []):
        try:
            user_id = int(p.get("user_id"))
            can_access = 1 if int(p.get("can_access", 0) or 0) == 1 else 0
            can_edit = 1 if int(p.get("can_edit", 0) or 0) == 1 else 0
        except Exception:
            continue

        if can_access == 0:
            can_edit = 0

        if can_access or can_edit:
            safe_insert(user_id, can_access, can_edit)


def desired_drive_shares(conn, item_id):
    rows = conn.execute(
        """
        SELECT u.google_email,
               MAX(COALESCE(dp.can_edit, 0)) AS can_edit,
               MAX(COALESCE(dp.can_access, 0)) AS can_access
        FROM doc_item_permissions dp
        JOIN users u ON u.id = dp.user_id
        WHERE dp.item_id=%s
          AND u.active=1
          AND u.google_email IS NOT NULL
          AND TRIM(u.google_email) <> ''
        GROUP BY u.google_email
        """,
        (item_id,),
    ).fetchall()

    desired = {}
    for r in rows:
        role = "writer" if int(r["can_edit"] or 0) == 1 else "reader"
        if int(r["can_access"] or 0) == 1 or int(r["can_edit"] or 0) == 1:
            desired[(r["google_email"] or "").strip().lower()] = role
    return desired


def sync_drive_shares(conn, item_id, drive_id):
    if not drive_id:
        return

    desired = desired_drive_shares(conn, item_id)
    service = get_oauth_drive_service()

    current = service.permissions().list(
        fileId=drive_id,
        fields="permissions(id,emailAddress,role)",
        supportsAllDrives=True,
    ).execute().get("permissions", [])

    current_map = {}
    for p in current:
        email = (p.get("emailAddress") or "").strip().lower()
        if not email:
            continue
        current_map[email] = p

    for email, perm in list(current_map.items()):
        role = perm.get("role")
        if role in ("owner", "organizer", "fileOrganizer"):
            continue
        wanted = desired.get(email)
        if not wanted:
            try:
                service.permissions().delete(
                    fileId=drive_id,
                    permissionId=perm["id"],
                    supportsAllDrives=True,
                ).execute()
            except Exception:
                pass
            continue
        if wanted != role:
            try:
                service.permissions().update(
                    fileId=drive_id,
                    permissionId=perm["id"],
                    body={"role": wanted},
                    supportsAllDrives=True,
                ).execute()
            except Exception:
                pass

    for email, role in desired.items():
        if email in current_map:
            continue
        try:
            service.permissions().create(
                fileId=drive_id,
                body={
                    "type": "user",
                    "role": role,
                    "emailAddress": email,
                },
                fields="id",
                sendNotificationEmail=False,
                supportsAllDrives=True,
            ).execute()
        except Exception:
            pass


def sync_drive_shares_safe(conn, item_id, drive_id):
    try:
        sync_drive_shares(conn, item_id, drive_id)
    except Exception as e:
        current_app.logger.warning(
            "Document Storage Drive share sync skipped for item_id=%s drive_id=%s: %s",
            item_id,
            drive_id,
            e,
        )


@document_storage_bp.route("/google-drive/connect", methods=["GET"])
@login_required
def google_drive_connect():
    try:
        clear_oauth_token_file()

        flow = Flow.from_client_secrets_file(
            oauth_client_file(),
            scopes=DRIVE_SCOPES,
        )
        flow.redirect_uri = get_redirect_uri()

        authorization_url, state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",
            code_challenge_method="S256",
        )

        session["google_drive_oauth_state"] = state
        session["google_drive_code_verifier"] = flow.code_verifier

        return redirect(authorization_url)

    except Exception as e:
        return jsonify({"ok": False, "error": f"Google Drive connect failed: {str(e)}"}), 500


@document_storage_bp.route("/google-drive/callback", methods=["GET"])
@login_required
def google_drive_callback():
    state = session.get("google_drive_oauth_state")
    code_verifier = session.get("google_drive_code_verifier")

    if not state:
        return jsonify({"ok": False, "error": "Missing OAuth state. Please connect again."}), 400

    if not code_verifier:
        return jsonify({"ok": False, "error": "Missing OAuth code verifier. Please connect again."}), 400

    try:
        flow = Flow.from_client_secrets_file(
            oauth_client_file(),
            scopes=DRIVE_SCOPES,
            state=state,
        )
        flow.redirect_uri = get_redirect_uri()
        flow.code_verifier = code_verifier
        flow.fetch_token(authorization_response=request.url)

        creds = flow.credentials
        token_json = creds.to_json()
        save_runtime_token(token_json)

        session.pop("google_drive_oauth_state", None)
        session.pop("google_drive_code_verifier", None)

        return redirect("/document-storage")

    except Exception as e:
        session.pop("google_drive_oauth_state", None)
        session.pop("google_drive_code_verifier", None)
        clear_oauth_token_file()
        return jsonify({"ok": False, "error": f"Google Drive callback failed: {str(e)}"}), 500


@document_storage_bp.route("/api/docs/users", methods=["GET"])
@login_required
@require_module("DOCUMENT_STORAGE")
def api_docs_users():
    conn = db()
    rows = conn.execute(
        """
        SELECT id, username, role, active, full_name, google_email
        FROM users
        WHERE active=1
        ORDER BY role DESC, username ASC
        """
    ).fetchall()
    conn.close()
    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


@document_storage_bp.route("/api/docs/items", methods=["GET"])
@login_required
@require_module("DOCUMENT_STORAGE")
def api_docs_items_list():
    parent_id = request.args.get("parent_id")
    q = (request.args.get("q") or "").strip().lower()

    uid = get_me_user_id()
    username = session.get("user")
    role = session.get("role")
    conn = db()

    if parent_id in (None, "", "ROOT", "null"):
        if role == "ADMIN":
            rows = conn.execute(
                """
                SELECT *
                FROM doc_items
                WHERE is_active=1
                  AND deleted_at IS NULL
                  AND parent_id IS NULL
                ORDER BY
                  CASE WHEN item_type='FOLDER' THEN 0 ELSE 1 END,
                  LOWER(name) ASC
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT *
                FROM doc_items
                WHERE is_active=1
                  AND deleted_at IS NULL
                  AND created_by=%s
                ORDER BY
                  CASE WHEN item_type='FOLDER' THEN 0 ELSE 1 END,
                  LOWER(name) ASC
                """,
                (username,),
            ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM doc_items
            WHERE is_active=1
              AND deleted_at IS NULL
              AND parent_id=%s
            ORDER BY
              CASE WHEN item_type='FOLDER' THEN 0 ELSE 1 END,
              LOWER(name) ASC
            """,
            (int(parent_id),),
        ).fetchall()

    out = []
    for r in rows:
        item = dict(r)
        if q and q not in (item["name"] or "").lower():
            continue

        if role == "ADMIN":
            out.append(item)
            continue

        if parent_id in (None, "", "ROOT", "null"):
            ancestors = get_item_ancestors(conn, r)
            nested_under_my_item = False

            for parent in ancestors:
                if parent["deleted_at"] or int(parent["is_active"] or 0) != 1:
                    continue
                if (parent["created_by"] or "") == username:
                    nested_under_my_item = True
                    break

            if nested_under_my_item:
                continue

        if has_item_access_or_inherited(conn, r, uid, username, role, need_edit=False):
            out.append(item)

    conn.close()
    return jsonify({"ok": True, "data": out})


@document_storage_bp.route("/api/docs/shared", methods=["GET"])
@login_required
@require_module("DOCUMENT_STORAGE")
def api_docs_shared_items():
    uid = get_me_user_id()
    username = session.get("user")
    role = session.get("role")
    q = (request.args.get("q") or "").strip().lower()

    conn = db()

    if role == "ADMIN":
        rows = conn.execute(
            """
            SELECT *
            FROM doc_items
            WHERE is_active=1
              AND deleted_at IS NULL
            ORDER BY LOWER(name) ASC
            LIMIT 1000
            """
        ).fetchall()

        all_items = [dict(r) for r in rows]
        item_map = {int(r["id"]): r for r in all_items}

        visible = []
        for item in all_items:
            parent_id = item.get("parent_id")
            skip_item = False

            while parent_id is not None:
                parent = item_map.get(int(parent_id))
                if not parent:
                    break

                if parent.get("is_active") == 1 and not parent.get("deleted_at"):
                    skip_item = True
                    break

                parent_id = parent.get("parent_id")

            if not skip_item:
                if q and q not in (item.get("name") or "").lower():
                    continue
                visible.append(item)

        visible.sort(key=lambda x: (0 if x["item_type"] == "FOLDER" else 1, (x["name"] or "").lower()))
        conn.close()
        return jsonify({"ok": True, "data": visible})

    rows = conn.execute(
        """
        SELECT di.*
        FROM doc_items di
        JOIN doc_item_permissions dp ON dp.item_id = di.id
        WHERE di.is_active=1
          AND di.deleted_at IS NULL
          AND COALESCE(di.admin_locked, 0)=0
          AND dp.user_id=%s
          AND COALESCE(dp.can_access, 0)=1
          AND di.created_by <> %s
        ORDER BY
          CASE WHEN di.item_type='FOLDER' THEN 0 ELSE 1 END,
          LOWER(di.name) ASC
        LIMIT 500
        """,
        (uid, username),
    ).fetchall()

    out = []
    seen = set()

    for r in rows:
        item = dict(r)
        if item["id"] in seen:
            continue
        seen.add(item["id"])

        if q and q not in (item["name"] or "").lower():
            continue

        out.append(item)

    conn.close()
    return jsonify({"ok": True, "data": out})


@document_storage_bp.route("/api/docs/trash", methods=["GET"])
@login_required
@require_module("DOCUMENT_STORAGE")
def api_docs_trash():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    q = (request.args.get("q") or "").strip().lower()
    conn = db()
    rows = conn.execute(
        """
        SELECT *
        FROM doc_items
        WHERE deleted_at IS NOT NULL
        ORDER BY deleted_at DESC, LOWER(name) ASC
        """
    ).fetchall()

    out = []
    for r in rows:
        item = dict(r)
        if q and q not in (item["name"] or "").lower():
            continue
        out.append(item)
    conn.close()
    return jsonify({"ok": True, "data": out})


@document_storage_bp.route("/api/docs/items/<int:item_id>", methods=["GET"])
@login_required
@require_module("DOCUMENT_STORAGE")
def api_docs_item_get(item_id):
    uid = get_me_user_id()
    username = session.get("user")
    role = session.get("role")

    conn = db()
    item = conn.execute("SELECT * FROM doc_items WHERE id=%s LIMIT 1", (item_id,)).fetchone()
    if not item:
        conn.close()
        return jsonify({"ok": False, "error": "Item not found"}), 404

    if item["deleted_at"] and not is_admin():
        conn.close()
        return jsonify({"ok": False, "error": "Item is in trash"}), 403

    if not item["deleted_at"] and not can_view_item(conn, item_id, uid, username, role):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    perms = conn.execute(
        """
        SELECT u.id AS user_id, u.username, u.role, u.full_name, u.google_email, dp.can_access, dp.can_edit
        FROM doc_item_permissions dp
        JOIN users u ON u.id = dp.user_id
        WHERE dp.item_id=%s
        ORDER BY u.username ASC
        """,
        (item_id,),
    ).fetchall()

    conn.close()
    return jsonify({
        "ok": True,
        "data": {
            "item": dict(item),
            "permissions": [dict(r) for r in perms],
        },
    })


@document_storage_bp.route("/api/docs/folders", methods=["POST"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_docs_create_folder():
    conn = None
    try:
        data = request.json or {}
        parent_id = data.get("parent_id")
        name = (data.get("name") or "").strip()
        notes = (data.get("notes") or "").strip()
        permissions = data.get("permissions") or []

        if not name:
            return jsonify({"ok": False, "error": "Folder name is required"}), 400

        conn = db()

        if parent_id in (None, "", "ROOT", "null"):
            if not is_admin():
                conn.close()
                return jsonify({"ok": False, "error": "Only admin can create folders at root level"}), 403
            parent_db_id = None
        else:
            parent_db_id = int(parent_id)
            if not can_edit_item(conn, parent_db_id, get_me_user_id(), session["user"], session["role"]):
                conn.close()
                return jsonify({"ok": False, "error": "No edit access to parent folder"}), 403

        dupe = conn.execute(
            """
            SELECT id
            FROM doc_items
            WHERE is_active=1
              AND deleted_at IS NULL
              AND COALESCE(parent_id, -1) = COALESCE(%s, -1)
              AND LOWER(name)=LOWER(%s)
            LIMIT 1
            """,
            (parent_db_id, name),
        ).fetchone()

        if dupe:
            conn.close()
            return jsonify({"ok": False, "error": "A folder/file with this name already exists here"}), 400

        parent_drive_id = get_parent_drive_id(conn, parent_id)
        created = drive_create_folder(name, parent_drive_id)

        conn.execute(
            """
            INSERT INTO doc_items (
                parent_id, item_type, name, category,
                drive_id, web_view_link, mime_type, notes,
                admin_locked, is_active, created_at, created_by,
                deleted_at, deleted_by
            ) VALUES (%s, 'FOLDER', %s, 'GENERAL', %s, %s, %s, %s, 0, 1, %s, %s, NULL, NULL)
            """,
            (
                parent_db_id,
                name,
                created.get("id"),
                created.get("webViewLink"),
                created.get("mimeType"),
                notes,
                now_iso(),
                session["user"],
            ),
        )
        conn.commit()

        row = conn.execute(
            "SELECT * FROM doc_items WHERE drive_id=%s LIMIT 1",
            (created.get("id"),)
        ).fetchone()

        if not row:
            conn.close()
            return jsonify({"ok": False, "error": "Folder created in Drive but failed to save in ERP"}), 500

        item_id = row["id"]

        save_item_permissions(conn, item_id, session["user"], permissions)
        sync_drive_shares_safe(conn, item_id, created.get("id"))
        conn.commit()

        row = conn.execute("SELECT * FROM doc_items WHERE id=%s", (item_id,)).fetchone()
        conn.close()
        return jsonify({"ok": True, "data": dict(row)})

    except Exception as e:
        try:
            if conn:
                conn.rollback()
                conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(e)}), 500


@document_storage_bp.route("/api/docs/upload", methods=["POST"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_docs_upload():
    conn = None
    try:
        parent_id = request.form.get("parent_id")
        display_name = (request.form.get("name") or "").strip()
        notes = (request.form.get("notes") or "").strip()

        try:
            permissions = json.loads(request.form.get("permissions_json") or "[]")
        except Exception:
            permissions = []

        file = request.files.get("file")
        if not file or not file.filename:
            return jsonify({"ok": False, "error": "Please choose a file"}), 400

        conn = db()

        if parent_id in (None, "", "ROOT", "null"):
            if not is_admin():
                conn.close()
                return jsonify({"ok": False, "error": "Only admin can upload at root level"}), 403
            parent_db_id = None
        else:
            parent_db_id = int(parent_id)
            if not can_edit_item(conn, parent_db_id, get_me_user_id(), session["user"], session["role"]):
                conn.close()
                return jsonify({"ok": False, "error": "No edit access to parent folder"}), 403

        parent_drive_id = get_parent_drive_id(conn, parent_id)
        uploaded = drive_upload_file(file, parent_drive_id, display_name or file.filename)

        conn.execute(
            """
            INSERT INTO doc_items (
                parent_id, item_type, name, category,
                drive_id, web_view_link, mime_type, notes,
                admin_locked, is_active, created_at, created_by,
                deleted_at, deleted_by
            ) VALUES (%s, 'DOCUMENT', %s, 'GENERAL', %s, %s, %s, %s, 0, 1, %s, %s, NULL, NULL)
            """,
            (
                parent_db_id,
                uploaded.get("name"),
                uploaded.get("id"),
                uploaded.get("webViewLink"),
                uploaded.get("mimeType"),
                notes,
                now_iso(),
                session["user"],
            ),
        )
        conn.commit()

        row = conn.execute(
            "SELECT * FROM doc_items WHERE drive_id=%s LIMIT 1",
            (uploaded.get("id"),)
        ).fetchone()

        if not row:
            conn.close()
            return jsonify({"ok": False, "error": "File uploaded to Drive but failed to save in ERP"}), 500

        item_id = row["id"]

        save_item_permissions(conn, item_id, session["user"], permissions)
        sync_drive_shares_safe(conn, item_id, uploaded.get("id"))
        conn.commit()

        row = conn.execute("SELECT * FROM doc_items WHERE id=%s", (item_id,)).fetchone()
        conn.close()
        return jsonify({"ok": True, "data": dict(row)})

    except Exception as e:
        try:
            if conn:
                conn.rollback()
                conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(e)}), 500


@document_storage_bp.route("/api/docs/items/<int:item_id>", methods=["PUT"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_docs_item_update(item_id):
    data = request.json or {}
    uid = get_me_user_id()
    username = session.get("user")
    role = session.get("role")

    conn = db()
    item = conn.execute("SELECT * FROM doc_items WHERE id=%s LIMIT 1", (item_id,)).fetchone()
    if not item:
        conn.close()
        return jsonify({"ok": False, "error": "Item not found"}), 404

    if item["deleted_at"]:
        conn.close()
        return jsonify({"ok": False, "error": "Cannot edit item in trash"}), 400

    if not can_edit_item(conn, item_id, uid, username, role):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    new_name = (data.get("name") or item["name"]).strip()
    notes = (data.get("notes") or item["notes"] or "").strip()
    permissions = data.get("permissions")

    if permissions is not None and item["admin_locked"] and not is_admin():
        conn.close()
        return jsonify({"ok": False, "error": "Admin locked item. Only admin can change permissions."}), 403

    web_view_link = item["web_view_link"]

    if new_name != item["name"]:
        same_parent = conn.execute(
            """
            SELECT id
            FROM doc_items
            WHERE is_active=1
              AND deleted_at IS NULL
              AND id<>%s
              AND COALESCE(parent_id,-1)=COALESCE(%s, -1)
              AND LOWER(name)=LOWER(%s)
            LIMIT 1
            """,
            (item_id, item["parent_id"], new_name),
        ).fetchone()

        if same_parent:
            conn.close()
            return jsonify({"ok": False, "error": "Another item with this name already exists here"}), 400

        updated_drive = drive_rename_item(item["drive_id"], new_name)
        web_view_link = updated_drive.get("webViewLink") or item["web_view_link"]

    conn.execute(
        """
        UPDATE doc_items
        SET name=%s, notes=%s, web_view_link=%s, edited_at=%s, edited_by=%s
        WHERE id=%s
        """,
        (new_name, notes, web_view_link, now_iso(), session["user"], item_id),
    )

    if permissions is not None:
        save_item_permissions(conn, item_id, item["created_by"], permissions)
        sync_drive_shares_safe(conn, item_id, item["drive_id"])

    conn.commit()
    row = conn.execute("SELECT * FROM doc_items WHERE id=%s", (item_id,)).fetchone()
    conn.close()
    return jsonify({"ok": True, "data": dict(row)})


@document_storage_bp.route("/api/docs/items/<int:item_id>/toggle-lock", methods=["POST"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_docs_toggle_lock(item_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    item = conn.execute("SELECT id, admin_locked, deleted_at FROM doc_items WHERE id=%s LIMIT 1", (item_id,)).fetchone()
    if not item:
        conn.close()
        return jsonify({"ok": False, "error": "Item not found"}), 404

    if item["deleted_at"]:
        conn.close()
        return jsonify({"ok": False, "error": "Cannot lock item in trash"}), 400

    new_val = 0 if int(item["admin_locked"] or 0) == 1 else 1
    conn.execute(
        """
        UPDATE doc_items
        SET admin_locked=%s, edited_at=%s, edited_by=%s
        WHERE id=%s
        """,
        (new_val, now_iso(), session["user"], item_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": {"id": item_id, "admin_locked": new_val}})


@document_storage_bp.route("/api/docs/items/<int:item_id>", methods=["DELETE"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_docs_item_delete(item_id):
    uid = get_me_user_id()
    username = session.get("user")
    role = session.get("role")

    conn = db()
    item = conn.execute("SELECT * FROM doc_items WHERE id=%s LIMIT 1", (item_id,)).fetchone()
    if not item:
        conn.close()
        return jsonify({"ok": False, "error": "Item not found"}), 404

    if item["deleted_at"]:
        conn.close()
        return jsonify({"ok": False, "error": "Item already in trash"}), 400

    if not can_edit_item(conn, item_id, uid, username, role):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    child = conn.execute(
        """
        SELECT id
        FROM doc_items
        WHERE parent_id=%s
          AND is_active=1
          AND deleted_at IS NULL
        LIMIT 1
        """,
        (item_id,),
    ).fetchone()

    if child:
        conn.close()
        return jsonify({"ok": False, "error": "This folder contains items. Delete those first."}), 400

    drive_result = drive_delete_item_safe(item["drive_id"])

    conn.execute(
        """
        UPDATE doc_items
        SET deleted_at=%s, deleted_by=%s, edited_at=%s, edited_by=%s
        WHERE id=%s
        """,
        (now_iso(), session["user"], now_iso(), session["user"], item_id),
    )
    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "warning": drive_result.get("warning", "")
    })


@document_storage_bp.route("/api/docs/items/<int:item_id>/restore", methods=["POST"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_docs_restore(item_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    item = conn.execute("SELECT * FROM doc_items WHERE id=%s LIMIT 1", (item_id,)).fetchone()
    if not item:
        conn.close()
        return jsonify({"ok": False, "error": "Item not found"}), 404

    if not item["deleted_at"]:
        conn.close()
        return jsonify({"ok": False, "error": "Item is not in trash"}), 400

    conn.execute(
        """
        UPDATE doc_items
        SET deleted_at=NULL, deleted_by=NULL, edited_at=%s, edited_by=%s
        WHERE id=%s
        """,
        (now_iso(), session["user"], item_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@document_storage_bp.route("/api/docs/items/<int:item_id>/delete-forever", methods=["POST"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_docs_delete_forever(item_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    item = conn.execute("SELECT * FROM doc_items WHERE id=%s LIMIT 1", (item_id,)).fetchone()
    if not item:
        conn.close()
        return jsonify({"ok": False, "error": "Item not found"}), 404

    if not item["deleted_at"]:
        conn.close()
        return jsonify({"ok": False, "error": "Move item to trash first"}), 400

    conn.execute("DELETE FROM doc_item_permissions WHERE item_id=%s", (item_id,))
    conn.execute("DELETE FROM doc_items WHERE id=%s", (item_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@document_storage_bp.route("/api/docs/bulk-trash-action", methods=["POST"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_docs_bulk_trash_action():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    action = (data.get("action") or "").strip().lower()
    item_ids = data.get("item_ids") or []

    valid_ids = []
    for x in item_ids:
        try:
            valid_ids.append(int(x))
        except Exception:
            pass

    if not valid_ids:
        return jsonify({"ok": False, "error": "No items selected"}), 400

    conn = db()
    updated = 0

    if action == "restore":
        for item_id in valid_ids:
            row = conn.execute("SELECT id FROM doc_items WHERE id=%s AND deleted_at IS NOT NULL", (item_id,)).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE doc_items
                    SET deleted_at=NULL, deleted_by=NULL, edited_at=%s, edited_by=%s
                    WHERE id=%s
                    """,
                    (now_iso(), session["user"], item_id),
                )
                updated += 1

    elif action == "delete_forever":
        for item_id in valid_ids:
            row = conn.execute("SELECT id FROM doc_items WHERE id=%s AND deleted_at IS NOT NULL", (item_id,)).fetchone()
            if row:
                conn.execute("DELETE FROM doc_item_permissions WHERE item_id=%s", (item_id,))
                conn.execute("DELETE FROM doc_items WHERE id=%s", (item_id,))
                updated += 1
    else:
        conn.close()
        return jsonify({"ok": False, "error": "Invalid bulk action"}), 400

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": {"updated": updated}})


@document_storage_bp.route("/api/document-storage/sync-drive", methods=["POST"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_document_storage_sync_drive():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = None
    try:
        conn = db()
        root_drive_id = get_root_drive_folder_id()
        drive_items_raw = drive_list_children(root_drive_id)

        drive_items = []
        skipped = 0
        errors = []

        for item in drive_items_raw:
            name = (item.get("name") or "").strip()
            if name.lower() == "messages":
                skipped += 1
                continue
            drive_id = (item.get("id") or "").strip()
            if not drive_id:
                errors.append("Skipped a Drive item with no id.")
                continue
            drive_items.append({
                "drive_id": drive_id,
                "name": name,
                "mime_type": item.get("mimeType"),
                "web_view_link": item.get("webViewLink"),
                "item_type": "FOLDER" if item.get("mimeType") == "application/vnd.google-apps.folder" else "DOCUMENT",
            })

        db_rows = conn.execute(
            """
            SELECT *
            FROM doc_items
            WHERE parent_id IS NULL
            ORDER BY id ASC
            """
        ).fetchall()

        db_rows = [dict(r) for r in db_rows]
        active_db_by_drive = {}
        any_db_by_drive = {}

        for row in db_rows:
            drive_id = (row.get("drive_id") or "").strip()
            if not drive_id:
                continue
            if row.get("item_type") == "FOLDER" and (row.get("name") or "").strip().lower() == "messages":
                continue

            any_db_by_drive.setdefault(drive_id, row)
            if not row.get("deleted_at") and int(row.get("is_active") or 0) == 1:
                active_db_by_drive.setdefault(drive_id, row)

        admin_rows = conn.execute(
            "SELECT id FROM users WHERE role='ADMIN' AND active=1 ORDER BY id ASC"
        ).fetchall()
        admin_user_ids = [int(r["id"]) for r in admin_rows]

        current_user = session["user"]
        sync_ts = now_iso()
        drive_ids = set()
        added = 0
        removed = 0
        unchanged = 0

        def grant_admin_only_permissions(item_id):
            conn.execute("DELETE FROM doc_item_permissions WHERE item_id=%s", (item_id,))
            for user_id in admin_user_ids:
                conn.execute(
                    """
                    INSERT INTO doc_item_permissions (item_id, user_id, can_access, can_edit)
                    VALUES (%s, %s, 1, 1)
                    """,
                    (item_id, user_id),
                )

        for item in drive_items:
            drive_id = item["drive_id"]
            drive_ids.add(drive_id)

            existing_active = active_db_by_drive.get(drive_id)
            if existing_active:
                unchanged += 1
                continue

            existing_any = any_db_by_drive.get(drive_id)
            if existing_any:
                conn.execute(
                    """
                    UPDATE doc_items
                    SET parent_id=NULL,
                        item_type=%s,
                        name=%s,
                        web_view_link=%s,
                        mime_type=%s,
                        notes=%s,
                        is_active=1,
                        deleted_at=NULL,
                        deleted_by=NULL,
                        edited_at=%s,
                        edited_by=%s
                    WHERE id=%s
                    """,
                    (
                        item["item_type"],
                        item["name"],
                        item["web_view_link"],
                        item["mime_type"],
                        "Synced from Google Drive",
                        sync_ts,
                        current_user,
                        existing_any["id"],
                    ),
                )
                grant_admin_only_permissions(int(existing_any["id"]))
                added += 1
                continue

            row = conn.execute(
                """
                INSERT INTO doc_items (
                    parent_id, item_type, name, category,
                    drive_id, web_view_link, mime_type, notes,
                    admin_locked, is_active, created_at, created_by,
                    deleted_at, deleted_by
                ) VALUES (NULL, %s, %s, 'GENERAL', %s, %s, %s, %s, 0, 1, %s, %s, NULL, NULL)
                RETURNING id
                """,
                (
                    item["item_type"],
                    item["name"],
                    drive_id,
                    item["web_view_link"],
                    item["mime_type"],
                    "Synced from Google Drive",
                    sync_ts,
                    current_user,
                ),
            ).fetchone()

            grant_admin_only_permissions(int(row["id"]))
            added += 1

        for row in db_rows:
            drive_id = (row.get("drive_id") or "").strip()
            if not drive_id:
                continue
            if row.get("item_type") == "FOLDER" and (row.get("name") or "").strip().lower() == "messages":
                continue
            if row.get("deleted_at") or int(row.get("is_active") or 0) != 1:
                continue
            if drive_id in drive_ids:
                continue

            conn.execute(
                """
                UPDATE doc_items
                SET deleted_at=%s, deleted_by=%s, edited_at=%s, edited_by=%s
                WHERE id=%s
                """,
                (sync_ts, current_user, sync_ts, current_user, row["id"]),
            )
            removed += 1

        conn.commit()
        conn.close()

        data = {
            "added": added,
            "removed": removed,
            "unchanged": unchanged,
            "updated": 0,
            "skipped": skipped,
            "skipped_messages_folders": skipped,
            "errors": errors,
        }

        return jsonify({
            "ok": True,
            "success": True,
            "added": added,
            "removed": removed,
            "unchanged": unchanged,
            "skipped": skipped,
            "errors": errors,
            "data": data,
        })

    except Exception as e:
        current_app.logger.exception("Document Storage Drive sync failed")
        try:
            if conn:
                conn.rollback()
                conn.close()
        except Exception:
            pass
        return jsonify({
            "ok": False,
            "success": False,
            "error": str(e),
            "errors": [str(e)],
        }), 500
