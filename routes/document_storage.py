from flask import Blueprint, request, session, jsonify, current_app, redirect
import os
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

import sqlite3
import io
import json
from functools import wraps
from datetime import datetime

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError


document_storage_bp = Blueprint("document_storage", __name__)
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]


def oauth_client_file():
    return current_app.config["GOOGLE_OAUTH_CLIENT_FILE"]


def oauth_token_file():
    return current_app.config["GOOGLE_OAUTH_TOKEN_FILE"]


def get_oauth_drive_service():
    token_path = oauth_token_file()
    if not os.path.exists(token_path):
        raise ValueError("Google Drive is not connected yet. Please connect your Google account first.")

    creds = Credentials.from_authorized_user_file(token_path, DRIVE_SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def db():
    conn = sqlite3.connect(current_app.config["DB_PATH"])
    conn.row_factory = sqlite3.Row
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


def get_root_drive_folder_id():
    root_id = current_app.config.get("GOOGLE_DRIVE_ROOT_FOLDER_ID")
    if not root_id or root_id == "PASTE_YOUR_ROOT_FOLDER_ID_HERE":
        raise RuntimeError("GOOGLE_DRIVE_ROOT_FOLDER_ID is not configured")
    return root_id


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
    """
    Do not crash ERP if Google Drive delete fails.
    """
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
        WHERE id=? LIMIT 1
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


def can_view_item(conn, item_id, uid, username, role):
    if role == "ADMIN":
        return True

    row = conn.execute(
        """
        SELECT di.id
        FROM doc_items di
        LEFT JOIN doc_item_permissions dp
          ON dp.item_id = di.id AND dp.user_id = ?
        WHERE di.id = ?
          AND di.is_active = 1
          AND di.deleted_at IS NULL
          AND COALESCE(di.admin_locked, 0) = 0
          AND (
              di.created_by = ?
              OR COALESCE(dp.can_access, 0) = 1
          )
        LIMIT 1
        """,
        (uid, item_id, username),
    ).fetchone()
    return row is not None


def can_edit_item(conn, item_id, uid, username, role):
    if role == "ADMIN":
        return True

    row = conn.execute(
        """
        SELECT di.id
        FROM doc_items di
        LEFT JOIN doc_item_permissions dp
          ON dp.item_id = di.id AND dp.user_id = ?
        WHERE di.id = ?
          AND di.is_active = 1
          AND di.deleted_at IS NULL
          AND COALESCE(di.admin_locked, 0) = 0
          AND (
              di.created_by = ?
              OR COALESCE(dp.can_edit, 0) = 1
          )
        LIMIT 1
        """,
        (uid, item_id, username),
    ).fetchone()
    return row is not None


def save_item_permissions(conn, item_id, creator_username, permissions):
    conn.execute("DELETE FROM doc_item_permissions WHERE item_id=?", (item_id,))

    creator = conn.execute("SELECT id FROM users WHERE username=? LIMIT 1", (creator_username,)).fetchone()
    if creator:
        conn.execute(
            """
            INSERT OR REPLACE INTO doc_item_permissions (item_id, user_id, can_access, can_edit)
            VALUES (?,?,1,1)
            """,
            (item_id, creator["id"]),
        )

    admins = conn.execute("SELECT id FROM users WHERE role='ADMIN' AND active=1").fetchall()
    for a in admins:
        conn.execute(
            """
            INSERT OR REPLACE INTO doc_item_permissions (item_id, user_id, can_access, can_edit)
            VALUES (?,?,1,1)
            """,
            (item_id, a["id"]),
        )

    for p in (permissions or []):
        try:
            user_id = int(p.get("user_id"))
            can_access = 1 if int(p.get("can_access", 0) or 0) == 1 else 0
            can_edit = 1 if int(p.get("can_edit", 0) or 0) == 1 else 0
        except Exception:
            continue

        if can_access == 0:
            can_edit = 0

        conn.execute(
            """
            INSERT OR REPLACE INTO doc_item_permissions (item_id, user_id, can_access, can_edit)
            VALUES (?,?,?,?)
            """,
            (item_id, user_id, can_access, can_edit),
        )


def desired_drive_shares(conn, item_id):
    rows = conn.execute(
        """
        SELECT u.google_email,
               MAX(COALESCE(dp.can_edit, 0)) AS can_edit,
               MAX(COALESCE(dp.can_access, 0)) AS can_access
        FROM doc_item_permissions dp
        JOIN users u ON u.id = dp.user_id
        WHERE dp.item_id=?
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


@document_storage_bp.route("/google-drive/connect", methods=["GET"])
@login_required
def google_drive_connect():
    flow = Flow.from_client_secrets_file(
        oauth_client_file(),
        scopes=DRIVE_SCOPES,
    )
    flow.redirect_uri = "https://erp2-vpd7.onrender.com/google-drive/callback"

    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        code_challenge_method="S256",
    )

    session["google_drive_oauth_state"] = state
    session["google_drive_code_verifier"] = flow.code_verifier
    return redirect(authorization_url)


@document_storage_bp.route("/google-drive/callback", methods=["GET"])
@login_required
def google_drive_callback():
    state = session.get("google_drive_oauth_state")
    code_verifier = session.get("google_drive_code_verifier")

    if not state:
        return jsonify({"ok": False, "error": "Missing OAuth state. Please connect again."}), 400
    if not code_verifier:
        return jsonify({"ok": False, "error": "Missing OAuth code verifier. Please connect again."}), 400

    flow = Flow.from_client_secrets_file(
        oauth_client_file(),
        scopes=DRIVE_SCOPES,
        state=state,
    )
    flow.redirect_uri = "https://erp2-vpd7.onrender.com/google-drive/callback"
    flow.code_verifier = code_verifier
    flow.fetch_token(authorization_response=request.url)
    creds = flow.credentials

    with open(oauth_token_file(), "w") as f:
        f.write(creds.to_json())

    session.pop("google_drive_oauth_state", None)
    session.pop("google_drive_code_verifier", None)
    return redirect("/document-storage")


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
        parent_sql = "di.parent_id IS NULL"
        parent_params = []
    else:
        parent_sql = "di.parent_id = ?"
        parent_params = [int(parent_id)]

    sql = f"""
        SELECT di.*
        FROM doc_items di
        LEFT JOIN doc_item_permissions dp
          ON dp.item_id = di.id AND dp.user_id = ?
        WHERE di.is_active = 1
          AND di.deleted_at IS NULL
          AND {parent_sql}
          AND (
            ? = 'ADMIN'
            OR (
                COALESCE(di.admin_locked, 0) = 0
                AND (
                    di.created_by = ?
                    OR COALESCE(dp.can_access, 0) = 1
                )
            )
          )
        ORDER BY
          CASE WHEN di.item_type='FOLDER' THEN 0 ELSE 1 END,
          LOWER(di.name) ASC
    """

    rows = conn.execute(sql, [uid] + parent_params + [role, username]).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        if q and q not in (item["name"] or "").lower():
            continue
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

    else:
        rows = conn.execute(
            """
            SELECT DISTINCT di.*
            FROM doc_items di
            JOIN doc_item_permissions dp ON dp.item_id = di.id
            WHERE di.is_active=1
              AND di.deleted_at IS NULL
              AND COALESCE(di.admin_locked, 0) = 0
              AND dp.user_id=?
              AND COALESCE(dp.can_access, 0)=1
              AND di.created_by <> ?
            ORDER BY CASE WHEN di.item_type='FOLDER' THEN 0 ELSE 1 END, LOWER(di.name) ASC
            LIMIT 500
            """,
            (uid, username),
        ).fetchall()

        out = []
        for r in rows:
            item = dict(r)
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
    item = conn.execute("SELECT * FROM doc_items WHERE id=? LIMIT 1", (item_id,)).fetchone()
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
        WHERE dp.item_id=?
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
          AND COALESCE(parent_id, -1) = COALESCE(?, -1)
          AND LOWER(name)=LOWER(?)
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
        ) VALUES (?, 'FOLDER', ?, 'GENERAL', ?, ?, ?, ?, 0, 1, ?, ?, NULL, NULL)
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

    item_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    save_item_permissions(conn, item_id, session["user"], permissions)
    sync_drive_shares(conn, item_id, created.get("id"))
    conn.commit()

    row = conn.execute("SELECT * FROM doc_items WHERE id=?", (item_id,)).fetchone()
    conn.close()
    return jsonify({"ok": True, "data": dict(row)})


@document_storage_bp.route("/api/docs/upload", methods=["POST"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_docs_upload():
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
        ) VALUES (?, 'DOCUMENT', ?, 'GENERAL', ?, ?, ?, ?, 0, 1, ?, ?, NULL, NULL)
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

    item_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    save_item_permissions(conn, item_id, session["user"], permissions)
    sync_drive_shares(conn, item_id, uploaded.get("id"))
    conn.commit()

    row = conn.execute("SELECT * FROM doc_items WHERE id=?", (item_id,)).fetchone()
    conn.close()
    return jsonify({"ok": True, "data": dict(row)})


@document_storage_bp.route("/api/docs/items/<int:item_id>", methods=["PUT"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_docs_item_update(item_id):
    data = request.json or {}
    uid = get_me_user_id()
    username = session.get("user")
    role = session.get("role")

    conn = db()
    item = conn.execute("SELECT * FROM doc_items WHERE id=? LIMIT 1", (item_id,)).fetchone()
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
              AND id<>?
              AND COALESCE(parent_id,-1)=COALESCE(?, -1)
              AND LOWER(name)=LOWER(?)
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
        SET name=?, notes=?, web_view_link=?, edited_at=?, edited_by=?
        WHERE id=?
        """,
        (new_name, notes, web_view_link, now_iso(), session["user"], item_id),
    )

    if permissions is not None:
        save_item_permissions(conn, item_id, item["created_by"], permissions)
        sync_drive_shares(conn, item_id, item["drive_id"])

    conn.commit()
    row = conn.execute("SELECT * FROM doc_items WHERE id=?", (item_id,)).fetchone()
    conn.close()
    return jsonify({"ok": True, "data": dict(row)})


@document_storage_bp.route("/api/docs/items/<int:item_id>/toggle-lock", methods=["POST"])
@login_required
@require_module("DOCUMENT_STORAGE", need_edit=True)
def api_docs_toggle_lock(item_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    item = conn.execute("SELECT id, admin_locked, deleted_at FROM doc_items WHERE id=? LIMIT 1", (item_id,)).fetchone()
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
        SET admin_locked=?, edited_at=?, edited_by=?
        WHERE id=?
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
    item = conn.execute("SELECT * FROM doc_items WHERE id=? LIMIT 1", (item_id,)).fetchone()
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
        WHERE parent_id=?
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
        SET deleted_at=?, deleted_by=?, edited_at=?, edited_by=?
        WHERE id=?
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
    item = conn.execute("SELECT * FROM doc_items WHERE id=? LIMIT 1", (item_id,)).fetchone()
    if not item:
        conn.close()
        return jsonify({"ok": False, "error": "Item not found"}), 404

    if not item["deleted_at"]:
        conn.close()
        return jsonify({"ok": False, "error": "Item is not in trash"}), 400

    conn.execute(
        """
        UPDATE doc_items
        SET deleted_at=NULL, deleted_by=NULL, edited_at=?, edited_by=?
        WHERE id=?
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
    item = conn.execute("SELECT * FROM doc_items WHERE id=? LIMIT 1", (item_id,)).fetchone()
    if not item:
        conn.close()
        return jsonify({"ok": False, "error": "Item not found"}), 404

    if not item["deleted_at"]:
        conn.close()
        return jsonify({"ok": False, "error": "Move item to trash first"}), 400

    conn.execute("DELETE FROM doc_item_permissions WHERE item_id=?", (item_id,))
    conn.execute("DELETE FROM doc_items WHERE id=?", (item_id,))
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
            row = conn.execute("SELECT id FROM doc_items WHERE id=? AND deleted_at IS NOT NULL", (item_id,)).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE doc_items
                    SET deleted_at=NULL, deleted_by=NULL, edited_at=?, edited_by=?
                    WHERE id=?
                    """,
                    (now_iso(), session["user"], item_id),
                )
                updated += 1

    elif action == "delete_forever":
        for item_id in valid_ids:
            row = conn.execute("SELECT id FROM doc_items WHERE id=? AND deleted_at IS NOT NULL", (item_id,)).fetchone()
            if row:
                conn.execute("DELETE FROM doc_item_permissions WHERE item_id=?", (item_id,))
                conn.execute("DELETE FROM doc_items WHERE id=?", (item_id,))
                updated += 1
    else:
        conn.close()
        return jsonify({"ok": False, "error": "Invalid bulk action"}), 400

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": {"updated": updated}})