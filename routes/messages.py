from flask import Blueprint, request, session, jsonify, current_app, url_for
from functools import wraps
from datetime import datetime
import os
import uuid

from werkzeug.utils import secure_filename

from .db_compat import sqlite3

messages_bp = Blueprint("messages", __name__)


# ======================
# HELPERS
# ======================
def db():
    database_url = current_app.config["DATABASE_URL"]
    return sqlite3.connect(database_url)


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


def ensure_message_upload_dir():
    upload_dir = os.path.join(current_app.root_path, "static", "uploads", "messages")
    os.makedirs(upload_dir, exist_ok=True)
    return upload_dir


def save_message_attachment(file_storage):
    if not file_storage or not getattr(file_storage, "filename", ""):
        return None

    upload_dir = ensure_message_upload_dir()
    safe_name = secure_filename(file_storage.filename) or "attachment"
    ext = os.path.splitext(safe_name)[1]
    stored_name = f"{uuid.uuid4().hex}{ext}"
    abs_path = os.path.join(upload_dir, stored_name)
    file_storage.save(abs_path)

    rel_path = f"uploads/messages/{stored_name}"

    return {
        "attachment_name": safe_name,
        "attachment_url": url_for("static", filename=rel_path),
        "attachment_mime": (file_storage.mimetype or "application/octet-stream"),
    }


def get_active_user_brief_rows(conn):
    return conn.execute("""
        SELECT id, username, role, active, full_name
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
        HAVING COUNT(DISTINCT CASE WHEN m1.user_id IS NOT NULL THEN m1.user_id END) >= 1
           AND COUNT(DISTINCT CASE WHEN m2.user_id IS NOT NULL THEN m2.user_id END) >= 1
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
               mm.attachment_name, mm.attachment_url,
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


def serialize_conversation(conn, conversation_row, user_id: int):
    convo = dict(conversation_row)

    members = conn.execute("""
        SELECT u.id, u.username, u.full_name, u.role
        FROM message_conversation_members m
        JOIN users u ON u.id = m.user_id
        WHERE m.conversation_id=? AND m.active=1
        ORDER BY u.role DESC, COALESCE(u.full_name, u.username) ASC
    """, (convo["id"],)).fetchall()

    convo["members"] = [dict(r) for r in members]
    convo["member_count"] = len(convo["members"])

    last_msg = get_conversation_last_message(conn, convo["id"])
    convo["last_message"] = dict(last_msg) if last_msg else None
    convo["unread_count"] = get_conversation_unread_count(conn, convo["id"], user_id)

    if convo["conversation_type"] == "DIRECT":
        other = None
        for m in convo["members"]:
            if int(m["id"]) != int(user_id):
                other = m
                break
        convo["display_name"] = (other or {}).get("full_name") or (other or {}).get("username") or "Direct Chat"
    else:
        convo["display_name"] = convo.get("title") or "Group Chat"

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

    total = sum(get_conversation_unread_count(conn, int(r["id"]), uid) for r in rows)
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

    data = []
    for r in rows:
        item = serialize_conversation(conn, r, uid)

        if (r["conversation_type"] or "").upper() == "DIRECT":
            other = conn.execute("""
                SELECT u.username, u.full_name
                FROM message_conversation_members m
                JOIN users u ON u.id = m.user_id
                WHERE m.conversation_id=?
                  AND m.active=1
                  AND u.id<>?
                LIMIT 1
            """, (r["id"], uid)).fetchone()

            if other:
                display_name = other["full_name"] or other["username"]
                item["other_user_name"] = display_name
                item["title"] = display_name

        data.append(item)

    data.sort(
        key=lambda x: (x["last_message"]["created_at"] if x.get("last_message") else x["created_at"]),
        reverse=True
    )

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

    payload = serialize_conversation(conn, row, uid)
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

    payload = serialize_conversation(conn, row, int(session.get("uid")))
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

    payload = serialize_conversation(conn, row, uid)

    if (row["conversation_type"] or "").upper() == "DIRECT":
        other = conn.execute("""
            SELECT u.username, u.full_name
            FROM message_conversation_members m
            JOIN users u ON u.id = m.user_id
            WHERE m.conversation_id=?
              AND m.active=1
              AND u.id<>?
            LIMIT 1
        """, (conversation_id, uid)).fetchone()
        if other:
            payload["other_user_name"] = other["full_name"] or other["username"]
            payload["title"] = other["full_name"] or other["username"]

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
        SELECT u.id, u.username, u.full_name, u.role
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
        LIMIT 2000
    """, (conversation_id,)).fetchall()

    mark_conversation_read(conn, conversation_id, uid)
    conn.commit()

    data = []
    for r in rows:
        item = dict(r)
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
    attachment_url = None
    attachment_mime = None
    reply_to = None

    if request.content_type and request.content_type.startswith("multipart/form-data"):
        message_text = (request.form.get("message_text") or "").strip()
        reply_to = request.form.get("reply_to")
        file = request.files.get("file")
        att = save_message_attachment(file) if file else None

        if att:
            attachment_name = att["attachment_name"]
            attachment_url = att["attachment_url"]
            attachment_mime = att["attachment_mime"]
    else:
        data = request.json or {}
        message_text = (data.get("message_text") or "").strip()
        reply_to = data.get("reply_to")
        attachment_name = (data.get("attachment_name") or "").strip() or None
        attachment_url = (data.get("attachment_url") or "").strip() or None
        attachment_mime = (data.get("attachment_mime") or "").strip() or None

    if reply_to in ("", None):
        reply_to = None
    else:
        try:
            reply_to = int(reply_to)
        except Exception:
            reply_to = None

    if not message_text and not attachment_url:
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
            created_at
        ) VALUES (?,?,?,?,?,?,?,?)
    """, (
        conversation_id,
        uid,
        message_text,
        reply_to,
        attachment_name,
        attachment_url,
        attachment_mime,
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

    payload = dict(row)
    payload["can_delete"] = can_delete_message(row, uid)

    conn.close()
    return jsonify({"ok": True, "data": payload})


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

    conn.execute("""
        UPDATE message_messages
        SET deleted_at=?,
            deleted_by=?,
            message_text='This message was deleted',
            attachment_name=NULL,
            attachment_url=NULL,
            attachment_mime=NULL
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
        item = serialize_conversation(conn, r, uid)
        item["user_deleted_at"] = r["user_deleted_at"]

        if (r["conversation_type"] or "").upper() == "DIRECT":
            other = conn.execute("""
                SELECT u.username, u.full_name
                FROM message_conversation_members m
                JOIN users u ON u.id = m.user_id
                WHERE m.conversation_id=?
                  AND m.active=1
                  AND u.id<>?
                LIMIT 1
            """, (r["id"], uid)).fetchone()
            if other:
                item["other_user_name"] = other["full_name"] or other["username"]
                item["title"] = other["full_name"] or other["username"]

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

    conn.commit()
    conn.close()
    return jsonify({"ok": True})