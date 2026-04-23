from datetime import datetime
from functools import wraps
from zoneinfo import ZoneInfo

from flask import Blueprint, current_app, jsonify, render_template, request, session

from .db import connect, get_table_columns

notes_bp = Blueprint("notes", __name__)

NOTE_STATUSES = ("Active", "Done", "Cancelled")
NOTE_AUDIENCES = ("EVERYONE", "ADMINS")
NOTES_PER_PAGE = 8
MAX_NOTE_TEXT_LENGTH = 1000
APP_TZ = ZoneInfo("Asia/Colombo")


def db():
    return connect(current_app.config["DATABASE_URL"])


def now_iso():
    return datetime.now(APP_TZ).replace(tzinfo=None).isoformat(timespec="seconds")


def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if "user" not in session:
            if request.path.startswith("/api/"):
                return jsonify({"ok": False, "error": "Login required"}), 401
            return current_app.config["LOGIN_REQUIRED_FUNC"](f)(*args, **kwargs)
        return f(*args, **kwargs)
    return wrapped


def is_admin():
    return session.get("role") == "ADMIN"


def current_user_id():
    return session.get("uid")


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
                if request.path.startswith("/api/"):
                    return jsonify({
                        "ok": False,
                        "error": f"No permission for {module}{' (edit)' if need_edit else ''}"
                    }), 403
                return current_app.config["REQUIRE_MODULE_FUNC"](module, need_edit)
            return f(*args, **kwargs)
        return wrapped
    return deco


def clean_text(value):
    return (value or "").strip()


def clean_due(value):
    return clean_text(value) or None


def validate_note_text(value):
    text = clean_text(value)
    if not text:
        return None, "Note text is required"
    if len(text) > MAX_NOTE_TEXT_LENGTH:
        return None, f"Note text must be {MAX_NOTE_TEXT_LENGTH} characters or less"
    return text, None


def normalize_status(value):
    status = clean_text(value) or "Active"
    return status if status in NOTE_STATUSES else None


def normalize_audience(value):
    audience = clean_text(value).upper() or "EVERYONE"
    return audience if audience in NOTE_AUDIENCES else "EVERYONE"


def ensure_notes_tables():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            id BIGSERIAL PRIMARY KEY,
            note_text TEXT NOT NULL,
            expected_end_date TEXT,
            status TEXT NOT NULL DEFAULT 'Active',
            done_at TEXT,
            cancelled_at TEXT,
            created_by_user_id BIGINT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            edited_by TEXT,
            edited_at TEXT,
            audience TEXT NOT NULL DEFAULT 'EVERYONE',
            is_deleted INTEGER NOT NULL DEFAULT 0,
            deleted_by TEXT,
            deleted_by_user_id BIGINT,
            deleted_at TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS note_due_date_history (
            id BIGSERIAL PRIMARY KEY,
            note_id BIGINT NOT NULL,
            old_expected_end_date TEXT,
            new_expected_end_date TEXT,
            changed_at TEXT NOT NULL,
            changed_by TEXT NOT NULL,
            changed_by_user_id BIGINT,
            FOREIGN KEY(note_id) REFERENCES notes(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS note_edit_history (
            id BIGSERIAL PRIMARY KEY,
            note_id BIGINT NOT NULL,
            edit_type TEXT NOT NULL DEFAULT 'UPDATE',
            edited_at TEXT NOT NULL,
            edited_by TEXT NOT NULL,
            edited_by_user_id BIGINT,
            FOREIGN KEY(note_id) REFERENCES notes(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS note_settings (
            setting_key TEXT PRIMARY KEY,
            setting_value TEXT,
            edited_at TEXT,
            edited_by TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS note_notification_state (
            user_id BIGINT PRIMARY KEY,
            last_seen_at TEXT NOT NULL DEFAULT ''
        )
    """)

    cols = get_table_columns(conn, "notes")
    missing = {
        "note_text": "ALTER TABLE notes ADD COLUMN note_text TEXT NOT NULL DEFAULT ''",
        "expected_end_date": "ALTER TABLE notes ADD COLUMN expected_end_date TEXT",
        "status": "ALTER TABLE notes ADD COLUMN status TEXT NOT NULL DEFAULT 'Active'",
        "done_at": "ALTER TABLE notes ADD COLUMN done_at TEXT",
        "cancelled_at": "ALTER TABLE notes ADD COLUMN cancelled_at TEXT",
        "created_by_user_id": "ALTER TABLE notes ADD COLUMN created_by_user_id BIGINT",
        "created_by": "ALTER TABLE notes ADD COLUMN created_by TEXT NOT NULL DEFAULT ''",
        "created_at": "ALTER TABLE notes ADD COLUMN created_at TEXT NOT NULL DEFAULT ''",
        "edited_by": "ALTER TABLE notes ADD COLUMN edited_by TEXT",
        "edited_at": "ALTER TABLE notes ADD COLUMN edited_at TEXT",
        "audience": "ALTER TABLE notes ADD COLUMN audience TEXT NOT NULL DEFAULT 'EVERYONE'",
        "is_deleted": "ALTER TABLE notes ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0",
        "deleted_by": "ALTER TABLE notes ADD COLUMN deleted_by TEXT",
        "deleted_by_user_id": "ALTER TABLE notes ADD COLUMN deleted_by_user_id BIGINT",
        "deleted_at": "ALTER TABLE notes ADD COLUMN deleted_at TEXT",
    }
    for col, sql in missing.items():
        if col not in cols:
            cur.execute(sql)

    hcols = get_table_columns(conn, "note_due_date_history")
    history_missing = {
        "note_id": "ALTER TABLE note_due_date_history ADD COLUMN note_id BIGINT",
        "old_expected_end_date": "ALTER TABLE note_due_date_history ADD COLUMN old_expected_end_date TEXT",
        "new_expected_end_date": "ALTER TABLE note_due_date_history ADD COLUMN new_expected_end_date TEXT",
        "changed_at": "ALTER TABLE note_due_date_history ADD COLUMN changed_at TEXT NOT NULL DEFAULT ''",
        "changed_by": "ALTER TABLE note_due_date_history ADD COLUMN changed_by TEXT NOT NULL DEFAULT ''",
        "changed_by_user_id": "ALTER TABLE note_due_date_history ADD COLUMN changed_by_user_id BIGINT",
    }
    for col, sql in history_missing.items():
        if col not in hcols:
            cur.execute(sql)

    ecols = get_table_columns(conn, "note_edit_history")
    edit_missing = {
        "note_id": "ALTER TABLE note_edit_history ADD COLUMN note_id BIGINT",
        "edit_type": "ALTER TABLE note_edit_history ADD COLUMN edit_type TEXT NOT NULL DEFAULT 'UPDATE'",
        "edited_at": "ALTER TABLE note_edit_history ADD COLUMN edited_at TEXT NOT NULL DEFAULT ''",
        "edited_by": "ALTER TABLE note_edit_history ADD COLUMN edited_by TEXT NOT NULL DEFAULT ''",
        "edited_by_user_id": "ALTER TABLE note_edit_history ADD COLUMN edited_by_user_id BIGINT",
    }
    for col, sql in edit_missing.items():
        if col not in ecols:
            cur.execute(sql)

    scols = get_table_columns(conn, "note_settings")
    settings_missing = {
        "setting_key": "ALTER TABLE note_settings ADD COLUMN setting_key TEXT",
        "setting_value": "ALTER TABLE note_settings ADD COLUMN setting_value TEXT",
        "edited_at": "ALTER TABLE note_settings ADD COLUMN edited_at TEXT",
        "edited_by": "ALTER TABLE note_settings ADD COLUMN edited_by TEXT",
    }
    for col, sql in settings_missing.items():
        if col not in scols:
            cur.execute(sql)

    ncols = get_table_columns(conn, "note_notification_state")
    notification_missing = {
        "user_id": "ALTER TABLE note_notification_state ADD COLUMN user_id BIGINT",
        "last_seen_at": "ALTER TABLE note_notification_state ADD COLUMN last_seen_at TEXT NOT NULL DEFAULT ''",
    }
    for col, sql in notification_missing.items():
        if col not in ncols:
            cur.execute(sql)

    cur.execute("""
        UPDATE notes
        SET audience='EVERYONE'
        WHERE audience IS NULL OR TRIM(audience)=''
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_notes_deleted_status ON notes (is_deleted, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notes_created_by_user ON notes (created_by_user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notes_expected_end_date ON notes (expected_end_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notes_audience ON notes (audience)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notes_created_at ON notes (created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_note_due_history_note_id ON note_due_date_history (note_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_note_edit_history_note_id ON note_edit_history (note_id)")

    conn.commit()
    conn.close()


@notes_bp.before_app_request
def notes_tables_ready():
    if current_app.config.get("NOTES_TABLES_READY"):
        return
    ensure_notes_tables()
    current_app.config["NOTES_TABLES_READY"] = True


def can_modify_note(row):
    if not row:
        return False
    if is_admin():
        return True
    return int(row["created_by_user_id"] or 0) == int(current_user_id() or 0)


def can_status_note(row):
    return bool(row) and int(row["is_deleted"] or 0) == 0


def note_visible_to_viewer(row, viewer_created_at=None):
    if not row:
        return False
    if is_admin():
        return True
    if (row["audience"] or "EVERYONE") != "EVERYONE":
        return False
    if not viewer_created_at:
        return True
    expected_due = clean_text(row["expected_end_date"])
    done_at = clean_text(row["done_at"])
    cancelled_at = clean_text(row["cancelled_at"])
    if expected_due and expected_due < viewer_created_at:
        return False
    if done_at and done_at < viewer_created_at:
        return False
    if cancelled_at and cancelled_at < viewer_created_at:
        return False
    return True


def get_note(conn, note_id, include_deleted=False):
    deleted_sql = "" if include_deleted else "AND is_deleted=0"
    return conn.execute(f"""
        SELECT *
        FROM notes
        WHERE id=%s {deleted_sql}
        LIMIT 1
    """, (note_id,)).fetchone()


def get_current_user_row(conn):
    uid = current_user_id()
    if not uid:
        return None
    return conn.execute("""
        SELECT id, username, role, created_at
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (uid,)).fetchone()


def get_note_setting(conn, key):
    row = conn.execute("""
        SELECT setting_value
        FROM note_settings
        WHERE setting_key=%s
        LIMIT 1
    """, (key,)).fetchone()
    return row["setting_value"] if row else None


def set_note_setting(conn, key, value):
    conn.execute("""
        INSERT INTO note_settings (setting_key, setting_value, edited_at, edited_by)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT(setting_key)
        DO UPDATE SET
            setting_value=excluded.setting_value,
            edited_at=excluded.edited_at,
            edited_by=excluded.edited_by
    """, (key, value, now_iso(), session["user"]))


def get_note_notification_seen_at(conn, user_id):
    row = conn.execute("""
        SELECT last_seen_at
        FROM note_notification_state
        WHERE user_id=%s
        LIMIT 1
    """, (user_id,)).fetchone()
    return row["last_seen_at"] if row else ""


def mark_note_notifications_seen(conn):
    uid = current_user_id()
    if not uid:
        return
    conn.execute("""
        INSERT INTO note_notification_state (user_id, last_seen_at)
        VALUES (%s,%s)
        ON CONFLICT(user_id)
        DO UPDATE SET last_seen_at=excluded.last_seen_at
    """, (uid, now_iso()))


def note_dict(row):
    out = dict(row)
    out["can_edit"] = 1 if can_modify_note(row) and int(row["is_deleted"] or 0) == 0 else 0
    out["can_delete"] = out["can_edit"]
    out["can_status"] = 1 if can_status_note(row) and has_module_access("NOTES", True) else 0
    return out


def record_edit_history(conn, note_id, edited_at, edit_type="UPDATE"):
    conn.execute("""
        INSERT INTO note_edit_history (
            note_id, edit_type, edited_at, edited_by, edited_by_user_id
        )
        VALUES (%s,%s,%s,%s,%s)
    """, (
        note_id,
        edit_type,
        edited_at,
        session["user"],
        current_user_id(),
    ))


def has_search_filters(args):
    q = clean_text(args.get("q"))
    status = clean_text(args.get("status"))
    expected_date = clean_text(args.get("expected_date"))
    user_id = clean_text(args.get("user_id"))
    return bool(
        q
        or expected_date
        or (status and status.upper() != "ALL")
        or (user_id and user_id.upper() != "ALL")
    )


def build_note_filters(args, include_deleted=False, viewer_created_at=None):
    tab = clean_text(args.get("tab")) or "active"
    q = clean_text(args.get("q"))
    status = clean_text(args.get("status"))
    expected_date = clean_text(args.get("expected_date"))
    user_id = clean_text(args.get("user_id"))

    where = ["n.is_deleted=%s"]
    vals = [1 if include_deleted else 0]

    if not is_admin():
        where.append("COALESCE(n.audience, 'EVERYONE')='EVERYONE'")
        if viewer_created_at:
            where.append("""
                NOT (
                    (n.expected_end_date IS NOT NULL AND TRIM(n.expected_end_date)<>'' AND n.expected_end_date < %s)
                    OR (n.done_at IS NOT NULL AND TRIM(n.done_at)<>'' AND n.done_at < %s)
                    OR (n.cancelled_at IS NOT NULL AND TRIM(n.cancelled_at)<>'' AND n.cancelled_at < %s)
                )
            """)
            vals.extend([viewer_created_at, viewer_created_at, viewer_created_at])

    if q:
        where.append("n.note_text ILIKE %s")
        vals.append(f"%{q}%")

    if status and status.upper() != "ALL":
        if status in NOTE_STATUSES:
            where.append("n.status=%s")
            vals.append(status)

    if expected_date:
        where.append("COALESCE(n.expected_end_date, '') LIKE %s")
        vals.append(f"{expected_date}%")

    if tab == "mine" and not include_deleted:
        if is_admin():
            if user_id and user_id.upper() != "ALL":
                try:
                    where.append("n.created_by_user_id=%s")
                    vals.append(int(user_id))
                except ValueError:
                    where.append("n.created_by_user_id=%s")
                    vals.append(int(current_user_id() or 0))
            elif not user_id:
                where.append("n.created_by_user_id=%s")
                vals.append(int(current_user_id() or 0))
        else:
            where.append("n.created_by_user_id=%s")
            vals.append(int(current_user_id() or 0))
    elif user_id and user_id.upper() != "ALL" and is_admin():
        try:
            where.append("n.created_by_user_id=%s")
            vals.append(int(user_id))
        except ValueError:
            pass

    return " AND ".join(where), vals


def previous_active_pages(conn, where_sql, vals, page, per_page):
    offset = max(0, (page - 1) * per_page)
    if offset <= 0:
        return []

    rows = conn.execute(f"""
        SELECT n.status
        FROM notes n
        WHERE {where_sql}
        ORDER BY n.id ASC
        LIMIT %s
    """, tuple(vals + [offset])).fetchall()

    pages = []
    for idx, row in enumerate(rows):
        if row["status"] == "Active":
            p = (idx // per_page) + 1
            if p not in pages:
                pages.append(p)
    return pages


@notes_bp.route("/notes")
@login_required
@require_module("NOTES")
def notes_page():
    return render_template("notes.html", user=session.get("user"), role=session.get("role"))


@notes_bp.route("/api/notes/users", methods=["GET"])
@login_required
@require_module("NOTES")
def api_notes_users():
    if not is_admin():
        return jsonify({"ok": True, "data": []})

    conn = db()
    rows = conn.execute("""
        SELECT id, username, role, active
        FROM users
        ORDER BY username ASC
    """).fetchall()
    conn.close()
    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


@notes_bp.route("/api/notes", methods=["GET"])
@login_required
@require_module("NOTES")
def api_notes_list():
    conn = db()
    viewer = get_current_user_row(conn)
    viewer_created_at = viewer["created_at"] if viewer else None
    tab = clean_text(request.args.get("tab")) or "active"
    where_sql, vals = build_note_filters(
        request.args,
        include_deleted=False,
        viewer_created_at=viewer_created_at
    )
    search_mode = has_search_filters(request.args)

    total = conn.execute(f"""
        SELECT COUNT(*) AS c
        FROM notes n
        WHERE {where_sql}
    """, tuple(vals)).fetchone()["c"]
    total = int(total or 0)
    per_page = NOTES_PER_PAGE
    total_pages = max(1, ((total + per_page - 1) // per_page))

    configured_default = get_note_setting(conn, "default_active_page")
    configured_page = None
    if configured_default:
        try:
            configured_page = max(1, int(configured_default))
        except Exception:
            configured_page = None

    raw_page = clean_text(request.args.get("page")) or "default"
    if search_mode:
        page = 1
        rows = conn.execute(f"""
            SELECT n.*
            FROM notes n
            WHERE {where_sql}
            ORDER BY n.id ASC
        """, tuple(vals)).fetchall()
        active_pages = []
    else:
        if raw_page.lower() == "default":
            page = (configured_page if tab == "active" else None) or total_pages
        else:
            try:
                page = max(1, int(raw_page))
            except Exception:
                page = (configured_page if tab == "active" else None) or total_pages
        page = min(max(1, page), total_pages)
        offset = (page - 1) * per_page

        rows = conn.execute(f"""
            SELECT n.*
            FROM notes n
            WHERE {where_sql}
            ORDER BY n.id ASC
            LIMIT %s OFFSET %s
        """, tuple(vals + [per_page, offset])).fetchall()

        active_pages = previous_active_pages(conn, where_sql, vals, page, per_page)
    mark_note_notifications_seen(conn)
    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "data": [note_dict(r) for r in rows],
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": total_pages,
        "previous_active_pages": active_pages,
        "search_mode": search_mode,
        "configured_default_page": configured_page,
    })


@notes_bp.route("/api/notes/notification-count", methods=["GET"])
@login_required
@require_module("NOTES")
def api_notes_notification_count():
    conn = db()
    viewer = get_current_user_row(conn)
    if not viewer:
        conn.close()
        return jsonify({"ok": True, "data": {"notification_count": 0}})

    uid = int(viewer["id"])
    last_seen = get_note_notification_seen_at(conn, uid)
    where = [
        "n.is_deleted=0",
        "n.created_by_user_id<>%s",
    ]
    vals = [uid]

    if last_seen:
        where.append("n.created_at>%s")
        vals.append(last_seen)

    if is_admin():
        where.append("COALESCE(n.audience, 'EVERYONE') IN ('EVERYONE','ADMINS')")
    else:
        where.append("COALESCE(n.audience, 'EVERYONE')='EVERYONE'")
        viewer_created_at = viewer["created_at"]
        if viewer_created_at:
            where.append("""
                NOT (
                    (n.expected_end_date IS NOT NULL AND TRIM(n.expected_end_date)<>'' AND n.expected_end_date < %s)
                    OR (n.done_at IS NOT NULL AND TRIM(n.done_at)<>'' AND n.done_at < %s)
                    OR (n.cancelled_at IS NOT NULL AND TRIM(n.cancelled_at)<>'' AND n.cancelled_at < %s)
                )
            """)
            vals.extend([viewer_created_at, viewer_created_at, viewer_created_at])

    row = conn.execute(f"""
        SELECT COUNT(n.id) AS notification_count
        FROM notes n
        WHERE {" AND ".join(where)}
    """, tuple(vals)).fetchone()
    conn.close()
    return jsonify({"ok": True, "data": {"notification_count": int(row["notification_count"] or 0)}})


@notes_bp.route("/api/notes/default-page", methods=["POST"])
@login_required
@require_module("NOTES", need_edit=True)
def api_notes_default_page_set():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    try:
        page = max(1, int(data.get("page") or 1))
    except Exception:
        return jsonify({"ok": False, "error": "Invalid page"}), 400

    conn = db()
    set_note_setting(conn, "default_active_page", str(page))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": {"page": page}, "message": f"Default opening page set to {page}"})


@notes_bp.route("/api/notes", methods=["POST"])
@login_required
@require_module("NOTES", need_edit=True)
def api_notes_create():
    data = request.json or {}
    note_text, text_error = validate_note_text(data.get("note_text"))
    expected_end_date = clean_due(data.get("expected_end_date"))
    audience = normalize_audience(data.get("audience")) if is_admin() else "EVERYONE"

    if text_error:
        return jsonify({"ok": False, "error": text_error}), 400

    created_at = now_iso()
    conn = db()
    row = conn.execute("""
        INSERT INTO notes (
            note_text, expected_end_date, status, audience,
            created_by_user_id, created_by, created_at
        )
        VALUES (%s,%s,'Active',%s,%s,%s,%s)
        RETURNING *
    """, (
        note_text,
        expected_end_date,
        audience,
        current_user_id(),
        session["user"],
        created_at,
    )).fetchone()

    if expected_end_date:
        conn.execute("""
            INSERT INTO note_due_date_history (
                note_id, old_expected_end_date, new_expected_end_date,
                changed_at, changed_by, changed_by_user_id
            )
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            row["id"],
            None,
            expected_end_date,
            created_at,
            session["user"],
            current_user_id(),
        ))

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": note_dict(row), "message": "Note saved successfully"})


@notes_bp.route("/api/notes/<int:note_id>", methods=["PUT"])
@login_required
@require_module("NOTES", need_edit=True)
def api_notes_update(note_id):
    data = request.json or {}
    note_text, text_error = validate_note_text(data.get("note_text"))
    expected_end_date = clean_due(data.get("expected_end_date"))
    status = normalize_status(data.get("status"))
    requested_audience = normalize_audience(data.get("audience"))

    if text_error:
        return jsonify({"ok": False, "error": text_error}), 400
    if not status:
        return jsonify({"ok": False, "error": "Invalid note status"}), 400

    conn = db()
    old = get_note(conn, note_id)
    if not old:
        conn.close()
        return jsonify({"ok": False, "error": "Note not found"}), 404
    viewer = get_current_user_row(conn)
    if not note_visible_to_viewer(old, viewer["created_at"] if viewer else None):
        conn.close()
        return jsonify({"ok": False, "error": "Note not found"}), 404
    if not can_modify_note(old):
        conn.close()
        return jsonify({"ok": False, "error": "You can edit only your own notes"}), 403

    changed_at = now_iso()
    done_at = old["done_at"]
    cancelled_at = old["cancelled_at"]
    audience = requested_audience if is_admin() else (old["audience"] or "EVERYONE")

    if status == "Done" and old["status"] != "Done":
        done_at = changed_at
        cancelled_at = None
    elif status == "Cancelled" and old["status"] != "Cancelled":
        cancelled_at = changed_at
        done_at = None
    elif status == "Active":
        done_at = None
        cancelled_at = None

    row = conn.execute("""
        UPDATE notes
        SET note_text=%s,
            expected_end_date=%s,
            status=%s,
            audience=%s,
            done_at=%s,
            cancelled_at=%s,
            edited_by=%s,
            edited_at=%s
        WHERE id=%s
        RETURNING *
    """, (
        note_text,
        expected_end_date,
        status,
        audience,
        done_at,
        cancelled_at,
        session["user"],
        changed_at,
        note_id,
    )).fetchone()

    old_due = old["expected_end_date"]
    if (old_due or "") != (expected_end_date or ""):
        conn.execute("""
            INSERT INTO note_due_date_history (
                note_id, old_expected_end_date, new_expected_end_date,
                changed_at, changed_by, changed_by_user_id
            )
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            note_id,
            old_due,
            expected_end_date,
            changed_at,
            session["user"],
            current_user_id(),
        ))

    record_edit_history(conn, note_id, changed_at, "UPDATE")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": note_dict(row), "message": "Note updated successfully"})


@notes_bp.route("/api/notes/<int:note_id>/status", methods=["POST"])
@login_required
@require_module("NOTES", need_edit=True)
def api_notes_status(note_id):
    data = request.json or {}
    status = normalize_status(data.get("status"))
    if not status:
        return jsonify({"ok": False, "error": "Invalid note status"}), 400

    conn = db()
    old = get_note(conn, note_id)
    if not old:
        conn.close()
        return jsonify({"ok": False, "error": "Note not found"}), 404
    viewer = get_current_user_row(conn)
    if not note_visible_to_viewer(old, viewer["created_at"] if viewer else None):
        conn.close()
        return jsonify({"ok": False, "error": "Note not found"}), 404
    if not can_status_note(old):
        conn.close()
        return jsonify({"ok": False, "error": "Cannot update this note"}), 403

    changed_at = now_iso()
    done_at = old["done_at"]
    cancelled_at = old["cancelled_at"]

    if status == "Done":
        done_at = done_at or changed_at
        cancelled_at = None
    elif status == "Cancelled":
        cancelled_at = cancelled_at or changed_at
        done_at = None
    else:
        done_at = None
        cancelled_at = None

    row = conn.execute("""
        UPDATE notes
        SET status=%s,
            done_at=%s,
            cancelled_at=%s,
            edited_by=%s,
            edited_at=%s
        WHERE id=%s
        RETURNING *
    """, (status, done_at, cancelled_at, session["user"], changed_at, note_id)).fetchone()

    record_edit_history(conn, note_id, changed_at, f"STATUS:{status}")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": note_dict(row), "message": "Status updated"})


@notes_bp.route("/api/notes/<int:note_id>", methods=["DELETE"])
@login_required
@require_module("NOTES", need_edit=True)
def api_notes_delete(note_id):
    conn = db()
    row = get_note(conn, note_id)
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Note not found"}), 404
    viewer = get_current_user_row(conn)
    if not note_visible_to_viewer(row, viewer["created_at"] if viewer else None):
        conn.close()
        return jsonify({"ok": False, "error": "Note not found"}), 404
    if not can_modify_note(row):
        conn.close()
        return jsonify({"ok": False, "error": "You can delete only your own notes"}), 403

    deleted_at = now_iso()
    conn.execute("""
        UPDATE notes
        SET is_deleted=1,
            deleted_at=%s,
            deleted_by=%s,
            deleted_by_user_id=%s,
            edited_at=%s,
            edited_by=%s
        WHERE id=%s
    """, (
        deleted_at,
        session["user"],
        current_user_id(),
        deleted_at,
        session["user"],
        note_id,
    ))
    record_edit_history(conn, note_id, deleted_at, "DELETE")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Note moved to trash"})


@notes_bp.route("/api/notes/<int:note_id>/history", methods=["GET"])
@login_required
@require_module("NOTES")
def api_notes_history(note_id):
    conn = db()
    note = get_note(conn, note_id, include_deleted=is_admin())
    if not note:
        conn.close()
        return jsonify({"ok": False, "error": "Note not found"}), 404
    if int(note["is_deleted"] or 0) == 1 and not is_admin():
        conn.close()
        return jsonify({"ok": False, "error": "Admin only"}), 403
    viewer = get_current_user_row(conn)
    if not note_visible_to_viewer(note, viewer["created_at"] if viewer else None):
        conn.close()
        return jsonify({"ok": False, "error": "Note not found"}), 404

    due_rows = conn.execute("""
        SELECT *
        FROM note_due_date_history
        WHERE note_id=%s
        ORDER BY id DESC
    """, (note_id,)).fetchall()
    edit_rows = conn.execute("""
        SELECT *
        FROM note_edit_history
        WHERE note_id=%s
        ORDER BY id ASC
    """, (note_id,)).fetchall()
    conn.close()
    return jsonify({
        "ok": True,
        "data": {
            "due_date_history": [dict(r) for r in due_rows],
            "edit_history": [dict(r) for r in edit_rows],
        }
    })


@notes_bp.route("/api/notes/trash", methods=["GET"])
@login_required
@require_module("NOTES")
def api_notes_trash():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    where_sql, vals = build_note_filters(request.args, include_deleted=True)
    conn = db()
    rows = conn.execute(f"""
        SELECT n.*
        FROM notes n
        WHERE {where_sql}
        ORDER BY n.deleted_at DESC NULLS LAST, n.id DESC
        LIMIT 500
    """, tuple(vals)).fetchall()
    conn.close()
    return jsonify({"ok": True, "data": [note_dict(r) for r in rows]})


@notes_bp.route("/api/notes/trash/<int:note_id>/recover", methods=["POST"])
@login_required
@require_module("NOTES", need_edit=True)
def api_notes_recover(note_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    row = get_note(conn, note_id, include_deleted=True)
    if not row or int(row["is_deleted"] or 0) != 1:
        conn.close()
        return jsonify({"ok": False, "error": "Trash note not found"}), 404

    changed_at = now_iso()
    row = conn.execute("""
        UPDATE notes
        SET is_deleted=0,
            deleted_at=NULL,
            deleted_by=NULL,
            deleted_by_user_id=NULL,
            edited_at=%s,
            edited_by=%s
        WHERE id=%s
        RETURNING *
    """, (changed_at, session["user"], note_id)).fetchone()
    record_edit_history(conn, note_id, changed_at, "RECOVER")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": note_dict(row), "message": "Note recovered"})


@notes_bp.route("/api/notes/trash/<int:note_id>/permanent", methods=["DELETE"])
@login_required
@require_module("NOTES", need_edit=True)
def api_notes_permanent_delete(note_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    row = get_note(conn, note_id, include_deleted=True)
    if not row or int(row["is_deleted"] or 0) != 1:
        conn.close()
        return jsonify({"ok": False, "error": "Trash note not found"}), 404

    conn.execute("DELETE FROM notes WHERE id=%s", (note_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Note permanently deleted"})
