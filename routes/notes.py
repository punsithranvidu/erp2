from datetime import datetime
from functools import wraps
from zoneinfo import ZoneInfo

from flask import Blueprint, current_app, jsonify, render_template, request, session

from .db import connect, get_table_columns

notes_bp = Blueprint("notes", __name__)

NOTE_STATUSES = ("Active", "Done", "Cancelled")
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


def normalize_status(value):
    status = clean_text(value) or "Active"
    return status if status in NOTE_STATUSES else None


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

    cur.execute("CREATE INDEX IF NOT EXISTS idx_notes_deleted_status ON notes (is_deleted, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notes_created_by_user ON notes (created_by_user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notes_expected_end_date ON notes (expected_end_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_note_due_history_note_id ON note_due_date_history (note_id)")

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


def get_note(conn, note_id, include_deleted=False):
    deleted_sql = "" if include_deleted else "AND is_deleted=0"
    return conn.execute(f"""
        SELECT *
        FROM notes
        WHERE id=%s {deleted_sql}
        LIMIT 1
    """, (note_id,)).fetchone()


def note_dict(row):
    out = dict(row)
    out["can_edit"] = 1 if can_modify_note(row) and int(row["is_deleted"] or 0) == 0 else 0
    out["can_delete"] = out["can_edit"]
    out["can_status"] = 1 if can_status_note(row) else 0
    return out


def build_note_filters(args, include_deleted=False):
    tab = clean_text(args.get("tab")) or "active"
    q = clean_text(args.get("q"))
    status = clean_text(args.get("status"))
    expected_date = clean_text(args.get("expected_date"))
    user_id = clean_text(args.get("user_id"))

    where = ["n.is_deleted=%s"]
    vals = [1 if include_deleted else 0]

    if q:
        where.append("n.note_text ILIKE %s")
        vals.append(f"%{q}%")

    if status and status.upper() != "ALL":
        if status in NOTE_STATUSES:
            where.append("n.status=%s")
            vals.append(status)
    elif tab == "active" and not include_deleted:
        where.append("n.status=%s")
        vals.append("Active")

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
        ORDER BY n.id DESC
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
    try:
        page = max(1, int(request.args.get("page", 1)))
    except Exception:
        page = 1
    try:
        per_page = int(request.args.get("per_page", 12))
        per_page = min(48, max(6, per_page))
    except Exception:
        per_page = 12

    where_sql, vals = build_note_filters(request.args, include_deleted=False)
    offset = (page - 1) * per_page

    conn = db()
    total = conn.execute(f"""
        SELECT COUNT(*) AS c
        FROM notes n
        WHERE {where_sql}
    """, tuple(vals)).fetchone()["c"]

    rows = conn.execute(f"""
        SELECT n.*
        FROM notes n
        WHERE {where_sql}
        ORDER BY n.id DESC
        LIMIT %s OFFSET %s
    """, tuple(vals + [per_page, offset])).fetchall()

    active_pages = previous_active_pages(conn, where_sql, vals, page, per_page)
    conn.close()

    total_pages = max(1, ((int(total or 0) + per_page - 1) // per_page))
    return jsonify({
        "ok": True,
        "data": [note_dict(r) for r in rows],
        "page": page,
        "per_page": per_page,
        "total": int(total or 0),
        "total_pages": total_pages,
        "previous_active_pages": active_pages,
    })


@notes_bp.route("/api/notes", methods=["POST"])
@login_required
@require_module("NOTES", need_edit=True)
def api_notes_create():
    data = request.json or {}
    note_text = clean_text(data.get("note_text"))
    expected_end_date = clean_due(data.get("expected_end_date"))

    if not note_text:
        return jsonify({"ok": False, "error": "Note text is required"}), 400

    created_at = now_iso()
    conn = db()
    row = conn.execute("""
        INSERT INTO notes (
            note_text, expected_end_date, status,
            created_by_user_id, created_by, created_at
        )
        VALUES (%s,%s,'Active',%s,%s,%s)
        RETURNING *
    """, (
        note_text,
        expected_end_date,
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
    note_text = clean_text(data.get("note_text"))
    expected_end_date = clean_due(data.get("expected_end_date"))
    status = normalize_status(data.get("status"))

    if not note_text:
        return jsonify({"ok": False, "error": "Note text is required"}), 400
    if not status:
        return jsonify({"ok": False, "error": "Invalid note status"}), 400

    conn = db()
    old = get_note(conn, note_id)
    if not old:
        conn.close()
        return jsonify({"ok": False, "error": "Note not found"}), 404
    if not can_modify_note(old):
        conn.close()
        return jsonify({"ok": False, "error": "You can edit only your own notes"}), 403

    changed_at = now_iso()
    done_at = old["done_at"]
    cancelled_at = old["cancelled_at"]

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

    rows = conn.execute("""
        SELECT *
        FROM note_due_date_history
        WHERE note_id=%s
        ORDER BY id DESC
    """, (note_id,)).fetchall()
    conn.close()
    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


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
