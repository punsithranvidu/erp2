from flask import Blueprint, render_template, request, jsonify, session, current_app
from functools import wraps
from datetime import datetime, timedelta
from .db_compat import sqlite3

worksheet_bp = Blueprint("worksheet", __name__)

def db():
    conn = sqlite3.connect(current_app.config["DATABASE_URL"])
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


def require_module(module: str, need_edit: bool = False):
    def deco(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            has_access = current_app.config["HAS_MODULE_ACCESS_FUNC"]
            if not has_access(module, need_edit=need_edit):
                return jsonify({"ok": False, "error": f"No permission for {module}"}), 403
            return f(*args, **kwargs)
        return wrapped
    return deco


def ensure_worksheet_tables():
    conn = db()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS worksheet_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            work_date TEXT NOT NULL,
            is_workday INTEGER NOT NULL DEFAULT 1,
            summary TEXT,
            status TEXT NOT NULL DEFAULT 'DRAFT',
            admin_comment TEXT,
            reopen_reason TEXT,
            saved_at TEXT,
            saved_by TEXT,
            submitted_at TEXT,
            submitted_by TEXT,
            approved_at TEXT,
            approved_by TEXT,
            reopened_at TEXT,
            reopened_by TEXT,
            returned_at TEXT,
            returned_by TEXT,
            UNIQUE(user_id, work_date)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_worksheet_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_user_id INTEGER NOT NULL,
            work_date TEXT NOT NULL,
            is_workday INTEGER NOT NULL DEFAULT 1,
            summary TEXT,
            status TEXT NOT NULL DEFAULT 'APPROVED',
            saved_at TEXT,
            saved_by TEXT,
            approved_at TEXT,
            approved_by TEXT,
            UNIQUE(admin_user_id, work_date)
        )
    """)

    conn.commit()
    conn.close()


@worksheet_bp.before_app_request
def _ensure_once():
    if not current_app.config.get("WORKSHEET_TABLES_READY"):
        ensure_worksheet_tables()
        current_app.config["WORKSHEET_TABLES_READY"] = True


def get_me(conn):
    return conn.execute("""
        SELECT id, username, role, full_name
        FROM users
        WHERE username=?
        LIMIT 1
    """, (session.get("user"),)).fetchone()


def get_active_users(conn):
    rows = conn.execute("""
        SELECT id, username, role, full_name
        FROM users
        WHERE active=1
        ORDER BY COALESCE(full_name, username) ASC
    """).fetchall()
    return [dict(r) for r in rows]


def parse_month(month_str):
    try:
        dt = datetime.strptime(month_str, "%Y-%m")
        return dt.year, dt.month
    except Exception:
        now = datetime.now()
        return now.year, now.month


def month_dates(month_str):
    year, month = parse_month(month_str)
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)

    out = []
    cur = start
    while cur < end:
        out.append(cur.date().isoformat())
        cur += timedelta(days=1)
    return out


def weekday_index_from_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").weekday()


def get_schedule_map(conn, user_id):
    rows = conn.execute("""
        SELECT weekday, is_working, start_time, end_time
        FROM employee_schedules
        WHERE user_id=?
    """, (user_id,)).fetchall()

    out = {}
    for r in rows:
        out[int(r["weekday"])] = {
            "is_working": int(r["is_working"] or 0),
            "start_time": r["start_time"] or "",
            "end_time": r["end_time"] or ""
        }
    return out


def worksheet_row_to_dict(r):
    data = dict(r)
    return {
        "id": data.get("id"),
        "user_id": data.get("user_id"),
        "work_date": data.get("work_date", ""),
        "is_workday": int(data.get("is_workday") or 0),
        "summary": data.get("summary") or "",
        "status": data.get("status") or "DRAFT",
        "admin_comment": data.get("admin_comment") or "",
        "reopen_reason": data.get("reopen_reason") or "",
        "saved_at": data.get("saved_at") or "",
        "submitted_at": data.get("submitted_at") or "",
        "approved_at": data.get("approved_at") or ""
    }


def build_employee_month_rows(conn, user_id, month_str):
    existing = conn.execute("""
        SELECT *
        FROM worksheet_entries
        WHERE user_id=?
          AND substr(work_date, 1, 7)=?
        ORDER BY work_date ASC
    """, (user_id, month_str)).fetchall()

    existing_map = {r["work_date"]: worksheet_row_to_dict(r) for r in existing}
    schedule_map = get_schedule_map(conn, user_id)

    rows = []
    for work_date in month_dates(month_str):
        weekday = weekday_index_from_date(work_date)
        sch = schedule_map.get(weekday, {
            "is_working": 0,
            "start_time": "",
            "end_time": ""
        })

        row = existing_map.get(work_date)
        if row:
            row["scheduled_workday"] = int(sch["is_working"])
            row["schedule_start"] = sch["start_time"]
            row["schedule_end"] = sch["end_time"]
            rows.append(row)
        else:
            rows.append({
                "id": None,
                "user_id": int(user_id),
                "work_date": work_date,
                "is_workday": int(sch["is_working"]),
                "summary": "",
                "status": "DRAFT",
                "admin_comment": "",
                "reopen_reason": "",
                "saved_at": "",
                "submitted_at": "",
                "approved_at": "",
                "scheduled_workday": int(sch["is_working"]),
                "schedule_start": sch["start_time"],
                "schedule_end": sch["end_time"]
            })

    return rows


@worksheet_bp.route("/worksheet", methods=["GET"])
@login_required
@require_module("WORKSHEET")
def worksheet_page():
    return render_template(
        "worksheet.html",
        user=session.get("user"),
        role=session.get("role")
    )


@worksheet_bp.route("/api/worksheet/users", methods=["GET"])
@login_required
@require_module("WORKSHEET")
def api_worksheet_users():
    conn = db()
    me = get_me(conn)

    if not me or (me["role"] or "").upper() != "ADMIN":
        conn.close()
        return jsonify({"ok": True, "data": []})

    users = get_active_users(conn)
    conn.close()
    return jsonify({"ok": True, "data": users})


@worksheet_bp.route("/api/worksheet/my", methods=["GET"])
@login_required
@require_module("WORKSHEET")
def api_worksheet_my():
    conn = db()
    me = get_me(conn)
    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    rows = build_employee_month_rows(conn, int(me["id"]), month)
    conn.close()
    return jsonify({"ok": True, "data": rows})

@worksheet_bp.route("/api/worksheet/admin/month", methods=["GET"])
@login_required
@require_module("WORKSHEET")
def api_worksheet_admin_month():
    conn = db()
    me = get_me(conn)

    if not me or (me["role"] or "").upper() != "ADMIN":
        conn.close()
        return jsonify({"ok": False, "error": "Admin only"}), 403

    user_id = request.args.get("user_id")
    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    if not user_id:
        conn.close()
        return jsonify({"ok": True, "data": []})

    try:
        user_id = int(user_id)
    except Exception:
        conn.close()
        return jsonify({"ok": False, "error": "Invalid user_id"}), 400

    target = conn.execute("""
        SELECT id, role, full_name, username
        FROM users
        WHERE id=? AND active=1
        LIMIT 1
    """, (user_id,)).fetchone()

    if not target:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    rows = build_employee_month_rows(conn, user_id, month)
    conn.close()
    return jsonify({"ok": True, "data": rows})


@worksheet_bp.route("/api/worksheet/my/save", methods=["POST"])
@login_required
@require_module("WORKSHEET", need_edit=True)
def api_worksheet_my_save():
    conn = db()
    me = get_me(conn)
    data = request.json or {}

    work_date = (data.get("work_date") or "").strip()
    if not work_date:
        conn.close()
        return jsonify({"ok": False, "error": "work_date is required"}), 400

    existing = conn.execute("""
        SELECT *
        FROM worksheet_entries
        WHERE user_id=? AND work_date=?
        LIMIT 1
    """, (me["id"], work_date)).fetchone()

    if existing:
        current_status = (existing["status"] or "DRAFT").upper()
        if current_status in ("SUBMITTED", "APPROVED", "REOPEN_REQUESTED"):
            conn.close()
            return jsonify({"ok": False, "error": "This row is locked. Ask admin to reopen it."}), 400

        conn.execute("""
            UPDATE worksheet_entries
            SET is_workday=?,
                summary=?,
                status='DRAFT',
                saved_at=?,
                saved_by=?
            WHERE id=?
        """, (
            1 if data.get("is_workday", True) else 0,
            (data.get("summary") or "").strip(),
            now_iso(),
            session.get("user"),
            existing["id"]
        ))
    else:
        conn.execute("""
            INSERT INTO worksheet_entries (
                user_id, work_date, is_workday, summary, status,
                admin_comment, reopen_reason, saved_at, saved_by
            )
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            int(me["id"]),
            work_date,
            1 if data.get("is_workday", True) else 0,
            (data.get("summary") or "").strip(),
            "DRAFT",
            "",
            "",
            now_iso(),
            session.get("user")
        ))

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Draft saved"})


@worksheet_bp.route("/api/worksheet/my/submit", methods=["POST"])
@login_required
@require_module("WORKSHEET", need_edit=True)
def api_worksheet_my_submit():
    conn = db()
    me = get_me(conn)
    data = request.json or {}

    work_date = (data.get("work_date") or "").strip()
    if not work_date:
        conn.close()
        return jsonify({"ok": False, "error": "work_date is required"}), 400

    existing = conn.execute("""
        SELECT *
        FROM worksheet_entries
        WHERE user_id=? AND work_date=?
        LIMIT 1
    """, (me["id"], work_date)).fetchone()

    if existing:
        current_status = (existing["status"] or "DRAFT").upper()
        if current_status in ("SUBMITTED", "APPROVED", "REOPEN_REQUESTED"):
            conn.close()
            return jsonify({"ok": False, "error": "This row is already locked."}), 400

        conn.execute("""
            UPDATE worksheet_entries
            SET is_workday=?,
                summary=?,
                status='SUBMITTED',
                saved_at=?,
                saved_by=?,
                submitted_at=?,
                submitted_by=?
            WHERE id=?
        """, (
            1 if data.get("is_workday", True) else 0,
            (data.get("summary") or "").strip(),
            now_iso(),
            session.get("user"),
            now_iso(),
            session.get("user"),
            existing["id"]
        ))
    else:
        conn.execute("""
            INSERT INTO worksheet_entries (
                user_id, work_date, is_workday, summary, status,
                admin_comment, reopen_reason, saved_at, saved_by,
                submitted_at, submitted_by
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            int(me["id"]),
            work_date,
            1 if data.get("is_workday", True) else 0,
            (data.get("summary") or "").strip(),
            "SUBMITTED",
            "",
            "",
            now_iso(),
            session.get("user"),
            now_iso(),
            session.get("user")
        ))

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Submitted to admin"})


@worksheet_bp.route("/api/worksheet/my/request-reopen", methods=["POST"])
@login_required
@require_module("WORKSHEET", need_edit=True)
def api_worksheet_my_request_reopen():
    conn = db()
    me = get_me(conn)
    data = request.json or {}

    work_date = (data.get("work_date") or "").strip()
    reason = (data.get("reason") or "").strip()

    if not work_date:
        conn.close()
        return jsonify({"ok": False, "error": "work_date is required"}), 400

    row = conn.execute("""
        SELECT *
        FROM worksheet_entries
        WHERE user_id=? AND work_date=?
        LIMIT 1
    """, (me["id"], work_date)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Worksheet row not found"}), 404

    conn.execute("""
        UPDATE worksheet_entries
        SET status='REOPEN_REQUESTED',
            reopen_reason=?,
            returned_at=?,
            returned_by=?
        WHERE id=?
    """, (
        reason,
        now_iso(),
        session.get("user"),
        int(row["id"])
    ))

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Reopen request sent"})


@worksheet_bp.route("/api/worksheet/admin/action", methods=["POST"])
@login_required
@require_module("WORKSHEET", need_edit=True)
def api_worksheet_admin_action():
    conn = db()
    me = get_me(conn)

    if not me or (me["role"] or "").upper() != "ADMIN":
        conn.close()
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    entry_id = data.get("entry_id")
    action = (data.get("action") or "").strip().upper()
    admin_comment = (data.get("admin_comment") or "").strip()

    if not entry_id or not action:
        conn.close()
        return jsonify({"ok": False, "error": "entry_id and action are required"}), 400

    row = conn.execute("""
        SELECT *
        FROM worksheet_entries
        WHERE id=?
        LIMIT 1
    """, (int(entry_id),)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Entry not found"}), 404

    if action == "APPROVE":
        conn.execute("""
            UPDATE worksheet_entries
            SET status='APPROVED',
                admin_comment=?,
                approved_at=?,
                approved_by=?
            WHERE id=?
        """, (
            admin_comment,
            now_iso(),
            session.get("user"),
            int(entry_id)
        ))

    elif action == "RETURN":
        conn.execute("""
            UPDATE worksheet_entries
            SET status='RETURNED',
                admin_comment=?,
                returned_at=?,
                returned_by=?
            WHERE id=?
        """, (
            admin_comment,
            now_iso(),
            session.get("user"),
            int(entry_id)
        ))

    elif action == "REOPEN":
        conn.execute("""
            UPDATE worksheet_entries
            SET status='DRAFT',
                admin_comment=?,
                reopened_at=?,
                reopened_by=?,
                reopen_reason=''
            WHERE id=?
        """, (
            admin_comment,
            now_iso(),
            session.get("user"),
            int(entry_id)
        ))

    elif action == "DELETE":
        conn.execute("""
            DELETE FROM worksheet_entries
            WHERE id=?
        """, (int(entry_id),))

    else:
        conn.close()
        return jsonify({"ok": False, "error": "Invalid action"}), 400

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Worksheet updated"})