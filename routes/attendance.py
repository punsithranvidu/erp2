from flask import Blueprint, render_template, request, jsonify, session, current_app
from functools import wraps
from datetime import datetime, timedelta
from .db import connect, get_table_columns as db_get_table_columns, table_exists

attendance_bp = Blueprint("attendance", __name__)


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


def get_table_columns(conn, table_name):
    if not table_exists(conn, table_name):
        return set()
    return db_get_table_columns(conn, table_name)


def has_unique_user_date(conn):
    cols = get_table_columns(conn, "attendance_entries")
    required = {"user_id", "attendance_date"}
    return required.issubset(cols)


def rebuild_attendance_table(conn):
    old_cols = get_table_columns(conn, "attendance_entries")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS attendance_entries_new (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            attendance_date TEXT NOT NULL,
            marked_status TEXT NOT NULL DEFAULT 'UNMARKED',
            employee_note TEXT,
            admin_confirmed INTEGER NOT NULL DEFAULT 0,
            employee_saved_at TEXT,
            employee_saved_by TEXT,
            admin_action_at TEXT,
            admin_action_by TEXT,
            UNIQUE(user_id, attendance_date)
        )
    """)

    if old_cols:
        if "attendance_date" in old_cols:
            date_expr = "attendance_date"
        elif "work_date" in old_cols:
            date_expr = "work_date"
        elif "date" in old_cols:
            date_expr = "date"
        else:
            date_expr = "NULL"

        if "marked_status" in old_cols:
            status_expr = "marked_status"
        elif "status" in old_cols:
            status_expr = "status"
        else:
            status_expr = "'UNMARKED'"

        note_expr = "employee_note" if "employee_note" in old_cols else ("note" if "note" in old_cols else "NULL")
        confirmed_expr = "admin_confirmed" if "admin_confirmed" in old_cols else "0"
        saved_at_expr = "employee_saved_at" if "employee_saved_at" in old_cols else ("saved_at" if "saved_at" in old_cols else "NULL")
        saved_by_expr = "employee_saved_by" if "employee_saved_by" in old_cols else ("saved_by" if "saved_by" in old_cols else "NULL")
        admin_at_expr = "admin_action_at" if "admin_action_at" in old_cols else "NULL"
        admin_by_expr = "admin_action_by" if "admin_action_by" in old_cols else "NULL"

        if "user_id" in old_cols and date_expr != "NULL":
            conn.execute(f"""
                INSERT INTO attendance_entries_new (
                    id,
                    user_id,
                    attendance_date,
                    marked_status,
                    employee_note,
                    admin_confirmed,
                    employee_saved_at,
                    employee_saved_by,
                    admin_action_at,
                    admin_action_by
                )
                SELECT
                    id,
                    user_id,
                    {date_expr},
                    COALESCE({status_expr}, 'UNMARKED'),
                    {note_expr},
                    COALESCE({confirmed_expr}, 0),
                    {saved_at_expr},
                    {saved_by_expr},
                    {admin_at_expr},
                    {admin_by_expr}
                FROM attendance_entries
                WHERE {date_expr} IS NOT NULL
            """)

    conn.execute("DROP TABLE IF EXISTS attendance_entries")
    conn.execute("ALTER TABLE attendance_entries_new RENAME TO attendance_entries")


def ensure_attendance_tables():
    conn = db()

    if not table_exists(conn, "attendance_entries"):
        conn.execute("""
            CREATE TABLE attendance_entries (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                attendance_date TEXT NOT NULL,
                marked_status TEXT NOT NULL DEFAULT 'UNMARKED',
                employee_note TEXT,
                admin_confirmed INTEGER NOT NULL DEFAULT 0,
                employee_saved_at TEXT,
                employee_saved_by TEXT,
                admin_action_at TEXT,
                admin_action_by TEXT,
                UNIQUE(user_id, attendance_date)
            )
        """)
        conn.commit()
        conn.close()
        return

    cols = get_table_columns(conn, "attendance_entries")

    required = {
        "id",
        "user_id",
        "attendance_date",
        "marked_status",
        "employee_note",
        "admin_confirmed",
        "employee_saved_at",
        "employee_saved_by",
        "admin_action_at",
        "admin_action_by",
    }

    if not required.issubset(cols) or not has_unique_user_date(conn):
        rebuild_attendance_table(conn)
        conn.commit()
        conn.close()
        return

    conn.commit()
    conn.close()


@attendance_bp.before_app_request
def _ensure_once():
    if not current_app.config.get("ATTENDANCE_TABLES_READY"):
        ensure_attendance_tables()
        current_app.config["ATTENDANCE_TABLES_READY"] = True


def get_me(conn):
    return conn.execute("""
        SELECT id, username, role, full_name
        FROM users
        WHERE username=%s
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
    if not table_exists(conn, "employee_schedules"):
        return {}

    rows = conn.execute("""
        SELECT weekday, is_working, start_time, end_time
        FROM employee_schedules
        WHERE user_id=%s
    """, (user_id,)).fetchall()

    out = {}
    for r in rows:
        out[int(r["weekday"])] = {
            "is_working": int(r["is_working"] or 0),
            "start_time": r["start_time"] or "",
            "end_time": r["end_time"] or ""
        }
    return out


def attendance_row_to_dict(r):
    return {
        "id": r["id"],
        "user_id": r["user_id"],
        "attendance_date": r["attendance_date"],
        "marked_status": (r["marked_status"] or "UNMARKED").upper(),
        "employee_note": r["employee_note"] or "",
        "admin_confirmed": int(r["admin_confirmed"] or 0),
        "employee_saved_at": r["employee_saved_at"] or "",
        "employee_saved_by": r["employee_saved_by"] or "",
        "admin_action_at": r["admin_action_at"] or "",
        "admin_action_by": r["admin_action_by"] or "",
    }


def build_month_rows(conn, user_id, month_str):
    existing = conn.execute("""
        SELECT *
        FROM attendance_entries
        WHERE user_id=%s
          AND substr(attendance_date, 1, 7)=%s
        ORDER BY attendance_date ASC
    """, (user_id, month_str)).fetchall()

    existing_map = {r["attendance_date"]: attendance_row_to_dict(r) for r in existing}
    schedule_map = get_schedule_map(conn, user_id)

    rows = []
    for attendance_date in month_dates(month_str):
        weekday = weekday_index_from_date(attendance_date)
        sch = schedule_map.get(weekday, {
            "is_working": 0,
            "start_time": "",
            "end_time": ""
        })

        row = existing_map.get(attendance_date)
        if row:
            row["scheduled_workday"] = int(sch["is_working"])
            row["schedule_start"] = sch["start_time"]
            row["schedule_end"] = sch["end_time"]
        else:
            row = {
                "id": None,
                "user_id": int(user_id),
                "attendance_date": attendance_date,
                "marked_status": "UNMARKED",
                "employee_note": "",
                "admin_confirmed": 0,
                "employee_saved_at": "",
                "employee_saved_by": "",
                "admin_action_at": "",
                "admin_action_by": "",
                "scheduled_workday": int(sch["is_working"]),
                "schedule_start": sch["start_time"],
                "schedule_end": sch["end_time"]
            }
        rows.append(row)

    return rows


@attendance_bp.route("/attendance", methods=["GET"])
@login_required
@require_module("ATTENDANCE")
def attendance_page():
    return render_template(
        "attendance.html",
        user=session.get("user"),
        role=session.get("role")
    )


@attendance_bp.route("/api/attendance/users", methods=["GET"])
@login_required
@require_module("ATTENDANCE")
def api_attendance_users():
    conn = db()
    me = get_me(conn)

    if not me or (me["role"] or "").upper() != "ADMIN":
        conn.close()
        return jsonify({"ok": True, "data": []})

    users = get_active_users(conn)
    conn.close()
    return jsonify({"ok": True, "data": users})


@attendance_bp.route("/api/attendance/my", methods=["GET"])
@login_required
@require_module("ATTENDANCE")
def api_attendance_my():
    conn = db()
    me = get_me(conn)
    month = request.args.get("month") or datetime.now().strftime("%Y-%m")

    rows = build_month_rows(conn, int(me["id"]), month)
    conn.close()
    return jsonify({"ok": True, "data": rows})


@attendance_bp.route("/api/attendance/admin/month", methods=["GET"])
@login_required
@require_module("ATTENDANCE")
def api_attendance_admin_month():
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
        SELECT id
        FROM users
        WHERE id=%s AND active=1
        LIMIT 1
    """, (user_id,)).fetchone()

    if not target:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    rows = build_month_rows(conn, user_id, month)
    conn.close()
    return jsonify({"ok": True, "data": rows})


@attendance_bp.route("/api/attendance/my/save", methods=["POST"])
@login_required
@require_module("ATTENDANCE", need_edit=True)
def api_attendance_my_save():
    conn = db()
    me = get_me(conn)
    data = request.json or {}

    attendance_date = (data.get("attendance_date") or "").strip()
    marked_status = (data.get("marked_status") or "UNMARKED").strip().upper()
    employee_note = (data.get("employee_note") or "").strip()

    if not attendance_date:
        conn.close()
        return jsonify({"ok": False, "error": "attendance_date is required"}), 400

    if marked_status not in ("PRESENT", "ABSENT", "UNMARKED"):
        conn.close()
        return jsonify({"ok": False, "error": "Invalid status"}), 400

    existing = conn.execute("""
        SELECT *
        FROM attendance_entries
        WHERE user_id=%s AND attendance_date=%s
        LIMIT 1
    """, (me["id"], attendance_date)).fetchone()

    if existing and int(existing["admin_confirmed"] or 0) == 1:
        conn.close()
        return jsonify({"ok": False, "error": "This row is already confirmed by admin and is locked"}), 400

    if existing:
        conn.execute("""
            UPDATE attendance_entries
            SET marked_status=%s,
                employee_note=%s,
                employee_saved_at=%s,
                employee_saved_by=%s
            WHERE id=%s
        """, (
            marked_status,
            employee_note,
            now_iso(),
            session.get("user"),
            int(existing["id"])
        ))
    else:
        conn.execute("""
            INSERT INTO attendance_entries (
                user_id, attendance_date, marked_status, employee_note,
                admin_confirmed, employee_saved_at, employee_saved_by
            )
            VALUES (%s, %s, %s, %s, 0, %s, %s)
        """, (
            int(me["id"]),
            attendance_date,
            marked_status,
            employee_note,
            now_iso(),
            session.get("user")
        ))

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Attendance saved"})


@attendance_bp.route("/api/attendance/admin/action", methods=["POST"])
@login_required
@require_module("ATTENDANCE", need_edit=True)
def api_attendance_admin_action():
    conn = db()
    me = get_me(conn)

    if not me or (me["role"] or "").upper() != "ADMIN":
        conn.close()
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    user_id = data.get("user_id")
    attendance_date = (data.get("attendance_date") or "").strip()
    action = (data.get("action") or "").strip().upper()
    employee_note = (data.get("employee_note") or "").strip()

    if not user_id or not attendance_date or not action:
        conn.close()
        return jsonify({"ok": False, "error": "user_id, attendance_date and action are required"}), 400

    try:
        user_id = int(user_id)
    except Exception:
        conn.close()
        return jsonify({"ok": False, "error": "Invalid user_id"}), 400

    target = conn.execute("""
        SELECT id
        FROM users
        WHERE id=%s AND active=1
        LIMIT 1
    """, (user_id,)).fetchone()

    if not target:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    existing = conn.execute("""
        SELECT *
        FROM attendance_entries
        WHERE user_id=%s AND attendance_date=%s
        LIMIT 1
    """, (user_id, attendance_date)).fetchone()

    if action == "DELETE":
        if existing:
            conn.execute("DELETE FROM attendance_entries WHERE id=%s", (int(existing["id"]),))
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "message": "Attendance deleted"})

    if action not in ("CONFIRM_PRESENT", "CONFIRM_ABSENT", "UNCONFIRM"):
        conn.close()
        return jsonify({"ok": False, "error": "Invalid action"}), 400

    if action == "CONFIRM_PRESENT":
        new_status = "PRESENT"
        confirmed = 1
    elif action == "CONFIRM_ABSENT":
        new_status = "ABSENT"
        confirmed = 1
    else:
        new_status = existing["marked_status"] if existing else "UNMARKED"
        confirmed = 0

    if existing:
        conn.execute("""
            UPDATE attendance_entries
            SET marked_status=%s,
                employee_note=%s,
                admin_confirmed=%s,
                admin_action_at=%s,
                admin_action_by=%s
            WHERE id=%s
        """, (
            new_status,
            employee_note if employee_note else (existing["employee_note"] or ""),
            confirmed,
            now_iso(),
            session.get("user"),
            int(existing["id"])
        ))
    else:
        conn.execute("""
            INSERT INTO attendance_entries (
                user_id, attendance_date, marked_status, employee_note,
                admin_confirmed, employee_saved_at, employee_saved_by,
                admin_action_at, admin_action_by
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            user_id,
            attendance_date,
            new_status,
            employee_note,
            confirmed,
            now_iso(),
            session.get("user"),
            now_iso(),
            session.get("user")
        ))

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Attendance updated"})
