from flask import Blueprint, request, jsonify, session, current_app
from functools import wraps
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

calendar_bp = Blueprint("calendar", __name__)
APP_TZ = ZoneInfo("Asia/Colombo")


def db():
    return psycopg.connect(
        current_app.config["DATABASE_URL"],
        row_factory=dict_row
    )


def now_iso():
    return datetime.now(APP_TZ).replace(tzinfo=None).isoformat(timespec="seconds")


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


def fetchone(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchone()


def fetchall(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchall()


def execute(conn, query, params=None, returning=False):
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        if returning:
            return cur.fetchone()
        return None


def column_exists(conn, table_name, column_name):
    row = fetchone(conn, """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
    """, (table_name, column_name))
    return bool(row)


def ensure_calendar_tables():
    conn = db()
    try:
        execute(conn, """
            CREATE TABLE IF NOT EXISTS employee_schedules (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                weekday INTEGER NOT NULL,
                is_working INTEGER NOT NULL DEFAULT 0,
                start_time TEXT,
                end_time TEXT,
                allow_employee_edit INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT,
                updated_by TEXT,
                UNIQUE(user_id, weekday)
            )
        """)

        execute(conn, """
            CREATE TABLE IF NOT EXISTS employee_unavailability (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                all_day INTEGER NOT NULL DEFAULT 1,
                start_time TEXT,
                end_time TEXT,
                reason TEXT,
                status TEXT NOT NULL DEFAULT 'PENDING',
                requested_at TEXT NOT NULL,
                requested_by TEXT NOT NULL,
                admin_note TEXT,
                decided_at TEXT,
                decided_by TEXT,
                cancel_requested_at TEXT,
                cancelled_at TEXT,
                cancelled_by TEXT,
                show_in_active INTEGER NOT NULL DEFAULT 0
            )
        """)

        execute(conn, """
            CREATE TABLE IF NOT EXISTS calendar_events (
                id BIGSERIAL PRIMARY KEY,
                calendar_scope TEXT NOT NULL DEFAULT 'SHARED',
                title TEXT NOT NULL,
                event_date TEXT NOT NULL,
                start_time TEXT,
                end_time TEXT,
                all_day INTEGER NOT NULL DEFAULT 1,
                notes TEXT,
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                audience_type TEXT NOT NULL DEFAULT 'ONLY_ME',
                audience_user_ids TEXT,
                reminder_times TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                show_in_active INTEGER NOT NULL DEFAULT 0
            )
        """)

        execute(conn, """
            CREATE TABLE IF NOT EXISTS calendar_event_acks (
                id BIGSERIAL PRIMARY KEY,
                event_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                remind_at TEXT NOT NULL,
                acked_at TEXT NOT NULL,
                UNIQUE(event_id, user_id, remind_at)
            )
        """)

        execute(conn, """
            CREATE TABLE IF NOT EXISTS calendar_holidays (
                id BIGSERIAL PRIMARY KEY,
                holiday_date TEXT NOT NULL,
                title TEXT NOT NULL,
                holiday_type TEXT NOT NULL DEFAULT 'GOVERNMENT_HOLIDAY',
                notes TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                edited_at TEXT,
                edited_by TEXT
            )
        """)

        execute(conn, """
            CREATE TABLE IF NOT EXISTS calendar_audit_logs (
                id BIGSERIAL PRIMARY KEY,
                action_type TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id BIGINT,
                changed_by TEXT NOT NULL,
                changed_at TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                note TEXT
            )
        """)

        execute(conn, """
            CREATE INDEX IF NOT EXISTS idx_employee_schedules_user_weekday
            ON employee_schedules (user_id, weekday)
        """)

        execute(conn, """
            CREATE INDEX IF NOT EXISTS idx_employee_unavailability_dates
            ON employee_unavailability (start_date, end_date)
        """)

        execute(conn, """
            CREATE INDEX IF NOT EXISTS idx_employee_unavailability_user
            ON employee_unavailability (user_id)
        """)

        execute(conn, """
            CREATE INDEX IF NOT EXISTS idx_calendar_events_date_active
            ON calendar_events (event_date, is_active)
        """)

        execute(conn, """
            CREATE INDEX IF NOT EXISTS idx_calendar_holidays_date_active
            ON calendar_holidays (holiday_date, is_active)
        """)

        execute(conn, """
            CREATE INDEX IF NOT EXISTS idx_calendar_audit_logs_changed_at
            ON calendar_audit_logs (changed_at DESC, id DESC)
        """)

        if not column_exists(conn, "calendar_events", "audience_type"):
            execute(conn, "ALTER TABLE calendar_events ADD COLUMN audience_type TEXT NOT NULL DEFAULT 'ONLY_ME'")
        if not column_exists(conn, "calendar_events", "audience_user_ids"):
            execute(conn, "ALTER TABLE calendar_events ADD COLUMN audience_user_ids TEXT")
        if not column_exists(conn, "calendar_events", "reminder_times"):
            execute(conn, "ALTER TABLE calendar_events ADD COLUMN reminder_times TEXT")
        if not column_exists(conn, "calendar_events", "is_active"):
            execute(conn, "ALTER TABLE calendar_events ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
        if not column_exists(conn, "calendar_events", "show_in_active"):
            execute(conn, "ALTER TABLE calendar_events ADD COLUMN show_in_active INTEGER NOT NULL DEFAULT 0")

        if not column_exists(conn, "employee_unavailability", "show_in_active"):
            execute(conn, "ALTER TABLE employee_unavailability ADD COLUMN show_in_active INTEGER NOT NULL DEFAULT 0")

        if not column_exists(conn, "calendar_holidays", "holiday_type"):
            execute(conn, "ALTER TABLE calendar_holidays ADD COLUMN holiday_type TEXT NOT NULL DEFAULT 'GOVERNMENT_HOLIDAY'")
        if not column_exists(conn, "calendar_holidays", "notes"):
            execute(conn, "ALTER TABLE calendar_holidays ADD COLUMN notes TEXT")
        if not column_exists(conn, "calendar_holidays", "is_active"):
            execute(conn, "ALTER TABLE calendar_holidays ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
        if not column_exists(conn, "calendar_holidays", "created_at"):
            execute(conn, "ALTER TABLE calendar_holidays ADD COLUMN created_at TEXT")
        if not column_exists(conn, "calendar_holidays", "created_by"):
            execute(conn, "ALTER TABLE calendar_holidays ADD COLUMN created_by TEXT")
        if not column_exists(conn, "calendar_holidays", "edited_at"):
            execute(conn, "ALTER TABLE calendar_holidays ADD COLUMN edited_at TEXT")
        if not column_exists(conn, "calendar_holidays", "edited_by"):
            execute(conn, "ALTER TABLE calendar_holidays ADD COLUMN edited_by TEXT")

        if not column_exists(conn, "calendar_audit_logs", "note"):
            execute(conn, "ALTER TABLE calendar_audit_logs ADD COLUMN note TEXT")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_me(conn):
    return fetchone(
        conn,
        "SELECT id, username, role, full_name FROM users WHERE username=%s LIMIT 1",
        (session.get("user"),)
    )


def get_all_active_users(conn):
    rows = fetchall(conn, """
        SELECT id, username, role, full_name
        FROM users
        WHERE COALESCE(active, 1) = 1
        ORDER BY COALESCE(full_name, username) ASC
    """)
    return [dict(r) for r in rows]


def parse_month(month_str):
    try:
        dt = datetime.strptime(month_str, "%Y-%m")
        return dt.year, dt.month
    except Exception:
        now = datetime.now()
        return now.year, now.month


def month_range(year, month):
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)
    return start.date().isoformat(), (end - timedelta(days=1)).date().isoformat()


def can_edit_schedule(conn, target_user_id, me):
    if me["role"] == "ADMIN":
        return True
    if int(me["id"]) != int(target_user_id):
        return False

    rows = fetchall(
        conn,
        "SELECT allow_employee_edit FROM employee_schedules WHERE user_id=%s",
        (target_user_id,)
    )
    if not rows:
        return True
    return all(int(r["allow_employee_edit"] or 0) == 1 for r in rows)


def event_visible_to_user(event_row, me_id, me_role):
    audience_type = (event_row["audience_type"] or "ONLY_ME").upper()
    raw_users = (event_row["audience_user_ids"] or "").strip()
    target_ids = {int(x) for x in raw_users.split(",") if x.strip().isdigit()}

    if audience_type == "EVERYONE":
        return True
    if audience_type == "ONLY_ME":
        return event_row["created_by"] == session.get("user")
    if audience_type == "ME_AND_ADMINS":
        return event_row["created_by"] == session.get("user") or me_role == "ADMIN"
    if audience_type == "SELECTED_USERS":
        return me_id in target_ids or event_row["created_by"] == session.get("user")
    return event_row["created_by"] == session.get("user")


def can_edit_event(event_row, me):
    return me["role"] == "ADMIN" or event_row["created_by"] == session.get("user")


def can_edit_unavailability(row, me):
    return me["role"] == "ADMIN" or int(row["user_id"]) == int(me["id"])


def serialize_schedule_row(r):
    return {
        "weekday": int(r["weekday"]),
        "is_working": int(r["is_working"] or 0),
        "start_time": r["start_time"] or "",
        "end_time": r["end_time"] or "",
        "allow_employee_edit": int(r["allow_employee_edit"] or 0),
    }


def row_to_dict(row):
    if not row:
        return None
    return dict(row)


def write_audit_log(conn, action_type, entity_type, entity_id=None, old_value=None, new_value=None, note=None):
    execute(conn, """
        INSERT INTO calendar_audit_logs (
            action_type, entity_type, entity_id, changed_by, changed_at,
            old_value, new_value, note
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        action_type,
        entity_type,
        entity_id,
        session.get("user"),
        now_iso(),
        json.dumps(old_value, ensure_ascii=False) if old_value is not None else None,
        json.dumps(new_value, ensure_ascii=False) if new_value is not None else None,
        note
    ))


def parse_reminder_entry(value: str):
    raw = (value or "").strip()
    if not raw:
        return None

    normalized = raw.replace("T", " ")
    formats = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(normalized, fmt)
        except Exception:
            pass
    return None


@calendar_bp.before_app_request
def _ensure_tables_once():
    if not current_app.config.get("CALENDAR_TABLES_READY"):
        ensure_calendar_tables()
        current_app.config["CALENDAR_TABLES_READY"] = True


@calendar_bp.route("/api/calendar/users", methods=["GET"])
@login_required
@require_module("CALENDAR")
def api_calendar_users():
    conn = db()
    try:
        data = get_all_active_users(conn)
        return jsonify({"ok": True, "data": data})
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/schedules", methods=["GET"])
@login_required
@require_module("CALENDAR")
def api_calendar_schedules_get():
    conn = db()
    try:
        me = get_me(conn)

        user_id = request.args.get("user_id")
        if not user_id:
            user_id = me["id"]

        try:
            user_id = int(user_id)
        except Exception:
            return jsonify({"ok": False, "error": "Invalid user_id"}), 400

        if me["role"] != "ADMIN" and user_id != int(me["id"]):
            return jsonify({"ok": False, "error": "Not allowed"}), 403

        rows = fetchall(conn, """
            SELECT *
            FROM employee_schedules
            WHERE user_id=%s
            ORDER BY weekday ASC
        """, (user_id,))

        return jsonify({"ok": True, "data": [serialize_schedule_row(r) for r in rows]})
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/schedules", methods=["POST"])
@login_required
@require_module("CALENDAR", need_edit=True)
def api_calendar_schedules_save():
    conn = db()
    try:
        me = get_me(conn)
        data = request.json or {}
        target_user_id = int(data.get("user_id") or me["id"])

        if not can_edit_schedule(conn, target_user_id, me):
            return jsonify({"ok": False, "error": "You cannot edit this schedule"}), 403

        old_rows = fetchall(conn, """
            SELECT *
            FROM employee_schedules
            WHERE user_id=%s
            ORDER BY weekday ASC
        """, (target_user_id,))

        rows = data.get("rows") or []
        for row in rows:
            weekday = int(row.get("weekday"))
            is_working = 1 if row.get("is_working") else 0
            start_time = (row.get("start_time") or "").strip() or None
            end_time = (row.get("end_time") or "").strip() or None
            allow_employee_edit = int(row.get("allow_employee_edit", 1 if me["role"] != "ADMIN" else 0))

            execute(conn, """
                INSERT INTO employee_schedules (
                    user_id, weekday, is_working, start_time, end_time,
                    allow_employee_edit, updated_at, updated_by
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (user_id, weekday)
                DO UPDATE SET
                    is_working = EXCLUDED.is_working,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    allow_employee_edit = EXCLUDED.allow_employee_edit,
                    updated_at = EXCLUDED.updated_at,
                    updated_by = EXCLUDED.updated_by
            """, (
                target_user_id, weekday, is_working, start_time, end_time,
                allow_employee_edit, now_iso(), session.get("user")
            ))

        new_rows = fetchall(conn, """
            SELECT *
            FROM employee_schedules
            WHERE user_id=%s
            ORDER BY weekday ASC
        """, (target_user_id,))

        write_audit_log(
            conn,
            action_type="UPDATE",
            entity_type="employee_schedule",
            entity_id=target_user_id,
            old_value=[row_to_dict(r) for r in old_rows],
            new_value=[row_to_dict(r) for r in new_rows],
            note=f"Updated weekly schedule for user_id={target_user_id}"
        )

        conn.commit()
        return jsonify({"ok": True, "message": "Schedule saved"})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/unavailability", methods=["POST"])
@login_required
@require_module("CALENDAR", need_edit=True)
def api_calendar_unavailability_create():
    conn = db()
    try:
        me = get_me(conn)
        data = request.json or {}
        target_user_id = int(data.get("user_id") or me["id"])

        if me["role"] != "ADMIN" and int(me["id"]) != target_user_id:
            return jsonify({"ok": False, "error": "Not allowed"}), 403

        start_date = (data.get("start_date") or "").strip()
        end_date = (data.get("end_date") or "").strip()
        if not start_date or not end_date:
            return jsonify({"ok": False, "error": "Start date and end date are required"}), 400

        status = "APPROVED" if me["role"] == "ADMIN" and data.get("force_approve") else "PENDING"
        show_in_active = 1 if data.get("show_in_active") else 0

        inserted = execute(conn, """
            INSERT INTO employee_unavailability (
                user_id, start_date, end_date, all_day, start_time, end_time,
                reason, status, requested_at, requested_by, show_in_active
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (
            target_user_id,
            start_date,
            end_date,
            1 if data.get("all_day", True) else 0,
            (data.get("start_time") or "").strip() or None,
            (data.get("end_time") or "").strip() or None,
            (data.get("reason") or "").strip(),
            status,
            now_iso(),
            session.get("user"),
            show_in_active
        ), returning=True)

        new_id = inserted["id"]
        new_row = fetchone(conn, """
            SELECT *
            FROM employee_unavailability
            WHERE id=%s
        """, (new_id,))

        write_audit_log(
            conn,
            action_type="CREATE",
            entity_type="unavailability",
            entity_id=new_id,
            old_value=None,
            new_value=row_to_dict(new_row),
            note="Created break/unavailability request"
        )

        conn.commit()

        return jsonify({
            "ok": True,
            "message": "Unavailability saved and sent to admin/HR" if status == "PENDING" else "Unavailability approved"
        })
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/unavailability/<int:item_id>", methods=["PUT"])
@login_required
@require_module("CALENDAR", need_edit=True)
def api_calendar_unavailability_update(item_id):
    conn = db()
    try:
        me = get_me(conn)
        data = request.json or {}

        row = fetchone(conn, """
            SELECT *
            FROM employee_unavailability
            WHERE id=%s
            LIMIT 1
        """, (item_id,))

        if not row:
            return jsonify({"ok": False, "error": "Request not found"}), 404

        if me["role"] != "ADMIN":
            return jsonify({"ok": False, "error": "Admin only"}), 403

        start_date = (data.get("start_date") or "").strip()
        end_date = (data.get("end_date") or "").strip()
        if not start_date or not end_date:
            return jsonify({"ok": False, "error": "Start date and end date are required"}), 400

        old_row = row_to_dict(row)

        execute(conn, """
            UPDATE employee_unavailability
            SET start_date=%s,
                end_date=%s,
                all_day=%s,
                start_time=%s,
                end_time=%s,
                reason=%s,
                admin_note=%s,
                show_in_active=%s,
                decided_at=%s,
                decided_by=%s
            WHERE id=%s
        """, (
            start_date,
            end_date,
            1 if data.get("all_day", True) else 0,
            (data.get("start_time") or "").strip() or None,
            (data.get("end_time") or "").strip() or None,
            (data.get("reason") or "").strip(),
            (data.get("admin_note") or "").strip() or None,
            1 if data.get("show_in_active") else 0,
            now_iso(),
            session.get("user"),
            item_id
        ))

        new_row = fetchone(conn, """
            SELECT *
            FROM employee_unavailability
            WHERE id=%s
        """, (item_id,))

        write_audit_log(
            conn,
            action_type="UPDATE",
            entity_type="unavailability",
            entity_id=item_id,
            old_value=old_row,
            new_value=row_to_dict(new_row),
            note="Admin edited unavailability request"
        )

        conn.commit()
        return jsonify({"ok": True, "message": "Request updated"})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/unavailability/<int:item_id>/status", methods=["POST"])
@login_required
@require_module("CALENDAR", need_edit=True)
def api_calendar_unavailability_status(item_id):
    conn = db()
    try:
        me = get_me(conn)
        data = request.json or {}
        row = fetchone(conn, """
            SELECT *
            FROM employee_unavailability
            WHERE id=%s
            LIMIT 1
        """, (item_id,))

        if not row:
            return jsonify({"ok": False, "error": "Request not found"}), 404

        old_row = row_to_dict(row)
        action = (data.get("action") or "").upper()

        if action in ("APPROVE", "DECLINE", "CONFIRM_CANCEL") and me["role"] != "ADMIN":
            return jsonify({"ok": False, "error": "Admin only"}), 403

        if action == "APPROVE":
            execute(conn, """
                UPDATE employee_unavailability
                SET status='APPROVED',
                    admin_note=%s,
                    decided_at=%s,
                    decided_by=%s,
                    show_in_active=%s
                WHERE id=%s
            """, (
                (data.get("admin_note") or "").strip() or None,
                now_iso(),
                session.get("user"),
                1 if data.get("show_in_active", row["show_in_active"]) else 0,
                item_id
            ))

        elif action == "DECLINE":
            execute(conn, """
                UPDATE employee_unavailability
                SET status='DECLINED',
                    admin_note=%s,
                    decided_at=%s,
                    decided_by=%s,
                    show_in_active=0
                WHERE id=%s
            """, (
                (data.get("admin_note") or "").strip() or None,
                now_iso(),
                session.get("user"),
                item_id
            ))

        elif action == "CANCEL":
            if not can_edit_unavailability(row, me):
                return jsonify({"ok": False, "error": "Not allowed"}), 403

            if (row["status"] or "").upper() == "APPROVED" and me["role"] != "ADMIN":
                execute(conn, """
                    UPDATE employee_unavailability
                    SET status='CANCEL_REQUESTED', cancel_requested_at=%s
                    WHERE id=%s
                """, (now_iso(), item_id))
            else:
                execute(conn, """
                    UPDATE employee_unavailability
                    SET status='CANCELLED',
                        cancelled_at=%s,
                        cancelled_by=%s,
                        show_in_active=0
                    WHERE id=%s
                """, (now_iso(), session.get("user"), item_id))

        elif action == "CONFIRM_CANCEL":
            execute(conn, """
                UPDATE employee_unavailability
                SET status='CANCELLED',
                    cancelled_at=%s,
                    cancelled_by=%s,
                    show_in_active=0
                WHERE id=%s
            """, (now_iso(), session.get("user"), item_id))

        else:
            return jsonify({"ok": False, "error": "Invalid action"}), 400

        new_row = fetchone(conn, """
            SELECT *
            FROM employee_unavailability
            WHERE id=%s
        """, (item_id,))

        write_audit_log(
            conn,
            action_type=action,
            entity_type="unavailability",
            entity_id=item_id,
            old_value=old_row,
            new_value=row_to_dict(new_row),
            note=f"Changed unavailability status using action={action}"
        )

        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/events", methods=["POST"])
@login_required
@require_module("CALENDAR", need_edit=True)
def api_calendar_event_create():
    conn = db()
    try:
        me = get_me(conn)
        data = request.json or {}

        scope_value = (data.get("calendar_scope") or "SHARED").upper()
        if scope_value == "ADMIN" and me["role"] != "ADMIN":
            return jsonify({"ok": False, "error": "Admin only"}), 403

        title = (data.get("title") or "").strip()
        event_date = (data.get("event_date") or "").strip()
        if not title or not event_date:
            return jsonify({"ok": False, "error": "Title and event date are required"}), 400

        reminder_times_raw = data.get("reminder_times") or []
        cleaned_reminders = []
        for item in reminder_times_raw[:4]:
            parsed = parse_reminder_entry(item)
            if parsed:
                cleaned_reminders.append(parsed.isoformat(timespec="seconds"))

        inserted = execute(conn, """
            INSERT INTO calendar_events (
                calendar_scope, title, event_date, start_time, end_time, all_day,
                notes, created_at, created_by, audience_type, audience_user_ids,
                reminder_times, is_active, show_in_active
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s)
            RETURNING id
        """, (
            scope_value,
            title,
            event_date,
            (data.get("start_time") or "").strip() or None,
            (data.get("end_time") or "").strip() or None,
            1 if data.get("all_day", True) else 0,
            (data.get("notes") or "").strip() or None,
            now_iso(),
            session.get("user"),
            (data.get("audience_type") or "ONLY_ME").upper(),
            ",".join(str(int(x)) for x in (data.get("audience_user_ids") or []) if str(x).isdigit()),
            ",".join(cleaned_reminders),
            1 if data.get("show_in_active") else 0,
        ), returning=True)

        new_id = inserted["id"]
        new_row = fetchone(conn, """
            SELECT *
            FROM calendar_events
            WHERE id=%s
        """, (new_id,))

        write_audit_log(
            conn,
            action_type="CREATE",
            entity_type="event",
            entity_id=new_id,
            old_value=None,
            new_value=row_to_dict(new_row),
            note="Created calendar event/reminder"
        )

        conn.commit()
        return jsonify({"ok": True, "message": "Reminder/Event saved"})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/events/<int:event_id>", methods=["PUT"])
@login_required
@require_module("CALENDAR", need_edit=True)
def api_calendar_event_update(event_id):
    conn = db()
    try:
        me = get_me(conn)
        data = request.json or {}

        row = fetchone(conn, """
            SELECT *
            FROM calendar_events
            WHERE id=%s AND is_active=1
            LIMIT 1
        """, (event_id,))

        if not row:
            return jsonify({"ok": False, "error": "Event not found"}), 404

        if not can_edit_event(row, me):
            return jsonify({"ok": False, "error": "Not allowed"}), 403

        scope_value = (data.get("calendar_scope") or row["calendar_scope"] or "SHARED").upper()
        if scope_value == "ADMIN" and me["role"] != "ADMIN":
            return jsonify({"ok": False, "error": "Admin only"}), 403

        title = (data.get("title") or "").strip()
        event_date = (data.get("event_date") or "").strip()
        if not title or not event_date:
            return jsonify({"ok": False, "error": "Title and event date are required"}), 400

        old_row = row_to_dict(row)

        reminder_times_raw = data.get("reminder_times") or []
        cleaned_reminders = []
        for item in reminder_times_raw[:4]:
            parsed = parse_reminder_entry(item)
            if parsed:
                cleaned_reminders.append(parsed.isoformat(timespec="seconds"))

        execute(conn, """
            UPDATE calendar_events
            SET calendar_scope=%s,
                title=%s,
                event_date=%s,
                start_time=%s,
                end_time=%s,
                all_day=%s,
                notes=%s,
                audience_type=%s,
                audience_user_ids=%s,
                reminder_times=%s,
                show_in_active=%s
            WHERE id=%s
        """, (
            scope_value,
            title,
            event_date,
            (data.get("start_time") or "").strip() or None,
            (data.get("end_time") or "").strip() or None,
            1 if data.get("all_day", True) else 0,
            (data.get("notes") or "").strip() or None,
            (data.get("audience_type") or "ONLY_ME").upper(),
            ",".join(str(int(x)) for x in (data.get("audience_user_ids") or []) if str(x).isdigit()),
            ",".join(cleaned_reminders),
            1 if data.get("show_in_active") else 0,
            event_id
        ))

        new_row = fetchone(conn, """
            SELECT *
            FROM calendar_events
            WHERE id=%s
        """, (event_id,))

        write_audit_log(
            conn,
            action_type="UPDATE",
            entity_type="event",
            entity_id=event_id,
            old_value=old_row,
            new_value=row_to_dict(new_row),
            note="Updated calendar event/reminder"
        )

        conn.commit()
        return jsonify({"ok": True, "message": "Event updated"})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/events/<int:event_id>", methods=["DELETE"])
@login_required
@require_module("CALENDAR", need_edit=True)
def api_calendar_event_delete(event_id):
    conn = db()
    try:
        me = get_me(conn)

        row = fetchone(conn, """
            SELECT *
            FROM calendar_events
            WHERE id=%s AND is_active=1
            LIMIT 1
        """, (event_id,))

        if not row:
            return jsonify({"ok": False, "error": "Event not found"}), 404

        if not can_edit_event(row, me):
            return jsonify({"ok": False, "error": "Not allowed"}), 403

        old_row = row_to_dict(row)

        execute(conn, """
            UPDATE calendar_events
            SET is_active=0, show_in_active=0
            WHERE id=%s
        """, (event_id,))

        execute(conn, """
            DELETE FROM calendar_event_acks
            WHERE event_id=%s
        """, (event_id,))

        write_audit_log(
            conn,
            action_type="DELETE",
            entity_type="event",
            entity_id=event_id,
            old_value=old_row,
            new_value={"id": event_id, "is_active": 0, "show_in_active": 0},
            note="Deleted calendar event/reminder"
        )

        conn.commit()
        return jsonify({"ok": True, "message": "Event deleted"})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/holidays", methods=["POST"])
@login_required
@require_module("CALENDAR", need_edit=True)
def api_calendar_holiday_create():
    conn = db()
    try:
        me = get_me(conn)

        if me["role"] != "ADMIN":
            return jsonify({"ok": False, "error": "Admin only"}), 403

        data = request.json or {}
        holiday_date = (data.get("holiday_date") or "").strip()
        title = (data.get("title") or "").strip()
        holiday_type = (data.get("holiday_type") or "GOVERNMENT_HOLIDAY").strip().upper()
        notes = (data.get("notes") or "").strip() or None

        if not holiday_date or not title:
            return jsonify({"ok": False, "error": "holiday_date and title are required"}), 400

        inserted = execute(conn, """
            INSERT INTO calendar_holidays (
                holiday_date, title, holiday_type, notes,
                is_active, created_at, created_by
            ) VALUES (%s,%s,%s,%s,1,%s,%s)
            RETURNING id
        """, (
            holiday_date,
            title,
            holiday_type,
            notes,
            now_iso(),
            session.get("user")
        ), returning=True)

        holiday_id = inserted["id"]
        new_row = fetchone(conn, """
            SELECT *
            FROM calendar_holidays
            WHERE id=%s
        """, (holiday_id,))

        write_audit_log(
            conn,
            action_type="CREATE",
            entity_type="holiday",
            entity_id=holiday_id,
            old_value=None,
            new_value=row_to_dict(new_row),
            note="Created holiday/special day"
        )

        conn.commit()
        return jsonify({"ok": True, "message": "Holiday saved"})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/holidays/<int:holiday_id>", methods=["PUT"])
@login_required
@require_module("CALENDAR", need_edit=True)
def api_calendar_holiday_update(holiday_id):
    conn = db()
    try:
        me = get_me(conn)

        if me["role"] != "ADMIN":
            return jsonify({"ok": False, "error": "Admin only"}), 403

        row = fetchone(conn, """
            SELECT *
            FROM calendar_holidays
            WHERE id=%s AND is_active=1
            LIMIT 1
        """, (holiday_id,))

        if not row:
            return jsonify({"ok": False, "error": "Holiday not found"}), 404

        data = request.json or {}
        holiday_date = (data.get("holiday_date") or "").strip()
        title = (data.get("title") or "").strip()
        holiday_type = (data.get("holiday_type") or "GOVERNMENT_HOLIDAY").strip().upper()
        notes = (data.get("notes") or "").strip() or None

        if not holiday_date or not title:
            return jsonify({"ok": False, "error": "holiday_date and title are required"}), 400

        old_row = row_to_dict(row)

        execute(conn, """
            UPDATE calendar_holidays
            SET holiday_date=%s,
                title=%s,
                holiday_type=%s,
                notes=%s,
                edited_at=%s,
                edited_by=%s
            WHERE id=%s
        """, (
            holiday_date,
            title,
            holiday_type,
            notes,
            now_iso(),
            session.get("user"),
            holiday_id
        ))

        new_row = fetchone(conn, """
            SELECT *
            FROM calendar_holidays
            WHERE id=%s
        """, (holiday_id,))

        write_audit_log(
            conn,
            action_type="UPDATE",
            entity_type="holiday",
            entity_id=holiday_id,
            old_value=old_row,
            new_value=row_to_dict(new_row),
            note="Updated holiday/special day"
        )

        conn.commit()
        return jsonify({"ok": True, "message": "Holiday updated"})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/holidays/<int:holiday_id>", methods=["DELETE"])
@login_required
@require_module("CALENDAR", need_edit=True)
def api_calendar_holiday_delete(holiday_id):
    conn = db()
    try:
        me = get_me(conn)

        if me["role"] != "ADMIN":
            return jsonify({"ok": False, "error": "Admin only"}), 403

        row = fetchone(conn, """
            SELECT *
            FROM calendar_holidays
            WHERE id=%s AND is_active=1
            LIMIT 1
        """, (holiday_id,))

        if not row:
            return jsonify({"ok": False, "error": "Holiday not found"}), 404

        old_row = row_to_dict(row)

        execute(conn, """
            UPDATE calendar_holidays
            SET is_active=0,
                edited_at=%s,
                edited_by=%s
            WHERE id=%s
        """, (
            now_iso(),
            session.get("user"),
            holiday_id
        ))

        write_audit_log(
            conn,
            action_type="DELETE",
            entity_type="holiday",
            entity_id=holiday_id,
            old_value=old_row,
            new_value={"id": holiday_id, "is_active": 0},
            note="Deleted holiday/special day"
        )

        conn.commit()
        return jsonify({"ok": True, "message": "Holiday deleted"})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/month", methods=["GET"])
@login_required
@require_module("CALENDAR")
def api_calendar_month():
    conn = db()
    try:
        me = get_me(conn)

        month = request.args.get("month") or datetime.now().strftime("%Y-%m")
        view = (request.args.get("view") or "ACTIVE").upper()
        requested_user_id = request.args.get("user_id")

        year, month_num = parse_month(month)
        start_date, end_date = month_range(year, month_num)

        users = {u["id"]: (u["full_name"] or u["username"]) for u in get_all_active_users(conn)}

        if view == "ACTIVE":
            target_user_id = None
        elif view == "MY":
            target_user_id = int(me["id"])
        else:
            if requested_user_id:
                try:
                    target_user_id = int(requested_user_id)
                except Exception:
                    return jsonify({"ok": False, "error": "Invalid user_id"}), 400
            else:
                target_user_id = int(me["id"])

            if me["role"] != "ADMIN" and target_user_id != int(me["id"]):
                return jsonify({"ok": False, "error": "Not allowed"}), 403

        schedules = fetchall(conn, "SELECT * FROM employee_schedules")

        if view == "ACTIVE":
            if me["role"] == "ADMIN":
                unavailability = fetchall(conn, """
                    SELECT *
                    FROM employee_unavailability
                    WHERE start_date <= %s
                      AND end_date >= %s
                      AND (
                            (status='APPROVED' AND show_in_active=1)
                            OR status='PENDING'
                            OR status='CANCEL_REQUESTED'
                      )
                """, (end_date, start_date))
            else:
                unavailability = fetchall(conn, """
                    SELECT *
                    FROM employee_unavailability
                    WHERE start_date <= %s
                      AND end_date >= %s
                      AND status='APPROVED'
                      AND show_in_active=1
                """, (end_date, start_date))
        else:
            unavailability = fetchall(conn, """
                SELECT *
                FROM employee_unavailability
                WHERE start_date <= %s
                  AND end_date >= %s
                  AND status <> 'CANCELLED'
                  AND user_id=%s
            """, (end_date, start_date, target_user_id))

        events = fetchall(conn, """
            SELECT *
            FROM calendar_events
            WHERE is_active=1
              AND calendar_scope='SHARED'
              AND event_date >= %s
              AND event_date <= %s
            ORDER BY event_date ASC, COALESCE(start_time,'') ASC
        """, (start_date, end_date))

        holidays = fetchall(conn, """
            SELECT *
            FROM calendar_holidays
            WHERE is_active=1
              AND holiday_date >= %s
              AND holiday_date <= %s
            ORDER BY holiday_date ASC, id ASC
        """, (start_date, end_date))

        days = {}
        d = datetime(year, month_num, 1)
        while d.month == month_num:
            days[d.date().isoformat()] = {
                "date": d.date().isoformat(),
                "weekday": d.weekday(),
                "schedule": [],
                "unavailability": [],
                "events": [],
                "holidays": []
            }
            d += timedelta(days=1)

        if view != "ACTIVE":
            for day in days.values():
                for s in schedules:
                    if int(s["weekday"]) == int(day["weekday"]) and int(s["user_id"]) == int(target_user_id):
                        day["schedule"].append({
                            "user_id": s["user_id"],
                            "user_name": users.get(s["user_id"], f"User {s['user_id']}"),
                            "is_working": int(s["is_working"] or 0),
                            "start_time": s["start_time"] or "",
                            "end_time": s["end_time"] or "",
                        })

        for h in holidays:
            key = h["holiday_date"]
            if key in days:
                days[key]["holidays"].append({
                    "id": h["id"],
                    "holiday_date": h["holiday_date"],
                    "title": h["title"],
                    "holiday_type": h["holiday_type"],
                    "notes": h["notes"] or "",
                })

        for r in unavailability:
            cur = datetime.fromisoformat(r["start_date"])
            end = datetime.fromisoformat(r["end_date"])
            while cur <= end:
                key = cur.date().isoformat()
                if key in days:
                    days[key]["unavailability"].append({
                        "id": r["id"],
                        "user_id": r["user_id"],
                        "user_name": users.get(r["user_id"], f"User {r['user_id']}"),
                        "status": r["status"],
                        "all_day": int(r["all_day"] or 0),
                        "start_time": r["start_time"] or "",
                        "end_time": r["end_time"] or "",
                        "reason": r["reason"] or "",
                        "requested_by": r["requested_by"] or "",
                        "show_in_active": int(r["show_in_active"] or 0),
                        "admin_note": r["admin_note"] or "",
                        "is_admin_review": 1 if (view == "ACTIVE" and me["role"] == "ADMIN" and str(r["status"]).upper() in ("PENDING", "CANCEL_REQUESTED")) else 0,
                    })
                cur += timedelta(days=1)

        for e in events:
            if view == "ACTIVE":
                if int(e["show_in_active"] or 0) != 1:
                    continue
            else:
                if not event_visible_to_user(e, int(me["id"]), me["role"]):
                    continue

            key = e["event_date"]
            if key in days:
                days[key]["events"].append({
                    "id": e["id"],
                    "title": e["title"],
                    "event_date": e["event_date"],
                    "start_time": e["start_time"] or "",
                    "end_time": e["end_time"] or "",
                    "all_day": int(e["all_day"] or 0),
                    "notes": e["notes"] or "",
                    "created_by": e["created_by"],
                    "audience_type": e["audience_type"],
                    "audience_user_ids": e["audience_user_ids"] or "",
                    "reminder_times": (e["reminder_times"] or "").split(",") if e["reminder_times"] else [],
                    "show_in_active": int(e["show_in_active"] or 0),
                })

        return jsonify({
            "ok": True,
            "data": {
                "month": f"{year:04d}-{month_num:02d}",
                "days": list(days.values())
            }
        })
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/requests", methods=["GET"])
@login_required
@require_module("CALENDAR")
def api_calendar_requests():
    conn = db()
    try:
        me = get_me(conn)

        month = request.args.get("month") or datetime.now().strftime("%Y-%m")
        year, month_num = parse_month(month)
        start_date, end_date = month_range(year, month_num)

        if me["role"] == "ADMIN":
            rows = fetchall(conn, """
                SELECT *
                FROM employee_unavailability
                WHERE start_date <= %s
                  AND end_date >= %s
                ORDER BY requested_at DESC
            """, (end_date, start_date))
        else:
            rows = fetchall(conn, """
                SELECT *
                FROM employee_unavailability
                WHERE user_id=%s
                  AND start_date <= %s
                  AND end_date >= %s
                ORDER BY requested_at DESC
            """, (me["id"], end_date, start_date))

        users = {u["id"]: (u["full_name"] or u["username"]) for u in get_all_active_users(conn)}
        data = []

        for r in rows:
            item = dict(r)
            item["user_name"] = users.get(r["user_id"], f"User {r['user_id']}")
            data.append(item)

        return jsonify({"ok": True, "data": data})
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/requests/delete", methods=["POST"])
@login_required
@require_module("CALENDAR", need_edit=True)
def api_calendar_requests_delete():
    conn = db()
    try:
        me = get_me(conn)
        data = request.json or {}
        ids = data.get("ids") or []

        clean_ids = []
        for x in ids:
            try:
                clean_ids.append(int(x))
            except Exception:
                continue

        if not clean_ids:
            return jsonify({"ok": False, "error": "No valid items selected"}), 400

        placeholders = ",".join(["%s"] * len(clean_ids))

        if me["role"] == "ADMIN":
            old_rows = fetchall(conn, f"""
                SELECT *
                FROM employee_unavailability
                WHERE id IN ({placeholders})
            """, tuple(clean_ids))

            execute(conn, f"""
                DELETE FROM employee_unavailability
                WHERE id IN ({placeholders})
            """, tuple(clean_ids))
        else:
            old_rows = fetchall(conn, f"""
                SELECT *
                FROM employee_unavailability
                WHERE id IN ({placeholders})
                  AND user_id=%s
            """, tuple(clean_ids) + (int(me["id"]),))

            execute(conn, f"""
                DELETE FROM employee_unavailability
                WHERE id IN ({placeholders})
                  AND user_id=%s
            """, tuple(clean_ids) + (int(me["id"]),))

        write_audit_log(
            conn,
            action_type="BULK_DELETE",
            entity_type="unavailability",
            entity_id=None,
            old_value=[row_to_dict(r) for r in old_rows],
            new_value=None,
            note="Deleted selected request/history items"
        )

        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/audit", methods=["GET"])
@login_required
@require_module("CALENDAR")
def api_calendar_audit():
    conn = db()
    try:
        me = get_me(conn)

        if me["role"] != "ADMIN":
            return jsonify({"ok": False, "error": "Admin only"}), 403

        limit = request.args.get("limit", 100)
        try:
            limit = max(1, min(int(limit), 500))
        except Exception:
            limit = 100

        rows = fetchall(conn, """
            SELECT *
            FROM calendar_audit_logs
            ORDER BY changed_at DESC, id DESC
            LIMIT %s
        """, (limit,))

        data = []
        for r in rows:
            item = dict(r)
            for key in ("old_value", "new_value"):
                try:
                    item[key] = json.loads(item[key]) if item.get(key) else None
                except Exception:
                    item[key] = item.get(key)
            data.append(item)

        return jsonify({"ok": True, "data": data})
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/reminders/pending", methods=["GET"])
@login_required
@require_module("CALENDAR")
def api_calendar_reminders_pending():
    conn = db()
    try:
        me = get_me(conn)
        now = datetime.now(APP_TZ).replace(tzinfo=None)

        month_start = (now - timedelta(days=60)).date().isoformat()

        rows = fetchall(conn, """
            SELECT *
            FROM calendar_events
            WHERE is_active=1
              AND event_date >= %s
            ORDER BY event_date ASC
        """, (month_start,))

        pending = []

        for e in rows:
            if not event_visible_to_user(e, int(me["id"]), me["role"]):
                continue

            raw_times = [x.strip() for x in (e["reminder_times"] or "").split(",") if x.strip()]
            for t in raw_times:
                remind_dt = parse_reminder_entry(t)
                if not remind_dt:
                    continue

                if remind_dt <= now:
                    remind_at_value = remind_dt.isoformat(timespec="seconds")
                    ack = fetchone(conn, """
                        SELECT 1
                        FROM calendar_event_acks
                        WHERE event_id=%s AND user_id=%s AND remind_at=%s
                        LIMIT 1
                    """, (e["id"], me["id"], remind_at_value))

                    if not ack:
                        pending.append({
                            "event_id": e["id"],
                            "title": e["title"],
                            "event_date": e["event_date"],
                            "remind_at": remind_at_value,
                        })

        pending.sort(key=lambda x: x["remind_at"])
        return jsonify({"ok": True, "data": pending})
    finally:
        conn.close()


@calendar_bp.route("/api/calendar/reminders/ack", methods=["POST"])
@login_required
@require_module("CALENDAR")
def api_calendar_reminders_ack():
    conn = db()
    try:
        me = get_me(conn)
        data = request.json or {}

        event_id = data.get("event_id")
        remind_at = data.get("remind_at")

        if not event_id or not remind_at:
            return jsonify({"ok": False, "error": "event_id and remind_at are required"}), 400

        execute(conn, """
            INSERT INTO calendar_event_acks (event_id, user_id, remind_at, acked_at)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (event_id, user_id, remind_at) DO NOTHING
        """, (
            int(event_id),
            int(me["id"]),
            remind_at,
            now_iso()
        ))

        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@calendar_bp.route("/api/messages/calendar-badge", methods=["GET"])
@login_required
def api_calendar_badge():
    return api_calendar_reminders_pending()