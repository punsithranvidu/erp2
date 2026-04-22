from calendar import monthrange
from datetime import datetime
from functools import wraps
from zoneinfo import ZoneInfo

from flask import Blueprint, current_app, jsonify, render_template, request, session

from .db import connect, get_table_columns

weekly_tasks_bp = Blueprint("weekly_tasks", __name__)

APP_TZ = ZoneInfo("Asia/Colombo")
TASK_STATUSES = ("Pending", "Active", "Started", "Done", "Cancelled")
CONFIRMATION_STATUSES = ("PENDING", "CONFIRMED", "DENIED")
EDIT_REQUEST_STATUSES = ("PENDING", "APPROVED", "DENIED")
WEEKS = (1, 2, 3, 4)


def db():
    return connect(current_app.config["DATABASE_URL"])


def now_iso():
    return datetime.now(APP_TZ).replace(tzinfo=None).isoformat(timespec="seconds")


def current_year_month():
    now = datetime.now(APP_TZ)
    return now.year, now.month


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


def parse_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def normalize_status(value):
    status = clean_text(value) or "Pending"
    return status if status in TASK_STATUSES else None


def normalize_confirmation(value):
    status = clean_text(value).upper() or "PENDING"
    return status if status in CONFIRMATION_STATUSES else None


def ensure_weekly_task_tables():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weekly_tasks (
            id BIGSERIAL PRIMARY KEY,
            owner_user_id BIGINT NOT NULL,
            task_text TEXT NOT NULL,
            task_year INTEGER NOT NULL,
            task_month INTEGER NOT NULL,
            week_number INTEGER NOT NULL,
            target_year INTEGER NOT NULL,
            target_month INTEGER NOT NULL,
            target_week INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'Pending',
            confirmation_status TEXT NOT NULL DEFAULT 'PENDING',
            carry_forward_count INTEGER NOT NULL DEFAULT 0,
            source_task_id BIGINT,
            created_by_user_id BIGINT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_by TEXT,
            updated_at TEXT,
            confirmed_by TEXT,
            confirmed_at TEXT,
            started_by TEXT,
            started_at TEXT,
            done_by TEXT,
            done_at TEXT,
            cancelled_by TEXT,
            cancelled_at TEXT,
            moved_by TEXT,
            moved_at TEXT,
            deleted_at TEXT,
            deleted_by TEXT,
            deleted_by_user_id BIGINT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weekly_task_edit_requests (
            id BIGSERIAL PRIMARY KEY,
            task_id BIGINT NOT NULL,
            requested_text TEXT NOT NULL,
            request_status TEXT NOT NULL DEFAULT 'PENDING',
            requested_by_user_id BIGINT,
            requested_by TEXT NOT NULL,
            requested_at TEXT NOT NULL,
            reviewed_by TEXT,
            reviewed_at TEXT,
            review_note TEXT,
            FOREIGN KEY(task_id) REFERENCES weekly_tasks(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weekly_task_history (
            id BIGSERIAL PRIMARY KEY,
            task_id BIGINT NOT NULL,
            action_type TEXT NOT NULL,
            old_task_text TEXT,
            new_task_text TEXT,
            old_status TEXT,
            new_status TEXT,
            old_year INTEGER,
            old_month INTEGER,
            old_week INTEGER,
            new_year INTEGER,
            new_month INTEGER,
            new_week INTEGER,
            acted_by TEXT NOT NULL,
            acted_by_user_id BIGINT,
            acted_at TEXT NOT NULL,
            note TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weekly_task_week_locks (
            id BIGSERIAL PRIMARY KEY,
            owner_user_id BIGINT NOT NULL,
            task_year INTEGER NOT NULL,
            task_month INTEGER NOT NULL,
            week_number INTEGER NOT NULL,
            is_frozen INTEGER NOT NULL DEFAULT 1,
            manual_unfrozen INTEGER NOT NULL DEFAULT 0,
            frozen_at TEXT,
            frozen_by TEXT,
            unfrozen_at TEXT,
            unfrozen_by TEXT,
            UNIQUE(owner_user_id, task_year, task_month, week_number)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weekly_task_notification_state (
            user_id BIGINT PRIMARY KEY,
            last_seen_at TEXT NOT NULL DEFAULT ''
        )
    """)

    task_cols = get_table_columns(conn, "weekly_tasks")
    task_missing = {
        "owner_user_id": "ALTER TABLE weekly_tasks ADD COLUMN owner_user_id BIGINT",
        "task_text": "ALTER TABLE weekly_tasks ADD COLUMN task_text TEXT NOT NULL DEFAULT ''",
        "task_year": "ALTER TABLE weekly_tasks ADD COLUMN task_year INTEGER NOT NULL DEFAULT 0",
        "task_month": "ALTER TABLE weekly_tasks ADD COLUMN task_month INTEGER NOT NULL DEFAULT 0",
        "week_number": "ALTER TABLE weekly_tasks ADD COLUMN week_number INTEGER NOT NULL DEFAULT 1",
        "target_year": "ALTER TABLE weekly_tasks ADD COLUMN target_year INTEGER NOT NULL DEFAULT 0",
        "target_month": "ALTER TABLE weekly_tasks ADD COLUMN target_month INTEGER NOT NULL DEFAULT 0",
        "target_week": "ALTER TABLE weekly_tasks ADD COLUMN target_week INTEGER NOT NULL DEFAULT 1",
        "status": "ALTER TABLE weekly_tasks ADD COLUMN status TEXT NOT NULL DEFAULT 'Pending'",
        "confirmation_status": "ALTER TABLE weekly_tasks ADD COLUMN confirmation_status TEXT NOT NULL DEFAULT 'PENDING'",
        "carry_forward_count": "ALTER TABLE weekly_tasks ADD COLUMN carry_forward_count INTEGER NOT NULL DEFAULT 0",
        "source_task_id": "ALTER TABLE weekly_tasks ADD COLUMN source_task_id BIGINT",
        "created_by_user_id": "ALTER TABLE weekly_tasks ADD COLUMN created_by_user_id BIGINT",
        "created_by": "ALTER TABLE weekly_tasks ADD COLUMN created_by TEXT NOT NULL DEFAULT ''",
        "created_at": "ALTER TABLE weekly_tasks ADD COLUMN created_at TEXT NOT NULL DEFAULT ''",
        "updated_by": "ALTER TABLE weekly_tasks ADD COLUMN updated_by TEXT",
        "updated_at": "ALTER TABLE weekly_tasks ADD COLUMN updated_at TEXT",
        "confirmed_by": "ALTER TABLE weekly_tasks ADD COLUMN confirmed_by TEXT",
        "confirmed_at": "ALTER TABLE weekly_tasks ADD COLUMN confirmed_at TEXT",
        "started_by": "ALTER TABLE weekly_tasks ADD COLUMN started_by TEXT",
        "started_at": "ALTER TABLE weekly_tasks ADD COLUMN started_at TEXT",
        "done_by": "ALTER TABLE weekly_tasks ADD COLUMN done_by TEXT",
        "done_at": "ALTER TABLE weekly_tasks ADD COLUMN done_at TEXT",
        "cancelled_by": "ALTER TABLE weekly_tasks ADD COLUMN cancelled_by TEXT",
        "cancelled_at": "ALTER TABLE weekly_tasks ADD COLUMN cancelled_at TEXT",
        "moved_by": "ALTER TABLE weekly_tasks ADD COLUMN moved_by TEXT",
        "moved_at": "ALTER TABLE weekly_tasks ADD COLUMN moved_at TEXT",
        "deleted_at": "ALTER TABLE weekly_tasks ADD COLUMN deleted_at TEXT",
        "deleted_by": "ALTER TABLE weekly_tasks ADD COLUMN deleted_by TEXT",
        "deleted_by_user_id": "ALTER TABLE weekly_tasks ADD COLUMN deleted_by_user_id BIGINT",
    }
    for col, sql in task_missing.items():
        if col not in task_cols:
            cur.execute(sql)

    req_cols = get_table_columns(conn, "weekly_task_edit_requests")
    req_missing = {
        "task_id": "ALTER TABLE weekly_task_edit_requests ADD COLUMN task_id BIGINT",
        "requested_text": "ALTER TABLE weekly_task_edit_requests ADD COLUMN requested_text TEXT NOT NULL DEFAULT ''",
        "request_status": "ALTER TABLE weekly_task_edit_requests ADD COLUMN request_status TEXT NOT NULL DEFAULT 'PENDING'",
        "requested_by_user_id": "ALTER TABLE weekly_task_edit_requests ADD COLUMN requested_by_user_id BIGINT",
        "requested_by": "ALTER TABLE weekly_task_edit_requests ADD COLUMN requested_by TEXT NOT NULL DEFAULT ''",
        "requested_at": "ALTER TABLE weekly_task_edit_requests ADD COLUMN requested_at TEXT NOT NULL DEFAULT ''",
        "reviewed_by": "ALTER TABLE weekly_task_edit_requests ADD COLUMN reviewed_by TEXT",
        "reviewed_at": "ALTER TABLE weekly_task_edit_requests ADD COLUMN reviewed_at TEXT",
        "review_note": "ALTER TABLE weekly_task_edit_requests ADD COLUMN review_note TEXT",
    }
    for col, sql in req_missing.items():
        if col not in req_cols:
            cur.execute(sql)

    hist_cols = get_table_columns(conn, "weekly_task_history")
    hist_missing = {
        "task_id": "ALTER TABLE weekly_task_history ADD COLUMN task_id BIGINT",
        "action_type": "ALTER TABLE weekly_task_history ADD COLUMN action_type TEXT NOT NULL DEFAULT 'UPDATE'",
        "old_task_text": "ALTER TABLE weekly_task_history ADD COLUMN old_task_text TEXT",
        "new_task_text": "ALTER TABLE weekly_task_history ADD COLUMN new_task_text TEXT",
        "old_status": "ALTER TABLE weekly_task_history ADD COLUMN old_status TEXT",
        "new_status": "ALTER TABLE weekly_task_history ADD COLUMN new_status TEXT",
        "old_year": "ALTER TABLE weekly_task_history ADD COLUMN old_year INTEGER",
        "old_month": "ALTER TABLE weekly_task_history ADD COLUMN old_month INTEGER",
        "old_week": "ALTER TABLE weekly_task_history ADD COLUMN old_week INTEGER",
        "new_year": "ALTER TABLE weekly_task_history ADD COLUMN new_year INTEGER",
        "new_month": "ALTER TABLE weekly_task_history ADD COLUMN new_month INTEGER",
        "new_week": "ALTER TABLE weekly_task_history ADD COLUMN new_week INTEGER",
        "acted_by": "ALTER TABLE weekly_task_history ADD COLUMN acted_by TEXT NOT NULL DEFAULT ''",
        "acted_by_user_id": "ALTER TABLE weekly_task_history ADD COLUMN acted_by_user_id BIGINT",
        "acted_at": "ALTER TABLE weekly_task_history ADD COLUMN acted_at TEXT NOT NULL DEFAULT ''",
        "note": "ALTER TABLE weekly_task_history ADD COLUMN note TEXT",
    }
    for col, sql in hist_missing.items():
        if col not in hist_cols:
            cur.execute(sql)

    lock_cols = get_table_columns(conn, "weekly_task_week_locks")
    lock_missing = {
        "owner_user_id": "ALTER TABLE weekly_task_week_locks ADD COLUMN owner_user_id BIGINT",
        "task_year": "ALTER TABLE weekly_task_week_locks ADD COLUMN task_year INTEGER NOT NULL DEFAULT 0",
        "task_month": "ALTER TABLE weekly_task_week_locks ADD COLUMN task_month INTEGER NOT NULL DEFAULT 0",
        "week_number": "ALTER TABLE weekly_task_week_locks ADD COLUMN week_number INTEGER NOT NULL DEFAULT 1",
        "is_frozen": "ALTER TABLE weekly_task_week_locks ADD COLUMN is_frozen INTEGER NOT NULL DEFAULT 1",
        "manual_unfrozen": "ALTER TABLE weekly_task_week_locks ADD COLUMN manual_unfrozen INTEGER NOT NULL DEFAULT 0",
        "frozen_at": "ALTER TABLE weekly_task_week_locks ADD COLUMN frozen_at TEXT",
        "frozen_by": "ALTER TABLE weekly_task_week_locks ADD COLUMN frozen_by TEXT",
        "unfrozen_at": "ALTER TABLE weekly_task_week_locks ADD COLUMN unfrozen_at TEXT",
        "unfrozen_by": "ALTER TABLE weekly_task_week_locks ADD COLUMN unfrozen_by TEXT",
    }
    for col, sql in lock_missing.items():
        if col not in lock_cols:
            cur.execute(sql)

    notification_cols = get_table_columns(conn, "weekly_task_notification_state")
    notification_missing = {
        "user_id": "ALTER TABLE weekly_task_notification_state ADD COLUMN user_id BIGINT",
        "last_seen_at": "ALTER TABLE weekly_task_notification_state ADD COLUMN last_seen_at TEXT NOT NULL DEFAULT ''",
    }
    for col, sql in notification_missing.items():
        if col not in notification_cols:
            cur.execute(sql)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_weekly_tasks_owner_period ON weekly_tasks (owner_user_id, task_year, task_month, week_number)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_weekly_tasks_deleted ON weekly_tasks (deleted_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_weekly_tasks_status ON weekly_tasks (status, confirmation_status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_weekly_tasks_created_at ON weekly_tasks (created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_weekly_task_edit_requests_task_status ON weekly_task_edit_requests (task_id, request_status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_weekly_task_history_task ON weekly_task_history (task_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_weekly_task_week_locks_period ON weekly_task_week_locks (owner_user_id, task_year, task_month, week_number)")

    conn.commit()
    conn.close()


@weekly_tasks_bp.before_app_request
def weekly_task_tables_ready():
    if current_app.config.get("WEEKLY_TASK_TABLES_READY"):
        return
    ensure_weekly_task_tables()
    current_app.config["WEEKLY_TASK_TABLES_READY"] = True


def get_user(conn, uid):
    return conn.execute("""
        SELECT id, username, role, active, full_name
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (uid,)).fetchone()


def active_users(conn):
    rows = conn.execute("""
        SELECT id, username, role, active, full_name
        FROM users
        WHERE active=1
        ORDER BY COALESCE(full_name, username) ASC
    """).fetchall()
    return [dict(r) for r in rows]


def get_weekly_task_notification_seen_at(conn, user_id):
    row = conn.execute("""
        SELECT last_seen_at
        FROM weekly_task_notification_state
        WHERE user_id=%s
        LIMIT 1
    """, (user_id,)).fetchone()
    return row["last_seen_at"] if row else ""


def mark_weekly_task_notifications_seen(conn):
    uid = current_user_id()
    if not uid:
        return
    conn.execute("""
        INSERT INTO weekly_task_notification_state (user_id, last_seen_at)
        VALUES (%s,%s)
        ON CONFLICT(user_id)
        DO UPDATE SET last_seen_at=excluded.last_seen_at
    """, (uid, now_iso()))


def selected_owner_id(conn, requested_owner_id=None):
    if is_admin():
        owner_id = parse_int(requested_owner_id, None)
        if owner_id:
            return owner_id
        row = conn.execute("""
            SELECT id
            FROM users
            WHERE active=1 AND role='EMP'
            ORDER BY username ASC
            LIMIT 1
        """).fetchone()
        return row["id"] if row else current_user_id()
    return current_user_id()


def get_task(conn, task_id, include_deleted=False):
    deleted_sql = "" if include_deleted else "AND deleted_at IS NULL"
    return conn.execute(f"""
        SELECT t.*, u.username AS owner_username, u.full_name AS owner_full_name
        FROM weekly_tasks t
        LEFT JOIN users u ON u.id=t.owner_user_id
        WHERE t.id=%s {deleted_sql}
        LIMIT 1
    """, (task_id,)).fetchone()


def can_access_task(row):
    if not row:
        return False
    return is_admin() or int(row["owner_user_id"]) == int(current_user_id() or 0)


def can_modify_task(row):
    if not row:
        return False
    return is_admin() or int(row["owner_user_id"]) == int(current_user_id() or 0)


def add_history(conn, task_id, action_type, old=None, new=None, note=None, actor_name=None, actor_user_id=None):
    old = old or {}
    new = new or {}
    actor_name = actor_name if actor_name is not None else session.get("user", "system")
    actor_user_id = actor_user_id if actor_user_id is not None else current_user_id()
    conn.execute("""
        INSERT INTO weekly_task_history (
            task_id, action_type, old_task_text, new_task_text,
            old_status, new_status, old_year, old_month, old_week,
            new_year, new_month, new_week, acted_by, acted_by_user_id, acted_at, note
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        task_id,
        action_type,
        old.get("task_text"),
        new.get("task_text"),
        old.get("status"),
        new.get("status"),
        old.get("task_year"),
        old.get("task_month"),
        old.get("week_number"),
        new.get("task_year"),
        new.get("task_month"),
        new.get("week_number"),
        actor_name,
        actor_user_id,
        now_iso(),
        note,
    ))


def next_week(year, month, week):
    if week < 4:
        return year, month, week + 1
    if month == 12:
        return year + 1, 1, 1
    return year, month + 1, 1


def week_end_date(year, month, week):
    last_day = monthrange(year, month)[1]
    if week == 1:
        day = 7
    elif week == 2:
        day = 14
    elif week == 3:
        day = 21
    else:
        day = last_day
    return datetime(year, month, min(day, last_day), 23, 59, 59)


def week_is_over(year, month, week):
    return datetime.now(APP_TZ).replace(tzinfo=None) > week_end_date(year, month, week)


def get_week_lock(conn, owner_id, year, month, week):
    return conn.execute("""
        SELECT *
        FROM weekly_task_week_locks
        WHERE owner_user_id=%s AND task_year=%s AND task_month=%s AND week_number=%s
        LIMIT 1
    """, (owner_id, year, month, week)).fetchone()


def is_week_frozen(conn, owner_id, year, month, week):
    row = get_week_lock(conn, owner_id, year, month, week)
    return bool(row and int(row["is_frozen"] or 0) == 1)


def ensure_week_frozen(conn, owner_id, year, month, week, actor="system"):
    existing = get_week_lock(conn, owner_id, year, month, week)
    if existing and int(existing["manual_unfrozen"] or 0) == 1:
        return
    frozen_at = now_iso()
    conn.execute("""
        INSERT INTO weekly_task_week_locks (
            owner_user_id, task_year, task_month, week_number,
            is_frozen, manual_unfrozen, frozen_at, frozen_by
        )
        VALUES (%s,%s,%s,%s,1,0,%s,%s)
        ON CONFLICT(owner_user_id, task_year, task_month, week_number)
        DO UPDATE SET
            is_frozen=1,
            frozen_at=COALESCE(weekly_task_week_locks.frozen_at, excluded.frozen_at),
            frozen_by=COALESCE(weekly_task_week_locks.frozen_by, excluded.frozen_by)
        WHERE weekly_task_week_locks.manual_unfrozen=0
    """, (owner_id, year, month, week, frozen_at, actor))


def auto_carry_task(conn, old, actor="system"):
    ny, nm, nw = next_week(int(old["task_year"]), int(old["task_month"]), int(old["week_number"]))
    moved_at = now_iso()
    new_carry = int(old["carry_forward_count"] or 0) + 1
    old_after = conn.execute("""
        UPDATE weekly_tasks
        SET status='Cancelled',
            moved_by=%s,
            moved_at=%s,
            cancelled_by=%s,
            cancelled_at=%s,
            updated_by=%s,
            updated_at=%s
        WHERE id=%s
        RETURNING *
    """, (actor, moved_at, actor, moved_at, actor, moved_at, old["id"])).fetchone()
    new_row = conn.execute("""
        INSERT INTO weekly_tasks (
            owner_user_id, task_text, task_year, task_month, week_number,
            target_year, target_month, target_week, status, confirmation_status,
            carry_forward_count, source_task_id, created_by_user_id, created_by, created_at,
            confirmed_by, confirmed_at, started_by, started_at, moved_by, moved_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'CONFIRMED',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING *
    """, (
        old["owner_user_id"], old["task_text"], ny, nm, nw, ny, nm, nw,
        old["status"], new_carry, old["id"], None, actor, moved_at,
        actor, moved_at,
        old["started_by"] if old["status"] == "Started" else None,
        old["started_at"] if old["status"] == "Started" else None,
        actor, moved_at,
    )).fetchone()
    add_history(conn, old["id"], "AUTO_CARRY_FROM", old=dict(old), new=dict(old_after), note=f"Auto moved to {ny}-{nm:02d} week {nw}", actor_name=actor, actor_user_id=None)
    add_history(conn, new_row["id"], "AUTO_CARRY_TO", old=dict(old), new=dict(new_row), note=f"Auto moved from task #{old['id']}", actor_name=actor, actor_user_id=None)
    return new_row


def auto_process_completed_weeks(conn, owner_id, year, month):
    for week in WEEKS:
        if not week_is_over(year, month, week):
            continue
        lock = get_week_lock(conn, owner_id, year, month, week)
        if lock and int(lock["manual_unfrozen"] or 0) == 1:
            continue
        tasks = conn.execute("""
            SELECT *
            FROM weekly_tasks
            WHERE deleted_at IS NULL
              AND owner_user_id=%s
              AND task_year=%s
              AND task_month=%s
              AND week_number=%s
              AND confirmation_status='CONFIRMED'
              AND status IN ('Active','Started')
            ORDER BY id ASC
        """, (owner_id, year, month, week)).fetchall()
        for task in tasks:
            auto_carry_task(conn, task, actor="system")
        ensure_week_frozen(conn, owner_id, year, month, week, actor="system")


def task_dict(row):
    d = dict(row)
    pending = d.get("pending_edit_count")
    d["pending_edit_count"] = int(pending or 0)
    d["can_admin"] = 1 if is_admin() else 0
    d["can_edit_direct"] = 1 if is_admin() else 0
    d["can_request_edit"] = 1 if not is_admin() and int(row["owner_user_id"]) == int(current_user_id() or 0) and row["confirmation_status"] == "CONFIRMED" else 0
    return d


def block_if_week_frozen(conn, row):
    if is_week_frozen(conn, row["owner_user_id"], row["task_year"], row["task_month"], row["week_number"]):
        return jsonify({"ok": False, "error": "This week is frozen. Ask admin to unfreeze it if changes are needed."}), 403
    return None


def week_health(tasks):
    carry = sum(int(t["carry_forward_count"] or 0) for t in tasks)
    open_count = sum(1 for t in tasks if t["status"] not in ("Done", "Cancelled"))
    if carry == 0 and open_count == 0:
        return "healthy"
    if carry <= 1:
        return "warm"
    if carry <= 3:
        return "hot"
    return "risk"


@weekly_tasks_bp.route("/weekly-tasks")
@login_required
@require_module("WEEKLY_TASKS")
def weekly_tasks_page():
    year, month = current_year_month()
    return render_template("weekly_tasks.html", user=session.get("user"), role=session.get("role"), year=year, month=month)


@weekly_tasks_bp.route("/api/weekly-tasks/users", methods=["GET"])
@login_required
@require_module("WEEKLY_TASKS")
def api_weekly_task_users():
    if not is_admin():
        return jsonify({"ok": True, "data": []})
    conn = db()
    rows = active_users(conn)
    conn.close()
    return jsonify({"ok": True, "data": rows})


@weekly_tasks_bp.route("/api/weekly-tasks", methods=["GET"])
@login_required
@require_module("WEEKLY_TASKS")
def api_weekly_tasks_list():
    year, month = current_year_month()
    year = parse_int(request.args.get("year"), year)
    month = parse_int(request.args.get("month"), month)
    week = parse_int(request.args.get("week"), 0)
    status = clean_text(request.args.get("status"))
    q = clean_text(request.args.get("q"))

    conn = db()
    owner_id = selected_owner_id(conn, request.args.get("owner_user_id"))
    owner = get_user(conn, owner_id) if owner_id else None
    if owner_id:
        auto_process_completed_weeks(conn, owner_id, year, month)
        conn.commit()

    where = [
        "t.deleted_at IS NULL",
        "t.owner_user_id=%s",
        "t.task_year=%s",
        "t.task_month=%s",
    ]
    vals = [owner_id, year, month]

    if week in WEEKS:
        where.append("t.week_number=%s")
        vals.append(week)
    if status and status.upper() != "ALL":
        where.append("t.status=%s")
        vals.append(status)
    if q:
        where.append("t.task_text ILIKE %s")
        vals.append(f"%{q}%")

    rows = conn.execute(f"""
        SELECT t.*, u.username AS owner_username, u.full_name AS owner_full_name,
               COALESCE(er.pending_count, 0) AS pending_edit_count
        FROM weekly_tasks t
        LEFT JOIN users u ON u.id=t.owner_user_id
        LEFT JOIN (
            SELECT task_id, COUNT(*) AS pending_count
            FROM weekly_task_edit_requests
            WHERE request_status='PENDING'
            GROUP BY task_id
        ) er ON er.task_id=t.id
        WHERE {" AND ".join(where)}
        ORDER BY t.week_number ASC, t.id ASC
    """, tuple(vals)).fetchall()

    by_week = {str(w): [] for w in WEEKS}
    for r in rows:
        by_week[str(r["week_number"])].append(task_dict(r))

    weeks = []
    for w in WEEKS:
        tasks = by_week[str(w)]
        lock = get_week_lock(conn, owner_id, year, month, w) if owner_id else None
        weeks.append({
            "week": w,
            "tasks": tasks,
            "health": week_health(tasks),
            "carry_forward_total": sum(int(t["carry_forward_count"] or 0) for t in tasks),
            "is_frozen": 1 if lock and int(lock["is_frozen"] or 0) == 1 else 0,
            "manual_unfrozen": 1 if lock and int(lock["manual_unfrozen"] or 0) == 1 else 0,
            "frozen_at": lock["frozen_at"] if lock else None,
            "frozen_by": lock["frozen_by"] if lock else None,
        })

    mark_weekly_task_notifications_seen(conn)
    conn.commit()
    conn.close()
    return jsonify({
        "ok": True,
        "data": {
            "year": year,
            "month": month,
            "owner": dict(owner) if owner else None,
            "weeks": weeks,
        }
    })


@weekly_tasks_bp.route("/api/weekly-tasks/notification-count", methods=["GET"])
@login_required
@require_module("WEEKLY_TASKS")
def api_weekly_tasks_notification_count():
    conn = db()
    uid = int(current_user_id() or 0)
    last_seen = get_weekly_task_notification_seen_at(conn, uid)

    where = [
        "t.deleted_at IS NULL",
        "t.created_by_user_id<>%s",
    ]
    vals = [uid]
    if last_seen:
        where.append("t.created_at>%s")
        vals.append(last_seen)

    if is_admin():
        where.append("creator.role='EMP'")
    else:
        where.append("t.owner_user_id=%s")
        vals.append(uid)

    row = conn.execute(f"""
        SELECT COUNT(t.id) AS notification_count
        FROM weekly_tasks t
        LEFT JOIN users creator ON creator.id=t.created_by_user_id
        WHERE {" AND ".join(where)}
    """, tuple(vals)).fetchone()
    conn.close()
    return jsonify({"ok": True, "data": {"notification_count": int(row["notification_count"] or 0)}})


@weekly_tasks_bp.route("/api/weekly-tasks", methods=["POST"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_create():
    data = request.json or {}
    text = clean_text(data.get("task_text"))
    year = parse_int(data.get("year"), current_year_month()[0])
    month = parse_int(data.get("month"), current_year_month()[1])
    week = parse_int(data.get("week_number"), None)
    owner_requested = data.get("owner_user_id")

    if not text:
        return jsonify({"ok": False, "error": "Task text is required"}), 400
    if week not in WEEKS:
        return jsonify({"ok": False, "error": "Week must be 1 to 4"}), 400

    conn = db()
    owner_id = selected_owner_id(conn, owner_requested)
    if not owner_id:
        conn.close()
        return jsonify({"ok": False, "error": "Owner user not found"}), 404
    if is_week_frozen(conn, owner_id, year, month, week):
        conn.close()
        return jsonify({"ok": False, "error": "This week is frozen. Ask admin to unfreeze it if changes are needed."}), 403

    created_at = now_iso()
    confirmation = "CONFIRMED" if is_admin() else "PENDING"
    status = "Active"
    row = conn.execute("""
        INSERT INTO weekly_tasks (
            owner_user_id, task_text, task_year, task_month, week_number,
            target_year, target_month, target_week, status, confirmation_status,
            carry_forward_count, created_by_user_id, created_by, created_at,
            confirmed_by, confirmed_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s,%s,%s,%s)
        RETURNING *
    """, (
        owner_id, text, year, month, week, year, month, week,
        status, confirmation, current_user_id(), session["user"], created_at,
        session["user"] if is_admin() else None,
        created_at if is_admin() else None,
    )).fetchone()
    add_history(conn, row["id"], "CREATE", new=dict(row), note="Task created")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": task_dict(row), "message": "Task saved"})


@weekly_tasks_bp.route("/api/weekly-tasks/<int:task_id>/confirm", methods=["POST"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_confirm(task_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403
    conn = db()
    old = get_task(conn, task_id)
    if not old:
        conn.close()
        return jsonify({"ok": False, "error": "Task not found"}), 404
    frozen_response = block_if_week_frozen(conn, old)
    if frozen_response:
        conn.close()
        return frozen_response
    updated_at = now_iso()
    row = conn.execute("""
        UPDATE weekly_tasks
        SET confirmation_status='CONFIRMED',
            status=CASE WHEN status='Pending' THEN 'Active' ELSE status END,
            confirmed_by=%s,
            confirmed_at=%s,
            updated_by=%s,
            updated_at=%s
        WHERE id=%s
        RETURNING *
    """, (session["user"], updated_at, session["user"], updated_at, task_id)).fetchone()
    add_history(conn, task_id, "CONFIRM", old=dict(old), new=dict(row), note="Task confirmed by admin")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": task_dict(row), "message": "Task confirmed"})


@weekly_tasks_bp.route("/api/weekly-tasks/<int:task_id>/status", methods=["POST"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_status(task_id):
    data = request.json or {}
    status = normalize_status(data.get("status"))
    if not status:
        return jsonify({"ok": False, "error": "Invalid task status"}), 400
    if status == "Pending":
        return jsonify({"ok": False, "error": "Pending is only for new unconfirmed tasks"}), 400

    conn = db()
    old = get_task(conn, task_id)
    if not old:
        conn.close()
        return jsonify({"ok": False, "error": "Task not found"}), 404
    if not can_access_task(old):
        conn.close()
        return jsonify({"ok": False, "error": "No access to this task"}), 403
    frozen_response = block_if_week_frozen(conn, old)
    if frozen_response:
        conn.close()
        return frozen_response
    if old["confirmation_status"] != "CONFIRMED":
        conn.close()
        return jsonify({"ok": False, "error": "Task must be confirmed first"}), 400

    updated_at = now_iso()
    sets = ["status=%s", "updated_by=%s", "updated_at=%s"]
    vals = [status, session["user"], updated_at]
    if status == "Started" and old["status"] != "Started":
        sets.extend(["started_by=%s", "started_at=%s"])
        vals.extend([session["user"], updated_at])
    if status == "Done" and old["status"] != "Done":
        sets.extend(["done_by=%s", "done_at=%s"])
        vals.extend([session["user"], updated_at])
    if status == "Cancelled" and old["status"] != "Cancelled":
        sets.extend(["cancelled_by=%s", "cancelled_at=%s"])
        vals.extend([session["user"], updated_at])
    vals.append(task_id)
    row = conn.execute(f"""
        UPDATE weekly_tasks
        SET {", ".join(sets)}
        WHERE id=%s
        RETURNING *
    """, tuple(vals)).fetchone()
    add_history(conn, task_id, "STATUS", old=dict(old), new=dict(row), note=f"Status changed to {status}")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": task_dict(row), "message": "Status updated"})


@weekly_tasks_bp.route("/api/weekly-tasks/<int:task_id>", methods=["PUT"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_update(task_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403
    data = request.json or {}
    text = clean_text(data.get("task_text"))
    status = normalize_status(data.get("status"))
    if not text:
        return jsonify({"ok": False, "error": "Task text is required"}), 400
    if not status:
        return jsonify({"ok": False, "error": "Invalid status"}), 400

    conn = db()
    old = get_task(conn, task_id)
    if not old:
        conn.close()
        return jsonify({"ok": False, "error": "Task not found"}), 404
    frozen_response = block_if_week_frozen(conn, old)
    if frozen_response:
        conn.close()
        return frozen_response
    updated_at = now_iso()
    sets = ["task_text=%s", "status=%s", "updated_by=%s", "updated_at=%s"]
    vals = [text, status, session["user"], updated_at]
    if status == "Started" and old["status"] != "Started":
        sets.extend(["started_by=%s", "started_at=%s"])
        vals.extend([session["user"], updated_at])
    if status == "Done" and old["status"] != "Done":
        sets.extend(["done_by=%s", "done_at=%s"])
        vals.extend([session["user"], updated_at])
    if status == "Cancelled" and old["status"] != "Cancelled":
        sets.extend(["cancelled_by=%s", "cancelled_at=%s"])
        vals.extend([session["user"], updated_at])
    vals.append(task_id)
    row = conn.execute(f"""
        UPDATE weekly_tasks
        SET {", ".join(sets)}
        WHERE id=%s
        RETURNING *
    """, tuple(vals)).fetchone()
    add_history(conn, task_id, "ADMIN_EDIT", old=dict(old), new=dict(row), note="Admin edited live task")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": task_dict(row), "message": "Task updated"})


@weekly_tasks_bp.route("/api/weekly-tasks/<int:task_id>/edit-request", methods=["POST"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_edit_request(task_id):
    data = request.json or {}
    text = clean_text(data.get("requested_text"))
    if not text:
        return jsonify({"ok": False, "error": "Requested task text is required"}), 400

    conn = db()
    task = get_task(conn, task_id)
    if not task:
        conn.close()
        return jsonify({"ok": False, "error": "Task not found"}), 404
    if not can_access_task(task):
        conn.close()
        return jsonify({"ok": False, "error": "No access to this task"}), 403
    frozen_response = block_if_week_frozen(conn, task)
    if frozen_response:
        conn.close()
        return frozen_response
    if task["confirmation_status"] != "CONFIRMED":
        conn.close()
        return jsonify({"ok": False, "error": "Only confirmed tasks can request edits"}), 400

    requested_at = now_iso()
    row = conn.execute("""
        INSERT INTO weekly_task_edit_requests (
            task_id, requested_text, request_status,
            requested_by_user_id, requested_by, requested_at
        )
        VALUES (%s,%s,'PENDING',%s,%s,%s)
        RETURNING *
    """, (task_id, text, current_user_id(), session["user"], requested_at)).fetchone()
    add_history(conn, task_id, "EDIT_REQUEST", old=dict(task), new={"task_text": text}, note="Edit request submitted")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": dict(row), "message": "Edit request submitted"})


def review_edit_request(request_id, approve: bool):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403
    data = request.json or {}
    note = clean_text(data.get("review_note")) or None
    conn = db()
    req = conn.execute("""
        SELECT er.*, t.task_text, t.status, t.task_year, t.task_month, t.week_number
        FROM weekly_task_edit_requests er
        JOIN weekly_tasks t ON t.id=er.task_id
        WHERE er.id=%s
        LIMIT 1
    """, (request_id,)).fetchone()
    if not req:
        conn.close()
        return jsonify({"ok": False, "error": "Edit request not found"}), 404
    if req["request_status"] != "PENDING":
        conn.close()
        return jsonify({"ok": False, "error": "Request already reviewed"}), 400

    reviewed_at = now_iso()
    status = "APPROVED" if approve else "DENIED"
    conn.execute("""
        UPDATE weekly_task_edit_requests
        SET request_status=%s, reviewed_by=%s, reviewed_at=%s, review_note=%s
        WHERE id=%s
    """, (status, session["user"], reviewed_at, note, request_id))

    if approve:
        old_task = get_task(conn, req["task_id"])
        row = conn.execute("""
            UPDATE weekly_tasks
            SET task_text=%s, updated_by=%s, updated_at=%s
            WHERE id=%s
            RETURNING *
        """, (req["requested_text"], session["user"], reviewed_at, req["task_id"])).fetchone()
        add_history(conn, req["task_id"], "EDIT_APPROVED", old=dict(old_task), new=dict(row), note=note or "Edit request approved")
    else:
        add_history(conn, req["task_id"], "EDIT_DENIED", old=dict(req), new=dict(req), note=note or "Edit request denied")

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": f"Edit request {status.lower()}"})


@weekly_tasks_bp.route("/api/weekly-tasks/edit-requests/<int:request_id>/approve", methods=["POST"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_edit_request_approve(request_id):
    return review_edit_request(request_id, True)


@weekly_tasks_bp.route("/api/weekly-tasks/edit-requests/<int:request_id>/deny", methods=["POST"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_edit_request_deny(request_id):
    return review_edit_request(request_id, False)


@weekly_tasks_bp.route("/api/weekly-tasks/<int:task_id>/carry-forward", methods=["POST"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_carry_forward(task_id):
    conn = db()
    old = get_task(conn, task_id)
    if not old:
        conn.close()
        return jsonify({"ok": False, "error": "Task not found"}), 404
    if not can_access_task(old):
        conn.close()
        return jsonify({"ok": False, "error": "No access to this task"}), 403
    frozen_response = block_if_week_frozen(conn, old)
    if frozen_response:
        conn.close()
        return frozen_response
    if old["confirmation_status"] != "CONFIRMED":
        conn.close()
        return jsonify({"ok": False, "error": "Task must be confirmed before moving to next week"}), 400
    if old["status"] in ("Done", "Cancelled"):
        conn.close()
        return jsonify({"ok": False, "error": "Done or cancelled tasks cannot be carried forward"}), 400

    ny, nm, nw = next_week(int(old["task_year"]), int(old["task_month"]), int(old["week_number"]))
    created_at = now_iso()
    new_carry = int(old["carry_forward_count"] or 0) + 1
    old_after = conn.execute("""
        UPDATE weekly_tasks
        SET status='Cancelled',
            updated_by=%s,
            updated_at=%s,
            moved_by=%s,
            moved_at=%s,
            cancelled_by=%s,
            cancelled_at=%s
        WHERE id=%s
        RETURNING *
    """, (session["user"], created_at, session["user"], created_at, session["user"], created_at, task_id)).fetchone()
    new_row = conn.execute("""
        INSERT INTO weekly_tasks (
            owner_user_id, task_text, task_year, task_month, week_number,
            target_year, target_month, target_week, status, confirmation_status,
            carry_forward_count, source_task_id, created_by_user_id, created_by, created_at,
            confirmed_by, confirmed_at, started_by, started_at, moved_by, moved_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'CONFIRMED',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING *
    """, (
        old["owner_user_id"], old["task_text"], ny, nm, nw, ny, nm, nw,
        old["status"], new_carry, task_id, current_user_id(), session["user"], created_at,
        session["user"], created_at,
        old["started_by"] if old["status"] == "Started" else None,
        old["started_at"] if old["status"] == "Started" else None,
        session["user"], created_at,
    )).fetchone()
    add_history(conn, task_id, "CARRY_FORWARD_FROM", old=dict(old), new=dict(old_after), note=f"Moved to {ny}-{nm:02d} week {nw}")
    add_history(conn, new_row["id"], "CARRY_FORWARD_TO", old=dict(old), new=dict(new_row), note=f"Moved from task #{task_id}")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": task_dict(new_row), "message": "Task moved to next week"})


@weekly_tasks_bp.route("/api/weekly-tasks/<int:task_id>", methods=["DELETE"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_delete(task_id):
    conn = db()
    old = get_task(conn, task_id)
    if not old:
        conn.close()
        return jsonify({"ok": False, "error": "Task not found"}), 404
    if not can_modify_task(old):
        conn.close()
        return jsonify({"ok": False, "error": "No access to delete this task"}), 403
    frozen_response = block_if_week_frozen(conn, old)
    if frozen_response:
        conn.close()
        return frozen_response
    deleted_at = now_iso()
    row = conn.execute("""
        UPDATE weekly_tasks
        SET deleted_at=%s, deleted_by=%s, deleted_by_user_id=%s, updated_by=%s, updated_at=%s
        WHERE id=%s
        RETURNING *
    """, (deleted_at, session["user"], current_user_id(), session["user"], deleted_at, task_id)).fetchone()
    add_history(conn, task_id, "DELETE", old=dict(old), new=dict(row), note="Task moved to trash")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Task moved to trash"})


@weekly_tasks_bp.route("/api/weekly-tasks/review", methods=["GET"])
@login_required
@require_module("WEEKLY_TASKS")
def api_weekly_tasks_review():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403
    conn = db()
    pending_tasks = conn.execute("""
        SELECT t.*, u.username AS owner_username, u.full_name AS owner_full_name,
               0 AS pending_edit_count
        FROM weekly_tasks t
        LEFT JOIN users u ON u.id=t.owner_user_id
        WHERE t.deleted_at IS NULL AND t.confirmation_status='PENDING'
        ORDER BY t.created_at ASC, t.id ASC
        LIMIT 500
    """).fetchall()
    edit_requests = conn.execute("""
        SELECT er.*, t.task_text, t.owner_user_id, u.username AS owner_username, u.full_name AS owner_full_name
        FROM weekly_task_edit_requests er
        JOIN weekly_tasks t ON t.id=er.task_id
        LEFT JOIN users u ON u.id=t.owner_user_id
        WHERE er.request_status='PENDING' AND t.deleted_at IS NULL
        ORDER BY er.requested_at ASC, er.id ASC
        LIMIT 500
    """).fetchall()
    conn.close()
    return jsonify({
        "ok": True,
        "data": {
            "pending_tasks": [task_dict(r) for r in pending_tasks],
            "edit_requests": [dict(r) for r in edit_requests],
        }
    })


@weekly_tasks_bp.route("/api/weekly-tasks/week-locks/unfreeze", methods=["POST"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_unfreeze():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403
    data = request.json or {}
    owner_id = parse_int(data.get("owner_user_id"), None)
    year = parse_int(data.get("year"), None)
    month = parse_int(data.get("month"), None)
    week = parse_int(data.get("week"), None)
    if not owner_id or not year or not month or week not in WEEKS:
        return jsonify({"ok": False, "error": "Owner, year, month, and week are required"}), 400

    unfrozen_at = now_iso()
    conn = db()
    conn.execute("""
        INSERT INTO weekly_task_week_locks (
            owner_user_id, task_year, task_month, week_number,
            is_frozen, manual_unfrozen, unfrozen_at, unfrozen_by
        )
        VALUES (%s,%s,%s,%s,0,1,%s,%s)
        ON CONFLICT(owner_user_id, task_year, task_month, week_number)
        DO UPDATE SET
            is_frozen=0,
            manual_unfrozen=1,
            unfrozen_at=excluded.unfrozen_at,
            unfrozen_by=excluded.unfrozen_by
    """, (owner_id, year, month, week, unfrozen_at, session["user"]))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": f"Week {week} unfrozen"})


@weekly_tasks_bp.route("/api/weekly-tasks/trash", methods=["GET"])
@login_required
@require_module("WEEKLY_TASKS")
def api_weekly_tasks_trash():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403
    conn = db()
    rows = conn.execute("""
        SELECT t.*, u.username AS owner_username, u.full_name AS owner_full_name,
               0 AS pending_edit_count
        FROM weekly_tasks t
        LEFT JOIN users u ON u.id=t.owner_user_id
        WHERE t.deleted_at IS NOT NULL
        ORDER BY t.deleted_at DESC NULLS LAST, t.id DESC
        LIMIT 500
    """).fetchall()
    conn.close()
    return jsonify({"ok": True, "data": [task_dict(r) for r in rows]})


@weekly_tasks_bp.route("/api/weekly-tasks/trash/<int:task_id>/recover", methods=["POST"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_recover(task_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403
    conn = db()
    old = get_task(conn, task_id, include_deleted=True)
    if not old or not old["deleted_at"]:
        conn.close()
        return jsonify({"ok": False, "error": "Trash task not found"}), 404
    updated_at = now_iso()
    row = conn.execute("""
        UPDATE weekly_tasks
        SET deleted_at=NULL, deleted_by=NULL, deleted_by_user_id=NULL, updated_by=%s, updated_at=%s
        WHERE id=%s
        RETURNING *
    """, (session["user"], updated_at, task_id)).fetchone()
    add_history(conn, task_id, "RECOVER", old=dict(old), new=dict(row), note="Task recovered from trash")
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Task recovered"})


@weekly_tasks_bp.route("/api/weekly-tasks/trash/<int:task_id>/permanent", methods=["DELETE"])
@login_required
@require_module("WEEKLY_TASKS", need_edit=True)
def api_weekly_tasks_permanent(task_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403
    conn = db()
    row = get_task(conn, task_id, include_deleted=True)
    if not row or not row["deleted_at"]:
        conn.close()
        return jsonify({"ok": False, "error": "Trash task not found"}), 404
    conn.execute("DELETE FROM weekly_task_edit_requests WHERE task_id=%s", (task_id,))
    conn.execute("DELETE FROM weekly_task_history WHERE task_id=%s", (task_id,))
    conn.execute("DELETE FROM weekly_tasks WHERE id=%s", (task_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Task permanently deleted"})
