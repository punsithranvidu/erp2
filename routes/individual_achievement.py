from datetime import datetime
from functools import wraps

from flask import Blueprint, current_app, jsonify, render_template, request, session

from .db import connect, get_table_columns, placeholders

individual_achievement_bp = Blueprint("individual_achievement", __name__)

VISIBILITY_OPTIONS = ("EVERYONE", "ADMINS", "ONLY_ME")


def db():
    return connect(current_app.config["DATABASE_URL"])


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


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
                        "error": f"No permission for {module}{' (edit)' if need_edit else ''}",
                    }), 403
                return current_app.config["REQUIRE_MODULE_FUNC"](module, need_edit)
            return f(*args, **kwargs)

        return wrapped

    return deco


def clean_text(value):
    if value is None:
        return ""
    return str(value).strip()


def normalize_visibility(value):
    visibility = clean_text(value).upper() or "EVERYONE"
    return visibility if visibility in VISIBILITY_OPTIONS else None


def visibility_label(value):
    mapping = {
        "EVERYONE": "Share with Everyone",
        "ADMINS": "Admins Only",
        "ONLY_ME": "Only Me",
    }
    return mapping.get(value or "EVERYONE", "Share with Everyone")


def validate_text(value, label, max_length=4000):
    text = clean_text(value)
    if not text:
        return None, f"{label} is required"
    if len(text) > max_length:
        return None, f"{label} must be {max_length} characters or less"
    return text, None


def parse_int(value):
    raw = clean_text(value)
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def parse_id_list(value):
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = str(value).split(",")

    clean_ids = []
    seen = set()
    for raw in raw_items:
        parsed = parse_int(raw)
        if not parsed or parsed in seen:
            continue
        seen.add(parsed)
        clean_ids.append(parsed)
    return clean_ids


def ensure_individual_achievement_tables():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS individual_achievements (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            time_period TEXT NOT NULL,
            achieved_work TEXT NOT NULL,
            time_spent TEXT NOT NULL,
            results_got TEXT NOT NULL,
            visibility TEXT NOT NULL DEFAULT 'EVERYONE',
            created_by TEXT NOT NULL,
            created_by_user_id BIGINT,
            created_at TEXT NOT NULL,
            updated_at TEXT,
            updated_by TEXT,
            updated_by_user_id BIGINT,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            deleted_at TEXT,
            deleted_by TEXT,
            deleted_by_user_id BIGINT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    cols = get_table_columns(conn, "individual_achievements")
    missing = {
        "user_id": "ALTER TABLE individual_achievements ADD COLUMN user_id BIGINT",
        "time_period": "ALTER TABLE individual_achievements ADD COLUMN time_period TEXT NOT NULL DEFAULT ''",
        "achieved_work": "ALTER TABLE individual_achievements ADD COLUMN achieved_work TEXT NOT NULL DEFAULT ''",
        "time_spent": "ALTER TABLE individual_achievements ADD COLUMN time_spent TEXT NOT NULL DEFAULT ''",
        "results_got": "ALTER TABLE individual_achievements ADD COLUMN results_got TEXT NOT NULL DEFAULT ''",
        "visibility": "ALTER TABLE individual_achievements ADD COLUMN visibility TEXT NOT NULL DEFAULT 'EVERYONE'",
        "created_by": "ALTER TABLE individual_achievements ADD COLUMN created_by TEXT NOT NULL DEFAULT ''",
        "created_by_user_id": "ALTER TABLE individual_achievements ADD COLUMN created_by_user_id BIGINT",
        "created_at": "ALTER TABLE individual_achievements ADD COLUMN created_at TEXT NOT NULL DEFAULT ''",
        "updated_at": "ALTER TABLE individual_achievements ADD COLUMN updated_at TEXT",
        "updated_by": "ALTER TABLE individual_achievements ADD COLUMN updated_by TEXT",
        "updated_by_user_id": "ALTER TABLE individual_achievements ADD COLUMN updated_by_user_id BIGINT",
        "is_deleted": "ALTER TABLE individual_achievements ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0",
        "deleted_at": "ALTER TABLE individual_achievements ADD COLUMN deleted_at TEXT",
        "deleted_by": "ALTER TABLE individual_achievements ADD COLUMN deleted_by TEXT",
        "deleted_by_user_id": "ALTER TABLE individual_achievements ADD COLUMN deleted_by_user_id BIGINT",
    }
    for col, sql in missing.items():
        if col not in cols:
            cur.execute(sql)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS individual_achievement_contributors (
            id BIGSERIAL PRIMARY KEY,
            achievement_id BIGINT NOT NULL REFERENCES individual_achievements(id) ON DELETE CASCADE,
            user_id BIGINT NOT NULL REFERENCES users(id),
            added_by TEXT,
            added_by_user_id BIGINT,
            added_at TEXT NOT NULL,
            UNIQUE (achievement_id, user_id)
        )
    """)

    contributor_cols = get_table_columns(conn, "individual_achievement_contributors")
    contributor_missing = {
        "achievement_id": "ALTER TABLE individual_achievement_contributors ADD COLUMN achievement_id BIGINT",
        "user_id": "ALTER TABLE individual_achievement_contributors ADD COLUMN user_id BIGINT",
        "added_by": "ALTER TABLE individual_achievement_contributors ADD COLUMN added_by TEXT",
        "added_by_user_id": "ALTER TABLE individual_achievement_contributors ADD COLUMN added_by_user_id BIGINT",
        "added_at": "ALTER TABLE individual_achievement_contributors ADD COLUMN added_at TEXT NOT NULL DEFAULT ''",
    }
    for col, sql in contributor_missing.items():
        if col not in contributor_cols:
            cur.execute(sql)

    cur.execute("""
        UPDATE individual_achievements
        SET visibility='EVERYONE'
        WHERE visibility IS NULL
           OR TRIM(visibility)=''
           OR UPPER(TRIM(visibility)) NOT IN ('EVERYONE', 'ADMINS', 'ONLY_ME')
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_individual_achievements_owner
        ON individual_achievements (user_id)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_individual_achievements_visibility
        ON individual_achievements (visibility)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_individual_achievements_deleted
        ON individual_achievements (is_deleted)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_individual_achievements_created_at
        ON individual_achievements (created_at)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_individual_achievement_contributors_achievement
        ON individual_achievement_contributors (achievement_id)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_individual_achievement_contributors_user
        ON individual_achievement_contributors (user_id)
    """)

    conn.commit()
    conn.close()


@individual_achievement_bp.before_app_request
def individual_achievement_tables_ready():
    if current_app.config.get("INDIVIDUAL_ACHIEVEMENT_TABLES_READY"):
        return
    ensure_individual_achievement_tables()
    current_app.config["INDIVIDUAL_ACHIEVEMENT_TABLES_READY"] = True


def get_current_user_row(conn):
    uid = current_user_id()
    if uid:
        row = conn.execute("""
            SELECT id, username, role, full_name
            FROM users
            WHERE id=%s
            LIMIT 1
        """, (uid,)).fetchone()
        if row:
            return row

    return conn.execute("""
        SELECT id, username, role, full_name
        FROM users
        WHERE username=%s
        LIMIT 1
    """, (session.get("user"),)).fetchone()


def get_user_row(conn, user_id):
    return conn.execute("""
        SELECT id, username, role, full_name
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (user_id,)).fetchone()


def get_active_users(conn):
    rows = conn.execute("""
        SELECT id, username, role, full_name
        FROM users
        WHERE active=1
        ORDER BY COALESCE(NULLIF(TRIM(full_name), ''), username) ASC
    """).fetchall()
    return [dict(r) for r in rows]


def can_manage_achievement(row):
    if not row:
        return False
    if is_admin():
        return True
    return int(row["user_id"] or 0) == int(current_user_id() or 0)


def normalize_contributor_ids(conn, contributor_ids, owner_user_id):
    clean_ids = []
    seen = set()
    owner_id = int(owner_user_id or 0)

    for contributor_id in contributor_ids:
        user_id = parse_int(contributor_id)
        if not user_id or user_id == owner_id or user_id in seen:
            continue
        seen.add(user_id)
        clean_ids.append(user_id)

    if not clean_ids:
        return [], None

    rows = conn.execute(f"""
        SELECT id
        FROM users
        WHERE active=1
          AND id IN ({placeholders(len(clean_ids))})
    """, tuple(clean_ids)).fetchall()
    valid_ids = {int(r["id"]) for r in rows}

    if len(valid_ids) != len(clean_ids):
        return None, "One or more contributors are invalid"

    return clean_ids, None


def replace_contributors(conn, achievement_id, contributor_ids):
    conn.execute(
        "DELETE FROM individual_achievement_contributors WHERE achievement_id=%s",
        (achievement_id,),
    )

    if not contributor_ids:
        return

    added_at = now_iso()
    for user_id in contributor_ids:
        conn.execute("""
            INSERT INTO individual_achievement_contributors (
                achievement_id,
                user_id,
                added_by,
                added_by_user_id,
                added_at
            )
            VALUES (%s,%s,%s,%s,%s)
        """, (
            achievement_id,
            user_id,
            session.get("user"),
            current_user_id(),
            added_at,
        ))


def build_search_sql(q, vals):
    if not q:
        return ""

    like = f"%{q}%"
    vals.extend([like, like, like, like, like, like, like, like])
    return """
        AND (
            COALESCE(ia.time_period, '') ILIKE %s
            OR COALESCE(ia.achieved_work, '') ILIKE %s
            OR COALESCE(ia.time_spent, '') ILIKE %s
            OR COALESCE(ia.results_got, '') ILIKE %s
            OR COALESCE(u.username, '') ILIKE %s
            OR COALESCE(u.full_name, '') ILIKE %s
            OR EXISTS (
                SELECT 1
                FROM individual_achievement_contributors iac_search
                LEFT JOIN users cu_search ON cu_search.id = iac_search.user_id
                WHERE iac_search.achievement_id = ia.id
                  AND (
                    COALESCE(cu_search.username, '') ILIKE %s
                    OR COALESCE(cu_search.full_name, '') ILIKE %s
                  )
            )
        )
    """


def get_achievement(conn, achievement_id, include_deleted=False):
    where = "ia.id=%s"
    if not include_deleted:
        where += " AND COALESCE(ia.is_deleted, 0)=0"

    return conn.execute(f"""
        SELECT
            ia.*,
            u.username AS owner_username,
            u.full_name AS owner_full_name,
            u.role AS owner_role
        FROM individual_achievements ia
        LEFT JOIN users u ON u.id=ia.user_id
        WHERE {where}
        LIMIT 1
    """, (achievement_id,)).fetchone()


def get_contributors_map(conn, achievement_ids):
    clean_ids = [int(x) for x in achievement_ids if parse_int(x)]
    if not clean_ids:
        return {}

    rows = conn.execute(f"""
        SELECT
            iac.achievement_id,
            u.id AS user_id,
            u.username,
            u.full_name,
            u.role
        FROM individual_achievement_contributors iac
        LEFT JOIN users u ON u.id = iac.user_id
        WHERE iac.achievement_id IN ({placeholders(len(clean_ids))})
        ORDER BY COALESCE(NULLIF(TRIM(u.full_name), ''), u.username) ASC, u.id ASC
    """, tuple(clean_ids)).fetchall()

    mapping = {}
    for row in rows:
        achievement_id = int(row["achievement_id"])
        mapping.setdefault(achievement_id, []).append({
            "id": row["user_id"],
            "username": row["username"] or "",
            "full_name": row["full_name"] or "",
            "role": row["role"] or "",
            "display_name": clean_text(row["full_name"]) or clean_text(row["username"]) or f"User {row['user_id']}",
        })
    return mapping


def achievement_to_dict(row, contributors=None):
    contributor_rows = contributors or []
    contributor_ids = [int(item["id"]) for item in contributor_rows if item.get("id")]
    owner_name = clean_text(row.get("owner_full_name")) or clean_text(row.get("owner_username")) or clean_text(row.get("created_by"))
    visibility = row.get("visibility") or "EVERYONE"
    edited_at = row.get("updated_at") or row.get("created_at") or ""
    edited_by = row.get("updated_by") or row.get("created_by") or ""
    viewer_id = int(current_user_id() or 0)

    return {
        "id": row.get("id"),
        "user_id": row.get("user_id"),
        "owner_username": row.get("owner_username") or "",
        "owner_full_name": row.get("owner_full_name") or "",
        "owner_name": owner_name,
        "time_period": row.get("time_period") or "",
        "achieved_work": row.get("achieved_work") or "",
        "time_spent": row.get("time_spent") or "",
        "results_got": row.get("results_got") or "",
        "visibility": visibility,
        "visibility_label": visibility_label(visibility),
        "created_by": row.get("created_by") or "",
        "created_by_user_id": row.get("created_by_user_id"),
        "created_at": row.get("created_at") or "",
        "updated_at": row.get("updated_at") or "",
        "updated_by": row.get("updated_by") or "",
        "updated_by_user_id": row.get("updated_by_user_id"),
        "deleted_at": row.get("deleted_at") or "",
        "deleted_by": row.get("deleted_by") or "",
        "deleted_by_user_id": row.get("deleted_by_user_id"),
        "edited_at": edited_at,
        "edited_by": edited_by,
        "contributors": contributor_rows,
        "contributor_ids": contributor_ids,
        "contributor_count": len(contributor_rows),
        "can_edit": 1 if can_manage_achievement(row) else 0,
        "can_delete": 1 if can_manage_achievement(row) else 0,
        "is_owner": 1 if int(row.get("user_id") or 0) == viewer_id else 0,
        "is_contributor": 1 if viewer_id in contributor_ids else 0,
    }


def fetch_achievement_rows(conn, where_sql, vals, q):
    params = list(vals)
    search_sql = build_search_sql(q, params)

    rows = conn.execute(f"""
        SELECT
            ia.*,
            u.username AS owner_username,
            u.full_name AS owner_full_name,
            u.role AS owner_role
        FROM individual_achievements ia
        LEFT JOIN users u ON u.id=ia.user_id
        WHERE COALESCE(ia.is_deleted, 0)=0
          {where_sql}
          {search_sql}
        ORDER BY COALESCE(ia.updated_at, ia.created_at) DESC, ia.id DESC
    """, tuple(params)).fetchall()

    row_dicts = [dict(r) for r in rows]
    contributor_map = get_contributors_map(conn, [row["id"] for row in row_dicts])
    return [achievement_to_dict(row, contributor_map.get(int(row["id"]), [])) for row in row_dicts]


def fetch_deleted_achievement_rows(conn):
    rows = conn.execute("""
        SELECT
            ia.*,
            u.username AS owner_username,
            u.full_name AS owner_full_name,
            u.role AS owner_role
        FROM individual_achievements ia
        LEFT JOIN users u ON u.id=ia.user_id
        WHERE COALESCE(ia.is_deleted, 0)=1
        ORDER BY COALESCE(ia.deleted_at, ia.updated_at, ia.created_at) DESC, ia.id DESC
        LIMIT 1000
    """).fetchall()

    row_dicts = [dict(r) for r in rows]
    contributor_map = get_contributors_map(conn, [row["id"] for row in row_dicts])
    return [achievement_to_dict(row, contributor_map.get(int(row["id"]), [])) for row in row_dicts]


def build_access_scope_meta(selected_user_row=None):
    if is_admin():
        if selected_user_row:
            selected_name = clean_text(selected_user_row.get("full_name")) or clean_text(selected_user_row.get("username"))
            return {
                "scope_text": f"Showing achievements visible to {selected_name}. Admins can clear the filter to see every record.",
                "table_title": f"{selected_name} View",
                "selected_user_id": selected_user_row.get("id"),
                "selected_user_name": selected_name,
            }
        return {
            "scope_text": "Admin view: all achievement records, including private and admin-only items.",
            "table_title": "All Achievements",
            "selected_user_id": None,
            "selected_user_name": "",
        }

    return {
        "scope_text": "Employee view: your own achievements plus records shared to everyone or linked to you as a contributor.",
        "table_title": "Achievements",
        "selected_user_id": None,
        "selected_user_name": "",
    }


@individual_achievement_bp.route("/individual-achievement")
@login_required
@require_module("INDIVIDUAL_ACHIEVEMENT")
def individual_achievement_page():
    return render_template(
        "individual_achievement.html",
        user=session.get("user"),
        role=session.get("role"),
    )


@individual_achievement_bp.route("/api/individual-achievement/users", methods=["GET"])
@login_required
@require_module("INDIVIDUAL_ACHIEVEMENT")
def api_individual_achievement_users():
    conn = db()
    rows = get_active_users(conn)
    conn.close()
    return jsonify({"ok": True, "data": rows})


@individual_achievement_bp.route("/api/individual-achievement/list", methods=["GET"])
@login_required
@require_module("INDIVIDUAL_ACHIEVEMENT")
def api_individual_achievement_list():
    uid = current_user_id()
    if not uid:
        return jsonify({"ok": False, "error": "Login required"}), 401

    q = clean_text(request.args.get("q"))
    requested_user_id = clean_text(request.args.get("user_id"))
    selected_user_id = parse_int(requested_user_id) if requested_user_id else None
    if requested_user_id and selected_user_id is None:
        return jsonify({"ok": False, "error": "Invalid user filter"}), 400

    conn = db()
    selected_user_row = None

    if is_admin():
        if selected_user_id is not None:
            selected_user_row = get_user_row(conn, selected_user_id)
            if not selected_user_row:
                conn.close()
                return jsonify({"ok": False, "error": "Selected user not found"}), 404

            selected_role = clean_text(selected_user_row.get("role")).upper()
            rows = fetch_achievement_rows(
                conn,
                """
                    AND (
                        ia.user_id=%s
                        OR EXISTS (
                            SELECT 1
                            FROM individual_achievement_contributors iac_scope
                            WHERE iac_scope.achievement_id = ia.id
                              AND iac_scope.user_id = %s
                        )
                        OR COALESCE(ia.visibility, 'EVERYONE')='EVERYONE'
                        OR (%s='ADMIN' AND COALESCE(ia.visibility, 'EVERYONE')='ADMINS')
                    )
                """,
                [selected_user_id, selected_user_id, selected_role],
                q,
            )
        else:
            rows = fetch_achievement_rows(conn, "", [], q)
    else:
        if selected_user_id is not None and int(selected_user_id) != int(uid):
            conn.close()
            return jsonify({"ok": False, "error": "User filter is available only for admins"}), 403

        rows = fetch_achievement_rows(
            conn,
            """
                AND (
                    ia.user_id=%s
                    OR EXISTS (
                        SELECT 1
                        FROM individual_achievement_contributors iac_scope
                        WHERE iac_scope.achievement_id = ia.id
                          AND iac_scope.user_id = %s
                    )
                    OR COALESCE(ia.visibility, 'EVERYONE')='EVERYONE'
                )
            """,
            [uid, uid],
            q,
        )

    meta = build_access_scope_meta(dict(selected_user_row) if selected_user_row else None)
    conn.close()
    return jsonify({"ok": True, "data": rows, "meta": meta})


@individual_achievement_bp.route("/api/individual-achievement/my", methods=["GET"])
@login_required
@require_module("INDIVIDUAL_ACHIEVEMENT")
def api_individual_achievement_my():
    uid = current_user_id()
    if not uid:
        return jsonify({"ok": False, "error": "Login required"}), 401

    q = clean_text(request.args.get("q"))
    conn = db()
    rows = fetch_achievement_rows(conn, "AND ia.user_id=%s", [uid], q)
    conn.close()
    return jsonify({"ok": True, "data": rows})


@individual_achievement_bp.route("/api/individual-achievement/visible", methods=["GET"])
@login_required
@require_module("INDIVIDUAL_ACHIEVEMENT")
def api_individual_achievement_visible():
    uid = current_user_id()
    if not uid:
        return jsonify({"ok": False, "error": "Login required"}), 401

    q = clean_text(request.args.get("q"))
    requested_user_id = clean_text(request.args.get("user_id"))
    user_id = parse_int(requested_user_id) if requested_user_id else None
    if requested_user_id and user_id is None:
        return jsonify({"ok": False, "error": "Invalid user filter"}), 400

    where_sql = """
        AND (
            ia.user_id=%s
            OR EXISTS (
                SELECT 1
                FROM individual_achievement_contributors iac_scope
                WHERE iac_scope.achievement_id = ia.id
                  AND iac_scope.user_id = %s
            )
            OR COALESCE(ia.visibility, 'EVERYONE')='EVERYONE'
        )
    """
    vals = [uid, uid]

    if is_admin():
        where_sql = ""
        vals = []

    if user_id is not None:
        where_sql += """
            AND (
                ia.user_id=%s
                OR EXISTS (
                    SELECT 1
                    FROM individual_achievement_contributors iac_filter
                    WHERE iac_filter.achievement_id = ia.id
                      AND iac_filter.user_id = %s
                )
            )
        """
        vals.extend([user_id, user_id])

    conn = db()
    rows = fetch_achievement_rows(conn, where_sql, vals, q)
    conn.close()
    return jsonify({"ok": True, "data": rows})


@individual_achievement_bp.route("/api/individual-achievement", methods=["POST"])
@login_required
@require_module("INDIVIDUAL_ACHIEVEMENT", need_edit=True)
def api_individual_achievement_create():
    data = request.json or {}
    time_period, error = validate_text(data.get("time_period"), "Time period", 300)
    if error:
        return jsonify({"ok": False, "error": error}), 400

    achieved_work, error = validate_text(data.get("achieved_work"), "Achieved work", 5000)
    if error:
        return jsonify({"ok": False, "error": error}), 400

    time_spent, error = validate_text(data.get("time_spent"), "Time spent", 300)
    if error:
        return jsonify({"ok": False, "error": error}), 400

    results_got, error = validate_text(data.get("results_got"), "Results got", 5000)
    if error:
        return jsonify({"ok": False, "error": error}), 400

    visibility = normalize_visibility(data.get("visibility"))
    if not visibility:
        return jsonify({"ok": False, "error": "Invalid visibility"}), 400

    uid = current_user_id()
    created_at = now_iso()
    conn = db()

    contributor_ids, contributor_error = normalize_contributor_ids(
        conn,
        parse_id_list(data.get("contributor_ids")),
        uid,
    )
    if contributor_error:
        conn.close()
        return jsonify({"ok": False, "error": contributor_error}), 400

    row = conn.execute("""
        INSERT INTO individual_achievements (
            user_id,
            time_period,
            achieved_work,
            time_spent,
            results_got,
            visibility,
            created_by,
            created_by_user_id,
            created_at,
            updated_at,
            updated_by,
            updated_by_user_id
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING *
    """, (
        uid,
        time_period,
        achieved_work,
        time_spent,
        results_got,
        visibility,
        session.get("user"),
        uid,
        created_at,
        created_at,
        session.get("user"),
        uid,
    )).fetchone()

    replace_contributors(conn, row["id"], contributor_ids)
    full_row = get_achievement(conn, row["id"])
    contributor_map = get_contributors_map(conn, [row["id"]])
    conn.commit()
    conn.close()
    return jsonify({
        "ok": True,
        "data": achievement_to_dict(dict(full_row), contributor_map.get(int(row["id"]), [])),
        "message": "Achievement added successfully",
    })


@individual_achievement_bp.route("/api/individual-achievement/<int:achievement_id>", methods=["PUT"])
@login_required
@require_module("INDIVIDUAL_ACHIEVEMENT", need_edit=True)
def api_individual_achievement_update(achievement_id):
    data = request.json or {}
    time_period, error = validate_text(data.get("time_period"), "Time period", 300)
    if error:
        return jsonify({"ok": False, "error": error}), 400

    achieved_work, error = validate_text(data.get("achieved_work"), "Achieved work", 5000)
    if error:
        return jsonify({"ok": False, "error": error}), 400

    time_spent, error = validate_text(data.get("time_spent"), "Time spent", 300)
    if error:
        return jsonify({"ok": False, "error": error}), 400

    results_got, error = validate_text(data.get("results_got"), "Results got", 5000)
    if error:
        return jsonify({"ok": False, "error": error}), 400

    visibility = normalize_visibility(data.get("visibility"))
    if not visibility:
        return jsonify({"ok": False, "error": "Invalid visibility"}), 400

    conn = db()
    row = get_achievement(conn, achievement_id)
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Achievement not found"}), 404
    if not can_manage_achievement(row):
        conn.close()
        return jsonify({"ok": False, "error": "Only the owner or an admin can edit this achievement"}), 403

    contributor_ids, contributor_error = normalize_contributor_ids(
        conn,
        parse_id_list(data.get("contributor_ids")),
        row["user_id"],
    )
    if contributor_error:
        conn.close()
        return jsonify({"ok": False, "error": contributor_error}), 400

    updated_at = now_iso()
    conn.execute("""
        UPDATE individual_achievements
        SET time_period=%s,
            achieved_work=%s,
            time_spent=%s,
            results_got=%s,
            visibility=%s,
            updated_at=%s,
            updated_by=%s,
            updated_by_user_id=%s
        WHERE id=%s
    """, (
        time_period,
        achieved_work,
        time_spent,
        results_got,
        visibility,
        updated_at,
        session.get("user"),
        current_user_id(),
        achievement_id,
    ))

    replace_contributors(conn, achievement_id, contributor_ids)
    full_row = get_achievement(conn, achievement_id)
    contributor_map = get_contributors_map(conn, [achievement_id])
    conn.commit()
    conn.close()
    return jsonify({
        "ok": True,
        "data": achievement_to_dict(dict(full_row), contributor_map.get(int(achievement_id), [])),
        "message": "Achievement updated successfully",
    })


@individual_achievement_bp.route("/api/individual-achievement/<int:achievement_id>", methods=["DELETE"])
@login_required
@require_module("INDIVIDUAL_ACHIEVEMENT", need_edit=True)
def api_individual_achievement_delete(achievement_id):
    conn = db()
    row = get_achievement(conn, achievement_id)
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Achievement not found"}), 404
    if not can_manage_achievement(row):
        conn.close()
        return jsonify({"ok": False, "error": "Only the owner or an admin can delete this achievement"}), 403

    deleted_at = now_iso()
    conn.execute("""
        UPDATE individual_achievements
        SET is_deleted=1,
            deleted_at=%s,
            deleted_by=%s,
            deleted_by_user_id=%s,
            updated_at=%s,
            updated_by=%s,
            updated_by_user_id=%s
        WHERE id=%s
    """, (
        deleted_at,
        session.get("user"),
        current_user_id(),
        deleted_at,
        session.get("user"),
        current_user_id(),
        achievement_id,
    ))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "Achievement moved to trash successfully"})


@individual_achievement_bp.route("/api/individual-achievement/trash", methods=["GET"])
@login_required
@require_module("INDIVIDUAL_ACHIEVEMENT")
def api_individual_achievement_trash():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    rows = fetch_deleted_achievement_rows(conn)
    conn.close()
    return jsonify({"ok": True, "data": rows})


@individual_achievement_bp.route("/api/individual-achievement/trash/<int:achievement_id>/recover", methods=["POST"])
@login_required
@require_module("INDIVIDUAL_ACHIEVEMENT", need_edit=True)
def api_individual_achievement_trash_recover(achievement_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    restored_at = now_iso()
    conn = db()
    row = conn.execute("""
        UPDATE individual_achievements
        SET is_deleted=0,
            deleted_at=NULL,
            deleted_by=NULL,
            deleted_by_user_id=NULL,
            updated_at=%s,
            updated_by=%s,
            updated_by_user_id=%s
        WHERE id=%s
          AND COALESCE(is_deleted, 0)=1
        RETURNING id
    """, (
        restored_at,
        session.get("user"),
        current_user_id(),
        achievement_id,
    )).fetchone()

    conn.commit()
    conn.close()

    if not row:
        return jsonify({"ok": False, "error": "Trash achievement not found"}), 404
    return jsonify({"ok": True, "message": "Achievement restored successfully"})


@individual_achievement_bp.route("/api/individual-achievement/trash/<int:achievement_id>/permanent", methods=["DELETE"])
@login_required
@require_module("INDIVIDUAL_ACHIEVEMENT", need_edit=True)
def api_individual_achievement_trash_permanent(achievement_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    row = conn.execute("""
        DELETE FROM individual_achievements
        WHERE id=%s
          AND COALESCE(is_deleted, 0)=1
        RETURNING id
    """, (achievement_id,)).fetchone()
    conn.commit()
    conn.close()

    if not row:
        return jsonify({"ok": False, "error": "Trash achievement not found"}), 404
    return jsonify({"ok": True, "message": "Achievement permanently deleted"})
