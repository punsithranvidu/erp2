from flask import Blueprint, request, session, jsonify, current_app
import psycopg
from werkzeug.security import generate_password_hash
from functools import wraps

from .db import connect

users_bp = Blueprint("users", __name__)


# ======================
# HELPERS
# ======================
def db():
    database_url = current_app.config["DATABASE_URL"]
    return connect(database_url)


def now_iso():
    from datetime import datetime
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

def normalize_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    low = u.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return u
    if low.startswith("mailto:") or low.startswith("tel:"):
        return u
    return "https://" + u

def count_active_admins(conn):
    row = conn.execute("""
        SELECT COUNT(*) AS c
        FROM users
        WHERE role='ADMIN' AND active=1
    """).fetchone()
    return int(row["c"] or 0)


def would_remove_last_admin(conn, target_user_id: int) -> bool:
    target = conn.execute("""
        SELECT id, role, active
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (target_user_id,)).fetchone()

    if not target:
        return False

    if target["role"] != "ADMIN" or int(target["active"]) != 1:
        return False

    admins = count_active_admins(conn)
    return admins <= 1


def get_modules():
    return current_app.config.get("MODULES", [
        "FINANCE",
        "DOCUMENT_STORAGE",
        "USERS",
        "CLIENTS",
        "INVOICES",
        "CASH_ADVANCES",
        "FINANCE_TRASH",
        "REPORTS",
        "MESSAGES",
        "CALENDAR",
        "HS_CODES",
        "WORKSHEET",
        "ADMIN_WORKSHEET",
        "ATTENDANCE",
        "MARKETING_EMAILS",
    ])


# ======================
# PERMISSIONS (ADMIN ONLY)
# ======================
@users_bp.route("/api/users/<int:uid>/permissions", methods=["GET"])
@login_required
def api_user_permissions_get(uid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    modules = get_modules()

    conn = db()
    u = conn.execute("""
        SELECT id, username, role
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (uid,)).fetchone()

    if not u:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    rows = conn.execute("""
        SELECT module, can_access, can_edit
        FROM user_permissions
        WHERE user_id=%s
        ORDER BY module ASC
    """, (uid,)).fetchall()
    conn.close()

    out = {
        r["module"]: {
            "can_access": int(r["can_access"]),
            "can_edit": int(r["can_edit"])
        }
        for r in rows
    }

    for m in modules:
        if m not in out:
            out[m] = {"can_access": 0, "can_edit": 0}

    return jsonify({
        "ok": True,
        "data": {
            "user_id": uid,
            "username": u["username"],
            "role": u["role"],
            "permissions": out
        }
    })


@users_bp.route("/api/users/<int:uid>/permissions", methods=["PUT"])
@login_required
def api_user_permissions_set(uid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    perms = data.get("permissions")
    modules = get_modules()

    if not isinstance(perms, dict):
        return jsonify({"ok": False, "error": "permissions must be an object"}), 400

    conn = db()
    u = conn.execute("""
        SELECT id, username, role
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (uid,)).fetchone()

    if not u:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    if u["role"] == "ADMIN":
        for m in modules:
            conn.execute("""
                INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                VALUES (%s,%s,1,1)
                ON CONFLICT(user_id, module)
                DO UPDATE SET can_access=1, can_edit=1
            """, (uid, m))
        conn.commit()
        conn.close()
        return jsonify({
            "ok": True,
            "data": {
                "user_id": uid,
                "username": u["username"],
                "role": u["role"]
            }
        })

    for m in modules:
        v = perms.get(m, {})
        can_access = int(v.get("can_access", 0) or 0)
        can_edit = int(v.get("can_edit", 0) or 0)

        if can_access not in (0, 1):
            can_access = 0
        if can_edit not in (0, 1):
            can_edit = 0
        if can_access == 0:
            can_edit = 0

        conn.execute("""
            INSERT INTO user_permissions (user_id, module, can_access, can_edit)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT(user_id, module)
            DO UPDATE SET can_access=excluded.can_access, can_edit=excluded.can_edit
        """, (uid, m, can_access, can_edit))

    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "data": {
            "user_id": uid,
            "username": u["username"],
            "role": u["role"]
        }
    })


# ======================
# USERS
# ======================
@users_bp.route("/api/users", methods=["GET"])
@login_required
@require_module("USERS")
def api_users_list():
    conn = db()
    rows = conn.execute("""
        SELECT id, username, role, active, full_name, nic, join_date, job_role, address, google_email, file_name, file_link, created_at
        FROM users
        ORDER BY id DESC
        LIMIT 2000
    """).fetchall()
    conn.close()

    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


@users_bp.route("/api/users", methods=["POST"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_create():
    data = request.json or {}
    modules = get_modules()

    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = (data.get("role") or "EMP").strip().upper()

    if not username:
        return jsonify({"ok": False, "error": "username is required"}), 400
    if len(password) < 3:
        return jsonify({"ok": False, "error": "password must be at least 3 characters"}), 400
    if role not in ("ADMIN", "EMP"):
        return jsonify({"ok": False, "error": "role must be ADMIN or EMP"}), 400

    full_name = (data.get("full_name") or "").strip() or None
    nic = (data.get("nic") or "").strip() or None
    join_date = (data.get("join_date") or "").strip() or None
    job_role = (data.get("job_role") or "").strip() or None
    address = (data.get("address") or "").strip() or None
    google_email = (data.get("google_email") or "").strip().lower() or None
    file_name = (data.get("file_name") or "").strip() or None
    file_link = normalize_url(data.get("file_link", "")) or None

    conn = db()
    try:
        conn.execute("""
            INSERT INTO users (
                username, password_hash, role, active,
                full_name, nic, join_date, job_role, address, google_email, file_name, file_link,
                created_at, created_by
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            username,
            generate_password_hash(password),
            role,
            1,
            full_name,
            nic,
            join_date,
            job_role,
            address,
            google_email,
            file_name,
            file_link,
            now_iso(),
            session["user"]
        ))
        conn.commit()

        new_row = conn.execute("""
            SELECT id, role
            FROM users
            WHERE username=%s
            LIMIT 1
        """, (username,)).fetchone()

        new_id = new_row["id"]
        new_role = new_row["role"]

        for m in modules:
            if new_role == "ADMIN":
                conn.execute("""
                    INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                    VALUES (%s,%s,1,1)
                    ON CONFLICT(user_id, module)
                    DO UPDATE SET can_access=1, can_edit=1
                """, (new_id, m))
            else:
                if m in ("FINANCE", "CASH_ADVANCES"):
                    conn.execute("""
                        INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                        VALUES (%s,%s,1,1)
                        ON CONFLICT(user_id, module)
                        DO UPDATE SET can_access=1, can_edit=1
                    """, (new_id, m))
                else:
                    conn.execute("""
                        INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                        VALUES (%s,%s,0,0)
                        ON CONFLICT(user_id, module)
                        DO UPDATE SET can_access=0, can_edit=0
                    """, (new_id, m))

        conn.commit()

        row = conn.execute("""
            SELECT id, username, role, active, full_name, nic, join_date, job_role, address, google_email, file_name, file_link, created_at
            FROM users
            WHERE username=%s
            LIMIT 1
        """, (username,)).fetchone()

        conn.close()
        return jsonify({"ok": True, "data": dict(row)})

    except psycopg.IntegrityError:
        conn.close()
        return jsonify({"ok": False, "error": "This username already exists"}), 400


@users_bp.route("/api/users/<int:uid>", methods=["PUT"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_update(uid):
    data = request.json or {}
    modules = get_modules()

    role = (data.get("role") or "").strip().upper()
    active = data.get("active")

    full_name = (data.get("full_name") or "").strip() if "full_name" in data else None
    nic = (data.get("nic") or "").strip() if "nic" in data else None
    join_date = (data.get("join_date") or "").strip() if "join_date" in data else None
    job_role = (data.get("job_role") or "").strip() if "job_role" in data else None
    address = (data.get("address") or "").strip() if "address" in data else None
    google_email = (data.get("google_email") or "").strip().lower() if "google_email" in data else None
    file_name = (data.get("file_name") or "").strip() if "file_name" in data else None
    file_link = normalize_url(data.get("file_link", "")) if "file_link" in data else None

    if role and role not in ("ADMIN", "EMP"):
        return jsonify({"ok": False, "error": "role must be ADMIN or EMP"}), 400

    if active is not None:
        try:
            active = int(active)
            if active not in (0, 1):
                raise ValueError()
        except Exception:
            return jsonify({"ok": False, "error": "active must be 0 or 1"}), 400

    conn = db()

    if active == 0 and would_remove_last_admin(conn, uid):
        conn.close()
        return jsonify({"ok": False, "error": "You cannot disable the last active ADMIN."}), 400

    if role == "EMP" and would_remove_last_admin(conn, uid):
        conn.close()
        return jsonify({"ok": False, "error": "You cannot change the last active ADMIN into EMP."}), 400

    sets = []
    vals = []

    if role:
        sets.append("role=%s")
        vals.append(role)
    if active is not None:
        sets.append("active=%s")
        vals.append(active)

    if "full_name" in data:
        sets.append("full_name=%s")
        vals.append(full_name or None)
    if "nic" in data:
        sets.append("nic=%s")
        vals.append(nic or None)
    if "join_date" in data:
        sets.append("join_date=%s")
        vals.append(join_date or None)
    if "job_role" in data:
        sets.append("job_role=%s")
        vals.append(job_role or None)
    if "address" in data:
        sets.append("address=%s")
        vals.append(address or None)
    if "google_email" in data:
        sets.append("google_email=%s")
        vals.append(google_email or None)
    if "file_name" in data:
        sets.append("file_name=%s")
        vals.append(file_name or None)
    if "file_link" in data:
        sets.append("file_link=%s")
        vals.append(file_link or None)    

    if not sets:
        conn.close()
        return jsonify({"ok": False, "error": "Nothing to update"}), 400

    sets.append("edited_at=%s")
    sets.append("edited_by=%s")
    vals.append(now_iso())
    vals.append(session["user"])
    vals.append(uid)

    conn.execute(f"UPDATE users SET {', '.join(sets)} WHERE id=%s", vals)
    conn.commit()

    if role in ("ADMIN", "EMP"):
        for m in modules:
            conn.execute("""
                INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                VALUES (%s,%s,0,0)
                ON CONFLICT(user_id, module) DO NOTHING
            """, (uid, m))

        if role == "ADMIN":
            for m in modules:
                conn.execute("""
                    UPDATE user_permissions
                    SET can_access=1, can_edit=1
                    WHERE user_id=%s AND module=%s
                """, (uid, m))
        conn.commit()

    row = conn.execute("""
        SELECT id, username, role, active, full_name, nic, join_date, job_role, address, google_email, file_name, file_link, created_at
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (uid,)).fetchone()

    conn.close()
    return jsonify({"ok": True, "data": dict(row) if row else None})


@users_bp.route("/api/users/<int:uid>/password", methods=["POST"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_reset_password(uid):
    data = request.json or {}
    pw = data.get("password") or ""

    if len(pw) < 3:
        return jsonify({"ok": False, "error": "password must be at least 3 characters"}), 400

    conn = db()
    row = conn.execute("""
        SELECT username
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (uid,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    conn.execute("""
        UPDATE users
        SET password_hash=%s, edited_at=%s, edited_by=%s
        WHERE id=%s
    """, (generate_password_hash(pw), now_iso(), session["user"], uid))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "data": {"id": uid, "username": row["username"]}})


@users_bp.route("/api/users/<int:uid>/rename", methods=["POST"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_rename(uid):
    data = request.json or {}
    new_username = (data.get("username") or "").strip()

    if not new_username:
        return jsonify({"ok": False, "error": "New username is required"}), 400

    conn = db()
    old = conn.execute("""
        SELECT username
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (uid,)).fetchone()

    if not old:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    old_username = old["username"]

    try:
        conn.execute("""
            UPDATE users
            SET username=%s, edited_at=%s, edited_by=%s
            WHERE id=%s
        """, (new_username, now_iso(), session["user"], uid))

        conn.execute("""
            UPDATE cash_advances
            SET employee_username=%s
            WHERE employee_username=%s
        """, (new_username, old_username))

        conn.commit()
    except psycopg.IntegrityError:
        conn.close()
        return jsonify({"ok": False, "error": "That username already exists"}), 400

    conn.close()
    return jsonify({"ok": True, "data": {"id": uid, "username": new_username}})


@users_bp.route("/api/users/<int:uid>/disable", methods=["POST"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_disable(uid):
    conn = db()

    if would_remove_last_admin(conn, uid):
        conn.close()
        return jsonify({"ok": False, "error": "You cannot disable the last active ADMIN."}), 400

    row = conn.execute("""
        SELECT id, username
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (uid,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    conn.execute("""
        UPDATE users
        SET active=0, edited_at=%s, edited_by=%s
        WHERE id=%s
    """, (now_iso(), session["user"], uid))
    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "data": {
            "id": uid,
            "username": row["username"],
            "active": 0
        }
    })


@users_bp.route("/api/users/<int:uid>", methods=["DELETE"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_delete(uid):
    conn = db()

    if would_remove_last_admin(conn, uid):
        conn.close()
        return jsonify({"ok": False, "error": "You cannot delete the last active ADMIN."}), 400

    row = conn.execute("""
        SELECT id, username
        FROM users
        WHERE id=%s
        LIMIT 1
    """, (uid,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    me = session.get("user")
    if me and row["username"] == me:
        conn.close()
        return jsonify({"ok": False, "error": "You cannot delete your own account."}), 400

    conn.execute("DELETE FROM user_permissions WHERE user_id=%s", (uid,))
    conn.execute("DELETE FROM users WHERE id=%s", (uid,))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "data": {"id": uid, "username": row["username"]}})


@users_bp.route("/api/users/employees", methods=["GET"])
@login_required
@require_module("CASH_ADVANCES")
def api_employees_list():
    conn = db()
    rows = conn.execute("""
        SELECT username, role, active, full_name
        FROM users
        WHERE active=1 AND role IN ('EMP','ADMIN')
        ORDER BY username ASC
    """).fetchall()
    conn.close()

    return jsonify({"ok": True, "data": [dict(r) for r in rows]})
