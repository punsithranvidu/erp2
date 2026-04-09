from flask import Blueprint, request, session, jsonify, current_app, Response
from functools import wraps
from io import StringIO
import csv

from .db_compat import sqlite3

cash_advances_bp = Blueprint("cash_advances", __name__)


# ======================
# HELPERS
# ======================
def db():
    database_url = current_app.config["DATABASE_URL"]
    conn = sqlite3.connect(database_url)
    conn.row_factory = sqlite3.Row
    return conn


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


def get_default_bank_id():
    conn = db()
    row = conn.execute("""
        SELECT id
        FROM bank_accounts
        WHERE active=1
        ORDER BY id ASC
        LIMIT 1
    """).fetchone()
    conn.close()
    return row["id"] if row else None


def _advance_money_total(conn, adv_id: int) -> float:
    base = conn.execute("""
        SELECT COALESCE(amount_given,0) AS a
        FROM cash_advances
        WHERE id=?
    """, (adv_id,)).fetchone()["a"]

    top = conn.execute("""
        SELECT COALESCE(SUM(amount),0) AS s
        FROM cash_advance_topups
        WHERE advance_id=?
    """, (adv_id,)).fetchone()["s"]

    return float(base or 0) + float(top or 0)


def _advance_spent_total(conn, adv_id: int) -> float:
    spent = conn.execute("""
        SELECT COALESCE(SUM(amount),0) AS s
        FROM cash_advance_expenses
        WHERE advance_id=?
    """, (adv_id,)).fetchone()["s"]

    return float(spent or 0)


def _advance_row_with_balance(conn, adv_row):
    adv_id = adv_row["id"]

    topups = conn.execute("""
        SELECT COALESCE(SUM(amount),0) AS s
        FROM cash_advance_topups
        WHERE advance_id=?
    """, (adv_id,)).fetchone()["s"]

    spent_company = conn.execute("""
        SELECT COALESCE(SUM(amount),0) AS s
        FROM cash_advance_expenses
        WHERE advance_id=? AND COALESCE(paid_by,'COMPANY_ADVANCE')='COMPANY_ADVANCE'
    """, (adv_id,)).fetchone()["s"]

    spent_personal = conn.execute("""
        SELECT COALESCE(SUM(amount),0) AS s
        FROM cash_advance_expenses
        WHERE advance_id=? AND COALESCE(paid_by,'COMPANY_ADVANCE')='EMPLOYEE_PERSONAL'
    """, (adv_id,)).fetchone()["s"]

    total_given = float(adv_row["amount_given"]) + float(topups)
    spent_total = float(spent_company) + float(spent_personal)
    balance = total_given - spent_total

    out = dict(adv_row)
    out["topups_total"] = round(float(topups), 2)
    out["spent_company"] = round(float(spent_company), 2)
    out["spent_personal"] = round(float(spent_personal), 2)
    out["spent_total"] = round(float(spent_total), 2)
    out["balance"] = round(float(balance), 2)
    out["status"] = "CLOSED" if int(adv_row["closed"] or 0) == 1 else "OPEN"
    return out


# ==========================
# CASH ADVANCES
# ==========================
@cash_advances_bp.route("/api/advances", methods=["GET"])
@login_required
@require_module("CASH_ADVANCES")
def api_advances_list():
    conn = db()
    role = session.get("role")
    user = session.get("user")

    if role == "ADMIN":
        rows = conn.execute("""
            SELECT ca.*, ba.name AS bank_name
            FROM cash_advances ca
            LEFT JOIN bank_accounts ba ON ba.id = ca.bank_id
            ORDER BY ca.id DESC
            LIMIT 1000
        """).fetchall()
    else:
        rows = conn.execute("""
            SELECT ca.*, ba.name AS bank_name
            FROM cash_advances ca
            LEFT JOIN bank_accounts ba ON ba.id = ca.bank_id
            WHERE ca.employee_username=?
            ORDER BY ca.id DESC
            LIMIT 1000
        """, (user,)).fetchall()

    data = [_advance_row_with_balance(conn, r) for r in rows]
    conn.close()
    return jsonify({"ok": True, "data": data})


@cash_advances_bp.route("/api/advances", methods=["POST"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_advances_create():
    data = request.json or {}

    employee_username = (data.get("employee_username") or "").strip()
    bank_id = data.get("bank_id")
    currency = (data.get("currency") or "").upper().strip()
    amount_given = data.get("amount_given")
    purpose = (data.get("purpose") or "").strip()
    given_date = (data.get("given_date") or "").strip()
    proof_link = normalize_url(data.get("proof_link") or "")

    if not employee_username:
        return jsonify({"ok": False, "error": "employee_username is required"}), 400

    conn = db()

    emp = conn.execute("""
        SELECT username, role
        FROM users
        WHERE username=? AND active=1
        LIMIT 1
    """, (employee_username,)).fetchone()

    if not emp:
        conn.close()
        return jsonify({"ok": False, "error": "Invalid username"}), 400

    if not currency:
        conn.close()
        return jsonify({"ok": False, "error": "currency is required"}), 400

    try:
        amt = float(amount_given)
        if amt <= 0:
            raise ValueError()
    except Exception:
        conn.close()
        return jsonify({"ok": False, "error": "amount_given must be a number > 0"}), 400

    if bank_id is None or str(bank_id).strip() == "":
        bank_id = get_default_bank_id()

    conn.execute("""
        INSERT INTO cash_advances (
            employee_username, bank_id, currency, amount_given,
            purpose, given_date, proof_link,
            created_at, created_by, closed, closed_at, closed_by
        ) VALUES (?,?,?,?,?,?,?,?,?,0,NULL,NULL)
    """, (
        employee_username,
        int(bank_id) if bank_id else None,
        currency,
        float(amt),
        purpose,
        given_date,
        proof_link,
        now_iso(),
        session["user"]
    ))
    conn.commit()

    new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

    row = conn.execute("""
        SELECT ca.*, ba.name AS bank_name
        FROM cash_advances ca
        LEFT JOIN bank_accounts ba ON ba.id=ca.bank_id
        WHERE ca.id=?
    """, (new_id,)).fetchone()

    out = _advance_row_with_balance(conn, row)
    conn.close()

    return jsonify({"ok": True, "data": out})


@cash_advances_bp.route("/api/advances/<int:aid>/close", methods=["POST"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_advances_close(aid):
    conn = db()

    row = conn.execute("""
        SELECT id, closed
        FROM cash_advances
        WHERE id=?
    """, (aid,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    if int(row["closed"]) == 1:
        conn.close()
        return jsonify({"ok": True, "data": {"id": aid, "closed": 1}})

    conn.execute("""
        UPDATE cash_advances
        SET closed=1, closed_at=?, closed_by=?
        WHERE id=?
    """, (now_iso(), session["user"], aid))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "data": {"id": aid, "closed": 1}})


@cash_advances_bp.route("/api/advances/<int:aid>/reopen", methods=["POST"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_advances_reopen(aid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    row = conn.execute("""
        SELECT id
        FROM cash_advances
        WHERE id=?
    """, (aid,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    conn.execute("""
        UPDATE cash_advances
        SET closed=0, closed_at=NULL, closed_by=NULL
        WHERE id=?
    """, (aid,))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "data": {"id": aid, "closed": 0}})


# ==========================
# TOPUPS
# ==========================
@cash_advances_bp.route("/api/advances/<int:aid>/topups", methods=["GET"])
@login_required
@require_module("CASH_ADVANCES")
def api_topups_list(aid):
    conn = db()

    adv = conn.execute("""
        SELECT *
        FROM cash_advances
        WHERE id=?
    """, (aid,)).fetchone()

    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    if not is_admin() and adv["employee_username"] != session["user"]:
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    rows = conn.execute("""
        SELECT *
        FROM cash_advance_topups
        WHERE advance_id=?
        ORDER BY id DESC
        LIMIT 2000
    """, (aid,)).fetchall()

    conn.close()
    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


@cash_advances_bp.route("/api/advances/<int:aid>/topups", methods=["POST"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_topups_add(aid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    amount = data.get("amount")
    topup_date = (data.get("topup_date") or "").strip()
    proof_link = normalize_url(data.get("proof_link") or "")
    ref_type = (data.get("ref_type") or "").strip().upper()
    ref_id = (data.get("ref_id") or "").strip()
    note = (data.get("note") or "").strip()

    try:
        amt = float(amount)
        if amt <= 0:
            raise ValueError()
    except Exception:
        return jsonify({"ok": False, "error": "Top-up amount must be a number > 0"}), 400

    conn = db()

    adv = conn.execute("""
        SELECT id
        FROM cash_advances
        WHERE id=?
    """, (aid,)).fetchone()

    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    conn.execute("""
        INSERT INTO cash_advance_topups
        (advance_id, amount, topup_date, proof_link, ref_type, ref_id, note, created_at, created_by)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        aid,
        float(amt),
        topup_date,
        proof_link,
        ref_type,
        ref_id,
        note,
        now_iso(),
        session["user"]
    ))
    conn.commit()

    new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    row = conn.execute("""
        SELECT *
        FROM cash_advance_topups
        WHERE id=?
    """, (new_id,)).fetchone()

    conn.close()
    return jsonify({"ok": True, "data": dict(row)})


@cash_advances_bp.route("/api/topups/<int:tid>", methods=["DELETE"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_topups_delete(tid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()

    row = conn.execute("""
        SELECT id
        FROM cash_advance_topups
        WHERE id=?
    """, (tid,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Top-up not found"}), 404

    conn.execute("""
        DELETE FROM cash_advance_topups
        WHERE id=?
    """, (tid,))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "data": {"id": tid}})


# ==========================
# EXPENSES
# ==========================
@cash_advances_bp.route("/api/advances/<int:aid>/expenses", methods=["GET"])
@login_required
@require_module("CASH_ADVANCES")
def api_advances_expenses_list(aid):
    conn = db()

    adv = conn.execute("""
        SELECT *
        FROM cash_advances
        WHERE id=?
    """, (aid,)).fetchone()

    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    if not is_admin() and adv["employee_username"] != session["user"]:
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    rows = conn.execute("""
        SELECT *
        FROM cash_advance_expenses
        WHERE advance_id=?
        ORDER BY id DESC
        LIMIT 2000
    """, (aid,)).fetchall()

    conn.close()
    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


@cash_advances_bp.route("/api/advances/<int:aid>/expenses", methods=["POST"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_advances_expenses_add(aid):
    data = request.json or {}

    category = (data.get("category") or "").strip()
    description = (data.get("description") or "").strip()
    paid_by = (data.get("paid_by") or "COMPANY_ADVANCE").strip().upper()
    amount = data.get("amount")
    spent_date = (data.get("spent_date") or "").strip()
    proof_link = normalize_url(data.get("proof_link") or "")

    if paid_by not in ("COMPANY_ADVANCE", "EMPLOYEE_PERSONAL"):
        return jsonify({"ok": False, "error": "paid_by must be COMPANY_ADVANCE or EMPLOYEE_PERSONAL"}), 400

    if not category:
        return jsonify({"ok": False, "error": "category is required"}), 400

    try:
        amt = float(amount)
        if amt <= 0:
            raise ValueError()
    except Exception:
        return jsonify({"ok": False, "error": "amount must be a number > 0"}), 400

    conn = db()

    adv = conn.execute("""
        SELECT *
        FROM cash_advances
        WHERE id=?
    """, (aid,)).fetchone()

    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    if int(adv["closed"]) == 1:
        conn.close()
        return jsonify({"ok": False, "error": "This advance is CLOSED. You cannot add expenses."}), 400

    if not is_admin() and adv["employee_username"] != session["user"]:
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    if paid_by == "COMPANY_ADVANCE":
        topups = conn.execute("""
            SELECT COALESCE(SUM(amount),0) AS s
            FROM cash_advance_topups
            WHERE advance_id=?
        """, (aid,)).fetchone()["s"]

        spent_company = conn.execute("""
            SELECT COALESCE(SUM(amount),0) AS s
            FROM cash_advance_expenses
            WHERE advance_id=? AND COALESCE(paid_by,'COMPANY_ADVANCE')='COMPANY_ADVANCE'
        """, (aid,)).fetchone()["s"]

        remaining_company = (float(adv["amount_given"]) + float(topups)) - float(spent_company)

        if float(amt) > float(remaining_company) + 1e-9:
            conn.close()
            return jsonify({
                "ok": False,
                "error": f"Not enough COMPANY balance. Remaining: {round(float(remaining_company), 2)}"
            }), 400

    conn.execute("""
        INSERT INTO cash_advance_expenses (
            advance_id, category, description, paid_by, amount, proof_link, spent_date,
            created_at, created_by
        ) VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        aid,
        category,
        description,
        paid_by,
        float(amt),
        proof_link,
        spent_date,
        now_iso(),
        session["user"]
    ))
    conn.commit()

    new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    row = conn.execute("""
        SELECT *
        FROM cash_advance_expenses
        WHERE id=?
    """, (new_id,)).fetchone()

    conn.close()
    return jsonify({"ok": True, "data": dict(row)})


@cash_advances_bp.route("/api/expenses/<int:eid>", methods=["PUT"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_expense_update(eid):
    data = request.json or {}

    category = (data.get("category") or "").strip()
    description = (data.get("description") or "").strip()
    paid_by = (data.get("paid_by") or "COMPANY_ADVANCE").strip().upper()
    amount = data.get("amount")
    spent_date = (data.get("spent_date") or "").strip()
    proof_link = normalize_url(data.get("proof_link") or "")

    if paid_by not in ("COMPANY_ADVANCE", "EMPLOYEE_PERSONAL"):
        return jsonify({"ok": False, "error": "paid_by must be COMPANY_ADVANCE or EMPLOYEE_PERSONAL"}), 400

    if not category:
        return jsonify({"ok": False, "error": "category is required"}), 400

    try:
        amt = float(amount)
        if amt <= 0:
            raise ValueError()
    except Exception:
        return jsonify({"ok": False, "error": "amount must be a number > 0"}), 400

    conn = db()

    exp = conn.execute("""
        SELECT id, advance_id
        FROM cash_advance_expenses
        WHERE id=?
        LIMIT 1
    """, (eid,)).fetchone()

    if not exp:
        conn.close()
        return jsonify({"ok": False, "error": "Expense not found"}), 404

    adv = conn.execute("""
        SELECT *
        FROM cash_advances
        WHERE id=?
        LIMIT 1
    """, (exp["advance_id"],)).fetchone()

    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    if int(adv["closed"]) == 1:
        conn.close()
        return jsonify({"ok": False, "error": "This advance is CLOSED. You cannot edit expenses."}), 400

    if not is_admin() and adv["employee_username"] != session["user"]:
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    if paid_by == "COMPANY_ADVANCE":
        topups = conn.execute("""
            SELECT COALESCE(SUM(amount),0) AS s
            FROM cash_advance_topups
            WHERE advance_id=?
        """, (exp["advance_id"],)).fetchone()["s"]

        spent_company_other = conn.execute("""
            SELECT COALESCE(SUM(amount),0) AS s
            FROM cash_advance_expenses
            WHERE advance_id=? AND id<>? AND COALESCE(paid_by,'COMPANY_ADVANCE')='COMPANY_ADVANCE'
        """, (exp["advance_id"], eid)).fetchone()["s"]

        remaining_company = (float(adv["amount_given"]) + float(topups)) - float(spent_company_other)

        if float(amt) > float(remaining_company) + 1e-9:
            conn.close()
            return jsonify({
                "ok": False,
                "error": f"Not enough COMPANY balance. Remaining: {round(float(remaining_company), 2)}"
            }), 400

    conn.execute("""
        UPDATE cash_advance_expenses
        SET category=?, description=?, paid_by=?, amount=?, spent_date=?, proof_link=?
        WHERE id=?
    """, (category, description, paid_by, float(amt), spent_date, proof_link, eid))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "data": {"id": eid}})


@cash_advances_bp.route("/api/expenses/<int:eid>", methods=["DELETE"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_expense_delete(eid):
    conn = db()

    exp = conn.execute("""
        SELECT id, advance_id
        FROM cash_advance_expenses
        WHERE id=?
        LIMIT 1
    """, (eid,)).fetchone()

    if not exp:
        conn.close()
        return jsonify({"ok": False, "error": "Expense not found"}), 404

    adv = conn.execute("""
        SELECT *
        FROM cash_advances
        WHERE id=?
        LIMIT 1
    """, (exp["advance_id"],)).fetchone()

    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    if int(adv["closed"]) == 1:
        conn.close()
        return jsonify({"ok": False, "error": "This advance is CLOSED. You cannot delete expenses."}), 400

    if not is_admin() and adv["employee_username"] != session["user"]:
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    conn.execute("""
        DELETE FROM cash_advance_expenses
        WHERE id=?
    """, (eid,))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "data": {"id": eid}})


# ==========================
# ADVANCE EDIT / DELETE / SUMMARY
# ==========================
@cash_advances_bp.route("/api/advances/<int:aid>", methods=["PUT"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_advances_update(aid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}

    employee_username = (data.get("employee_username") or "").strip()
    bank_id = data.get("bank_id")
    currency = (data.get("currency") or "").upper().strip()
    amount_given = data.get("amount_given")
    purpose = (data.get("purpose") or "").strip()
    given_date = (data.get("given_date") or "").strip()
    proof_link = normalize_url(data.get("proof_link") or "")

    if not employee_username:
        return jsonify({"ok": False, "error": "employee_username is required"}), 400
    if not currency:
        return jsonify({"ok": False, "error": "currency is required"}), 400

    try:
        amt = float(amount_given)
        if amt <= 0:
            raise ValueError()
    except Exception:
        return jsonify({"ok": False, "error": "amount_given must be a number > 0"}), 400

    conn = db()

    adv = conn.execute("""
        SELECT id
        FROM cash_advances
        WHERE id=?
        LIMIT 1
    """, (aid,)).fetchone()

    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    emp = conn.execute("""
        SELECT username
        FROM users
        WHERE username=? AND active=1
        LIMIT 1
    """, (employee_username,)).fetchone()

    if not emp:
        conn.close()
        return jsonify({"ok": False, "error": "Invalid username"}), 400

    if bank_id is None or str(bank_id).strip() == "":
        bank_id = get_default_bank_id()

    conn.execute("""
        UPDATE cash_advances
        SET employee_username=?, bank_id=?, currency=?, amount_given=?,
            purpose=?, given_date=?, proof_link=?
        WHERE id=?
    """, (
        employee_username,
        int(bank_id) if bank_id else None,
        currency,
        float(amt),
        purpose,
        given_date,
        proof_link,
        aid
    ))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "data": {"id": aid}})


@cash_advances_bp.route("/api/advances/<int:aid>", methods=["DELETE"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_advances_delete(aid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()

    row = conn.execute("""
        SELECT id
        FROM cash_advances
        WHERE id=?
        LIMIT 1
    """, (aid,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    conn.execute("DELETE FROM cash_advance_expenses WHERE advance_id=?", (aid,))
    conn.execute("DELETE FROM cash_advance_topups WHERE advance_id=?", (aid,))
    conn.execute("DELETE FROM cash_advances WHERE id=?", (aid,))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "data": {"id": aid}})


@cash_advances_bp.route("/api/advances/summary", methods=["GET"])
@login_required
@require_module("CASH_ADVANCES")
def api_advances_summary():
    employee = (request.args.get("employee") or "").strip()
    month = (request.args.get("month") or "").strip()
    status = (request.args.get("status") or "").strip().upper()

    if not is_admin():
        employee = session["user"]

    where = ["1=1"]
    params = []

    if employee and employee.upper() != "ALL":
        where.append("ca.employee_username = ?")
        params.append(employee)

    if month:
        where.append("substr(COALESCE(NULLIF(ca.given_date,''), ca.created_at), 1, 7) = ?")
        params.append(month)

    if status in ("OPEN", "CLOSED"):
        where.append("ca.closed = ?")
        params.append(1 if status == "CLOSED" else 0)

    conn = db()
    rows = conn.execute(f"""
        SELECT
            ca.employee_username,
            ca.currency,
            SUM(COALESCE(ca.amount_given, 0)) AS total_given,

            SUM(
                COALESCE((
                    SELECT SUM(t.amount)
                    FROM cash_advance_topups t
                    WHERE t.advance_id = ca.id
                ), 0)
            ) AS total_topups,

            SUM(
                COALESCE((
                    SELECT SUM(e.amount)
                    FROM cash_advance_expenses e
                    WHERE e.advance_id = ca.id
                ), 0)
            ) AS total_spent,

            COUNT(*) AS count
        FROM cash_advances ca
        WHERE {" AND ".join(where)}
        GROUP BY ca.employee_username, ca.currency
        ORDER BY ca.employee_username ASC, ca.currency ASC
    """, params).fetchall()
    conn.close()

    data = []
    for r in rows:
        given = float(r["total_given"] or 0)
        topups = float(r["total_topups"] or 0)
        spent = float(r["total_spent"] or 0)
        count = int(r["count"] or 0)

        data.append({
            "employee_username": r["employee_username"],
            "currency": r["currency"],
            "total_given": round(given, 2),
            "total_topups": round(topups, 2),
            "total_spent": round(spent, 2),
            "total_balance": round((given + topups) - spent, 2),
            "advances_count": count,
        })

    return jsonify({"ok": True, "data": data})


@cash_advances_bp.route("/api/advances/summary.csv", methods=["GET"])
@login_required
@require_module("CASH_ADVANCES")
def api_advances_summary_csv():
    employee = (request.args.get("employee") or "").strip()
    month = (request.args.get("month") or "").strip()
    status = (request.args.get("status") or "").strip().upper()

    if not is_admin():
        employee = session["user"]

    where = ["1=1"]
    params = []

    if employee and employee.upper() != "ALL":
        where.append("ca.employee_username = ?")
        params.append(employee)

    if month:
        where.append("substr(COALESCE(NULLIF(ca.given_date,''), ca.created_at), 1, 7) = ?")
        params.append(month)

    if status in ("OPEN", "CLOSED"):
        where.append("ca.closed = ?")
        params.append(1 if status == "CLOSED" else 0)

    conn = db()
    rows = conn.execute(f"""
        SELECT
            ca.employee_username,
            ca.currency,
            SUM(COALESCE(ca.amount_given, 0)) AS total_given,

            SUM(
                COALESCE((
                    SELECT SUM(t.amount)
                    FROM cash_advance_topups t
                    WHERE t.advance_id = ca.id
                ), 0)
            ) AS total_topups,

            SUM(
                COALESCE((
                    SELECT SUM(e.amount)
                    FROM cash_advance_expenses e
                    WHERE e.advance_id = ca.id
                ), 0)
            ) AS total_spent,

            COUNT(*) AS count
        FROM cash_advances ca
        WHERE {" AND ".join(where)}
        GROUP BY ca.employee_username, ca.currency
        ORDER BY ca.employee_username ASC, ca.currency ASC
    """, params).fetchall()
    conn.close()

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Employee",
        "Currency",
        "Total Given",
        "Total Top-Ups",
        "Total Spent",
        "Balance",
        "Advances Count"
    ])

    for r in rows:
        given = float(r["total_given"] or 0)
        topups = float(r["total_topups"] or 0)
        spent = float(r["total_spent"] or 0)
        balance = (given + topups) - spent

        writer.writerow([
            r["employee_username"],
            r["currency"],
            round(given, 2),
            round(topups, 2),
            round(spent, 2),
            round(balance, 2),
            int(r["count"] or 0),
        ])

    csv_text = output.getvalue()
    output.close()

    filename = "cash_advance_summary.csv"
    if month:
        filename = f"cash_advance_summary_{month}.csv"

    return Response(
        csv_text,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )