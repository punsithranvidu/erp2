from flask import Blueprint, request, session, jsonify, Response, current_app
from functools import wraps
from io import StringIO
import csv

from .db_compat import sqlite3

finance_bp = Blueprint("finance", __name__)


# ======================
# HELPERS
# ======================
def db():
    database_url = current_app.config["DATABASE_URL"]
    return sqlite3.connect(database_url)


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


def purge_deleted_older_than_30_days():
    """
    Safe purge.
    If DB pool is temporarily full, do not break the whole page load.
    """
    conn = None
    try:
        conn = db()
        conn.execute("""
            DELETE FROM finance_records
            WHERE deleted_at IS NOT NULL
              AND CAST(REPLACE(deleted_at, 'T', ' ') AS timestamp) <= NOW() - INTERVAL '30 days'
        """)
        conn.commit()
    except Exception:
        # Ignore purge failures so finance page can still work
        pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


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


def get_bank_id_from_request():
    bank_id = request.args.get("bank_id")
    if bank_id is None:
        data = request.json or {}
        bank_id = data.get("bank_id")

    if bank_id is None or bank_id == "":
        return None

    if isinstance(bank_id, str) and bank_id.strip().upper() == "ALL":
        return "ALL"

    try:
        return int(bank_id)
    except Exception:
        return None


def is_all_banks(bank_id):
    return bank_id == "ALL"


# ======================
# BANKS
# ======================
@finance_bp.route("/api/banks", methods=["GET"])
@login_required
@require_module("FINANCE")
def api_banks_list():
    """
    EMP/ADMIN: returns active banks by default.
    ADMIN can request all banks (active + disabled): /api/banks?all=1
    """
    show_all = (request.args.get("all") == "1")
    admin_user = (session.get("role") == "ADMIN")

    conn = db()
    if show_all and admin_user:
        rows = conn.execute("""
            SELECT id, name, active, created_at, created_by
            FROM bank_accounts
            ORDER BY id ASC
        """).fetchall()
    else:
        rows = conn.execute("""
            SELECT id, name, active, created_at, created_by
            FROM bank_accounts
            WHERE active=1
            ORDER BY id ASC
        """).fetchall()

    conn.close()
    return jsonify([dict(r) for r in rows])


@finance_bp.route("/api/banks", methods=["POST"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_banks_create():
    data = request.json or {}
    name = (data.get("name") or "").strip()

    if not name:
        return jsonify({"ok": False, "error": "Bank account name is required"}), 400

    conn = None
    try:
        conn = db()
        conn.execute("""
            INSERT INTO bank_accounts (name, created_at, created_by, active)
            VALUES (?,?,?,1)
        """, (name, now_iso(), session["user"]))
        conn.commit()

        row = conn.execute("""
            SELECT id, name, active
            FROM bank_accounts
            WHERE name=?
            LIMIT 1
        """, (name,)).fetchone()
        conn.close()

        return jsonify({
            "ok": True,
            "id": row["id"],
            "name": row["name"],
            "active": row["active"]
        })
    except sqlite3.IntegrityError:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "This bank account already exists"}), 400
    except Exception as e:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(e)}), 500


@finance_bp.route("/api/banks/<int:bid>", methods=["PUT"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_banks_rename(bid):
    data = request.json or {}
    new_name = (data.get("name") or "").strip()

    if not new_name:
        return jsonify({"ok": False, "error": "New bank name is required"}), 400

    conn = None
    try:
        conn = db()

        exists = conn.execute("""
            SELECT id
            FROM bank_accounts
            WHERE id=?
        """, (bid,)).fetchone()

        if not exists:
            conn.close()
            return jsonify({"ok": False, "error": "Bank account not found"}), 404

        dupe = conn.execute("""
            SELECT id
            FROM bank_accounts
            WHERE name=? AND id<>?
        """, (new_name, bid)).fetchone()

        if dupe:
            conn.close()
            return jsonify({"ok": False, "error": "This bank account name already exists"}), 400

        conn.execute("""
            UPDATE bank_accounts
            SET name=?
            WHERE id=?
        """, (new_name, bid))
        conn.commit()
        conn.close()

        return jsonify({"ok": True, "id": bid, "name": new_name})
    except sqlite3.IntegrityError:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "This bank account name already exists"}), 400
    except Exception as e:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(e)}), 500


@finance_bp.route("/api/banks/<int:bid>", methods=["DELETE"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_banks_delete(bid):
    conn = db()

    bank = conn.execute("""
        SELECT id, name, active
        FROM bank_accounts
        WHERE id=?
    """, (bid,)).fetchone()

    if not bank:
        conn.close()
        return jsonify({"ok": False, "error": "Bank account not found"}), 404

    if int(bank["active"]) == 0:
        conn.close()
        return jsonify({"ok": False, "error": "Bank already disabled"}), 400

    cnt = conn.execute("""
        SELECT COUNT(*) AS c
        FROM finance_records
        WHERE bank_id=?
    """, (bid,)).fetchone()["c"]

    if cnt > 0:
        conn.close()
        return jsonify({
            "ok": False,
            "error": f"Cannot delete bank. It has {cnt} finance record(s). Delete/move those records first."
        }), 400

    conn.execute("""
        UPDATE bank_accounts
        SET active=0
        WHERE id=?
    """, (bid,))
    conn.commit()
    conn.close()

    return jsonify({"ok": True})


# ======================
# FINANCE
# ======================
@finance_bp.route("/api/finance", methods=["GET"])
@login_required
@require_module("FINANCE")
def api_finance_list():
    purge_deleted_older_than_30_days()

    ftype = (request.args.get("type") or "").upper().strip()
    month = (request.args.get("month") or "").strip()

    bank_id = get_bank_id_from_request()
    if bank_id is None:
        bank_id = get_default_bank_id()

    where = ["fr.deleted_at IS NULL"]
    params = []

    if not is_all_banks(bank_id):
        where.append("fr.bank_id = ?")
        params.append(bank_id)

    if ftype in ("INCOME", "OUTCOME"):
        where.append("fr.type = ?")
        params.append(ftype)

    if month:
        where.append("substr(COALESCE(NULLIF(fr.paid_date,''), fr.created_at), 1, 7) = ?")
        params.append(month)

    sql = f"""
        SELECT fr.*, ba.name AS bank_name
        FROM finance_records fr
        LEFT JOIN bank_accounts ba ON ba.id = fr.bank_id
        WHERE {" AND ".join(where)}
        ORDER BY fr.id DESC
    """

    conn = db()
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@finance_bp.route("/api/finance", methods=["POST"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_finance_create():
    data = request.json or {}
    required = [
        "type", "client_name", "category", "currency", "amount",
        "payment_type", "status", "proof_of_payment", "bank_id"
    ]

    for k in required:
        if not data.get(k):
            return jsonify({"ok": False, "error": f"Missing field: {k}"}), 400

    conn = None
    try:
        conn = db()
        conn.execute("""
            INSERT INTO finance_records (
                bank_id,
                type, client_name, category, description, currency, amount,
                payment_type, status, proof_of_payment, invoice_ref, po_number, quotation_number,
                paid_date, folder_link, proof_link, invoice_link, po_link, quotation_link,
                created_at, created_by,
                deleted_at, deleted_by
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL)
        """, (
            int(data["bank_id"]),
            data["type"],
            data["client_name"],
            data["category"],
            data.get("description", ""),
            data["currency"],
            float(data["amount"]),
            data["payment_type"],
            data["status"],
            data["proof_of_payment"],
            data.get("invoice_ref", ""),
            data.get("po_number", ""),
            data.get("quotation_number", ""),
            data.get("paid_date", ""),
            normalize_url(data.get("folder_link", "")),
            normalize_url(data.get("proof_link", "")),
            normalize_url(data.get("invoice_link", "")),
            normalize_url(data.get("po_link", "")),
            normalize_url(data.get("quotation_link", "")),
            now_iso(),
            session["user"],
        ))
        conn.commit()
        conn.close()

        return jsonify({"ok": True})
    except Exception as e:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(e)}), 500


@finance_bp.route("/api/finance/<int:rid>", methods=["PUT"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_finance_update(rid):
    data = request.json or {}
    allowed = [
        "bank_id",
        "type", "client_name", "category", "description", "currency", "amount",
        "payment_type", "status", "proof_of_payment", "invoice_ref", "po_number",
        "quotation_number", "paid_date", "folder_link", "proof_link",
        "invoice_link", "po_link", "quotation_link"
    ]

    sets = []
    vals = []

    for k in allowed:
        if k in data:
            v = data[k]
            if k.endswith("_link") or k in ("folder_link", "proof_link", "invoice_link", "quotation_link"):
                v = normalize_url(v)
            sets.append(f"{k}=?")
            vals.append(v)

    if not sets:
        return jsonify({"ok": False, "error": "No fields to update"}), 400

    sets.append("edited_at=?")
    sets.append("edited_by=?")
    vals.append(now_iso())
    vals.append(session["user"])

    conn = db()

    if is_admin():
        vals.append(rid)
        cur = conn.execute(
            f"UPDATE finance_records SET {', '.join(sets)} WHERE id=? AND deleted_at IS NULL",
            vals
        )
    else:
        vals.append(rid)
        vals.append(session["user"])
        cur = conn.execute(
            f"UPDATE finance_records SET {', '.join(sets)} WHERE id=? AND deleted_at IS NULL AND created_by=?",
            vals
        )

    conn.commit()
    updated = cur.rowcount
    conn.close()

    if updated == 0:
        return jsonify({"ok": False, "error": "Not allowed or record not found"}), 403

    return jsonify({"ok": True})


@finance_bp.route("/api/finance/<int:rid>", methods=["DELETE"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_finance_soft_delete(rid):
    conn = db()

    if is_admin():
        cur = conn.execute("""
            UPDATE finance_records
            SET deleted_at=?, deleted_by=?
            WHERE id=? AND deleted_at IS NULL
        """, (now_iso(), session["user"], rid))
    else:
        cur = conn.execute("""
            UPDATE finance_records
            SET deleted_at=?, deleted_by=?
            WHERE id=? AND deleted_at IS NULL AND created_by=?
        """, (now_iso(), session["user"], rid, session["user"]))

    conn.commit()
    affected = cur.rowcount
    conn.close()

    if affected == 0:
        return jsonify({"ok": False, "error": "Not allowed or record not found"}), 403

    return jsonify({"ok": True})


# ======================
# FINANCE SUMMARY
# ======================
@finance_bp.route("/api/finance/summary", methods=["GET"])
@login_required
@require_module("FINANCE")
def api_finance_summary():
    purge_deleted_older_than_30_days()

    ftype = (request.args.get("type") or "INCOME").upper().strip()
    group = (request.args.get("group") or "month").lower().strip()
    month = (request.args.get("month") or "").strip()
    user_filter = (request.args.get("user") or "").strip()

    bank_id = get_bank_id_from_request()
    if bank_id is None:
        bank_id = get_default_bank_id()

    group_map = {
        "year": "substr(COALESCE(NULLIF(paid_date,''), created_at), 1, 4)",
        "month": "substr(COALESCE(NULLIF(paid_date,''), created_at), 1, 7)",
        "client": "client_name",
        "category": "category",
        "payment_type": "payment_type",
        "status": "status",
        "created_by": "created_by",
        "bank": "CAST(bank_id AS TEXT)"
    }
    key_expr = group_map.get(group, group_map["month"])

    where = ["deleted_at IS NULL", "type = ?"]
    params = [ftype]

    if not is_all_banks(bank_id):
        where.append("bank_id = ?")
        params.append(bank_id)

    if month:
        where.append("substr(COALESCE(NULLIF(paid_date,''), created_at), 1, 7) = ?")
        params.append(month)

    if user_filter:
        where.append("created_by = ?")
        params.append(user_filter)

    conn = db()
    rows = conn.execute(f"""
        SELECT
          {key_expr} AS key,
          currency AS currency,
          SUM(amount) AS total,
          COUNT(*) AS count
        FROM finance_records
        WHERE {" AND ".join(where)}
        GROUP BY key, currency
        ORDER BY key DESC, currency ASC
        LIMIT 1000
    """, params).fetchall()
    conn.close()

    out = []
    for r in rows:
        out.append({
            "key": r["key"],
            "currency": r["currency"],
            "total": round(float(r["total"] or 0), 2),
            "count": int(r["count"] or 0),
        })

    return jsonify(out)


@finance_bp.route("/api/finance/summary.csv", methods=["GET"])
@login_required
@require_module("FINANCE")
def api_finance_summary_csv():
    purge_deleted_older_than_30_days()

    ftype = (request.args.get("type") or "INCOME").upper().strip()
    group = (request.args.get("group") or "month").lower().strip()
    month = (request.args.get("month") or "").strip()
    user_filter = (request.args.get("user") or "").strip()

    bank_id = get_bank_id_from_request()
    if bank_id is None:
        bank_id = get_default_bank_id()

    group_map = {
        "year": "substr(COALESCE(NULLIF(paid_date,''), created_at), 1, 4)",
        "month": "substr(COALESCE(NULLIF(paid_date,''), created_at), 1, 7)",
        "client": "client_name",
        "category": "category",
        "payment_type": "payment_type",
        "status": "status",
        "created_by": "created_by",
        "bank": "CAST(bank_id AS TEXT)"
    }
    key_expr = group_map.get(group, group_map["month"])

    where = ["deleted_at IS NULL", "type = ?"]
    params = [ftype]

    if not is_all_banks(bank_id):
        where.append("bank_id = ?")
        params.append(bank_id)

    if month:
        where.append("substr(COALESCE(NULLIF(paid_date,''), created_at), 1, 7) = ?")
        params.append(month)

    if user_filter:
        where.append("created_by = ?")
        params.append(user_filter)

    conn = db()
    rows = conn.execute(f"""
        SELECT
          {key_expr} AS key,
          currency AS currency,
          SUM(amount) AS total,
          COUNT(*) AS count
        FROM finance_records
        WHERE {" AND ".join(where)}
        GROUP BY key, currency
        ORDER BY key DESC, currency ASC
        LIMIT 5000
    """, params).fetchall()
    conn.close()

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["key", "currency", "total", "count"])

    for r in rows:
        writer.writerow([
            r["key"],
            r["currency"],
            round(float(r["total"] or 0), 2),
            int(r["count"] or 0)
        ])

    bank_tag = "allbanks" if is_all_banks(bank_id) else f"bank{bank_id}"
    filename = f"summary_{ftype.lower()}_{group}{'_'+month if month else ''}{'_user-'+user_filter if user_filter else ''}_{bank_tag}.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@finance_bp.route("/api/finance/export.csv", methods=["GET"])
@login_required
@require_module("FINANCE")
def api_finance_export_csv():
    purge_deleted_older_than_30_days()

    ftype = (request.args.get("type") or "").upper().strip()
    month = (request.args.get("month") or "").strip()

    bank_id = request.args.get("bank_id") or "ALL"
    bank_id = str(bank_id).strip()

    where = ["fr.deleted_at IS NULL"]
    params = []

    if bank_id.upper() != "ALL":
        where.append("fr.bank_id = ?")
        params.append(int(bank_id))

    if ftype in ("INCOME", "OUTCOME"):
        where.append("fr.type = ?")
        params.append(ftype)

    if month:
        where.append("substr(COALESCE(NULLIF(fr.paid_date,''), fr.created_at), 1, 7) = ?")
        params.append(month)

    sql = f"""
        SELECT
            fr.id,
            ba.name AS bank_name,
            fr.type,
            fr.client_name,
            fr.category,
            fr.description,
            fr.currency,
            fr.amount,
            fr.payment_type,
            fr.status,
            fr.proof_of_payment,
            fr.invoice_ref,
            fr.po_number,
            fr.quotation_number,
            fr.paid_date,
            fr.created_at,
            fr.created_by,
            fr.edited_at,
            fr.edited_by,
            fr.folder_link,
            fr.proof_link,
            fr.invoice_link,
            fr.po_link,
            fr.quotation_link
        FROM finance_records fr
        LEFT JOIN bank_accounts ba ON ba.id = fr.bank_id
        WHERE {" AND ".join(where)}
        ORDER BY fr.id DESC
    """

    conn = db()
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    si = StringIO()
    cw = csv.writer(si)

    cw.writerow([
        "ID", "Bank", "Type", "Client", "Category", "Description", "Currency", "Amount",
        "Payment Type", "Status", "Proof Of Payment", "Invoice Ref", "PO Number", "Quotation Number",
        "Paid Date", "Created At", "Created By", "Edited At", "Edited By",
        "Client Folder Link", "Proof Link", "Invoice Link","PO Link", "Quotation Link"
    ])

    for r in rows:
        r = dict(r)
        cw.writerow([
            r.get("id"),
            r.get("bank_name"),
            r.get("type"),
            r.get("client_name"),
            r.get("category"),
            r.get("description"),
            r.get("currency"),
            r.get("amount"),
            r.get("payment_type"),
            r.get("status"),
            r.get("proof_of_payment"),
            r.get("invoice_ref"),
            r.get("po_number"),
            r.get("quotation_number"),
            r.get("paid_date"),
            r.get("created_at"),
            r.get("created_by"),
            r.get("edited_at"),
            r.get("edited_by"),
            r.get("folder_link"),
            r.get("proof_link"),
            r.get("invoice_link"),
            r.get("po_link"),
            r.get("quotation_link")
        ])

    output = si.getvalue()
    return Response(
        output,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=finance_export.csv"}
    )


# ======================
# FINANCE TRASH
# ======================
@finance_bp.route("/api/finance/trash", methods=["GET"])
@login_required
@require_module("FINANCE_TRASH")
def api_finance_trash_list():
    purge_deleted_older_than_30_days()

    conn = db()
    rows = conn.execute("""
        SELECT *
        FROM finance_records
        WHERE deleted_at IS NOT NULL
        ORDER BY deleted_at DESC
        LIMIT 1000
    """).fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])


@finance_bp.route("/api/finance/<int:rid>/restore", methods=["POST"])
@login_required
@require_module("FINANCE_TRASH", need_edit=True)
def api_finance_restore(rid):
    conn = db()
    conn.execute("""
        UPDATE finance_records
        SET deleted_at=NULL, deleted_by=NULL
        WHERE id=?
    """, (rid,))
    conn.commit()
    conn.close()

    return jsonify({"ok": True})


@finance_bp.route("/api/finance/purge", methods=["POST"])
@login_required
@require_module("FINANCE_TRASH", need_edit=True)
def api_finance_purge():
    purge_deleted_older_than_30_days()
    return jsonify({"ok": True})


# ======================
# MONTH BALANCE
# ======================
@finance_bp.route("/api/balance", methods=["GET"])
@login_required
@require_module("FINANCE")
def api_balance_get():
    month = (request.args.get("month") or "").strip()
    currency = (request.args.get("currency") or "").upper().strip()

    bank_id = get_bank_id_from_request()
    if bank_id is None:
        bank_id = get_default_bank_id()

    if is_all_banks(bank_id):
        return jsonify({"ok": False, "error": "Closing balance is per-bank. Select a specific bank account."}), 400

    if not month or not currency:
        return jsonify({"ok": False, "error": "month and currency are required"}), 400

    conn = db()
    row = conn.execute("""
        SELECT *
        FROM month_balances
        WHERE bank_id=? AND month_key=? AND currency=?
        LIMIT 1
    """, (bank_id, month, currency)).fetchone()
    conn.close()

    return jsonify({"ok": True, "data": dict(row) if row else None})


@finance_bp.route("/api/balance", methods=["POST"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_balance_upsert():
    data = request.json or {}

    month = (data.get("month_key") or "").strip()
    currency = (data.get("currency") or "").upper().strip()
    closing_balance = data.get("closing_balance")
    bank_id = data.get("bank_id")

    if is_all_banks(bank_id):
        return jsonify({"ok": False, "error": "Closing balance must be saved for a specific bank (not ALL)."}), 400

    if not month or not currency or closing_balance is None or not bank_id:
        return jsonify({"ok": False, "error": "bank_id, month_key, currency, closing_balance are required"}), 400

    note = data.get("note", "")
    now = now_iso()

    conn = db()
    conn.execute("""
        INSERT INTO month_balances (bank_id, month_key, currency, closing_balance, note, created_at, created_by)
        VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(bank_id, month_key, currency)
        DO UPDATE SET
          closing_balance=excluded.closing_balance,
          note=excluded.note,
          edited_at=?,
          edited_by=?
    """, (
        int(bank_id),
        month,
        currency,
        float(closing_balance),
        note,
        now,
        session["user"],
        now,
        session["user"]
    ))
    conn.commit()
    conn.close()

    return jsonify({"ok": True})