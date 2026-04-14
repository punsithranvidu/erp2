from flask import Blueprint, request, jsonify, session, current_app
from datetime import datetime
import pytz
from .db import connect

invoices_bp = Blueprint("invoices", __name__, url_prefix="/api/invoices")


def db():
    return connect(current_app.config["DATABASE_URL"])


def now_local():
    return datetime.now(pytz.timezone("Asia/Colombo"))


def reservation_lock_key(doc_type: str) -> int:
    return {
        "INV": 1,
        "QT": 2,
        "PO": 3,
    }.get((doc_type or "").upper(), 0)


def get_next_number(doc_type):
    now = now_local()
    year = now.year
    month = now.month

    conn = db()
    c = conn.cursor()

    c.execute("""
        SELECT number
        FROM document_numbers
        WHERE doc_type=%s AND year=%s AND status='RESTORED'
        ORDER BY number ASC
        LIMIT 1
    """, (doc_type, year))

    row = c.fetchone()

    if row:
        conn.close()
        return row["number"], year, month

    c.execute("""
        SELECT MAX(number) AS max_num
        FROM document_numbers
        WHERE doc_type=%s AND year=%s
    """, (doc_type, year))

    max_row = c.fetchone()
    max_num = max_row["max_num"] if max_row and max_row["max_num"] else 0

    conn.close()
    return max_num + 1, year, month


def format_number(doc_type, year, month, number):
    return f"{doc_type}-{year}/{str(month).zfill(2)}-{str(number).zfill(3)}"


@invoices_bp.route("/next")
def next_number():
    doc_type = (request.args.get("type") or "INV").strip().upper()

    if doc_type not in ("INV", "QT", "PO"):
        return jsonify({"ok": False, "error": "Invalid document type"}), 400

    num, year, month = get_next_number(doc_type)

    return jsonify({
        "ok": True,
        "number": format_number(doc_type, year, month, num)
    })


@invoices_bp.route("/reserve", methods=["POST"])
def reserve():
    data = request.json or {}
    doc_type = (data.get("type") or "INV").strip().upper()

    if doc_type not in ("INV", "QT", "PO"):
        return jsonify({"ok": False, "error": "Invalid document type"}), 400

    conn = db()
    c = conn.cursor()
    now = now_local()
    year = now.year
    month = now.month

    c.execute(
        "SELECT pg_advisory_xact_lock(%s, %s)",
        (reservation_lock_key(doc_type), year)
    )

    c.execute("""
        SELECT id, number
        FROM document_numbers
        WHERE doc_type=%s AND year=%s AND status='RESTORED'
        ORDER BY number ASC
        LIMIT 1
        FOR UPDATE
    """, (doc_type, year))

    restored = c.fetchone()

    if restored:
        num = int(restored["number"])
        c.execute("""
            UPDATE document_numbers
            SET status='RESERVED',
                reserved_by=%s,
                reserved_at=%s,
                restored_at=NULL
            WHERE id=%s
        """, (
            session.get("user"),
            now.isoformat(),
            restored["id"]
        ))
    else:
        c.execute("""
            SELECT COALESCE(MAX(number), 0) AS max_num
            FROM document_numbers
            WHERE doc_type=%s AND year=%s
        """, (doc_type, year))
        max_row = c.fetchone()
        num = int(max_row["max_num"] or 0) + 1

        c.execute("""
            INSERT INTO document_numbers
            (doc_type, year, month, number, status, reserved_by, reserved_at)
            VALUES (%s, %s, %s, %s, 'RESERVED', %s, %s)
        """, (
            doc_type,
            year,
            month,
            num,
            session.get("user"),
            now.isoformat()
        ))

    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "number": format_number(doc_type, year, month, num)
    })


@invoices_bp.route("/restore", methods=["POST"])
def restore():
    data = request.json or {}
    number = (data.get("number") or "").strip()

    if not number:
        return jsonify({"ok": False, "error": "Number is required"}), 400

    try:
        doc_type, rest = number.split("-", 1)
        year_month, num = rest.split("-")
        year, month = year_month.split("/")
        year = int(year)
        month = int(month)
        num = int(num)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid number format"}), 400

    conn = db()
    c = conn.cursor()

    c.execute("""
        UPDATE document_numbers
        SET status='RESTORED',
            restored_at=%s
        WHERE doc_type=%s AND year=%s AND month=%s AND number=%s
    """, (
        now_local().isoformat(),
        doc_type,
        year,
        month,
        num
    ))

    conn.commit()
    changed = c.rowcount
    conn.close()

    if changed == 0:
        return jsonify({"ok": False, "error": "Number not found"}), 404

    return jsonify({"ok": True})


@invoices_bp.route("/use", methods=["POST"])
def use():
    data = request.json or {}
    number = (data.get("number") or "").strip()

    if not number:
        return jsonify({"ok": False, "error": "Number is required"}), 400

    try:
        doc_type, rest = number.split("-", 1)
        year_month, num = rest.split("-")
        year, month = year_month.split("/")
        year = int(year)
        month = int(month)
        num = int(num)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid number format"}), 400

    conn = db()
    c = conn.cursor()

    c.execute("""
        UPDATE document_numbers
        SET status='USED',
            used_at=%s
        WHERE doc_type=%s AND year=%s AND month=%s AND number=%s
    """, (
        now_local().isoformat(),
        doc_type,
        year,
        month,
        num
    ))

    conn.commit()
    changed = c.rowcount
    conn.close()

    if changed == 0:
        return jsonify({"ok": False, "error": "Number not found"}), 404

    return jsonify({"ok": True})


@invoices_bp.route("/search")
def search():
    q = (request.args.get("q") or "").strip()

    if not q:
        return jsonify({"ok": False, "error": "Search value is required"}), 400

    conn = db()
    c = conn.cursor()

    c.execute("""
        SELECT doc_type, year, month, number, status
        FROM document_numbers
        WHERE doc_type || '-' || year::text || '/' || LPAD(month::text, 2, '0') || '-' || LPAD(number::text, 3, '0') = %s
        LIMIT 1
    """, (q,))

    row = c.fetchone()
    conn.close()

    if not row:
        return jsonify({
            "ok": True,
            "found": False
        })

    return jsonify({
        "ok": True,
        "found": True,
        "status": row["status"],
        "number": f"{row['doc_type']}-{row['year']}/{str(row['month']).zfill(2)}-{str(row['number']).zfill(3)}"
    })
