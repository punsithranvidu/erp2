from flask import Blueprint, render_template, request, jsonify, session, current_app
from functools import wraps
from datetime import datetime
from .db import connect, get_table_columns

hs_codes_bp = Blueprint("hs_codes", __name__)


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


def ensure_hs_code_tables():
    conn = db()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS hs_codes (
            id BIGSERIAL PRIMARY KEY,
            product_name TEXT NOT NULL,
            hs_code TEXT NOT NULL,
            proof_link TEXT,
            notes TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            updated_at TEXT,
            updated_by TEXT,
            is_deleted INTEGER NOT NULL DEFAULT 0
        )
    """)

    conn.commit()

    cols = get_table_columns(conn, "hs_codes")

    if "proof_link" not in cols:
        conn.execute("ALTER TABLE hs_codes ADD COLUMN proof_link TEXT")
    if "notes" not in cols:
        conn.execute("ALTER TABLE hs_codes ADD COLUMN notes TEXT")
    if "created_at" not in cols:
        conn.execute("ALTER TABLE hs_codes ADD COLUMN created_at TEXT")
    if "created_by" not in cols:
        conn.execute("ALTER TABLE hs_codes ADD COLUMN created_by TEXT")
    if "updated_at" not in cols:
        conn.execute("ALTER TABLE hs_codes ADD COLUMN updated_at TEXT")
    if "updated_by" not in cols:
        conn.execute("ALTER TABLE hs_codes ADD COLUMN updated_by TEXT")
    if "is_deleted" not in cols:
        conn.execute("ALTER TABLE hs_codes ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0")

    conn.commit()
    conn.close()


@hs_codes_bp.before_app_request
def _ensure_once():
    if not current_app.config.get("HS_CODES_TABLES_READY"):
        ensure_hs_code_tables()
        current_app.config["HS_CODES_TABLES_READY"] = True


def clean_text(value, max_len=500):
    return (value or "").strip()[:max_len]


def clean_hs_code(value):
    raw = (value or "").strip()
    # keep only digits and dots
    allowed = []
    for ch in raw:
        if ch.isdigit() or ch == ".":
            allowed.append(ch)
    return "".join(allowed)[:32]


@hs_codes_bp.route("/hs-codes", methods=["GET"])
@login_required
@require_module("HS_CODES")
def hs_codes_page():
    return render_template(
        "hs_codes.html",
        user=session.get("user"),
        role=session.get("role")
    )


@hs_codes_bp.route("/api/hs-codes", methods=["GET"])
@login_required
@require_module("HS_CODES")
def api_hs_codes_list():
    conn = db()
    q = clean_text(request.args.get("q"), 120)

    if q:
        like_q = f"%{q}%"
        rows = conn.execute("""
            SELECT *
            FROM hs_codes
            WHERE is_deleted=0
              AND (
                product_name LIKE %s
                OR hs_code LIKE %s
                OR proof_link LIKE %s
                OR notes LIKE %s
              )
            ORDER BY product_name ASC, hs_code ASC, id DESC
            LIMIT 500
        """, (like_q, like_q, like_q, like_q)).fetchall()
    else:
        rows = conn.execute("""
            SELECT *
            FROM hs_codes
            WHERE is_deleted=0
            ORDER BY product_name ASC, hs_code ASC, id DESC
            LIMIT 500
        """).fetchall()

    data = [dict(r) for r in rows]
    conn.close()
    return jsonify({"ok": True, "data": data})


@hs_codes_bp.route("/api/hs-codes", methods=["POST"])
@login_required
@require_module("HS_CODES", need_edit=True)
def api_hs_codes_create():
    conn = db()
    data = request.json or {}

    product_name = clean_text(data.get("product_name"), 255)
    hs_code = clean_hs_code(data.get("hs_code"))
    proof_link = clean_text(data.get("proof_link"), 1000)
    notes = clean_text(data.get("notes"), 1000)

    if not product_name:
        conn.close()
        return jsonify({"ok": False, "error": "Product name is required"}), 400

    if not hs_code:
        conn.close()
        return jsonify({"ok": False, "error": "HS code is required"}), 400

    conn.execute("""
        INSERT INTO hs_codes (
            product_name, hs_code, proof_link, notes,
            created_at, created_by, updated_at, updated_by, is_deleted
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0)
    """, (
        product_name,
        hs_code,
        proof_link or None,
        notes or None,
        now_iso(),
        session.get("user"),
        now_iso(),
        session.get("user"),
    ))

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "HS code added"})


@hs_codes_bp.route("/api/hs-codes/<int:item_id>", methods=["PUT"])
@login_required
@require_module("HS_CODES", need_edit=True)
def api_hs_codes_update(item_id):
    conn = db()
    data = request.json or {}

    row = conn.execute("""
        SELECT *
        FROM hs_codes
        WHERE id=%s AND is_deleted=0
        LIMIT 1
    """, (item_id,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "HS code item not found"}), 404

    product_name = clean_text(data.get("product_name"), 255)
    hs_code = clean_hs_code(data.get("hs_code"))
    proof_link = clean_text(data.get("proof_link"), 1000)
    notes = clean_text(data.get("notes"), 1000)

    if not product_name:
        conn.close()
        return jsonify({"ok": False, "error": "Product name is required"}), 400

    if not hs_code:
        conn.close()
        return jsonify({"ok": False, "error": "HS code is required"}), 400

    conn.execute("""
        UPDATE hs_codes
        SET product_name=%s,
            hs_code=%s,
            proof_link=%s,
            notes=%s,
            updated_at=%s,
            updated_by=%s
        WHERE id=%s
    """, (
        product_name,
        hs_code,
        proof_link or None,
        notes or None,
        now_iso(),
        session.get("user"),
        item_id
    ))

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "HS code updated"})


@hs_codes_bp.route("/api/hs-codes/<int:item_id>", methods=["DELETE"])
@login_required
@require_module("HS_CODES", need_edit=True)
def api_hs_codes_delete(item_id):
    conn = db()

    row = conn.execute("""
        SELECT id
        FROM hs_codes
        WHERE id=%s AND is_deleted=0
        LIMIT 1
    """, (item_id,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "HS code item not found"}), 404

    conn.execute("""
        UPDATE hs_codes
        SET is_deleted=1,
            updated_at=%s,
            updated_by=%s
        WHERE id=%s
    """, (
        now_iso(),
        session.get("user"),
        item_id
    ))

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "message": "HS code deleted"})
