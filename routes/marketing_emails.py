from datetime import datetime
from functools import wraps
from io import StringIO
import json
import re
from zoneinfo import ZoneInfo

import psycopg
from flask import Blueprint, Response, current_app, jsonify, redirect, render_template, request, session, url_for

from .db import connect, get_table_columns


marketing_emails_bp = Blueprint("marketing_emails", __name__)
APP_TZ = ZoneInfo("Asia/Colombo")


PREDEFINED_CATEGORIES = [
    "Spice",
    "Tea",
    "Coconut",
    "Spice & Tea",
    "Tea & Coconut",
    "Spice & Coconut",
    "Spice, Tea & Coconut",
]

PREDEFINED_COUNTRIES = [
    "UAE",
    "Dubai",
    "Australia",
    "Singapore",
    "Russia",
    "China",
    "Germany",
    "Belgium",
    "UK",
    "USA",
]

VERIFICATION_STATUSES = [
    "Not Yet Checked",
    "Checked",
    "Invalid",
]

CALL_STATUSES = [
    "Not Yet Called",
    "No Number Available",
    "Not Answered",
    "Called - Positive",
    "Called - Negative",
    "Call Positive - Followed Up",
]

EXPORT_CONFIRMATION_STATUSES = [
    "Pending",
    "Confirmed",
    "Send First Email",
    "Send Second Email",
    "Custom Send Stage",
    "Invalid Email List",
]

def db():
    return connect(current_app.config["DATABASE_URL"])


def now_iso():
    return datetime.now(APP_TZ).replace(tzinfo=None).isoformat(timespec="seconds")


def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if "user" not in session:
            if request.path.startswith("/api/"):
                return jsonify({"ok": False, "error": "Login required"}), 401
            return redirect(url_for("auth.login"))
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
                if request.path.startswith("/api/"):
                    return jsonify({
                        "ok": False,
                        "error": f"No permission for {module}{' (edit)' if need_edit else ''}",
                    }), 403
                return redirect(url_for("pages.dashboard"))
            return f(*args, **kwargs)

        return wrapped

    return deco


def clean_text(value, max_len=500):
    return (value or "").strip()[:max_len]


def normalize_email(value):
    return clean_text(value, 320).lower()


def is_valid_email(value):
    if not value:
        return False
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", value))


def normalize_url(url: str) -> str:
    fn = current_app.config.get("NORMALIZE_URL_FUNC")
    if callable(fn):
        return fn(url)

    u = (url or "").strip()
    if not u:
        return ""
    low = u.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return u
    return "https://" + u


def slug_part(value):
    raw = clean_text(value, 80).lower()
    out = []
    for ch in raw:
        if ch.isalnum():
            out.append(ch)
        elif ch in (" ", "-", "_", "&", ","):
            out.append("_")
    slug = "".join(out).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or "emails"


def parse_id_list(value):
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = str(value or "").split(",")

    ids = []
    seen = set()
    for item in raw_items:
        try:
            item_id = int(item)
        except Exception:
            continue
        if item_id <= 0 or item_id in seen:
            continue
        ids.append(item_id)
        seen.add(item_id)
    return ids


def selected_ids_to_text(ids):
    return ",".join(str(i) for i in ids)


def normalize_verification_status(value):
    status = clean_text(value, 80)
    if status in ("Not Checked Yet", "Duplicate", "Confirmed Ready"):
        return "Not Yet Checked"
    return status


def normalize_export_status(value):
    status = clean_text(value, 80)
    mapping = {
        "Sent First Email": "Send First Email",
        "Sent Second Email": "Send Second Email",
        "Completed": "Invalid Email List",
        "Blocked": "Invalid Email List",
    }
    return mapping.get(status, status)


def clean_call_status(value):
    status = clean_text(value, 80) or "Not Yet Called"
    return status if status in CALL_STATUSES else "Not Yet Called"


def load_extra_send_dates(value):
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [clean_text(item, 32) for item in parsed]


def normalize_extra_send_dates(value, existing=None):
    if isinstance(value, list):
        raw = value
    else:
        raw = []
    dates = [clean_text(item, 32) for item in raw]
    existing_dates = existing or []
    if len(dates) < len(existing_dates):
        dates.extend(existing_dates[len(dates):])
    return dates


def send_round_for_status(status, requested_round=0):
    if status == "Send First Email":
        return 1
    if status == "Send Second Email":
        return 2
    if status == "Custom Send Stage":
        try:
            return max(3, int(requested_round or 3))
        except Exception:
            return 3
    return 0


def ensure_marketing_email_tables():
    conn = db()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS marketing_email_leads (
            id BIGSERIAL PRIMARY KEY,
            company_name TEXT,
            email TEXT NOT NULL,
            email_normalized TEXT NOT NULL UNIQUE,
            website TEXT,
            category TEXT NOT NULL,
            country TEXT NOT NULL,
            note TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            edited_by TEXT,
            edited_at TEXT,
            verification_status TEXT NOT NULL DEFAULT 'Not Yet Checked',
            call_status TEXT NOT NULL DEFAULT 'Not Yet Called',
            is_deleted INTEGER NOT NULL DEFAULT 0,
            deleted_at TEXT,
            deleted_by TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS marketing_email_export_files (
            id BIGSERIAL PRIMARY KEY,
            file_name TEXT NOT NULL,
            category TEXT,
            country TEXT,
            lead_count INTEGER NOT NULL DEFAULT 0,
            confirmation_status TEXT NOT NULL DEFAULT 'Pending',
            first_send_date TEXT,
            second_send_date TEXT,
            send_round INTEGER NOT NULL DEFAULT 0,
            extra_send_dates TEXT,
            notes TEXT,
            selected_lead_ids TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            edited_by TEXT,
            edited_at TEXT,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            deleted_at TEXT,
            deleted_by TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS marketing_email_download_permissions (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL UNIQUE,
            can_download INTEGER NOT NULL DEFAULT 0,
            notes TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            updated_at TEXT,
            updated_by TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS marketing_email_download_requests (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            request_note TEXT,
            status TEXT NOT NULL DEFAULT 'PENDING',
            requested_at TEXT NOT NULL,
            decided_at TEXT,
            decided_by TEXT,
            decision_note TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS marketing_email_download_logs (
            id BIGSERIAL PRIMARY KEY,
            export_file_id BIGINT NOT NULL,
            file_name TEXT NOT NULL,
            downloaded_by_user_id BIGINT,
            downloaded_by TEXT NOT NULL,
            downloaded_at TEXT NOT NULL,
            FOREIGN KEY(export_file_id) REFERENCES marketing_email_export_files(id)
        )
    """)

    conn.commit()

    lead_cols = get_table_columns(conn, "marketing_email_leads")
    lead_columns = {
        "company_name": "TEXT",
        "email": "TEXT",
        "email_normalized": "TEXT",
        "website": "TEXT",
        "category": "TEXT",
        "country": "TEXT",
        "note": "TEXT",
        "created_by": "TEXT",
        "created_at": "TEXT",
        "edited_by": "TEXT",
        "edited_at": "TEXT",
        "verification_status": "TEXT NOT NULL DEFAULT 'Not Yet Checked'",
        "call_status": "TEXT NOT NULL DEFAULT 'Not Yet Called'",
        "is_deleted": "INTEGER NOT NULL DEFAULT 0",
        "deleted_at": "TEXT",
        "deleted_by": "TEXT",
    }
    for col, sql_type in lead_columns.items():
        if col not in lead_cols:
            conn.execute(f"ALTER TABLE marketing_email_leads ADD COLUMN {col} {sql_type}")

    export_cols = get_table_columns(conn, "marketing_email_export_files")
    export_columns = {
        "file_name": "TEXT",
        "category": "TEXT",
        "country": "TEXT",
        "lead_count": "INTEGER NOT NULL DEFAULT 0",
        "confirmation_status": "TEXT NOT NULL DEFAULT 'Pending'",
        "first_send_date": "TEXT",
        "second_send_date": "TEXT",
        "send_round": "INTEGER NOT NULL DEFAULT 0",
        "extra_send_dates": "TEXT",
        "notes": "TEXT",
        "selected_lead_ids": "TEXT",
        "created_by": "TEXT",
        "created_at": "TEXT",
        "edited_by": "TEXT",
        "edited_at": "TEXT",
        "is_deleted": "INTEGER NOT NULL DEFAULT 0",
        "deleted_at": "TEXT",
        "deleted_by": "TEXT",
    }
    for col, sql_type in export_columns.items():
        if col not in export_cols:
            conn.execute(f"ALTER TABLE marketing_email_export_files ADD COLUMN {col} {sql_type}")

    perm_columns = {
        "user_id": "BIGINT",
        "can_download": "INTEGER NOT NULL DEFAULT 0",
        "notes": "TEXT",
        "created_at": "TEXT",
        "created_by": "TEXT",
        "updated_at": "TEXT",
        "updated_by": "TEXT",
    }
    perm_cols = get_table_columns(conn, "marketing_email_download_permissions")
    for col, sql_type in perm_columns.items():
        if col not in perm_cols:
            conn.execute(f"ALTER TABLE marketing_email_download_permissions ADD COLUMN {col} {sql_type}")

    req_columns = {
        "user_id": "BIGINT",
        "request_note": "TEXT",
        "status": "TEXT NOT NULL DEFAULT 'PENDING'",
        "requested_at": "TEXT",
        "decided_at": "TEXT",
        "decided_by": "TEXT",
        "decision_note": "TEXT",
    }
    req_cols = get_table_columns(conn, "marketing_email_download_requests")
    for col, sql_type in req_columns.items():
        if col not in req_cols:
            conn.execute(f"ALTER TABLE marketing_email_download_requests ADD COLUMN {col} {sql_type}")

    log_columns = {
        "export_file_id": "BIGINT",
        "file_name": "TEXT",
        "downloaded_by_user_id": "BIGINT",
        "downloaded_by": "TEXT",
        "downloaded_at": "TEXT",
    }
    log_cols = get_table_columns(conn, "marketing_email_download_logs")
    for col, sql_type in log_columns.items():
        if col not in log_cols:
            conn.execute(f"ALTER TABLE marketing_email_download_logs ADD COLUMN {col} {sql_type}")

    conn.execute("""
        UPDATE marketing_email_leads
        SET email_normalized=LOWER(TRIM(email))
        WHERE email_normalized IS NULL OR TRIM(email_normalized)=''
    """)
    conn.execute("""
        UPDATE marketing_email_leads
        SET verification_status='Not Yet Checked'
        WHERE verification_status IS NULL OR TRIM(verification_status)=''
    """)
    conn.execute("""
        UPDATE marketing_email_leads
        SET verification_status='Not Yet Checked'
        WHERE verification_status IN ('Not Checked Yet', 'Duplicate', 'Confirmed Ready')
    """)
    conn.execute("""
        UPDATE marketing_email_leads
        SET call_status='Not Yet Called'
        WHERE call_status IS NULL OR TRIM(call_status)=''
    """)
    conn.execute("""
        UPDATE marketing_email_export_files
        SET confirmation_status='Pending'
        WHERE confirmation_status IS NULL OR TRIM(confirmation_status)=''
    """)
    conn.execute("""
        UPDATE marketing_email_export_files
        SET confirmation_status='Send First Email'
        WHERE confirmation_status='Sent First Email'
    """)
    conn.execute("""
        UPDATE marketing_email_export_files
        SET confirmation_status='Send Second Email'
        WHERE confirmation_status='Sent Second Email'
    """)
    conn.execute("""
        UPDATE marketing_email_export_files
        SET confirmation_status='Invalid Email List'
        WHERE confirmation_status IN ('Blocked', 'Completed')
    """)
    conn.execute("""
        UPDATE marketing_email_export_files
        SET send_round = CASE
            WHEN confirmation_status='Send First Email' THEN GREATEST(COALESCE(send_round, 0), 1)
            WHEN confirmation_status='Send Second Email' THEN GREATEST(COALESCE(send_round, 0), 2)
            ELSE COALESCE(send_round, 0)
        END
    """)

    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_marketing_email_leads_email_normalized
        ON marketing_email_leads (email_normalized)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_marketing_email_leads_created_by
        ON marketing_email_leads (created_by)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_marketing_email_leads_filters
        ON marketing_email_leads (category, country, verification_status, call_status)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_marketing_email_exports_created_by
        ON marketing_email_export_files (created_by)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_marketing_email_requests_user_status
        ON marketing_email_download_requests (user_id, status)
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_marketing_email_download_permissions_user
        ON marketing_email_download_permissions (user_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_marketing_email_download_logs_export
        ON marketing_email_download_logs (export_file_id)
    """)

    conn.commit()
    conn.close()


@marketing_emails_bp.before_app_request
def _ensure_once():
    if not current_app.config.get("MARKETING_EMAILS_TABLES_READY"):
        ensure_marketing_email_tables()
        current_app.config["MARKETING_EMAILS_TABLES_READY"] = True


def current_user_id():
    return session.get("uid")


def can_access_lead(row):
    return bool(row) and (is_admin() or row["created_by"] == session.get("user"))


def can_access_export(row):
    return bool(row) and (is_admin() or row["created_by"] == session.get("user"))


def get_download_permission(conn, user_id):
    row = conn.execute("""
        SELECT can_download
        FROM marketing_email_download_permissions
        WHERE user_id=%s
        LIMIT 1
    """, (user_id,)).fetchone()
    return int(row["can_download"] or 0) == 1 if row else False


def fetch_accessible_leads(conn, lead_ids):
    if not lead_ids:
        return []

    if is_admin():
        rows = conn.execute("""
            SELECT *
            FROM marketing_email_leads
            WHERE is_deleted=0 AND id = ANY(%s)
            ORDER BY id ASC
        """, (lead_ids,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT *
            FROM marketing_email_leads
            WHERE is_deleted=0
              AND created_by=%s
              AND id = ANY(%s)
            ORDER BY id ASC
        """, (session.get("user"), lead_ids)).fetchall()

    return [dict(r) for r in rows]


@marketing_emails_bp.route("/marketing-emails", methods=["GET"])
@login_required
@require_module("MARKETING_EMAILS")
def marketing_emails_page():
    return render_template(
        "marketing_emails.html",
        user=session.get("user"),
        role=session.get("role"),
    )


@marketing_emails_bp.route("/api/marketing-emails/options", methods=["GET"])
@login_required
@require_module("MARKETING_EMAILS")
def api_options():
    conn = db()

    option_where = ["is_deleted=0", "category IS NOT NULL", "TRIM(category) <> ''"]
    country_where = ["is_deleted=0", "country IS NOT NULL", "TRIM(country) <> ''"]
    option_params = []
    country_params = []
    if not is_admin():
        option_where.append("created_by=%s")
        country_where.append("created_by=%s")
        option_params.append(session.get("user"))
        country_params.append(session.get("user"))

    custom_categories = conn.execute(f"""
        SELECT DISTINCT category
        FROM marketing_email_leads
        WHERE {" AND ".join(option_where)}
        ORDER BY category ASC
        LIMIT 300
    """, option_params).fetchall()
    custom_countries = conn.execute(f"""
        SELECT DISTINCT country
        FROM marketing_email_leads
        WHERE {" AND ".join(country_where)}
        ORDER BY country ASC
        LIMIT 300
    """, country_params).fetchall()

    employees = []
    if is_admin():
        employees = conn.execute("""
            SELECT id, username, full_name, role, active
            FROM users
            WHERE active=1
            ORDER BY username ASC
        """).fetchall()

    can_download = True if is_admin() else get_download_permission(conn, current_user_id())
    conn.close()

    categories = list(PREDEFINED_CATEGORIES)
    for r in custom_categories:
        value = r["category"]
        if value and value not in categories:
            categories.append(value)

    countries = list(PREDEFINED_COUNTRIES)
    for r in custom_countries:
        value = r["country"]
        if value and value not in countries:
            countries.append(value)

    return jsonify({
        "ok": True,
        "data": {
            "categories": categories,
            "countries": countries,
            "verification_statuses": VERIFICATION_STATUSES,
            "call_statuses": CALL_STATUSES,
            "export_statuses": EXPORT_CONFIRMATION_STATUSES,
            "employees": [dict(r) for r in employees],
            "can_download": can_download,
            "is_admin": is_admin(),
        },
    })


@marketing_emails_bp.route("/api/marketing-emails/leads", methods=["GET"])
@login_required
@require_module("MARKETING_EMAILS")
def api_leads_list():
    q = clean_text(request.args.get("q"), 120)
    employee = clean_text(request.args.get("employee"), 80)
    category = clean_text(request.args.get("category"), 120)
    country = clean_text(request.args.get("country"), 120)
    status = clean_text(request.args.get("status"), 80)
    call_status = clean_text(request.args.get("call_status"), 80)

    where = ["is_deleted=0"]
    params = []

    if is_admin():
        if employee and employee.upper() != "ALL":
            where.append("created_by=%s")
            params.append(employee)
    else:
        where.append("created_by=%s")
        params.append(session.get("user"))

    if category and category.upper() != "ALL":
        where.append("category=%s")
        params.append(category)
    if country and country.upper() != "ALL":
        where.append("country=%s")
        params.append(country)
    if status and status.upper() != "ALL":
        where.append("verification_status=%s")
        params.append(normalize_verification_status(status))
    if call_status and call_status.upper() != "ALL":
        where.append("call_status=%s")
        params.append(call_status)
    if q:
        like_q = f"%{q}%"
        where.append("""
            (
                company_name ILIKE %s
                OR email ILIKE %s
                OR website ILIKE %s
                OR note ILIKE %s
            )
        """)
        params.extend([like_q, like_q, like_q, like_q])

    conn = db()
    rows = conn.execute(f"""
        SELECT *
        FROM marketing_email_leads
        WHERE {" AND ".join(where)}
        ORDER BY id DESC
        LIMIT 1500
    """, params).fetchall()
    conn.close()

    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


@marketing_emails_bp.route("/api/marketing-emails/leads", methods=["POST"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_leads_create():
    data = request.json or {}
    company_name = clean_text(data.get("company_name"), 255) or None
    email = normalize_email(data.get("email"))
    website = normalize_url(clean_text(data.get("website"), 1000)) or None
    category = clean_text(data.get("category"), 120)
    country = clean_text(data.get("country"), 120)
    call_status = clean_call_status(data.get("call_status"))
    note = clean_text(data.get("note"), 1500) or None

    if not is_valid_email(email):
        return jsonify({"ok": False, "error": "Valid email is required"}), 400
    if not category:
        return jsonify({"ok": False, "error": "Category is required"}), 400
    if not country:
        return jsonify({"ok": False, "error": "Country is required"}), 400

    conn = db()
    duplicate = conn.execute("""
        SELECT id
        FROM marketing_email_leads
        WHERE email_normalized=%s
        LIMIT 1
    """, (email,)).fetchone()
    if duplicate:
        conn.close()
        return jsonify({"ok": False, "error": "This email is already used"}), 400

    try:
        row = conn.execute("""
            INSERT INTO marketing_email_leads (
                company_name, email, email_normalized, website,
                category, country, note, call_status,
                created_by, created_at, edited_by, edited_at,
                verification_status, is_deleted
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL,'Not Yet Checked',0)
            RETURNING *
        """, (
            company_name,
            email,
            email,
            website,
            category,
            country,
            note,
            call_status,
            session.get("user"),
            now_iso(),
        )).fetchone()
        conn.commit()
    except psycopg.IntegrityError:
        conn.rollback()
        conn.close()
        return jsonify({"ok": False, "error": "This email is already used"}), 400

    out = dict(row)
    conn.close()
    return jsonify({"ok": True, "message": "Email saved successfully", "data": out})


@marketing_emails_bp.route("/api/marketing-emails/leads/<int:lead_id>", methods=["PUT"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_leads_update(lead_id):
    data = request.json or {}

    conn = db()
    row = conn.execute("""
        SELECT *
        FROM marketing_email_leads
        WHERE id=%s AND is_deleted=0
        LIMIT 1
    """, (lead_id,)).fetchone()

    if not can_access_lead(row):
        conn.close()
        return jsonify({"ok": False, "error": "Lead not found or no access"}), 404

    company_name = clean_text(data.get("company_name"), 255) or None
    email = normalize_email(data.get("email"))
    website = normalize_url(clean_text(data.get("website"), 1000)) or None
    category = clean_text(data.get("category"), 120)
    country = clean_text(data.get("country"), 120)
    call_status = clean_call_status(data.get("call_status"))
    note = clean_text(data.get("note"), 1500) or None

    if not is_valid_email(email):
        conn.close()
        return jsonify({"ok": False, "error": "Valid email is required"}), 400
    if not category:
        conn.close()
        return jsonify({"ok": False, "error": "Category is required"}), 400
    if not country:
        conn.close()
        return jsonify({"ok": False, "error": "Country is required"}), 400

    duplicate = conn.execute("""
        SELECT id
        FROM marketing_email_leads
        WHERE email_normalized=%s AND id<>%s
        LIMIT 1
    """, (email, lead_id)).fetchone()
    if duplicate:
        conn.close()
        return jsonify({"ok": False, "error": "This email is already used"}), 400

    try:
        updated = conn.execute("""
            UPDATE marketing_email_leads
            SET company_name=%s,
                email=%s,
                email_normalized=%s,
                website=%s,
                category=%s,
                country=%s,
                call_status=%s,
                note=%s,
                edited_by=%s,
                edited_at=%s
            WHERE id=%s
            RETURNING *
        """, (
            company_name,
            email,
            email,
            website,
            category,
            country,
            call_status,
            note,
            session.get("user"),
            now_iso(),
            lead_id,
        )).fetchone()
        conn.commit()
    except psycopg.IntegrityError:
        conn.rollback()
        conn.close()
        return jsonify({"ok": False, "error": "This email is already used"}), 400

    conn.close()
    return jsonify({"ok": True, "message": "Lead updated", "data": dict(updated)})


@marketing_emails_bp.route("/api/marketing-emails/leads/<int:lead_id>/verify", methods=["POST"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_lead_verify(lead_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    status = normalize_verification_status((request.json or {}).get("verification_status"))
    if status not in VERIFICATION_STATUSES:
        return jsonify({"ok": False, "error": "Invalid verification status"}), 400

    conn = db()
    row = conn.execute("""
        UPDATE marketing_email_leads
        SET verification_status=%s,
            edited_by=%s,
            edited_at=%s
        WHERE id=%s AND is_deleted=0
        RETURNING *
    """, (status, session.get("user"), now_iso(), lead_id)).fetchone()
    conn.commit()
    conn.close()

    if not row:
        return jsonify({"ok": False, "error": "Lead not found"}), 404
    return jsonify({"ok": True, "data": dict(row)})


@marketing_emails_bp.route("/api/marketing-emails/leads/<int:lead_id>/call-status", methods=["POST"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_lead_call_status(lead_id):
    status = clean_call_status((request.json or {}).get("call_status"))

    conn = db()
    existing = conn.execute("""
        SELECT *
        FROM marketing_email_leads
        WHERE id=%s AND is_deleted=0
        LIMIT 1
    """, (lead_id,)).fetchone()

    if not can_access_lead(existing):
        conn.close()
        return jsonify({"ok": False, "error": "Lead not found or no access"}), 404

    row = conn.execute("""
        UPDATE marketing_email_leads
        SET call_status=%s,
            edited_by=%s,
            edited_at=%s
        WHERE id=%s AND is_deleted=0
        RETURNING *
    """, (status, session.get("user"), now_iso(), lead_id)).fetchone()
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "data": dict(row)})


@marketing_emails_bp.route("/api/marketing-emails/leads/bulk-verify", methods=["POST"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_leads_bulk_verify():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    ids = parse_id_list(data.get("lead_ids"))
    status = normalize_verification_status(data.get("verification_status"))
    if not ids:
        return jsonify({"ok": False, "error": "Select at least one lead"}), 400
    if status not in VERIFICATION_STATUSES:
        return jsonify({"ok": False, "error": "Invalid verification status"}), 400

    conn = db()
    rows = conn.execute("""
        UPDATE marketing_email_leads
        SET verification_status=%s,
            edited_by=%s,
            edited_at=%s
        WHERE is_deleted=0 AND id = ANY(%s)
        RETURNING *
    """, (status, session.get("user"), now_iso(), ids)).fetchall()
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "message": "Leads updated", "data": [dict(r) for r in rows]})


@marketing_emails_bp.route("/api/marketing-emails/leads/<int:lead_id>/delete", methods=["POST"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_leads_soft_delete(lead_id):
    conn = db()
    row = conn.execute("""
        SELECT *
        FROM marketing_email_leads
        WHERE id=%s AND is_deleted=0
        LIMIT 1
    """, (lead_id,)).fetchone()

    if not can_access_lead(row):
        conn.close()
        return jsonify({"ok": False, "error": "Lead not found or no access"}), 404

    deleted = conn.execute("""
        UPDATE marketing_email_leads
        SET is_deleted=1,
            deleted_at=%s,
            deleted_by=%s,
            edited_at=%s,
            edited_by=%s
        WHERE id=%s
        RETURNING *
    """, (now_iso(), session.get("user"), now_iso(), session.get("user"), lead_id)).fetchone()
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "message": "Lead moved to trash", "data": dict(deleted)})


@marketing_emails_bp.route("/api/marketing-emails/exports", methods=["GET"])
@login_required
@require_module("MARKETING_EMAILS")
def api_exports_list():
    employee = clean_text(request.args.get("employee"), 80)
    category = clean_text(request.args.get("category"), 120)
    country = clean_text(request.args.get("country"), 120)
    status = clean_text(request.args.get("status"), 80)

    where = ["is_deleted=0"]
    params = []
    if is_admin():
        if employee and employee.upper() != "ALL":
            where.append("created_by=%s")
            params.append(employee)
    else:
        where.append("created_by=%s")
        params.append(session.get("user"))

    if category and category.upper() != "ALL":
        where.append("category=%s")
        params.append(category)
    if country and country.upper() != "ALL":
        where.append("country=%s")
        params.append(country)
    if status and status.upper() != "ALL":
        where.append("confirmation_status=%s")
        params.append(status)

    conn = db()
    rows = conn.execute(f"""
        SELECT *
        FROM marketing_email_export_files
        WHERE {" AND ".join(where)}
        ORDER BY id DESC
        LIMIT 1000
    """, params).fetchall()
    conn.close()

    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


@marketing_emails_bp.route("/api/marketing-emails/exports", methods=["POST"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_exports_create():
    data = request.json or {}
    lead_ids = parse_id_list(data.get("lead_ids"))
    category = clean_text(data.get("category"), 120)
    country = clean_text(data.get("country"), 120)
    confirmation_status = normalize_export_status(data.get("confirmation_status")) or "Pending"
    first_send_date = clean_text(data.get("first_send_date"), 32) or None
    second_send_date = clean_text(data.get("second_send_date"), 32) or None
    send_round = send_round_for_status(confirmation_status, data.get("send_round"))
    extra_send_dates = normalize_extra_send_dates(data.get("extra_send_dates"))
    notes = clean_text(data.get("notes"), 1500) or None
    file_name = clean_text(data.get("file_name"), 180)

    if confirmation_status not in EXPORT_CONFIRMATION_STATUSES:
        return jsonify({"ok": False, "error": "Invalid confirmation status"}), 400
    if not lead_ids:
        return jsonify({"ok": False, "error": "Select at least one lead"}), 400

    conn = db()
    leads = fetch_accessible_leads(conn, lead_ids)
    if not leads:
        conn.close()
        return jsonify({"ok": False, "error": "No accessible leads selected"}), 400

    accessible_ids = [int(r["id"]) for r in leads]
    if len(accessible_ids) != len(lead_ids):
        conn.close()
        return jsonify({"ok": False, "error": "Some selected leads are not accessible"}), 403

    if not category:
        category = leads[0].get("category") or ""
    if not country:
        country = leads[0].get("country") or ""
    if not file_name:
        file_name = f"{slug_part(category)}_{slug_part(country)}.txt"
    if not file_name.lower().endswith(".txt"):
        file_name += ".txt"

    row = conn.execute("""
        INSERT INTO marketing_email_export_files (
            file_name, category, country, lead_count, confirmation_status,
            first_send_date, second_send_date, send_round, extra_send_dates,
            notes, selected_lead_ids,
            created_by, created_at, edited_by, edited_at, is_deleted
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,NULL,0)
        RETURNING *
    """, (
        file_name,
        category or None,
        country or None,
        len(accessible_ids),
        confirmation_status,
        first_send_date,
        second_send_date,
        send_round,
        json.dumps(extra_send_dates),
        notes,
        selected_ids_to_text(accessible_ids),
        session.get("user"),
        now_iso(),
    )).fetchone()
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "message": "Export file record created", "data": dict(row)})


@marketing_emails_bp.route("/api/marketing-emails/exports/<int:export_id>", methods=["PUT"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_exports_update(export_id):
    data = request.json or {}

    conn = db()
    row = conn.execute("""
        SELECT *
        FROM marketing_email_export_files
        WHERE id=%s AND is_deleted=0
        LIMIT 1
    """, (export_id,)).fetchone()
    if not can_access_export(row):
        conn.close()
        return jsonify({"ok": False, "error": "Export file not found or no access"}), 404

    file_name = clean_text(data.get("file_name"), 180)
    category = clean_text(data.get("category"), 120) or None
    country = clean_text(data.get("country"), 120) or None
    confirmation_status = normalize_export_status(data.get("confirmation_status")) or "Pending"
    first_send_date = clean_text(data.get("first_send_date"), 32) or None
    second_send_date = clean_text(data.get("second_send_date"), 32) or None
    existing_extra_dates = load_extra_send_dates(row.get("extra_send_dates"))
    send_round = send_round_for_status(confirmation_status, data.get("send_round"))
    extra_send_dates = normalize_extra_send_dates(data.get("extra_send_dates"), existing_extra_dates)
    notes = clean_text(data.get("notes"), 1500) or None

    if not file_name:
        conn.close()
        return jsonify({"ok": False, "error": "File name is required"}), 400
    if not file_name.lower().endswith(".txt"):
        file_name += ".txt"
    if confirmation_status not in EXPORT_CONFIRMATION_STATUSES:
        conn.close()
        return jsonify({"ok": False, "error": "Invalid confirmation status"}), 400

    updated = conn.execute("""
        UPDATE marketing_email_export_files
        SET file_name=%s,
            category=%s,
            country=%s,
            confirmation_status=%s,
            first_send_date=%s,
            second_send_date=%s,
            send_round=%s,
            extra_send_dates=%s,
            notes=%s,
            edited_by=%s,
            edited_at=%s
        WHERE id=%s
        RETURNING *
    """, (
        file_name,
        category,
        country,
        confirmation_status,
        first_send_date,
        second_send_date,
        send_round,
        json.dumps(extra_send_dates),
        notes,
        session.get("user"),
        now_iso(),
        export_id,
    )).fetchone()
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "message": "Export file updated", "data": dict(updated)})


@marketing_emails_bp.route("/api/marketing-emails/exports/<int:export_id>/delete", methods=["POST"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_exports_soft_delete(export_id):
    conn = db()
    row = conn.execute("""
        SELECT *
        FROM marketing_email_export_files
        WHERE id=%s AND is_deleted=0
        LIMIT 1
    """, (export_id,)).fetchone()

    if not can_access_export(row):
        conn.close()
        return jsonify({"ok": False, "error": "Export file not found or no access"}), 404

    deleted = conn.execute("""
        UPDATE marketing_email_export_files
        SET is_deleted=1,
            deleted_at=%s,
            deleted_by=%s,
            edited_at=%s,
            edited_by=%s
        WHERE id=%s
        RETURNING *
    """, (now_iso(), session.get("user"), now_iso(), session.get("user"), export_id)).fetchone()
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "message": "Export file moved to trash", "data": dict(deleted)})


@marketing_emails_bp.route("/api/marketing-emails/exports/<int:export_id>/download", methods=["GET"])
@login_required
@require_module("MARKETING_EMAILS")
def api_export_download(export_id):
    conn = db()
    export_row = conn.execute("""
        SELECT *
        FROM marketing_email_export_files
        WHERE id=%s AND is_deleted=0
        LIMIT 1
    """, (export_id,)).fetchone()

    if not can_access_export(export_row):
        conn.close()
        return jsonify({"ok": False, "error": "Export file not found or no access"}), 404

    if not is_admin() and not get_download_permission(conn, current_user_id()):
        conn.close()
        return jsonify({
            "ok": False,
            "error": "Download permission is blocked. Please request access from admin.",
        }), 403

    lead_ids = parse_id_list(export_row["selected_lead_ids"])
    leads = fetch_accessible_leads(conn, lead_ids)

    output = StringIO()
    for lead in leads:
        output.write((lead.get("email") or "").strip())
        output.write("\n")

    file_name = export_row["file_name"] or "marketing_emails.txt"
    conn.execute("""
        INSERT INTO marketing_email_download_logs (
            export_file_id, file_name, downloaded_by_user_id, downloaded_by, downloaded_at
        )
        VALUES (%s,%s,%s,%s,%s)
    """, (
        export_id,
        file_name,
        current_user_id(),
        session.get("user"),
        now_iso(),
    ))
    conn.commit()
    conn.close()

    return Response(
        output.getvalue(),
        mimetype="text/plain",
        headers={"Content-Disposition": f"attachment; filename={file_name}"},
    )


@marketing_emails_bp.route("/api/marketing-emails/trash", methods=["GET"])
@login_required
@require_module("MARKETING_EMAILS")
def api_trash_list():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    leads = conn.execute("""
        SELECT *
        FROM marketing_email_leads
        WHERE is_deleted=1
        ORDER BY deleted_at DESC NULLS LAST, id DESC
        LIMIT 1000
    """).fetchall()
    exports = conn.execute("""
        SELECT *
        FROM marketing_email_export_files
        WHERE is_deleted=1
        ORDER BY deleted_at DESC NULLS LAST, id DESC
        LIMIT 1000
    """).fetchall()
    conn.close()

    return jsonify({
        "ok": True,
        "data": {
            "leads": [dict(r) for r in leads],
            "exports": [dict(r) for r in exports],
        },
    })


@marketing_emails_bp.route("/api/marketing-emails/trash/<item_type>/<int:item_id>/recover", methods=["POST"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_trash_recover(item_type, item_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    if item_type == "lead":
        row = conn.execute("""
            UPDATE marketing_email_leads
            SET is_deleted=0,
                deleted_at=NULL,
                deleted_by=NULL,
                edited_at=%s,
                edited_by=%s
            WHERE id=%s AND is_deleted=1
            RETURNING *
        """, (now_iso(), session.get("user"), item_id)).fetchone()
    elif item_type == "export":
        row = conn.execute("""
            UPDATE marketing_email_export_files
            SET is_deleted=0,
                deleted_at=NULL,
                deleted_by=NULL,
                edited_at=%s,
                edited_by=%s
            WHERE id=%s AND is_deleted=1
            RETURNING *
        """, (now_iso(), session.get("user"), item_id)).fetchone()
    else:
        conn.close()
        return jsonify({"ok": False, "error": "Invalid trash item type"}), 400

    conn.commit()
    conn.close()

    if not row:
        return jsonify({"ok": False, "error": "Trash item not found"}), 404
    return jsonify({"ok": True, "message": "Item recovered", "data": dict(row)})


@marketing_emails_bp.route("/api/marketing-emails/trash/<item_type>/<int:item_id>/permanent", methods=["DELETE"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_trash_permanent_delete(item_type, item_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    if item_type == "lead":
        row = conn.execute("""
            DELETE FROM marketing_email_leads
            WHERE id=%s AND is_deleted=1
            RETURNING id
        """, (item_id,)).fetchone()
    elif item_type == "export":
        exists = conn.execute("""
            SELECT id
            FROM marketing_email_export_files
            WHERE id=%s AND is_deleted=1
            LIMIT 1
        """, (item_id,)).fetchone()
        if not exists:
            conn.close()
            return jsonify({"ok": False, "error": "Trash item not found"}), 404
        conn.execute("DELETE FROM marketing_email_download_logs WHERE export_file_id=%s", (item_id,))
        row = conn.execute("""
            DELETE FROM marketing_email_export_files
            WHERE id=%s AND is_deleted=1
            RETURNING id
        """, (item_id,)).fetchone()
    else:
        conn.close()
        return jsonify({"ok": False, "error": "Invalid trash item type"}), 400

    conn.commit()
    conn.close()

    if not row:
        return jsonify({"ok": False, "error": "Trash item not found"}), 404
    return jsonify({"ok": True, "message": "Item permanently deleted"})


@marketing_emails_bp.route("/api/marketing-emails/download-permissions", methods=["GET"])
@login_required
@require_module("MARKETING_EMAILS")
def api_download_permissions_list():
    conn = db()

    if not is_admin():
        row = conn.execute("""
            SELECT p.*, u.username, u.full_name, u.role
            FROM users u
            LEFT JOIN marketing_email_download_permissions p ON p.user_id = u.id
            WHERE u.id=%s
            LIMIT 1
        """, (current_user_id(),)).fetchone()
        conn.close()
        data = dict(row) if row else {}
        data["can_download"] = int(data.get("can_download") or 0)
        return jsonify({"ok": True, "data": [data]})

    rows = conn.execute("""
        SELECT
            u.id AS user_id,
            u.username,
            u.full_name,
            u.role,
            COALESCE(p.can_download, 0) AS can_download,
            p.notes,
            p.created_at,
            p.created_by,
            p.updated_at,
            p.updated_by
        FROM users u
        LEFT JOIN marketing_email_download_permissions p ON p.user_id = u.id
        WHERE u.active=1 AND u.role='EMP'
        ORDER BY u.username ASC
    """).fetchall()
    conn.close()

    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


@marketing_emails_bp.route("/api/marketing-emails/download-permissions", methods=["PUT"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_download_permissions_set():
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    try:
        user_id = int(data.get("user_id"))
    except Exception:
        return jsonify({"ok": False, "error": "Valid user is required"}), 400
    can_download = 1 if int(data.get("can_download") or 0) == 1 else 0
    notes = clean_text(data.get("notes"), 1000) or None

    conn = db()
    user = conn.execute("""
        SELECT id, username, role
        FROM users
        WHERE id=%s AND active=1
        LIMIT 1
    """, (user_id,)).fetchone()
    if not user:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404
    if user["role"] != "EMP":
        conn.close()
        return jsonify({"ok": False, "error": "Download permissions are managed per employee"}), 400

    conn.execute("""
        INSERT INTO marketing_email_download_permissions (
            user_id, can_download, notes, created_at, created_by, updated_at, updated_by
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(user_id)
        DO UPDATE SET
            can_download=excluded.can_download,
            notes=excluded.notes,
            updated_at=excluded.updated_at,
            updated_by=excluded.updated_by
    """, (
        user_id,
        can_download,
        notes,
        now_iso(),
        session.get("user"),
        now_iso(),
        session.get("user"),
    ))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "message": "Download permission updated"})


@marketing_emails_bp.route("/api/marketing-emails/download-requests", methods=["GET"])
@login_required
@require_module("MARKETING_EMAILS")
def api_download_requests_list():
    conn = db()
    if is_admin():
        rows = conn.execute("""
            SELECT r.*, u.username, u.full_name
            FROM marketing_email_download_requests r
            LEFT JOIN users u ON u.id = r.user_id
            ORDER BY r.id DESC
            LIMIT 500
        """).fetchall()
    else:
        rows = conn.execute("""
            SELECT r.*, u.username, u.full_name
            FROM marketing_email_download_requests r
            LEFT JOIN users u ON u.id = r.user_id
            WHERE r.user_id=%s
            ORDER BY r.id DESC
            LIMIT 100
        """, (current_user_id(),)).fetchall()
    conn.close()

    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


@marketing_emails_bp.route("/api/marketing-emails/download-requests", methods=["POST"])
@login_required
@require_module("MARKETING_EMAILS")
def api_download_request_create():
    conn = db()

    if is_admin():
        conn.close()
        return jsonify({"ok": False, "error": "Admin already has download access"}), 400

    if get_download_permission(conn, current_user_id()):
        conn.close()
        return jsonify({"ok": True, "message": "Download access is already allowed"})

    pending = conn.execute("""
        SELECT id
        FROM marketing_email_download_requests
        WHERE user_id=%s AND status='PENDING'
        ORDER BY id DESC
        LIMIT 1
    """, (current_user_id(),)).fetchone()
    if pending:
        conn.close()
        return jsonify({"ok": True, "message": "Download access request is already pending"})

    request_note = clean_text((request.json or {}).get("request_note"), 1000) or None
    row = conn.execute("""
        INSERT INTO marketing_email_download_requests (
            user_id, request_note, status, requested_at,
            decided_at, decided_by, decision_note
        )
        VALUES (%s,%s,'PENDING',%s,NULL,NULL,NULL)
        RETURNING *
    """, (current_user_id(), request_note, now_iso())).fetchone()
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "message": "Download access request sent", "data": dict(row)})


@marketing_emails_bp.route("/api/marketing-emails/download-requests/<int:request_id>", methods=["PUT"])
@login_required
@require_module("MARKETING_EMAILS")
def api_download_request_update(request_id):
    if is_admin():
        return jsonify({"ok": False, "error": "Admin can decide or delete requests"}), 400

    request_note = clean_text((request.json or {}).get("request_note"), 1000) or None

    conn = db()
    row = conn.execute("""
        UPDATE marketing_email_download_requests
        SET request_note=%s
        WHERE id=%s
          AND user_id=%s
          AND status='PENDING'
        RETURNING *
    """, (request_note, request_id, current_user_id())).fetchone()
    conn.commit()
    conn.close()

    if not row:
        return jsonify({"ok": False, "error": "Only pending requests can be edited"}), 404
    return jsonify({"ok": True, "message": "Request updated", "data": dict(row)})


@marketing_emails_bp.route("/api/marketing-emails/download-requests/<int:request_id>", methods=["DELETE"])
@login_required
@require_module("MARKETING_EMAILS")
def api_download_request_delete(request_id):
    conn = db()
    if is_admin():
        row = conn.execute("""
            DELETE FROM marketing_email_download_requests
            WHERE id=%s
            RETURNING id
        """, (request_id,)).fetchone()
    else:
        row = conn.execute("""
            DELETE FROM marketing_email_download_requests
            WHERE id=%s
              AND user_id=%s
              AND status='PENDING'
            RETURNING id
        """, (request_id, current_user_id())).fetchone()
    conn.commit()
    conn.close()

    if not row:
        return jsonify({"ok": False, "error": "Request not found or cannot be deleted"}), 404
    return jsonify({"ok": True, "message": "Request deleted"})


@marketing_emails_bp.route("/api/marketing-emails/download-requests/<int:request_id>/decide", methods=["POST"])
@login_required
@require_module("MARKETING_EMAILS", need_edit=True)
def api_download_request_decide(request_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    status = clean_text(data.get("status"), 40).upper()
    decision_note = clean_text(data.get("decision_note"), 1000) or None
    if status not in ("APPROVED", "DENIED"):
        return jsonify({"ok": False, "error": "Decision must be approved or denied"}), 400

    conn = db()
    req = conn.execute("""
        SELECT *
        FROM marketing_email_download_requests
        WHERE id=%s
        LIMIT 1
    """, (request_id,)).fetchone()
    if not req:
        conn.close()
        return jsonify({"ok": False, "error": "Request not found"}), 404

    updated = conn.execute("""
        UPDATE marketing_email_download_requests
        SET status=%s,
            decided_at=%s,
            decided_by=%s,
            decision_note=%s
        WHERE id=%s
        RETURNING *
    """, (status, now_iso(), session.get("user"), decision_note, request_id)).fetchone()

    conn.execute("""
        INSERT INTO marketing_email_download_permissions (
            user_id, can_download, notes, created_at, created_by, updated_at, updated_by
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(user_id)
        DO UPDATE SET
            can_download=excluded.can_download,
            notes=excluded.notes,
            updated_at=excluded.updated_at,
            updated_by=excluded.updated_by
    """, (
        req["user_id"],
        1 if status == "APPROVED" else 0,
        decision_note,
        now_iso(),
        session.get("user"),
        now_iso(),
        session.get("user"),
    ))

    conn.commit()
    conn.close()

    return jsonify({"ok": True, "message": "Request updated", "data": dict(updated)})


@marketing_emails_bp.route("/api/marketing-emails/download-logs", methods=["GET"])
@login_required
@require_module("MARKETING_EMAILS")
def api_download_logs_list():
    conn = db()
    if is_admin():
        rows = conn.execute("""
            SELECT *
            FROM marketing_email_download_logs
            ORDER BY id DESC
            LIMIT 500
        """).fetchall()
    else:
        rows = conn.execute("""
            SELECT *
            FROM marketing_email_download_logs
            WHERE downloaded_by_user_id=%s
            ORDER BY id DESC
            LIMIT 200
        """, (current_user_id(),)).fetchall()
    conn.close()

    return jsonify({"ok": True, "data": [dict(r) for r in rows]})
