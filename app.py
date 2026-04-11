from flask import Flask, request, redirect, url_for, session, jsonify
import os
from datetime import datetime
from functools import wraps

from routes.db_compat import sqlite3
from werkzeug.security import generate_password_hash, check_password_hash

# Blueprints
from routes.auth import auth_bp
from routes.pages import pages_bp
from routes.document_storage import document_storage_bp
from routes.invoices import invoices_bp
from routes.calendar import calendar_bp
from routes.worksheet import worksheet_bp
from routes.attendance import attendance_bp
from routes.hs_codes import hs_codes_bp
from routes.finance import finance_bp
from routes.users import users_bp
from routes.clients import clients_bp
from routes.cash_advances import cash_advances_bp
from routes.messages import messages_bp

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "CHANGE_THIS_TO_A_RANDOM_SECRET")

DATABASE_URL = (os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL") or "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required. Set it in your environment before starting the app.")

app.config["DATABASE_URL"] = DATABASE_URL

app.config["GOOGLE_OAUTH_REDIRECT_URI"] = os.environ.get(
    "GOOGLE_OAUTH_REDIRECT_URI",
    "https://erp2-vpd7.onrender.com/google-drive/callback"
)

app.config["GOOGLE_DRIVE_SERVICE_ACCOUNT_FILE"] = os.environ.get(
    "GOOGLE_DRIVE_SERVICE_ACCOUNT_FILE",
    "/etc/secrets/the-ceylon-spice-haven-erp-a23cffff0d8d.json"
)
app.config["GOOGLE_DRIVE_ROOT_FOLDER_ID"] = os.environ.get(
    "GOOGLE_DRIVE_ROOT_FOLDER_ID",
    "1jhC2WBUyFq2TRVUnBOcIJqJAySdlPjA0"
)
app.config["GOOGLE_OAUTH_CLIENT_FILE"] = os.environ.get(
    "GOOGLE_OAUTH_CLIENT_FILE",
    "/etc/secrets/google-oauth-client.json"
)
app.config["GOOGLE_OAUTH_TOKEN_FILE"] = os.environ.get(
    "GOOGLE_OAUTH_TOKEN_FILE",
    "/tmp/google-oauth-token.json"
)
app.config["GOOGLE_SERVICE_ACCOUNT_JSON"] = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

USERS = {
    "punsith": {"password": "punsith123", "role": "ADMIN"},
    "dulmina": {"password": "dulmina123", "role": "ADMIN"},
    "mihiran": {"password": "mihiran123", "role": "ADMIN"},
    "emp1": {"password": "emp123", "role": "EMP"},
}

MODULES = [
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
]


# ======================
# DB / HELPERS
# ======================
def db():
    return sqlite3.connect(DATABASE_URL)


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


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


def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("auth.login"))
        return f(*args, **kwargs)
    return wrapped


def is_admin():
    return session.get("role") == "ADMIN"


def get_user_row_by_username(username: str):
    username = (username or "").strip()
    if not username:
        return None
    conn = db()
    row = conn.execute("SELECT * FROM users WHERE username=? LIMIT 1", (username,)).fetchone()
    conn.close()
    return row


def get_user_id():
    return session.get("uid")


# ======================
# PERMISSIONS HELPERS
# ======================
def has_module_access(module: str, need_edit: bool = False) -> bool:
    module = (module or "").strip().upper()
    if not module:
        return False

    if is_admin():
        return True

    uid = get_user_id()
    if not uid:
        return False

    conn = db()
    row = conn.execute("""
        SELECT can_access, can_edit
        FROM user_permissions
        WHERE user_id=? AND module=?
        LIMIT 1
    """, (uid, module)).fetchone()
    conn.close()

    if not row:
        return False

    if int(row["can_access"]) != 1:
        return False

    if need_edit and int(row["can_edit"]) != 1:
        return False

    return True


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
                return redirect(url_for("pages.dashboard"))
            return f(*args, **kwargs)
        return wrapped
    return deco


def require_module_response(module: str, need_edit: bool = False):
    if has_module_access(module, need_edit=need_edit):
        return None

    if request.path.startswith("/api/"):
        return jsonify({
            "ok": False,
            "error": f"No permission for {module}{' (edit)' if need_edit else ''}"
        }), 403

    return redirect(url_for("pages.dashboard"))


# ======================
# INVOICE TABLE
# ======================
def init_invoice_table():
    conn = sqlite3.connect(DATABASE_URL)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS document_numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_type TEXT,
            year INTEGER,
            month INTEGER,
            number INTEGER,
            status TEXT DEFAULT 'AVAILABLE',
            reserved_by TEXT,
            reserved_at TEXT,
            used_at TEXT,
            restored_at TEXT
        )
    """)
    conn.commit()
    conn.close()


init_invoice_table()


# ======================
# SAFETY: NEVER LOSE ALL ADMINS
# ======================
def count_active_admins(conn):
    return conn.execute("""
        SELECT COUNT(*) AS c
        FROM users
        WHERE role='ADMIN' AND active=1
    """).fetchone()["c"]


def would_remove_last_admin(conn, target_user_id: int) -> bool:
    target = conn.execute("""
        SELECT id, role, active
        FROM users
        WHERE id=?
        LIMIT 1
    """, (target_user_id,)).fetchone()

    if not target:
        return False

    if target["role"] != "ADMIN" or int(target["active"]) != 1:
        return False

    admins = count_active_admins(conn)
    return admins <= 1


# ======================
# DB INIT
# ======================
def init_db():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('ADMIN','EMP')),
            active INTEGER NOT NULL DEFAULT 1,
            full_name TEXT,
            nic TEXT,
            join_date TEXT,
            job_role TEXT,
            address TEXT,
            google_email TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            edited_at TEXT,
            edited_by TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_permissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            module TEXT NOT NULL,
            can_access INTEGER NOT NULL DEFAULT 0,
            can_edit INTEGER NOT NULL DEFAULT 0,
            UNIQUE(user_id, module),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    cur.execute("PRAGMA table_info(user_permissions)")
    pcols = {r["name"] for r in cur.fetchall()}
    if "module" not in pcols:
        cur.execute("ALTER TABLE user_permissions RENAME TO user_permissions_old")
        cur.execute("""
            CREATE TABLE user_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                module TEXT NOT NULL,
                can_access INTEGER NOT NULL DEFAULT 0,
                can_edit INTEGER NOT NULL DEFAULT 0,
                UNIQUE(user_id, module),
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        """)
        cur.execute("DROP TABLE IF EXISTS user_permissions_old")
    conn.commit()

    cur.execute("PRAGMA table_info(users)")
    ucols = {r["name"] for r in cur.fetchall()}
    if "google_email" not in ucols:
        cur.execute("ALTER TABLE users ADD COLUMN google_email TEXT")
        conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bank_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS finance_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_id INTEGER,
            type TEXT NOT NULL,
            client_name TEXT NOT NULL,
            category TEXT NOT NULL,
            description TEXT,
            currency TEXT NOT NULL,
            amount REAL NOT NULL,
            payment_type TEXT NOT NULL,
            status TEXT NOT NULL,
            proof_of_payment TEXT NOT NULL,
            invoice_ref TEXT,
            po_number TEXT,
            quotation_number TEXT,
            paid_date TEXT,
            folder_link TEXT,
            proof_link TEXT,
            invoice_link TEXT,
            quotation_link TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            edited_at TEXT,
            edited_by TEXT,
            deleted_at TEXT,
            deleted_by TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS month_balances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_id INTEGER,
            month_key TEXT NOT NULL,
            currency TEXT NOT NULL,
            closing_balance REAL NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            edited_at TEXT,
            edited_by TEXT,
            UNIQUE(bank_id, month_key, currency)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cash_advances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_username TEXT NOT NULL,
            bank_id INTEGER,
            currency TEXT NOT NULL,
            amount_given REAL NOT NULL,
            purpose TEXT,
            given_date TEXT,
            proof_link TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            closed INTEGER NOT NULL DEFAULT 0,
            closed_at TEXT,
            closed_by TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cash_advance_expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            advance_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            description TEXT,
            amount REAL NOT NULL,
            proof_link TEXT,
            spent_date TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            FOREIGN KEY(advance_id) REFERENCES cash_advances(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cash_advance_topups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            advance_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            topup_date TEXT,
            proof_link TEXT,
            ref_type TEXT,
            ref_id TEXT,
            note TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            FOREIGN KEY(advance_id) REFERENCES cash_advances(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS doc_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_id INTEGER,
            item_type TEXT NOT NULL CHECK(item_type IN ('FOLDER','DOCUMENT')),
            name TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT 'GENERAL',
            drive_id TEXT,
            web_view_link TEXT,
            mime_type TEXT,
            notes TEXT,
            admin_locked INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            edited_at TEXT,
            edited_by TEXT,
            deleted_at TEXT,
            deleted_by TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS doc_item_permissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            can_access INTEGER NOT NULL DEFAULT 1,
            can_edit INTEGER NOT NULL DEFAULT 0,
            UNIQUE(item_id, user_id),
            FOREIGN KEY(item_id) REFERENCES doc_items(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    cur.execute("PRAGMA table_info(doc_items)")
    doc_cols = {r["name"] for r in cur.fetchall()}
    if "category" not in doc_cols:
        cur.execute("ALTER TABLE doc_items ADD COLUMN category TEXT NOT NULL DEFAULT 'GENERAL'")
    if "drive_id" not in doc_cols:
        cur.execute("ALTER TABLE doc_items ADD COLUMN drive_id TEXT")
    if "web_view_link" not in doc_cols:
        cur.execute("ALTER TABLE doc_items ADD COLUMN web_view_link TEXT")
    if "mime_type" not in doc_cols:
        cur.execute("ALTER TABLE doc_items ADD COLUMN mime_type TEXT")
    if "notes" not in doc_cols:
        cur.execute("ALTER TABLE doc_items ADD COLUMN notes TEXT")
    if "admin_locked" not in doc_cols:
        cur.execute("ALTER TABLE doc_items ADD COLUMN admin_locked INTEGER NOT NULL DEFAULT 0")
    if "is_active" not in doc_cols:
        cur.execute("ALTER TABLE doc_items ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    if "deleted_at" not in doc_cols:
        cur.execute("ALTER TABLE doc_items ADD COLUMN deleted_at TEXT")
    if "deleted_by" not in doc_cols:
        cur.execute("ALTER TABLE doc_items ADD COLUMN deleted_by TEXT")

    cur.execute("""
        UPDATE doc_items
        SET category='GENERAL'
        WHERE category IS NULL OR TRIM(category)=''
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS message_conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_type TEXT NOT NULL CHECK(conversation_type IN ('DIRECT','GROUP')),
            title TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS message_conversation_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            last_read_message_id INTEGER,
            UNIQUE(conversation_id, user_id),
            FOREIGN KEY(conversation_id) REFERENCES message_conversations(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS message_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id INTEGER NOT NULL,
            sender_user_id INTEGER NOT NULL,
            message_text TEXT NOT NULL DEFAULT '',
            attachment_name TEXT,
            attachment_url TEXT,
            attachment_mime TEXT,
            created_at TEXT NOT NULL,
            edited_at TEXT,
            deleted_at TEXT,
            deleted_by TEXT,
            FOREIGN KEY(conversation_id) REFERENCES message_conversations(id),
            FOREIGN KEY(sender_user_id) REFERENCES users(id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS message_conversation_deleted (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            deleted_at TEXT NOT NULL,
            deleted_by TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            UNIQUE(conversation_id, user_id),
            FOREIGN KEY(conversation_id) REFERENCES message_conversations(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    cur.execute("PRAGMA table_info(finance_records)")
    cols = {r["name"] for r in cur.fetchall()}
    if "deleted_at" not in cols:
        cur.execute("ALTER TABLE finance_records ADD COLUMN deleted_at TEXT")
    if "deleted_by" not in cols:
        cur.execute("ALTER TABLE finance_records ADD COLUMN deleted_by TEXT")
    if "bank_id" not in cols:
        cur.execute("ALTER TABLE finance_records ADD COLUMN bank_id INTEGER")

    cur.execute("PRAGMA table_info(month_balances)")
    bcols = {r["name"] for r in cur.fetchall()}
    if "bank_id" not in bcols:
        cur.execute("ALTER TABLE month_balances ADD COLUMN bank_id INTEGER")

    cur.execute("PRAGMA table_info(cash_advance_expenses)")
    cexp_cols = {r["name"] for r in cur.fetchall()}
    if "paid_by" not in cexp_cols:
        cur.execute("ALTER TABLE cash_advance_expenses ADD COLUMN paid_by TEXT DEFAULT 'COMPANY_ADVANCE'")

    cur.execute("PRAGMA table_info(message_messages)")
    msg_cols = {r["name"] for r in cur.fetchall()}
    if "reply_to" not in msg_cols:
        cur.execute("ALTER TABLE message_messages ADD COLUMN reply_to INTEGER")
    if "attachment_drive_id" not in msg_cols:
        cur.execute("ALTER TABLE message_messages ADD COLUMN attachment_drive_id TEXT")

    conn.commit()

    count = cur.execute("SELECT COUNT(*) AS c FROM bank_accounts").fetchone()["c"]
    if count == 0:
        defaults = ["HNB-LKR-107010008865", "HNB-USD-107010008866"]
        for name in defaults:
            cur.execute(
                "INSERT INTO bank_accounts (name, created_at, created_by, active) VALUES (?,?,?,1)",
                (name, now_iso(), "system")
            )
        conn.commit()

    first_bank = cur.execute("SELECT id FROM bank_accounts WHERE active=1 ORDER BY id ASC LIMIT 1").fetchone()
    default_bank_id = first_bank["id"] if first_bank else None
    if default_bank_id:
        cur.execute("UPDATE finance_records SET bank_id=? WHERE bank_id IS NULL", (default_bank_id,))
        cur.execute("UPDATE month_balances SET bank_id=? WHERE bank_id IS NULL", (default_bank_id,))
        conn.commit()

    ucount = cur.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    if ucount == 0:
        for uname, info in USERS.items():
            cur.execute("""
                INSERT INTO users (username, password_hash, role, active, created_at, created_by)
                VALUES (?,?,?,?,?,?)
            """, (
                uname,
                generate_password_hash(info["password"]),
                info["role"],
                1,
                now_iso(),
                "system"
            ))
        conn.commit()

    rows = cur.execute("SELECT id, role FROM users").fetchall()
    for r in rows:
        uid = r["id"]
        role = r["role"]
        for m in MODULES:
            existing = cur.execute("""
                SELECT id FROM user_permissions WHERE user_id=? AND module=? LIMIT 1
            """, (uid, m)).fetchone()
            if existing:
                continue

            if role == "ADMIN":
                cur.execute("""
                    INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                    VALUES (?,?,1,1)
                """, (uid, m))
            else:
                if m in ("FINANCE", "CASH_ADVANCES", "DOCUMENT_STORAGE"):
                    cur.execute("""
                        INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                        VALUES (?,?,1,1)
                    """, (uid, m))
                else:
                    cur.execute("""
                        INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                        VALUES (?,?,0,0)
                    """, (uid, m))
    conn.commit()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS employee_schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS employee_unavailability (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
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
            cancelled_by TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS calendar_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            is_active INTEGER NOT NULL DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS calendar_event_acks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            remind_at TEXT NOT NULL,
            acked_at TEXT NOT NULL,
            UNIQUE(event_id, user_id, remind_at)
        )
    """)

    conn.close()


def purge_deleted_older_than_30_days():
    conn = db()
    conn.execute("""
        DELETE FROM finance_records
        WHERE deleted_at IS NOT NULL
          AND CAST(REPLACE(deleted_at, 'T', ' ') AS timestamp) <= NOW() - INTERVAL '30 days'
    """)
    conn.commit()
    conn.close()


def get_default_bank_id():
    conn = db()
    row = conn.execute("SELECT id FROM bank_accounts WHERE active=1 ORDER BY id ASC LIMIT 1").fetchone()
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
# AUTH
# ======================
def auth_user(username, password):
    username = (username or "").strip()
    password = password or ""

    conn = db()
    row = conn.execute("SELECT * FROM users WHERE username=? LIMIT 1", (username,)).fetchone()
    conn.close()

    if row:
        if int(row["active"]) != 1:
            return None, "This account is disabled"
        if not check_password_hash(row["password_hash"], password):
            return None, "Invalid username or password"
        return {"id": row["id"], "username": row["username"], "role": row["role"]}, None

    u = USERS.get(username)
    if not u or u["password"] != password:
        return None, "Invalid username or password"

    return {"id": None, "username": username, "role": u["role"]}, None


# expose helpers to blueprints
app.config["AUTH_USER_FUNC"] = auth_user
app.config["HAS_MODULE_ACCESS_FUNC"] = has_module_access
app.config["REQUIRE_MODULE_FUNC"] = require_module_response

app.config["DB_CONN_FUNC"] = db
app.config["NOW_ISO_FUNC"] = now_iso
app.config["NORMALIZE_URL_FUNC"] = normalize_url
app.config["LOGIN_REQUIRED_FUNC"] = login_required
app.config["IS_ADMIN_FUNC"] = is_admin
app.config["GET_USER_ID_FUNC"] = get_user_id
app.config["GET_USER_ROW_BY_USERNAME_FUNC"] = get_user_row_by_username
app.config["COUNT_ACTIVE_ADMINS_FUNC"] = count_active_admins
app.config["WOULD_REMOVE_LAST_ADMIN_FUNC"] = would_remove_last_admin
app.config["GET_DEFAULT_BANK_ID_FUNC"] = get_default_bank_id
app.config["GET_BANK_ID_FROM_REQUEST_FUNC"] = get_bank_id_from_request
app.config["IS_ALL_BANKS_FUNC"] = is_all_banks
app.config["PURGE_DELETED_OLDER_THAN_30_DAYS_FUNC"] = purge_deleted_older_than_30_days
app.config["MODULES"] = MODULES

init_db()
purge_deleted_older_than_30_days()

# register blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(pages_bp)
app.register_blueprint(document_storage_bp)
app.register_blueprint(invoices_bp)
app.register_blueprint(calendar_bp)
app.register_blueprint(worksheet_bp)
app.register_blueprint(attendance_bp)
app.register_blueprint(hs_codes_bp)
app.register_blueprint(finance_bp)
app.register_blueprint(users_bp)
app.register_blueprint(clients_bp)
app.register_blueprint(cash_advances_bp)
app.register_blueprint(messages_bp)


# ======================
# SMALL CORE API
# ======================
@app.route("/api/me")
@login_required
def api_me():
    if is_admin():
        perms = {m: {"can_access": 1, "can_edit": 1} for m in MODULES}
        return jsonify({"user": session["user"], "role": session["role"], "permissions": perms})

    uid = get_user_id()
    perms = {}
    if uid:
        conn = db()
        rows = conn.execute("""
            SELECT module, can_access, can_edit
            FROM user_permissions
            WHERE user_id=?
        """, (uid,)).fetchall()
        conn.close()
        for r in rows:
            perms[r["module"]] = {
                "can_access": int(r["can_access"]),
                "can_edit": int(r["can_edit"])
            }

    return jsonify({"user": session["user"], "role": session["role"], "permissions": perms})


@app.route("/api/modules", methods=["GET"])
@login_required
def api_modules_list():
    return jsonify({"ok": True, "data": MODULES})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))