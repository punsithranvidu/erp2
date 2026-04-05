from flask import Flask, render_template, request, redirect, url_for, session, jsonify, Response
import os
import re
import uuid
from datetime import datetime
from functools import wraps
from io import StringIO
import csv



from routes.db_compat import sqlite3
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

# Blueprints (split routes)
from routes.auth import auth_bp
from routes.pages import pages_bp
from routes.document_storage import document_storage_bp
from routes.invoices import invoices_bp
from routes.calendar import calendar_bp
from routes.worksheet import worksheet_bp
from routes.attendance import attendance_bp
from routes.hs_codes import hs_codes_bp


app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "CHANGE_THIS_TO_A_RANDOM_SECRET")

DATABASE_URL = (os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL") or "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required. Set it in your environment before starting the app.")

# Keep old config key name too, in case existing blueprints still read it
app.config["DATABASE_URL"] = DATABASE_URL
app.config["DATABASE_URL"] = DATABASE_URL

app.config["GOOGLE_DRIVE_SERVICE_ACCOUNT_FILE"] = os.environ.get("GOOGLE_DRIVE_SERVICE_ACCOUNT_FILE", "/etc/secrets/the-ceylon-spice-haven-erp-a23cffff0d8d.json")
app.config["GOOGLE_DRIVE_ROOT_FOLDER_ID"] = os.environ.get("GOOGLE_DRIVE_ROOT_FOLDER_ID", "1jhC2WBUyFq2TRVUnBOcIJqJAySdlPjA0")
app.config["GOOGLE_OAUTH_CLIENT_FILE"] = os.environ.get("GOOGLE_OAUTH_CLIENT_FILE", "/etc/secrets/google-oauth-client.json")
app.config["GOOGLE_OAUTH_TOKEN_FILE"] = os.environ.get("GOOGLE_OAUTH_TOKEN_FILE", "/tmp/google-oauth-token.json")

# fallback only (for safety)
USERS = {
    "punsith":  {"password": "punsith123",  "role": "ADMIN"},
    "dulmina":  {"password": "dulmina123",  "role": "ADMIN"},
    "mihiran":  {"password": "mihiran123",  "role": "ADMIN"},
    "emp1":     {"password": "emp123",      "role": "EMP"},
}

# ====== MODULES LIST (for permissions) ======
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


def ensure_message_upload_dir():
    upload_dir = os.path.join(app.root_path, "static", "uploads", "messages")
    os.makedirs(upload_dir, exist_ok=True)
    return upload_dir

def save_message_attachment(file_storage):
    if not file_storage or not getattr(file_storage, "filename", ""):
        return None
    upload_dir = ensure_message_upload_dir()
    safe_name = secure_filename(file_storage.filename) or "attachment"
    ext = os.path.splitext(safe_name)[1]
    stored_name = f"{uuid.uuid4().hex}{ext}"
    abs_path = os.path.join(upload_dir, stored_name)
    file_storage.save(abs_path)
    rel_path = f"uploads/messages/{stored_name}"
    return {
        "attachment_name": safe_name,
        "attachment_url": url_for("static", filename=rel_path),
        "attachment_mime": (file_storage.mimetype or "application/octet-stream"),
    }

def get_active_user_brief_rows(conn):
    return conn.execute("""
        SELECT id, username, role, active, full_name
        FROM users
        WHERE active=1
        ORDER BY role DESC, COALESCE(full_name, username) ASC, username ASC
    """).fetchall()

def message_can_access_conversation(conn, conversation_id: int, user_id: int) -> bool:
    row = conn.execute("""
        SELECT 1
        FROM message_conversation_members
        WHERE conversation_id=? AND user_id=? AND active=1
        LIMIT 1
    """, (conversation_id, user_id)).fetchone()
    return row is not None

def ensure_direct_conversation(conn, user_a: int, user_b: int):
    low, high = sorted([int(user_a), int(user_b)])
    row = conn.execute("""
        SELECT mc.id
        FROM message_conversations mc
        JOIN message_conversation_members m1 ON m1.conversation_id = mc.id AND m1.user_id=? AND m1.active=1
        JOIN message_conversation_members m2 ON m2.conversation_id = mc.id AND m2.user_id=? AND m2.active=1
        WHERE mc.conversation_type='DIRECT' AND mc.active=1
        GROUP BY mc.id
        HAVING COUNT(DISTINCT CASE WHEN m1.user_id IS NOT NULL THEN m1.user_id END) >= 1
           AND COUNT(DISTINCT CASE WHEN m2.user_id IS NOT NULL THEN m2.user_id END) >= 1
        ORDER BY mc.id ASC
        LIMIT 1
    """, (low, high)).fetchone()
    if row:
        return int(row["id"])

    now = now_iso()
    conn.execute("""
        INSERT INTO message_conversations (conversation_type, title, created_at, created_by, active)
        VALUES ('DIRECT', '', ?, ?, 1)
    """, (now, session["user"]))
    conversation_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    for uid in (low, high):
        conn.execute("""
            INSERT INTO message_conversation_members (conversation_id, user_id, joined_at, active, last_read_message_id)
            VALUES (?,?,?,?,NULL)
        """, (conversation_id, uid, now, 1))
    return int(conversation_id)

def get_conversation_last_message(conn, conversation_id: int):
    return conn.execute("""
        SELECT mm.id, mm.message_text, mm.created_at, mm.sender_user_id,
               mm.attachment_name, mm.attachment_url, u.username AS sender_username, u.full_name AS sender_full_name
        FROM message_messages mm
        LEFT JOIN users u ON u.id = mm.sender_user_id
        WHERE mm.conversation_id=? AND mm.deleted_at IS NULL
        ORDER BY mm.id DESC
        LIMIT 1
    """, (conversation_id,)).fetchone()

def get_conversation_unread_count(conn, conversation_id: int, user_id: int) -> int:
    member = conn.execute("""
        SELECT last_read_message_id
        FROM message_conversation_members
        WHERE conversation_id=? AND user_id=? AND active=1
        LIMIT 1
    """, (conversation_id, user_id)).fetchone()
    last_read = int(member["last_read_message_id"] or 0) if member else 0
    row = conn.execute("""
        SELECT COUNT(*) AS c
        FROM message_messages
        WHERE conversation_id=? AND deleted_at IS NULL AND id>? AND sender_user_id<>?
    """, (conversation_id, last_read, user_id)).fetchone()
    return int(row["c"] or 0)

def serialize_conversation(conn, conversation_row, user_id: int):
    convo = dict(conversation_row)
    members = conn.execute("""
        SELECT u.id, u.username, u.full_name, u.role
        FROM message_conversation_members m
        JOIN users u ON u.id = m.user_id
        WHERE m.conversation_id=? AND m.active=1
        ORDER BY u.role DESC, COALESCE(u.full_name, u.username) ASC
    """, (convo["id"],)).fetchall()
    convo["members"] = [dict(r) for r in members]
    convo["member_count"] = len(convo["members"])
    last_msg = get_conversation_last_message(conn, convo["id"])
    convo["last_message"] = dict(last_msg) if last_msg else None
    convo["unread_count"] = get_conversation_unread_count(conn, convo["id"], user_id)

    if convo["conversation_type"] == "DIRECT":
        other = None
        for m in convo["members"]:
            if int(m["id"]) != int(user_id):
                other = m
                break
        convo["display_name"] = (other or {}).get("full_name") or (other or {}).get("username") or "Direct Chat"
    else:
        convo["display_name"] = convo.get("title") or "Group Chat"
    return convo

def mark_conversation_read(conn, conversation_id: int, user_id: int):
    last_msg = conn.execute("""
        SELECT id
        FROM message_messages
        WHERE conversation_id=? AND deleted_at IS NULL
        ORDER BY id DESC
        LIMIT 1
    """, (conversation_id,)).fetchone()
    last_id = int(last_msg["id"]) if last_msg else None
    conn.execute("""
        UPDATE message_conversation_members
        SET last_read_message_id=?
        WHERE conversation_id=? AND user_id=?
    """, (last_id, conversation_id, user_id))

def can_delete_message(message_row, current_user_id: int) -> bool:
    if int(current_user_id) == int(message_row["sender_user_id"] or 0):
        created_at = datetime.fromisoformat(message_row["created_at"])
        age_seconds = (datetime.now() - created_at).total_seconds()
        return age_seconds <= 900
    return is_admin()

# ======================
# PERMISSIONS HELPERS
# ======================
def has_module_access(module: str, need_edit: bool = False) -> bool:
    """
    - ADMIN: always True
    - EMP: check user_permissions table
    """
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
                    return jsonify({"ok": False, "error": f"No permission for {module}{' (edit)' if need_edit else ''}"}), 403
                return redirect(url_for("pages.dashboard"))
            return f(*args, **kwargs)
        return wrapped
    return deco

# ✅ This is what the Blueprints need (pages.py calls this)
def require_module_response(module: str, need_edit: bool = False):
    if has_module_access(module, need_edit=need_edit):
        return None

    # API requests -> JSON 403
    if request.path.startswith("/api/"):
        return jsonify({"ok": False, "error": f"No permission for {module}{' (edit)' if need_edit else ''}"}), 403

    # Normal pages -> redirect (NO browser 403 page)
    return redirect(url_for("pages.dashboard"))




# ======================
# invoice table 
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
    """
    Returns True if this action would cause active admins to become 0.
    We block that.
    """
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

    # ---------------- USERS ----------------
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

    # ---------------- USER PERMISSIONS ----------------
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

    # --- Permanent fix: repair old user_permissions schema (missing "module") ---
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

    # --- Safe migrate users table ---
    cur.execute("PRAGMA table_info(users)")
    ucols = {r["name"] for r in cur.fetchall()}
    if "google_email" not in ucols:
        cur.execute("ALTER TABLE users ADD COLUMN google_email TEXT")
        conn.commit()

    # ---------------- BANK ACCOUNTS ----------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bank_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )
    """)

    # ---------------- FINANCE RECORDS ----------------
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

    # ---------------- MONTH BALANCES ----------------
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

    # ---------------- CASH ADVANCES ----------------
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

# ---------------- DOCUMENT STORAGE ----------------
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

    # Safe migrate old doc_items table
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

    # Fill old NULL / blank categories safely
    cur.execute("""
        UPDATE doc_items
        SET category = 'GENERAL'
        WHERE category IS NULL OR TRIM(category) = ''
    """)


    # ---------------- MESSAGES ----------------
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

    # ---- Safe migrate columns if missing (old installs) ----
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

    conn.commit()

    # Seed default banks if none exist
    count = cur.execute("SELECT COUNT(*) AS c FROM bank_accounts").fetchone()["c"]
    if count == 0:
        defaults = [
            "HNB-LKR-107010008865",
            "HNB-USD-107010008866",
        ]
        for name in defaults:
            cur.execute(
                "INSERT INTO bank_accounts (name, created_at, created_by, active) VALUES (?,?,?,1)",
                (name, now_iso(), "system")
            )
        conn.commit()

    # Assign existing finance + balances to first bank if bank_id is NULL
    first_bank = cur.execute("SELECT id FROM bank_accounts WHERE active=1 ORDER BY id ASC LIMIT 1").fetchone()
    default_bank_id = first_bank["id"] if first_bank else None
    if default_bank_id:
        cur.execute("UPDATE finance_records SET bank_id=? WHERE bank_id IS NULL", (default_bank_id,))
        cur.execute("UPDATE month_balances SET bank_id=? WHERE bank_id IS NULL", (default_bank_id,))
        conn.commit()

    # Seed users table from USERS dict if empty
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

    # Seed permissions for all users (if missing)
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


# ---------------- CALENDAR ----------------
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
    except:
        return None

def is_all_banks(bank_id):
    return bank_id == "ALL"

# ======================
# AUTH (DB FIRST, fallback dict)
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


# Expose core auth/permission helpers to blueprints
app.config["AUTH_USER_FUNC"] = auth_user
app.config["HAS_MODULE_ACCESS_FUNC"] = has_module_access
app.config["REQUIRE_MODULE_FUNC"] = require_module_response  # ✅ THIS FIXES YOUR 403 ISSUE

# init
init_db()
purge_deleted_older_than_30_days()

# ======================
# ROUTES (via Blueprints)
# ======================

app.register_blueprint(auth_bp)
app.register_blueprint(pages_bp)
app.register_blueprint(document_storage_bp)
app.register_blueprint(invoices_bp)
app.register_blueprint(calendar_bp)
app.register_blueprint(worksheet_bp)
app.register_blueprint(attendance_bp)
app.register_blueprint(hs_codes_bp)

# ======================
# API
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
            perms[r["module"]] = {"can_access": int(r["can_access"]), "can_edit": int(r["can_edit"])}
    return jsonify({"user": session["user"], "role": session["role"], "permissions": perms})

@app.route("/api/modules", methods=["GET"])
@login_required
def api_modules_list():
    return jsonify({"ok": True, "data": MODULES})

# ----------------------------
# KEEP THE REST OF YOUR FILE
# (Everything below this point stays exactly the same as you already have)
# ----------------------------

# >>> IMPORTANT:
# Your remaining APIs (users/banks/finance/advances/etc.) should stay unchanged.
# Just paste the rest of your existing app.py below this comment.


# ---------- PERMISSIONS (ADMIN ONLY) ----------
@app.route("/api/users/<int:uid>/permissions", methods=["GET"])
@login_required
def api_user_permissions_get(uid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    u = conn.execute("SELECT id, username, role FROM users WHERE id=? LIMIT 1", (uid,)).fetchone()
    if not u:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    rows = conn.execute("""
        SELECT module, can_access, can_edit
        FROM user_permissions
        WHERE user_id=?
        ORDER BY module ASC
    """, (uid,)).fetchall()
    conn.close()

    out = {r["module"]: {"can_access": int(r["can_access"]), "can_edit": int(r["can_edit"])} for r in rows}
    for m in MODULES:
        if m not in out:
            out[m] = {"can_access": 0, "can_edit": 0}

    return jsonify({"ok": True, "data": {"user_id": uid, "username": u["username"], "role": u["role"], "permissions": out}})

@app.route("/api/users/<int:uid>/permissions", methods=["PUT"])
@login_required
def api_user_permissions_set(uid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    perms = data.get("permissions")

    if not isinstance(perms, dict):
        return jsonify({"ok": False, "error": "permissions must be an object"}), 400

    conn = db()
    u = conn.execute("SELECT id, username, role FROM users WHERE id=? LIMIT 1", (uid,)).fetchone()
    if not u:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    if u["role"] == "ADMIN":
        for m in MODULES:
            conn.execute("""
                INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                VALUES (?,?,1,1)
                ON CONFLICT(user_id, module)
                DO UPDATE SET can_access=1, can_edit=1
            """, (uid, m))
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "data": {"user_id": uid, "username": u["username"], "role": u["role"]}})

    for m in MODULES:
        v = perms.get(m, {})
        can_access = int(v.get("can_access", 0) or 0)
        can_edit = int(v.get("can_edit", 0) or 0)

        if can_access not in (0, 1): can_access = 0
        if can_edit not in (0, 1): can_edit = 0
        if can_access == 0:
            can_edit = 0

        conn.execute("""
            INSERT INTO user_permissions (user_id, module, can_access, can_edit)
            VALUES (?,?,?,?)
            ON CONFLICT(user_id, module)
            DO UPDATE SET can_access=excluded.can_access, can_edit=excluded.can_edit
        """, (uid, m, can_access, can_edit))

    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": {"user_id": uid, "username": u["username"], "role": u["role"]}})

# -------- USERS ----------
@app.route("/api/users", methods=["GET"])
@login_required
@require_module("USERS")
def api_users_list():
    conn = db()
    rows = conn.execute("""
        SELECT id, username, role, active, full_name, nic, join_date, job_role, address, google_email, created_at
        FROM users
        ORDER BY id DESC
        LIMIT 2000
    """).fetchall()
    conn.close()
    return jsonify({"ok": True, "data": [dict(r) for r in rows]})

@app.route("/api/users", methods=["POST"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_create():
    data = request.json or {}
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

    conn = db()
    try:
        conn.execute("""
            INSERT INTO users (
              username, password_hash, role, active,
              full_name, nic, join_date, job_role, address, google_email,
              created_at, created_by
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            username,
            generate_password_hash(password),
            role,
            1,
            full_name, nic, join_date, job_role, address, google_email,
            now_iso(),
            session["user"]
        ))
        conn.commit()

        new_row = conn.execute("SELECT id, role FROM users WHERE username=? LIMIT 1", (username,)).fetchone()
        new_id = new_row["id"]
        new_role = new_row["role"]

        for m in MODULES:
            if new_role == "ADMIN":
                conn.execute("""
                    INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                    VALUES (?,?,1,1)
                    ON CONFLICT(user_id, module)
                    DO UPDATE SET can_access=1, can_edit=1
                """, (new_id, m))
            else:
                if m in ("FINANCE", "CASH_ADVANCES"):
                    conn.execute("""
                        INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                        VALUES (?,?,1,1)
                        ON CONFLICT(user_id, module)
                        DO UPDATE SET can_access=1, can_edit=1
                    """, (new_id, m))
                else:
                    conn.execute("""
                        INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                        VALUES (?,?,0,0)
                        ON CONFLICT(user_id, module)
                        DO UPDATE SET can_access=0, can_edit=0
                    """, (new_id, m))

        conn.commit()

        row = conn.execute("""
            SELECT id, username, role, active, full_name, nic, join_date, job_role, address, google_email, created_at
            FROM users WHERE username=? LIMIT 1
        """, (username,)).fetchone()
        conn.close()
        return jsonify({"ok": True, "data": dict(row)})

    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({"ok": False, "error": "This username already exists"}), 400

@app.route("/api/users/<int:uid>", methods=["PUT"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_update(uid):
    data = request.json or {}

    role = (data.get("role") or "").strip().upper()
    active = data.get("active")

    full_name = (data.get("full_name") or "").strip() if "full_name" in data else None
    nic = (data.get("nic") or "").strip() if "nic" in data else None
    join_date = (data.get("join_date") or "").strip() if "join_date" in data else None
    job_role = (data.get("job_role") or "").strip() if "job_role" in data else None
    address = (data.get("address") or "").strip() if "address" in data else None
    google_email = (data.get("google_email") or "").strip().lower() if "google_email" in data else None

    if role and role not in ("ADMIN", "EMP"):
        return jsonify({"ok": False, "error": "role must be ADMIN or EMP"}), 400

    if active is not None:
        try:
            active = int(active)
            if active not in (0, 1):
                raise ValueError()
        except:
            return jsonify({"ok": False, "error": "active must be 0 or 1"}), 400

    conn = db()

    if active == 0:
        if would_remove_last_admin(conn, uid):
            conn.close()
            return jsonify({"ok": False, "error": "You cannot disable the last active ADMIN."}), 400

    if role == "EMP":
        if would_remove_last_admin(conn, uid):
            conn.close()
            return jsonify({"ok": False, "error": "You cannot change the last active ADMIN into EMP."}), 400

    sets = []
    vals = []

    if role:
        sets.append("role=?")
        vals.append(role)
    if active is not None:
        sets.append("active=?")
        vals.append(active)

    if "full_name" in data:
        sets.append("full_name=?")
        vals.append(full_name or None)
    if "nic" in data:
        sets.append("nic=?")
        vals.append(nic or None)
    if "join_date" in data:
        sets.append("join_date=?")
        vals.append(join_date or None)
    if "job_role" in data:
        sets.append("job_role=?")
        vals.append(job_role or None)
    if "address" in data:
        sets.append("address=?")
        vals.append(address or None)
    if "google_email" in data:
        sets.append("google_email=?")
        vals.append(google_email or None)

    if not sets:
        conn.close()
        return jsonify({"ok": False, "error": "Nothing to update"}), 400

    sets.append("edited_at=?")
    sets.append("edited_by=?")
    vals.append(now_iso())
    vals.append(session["user"])
    vals.append(uid)

    conn.execute(f"UPDATE users SET {', '.join(sets)} WHERE id=?", vals)
    conn.commit()

    if role in ("ADMIN", "EMP"):
        for m in MODULES:
            conn.execute("""
                INSERT INTO user_permissions (user_id, module, can_access, can_edit)
                VALUES (?,?,0,0)
                ON CONFLICT(user_id, module) DO NOTHING
            """, (uid, m))

        if role == "ADMIN":
            for m in MODULES:
                conn.execute("""
                    UPDATE user_permissions
                    SET can_access=1, can_edit=1
                    WHERE user_id=? AND module=?
                """, (uid, m))
        conn.commit()

    row = conn.execute("""
        SELECT id, username, role, active, full_name, nic, join_date, job_role, address, google_email, created_at
        FROM users WHERE id=? LIMIT 1
    """, (uid,)).fetchone()
    conn.close()
    return jsonify({"ok": True, "data": dict(row) if row else None})

@app.route("/api/users/<int:uid>/password", methods=["POST"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_reset_password(uid):
    data = request.json or {}
    pw = data.get("password") or ""
    if len(pw) < 3:
        return jsonify({"ok": False, "error": "password must be at least 3 characters"}), 400

    conn = db()
    row = conn.execute("SELECT username FROM users WHERE id=? LIMIT 1", (uid,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    conn.execute("""
        UPDATE users
        SET password_hash=?, edited_at=?, edited_by=?
        WHERE id=?
    """, (generate_password_hash(pw), now_iso(), session["user"], uid))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": {"id": uid, "username": row["username"]}})

@app.route("/api/users/<int:uid>/rename", methods=["POST"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_rename(uid):
    data = request.json or {}
    new_username = (data.get("username") or "").strip()
    if not new_username:
        return jsonify({"ok": False, "error": "New username is required"}), 400

    conn = db()
    old = conn.execute("SELECT username FROM users WHERE id=? LIMIT 1", (uid,)).fetchone()
    if not old:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    old_username = old["username"]

    try:
        conn.execute("""
            UPDATE users
            SET username=?, edited_at=?, edited_by=?
            WHERE id=?
        """, (new_username, now_iso(), session["user"], uid))

        conn.execute("""
            UPDATE cash_advances
            SET employee_username=?
            WHERE employee_username=?
        """, (new_username, old_username))

        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({"ok": False, "error": "That username already exists"}), 400

    conn.close()
    return jsonify({"ok": True, "data": {"id": uid, "username": new_username}})

@app.route("/api/users/<int:uid>/disable", methods=["POST"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_disable(uid):
    conn = db()

    if would_remove_last_admin(conn, uid):
        conn.close()
        return jsonify({"ok": False, "error": "You cannot disable the last active ADMIN."}), 400

    row = conn.execute("SELECT id, username FROM users WHERE id=? LIMIT 1", (uid,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    conn.execute("""
        UPDATE users
        SET active=0, edited_at=?, edited_by=?
        WHERE id=?
    """, (now_iso(), session["user"], uid))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": {"id": uid, "username": row["username"], "active": 0}})

@app.route("/api/users/<int:uid>", methods=["DELETE"])
@login_required
@require_module("USERS", need_edit=True)
def api_users_delete(uid):
    conn = db()

    if would_remove_last_admin(conn, uid):
        conn.close()
        return jsonify({"ok": False, "error": "You cannot delete the last active ADMIN."}), 400

    row = conn.execute("SELECT id, username FROM users WHERE id=? LIMIT 1", (uid,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    me = session.get("user")
    if me and row["username"] == me:
        conn.close()
        return jsonify({"ok": False, "error": "You cannot delete your own account."}), 400

    conn.execute("DELETE FROM user_permissions WHERE user_id=?", (uid,))
    conn.execute("DELETE FROM users WHERE id=?", (uid,))
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "data": {"id": uid, "username": row["username"]}})

# ---------- BANKS ----------
@app.route("/api/banks", methods=["GET"])
@login_required
@require_module("FINANCE")
def api_banks_list():
    """
    EMP/ADMIN: returns active banks by default.
    ADMIN can request all banks (active + disabled): /api/banks?all=1
    """
    show_all = (request.args.get("all") == "1")
    is_admin = (session.get("role") == "ADMIN")

    conn = db()
    if show_all and is_admin:
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


@app.route("/api/banks", methods=["POST"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_banks_create():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "Bank account name is required"}), 400

    try:
        conn = db()
        conn.execute(
            "INSERT INTO bank_accounts (name, created_at, created_by, active) VALUES (?,?,?,1)",
            (name, now_iso(), session["user"])
        )
        conn.commit()
        row = conn.execute(
            "SELECT id, name, active FROM bank_accounts WHERE name=?",
            (name,)
        ).fetchone()
        conn.close()
        return jsonify({"ok": True, "id": row["id"], "name": row["name"], "active": row["active"]})
    except sqlite3.IntegrityError:
        return jsonify({"ok": False, "error": "This bank account already exists"}), 400


@app.route("/api/banks/<int:bid>", methods=["PUT"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_banks_rename(bid):
    data = request.json or {}
    new_name = (data.get("name") or "").strip()
    if not new_name:
        return jsonify({"ok": False, "error": "New bank name is required"}), 400

    try:
        conn = db()
        exists = conn.execute("SELECT id FROM bank_accounts WHERE id=?", (bid,)).fetchone()
        if not exists:
            conn.close()
            return jsonify({"ok": False, "error": "Bank account not found"}), 404

        # unique name check (safe)
        dupe = conn.execute("SELECT id FROM bank_accounts WHERE name=? AND id<>?", (new_name, bid)).fetchone()
        if dupe:
            conn.close()
            return jsonify({"ok": False, "error": "This bank account name already exists"}), 400

        conn.execute("UPDATE bank_accounts SET name=? WHERE id=?", (new_name, bid))
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "id": bid, "name": new_name})
    except sqlite3.IntegrityError:
        return jsonify({"ok": False, "error": "This bank account name already exists"}), 400


@app.route("/api/banks/<int:bid>", methods=["DELETE"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_banks_delete(bid):
    conn = db()

    # must exist + active
    bank = conn.execute("SELECT id, name, active FROM bank_accounts WHERE id=?", (bid,)).fetchone()
    if not bank:
        conn.close()
        return jsonify({"ok": False, "error": "Bank account not found"}), 404
    if int(bank["active"]) == 0:
        conn.close()
        return jsonify({"ok": False, "error": "Bank already disabled"}), 400

    # SAFETY BARRIER: block delete if finance records exist under this bank
    cnt = conn.execute("SELECT COUNT(*) AS c FROM finance_records WHERE bank_id=?", (bid,)).fetchone()["c"]
    if cnt > 0:
        conn.close()
        return jsonify({
            "ok": False,
            "error": f"Cannot delete bank. It has {cnt} finance record(s). Delete/move those records first."
        }), 400

    # disable
    conn.execute("UPDATE bank_accounts SET active=0 WHERE id=?", (bid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


# ---------- FINANCE LIST ----------

@app.route("/api/finance", methods=["GET"])
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

# ---------- FINANCE CREATE ----------
@app.route("/api/finance", methods=["POST"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_finance_create():
    data = request.json or {}
    required = ["type", "client_name", "category", "currency", "amount", "payment_type", "status", "proof_of_payment", "bank_id"]
    for k in required:
        if not data.get(k):
            return jsonify({"ok": False, "error": f"Missing field: {k}"}), 400

    conn = db()
    conn.execute("""
        INSERT INTO finance_records (
            bank_id,
            type, client_name, category, description, currency, amount,
            payment_type, status, proof_of_payment, invoice_ref, po_number, quotation_number,
            paid_date, folder_link, proof_link, invoice_link, quotation_link,
            created_at, created_by,
            deleted_at, deleted_by
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL)
    """, (
        int(data["bank_id"]),
        data["type"],
        data["client_name"],
        data["category"],
        data.get("description",""),
        data["currency"],
        float(data["amount"]),
        data["payment_type"],
        data["status"],
        data["proof_of_payment"],
        data.get("invoice_ref",""),
        data.get("po_number",""),
        data.get("quotation_number",""),
        data.get("paid_date",""),
        normalize_url(data.get("folder_link","")),
        normalize_url(data.get("proof_link","")),
        normalize_url(data.get("invoice_link","")),
        normalize_url(data.get("quotation_link","")),
        now_iso(),
        session["user"],
    ))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

# ---------- FINANCE UPDATE (EMP: only own rows) ----------
@app.route("/api/finance/<int:rid>", methods=["PUT"])
@login_required
@require_module("FINANCE", need_edit=True)
def api_finance_update(rid):
    data = request.json or {}
    allowed = [
        "bank_id",
        "type","client_name","category","description","currency","amount","payment_type","status",
        "proof_of_payment","invoice_ref","po_number","quotation_number","paid_date",
        "folder_link","proof_link","invoice_link","quotation_link"
    ]
    sets = []
    vals = []
    for k in allowed:
        if k in data:
            v = data[k]
            if k.endswith("_link") or k in ("folder_link","proof_link","invoice_link","quotation_link"):
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

# ---------- FINANCE DELETE (EMP: only own rows) ----------
@app.route("/api/finance/<int:rid>", methods=["DELETE"])
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

# ---------- SUMMARY (upgraded) ----------
@app.route("/api/finance/summary")
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
          ROUND(SUM(amount), 2) AS total,
          COUNT(*) AS count
        FROM finance_records
        WHERE {" AND ".join(where)}
        GROUP BY key, currency
        ORDER BY key DESC, currency ASC
        LIMIT 1000
    """, params).fetchall()
    conn.close()

    return jsonify([dict(r) for r in rows])

@app.route("/api/finance/summary.csv")
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
          ROUND(SUM(amount), 2) AS total,
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
        writer.writerow([r["key"], r["currency"], r["total"], r["count"]])

    bank_tag = "allbanks" if is_all_banks(bank_id) else f"bank{bank_id}"
    filename = f"summary_{ftype.lower()}_{group}{'_'+month if month else ''}{'_user-'+user_filter if user_filter else ''}_{bank_tag}.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
@app.route("/api/finance/export.csv")
@login_required
@require_module("FINANCE")
def api_finance_export_csv():
    purge_deleted_older_than_30_days()

    # IMPORTANT: type is optional now
    ftype = (request.args.get("type") or "").upper().strip()   # INCOME / OUTCOME / ""
    month = (request.args.get("month") or "").strip()          # YYYY-MM / ""

    bank_id = request.args.get("bank_id") or "ALL"
    bank_id = str(bank_id).strip()

    where = ["fr.deleted_at IS NULL"]
    params = []

    # Bank filter
    if bank_id.upper() != "ALL":
        where.append("fr.bank_id = ?")
        params.append(int(bank_id))

    # Type filter (ONLY if user selected INCOME or OUTCOME)
    if ftype in ("INCOME", "OUTCOME"):
        where.append("fr.type = ?")
        params.append(ftype)

    # Month filter
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
            fr.quotation_link
        FROM finance_records fr
        LEFT JOIN bank_accounts ba ON ba.id = fr.bank_id
        WHERE {" AND ".join(where)}
        ORDER BY fr.id DESC
    """

    conn = db()
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    import csv
    from io import StringIO
    si = StringIO()
    cw = csv.writer(si)

    # header
    cw.writerow([
        "ID","Bank","Type","Client","Category","Description","Currency","Amount",
        "Payment Type","Status","Proof Of Payment","Invoice Ref","PO Number","Quotation Number",
        "Paid Date","Created At","Created By","Edited At","Edited By",
        "Client Folder Link","Proof Link","Invoice Link","Quotation Link"
    ])

    for r in rows:
        r = dict(r)
        cw.writerow([
            r.get("id"), r.get("bank_name"), r.get("type"), r.get("client_name"),
            r.get("category"), r.get("description"), r.get("currency"), r.get("amount"),
            r.get("payment_type"), r.get("status"), r.get("proof_of_payment"),
            r.get("invoice_ref"), r.get("po_number"), r.get("quotation_number"),
            r.get("paid_date"), r.get("created_at"), r.get("created_by"),
            r.get("edited_at"), r.get("edited_by"),
            r.get("folder_link"), r.get("proof_link"), r.get("invoice_link"), r.get("quotation_link")
        ])

    output = si.getvalue()
    return Response(
        output,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=finance_export.csv"}
    )


# --------- TRASH API ---------
@app.route("/api/finance/trash", methods=["GET"])
@login_required
@require_module("FINANCE_TRASH")
def api_finance_trash_list():
    purge_deleted_older_than_30_days()

    conn = db()
    rows = conn.execute("""
        SELECT * FROM finance_records
        WHERE deleted_at IS NOT NULL
        ORDER BY deleted_at DESC
        LIMIT 1000
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/finance/<int:rid>/restore", methods=["POST"])
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

@app.route("/api/finance/purge", methods=["POST"])
@login_required
@require_module("FINANCE_TRASH", need_edit=True)
def api_finance_purge():
    purge_deleted_older_than_30_days()
    return jsonify({"ok": True})

# --------- Month Balance ---------
@app.route("/api/balance", methods=["GET"])
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
        SELECT * FROM month_balances
        WHERE bank_id=? AND month_key=? AND currency=?
        LIMIT 1
    """, (bank_id, month, currency)).fetchone()
    conn.close()

    return jsonify({"ok": True, "data": dict(row) if row else None})

@app.route("/api/balance", methods=["POST"])
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
    """, (int(bank_id), month, currency, float(closing_balance), note, now, session["user"], now, session["user"]))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})
# ==========================
# CASH ADVANCES API
# ==========================
@app.route("/api/users/employees", methods=["GET"])
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


def _advance_money_total(conn, adv_id: int) -> float:
    """Original given money + any later topups"""
    base = conn.execute(
        "SELECT COALESCE(amount_given,0) AS a FROM cash_advances WHERE id=?",
        (adv_id,)
    ).fetchone()["a"]
    top = conn.execute(
        "SELECT COALESCE(SUM(amount),0) AS s FROM cash_advance_topups WHERE advance_id=?",
        (adv_id,)
    ).fetchone()["s"]
    return float(base or 0) + float(top or 0)


def _advance_spent_total(conn, adv_id: int) -> float:
    spent = conn.execute(
        "SELECT COALESCE(SUM(amount),0) AS s FROM cash_advance_expenses WHERE advance_id=?",
        (adv_id,)
    ).fetchone()["s"]
    return float(spent or 0)


def _advance_row_with_balance(conn, adv_row):
    adv_id = adv_row["id"]

    topups = conn.execute(
        "SELECT COALESCE(SUM(amount),0) AS s FROM cash_advance_topups WHERE advance_id=?",
        (adv_id,)
    ).fetchone()["s"]

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


@app.route("/api/advances", methods=["GET"])
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


@app.route("/api/advances", methods=["POST"])
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
        SELECT username, role FROM users
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
    except:
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


@app.route("/api/advances/<int:aid>/close", methods=["POST"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_advances_close(aid):
    conn = db()
    row = conn.execute("SELECT id, closed FROM cash_advances WHERE id=?", (aid,)).fetchone()
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


@app.route("/api/advances/<int:aid>/reopen", methods=["POST"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_advances_reopen(aid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    row = conn.execute("SELECT id FROM cash_advances WHERE id=?", (aid,)).fetchone()
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


# --------------------------
# TOPUPS (Admin sends more money later, linked to same advance)
# --------------------------
@app.route("/api/advances/<int:aid>/topups", methods=["GET"])
@login_required
@require_module("CASH_ADVANCES")
def api_advances_topups_list(aid):
    conn = db()
    adv = conn.execute("SELECT * FROM cash_advances WHERE id=?", (aid,)).fetchone()
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


@app.route("/api/advances/<int:aid>/topups", methods=["POST"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_advances_topups_add(aid):
    # Admin (or editor) can add topup
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    amount = data.get("amount")
    topup_date = (data.get("topup_date") or "").strip()
    proof_link = normalize_url(data.get("proof_link") or "")
    note = (data.get("note") or "").strip()

    try:
        amt = float(amount)
        if amt <= 0:
            raise ValueError()
    except:
        return jsonify({"ok": False, "error": "amount must be a number > 0"}), 400

    conn = db()
    adv = conn.execute("SELECT id FROM cash_advances WHERE id=?", (aid,)).fetchone()
    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    conn.execute("""
        INSERT INTO cash_advance_topups (
          advance_id, amount, topup_date, proof_link, note,
          created_at, created_by
        ) VALUES (?,?,?,?,?,?,?)
    """, (aid, float(amt), topup_date, proof_link, note, now_iso(), session["user"]))
    conn.commit()
    new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    row = conn.execute("SELECT * FROM cash_advance_topups WHERE id=?", (new_id,)).fetchone()
    conn.close()
    return jsonify({"ok": True, "data": dict(row)})


# --------------------------
# EXPENSES
# --------------------------
@app.route("/api/advances/<int:aid>/expenses", methods=["GET"])
@login_required
@require_module("CASH_ADVANCES")
def api_advances_expenses_list(aid):
    conn = db()
    adv = conn.execute("SELECT * FROM cash_advances WHERE id=?", (aid,)).fetchone()
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

@app.route("/api/advances/<int:aid>/expenses", methods=["POST"])
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
    except:
        return jsonify({"ok": False, "error": "amount must be a number > 0"}), 400

    conn = db()
    adv = conn.execute("SELECT * FROM cash_advances WHERE id=?", (aid,)).fetchone()
    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    if int(adv["closed"]) == 1:
        conn.close()
        return jsonify({"ok": False, "error": "This advance is CLOSED. You cannot add expenses."}), 400

    if not is_admin() and adv["employee_username"] != session["user"]:
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    # ✅ Only restrict if COMPANY is paying from advance money
    if paid_by == "COMPANY_ADVANCE":
        topups = conn.execute(
            "SELECT COALESCE(SUM(amount),0) AS s FROM cash_advance_topups WHERE advance_id=?",
            (aid,)
        ).fetchone()["s"]

        spent_company = conn.execute("""
            SELECT COALESCE(SUM(amount),0) AS s
            FROM cash_advance_expenses
            WHERE advance_id=? AND COALESCE(paid_by,'COMPANY_ADVANCE')='COMPANY_ADVANCE'
        """, (aid,)).fetchone()["s"]

        remaining_company = (float(adv["amount_given"]) + float(topups)) - float(spent_company)

        if float(amt) > float(remaining_company) + 1e-9:
            conn.close()
            return jsonify({"ok": False, "error": f"Not enough COMPANY balance. Remaining: {round(float(remaining_company), 2)}"}), 400

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
    row = conn.execute("SELECT * FROM cash_advance_expenses WHERE id=?", (new_id,)).fetchone()
    conn.close()
    return jsonify({"ok": True, "data": dict(row)})


@app.route("/api/expenses/<int:eid>", methods=["PUT"])
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
    except:
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

    adv = conn.execute("SELECT * FROM cash_advances WHERE id=? LIMIT 1", (exp["advance_id"],)).fetchone()
    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    if int(adv["closed"]) == 1:
        conn.close()
        return jsonify({"ok": False, "error": "This advance is CLOSED. You cannot edit expenses."}), 400

    if not is_admin() and adv["employee_username"] != session["user"]:
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    # ✅ Only restrict if COMPANY_ADVANCE
    if paid_by == "COMPANY_ADVANCE":
        topups = conn.execute(
            "SELECT COALESCE(SUM(amount),0) AS s FROM cash_advance_topups WHERE advance_id=?",
            (exp["advance_id"],)
        ).fetchone()["s"]

        spent_company_other = conn.execute("""
            SELECT COALESCE(SUM(amount),0) AS s
            FROM cash_advance_expenses
            WHERE advance_id=? AND id<>? AND COALESCE(paid_by,'COMPANY_ADVANCE')='COMPANY_ADVANCE'
        """, (exp["advance_id"], eid)).fetchone()["s"]

        remaining_company = (float(adv["amount_given"]) + float(topups)) - float(spent_company_other)

        if float(amt) > float(remaining_company) + 1e-9:
            conn.close()
            return jsonify({"ok": False, "error": f"Not enough COMPANY balance. Remaining: {round(float(remaining_company), 2)}"}), 400

    conn.execute("""
        UPDATE cash_advance_expenses
        SET category=?, description=?, paid_by=?, amount=?, spent_date=?, proof_link=?
        WHERE id=?
    """, (category, description, paid_by, float(amt), spent_date, proof_link, eid))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": {"id": eid}})


@app.route("/api/expenses/<int:eid>", methods=["DELETE"])
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

    adv = conn.execute("SELECT * FROM cash_advances WHERE id=? LIMIT 1", (exp["advance_id"],)).fetchone()
    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    if int(adv["closed"]) == 1:
        conn.close()
        return jsonify({"ok": False, "error": "This advance is CLOSED. You cannot delete expenses."}), 400

    if not is_admin() and adv["employee_username"] != session["user"]:
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    conn.execute("DELETE FROM cash_advance_expenses WHERE id=?", (eid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": {"id": eid}})


@app.route("/api/advances/<int:aid>/topups", methods=["GET"])
@login_required
@require_module("CASH_ADVANCES")
def api_topups_list(aid):
    conn = db()
    adv = conn.execute("SELECT * FROM cash_advances WHERE id=?", (aid,)).fetchone()
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


@app.route("/api/advances/<int:aid>/topups", methods=["POST"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_topups_add(aid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    data = request.json or {}
    amount = data.get("amount")
    topup_date = (data.get("topup_date") or "").strip()
    proof_link = normalize_url(data.get("proof_link") or "")
    ref_type = (data.get("ref_type") or "").strip().upper()   # e.g. FINANCE
    ref_id = (data.get("ref_id") or "").strip()               # e.g. "123"
    note = (data.get("note") or "").strip()

    try:
        amt = float(amount)
        if amt <= 0:
            raise ValueError()
    except:
        return jsonify({"ok": False, "error": "Top-up amount must be a number > 0"}), 400

    conn = db()
    adv = conn.execute("SELECT id FROM cash_advances WHERE id=?", (aid,)).fetchone()
    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    conn.execute("""
        INSERT INTO cash_advance_topups
          (advance_id, amount, topup_date, proof_link, ref_type, ref_id, note, created_at, created_by)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (aid, float(amt), topup_date, proof_link, ref_type, ref_id, note, now_iso(), session["user"]))
    conn.commit()

    new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    row = conn.execute("SELECT * FROM cash_advance_topups WHERE id=?", (new_id,)).fetchone()
    conn.close()
    return jsonify({"ok": True, "data": dict(row)})


@app.route("/api/topups/<int:tid>", methods=["DELETE"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_topups_delete(tid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    row = conn.execute("SELECT id FROM cash_advance_topups WHERE id=?", (tid,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Top-up not found"}), 404

    conn.execute("DELETE FROM cash_advance_topups WHERE id=?", (tid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": {"id": tid}})



# ==========================
# CASH ADVANCES EXTRA FEATURES
# (Edit/Delete Advance + Summary + CSV)
# ==========================
@app.route("/api/advances/<int:aid>", methods=["PUT"])
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
    except:
        return jsonify({"ok": False, "error": "amount_given must be a number > 0"}), 400

    conn = db()
    adv = conn.execute("SELECT id FROM cash_advances WHERE id=? LIMIT 1", (aid,)).fetchone()
    if not adv:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    emp = conn.execute("""
        SELECT username FROM users
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


@app.route("/api/advances/<int:aid>", methods=["DELETE"])
@login_required
@require_module("CASH_ADVANCES", need_edit=True)
def api_advances_delete(aid):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()
    row = conn.execute("SELECT id FROM cash_advances WHERE id=? LIMIT 1", (aid,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Advance not found"}), 404

    conn.execute("DELETE FROM cash_advance_expenses WHERE advance_id=?", (aid,))
    conn.execute("DELETE FROM cash_advance_topups WHERE advance_id=?", (aid,))
    conn.execute("DELETE FROM cash_advances WHERE id=?", (aid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "data": {"id": aid}})

@app.route("/api/advances/summary", methods=["GET"])
@login_required
@require_module("CASH_ADVANCES")
def api_advances_summary():
    employee = (request.args.get("employee") or "").strip()
    month = (request.args.get("month") or "").strip()   # YYYY-MM
    status = (request.args.get("status") or "").strip().upper()  # OPEN/CLOSED

    # EMP can only see their own
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
          ROUND(SUM(ca.amount_given), 2) AS total_given,

          ROUND(SUM(
            COALESCE((
              SELECT SUM(t.amount)
              FROM cash_advance_topups t
              WHERE t.advance_id = ca.id
            ), 0)
          ), 2) AS total_topups,

          ROUND(SUM(
            COALESCE((
              SELECT SUM(e.amount)
              FROM cash_advance_expenses e
              WHERE e.advance_id = ca.id
            ), 0)
          ), 2) AS total_spent,

          COUNT(*) AS count
        FROM cash_advances ca
        WHERE {" AND ".join(where)}
        GROUP BY ca.employee_username, ca.currency
        ORDER BY ca.employee_username ASC, ca.currency ASC
    """, params).fetchall()
    conn.close()

    data = []
    for r in rows:
        topups = float(r["total_topups"] or 0)
        given  = float(r["total_given"] or 0)
        spent  = float(r["total_spent"] or 0)
        count  = int(r["count"] or 0)

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

# ==========================
# MESSAGES API
# ==========================

@app.route("/api/messages/users", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_users():
    conn = db()
    rows = get_active_user_brief_rows(conn)
    me_id = int(session.get("uid"))
    out = [dict(r) for r in rows if int(r["id"]) != me_id]
    conn.close()
    return jsonify({"ok": True, "data": out})


@app.route("/api/messages/unread-count", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_unread_count():
    conn = db()
    uid = int(session.get("uid"))

    rows = conn.execute("""
        SELECT mc.id
        FROM message_conversations mc
        JOIN message_conversation_members m
          ON m.conversation_id = mc.id
        WHERE mc.active=1
          AND m.user_id=?
          AND m.active=1
          AND NOT EXISTS (
              SELECT 1
              FROM message_conversation_deleted d
              WHERE d.conversation_id = mc.id
                AND d.user_id = ?
                AND COALESCE(d.active,1)=1
          )
    """, (uid, uid)).fetchall()

    total = sum(get_conversation_unread_count(conn, int(r["id"]), uid) for r in rows)
    conn.close()
    return jsonify({"ok": True, "data": {"unread_count": total}})


@app.route("/api/messages/conversations", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_conversations():
    conn = db()
    uid = int(session.get("uid"))

    rows = conn.execute("""
        SELECT mc.*
        FROM message_conversations mc
        JOIN message_conversation_members m
          ON m.conversation_id = mc.id
        WHERE mc.active=1
          AND m.user_id=?
          AND m.active=1
          AND NOT EXISTS (
              SELECT 1
              FROM message_conversation_deleted d
              WHERE d.conversation_id = mc.id
                AND d.user_id = ?
                AND COALESCE(d.active,1)=1
          )
        ORDER BY mc.id DESC
    """, (uid, uid)).fetchall()

    data = []
    for r in rows:
        item = serialize_conversation(conn, r, uid)

        # Fix direct chat title -> show the other person's name
        if (r["conversation_type"] or "").upper() == "DIRECT":
            other = conn.execute("""
                SELECT u.username, u.full_name
                FROM message_conversation_members m
                JOIN users u ON u.id = m.user_id
                WHERE m.conversation_id=?
                  AND m.active=1
                  AND u.id<>?
                LIMIT 1
            """, (r["id"], uid)).fetchone()

            if other:
                display_name = other["full_name"] or other["username"]
                item["other_user_name"] = display_name
                item["title"] = display_name

        data.append(item)

    data.sort(
        key=lambda x: (x["last_message"]["created_at"] if x.get("last_message") else x["created_at"]),
        reverse=True
    )

    conn.close()
    return jsonify({"ok": True, "data": data})


@app.route("/api/messages/direct", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_create_direct():
    data = request.json or {}
    target_user_id = data.get("user_id")

    try:
        target_user_id = int(target_user_id)
    except Exception:
        return jsonify({"ok": False, "error": "Valid user_id is required"}), 400

    uid = int(session.get("uid"))
    if target_user_id == uid:
        return jsonify({"ok": False, "error": "You cannot message yourself here"}), 400

    conn = db()
    target = conn.execute("""
        SELECT id, active, username, full_name
        FROM users
        WHERE id=?
        LIMIT 1
    """, (target_user_id,)).fetchone()

    if not target or int(target["active"] or 0) != 1:
        conn.close()
        return jsonify({"ok": False, "error": "User not found"}), 404

    conversation_id = ensure_direct_conversation(conn, uid, target_user_id)

    # If user previously deleted this chat, restore it for them
    conn.execute("""
        UPDATE message_conversation_deleted
        SET active=0
        WHERE conversation_id=? AND user_id=?
    """, (conversation_id, uid))

    conn.commit()

    row = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=?
    """, (conversation_id,)).fetchone()

    payload = serialize_conversation(conn, row, uid)
    payload["other_user_name"] = target["full_name"] or target["username"]
    payload["title"] = target["full_name"] or target["username"]

    conn.close()
    return jsonify({"ok": True, "data": payload})


@app.route("/api/messages/groups", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_create_group():
    data = request.json or {}
    title = (data.get("title") or "").strip()
    member_user_ids = data.get("member_user_ids") or []

    if not title:
        return jsonify({"ok": False, "error": "Group title is required"}), 400

    clean_ids = set()
    for x in member_user_ids:
        try:
            clean_ids.add(int(x))
        except Exception:
            continue

    clean_ids.add(int(session.get("uid")))

    conn = db()

    if not clean_ids:
        conn.close()
        return jsonify({"ok": False, "error": "No members selected"}), 400

    placeholders = ",".join("?" for _ in clean_ids)
    existing = conn.execute(f"""
        SELECT id
        FROM users
        WHERE active=1
          AND id IN ({placeholders})
    """, tuple(clean_ids)).fetchall()

    existing_ids = {int(r["id"]) for r in existing}

    if int(session.get("uid")) not in existing_ids:
        conn.close()
        return jsonify({"ok": False, "error": "Current user missing"}), 400

    now = now_iso()

    conn.execute("""
        INSERT INTO message_conversations (
            conversation_type,
            title,
            created_at,
            created_by,
            active
        ) VALUES ('GROUP', ?, ?, ?, 1)
    """, (title, now, session["user"]))

    cid = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

    for member_uid in sorted(existing_ids):
        conn.execute("""
            INSERT INTO message_conversation_members (
                conversation_id,
                user_id,
                joined_at,
                active,
                last_read_message_id
            ) VALUES (?,?,?,?,NULL)
        """, (cid, member_uid, now, 1))

    conn.commit()

    row = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=?
    """, (cid,)).fetchone()

    payload = serialize_conversation(conn, row, int(session.get("uid")))
    conn.close()
    return jsonify({"ok": True, "data": payload})


@app.route("/api/messages/groups/<int:conversation_id>/add-members", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_group_add_members(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    convo = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=? AND active=1
        LIMIT 1
    """, (conversation_id,)).fetchone()

    if not convo:
        conn.close()
        return jsonify({"ok": False, "error": "Conversation not found"}), 404

    if (convo["conversation_type"] or "").upper() != "GROUP":
        conn.close()
        return jsonify({"ok": False, "error": "This is not a group"}), 400

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    data = request.json or {}
    member_user_ids = data.get("member_user_ids") or []

    clean_ids = set()
    for x in member_user_ids:
        try:
            clean_ids.add(int(x))
        except Exception:
            continue

    if not clean_ids:
        conn.close()
        return jsonify({"ok": False, "error": "No members selected"}), 400

    placeholders = ",".join("?" for _ in clean_ids)
    rows = conn.execute(f"""
        SELECT id
        FROM users
        WHERE active=1
          AND id IN ({placeholders})
    """, tuple(clean_ids)).fetchall()

    valid_ids = {int(r["id"]) for r in rows}
    now = now_iso()

    for member_uid in valid_ids:
        exists = conn.execute("""
            SELECT id
            FROM message_conversation_members
            WHERE conversation_id=? AND user_id=?
            LIMIT 1
        """, (conversation_id, member_uid)).fetchone()

        if exists:
            conn.execute("""
                UPDATE message_conversation_members
                SET active=1
                WHERE conversation_id=? AND user_id=?
            """, (conversation_id, member_uid))
        else:
            conn.execute("""
                INSERT INTO message_conversation_members (
                    conversation_id,
                    user_id,
                    joined_at,
                    active,
                    last_read_message_id
                ) VALUES (?,?,?,?,NULL)
            """, (conversation_id, member_uid, now, 1))

        # if this user had deleted the chat before, restore it
        conn.execute("""
            UPDATE message_conversation_deleted
            SET active=0
            WHERE conversation_id=? AND user_id=?
        """, (conversation_id, member_uid))

    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/messages/conversations/<int:conversation_id>", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_conversation_get(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    row = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=?
          AND active=1
    """, (conversation_id,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Conversation not found"}), 404

    payload = serialize_conversation(conn, row, uid)

    if (row["conversation_type"] or "").upper() == "DIRECT":
        other = conn.execute("""
            SELECT u.username, u.full_name
            FROM message_conversation_members m
            JOIN users u ON u.id = m.user_id
            WHERE m.conversation_id=?
              AND m.active=1
              AND u.id<>?
            LIMIT 1
        """, (conversation_id, uid)).fetchone()
        if other:
            payload["other_user_name"] = other["full_name"] or other["username"]
            payload["title"] = other["full_name"] or other["username"]

    conn.close()
    return jsonify({"ok": True, "data": payload})


@app.route("/api/messages/conversations/<int:conversation_id>/members", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_conversation_members(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    rows = conn.execute("""
        SELECT u.id, u.username, u.full_name, u.role
        FROM message_conversation_members m
        JOIN users u ON u.id = m.user_id
        WHERE m.conversation_id=?
          AND m.active=1
        ORDER BY COALESCE(u.full_name, u.username) ASC
    """, (conversation_id,)).fetchall()

    conn.close()
    return jsonify({"ok": True, "data": [dict(r) for r in rows]})


@app.route("/api/messages/conversations/<int:conversation_id>/messages", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_list(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    rows = conn.execute("""
        SELECT
            mm.*,
            u.username AS sender_username,
            u.full_name AS sender_full_name
        FROM message_messages mm
        JOIN users u
          ON u.id = mm.sender_user_id
        WHERE mm.conversation_id=?
        ORDER BY mm.id ASC
        LIMIT 2000
    """, (conversation_id,)).fetchall()

    mark_conversation_read(conn, conversation_id, uid)
    conn.commit()

    data = []
    for r in rows:
        item = dict(r)
        item["can_delete"] = False if item.get("deleted_at") else can_delete_message(r, uid)
        data.append(item)

    conn.close()
    return jsonify({"ok": True, "data": data})


@app.route("/api/messages/conversations/<int:conversation_id>/read", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_mark_read(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    mark_conversation_read(conn, conversation_id, uid)
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/messages/conversations/<int:conversation_id>/messages", methods=["POST"])
@login_required
@require_module("MESSAGES", need_edit=True)
def api_messages_send(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    message_text = ""
    attachment_name = None
    attachment_url = None
    attachment_mime = None
    reply_to = None

    if request.content_type and request.content_type.startswith("multipart/form-data"):
        message_text = (request.form.get("message_text") or "").strip()
        reply_to = request.form.get("reply_to")
        file = request.files.get("file")
        att = save_message_attachment(file) if file else None

        if att:
            attachment_name = att["attachment_name"]
            attachment_url = att["attachment_url"]
            attachment_mime = att["attachment_mime"]
    else:
        data = request.json or {}
        message_text = (data.get("message_text") or "").strip()
        reply_to = data.get("reply_to")
        attachment_name = (data.get("attachment_name") or "").strip() or None
        attachment_url = (data.get("attachment_url") or "").strip() or None
        attachment_mime = (data.get("attachment_mime") or "").strip() or None

    if reply_to in ("", None):
        reply_to = None
    else:
        try:
            reply_to = int(reply_to)
        except Exception:
            reply_to = None

    if not message_text and not attachment_url:
        conn.close()
        return jsonify({"ok": False, "error": "Message or attachment is required"}), 400

    now = now_iso()

    conn.execute("""
        INSERT INTO message_messages (
            conversation_id,
            sender_user_id,
            message_text,
            reply_to,
            attachment_name,
            attachment_url,
            attachment_mime,
            created_at
        ) VALUES (?,?,?,?,?,?,?,?)
    """, (
        conversation_id,
        uid,
        message_text,
        reply_to,
        attachment_name,
        attachment_url,
        attachment_mime,
        now
    ))

    message_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

    conn.execute("""
        UPDATE message_conversation_members
        SET last_read_message_id=?
        WHERE conversation_id=? AND user_id=?
    """, (message_id, conversation_id, uid))

    conn.commit()

    row = conn.execute("""
        SELECT
            mm.*,
            u.username AS sender_username,
            u.full_name AS sender_full_name
        FROM message_messages mm
        JOIN users u
          ON u.id = mm.sender_user_id
        WHERE mm.id=?
    """, (message_id,)).fetchone()

    payload = dict(row)
    payload["can_delete"] = can_delete_message(row, uid)

    conn.close()
    return jsonify({"ok": True, "data": payload})


@app.route("/api/messages/messages/<int:message_id>", methods=["DELETE"])
@login_required
@require_module("MESSAGES", need_edit=True)
def api_messages_delete(message_id):
    conn = db()

    row = conn.execute("""
        SELECT *
        FROM message_messages
        WHERE id=?
        LIMIT 1
    """, (message_id,)).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Message not found"}), 404

    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, int(row["conversation_id"]), uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    if row["deleted_at"]:
        conn.close()
        return jsonify({"ok": True})

    if not can_delete_message(row, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Delete window expired"}), 400

    conn.execute("""
        UPDATE message_messages
        SET deleted_at=?,
            deleted_by=?,
            message_text='This message was deleted',
            attachment_name=NULL,
            attachment_url=NULL,
            attachment_mime=NULL
        WHERE id=?
    """, (now_iso(), session["user"], message_id))

    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/messages/conversations/<int:conversation_id>/delete", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_delete_conversation(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    if not message_can_access_conversation(conn, conversation_id, uid):
        conn.close()
        return jsonify({"ok": False, "error": "Not allowed"}), 403

    exists = conn.execute("""
        SELECT id
        FROM message_conversation_deleted
        WHERE conversation_id=? AND user_id=?
        LIMIT 1
    """, (conversation_id, uid)).fetchone()

    if exists:
        conn.execute("""
            UPDATE message_conversation_deleted
            SET deleted_at=?, deleted_by=?, active=1
            WHERE conversation_id=? AND user_id=?
        """, (now_iso(), session["user"], conversation_id, uid))
    else:
        conn.execute("""
            INSERT INTO message_conversation_deleted (
                conversation_id,
                user_id,
                deleted_at,
                deleted_by,
                active
            ) VALUES (?,?,?,?,1)
        """, (conversation_id, uid, now_iso(), session["user"]))

    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/messages/trash", methods=["GET"])
@login_required
@require_module("MESSAGES")
def api_messages_trash():
    conn = db()
    uid = int(session.get("uid"))

    rows = conn.execute("""
        SELECT mc.*, d.deleted_at AS user_deleted_at
        FROM message_conversation_deleted d
        JOIN message_conversations mc ON mc.id = d.conversation_id
        WHERE d.user_id=?
          AND COALESCE(d.active,1)=1
        ORDER BY d.deleted_at DESC, mc.id DESC
    """, (uid,)).fetchall()

    data = []
    for r in rows:
        item = serialize_conversation(conn, r, uid)
        item["user_deleted_at"] = r["user_deleted_at"]

        if (r["conversation_type"] or "").upper() == "DIRECT":
            other = conn.execute("""
                SELECT u.username, u.full_name
                FROM message_conversation_members m
                JOIN users u ON u.id = m.user_id
                WHERE m.conversation_id=?
                  AND m.active=1
                  AND u.id<>?
                LIMIT 1
            """, (r["id"], uid)).fetchone()
            if other:
                item["other_user_name"] = other["full_name"] or other["username"]
                item["title"] = other["full_name"] or other["username"]

        data.append(item)

    conn.close()
    return jsonify({"ok": True, "data": data})


@app.route("/api/messages/conversations/<int:conversation_id>/restore", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_restore_conversation(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    conn.execute("""
        UPDATE message_conversation_deleted
        SET active=0
        WHERE conversation_id=? AND user_id=?
    """, (conversation_id, uid))

    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/messages/conversations/<int:conversation_id>/permanent-delete", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_permanent_delete_conversation(conversation_id):
    if not is_admin():
        return jsonify({"ok": False, "error": "Admin only"}), 403

    conn = db()

    # remove conversation + members + messages + deleted markers
    conn.execute("DELETE FROM message_conversation_deleted WHERE conversation_id=?", (conversation_id,))
    conn.execute("DELETE FROM message_conversation_members WHERE conversation_id=?", (conversation_id,))
    conn.execute("DELETE FROM message_messages WHERE conversation_id=?", (conversation_id,))
    conn.execute("DELETE FROM message_conversations WHERE id=?", (conversation_id,))

    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/messages/groups/<int:conversation_id>/leave", methods=["POST"])
@login_required
@require_module("MESSAGES")
def api_messages_leave_group(conversation_id):
    conn = db()
    uid = int(session.get("uid"))

    convo = conn.execute("""
        SELECT *
        FROM message_conversations
        WHERE id=? AND active=1
        LIMIT 1
    """, (conversation_id,)).fetchone()

    if not convo:
        conn.close()
        return jsonify({"ok": False, "error": "Conversation not found"}), 404

    if (convo["conversation_type"] or "").upper() != "GROUP":
        conn.close()
        return jsonify({"ok": False, "error": "This is not a group"}), 400

    conn.execute("""
        UPDATE message_conversation_members
        SET active=0
        WHERE conversation_id=? AND user_id=?
    """, (conversation_id, uid))

    # also move it to deleted for this user
    exists = conn.execute("""
        SELECT id
        FROM message_conversation_deleted
        WHERE conversation_id=? AND user_id=?
        LIMIT 1
    """, (conversation_id, uid)).fetchone()

    if exists:
        conn.execute("""
            UPDATE message_conversation_deleted
            SET deleted_at=?, deleted_by=?, active=1
            WHERE conversation_id=? AND user_id=?
        """, (now_iso(), session["user"], conversation_id, uid))
    else:
        conn.execute("""
            INSERT INTO message_conversation_deleted (
                conversation_id,
                user_id,
                deleted_at,
                deleted_by,
                active
            ) VALUES (?,?,?,?,1)
        """, (conversation_id, uid, now_iso(), session["user"]))

    conn.commit()
    conn.close()
    return jsonify({"ok": True})

    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
