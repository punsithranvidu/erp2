from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify
from functools import wraps

pages_bp = Blueprint("pages", __name__)

def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("auth.login"))
        return f(*args, **kwargs)
    return wrapped

def require_module(module: str, need_edit: bool = False):
    def deco(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            from flask import current_app
            has_access = current_app.config["HAS_MODULE_ACCESS_FUNC"]
            if not has_access(module, need_edit=need_edit):
                if request.path.startswith("/api/"):
                    return jsonify({"ok": False, "error": f"No permission for {module}{' (edit)' if need_edit else ''}"}), 403
                return redirect(url_for("pages.dashboard"))
            return f(*args, **kwargs)
        return wrapped
    return deco

@pages_bp.route("/dashboard")
@login_required
def dashboard():
    if session.get("role") == "ADMIN":
        return render_template("admin_dashboard.html", user=session["user"], role=session["role"])
    return render_template("emp_dashboard.html", user=session["user"], role=session["role"])

@pages_bp.route("/finance")
@login_required
@require_module("FINANCE")
def finance_page():
    return render_template("finance.html", user=session["user"], role=session["role"])

@pages_bp.route("/finance-trash")
@login_required
@require_module("FINANCE_TRASH")
def finance_trash_page():
    return render_template("finance_trash.html", user=session["user"], role=session["role"])

@pages_bp.route("/cash-advances")
@login_required
@require_module("CASH_ADVANCES")
def cash_advances_page():
    return render_template("cash_advances.html", user=session["user"], role=session["role"])

@pages_bp.route("/users")
@login_required
@require_module("USERS")
def users_page():
    return render_template("users.html", user=session["user"], role=session["role"])

@pages_bp.route("/document-storage")
@login_required
@require_module("DOCUMENT_STORAGE")
def document_storage_page():
    return render_template("document_storage.html", user=session["user"], role=session["role"])


@pages_bp.route("/messages")
@login_required
@require_module("MESSAGES")
def messages_page():
    return render_template("messages.html", user=session["user"], role=session["role"])




@pages_bp.route("/invoices")
@login_required
@require_module("INVOICES")
def invoices_page():
    return render_template("invoices.html", user=session["user"], role=session["role"])


@pages_bp.route("/calendar")
@login_required
@require_module("CALENDAR")
def calendar_page():
    return render_template("calendar.html", user=session["user"], role=session["role"])


from flask import Blueprint, render_template, session

hs_codes_bp = Blueprint("hs_codes", __name__)

@hs_codes_bp.route("/hs-codes", methods=["GET"])
def hs_codes_page():
    return render_template("hs_codes.html", user=session["user"], role=session["role"])



