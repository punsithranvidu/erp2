from flask import Blueprint, render_template, request, redirect, url_for, session

auth_bp = Blueprint("auth", __name__)

@auth_bp.route("/", methods=["GET"])
def home():
    # If logged in, go dashboard; else show login
    return redirect(url_for("pages.dashboard") if "user" in session else url_for("auth.login"))

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    from flask import current_app
    auth_user = current_app.config["AUTH_USER_FUNC"]
    register_failed_login = current_app.config.get("REGISTER_FAILED_LOGIN_FUNC")
    clear_failed_logins = current_app.config.get("CLEAR_FAILED_LOGINS_FUNC")
    failed_login_delay = current_app.config.get("FAILED_LOGIN_DELAY_FUNC")

    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        user, err = auth_user(username, password)

        if err:
            if callable(register_failed_login):
                register_failed_login(username)
            if callable(failed_login_delay):
                failed_login_delay()
            return render_template("login.html", error=err)

        if callable(clear_failed_logins):
            clear_failed_logins()
        session["user"] = user["username"]
        session["role"] = user["role"]
        session["uid"] = user["id"]
        return redirect(url_for("pages.dashboard"))

    return render_template("login.html", error=None)

@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))
