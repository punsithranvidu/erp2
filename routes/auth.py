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

    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        user, err = auth_user(username, password)

        if err:
            return render_template("login.html", error=err)

        session["user"] = user["username"]
        session["role"] = user["role"]
        session["uid"] = user["id"]
        return redirect(url_for("pages.dashboard"))

    return render_template("login.html", error=None)

@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))
