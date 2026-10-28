from flask import Blueprint, render_template, request, redirect, url_for, session

auth_bp = Blueprint("auth", __name__)

@auth_bp.route("/", methods=["GET"])
def home():
    # If logged in, go dashboard; else show login
    return redirect(url_for("pages.dashboard") if "user" in session else url_for("auth.login"))

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    from flask import current_app, g, make_response
    from routes.login_security import browser_key

    key = browser_key()
    protection = current_app.config["LOGIN_PROTECTION"]
    try:
        if request.method == "POST" and getattr(g, "login_device_missing", False):
            # No password verification without a valid signed browser cookie.
            # Ordinary browsers receive it on GET; cookie-less scripts must too.
            return render_template("login.html", error="Please enable cookies and try signing in again."), 400
        result = protection.check(
            key,
            authenticate=current_app.config["AUTH_USER_FUNC"] if request.method == "POST" else None,
            username=request.form.get("username"),
            password=request.form.get("password"),
        )
    except Exception:
        current_app.logger.exception("Login protection or authentication unavailable")
        return render_template("login.html", error="Sign-in is temporarily unavailable. Please try again shortly."), 503

    if result.user is not None:
        old_sid = session.get('login_session_id')
        session.clear()
        if old_sid:
            session['login_session_id'] = old_sid
        session["user"] = result.user["username"]
        session["role"] = result.user["role"]
        session["uid"] = result.user["id"]
        start_session = current_app.config.get('SESSION_START_FUNC')
        if callable(start_session):
            try:
                start_session()
            except Exception:
                session.clear()
                current_app.logger.exception('Unable to create sign-in session')
                return render_template('login.html', error='Sign-in is temporarily unavailable. Please try again shortly.'), 503
        return redirect(url_for("pages.dashboard"))

    response = make_response(render_template(
        "login.html", error=result.error, retry_after=result.retry_after,
    ), 429 if result.retry_after else 200)
    response.headers["Cache-Control"] = "no-store"
    if result.retry_after:
        response.headers["Retry-After"] = str(result.retry_after)
    return response

@auth_bp.route("/logout")
def logout():
    from flask import current_app
    end_session = current_app.config.get('SESSION_END_FUNC')
    if callable(end_session):
        end_session()
    else:
        session.clear()
    return redirect(url_for("auth.login"))
