"""Shared, revocable sign-in sessions. Background API polling is not activity."""
import hashlib
import secrets
import time
from urllib.parse import urlsplit

from flask import current_app, jsonify, redirect, request, session, url_for, g

IDLE_SECONDS = 30 * 60
MAX_SECONDS = 12 * 60 * 60


def init_session_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_login_sessions (
            session_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            created_at DOUBLE PRECISION NOT NULL,
            last_activity DOUBLE PRECISION NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS user_login_sessions_activity_idx ON user_login_sessions(last_activity)")


class SessionStore:
    def __init__(self, connect):
        self.connect = connect

    def create(self, sid, username, now):
        with self.connect() as conn:
            conn.execute("INSERT INTO user_login_sessions VALUES (%s,%s,%s,%s)",
                         (sid, username, now, now))
            # At most one cleanup on sign-in, never on frequent polling requests.
            conn.execute("DELETE FROM user_login_sessions WHERE last_activity <= %s OR created_at <= %s",
                         (now - IDLE_SECONDS, now - MAX_SECONDS))

    def get(self, sid):
        with self.connect() as conn:
            return conn.execute("SELECT * FROM user_login_sessions WHERE session_id=%s", (sid,)).fetchone()

    def touch(self, sid, now):
        with self.connect() as conn:
            return conn.execute("""
                UPDATE user_login_sessions SET last_activity=GREATEST(last_activity,%s)
                WHERE session_id=%s AND last_activity>%s AND created_at>%s
                RETURNING *
            """, (now, sid, now - IDLE_SECONDS, now - MAX_SECONDS)).fetchone()

    def revoke(self, sid):
        if sid:
            with self.connect() as conn:
                conn.execute("DELETE FROM user_login_sessions WHERE session_id=%s", (sid,))


def configure_sessions(app, connect=None, store=None, clock=time.time):
    store = store or SessionStore(connect)
    app.config['SESSION_STORE'] = store

    def start():
        # Rotate the session identifier on every successful authentication.
        previous = session.get('login_session_id')
        store.revoke(previous)
        sid = secrets.token_urlsafe(32)
        store.create(sid, session['user'], clock())
        session['login_session_id'] = sid
        session.permanent = False

    def end():
        store.revoke(session.get('login_session_id'))
        session.clear()

    app.config['SESSION_START_FUNC'] = start
    app.config['SESSION_END_FUNC'] = end

    def expired_response():
        session.clear()
        if request.path.startswith('/api/'):
            return jsonify(ok=False, code='SESSION_EXPIRED', error='Your session has ended. Please log in again.'), 401
        if request.path != '/login':
            return redirect(url_for('auth.login', reason='expired'))
        return None

    @app.before_request
    def enforce_session():
        if request.path.startswith('/static/') or request.path == '/login' or 'user' not in session:
            return None
        sid = session.get('login_session_id')
        try:
            record = store.get(sid) if sid else None
            now = clock()
            if (not record or record['username'] != session['user'] or
                now - record['last_activity'] >= IDLE_SECONDS or
                now - record['created_at'] >= MAX_SECONDS):
                # Expired records cannot be revived by touch(); sign-in cleanup
                # removes them without adding a second database call to this request.
                return expired_response()
            # Only a foreground page navigation or explicit activity heartbeat
            # refreshes inactivity. GET API polls never keep a session alive.
            if request.method == 'GET' and not request.path.startswith('/api/') and request.path not in ('/login', '/logout'):
                record = store.touch(sid, now)
                if not record:
                    return expired_response()
            g.login_session = record
        except Exception:
            current_app.logger.exception('Session verification unavailable')
            if request.path.startswith('/api/'):
                return jsonify(ok=False, error='Session verification is temporarily unavailable. Please retry.'), 503
            return 'Sign-in verification is temporarily unavailable. Please retry shortly.', 503

    def state(record):
        return dict(ok=True, user=record['username'],
                    session_tag=hashlib.sha256(record['session_id'].encode()).hexdigest()[:16],
                    server_time=clock(), idle_expires_at=record['last_activity'] + IDLE_SECONDS,
                    absolute_expires_at=record['created_at'] + MAX_SECONDS, warning_seconds=120)

    def authenticated():
        return 'user' in session and getattr(g, 'login_session', None) is not None

    def same_origin():
        # Custom headers force a CORS preflight; cross-origin preflights are
        # rejected explicitly (the application also uses Flask-CORS elsewhere).
        if request.headers.get('X-ERP-Activity') != '1':
            return False
        origin = request.headers.get('Origin')
        return not origin or (urlsplit(origin).scheme in ('http', 'https') and urlsplit(origin).netloc == request.host)

    @app.route('/api/session/status', methods=['GET'])
    def session_status():
        if not authenticated():
            return jsonify(ok=False, code='SESSION_EXPIRED'), 401
        return jsonify(state(g.login_session))

    @app.route('/api/session/activity', methods=['POST'])
    def session_activity():
        if not authenticated():
            return jsonify(ok=False, code='SESSION_EXPIRED'), 401
        if not same_origin():
            return jsonify(ok=False, error='Invalid activity request.'), 403
        record = store.touch(session['login_session_id'], clock())
        if not record:
            return expired_response()
        return jsonify(state(record))

    @app.route('/api/session/end', methods=['POST'])
    def session_end():
        if not same_origin():
            return jsonify(ok=False, error='Invalid sign-out request.'), 403
        end()
        return jsonify(ok=True)

    @app.after_request
    def protect_cached_pages(response):
        if not request.path.startswith('/static/'):
            response.headers['Cache-Control'] = 'no-store'
        return response
