"""Verify expiry using server time and actual session-store SQL in isolation."""
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch
from flask import Flask, jsonify, session
from routes.auth import auth_bp
from routes.login_security import LoginResult, set_device_cookie
from routes.session_security import configure_sessions, init_session_table, IDLE_SECONDS, MAX_SECONDS


class Connection:
    def __init__(self, conn): self.conn = conn
    def execute(self, sql, params=()): return self.conn.execute(sql.replace('%s','?'), params)
    def __enter__(self): return self
    def __exit__(self, error, value, traceback):
        self.conn.rollback() if error else self.conn.commit()


class Protection:
    def check(self, key, authenticate=None, **kwargs):
        return LoginResult(user=dict(username='Punsith', role='ADMIN', id=1)) if authenticate else LoginResult()


class SessionSecurityTests(unittest.TestCase):
    def setUp(self):
        self.now = 1000000
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = lambda cursor, row: dict(zip([c[0] for c in cursor.description], row))
        self.db.create_function('GREATEST', 2, max)
        init_session_table(Connection(self.db))
        self.db.commit()
        self.app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'templates'))
        self.app.config.update(SECRET_KEY='test-secret', TESTING=True, SESSION_COOKIE_SECURE=False,
                               LOGIN_PROTECTION=Protection(), AUTH_USER_FUNC=lambda *a: None)
        configure_sessions(self.app, connect=lambda: Connection(self.db), clock=lambda: self.now)
        self.app.register_blueprint(auth_bp)
        self.app.after_request(set_device_cookie)
        self.app.add_url_rule('/dashboard', endpoint='pages.dashboard', view_func=lambda: 'Dashboard')
        self.app.add_url_rule('/api/data', view_func=lambda: jsonify(ok=True) if 'user' in session else (jsonify(ok=False),401))
        self.client = self.app.test_client()
        self.login(self.client)

    def tearDown(self): self.db.close()

    def login(self, client):
        client.get('/login')
        return client.post('/login', data={'username':'Punsith', 'password':'test'})

    def test_session_cookie_is_not_persistent(self):
        response = self.login(self.client)
        cookie = next(value for value in response.headers.getlist('Set-Cookie') if value.startswith('session='))
        self.assertNotIn('Expires=', cookie)
        self.assertNotIn('Max-Age=', cookie)
        self.assertIn('HttpOnly', cookie)

    def test_background_api_polling_does_not_extend_idle_timeout(self):
        for minute in range(1, 30):
            self.now = 1000000 + minute * 60
            self.assertEqual(self.client.get('/api/data').status_code, 200)
            self.assertEqual(self.client.get('/api/session/status').status_code, 200)
        self.now = 1000000 + IDLE_SECONDS
        response = self.client.get('/api/data')
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json['code'], 'SESSION_EXPIRED')

    def test_user_activity_extends_idle_but_not_absolute_deadline(self):
        self.now += 1700
        response = self.client.post('/api/session/activity', headers={'X-ERP-Activity':'1'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['idle_expires_at'], self.now + IDLE_SECONDS)
        self.assertEqual(response.json['absolute_expires_at'], 1000000 + MAX_SECONDS)
        self.now += 1700
        self.assertEqual(self.client.get('/api/data').status_code, 200)

    def test_expired_session_cannot_be_revived_by_activity(self):
        self.now += IDLE_SECONDS
        self.assertEqual(self.client.post('/api/session/activity', headers={'X-ERP-Activity':'1'}).status_code, 401)

    def test_absolute_expiry_even_when_recently_active(self):
        self.db.execute('UPDATE user_login_sessions SET last_activity=?', (1000000 + MAX_SECONDS - 10,))
        self.db.commit()
        self.now = 1000000 + MAX_SECONDS
        self.assertEqual(self.client.get('/api/data').status_code, 401)

    def test_session_is_shared_across_tabs_but_other_devices_are_independent(self):
        tab = self.app.test_client()
        tab.set_cookie('session', self.client.get_cookie('session').value)
        other = self.app.test_client()
        self.login(other)
        self.now += 1700
        self.assertEqual(tab.post('/api/session/activity', headers={'X-ERP-Activity':'1'}).status_code, 200)
        self.now += 200
        self.assertEqual(self.client.get('/api/data').status_code, 200)
        self.assertEqual(other.get('/api/data').status_code, 401)

    def test_logout_revokes_even_a_copied_signed_cookie(self):
        cookie = self.client.get_cookie('session').value
        self.client.get('/logout')
        self.client.set_cookie('session', cookie)
        self.assertEqual(self.client.get('/api/data').status_code, 401)

    def test_legacy_sessions_require_one_fresh_sign_in(self):
        with self.client.session_transaction() as current:
            current.pop('login_session_id')
        response = self.client.get('/dashboard')
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login', response.location)

    def test_activity_and_end_require_same_origin_request(self):
        self.assertEqual(self.client.post('/api/session/activity').status_code, 403)
        headers={'X-ERP-Activity':'1', 'Origin':'https://attacker.example'}
        self.assertEqual(self.client.post('/api/session/activity', headers=headers).status_code, 403)
        self.assertEqual(self.client.post('/api/session/end', headers=headers).status_code, 403)
        self.assertEqual(self.client.post('/api/session/activity', headers={'X-ERP-Activity':'1', 'Origin':'https://localhost'}).status_code, 200)

    def test_protected_responses_are_not_cached(self):
        self.assertEqual(self.client.get('/dashboard').headers['Cache-Control'], 'no-store')
        self.assertEqual(self.client.get('/api/data').headers['Cache-Control'], 'no-store')

    def test_new_login_rotates_and_revokes_previous_session(self):
        old = self.client.get_cookie('session').value
        self.login(self.client)
        other = self.app.test_client()
        other.set_cookie('session', old)
        self.assertEqual(other.get('/api/data').status_code, 401)
        self.assertEqual(self.client.get('/api/data').status_code, 200)

    def test_public_login_does_not_depend_on_a_working_session_database(self):
        store = self.app.config['SESSION_STORE']
        with patch.object(store, 'get', side_effect=RuntimeError('test outage')) as get:
            self.assertEqual(self.client.get('/login').status_code, 200)
        get.assert_not_called()

    def test_idle_expiry_does_not_open_an_extra_connection_to_delete_the_row(self):
        self.now += IDLE_SECONDS
        store = self.app.config['SESSION_STORE']
        with patch.object(store, 'revoke') as revoke:
            self.assertEqual(self.client.get('/api/data').status_code, 401)
        revoke.assert_not_called()
