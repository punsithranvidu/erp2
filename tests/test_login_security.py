import copy
from contextlib import contextmanager
from pathlib import Path
import threading
import unittest
import unittest.mock

from flask import Blueprint, Flask
from routes.auth import auth_bp
from routes.login_security import (LoginProtection, COOKIE_NAME, set_device_cookie,
                                   HISTORY_SECONDS)


class MemoryStore:
    """Test store with the same transaction boundary as PostgreSQL row locking."""
    def __init__(self):
        self.records = {}
        self.lock = threading.Lock()

    @contextmanager
    def locked(self, key):
        with self.lock:
            state = copy.deepcopy(self.records.get(key, dict(
                failures=0, level=0, blocked_until=0, updated_at=0)))
            yield state
            self.records[key] = state


class LoginSecurityTests(unittest.TestCase):
    def setUp(self):
        self.now = 1000000
        self.calls = 0
        self.store = MemoryStore()
        self.protection = LoginProtection(self.store, lambda *a, **k: None,
                                          clock=lambda: self.now)

    def auth(self, username, password):
        self.calls += 1
        if password == 'correct':
            return dict(username='employee', role='EMP', id=1), None
        return None, 'Invalid username or password'

    def fail(self, key='browser-a'):
        return self.protection.check(key, self.auth, 'any-user', 'wrong')

    def test_attempt_counts_and_progressive_penalties(self):
        for duration in (900, 1800, 3600, 3600):
            for left in (3, 2, 1):
                result = self.fail()
                self.assertEqual(result.attempts_left, left)
                self.assertIn(f'{left} attempt', result.error)
            result = self.fail()
            self.assertEqual(result.retry_after, duration)
            calls = self.calls
            self.now += 1
            blocked = self.protection.check('browser-a', self.auth, 'employee', 'correct')
            self.assertIsNone(blocked.user)
            self.assertEqual(self.calls, calls)
            self.assertEqual(blocked.retry_after, duration - 1)
            self.now += duration - 1

    def test_other_device_and_successful_reset(self):
        for _ in range(4):
            self.fail()
        self.assertIsNotNone(self.protection.check('browser-b', self.auth, '', 'correct').user)
        self.assertEqual(self.protection.check('browser-a').retry_after, 900)
        self.now += 900
        self.protection.check('browser-a', self.auth, '', 'correct')
        self.assertEqual(self.fail().attempts_left, 3)
        for _ in range(3):
            result = self.fail()
        self.assertEqual(result.retry_after, 900)

    def test_history_expires_after_24_hours(self):
        self.fail()
        self.now += HISTORY_SECONDS
        self.assertEqual(self.fail().attempts_left, 3)

    def test_concurrent_failures_verify_only_four_passwords(self):
        threads = [threading.Thread(target=self.fail) for _ in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(self.calls, 4)

    def app(self):
        root = Path(__file__).resolve().parents[1]
        app = Flask(__name__, template_folder=str(root / 'templates'),
                    static_folder=str(root / 'static'))
        app.config.update(SECRET_KEY='test-only', TESTING=True, SESSION_COOKIE_SECURE=False,
                          LOGIN_PROTECTION=self.protection, AUTH_USER_FUNC=self.auth)
        pages = Blueprint('pages', __name__)
        pages.add_url_rule('/dashboard', endpoint='dashboard', view_func=lambda: 'Dashboard')
        app.register_blueprint(pages)
        app.register_blueprint(auth_bp)
        app.after_request(set_device_cookie)
        return app

    def test_shared_ip_isolation_ip_change_and_styled_429(self):
        app = self.app()
        a, b = app.test_client(), app.test_client()
        a.get('/login')
        b.get('/login')
        for _ in range(4):
            response = a.post('/login', data=dict(username='x', password='wrong'))
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers['Retry-After'], '900')
        self.assertIn(b'lockout-timer', response.data)
        self.assertIn(b'ERP Portal', response.data)
        changed_ip = a.post('/login', data=dict(username='different', password='correct'),
                            environ_overrides={'REMOTE_ADDR': '192.0.2.10'})
        self.assertEqual(changed_ip.status_code, 429)
        self.assertEqual(b.get('/login').status_code, 200)
        other = b.post('/login', data=dict(username='x', password='wrong'))
        self.assertIn(b'3 attempts left', other.data)

    def test_missing_and_forged_cookie_do_not_verify_password(self):
        client = self.app().test_client()
        response = client.post('/login', data=dict(username='x', password='correct'))
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.calls, 0)
        client.set_cookie(COOKIE_NAME, 'forged', path='/login')
        self.assertEqual(client.post('/login').status_code, 400)
        self.assertEqual(self.calls, 0)

    def test_success_preserves_existing_session_fields_and_redirect(self):
        client = self.app().test_client()
        client.get('/login')
        response = client.post('/login', data=dict(username='employee', password='correct'))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, '/dashboard')
        with client.session_transaction() as session:
            self.assertEqual(dict(session), dict(user='employee', role='EMP', uid=1))
        for _ in range(4):
            client.post('/login', data=dict(username='x', password='wrong'))
        # A login lock never clears an existing working session.
        with client.session_transaction() as session:
            self.assertEqual(session['user'], 'employee')

    def test_lockout_countdown_survives_refresh_without_database_access(self):
        app = self.app()
        client = app.test_client()
        client.get('/login')
        for _ in range(4):
            response = client.post('/login', data={'username':'x','password':'wrong'})
        self.assertEqual(response.status_code, 429)
        with unittest.mock.patch.object(self.store, 'locked', side_effect=AssertionError('GET must not query database')):
            response = client.get('/login')
        self.assertEqual(response.status_code, 429)
        self.assertIn(b'lockout-timer', response.data)

    def test_unavailable_store_is_login_only_503(self):
        app = self.app()
        class BrokenStore:
            def locked(self, key):
                raise RuntimeError('test outage')
        app.config['LOGIN_PROTECTION'] = LoginProtection(BrokenStore(), lambda *a, **k: None)
        client = app.test_client()
        self.assertEqual(client.get('/login').status_code, 200)
        with self.assertLogs(app.logger, level='ERROR'):
            response = client.post('/login', data={'username':'employee','password':'correct'})
        self.assertEqual(response.status_code, 503)
        self.assertIn(b'Sign-in is temporarily unavailable', response.data)
        with client.get('/static/css/login.css') as static_response:
            self.assertEqual(static_response.status_code, 200)


if __name__ == '__main__':
    unittest.main()
