from contextlib import ExitStack
import importlib
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import psycopg
from flask import Flask
from routes.auth import auth_bp
from routes.db import connect, register_database_error_handlers
from routes.login_security import set_device_cookie

MODULES = [
 ('notes','notes_bp','ensure_notes_tables'),
 ('individual_achievement','individual_achievement_bp','ensure_individual_achievement_tables'),
 ('weekly_tasks','weekly_tasks_bp','ensure_weekly_task_tables'),
 ('calendar','calendar_bp','ensure_calendar_tables'),
 ('worksheet','worksheet_bp','ensure_worksheet_tables'),
 ('marketing_emails','marketing_emails_bp','ensure_marketing_email_tables'),
 ('hs_codes','hs_codes_bp','ensure_hs_code_tables'),
 ('attendance','attendance_bp','ensure_attendance_tables'),
]


class DatabaseResilienceTests(unittest.TestCase):
    def test_connections_and_statements_have_bounded_waits(self):
        conn=Mock()
        with patch('routes.db.psycopg.connect',return_value=conn) as driver:
            self.assertIs(connect('test-only-dsn'),conn)
        self.assertEqual(driver.call_args.kwargs['connect_timeout'],5)
        self.assertIsNone(driver.call_args.kwargs['prepare_threshold'])
        self.assertEqual(driver.call_args.kwargs['tcp_user_timeout'],10000)
        self.assertEqual(driver.call_args.kwargs['keepalives_idle'],10)
        self.assertIn("set_config('statement_timeout', '8000', true)",conn.execute.call_args.args[0])
        self.assertIn("set_config('lock_timeout', '3000', true)",conn.execute.call_args.args[0])

    def test_connection_is_closed_if_initial_configuration_fails(self):
        conn=Mock();conn.execute.side_effect=psycopg.OperationalError('test outage')
        with patch('routes.db.psycopg.connect',return_value=conn):
            with self.assertRaises(psycopg.OperationalError):connect('test-only-dsn')
        conn.close.assert_called_once()

    def app(self):
        app=Flask(__name__,template_folder=str(Path(__file__).resolve().parents[1]/'templates'))
        app.config.update(SECRET_KEY='test-only',TESTING=True,LOGIN_PROTECTION=Mock())
        app.register_blueprint(auth_bp)
        app.after_request(set_device_cookie)
        for name,blueprint,_ in MODULES:app.register_blueprint(getattr(importlib.import_module('routes.'+name),blueprint))
        app.add_url_rule('/dashboard',endpoint='pages.dashboard',view_func=lambda:'Dashboard')
        register_database_error_handlers(app)
        return app

    def test_public_and_dashboard_requests_do_not_initialize_unrelated_modules(self):
        app=self.app();client=app.test_client()
        with ExitStack() as stack:
            mocks=[stack.enter_context(patch('routes.'+name+'.'+ensure,side_effect=psycopg.OperationalError('test outage'))) for name,_,ensure in MODULES]
            for path in ('/','/login','/dashboard'):
                self.assertLess(client.get(path).status_code,400)
            with client.session_transaction() as session:session.update(user='Example',role='ADMIN')
            self.assertEqual(client.get('/dashboard').status_code,200)
            self.assertEqual(client.get('/login').status_code,200)
            for mock in mocks:mock.assert_not_called()
        self.assertNotIn(None,app.before_request_funcs)

    def test_a_module_setup_failure_is_contained_to_that_module(self):
        app=self.app();client=app.test_client()
        with client.session_transaction() as session:session.update(user='Example',role='ADMIN')
        with ExitStack() as stack:
            mocks={name:stack.enter_context(patch('routes.'+name+'.'+ensure,side_effect=psycopg.OperationalError('test outage'))) for name,_,ensure in MODULES}
            with self.assertLogs(app.logger,level='ERROR'):
                response=client.get('/api/notes/notification-count')
            self.assertEqual(response.status_code,503)
            self.assertEqual(response.json['code'],'DATABASE_UNAVAILABLE')
            self.assertEqual(response.headers['Retry-After'],'10')
            mocks['notes'].assert_called_once()
            for name,mock in mocks.items():
                if name!='notes':mock.assert_not_called()
            self.assertEqual(client.get('/login').status_code,200)
            self.assertEqual(client.get('/dashboard').status_code,200)

    def test_logged_out_api_request_does_not_initialize_tables(self):
        app=self.app();client=app.test_client()
        with patch('routes.notes.ensure_notes_tables') as setup:
            self.assertEqual(client.get('/api/notes/notification-count').status_code,401)
        setup.assert_not_called()

    def test_failed_schema_setup_leaves_context_to_rollback_and_close(self):
        module=importlib.import_module('routes.individual_achievement')
        app=Flask(__name__)
        context=Mock();connection=Mock();context.__enter__=Mock(return_value=connection);context.__exit__=Mock(return_value=False)
        connection.cursor.return_value.execute.side_effect=psycopg.OperationalError('test failure')
        with app.app_context(),patch.object(module,'db',return_value=context):
            with self.assertRaises(psycopg.OperationalError):module.ensure_individual_achievement_tables()
        self.assertEqual(context.__exit__.call_args.args[0],psycopg.OperationalError)
