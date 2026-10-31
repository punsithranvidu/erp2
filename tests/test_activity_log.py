from pathlib import Path
import re
from unittest.mock import Mock
import unittest
from flask import Flask
from routes.activity_log import activity_bp, configure_activity_log, filters, request_context, TABLE_MODULES

ROOT=Path(__file__).resolve().parents[1]

class ActivityAPITests(unittest.TestCase):
    def setUp(self):
        self.app=Flask(__name__,template_folder=str(ROOT/'templates'))
        self.app.config.update(TESTING=True,SECRET_KEY='test')
        self.db=Mock();self.conn=Mock()
        self.conn.execute.return_value.fetchone.return_value={'id':3}
        class Store:
            def __enter__(inner):return self.conn
            def __exit__(inner,*args):pass
        self.db.return_value=Store()
        configure_activity_log(self.app,self.db)
        self.app.register_blueprint(activity_bp)
        self.client=self.app.test_client()
    def login(self,role):
        with self.client.session_transaction() as s:s.update(user='Example',uid=3,role=role)
    def test_every_route_is_admin_only(self):
        for path in ('/activity-log','/api/activity-log','/api/activity-log/1','/api/activity-log/options'):
            self.assertEqual(self.client.get(path).status_code,401)
        self.login('EMP')
        for path in ('/activity-log','/api/activity-log','/api/activity-log/1','/api/activity-log/options'):
            self.assertEqual(self.client.get(path).status_code,403)
        self.db.assert_not_called()
    def test_invalid_filters_and_no_mutation_routes(self):
        self.login('ADMIN')
        for query in ('from=bad','from=2026-11-02&to=2026-11-01','before=-1'):
            self.assertEqual(self.client.get('/api/activity-log?'+query).status_code,400)
        for method in ('post','put','delete','patch'):
            self.assertEqual(getattr(self.client,method)('/api/activity-log/1').status_code,405)
        self.assertTrue(all('SELECT id FROM users' in call.args[0] for call in self.conn.execute.call_args_list))
    def test_keyset_pagination_is_bounded_and_uses_parameters(self):
        self.login('ADMIN')
        self.conn.execute.return_value.fetchall.return_value=[dict(id=n,actor='Example') for n in range(100,49,-1)]
        response=self.client.get('/api/activity-log?user=O%27Brien&before=200')
        self.assertEqual(response.status_code,200)
        self.assertEqual(len(response.json['items']),50)
        self.assertEqual(response.json['next_cursor'],'51')
        sql,args=self.conn.execute.call_args.args
        self.assertIn('LIMIT 51',sql);self.assertNotIn("O'Brien",sql);self.assertEqual(args,["O'Brien",200])
        self.assertEqual(response.headers['Cache-Control'],'no-store')
    def test_sri_lanka_date_boundaries_and_actor_identity(self):
        with self.app.test_request_context('/api/activity-log?from=2026-11-01&to=2026-11-01',headers={'X-Forwarded-For':'203.0.113.1, 127.0.0.1'},json={'user':'attacker'}):
            from flask import session
            session.update(user='Verified user',uid=3)
            _,args=filters()
            self.assertEqual(args[0].isoformat(),'2026-11-01T00:00:00+05:30')
            self.assertEqual(args[1].isoformat(),'2026-11-02T00:00:00+05:30')
            context=request_context()
            self.assertEqual(context[1],'Verified user');self.assertEqual(context[4],'203.0.113.1')
    def test_primary_tables_across_all_modules_are_covered(self):
        self.assertEqual(len(set(TABLE_MODULES.values())),14)
        for table in ('finance_records','cash_advances','users','user_permissions','notes','weekly_tasks','worksheet_entries','attendance_entries','hs_codes','doc_items','doc_item_permissions','message_messages','document_numbers','individual_achievements','calendar_events','marketing_email_leads'):
            self.assertIn(table,TABLE_MODULES)
        for table in ('google_oauth_tokens','login_device_limits','user_login_sessions'):
            self.assertNotIn(table,TABLE_MODULES)

    def test_every_schema_table_has_auditing_or_documented_exclusion(self):
        tables=set()
        for path in [ROOT/'app.py',*(ROOT/'routes').glob('*.py')]:
            tables.update(re.findall(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z_]+)',path.read_text(),re.I))
        excluded={'attendance_entries_new','google_oauth_tokens','user_login_sessions','login_device_limits','erp_data_migrations',
            'calendar_audit_logs','note_due_date_history','note_edit_history','note_notification_state',
            'weekly_task_history','weekly_task_notification_state','erp_activity_log'}
        self.assertEqual(tables-set(TABLE_MODULES)-excluded,set(), 'Register new business tables in TABLE_MODULES')
