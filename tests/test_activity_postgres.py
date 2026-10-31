"""Real PostgreSQL tests. Supply ERP_AUDIT_TEST_DSN for a disposable local database."""
import os
import secrets
import unittest
from flask import Flask, session
import psycopg
from routes.activity_log import TABLE_MODULES, init_activity_log, activity_bp
from routes.db import connect

DSN=os.environ.get('ERP_AUDIT_TEST_DSN')

@unittest.skipUnless(DSN,'Requires isolated PostgreSQL test database')
class ActivityPostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Never allow this suite to run against an application database by accident.
        from psycopg.conninfo import conninfo_to_dict
        info=conninfo_to_dict(DSN)
        assert info.get('host')=='127.0.0.1' and info.get('dbname')=='erp_audit_test'
        with connect(DSN) as conn:
            for name in TABLE_MODULES:
                conn.execute(psycopg.sql.SQL('CREATE TABLE IF NOT EXISTS {} (id BIGSERIAL PRIMARY KEY, username TEXT,role TEXT,active INTEGER,description TEXT, amount NUMERIC, password_hash TEXT, payload JSONB,deleted_at TEXT,is_deleted INTEGER DEFAULT 0)').format(psycopg.sql.Identifier(name)))
        init_activity_log(lambda:connect(DSN))
        cls.app=Flask(__name__);cls.app.config.update(SECRET_KEY='test',TESTING=True,ACTIVITY_DB=lambda:connect(DSN))
        cls.app.register_blueprint(activity_bp)

    def test_all_business_tables_and_atomic_rollback(self):
        with self.app.test_request_context('/api/test'):
            session.update(user='Alice',uid=1,role='ADMIN')
            with connect(DSN) as conn:
                for table in TABLE_MODULES:
                    conn.execute(psycopg.sql.SQL("INSERT INTO {} (description,amount) VALUES ('first',12)").format(psycopg.sql.Identifier(table)))
                events=conn.execute("SELECT * FROM erp_activity_log WHERE actor='Alice'").fetchall()
                self.assertTrue(set(TABLE_MODULES).issubset({r['entity'] for r in events}))
                self.assertTrue(all(r['actor']=='Alice' for r in events))
            conn=connect(DSN)
            conn.execute("INSERT INTO notes(description) VALUES ('must roll back')")
            conn.rollback();conn.close()
            with connect(DSN) as conn:
                self.assertFalse(conn.execute("SELECT 1 FROM erp_activity_log WHERE changes::text LIKE '%%must roll back%%'").fetchone())

    def test_diff_snapshot_soft_delete_and_secret_redaction(self):
        with self.app.test_request_context('/api/users/1'):
            session.update(user='Audit tester',uid=1)
            with connect(DSN) as conn:
                item=conn.execute("INSERT INTO notes(description,amount,password_hash,payload) VALUES (%s,12,%s,%s::jsonb) RETURNING id",('Exact text  \n','NEVER_STORE_ME','{"nested":{"token":"ALSO_SECRET","safe":"yes"}}')).fetchone()['id']
                conn.execute("UPDATE notes SET amount=15,password_hash='CHANGED_SECRET' WHERE id=%s",(item,))
                conn.execute("UPDATE notes SET deleted_at='2026-11-01' WHERE id=%s",(item,))
                conn.execute("DELETE FROM notes WHERE id=%s",(item,))
                entries=conn.execute("SELECT action,changes FROM erp_activity_log WHERE entity='notes' AND record_key=%s ORDER BY id",(str(item),)).fetchall()
                self.assertEqual([r['action'] for r in entries],['Created','Edited','Deleted','Deleted'])
                self.assertEqual(set(entries[1]['changes']),{'amount','password_hash'})
                self.assertEqual(entries[1]['changes']['amount'],{'before':12,'after':15})
                self.assertEqual(entries[0]['changes']['after']['description'],'Exact text  \n')
                self.assertIn('_record_snapshot',entries[2]['changes'])
                self.assertNotIn('NEVER_STORE_ME',str(entries));self.assertNotIn('ALSO_SECRET',str(entries));self.assertNotIn('CHANGED_SECRET',str(entries))

    def test_audit_failure_aborts_business_write(self):
        with self.assertRaises(psycopg.errors.CheckViolation):
            with connect(DSN) as conn:
                conn.execute("ALTER TABLE erp_activity_log ADD CONSTRAINT test_reject_notes CHECK (entity<>'notes') NOT VALID")
                conn.execute("INSERT INTO notes(description) VALUES ('audit failure must roll back')")
        with connect(DSN) as conn:
            self.assertFalse(conn.execute("SELECT 1 FROM notes WHERE description='audit failure must roll back'").fetchone())

    def test_identity_reapplied_after_commit_on_reused_cursor(self):
        conn=connect(DSN);cur=conn.cursor()
        with self.app.test_request_context('/api/one'):
            session.update(user='First actor')
            cur.execute("INSERT INTO notes(description) VALUES ('one')");conn.commit()
        with self.app.test_request_context('/api/two'):
            session.update(user='Second actor')
            cur.execute("INSERT INTO notes(description) VALUES ('two')");conn.commit()
        conn.close()
        with connect(DSN) as conn:
            rows=conn.execute("SELECT actor,changes->'after'->>'description' AS description FROM erp_activity_log WHERE actor IN ('First actor','Second actor')").fetchall()
            self.assertIn(dict(actor='First actor',description='one'),rows)
            self.assertIn(dict(actor='Second actor',description='two'),rows)

    def test_immutability_and_retention_boundary(self):
        with connect(DSN) as conn:
            old=conn.execute("INSERT INTO erp_activity_log(occurred_at,actor,module,action,entity) VALUES(now()-interval '91 days','System','Test','Created','test') RETURNING id").fetchone()['id']
            recent=conn.execute("INSERT INTO erp_activity_log(actor,module,action,entity) VALUES('System','Test','Created','test') RETURNING id").fetchone()['id']
        for query in ("UPDATE erp_activity_log SET actor='changed' WHERE id=%s",'DELETE FROM erp_activity_log WHERE id=%s'):
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                with connect(DSN) as conn:conn.execute(query,(recent,))
        with self.assertRaises(psycopg.errors.InsufficientPrivilege):
            with connect(DSN) as conn:conn.execute('TRUNCATE erp_activity_log')
        with connect(DSN) as conn:
            conn.execute('SELECT erp_prune_activity()')
            self.assertFalse(conn.execute('SELECT 1 FROM erp_activity_log WHERE id=%s',(old,)).fetchone())
            self.assertTrue(conn.execute('SELECT 1 FROM erp_activity_log WHERE id=%s',(recent,)).fetchone())

    def test_supabase_explicit_default_grants_are_revoked(self):
        with connect(DSN) as conn:
            conn.execute("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='anon') THEN CREATE ROLE anon NOLOGIN; END IF; IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='authenticated') THEN CREATE ROLE authenticated NOLOGIN; END IF; END $$")
            conn.execute('GRANT ALL ON erp_activity_log TO anon,authenticated')
            conn.execute('GRANT ALL ON SEQUENCE erp_activity_log_id_seq TO anon,authenticated')
            conn.execute('GRANT EXECUTE ON FUNCTION erp_prune_activity() TO anon,authenticated')
        init_activity_log(lambda:connect(DSN))
        with connect(DSN) as conn:
            for role in ('anon','authenticated'):
                self.assertFalse(conn.execute("SELECT has_table_privilege(%s,'erp_activity_log','SELECT') AS allowed",(role,)).fetchone()['allowed'])
                self.assertFalse(conn.execute("SELECT has_function_privilege(%s,'erp_prune_activity()','EXECUTE') AS allowed",(role,)).fetchone()['allowed'])

    def test_real_api_and_demoted_admin_cannot_read(self):
        username='API admin '+secrets.token_hex(5)
        with connect(DSN) as conn:
            admin=conn.execute("INSERT INTO users(username,role,active) VALUES(%s,'ADMIN',1) RETURNING id",(username,)).fetchone()['id']
        client=self.app.test_client()
        with client.session_transaction() as s:s.update(user=username,uid=admin,role='ADMIN')
        response=client.get('/api/activity-log?module=Notes')
        self.assertEqual(response.status_code,200)
        self.assertTrue(all(r['module']=='Notes' for r in response.json['items']))
        options=client.get('/api/activity-log/options')
        self.assertEqual(options.status_code,200);self.assertEqual(len(options.json['covered_tables']),len(TABLE_MODULES))
        with connect(DSN) as conn:conn.execute("UPDATE users SET role='EMP' WHERE id=%s",(admin,))
        self.assertEqual(client.get('/api/activity-log').status_code,403)
