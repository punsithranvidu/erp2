"""Exercise update SQL and permissions against an isolated in-memory SQL database.

The adapter translates psycopg placeholders and omits PostgreSQL's row lock;
production PostgreSQL locking itself is not tested here.
"""
import sqlite3
import unittest
from unittest.mock import patch
from flask import Flask
from routes.cash_advances import cash_advances_bp, advance_edit_version, _format_advance_row


class TestConnection:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, query, params=()):
        return self.connection.execute(query.replace('%s', '?').replace(' FOR UPDATE', ''), params)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        if exc_type:
            self.connection.rollback()
        else:
            self.connection.commit()


class CashRecordEditTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = lambda cursor, values: dict(zip([c[0] for c in cursor.description], values))
        self.conn.executescript('''
          CREATE TABLE users (username TEXT PRIMARY KEY, active INTEGER);
          CREATE TABLE bank_accounts (id INTEGER PRIMARY KEY, active INTEGER);
          CREATE TABLE cash_advances (id INTEGER PRIMARY KEY, employee_username TEXT,
            bank_id INTEGER, currency TEXT, amount_given REAL, purpose TEXT,
            given_date TEXT, proof_link TEXT, closed INTEGER);
          CREATE TABLE cash_advance_expenses (id INTEGER PRIMARY KEY, advance_id INTEGER, paid_by TEXT, amount REAL);
          CREATE TABLE cash_advance_topups (id INTEGER PRIMARY KEY, advance_id INTEGER, amount REAL);
          INSERT INTO users VALUES ('Punsith',1),('Other',1),('Inactive',0);
          INSERT INTO bank_accounts VALUES (1,1),(2,1),(3,0);
          INSERT INTO cash_advances VALUES (13,'Punsith',1,'LKR',1000,'Original','2026-10-28','',0);
          INSERT INTO cash_advance_expenses VALUES (1,13,'COMPANY_ADVANCE',700);
          INSERT INTO cash_advance_topups VALUES (1,13,100);
        ''')
        self.app = Flask(__name__)
        self.app.config.update(SECRET_KEY='test-only', TESTING=True,
                               HAS_MODULE_ACCESS_FUNC=lambda module, edit=False: True)
        self.app.register_blueprint(cash_advances_bp)
        self.client = self.app.test_client()
        self.login('ADMIN')
        self.mock_db = patch('routes.cash_advances.db', return_value=TestConnection(self.conn))
        self.mock_db.start()

    def tearDown(self):
        self.mock_db.stop()
        self.conn.close()

    def login(self, role):
        with self.client.session_transaction() as session:
            session.update(user='Punsith', role=role)

    def row(self):
        return self.conn.execute('SELECT * FROM cash_advances WHERE id=13').fetchone()

    def update(self, **changes):
        payload = dict(employee_username='Punsith', bank_id=1, currency='LKR',
                       amount_given='1200', purpose='Updated transport',
                       given_date='2026-10-27', proof_link='example.com/proof')
        payload.update(changes)
        return self.client.put('/api/advances/13', json=payload)

    def test_edits_existing_record_and_preserves_expenses_and_topups(self):
        self.assertEqual(self.update().status_code, 200)
        row = self.row()
        self.assertEqual(row['purpose'], 'Updated transport')
        self.assertEqual(row['amount_given'], 1200)
        self.assertEqual(row['proof_link'], 'https://example.com/proof')
        row.update(topups_total=100, spent_company=700, spent_personal=0, spent_total=700)
        self.assertEqual(_format_advance_row(row)['balance'], 600)
        self.assertEqual(self.conn.execute('SELECT amount FROM cash_advance_expenses').fetchone()['amount'], 700)
        self.assertEqual(self.conn.execute('SELECT amount FROM cash_advance_topups').fetchone()['amount'], 100)

    def test_closed_record_must_be_reopened(self):
        self.conn.execute('UPDATE cash_advances SET closed=1')
        self.conn.commit()
        self.assertEqual(self.update().status_code, 409)
        self.assertEqual(self.row()['purpose'], 'Original')

    def test_no_currency_change_when_transactions_exist(self):
        self.assertEqual(self.update(currency='USD').status_code, 409)
        self.assertEqual(self.row()['currency'], 'LKR')

    def test_currency_can_change_before_any_transactions(self):
        self.conn.execute('DELETE FROM cash_advance_expenses')
        self.conn.execute('DELETE FROM cash_advance_topups')
        self.conn.commit()
        self.assertEqual(self.update(currency='USD').status_code, 200)

    def test_prevents_reducing_allocation_below_company_spending(self):
        self.assertEqual(self.update(amount_given='599.99').status_code, 400)
        self.assertEqual(self.update(amount_given='600').status_code, 200)

    def test_rejects_nonfinite_and_invalid_amounts(self):
        for value in ('NaN', 'Infinity', '-Infinity', '0', '-1', 'abc', None, '1e999', '1e-999'):
            with self.subTest(amount=value):
                self.assertEqual(self.update(amount_given=value).status_code, 400)

    def test_rejects_invalid_date_employee_and_bank(self):
        for change in ({'given_date':'2026-02-30'}, {'employee_username':'Missing'},
                       {'employee_username':'Inactive'}, {'bank_id':'bad'},
                       {'bank_id':999}, {'bank_id':3}):
            with self.subTest(change=change):
                self.assertEqual(self.update(**change).status_code, 400)

    def test_stale_edit_does_not_overwrite_another_edit(self):
        version = advance_edit_version(self.row())
        self.assertEqual(self.update(edit_version=version).status_code, 200)
        self.assertEqual(self.update(edit_version=version, purpose='Stale form').status_code, 409)
        self.assertEqual(self.row()['purpose'], 'Updated transport')

    def test_employee_and_readonly_and_logged_out_requests_cannot_edit(self):
        self.login('EMP')
        self.assertEqual(self.update().status_code, 403)
        self.login('ADMIN')
        self.app.config['HAS_MODULE_ACCESS_FUNC'] = lambda module, edit=False: False
        self.assertEqual(self.update().status_code, 403)
        with self.client.session_transaction() as session:
            session.clear()
        self.assertEqual(self.update().status_code, 401)

    def test_missing_record(self):
        self.conn.execute('DELETE FROM cash_advances')
        self.conn.commit()
        self.assertEqual(self.update().status_code, 404)
