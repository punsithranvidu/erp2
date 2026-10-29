from datetime import date
from decimal import Decimal
import sqlite3
import unittest
from unittest.mock import patch
from flask import Flask
from routes.finance_insights import build_analysis, category_path
from routes.finance import finance_bp


def row(**changes):
    result=dict(type='OUTCOME',category='Bank Charges',description='',currency='LKR',amount='100.25',status='PAID',paid_date='2026-01-05',created_at='2026-01-06',deleted_at=None)
    result.update(changes)
    return result


class FinanceAnalysisTests(unittest.TestCase):
    def test_all_records_includes_pending_without_mixing_currencies(self):
        data=[row(type='INCOME',amount='200',status='PENDING'),row(),row(currency='USD',amount='900'),row(deleted_at='2026-02-01')]
        report=build_analysis(data,'LKR')
        self.assertEqual(report['count'],2)
        self.assertEqual(report['income'],200)
        self.assertEqual(report['outgoing'],100.25)
        self.assertEqual(report['net_movement'],99.75)
        self.assertEqual(report['unsettled_count'],1)
        self.assertEqual(sum(group['amount'] for group in report['categories']),report['outgoing'])

    def test_settled_filter_uses_direction_and_status(self):
        report=build_analysis([row(),row(type='INCOME',status='RECEIVED'),row(status='RECEIVED'),row(status='PENDING')],'LKR',basis='settled')
        self.assertEqual(report['count'],2)
        self.assertEqual(report['unsettled_count'],0)

    def test_dates_use_paid_date_then_creation_and_fill_month_gaps(self):
        report=build_analysis([row(),row(paid_date='',created_at='2026-03-10T12:00:00')],'LKR')
        self.assertEqual([r['period'] for r in report['monthly']],['2026-01','2026-02','2026-03'])
        self.assertEqual(report['monthly'][1]['outgoing'],0)
        self.assertEqual(report['created_date_count'],1)
        self.assertEqual(build_analysis([row()],'LKR',date(2026,2,1))['count'],0)

    def test_invalid_values_are_reported_and_decimal_sums_are_exact(self):
        report=build_analysis([row(amount='0.1'),row(amount='0.2'),row(amount='NaN'),row(paid_date='invalid'),row(amount='-5')],'LKR')
        self.assertEqual(report['outgoing'],0.3)
        self.assertEqual(report['omitted_count'],3)

    def test_category_classification_is_non_destructive_and_conservative(self):
        original=row(category='Bottles Samples Colombo Director Change Doc Print')
        self.assertEqual(category_path(original)[0],'Needs classification')
        self.assertEqual(original['category'],'Bottles Samples Colombo Director Change Doc Print')
        self.assertEqual(category_path(row(category='Jayabima / misrat 2 - Consignment Cetificat'))[0],'Export')
        self.assertEqual(category_path(row(category='Transport / Local deliveries')),('Transport','Local deliveries','structured'))
        self.assertEqual(category_path(row(category='remittence'))[:2],('Bank Charges','International transfer fees'))
        self.assertEqual(category_path(row(type='INCOME',category='Deposit'))[0],'Capital')

    def test_subcategory_totals_reconcile_and_custom_choices_are_supported(self):
        report=build_analysis([row(category='Custom / Detail'),row(category='Transport / Local deliveries')],'LKR')
        self.assertEqual(sum(child['amount'] for group in report['categories'] for child in group['children']),report['outgoing'])


class DB:
    def __init__(self,conn):self.conn=conn
    def execute(self,sql,params=()):return self.conn.execute(sql.replace('%s','?'),params)
    def close(self):pass
    def __enter__(self):return self
    def __exit__(self,*args):self.conn.commit()


class FinanceAnalysisAPITests(unittest.TestCase):
    def setUp(self):
        self.conn=sqlite3.connect(':memory:')
        self.conn.row_factory=lambda cursor,row:dict(zip([c[0] for c in cursor.description],row))
        self.conn.executescript('''CREATE TABLE finance_records (bank_id INTEGER,type TEXT,category TEXT,description TEXT,currency TEXT,amount REAL,status TEXT,paid_date TEXT,created_at TEXT,deleted_at TEXT);
        INSERT INTO finance_records VALUES (1,'INCOME','Sales / Export sales','','LKR',200,'PENDING','2026-01-01','2026-01-01',NULL),(1,'OUTCOME','Transport / Local deliveries','','LKR',50,'PAID','2026-01-01','2026-01-01',NULL),(2,'OUTCOME','Bank Charges','','LKR',25,'PAID','2026-01-01','2026-01-01',NULL);''')
        self.conn.executescript("ALTER TABLE finance_records ADD COLUMN subcategory TEXT DEFAULT ''; ALTER TABLE finance_records ADD COLUMN original_category TEXT; ALTER TABLE finance_records ADD COLUMN category_review TEXT DEFAULT '';")
        self.app=Flask(__name__)
        self.app.config.update(SECRET_KEY='test',TESTING=True,HAS_MODULE_ACCESS_FUNC=lambda module,edit=False:True)
        self.app.register_blueprint(finance_bp)
        self.client=self.app.test_client()
        with self.client.session_transaction() as session:session.update(user='Example',role='ADMIN')
        self.db=patch('routes.finance.db',return_value=DB(self.conn));self.db.start()

    def tearDown(self):self.db.stop();self.conn.close()

    def test_bank_filter_and_default_pending_policy(self):
        data=self.client.get('/api/finance/analysis?bank_id=1&currency=LKR').json['data']
        self.assertEqual(data['income'],200)
        self.assertEqual(data['outgoing'],50)
        self.assertEqual(data['basis'],'all')

    def test_invalid_filters_and_permissions(self):
        for query in ('bank_id=bad','start=2026-04-01&end=2026-01-01','basis=invalid'):
            self.assertEqual(self.client.get('/api/finance/analysis?'+query).status_code,400)
        self.app.config['HAS_MODULE_ACCESS_FUNC']=lambda module,edit=False:False
        self.assertEqual(self.client.get('/api/finance/analysis').status_code,403)
        self.assertEqual(self.client.get('/api/finance/categories').status_code,403)

    def test_saved_custom_categories_are_returned_without_rewriting_rows(self):
        data=self.client.get('/api/finance/categories').json
        self.assertIn({'type':'OUTCOME','category':'Transport / Local deliveries','subcategory':''},data['existing'])
        self.assertIn('Transport',data['expense'])

    def test_all_direction_summary_keeps_incoming_and_outgoing_separate(self):
        with patch('routes.finance.purge_deleted_older_than_30_days'):
            response=self.client.get('/api/finance/summary?bank_id=ALL&type=ALL')
        self.assertEqual(response.status_code,200)
        self.assertEqual({r['direction'] for r in response.json},{'INCOME','OUTCOME'})
