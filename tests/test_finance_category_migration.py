import csv
from pathlib import Path
import sqlite3
import unittest
from routes.finance_category_migration import migrate_categories
from routes.finance_insights import category_path, CATEGORIES, build_analysis

class Adapter:
    def __init__(self, conn): self.conn=conn
    def __enter__(self): return self
    def __exit__(self, typ, value, tb):
        self.conn.rollback() if typ else self.conn.commit()
    def execute(self, sql, args=()):
        if 'pg_advisory' in sql: return self.conn.execute('SELECT 1')
        return self.conn.execute(sql.replace('ADD COLUMN IF NOT EXISTS','ADD COLUMN').replace('%s','?').replace(' FOR UPDATE',''), args)

class CategoryMigrationTests(unittest.TestCase):
    def test_optional_child_and_user_corrections(self):
        self.assertEqual(category_path(dict(category='Label Sample Desing Mask City'))[:2],('Production','Labels & design'))
        self.assertEqual(category_path(dict(category='Samples',description='Spice samples sent to buyer'))[:2],('Samples',''))
        self.assertEqual(category_path(dict(category='Capital',subcategory=''))[:2],('Capital',''))
        self.assertIn('Loans', CATEGORIES)
        self.assertNotIn('Loans', CATEGORIES['Capital'])

    def test_migration_preserves_all_other_fields_and_runs_once(self):
        conn=sqlite3.connect(':memory:')
        conn.row_factory=lambda cur,row:dict(zip([c[0] for c in cur.description],row))
        conn.execute('CREATE TABLE finance_records (id INTEGER PRIMARY KEY,type TEXT,category TEXT,description TEXT,amount TEXT,currency TEXT,status TEXT,paid_date TEXT,created_at TEXT)')
        examples=[(1,'OUTCOME','Label Sample Desing Mask City','Do not trim this  \n','123','LKR','PAID','2026-01-01','2026-01-01'),(2,'INCOME','Deposit','Deposit','500','LKR','RECEIVED','2026-01-01','2026-01-01'),(3,'OUTCOME','Bottles Samples Colombo Director Change Doc Print','Exact mixed text','12','LKR','PAID','2026-01-01','2026-01-01')]
        conn.executemany('INSERT INTO finance_records VALUES (?,?,?,?,?,?,?,?,?)',examples);conn.commit()
        before=conn.execute('SELECT * FROM finance_records').fetchall()
        migrate_categories(lambda:Adapter(conn))
        after=conn.execute('SELECT * FROM finance_records').fetchall()
        for old,new in zip(before,after):
            for key in old:
                if key!='category':self.assertEqual(old[key],new[key])
            self.assertEqual(new['original_category'],old['category'])
        self.assertEqual(after[0]['category'],'Production')
        self.assertTrue(after[2]['category_review'])
        self.assertEqual(build_analysis(before,'LKR')['outgoing'],build_analysis(after,'LKR')['outgoing'])
        conn.execute("UPDATE finance_records SET category='Transport' WHERE id=1");conn.commit()
        migrate_categories(lambda:Adapter(conn))
        self.assertEqual(conn.execute('SELECT category FROM finance_records WHERE id=1').fetchone()['category'],'Transport')
        conn.close()
