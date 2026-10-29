"""One-time, transactional classification migration; never edits descriptions."""
from .finance_insights import category_path

VERSION = 'finance_categories_v2'

def migrate_categories(db):
    with db() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(284202610)")
        conn.execute("CREATE TABLE IF NOT EXISTS erp_data_migrations (name TEXT PRIMARY KEY)")
        if conn.execute("SELECT name FROM erp_data_migrations WHERE name=%s", (VERSION,)).fetchone():
            return
        conn.execute("ALTER TABLE finance_records ADD COLUMN IF NOT EXISTS subcategory TEXT NOT NULL DEFAULT ''")
        conn.execute("ALTER TABLE finance_records ADD COLUMN IF NOT EXISTS original_category TEXT")
        conn.execute("ALTER TABLE finance_records ADD COLUMN IF NOT EXISTS category_review TEXT NOT NULL DEFAULT ''")
        rows = conn.execute("SELECT id,type,category,description FROM finance_records FOR UPDATE").fetchall()
        for row in rows:
            row = dict(row)
            group, child, source = category_path(row)
            review = ''
            if row['category'] == 'Bottles Samples Colombo Director Change Doc Print':
                group, child = 'Company Administration & Compliance', ''
                review = 'Mixed payment: bottles, samples, travel and director-change printing. Review allocation; no split inferred.'
            elif source == 'unclassified':
                review = 'Choose a category after reviewing the original category and description.'
            conn.execute("UPDATE finance_records SET original_category=category, category=%s, subcategory=%s, category_review=%s WHERE id=%s", (group, child, review, row['id']))
        conn.execute("INSERT INTO erp_data_migrations (name) VALUES (%s)", (VERSION,))
