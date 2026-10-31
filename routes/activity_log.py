"""Admin-only audit queries; PostgreSQL triggers capture committed business changes."""
from datetime import date, datetime, time, timedelta
from functools import wraps
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import secrets
import time as clock

from flask import Blueprint, current_app, g, has_request_context, jsonify, render_template, request, session
from psycopg import Cursor, pq

activity_bp = Blueprint('activity_log', __name__)
RETENTION_DAYS = 90
# Derived notification/history tables and authentication secrets are deliberately excluded.
TABLE_MODULES = {
    'storage_files': 'Storage', 'storage_references': 'Storage',
    'users': 'Employee Access', 'user_permissions': 'Employee Access',
    'bank_accounts': 'Finance', 'finance_records': 'Finance', 'month_balances': 'Finance',
    'cash_advances': 'Petty Cash', 'cash_advance_expenses': 'Petty Cash', 'cash_advance_topups': 'Petty Cash',
    'doc_items': 'Documents', 'doc_item_permissions': 'Documents',
    'document_numbers': 'Invoices',
    'message_conversations': 'Messages', 'message_conversation_members': 'Messages',
    'message_messages': 'Messages', 'message_conversation_deleted': 'Messages',
    'employee_schedules': 'Calendar', 'employee_unavailability': 'Calendar',
    'calendar_events': 'Calendar', 'calendar_event_acks': 'Calendar', 'calendar_holidays': 'Calendar',
    'worksheet_entries': 'Worksheet', 'attendance_entries': 'Attendance',
    'hs_codes': 'HS Codes', 'weekly_tasks': 'Weekly Tasks',
    'weekly_task_edit_requests': 'Weekly Tasks', 'weekly_task_week_locks': 'Weekly Tasks',
    'marketing_email_leads': 'Marketing Emails', 'marketing_email_export_files': 'Marketing Emails',
    'marketing_email_download_permissions': 'Marketing Emails',
    'marketing_email_download_requests': 'Marketing Emails', 'marketing_email_download_logs': 'Marketing Emails',
    'notes': 'Notes', 'note_settings': 'Notes',
    'individual_achievements': 'Achievements', 'individual_achievement_contributors': 'Achievements',
}


def request_context():
    if not has_request_context():
        return '', 'System', '', '', '', ''
    if not getattr(g, 'activity_request_id', None):
        g.activity_request_id = secrets.token_hex(16)
    # Identity comes from the validated signed server session, never JSON or form fields.
    return (g.activity_request_id, str(session.get('user') or 'Anonymous')[:200],
            str(session.get('uid') or '')[:40], request.path[:500],
            (request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or request.remote_addr or '')[:100],
            request.headers.get('User-Agent', '')[:400])


def context_statement():
    values = request_context()
    return ("SELECT set_config('erp.audit_request',%s,true),set_config('erp.audit_actor',%s,true),"
            "set_config('erp.audit_uid',%s,true),set_config('erp.audit_path',%s,true),"
            "set_config('erp.audit_ip',%s,true),set_config('erp.audit_agent',%s,true)", values)


class ActivityCursor(Cursor):
    """Reapply transaction-local identity even when modules commit and reuse cursors."""
    def execute(self, query, params=None, **kwargs):
        sql, values = context_statement()
        if (self.connection.info.transaction_status == pq.TransactionStatus.IDLE or
                getattr(self.connection, '_erp_audit_context', None) != values):
            super().execute(sql, values)
            self.connection._erp_audit_context = values
        return super().execute(query, params, **kwargs)


def install_table_triggers(conn):
    from psycopg import sql
    existing = {row['table_name'] for row in conn.execute("""
        SELECT t.table_name FROM information_schema.tables t
        WHERE t.table_schema='public' AND t.table_type='BASE TABLE'
        AND NOT EXISTS (SELECT 1 FROM pg_trigger tr
          JOIN pg_class c ON c.oid=tr.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='public' AND c.relname=t.table_name AND tr.tgname='erp_activity_capture')
    """).fetchall()}
    for table, module in TABLE_MODULES.items():
        if table in existing:
            conn.execute(sql.SQL('CREATE TRIGGER erp_activity_capture AFTER INSERT OR UPDATE OR DELETE ON public.{} FOR EACH ROW EXECUTE FUNCTION public.erp_capture_activity({})').format(sql.Identifier(table), sql.Literal(module)))


def ensure_module_audit(db):
    if current_app.config.get('ACTIVITY_LOG_READY'):
        with db() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(284202611)")
            install_table_triggers(conn)


def init_activity_log(db):
    with db() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(284202611)")
        conn.execute(Path(__file__).resolve().parents[1].joinpath('migrations/activity_log.sql').read_text())
        install_table_triggers(conn)


def record_auth(action, attempted_user=None):
    if not current_app.config.get('ACTIVITY_LOG_READY'):
        return
    rid, actor, uid, path, ip, agent = request_context()
    with current_app.config['ACTIVITY_DB']() as conn:
        conn.execute("""INSERT INTO public.erp_activity_log
          (actor,actor_id,module,action,entity,record_key,request_id,path,ip_address,browser,changes)
          VALUES (%s,%s,'Authentication',%s,'login','',%s,%s,%s,%s,%s::jsonb)""",
          (actor, uid,
           action, rid, path, ip, agent, json.dumps({'result': action, **({'attempted_username':(attempted_user or '')[:200]} if action == 'Login failed' else {})})))


def configure_activity_log(app, db):
    app.config['ACTIVITY_DB'] = db
    app.config['ACTIVITY_AUTH_FUNC'] = record_auth

    @app.before_request
    def maintain_activity_retention():
        if not app.config.get('ACTIVITY_LOG_READY') or 'user' not in session:
            return
        now = clock.monotonic()
        if now - app.config.get('ACTIVITY_LAST_CLEANUP', -3600) < 3600:
            return
        app.config['ACTIVITY_LAST_CLEANUP'] = now
        try:
            with db() as conn:
                # Bounded batches avoid locking a large table on a user request.
                conn.execute("SELECT public.erp_prune_activity()")
        except Exception:
            app.logger.exception('Activity log retention cleanup deferred')


def admin_only(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        if 'user' not in session:
            return jsonify(ok=False, error='Login required'), 401
        if session.get('role') != 'ADMIN':
            return jsonify(ok=False, error='Administrator access required'), 403
        with current_app.config['ACTIVITY_DB']() as conn:
            admin = conn.execute("SELECT id FROM users WHERE username=%s AND role='ADMIN' AND active=1", (session['user'],)).fetchone()
        if not admin or (session.get('uid') is not None and str(admin['id']) != str(session['uid'])):
            return jsonify(ok=False, error='Administrator access required'), 403
        return function(*args, **kwargs)
    return wrapped


@activity_bp.get('/activity-log')
@admin_only
def activity_page():
    return render_template('activity_log.html', user=session['user'], role='ADMIN')


def filters():
    clauses, params = ['occurred_at >= now() - interval \'90 days\''], []
    for field, arg in [('actor','user'),('module','module'),('action','action')]:
        value = request.args.get(arg, '').strip()
        if value:
            if len(value) > 200:
                raise ValueError('Filter is too long')
            clauses.append(field+'=%s'); params.append(value)
    zone = ZoneInfo('Asia/Colombo')
    start, end = request.args.get('from',''), request.args.get('to','')
    if start and end and date.fromisoformat(start) > date.fromisoformat(end):
        raise ValueError('The start date must be before the end date')
    for value, comparison, offset in [(start,'>=',0),(end,'<',1)]:
        if value:
            value = datetime.combine(date.fromisoformat(value)+timedelta(days=offset), time.min, zone)
            clauses.append('occurred_at '+comparison+' %s'); params.append(value)
    record = request.args.get('record','').strip()
    if record:
        if len(record)>200:raise ValueError('Record filter is too long')
        clauses.append('record_key=%s');params.append(record.lstrip('#'))
    cursor = request.args.get('before','')
    if cursor:
        if not cursor.isdigit() or int(cursor)<1:raise ValueError('Invalid page')
        clauses.append('id<%s');params.append(int(cursor))
    return ' AND '.join(clauses), params


@activity_bp.get('/api/activity-log')
@admin_only
def list_activity():
    try:
        where, params = filters()
    except (ValueError, OverflowError):
        return jsonify(ok=False,error='Choose valid filters and a start date before the end date.'),400
    with current_app.config['ACTIVITY_DB']() as conn:
        rows=conn.execute('SELECT id,occurred_at,actor,module,action,entity,record_key,request_id FROM public.erp_activity_log WHERE '+where+' ORDER BY id DESC LIMIT 51',params).fetchall()
    rows=[dict(row) for row in rows]
    response=jsonify(items=rows[:50],next_cursor=str(rows[49]['id']) if len(rows)>50 else None)
    response.headers['Cache-Control']='no-store'
    return response


@activity_bp.get('/api/activity-log/<int:event_id>')
@admin_only
def activity_detail(event_id):
    with current_app.config['ACTIVITY_DB']() as conn:
        row=conn.execute("SELECT * FROM public.erp_activity_log WHERE id=%s AND occurred_at>=now()-interval '90 days'",(event_id,)).fetchone()
    if not row:return jsonify(ok=False,error='Activity not found or expired'),404
    response=jsonify(dict(row));response.headers['Cache-Control']='no-store';return response


@activity_bp.get('/api/activity-log/options')
@admin_only
def activity_options():
    with current_app.config['ACTIVITY_DB']() as conn:
        choices=conn.execute("SELECT DISTINCT actor,module,action FROM public.erp_activity_log WHERE occurred_at>=now()-interval '90 days'").fetchall()
        size=conn.execute("SELECT pg_total_relation_size('public.erp_activity_log') AS bytes").fetchone()['bytes']
        coverage=conn.execute("""SELECT c.relname AS name FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
            WHERE t.tgname='erp_activity_capture' AND NOT t.tgisinternal""").fetchall()
    response=jsonify(users=sorted({r['actor'] for r in choices}),modules=sorted(set(TABLE_MODULES.values())|{'Authentication'}),
        actions=sorted({r['action'] for r in choices}|{'Created','Edited','Deleted','Restored','Logged in','Logged out','Login failed'}),
        retention_days=90,storage_bytes=size,covered_tables=sorted(r['name'] for r in coverage))
    response.headers['Cache-Control']='no-store';return response
