import psycopg
from psycopg.rows import dict_row


def connect(dsn: str):
    # Bound connection establishment below Gunicorn's worker timeout. Transaction-
    # local settings work with Supabase transaction pooling without session leaks.
    conn = psycopg.connect(
        dsn,
        row_factory=dict_row,
        prepare_threshold=None,
        connect_timeout=5,
        keepalives=1,
        keepalives_idle=10,
        keepalives_interval=3,
        keepalives_count=3,
        tcp_user_timeout=10000,
    )
    try:
        set_transaction_timeouts(conn)
    except BaseException:
        conn.close()
        raise
    return conn


def set_transaction_timeouts(conn):
    conn.execute("""
        SELECT set_config('statement_timeout', '8000', true),
               set_config('lock_timeout', '3000', true)
    """)


def fetchone(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchone()


def fetchall(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchall()


def execute(conn, query, params=None, returning=False):
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        if returning:
            return cur.fetchone()
        return None


def table_exists(conn, table_name: str) -> bool:
    row = fetchone(
        conn,
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = %s
        LIMIT 1
        """,
        (table_name,),
    )
    return bool(row)


def column_exists(conn, table_name: str, column_name: str) -> bool:
    row = fetchone(
        conn,
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table_name, column_name),
    )
    return bool(row)


def get_table_columns(conn, table_name: str) -> set[str]:
    rows = fetchall(
        conn,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    return {r["column_name"] for r in rows}


def placeholders(count: int) -> str:
    return ", ".join(["%s"] * count)

def register_database_error_handlers(app):
    """Unavailable database requests should return a retryable response, not crash a worker."""
    from flask import current_app, jsonify, render_template, request

    @app.errorhandler(psycopg.OperationalError)
    def database_unavailable(error):
        current_app.logger.exception('Database request unavailable')
        if request.path.startswith('/api/'):
            response = jsonify(ok=False, code='DATABASE_UNAVAILABLE', error='The database is temporarily unavailable. Please retry shortly.')
            response.status_code = 503
        else:
            response = current_app.make_response((render_template('service_unavailable.html'), 503))
        response.headers['Retry-After'] = '10'
        response.headers['Cache-Control'] = 'no-store'
        return response
