import psycopg
from psycopg.rows import dict_row


def connect(dsn: str):
    return psycopg.connect(dsn, row_factory=dict_row)


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
