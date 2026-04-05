import os
import re
import psycopg
from psycopg.rows import dict_row


def _get_database_url():
    dsn = (os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL") or "").strip()
    if dsn.startswith("postgres://"):
        dsn = dsn.replace("postgres://", "postgresql://", 1)
    if not dsn:
        raise RuntimeError("DATABASE_URL is required.")
    return dsn


def _replace_qmark_placeholders(sql: str) -> str:
    out = []
    in_single = False
    in_double = False
    i = 0

    while i < len(sql):
        ch = sql[i]

        if ch == "'" and not in_double:
            if i + 1 < len(sql) and sql[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            in_single = not in_single
            out.append(ch)

        elif ch == '"' and not in_single:
            in_double = not in_double
            out.append(ch)

        elif ch == "?" and not in_single and not in_double:
            out.append("%s")

        else:
            out.append(ch)

        i += 1

    return "".join(out)


def _translate_sql(sql: str):
    stripped = sql.strip()

    pragma = re.match(r"^PRAGMA\s+table_info\(([^)]+)\)\s*$", stripped, flags=re.IGNORECASE)
    if pragma:
        table_name = pragma.group(1).strip().strip("'\"")
        return (
            """
            SELECT
                ordinal_position AS cid,
                column_name AS name,
                data_type AS type,
                CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,
                column_default AS dflt_value,
                0 AS pk
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
            "pragma",
        )

    sqlite_master = re.match(
        r"^SELECT\s+name\s+FROM\s+sqlite_master\s+WHERE\s+type='table'\s+AND\s+name=\?\s*$",
        stripped,
        flags=re.IGNORECASE,
    )
    if sqlite_master:
        return (
            """
            SELECT table_name AS name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
            LIMIT 1
            """,
            None,
            "normal",
        )

    if re.match(r"^SELECT\s+last_insert_rowid\(\)\s+AS\s+id\s*$", stripped, flags=re.IGNORECASE):
        return ("SELECT %s AS id", None, "last_insert_id")

    translated = sql
    translated = re.sub(
        r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
        "SERIAL PRIMARY KEY",
        translated,
        flags=re.IGNORECASE,
    )
    translated = re.sub(r"\bAUTOINCREMENT\b", "", translated, flags=re.IGNORECASE)

    translated = translated.replace(
        "replace(deleted_at,'T',' ') <= datetime('now','-30 days')",
        "CAST(REPLACE(deleted_at,'T',' ') AS timestamp) <= NOW() - INTERVAL '30 days'",
    )

    translated = translated.replace(
        "datetime('now','-30 days')",
        "NOW() - INTERVAL '30 days'",
    )

    translated = _replace_qmark_placeholders(translated)

    return (translated, None, "normal")


class PgCompatCursor:
    def __init__(self, wrapped_conn, cursor):
        self._wrapped_conn = wrapped_conn
        self._cursor = cursor
        self._last_special_fetchone = None
        self._last_special_fetchall = None

    def execute(self, sql, params=None):
        translated_sql, forced_params, mode = _translate_sql(sql)
        final_params = forced_params if forced_params is not None else params

        if mode == "last_insert_id":
            self._last_special_fetchone = {"id": self._wrapped_conn.last_insert_id}
            self._last_special_fetchall = [self._last_special_fetchone]
            return self

        self._last_special_fetchone = None
        self._last_special_fetchall = None

        self._cursor.execute(translated_sql, final_params)

        if translated_sql.lstrip().upper().startswith("INSERT"):
            try:
                tmp = self._wrapped_conn._conn.cursor()
                tmp.execute("SELECT LASTVAL() AS id")
                row = tmp.fetchone()
                if row and row.get("id") is not None:
                    self._wrapped_conn.last_insert_id = row["id"]
                tmp.close()
            except Exception:
                pass

        return self

    def fetchone(self):
        if self._last_special_fetchone is not None:
            return self._last_special_fetchone
        return self._cursor.fetchone()

    def fetchall(self):
        if self._last_special_fetchall is not None:
            return self._last_special_fetchall
        return self._cursor.fetchall()

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def close(self):
        self._cursor.close()


class PgCompatConnection:
    def __init__(self, dsn):
        self._conn = psycopg.connect(dsn, row_factory=dict_row)
        self.last_insert_id = None
        self.row_factory = None

    def cursor(self):
        return PgCompatCursor(self, self._conn.cursor())

    def execute(self, sql, params=None):
        cur = self.cursor()
        return cur.execute(sql, params)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


class sqlite3:
    IntegrityError = psycopg.IntegrityError
    Row = dict

    @staticmethod
    def connect(dsn=None):
        return PgCompatConnection(dsn or _get_database_url())