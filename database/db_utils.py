import os
import sqlite3

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None


def is_postgres_url(db_url=None):
    if not db_url:
        db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return False
    return db_url.startswith(("postgres://", "postgresql://"))


def get_db_type(db_url=None):
    return "postgres" if is_postgres_url(db_url) else "sqlite"


class PostgresConnection:
    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return self._conn.cursor(row_factory=dict_row)

    def execute(self, query, params=None):
        params = params if params is not None else ()
        query = translate_placeholders(query)
        cursor = self.cursor()
        cursor.execute(query, params)
        return cursor

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def translate_placeholders(query, db_url=None):
    if is_postgres_url(db_url):
        return query.replace("?", "%s")
    return query


def get_db(db_url=None):
    if db_url is None:
        db_url = os.environ.get("DATABASE_URL")

    if is_postgres_url(db_url):
        if psycopg is None:
            raise ImportError("psycopg3 is required for PostgreSQL connections.")
        conn = psycopg.connect(db_url, row_factory=dict_row)
        return PostgresConnection(conn)

    default_sqlite_path = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "instance", "vyapaariq.db"))
    sqlite_path = db_url or os.environ.get("SQLITE_DB_PATH", default_sqlite_path)
    conn = sqlite3.connect(sqlite_path, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def execute_query(conn, query, params=None, fetchone=False, fetchall=False, commit=False):
    params = params if params is not None else ()
    if get_db_type() == "postgres" and isinstance(conn, PostgresConnection):
        cursor = conn.execute(query, params)
    else:
        cursor = conn.execute(query, params)

    if commit:
        conn.commit()

    if fetchone:
        return cursor.fetchone()
    if fetchall:
        return cursor.fetchall()
    return cursor


def get_table_columns(conn, table_name):
    if isinstance(conn, sqlite3.Connection):
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return [row["name"] for row in rows]

    if isinstance(conn, PostgresConnection):
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            AND table_schema = current_schema()
            ORDER BY ordinal_position
        """
        rows = conn.execute(query, (table_name,)).fetchall()
        return [row["column_name"] for row in rows]

    # fallback: try sqlite-style
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row["name"] for row in rows]


def get_last_insert_id(cursor):
    if hasattr(cursor, "lastrowid") and cursor.lastrowid:
        return cursor.lastrowid
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        return int(next(iter(row.values())))
    return int(row[0])


def sql_month_group_by(column):
    if get_db_type() == "postgres":
        return f"TO_CHAR(DATE({column}), 'YYYY-MM')"
    return f"STRFTIME('%Y-%m', DATE({column}))"


def sql_month_from_date_param_expr():
    if get_db_type() == "postgres":
        return "TO_CHAR(DATE(%s) - INTERVAL '1 month', 'YYYY-MM')"
    return "STRFTIME('%Y-%m', DATE(?, 'start of month', '-1 month'))"


def sql_date_range_filter(column, range_days):
    if range_days is None:
        return ""

    if get_db_type() == "postgres":
        return f"AND DATE({column}) >= DATE(%s) - INTERVAL '{int(range_days)} days'"
    return f"AND DATE({column}) >= DATE(?, '-{int(range_days)} days')"
