import logging
import sqlite3
from typing import Dict, List

from werkzeug.security import generate_password_hash


PASSWORD_HASH_PREFIXES = ("pbkdf2:", "scrypt:")

USER_SCOPED_TABLES = [
    "categories",
    "products",
    "customers",
    "suppliers",
    "sales",
    "sale_items",
    "purchases",
    "purchase_items",
    "expenses",
    "inventory",
]

USER_LOOKUP_INDEXES = {
    "categories": ["name"],
    "products": ["name", "sku"],
    "customers": ["name", "email", "phone"],
    "suppliers": ["name", "email", "phone"],
    "sales": ["sale_date"],
    "purchases": ["purchase_date"],
    "expenses": ["expense_date"],
    "sale_items": ["sale_id", "product_id"],
    "purchase_items": ["purchase_id", "product_id"],
}


def is_password_hash(value):
    if not value or not isinstance(value, str):
        return False
    return value.startswith(PASSWORD_HASH_PREFIXES)


def hash_password(password):
    return generate_password_hash(password, method="pbkdf2:sha256")


def _table_exists(conn, table_name):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_names(conn, table_name):
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]


def _create_index_name(table_name, columns):
    return f"idx_{table_name}_{'_'.join(columns)}"


def _ensure_user_id_schema(conn, logger):
    migrated_tables = []
    skipped_tables = []

    for table_name in USER_SCOPED_TABLES:
        if not _table_exists(conn, table_name):
            skipped_tables.append(table_name)
            continue

        columns = _column_names(conn, table_name)
        if "user_id" not in columns:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN user_id INTEGER")
            logger.info("Added user_id column to %s", table_name)

        conn.execute(
            f"UPDATE {table_name} SET user_id = 1 WHERE user_id IS NULL OR user_id = 0"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_user_id ON {table_name}(user_id)"
        )

        for column_name in USER_LOOKUP_INDEXES.get(table_name, []):
            if column_name in _column_names(conn, table_name):
                index_name = _create_index_name(table_name, ["user_id", column_name])
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}(user_id, {column_name})"
                )

        migrated_tables.append(table_name)

    return {
        "migrated_tables": migrated_tables,
        "skipped_tables": skipped_tables,
    }


def _migrate_passwords_in_table(conn, table_name, logger):
    if not _table_exists(conn, table_name):
        return 0

    columns = _column_names(conn, table_name)
    if "password" not in columns:
        return 0

    updated_rows = 0
    rows = conn.execute(f"SELECT id, password FROM {table_name}").fetchall()
    for row_id, password_value in rows:
        if not password_value or is_password_hash(password_value):
            continue
        conn.execute(
            f"UPDATE {table_name} SET password = ? WHERE id = ?",
            (hash_password(password_value), row_id),
        )
        updated_rows += 1

    if updated_rows:
        logger.info("Migrated %s plaintext password(s) in %s", updated_rows, table_name)

    return updated_rows


def run_production_migration(db_path, logger=None):
    active_logger = logger or logging.getLogger(__name__)
    conn = sqlite3.connect(db_path, timeout=10, check_same_thread=False)

    try:
        schema_result = _ensure_user_id_schema(conn, active_logger)
        migrated_user_passwords = _migrate_passwords_in_table(conn, "users", active_logger)
        migrated_otp_passwords = _migrate_passwords_in_table(conn, "otp_verification", active_logger)
        conn.commit()

        result = {
            "schema": schema_result,
            "migrated_user_passwords": migrated_user_passwords,
            "migrated_otp_passwords": migrated_otp_passwords,
        }
        active_logger.info("Production migration completed: %s", result)
        return result
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run_production_migration("vyapaariq.db"))
