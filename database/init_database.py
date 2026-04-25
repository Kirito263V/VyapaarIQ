import os
import sqlite3
from pathlib import Path

try:
    import psycopg
except ImportError:
    psycopg = None

DB_PATH = Path(__file__).resolve().parent / "vyapaariq.db"


def _is_postgres_db(db_path):
    if not isinstance(db_path, str):
        return False
    db_path = db_path.strip().lower()
    return db_path.startswith(("postgres://", "postgresql://"))


def _create_database_schema(conn, postgres=False):
    if postgres:
        id_type = "SERIAL PRIMARY KEY"
        expires_type = "TIMESTAMP"
    else:
        id_type = "INTEGER PRIMARY KEY AUTOINCREMENT"
        expires_type = "DATETIME"

    schema_sql = f"""
    CREATE TABLE IF NOT EXISTS users (
        id {id_type},
        name TEXT NOT NULL,
        username TEXT UNIQUE,
        email TEXT UNIQUE NOT NULL,
        phone TEXT,
        password TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS otp_verification (
        id {id_type},
        name TEXT,
        email TEXT,
        phone TEXT,
        password TEXT,
        otp_code TEXT,
        otp_expires_at {expires_type},
        is_verified INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS categories (
        id {id_type},
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        UNIQUE(user_id, name)
    );

    CREATE TABLE IF NOT EXISTS suppliers (
        id {id_type},
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        contact_person TEXT,
        phone TEXT,
        email TEXT,
        city TEXT,
        rating INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        UNIQUE(user_id, name)
    );

    CREATE TABLE IF NOT EXISTS customers (
        id {id_type},
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        phone TEXT,
        email TEXT,
        city TEXT,
        customer_type TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        UNIQUE(user_id, name)
    );

    CREATE TABLE IF NOT EXISTS products (
        id {id_type},
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        category_id INTEGER,
        supplier_id INTEGER,
        sku TEXT,
        unit TEXT DEFAULT 'pcs',
        cost_price REAL DEFAULT 0,
        selling_price REAL DEFAULT 0,
        current_stock INTEGER DEFAULT 0,
        reorder_level INTEGER DEFAULT 10,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL,
        FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE SET NULL,
        UNIQUE(user_id, name)
    );

    CREATE TABLE IF NOT EXISTS sales (
        id {id_type},
        user_id INTEGER NOT NULL,
        customer_id INTEGER,
        sale_date DATE,
        total_amount REAL DEFAULT 0,
        payment_method TEXT,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS sale_items (
        id {id_type},
        user_id INTEGER NOT NULL,
        sale_id INTEGER,
        product_id INTEGER,
        quantity INTEGER DEFAULT 1,
        price REAL DEFAULT 0,
        discount REAL DEFAULT 0,
        subtotal REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (sale_id) REFERENCES sales(id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS purchases (
        id {id_type},
        user_id INTEGER NOT NULL,
        supplier_id INTEGER,
        purchase_date DATE,
        total_amount REAL DEFAULT 0,
        status TEXT DEFAULT 'Delivered',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS purchase_items (
        id {id_type},
        user_id INTEGER NOT NULL,
        purchase_id INTEGER,
        product_id INTEGER,
        quantity INTEGER DEFAULT 1,
        unit_cost REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (purchase_id) REFERENCES purchases(id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS expenses (
        id {id_type},
        user_id INTEGER NOT NULL,
        category TEXT,
        amount REAL DEFAULT 0,
        expense_date DATE,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS stock_alerts (
        id {id_type},
        user_id INTEGER NOT NULL,
        product_id INTEGER,
        alert_type TEXT,
        threshold INTEGER,
        is_active INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS business_profiles (
        id {id_type},
        user_id INTEGER UNIQUE,
        business_name TEXT,
        business_type TEXT,
        gst_number TEXT,
        city TEXT,
        address TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    """

    if postgres:
        with conn.cursor() as cursor:
            for statement in schema_sql.strip().split(";"):
                statement = statement.strip()
                if not statement:
                    continue
                cursor.execute(statement)
    else:
        conn.executescript(schema_sql)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_user_date ON sales(user_id, sale_date);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_expenses_user_date ON expenses(user_id, expense_date);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_user ON products(user_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_categories_user_name ON categories(user_id, name);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_suppliers_user_name ON suppliers(user_id, name);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_customers_user_name ON customers(user_id, name);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_user_name ON products(user_id, name);")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_products_user_sku ON products(user_id, sku) WHERE sku IS NOT NULL AND TRIM(sku) <> '';")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sale_items_user_sale ON sale_items(user_id, sale_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_purchase_items_user_purchase ON purchase_items(user_id, purchase_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_alerts_user_product ON stock_alerts(user_id, product_id);")


def ensure_database_schema(db_path=DB_PATH):
    if _is_postgres_db(db_path):
        if psycopg is None:
            raise RuntimeError("psycopg3 is required for PostgreSQL schema initialization")
        conn = psycopg.connect(db_path)
        try:
            _create_database_schema(conn, postgres=True)
            conn.commit()
        finally:
            conn.close()
        return db_path

    db_path = Path(db_path)
    if not db_path.exists():
        reset_database(db_path)
    return db_path


def reset_database(db_path=DB_PATH):
    db_path = Path(db_path)

    if db_path.exists():
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    try:
        _create_database_schema(conn, postgres=False)
        conn.commit()
    finally:
        conn.close()

    return db_path


if __name__ == '__main__':
    created_db = reset_database()
    print(f"Database recreated at: {created_db}")
