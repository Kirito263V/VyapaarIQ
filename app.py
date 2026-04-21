from flask import Flask, render_template, request, jsonify, session, redirect
import logging
import math
import sqlite3
import random
import smtplib
import pandas as pd
from email.mime.text import MIMEText
from datetime import datetime, timedelta


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ================= COLUMN NORMALIZATION ENGINE =================
COLUMN_ALIASES = {

    "customers": {
        "name": ["name", "customer", "customer_name", "full_name"],
        "phone": ["phone", "mobile", "contact_no", "phone_number"],
        "email": ["email", "email_address"],
        "city": ["city", "location"],
        "customer_type": ["type", "customer_type", "segment"]
    },

    "suppliers": {
        "name": ["name", "supplier", "supplier_name", "company"],
        "contact_person": ["contact", "contact_person", "contact_name"],
        "phone": ["phone", "mobile", "contact_no"],
        "email": ["email"],
        "city": ["city", "location"],
        "rating": ["rating", "score"]
    },

    "products": {
        "name": ["name", "product", "product_name", "item"],
        "category_id": ["category", "category_id", "category_name"],
        "supplier_id": ["supplier", "supplier_id", "supplier_name"],
        "sku": ["sku", "barcode", "code", "item_code"],
        "unit": ["unit", "uom", "unit_of_measure"],
        "cost_price": ["cost", "cost_price", "purchase_price", "cp"],
        "selling_price": ["price", "selling_price", "mrp", "sp", "sale_price"],
        "current_stock": ["stock", "quantity", "current_stock", "qty", "opening_stock"],
        "reorder_level": ["reorder_level", "reorder", "min_stock"]
    },

    "sales": {
        "customer_id": ["customer_id", "customer", "customer_name"],
        "sale_date": ["sale_date", "date", "invoice_date"],
        "total_amount": ["total", "total_amount", "amount", "invoice_amount"],
        "payment_method": ["payment", "payment_method", "mode"],
        "notes": ["notes", "remarks", "comment"]
    },

    "sale_items": {
        "sale_id": ["sale_id", "invoice_id"],
        "product_id": ["product", "product_id", "product_name", "item"],
        "quantity": ["quantity", "qty"],
        "price": ["price", "unit_price", "rate"],
        "discount": ["discount", "disc", "discount_pct"],
        "subtotal": ["subtotal", "line_total", "amount"]
    },

    "purchases": {
        "supplier_id": ["supplier", "supplier_id", "supplier_name"],
        "purchase_date": ["purchase_date", "date", "po_date"],
        "total_amount": ["total", "total_amount", "po_amount"],
        "status": ["status"]
    },

    "purchase_items": {
        "purchase_id": ["purchase_id", "po_id"],
        "product_id": ["product", "product_id", "product_name", "item"],
        "quantity": ["quantity", "qty"],
        "unit_cost": ["cost", "unit_cost", "rate"]
    },

    "expenses": {
        "category": ["category", "expense_category", "type"],
        "amount": ["amount", "total", "expense_amount"],
        "expense_date": ["date", "expense_date"],
        "description": ["description", "remarks", "details"]
    },

    "categories": {
        "name": ["name", "category", "category_name"],
        "description": ["description", "details"]
    },

    "stock_alerts": {
        "product_id": ["product", "product_id", "product_name"],
        "alert_type": ["alert_type", "type"],
        "threshold": ["threshold", "min_qty"],
        "is_active": ["is_active", "active", "status"]
    }
}

FK_MAP = {
    "products": [
        ("category_id", "categories", "name"),
        ("supplier_id", "suppliers", "name")
    ],
    "sales": [
        ("customer_id", "customers", "name")
    ],
    "purchases": [
        ("supplier_id", "suppliers", "name")
    ],
    "sale_items": [
        ("product_id", "products", "name")
    ],
    "purchase_items": [
        ("product_id", "products", "name")
    ],
    "stock_alerts": [
        ("product_id", "products", "name")
    ]
}


def _safe_val(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    if isinstance(val, str):
        stripped = val.strip()
        return stripped if stripped else None
    return val


def _is_resolvable(val):
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    if not s:
        return False
    if s.isdigit():
        return False
    return True


def _lookup_fk(cursor, ref_table, ref_col, raw_val):
    clean = str(raw_val).strip()
    try:
        row = cursor.execute(
            f"SELECT id FROM {ref_table} WHERE LOWER(TRIM({ref_col})) = LOWER(TRIM(?))",
            (clean,)
        ).fetchone()
        if row:
            return int(row["id"])
        logger.warning(
            "FK unresolved: no row in %s where %s = %r; column will be NULL.",
            ref_table, ref_col, clean
        )
        return None
    except Exception as exc:
        logger.error(
            "FK lookup error [%s.%s = %r]: %s",
            ref_table, ref_col, clean, exc
        )
        return None


def g(row, key):
    return _safe_val(row.get(key))


def normalize_columns(df, dataset):

    if dataset not in COLUMN_ALIASES:
        return df

    df.columns = [str(c).strip() for c in df.columns]

    mapping = {}
    for standard_col, aliases in COLUMN_ALIASES[dataset].items():
        normalized_aliases = {alias.lower().strip() for alias in aliases}
        for col in df.columns:
            if col.lower() in normalized_aliases:
                mapping[col] = standard_col
                break

    renamed = df.rename(columns=mapping)

    expected = set(COLUMN_ALIASES[dataset].keys())
    found = set(mapping.values())
    also_found = expected & set(renamed.columns)
    missing = expected - found - also_found
    if missing:
        logger.warning(
            "normalize_columns(%s): expected columns not found in Excel -> %s",
            dataset, missing
        )

    logger.info(
        "normalize_columns(%s): mapped %d column(s) -> %s",
        dataset, len(mapping), mapping
    )
    return renamed

def resolve_foreign_keys(row, dataset, conn):
    if hasattr(row, "to_dict"):
        row = {k: _safe_val(v) for k, v in row.to_dict().items()}
    else:
        row = {k: _safe_val(v) for k, v in dict(row).items()}

    if dataset not in FK_MAP:
        return row

    cursor = conn.cursor()

    for fk_col, ref_table, ref_col in FK_MAP[dataset]:
        raw_val = row.get(fk_col)

        if not _is_resolvable(raw_val):
            continue

        resolved_id = _lookup_fk(cursor, ref_table, ref_col, raw_val)
        row[fk_col] = resolved_id

        if resolved_id is not None:
            logger.debug(
                "Resolved %s.%s: %r -> %d",
                dataset, fk_col, raw_val, resolved_id
            )

    return row


app = Flask(__name__)
app.secret_key = "vyapaariq_secret_key"

DB = "vyapaariq.db"

# ================= DATABASE CONNECTION =================

def get_db():
    conn = sqlite3.connect(DB, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


# ================= LOGIN REQUIRED DECORATOR =================

def login_required(func):
    from functools import wraps

    @wraps(func)
    def wrapper(*args, **kwargs):
        if "user_email" not in session:
            return redirect("/login")
        return func(*args, **kwargs)

    return wrapper


# ================= SMTP CONFIG =================

EMAIL = "smurfgaming263@gmail.com"
APP_PASSWORD = "smmrxyyrzktiwgnr"


def send_email_otp(receiver, otp):

    msg = MIMEText(f"""
    VyapaarIQ Verification Code

    Your OTP is:

    {otp}

    Valid for 5 minutes.
    """)

    msg["Subject"] = "VyapaarIQ OTP Verification"
    msg["From"] = EMAIL
    msg["To"] = receiver

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL, APP_PASSWORD)
        smtp.send_message(msg)


# ================= ROUTES =================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/signup")
def signup_page():
    return render_template("signup.html")


@app.route("/login")
def login_page():
    return render_template("login.html")


@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html")


@app.route("/analytics")
@login_required
def analytics():
    return render_template("analytics.html")


@app.route("/import")
@login_required
def import_page():
    return render_template("import.html")


# ================= SEND OTP =================

@app.route("/send-otp", methods=["POST"])
def send_otp():

    data = request.get_json()

    name = data["name"]
    email = data["email"]
    phone = data["phone"]
    password = data["password"]

    otp = str(random.randint(100000, 999999))

    expires = datetime.now() + timedelta(minutes=5)

    conn = get_db()

    conn.execute("""
        INSERT INTO otp_verification
        (name,email,phone,password,otp_code,otp_expires_at)
        VALUES(?,?,?,?,?,?)
    """, (name, email, phone, password, otp, expires))

    conn.commit()

    send_email_otp(email, otp)

    return jsonify({"message": "OTP Sent Successfully"})


# ================= VERIFY OTP =================

@app.route("/verify-otp", methods=["POST"])
def verify_otp():

    data = request.get_json()

    email = data["email"]
    otp = data["otp"]

    conn = get_db()

    record = conn.execute("""
        SELECT * FROM otp_verification
        WHERE email=?
        ORDER BY id DESC
        LIMIT 1
    """, (email,)).fetchone()

    if not record:
        return jsonify({"message": "OTP not found"}), 400

    if record["otp_code"] != otp:
        return jsonify({"message": "Invalid OTP"}), 400

    if datetime.now() > datetime.fromisoformat(record["otp_expires_at"]):
        return jsonify({"message": "OTP expired"}), 400


    # ✅ Check if user already exists before inserting
    existing_user = conn.execute("""
        SELECT id FROM users WHERE email=?
    """, (email,)).fetchone()

    if existing_user:
        return jsonify({"message": "User already verified. Please login."})


    conn.execute("""
        INSERT INTO users(name,email,phone,password)
        VALUES(?,?,?,?)
    """, (
        record["name"],
        record["email"],
        record["phone"],
        record["password"]
    ))

    conn.commit()

    return jsonify({"message": "Signup successful"})

# ================= LOGIN =================

@app.route("/login", methods=["POST"])
def login():

    try:

        data = request.get_json()

        email = data.get("email")
        password = data.get("password")

        conn = get_db()

        user = conn.execute("""
            SELECT * FROM users
            WHERE email=? AND password=?
        """, (email, password)).fetchone()

        if not user:

            return jsonify({
                "error": "Invalid login credentials"
            }), 401


        session["user_email"] = email


        return jsonify({
            "success": True
        }), 200


    except Exception as e:

        print("LOGIN ERROR:", str(e))

        return jsonify({
            "error": "Server error during login"
        }), 500


# ================= LOGOUT =================

@app.route("/logout")
def logout():

    session.clear()

    return redirect("/")


# ================= DROPDOWN APIs =================

@app.route("/api/categories")
def categories():

    conn = get_db()

    rows = conn.execute("SELECT * FROM categories").fetchall()

    return jsonify([dict(r) for r in rows])


@app.route("/api/products")
def products():

    conn = get_db()

    rows = conn.execute("SELECT * FROM products").fetchall()

    return jsonify([dict(r) for r in rows])


@app.route("/api/customers")
def customers():

    conn = get_db()

    rows = conn.execute("SELECT * FROM customers").fetchall()

    return jsonify([dict(r) for r in rows])


@app.route("/api/suppliers")
def suppliers():

    conn = get_db()

    rows = conn.execute("SELECT * FROM suppliers").fetchall()

    return jsonify([dict(r) for r in rows])

@app.route("/api/stock-alerts")
@login_required
def stock_alerts():

    conn = get_db()

    rows = conn.execute("""
        SELECT
            stock_alerts.id,
            products.name AS product_name,
            stock_alerts.product_id,
            stock_alerts.alert_type,
            stock_alerts.threshold,
            stock_alerts.is_active
        FROM stock_alerts
        LEFT JOIN products
        ON stock_alerts.product_id = products.id
        ORDER BY stock_alerts.created_at DESC
    """).fetchall()

    return jsonify([dict(row) for row in rows])

@app.route("/add-category", methods=["POST"])
@login_required
def add_category():

    try:
        data = request.get_json()

        name = data.get("name")
        description = data.get("description")

        if not name:
            return jsonify({"error": "Category name required"}), 400

        conn = get_db()

        conn.execute("""
            INSERT INTO categories(name, description)
            VALUES(?, ?)
        """, (name, description))

        conn.commit()

        return jsonify({"success": True})

    except Exception as e:

        print("ADD CATEGORY ERROR:", e)

        return jsonify({"error": "Failed to add category"}), 500
    
    
    
@app.route("/add-product", methods=["POST"])
@login_required
def add_product():

    try:
        data = request.get_json()

        conn = get_db()

        conn.execute("""
            INSERT INTO products(
                name,
                category_id,
                supplier_id,
                sku,
                unit,
                cost_price,
                selling_price,
                current_stock,
                reorder_level
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (

            data.get("name"),
            data.get("category_id"),
            data.get("supplier_id"),
            data.get("sku"),
            data.get("unit"),
            data.get("cost_price"),
            data.get("selling_price"),
            data.get("current_stock"),
            data.get("reorder_level")

        ))

        conn.commit()

        return jsonify({"success": True})

    except Exception as e:

        print("ADD PRODUCT ERROR:", e)

        return jsonify({"error": "Failed to add product"}), 500
    

@app.route("/add-supplier", methods=["POST"])
@login_required
def add_supplier():

    try:

        data = request.get_json()

        conn = get_db()

        conn.execute("""
            INSERT INTO suppliers(
                name,
                contact_person,
                phone,
                email,
                city,
                rating
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (

            data.get("name"),
            data.get("contact_person"),
            data.get("phone"),
            data.get("email"),
            data.get("city"),
            data.get("rating")

        ))

        conn.commit()

        return jsonify({"success": True})

    except Exception as e:

        print("ADD SUPPLIER ERROR:", e)

        return jsonify({"error": "Failed to add supplier"}), 500
    

@app.route("/add-customer", methods=["POST"])
@login_required
def add_customer():

    try:

        data = request.get_json()

        conn = get_db()

        conn.execute("""
            INSERT INTO customers
            (name, phone, email, city, customer_type)
            VALUES (?, ?, ?, ?, ?)
        """, (

            data.get("name"),
            data.get("phone"),
            data.get("email"),
            data.get("city"),
            data.get("customer_type")

        ))

        conn.commit()

        return jsonify({
            "message": "Customer added successfully"
        })

    except Exception as e:

        print("CUSTOMER ERROR:", e)

        return jsonify({
            "error": "Failed to add customer"
        }), 500


@app.route("/add-expense", methods=["POST"])
@login_required
def add_expense():

    try:

        data = request.get_json()

        conn = get_db()

        conn.execute("""
            INSERT INTO expenses
            (category, amount, expense_date, description)
            VALUES (?, ?, ?, ?)
        """, (

            data.get("category"),
            data.get("amount"),
            data.get("expense_date"),
            data.get("description")

        ))

        conn.commit()

        return jsonify({
            "message": "Expense recorded successfully"
        })

    except Exception as e:

        print("EXPENSE ERROR:", e)

        return jsonify({
            "error": "Failed to record expense"
        }), 500
        
        
@app.route("/add-business-profile", methods=["POST"])
@login_required
def add_business_profiles():

    try:

        data = request.get_json()

        conn = get_db()

        user = conn.execute("""
            SELECT id FROM users
            WHERE email=?
        """, (session.get("user_email"),)).fetchone()

        if not user:
            return jsonify({
                "error": "Logged-in user not found"
            }), 404

        existing_profile = conn.execute("""
            SELECT id FROM business_profiles
            WHERE user_id=?
        """, (user["id"],)).fetchone()

        if existing_profile:
            conn.execute("""
                UPDATE business_profiles
                SET business_name=?, business_type=?, gst_number=?, city=?, address=?
                WHERE user_id=?
            """, (

                data.get("business_name"),
                data.get("business_type"),
                data.get("gst_number"),
                data.get("city"),
                data.get("address"),
                user["id"]

            ))
        else:

            conn.execute("""
                INSERT INTO business_profiles
                (user_id, business_name, business_type, gst_number, city, address)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (

                user["id"],
                data.get("business_name"),
                data.get("business_type"),
                data.get("gst_number"),
                data.get("city"),
                data.get("address")

            ))

        conn.commit()

        return jsonify({
            "message": "Business profile saved"
        })

    except Exception as e:

        print("PROFILE ERROR:", e)

        return jsonify({
            "error": "Failed to save profile"
        }), 500        


    
@app.route("/add-stock-alert", methods=["POST"])
@login_required
def add_stock_alert():

    try:
        data = request.get_json()

        conn = get_db()

        conn.execute("""
            INSERT INTO stock_alerts(
                product_id,
                alert_type,
                threshold,
                is_active
            )
            VALUES (?, ?, ?, ?)
        """, (

            data.get("product_id"),
            data.get("alert_type"),
            data.get("threshold"),
            1

        ))

        conn.commit()

        return jsonify({"success": True})

    except Exception as e:

        print("ADD STOCK ALERT ERROR:", e)

        return jsonify({"error": "Failed to add stock alert"}), 500
    
    

@app.route("/add-sale", methods=["POST"])
@login_required
def add_sale():

    try:

        data = request.get_json()

        conn = get_db()

        total_amount = 0

        # calculate subtotal per item
        for item in data["items"]:

            qty = float(item["quantity"])
            price = float(item["price"])
            discount = float(item.get("discount", 0))

            subtotal = qty * price * (1 - discount/100)

            total_amount += subtotal

        # insert into sales table
        cursor = conn.execute("""
            INSERT INTO sales
            (customer_id, sale_date, total_amount, payment_method, notes)
            VALUES (?, ?, ?, ?, ?)
        """, (

            int(data["customer_id"]),
            data["sale_date"],
            total_amount,
            data["payment_method"],
            data.get("notes", "")

        ))

        sale_id = cursor.lastrowid


        # insert each item
        for item in data["items"]:

            qty = float(item["quantity"])
            price = float(item["price"])
            discount = float(item.get("discount", 0))

            subtotal = qty * price * (1 - discount/100)

            conn.execute("""
                INSERT INTO sale_items
                (sale_id, product_id, quantity, price, discount, subtotal)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (

                sale_id,
                int(item["product_id"]),
                qty,
                price,
                discount,
                subtotal

            ))


        conn.commit()

        return jsonify({
            "message": "Sale recorded successfully"
        })

    except Exception as e:

        print("SALE ERROR:", e)

        return jsonify({
            "error": "Failed to record sale"
        }), 500   
            

@app.route("/add-purchase", methods=["POST"])
@login_required
def add_purchase():

    try:

        data = request.get_json()

        conn = get_db()

        cursor = conn.execute("""
            INSERT INTO purchases
            (supplier_id, purchase_date, status)
            VALUES (?, ?, ?)
        """, (

            data.get("supplier_id"),
            data.get("purchase_date"),
            data.get("status")

        ))

        purchase_id = cursor.lastrowid

        for item in data.get("items", []):

            conn.execute("""
                INSERT INTO purchase_items
                (purchase_id, product_id, quantity, unit_cost)
                VALUES (?, ?, ?, ?)
            """, (

                purchase_id,
                item["product_id"],
                item["quantity"],
                item["unit_cost"]

            ))

        conn.commit()

        return jsonify({
            "message": "Purchase recorded successfully"
        })

    except Exception as e:

        print("PURCHASE ERROR:", e)

        return jsonify({
            "error": "Failed to record purchase"
        }), 500    

###################################
# ANALYTICS ROUTES
###################################

@app.route("/api/dashboard-summary")
def dashboard_summary():

    conn = get_db()

    total_sales = conn.execute("""
        SELECT COALESCE(SUM(total_amount), 0)
        FROM sales
    """).fetchone()[0]

    total_purchases = conn.execute("""
        SELECT COALESCE(SUM(total_amount), 0)
        FROM purchases
    """).fetchone()[0]

    total_expenses = conn.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM expenses
    """).fetchone()[0]

    total_customers = conn.execute("""
        SELECT COUNT(*)
        FROM customers
    """).fetchone()[0]

    total_products = conn.execute("""
        SELECT COUNT(*)
        FROM products
    """).fetchone()[0]

    total_suppliers = conn.execute("""
        SELECT COUNT(*)
        FROM suppliers
    """).fetchone()[0]

    total_orders = conn.execute("""
        SELECT COUNT(*)
        FROM sales
    """).fetchone()[0]

    low_stock = conn.execute("""
        SELECT COUNT(*)
        FROM products
        WHERE current_stock <= COALESCE(reorder_level, 0)
    """).fetchone()[0]

    expense_ratio = (total_expenses / total_sales * 100) if total_sales else 0
    avg_order_value = (total_sales / total_orders) if total_orders else 0

    return jsonify({
        "total_sales": total_sales,
        "total_purchases": total_purchases,
        "total_expenses": total_expenses,
        "total_customers": total_customers,
        "total_products": total_products,
        "total_suppliers": total_suppliers,
        "total_orders": total_orders,
        "avg_order_value": avg_order_value,
        "expense_ratio": round(expense_ratio, 2),
        "low_stock_count": low_stock
    })


@app.route("/api/sales-trend")
def sales_trend():

    conn = get_db()

    rows = conn.execute("""
        SELECT
            strftime('%Y-%m', sale_date) AS sale_month,
            ROUND(COALESCE(SUM(total_amount), 0), 2) AS total
        FROM sales
        WHERE sale_date IS NOT NULL
        GROUP BY sale_month
        ORDER BY sale_month
    """).fetchall()

    labels = []
    values = []

    for row in rows:
        month_value = row["sale_month"]
        if not month_value:
            continue
        year, month = month_value.split("-")
        labels.append(datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d").strftime("%b"))
        values.append(float(row["total"] or 0))

    target = max(values) if values else 0

    return jsonify({
        "labels": labels,
        "values": values,
        "target": target
    })


@app.route("/api/profit-analysis")
def profit_analysis():

    conn = get_db()

    revenue = conn.execute("""
        SELECT COALESCE(SUM(total_amount), 0)
        FROM sales
    """).fetchone()[0]

    cost = conn.execute("""
        SELECT COALESCE(SUM(total_amount), 0)
        FROM purchases
    """).fetchone()[0]

    expenses = conn.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM expenses
    """).fetchone()[0]

    profit = revenue - cost - expenses
    gross_margin = ((revenue - cost) / revenue * 100) if revenue else 0
    net_margin = (profit / revenue * 100) if revenue else 0
    roi = (profit / cost * 100) if cost else 0

    return jsonify({
        "revenue": revenue,
        "cost": cost,
        "expenses": expenses,
        "profit": profit,
        "cogs": cost,
        "gross_margin": round(gross_margin, 2),
        "net_margin": round(net_margin, 2),
        "roi": round(roi, 2)
    })


@app.route("/api/customer-insights")
def customer_insights():

    conn = get_db()

    rows = conn.execute("""
        SELECT
            customers.name,
            ROUND(COALESCE(SUM(sales.total_amount), 0), 2) AS total
        FROM sales
        JOIN customers
        ON customers.id = sales.customer_id
        GROUP BY customers.id, customers.name
        ORDER BY total DESC
        LIMIT 5
    """).fetchall()

    return jsonify([dict(row) for row in rows])


@app.route("/api/inventory-insights")
def inventory_insights():

    conn = get_db()

    rows = conn.execute("""
        SELECT
            products.id,
            products.name,
            categories.name AS category,
            COALESCE(products.current_stock, 0) AS current_stock,
            COALESCE(products.reorder_level, 0) AS reorder_level,
            COALESCE(products.cost_price, 0) AS cost_price
        FROM products
        LEFT JOIN categories
        ON categories.id = products.category_id
        ORDER BY products.name
    """).fetchall()

    return jsonify([dict(row) for row in rows])


@app.route("/api/expense-breakdown")
def expense_breakdown():

    conn = get_db()

    total_expenses = conn.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM expenses
    """).fetchone()[0]

    current_month = conn.execute("""
        SELECT strftime('%Y-%m', MAX(expense_date))
        FROM expenses
        WHERE expense_date IS NOT NULL
    """).fetchone()[0]

    previous_month = None
    if current_month:
        previous_month = conn.execute("""
            SELECT strftime('%Y-%m', date(?, 'start of month', '-1 month'))
        """, (f"{current_month}-01",)).fetchone()[0]

    rows = conn.execute("""
        SELECT
            category,
            ROUND(COALESCE(SUM(amount), 0), 2) AS amount
        FROM expenses
        GROUP BY category
        ORDER BY amount DESC
    """).fetchall()

    result = []
    for row in rows:
        category = row["category"] or "Uncategorized"
        amount = float(row["amount"] or 0)

        current_amount = 0
        previous_amount = 0

        if current_month:
            current_amount = conn.execute("""
                SELECT COALESCE(SUM(amount), 0)
                FROM expenses
                WHERE category = ? AND strftime('%Y-%m', expense_date) = ?
            """, (row["category"], current_month)).fetchone()[0] or 0

        if previous_month:
            previous_amount = conn.execute("""
                SELECT COALESCE(SUM(amount), 0)
                FROM expenses
                WHERE category = ? AND strftime('%Y-%m', expense_date) = ?
            """, (row["category"], previous_month)).fetchone()[0] or 0

        if previous_amount:
            change = ((current_amount - previous_amount) / previous_amount) * 100
        elif current_amount:
            change = 100
        else:
            change = 0

        result.append({
            "category": category,
            "amount": amount,
            "pct": round((amount / total_expenses * 100), 2) if total_expenses else 0,
            "change": round(change, 2)
        })

    return jsonify(result)


@app.route("/api/top-products")
def top_products():

    conn = get_db()

    rows = conn.execute("""
        SELECT
            products.name,
            ROUND(SUM(sale_items.quantity * sale_items.price), 2) AS revenue
        FROM sale_items
        JOIN products
        ON products.id = sale_items.product_id
        GROUP BY products.id, products.name
        ORDER BY revenue DESC
        LIMIT 5
    """).fetchall()

    return jsonify([dict(row) for row in rows])


@app.route("/api/inventory-value")
def inventory_value():

    conn = get_db()

    value = conn.execute("""
        SELECT ROUND(COALESCE(SUM(COALESCE(current_stock, 0) * COALESCE(cost_price, 0)), 0), 2)
        FROM products
    """).fetchone()[0] or 0

    return jsonify({
        "inventory_value": value
    })


# ================= FILE PREVIEW =================

@app.route("/upload-preview", methods=["POST"])
def upload_preview():

    file = request.files.get("file")
    sheet = request.form.get("sheet")

    if not file:
        return jsonify({"columns": [], "rows": [], "total_rows": 0})

    try:

        if file.filename.endswith(("xlsx", "xls")):
            df = pd.read_excel(file, sheet_name=sheet)
        else:
            df = pd.read_csv(file)

        preview = df.head(10)

        return jsonify(dict(
            columns=list(preview.columns),
            rows=preview.values.tolist(),
            total_rows=len(df),
            sheet=sheet
        ))

    except Exception as e:

        print("Preview error:", e)

        return jsonify({"columns": [], "rows": [], "total_rows": 0})


@app.route("/get-excel-sheets", methods=["POST"])
def get_excel_sheets():

    file = request.files.get("file")

    if not file:
        return jsonify({"sheets": []})

    try:

        excel = pd.ExcelFile(file)

        return jsonify({
            "sheets": excel.sheet_names
        })

    except Exception as e:

        print("Sheet detection error:", e)

        return jsonify({"sheets": []})

# ================= FILE IMPORT =================

@app.route("/upload-confirm", methods=["POST"])
def upload_confirm():

    file = request.files["file"]

    dtype = request.form["type"]

    df = pd.read_excel(file) if file.filename.endswith("xlsx") else pd.read_csv(file)

    conn = get_db()

    inserted = 0

    if dtype == "customers":

        for _, row in df.iterrows():

            conn.execute("""
                INSERT INTO customers(name,phone,email,city,customer_type)
                VALUES(?,?,?,?,?)
            """, tuple(row))

            inserted += 1

    conn.commit()

    return jsonify(dict(
        inserted=inserted,
        errors=0,
        type=dtype
    ))


@app.route("/import-sheet", methods=["POST"])
def import_sheet():

    file = request.files.get("file")
    sheet = request.form.get("sheet")
    dataset = request.form.get("dataset")

    if not file or not dataset:

        return jsonify(dict(inserted=0, errors=1))

    try:

        df = pd.read_excel(file, sheet_name=sheet)

        df = normalize_columns(df, dataset)

    

        conn = get_db()

        cursor = conn.cursor()

        inserted = 0
        errors = 0


        for index, raw_row in df.iterrows():
            
            row = resolve_foreign_keys(raw_row, dataset, conn)
            logger.debug("Import row dataset=%s index=%s row=%s", dataset, index, row)
            
            try:

                if dataset == "customers":

                    cursor.execute("""
                        INSERT INTO customers
                        (name, phone, email, city, customer_type)
                        VALUES (?, ?, ?, ?, ?)
                    """, (

                        g(row, "name"),
                        g(row, "phone"),
                        g(row, "email"),
                        g(row, "city"),
                        g(row, "customer_type")

                    ))


                elif dataset == "suppliers":

                    cursor.execute("""
                        INSERT INTO suppliers
                        (name, contact_person, phone, email, city, rating)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (

                        g(row, "name"),
                        g(row, "contact_person"),
                        g(row, "phone"),
                        g(row, "email"),
                        g(row, "city"),
                        g(row, "rating")

                    ))


                elif dataset == "categories":

                    cursor.execute("""
                        INSERT INTO categories
                        (name, description)
                        VALUES (?, ?)
                    """, (

                        g(row, "name"),
                        g(row, "description")

                    ))


                elif dataset == "products":
                    if row.get("category_id") is None and "category_id" in row:
                        logger.warning("Unresolved product category at row %s: %s", index, row)
                    if row.get("supplier_id") is None and "supplier_id" in row:
                        logger.warning("Unresolved product supplier at row %s: %s", index, row)

                    cursor.execute("""
                        INSERT INTO products
                        (
                            name,
                            category_id,
                            supplier_id,
                            sku,
                            unit,
                            cost_price,
                            selling_price,
                            current_stock,
                            reorder_level
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (

                        g(row, "name"),
                        g(row, "category_id"),
                        g(row, "supplier_id"),
                        g(row, "sku"),
                        g(row, "unit"),
                        g(row, "cost_price"),
                        g(row, "selling_price"),
                        g(row, "current_stock"),
                        g(row, "reorder_level")

                    ))


                elif dataset == "purchases":
                    if row.get("supplier_id") is None and "supplier_id" in row:
                        logger.warning("Unresolved purchase supplier at row %s: %s", index, row)

                    cursor.execute("""
                        INSERT INTO purchases
                        (supplier_id, purchase_date, total_amount, status)
                        VALUES (?, ?, ?, ?)
                    """, (

                        g(row, "supplier_id"),
                        g(row, "purchase_date"),
                        g(row, "total_amount"),
                        g(row, "status")

                    ))


                elif dataset == "purchase_items":
                    if row.get("product_id") is None and "product_id" in row:
                        logger.warning("Unresolved purchase item product at row %s: %s", index, row)

                    cursor.execute("""
                        INSERT INTO purchase_items
                        (purchase_id, product_id, quantity, unit_cost)
                        VALUES (?, ?, ?, ?)
                    """, (

                        g(row, "purchase_id"),
                        g(row, "product_id"),
                        g(row, "quantity"),
                        g(row, "unit_cost")

                    ))


                elif dataset == "sales":
                    if row.get("customer_id") is None and "customer_id" in row:
                        logger.warning("Unresolved sale customer at row %s: %s", index, row)

                    cursor.execute("""
                        INSERT INTO sales
                        (customer_id, sale_date, total_amount, payment_method, notes)
                        VALUES (?, ?, ?, ?, ?)
                    """, (

                        g(row, "customer_id"),
                        g(row, "sale_date"),
                        g(row, "total_amount"),
                        g(row, "payment_method"),
                        g(row, "notes")

                    ))


                elif dataset == "sale_items":
                    if row.get("product_id") is None and "product_id" in row:
                        logger.warning("Unresolved sale item product at row %s: %s", index, row)

                    cursor.execute("""
                        INSERT INTO sale_items
                        (sale_id, product_id, quantity, price, discount, subtotal)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (

                        g(row, "sale_id"),
                        g(row, "product_id"),
                        g(row, "quantity"),
                        g(row, "price"),
                        g(row, "discount"),
                        g(row, "subtotal")

                    ))


                elif dataset == "expenses":

                    cursor.execute("""
                        INSERT INTO expenses
                        (category, amount, expense_date, description)
                        VALUES (?, ?, ?, ?)
                    """, (

                        g(row, "category"),
                        g(row, "amount"),
                        g(row, "expense_date"),
                        g(row, "description")

                    ))


                elif dataset == "stock_alerts":

                    cursor.execute("""
                        INSERT INTO stock_alerts
                        (product_id, alert_type, threshold, is_active)
                        VALUES (?, ?, ?, ?)
                    """, (

                        g(row, "product_id"),
                        g(row, "alert_type"),
                        g(row, "threshold"),
                        g(row, "is_active")

                    ))


                inserted += 1


            except Exception as e:

                logger.exception("Row skipped for dataset=%s index=%s", dataset, index)
                errors += 1


        conn.commit()


        return jsonify(dict(

            inserted=inserted,
            errors=errors,
            dataset=dataset

        ))


    except Exception as e:

        print("Import failed:", e)

        return jsonify(dict(

            inserted=0,
            errors=1,
            dataset=dataset

        ))
     

# ================= RUN SERVER =================

if __name__ == "__main__":
    app.run(debug=True)
