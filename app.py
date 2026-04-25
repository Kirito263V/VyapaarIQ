from flask import Flask, render_template, request, jsonify, session, redirect, send_file
import os
import csv
import io
import logging
import random
import smtplib
import zipfile
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from werkzeug.security import check_password_hash

from database.db_utils import (
    execute_query,
    get_db,
    get_last_insert_id,
    get_table_columns,
    sql_month_from_date_param_expr,
    sql_month_group_by,
)
from database.migration_production import hash_password, is_password_hash, run_production_migration
from database.init_database import reset_database
from routes.import_routes import import_bp
from services.analytics_service import apply_date_filter


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


app = Flask(__name__, template_folder="templates", static_folder="static")

app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")

app.permanent_session_lifetime = timedelta(hours=2)

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    DB_TYPE = "postgres"
    DB = DATABASE_URL
else:
    DB_TYPE = "sqlite"
    DB = os.path.join(BASE_DIR, "instance", "vyapaariq.db")

app.config["DATABASE"] = DB

try:
    logger.info("Initializing database schema...")

    if DB_TYPE == "sqlite":
        reset_database(DB)

    logger.info("Running production migration...")

    if DB_TYPE == "sqlite":
        run_production_migration(DB, logger)

except Exception as e:
    logger.error(f"Database setup failed: {e}")

app.register_blueprint(import_bp)
# ================= DATABASE CONNECTION =================


def _auth_required_response():
    return jsonify({"error": "authentication required"}), 401


def _current_user_id():
    return session.get("user_id")


def _record_belongs_to_user(conn, table, record_id, user_id):
    if record_id in (None, "", []):
        return True
    row = conn.execute(
        f"SELECT id FROM {table} WHERE id = ? AND user_id = ?",
        (record_id, user_id),
    ).fetchone()
    return row is not None


def _get_user_by_id(conn, user_id):
    return conn.execute(
        "SELECT id, name, email, password FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()


def _password_matches(stored_password, provided_password):
    if not stored_password:
        return False
    if not is_password_hash(stored_password):
        return stored_password == provided_password
    try:
        return check_password_hash(stored_password, provided_password)
    except ValueError:
        return False


def _qparams(user_id, filter_params):
    return (user_id, *filter_params) if filter_params else (user_id,)


# ================= LOGIN REQUIRED DECORATOR =================

def login_required(func):
    from functools import wraps

    @wraps(func)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            if request.path.startswith("/api/") or request.method != "GET":
                return _auth_required_response()
            return redirect("/login")
        return func(*args, **kwargs)

    return wrapper


def _get_analytics_start_date():
    range_days = request.args.get("range_days", default=None, type=int)
    if range_days is None:
        range_value = request.args.get("range", None)
        if range_value == "all":
            return None
        try:
            range_days = int(range_value) if range_value is not None else None
        except (TypeError, ValueError):
            range_days = None

    if range_days is None or range_days == 0:
        return None

    if range_days < 0:
        return None

    return (datetime.now().date() - timedelta(days=range_days)).isoformat()


# ================= SMTP CONFIG =================

import os

EMAIL = os.environ.get("SMTP_EMAIL", "")
APP_PASSWORD = os.environ.get("SMTP_APP_PASSWORD", "")

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
    return render_template(
        "import.html",
        validation_summary={},
        import_result={},
        detected_columns=[],
        db_columns=[]
    )


@app.route("/settings")
@login_required
def settings():

    conn = get_db()
    user = _get_user_by_id(conn, _current_user_id())
    conn.close()

    if not user:
        session.clear()
        return redirect("/login")

    return render_template("settings.html", user=user)


# ================= SEND OTP =================

@app.route("/send-otp", methods=["POST"])
def send_otp():

    data = request.get_json()

    name = data["name"]
    email = data["email"]
    phone = data["phone"]
    password = data["password"]

    hashed_password = hash_password(password)

    otp = str(random.randint(100000, 999999))
    expires = datetime.now() + timedelta(minutes=5)

    conn = get_db()

    conn.execute("""
        INSERT INTO otp_verification
        (name, email, phone, password, otp_code, otp_expires_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (name, email, phone, hashed_password, otp, expires))

    conn.commit()
    conn.close()

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
        conn.close()
        return jsonify({"message": "OTP not found"}), 400

    if record["otp_code"] != otp:
        conn.close()
        return jsonify({"message": "Invalid OTP"}), 400

    if datetime.now() > datetime.fromisoformat(record["otp_expires_at"]):
        conn.close()
        return jsonify({"message": "OTP expired"}), 400


    # ✅ Check if user already exists before inserting
    existing_user = conn.execute("""
        SELECT id FROM users WHERE email=?
    """, (email,)).fetchone()

    if existing_user:
        conn.close()
        return jsonify({"message": "User already verified. Please login."})

    password_to_store = record["password"]
    if not is_password_hash(password_to_store):
        password_to_store = hash_password(password_to_store)

    conn.execute("""
        INSERT INTO users(name,email,phone,password)
        VALUES(?,?,?,?)
    """, (
        record["name"],
        record["email"],
        record["phone"],
        password_to_store
    ))

    conn.commit()
    conn.close()

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
            WHERE email=?
        """, (email,)).fetchone()

        if not user or not _password_matches(user["password"], password):
            conn.close()

            return jsonify({
                "error": "Invalid login credentials"
            }), 401

        session.clear()
        session.permanent = True
        session["user_id"] = user["id"]
        session["user_email"] = user["email"]
        conn.close()


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

    return redirect("/login")


@app.route("/change-password", methods=["POST"])
@login_required
def change_password():

    current_password = request.form.get("current_password", "")
    new_password = request.form.get("new_password", "")
    confirm_password = request.form.get("confirm_password", "")

    if not current_password or not new_password or not confirm_password:
        return jsonify({"success": False, "error": "All password fields are required"}), 400

    if new_password != confirm_password:
        return jsonify({"success": False, "error": "Passwords do not match"}), 400

    if len(new_password) < 8:
        return jsonify({"success": False, "error": "New password must be at least 8 characters"}), 400

    conn = get_db()
    user = _get_user_by_id(conn, _current_user_id())

    if not user:
        conn.close()
        session.clear()
        return redirect("/login")

    if not _password_matches(user["password"], current_password):
        conn.close()
        return jsonify({"success": False, "error": "Incorrect password"}), 400

    conn.execute(
        "UPDATE users SET password = ? WHERE id = ?",
        (hash_password(new_password), user["id"]),
    )
    conn.commit()
    conn.close()

    return jsonify({"success": True, "message": "Password updated successfully"})


@app.route("/delete-my-data", methods=["POST"])
@login_required
def delete_my_data():

    user_id = _current_user_id()
    conn = get_db()

    delete_order = [
        "stock_alerts",
        "sale_items",
        "purchase_items",
        "sales",
        "purchases",
        "products",
        "customers",
        "suppliers",
        "categories",
        "expenses",
    ]

    for table in delete_order:
        conn.execute(f"DELETE FROM {table} WHERE user_id = ?", (user_id,))

    conn.commit()
    conn.close()

    return jsonify({"success": True, "message": "Data deleted successfully"})


@app.route("/export-my-data")
@login_required
def export_my_data():

    user_id = _current_user_id()
    conn = get_db()
    export_tables = ["customers", "products", "sales", "expenses", "suppliers"]

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for table in export_tables:
            rows = conn.execute(
                f"SELECT * FROM {table} WHERE user_id = ? ORDER BY id",
                (user_id,),
            ).fetchall()
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)

            if rows:
                headers = rows[0].keys()
                writer.writerow(headers)
                for row in rows:
                    writer.writerow([row[key] for key in headers])
            else:
                columns = get_table_columns(conn, table)
                writer.writerow(columns)

            archive.writestr(f"{table}.csv", csv_buffer.getvalue())

    conn.close()

    zip_buffer.seek(0)

    return send_file(
        zip_buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"vyapaariq_user_{user_id}_export.zip",
    )


# ================= DROPDOWN APIs =================

@app.route("/api/categories")
@login_required
def categories():

    conn = get_db()
    user_id = _current_user_id()

    rows = conn.execute(
        "SELECT * FROM categories WHERE user_id = ? ORDER BY name",
        (user_id,),
    ).fetchall()

    return jsonify([dict(r) for r in rows])


@app.route("/api/products")
@login_required
def products():

    conn = get_db()
    user_id = _current_user_id()

    rows = conn.execute(
        "SELECT * FROM products WHERE user_id = ? ORDER BY name",
        (user_id,),
    ).fetchall()

    return jsonify([dict(r) for r in rows])


@app.route("/api/customers")
@login_required
def customers():

    conn = get_db()
    user_id = _current_user_id()

    rows = conn.execute(
        "SELECT * FROM customers WHERE user_id = ? ORDER BY name",
        (user_id,),
    ).fetchall()

    return jsonify([dict(r) for r in rows])


@app.route("/api/suppliers")
@login_required
def suppliers():

    conn = get_db()
    user_id = _current_user_id()

    rows = conn.execute(
        "SELECT * FROM suppliers WHERE user_id = ? ORDER BY name",
        (user_id,),
    ).fetchall()

    return jsonify([dict(r) for r in rows])

@app.route("/api/stock-alerts")
@login_required
def stock_alerts():

    conn = get_db()
    user_id = _current_user_id()

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
        AND products.user_id = stock_alerts.user_id
        WHERE stock_alerts.user_id = ?
        ORDER BY stock_alerts.created_at DESC
    """, (user_id,)).fetchall()

    return jsonify([dict(row) for row in rows])

@app.route("/add-category", methods=["POST"])
@login_required
def add_category():

    try:
        data = request.get_json()
        user_id = _current_user_id()

        name = data.get("name")
        description = data.get("description")

        if not name:
            return jsonify({"error": "Category name required"}), 400

        conn = get_db()

        conn.execute("""
            INSERT INTO categories(name, description, user_id)
            VALUES(?, ?, ?)
        """, (name, description, user_id))

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
        user_id = _current_user_id()

        conn = get_db()
        category_id = data.get("category_id")
        supplier_id = data.get("supplier_id")

        if category_id and not _record_belongs_to_user(conn, "categories", category_id, user_id):
            return jsonify({"error": "Invalid category for this account"}), 400

        if supplier_id and not _record_belongs_to_user(conn, "suppliers", supplier_id, user_id):
            return jsonify({"error": "Invalid supplier for this account"}), 400

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
                reorder_level,
                user_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (

            data.get("name"),
            category_id,
            supplier_id,
            data.get("sku"),
            data.get("unit"),
            data.get("cost_price"),
            data.get("selling_price"),
            data.get("current_stock"),
            data.get("reorder_level"),
            user_id

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
        user_id = _current_user_id()

        conn = get_db()

        conn.execute("""
            INSERT INTO suppliers(
                name,
                contact_person,
                phone,
                email,
                city,
                rating,
                user_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (

            data.get("name"),
            data.get("contact_person"),
            data.get("phone"),
            data.get("email"),
            data.get("city"),
            data.get("rating"),
            user_id

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
        user_id = _current_user_id()

        conn = get_db()

        conn.execute("""
            INSERT INTO customers
            (name, phone, email, city, customer_type, user_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (

            data.get("name"),
            data.get("phone"),
            data.get("email"),
            data.get("city"),
            data.get("customer_type"),
            user_id

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
        user_id = _current_user_id()

        conn = get_db()

        conn.execute("""
            INSERT INTO expenses
            (category, amount, expense_date, description, user_id)
            VALUES (?, ?, ?, ?, ?)
        """, (

            data.get("category"),
            data.get("amount"),
            data.get("expense_date"),
            data.get("description"),
            user_id

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
        user_id = _current_user_id()

        conn = get_db()

        existing_profile = conn.execute("""
            SELECT id FROM business_profiles
            WHERE user_id=?
        """, (user_id,)).fetchone()

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
                user_id

            ))
        else:

            conn.execute("""
                INSERT INTO business_profiles
                (user_id, business_name, business_type, gst_number, city, address)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (

                user_id,
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
        user_id = _current_user_id()

        conn = get_db()
        product_id = data.get("product_id")

        if product_id and not _record_belongs_to_user(conn, "products", product_id, user_id):
            return jsonify({"error": "Invalid product for this account"}), 400

        conn.execute("""
            INSERT INTO stock_alerts(
                product_id,
                alert_type,
                threshold,
                is_active,
                user_id
            )
            VALUES (?, ?, ?, ?, ?)
        """, (

            product_id,
            data.get("alert_type"),
            data.get("threshold"),
            1,
            user_id

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
        user_id = _current_user_id()

        conn = get_db()
        customer_id = int(data["customer_id"])

        if not _record_belongs_to_user(conn, "customers", customer_id, user_id):
            return jsonify({"error": "Invalid customer for this account"}), 400

        for item in data["items"]:
            product_id = int(item["product_id"])
            if not _record_belongs_to_user(conn, "products", product_id, user_id):
                return jsonify({"error": "Invalid product for this account"}), 400

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
            (customer_id, sale_date, total_amount, payment_method, notes, user_id)
            VALUES (?, ?, ?, ?, ?, ?)
            RETURNING id
        """, (

            customer_id,
            data["sale_date"],
            total_amount,
            data["payment_method"],
            data.get("notes", ""),
            user_id

        ))

        sale_id = get_last_insert_id(cursor)


        # insert each item
        for item in data["items"]:
            product_id = int(item["product_id"])

            qty = float(item["quantity"])
            price = float(item["price"])
            discount = float(item.get("discount", 0))

            subtotal = qty * price * (1 - discount/100)

            conn.execute("""
                INSERT INTO sale_items
                (sale_id, product_id, quantity, price, discount, subtotal, user_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (

                sale_id,
                product_id,
                qty,
                price,
                discount,
                subtotal,
                user_id

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
        user_id = _current_user_id()

        conn = get_db()
        supplier_id = data.get("supplier_id")

        if supplier_id and not _record_belongs_to_user(conn, "suppliers", supplier_id, user_id):
            return jsonify({"error": "Invalid supplier for this account"}), 400

        for item in data.get("items", []):
            product_id = item["product_id"]
            if not _record_belongs_to_user(conn, "products", product_id, user_id):
                return jsonify({"error": "Invalid product for this account"}), 400

        cursor = conn.execute("""
            INSERT INTO purchases
            (supplier_id, purchase_date, status, user_id)
            VALUES (?, ?, ?, ?)
            RETURNING id
        """, (

            supplier_id,
            data.get("purchase_date"),
            data.get("status"),
            user_id

        ))

        purchase_id = get_last_insert_id(cursor)

        for item in data.get("items", []):
            product_id = item["product_id"]

            conn.execute("""
                INSERT INTO purchase_items
                (purchase_id, product_id, quantity, unit_cost, user_id)
                VALUES (?, ?, ?, ?, ?)
            """, (

                purchase_id,
                product_id,
                item["quantity"],
                item["unit_cost"],
                user_id

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
@login_required
def dashboard_summary():

    conn = get_db()
    user_id = _current_user_id()

    total_sales = conn.execute("""
        SELECT COALESCE(SUM(total_amount), 0)
        FROM sales
        WHERE user_id = ?
    """, (user_id,)).fetchone()[0]

    total_purchases = conn.execute("""
        SELECT COALESCE(SUM(total_amount), 0)
        FROM purchases
        WHERE user_id = ?
    """, (user_id,)).fetchone()[0]

    total_expenses = conn.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM expenses
        WHERE user_id = ?
    """, (user_id,)).fetchone()[0]

    total_customers = conn.execute("""
        SELECT COUNT(*)
        FROM customers
        WHERE user_id = ?
    """, (user_id,)).fetchone()[0]

    total_products = conn.execute("""
        SELECT COUNT(*)
        FROM products
        WHERE user_id = ?
    """, (user_id,)).fetchone()[0]

    total_suppliers = conn.execute("""
        SELECT COUNT(*)
        FROM suppliers
        WHERE user_id = ?
    """, (user_id,)).fetchone()[0]

    total_orders = conn.execute("""
        SELECT COUNT(*)
        FROM sales
        WHERE user_id = ?
    """, (user_id,)).fetchone()[0]

    total_profit = conn.execute("""
        SELECT ROUND(COALESCE(SUM(
            COALESCE(si.subtotal, 0) - (COALESCE(si.quantity, 0) * COALESCE(p.cost_price, 0))
        ), 0), 2)
        FROM sale_items si
        JOIN sales s
        ON s.id = si.sale_id
        AND s.user_id = si.user_id
        JOIN products p
        ON p.id = si.product_id
        AND p.user_id = si.user_id
        WHERE si.user_id = ?
    """, (user_id,)).fetchone()[0]

    top_product_row = conn.execute("""
        SELECT
            p.name,
            COALESCE(SUM(si.quantity), 0) AS total_qty,
            ROUND(COALESCE(SUM(si.subtotal), 0), 2) AS revenue
        FROM sale_items si
        JOIN sales s
        ON s.id = si.sale_id
        AND s.user_id = si.user_id
        JOIN products p
        ON p.id = si.product_id
        AND p.user_id = si.user_id
        WHERE si.user_id = ?
        GROUP BY p.id, p.name
        ORDER BY total_qty DESC, revenue DESC, p.name ASC
        LIMIT 1
    """, (user_id,)).fetchone()

    trend_rows = conn.execute(f"""
        SELECT
            {sql_month_group_by('sale_date')} AS month,
            ROUND(COALESCE(SUM(total_amount), 0), 2) AS revenue
        FROM sales
        WHERE user_id = ?
        AND sale_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """, (user_id,)).fetchall()

    low_stock = conn.execute("""
        SELECT COUNT(*)
        FROM products
        WHERE user_id = ?
        AND current_stock <= reorder_level
    """, (user_id,)).fetchone()[0]

    expense_ratio = (total_expenses / total_sales * 100) if total_sales else 0
    avg_order_value = (total_sales / total_orders) if total_orders else 0
    profit_margin = (total_profit / total_sales * 100) if total_sales else 0
    revenue_chart = [float(row["revenue"] or 0) for row in trend_rows[-6:]]
    revenue_labels = [row["month"] for row in trend_rows[-6:] if row["month"]]

    return jsonify({
        "total_sales": total_sales,
        "total_purchases": total_purchases,
        "total_expenses": total_expenses,
        "total_customers": total_customers,
        "total_products": total_products,
        "total_suppliers": total_suppliers,
        "total_orders": total_orders,
        "total_profit": total_profit,
        "profit_margin": round(profit_margin, 2),
        "avg_order_value": avg_order_value,
        "expense_ratio": round(expense_ratio, 2),
        "low_stock_count": low_stock,
        "top_product": top_product_row["name"] if top_product_row else "N/A",
        "top_product_qty": int(top_product_row["total_qty"]) if top_product_row else 0,
        "top_product_revenue": float(top_product_row["revenue"] or 0) if top_product_row else 0,
        "revenue_chart": revenue_chart,
        "revenue_chart_labels": revenue_labels,
        "range_days": 0,
    })


@app.route("/api/analytics-summary")
@login_required
def analytics_summary():

    conn = get_db()
    user_id = _current_user_id()
    range_days = request.args.get("range_days", default=None, type=int)
    if range_days is not None and range_days < 0:
        range_days = None

    sales_filter, sales_params = apply_date_filter("sale_date", range_days, user_id)
    purchase_filter, purchase_params = apply_date_filter("purchase_date", range_days, user_id)
    expense_filter, expense_params = apply_date_filter("expense_date", range_days, user_id)
    customer_filter, customer_params = apply_date_filter("sale_date", range_days, user_id)
    order_filter, order_params = apply_date_filter("sale_date", range_days, user_id)
    profit_filter, profit_params = apply_date_filter("s.sale_date", range_days, user_id)
    top_product_filter, top_product_params = apply_date_filter("s.sale_date", range_days, user_id)
    trend_filter, trend_params = apply_date_filter("sale_date", range_days, user_id)

    total_sales = conn.execute("""
        SELECT COALESCE(SUM(total_amount), 0)
        FROM sales
        WHERE user_id = ?
    """ + sales_filter, _qparams(user_id, sales_params)).fetchone()[0]

    total_purchases = conn.execute("""
        SELECT COALESCE(SUM(total_amount), 0)
        FROM purchases
        WHERE user_id = ?
    """ + purchase_filter, _qparams(user_id, purchase_params)).fetchone()[0]

    total_expenses = conn.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM expenses
        WHERE user_id = ?
    """ + expense_filter, _qparams(user_id, expense_params)).fetchone()[0]

    if range_days:
        total_customers = conn.execute("""
            SELECT COUNT(DISTINCT customer_id)
            FROM sales
            WHERE user_id = ?
            AND customer_id IS NOT NULL
        """ + customer_filter, _qparams(user_id, customer_params)).fetchone()[0]
    else:
        total_customers = conn.execute("""
            SELECT COUNT(*)
            FROM customers
            WHERE user_id = ?
        """, (user_id,)).fetchone()[0]

    total_products = conn.execute("""
        SELECT COUNT(*)
        FROM products
        WHERE user_id = ?
    """, (user_id,)).fetchone()[0]

    total_suppliers = conn.execute("""
        SELECT COUNT(*)
        FROM suppliers
        WHERE user_id = ?
    """, (user_id,)).fetchone()[0]

    total_orders = conn.execute("""
        SELECT COUNT(*)
        FROM sales
        WHERE user_id = ?
    """ + order_filter, _qparams(user_id, order_params)).fetchone()[0]

    total_profit = conn.execute("""
        SELECT ROUND(COALESCE(SUM(
            COALESCE(si.subtotal, 0) - (COALESCE(si.quantity, 0) * COALESCE(p.cost_price, 0))
        ), 0), 2)
        FROM sale_items si
        JOIN sales s
        ON s.id = si.sale_id
        AND s.user_id = si.user_id
        JOIN products p
        ON p.id = si.product_id
        AND p.user_id = si.user_id
        WHERE si.user_id = ?
    """ + profit_filter, _qparams(user_id, profit_params)).fetchone()[0]

    top_product_row = conn.execute("""
        SELECT
            p.name,
            COALESCE(SUM(si.quantity), 0) AS total_qty,
            ROUND(COALESCE(SUM(si.subtotal), 0), 2) AS revenue
        FROM sale_items si
        JOIN sales s
        ON s.id = si.sale_id
        AND s.user_id = si.user_id
        JOIN products p
        ON p.id = si.product_id
        AND p.user_id = si.user_id
        WHERE si.user_id = ?
    """ + top_product_filter + """
        GROUP BY p.id, p.name
        ORDER BY total_qty DESC, revenue DESC, p.name ASC
        LIMIT 1
    """, _qparams(user_id, top_product_params)).fetchone()

    trend_rows = conn.execute(f"""
        SELECT
            {sql_month_group_by('sale_date')} AS month,
            ROUND(COALESCE(SUM(total_amount), 0), 2) AS revenue
        FROM sales
        WHERE user_id = ?
        AND sale_date IS NOT NULL
    """ + trend_filter + """
        GROUP BY month
        ORDER BY month
    """, _qparams(user_id, trend_params)).fetchall()

    low_stock = conn.execute("""
        SELECT COUNT(*)
        FROM products
        WHERE user_id = ?
        AND current_stock <= reorder_level
    """, (user_id,)).fetchone()[0]

    expense_ratio = (total_expenses / total_sales * 100) if total_sales else 0
    avg_order_value = (total_sales / total_orders) if total_orders else 0
    profit_margin = (total_profit / total_sales * 100) if total_sales else 0
    revenue_chart = [float(row["revenue"] or 0) for row in trend_rows]
    revenue_labels = [row["month"] for row in trend_rows if row["month"]]

    return jsonify({
        "total_sales": total_sales,
        "total_purchases": total_purchases,
        "total_expenses": total_expenses,
        "total_customers": total_customers,
        "total_products": total_products,
        "total_suppliers": total_suppliers,
        "total_orders": total_orders,
        "total_profit": total_profit,
        "profit_margin": round(profit_margin, 2),
        "avg_order_value": avg_order_value,
        "expense_ratio": round(expense_ratio, 2),
        "low_stock_count": low_stock,
        "top_product": top_product_row["name"] if top_product_row else "N/A",
        "top_product_qty": int(top_product_row["total_qty"]) if top_product_row else 0,
        "top_product_revenue": float(top_product_row["revenue"] or 0) if top_product_row else 0,
        "revenue_chart": revenue_chart,
        "revenue_chart_labels": revenue_labels,
        "range_days": range_days or 0,
    })



@app.route("/api/sales-trend")
@app.route("/api/monthly-sales-trend")
@login_required
def sales_trend():

    conn = get_db()
    user_id = _current_user_id()
    range_days = request.args.get("range_days", default=None, type=int)
    if range_days is not None and range_days < 0:
        range_days = None

    sales_filter, sales_params = apply_date_filter("sale_date", range_days, user_id)

    rows = conn.execute(f"""
        SELECT
            {sql_month_group_by('sale_date')} AS month,
            ROUND(COALESCE(SUM(total_amount), 0), 2) AS revenue
        FROM sales
        WHERE user_id = ?
        AND sale_date IS NOT NULL
    """ + sales_filter + """
        GROUP BY month
        ORDER BY month
    """, _qparams(user_id, sales_params)).fetchall()

    revenue_chart_labels = []
    revenue_chart = []
    labels = []
    values = []
    rows_payload = []

    for row in rows:
        month_value = row["month"]
        if not month_value:
            continue
        revenue = float(row["revenue"] or 0)
        revenue_chart_labels.append(month_value)
        revenue_chart.append(revenue)
        year, month = month_value.split("-")
        labels.append(datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d").strftime("%b"))
        values.append(revenue)
        rows_payload.append({
            "month": month_value,
            "revenue": revenue
        })

    print("Monthly revenue labels:", revenue_chart_labels)
    print("Monthly revenue values:", revenue_chart)

    return jsonify({
        "labels": labels,
        "values": values,
        "revenue_chart_labels": revenue_chart_labels,
        "revenue_chart": revenue_chart,
        "rows": rows_payload
    })


@app.route("/api/profit-analysis")
@login_required
def profit_analysis():

    conn = get_db()
    user_id = _current_user_id()
    range_days = request.args.get("range_days", default=None, type=int)
    if range_days is not None and range_days < 0:
        range_days = None

    revenue_filter, revenue_params = apply_date_filter("sale_date", range_days, user_id)
    purchase_filter, purchase_params = apply_date_filter("purchase_date", range_days, user_id)
    expense_filter, expense_params = apply_date_filter("expense_date", range_days, user_id)

    revenue = conn.execute("""
        SELECT COALESCE(SUM(total_amount), 0)
        FROM sales
        WHERE user_id = ?
    """ + revenue_filter, _qparams(user_id, revenue_params)).fetchone()[0]

    cost = conn.execute("""
        SELECT COALESCE(SUM(total_amount), 0)
        FROM purchases
        WHERE user_id = ?
    """ + purchase_filter, _qparams(user_id, purchase_params)).fetchone()[0]

    expenses = conn.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM expenses
        WHERE user_id = ?
    """ + expense_filter, _qparams(user_id, expense_params)).fetchone()[0]

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


@app.route("/api/revenue-cost-trend")
@login_required
def revenue_cost_trend():

    conn = get_db()
    user_id = _current_user_id()
    range_days = request.args.get("range_days", default=None, type=int)
    if range_days is not None and range_days < 0:
        range_days = None

    sales_filter, sales_params = apply_date_filter("sale_date", range_days, user_id)
    purchase_filter, purchase_params = apply_date_filter("purchase_date", range_days, user_id)
    expense_filter, expense_params = apply_date_filter("expense_date", range_days, user_id)

    sales_rows = conn.execute(f"""
        SELECT
            {sql_month_group_by('sale_date')} AS month,
            ROUND(COALESCE(SUM(total_amount), 0), 2) AS revenue
        FROM sales
        WHERE user_id = ?
        AND sale_date IS NOT NULL
    """ + sales_filter + """
        GROUP BY month
        ORDER BY month
    """, _qparams(user_id, sales_params)).fetchall()

    if not sales_rows:
        return jsonify({
            "labels": [],
            "revenue": [],
            "cost": []
        })

    purchase_rows = conn.execute(f"""
        SELECT
            {sql_month_group_by('purchase_date')} AS month,
            ROUND(COALESCE(SUM(total_amount), 0), 2) AS amount
        FROM purchases
        WHERE user_id = ?
        AND purchase_date IS NOT NULL
    """ + purchase_filter + """
        GROUP BY month
        ORDER BY month
    """, _qparams(user_id, purchase_params)).fetchall()

    expense_rows = conn.execute(f"""
        SELECT
            {sql_month_group_by('expense_date')} AS month,
            ROUND(COALESCE(SUM(amount), 0), 2) AS amount
        FROM expenses
        WHERE user_id = ?
        AND expense_date IS NOT NULL
    """ + expense_filter + """
        GROUP BY month
        ORDER BY month
    """, _qparams(user_id, expense_params)).fetchall()

    revenue_by_month = {
        row["month"]: float(row["revenue"] or 0)
        for row in sales_rows
        if row["month"]
    }
    purchase_by_month = {
        row["month"]: float(row["amount"] or 0)
        for row in purchase_rows
        if row["month"]
    }
    expense_by_month = {
        row["month"]: float(row["amount"] or 0)
        for row in expense_rows
        if row["month"]
    }

    months = sorted(set(revenue_by_month) | set(purchase_by_month) | set(expense_by_month))

    return jsonify({
        "labels": [
            datetime.strptime(f"{month}-01", "%Y-%m-%d").strftime("%b")
            for month in months
        ],
        "revenue": [revenue_by_month.get(month, 0) for month in months],
        "cost": [
            round(purchase_by_month.get(month, 0) + expense_by_month.get(month, 0), 2)
            for month in months
        ]
    })


@app.route("/api/customer-insights")
@login_required
def customer_insights():

    conn = get_db()
    user_id = _current_user_id()
    range_days = request.args.get("range_days", default=None, type=int)
    if range_days is not None and range_days < 0:
        range_days = None

    sales_filter, sales_params = apply_date_filter("sales.sale_date", range_days, user_id)

    rows = conn.execute("""
        SELECT
            customers.name,
            ROUND(COALESCE(SUM(sales.total_amount), 0), 2) AS total
        FROM sales
        JOIN customers
        ON customers.id = sales.customer_id
        AND customers.user_id = sales.user_id
        WHERE sales.user_id = ?
    """ + sales_filter + """
        GROUP BY customers.id, customers.name
        ORDER BY total DESC
        LIMIT 5
    """, _qparams(user_id, sales_params)).fetchall()

    return jsonify([dict(row) for row in rows])


@app.route("/api/inventory-insights")
@login_required
def inventory_insights():

    conn = get_db()
    user_id = _current_user_id()

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
        AND categories.user_id = products.user_id
        WHERE products.user_id = ?
        ORDER BY products.name
    """, (user_id,)).fetchall()

    return jsonify([dict(row) for row in rows])


@app.route("/api/expense-breakdown")
@login_required
def expense_breakdown():

    conn = get_db()
    user_id = _current_user_id()
    range_days = request.args.get("range_days", default=None, type=int)
    if range_days is not None and range_days < 0:
        range_days = None

    expense_filter, expense_params = apply_date_filter("expense_date", range_days, user_id)

    total_expenses = conn.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM expenses
        WHERE user_id = ?
    """ + expense_filter, _qparams(user_id, expense_params)).fetchone()[0]

    current_month = conn.execute(f"""
        SELECT {sql_month_group_by('expense_date')}
        FROM expenses
        WHERE user_id = ?
        AND expense_date IS NOT NULL
    """ + expense_filter, (user_id,)).fetchone()[0]

    previous_month = None
    if current_month:
        previous_month = conn.execute(f"""
            SELECT {sql_month_from_date_param_expr()}
        """, (f"{current_month}-01",)).fetchone()[0]

    rows = conn.execute("""
        SELECT
            category,
            ROUND(COALESCE(SUM(amount), 0), 2) AS amount
        FROM expenses
        WHERE user_id = ?
    """ + expense_filter + """
        GROUP BY category
        ORDER BY amount DESC
    """, (user_id,)).fetchall()

    result = []
    for row in rows:
        category = row["category"] or "Uncategorized"
        amount = float(row["amount"] or 0)

        current_amount = 0
        previous_amount = 0

        if current_month:
            current_amount = conn.execute(f"""
                SELECT COALESCE(SUM(amount), 0)
                FROM expenses
                WHERE user_id = ? AND category IS ? AND {sql_month_group_by('expense_date')} = ?
            """ + expense_filter, (user_id, row["category"], current_month)).fetchone()[0] or 0

        if previous_month:
            previous_amount = conn.execute(f"""
                SELECT COALESCE(SUM(amount), 0)
                FROM expenses
                WHERE user_id = ? AND category IS ? AND {sql_month_group_by('expense_date')} = ?
            """ + expense_filter, (user_id, row["category"], previous_month)).fetchone()[0] or 0

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
@login_required
def top_products():

    conn = get_db()
    user_id = _current_user_id()
    range_days = request.args.get("range_days", default=None, type=int)
    if range_days is not None and range_days < 0:
        range_days = None

    sales_filter, sales_params = apply_date_filter("sales.sale_date", range_days, user_id)

    rows = conn.execute("""
        SELECT
            products.name,
            ROUND(SUM(sale_items.quantity * sale_items.price), 2) AS revenue
        FROM sale_items
        JOIN sales
        ON sales.id = sale_items.sale_id
        AND sales.user_id = sale_items.user_id
        JOIN products
        ON products.id = sale_items.product_id
        AND products.user_id = sale_items.user_id
        WHERE sale_items.user_id = ?
    """ + sales_filter + """
        GROUP BY products.id, products.name
        ORDER BY revenue DESC
        LIMIT 5
    """, _qparams(user_id, sales_params)).fetchall()

    return jsonify([dict(row) for row in rows])


@app.route("/api/inventory-value")
@login_required
def inventory_value():

    conn = get_db()
    user_id = _current_user_id()

    value = conn.execute("""
        SELECT ROUND(COALESCE(SUM(COALESCE(current_stock, 0) * COALESCE(cost_price, 0)), 0), 2)
        FROM products
        WHERE user_id = ?
    """, (user_id,)).fetchone()[0] or 0

    return jsonify({
        "inventory_value": value
    })

# ================= RUN SERVER =================

import os

if __name__ == "__main__":
    app.run(
        debug=os.environ.get("FLASK_DEBUG", "True") == "True",
        host="0.0.0.0",
        port=5000
    )