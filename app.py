from flask import Flask, render_template, request, jsonify, session, redirect
import sqlite3
import random
import smtplib
import pandas as pd
from email.mime.text import MIMEText
from datetime import datetime, timedelta


# ================= COLUMN NORMALIZATION ENGINE =================
COLUMN_ALIASES = {

    "customers": {
        "name": ["name", "customer", "customer_name"],
        "phone": ["phone", "mobile"],
        "email": ["email"],
        "city": ["city"],
        "customer_type": ["type", "customer_type"]
    },

    "suppliers": {
        "name": ["name", "supplier"],
        "contact_person": ["contact", "contact_person"],
        "phone": ["phone"],
        "email": ["email"],
        "city": ["city"],
        "rating": ["rating"]
    },

    "products": {
        "name": ["name", "product"],
        "category_id": ["category", "category_id"],
        "supplier_id": ["supplier", "supplier_id"],
        "sku": ["sku"],
        "unit": ["unit"],
        "cost_price": ["cost", "cost_price"],
        "selling_price": ["price", "selling_price"],
        "current_stock": ["stock", "quantity"],
        "reorder_level": ["reorder_level"]
    },

    "sales": {
        "customer_id": ["customer_id", "customer"],
        "sale_date": ["sale_date", "date"],
        "total_amount": ["total", "total_amount"],
        "payment_method": ["payment"],
        "notes": ["notes"]
    },

    "sale_items": {
        "sale_id": ["sale_id"],
        "product_id": ["product", "product_id"],
        "quantity": ["quantity", "qty"],
        "price": ["price"],
        "discount": ["discount"],
        "subtotal": ["subtotal"]
    },

    "purchases": {
        "supplier_id": ["supplier", "supplier_id"],
        "purchase_date": ["purchase_date", "date"],
        "total_amount": ["total", "total_amount"],
        "status": ["status"]
    },

    "purchase_items": {
        "purchase_id": ["purchase_id"],
        "product_id": ["product", "product_id"],
        "quantity": ["quantity", "qty"],
        "unit_cost": ["cost", "unit_cost"]
    },

    "expenses": {
        "category": ["category", "expense_category"],
        "amount": ["amount", "total"],
        "expense_date": ["date", "expense_date"],
        "description": ["description", "remarks"]
    },

    "categories": {
        "name": ["name", "category"],
        "description": ["description"]
    },

    "stock_alerts": {
        "product_id": ["product", "product_id"],
        "alert_type": ["alert_type"],
        "threshold": ["threshold"],
        "is_active": ["is_active"]
    }
}

def normalize_columns(df, dataset):

    if dataset not in COLUMN_ALIASES:
        return df

    mapping = {}

    for standard, aliases in COLUMN_ALIASES[dataset].items():

        for col in df.columns:

            if col.lower().strip() in aliases:
                mapping[col] = standard

    return df.rename(columns=mapping)

def resolve_foreign_keys(row, dataset, conn):

    cursor = conn.cursor()

    try:

        if dataset == "products":

            if row.get("category_id") and not str(row["category_id"]).isdigit():

                r = cursor.execute(
                    "SELECT id FROM categories WHERE name=?",
                    (row["category_id"],)
                ).fetchone()

                if r:
                    row["category_id"] = r["id"]

            if row.get("supplier_id") and not str(row["supplier_id"]).isdigit():

                r = cursor.execute(
                    "SELECT id FROM suppliers WHERE name=?",
                    (row["supplier_id"],)
                ).fetchone()

                if r:
                    row["supplier_id"] = r["id"]


        if dataset == "sales":

            if row.get("customer_id") and not str(row["customer_id"]).isdigit():

                r = cursor.execute(
                    "SELECT id FROM customers WHERE name=?",
                    (row["customer_id"],)
                ).fetchone()

                if r:
                    row["customer_id"] = r["id"]


        if dataset == "purchases":

            if row.get("supplier_id") and not str(row["supplier_id"]).isdigit():

                r = cursor.execute(
                    "SELECT id FROM suppliers WHERE name=?",
                    (row["supplier_id"],)
                ).fetchone()

                if r:
                    row["supplier_id"] = r["id"]


        if dataset == "sale_items":

            if row.get("product_id") and not str(row["product_id"]).isdigit():

                r = cursor.execute(
                    "SELECT id FROM products WHERE name=?",
                    (row["product_id"],)
                ).fetchone()

                if r:
                    row["product_id"] = r["id"]


        if dataset == "purchase_items":

            if row.get("product_id") and not str(row["product_id"]).isdigit():

                r = cursor.execute(
                    "SELECT id FROM products WHERE name=?",
                    (row["product_id"],)
                ).fetchone()

                if r:
                    row["product_id"] = r["id"]

    except:
        pass

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

        conn.execute("""
            INSERT INTO business_profiles
            (business_name, business_type, gst_number, city, address)
            VALUES (?, ?, ?, ?, ?)
        """, (

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

# ================= DASHBOARD SUMMARY =================

@app.route("/api/dashboard-summary")
def dashboard_summary():

    conn = get_db()

    total_sales = conn.execute("SELECT SUM(total_amount) FROM sales").fetchone()[0] or 0

    total_expenses = conn.execute("SELECT SUM(amount) FROM expenses").fetchone()[0] or 0

    total_customers = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]

    total_products = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]

    total_suppliers = conn.execute("SELECT COUNT(*) FROM suppliers").fetchone()[0]

    total_purchases = conn.execute("SELECT SUM(total_amount) FROM purchases").fetchone()[0] or 0

    low_stock = conn.execute("""
        SELECT COUNT(*)
        FROM products
        WHERE current_stock < reorder_level
    """).fetchone()[0]

    return jsonify(dict(
        total_sales=total_sales,
        total_expenses=total_expenses,
        total_customers=total_customers,
        total_products=total_products,
        total_suppliers=total_suppliers,
        total_purchases=total_purchases,
        low_stock_count=low_stock
    ))


# ================= ANALYTICS OVERVIEW =================

@app.route("/api/analytics-overview")
def analytics_overview():

    conn = get_db()

    revenue = conn.execute("""
        SELECT SUM(total_amount)
        FROM sales
    """).fetchone()[0] or 0

    expenses = conn.execute("""
        SELECT SUM(amount)
        FROM expenses
    """).fetchone()[0] or 0

    profit = revenue - expenses

    customers = conn.execute("""
        SELECT COUNT(*)
        FROM customers
    """).fetchone()[0]

    return jsonify(dict(
        revenue=revenue,
        profit=profit,
        customers=customers,
        expense_ratio=(expenses / revenue * 100) if revenue else 0
    ))


# ================= SALES TREND =================

@app.route("/api/sales-trend")
def sales_trend():

    conn = get_db()

    rows = conn.execute("""
        SELECT strftime('%m', sale_date) month,
        SUM(total_amount) total
        FROM sales
        GROUP BY month
    """).fetchall()

    return jsonify([
        dict(month=r["month"], total=r["total"])
        for r in rows
    ])


# ================= PROFIT ANALYSIS =================

@app.route("/api/profit-analysis")
def profit_analysis():

    conn = get_db()

    revenue = conn.execute("SELECT SUM(total_amount) FROM sales").fetchone()[0] or 0

    purchases = conn.execute("SELECT SUM(total_amount) FROM purchases").fetchone()[0] or 0

    expenses = conn.execute("SELECT SUM(amount) FROM expenses").fetchone()[0] or 0

    profit = revenue - purchases - expenses

    return jsonify(dict(
        revenue=revenue,
        cost=purchases,
        expenses=expenses,
        profit=profit
    ))


# ================= INVENTORY INSIGHTS =================

@app.route("/api/inventory-insights")
def inventory_insights():

    conn = get_db()

    rows = conn.execute("""
        SELECT name,current_stock
        FROM products
        WHERE current_stock < reorder_level
    """).fetchall()

    return jsonify([dict(r) for r in rows])


# ================= CUSTOMER INSIGHTS =================

@app.route("/api/customer-insights")
def customer_insights():

    conn = get_db()

    rows = conn.execute("""
        SELECT customers.name,
        SUM(sales.total_amount) total
        FROM sales
        JOIN customers
        ON customers.id=sales.customer_id
        GROUP BY customers.id
        ORDER BY total DESC
        LIMIT 5
    """).fetchall()

    return jsonify([dict(r) for r in rows])


# ================= EXPENSE BREAKDOWN =================

@app.route("/api/expense-breakdown")
def expense_breakdown():

    conn = get_db()

    rows = conn.execute("""
        SELECT category,
        SUM(amount) total
        FROM expenses
        GROUP BY category
    """).fetchall()

    return jsonify([dict(r) for r in rows])


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


        for _, row in df.iterrows():
            
            row = resolve_foreign_keys(row, dataset, conn)
            
            try:

                if dataset == "customers":

                    cursor.execute("""
                        INSERT INTO customers
                        (name, phone, email, city, customer_type)
                        VALUES (?, ?, ?, ?, ?)
                    """, (

                        row.get("name"),
                        row.get("phone"),
                        row.get("email"),
                        row.get("city"),
                        row.get("customer_type")

                    ))


                elif dataset == "suppliers":

                    cursor.execute("""
                        INSERT INTO suppliers
                        (name, contact_person, phone, email, city, rating)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (

                        row.get("name"),
                        row.get("contact_person"),
                        row.get("phone"),
                        row.get("email"),
                        row.get("city"),
                        row.get("rating")

                    ))


                elif dataset == "categories":

                    cursor.execute("""
                        INSERT INTO categories
                        (name, description)
                        VALUES (?, ?)
                    """, (

                        row.get("name"),
                        row.get("description")

                    ))


                elif dataset == "products":

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

                        row.get("name"),
                        row.get("category_id"),
                        row.get("supplier_id"),
                        row.get("sku"),
                        row.get("unit"),
                        row.get("cost_price"),
                        row.get("selling_price"),
                        row.get("current_stock"),
                        row.get("reorder_level")

                    ))


                elif dataset == "purchases":

                    cursor.execute("""
                        INSERT INTO purchases
                        (supplier_id, purchase_date, total_amount, status)
                        VALUES (?, ?, ?, ?)
                    """, (

                        row.get("supplier_id"),
                        row.get("purchase_date"),
                        row.get("total_amount"),
                        row.get("status")

                    ))


                elif dataset == "purchase_items":

                    cursor.execute("""
                        INSERT INTO purchase_items
                        (purchase_id, product_id, quantity, unit_cost)
                        VALUES (?, ?, ?, ?)
                    """, (

                        row.get("purchase_id"),
                        row.get("product_id"),
                        row.get("quantity"),
                        row.get("unit_cost")

                    ))


                elif dataset == "sales":

                    cursor.execute("""
                        INSERT INTO sales
                        (customer_id, sale_date, total_amount, payment_method, notes)
                        VALUES (?, ?, ?, ?, ?)
                    """, (

                        row.get("customer_id"),
                        row.get("sale_date"),
                        row.get("total_amount"),
                        row.get("payment_method"),
                        row.get("notes")

                    ))


                elif dataset == "sale_items":

                    cursor.execute("""
                        INSERT INTO sale_items
                        (sale_id, product_id, quantity, price, discount, subtotal)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (

                        row.get("sale_id"),
                        row.get("product_id"),
                        row.get("quantity"),
                        row.get("price"),
                        row.get("discount"),
                        row.get("subtotal")

                    ))


                elif dataset == "expenses":

                    cursor.execute("""
                        INSERT INTO expenses
                        (category, amount, expense_date, description)
                        VALUES (?, ?, ?, ?)
                    """, (

                        row.get("category"),
                        row.get("amount"),
                        row.get("expense_date"),
                        row.get("description")

                    ))


                elif dataset == "stock_alerts":

                    cursor.execute("""
                        INSERT INTO stock_alerts
                        (product_id, alert_type, threshold, is_active)
                        VALUES (?, ?, ?, ?)
                    """, (

                        row.get("product_id"),
                        row.get("alert_type"),
                        row.get("threshold"),
                        row.get("is_active")

                    ))


                inserted += 1


            except Exception as e:

                print("Row skipped:", e)

                print("Row skipped:", e)
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