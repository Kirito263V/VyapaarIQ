from flask import Flask, render_template, request, jsonify, session, redirect
import sqlite3
import random
import smtplib
import pandas as pd
from email.mime.text import MIMEText
from datetime import datetime, timedelta

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

    file = request.files["file"]

    df = pd.read_excel(file) if file.filename.endswith("xlsx") else pd.read_csv(file)

    preview = df.head(10)

    return jsonify(dict(
        columns=list(preview.columns),
        rows=preview.values.tolist(),
        total_rows=len(df)
    ))


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


# ================= RUN SERVER =================

if __name__ == "__main__":
    app.run(debug=True)