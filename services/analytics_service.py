import os
import sqlite3

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB = os.path.normpath(os.path.join(BASE_DIR, "..", "instance", "vyapaariq.db"))


def execute_query(query, params=()):
    conn = sqlite3.connect(DB, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(query, params)
        return cur.fetchall()
    finally:
        conn.close()


def get_latest_sale_date(user_id):
    query = """

    SELECT MAX(DATE(sale_date))
    FROM sales
    WHERE user_id = ?

    """
    result = execute_query(query, (user_id,))
    if result and result[0][0]:
        return result[0][0]
    return None


def get_latest_purchase_date(user_id):
    query = """

    SELECT MAX(DATE(purchase_date))
    FROM purchases
    WHERE user_id = ?

    """
    result = execute_query(query, (user_id,))
    if result and result[0][0]:
        return result[0][0]
    return None


def get_latest_expense_date(user_id):
    query = """

    SELECT MAX(DATE(expense_date))
    FROM expenses
    WHERE user_id = ?

    """
    result = execute_query(query, (user_id,))
    if result and result[0][0]:
        return result[0][0]
    return None


def apply_date_filter(column_name, range_days, user_id):
    if not range_days or not user_id:
        return "", []

    if "purchase_date" in column_name:
        latest_date = get_latest_purchase_date(user_id)
    elif "expense_date" in column_name:
        latest_date = get_latest_expense_date(user_id)
    else:
        latest_date = get_latest_sale_date(user_id)

    if not latest_date:
        return "", []

    clause = f"""

    AND DATE({column_name}) >=
    DATE(?, '-{range_days} days')

    """
    return clause, [latest_date]
