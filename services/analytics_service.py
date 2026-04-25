import os

from database.db_utils import execute_query as db_execute_query, get_db, get_db_type


def run_query(query, params=(), fetchone=False, fetchall=False):
    conn = get_db()
    try:
        return db_execute_query(conn, query, params, fetchone=fetchone, fetchall=fetchall)
    finally:
        conn.close()


def get_latest_sale_date(user_id):
    query = """

    SELECT MAX(DATE(sale_date))
    FROM sales
    WHERE user_id = ?

    """
    result = run_query(query, (user_id,), fetchone=True)
    if result and result[0]:
        return result[0]
    return None


def get_latest_purchase_date(user_id):
    query = """

    SELECT MAX(DATE(purchase_date))
    FROM purchases
    WHERE user_id = ?

    """
    result = run_query(query, (user_id,), fetchone=True)
    if result and result[0]:
        return result[0]
    return None


def get_latest_expense_date(user_id):
    query = """

    SELECT MAX(DATE(expense_date))
    FROM expenses
    WHERE user_id = ?

    """
    result = run_query(query, (user_id,), fetchone=True)
    if result and result[0]:
        return result[0]
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

    if get_db_type() == "postgres":
        clause = f"""

        AND DATE({column_name}) >= DATE(%s) - INTERVAL '{int(range_days)} days'

        """
    else:
        clause = f"""

        AND DATE({column_name}) >= DATE(?, '-{int(range_days)} days')

        """

    return clause, [latest_date]
