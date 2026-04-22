import logging

from services.normalization_service import DATASET_COLUMNS, normalize_columns, normalize_row


logger = logging.getLogger(__name__)


INSERT_CONFIG = {
    "customers": {
        "table": "customers",
        "columns": ["name", "phone", "email", "city", "customer_type", "user_id"],
    },
    "suppliers": {
        "table": "suppliers",
        "columns": ["name", "contact_person", "phone", "email", "city", "rating", "user_id"],
    },
    "categories": {
        "table": "categories",
        "columns": ["name", "description", "user_id"],
    },
    "products": {
        "table": "products",
        "columns": ["name", "category_id", "supplier_id", "sku", "unit", "cost_price", "selling_price", "current_stock", "reorder_level", "user_id"],
    },
    "purchases": {
        "table": "purchases",
        "columns": ["supplier_id", "purchase_date", "total_amount", "status", "user_id"],
    },
    "purchase_items": {
        "table": "purchase_items",
        "columns": ["purchase_id", "product_id", "quantity", "unit_cost", "user_id"],
    },
    "sales": {
        "table": "sales",
        "columns": ["customer_id", "sale_date", "total_amount", "payment_method", "notes", "user_id"],
    },
    "sale_items": {
        "table": "sale_items",
        "columns": ["sale_id", "product_id", "quantity", "price", "discount", "subtotal", "user_id"],
    },
    "expenses": {
        "table": "expenses",
        "columns": ["category", "amount", "expense_date", "description", "user_id"],
    },
    "stock_alerts": {
        "table": "stock_alerts",
        "columns": ["product_id", "alert_type", "threshold", "is_active", "user_id"],
    },
}


def _build_insert_sql(table, columns):
    placeholders = ", ".join(["?"] * len(columns))
    column_sql = ", ".join(columns)
    return f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"


def execute_import(df, dataset, conn, user_id, skip_invalid=True):
    working_df = normalize_columns(df.copy(), dataset)
    config = INSERT_CONFIG.get(dataset)

    if not config:
        return {
            "inserted": 0,
            "skipped": len(working_df),
            "errors": 1,
            "error_detail": [
                {
                    "row": 0,
                    "field": None,
                    "message": f"Unsupported dataset: {dataset}",
                    "type": "type",
                    "code": "UNSUPPORTED_DATASET",
                }
            ],
        }

    insert_sql = _build_insert_sql(config["table"], config["columns"])
    cursor = conn.cursor()
    inserted = 0
    skipped = 0
    error_rows = 0
    error_detail = []

    try:
        for index, raw_row in working_df.iterrows():
            row_number = int(index) + 2
            normalized_row, row_errors = normalize_row(raw_row, dataset, conn, user_id)

            if row_errors:
                skipped += 1
                error_rows += 1
                row_detail = [
                    {
                        "row": row_number,
                        "field": err.get("field"),
                        "message": err.get("message"),
                        "type": err.get("type"),
                        "code": err.get("code"),
                    }
                    for err in row_errors
                ]
                error_detail.extend(row_detail)
                logger.warning("execute_import(%s): skipped row %s -> %s", dataset, row_number, row_detail)

                if not skip_invalid:
                    conn.rollback()
                    return {
                        "inserted": 0,
                        "skipped": skipped,
                        "errors": error_rows,
                        "error_detail": error_detail,
                        "aborted": True,
                    }
                continue

            insert_values = [normalized_row.get(column) for column in config["columns"]]

            if any(value is None for value in insert_values):
                skipped += 1
                error_rows += 1
                null_fields = [column for column, value in zip(config["columns"], insert_values) if value is None]
                row_error = {
                    "row": row_number,
                    "field": ",".join(null_fields),
                    "message": f"NULL values remain after normalization for fields: {', '.join(null_fields)}",
                    "type": "missing",
                    "code": "NULL_VALUE_ERROR",
                }
                error_detail.append(row_error)
                logger.warning("execute_import(%s): skipped row %s because NULL values remained -> %s", dataset, row_number, null_fields)

                if not skip_invalid:
                    conn.rollback()
                    return {
                        "inserted": 0,
                        "skipped": skipped,
                        "errors": error_rows,
                        "error_detail": error_detail,
                        "aborted": True,
                    }
                continue

            try:
                cursor.execute(insert_sql, insert_values)
                inserted += 1
            except Exception as exc:
                skipped += 1
                error_rows += 1
                row_error = {
                    "row": row_number,
                    "field": None,
                    "message": str(exc),
                    "type": "type",
                    "code": "INSERT_ERROR",
                }
                error_detail.append(row_error)
                logger.exception("execute_import(%s): insert failed for row %s", dataset, row_number)

                if not skip_invalid:
                    conn.rollback()
                    return {
                        "inserted": 0,
                        "skipped": skipped,
                        "errors": error_rows,
                        "error_detail": error_detail,
                        "aborted": True,
                    }

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    logger.info(
        "execute_import(%s): inserted=%s skipped=%s total_columns=%s",
        dataset,
        inserted,
        skipped,
        DATASET_COLUMNS.get(dataset, []),
    )
    return {
        "inserted": inserted,
        "skipped": skipped,
        "errors": error_rows,
        "error_detail": error_detail,
    }
