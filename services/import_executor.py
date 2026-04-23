import logging

import pandas as pd

from services.normalization_service import DATASET_COLUMNS, FK_RESOLUTION_MAP, OPTIONAL_DEFAULT_FIELDS, normalize_columns, normalize_row


logger = logging.getLogger(__name__)

OPTIONAL_INSERT_FIELDS = set(OPTIONAL_DEFAULT_FIELDS)


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


UPSERT_LOOKUPS = {
    "categories": [("name",)],
    "products": [("sku",), ("name",)],
    "customers": [("email",), ("phone",), ("name",)],
    "suppliers": [("email",), ("phone",), ("name",)],
}


def _present_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def _find_existing_record_id(cursor, table, normalized_row, user_id):
    for fields in UPSERT_LOOKUPS.get(table, []):
        values = [_present_value(normalized_row.get(field)) for field in fields]
        if any(value is None for value in values):
            continue

        conditions = []
        params = [user_id]

        for field, value in zip(fields, values):
            if isinstance(value, str):
                conditions.append(f"LOWER(TRIM({field})) = LOWER(TRIM(?))")
                params.append(value)
            else:
                conditions.append(f"{field} = ?")
                params.append(value)

        query = f"""
            SELECT id
            FROM {table}
            WHERE user_id = ?
            AND {' AND '.join(conditions)}
            ORDER BY id
            LIMIT 1
        """
        row = cursor.execute(query, tuple(params)).fetchone()
        if row:
            return int(row["id"])

    return None


def _update_existing_row(cursor, table, columns, normalized_row, user_id, existing_id):
    update_columns = [column for column in columns if column != "user_id"]
    assignments = ", ".join([f"{column} = ?" for column in update_columns])
    values = [normalized_row.get(column) for column in update_columns]
    values.extend([existing_id, user_id])
    cursor.execute(
        f"UPDATE {table} SET {assignments} WHERE id = ? AND user_id = ?",
        tuple(values),
    )


def _validate_purchase_ownership(cursor, purchase_id, user_id):
    """Validate that purchase_id belongs to the user."""
    if not purchase_id:
        return False
    
    try:
        pid = int(purchase_id)
    except (ValueError, TypeError):
        return False
    
    row = cursor.execute(
        """
        SELECT id FROM purchases
        WHERE id = ?
        AND user_id = ?
        """,
        (pid, user_id)
    ).fetchone()
    
    return row is not None


def _resolve_product_id_from_name(cursor, product_name, user_id):
    """Resolve product_id from product_name for purchase_items import."""
    if not product_name:
        return None
    
    clean_name = str(product_name).strip()
    if not clean_name:
        return None
    
    row = cursor.execute(
        """
        SELECT id FROM products
        WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))
        AND user_id = ?
        """,
        (clean_name, user_id)
    ).fetchone()
    
    return int(row["id"]) if row else None


def _purchase_items_dependencies_exist(cursor, user_id):
    """Return True when the current user has both purchases and products available."""
    purchase_exists = cursor.execute(
        "SELECT 1 FROM purchases WHERE user_id = ? LIMIT 1",
        (user_id,),
    ).fetchone() is not None
    product_exists = cursor.execute(
        "SELECT 1 FROM products WHERE user_id = ? LIMIT 1",
        (user_id,),
    ).fetchone() is not None
    return purchase_exists and product_exists


def execute_import(df, dataset, conn, user_id, skip_invalid=True):
    working_df = df.copy()
    if dataset in FK_RESOLUTION_MAP:
        lower_columns = [str(column).strip().lower() for column in working_df.columns]
        for config in FK_RESOLUTION_MAP[dataset]:
            if config["id_field"] not in lower_columns and config["name_field"] in lower_columns:
                working_df[config["id_field"]] = None
    working_df = normalize_columns(working_df, dataset)
    working_df["user_id"] = user_id
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

    if dataset == "purchase_items" and not _purchase_items_dependencies_exist(cursor, user_id):
        return {
            "inserted": 0,
            "skipped": len(working_df),
            "errors": 1,
            "error_detail": [
                {
                    "row": 0,
                    "field": None,
                    "message": "Import purchases and products before purchase_items",
                    "type": "lookup",
                    "code": "MISSING_DEPENDENCIES",
                }
            ],
        }

    inserted = 0
    skipped = 0
    error_rows = 0
    missing_product_matches = 0
    missing_purchase_matches = 0
    error_detail = []

    try:
        for index, raw_row in working_df.iterrows():
            row_number = int(index) + 2
            
            # Pre-processing for purchase_items: resolve product_name and validate purchase_id BEFORE normalization
            lookup_errors = []
            if dataset == "purchase_items":
                raw_dict = raw_row.to_dict() if hasattr(raw_row, "to_dict") else dict(raw_row)
                raw_dict["user_id"] = user_id

                # Normalize product_id zero values to None so validation can catch missing values
                product_id = raw_dict.get("product_id")
                if product_id is not None:
                    try:
                        if int(product_id) == 0:
                            product_id = None
                            raw_dict["product_id"] = None
                    except (ValueError, TypeError):
                        pass

                # Find product_name in raw data (check aliases)
                product_name = None
                for key in raw_dict.keys():
                    key_lower = str(key).strip().lower()
                    if key_lower in ["product_name", "product", "item"]:
                        product_name = raw_dict[key]
                        break

                # Resolve product_name to product_id if product_id not provided
                if product_id is None and product_name:
                    resolved_product_id = _resolve_product_id_from_name(cursor, product_name, user_id)
                    if resolved_product_id is not None:
                        raw_dict["product_id"] = resolved_product_id
                        raw_row = pd.Series(raw_dict)
                        logger.info(
                            "Resolved %r → product_id=%s for user_id=%s",
                            product_name,
                            resolved_product_id,
                            user_id,
                        )
                    else:
                        missing_product_matches += 1
                        lookup_errors.append({
                            "code": "FK_LOOKUP_ERROR",
                            "type": "lookup",
                            "field": "product_id",
                            "message": f"product_name not found for current user: {product_name}",
                            "value": product_name,
                        })
                        logger.warning(
                            "product lookup failed for %r and user_id=%s",
                            product_name,
                            user_id,
                        )

                # Validate purchase_id ownership
                purchase_id = raw_dict.get("purchase_id")
                if purchase_id and not lookup_errors:
                    if not _validate_purchase_ownership(cursor, purchase_id, user_id):
                        missing_purchase_matches += 1
                        lookup_errors.append({
                            "code": "FK_LOOKUP_ERROR",
                            "type": "lookup",
                            "field": "purchase_id",
                            "message": f"purchase_id does not belong to current user: {purchase_id}",
                            "value": purchase_id,
                        })
                        logger.warning(
                            "purchase_id ownership failed for %r and user_id=%s",
                            purchase_id,
                            user_id,
                        )

            # If pre-processing lookups failed, skip this row
            if lookup_errors:
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
                    for err in lookup_errors
                ]
                error_detail.extend(row_detail)
                logger.warning("execute_import(%s): skipped row %s (lookup) -> %s", dataset, row_number, row_detail)

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
            
            # Now run normal normalization and validation
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

            required_null_fields = [
                column
                for column, value in zip(config["columns"], insert_values)
                if value is None and column not in OPTIONAL_INSERT_FIELDS
            ]

            if required_null_fields:
                skipped += 1
                error_rows += 1
                row_error = {
                    "row": row_number,
                    "field": ",".join(required_null_fields),
                    "message": f"NULL values remain after normalization for fields: {', '.join(required_null_fields)}",
                    "type": "missing",
                    "code": "NULL_VALUE_ERROR",
                }
                error_detail.append(row_error)
                logger.warning("execute_import(%s): skipped row %s because NULL values remained -> %s", dataset, row_number, required_null_fields)

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
                existing_id = _find_existing_record_id(
                    cursor,
                    config["table"],
                    normalized_row,
                    user_id,
                )
                if existing_id and config["table"] in UPSERT_LOOKUPS:
                    _update_existing_row(
                        cursor,
                        config["table"],
                        config["columns"],
                        normalized_row,
                        user_id,
                        existing_id,
                    )
                    logger.info(
                        "execute_import(%s): updated existing row id=%s for user_id=%s",
                        dataset,
                        existing_id,
                        user_id,
                    )
                else:
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
    result = {
        "inserted": inserted,
        "skipped": skipped,
        "errors": error_rows,
        "error_detail": error_detail,
    }

    if dataset == "purchase_items":
        result["missing_product_matches"] = missing_product_matches
        result["missing_purchase_matches"] = missing_purchase_matches

    return result
