import logging
import math
from datetime import date, datetime

import pandas as pd


logger = logging.getLogger(__name__)


COLUMN_ALIASES = {
    "customers": {
        "name": ["name", "customer", "customer_name", "full_name"],
        "phone": ["phone", "mobile", "contact_no", "phone_number"],
        "email": ["email", "email_address"],
        "city": ["city", "location"],
        "customer_type": ["type", "customer_type", "segment"],
    },
    "suppliers": {
        "name": ["name", "supplier", "supplier_name", "company"],
        "contact_person": ["contact", "contact_person", "contact_name"],
        "phone": ["phone", "mobile", "contact_no"],
        "email": ["email"],
        "city": ["city", "location"],
        "rating": ["rating", "score"],
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
        "reorder_level": ["reorder_level", "reorder", "min_stock"],
    },
    "sales": {
        "customer_id": ["customer_id", "customer", "customer_name"],
        "sale_date": ["sale_date", "date", "invoice_date"],
        "total_amount": ["total", "total_amount", "amount", "invoice_amount"],
        "payment_method": ["payment", "payment_method", "mode"],
        "notes": ["notes", "remarks", "comment"],
    },
    "sale_items": {
        "sale_id": ["sale_id", "invoice_id"],
        "product_id": ["product", "product_id", "product_name", "item"],
        "quantity": ["quantity", "qty"],
        "price": ["price", "unit_price", "rate"],
        "discount": ["discount", "disc", "discount_pct"],
        "subtotal": ["subtotal", "line_total", "amount"],
    },
    "purchases": {
        "supplier_id": ["supplier", "supplier_id", "supplier_name"],
        "purchase_date": ["purchase_date", "date", "po_date"],
        "total_amount": ["total", "total_amount", "po_amount"],
        "status": ["status"],
    },
    "purchase_items": {
        "purchase_id": ["purchase_id", "po_id"],
        "product_id": ["product", "product_id", "product_name", "item"],
        "quantity": ["quantity", "qty"],
        "unit_cost": ["cost", "unit_cost", "rate"],
    },
    "expenses": {
        "category": ["category", "expense_category", "type"],
        "amount": ["amount", "total", "expense_amount"],
        "expense_date": ["date", "expense_date"],
        "description": ["description", "remarks", "details"],
    },
    "categories": {
        "name": ["name", "category", "category_name"],
        "description": ["description", "details"],
    },
    "stock_alerts": {
        "product_id": ["product", "product_id", "product_name"],
        "alert_type": ["alert_type", "type"],
        "threshold": ["threshold", "min_qty"],
        "is_active": ["is_active", "active", "status"],
    },
}

FK_MAP = {
    "products": [
        ("category_id", "categories", "name"),
        ("supplier_id", "suppliers", "name"),
    ],
    "sales": [
        ("customer_id", "customers", "name"),
    ],
    "purchases": [
        ("supplier_id", "suppliers", "name"),
    ],
    "sale_items": [
        ("product_id", "products", "name"),
    ],
    "purchase_items": [
        ("product_id", "products", "name"),
    ],
    "stock_alerts": [
        ("product_id", "products", "name"),
    ],
}

ID_REFERENCE_MAP = {
    "products": [
        ("category_id", "categories"),
        ("supplier_id", "suppliers"),
    ],
    "sales": [
        ("customer_id", "customers"),
    ],
    "purchases": [
        ("supplier_id", "suppliers"),
    ],
    "sale_items": [
        ("sale_id", "sales"),
        ("product_id", "products"),
    ],
    "purchase_items": [
        ("purchase_id", "purchases"),
        ("product_id", "products"),
    ],
    "stock_alerts": [
        ("product_id", "products"),
    ],
}

FK_FIELDS = {
    dataset: {fk_col for fk_col, _, _ in mappings}
    for dataset, mappings in FK_MAP.items()
}

DATASET_COLUMNS = {
    dataset: list(columns.keys())
    for dataset, columns in COLUMN_ALIASES.items()
}

REQUIRED_FIELDS = {
    "customers": {"name"},
    "products": {"name"},
    "sales": {"customer_id"},
    "sale_items": {"product_id"},
    "purchases": {"supplier_id"},
}

DATE_FIELDS = {
    "sales": {"sale_date"},
    "purchases": {"purchase_date"},
    "expenses": {"expense_date"},
}

NUMERIC_FIELDS = {
    "suppliers": {"rating"},
    "products": {"category_id", "supplier_id", "cost_price", "selling_price", "current_stock", "reorder_level"},
    "sales": {"customer_id", "total_amount"},
    "sale_items": {"sale_id", "product_id", "quantity", "price", "discount", "subtotal"},
    "purchases": {"supplier_id", "total_amount"},
    "purchase_items": {"purchase_id", "product_id", "quantity", "unit_cost"},
    "expenses": {"amount"},
    "stock_alerts": {"product_id", "threshold", "is_active"},
}

INTEGER_FIELDS = {
    "suppliers": {"rating"},
    "products": {"category_id", "supplier_id", "current_stock", "reorder_level"},
    "sales": {"customer_id"},
    "sale_items": {"sale_id", "product_id", "quantity"},
    "purchases": {"supplier_id"},
    "purchase_items": {"purchase_id", "product_id", "quantity"},
    "stock_alerts": {"product_id", "threshold", "is_active"},
}

PARENT_LOOKUP_FIELDS = {
    "sale_items": {
        "parent_id": "sale_id",
        "owner_table": "sales",
        "party_table": "customers",
        "party_field": "customer_name",
        "party_aliases": ("customer_name", "customer", "customer_id"),
        "date_field": "sale_date",
        "date_aliases": ("sale_date", "date", "invoice_date"),
        "sql": """
            SELECT id
            FROM sales
            WHERE customer_id = ? AND sale_date = ? AND user_id = ?
            ORDER BY id
        """,
    },
    "purchase_items": {
        "parent_id": "purchase_id",
        "owner_table": "purchases",
        "party_table": "suppliers",
        "party_field": "supplier_name",
        "party_aliases": ("supplier_name", "supplier", "supplier_id"),
        "date_field": "purchase_date",
        "date_aliases": ("purchase_date", "date", "po_date"),
        "sql": """
            SELECT id
            FROM purchases
            WHERE supplier_id = ? AND purchase_date = ? AND user_id = ?
            ORDER BY id
        """,
    },
}


def _error(code, err_type, field, message, value=None):
    return {
        "code": code,
        "type": err_type,
        "field": field,
        "message": message,
        "value": value,
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
        if not stripped:
            return None
        lowered = stripped.lower()
        if lowered in {"nan", "null", "none"}:
            return None
        return stripped

    return val


def _is_resolvable(val):
    if val is None:
        return False

    try:
        if pd.isna(val):
            return False
    except (TypeError, ValueError):
        pass

    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return False

    s = str(val).strip()
    if not s:
        return False
    if s.isdigit():
        return False
    return True


def _lookup_fk(cursor, ref_table, ref_col, raw_val, user_id):
    clean = str(raw_val).strip()
    row = cursor.execute(
        f"SELECT id FROM {ref_table} WHERE LOWER(TRIM({ref_col})) = LOWER(TRIM(?)) AND user_id = ?",
        (clean, user_id),
    ).fetchone()
    if row:
        return int(row["id"])
    return None


def _id_belongs_to_user(cursor, ref_table, record_id, user_id):
    row = cursor.execute(
        f"SELECT id FROM {ref_table} WHERE id = ? AND user_id = ?",
        (record_id, user_id),
    ).fetchone()
    return row is not None


def _first_present_value(row, aliases):
    for alias in aliases:
        value = _safe_val(row.get(alias))
        if value is not None:
            return value
    return None


def _normalize_integer_id(field, value):
    try:
        return _normalize_numeric_value(field, value, {field}), None
    except Exception:
        return None, _error(
            "INVALID_NUMERIC",
            "type",
            field,
            f"{field} has invalid numeric value: {value!r}",
            value=value,
        )


def _resolve_party_id(cursor, ref_table, field_name, raw_value, user_id):
    if raw_value is None:
        return None, _error(
            "FK_LOOKUP_ERROR",
            "lookup",
            field_name,
            f"{field_name} is required to resolve the parent transaction",
        )

    if _is_resolvable(raw_value):
        resolved_id = _lookup_fk(cursor, ref_table, "name", raw_value, user_id)
        if resolved_id is None:
            return None, _error(
                "FK_LOOKUP_ERROR",
                "lookup",
                field_name,
                f"{field_name} value {raw_value!r} was not found in {ref_table}.name for this user",
                value=raw_value,
            )
        return resolved_id, None

    resolved_id, numeric_error = _normalize_integer_id(field_name, raw_value)
    if numeric_error:
        return None, numeric_error
    if not _id_belongs_to_user(cursor, ref_table, resolved_id, user_id):
        return None, _error(
            "FK_LOOKUP_ERROR",
            "lookup",
            field_name,
            f"{field_name} value {resolved_id!r} does not belong to this user in {ref_table}",
            value=raw_value,
        )
    return resolved_id, None


def _resolve_parent_transaction_id(normalized, dataset, cursor, user_id):
    config = PARENT_LOOKUP_FIELDS.get(dataset)
    if not config:
        return normalized, []

    parent_field = config["parent_id"]
    parent_value = _safe_val(normalized.get(parent_field))
    needs_lookup = parent_value is None or _is_resolvable(parent_value)

    if not needs_lookup:
        return normalized, []

    party_value = _first_present_value(normalized, config["party_aliases"])
    date_value = _first_present_value(normalized, config["date_aliases"])
    missing_fields = []

    if party_value is None:
        missing_fields.append(config["party_field"])
    if date_value is None:
        missing_fields.append(config["date_field"])

    if missing_fields:
        return normalized, [
            _error(
                "FK_LOOKUP_ERROR",
                "lookup",
                parent_field,
                f"{parent_field} could not be resolved automatically. Provide {parent_field} or {', '.join(missing_fields)}.",
                value=parent_value,
            )
        ]

    party_id, party_error = _resolve_party_id(
        cursor, config["party_table"], config["party_field"], party_value, user_id
    )
    if party_error:
        return normalized, [party_error]

    normalized_date, date_error = _normalize_date_value(config["date_field"], date_value)
    if date_error:
        return normalized, [date_error]

    matches = cursor.execute(config["sql"], (party_id, normalized_date, user_id)).fetchall()
    if not matches:
        return normalized, [
            _error(
                "FK_LOOKUP_ERROR",
                "lookup",
                parent_field,
                f"{parent_field} could not be resolved from {config['party_field']}={party_value!r} and {config['date_field']}={normalized_date!r}",
                value=parent_value,
            )
        ]

    if len(matches) > 1:
        return normalized, [
            _error(
                "FK_LOOKUP_ERROR",
                "lookup",
                parent_field,
                f"Multiple {config['owner_table']} rows matched {config['party_field']}={party_value!r} and {config['date_field']}={normalized_date!r}; provide {parent_field} explicitly",
                value=parent_value,
            )
        ]

    normalized[parent_field] = int(matches[0]["id"])
    logger.info(
        "resolve_foreign_keys(%s): %s resolved via %s=%r and %s=%r -> %s",
        dataset,
        parent_field,
        config["party_field"],
        party_value,
        config["date_field"],
        normalized_date,
        normalized[parent_field],
    )
    return normalized, []


def _dataset_alias_lookup(dataset):
    alias_lookup = {}
    for standard_col, aliases in COLUMN_ALIASES.get(dataset, {}).items():
        alias_lookup[standard_col.lower()] = standard_col
        for alias in aliases:
            alias_lookup[str(alias).strip().lower()] = standard_col
    return alias_lookup


def _collapse_duplicate_columns(df):
    ordered_columns = list(dict.fromkeys(df.columns.tolist()))
    if len(ordered_columns) == len(df.columns):
        return df

    collapsed = {}
    for column in ordered_columns:
        column_frame = df.loc[:, df.columns == column]
        if column_frame.shape[1] == 1:
            collapsed[column] = column_frame.iloc[:, 0]
        else:
            collapsed[column] = column_frame.bfill(axis=1).iloc[:, 0]

    logger.warning("Collapsed duplicate mapped columns: %s", [c for c in ordered_columns if list(df.columns).count(c) > 1])
    return pd.DataFrame(collapsed)


def normalize_columns(data, dataset):
    if dataset not in COLUMN_ALIASES:
        return data

    alias_lookup = _dataset_alias_lookup(dataset)

    if isinstance(data, pd.DataFrame):
        working_df = data.copy()
        working_df.columns = [str(column).strip() for column in working_df.columns]

        mapping = {}
        for column in working_df.columns:
            standard_col = alias_lookup.get(column.lower())
            if standard_col:
                mapping[column] = standard_col

        renamed = working_df.rename(columns=mapping)
        renamed = _collapse_duplicate_columns(renamed)

        expected = set(COLUMN_ALIASES[dataset].keys())
        found = set(renamed.columns)
        missing = sorted(expected - found)
        if missing:
            logger.warning("normalize_columns(%s): expected columns not found -> %s", dataset, missing)

        logger.info("normalize_columns(%s): mapped %d column(s) -> %s", dataset, len(mapping), mapping)
        return renamed

    raw_row = data.to_dict() if hasattr(data, "to_dict") else dict(data)
    normalized = {}
    for key, value in raw_row.items():
        clean_key = str(key).strip()
        standard_col = alias_lookup.get(clean_key.lower(), clean_key)
        if standard_col not in normalized:
            normalized[standard_col] = value
            continue

        if _safe_val(normalized[standard_col]) is None and _safe_val(value) is not None:
            normalized[standard_col] = value

    return normalized


def _normalize_date_value(field, value):
    try:
        if isinstance(value, (datetime, pd.Timestamp)):
            parsed = pd.Timestamp(value)
        elif isinstance(value, date):
            parsed = pd.Timestamp(value)
        else:
            parsed = pd.to_datetime(value, errors="raise")
        return parsed.date().isoformat(), None
    except Exception:
        return None, _error(
            "INVALID_DATE",
            "type",
            field,
            f"{field} has invalid date value: {value!r}",
            value=value,
        )


def _normalize_numeric_value(field, value, integer_fields):
    if isinstance(value, bool):
        numeric_value = 1 if value else 0
    elif isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "active"} and field == "is_active":
            numeric_value = 1
        elif lowered in {"false", "no", "inactive"} and field == "is_active":
            numeric_value = 0
        else:
            numeric_value = float(value.replace(",", ""))
    else:
        numeric_value = float(value)

    if math.isnan(numeric_value) or math.isinf(numeric_value):
        raise ValueError("non-finite numeric value")

    if field in integer_fields:
        if not float(numeric_value).is_integer():
            raise ValueError("expected integer-like numeric value")
        return int(numeric_value)

    return float(numeric_value)


def normalize_datatypes(row, dataset):
    normalized = dict(row)
    errors = []
    date_fields = DATE_FIELDS.get(dataset, set())
    numeric_fields = NUMERIC_FIELDS.get(dataset, set())
    integer_fields = INTEGER_FIELDS.get(dataset, set())
    fk_fields = FK_FIELDS.get(dataset, set())

    for field in DATASET_COLUMNS.get(dataset, []):
        value = _safe_val(normalized.get(field))
        normalized[field] = value

        if value is None:
            continue

        original_value = value

        if field in date_fields:
            converted, error = _normalize_date_value(field, value)
            if error:
                errors.append(error)
                continue
            normalized[field] = converted
            if original_value != converted:
                logger.info("normalize_datatypes(%s): date corrected %s=%r -> %r", dataset, field, original_value, converted)
            continue

        if field in numeric_fields:
            if dataset in PARENT_LOOKUP_FIELDS and field == PARENT_LOOKUP_FIELDS[dataset]["parent_id"]:
                if _is_resolvable(value):
                    continue
            if field in fk_fields and _is_resolvable(value):
                continue
            try:
                converted = _normalize_numeric_value(field, value, integer_fields)
                normalized[field] = converted
                if original_value != converted:
                    logger.info("normalize_datatypes(%s): numeric corrected %s=%r -> %r", dataset, field, original_value, converted)
            except Exception:
                errors.append(
                    _error(
                        "INVALID_NUMERIC",
                        "type",
                        field,
                        f"{field} has invalid numeric value: {value!r}",
                        value=value,
                    )
                )

    return normalized, errors


def resolve_foreign_keys(row, dataset, conn, user_id):
    normalized = {key: _safe_val(value) for key, value in dict(row).items()}
    errors = []

    cursor = conn.cursor()

    for fk_col, ref_table, ref_col in FK_MAP.get(dataset, []):
        raw_val = normalized.get(fk_col)
        if raw_val is None:
            continue

        if _is_resolvable(raw_val):
            resolved_id = _lookup_fk(cursor, ref_table, ref_col, raw_val, user_id)
            if resolved_id is None:
                logger.warning(
                    "FK lookup failure dataset=%s field=%s value=%r target=%s.%s user_id=%s",
                    dataset,
                    fk_col,
                    raw_val,
                    ref_table,
                    ref_col,
                    user_id,
                )
                errors.append(
                    _error(
                        "FK_LOOKUP_ERROR",
                        "lookup",
                        fk_col,
                        f"{fk_col} value {raw_val!r} was not found in {ref_table}.{ref_col} for this user",
                        value=raw_val,
                    )
                )
                continue

            normalized[fk_col] = resolved_id
            logger.info("resolve_foreign_keys(%s): %s=%r -> %s", dataset, fk_col, raw_val, resolved_id)

    normalized, parent_errors = _resolve_parent_transaction_id(normalized, dataset, cursor, user_id)
    errors.extend(parent_errors)

    for fk_col, ref_table in ID_REFERENCE_MAP.get(dataset, []):
        record_id = _safe_val(normalized.get(fk_col))
        if record_id is None:
            continue
        if isinstance(record_id, str) and _is_resolvable(record_id):
            continue
        if not _id_belongs_to_user(cursor, ref_table, record_id, user_id):
            logger.warning(
                "FK ownership failure dataset=%s field=%s record_id=%r target=%s user_id=%s",
                dataset,
                fk_col,
                record_id,
                ref_table,
                user_id,
            )
            errors.append(
                _error(
                    "FK_LOOKUP_ERROR",
                    "lookup",
                    fk_col,
                    f"{fk_col} value {record_id!r} does not belong to this user in {ref_table}",
                    value=record_id,
                )
            )

    return normalized, errors


def validate_required_fields(row, dataset):
    errors = []
    for field in REQUIRED_FIELDS.get(dataset, set()):
        if _safe_val(row.get(field)) is None:
            errors.append(
                _error(
                    "MISSING_REQUIRED_FIELD",
                    "missing",
                    field,
                    f"{field} is required for dataset {dataset}",
                )
            )
    return errors


def validate_null_values(row, dataset):
    errors = []
    for field in DATASET_COLUMNS.get(dataset, []):
        if _safe_val(row.get(field)) is None:
            errors.append(
                _error(
                    "NULL_VALUE_ERROR",
                    "missing",
                    field,
                    f"{field} is NULL or empty after normalization",
                )
            )
    return errors


def normalize_row(row, dataset, conn, user_id):
    sanitized = {
        key: _safe_val(value)
        for key, value in (row.to_dict().items() if hasattr(row, "to_dict") else dict(row).items())
    }
    normalized = normalize_columns(sanitized, dataset)
    normalized, datatype_errors = normalize_datatypes(normalized, dataset)
    normalized, fk_errors = resolve_foreign_keys(normalized, dataset, conn, user_id)
    required_errors = validate_required_fields(normalized, dataset)
    normalized["user_id"] = user_id
    null_errors = validate_null_values(normalized, dataset)

    error_list = datatype_errors + fk_errors + required_errors + null_errors
    return normalized, error_list
