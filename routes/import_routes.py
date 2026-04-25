import json
import logging

import pandas as pd
from flask import Blueprint, current_app, jsonify, request, session

from database.db_utils import get_db, get_table_columns
from services.import_executor import (
    execute_import,
    _resolve_product_id_from_name,
    _validate_purchase_ownership,
)
from services.normalization_service import normalize_columns
from services.validation_service import validate_dataset


logger = logging.getLogger(__name__)

import_bp = Blueprint("import_bp", __name__)

DATASET_MAP = {
    "categories": "categories",
    "products": "products",
    "customers": "customers",
    "suppliers": "suppliers",
    "sales": "sales",
    "sale_items": "sale_items",
    "sales_items": "sale_items",
    "purchases": "purchases",
    "purchase_items": "purchase_items",
    "expenses": "expenses",
    "inventory": "inventory",
    "stock_alerts": "stock_alerts",
}

SYSTEM_COLUMNS = {"id", "user_id"}


def _get_db():
    db_url = current_app.config.get("DATABASE")
    return get_db(db_url)


def _auth_error():
    return jsonify({"error": "authentication required"}), 401


@import_bp.before_request
def ensure_import_auth():
    if "user_id" not in session:
        return _auth_error()


def _serialize_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            return str(value)
    return value.item() if hasattr(value, "item") else value


def _parse_column_mapping(raw_mapping):
    if not raw_mapping:
        return {}
    try:
        parsed = json.loads(raw_mapping)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _normalize_dataset_name(raw_dataset):
    if not raw_dataset:
        return None
    normalized = str(raw_dataset).strip().lower()
    return DATASET_MAP.get(normalized, normalized)


def _normalize_sheet_name(raw_sheet):
    if raw_sheet in (None, ""):
        return None
    return str(raw_sheet).strip().lower()


def _coalesce_duplicate_columns(df):
    if not df.columns.duplicated().any():
        return df
    ordered_columns = list(dict.fromkeys(df.columns.tolist()))
    return pd.DataFrame(
        {
            column: df.loc[:, df.columns == column].bfill(axis=1).iloc[:, 0]
            for column in ordered_columns
        }
    )


def _normalize_dataframe_columns(df):
    working_df = df.copy()
    working_df.columns = [str(column).strip().lower() for column in working_df.columns]
    return _coalesce_duplicate_columns(working_df)


def _apply_column_mapping(df, mapping):
    if not mapping:
        return df

    rename_map = {}
    skip_columns = []

    for source_column, target_column in mapping.items():
        if source_column not in df.columns:
            continue
        if not target_column or target_column == "SKIP":
            skip_columns.append(source_column)
        else:
            rename_map[source_column] = target_column

    working_df = df.drop(columns=skip_columns, errors="ignore").rename(columns=rename_map)
    working_df = _coalesce_duplicate_columns(working_df)

    if rename_map:
        logger.info("Explicit column mapping applied: %s", rename_map)
    if skip_columns:
        logger.info("Skipped source columns during import mapping: %s", skip_columns)

    return working_df


def _get_table_columns(conn, dataset):
    if not dataset:
        return []
    return get_table_columns(conn, dataset)


def _get_required_columns(table_columns):
    return [col for col in table_columns if col not in SYSTEM_COLUMNS]


def _resolve_excel_sheet_name(file_storage, sheet=None):
    file_storage.stream.seek(0)
    excel_file = pd.ExcelFile(file_storage)
    sheet_names = excel_file.sheet_names

    if not sheet_names:
        raise ValueError("No sheets found in Excel file")

    if sheet in (None, ""):
        return excel_file, sheet_names, sheet_names[0]

    normalized_sheet = _normalize_sheet_name(sheet)
    lookup = {str(name).strip().lower(): name for name in sheet_names}
    actual_sheet = lookup.get(normalized_sheet)

    if actual_sheet is None:
        raise KeyError("Sheet not found in Excel file")

    return excel_file, sheet_names, actual_sheet


def _load_dataframe(file_storage, sheet=None):
    if not file_storage or not file_storage.filename:
        raise ValueError("A file upload is required.")

    file_storage.stream.seek(0)
    lower_name = file_storage.filename.lower()

    if lower_name.endswith((".xlsx", ".xls")):
        try:
            excel_file, sheet_names, actual_sheet = _resolve_excel_sheet_name(file_storage, sheet=sheet)
            df = pd.read_excel(excel_file, sheet_name=actual_sheet)
        except KeyError as exc:
            raise ValueError("Sheet not found in Excel file") from exc
        if df.empty:
            raise ValueError("Selected sheet contains no rows")
        return _normalize_dataframe_columns(df), actual_sheet, sheet_names

    df = pd.read_csv(file_storage)
    if df.empty:
        raise ValueError("Selected sheet contains no rows")
    return _normalize_dataframe_columns(df), None, []


def _load_excel_sheets(file_storage):
    if not file_storage or not file_storage.filename:
        return []

    lower_name = file_storage.filename.lower()
    if not lower_name.endswith((".xlsx", ".xls")):
        return []

    file_storage.stream.seek(0)
    excel_file = pd.ExcelFile(file_storage)
    return excel_file.sheet_names


def _prepare_dataframe(file_storage, sheet=None, column_mapping=None, dataset=None):
    working_df, actual_sheet, sheet_names = _load_dataframe(file_storage, sheet=sheet)
    working_df = _apply_column_mapping(working_df, column_mapping or {})
    working_df["user_id"] = session["user_id"]
    if dataset:
        working_df = normalize_columns(working_df, dataset)
    return working_df, actual_sheet, sheet_names


def _preview_rows(df, limit=10):
    preview = df.head(limit).astype(object)
    rows = []
    for _, row in preview.iterrows():
        rows.append([_serialize_value(value) for value in row.tolist()])
    return rows


def _purchase_items_preview_summary(df, conn, user_id):
    cursor = conn.cursor()
    valid_rows = 0
    missing_products = 0
    missing_purchases = 0

    for _, row in df.iterrows():
        raw_dict = row.to_dict() if hasattr(row, "to_dict") else dict(row)
        raw_dict["user_id"] = user_id

        product_id = raw_dict.get("product_id")
        if product_id is not None:
            try:
                if int(product_id) == 0:
                    product_id = None
            except (TypeError, ValueError):
                pass

        product_name = None
        for key in raw_dict.keys():
            key_lower = str(key).strip().lower()
            if key_lower in ["product_name", "product", "item"]:
                product_name = raw_dict[key]
                break

        if product_id is None and product_name:
            resolved_product_id = _resolve_product_id_from_name(cursor, product_name, user_id)
            if resolved_product_id is None:
                missing_products += 1
                continue
            product_id = resolved_product_id

        if product_id is None:
            missing_products += 1
            continue

        purchase_id = raw_dict.get("purchase_id")
        if not purchase_id or not _validate_purchase_ownership(cursor, purchase_id, user_id):
            missing_purchases += 1
            continue

        valid_rows += 1

    return {
        "valid_rows": valid_rows,
        "missing_products": missing_products,
        "missing_purchases": missing_purchases,
    }


@import_bp.route("/upload-dataset", methods=["POST"])
def upload_dataset():
    file = request.files.get("file")
    dataset = _normalize_dataset_name(request.form.get("dataset") or request.form.get("type"))
    if not file:
        return jsonify({"error": "file is required"}), 400

    try:
        sheets = _load_excel_sheets(file)
        selected_sheet = request.form.get("sheet") or (sheets[0] if sheets else None)
        df, actual_sheet, _ = _load_dataframe(file, sheet=selected_sheet)
        if dataset:
            print("Selected dataset:", dataset)
            print("Excel columns:", df.columns.tolist())
        return jsonify(
            {
                "filename": file.filename,
                "total_rows": len(df),
                "detected_columns": [str(column) for column in df.columns.tolist()],
                "detected_sheets": sheets,
                "sheet": actual_sheet or selected_sheet,
            }
        )
    except Exception as exc:
        logger.exception("upload_dataset failed")
        return jsonify({"error": str(exc)}), 400


@import_bp.route("/get-excel-sheets", methods=["POST"])
def get_excel_sheets():
    file = request.files.get("file")
    if not file:
        return jsonify({"sheets": []}), 400

    try:
        return jsonify({"sheets": _load_excel_sheets(file)})
    except Exception as exc:
        logger.exception("get_excel_sheets failed")
        return jsonify({"sheets": [], "error": str(exc)}), 400


@import_bp.route("/upload-preview", methods=["POST"])
def upload_preview():
    file = request.files.get("file")
    sheet = request.form.get("sheet")
    dataset = _normalize_dataset_name(request.form.get("dataset") or request.form.get("type"))

    if not file:
        return jsonify({"columns": [], "rows": [], "total_rows": 0}), 400

    conn = None
    try:
        df, actual_sheet, _ = _prepare_dataframe(file, sheet=sheet, dataset=dataset)
        if dataset:
            print("Selected dataset:", dataset)
            print("Excel columns:", df.columns.tolist())

        response = {
            "columns": [str(column) for column in df.columns.tolist()],
            "rows": _preview_rows(df, limit=10),
            "total_rows": len(df),
            "sheet": actual_sheet or sheet,
        }

        if dataset == "purchase_items":
            conn = _get_db()
            response["preview_summary"] = _purchase_items_preview_summary(df, conn, session["user_id"])

        return jsonify(response)
    except Exception as exc:
        logger.exception("upload_preview failed")
        return jsonify({"columns": [], "rows": [], "total_rows": 0, "error": str(exc)}), 400
    finally:
        if conn is not None:
            conn.close()


@import_bp.route("/validate-dataset", methods=["POST"])
def validate_dataset_route():
    file = request.files.get("file")
    sheet = request.form.get("sheet")
    dataset = _normalize_dataset_name(request.form.get("dataset") or request.form.get("type"))
    column_mapping = _parse_column_mapping(request.form.get("column_mapping"))
    user_id = session["user_id"]

    if not file or not dataset:
        return jsonify({"error": "file and dataset are required"}), 400

    conn = None
    try:
        conn = _get_db()
        table_columns = _get_table_columns(conn, dataset)
        required_columns = _get_required_columns(table_columns)
        df, actual_sheet, _ = _prepare_dataframe(
            file,
            sheet=sheet,
            column_mapping=column_mapping,
            dataset=dataset,
        )
        print("Selected dataset:", dataset)
        print("Excel columns:", df.columns.tolist())
        print("DB columns:", table_columns)
        report = validate_dataset(df, dataset, conn, user_id)
        report["db_columns"] = table_columns
        report["required_columns"] = required_columns
        report["sheet"] = actual_sheet or sheet
        return jsonify(report)
    except Exception as exc:
        logger.exception("validate_dataset failed")
        return jsonify({"error": str(exc)}), 400
    finally:
        if conn is not None:
            conn.close()


@import_bp.route("/execute-import", methods=["POST"])
@import_bp.route("/import-sheet", methods=["POST"])
def execute_import_route():
    file = request.files.get("file")
    sheet = request.form.get("sheet")
    dataset = _normalize_dataset_name(request.form.get("dataset") or request.form.get("type"))
    skip_invalid_raw = request.form.get("skip_invalid", "1")
    skip_invalid = str(skip_invalid_raw).lower() not in {"0", "false", "no"}
    column_mapping = _parse_column_mapping(request.form.get("column_mapping"))
    user_id = session["user_id"]

    if not file or not dataset:
        return jsonify({"inserted": 0, "skipped": 0, "errors": 1, "error_detail": [{"row": 0, "message": "file and dataset are required", "type": "missing", "code": "INVALID_REQUEST"}]}), 400

    conn = None
    try:
        conn = _get_db()
        table_columns = _get_table_columns(conn, dataset)
        print("Selected dataset:", dataset)
        df, actual_sheet, _ = _prepare_dataframe(
            file,
            sheet=sheet,
            column_mapping=column_mapping,
            dataset=dataset,
        )
        print("Excel columns:", df.columns.tolist())
        print("DB columns:", table_columns)
        result = execute_import(df, dataset, conn, user_id, skip_invalid=skip_invalid)
        result["sheet"] = actual_sheet or sheet
        return jsonify(result)
    except Exception as exc:
        logger.exception("execute_import failed")
        return jsonify(
            {
                "inserted": 0,
                "skipped": 0,
                "errors": 1,
                "error_detail": [
                    {
                        "row": 0,
                        "field": None,
                        "message": str(exc),
                        "type": "type",
                        "code": "IMPORT_EXECUTION_ERROR",
                    }
                ],
            }
        ), 400
    finally:
        if conn is not None:
            conn.close()
