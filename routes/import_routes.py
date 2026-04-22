import json
import logging
import sqlite3

import pandas as pd
from flask import Blueprint, current_app, jsonify, request, session

from services.import_executor import execute_import
from services.normalization_service import normalize_columns
from services.validation_service import validate_dataset


logger = logging.getLogger(__name__)

import_bp = Blueprint("import_bp", __name__)


def _get_db():
    db_path = current_app.config.get("DATABASE", "vyapaariq.db")
    conn = sqlite3.connect(db_path, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


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


def _load_dataframe(file_storage, sheet=None):
    if not file_storage or not file_storage.filename:
        raise ValueError("A file upload is required.")

    file_storage.stream.seek(0)
    lower_name = file_storage.filename.lower()

    if lower_name.endswith((".xlsx", ".xls")):
        return pd.read_excel(file_storage, sheet_name=sheet or 0)
    return pd.read_csv(file_storage)


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
    working_df = _load_dataframe(file_storage, sheet=sheet)
    working_df = _apply_column_mapping(working_df, column_mapping or {})
    if dataset:
        working_df = normalize_columns(working_df, dataset)
    return working_df


def _preview_rows(df, limit=10):
    preview = df.head(limit).astype(object)
    rows = []
    for _, row in preview.iterrows():
        rows.append([_serialize_value(value) for value in row.tolist()])
    return rows


@import_bp.route("/upload-dataset", methods=["POST"])
def upload_dataset():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "file is required"}), 400

    try:
        sheets = _load_excel_sheets(file)
        selected_sheet = request.form.get("sheet") or (sheets[0] if sheets else None)
        df = _load_dataframe(file, sheet=selected_sheet)
        return jsonify(
            {
                "filename": file.filename,
                "total_rows": len(df),
                "detected_columns": [str(column) for column in df.columns.tolist()],
                "detected_sheets": sheets,
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

    if not file:
        return jsonify({"columns": [], "rows": [], "total_rows": 0}), 400

    try:
        df = _load_dataframe(file, sheet=sheet)
        return jsonify(
            {
                "columns": [str(column) for column in df.columns.tolist()],
                "rows": _preview_rows(df, limit=10),
                "total_rows": len(df),
                "sheet": sheet,
            }
        )
    except Exception as exc:
        logger.exception("upload_preview failed")
        return jsonify({"columns": [], "rows": [], "total_rows": 0, "error": str(exc)}), 400


@import_bp.route("/validate-dataset", methods=["POST"])
def validate_dataset_route():
    file = request.files.get("file")
    sheet = request.form.get("sheet")
    dataset = request.form.get("dataset")
    column_mapping = _parse_column_mapping(request.form.get("column_mapping"))
    user_id = session["user_id"]

    if not file or not dataset:
        return jsonify({"error": "file and dataset are required"}), 400

    conn = None
    try:
        df = _prepare_dataframe(file, sheet=sheet, column_mapping=column_mapping, dataset=dataset)
        conn = _get_db()
        report = validate_dataset(df, dataset, conn, user_id)
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
    dataset = request.form.get("dataset")
    skip_invalid_raw = request.form.get("skip_invalid", "1")
    skip_invalid = str(skip_invalid_raw).lower() not in {"0", "false", "no"}
    column_mapping = _parse_column_mapping(request.form.get("column_mapping"))
    user_id = session["user_id"]

    if not file or not dataset:
        return jsonify({"inserted": 0, "skipped": 0, "errors": 1, "error_detail": [{"row": 0, "message": "file and dataset are required", "type": "missing", "code": "INVALID_REQUEST"}]}), 400

    conn = None
    try:
        df = _prepare_dataframe(file, sheet=sheet, column_mapping=column_mapping, dataset=dataset)
        conn = _get_db()
        result = execute_import(df, dataset, conn, user_id, skip_invalid=skip_invalid)
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
