import logging

from services.normalization_service import REQUIRED_FIELDS, normalize_columns, normalize_row


logger = logging.getLogger(__name__)


def validate_dataset(df, dataset, conn, user_id):
    working_df = normalize_columns(df.copy(), dataset)
    total_rows = len(working_df)
    missing_required_columns = sorted(set(REQUIRED_FIELDS.get(dataset, set())) - set(working_df.columns))

    errors = []
    missing_values = 0
    lookup_errors = 0
    type_errors = 0
    valid_rows = 0

    for column in missing_required_columns:
        errors.append(
            {
                "row": 0,
                "field": column,
                "message": f"Required column {column!r} is missing from dataset {dataset}",
                "type": "missing",
                "code": "MISSING_REQUIRED_COLUMN",
            }
        )
        missing_values += total_rows if total_rows else 1

    for index, raw_row in working_df.iterrows():
        row_number = int(index) + 2
        _, row_errors = normalize_row(raw_row, dataset, conn, user_id)

        if row_errors or missing_required_columns:
            for err in row_errors:
                detailed_error = {
                    "row": row_number,
                    "field": err.get("field"),
                    "message": err.get("message"),
                    "type": err.get("type"),
                    "code": err.get("code"),
                }
                errors.append(detailed_error)

                if err.get("type") == "lookup":
                    lookup_errors += 1
                elif err.get("type") == "type":
                    type_errors += 1
                else:
                    missing_values += 1
            continue

        valid_rows += 1

    if missing_required_columns:
        valid_rows = 0

    logger.info(
        "validate_dataset(%s): total=%s valid=%s missing=%s lookup=%s type=%s",
        dataset,
        total_rows,
        valid_rows,
        missing_values,
        lookup_errors,
        type_errors,
    )

    return {
        "total": total_rows,
        "valid": valid_rows,
        "missing_values": missing_values,
        "lookup_errors": lookup_errors,
        "type_errors": type_errors,
        "missing_required_columns": missing_required_columns,
        "errors": errors,
    }
