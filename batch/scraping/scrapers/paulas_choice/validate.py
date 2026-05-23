"""
Validation logic for Paula's Choice ingredient records.

Responsibilities:
- Detect missing fields
- Decide extraction_status
- Keep missing optional fields visible
- Never crash because optional fields are missing
"""

from config import (
    REQUIRED_INGREDIENT_FIELDS,
    OPTIONAL_INGREDIENT_FIELDS,
    STATUS_SUCCESS,
    STATUS_PARTIAL,
)


def is_missing(value):
    """
    Check if a field value should be considered missing.
    """

    if value is None:
        return True

    if isinstance(value, str) and value.strip() == "":
        return True

    if isinstance(value, list) and len(value) == 0:
        return True

    return False


def get_missing_fields(record, fields):
    """
    Return list of missing fields from a record.
    """

    missing_fields = []

    for field in fields:
        if is_missing(record.get(field)):
            missing_fields.append(field)

    return missing_fields


def validate_ingredient_record(record):
    """
    Validate parsed ingredient record.

    Rules:
    - If required fields are present -> success
    - If one or more required fields are missing -> partial
    - Optional missing fields are still listed in missing_fields
    """

    missing_required = get_missing_fields(
        record=record,
        fields=REQUIRED_INGREDIENT_FIELDS,
    )

    missing_optional = get_missing_fields(
        record=record,
        fields=OPTIONAL_INGREDIENT_FIELDS,
    )

    all_missing_fields = missing_required + missing_optional

    if missing_required:
        extraction_status = STATUS_PARTIAL
    else:
        extraction_status = STATUS_SUCCESS

    record["extraction_status"] = extraction_status
    record["missing_fields"] = all_missing_fields

    return record


def is_valid_for_saving(record):
    """
    Decide whether a record should be saved to ingredients output.

    We save both success and partial records because partial data is still useful.
    Failed fetch/parse records are handled separately in logs.
    """

    if not record:
        return False

    if is_missing(record.get("ingredient_url")):
        return False

    return True