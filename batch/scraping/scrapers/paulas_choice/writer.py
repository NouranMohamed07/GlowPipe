"""
Writer functions for Paula's Choice ingredient scraper.

Responsibilities:
- Save ingredients.csv
- Save ingredients.jsonl
- Save scrape_log.json
- Save failed_urls.csv
- Append records immediately for checkpoint-safe scraping
- Convert list fields safely for CSV
"""

import csv
import json
from pathlib import Path

from config import (
    INGREDIENTS_CSV,
    INGREDIENTS_JSONL,
    SCRAPE_LOG_JSON,
    FAILED_URLS_CSV,
)
from utils import create_directory, json_dumps_safe, write_json_file


INGREDIENT_FIELDNAMES = [
    # Metadata
    "record_id",
    "batch_id",
    "source_name",
    "source_type",
    "source_url",
    "ingredient_url",
    "ingestion_date",
    "collected_at",
    "extraction_status",
    "missing_fields",
    "pipeline_version",
    "raw_file_path",

    # Ingredient fields
    "raw_ingredient_name",
    "normalized_ingredient_name",
    "canonical_ingredient_name",
    "rating",
    "benefits",
    "ingredient_categories",
    "at_a_glance_points",
    "full_description",
    "synonyms",
    "concerns",
    "safety_notes",

    # Extra fields from page
    "related_ingredients",

]


FAILED_URL_FIELDNAMES = [
    "ingredient_url",
    "http_status",
    "attempts",
    "error_type",
    "error_message",
    "collected_at",
]


def prepare_record_for_csv(record):
    """
    Convert list/dict values into JSON strings for CSV.
    Keep normal strings as they are.
    """

    csv_record = {}

    for key in INGREDIENT_FIELDNAMES:
        value = record.get(key)

        if isinstance(value, (list, dict)):
            csv_record[key] = json_dumps_safe(value)
        else:
            csv_record[key] = value

    return csv_record


# ==========================================================
# Full-file writers
# These overwrite the whole output file.
# Useful only when saving everything at the end.
# ==========================================================

def write_csv(output_dir, records, filename=INGREDIENTS_CSV):
    """
    Write ingredient records to CSV.
    This overwrites the file.
    """

    output_path = Path(output_dir) / filename
    create_directory(output_path.parent)

    with open(output_path, "w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=INGREDIENT_FIELDNAMES,
            extrasaction="ignore",
        )

        writer.writeheader()

        for record in records:
            writer.writerow(prepare_record_for_csv(record))

    return output_path


def write_jsonl(output_dir, records, filename=INGREDIENTS_JSONL):
    """
    Write ingredient records to JSONL.
    This overwrites the file.
    """

    output_path = Path(output_dir) / filename
    create_directory(output_path.parent)

    with open(output_path, "w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")

    return output_path


def write_scrape_log(output_dir, log_records, filename=SCRAPE_LOG_JSON):
    """
    Write scrape log as JSON.
    This overwrites the file.
    """

    output_path = Path(output_dir) / filename
    write_json_file(output_path, log_records)

    return output_path


def write_failed_urls(output_dir, failed_records, filename=FAILED_URLS_CSV):
    """
    Write failed URLs to CSV.
    This overwrites the file.
    """

    output_path = Path(output_dir) / filename
    create_directory(output_path.parent)

    with open(output_path, "w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=FAILED_URL_FIELDNAMES,
            extrasaction="ignore",
        )

        writer.writeheader()

        for record in failed_records:
            writer.writerow(record)

    return output_path


def save_all_outputs(output_dir, ingredient_records, scrape_log_records, failed_url_records):
    """
    Save all final output files.
    This overwrites files at the end of the run.
    Do not use this if you are using append/checkpoint mode.
    """

    paths = {
        "ingredients_csv": write_csv(output_dir, ingredient_records),
        "ingredients_jsonl": write_jsonl(output_dir, ingredient_records),
        "scrape_log_json": write_scrape_log(output_dir, scrape_log_records),
        "failed_urls_csv": write_failed_urls(output_dir, failed_url_records),
    }

    return paths


# ==========================================================
# Append writers
# These save immediately after each URL.
# Best for long scraping runs.
# ==========================================================

def append_csv_record(output_dir, record, filename=INGREDIENTS_CSV):
    """
    Append one ingredient record to CSV immediately.
    Creates the CSV with header if it does not exist.

    This is checkpoint-safe:
    if the run stops, already scraped records stay saved.
    """

    output_path = Path(output_dir) / filename
    create_directory(output_path.parent)

    file_exists = output_path.exists() and output_path.stat().st_size > 0

    with open(output_path, "a", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=INGREDIENT_FIELDNAMES,
            extrasaction="ignore",
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow(prepare_record_for_csv(record))

    return output_path


def append_jsonl_record(output_dir, record, filename=INGREDIENTS_JSONL):
    """
    Append one ingredient record to JSONL immediately.
    """

    output_path = Path(output_dir) / filename
    create_directory(output_path.parent)

    with open(output_path, "a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")

    return output_path


def append_failed_url_record(output_dir, record, filename=FAILED_URLS_CSV):
    """
    Append one failed URL record to failed_urls.csv immediately.
    Creates file with header if it does not exist.
    """

    output_path = Path(output_dir) / filename
    create_directory(output_path.parent)

    file_exists = output_path.exists() and output_path.stat().st_size > 0

    with open(output_path, "a", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=FAILED_URL_FIELDNAMES,
            extrasaction="ignore",
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow(record)

    return output_path


def append_log_jsonl_record(output_dir, record, filename="scrape_log.jsonl"):
    """
    Append one scrape log record immediately as JSONL.

    JSONL is safer than scrape_log.json for long scraping,
    because each line is saved independently.
    """

    output_path = Path(output_dir) / filename
    create_directory(output_path.parent)

    with open(output_path, "a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")

    return output_path