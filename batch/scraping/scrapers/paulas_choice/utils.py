"""
Utility functions for Paula's Choice Ingredient Dictionary scraper.

This file contains helper functions used across the scraper:
- datetime helpers
- folder creation
- URL cleaning
- hashing record IDs
- safe text cleaning
- JSON serialization helpers
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urldefrag

from config import BASE_URL


def get_utc_now():
    """
    Return current UTC datetime object.
    """
    return datetime.now(timezone.utc)


def get_ingestion_date():
    """
    Return ingestion date in YYYY-MM-DD format.
    """
    return get_utc_now().strftime("%Y-%m-%d")


def get_batch_id():
    """
    Return batch ID in YYYYMMDD_HHMMSS format.
    """
    return get_utc_now().strftime("%Y%m%d_%H%M%S")


def get_collected_at():
    """
    Return current UTC timestamp in ISO format.
    """
    return get_utc_now().isoformat()


def create_directory(path):
    """
    Create a directory if it does not exist.
    """
    Path(path).mkdir(parents=True, exist_ok=True)


def create_output_paths(base_raw_dir, source_name, entity_name, ingestion_date, batch_id):
    """
    Create standard output folder structure.

    Example:
    data/raw/source=paulas_choice/entity=ingredients/
    ingestion_date=2026-05-18/batch_id=20260518_120000/
    """

    output_dir = (
        Path(base_raw_dir)
        / f"source={source_name}"
        / f"entity={entity_name}"
        / f"ingestion_date={ingestion_date}"
        / f"batch_id={batch_id}"
    )

    create_directory(output_dir)

    return output_dir


def create_raw_html_dir(output_dir, raw_html_dir_name="raw_html"):
    """
    Create raw_html directory inside batch output folder.
    """
    raw_html_dir = Path(output_dir) / raw_html_dir_name
    create_directory(raw_html_dir)
    return raw_html_dir


def normalize_url(url):
    """
    Convert relative URLs to absolute URLs and remove URL fragments.

    Example:
    /ingredient-dictionary/ingredient-acai.html?fdid=ingredients
    becomes:
    https://www.paulaschoice.com/ingredient-dictionary/ingredient-acai.html?fdid=ingredients
    """

    if not url:
        return None

    url = url.strip()
    absolute_url = urljoin(BASE_URL, url)
    clean_url, _ = urldefrag(absolute_url)

    return clean_url


def clean_text(text):
    """
    Light text cleaning:
    - Keep original meaning
    - Remove extra spaces
    - Do not over-clean raw scraped values
    """

    if text is None:
        return None

    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    text = text.strip()

    return text if text else None


def clean_list(values):
    """
    Clean list of text values and remove empty values.
    Keeps order and removes duplicates.
    """

    if not values:
        return None

    cleaned = []
    seen = set()

    for value in values:
        value = clean_text(value)

        if not value:
            continue

        key = value.lower()

        if key not in seen:
            cleaned.append(value)
            seen.add(key)

    return cleaned if cleaned else None


def generate_record_id(value, prefix="paulas_choice"):
    """
    Generate deterministic record ID from a stable value, usually URL.
    Same URL will always produce the same ID.
    """

    if not value:
        value = get_collected_at()

    hash_value = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]

    return f"{prefix}_{hash_value}"


def safe_filename(value, max_length=120):
    """
    Create safe filename from ingredient name or URL.
    """

    if not value:
        return "unknown"

    value = value.lower().strip()
    value = re.sub(r"https?://", "", value)
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = value.strip("_")

    if len(value) > max_length:
        value = value[:max_length].rstrip("_")

    return value or "unknown"


def save_text_file(path, content):
    """
    Save text content to file using UTF-8 encoding.
    """

    path = Path(path)
    create_directory(path.parent)

    with open(path, "w", encoding="utf-8") as file:
        file.write(content)


def read_text_file(path):
    """
    Read text file using UTF-8 encoding.
    """

    with open(path, "r", encoding="utf-8") as file:
        return file.read()


def json_dumps_safe(data):
    """
    Convert Python object to JSON string safely.
    Used for CSV cells that contain lists.
    """

    if data is None:
        return None

    return json.dumps(data, ensure_ascii=False)


def write_json_file(path, data):
    """
    Write Python object to JSON file.
    """

    path = Path(path)
    create_directory(path.parent)

    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def append_jsonl(path, record):
    """
    Append one record to JSONL file.
    """

    path = Path(path)
    create_directory(path.parent)

    with open(path, "a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")


def deduplicate_urls(urls):
    """
    Deduplicate URLs while preserving order.
    """

    unique_urls = []
    seen = set()

    for url in urls:
        clean_url_value = normalize_url(url)

        if not clean_url_value:
            continue

        if clean_url_value not in seen:
            unique_urls.append(clean_url_value)
            seen.add(clean_url_value)

    return unique_urls