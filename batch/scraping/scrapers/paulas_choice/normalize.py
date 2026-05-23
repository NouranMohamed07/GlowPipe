"""
Normalization logic for Paula's Choice ingredient data.

Important:
- Keep raw values exactly as extracted.
- Add normalized and canonical values in separate fields.
- Do not overwrite raw_ingredient_name.
"""

import re

from config import CANONICAL_NAME_MAP
from utils import clean_text


def normalize_ingredient_name(raw_name):
    """
    Create normalized ingredient name.

    Example:
    "  Vitamin B3 " -> "vitamin b3"
    "Aqua" -> "aqua"
    """

    if not raw_name:
        return None

    name = clean_text(raw_name)

    if not name:
        return None

    name = name.lower()
    name = re.sub(r"\s+", " ", name)
    name = name.strip()

    return name


def get_canonical_ingredient_name(normalized_name):
    """
    Map normalized ingredient name to canonical name.

    Example:
    aqua -> water
    vitamin b3 -> niacinamide

    If no mapping exists, return normalized name itself.
    """

    if not normalized_name:
        return None

    return CANONICAL_NAME_MAP.get(normalized_name, normalized_name)


def normalize_rating(rating):
    """
    Normalize rating text lightly.

    Example:
    "Best" -> "BEST"
    """

    if not rating:
        return None

    rating = clean_text(rating)

    if not rating:
        return None

    return rating.upper()


def normalize_parsed_ingredient(parsed_record):
    """
    Add normalized fields to parsed ingredient record.

    Keeps:
    - raw_ingredient_name

    Adds:
    - normalized_ingredient_name
    - canonical_ingredient_name
    """

    raw_name = parsed_record.get("raw_ingredient_name")

    normalized_name = normalize_ingredient_name(raw_name)
    canonical_name = get_canonical_ingredient_name(normalized_name)

    parsed_record["normalized_ingredient_name"] = normalized_name
    parsed_record["canonical_ingredient_name"] = canonical_name

    parsed_record["rating"] = normalize_rating(parsed_record.get("rating"))

    return parsed_record