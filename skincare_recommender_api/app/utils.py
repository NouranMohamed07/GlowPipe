import pandas as pd
from typing import List

BADGE_MAP = {
    "vegan": "Vegan",
    "cruelty_free": "Cruelty Free",
    "reef_safe": "Reef Safe",
    "fragrance_free": "Fragrance Free",
    "alcohol_free": "Alcohol Free",
    "paraben_free": "Paraben Free",
    "sulfate_free": "Sulfate Free",
    "silicone_free": "Silicone Free",
    "oil_free": "Oil Free",
    "pregnancy_safe": "Pregnancy Safe",
    "fungal_acne_safe": "Fungal Acne Safe",
}

RISK_KEYWORDS = ["fragrance", "parfum", "alcohol", "limonene", "linalool"]


def extract_badges(row: pd.Series) -> List[str]:
    badges = []
    for col, label in BADGE_MAP.items():
        if col in row and _is_true(row[col]):
            badges.append(label)
    return badges


def _is_true(val) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val == 1
    if isinstance(val, str):
        return val.lower() in ("1", "true", "yes")
    return False


def build_reason(
    skin_type: str,
    acne_prone: bool,
    matched_prefs: List[str],
    rating: float,
    comedogenic_rating: float,
    price: float,
    max_budget: float,
) -> str:
    parts = [f"matches {skin_type} skin"]

    if acne_prone:
        parts.append("is acne-prone friendly")

    if matched_prefs:
        if len(matched_prefs) == 1:
            parts.append(f"is {matched_prefs[0].lower()}")
        elif len(matched_prefs) <= 3:
            parts.append(f"is {', '.join(m.lower() for m in matched_prefs)}")
        else:
            parts.append(f"matches {len(matched_prefs)} of your preferences")

    if rating and rating >= 4.5:
        parts.append("has excellent reviews")
    elif rating and rating >= 4.0:
        parts.append("has strong reviews")

    if comedogenic_rating is not None and comedogenic_rating <= 1:
        parts.append("has very low comedogenic risk")
    elif comedogenic_rating is not None and comedogenic_rating <= 2:
        parts.append("has low comedogenic risk")

    if price and max_budget and (price / max_budget) <= 0.5:
        parts.append("fits comfortably within your budget")

    if not parts:
        return "A quality product that matches your profile."

    return "Recommended because it " + ", ".join(parts) + "."


def check_ingredient_warnings(ingredient_name: str) -> List[str]:
    name_lower = ingredient_name.lower()
    warnings = []
    for kw in RISK_KEYWORDS:
        if kw in name_lower:
            warnings.append(kw.title())
    return warnings


def safe_float(val, default=None):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return default
        return float(val)
    except Exception:
        return default


def safe_str(val, default=None):
    if val is None:
        return default
    if isinstance(val, float) and pd.isna(val):
        return default
    return str(val)