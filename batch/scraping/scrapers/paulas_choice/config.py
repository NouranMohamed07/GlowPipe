"""
Configuration file for Paula's Choice Ingredient Dictionary scraper.

This file contains only constants and settings.
No scraping logic should be written here.
"""

from pathlib import Path


# =========================
# Source Information
# =========================

SOURCE_NAME = "paulas_choice"
SOURCE_TYPE = "website"

SOURCE_URL = "https://www.paulaschoice.com/ingredient-dictionary"
BASE_URL = "https://www.paulaschoice.com"

ENTITY_URLS = "ingredient_urls"
ENTITY_INGREDIENTS = "ingredients"

PIPELINE_VERSION = "v1"


# =========================
# Project Paths
# =========================

# This assumes you run the script from the project root:
# D:\iti final project
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
LOGS_DIR = PROJECT_ROOT / "logs" / SOURCE_NAME

SOURCE_RAW_DIR = DATA_RAW_DIR / f"source={SOURCE_NAME}"


# =========================
# Request Settings
# =========================

REQUEST_TIMEOUT = 20

MAX_RETRIES = 3

DELAY_MIN = 2
DELAY_MAX = 6

BACKOFF_FACTOR = 2

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# =========================
# Scraping Behavior
# =========================

SAVE_RAW_HTML = False

STOP_ON_ERROR = False

RESPECTFUL_SCRAPING = True

MAX_URLS = None
# Use None to scrape all URLs.
# Use a number like 10 for testing.


# =========================
# Output File Names
# =========================

INGREDIENT_URLS_CSV = "ingredient_urls.csv"
INGREDIENT_URLS_JSONL = "ingredient_urls.jsonl"

INGREDIENTS_CSV = "ingredients.csv"
INGREDIENTS_JSONL = "ingredients.jsonl"

SCRAPE_LOG_JSON = "scrape_log.json"
FAILED_URLS_CSV = "failed_urls.csv"

RAW_HTML_DIR_NAME = "raw_html"


# =========================
# Extraction Status Values
# =========================

STATUS_SUCCESS = "success"
STATUS_PARTIAL = "partial"
STATUS_FAILED_FETCH = "failed_fetch"
STATUS_FAILED_PARSE = "failed_parse"
STATUS_SKIPPED_DUPLICATE = "skipped_duplicate"


# =========================
# Required / Optional Fields
# =========================

REQUIRED_INGREDIENT_FIELDS = [
    "ingredient_url",
    "raw_ingredient_name",
    "full_description",
]

OPTIONAL_INGREDIENT_FIELDS = [
    "rating",
    "benefits",
    "ingredient_categories",
    "at_a_glance_points",
    "synonyms",
    "concerns",
    "safety_notes",
]


# =========================
# Canonical Ingredient Name Mapping
# =========================

CANONICAL_NAME_MAP = {
    "aqua": "water",
    "water": "water",
    "vitamin b3": "niacinamide",
    "niacinamide": "niacinamide",
}