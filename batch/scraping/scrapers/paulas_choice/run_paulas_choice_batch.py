"""
Batch runner for Paula's Choice Ingredient Dictionary scraper using Selenium.

Fast + checkpoint-safe version.

Flow:
1. Load ingredient URLs from CSV
2. Deduplicate URLs
3. Open each ingredient detail page with Selenium
4. Save raw HTML if enabled
5. Parse ingredient fields
6. Normalize fields
7. Validate record
8. Append each record immediately to CSV + JSONL
9. Append each log immediately to scrape_log.jsonl
10. Append failed URLs immediately to failed_urls.csv
"""

import csv
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config import (
    SOURCE_NAME,
    SOURCE_TYPE,
    SOURCE_URL,
    PIPELINE_VERSION,
    DATA_RAW_DIR,
    ENTITY_INGREDIENTS,
    RAW_HTML_DIR_NAME,
    SAVE_RAW_HTML,
    MAX_URLS,
    STATUS_FAILED_FETCH,
    STATUS_FAILED_PARSE,
)
from fetcher import FetchResult
from logger_config import setup_logger
from normalize import normalize_parsed_ingredient
from parse_ingredient import parse_ingredient_page
from utils import (
    create_output_paths,
    create_raw_html_dir,
    deduplicate_urls,
    generate_record_id,
    get_batch_id,
    get_collected_at,
    get_ingestion_date,
    safe_filename,
    save_text_file,
)
from validate import validate_ingredient_record, is_valid_for_saving
from writer import (
    append_csv_record,
    append_jsonl_record,
    append_failed_url_record,
    append_log_jsonl_record,
)


# =========================
# Update this path
# =========================

INPUT_URLS_CSV_PATH = r"D:\iti final project\data\raw\source=paulas_choice\entity=ingredient_urls\ingestion_date=2026-05-18\batch_id=20260518_111454\ingredient_urls.csv"

URL_COLUMN_NAME = "ingredient_url"


# =========================
# Speed settings
# =========================

SELENIUM_WAIT_SECONDS = 8
PAGE_DELAY_SECONDS = 0.5
PAGE_LOAD_TIMEOUT = 20

HEADLESS = True
BLOCK_IMAGES = True


def create_driver():
    """
    Create faster Selenium Chrome driver.
    """

    chrome_options = Options()

    if HEADLESS:
        chrome_options.add_argument("--headless=new")

    chrome_options.page_load_strategy = "eager"

    chrome_options.add_argument("--window-size=1400,1000")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--log-level=3")

    if BLOCK_IMAGES:
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
        }
        chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

    return driver


def fetch_page_with_selenium(driver, url, logger=None):
    """
    Fetch ingredient detail page using Selenium.
    """

    attempts = 1

    try:
        if logger:
            logger.info(f"Opening ingredient detail with Selenium: {url}")

        driver.get(url)

        wait = WebDriverWait(driver, SELENIUM_WAIT_SECONDS)

        try:
            wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "h1")
                )
            )
        except Exception:
            if logger:
                logger.warning("h1 did not appear before timeout. Continuing anyway.")

        time.sleep(PAGE_DELAY_SECONDS)

        html = driver.page_source

        if not html or len(html.strip()) == 0:
            return FetchResult(
                url=url,
                success=False,
                html=None,
                http_status=None,
                attempts=attempts,
                error_type="EmptyResponse",
                error_message="Selenium returned empty HTML.",
                collected_at=get_collected_at(),
            )

        return FetchResult(
            url=url,
            success=True,
            html=html,
            http_status=200,
            attempts=attempts,
            error_type=None,
            error_message=None,
            collected_at=get_collected_at(),
        )

    except Exception as exc:
        return FetchResult(
            url=url,
            success=False,
            html=None,
            http_status=None,
            attempts=attempts,
            error_type=type(exc).__name__,
            error_message=str(exc),
            collected_at=get_collected_at(),
        )


def load_ingredient_urls(csv_path, url_column_name=URL_COLUMN_NAME):
    """
    Load ingredient URLs from CSV file.
    """

    csv_path = Path(csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"Input URLs CSV file not found: {csv_path}")

    urls = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        if url_column_name not in reader.fieldnames:
            raise ValueError(
                f"Column '{url_column_name}' not found in CSV. "
                f"Available columns: {reader.fieldnames}"
            )

        for row in reader:
            url = row.get(url_column_name)

            if url:
                urls.append(url.strip())

    return urls


def build_log_record(
    ingredient_url,
    status,
    fetch_result=None,
    parsed_record=None,
    error_type=None,
    error_message=None,
):
    """
    Build one scrape log record.
    """

    return {
        "ingredient_url": ingredient_url,
        "status": status,
        "http_status": fetch_result.http_status if fetch_result else None,
        "attempts": fetch_result.attempts if fetch_result else None,
        "error_type": error_type or (fetch_result.error_type if fetch_result else None),
        "error_message": error_message or (fetch_result.error_message if fetch_result else None),
        "missing_fields": parsed_record.get("missing_fields") if parsed_record else None,
        "collected_at": fetch_result.collected_at if fetch_result else None,
    }


def build_failed_url_record(
    ingredient_url,
    fetch_result=None,
    error_type=None,
    error_message=None,
):
    """
    Build failed URL record for failed_urls.csv.
    """

    return {
        "ingredient_url": ingredient_url,
        "http_status": fetch_result.http_status if fetch_result else None,
        "attempts": fetch_result.attempts if fetch_result else None,
        "error_type": error_type or (fetch_result.error_type if fetch_result else None),
        "error_message": error_message or (fetch_result.error_message if fetch_result else None),
        "collected_at": fetch_result.collected_at if fetch_result else None,
    }


def scrape_one_ingredient(
    driver,
    ingredient_url,
    batch_id,
    ingestion_date,
    output_dir,
    raw_html_dir,
    logger,
):
    """
    Scrape one ingredient detail page.
    """

    logger.info(f"Started ingredient URL: {ingredient_url}")

    fetch_result = fetch_page_with_selenium(
        driver=driver,
        url=ingredient_url,
        logger=logger,
    )

    if not fetch_result.success:
        logger.error(
            f"Failed fetch | url={ingredient_url} | error={fetch_result.error_message}"
        )

        log_record = build_log_record(
            ingredient_url=ingredient_url,
            status=STATUS_FAILED_FETCH,
            fetch_result=fetch_result,
        )

        failed_record = build_failed_url_record(
            ingredient_url=ingredient_url,
            fetch_result=fetch_result,
        )

        return None, log_record, failed_record

    raw_file_path = None

    if SAVE_RAW_HTML:
        html_filename = safe_filename(ingredient_url) + ".html"
        raw_file_path = Path(raw_html_dir) / html_filename
        save_text_file(raw_file_path, fetch_result.html)

    try:
        parsed_record = parse_ingredient_page(
            html=fetch_result.html,
            ingredient_url=ingredient_url,
        )

        parsed_record = normalize_parsed_ingredient(parsed_record)

        record = {
            "record_id": generate_record_id(
                ingredient_url,
                prefix="paulas_choice_ingredient",
            ),
            "batch_id": batch_id,
            "source_name": SOURCE_NAME,
            "source_type": SOURCE_TYPE,
            "source_url": SOURCE_URL,
            "ingredient_url": ingredient_url,
            "ingestion_date": ingestion_date,
            "collected_at": fetch_result.collected_at,
            "pipeline_version": PIPELINE_VERSION,
            "raw_file_path": str(raw_file_path) if raw_file_path else None,
            **parsed_record,
        }

        record = validate_ingredient_record(record)

        if not is_valid_for_saving(record):
            raise ValueError("Record is not valid for saving.")

        log_record = build_log_record(
            ingredient_url=ingredient_url,
            status=record.get("extraction_status"),
            fetch_result=fetch_result,
            parsed_record=record,
        )

        logger.info(
            f"Finished ingredient URL | status={record.get('extraction_status')} "
            f"| name={record.get('raw_ingredient_name')} | url={ingredient_url}"
        )

        return record, log_record, None

    except Exception as exc:
        logger.exception(f"Failed parse | url={ingredient_url} | error={exc}")

        log_record = build_log_record(
            ingredient_url=ingredient_url,
            status=STATUS_FAILED_PARSE,
            fetch_result=fetch_result,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )

        failed_record = build_failed_url_record(
            ingredient_url=ingredient_url,
            fetch_result=fetch_result,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )

        return None, log_record, failed_record


def run_batch():
    """
    Run full Paula's Choice ingredient scraping batch.
    """

    logger = setup_logger()

    batch_id = get_batch_id()
    ingestion_date = get_ingestion_date()

    output_dir = create_output_paths(
        base_raw_dir=DATA_RAW_DIR,
        source_name=SOURCE_NAME,
        entity_name=ENTITY_INGREDIENTS,
        ingestion_date=ingestion_date,
        batch_id=batch_id,
    )

    raw_html_dir = create_raw_html_dir(
        output_dir=output_dir,
        raw_html_dir_name=RAW_HTML_DIR_NAME,
    )

    logger.info("=" * 80)
    logger.info("Paula's Choice ingredient scraping batch started with fast Selenium.")
    logger.info(f"Batch ID: {batch_id}")
    logger.info(f"Ingestion date: {ingestion_date}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Input URLs CSV: {INPUT_URLS_CSV_PATH}")

    urls = load_ingredient_urls(INPUT_URLS_CSV_PATH)
    urls = deduplicate_urls(urls)

    if MAX_URLS is not None:
        urls = urls[:MAX_URLS]

    logger.info(f"Total unique URLs to scrape: {len(urls)}")

    processed_count = 0
    saved_count = 0
    failed_count = 0

    driver = create_driver()

    try:
        for index, ingredient_url in enumerate(urls, start=1):
            logger.info("=" * 40)
            logger.info(f"Processing {index}/{len(urls)}")

            ingredient_record, log_record, failed_record = scrape_one_ingredient(
                driver=driver,
                ingredient_url=ingredient_url,
                batch_id=batch_id,
                ingestion_date=ingestion_date,
                output_dir=output_dir,
                raw_html_dir=raw_html_dir,
                logger=logger,
            )

            processed_count += 1

            if ingredient_record:
                append_csv_record(output_dir, ingredient_record)
                append_jsonl_record(output_dir, ingredient_record)
                saved_count += 1

            if log_record:
                append_log_jsonl_record(output_dir, log_record)

            if failed_record:
                append_failed_url_record(output_dir, failed_record)
                failed_count += 1

            print(
                f"Progress: {processed_count}/{len(urls)} | "
                f"saved={saved_count} | failed={failed_count}"
            )

    finally:
        driver.quit()

    logger.info("=" * 80)
    logger.info("Batch finished.")
    logger.info(f"Processed URLs: {processed_count}")
    logger.info(f"Saved records: {saved_count}")
    logger.info(f"Failed URLs: {failed_count}")
    logger.info(f"Output directory: {output_dir}")

    print("\nDONE ✅")
    print(f"Processed URLs: {processed_count}")
    print(f"Saved records: {saved_count}")
    print(f"Failed URLs: {failed_count}")
    print(f"Output directory:\n{output_dir}")


if __name__ == "__main__":
    run_batch()