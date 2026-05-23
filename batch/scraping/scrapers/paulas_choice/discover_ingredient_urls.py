"""
Discover Paula's Choice Ingredient Dictionary URLs using Selenium.

Why Selenium here?
- The dictionary list page appears to load ingredient rows with JavaScript.
- requests returned 0 ingredient URLs.
- Selenium is used only for URL discovery.
- Ingredient detail scraping can still use requests later.

Output:
data/raw/source=paulas_choice/entity=ingredient_urls/ingestion_date=YYYY-MM-DD/batch_id=YYYYMMDD_HHMMSS/
    - ingredient_urls.csv
    - ingredient_urls.jsonl
    - scrape_log.json
"""

import csv
import json
import time
from pathlib import Path
from urllib.parse import urlencode

from bs4 import BeautifulSoup

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
    ENTITY_URLS,
    INGREDIENT_URLS_CSV,
    INGREDIENT_URLS_JSONL,
    SCRAPE_LOG_JSON,
)
from logger_config import setup_logger
from utils import (
    clean_text,
    create_output_paths,
    deduplicate_urls,
    generate_record_id,
    get_batch_id,
    get_collected_at,
    get_ingestion_date,
    normalize_url,
    write_json_file,
)


# =========================
# Test settings
# =========================
# First test:
# PAGE_SIZE = 10
# MAX_PAGES = 1
# MAX_URLS_TO_DISCOVER = 1
#
# Full run later:
# PAGE_SIZE = 2000
# MAX_PAGES = 50
# MAX_URLS_TO_DISCOVER = None

PAGE_SIZE = 2000
MAX_PAGES = 50
MAX_URLS_TO_DISCOVER = None

SELENIUM_WAIT_SECONDS = 25
PAGE_DELAY_SECONDS = 3


URL_FIELDNAMES = [
    "record_id",
    "batch_id",
    "source_name",
    "source_type",
    "source_url",
    "ingredient_name_from_list",
    "rating_from_list",
    "short_description",
    "ingredient_url",
    "ingestion_date",
    "collected_at",
    "extraction_status",
    "pipeline_version",
]


def build_dictionary_page_url(start=0, size=PAGE_SIZE):
    """
    Build paginated Ingredient Dictionary URL.
    """

    params = {
        "csortb1": "ingredientNotRated",
        "csortd1": "1",
        "csortb2": "ingredientRating",
        "csortd2": "2",
        "csortb3": "name",
        "csortd3": "1",
        "start": start,
        "sz": size,
    }

    return f"{SOURCE_URL}?{urlencode(params)}"


def create_driver():
    """
    Create Selenium Chrome driver.
    """

    chrome_options = Options()

    # Keep browser visible for debugging.
    # After it works, you can uncomment headless mode.
    # chrome_options.add_argument("--headless=new")

    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    return driver


def fetch_page_with_selenium(driver, url, logger):
    """
    Open page with Selenium and wait until ingredient links appear.
    """

    logger.info(f"Opening URL with Selenium: {url}")

    driver.get(url)

    wait = WebDriverWait(driver, SELENIUM_WAIT_SECONDS)

    try:
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'a[href*="/ingredient-dictionary/ingredient-"]')
            )
        )
    except Exception:
        logger.warning("Ingredient links did not appear before timeout.")

    time.sleep(PAGE_DELAY_SECONDS)

    html = driver.page_source

    return html


def parse_ingredient_rows(html, batch_id, ingestion_date):
    """
    Parse ingredient rows from dictionary list page.

    The visible text is:
        <span>Read More</span>

    But the real URL is in:
        <a href="/ingredient-dictionary/ingredient-name.html?fdid=ingredients">

    So we search for the <a href> directly.
    """

    soup = BeautifulSoup(html, "html.parser")
    records = []

    links = soup.select('a[href*="/ingredient-dictionary/ingredient-"]')

    print("DEBUG - HTML length:", len(html))
    print("DEBUG - ingredient detail links found:", len(links))

    for link in links:
        href = link.get("href")

        if not href:
            continue

        ingredient_url = normalize_url(href)

        if not ingredient_url:
            continue

        row = link.find_parent("tr")

        ingredient_name = None
        rating = None
        short_description = None

        if row:
            h3 = row.find("h3")
            if h3:
                ingredient_name = clean_text(h3.get_text(" ", strip=True))

            rating_element = row.select_one('[class*="ColoredIngredientRating"]')
            if rating_element:
                rating = clean_text(rating_element.get_text(" ", strip=True))

            description_element = row.select_one('[class*="IngredientList__Description"]')
            if description_element:
                short_description = clean_text(
                    description_element.get_text(" ", strip=True)
                )

        if not ingredient_name:
            ingredient_name = clean_text(link.get_text(" ", strip=True))

        if ingredient_name and ingredient_name.lower() == "read more":
            ingredient_name = None

        record = {
            "record_id": generate_record_id(
                ingredient_url,
                prefix="paulas_choice_ingredient_url",
            ),
            "batch_id": batch_id,
            "source_name": SOURCE_NAME,
            "source_type": SOURCE_TYPE,
            "source_url": SOURCE_URL,
            "ingredient_name_from_list": ingredient_name,
            "rating_from_list": rating,
            "short_description": short_description,
            "ingredient_url": ingredient_url,
            "ingestion_date": ingestion_date,
            "collected_at": get_collected_at(),
            "extraction_status": "success",
            "pipeline_version": PIPELINE_VERSION,
        }

        records.append(record)

    return records


def write_urls_csv(output_dir, records):
    """
    Save ingredient URL records to CSV.
    """

    output_path = Path(output_dir) / INGREDIENT_URLS_CSV

    with open(output_path, "w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=URL_FIELDNAMES,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(records)

    return output_path


def write_urls_jsonl(output_dir, records):
    """
    Save ingredient URL records to JSONL.
    """

    output_path = Path(output_dir) / INGREDIENT_URLS_JSONL

    with open(output_path, "w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")

    return output_path


def deduplicate_records_by_url(records):
    """
    Deduplicate records by ingredient_url while preserving order.
    """

    unique_records = []
    seen_urls = set()

    for record in records:
        ingredient_url = record.get("ingredient_url")

        if not ingredient_url:
            continue

        if ingredient_url not in seen_urls:
            unique_records.append(record)
            seen_urls.add(ingredient_url)

    return unique_records


def discover_ingredient_urls():
    """
    Run ingredient URL discovery batch.
    """

    logger = setup_logger(
        logger_name="paulas_choice_url_discovery",
        log_file_name="discover_urls.log",
    )

    batch_id = get_batch_id()
    ingestion_date = get_ingestion_date()

    output_dir = create_output_paths(
        base_raw_dir=DATA_RAW_DIR,
        source_name=SOURCE_NAME,
        entity_name=ENTITY_URLS,
        ingestion_date=ingestion_date,
        batch_id=batch_id,
    )

    logger.info("=" * 80)
    logger.info("Paula's Choice ingredient URL discovery started with Selenium.")
    logger.info(f"Batch ID: {batch_id}")
    logger.info(f"Output directory: {output_dir}")

    all_records = []
    scrape_log = []

    driver = create_driver()

    try:
        for page_number in range(MAX_PAGES):
            start = page_number * PAGE_SIZE
            page_url = build_dictionary_page_url(start=start, size=PAGE_SIZE)

            logger.info(f"Fetching dictionary page {page_number + 1} | start={start}")
            logger.info(f"URL: {page_url}")

            try:
                html = fetch_page_with_selenium(
                    driver=driver,
                    url=page_url,
                    logger=logger,
                )

                page_records = parse_ingredient_rows(
                    html=html,
                    batch_id=batch_id,
                    ingestion_date=ingestion_date,
                )

                log_record = {
                    "page_url": page_url,
                    "start": start,
                    "page_size": PAGE_SIZE,
                    "success": True,
                    "records_found": len(page_records),
                    "error_type": None,
                    "error_message": None,
                    "collected_at": get_collected_at(),
                }

                scrape_log.append(log_record)

                logger.info(f"Records found on page: {len(page_records)}")

                if not page_records:
                    logger.warning("No records found on this page.")
                    break

                before_count = len(all_records)
                all_records.extend(page_records)
                all_records = deduplicate_records_by_url(all_records)
                after_count = len(all_records)

                new_records_count = after_count - before_count
                logger.info(f"New unique records added: {new_records_count}")

                if new_records_count == 0:
                    logger.info("No new unique URLs found. Stopping pagination.")
                    break

            except Exception as exc:
                logger.exception(f"Failed page discovery | url={page_url} | error={exc}")

                scrape_log.append(
                    {
                        "page_url": page_url,
                        "start": start,
                        "page_size": PAGE_SIZE,
                        "success": False,
                        "records_found": 0,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "collected_at": get_collected_at(),
                    }
                )

                break

    finally:
        driver.quit()

    all_records = deduplicate_records_by_url(all_records)

    if MAX_URLS_TO_DISCOVER is not None:
        all_records = all_records[:MAX_URLS_TO_DISCOVER]

    urls = [record["ingredient_url"] for record in all_records]
    unique_urls = deduplicate_urls(urls)

    logger.info(f"Total discovered records before final URL check: {len(all_records)}")
    logger.info(f"Total unique URLs: {len(unique_urls)}")

    csv_path = write_urls_csv(output_dir, all_records)
    jsonl_path = write_urls_jsonl(output_dir, all_records)
    log_path = Path(output_dir) / SCRAPE_LOG_JSON
    write_json_file(log_path, scrape_log)

    logger.info("=" * 80)
    logger.info("Ingredient URL discovery finished.")
    logger.info(f"ingredient_urls.csv: {csv_path}")
    logger.info(f"ingredient_urls.jsonl: {jsonl_path}")
    logger.info(f"scrape_log.json: {log_path}")

    print("\nDONE ✅")
    print(f"Records saved: {len(all_records)}")
    print(f"Ingredient URLs CSV:\n{csv_path}")

    if all_records:
        print("\nSample discovered URL:")
        print(all_records[0]["ingredient_url"])

    print("\nCopy this path into run_paulas_choice_batch.py:")
    print(f'INPUT_URLS_CSV_PATH = r"{csv_path}"')


if __name__ == "__main__":
    discover_ingredient_urls()