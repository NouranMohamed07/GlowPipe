import csv
import random
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager


BASE_URL = "https://www.dermstore.com"
START_CATEGORY_URL = "https://www.dermstore.com/c/skin-care/"

MAX_PAGES = None
DELAY_MIN = 3
DELAY_MAX = 6


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    clean = parsed._replace(fragment="")
    return urlunparse(clean)


def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )

    return driver


def create_output_path():
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")

    output_dir = Path(
        f"data/raw/source=dermstore/"
        f"entity=product_urls/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    return output_dir / "product_urls.csv"


def save_urls_to_csv(urls, filepath):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_url"])

        for url in sorted(urls):
            writer.writerow([url])


def discover_product_urls():
    driver = create_driver()
    product_urls = set()

    page_url = START_CATEGORY_URL
    page_number = 1

    try:
        while page_url and (MAX_PAGES is None or page_number <= MAX_PAGES):
            print(f"\nOpening category page {page_number}: {page_url}")

            driver.get(page_url)
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            product_links = driver.find_elements(
                By.CSS_SELECTOR,
                "a.product-item"
            )

            print(f"Found product links on page: {len(product_links)}")

            before_count = len(product_urls)

            for link in product_links:
                href = link.get_attribute("href")

                if not href:
                    continue

                full_url = urljoin(BASE_URL, href)
                clean_url = normalize_url(full_url)

                if "/p/" in clean_url:
                    product_urls.add(clean_url)

            new_count = len(product_urls) - before_count

            print(f"New URLs from this page: {new_count}")
            print(f"Total unique product URLs so far: {len(product_urls)}")

            next_buttons = driver.find_elements(
                By.CSS_SELECTOR,
                "a.next-page-button[data-hasmore='true']"
            )

            if not next_buttons:
                print("No next page found.")
                break

            next_href = next_buttons[0].get_attribute("href")
            print(f"Next page href: {next_href}")

            if not next_href:
                print("Next button exists but href is empty.")
                break

            page_url = normalize_url(next_href)
            page_number += 1

    finally:
        driver.quit()

    output_path = create_output_path()
    save_urls_to_csv(product_urls, output_path)

    print("\nCSV saved:")
    print(output_path)

    return product_urls


if __name__ == "__main__":
    urls = discover_product_urls()
    print(f"\nFinal unique URLs: {len(urls)}")