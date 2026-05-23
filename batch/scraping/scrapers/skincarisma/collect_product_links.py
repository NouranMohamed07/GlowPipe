# save as: scrapers/skincarisma/collect_product_links.py

import os
import re
import time
import random
import logging
import hashlib
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup


BASE_URL = "https://www.skincarisma.com"
COLLECTION_URL = "https://www.skincarisma.com/collections/all"
SECTION_ID = "template--28199137935703__product-grid"

OUTPUT_DIR = "data/raw/source=skincarisma"
LOG_DIR = "logs"

PRODUCT_LINKS_CSV = os.path.join(OUTPUT_DIR, "product_links.csv")
LOG_FILE = os.path.join(LOG_DIR, "skincarisma_scraper.log")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

HEADERS = {
    "accept": "*/*",
    "referer": COLLECTION_URL,
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    )
}


def clean_text(value):
    if not value:
        return None
    return re.sub(r"\s+", " ", value).strip()


def fix_url(url):
    if not url:
        return None
    if url.startswith("//"):
        return "https:" + url
    return urljoin(BASE_URL, url)


def make_product_id(product_url):
    return hashlib.md5(product_url.encode("utf-8")).hexdigest()


def fetch_page(page, retries=3):
    params = {
        "page": page,
        "section_id": SECTION_ID
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(
                COLLECTION_URL,
                params=params,
                headers=HEADERS,
                timeout=30
            )

            if response.status_code == 200:
                return response.text

            logging.warning(f"Page {page}: status code {response.status_code}")

        except Exception as e:
            logging.warning(f"Page {page}: attempt {attempt} failed: {e}")

        time.sleep(2 * attempt)

    return None


def parse_product_cards(html):
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.f-column.card")

    products = []

    for card in cards:
        title_tag = card.select_one(".product-card__title a.reversed-link")
        if not title_tag:
            continue

        relative_url = title_tag.get("href")
        product_url = fix_url(relative_url)

        brand = clean_text(card.select_one(".product-card__vendor").get_text(" ", strip=True)) if card.select_one(".product-card__vendor") else None
        category = clean_text(card.select_one(".product-card__type").get_text(" ", strip=True)) if card.select_one(".product-card__type") else None
        name = clean_text(title_tag.get_text(" ", strip=True))

        # remove visible labels added by hidden spans
        if brand:
            brand = brand.replace("Vendor:", "").strip()
        if category:
            category = category.replace("Type:", "").strip() or None

        price_tag = card.select_one(".f-price__regular .f-price-item--regular")
        price = clean_text(price_tag.get_text(" ", strip=True)) if price_tag else None

        img = card.select_one(".product-card__image img")
        image_url = fix_url(img.get("src")) if img else None
        image_alt = clean_text(img.get("alt")) if img else None

        product_id = make_product_id(product_url)

        products.append({
            "product_id": product_id,
            "product_url": product_url,
            "product_name": name,
            "brand_name": brand,
            "category": category,
            "price": price,
            "image_url": image_url,
            "image_alt": image_alt,
            "source": "skincarisma"
        })

    return products


def load_existing():
    if os.path.exists(PRODUCT_LINKS_CSV):
        return pd.read_csv(PRODUCT_LINKS_CSV)
    return pd.DataFrame()


def save_progress(all_products):
    df = pd.DataFrame(all_products)
    df = df.drop_duplicates(subset=["product_url"])
    df.to_csv(PRODUCT_LINKS_CSV, index=False, encoding="utf-8-sig")
    return df


def get_product_links(start_page=1, max_pages=2000, delay_min=1.0, delay_max=2.5):
    existing_df = load_existing()
    all_products = existing_df.to_dict("records") if not existing_df.empty else []
    seen_urls = set(existing_df["product_url"].dropna()) if not existing_df.empty else set()

    empty_pages = 0

    for page in range(start_page, max_pages + 1):
        logging.info(f"Scraping collection page {page}")
        print(f"Scraping page {page}...")

        html = fetch_page(page)
        if not html:
            logging.warning(f"No HTML returned for page {page}")
            empty_pages += 1
            if empty_pages >= 3:
                break
            continue

        products = parse_product_cards(html)

        if not products:
            logging.info(f"No products found on page {page}")
            empty_pages += 1
            if empty_pages >= 3:
                print("Stopped: 3 empty pages in a row.")
                break
            continue

        empty_pages = 0
        new_count = 0

        for product in products:
            if product["product_url"] not in seen_urls:
                all_products.append(product)
                seen_urls.add(product["product_url"])
                new_count += 1

        df = save_progress(all_products)

        print(f"Page {page}: found {len(products)}, new {new_count}, total unique {len(df)}")
        logging.info(f"Page {page}: found {len(products)}, new {new_count}, total unique {len(df)}")

        if new_count == 0 and page > 5:
            logging.info(f"No new products on page {page}")

        time.sleep(random.uniform(delay_min, delay_max))

    return save_progress(all_products)


def main():
    df = get_product_links(start_page=1)
    print(f"Done. Saved {len(df)} products to {PRODUCT_LINKS_CSV}")


if __name__ == "__main__":
    main()