import json
import re
import time
from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


TEST_PRODUCT_URL = "https://www.dermstore.com/p/la-roche-posay-toleriane-double-repair-moisturiser-100ml/11429064/"


def clean_text(value):
    if not value:
        return None
    value = " ".join(value.split())
    return value if value else None


def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )


def get_text(soup, selector):
    element = soup.select_one(selector)
    return clean_text(element.get_text(" ", strip=True)) if element else None


def extract_section_by_title(soup, section_title):
    buttons = soup.select("button[data-tracking-push]")

    for button in buttons:
        title = clean_text(button.get_text(" ", strip=True))

        if title and title.lower() == section_title.lower():
            controls_id = button.get("aria-controls")
            if not controls_id:
                return None

            content = soup.select_one(f"div#{controls_id} .attribute-content")
            if not content:
                return None

            return clean_text(content.get_text(" ", strip=True))

    return None


def extract_sku(soup):
    container = soup.select_one("#view-item-container")
    if container:
        return container.get("data-track-push")

    rating = soup.select_one("#ratingSummary")
    if rating:
        return rating.get("data-sku")

    return None


def extract_image_url(html):
    match = re.search(r"const images = (\[.*?\]);", html, re.DOTALL)
    if not match:
        return None

    try:
        images = json.loads(match.group(1))
        if images and isinstance(images, list):
            return images[0].get("original")
    except json.JSONDecodeError:
        return None

    return None





def wait_for_reviews(driver, max_wait_seconds=30):
    wait = WebDriverWait(driver, max_wait_seconds)

    try:
        wait.until(
            lambda d: (
                re.search(r"Read\s+[0-9,]+\s+Reviews", d.page_source, re.I)
                and (
                    re.search(r"\b[0-5]\.\d\b", d.page_source)
                    or "out of 5" in d.page_source.lower()
                    or "bv-rnr__" in d.page_source
                )
            )
        )
    except Exception:
        pass

    return driver.page_source


def extract_rating_review_count(driver, soup):
    html = driver.page_source
    soup = BeautifulSoup(html, "lxml")

    rating = None
    review_count = None

    def to_float(value):
        try:
            return float(value)
        except Exception:
            return None

    def to_int(value):
        try:
            return int(value.replace(",", ""))
        except Exception:
            return None

    # 1) aria-label fallback: rating + reviews together
    for tag in soup.select("[aria-label]"):
        label = tag.get("aria-label", "")

        match = re.search(
            r"([0-5](?:\.\d+)?)\s+out of\s+5.*?Read\s+([0-9,]+)\s+reviews?",
            label,
            re.I,
        )
        if match:
            rating = to_float(match.group(1))
            review_count = to_int(match.group(2))
            return rating, review_count

        match = re.search(r"([0-5](?:\.\d+)?)\s+out of\s+5", label, re.I)
        if match and rating is None:
            rating = to_float(match.group(1))

        match = re.search(r"Read\s+([0-9,]+)\s+reviews?", label, re.I)
        if match and review_count is None:
            review_count = to_int(match.group(1))

    # 2) visible Bazaarvoice rating div, example:
    # <div class="bv-rnr__sc-157rd1w-1 fBhJNQ">4.4</div>
    for tag in soup.select("div[class*='bv-rnr__']"):
        text = clean_text(tag.get_text(" ", strip=True))
        if text and re.fullmatch(r"[0-5](?:\.\d+)?", text):
            rating = to_float(text)
            break

    # 3) review count from visible text
    if review_count is None:
        match = re.search(r"Read\s+([0-9,]+)\s+Reviews?", html, re.I)
        if match:
            review_count = to_int(match.group(1))

    # 4) JSON/script fallback
    if rating is None:
        patterns = [
            r'"ratingValue"\s*:\s*"?([0-5](?:\.\d+)?)"?',
            r'"averageRating"\s*:\s*"?([0-5](?:\.\d+)?)"?',
            r'"rating"\s*:\s*"?([0-5](?:\.\d+)?)"?',
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.I)
            if match:
                rating = to_float(match.group(1))
                break

    if review_count is None:
        patterns = [
            r'"reviewCount"\s*:\s*"?([0-9,]+)"?',
            r'"totalReviewCount"\s*:\s*"?([0-9,]+)"?',
            r'"numReviews"\s*:\s*"?([0-9,]+)"?',
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.I)
            if match:
                review_count = to_int(match.group(1))
                break

    # 5) final regex fallback from page source
    if rating is None:
        match = re.search(
            r">\s*([0-5]\.\d)\s*</div>",
            html,
            re.I,
        )
        if match:
            rating = to_float(match.group(1))

    return rating, review_count

def build_raw_ingredients_list(ingredients_text):
    if not ingredients_text:
        return None

    disclaimer = "For the latest information"
    ingredients_text = ingredients_text.split(disclaimer)[0].strip()

    return [
        ingredient.strip()
        for ingredient in ingredients_text.split(",")
        if ingredient.strip()
    ]


def extract_product(url):
    driver = create_driver()

    try:
        driver.get(url)

        html = wait_for_reviews(driver, max_wait_seconds=20)
        soup = BeautifulSoup(html, "lxml")

        rating, review_count = extract_rating_review_count(driver, soup)

        source_product_id = extract_sku(soup)
        ingredients_text = extract_section_by_title(soup, "Ingredients")

        product = {
            "record_id": source_product_id,
            "batch_id": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
            "source_name": "dermstore",
            "source_type": "website",
            "source_url": "https://www.dermstore.com",
            "source_product_id": source_product_id,
            "product_url": url,
            "product_name": get_text(soup, "h1#product-title"),
            "brand_name": get_text(soup, "a.override-pdp-brand"),
            "raw_category": None,
            "product_description": extract_section_by_title(soup, "Product Overview"),
            "ingredients_text": ingredients_text,
            "raw_ingredients_list": build_raw_ingredients_list(ingredients_text),
            "price": get_text(soup, "#product-price"),
            "currency": "USD",
            "sale_price": None,
            "rating": rating,
            "review_count": review_count,
            "size": extract_section_by_title(soup, "Volume"),
            "image_url": extract_image_url(html),
            "availability": None,
            "skin_type_claims": extract_section_by_title(soup, "Skin Type & Concerns"),
            "skin_concern_claims": extract_section_by_title(soup, "Skin Type & Concerns"),
            "product_highlights": extract_section_by_title(soup, "Highlights"),
            "usage_instructions": extract_section_by_title(soup, "How to Use"),
            "extraction_status": "success",
            "missing_fields": [],
            "scraped_at": datetime.utcnow().isoformat(),
            "ingestion_date": datetime.utcnow().strftime("%Y-%m-%d"),
            "pipeline_version": "v1",
            "raw_file_path": None,
        }

        product["missing_fields"] = [
            key for key, value in product.items()
            if value is None and key not in ["sale_price", "raw_file_path"]
        ]

        if product["missing_fields"]:
            product["extraction_status"] = "partial"

        return product

    finally:
        driver.quit()


if __name__ == "__main__":
    product_data = extract_product(TEST_PRODUCT_URL)
    print(json.dumps(product_data, indent=4, ensure_ascii=False))