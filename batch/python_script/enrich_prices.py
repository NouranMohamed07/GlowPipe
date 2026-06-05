import os
import re
import time
import pandas as pd
import requests

INPUT_FILE = "batch/enrichment/price_review_table.csv"
OUTPUT_FILE = "batch/enrichment/price_enriched_table.csv"

SERPAPI_KEY = os.environ.get("SERPAPI_KEY")


def extract_price_from_text(text):
    if not text:
        return None

    matches = re.findall(r"(\$|USD|EGP|£)?\s?([0-9]+(?:\.[0-9]{1,2})?)", text)

    prices = []
    for _, value in matches:
        try:
            price = float(value)
            if 1 <= price <= 10000:
                prices.append(price)
        except:
            pass

    if len(prices) == 0:
        return None

    return prices[0]


def search_price(product_name, brand_name):
    query = f"{brand_name} {product_name} skincare price"

    url = "https://serpapi.com/search.json"
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERPAPI_KEY
    }

    try:
        response = requests.get(url, params=params, timeout=20)
        data = response.json()

        results = data.get("organic_results", [])

        for result in results:
            text = " ".join([
                result.get("title", ""),
                result.get("snippet", "")
            ])

            price = extract_price_from_text(text)

            if price is not None:
                return price, result.get("link", None)

        return None, None

    except Exception:
        return None, None


df = pd.read_csv(
    INPUT_FILE,
    engine="python",
    on_bad_lines="skip"
)

print(f"Rows loaded after skipping bad lines: {len(df)}")

df["new_price"] = None
df["price_source_url"] = None
df["price_update_status"] = "not_updated"

total_rows = len(df)
updated_count = 0

for idx, row in df.iterrows():
    product_name = row.get("product_name", "")
    brand_name = row.get("brand_name", "")

    price, url = search_price(product_name, brand_name)

    if price is not None:
        df.at[idx, "new_price"] = price
        df.at[idx, "price_source_url"] = url
        df.at[idx, "price_update_status"] = "updated"
        updated_count += 1

    time.sleep(1)

remaining_null = total_rows - updated_count

print(f"Total invalid price rows: {total_rows}")
print(f"Updated prices: {updated_count}")
print(f"Still missing prices: {remaining_null}")

df.to_csv(OUTPUT_FILE, index=False)