import csv
import json
import random
import time
from datetime import datetime
from pathlib import Path

from extract_product import extract_product


URLS_CSV_PATH = r"D:\iti final project\data\raw\source=dermstore\entity=product_urls\ingestion_date=2026-05-16\batch_id=20260516_110358\product_urls.csv"

MAX_PRODUCTS = None
MAX_RETRIES = 2
DELAY_MIN = 2
DELAY_MAX = 5

SOURCE_NAME = "dermstore"
ENTITY_NAME = "products"
PIPELINE_VERSION = "v1"


def create_output_paths():
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")

    output_dir = Path(
        f"data/raw/source={SOURCE_NAME}/"
        f"entity={ENTITY_NAME}/"
        f"ingestion_date={ingestion_date}/"
        f"batch_id={batch_id}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    return {
        "batch_id": batch_id,
        "ingestion_date": ingestion_date,
        "directory": output_dir,
        "jsonl": output_dir / "products.jsonl",
        "csv": output_dir / "products.csv",
        "log": output_dir / "scrape_log.json",
    }


def read_product_urls(csv_path, max_products=None):
    urls = []

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            url = row.get("product_url")

            if url:
                urls.append(url.strip())

    urls = list(dict.fromkeys(urls))

    if max_products:
        urls = urls[:max_products]

    return urls


def append_jsonl(record, filepath):
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def append_csv(record, filepath):
    file_exists = filepath.exists()

    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=record.keys())

        if not file_exists:
            writer.writeheader()

        writer.writerow(record)


def scrape_with_retries(url):
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Scraping attempt {attempt}: {url}")
            return extract_product(url)

        except Exception as e:
            last_error = str(e)
            print(f"Failed attempt {attempt}: {last_error}")
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    return {
        "product_url": url,
        "extraction_status": "failed",
        "error_message": last_error,
        "scraped_at": datetime.utcnow().isoformat(),
    }


def run_batch():
    output_paths = create_output_paths()

    urls = read_product_urls(
        URLS_CSV_PATH,
        max_products=MAX_PRODUCTS,
    )

    log = {
        "batch_id": output_paths["batch_id"],
        "source_name": SOURCE_NAME,
        "entity": ENTITY_NAME,
        "pipeline_version": PIPELINE_VERSION,
        "started_at": datetime.utcnow().isoformat(),
        "finished_at": None,
        "input_url_file": URLS_CSV_PATH,
        "total_urls": len(urls),
        "success_count": 0,
        "partial_count": 0,
        "failed_count": 0,
        "failed_pages": [],
        "output_files": {
            "products_jsonl": str(output_paths["jsonl"]),
            "products_csv": str(output_paths["csv"]),
            "scrape_log": str(output_paths["log"]),
        },
    }

    print(f"Starting batch with {len(urls)} URLs")

    for index, url in enumerate(urls, start=1):
        print(f"\n[{index}/{len(urls)}] {url}")

        record = scrape_with_retries(url)

        if record.get("extraction_status") == "failed":
            log["failed_count"] += 1
            log["failed_pages"].append(record)
        elif record.get("extraction_status") == "partial":
            log["partial_count"] += 1
        else:
            log["success_count"] += 1

        append_jsonl(record, output_paths["jsonl"])
        append_csv(record, output_paths["csv"])

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    log["finished_at"] = datetime.utcnow().isoformat()

    with open(output_paths["log"], "w", encoding="utf-8") as f:
        json.dump(log, f, indent=4, ensure_ascii=False)

    print("\nBatch finished.")
    print(f"Products JSONL: {output_paths['jsonl']}")
    print(f"Products CSV: {output_paths['csv']}")
    print(f"Scrape log: {output_paths['log']}")


if __name__ == "__main__":
    run_batch()