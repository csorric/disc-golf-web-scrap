import argparse
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from google.cloud import bigquery
from google.cloud import storage

from getProductJson import ShopifyScraper
from loadShopifyDiscGolfData import (
    DEFAULT_DATASET,
    DEFAULT_PROJECT_ID,
    load_gcs_prefix_to_bigquery,
    load_local_directory_to_bigquery,
)
from runJsonParser import parse_gcs_prefix, parse_json_records, parse_local_directory, write_local_csvs

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_RAW_BUCKET_NAME = "disc-golf-web-data"
DEFAULT_PARSED_BUCKET_NAME = "disc-golf-parsed-files"
DEFAULT_LOCAL_OUTPUT_DIR = BASE_DIR / "output"
DEFAULT_RAW_OUTPUT_DIR = DEFAULT_LOCAL_OUTPUT_DIR / "raw-data"
DEFAULT_PARSED_OUTPUT_DIR = DEFAULT_LOCAL_OUTPUT_DIR / "parsed-data"
TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")


def load_env_file():
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_env_file()


def hello_http(request):
    run_all()
    return "Pipeline initiated."


try:
    import functions_framework
except Exception:
    functions_framework = None
else:
    hello_http = functions_framework.http(hello_http)


def get_storage_client():
    return storage.Client()


def save_file_to_storage(json_data, bucket_name, file_path, client=None):
    client = client or get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    blob.upload_from_string(json.dumps(json_data), content_type="application/json")


def save_file_locally(json_data, file_path):
    output_path = Path(file_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(json_data, indent=2), encoding="utf-8")


def log_step_start(step_name):
    logging.info("Starting %s", step_name)
    return time.time()


def log_step_end(step_name, started_at, item_count=None):
    elapsed = time.time() - started_at
    if item_count is None:
        logging.info("Finished %s in %.2fs", step_name, elapsed)
    else:
        logging.info("Finished %s in %.2fs (%s items)", step_name, elapsed, item_count)
    return elapsed


def print_run_summary(summary):
    print("")
    print("Run summary")
    print(f"- started_at: {summary['started_at']}")
    print(f"- scrape_count: {summary['scrape_count']}")
    print(f"- parse_count: {summary['parse_count']}")
    print(f"- load_count: {summary['load_count']}")
    print(f"- scrape_seconds: {summary['scrape_seconds']:.2f}")
    print(f"- parse_seconds: {summary['parse_seconds']:.2f}")
    print(f"- load_seconds: {summary['load_seconds']:.2f}")
    print(f"- total_seconds: {summary['total_seconds']:.2f}")


def get_output_mode():
    return os.getenv("OUTPUT_MODE", "local").strip().lower()


def get_raw_bucket_name():
    return os.getenv("RAW_BUCKET_NAME", os.getenv("BUCKET_NAME", DEFAULT_RAW_BUCKET_NAME)).strip()


def get_parsed_bucket_name():
    return os.getenv("PARSED_BUCKET_NAME", DEFAULT_PARSED_BUCKET_NAME).strip()


def get_local_output_dir():
    return Path(os.getenv("LOCAL_OUTPUT_DIR", str(DEFAULT_LOCAL_OUTPUT_DIR)))


def get_raw_output_dir():
    return Path(os.getenv("RAW_OUTPUT_DIR", str(DEFAULT_RAW_OUTPUT_DIR)))


def get_parsed_output_dir():
    return Path(os.getenv("PARSED_OUTPUT_DIR", str(DEFAULT_PARSED_OUTPUT_DIR)))


def get_raw_gcs_prefix():
    return os.getenv("RAW_GCS_PREFIX", "raw-data").strip().strip("/")


def get_parsed_gcs_prefix():
    return os.getenv("PARSED_GCS_PREFIX", "parsed-data").strip().strip("/")


def get_load_source():
    return os.getenv("LOAD_SOURCE", "local").strip().lower()


def get_gcp_project_id():
    return os.getenv("GCP_PROJECT_ID", DEFAULT_PROJECT_ID).strip()


def get_bigquery_dataset():
    return os.getenv("BIGQUERY_DATASET", DEFAULT_DATASET).strip()


def get_configured_store_urls():
    raw_urls = os.getenv("STORE_URLS", "")
    return [url.strip() for url in raw_urls.split(",") if url.strip()]


def query_stores():
    client = bigquery.Client(project=get_gcp_project_id())
    query = """
        SELECT DISTINCT API_URL AS URL FROM `disc-golf-price-compare.DiscGolfProducts.Stores` WHERE IsShopify = 1
    """
    query_job = client.query(query)
    urls = [row[0] for row in query_job]
    print(f"Found {len(urls)} store URLs from BigQuery")
    return urls


def get_store_urls():
    configured_urls = get_configured_store_urls()
    if configured_urls:
        print(f"Using {len(configured_urls)} STORE_URLS from environment")
        return configured_urls
    return query_stores()


def get_file_name_for_url(url):
    parsed_url = urlparse(url if url.startswith(("http://", "https://")) else f"https://{url}")
    hostname = parsed_url.hostname or "store"
    return hostname.replace(".", "__")


def scrape_store(url, storage_client=None):
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    scraper = ShopifyScraper(url)
    output_mode = get_output_mode()
    raw_output_dir = get_raw_output_dir()
    raw_bucket_name = get_raw_bucket_name()
    raw_gcs_prefix = get_raw_gcs_prefix()
    file_name = get_file_name_for_url(url)
    saved_targets = []

    for page in range(1, 40):
        try:
            data = scraper.downloadJson(page)
        except Exception as exc:
            logging.exception(f"Scraper error {url} page {page}: {exc}")
            break

        if not data:
            print(f"No data on {url} page {page}; stopping.")
            break

        relative_file_name = f"{file_name}_{page}_{TIMESTAMP}.json"
        if output_mode == "gcs":
            relative_path = f"{raw_gcs_prefix}/{relative_file_name}" if raw_gcs_prefix else relative_file_name
            save_file_to_storage(data, raw_bucket_name, relative_path, client=storage_client)
            saved_targets.append(relative_path)
            print(f"Uploaded {relative_path} to {raw_bucket_name}")
        else:
            local_path = raw_output_dir / relative_file_name
            save_file_locally(data, local_path)
            saved_targets.append(local_path)
            print(f"Saved {local_path}")

    return saved_targets


def scrape_all():
    started_at = log_step_start("scrape")
    urls = get_store_urls()
    print(f"Scraping {len(urls)} stores")

    storage_client = get_storage_client() if get_output_mode() == "gcs" else None
    saved_targets = []
    for url in urls:
        try:
            saved_targets.extend(scrape_store(url, storage_client=storage_client))
        except Exception as exc:
            logging.exception(f"Failed processing URL {url}: {exc}")
    log_step_end("scrape", started_at, len(saved_targets))
    return saved_targets


def parse_all():
    started_at = log_step_start("parse")
    if get_output_mode() == "gcs":
        targets = parse_gcs_prefix(
            get_raw_bucket_name(),
            get_raw_gcs_prefix(),
            get_parsed_bucket_name(),
            destination_prefix=get_parsed_gcs_prefix(),
        )
        print(f"Parsed {len(targets)} raw GCS objects into {get_parsed_bucket_name()}")
        log_step_end("parse", started_at, len(targets))
        return targets

    written_files = parse_local_directory(get_raw_output_dir(), get_parsed_output_dir())
    print(f"Parsed {len(written_files)} local CSV files into {get_parsed_output_dir()}")
    log_step_end("parse", started_at, len(written_files))
    return written_files


def load_all():
    started_at = log_step_start("load")
    if get_load_source() == "gcs":
        loaded_tables = load_gcs_prefix_to_bigquery(
            get_parsed_bucket_name(),
            prefix=get_parsed_gcs_prefix(),
            project_id=get_gcp_project_id(),
            dataset=get_bigquery_dataset(),
        )
        print(f"Loaded {len(loaded_tables)} GCS CSV objects into BigQuery")
        log_step_end("load", started_at, len(loaded_tables))
        return loaded_tables

    loaded_tables = load_local_directory_to_bigquery(
        get_parsed_output_dir(),
        project_id=get_gcp_project_id(),
        dataset=get_bigquery_dataset(),
    )
    print(f"Loaded {len(loaded_tables)} local CSV files into BigQuery")
    log_step_end("load", started_at, len(loaded_tables))
    return loaded_tables


def run_all():
    pipeline_started_at = time.time()
    summary = {
        "started_at": datetime.now().isoformat(),
        "scrape_count": 0,
        "parse_count": 0,
        "load_count": 0,
        "scrape_seconds": 0.0,
        "parse_seconds": 0.0,
        "load_seconds": 0.0,
        "total_seconds": 0.0,
    }

    step_started_at = time.time()
    scraped = scrape_all()
    summary["scrape_count"] = len(scraped)
    summary["scrape_seconds"] = time.time() - step_started_at

    step_started_at = time.time()
    parsed = parse_all()
    summary["parse_count"] = len(parsed)
    summary["parse_seconds"] = time.time() - step_started_at

    step_started_at = time.time()
    loaded = load_all()
    summary["load_count"] = len(loaded)
    summary["load_seconds"] = time.time() - step_started_at

    summary["total_seconds"] = time.time() - pipeline_started_at
    print_run_summary(summary)
    return summary


def parse_single_download(data, source_url, destination_base):
    store_reference = Path(destination_base).stem.rsplit("_", 2)[0]
    products, product_info = parse_json_records(data, store_reference, store_url=source_url)
    return write_local_csvs(products, product_info, destination_base)


def build_parser():
    parser = argparse.ArgumentParser(description="Disc golf Shopify scrape pipeline")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("scrape", help="Download raw Shopify JSON")
    subparsers.add_parser("parse", help="Parse raw JSON into CSV")
    subparsers.add_parser("load", help="Load parsed CSV into BigQuery")
    subparsers.add_parser("run-all", help="Run scrape, parse, and load")

    return parser


def main():
    logging.basicConfig(level=logging.INFO)
    parser = build_parser()
    args = parser.parse_args()
    command = args.command or "run-all"

    if command == "scrape":
        scrape_all()
    elif command == "parse":
        parse_all()
    elif command == "load":
        load_all()
    else:
        run_all()


if __name__ == "__main__":
    main()
