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

from disc_golf_pipeline.common.runtime import LOG_DIR, PROJECT_ROOT, disable_proxy_env, load_env_file
from disc_golf_pipeline.loaders.infinite_discs import (
    DEFAULT_TABLE as DEFAULT_INFINITE_DISCS_TABLE,
    aggregate_local_parquet as aggregate_infinite_discs_local_parquet,
    archive_gcs_object as archive_infinite_discs_gcs_object,
    archive_local_file as archive_infinite_discs_local_file,
    list_gcs_parquet_objects as list_infinite_discs_gcs_parquet_objects,
    load_gcs_parquet_to_bigquery as load_infinite_discs_gcs_parquet_to_bigquery,
    load_local_parquet_to_bigquery as load_infinite_discs_local_parquet_to_bigquery,
)
from disc_golf_pipeline.loaders.otb_discs import (
    DEFAULT_TABLE as DEFAULT_OTB_DISCS_TABLE,
    aggregate_local_parquet as aggregate_otb_discs_local_parquet,
    archive_gcs_object as archive_otb_discs_gcs_object,
    archive_local_file as archive_otb_discs_local_file,
    list_gcs_parquet_objects as list_otb_discs_gcs_parquet_objects,
    load_gcs_parquet_to_bigquery as load_otb_discs_gcs_parquet_to_bigquery,
    load_local_parquet_to_bigquery as load_otb_discs_local_parquet_to_bigquery,
)
from disc_golf_pipeline.loaders.shopify import (
    DEFAULT_DATASET,
    DEFAULT_PROJECT_ID,
    archive_gcs_object,
    archive_local_file,
    list_gcs_parquet_objects,
    load_gcs_prefix_to_bigquery,
    load_local_directory_to_bigquery,
)
from disc_golf_pipeline.loaders.sunking_discs import (
    DEFAULT_TABLE as DEFAULT_SUN_KING_DISCS_TABLE,
    aggregate_local_parquet as aggregate_sunking_discs_local_parquet,
    archive_gcs_object as archive_sunking_discs_gcs_object,
    archive_local_file as archive_sunking_discs_local_file,
    list_gcs_parquet_objects as list_sunking_discs_gcs_parquet_objects,
    load_gcs_parquet_to_bigquery as load_sunking_discs_gcs_parquet_to_bigquery,
    load_local_parquet_to_bigquery as load_sunking_discs_local_parquet_to_bigquery,
)
from disc_golf_pipeline.parsers.infinite_discs import (
    parse_gcs_prefix as parse_infinite_discs_gcs_prefix,
    parse_local_directory as parse_infinite_discs_local_directory,
)
from disc_golf_pipeline.parsers.otb_discs import (
    parse_gcs_prefix as parse_otb_discs_gcs_prefix,
    parse_local_directory as parse_otb_discs_local_directory,
)
from disc_golf_pipeline.parsers.shopify_aggregate import aggregate_directory
from disc_golf_pipeline.parsers.shopify_pipeline import (
    parse_gcs_prefix,
    parse_json_records,
    parse_local_directory,
    write_local_parquet,
)
from disc_golf_pipeline.parsers.sunking_discs import (
    parse_gcs_prefix as parse_sunking_discs_gcs_prefix,
    parse_local_directory as parse_sunking_discs_local_directory,
)
from disc_golf_pipeline.scrapers.infinite_discs import InfiniteDiscsScraper
from disc_golf_pipeline.scrapers.otb_discs import OTBDiscsScraper
from disc_golf_pipeline.scrapers.shopify import ShopifyScraper
from disc_golf_pipeline.scrapers.sunking_discs import SunKingDiscsScraper
from disc_golf_pipeline.services.indexer import run_indexer
from disc_golf_pipeline.services.process_data import run_process_data

BASE_DIR = PROJECT_ROOT
DEFAULT_RAW_BUCKET_NAME = "disc-golf-web-data"
DEFAULT_PARSED_BUCKET_NAME = "disc-golf-parsed-files"
DEFAULT_LOCAL_OUTPUT_DIR = BASE_DIR / "output"
DEFAULT_RAW_OUTPUT_DIR = DEFAULT_LOCAL_OUTPUT_DIR / "raw-data"
DEFAULT_PARSED_OUTPUT_DIR = DEFAULT_LOCAL_OUTPUT_DIR / "parsed-data"
DEFAULT_AGGREGATED_OUTPUT_DIR = DEFAULT_LOCAL_OUTPUT_DIR / "aggregated-data"
DEFAULT_ARCHIVE_OUTPUT_DIR = DEFAULT_LOCAL_OUTPUT_DIR / "archive"
TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")


load_env_file()


disable_proxy_env()


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
    print(f"- process_count: {summary['process_count']}")
    print(f"- index_count: {summary['index_count']}")
    print(f"- scrape_seconds: {summary['scrape_seconds']:.2f}")
    print(f"- parse_seconds: {summary['parse_seconds']:.2f}")
    print(f"- load_seconds: {summary['load_seconds']:.2f}")
    print(f"- process_seconds: {summary['process_seconds']:.2f}")
    print(f"- index_seconds: {summary['index_seconds']:.2f}")
    print(f"- total_seconds: {summary['total_seconds']:.2f}")


def get_log_file_path(command):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    date_stamp = datetime.now().strftime("%Y%m%d")
    safe_command = (command or "run-all").replace(" ", "-")
    return LOG_DIR / f"{safe_command}-{date_stamp}.log"


def configure_logging(command):
    log_file_path = get_log_file_path(command)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[stream_handler, file_handler],
        force=True,
    )
    logging.info("Writing logs to %s", log_file_path)
    return log_file_path


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


def get_aggregated_output_dir():
    return Path(os.getenv("AGGREGATED_OUTPUT_DIR", str(DEFAULT_AGGREGATED_OUTPUT_DIR)))


def get_archive_output_dir():
    return Path(os.getenv("ARCHIVE_OUTPUT_DIR", str(DEFAULT_ARCHIVE_OUTPUT_DIR)))


def get_raw_gcs_prefix():
    return os.getenv("RAW_GCS_PREFIX", "raw-data").strip().strip("/")


def get_parsed_gcs_prefix():
    return os.getenv("PARSED_GCS_PREFIX", "parsed-data").strip().strip("/")


def get_archive_gcs_prefix():
    return os.getenv("ARCHIVE_GCS_PREFIX", "archive").strip().strip("/")


def get_load_source():
    return os.getenv("LOAD_SOURCE", "local").strip().lower()


def get_gcp_project_id():
    return os.getenv("GCP_PROJECT_ID", DEFAULT_PROJECT_ID).strip()


def get_bigquery_dataset():
    return os.getenv("BIGQUERY_DATASET", DEFAULT_DATASET).strip()


def get_infinite_discs_page_size():
    return int(os.getenv("INFINITE_DISCS_PAGE_SIZE", "10000"))


def get_sunking_discs_max_pages():
    raw_value = os.getenv("SUNKING_DISCS_MAX_PAGES", "").strip()
    return int(raw_value) if raw_value else None


def get_sunking_discs_max_products():
    raw_value = os.getenv("SUNKING_DISCS_MAX_PRODUCTS", "").strip()
    return int(raw_value) if raw_value else None


def get_sunking_discs_delay_seconds():
    return float(os.getenv("SUNKING_DISCS_DELAY_SECONDS", "0"))


def get_otb_discs_max_products():
    raw_value = os.getenv("OTB_DISCS_MAX_PRODUCTS", "").strip()
    return int(raw_value) if raw_value else None


def get_otb_discs_delay_seconds():
    raw_value = os.getenv("OTB_DISCS_DELAY_SECONDS", "").strip()
    return float(raw_value) if raw_value else None


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


def scrape_infinite_discs():
    started_at = log_step_start("scrape-infinite-discs")
    scraper = InfiniteDiscsScraper()
    output_mode = get_output_mode()
    raw_output_dir = get_infinite_discs_raw_output_dir()
    raw_bucket_name = get_raw_bucket_name()
    raw_gcs_prefix = get_infinite_discs_raw_gcs_prefix()
    storage_client = get_storage_client() if output_mode == "gcs" else None

    page_size = get_infinite_discs_page_size()
    start = 0
    draw = 1
    saved_targets = []
    total = None

    while total is None or start < total:
        data = scraper.download_json(start=start, length=page_size, draw=draw)
        if total is None:
            total = int(data.get("recordsTotal") or 0)
            print(f"Infinite Discs reported {total} total records")
        relative_file_name = f"infinite-discs_{draw}_{TIMESTAMP}.json"

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

        start += page_size
        draw += 1

    log_step_end("scrape-infinite-discs", started_at, len(saved_targets))
    return saved_targets


def scrape_sunking_discs():
    started_at = log_step_start("scrape-sunking-discs")
    scraper = SunKingDiscsScraper(delay_seconds=get_sunking_discs_delay_seconds())
    output_mode = get_output_mode()
    raw_output_dir = get_sunking_discs_raw_output_dir()
    raw_bucket_name = get_raw_bucket_name()
    raw_gcs_prefix = get_sunking_discs_raw_gcs_prefix()
    storage_client = get_storage_client() if output_mode == "gcs" else None

    payload = scraper.crawl(
        max_pages=get_sunking_discs_max_pages(),
        max_products=get_sunking_discs_max_products(),
    )
    relative_file_name = f"sunking-discs_{TIMESTAMP}.json"

    if output_mode == "gcs":
        relative_path = f"{raw_gcs_prefix}/{relative_file_name}" if raw_gcs_prefix else relative_file_name
        save_file_to_storage(payload, raw_bucket_name, relative_path, client=storage_client)
        saved_target = relative_path
        print(f"Uploaded {relative_path} to {raw_bucket_name}")
    else:
        local_path = raw_output_dir / relative_file_name
        save_file_locally(payload, local_path)
        saved_target = local_path
        print(f"Saved {local_path}")

    log_step_end("scrape-sunking-discs", started_at, 1)
    return [saved_target]


def scrape_otb_discs():
    started_at = log_step_start("scrape-otb-discs")
    scraper = OTBDiscsScraper(delay_seconds=get_otb_discs_delay_seconds())
    output_mode = get_output_mode()
    raw_output_dir = get_otb_discs_raw_output_dir()
    raw_bucket_name = get_raw_bucket_name()
    raw_gcs_prefix = get_otb_discs_raw_gcs_prefix()
    storage_client = get_storage_client() if output_mode == "gcs" else None

    payload = scraper.crawl(max_products=get_otb_discs_max_products())
    relative_file_name = f"otb-discs_{TIMESTAMP}.json"

    if output_mode == "gcs":
        relative_path = f"{raw_gcs_prefix}/{relative_file_name}" if raw_gcs_prefix else relative_file_name
        save_file_to_storage(payload, raw_bucket_name, relative_path, client=storage_client)
        saved_target = relative_path
        print(f"Uploaded {relative_path} to {raw_bucket_name}")
    else:
        local_path = raw_output_dir / relative_file_name
        save_file_locally(payload, local_path)
        saved_target = local_path
        print(f"Saved {local_path}")

    log_step_end("scrape-otb-discs", started_at, 1)
    return [saved_target]


def get_infinite_discs_raw_output_dir():
    return get_raw_output_dir() / "infinite-discs"


def get_infinite_discs_parsed_output_dir():
    return get_parsed_output_dir() / "infinite-discs"


def get_infinite_discs_aggregated_output_dir():
    return get_aggregated_output_dir() / "infinite-discs"


def get_infinite_discs_archive_output_dir():
    return get_archive_output_dir() / "infinite-discs"


def get_infinite_discs_raw_gcs_prefix():
    base = get_raw_gcs_prefix()
    return f"{base}/infinite-discs" if base else "infinite-discs"


def get_infinite_discs_parsed_gcs_prefix():
    base = get_parsed_gcs_prefix()
    return f"{base}/infinite-discs" if base else "infinite-discs"


def get_infinite_discs_archive_gcs_prefix():
    base = get_archive_gcs_prefix()
    return f"{base}/infinite-discs" if base else "infinite-discs"


def get_sunking_discs_raw_output_dir():
    return get_raw_output_dir() / "sunking-discs"


def get_otb_discs_raw_output_dir():
    return get_raw_output_dir() / "otb-discs"


def get_otb_discs_parsed_output_dir():
    return get_parsed_output_dir() / "otb-discs"


def get_otb_discs_aggregated_output_dir():
    return get_aggregated_output_dir() / "otb-discs"


def get_otb_discs_archive_output_dir():
    return get_archive_output_dir() / "otb-discs"


def get_sunking_discs_parsed_output_dir():
    return get_parsed_output_dir() / "sunking-discs"


def get_sunking_discs_aggregated_output_dir():
    return get_aggregated_output_dir() / "sunking-discs"


def get_sunking_discs_archive_output_dir():
    return get_archive_output_dir() / "sunking-discs"


def get_sunking_discs_raw_gcs_prefix():
    base = get_raw_gcs_prefix()
    return f"{base}/sunking-discs" if base else "sunking-discs"


def get_otb_discs_raw_gcs_prefix():
    base = get_raw_gcs_prefix()
    return f"{base}/otb-discs" if base else "otb-discs"


def get_otb_discs_parsed_gcs_prefix():
    base = get_parsed_gcs_prefix()
    return f"{base}/otb-discs" if base else "otb-discs"


def get_otb_discs_archive_gcs_prefix():
    base = get_archive_gcs_prefix()
    return f"{base}/otb-discs" if base else "otb-discs"


def get_sunking_discs_parsed_gcs_prefix():
    base = get_parsed_gcs_prefix()
    return f"{base}/sunking-discs" if base else "sunking-discs"


def get_sunking_discs_archive_gcs_prefix():
    base = get_archive_gcs_prefix()
    return f"{base}/sunking-discs" if base else "sunking-discs"


def migrate_legacy_infinite_discs_raw_files():
    legacy_files = sorted(get_raw_output_dir().glob("infinite-discs_*.json"))
    target_dir = get_infinite_discs_raw_output_dir()
    target_dir.mkdir(parents=True, exist_ok=True)

    moved_files = []
    for legacy_file in legacy_files:
        destination = target_dir / legacy_file.name
        legacy_file.replace(destination)
        moved_files.append(destination)

    if moved_files:
        print(f"Migrated {len(moved_files)} legacy Infinite Discs raw files into {target_dir}")

    return moved_files


def cleanup_infinite_discs_test_artifacts():
    cleanup_patterns = [
        get_infinite_discs_raw_output_dir() / "infinite-discs_test.json",
        get_infinite_discs_parsed_output_dir() / "infinite-discs_test.parquet",
        get_infinite_discs_aggregated_output_dir() / "infinite-discs_test.parquet",
    ]

    removed_files = []
    for file_path in cleanup_patterns:
        if file_path.exists():
            file_path.unlink()
            removed_files.append(file_path)

    if removed_files:
        print(f"Removed {len(removed_files)} stale Infinite Discs test artifacts")

    return removed_files


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
    print(f"Parsed {len(written_files)} local Parquet files into {get_parsed_output_dir()}")
    log_step_end("parse", started_at, len(written_files))
    return written_files


def parse_infinite_discs():
    started_at = log_step_start("parse-infinite-discs")
    migrate_legacy_infinite_discs_raw_files()
    cleanup_infinite_discs_test_artifacts()

    if get_output_mode() == "gcs":
        written_files = parse_infinite_discs_gcs_prefix(
            get_raw_bucket_name(),
            get_infinite_discs_raw_gcs_prefix(),
            get_parsed_bucket_name(),
            destination_prefix=get_infinite_discs_parsed_gcs_prefix(),
        )
        print(f"Parsed {len(written_files)} Infinite Discs GCS objects into {get_parsed_bucket_name()}")
        log_step_end("parse-infinite-discs", started_at, len(written_files))
        return written_files

    written_files = parse_infinite_discs_local_directory(
        get_infinite_discs_raw_output_dir(),
        get_infinite_discs_parsed_output_dir(),
    )
    print(f"Parsed {len(written_files)} Infinite Discs local Parquet files into {get_infinite_discs_parsed_output_dir()}")
    log_step_end("parse-infinite-discs", started_at, len(written_files))
    return written_files


def parse_sunking_discs():
    started_at = log_step_start("parse-sunking-discs")

    if get_output_mode() == "gcs":
        written_files = parse_sunking_discs_gcs_prefix(
            get_raw_bucket_name(),
            get_sunking_discs_raw_gcs_prefix(),
            get_parsed_bucket_name(),
            destination_prefix=get_sunking_discs_parsed_gcs_prefix(),
        )
        print(f"Parsed {len(written_files)} Sun King Discs GCS objects into {get_parsed_bucket_name()}")
        log_step_end("parse-sunking-discs", started_at, len(written_files))
        return written_files

    written_files = parse_sunking_discs_local_directory(
        get_sunking_discs_raw_output_dir(),
        get_sunking_discs_parsed_output_dir(),
    )
    print(f"Parsed {len(written_files)} Sun King Discs local Parquet files into {get_sunking_discs_parsed_output_dir()}")
    log_step_end("parse-sunking-discs", started_at, len(written_files))
    return written_files


def parse_otb_discs():
    started_at = log_step_start("parse-otb-discs")

    if get_output_mode() == "gcs":
        written_files = parse_otb_discs_gcs_prefix(
            get_raw_bucket_name(),
            get_otb_discs_raw_gcs_prefix(),
            get_parsed_bucket_name(),
            destination_prefix=get_otb_discs_parsed_gcs_prefix(),
        )
        print(f"Parsed {len(written_files)} OTB Discs GCS objects into {get_parsed_bucket_name()}")
        log_step_end("parse-otb-discs", started_at, len(written_files))
        return written_files

    written_files = parse_otb_discs_local_directory(
        get_otb_discs_raw_output_dir(),
        get_otb_discs_parsed_output_dir(),
    )
    print(f"Parsed {len(written_files)} OTB Discs local Parquet files into {get_otb_discs_parsed_output_dir()}")
    log_step_end("parse-otb-discs", started_at, len(written_files))
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
        archived_objects = []
        if loaded_tables:
            parquet_objects = list_gcs_parquet_objects(get_parsed_bucket_name(), prefix=get_parsed_gcs_prefix())
            for blob_name in parquet_objects:
                archived_objects.append(archive_gcs_object(get_parsed_bucket_name(), blob_name, get_archive_gcs_prefix()))
            print(f"Archived {len(archived_objects)} GCS Parquet files into {get_archive_gcs_prefix()}")
        print(f"Loaded {len(loaded_tables)} GCS Parquet groups into BigQuery")
        log_step_end("load", started_at, len(loaded_tables))
        return loaded_tables

    aggregated_results = aggregate_directory(get_parsed_output_dir(), get_aggregated_output_dir())
    print(f"Aggregated {len(aggregated_results)} table files into {get_aggregated_output_dir()}")
    loaded_tables = load_local_directory_to_bigquery(
        get_aggregated_output_dir(),
        project_id=get_gcp_project_id(),
        dataset=get_bigquery_dataset(),
    )
    archived_files = []
    if loaded_tables:
        archive_root = get_archive_output_dir()
        parsed_archive_dir = archive_root / "parsed-data"
        aggregated_archive_dir = archive_root / "aggregated-data"

        for parquet_file in sorted(get_parsed_output_dir().glob("*.parquet")):
            archived_files.append(archive_local_file(parquet_file, parsed_archive_dir))
        for parquet_file in sorted(get_aggregated_output_dir().glob("*.parquet")):
            archived_files.append(archive_local_file(parquet_file, aggregated_archive_dir))
        print(f"Archived {len(archived_files)} local Parquet files into {archive_root}")
    print(f"Loaded {len(loaded_tables)} local Parquet files into BigQuery")
    log_step_end("load", started_at, len(loaded_tables))
    return loaded_tables


def load_infinite_discs():
    started_at = log_step_start("load-infinite-discs")

    if get_load_source() == "gcs":
        parquet_objects = list_infinite_discs_gcs_parquet_objects(
            get_parsed_bucket_name(),
            prefix=get_infinite_discs_parsed_gcs_prefix(),
        )
        table_id = load_infinite_discs_gcs_parquet_to_bigquery(
            get_parsed_bucket_name(),
            parquet_objects,
            project_id=get_gcp_project_id(),
            dataset=get_bigquery_dataset(),
        )
        archived_objects = []
        if table_id:
            for blob_name in parquet_objects:
                archived_objects.append(
                    archive_infinite_discs_gcs_object(
                        get_parsed_bucket_name(),
                        blob_name,
                        get_infinite_discs_archive_gcs_prefix(),
                    )
                )
            print(f"Archived {len(archived_objects)} Infinite Discs GCS Parquet files into {get_infinite_discs_archive_gcs_prefix()}")
            print(f"Loaded 1 Infinite Discs table into BigQuery: {table_id}")
            log_step_end("load-infinite-discs", started_at, 1)
            return [table_id]

        log_step_end("load-infinite-discs", started_at, 0)
        return []

    aggregated_file, row_count = aggregate_infinite_discs_local_parquet(
        get_infinite_discs_parsed_output_dir(),
        get_infinite_discs_aggregated_output_dir() / f"{DEFAULT_INFINITE_DISCS_TABLE}.parquet",
    )
    if not aggregated_file:
        print("No Infinite Discs Parquet files found to load.")
        log_step_end("load-infinite-discs", started_at, 0)
        return []

    print(f"Aggregated Infinite Discs parquet into {aggregated_file} ({row_count} rows)")
    table_id = load_infinite_discs_local_parquet_to_bigquery(
        aggregated_file,
        project_id=get_gcp_project_id(),
        dataset=get_bigquery_dataset(),
    )

    archive_root = get_infinite_discs_archive_output_dir()
    parsed_archive_dir = archive_root / "parsed-data"
    aggregated_archive_dir = archive_root / "aggregated-data"
    archived_files = []

    for parquet_file in sorted(get_infinite_discs_parsed_output_dir().glob("*.parquet")):
        archived_files.append(archive_infinite_discs_local_file(parquet_file, parsed_archive_dir))
    archived_files.append(archive_infinite_discs_local_file(aggregated_file, aggregated_archive_dir))

    print(f"Archived {len(archived_files)} Infinite Discs local Parquet files into {archive_root}")
    print(f"Loaded 1 Infinite Discs table into BigQuery: {table_id}")
    log_step_end("load-infinite-discs", started_at, 1)
    return [table_id]


def load_sunking_discs():
    started_at = log_step_start("load-sunking-discs")

    if get_load_source() == "gcs":
        parquet_objects = list_sunking_discs_gcs_parquet_objects(
            get_parsed_bucket_name(),
            prefix=get_sunking_discs_parsed_gcs_prefix(),
        )
        table_id = load_sunking_discs_gcs_parquet_to_bigquery(
            get_parsed_bucket_name(),
            parquet_objects,
            project_id=get_gcp_project_id(),
            dataset=get_bigquery_dataset(),
        )
        archived_objects = []
        if table_id:
            for blob_name in parquet_objects:
                archived_objects.append(
                    archive_sunking_discs_gcs_object(
                        get_parsed_bucket_name(),
                        blob_name,
                        get_sunking_discs_archive_gcs_prefix(),
                    )
                )
            print(f"Archived {len(archived_objects)} Sun King Discs GCS Parquet files into {get_sunking_discs_archive_gcs_prefix()}")
            print(f"Loaded 1 Sun King Discs table into BigQuery: {table_id}")
            log_step_end("load-sunking-discs", started_at, 1)
            return [table_id]

        log_step_end("load-sunking-discs", started_at, 0)
        return []

    aggregated_file, row_count = aggregate_sunking_discs_local_parquet(
        get_sunking_discs_parsed_output_dir(),
        get_sunking_discs_aggregated_output_dir() / f"{DEFAULT_SUN_KING_DISCS_TABLE}.parquet",
    )
    if not aggregated_file:
        print("No Sun King Discs Parquet files found to load.")
        log_step_end("load-sunking-discs", started_at, 0)
        return []

    print(f"Aggregated Sun King Discs parquet into {aggregated_file} ({row_count} rows)")
    table_id = load_sunking_discs_local_parquet_to_bigquery(
        aggregated_file,
        project_id=get_gcp_project_id(),
        dataset=get_bigquery_dataset(),
    )

    archive_root = get_sunking_discs_archive_output_dir()
    parsed_archive_dir = archive_root / "parsed-data"
    aggregated_archive_dir = archive_root / "aggregated-data"
    archived_files = []

    for parquet_file in sorted(get_sunking_discs_parsed_output_dir().glob("*.parquet")):
        archived_files.append(archive_sunking_discs_local_file(parquet_file, parsed_archive_dir))
    archived_files.append(archive_sunking_discs_local_file(aggregated_file, aggregated_archive_dir))

    print(f"Archived {len(archived_files)} Sun King Discs local Parquet files into {archive_root}")
    print(f"Loaded 1 Sun King Discs table into BigQuery: {table_id}")
    log_step_end("load-sunking-discs", started_at, 1)
    return [table_id]


def load_otb_discs():
    started_at = log_step_start("load-otb-discs")

    if get_load_source() == "gcs":
        parquet_objects = list_otb_discs_gcs_parquet_objects(
            get_parsed_bucket_name(),
            prefix=get_otb_discs_parsed_gcs_prefix(),
        )
        table_id = load_otb_discs_gcs_parquet_to_bigquery(
            get_parsed_bucket_name(),
            parquet_objects,
            project_id=get_gcp_project_id(),
            dataset=get_bigquery_dataset(),
        )
        archived_objects = []
        if table_id:
            for blob_name in parquet_objects:
                archived_objects.append(
                    archive_otb_discs_gcs_object(
                        get_parsed_bucket_name(),
                        blob_name,
                        get_otb_discs_archive_gcs_prefix(),
                    )
                )
            print(f"Archived {len(archived_objects)} OTB Discs GCS Parquet files into {get_otb_discs_archive_gcs_prefix()}")
            print(f"Loaded 1 OTB Discs table into BigQuery: {table_id}")
            log_step_end("load-otb-discs", started_at, 1)
            return [table_id]

        log_step_end("load-otb-discs", started_at, 0)
        return []

    aggregated_file, row_count = aggregate_otb_discs_local_parquet(
        get_otb_discs_parsed_output_dir(),
        get_otb_discs_aggregated_output_dir() / f"{DEFAULT_OTB_DISCS_TABLE}.parquet",
    )
    if not aggregated_file:
        print("No OTB Discs Parquet files found to load.")
        log_step_end("load-otb-discs", started_at, 0)
        return []

    print(f"Aggregated OTB Discs parquet into {aggregated_file} ({row_count} rows)")
    table_id = load_otb_discs_local_parquet_to_bigquery(
        aggregated_file,
        project_id=get_gcp_project_id(),
        dataset=get_bigquery_dataset(),
    )

    archive_root = get_otb_discs_archive_output_dir()
    parsed_archive_dir = archive_root / "parsed-data"
    aggregated_archive_dir = archive_root / "aggregated-data"
    archived_files = []

    for parquet_file in sorted(get_otb_discs_parsed_output_dir().glob("*.parquet")):
        archived_files.append(archive_otb_discs_local_file(parquet_file, parsed_archive_dir))
    archived_files.append(archive_otb_discs_local_file(aggregated_file, aggregated_archive_dir))

    print(f"Archived {len(archived_files)} OTB Discs local Parquet files into {archive_root}")
    print(f"Loaded 1 OTB Discs table into BigQuery: {table_id}")
    log_step_end("load-otb-discs", started_at, 1)
    return [table_id]


def run_all():
    pipeline_started_at = time.time()
    summary = {
        "started_at": datetime.now().isoformat(),
        "scrape_count": 0,
        "parse_count": 0,
        "load_count": 0,
        "process_count": 0,
        "index_count": 0,
        "scrape_seconds": 0.0,
        "parse_seconds": 0.0,
        "load_seconds": 0.0,
        "process_seconds": 0.0,
        "index_seconds": 0.0,
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

    step_started_at = time.time()
    run_process_data(project_id=get_gcp_project_id(), dataset=get_bigquery_dataset())
    summary["process_count"] = 1
    summary["process_seconds"] = time.time() - step_started_at

    step_started_at = time.time()
    index_summary = run_indexer()
    summary["index_count"] = 0 if index_summary.get("skipped") else 1
    summary["index_seconds"] = time.time() - step_started_at

    summary["total_seconds"] = time.time() - pipeline_started_at
    print_run_summary(summary)
    return summary


def parse_single_download(data, source_url, destination_base):
    store_reference = Path(destination_base).stem.rsplit("_", 2)[0]
    products, product_info = parse_json_records(data, store_reference, store_url=source_url)
    return write_local_parquet(products, product_info, destination_base)


def build_parser():
    parser = argparse.ArgumentParser(description="Disc golf Shopify scrape pipeline")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("scrape-shopify", help="Download raw Shopify JSON")
    subparsers.add_parser("parse-shopify", help="Parse Shopify JSON into Parquet")
    subparsers.add_parser("load-shopify", help="Load Shopify Parquet into BigQuery")
    subparsers.add_parser("run-all-shopify", help="Run scrape, parse, and load for Shopify")
    subparsers.add_parser("scrape-infinite-discs", help="Download raw Infinite Discs JSON")
    subparsers.add_parser("parse-infinite-discs", help="Parse Infinite Discs JSON into Parquet")
    subparsers.add_parser("load-infinite-discs", help="Load Infinite Discs Parquet into BigQuery")
    subparsers.add_parser("run-all-infinite-discs", help="Run the Infinite Discs scrape flow")
    subparsers.add_parser("scrape-sunking-discs", help="Download raw Sun King Discs product HTML in JSON")
    subparsers.add_parser("parse-sunking-discs", help="Parse Sun King Discs JSON into Parquet")
    subparsers.add_parser("load-sunking-discs", help="Load Sun King Discs Parquet into BigQuery")
    subparsers.add_parser("run-all-sunking-discs", help="Run the Sun King Discs scrape flow")
    subparsers.add_parser("scrape-otb-discs", help="Download raw OTB Discs product HTML in JSON")
    subparsers.add_parser("parse-otb-discs", help="Parse OTB Discs JSON into Parquet")
    subparsers.add_parser("load-otb-discs", help="Load OTB Discs Parquet into BigQuery")
    subparsers.add_parser("run-all-otb-discs", help="Run the OTB Discs scrape flow")
    subparsers.add_parser("process-data", help="Run post-load BigQuery processing steps")
    subparsers.add_parser("index-typesense", help="Run the incremental Typesense indexer")
    subparsers.add_parser("run-all", help="Run the default full pipeline")

    # Backward-compatible aliases for the current Shopify pipeline.
    subparsers.add_parser("scrape", help=argparse.SUPPRESS)
    subparsers.add_parser("parse", help=argparse.SUPPRESS)
    subparsers.add_parser("load", help=argparse.SUPPRESS)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    command = args.command or "run-all"
    configure_logging(command)

    if command in {"scrape", "scrape-shopify"}:
        scrape_all()
    elif command == "scrape-infinite-discs":
        scrape_infinite_discs()
    elif command == "scrape-sunking-discs":
        scrape_sunking_discs()
    elif command == "scrape-otb-discs":
        scrape_otb_discs()
    elif command == "parse-infinite-discs":
        parse_infinite_discs()
    elif command == "parse-sunking-discs":
        parse_sunking_discs()
    elif command == "parse-otb-discs":
        parse_otb_discs()
    elif command == "load-infinite-discs":
        load_infinite_discs()
    elif command == "load-sunking-discs":
        load_sunking_discs()
    elif command == "load-otb-discs":
        load_otb_discs()
    elif command in {"parse", "parse-shopify"}:
        parse_all()
    elif command in {"load", "load-shopify"}:
        load_all()
    elif command == "process-data":
        run_process_data(project_id=get_gcp_project_id(), dataset=get_bigquery_dataset())
    elif command == "index-typesense":
        run_indexer()
    elif command == "run-all-shopify":
        scrape_all()
        parse_all()
        load_all()
    elif command == "run-all-infinite-discs":
        scrape_infinite_discs()
        parse_infinite_discs()
        load_infinite_discs()
    elif command == "run-all-sunking-discs":
        scrape_sunking_discs()
        parse_sunking_discs()
        load_sunking_discs()
    elif command == "run-all-otb-discs":
        scrape_otb_discs()
        parse_otb_discs()
        load_otb_discs()
    else:
        run_all()


if __name__ == "__main__":
    main()

