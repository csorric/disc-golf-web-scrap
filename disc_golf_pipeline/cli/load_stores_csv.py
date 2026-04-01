import argparse
import csv
import os
from pathlib import Path
from urllib.parse import urlparse

from google.cloud import bigquery
from disc_golf_pipeline.common.runtime import PROJECT_ROOT, load_env_file

DEFAULT_PROJECT_ID = "disc-golf-price-compare"
DEFAULT_DATASET = "DiscGolfProducts"
DEFAULT_TABLE = "Stores"
DEFAULT_INPUT_FILE = Path("input/new_disc_golf_stores.csv")
BASE_DIR = PROJECT_ROOT


load_env_file()


def normalize_url(url):
    if not url:
        return None

    value = url.strip()
    if not value:
        return None
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"

    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    normalized = f"{scheme}://{netloc}{path}/"
    return normalized


def get_table_id(project_id, dataset, table):
    return f"{project_id}.{dataset}.{table}"


def load_csv_rows(csv_path):
    rows = []
    with Path(csv_path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            row = {key: (value.strip() if isinstance(value, str) else value) for key, value in raw_row.items()}
            row["URL"] = normalize_url(row.get("URL"))
            row["API_URL"] = normalize_url(row.get("API_URL"))
            if row.get("IsShopify") not in (None, ""):
                row["IsShopify"] = int(row["IsShopify"])
            if row.get("UseUnpagedProductsJson") not in (None, ""):
                row["UseUnpagedProductsJson"] = int(row["UseUnpagedProductsJson"])
            rows.append(row)
    return rows


def fetch_existing_urls(client, project_id, dataset, table):
    table_id = get_table_id(project_id, dataset, table)
    query = f"SELECT URL FROM `{table_id}` WHERE URL IS NOT NULL"
    return {normalize_url(row[0]) for row in client.query(query).result() if normalize_url(row[0])}


def filter_new_rows(rows, existing_urls):
    new_rows = []
    skipped_rows = []

    for row in rows:
        normalized_url = normalize_url(row.get("URL"))
        if not normalized_url:
            skipped_rows.append(row)
            continue
        if normalized_url in existing_urls:
            skipped_rows.append(row)
            continue

        existing_urls.add(normalized_url)
        new_rows.append(row)

    return new_rows, skipped_rows


def insert_rows(client, project_id, dataset, table, rows, batch_size=500):
    table_id = get_table_id(project_id, dataset, table)
    total_inserted = 0

    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            raise RuntimeError(f"BigQuery insert failed: {errors}")
        total_inserted += len(batch)

    return total_inserted


def main():
    parser = argparse.ArgumentParser(description="Load new store rows into BigQuery Stores table.")
    parser.add_argument("--input", default=str(DEFAULT_INPUT_FILE), help="CSV file containing store rows.")
    parser.add_argument("--project", default=DEFAULT_PROJECT_ID, help="GCP project id.")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="BigQuery dataset name.")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="BigQuery table name.")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)
    csv_rows = load_csv_rows(args.input)
    existing_urls = fetch_existing_urls(client, args.project, args.dataset, args.table)
    new_rows, skipped_rows = filter_new_rows(csv_rows, existing_urls)

    print(f"Input rows: {len(csv_rows)}")
    print(f"Existing URL matches skipped: {len(skipped_rows)}")

    if not new_rows:
        print("No new rows to insert.")
        return

    inserted_count = insert_rows(client, args.project, args.dataset, args.table, new_rows)
    print(f"Inserted rows: {inserted_count}")


if __name__ == "__main__":
    main()
