# Local development

This project is now set up to run as a VM-style pipeline from `main.py`.

Main commands:

- `python main.py scrape-shopify`
  Downloads raw Shopify JSON.
- `python main.py parse-shopify`
  Converts raw JSON into compressed Parquet files.
- `python main.py load-shopify`
  Aggregates parsed Parquet files, then replaces the BigQuery `Products` and `ProductInfo` tables.
- `python main.py run-all-shopify`
  Runs scrape, parse, and load for Shopify.
- `python main.py run-all`
  Runs the default full pipeline. Today this is the Shopify pipeline.
- `python main.py scrape-infinite-discs`
  Downloads raw Infinite Discs JSON separately from the Shopify flow.
- `python main.py parse-infinite-discs`
  Parses Infinite Discs JSON into Parquet and deletes the source JSON.
- `python main.py load-infinite-discs`
  Aggregates Infinite Discs Parquet, truncates and reloads the table, then archives the Parquet files.
- `python main.py run-all-infinite-discs`
  Runs the full Infinite Discs pipeline.
- `python loadStoresCsv.py`
  Ad hoc loader for new rows into `DiscGolfProducts.Stores`; inserts only URLs not already present.
- `functions-framework --target hello_http`
  Keeps the HTTP entrypoint available if you still want it.

## 1. Create and activate the virtual environment

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 2. Configure `.env`

The project already reads `.env` automatically.

Example:

```env
GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\service-account.json
OUTPUT_MODE=local
RAW_OUTPUT_DIR=C:\Users\chris\Documents\GitHub\disc-golf-web-scrap\output\raw-data
PARSED_OUTPUT_DIR=C:\Users\chris\Documents\GitHub\disc-golf-web-scrap\output\parsed-data
AGGREGATED_OUTPUT_DIR=C:\Users\chris\Documents\GitHub\disc-golf-web-scrap\output\aggregated-data
ARCHIVE_OUTPUT_DIR=C:\Users\chris\Documents\GitHub\disc-golf-web-scrap\output\archive
RAW_BUCKET_NAME=disc-golf-web-data
PARSED_BUCKET_NAME=disc-golf-parsed-files
RAW_GCS_PREFIX=raw-data
PARSED_GCS_PREFIX=parsed-data
ARCHIVE_GCS_PREFIX=archive
LOAD_SOURCE=local
GCP_PROJECT_ID=disc-golf-price-compare
BIGQUERY_DATASET=DiscGolfProducts
STORE_URLS=https://foundationdiscs.com/,https://discstore.com/
```

### Supported settings

- `GOOGLE_APPLICATION_CREDENTIALS`
  Required if you want to use BigQuery or GCS from your local machine.
- `OUTPUT_MODE`
  Use `local` to write raw JSON locally, or `gcs` to upload raw JSON to Cloud Storage.
- `RAW_OUTPUT_DIR`
  Local folder used for raw JSON files.
- `PARSED_OUTPUT_DIR`
  Local folder used for parsed Parquet output.
- `AGGREGATED_OUTPUT_DIR`
  Local folder used for combined `Products.parquet` and `ProductInfo.parquet` before loading.
- `ARCHIVE_OUTPUT_DIR`
  Local folder used to archive Parquet files after a successful load.
- `RAW_BUCKET_NAME`
  Bucket used for raw JSON files.
- `PARSED_BUCKET_NAME`
  Bucket used for parsed Parquet files.
- `RAW_GCS_PREFIX`
  Prefix used for raw JSON objects in the raw bucket.
- `PARSED_GCS_PREFIX`
  Prefix used for parsed Parquet objects in the parsed bucket.
- `ARCHIVE_GCS_PREFIX`
  Prefix used to archive loaded Parquet objects in GCS.
- `LOAD_SOURCE`
  Use `local` to load BigQuery from local parsed Parquet files, or `gcs` to load from the parsed bucket.
- `GCP_PROJECT_ID`
  GCP project used for BigQuery queries and loads.
- `BIGQUERY_DATASET`
  BigQuery dataset name. Defaults to `DiscGolfProducts`.
- `STORE_URLS`
  Optional comma-separated list of store URLs. If omitted, the app queries BigQuery.
- `INFINITE_DISCS_PAGE_SIZE`
  Number of Infinite Discs results requested per API call. Defaults to `10000`.

Infinite Discs no longer uses a hardcoded total. The scraper reads `recordsTotal` from the first response and keeps paging until all records are fetched.

## 3. Run locally

### Scrape only

```powershell
python main.py scrape-shopify
```

### Parse only

```powershell
python main.py parse-shopify
```

### Load only

```powershell
python main.py load-shopify
```

That command aggregates all parsed Parquet files into one `Products.parquet` and one `ProductInfo.parquet`, then runs one BigQuery load per table.
After a successful load, local or GCS Parquet files are archived.

### Full pipeline

```powershell
python main.py run-all-shopify
```

If you run `python main.py` with no command, it defaults to `run-all`.

## Infinite Discs

### Full pipeline

```powershell
python main.py run-all-infinite-discs
```

### Step by step

```powershell
python main.py scrape-infinite-discs
python main.py parse-infinite-discs
python main.py load-infinite-discs
```

Behavior:

- raw JSON is written first
- parse converts JSON to Parquet and deletes the JSON
- load aggregates Parquet, truncates and reloads `DiscGolfProducts.InfiniteDiscs`, then archives the Parquet

## Ad Hoc Stores Load

Use this only when you need to add store rows from `input/new_disc_golf_stores.csv`:

```powershell
python loadStoresCsv.py
```

The script normalizes `URL` and `API_URL` to end with `/` and inserts only rows whose `URL` is not already present in `DiscGolfProducts.Stores`.

### Emulate the HTTP Cloud Function

```powershell
functions-framework --target hello_http
```

Then hit `http://localhost:8080/`.

## Recommended local workflow

For development, use:

- `OUTPUT_MODE=local`
- `LOAD_SOURCE=local`
- `STORE_URLS=...`

That keeps raw and parsed files on disk and lets you switch to bucket-based flow later by changing env vars.

## Parse existing raw JSON files

To parse files you already downloaded:

```powershell
python main.py parse-shopify
```
