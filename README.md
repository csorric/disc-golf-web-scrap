# Local development

This project is now set up to run as a VM-style pipeline from `main.py`.

Main commands:

- `python main.py scrape`
  Downloads raw Shopify JSON.
- `python main.py parse`
  Converts raw JSON into parsed CSV files.
- `python main.py load`
  Replaces the BigQuery `Products` and `ProductInfo` tables from parsed CSV files.
- `python main.py run-all`
  Runs scrape, parse, and load in sequence.
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
RAW_BUCKET_NAME=disc-golf-web-data
PARSED_BUCKET_NAME=disc-golf-parsed-files
RAW_GCS_PREFIX=raw-data
PARSED_GCS_PREFIX=parsed-data
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
  Local folder used for parsed CSV output.
- `RAW_BUCKET_NAME`
  Bucket used for raw JSON files.
- `PARSED_BUCKET_NAME`
  Bucket used for parsed CSV files.
- `RAW_GCS_PREFIX`
  Prefix used for raw JSON objects in the raw bucket.
- `PARSED_GCS_PREFIX`
  Prefix used for parsed CSV objects in the parsed bucket.
- `LOAD_SOURCE`
  Use `local` to load BigQuery from local parsed CSV files, or `gcs` to load from the parsed bucket.
- `GCP_PROJECT_ID`
  GCP project used for BigQuery queries and loads.
- `BIGQUERY_DATASET`
  BigQuery dataset name. Defaults to `DiscGolfProducts`.
- `STORE_URLS`
  Optional comma-separated list of store URLs. If omitted, the app queries BigQuery.

## 3. Run locally

### Scrape only

```powershell
python main.py scrape
```

### Parse only

```powershell
python main.py parse
```

### Load only

```powershell
python main.py load
```

### Full pipeline

```powershell
python main.py run-all
```

If you run `python main.py` with no command, it defaults to `run-all`.

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
python main.py parse
```
