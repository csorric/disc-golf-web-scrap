# Repository Guidelines

## Project Structure & Module Organization
This repository is a Python data pipeline for Shopify-based disc golf stores.

- `main.py`: primary VM entrypoint; runs `scrape`, `parse`, `load`, or `run-all`.
- `getProductJson.py`: downloads raw `products.json` pages from each store.
- `jsonParser.py`: converts Shopify product JSON into product and variant records.
- `runJsonParser.py`: batch parser for local files or GCS objects.
- `loadShopifyDiscGolfData.py`: loads parsed CSV files into BigQuery tables.
- `output/raw-data/`: downloaded JSON files.
- `output/parsed-data/`: parsed CSV files.

There is no `tests/` directory yet.

## Build, Test, and Development Commands
Create and activate the environment, then install dependencies:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Key commands:

- `python main.py scrape`: download raw Shopify JSON.
- `python main.py parse`: parse raw JSON into CSV.
- `python main.py load`: replace BigQuery `Products` and `ProductInfo`.
- `python main.py run-all`: run the full pipeline.
- `python -m py_compile main.py getProductJson.py jsonParser.py runJsonParser.py loadShopifyDiscGolfData.py`: quick syntax check.

## Coding Style & Naming Conventions
Use 4-space indentation and follow standard Python style. Prefer small functions, clear names, and minimal inline comments. Use:

- `snake_case` for functions and variables
- `UPPER_CASE` for module-level constants
- descriptive file names matching the pipeline step, such as `loadShopifyDiscGolfData.py`

Keep new code ASCII unless the file already requires Unicode data handling.

## Testing Guidelines
There is no automated test suite yet. Before submitting changes:

- run `python -m py_compile ...`
- run the affected pipeline command locally
- verify outputs in `output/raw-data/` or `output/parsed-data/`

If you add tests, place them under `tests/` and use `test_*.py`.

## Commit & Pull Request Guidelines
This repository currently has no commit history, so no established commit convention exists yet. Use short, imperative commit messages such as `Add BigQuery load mode`.

For pull requests, include:

- a concise summary of behavior changes
- any `.env` or GCP configuration changes
- sample commands used for verification
- notes on whether the change affects local mode, GCS mode, or both

## Security & Configuration Tips
Do not commit `.env`, credentials, or generated output unless explicitly needed. Prefer environment variables for bucket names, dataset names, and store overrides. Validate changes in local mode before switching to GCS or BigQuery-backed runs.
