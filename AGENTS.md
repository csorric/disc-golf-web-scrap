# Repository Guidelines

## Project Structure & Module Organization
This repository is a Python data pipeline for disc golf store ingestion.

- `main.py`: primary CLI entrypoint for Shopify and Infinite Discs workflows.
- `getProductJson.py`, `runJsonParser.py`, `jsonParser.py`, `loadShopifyDiscGolfData.py`: Shopify scrape, parse, and load flow.
- `infinite_discs.py`, `parseInfiniteDiscs.py`, `loadInfiniteDiscsData.py`: Infinite Discs scrape, parse, and load flow.
- `loadStoresCsv.py`: ad hoc loader for `DiscGolfProducts.Stores`.
- `input/`: manual input files such as `new_disc_golf_stores.csv`.
- `output/`: generated raw JSON, parsed Parquet, aggregated Parquet, and archive files.

There is no `tests/` directory yet.

## Build, Test, and Development Commands
Set up the local environment:

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Main commands:

- `python main.py scrape-shopify`: download Shopify raw JSON.
- `python main.py parse-shopify`: parse Shopify JSON to Parquet and delete raw JSON.
- `python main.py load-shopify`: aggregate Shopify Parquet, load BigQuery, archive Parquet.
- `python main.py run-all-shopify`: run the full Shopify pipeline.
- `python main.py run-all-infinite-discs`: run the full Infinite Discs pipeline.
- `python loadStoresCsv.py`: insert only new store URLs into `DiscGolfProducts.Stores`.
- `python -m py_compile main.py loadStoresCsv.py`: quick syntax check.

## Coding Style & Naming Conventions
Use 4-space indentation and standard Python style. Prefer small functions and explicit names.

- `snake_case` for functions and variables
- `UPPER_CASE` for constants
- platform-specific modules named by source, for example `infinite_discs.py`

Keep generated data out of source files; write to `output/`.

## Testing Guidelines
No automated test suite exists yet. Before submitting changes:

- run `python -m py_compile` on edited modules
- run the affected CLI command locally
- verify outputs under `output/`
- confirm BigQuery/GCS behavior only when credentials are configured

If tests are added later, place them in `tests/` and name them `test_*.py`.

## Commit & Pull Request Guidelines
This repository has little or no useful commit history, so use short imperative commit messages such as `Add Infinite Discs parser`.

Pull requests should include:

- a concise behavior summary
- commands used for verification
- any `.env`, GCP, BigQuery, or bucket changes
- notes on whether the change affects Shopify, Infinite Discs, or the ad hoc stores loader

## Security & Configuration Tips
Never commit `.env`, credentials, or generated JSON/Parquet output. Store GCP settings in `.env`, especially `GOOGLE_APPLICATION_CREDENTIALS`, bucket names, and dataset names.
