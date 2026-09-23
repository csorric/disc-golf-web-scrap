# Local development

This project is now set up to run as a VM-style pipeline from `main.py`.

## Project layout

The implementation now lives under `disc_golf_pipeline/`:

- `disc_golf_pipeline/cli/`
  CLI entrypoints for the main pipeline and the ad hoc stores loader.
- `disc_golf_pipeline/common/`
  Shared runtime helpers such as project root and `.env` loading.
- `disc_golf_pipeline/scrapers/`
  Raw source scrapers for Shopify, Infinite Discs, Sun King Discs, OTB Discs, and related sources.
- `disc_golf_pipeline/parsers/`
  Source-specific parsing and Shopify aggregation helpers.
- `disc_golf_pipeline/loaders/`
  BigQuery and GCS/local Parquet load utilities.
- `disc_golf_pipeline/services/`
  Post-load processing and Typesense indexing services.

The root-level scripts `main.py`, `indexer.py`, `loadStoresCsv.py`, and `processData.py` are now thin wrappers that preserve the existing commands.

Main commands:

- `python main.py scrape-shopify`
  Downloads raw Shopify JSON.
- `python main.py parse-shopify`
  Converts raw JSON into compressed Parquet files.
- `python main.py load-shopify`
  Aggregates parsed Parquet files, then replaces the BigQuery `Products` and `ProductInfo` tables.
- `python main.py run-all-shopify`
  Runs scrape, parse, and load for Shopify. It does not run post-load processing or the Typesense indexer.
- `python main.py run-all-ingestion`
  Runs Shopify and Infinite Discs end to end, normalizes their products, builds and validates an immutable Typesense release, then activates `discs_prod`.
  See `RUN_ALL_INGESTION_PIPELINE.txt` for the complete execution order, LLM behavior, failure boundaries, and rollback procedure.
- `python main.py start-run-all-ingestion-job`
  Runs the complete Shopify and Infinite Discs production pipeline in a detached local worker.
- `python main.py normalize-data`
  Builds the code-managed Shopify and Infinite Discs source views, refreshes normalized products and variants, and runs normalization quality checks.
- `python main.py start-normalization-job`
  Starts `normalize-data` in a detached local worker and records its status and logs under `output/normalization-jobs/`.
- `python main.py normalization-job-status`
  Shows the latest detached normalization job, its exit status, and the tail of its output logs.
- `python main.py process-data`
  Runs normalization followed by the post-load BigQuery processing step that builds derived tables and variant changes.
- `python main.py prepare-llm-review-queue`
  Creates the constrained v2 LLM audit tables and refreshes the POSSIBLE-decision queue without making paid LLM calls.
- `python main.py run-llm-review-audit --limit <n>`
  Runs at most `<n>` paid Gemini reviews, records them for evaluation, and never promotes them into production normalization.
- `python main.py llm-review-audit-report`
  Reports review outcomes by prompt contract and representative sampling stratum, plus the latest run and token totals.
- `python main.py generate-llm-review-report`
  Rebuilds `reports/pdga_llm_review_report.html` from the current prompt contract's BigQuery audit rows.
- `python main.py promote-llm-resolutions`
  Revalidates accepted reviews against current candidates, rebuilds normalized state, and publishes a new immutable Typesense release.
- `python main.py start-llm-promotion-job`
  Runs the promotion, downstream rebuild, and Typesense publication in a detached worker.
- `python main.py index-typesense`
  Runs the incremental Typesense indexer as a standalone command.
- `python main.py create-typesense-v5`
  Creates or validates the side-by-side normalized Typesense collection without changing production traffic.
- `python main.py backfill-typesense-v5`
  Fully populates the normalized collection from BigQuery `VariantState` and validates row counts and facets.
- `python main.py start-typesense-v5-backfill-job`
  Runs the full normalized collection backfill in a detached local worker. Use `normalization-job-status` to inspect it.
- `python main.py build-typesense-release`
  Builds and validates a new timestamped, stable-keyed Typesense collection without changing an alias.
- `python main.py start-typesense-release-job`
  Runs the timestamped release build in a detached local worker.
- `python main.py publish-typesense-release`
  Builds, validates, and activates the exact immutable release created by the command.
- `python main.py start-typesense-publish-job`
  Runs release build, validation, and activation in a detached local worker.
- `python main.py validate-typesense-release`
  Revalidates the latest release against the current `VariantState` and refuses stale source batches without changing the alias.
- `python main.py activate-typesense-release`
  Revalidates and atomically points the stable production alias at the latest validated release.
- `python main.py rollback-typesense-release`
  Points the stable alias back to the collection recorded before the current activation.
- `python main.py typesense-release-status`
  Shows the alias target and latest BigQuery deployment-audit record.
- `python main.py typesense-cleanup-status`
  Checks whether the previous collection is safe to delete without modifying Typesense.
- `python main.py delete-previous-typesense-collection --confirm-collection <name>`
  Deletes only the audited previous collection after exact-name confirmation and safety checks.
- `python main.py run-all`
  Runs the default Shopify pipeline, post-load processing, then publishes an immutable Typesense release.
- `python main.py scrape-infinite-discs`
  Downloads raw Infinite Discs JSON separately from the Shopify flow.
- `python main.py parse-infinite-discs`
  Parses Infinite Discs JSON into Parquet and deletes the source JSON.
- `python main.py load-infinite-discs`
  Aggregates Infinite Discs Parquet, truncates and reloads the table, then archives the Parquet files.
- `python main.py run-all-infinite-discs`
  Runs scrape, parse, and load for Infinite Discs. It does not run post-load processing or the Typesense indexer.
- `python loadStoresCsv.py`
  Ad hoc loader for new rows into `DiscGolfProducts.Stores`; inserts only URLs not already present.
- `functions-framework --target hello_http`
  Keeps the HTTP entrypoint available if you still want it.

Logs are written to `logs/<command>-YYYYMMDD.log` and also printed to the terminal.

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
- `BQ_VARIANT_CHANGES_TABLE`
  Fully-qualified BigQuery table used by the indexer, for example `disc-golf-price-compare.DiscGolfProducts.VariantChanges`.
- `BQ_VARIANT_STATE_TABLE`
  Optional fully-qualified table used for full Typesense backfills. If omitted, it is derived from `BQ_VARIANT_CHANGES_TABLE`.
- `INDEXER_RUNS_TABLE`
  Optional fully-qualified BigQuery checkpoint table for indexer runs. If omitted, the app uses `project.dataset.IndexerRuns` derived from `BQ_VARIANT_CHANGES_TABLE`.
- `TYPESENSE_HOST`
  Typesense host URL for the incremental indexer.
- `TYPESENSE_ADMIN_KEY`
  Typesense admin API key for the incremental indexer.
- `TYPESENSE_COLLECTION`
  Optional Typesense collection name. Defaults to `discs_v4`.
- `TYPESENSE_V5_COLLECTION`
  Side-by-side normalized collection used by the creation and full-backfill commands. Defaults to `discs_v5` and must differ from `TYPESENSE_COLLECTION`.
- `TYPESENSE_RELEASE_PREFIX`
  Prefix for immutable timestamped release collections. Defaults to `discs`.
- `TYPESENSE_RELEASE_COLLECTION`
  Optional explicit physical collection name for a release build. Normally omitted so a unique timestamped name is generated.
- `TYPESENSE_PRODUCTION_ALIAS`
  Stable application alias recorded with deployments. Defaults to `discs_prod`; release builds do not activate it, and activation must be run explicitly.
- `TYPESENSE_DEPLOYMENTS_TABLE`
  Optional fully-qualified BigQuery release-audit table. Defaults to `project.dataset.TypesenseDeployments` beside `VariantState`.
- `INDEXER_BATCH_SIZE`
  Optional indexer batch size. Defaults to `200`.
- `LLM_RESOLUTION_MODE`
  Constrained resolver mode for standalone normalization: `off`, `audit`, or `promote`. Defaults to `off`. `run-all-ingestion` safely controls its own audit and promotion phases.
- `LLM_MODEL_NAME`
  Versioned model name included in evidence hashes. Defaults to `gemini-2.5-flash-lite`.
- `LLM_BIGQUERY_MODEL`
  Existing BigQuery remote-model object used by audit calls. Defaults to `DiscStandardizationLlm`.
- `LLM_PROMPT_VERSION`
  Prompt-contract version included in evidence hashes. Defaults to `disc-model-resolver-v2-2`.
- `LLM_CANDIDATE_LIMIT`
  Maximum supplied candidates per ambiguous review. Defaults to `3` and is restricted to `1` through `5`.
- `LLM_MAX_CALLS_PER_RUN`
  Per-run paid-call ceiling for standalone audit commands. Defaults to `100`.
- `LLM_FULL_INGESTION_MAX_CALLS`
  Fail-closed paid-call ceiling for `run-all-ingestion`. Defaults to `10000`; publication is blocked when the pending queue exceeds it.
- `STORE_URLS`
  Optional comma-separated list of store URLs. If omitted, the app queries BigQuery.
- `INFINITE_DISCS_PAGE_SIZE`
  Number of Infinite Discs results requested per API call. Defaults to `2000`.
- `INFINITE_DISCS_MIN_PAGE_SIZE`
  Lowest Infinite Discs page size the scraper will fall back to after repeated upstream failures. Defaults to `250`.

Infinite Discs no longer uses a hardcoded total. The scraper reads `recordsTotal` from the first response and keeps paging until all records are fetched.
If the upstream endpoint returns repeated `5xx` errors, the scraper now retries and reduces page size automatically before failing.

### Typesense release workflow

For a single production publish, build an immutable physical collection, validate it, and move the stable alias:

```powershell
python main.py start-typesense-publish-job
python main.py normalization-job-status
python main.py typesense-release-status
```

`publish-typesense-release` activates only the deployment ID it just built. A failed build or validation never changes the alias. Activation records the previous collection in BigQuery so `python main.py rollback-typesense-release` can restore it.

After application smoke testing, inspect and then explicitly delete the rollback collection:

```powershell
python main.py typesense-cleanup-status
python main.py delete-previous-typesense-collection --confirm-collection discs_v4
```

Replace `discs_v4` with the exact `previous_collection` returned by the status command. Cleanup refuses to delete the active collection, refuses mismatched confirmation, checks that the production alias still targets the active deployment, and checks that no other alias targets the previous collection. Deletion is permanent and disables rollback to that collection.

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

### Normalization only

```powershell
python main.py normalize-data
```

That command refreshes the Shopify and Infinite Discs normalization layer without changing `VariantState`, `VariantChanges`, or Typesense. It creates or updates storefront rules, normalized products, the normalized variant snapshot, the normalization audit, and quality-report views. The command fails when a required quality check does not pass.

To run the same command independently of the current terminal or Codex session:

```powershell
python main.py start-normalization-job
python main.py normalization-job-status
```

The start command refuses to create an overlapping job while the latest worker is active. Each job stores `status.json`, `stdout.log`, and `stderr.log` in its own ignored directory under `output/normalization-jobs/`. A `succeeded` status and exit code `0` mean the normalization command finished and all of its required quality checks passed. The detached job still requires the computer to remain awake and connected to BigQuery.

Model normalization is deterministic and versioned. It builds the PDGA-derived `DiscModelEntities` and `DiscModelAliases` catalogs, then refreshes product and variant candidate and decision tables. Only decisions with compatible manufacturer evidence and a single credible model are promoted into normalized search fields. Weaker matches remain `POSSIBLE` or `UNRESOLVED` for review and are not exposed as normalized models.

To prepare the constrained v2 LLM queue without making LLM calls:

```powershell
python main.py prepare-llm-review-queue
```

The command creates `DiscModelLlmReviews` and `DiscModelLlmRuns`, then rebuilds `DiscModelLlmQueue` from product- and variant-level `POSSIBLE` decisions. Each row contains at most the configured number of supplied entity IDs and a deterministic evidence hash over the product evidence, candidates, rules, exact prompt contract, prompt version, and model name. A terminal review with the same evidence hash and prompt hash is marked `CACHED`. The implementation checklist is in `LLM_V2_IMPLEMENTATION_CHECKLIST.txt`.

To run a small audit-only pilot in PowerShell:

```powershell
$env:LLM_RESOLUTION_MODE='audit'
$env:LLM_MAX_CALLS_PER_RUN='10'
python main.py run-llm-review-audit --limit 10
```

The command first writes exactly the capped pending rows to `DiscModelLlmBatchInput`, selecting round-robin from variant, multi-candidate, generic-alias, parenthetical-alias, missing-manufacturer, and other product strata. It then calls the existing `DiscStandardizationLlm` remote model with BigQuery `AI.GENERATE_TEXT`. The prompt treats catalog content as untrusted data, supplies only deterministic candidate IDs, disables thinking, and requires structured JSON containing a supplied ID or `NONE`. Audit mode stores requests, responses, validation failures, model metadata, and token counts but does not change `NormalizedProducts`, `VariantState`, or Typesense.

Inspect accumulated audit results without making any paid calls:

```powershell
python main.py llm-review-audit-report
```

After an audit set has been approved, promotion remains a separate operation:

```powershell
python main.py start-llm-promotion-job
python main.py normalization-job-status
```

Promotion writes an append-only `DiscModelLlmResolutions` record only when the accepted entity ID remains in the current candidate list and the evidence, prompt contract, model, and rules still match. Deterministic `ACCEPT` decisions retain priority. The worker then rebuilds `NormalizedProducts`, `NormalizedVariantSnapshot`, `VariantState`, and `VariantChanges` before publishing and atomically activating a new Typesense release. `NONE`, invalid, failed, and stale reviews remain unresolved.

Review and validation objects include:

- `ProductDiscModelCandidates` and `ProductDiscModelDecisions`
- `VariantDiscModelCandidates` and `VariantDiscModelDecisions`
- `v_ModelNormalizationQualityReport` and `v_ModelNormalizationQualityChecks`
- `v_ModelNormalizationComparison` and `v_ModelNormalizationReviewSample`

The quality checks reject generic or context-only aliases without sufficient evidence, multiple accepted models, model assignments on non-disc products, and model overrides on miniature, marker, or keychain variants.

### Post-load processing only

```powershell
python main.py process-data
```

That command runs normalization first, then rebuilds `DiscGolfProducts.DerivedProductType` and refreshes `VariantState` and `VariantChanges`.

`VariantState` and `VariantChanges` retain their existing `id` values and also carry `source_variant_key`, a stable logical identity intended for cross-run history. `VariantIdentityMap` preserves the relationship between each legacy ID and stable source key. Shopify keys use store plus Shopify variant ID; Infinite keys use the existing logical product/variant fingerprints so duplicate stock rows do not expand the search inventory.

### Legacy incremental Typesense indexing

```powershell
python main.py index-typesense
```

That command runs the legacy incremental Typesense indexer against the latest unprocessed `batch_run_id`. It remains available for diagnostics and compatibility, but production `run-all` commands now publish complete immutable releases instead.

To build the normalized search collection without switching production:

```powershell
python main.py create-typesense-v5
python main.py start-typesense-v5-backfill-job
python main.py normalization-job-status
```

The v5 backfill streams the complete `VariantState` table into a separate collection, checks every bulk-import response, and validates that the BigQuery ID count, Typesense document count, wildcard search count, schema, and normalized facets agree. It refuses to target the collection configured by `TYPESENSE_COLLECTION`.

To build and validate without activation, use:

```powershell
python main.py start-typesense-release-job
python main.py normalization-job-status
```

Each release uses `source_variant_key` as its Typesense document ID, retains the previous ID in `legacy_id`, and records its source batch, schema version, counts, validation output, status, and errors in `TypesenseDeployments`. A successful build ends in `VALIDATED`; it does not create or switch the production alias.

To build, validate, and activate in one guarded production operation, use:

```powershell
python main.py start-typesense-publish-job
python main.py normalization-job-status
```

You can still run the standalone module directly:

```powershell
python indexer.py
```

### Full pipeline

```powershell
python main.py run-all-shopify
```

That command runs Shopify scrape, parse, and load only.
It does not run `process-data` or `index-typesense`.

If you run `python main.py` with no command, it defaults to `run-all`.
The default `run-all` command also runs normalization and `process-data`, builds and validates a new immutable Typesense release, and activates `discs_prod` after validation succeeds.

To run Shopify and Infinite Discs together through production release publication:

```powershell
python main.py run-all-ingestion
```

The detailed operational guide is in `RUN_ALL_INGESTION_PIPELINE.txt`.

That command runs:

- every enabled Shopify store returned by the configured `Stores` table (or `STORE_URLS`), followed by parse and load
- Infinite Discs scrape, parse, and load
- deterministic normalization and current-candidate queue construction
- constrained Gemini review for every uncached `POSSIBLE` decision within the configured hard cap
- current-candidate validation and versioned promotion of accepted IDs
- a second normalization pass plus `VariantState` and `VariantChanges` refresh
- build a timestamped Typesense release from `VariantState`
- validate stable identities, schema, document count, wildcard count, and facets
- atomically activate `discs_prod`

Sun King and OTB remain standalone commands and are not included in `run-all-ingestion`. Cached reviews are not billed again unless evidence, candidates, rules, prompt contract, or model changes. `NONE` and invalid responses remain unresolved. A remote LLM failure, an exceeded call cap, a promotion validation failure, or any normalization failure stops the command before Typesense activation.

For a detached run that survives closing this session:

```powershell
python main.py start-run-all-ingestion-job
python main.py normalization-job-status
```

## Infinite Discs

### Full pipeline

```powershell
python main.py run-all-infinite-discs
```

That command runs Infinite Discs scrape, parse, and load only.
It does not run `process-data` or `index-typesense`.

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

