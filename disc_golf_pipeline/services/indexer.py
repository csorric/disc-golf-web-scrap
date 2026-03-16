import argparse
import ast
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from google.cloud import bigquery
from disc_golf_pipeline.common.runtime import LOG_DIR, load_env_file

DEFAULT_COLLECTION = "discs_v4"
DEFAULT_BATCH_SIZE = 200
DEFAULT_LOG_FILE = LOG_DIR / "indexer.log"
URL_PATTERN = re.compile(r"^https?://.+", re.IGNORECASE)


load_env_file()


def configure_logging() -> Path:
    log_file = Path(os.getenv("INDEXER_LOG_FILE", str(DEFAULT_LOG_FILE)))
    log_file.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[stream_handler, file_handler],
        force=True,
    )
    return log_file


def sanitize_text(value: str) -> str:
    if not value:
        return ""
    return value.encode("utf-8", errors="ignore").decode("utf-8")


def safe_string(value) -> str:
    if value is None:
        return ""
    return sanitize_text(str(value))


def safe_int(value) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def safe_float(value) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def safe_bool(value) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()
    return normalized in {"true", "1", "yes"}


def is_valid_url(value) -> bool:
    return bool(value and URL_PATTERN.match(str(value).strip()))


def parse_tags(raw_value) -> List[str]:
    if not raw_value:
        return []

    if isinstance(raw_value, list):
        return [sanitize_text(str(item)) for item in raw_value]

    if isinstance(raw_value, tuple):
        return [sanitize_text(str(item)) for item in raw_value]

    text = str(raw_value).strip()
    if not text:
        return []

    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
        except (SyntaxError, ValueError):
            parsed = None

        if isinstance(parsed, (list, tuple)):
            return [sanitize_text(str(item)) for item in parsed]

    return [sanitize_text(tag.strip()) for tag in text.split(",") if tag.strip()]


def chunked(items: List, batch_size: int) -> Iterable[List]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def derive_indexer_runs_table_name(variant_changes_table: str) -> str:
    if not variant_changes_table:
        return ""

    parts = variant_changes_table.split(".")
    if len(parts) != 3:
        raise ValueError(
            "BQ_VARIANT_CHANGES_TABLE must be in project.dataset.table format to derive INDEXER_RUNS_TABLE."
        )
    project_id, dataset, _ = parts
    return f"{project_id}.{dataset}.IndexerRuns"


def get_bigquery_client() -> bigquery.Client:
    project_id = os.getenv("GCP_PROJECT_ID", "").strip() or None
    return bigquery.Client(project=project_id)


def get_latest_batch_id(client: bigquery.Client, table_name: str) -> Optional[str]:
    query = f"""
        SELECT batch_run_id, batch_run_ts
        FROM `{table_name}`
        ORDER BY batch_run_ts DESC
        LIMIT 1
    """
    rows = list(client.query(query).result())
    if not rows:
        return None
    return safe_string(rows[0].get("batch_run_id"))


def iterate_changes_for_batch(
    client: bigquery.Client,
    table_name: str,
    batch_run_id: str,
):
    query = f"""
        WITH per_batch AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY id
              ORDER BY change_ts DESC, operation DESC
            ) AS rn
          FROM `{table_name}`
          WHERE batch_run_id = @batch_run_id
        )
        SELECT
          id,
          product_id,
          variant_id,
          operation,
          title,
          vendor,
          product_link,
          store,
          store_url,
          image,
          variant_title,
          price,
          weight_g,
          in_stock,
          variant_image,
          high_price,
          low_price,
          tags,
          IsDistanceDriver,
          IsFairwayDriver,
          IsMidrange,
          IsPutter,
          BodyHtml,
          product_type,
          row_hash,
          change_ts
        FROM per_batch
        WHERE rn = 1
        ORDER BY id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_run_id", "STRING", batch_run_id),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    return query_job.result(page_size=1000)


def ensure_indexer_runs_table(client: bigquery.Client, table_name: str) -> None:
    query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
          batch_run_id STRING NOT NULL,
          status STRING NOT NULL,
          started_at TIMESTAMP,
          completed_at TIMESTAMP,
          rows_seen INT64,
          upserts INT64,
          deletes INT64,
          unknown_ops INT64,
          upsert_batches INT64,
          delete_batches INT64,
          error_message STRING,
          updated_at TIMESTAMP
        )
    """
    client.query(query).result()


def is_batch_completed(client: bigquery.Client, table_name: str, batch_run_id: str) -> bool:
    query = f"""
        SELECT status
        FROM `{table_name}`
        WHERE batch_run_id = @batch_run_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_run_id", "STRING", batch_run_id),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return False
    return safe_string(rows[0].get("status")).upper() == "COMPLETED"


def upsert_indexer_run(
    client: bigquery.Client,
    table_name: str,
    batch_run_id: str,
    status: str,
    summary: Optional[Dict] = None,
    error_message: str = "",
) -> None:
    summary = summary or {}
    query = f"""
        MERGE `{table_name}` AS target
        USING (
          SELECT
            @batch_run_id AS batch_run_id,
            @status AS status,
            @rows_seen AS rows_seen,
            @upserts AS upserts,
            @deletes AS deletes,
            @unknown_ops AS unknown_ops,
            @upsert_batches AS upsert_batches,
            @delete_batches AS delete_batches,
            @error_message AS error_message
        ) AS source
        ON target.batch_run_id = source.batch_run_id
        WHEN MATCHED THEN
          UPDATE SET
            status = source.status,
            started_at = IF(source.status = 'STARTED', COALESCE(target.started_at, CURRENT_TIMESTAMP()), target.started_at),
            completed_at = IF(source.status = 'COMPLETED', CURRENT_TIMESTAMP(), target.completed_at),
            rows_seen = source.rows_seen,
            upserts = source.upserts,
            deletes = source.deletes,
            unknown_ops = source.unknown_ops,
            upsert_batches = source.upsert_batches,
            delete_batches = source.delete_batches,
            error_message = source.error_message,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (
            batch_run_id,
            status,
            started_at,
            completed_at,
            rows_seen,
            upserts,
            deletes,
            unknown_ops,
            upsert_batches,
            delete_batches,
            error_message,
            updated_at
          )
          VALUES (
            source.batch_run_id,
            source.status,
            IF(source.status = 'STARTED', CURRENT_TIMESTAMP(), NULL),
            IF(source.status = 'COMPLETED', CURRENT_TIMESTAMP(), NULL),
            source.rows_seen,
            source.upserts,
            source.deletes,
            source.unknown_ops,
            source.upsert_batches,
            source.delete_batches,
            source.error_message,
            CURRENT_TIMESTAMP()
          )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_run_id", "STRING", batch_run_id),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("rows_seen", "INT64", summary.get("rows_seen", 0)),
            bigquery.ScalarQueryParameter("upserts", "INT64", summary.get("upserts", 0)),
            bigquery.ScalarQueryParameter("deletes", "INT64", summary.get("deletes", 0)),
            bigquery.ScalarQueryParameter("unknown_ops", "INT64", summary.get("unknown_ops", 0)),
            bigquery.ScalarQueryParameter("upsert_batches", "INT64", summary.get("upsert_batches", 0)),
            bigquery.ScalarQueryParameter("delete_batches", "INT64", summary.get("delete_batches", 0)),
            bigquery.ScalarQueryParameter("error_message", "STRING", sanitize_text(error_message)[:5000]),
        ]
    )
    client.query(query, job_config=job_config).result()


def send_upsert_batch(
    session: requests.Session,
    host: str,
    admin_key: str,
    collection: str,
    docs: List[Dict],
    batch_index: int,
    timeout_seconds: int = 30,
) -> Dict:
    if not docs:
        return {"ok": True}

    url = f"{host.rstrip('/')}/collections/{collection}/documents/import?action=upsert"
    body = "\n".join(json_line(doc) for doc in docs)

    for attempt in range(1, 4):
        try:
            response = session.post(
                url,
                headers={
                    "Content-Type": "text/plain",
                    "X-TYPESENSE-API-KEY": admin_key,
                },
                data=body.encode("utf-8"),
                timeout=timeout_seconds,
            )
            text = response.text
            if response.ok:
                logging.info(
                    "UPSERT batch %s imported on attempt %s. Resp head: %s",
                    batch_index,
                    attempt,
                    text[:300],
                )
                return {"ok": True, "status": response.status_code, "text": text}

            logging.error(
                "UPSERT batch %s attempt %s failed: %s %s",
                batch_index,
                attempt,
                response.status_code,
                text[:400],
            )
        except requests.RequestException as exc:
            logging.error(
                "UPSERT batch %s attempt %s exception: %s",
                batch_index,
                attempt,
                str(exc)[:300],
            )

        if attempt < 3:
            time.sleep(0.5 * attempt)

    return {"ok": False}


def delete_document_by_id(
    session: requests.Session,
    host: str,
    admin_key: str,
    collection: str,
    document_id: str,
    batch_index: int,
    item_index: int,
    timeout_seconds: int = 30,
) -> Dict:
    url = f"{host.rstrip('/')}/collections/{collection}/documents/{requests.utils.quote(document_id, safe='')}"

    for attempt in range(1, 4):
        try:
            response = session.delete(
                url,
                headers={"X-TYPESENSE-API-KEY": admin_key},
                timeout=timeout_seconds,
            )
            text = response.text
            if response.ok or response.status_code == 404:
                return {
                    "ok": True,
                    "status": response.status_code,
                    "text": text,
                    "id": document_id,
                    "deleted": response.status_code != 404,
                }

            logging.error(
                "DELETE batch %s item %s attempt %s failed: %s %s",
                batch_index,
                item_index,
                attempt,
                response.status_code,
                text[:400],
            )
        except requests.RequestException as exc:
            logging.error(
                "DELETE batch %s item %s attempt %s exception: %s",
                batch_index,
                item_index,
                attempt,
                str(exc)[:300],
            )

        if attempt < 3:
            time.sleep(0.5 * attempt)

    return {"ok": False, "id": document_id}


def send_delete_batch(
    session: requests.Session,
    host: str,
    admin_key: str,
    collection: str,
    ids: List[str],
    batch_index: int,
) -> Dict:
    if not ids:
        return {"ok": True, "deleted_count": 0, "missing_count": 0}

    deleted_count = 0
    missing_count = 0
    for item_index, document_id in enumerate(ids, start=1):
        result = delete_document_by_id(
            session=session,
            host=host,
            admin_key=admin_key,
            collection=collection,
            document_id=document_id,
            batch_index=batch_index,
            item_index=item_index,
        )
        if not result.get("ok"):
            return result
        if result.get("deleted"):
            deleted_count += 1
        else:
            missing_count += 1

    logging.info(
        "DELETE batch %s completed. Deleted: %s. Missing: %s.",
        batch_index,
        deleted_count,
        missing_count,
    )
    return {"ok": True, "deleted_count": deleted_count, "missing_count": missing_count}


def json_line(document: Dict) -> str:
    import json

    return sanitize_text(json.dumps(document, separators=(",", ":"), ensure_ascii=False))


def build_document(row) -> Dict:
    tags = parse_tags(row.get("tags"))
    document = {
        "id": safe_string(row.get("id")),
        "product_id": safe_string(row.get("product_id")),
        "variant_id": safe_string(row.get("variant_id")),
        "title": safe_string(row.get("title")),
        "vendor": safe_string(row.get("vendor")),
        "product_link": safe_string(row.get("product_link")),
        "store": safe_string(row.get("store")),
        "store_url": safe_string(row.get("store_url")),
        "image": safe_string(row.get("image")),
        "variant_title": safe_string(row.get("variant_title")),
        "price": safe_float(row.get("price")),
        "weight_g": safe_int(row.get("weight_g")),
        "in_stock": safe_bool(row.get("in_stock")),
        "variant_image": safe_string(row.get("variant_image")),
        "high_price": safe_float(row.get("high_price")),
        "low_price": safe_float(row.get("low_price")),
        "tags": tags,
        "IsDistanceDriver": safe_bool(row.get("IsDistanceDriver")),
        "IsFairwayDriver": safe_bool(row.get("IsFairwayDriver")),
        "IsMidrange": safe_bool(row.get("IsMidrange")),
        "IsPutter": safe_bool(row.get("IsPutter")),
        "BodyHtml": safe_string(row.get("BodyHtml")),
        "product_type": safe_string(row.get("product_type")),
        "search_text": " ".join(
            part
            for part in [
                safe_string(row.get("title")),
                safe_string(row.get("variant_title")),
                safe_string(row.get("vendor")),
                safe_string(row.get("store")),
                " ".join(tags),
            ]
            if part
        ).lower(),
        "last_indexed_at": int(time.time() * 1000),
    }

    if not is_valid_url(document["image"]):
        document["image"] = ""
    if not is_valid_url(document["variant_image"]):
        document["variant_image"] = ""

    return document


def validate_required_env(host: str, admin_key: str, table_name: str) -> None:
    if not host or not admin_key:
        raise ValueError("Missing env vars TYPESENSE_HOST and/or TYPESENSE_ADMIN_KEY.")
    if not table_name:
        raise ValueError(
            "Missing env var BQ_VARIANT_CHANGES_TABLE. Example: disc-golf-price-compare.DiscGolfProducts.VariantChanges"
        )


def run_indexer(batch_run_id: Optional[str] = None) -> Dict:
    host = os.getenv("TYPESENSE_HOST", "").strip()
    admin_key = os.getenv("TYPESENSE_ADMIN_KEY", "").strip()
    collection = os.getenv("TYPESENSE_COLLECTION", DEFAULT_COLLECTION).strip() or DEFAULT_COLLECTION
    table_name = os.getenv("BQ_VARIANT_CHANGES_TABLE", "").strip()
    indexer_runs_table = os.getenv("INDEXER_RUNS_TABLE", "").strip()
    batch_size = safe_int(os.getenv("INDEXER_BATCH_SIZE", DEFAULT_BATCH_SIZE)) or DEFAULT_BATCH_SIZE
    forced_batch_run_id = batch_run_id or os.getenv("BQ_BATCH_RUN_ID", "").strip() or None

    validate_required_env(host, admin_key, table_name)

    client = get_bigquery_client()
    resolved_indexer_runs_table = indexer_runs_table or derive_indexer_runs_table_name(table_name)
    ensure_indexer_runs_table(client, resolved_indexer_runs_table)

    resolved_batch_run_id = forced_batch_run_id or get_latest_batch_id(client, table_name)
    if not resolved_batch_run_id:
        logging.info("No batches found in VariantChanges. Nothing to index.")
        return {
            "batch_run_id": None,
            "checkpoint_table": resolved_indexer_runs_table,
            "rows_seen": 0,
            "upserts": 0,
            "deletes": 0,
            "unknown_ops": 0,
            "upsert_batches": 0,
            "delete_batches": 0,
            "skipped": False,
        }

    if is_batch_completed(client, resolved_indexer_runs_table, resolved_batch_run_id):
        logging.info(
            "Batch %s is already marked COMPLETED in %s. Skipping.",
            resolved_batch_run_id,
            resolved_indexer_runs_table,
        )
        return {
            "batch_run_id": resolved_batch_run_id,
            "checkpoint_table": resolved_indexer_runs_table,
            "rows_seen": 0,
            "upserts": 0,
            "deletes": 0,
            "unknown_ops": 0,
            "upsert_batches": 0,
            "delete_batches": 0,
            "skipped": True,
        }

    logging.info("Starting indexer for batch_run_id: %s", resolved_batch_run_id)
    logging.info("Typesense collection: %s Batch size: %s", collection, batch_size)
    logging.info("Indexer checkpoint table: %s", resolved_indexer_runs_table)

    total_rows_seen = 0
    total_upserts = 0
    total_deletes = 0
    unknown_ops = 0
    upsert_batch_index = 0
    delete_batch_index = 0

    pending_upserts: List[Dict] = []
    pending_deletes: List[str] = []

    session = requests.Session()
    upsert_indexer_run(
        client=client,
        table_name=resolved_indexer_runs_table,
        batch_run_id=resolved_batch_run_id,
        status="STARTED",
    )

    try:
        for row in iterate_changes_for_batch(client, table_name, resolved_batch_run_id):
            total_rows_seen += 1
            operation = safe_string(row.get("operation")).upper()

            if operation == "DELETE":
                document_id = safe_string(row.get("id"))
                if not document_id:
                    continue
                pending_deletes.append(document_id)
                total_deletes += 1

                if len(pending_deletes) >= batch_size:
                    delete_batch_index += 1
                    logging.info(
                        "Sending DELETE batch %s (up to total deletes %s)",
                        delete_batch_index,
                        total_deletes,
                    )
                    result = send_delete_batch(
                        session=session,
                        host=host,
                        admin_key=admin_key,
                        collection=collection,
                        ids=pending_deletes,
                        batch_index=delete_batch_index,
                    )
                    if not result.get("ok"):
                        logging.error("DELETE batch %s error; continuing. %s", delete_batch_index, result)
                    pending_deletes = []
                continue

            if operation != "UPSERT":
                unknown_ops += 1
                logging.warning("Unknown operation, skipping row: %s %s", operation, row.get("id"))
                continue

            pending_upserts.append(build_document(row))
            total_upserts += 1

            if len(pending_upserts) >= batch_size:
                upsert_batch_index += 1
                logging.info(
                    "Sending UPSERT batch %s (up to total upserts %s)",
                    upsert_batch_index,
                    total_upserts,
                )
                result = send_upsert_batch(
                    session=session,
                    host=host,
                    admin_key=admin_key,
                    collection=collection,
                    docs=pending_upserts,
                    batch_index=upsert_batch_index,
                )
                if not result.get("ok"):
                    logging.error("UPSERT batch %s error; continuing. %s", upsert_batch_index, result)
                pending_upserts = []

        if pending_upserts:
            upsert_batch_index += 1
            logging.info(
                "Sending final UPSERT batch %s (up to total upserts %s)",
                upsert_batch_index,
                total_upserts,
            )
            result = send_upsert_batch(
                session=session,
                host=host,
                admin_key=admin_key,
                collection=collection,
                docs=pending_upserts,
                batch_index=upsert_batch_index,
            )
            if not result.get("ok"):
                logging.error("Final UPSERT batch %s error %s", upsert_batch_index, result)

        if pending_deletes:
            delete_batch_index += 1
            logging.info(
                "Sending final DELETE batch %s (up to total deletes %s)",
                delete_batch_index,
                total_deletes,
            )
            result = send_delete_batch(
                session=session,
                host=host,
                admin_key=admin_key,
                collection=collection,
                ids=pending_deletes,
                batch_index=delete_batch_index,
            )
            if not result.get("ok"):
                logging.error("Final DELETE batch %s error %s", delete_batch_index, result)
    except Exception as exc:
        failure_summary = {
            "batch_run_id": resolved_batch_run_id,
            "checkpoint_table": resolved_indexer_runs_table,
            "rows_seen": total_rows_seen,
            "upserts": total_upserts,
            "deletes": total_deletes,
            "unknown_ops": unknown_ops,
            "upsert_batches": upsert_batch_index,
            "delete_batches": delete_batch_index,
            "skipped": False,
        }
        upsert_indexer_run(
            client=client,
            table_name=resolved_indexer_runs_table,
            batch_run_id=resolved_batch_run_id,
            status="FAILED",
            summary=failure_summary,
            error_message=str(exc),
        )
        raise
    finally:
        session.close()

    logging.info(
        "Finished batch_run_id=%s. Total upserts: %s. Total deletes: %s.",
        resolved_batch_run_id,
        total_upserts,
        total_deletes,
    )
    logging.info(
        "Aggregate: rows seen=%s, upsert batches=%s, delete batches=%s, unknown ops skipped=%s. Note: upserts are inserts+updates.",
        total_rows_seen,
        upsert_batch_index,
        delete_batch_index,
        unknown_ops,
    )

    summary = {
        "batch_run_id": resolved_batch_run_id,
        "checkpoint_table": resolved_indexer_runs_table,
        "rows_seen": total_rows_seen,
        "upserts": total_upserts,
        "deletes": total_deletes,
        "unknown_ops": unknown_ops,
        "upsert_batches": upsert_batch_index,
        "delete_batches": delete_batch_index,
        "skipped": False,
    }
    upsert_indexer_run(
        client=client,
        table_name=resolved_indexer_runs_table,
        batch_run_id=resolved_batch_run_id,
        status="COMPLETED",
        summary=summary,
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Typesense incremental indexer for VariantChanges")
    parser.add_argument(
        "--batch-run-id",
        dest="batch_run_id",
        help="Process a specific batch_run_id instead of auto-detecting the latest batch.",
    )
    return parser


def main() -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()

    try:
        summary = run_indexer(batch_run_id=args.batch_run_id)
    except Exception as exc:
        logging.exception("Indexer crashed: %s", exc)
        return 1

    if summary["batch_run_id"]:
        logging.info("Summary: %s", summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
