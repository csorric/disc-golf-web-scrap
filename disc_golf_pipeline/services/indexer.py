import argparse
import ast
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from uuid import uuid4

import requests
from google.cloud import bigquery
from disc_golf_pipeline.common.runtime import LOG_DIR, load_env_file

DEFAULT_COLLECTION = "discs_v4"
DEFAULT_NORMALIZED_COLLECTION = "discs_v5"
DEFAULT_RELEASE_PREFIX = "discs"
RELEASE_SCHEMA_VERSION = "search-v2-source-identity"
DEFAULT_BATCH_SIZE = 200
DEFAULT_LOG_FILE = LOG_DIR / "indexer.log"
URL_PATTERN = re.compile(r"^https?://.+", re.IGNORECASE)

NORMALIZED_COLLECTION_FIELDS = [
    {"name": "product_id", "type": "string"},
    {"name": "variant_id", "type": "string"},
    {"name": "title", "type": "string"},
    {"name": "variant_title", "type": "string"},
    {"name": "search_text", "type": "string"},
    {"name": "vendor", "type": "string", "facet": True},
    {"name": "store", "type": "string", "facet": True},
    {"name": "product_type", "type": "string", "facet": True},
    {"name": "price", "type": "float", "sort": True},
    {"name": "weight_g", "type": "int32", "sort": True},
    {"name": "high_price", "type": "float", "sort": True},
    {"name": "low_price", "type": "float", "sort": True},
    {"name": "in_stock", "type": "bool", "facet": True, "sort": True},
    {"name": "image", "type": "string"},
    {"name": "variant_image", "type": "string"},
    {"name": "product_link", "type": "string"},
    {"name": "store_url", "type": "string"},
    {"name": "BodyHtml", "type": "string"},
    {"name": "IsDistanceDriver", "type": "bool", "facet": True, "sort": True},
    {"name": "IsFairwayDriver", "type": "bool", "facet": True, "sort": True},
    {"name": "IsMidrange", "type": "bool", "facet": True, "sort": True},
    {"name": "IsPutter", "type": "bool", "facet": True, "sort": True},
    {"name": "tags", "type": "string[]"},
    {"name": "source", "type": "string", "facet": True, "optional": True},
    {"name": "retailer", "type": "string", "facet": True, "optional": True},
    {"name": "raw_vendor", "type": "string", "optional": True},
    {
        "name": "normalized_manufacturer",
        "type": "string",
        "facet": True,
        "optional": True,
    },
    {"name": "normalized_model", "type": "string", "facet": True, "optional": True},
    {"name": "item_type", "type": "string", "facet": True, "optional": True},
    {
        "name": "is_disc",
        "type": "bool",
        "facet": True,
        "optional": True,
        "sort": True,
    },
    {"name": "item_type_confidence", "type": "float", "optional": True, "sort": False},
    {"name": "manufacturer_confidence", "type": "float", "optional": True, "sort": False},
    {"name": "model_confidence", "type": "float", "optional": True, "sort": False},
    {"name": "normalization_confidence", "type": "float", "optional": True, "sort": False},
    {"name": "normalization_source", "type": "string", "optional": True},
    {
        "name": "model_decision_level",
        "type": "string",
        "facet": True,
        "optional": True,
    },
    {
        "name": "normalization_version",
        "type": "string",
        "facet": True,
        "optional": True,
    },
    {
        "name": "model_rules_version",
        "type": "string",
        "facet": True,
        "optional": True,
    },
    {"name": "last_indexed_at", "type": "int64", "optional": True, "sort": False},
]

RELEASE_COLLECTION_FIELDS = [
    {"name": "legacy_id", "type": "string"},
    {"name": "source_variant_key", "type": "string"},
    *NORMALIZED_COLLECTION_FIELDS,
]


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


def derive_variant_state_table_name(variant_changes_table: str) -> str:
    if not variant_changes_table:
        return ""

    parts = variant_changes_table.split(".")
    if len(parts) != 3:
        raise ValueError(
            "BQ_VARIANT_CHANGES_TABLE must be in project.dataset.table format to derive BQ_VARIANT_STATE_TABLE."
        )
    project_id, dataset, _ = parts
    return f"{project_id}.{dataset}.VariantState"


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
          source_variant_key,
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
          source,
          retailer,
          raw_vendor,
          normalized_manufacturer,
          normalized_model,
          item_type,
          is_disc,
          item_type_confidence,
          manufacturer_confidence,
          model_confidence,
          normalization_confidence,
          normalization_source,
          model_decision_level,
          normalization_version,
          model_rules_version,
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


def iterate_variant_state(client: bigquery.Client, table_name: str):
    query = f"""
        SELECT
          id,
          product_id,
          variant_id,
          source_variant_key,
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
          source,
          retailer,
          raw_vendor,
          normalized_manufacturer,
          normalized_model,
          item_type,
          is_disc,
          item_type_confidence,
          manufacturer_confidence,
          model_confidence,
          normalization_confidence,
          normalization_source,
          model_decision_level,
          normalization_version,
          model_rules_version
        FROM `{table_name}`
        WHERE NULLIF(TRIM(CAST(id AS STRING)), '') IS NOT NULL
        ORDER BY id
    """
    return client.query(query).result(page_size=1000)


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
                failures = []
                for line_number, line in enumerate(text.splitlines(), start=1):
                    if not line.strip():
                        continue
                    try:
                        result = json.loads(line)
                    except json.JSONDecodeError:
                        failures.append({"line": line_number, "error": "invalid JSON response"})
                        continue
                    if not result.get("success", False):
                        failures.append(
                            {
                                "line": line_number,
                                "error": safe_string(result.get("error")),
                            }
                        )
                if failures:
                    logging.error(
                        "UPSERT batch %s had %s document failures. First failures: %s",
                        batch_index,
                        len(failures),
                        failures[:3],
                    )
                    return {
                        "ok": False,
                        "status": response.status_code,
                        "failures": failures,
                    }
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
    return sanitize_text(json.dumps(document, separators=(",", ":"), ensure_ascii=False))


def build_document(row, use_source_variant_key: bool = False) -> Dict:
    tags = parse_tags(row.get("tags"))
    normalized_manufacturer = safe_string(row.get("normalized_manufacturer"))
    normalized_model = safe_string(row.get("normalized_model"))
    legacy_id = safe_string(row.get("id"))
    source_variant_key = safe_string(row.get("source_variant_key"))
    document_id = source_variant_key if use_source_variant_key else legacy_id
    if not document_id:
        identity_field = "source_variant_key" if use_source_variant_key else "id"
        raise ValueError(f"Cannot build Typesense document without {identity_field}.")
    document = {
        "id": document_id,
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
                normalized_manufacturer,
                normalized_model,
                safe_string(row.get("vendor")),
                safe_string(row.get("store")),
                " ".join(tags),
            ]
            if part
        ).lower(),
        "last_indexed_at": int(time.time() * 1000),
    }

    if use_source_variant_key:
        document["legacy_id"] = legacy_id
        document["source_variant_key"] = source_variant_key

    optional_strings = (
        "source",
        "retailer",
        "raw_vendor",
        "item_type",
        "normalization_source",
        "model_decision_level",
        "normalization_version",
        "model_rules_version",
    )
    for field_name in optional_strings:
        value = safe_string(row.get(field_name))
        if value:
            document[field_name] = value

    if normalized_manufacturer:
        document["normalized_manufacturer"] = normalized_manufacturer
    if normalized_model:
        document["normalized_model"] = normalized_model

    if row.get("is_disc") is not None:
        document["is_disc"] = safe_bool(row.get("is_disc"))

    for field_name in (
        "item_type_confidence",
        "manufacturer_confidence",
        "model_confidence",
        "normalization_confidence",
    ):
        if row.get(field_name) is not None:
            document[field_name] = safe_float(row.get(field_name))

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


def build_normalized_collection_schema(
    collection: str = DEFAULT_NORMALIZED_COLLECTION,
    fields: Optional[List[Dict]] = None,
) -> Dict:
    return {
        "name": collection,
        "fields": fields or NORMALIZED_COLLECTION_FIELDS,
        "default_sorting_field": "price",
    }


def validate_collection_schema(
    collection_data: Dict,
    expected_fields: Optional[List[Dict]] = None,
) -> None:
    actual_fields = {field["name"]: field for field in collection_data.get("fields", [])}
    errors = []
    for expected in expected_fields or NORMALIZED_COLLECTION_FIELDS:
        actual = actual_fields.get(expected["name"])
        if actual is None:
            errors.append(f"missing field {expected['name']}")
            continue
        for property_name in ("type", "facet", "optional", "sort"):
            expected_value = expected.get(property_name, False)
            actual_value = actual.get(property_name, False)
            if actual_value != expected_value:
                errors.append(
                    f"{expected['name']}.{property_name}={actual_value!r}, expected {expected_value!r}"
                )
    if collection_data.get("default_sorting_field") != "price":
        errors.append("default_sorting_field must be price")
    if errors:
        raise RuntimeError("Typesense collection schema mismatch: " + "; ".join(errors))


def ensure_normalized_collection(
    host: str,
    admin_key: str,
    collection: str = DEFAULT_NORMALIZED_COLLECTION,
    session: Optional[requests.Session] = None,
    fields: Optional[List[Dict]] = None,
) -> Dict:
    owns_session = session is None
    resolved_session = session or requests.Session()
    headers = {"X-TYPESENSE-API-KEY": admin_key}
    collection_url = f"{host.rstrip('/')}/collections/{collection}"
    try:
        response = resolved_session.get(collection_url, headers=headers, timeout=30)
        if response.status_code == 200:
            collection_data = response.json()
            validate_collection_schema(collection_data, fields)
            logging.info("Typesense collection %s already exists with the expected schema.", collection)
            return collection_data
        if response.status_code != 404:
            response.raise_for_status()

        create_response = resolved_session.post(
            f"{host.rstrip('/')}/collections",
            headers={**headers, "Content-Type": "application/json"},
            json=build_normalized_collection_schema(collection, fields),
            timeout=30,
        )
        create_response.raise_for_status()
        collection_data = create_response.json()
        validate_collection_schema(collection_data, fields)
        logging.info("Created Typesense collection %s.", collection)
        return collection_data
    finally:
        if owns_session:
            resolved_session.close()


def create_normalized_collection() -> Dict:
    host = os.getenv("TYPESENSE_HOST", "").strip()
    admin_key = os.getenv("TYPESENSE_ADMIN_KEY", "").strip()
    collection = (
        os.getenv("TYPESENSE_V5_COLLECTION", DEFAULT_NORMALIZED_COLLECTION).strip()
        or DEFAULT_NORMALIZED_COLLECTION
    )
    validate_required_env(host, admin_key, "collection-creation-does-not-use-bigquery")
    return ensure_normalized_collection(host, admin_key, collection)


def get_variant_state_count(client: bigquery.Client, table_name: str) -> Dict:
    query = f"""
        SELECT
          COUNT(*) AS row_count,
          COUNT(DISTINCT id) AS id_count,
          COUNT(DISTINCT source_variant_key) AS source_variant_key_count,
          COUNTIF(NULLIF(TRIM(source_variant_key), '') IS NULL) AS missing_source_variant_keys
        FROM `{table_name}`
        WHERE NULLIF(TRIM(CAST(id AS STRING)), '') IS NOT NULL
    """
    row = next(iter(client.query(query).result()))
    return {
        "row_count": int(row["row_count"]),
        "id_count": int(row["id_count"]),
        "source_variant_key_count": int(row["source_variant_key_count"]),
        "missing_source_variant_keys": int(row["missing_source_variant_keys"]),
    }


def validate_normalized_collection(
    client: bigquery.Client,
    state_table: str,
    host: str,
    admin_key: str,
    collection: str,
    session: Optional[requests.Session] = None,
    expected_fields: Optional[List[Dict]] = None,
    use_source_variant_key: bool = False,
) -> Dict:
    owns_session = session is None
    resolved_session = session or requests.Session()
    headers = {"X-TYPESENSE-API-KEY": admin_key}
    try:
        collection_response = resolved_session.get(
            f"{host.rstrip('/')}/collections/{collection}",
            headers=headers,
            timeout=30,
        )
        collection_response.raise_for_status()
        collection_data = collection_response.json()
        validate_collection_schema(collection_data, expected_fields)

        state_counts = get_variant_state_count(client, state_table)
        document_count = int(collection_data.get("num_documents", 0))
        if state_counts["row_count"] != state_counts["id_count"]:
            raise RuntimeError(
                f"VariantState contains duplicate IDs: {state_counts}"
            )
        expected_document_count = (
            state_counts["source_variant_key_count"]
            if use_source_variant_key
            else state_counts["id_count"]
        )
        if use_source_variant_key and state_counts["missing_source_variant_keys"]:
            raise RuntimeError(
                f"VariantState contains missing source_variant_key values: {state_counts}"
            )
        if use_source_variant_key and state_counts["row_count"] != expected_document_count:
            raise RuntimeError(
                f"VariantState contains duplicate source_variant_key values: {state_counts}"
            )
        if document_count != expected_document_count:
            raise RuntimeError(
                f"Typesense document count {document_count} does not match VariantState ID count "
                f"{expected_document_count}"
            )

        search_response = resolved_session.get(
            f"{host.rstrip('/')}/collections/{collection}/documents/search",
            headers=headers,
            params={
                "q": "*",
                "query_by": "search_text",
                "facet_by": "source,item_type,normalized_manufacturer,normalized_model",
                "per_page": 1,
            },
            timeout=30,
        )
        search_response.raise_for_status()
        search_data = search_response.json()
        if int(search_data.get("found", -1)) != document_count:
            raise RuntimeError(
                f"Typesense wildcard search found {search_data.get('found')} documents, expected {document_count}"
            )
        return {
            "collection": collection,
            "variant_state_rows": state_counts["row_count"],
            "variant_state_ids": state_counts["id_count"],
            "variant_state_source_variant_keys": state_counts["source_variant_key_count"],
            "typesense_documents": document_count,
            "search_found": int(search_data["found"]),
            "facet_fields": [facet["field_name"] for facet in search_data.get("facet_counts", [])],
        }
    finally:
        if owns_session:
            resolved_session.close()


def run_full_normalized_backfill() -> Dict:
    host = os.getenv("TYPESENSE_HOST", "").strip()
    admin_key = os.getenv("TYPESENSE_ADMIN_KEY", "").strip()
    production_collection = os.getenv("TYPESENSE_COLLECTION", DEFAULT_COLLECTION).strip() or DEFAULT_COLLECTION
    collection = (
        os.getenv("TYPESENSE_V5_COLLECTION", DEFAULT_NORMALIZED_COLLECTION).strip()
        or DEFAULT_NORMALIZED_COLLECTION
    )
    changes_table = os.getenv("BQ_VARIANT_CHANGES_TABLE", "").strip()
    state_table = os.getenv("BQ_VARIANT_STATE_TABLE", "").strip() or derive_variant_state_table_name(
        changes_table
    )
    batch_size = safe_int(os.getenv("INDEXER_BATCH_SIZE", DEFAULT_BATCH_SIZE)) or DEFAULT_BATCH_SIZE

    validate_required_env(host, admin_key, state_table)
    if collection == production_collection:
        raise ValueError(
            f"Refusing full migration backfill into production collection {collection}. "
            "Set TYPESENSE_V5_COLLECTION to a side-by-side collection name."
        )

    client = get_bigquery_client()
    session = requests.Session()
    rows_seen = 0
    batches_sent = 0
    pending_documents = []
    try:
        ensure_normalized_collection(host, admin_key, collection, session=session)
        logging.info(
            "Starting full Typesense backfill from %s into %s with batch size %s.",
            state_table,
            collection,
            batch_size,
        )
        for row in iterate_variant_state(client, state_table):
            pending_documents.append(build_document(row))
            rows_seen += 1
            if len(pending_documents) < batch_size:
                continue

            batches_sent += 1
            result = send_upsert_batch(
                session,
                host,
                admin_key,
                collection,
                pending_documents,
                batches_sent,
            )
            if not result.get("ok"):
                raise RuntimeError(f"Typesense full backfill failed in batch {batches_sent}: {result}")
            pending_documents = []

        if pending_documents:
            batches_sent += 1
            result = send_upsert_batch(
                session,
                host,
                admin_key,
                collection,
                pending_documents,
                batches_sent,
            )
            if not result.get("ok"):
                raise RuntimeError(f"Typesense full backfill failed in batch {batches_sent}: {result}")

        validation = validate_normalized_collection(
            client,
            state_table,
            host,
            admin_key,
            collection,
            session=session,
        )
        summary = {
            "collection": collection,
            "rows_seen": rows_seen,
            "batches_sent": batches_sent,
            "validation": validation,
        }
        logging.info("Full normalized Typesense backfill completed: %s", summary)
        return summary
    finally:
        session.close()


def derive_typesense_deployments_table_name(variant_state_table: str) -> str:
    parts = variant_state_table.split(".")
    if len(parts) != 3:
        raise ValueError(
            "BQ_VARIANT_STATE_TABLE must be in project.dataset.table format to derive "
            "TYPESENSE_DEPLOYMENTS_TABLE."
        )
    project_id, dataset, _ = parts
    return f"{project_id}.{dataset}.TypesenseDeployments"


def build_release_collection_name(
    prefix: str = DEFAULT_RELEASE_PREFIX,
    now: Optional[datetime] = None,
    suffix: Optional[str] = None,
) -> str:
    safe_prefix = re.sub(r"[^a-zA-Z0-9_-]+", "_", prefix.strip()).strip("_")
    if not safe_prefix:
        raise ValueError("Typesense release prefix must contain letters or numbers.")
    resolved_now = now or datetime.now(timezone.utc)
    timestamp = resolved_now.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    resolved_suffix = suffix or uuid4().hex[:6]
    return f"{safe_prefix}_{timestamp}_{resolved_suffix}"


def ensure_typesense_deployments_table(client: bigquery.Client, table_name: str) -> None:
    query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
          deployment_id STRING NOT NULL,
          collection_name STRING NOT NULL,
          alias_name STRING,
          schema_version STRING NOT NULL,
          status STRING NOT NULL,
          source_table STRING NOT NULL,
          source_batch_run_id STRING,
          source_batch_run_ts TIMESTAMP,
          source_row_count INT64,
          source_identity_count INT64,
          typesense_document_count INT64,
          started_at TIMESTAMP,
          completed_at TIMESTAMP,
          activated_at TIMESTAMP,
          previous_collection STRING,
          previous_collection_deleted_at TIMESTAMP,
          validation_json STRING,
          error_message STRING,
          updated_at TIMESTAMP NOT NULL
        )
        CLUSTER BY status, collection_name
    """
    client.query(query).result()
    client.query(
        f"ALTER TABLE `{table_name}` "
        "ADD COLUMN IF NOT EXISTS previous_collection_deleted_at TIMESTAMP"
    ).result()


def upsert_typesense_deployment(
    client: bigquery.Client,
    table_name: str,
    deployment: Dict,
) -> None:
    query = f"""
        MERGE `{table_name}` AS target
        USING (
          SELECT
            @deployment_id AS deployment_id,
            @collection_name AS collection_name,
            @alias_name AS alias_name,
            @schema_version AS schema_version,
            @status AS status,
            @source_table AS source_table,
            @source_batch_run_id AS source_batch_run_id,
            @source_batch_run_ts AS source_batch_run_ts,
            @source_row_count AS source_row_count,
            @source_identity_count AS source_identity_count,
            @typesense_document_count AS typesense_document_count,
            @started_at AS started_at,
            @completed_at AS completed_at,
            @activated_at AS activated_at,
            @previous_collection AS previous_collection,
            @previous_collection_deleted_at AS previous_collection_deleted_at,
            @validation_json AS validation_json,
            @error_message AS error_message
        ) AS source
        ON target.deployment_id = source.deployment_id
        WHEN MATCHED THEN UPDATE SET
          collection_name = source.collection_name,
          alias_name = source.alias_name,
          schema_version = source.schema_version,
          status = source.status,
          source_table = source.source_table,
          source_batch_run_id = source.source_batch_run_id,
          source_batch_run_ts = source.source_batch_run_ts,
          source_row_count = source.source_row_count,
          source_identity_count = source.source_identity_count,
          typesense_document_count = source.typesense_document_count,
          started_at = COALESCE(target.started_at, source.started_at),
          completed_at = source.completed_at,
          activated_at = source.activated_at,
          previous_collection = source.previous_collection,
          previous_collection_deleted_at = source.previous_collection_deleted_at,
          validation_json = source.validation_json,
          error_message = source.error_message,
          updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          deployment_id, collection_name, alias_name, schema_version, status,
          source_table, source_batch_run_id, source_batch_run_ts,
          source_row_count, source_identity_count, typesense_document_count,
          started_at, completed_at, activated_at, previous_collection,
          previous_collection_deleted_at, validation_json, error_message, updated_at
        ) VALUES (
          source.deployment_id, source.collection_name, source.alias_name,
          source.schema_version, source.status, source.source_table,
          source.source_batch_run_id, source.source_batch_run_ts,
          source.source_row_count, source.source_identity_count,
          source.typesense_document_count, source.started_at, source.completed_at,
          source.activated_at, source.previous_collection,
          source.previous_collection_deleted_at, source.validation_json,
          source.error_message, CURRENT_TIMESTAMP()
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("deployment_id", "STRING", deployment["deployment_id"]),
            bigquery.ScalarQueryParameter("collection_name", "STRING", deployment["collection_name"]),
            bigquery.ScalarQueryParameter("alias_name", "STRING", deployment.get("alias_name")),
            bigquery.ScalarQueryParameter("schema_version", "STRING", deployment["schema_version"]),
            bigquery.ScalarQueryParameter("status", "STRING", deployment["status"]),
            bigquery.ScalarQueryParameter("source_table", "STRING", deployment["source_table"]),
            bigquery.ScalarQueryParameter(
                "source_batch_run_id", "STRING", deployment.get("source_batch_run_id")
            ),
            bigquery.ScalarQueryParameter(
                "source_batch_run_ts", "TIMESTAMP", deployment.get("source_batch_run_ts")
            ),
            bigquery.ScalarQueryParameter(
                "source_row_count", "INT64", deployment.get("source_row_count")
            ),
            bigquery.ScalarQueryParameter(
                "source_identity_count", "INT64", deployment.get("source_identity_count")
            ),
            bigquery.ScalarQueryParameter(
                "typesense_document_count", "INT64", deployment.get("typesense_document_count")
            ),
            bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", deployment.get("started_at")),
            bigquery.ScalarQueryParameter("completed_at", "TIMESTAMP", deployment.get("completed_at")),
            bigquery.ScalarQueryParameter("activated_at", "TIMESTAMP", deployment.get("activated_at")),
            bigquery.ScalarQueryParameter(
                "previous_collection", "STRING", deployment.get("previous_collection")
            ),
            bigquery.ScalarQueryParameter(
                "previous_collection_deleted_at",
                "TIMESTAMP",
                deployment.get("previous_collection_deleted_at"),
            ),
            bigquery.ScalarQueryParameter(
                "validation_json", "STRING", deployment.get("validation_json")
            ),
            bigquery.ScalarQueryParameter(
                "error_message", "STRING", safe_string(deployment.get("error_message"))[:5000]
            ),
        ]
    )
    client.query(query, job_config=job_config).result()


def get_latest_variant_batch(client: bigquery.Client, table_name: str) -> Dict:
    query = f"""
        SELECT batch_run_id, batch_run_ts
        FROM `{table_name}`
        ORDER BY batch_run_ts DESC
        LIMIT 1
    """
    rows = list(client.query(query).result())
    if not rows:
        return {"batch_run_id": None, "batch_run_ts": None}
    return {
        "batch_run_id": safe_string(rows[0].get("batch_run_id")),
        "batch_run_ts": rows[0].get("batch_run_ts"),
    }


def run_typesense_release_build(collection_name: Optional[str] = None) -> Dict:
    host = os.getenv("TYPESENSE_HOST", "").strip()
    admin_key = os.getenv("TYPESENSE_ADMIN_KEY", "").strip()
    production_collection = os.getenv("TYPESENSE_COLLECTION", DEFAULT_COLLECTION).strip() or DEFAULT_COLLECTION
    alias_name = os.getenv("TYPESENSE_PRODUCTION_ALIAS", "discs_prod").strip() or "discs_prod"
    prefix = os.getenv("TYPESENSE_RELEASE_PREFIX", DEFAULT_RELEASE_PREFIX).strip() or DEFAULT_RELEASE_PREFIX
    changes_table = os.getenv("BQ_VARIANT_CHANGES_TABLE", "").strip()
    state_table = os.getenv("BQ_VARIANT_STATE_TABLE", "").strip() or derive_variant_state_table_name(
        changes_table
    )
    deployments_table = os.getenv("TYPESENSE_DEPLOYMENTS_TABLE", "").strip() or (
        derive_typesense_deployments_table_name(state_table)
    )
    batch_size = safe_int(os.getenv("INDEXER_BATCH_SIZE", DEFAULT_BATCH_SIZE)) or DEFAULT_BATCH_SIZE
    resolved_collection = collection_name or os.getenv("TYPESENSE_RELEASE_COLLECTION", "").strip()
    if not resolved_collection:
        resolved_collection = build_release_collection_name(prefix)

    validate_required_env(host, admin_key, state_table)
    if resolved_collection in {production_collection, alias_name}:
        raise ValueError(
            f"Refusing release build into production target {resolved_collection}."
        )

    client = get_bigquery_client()
    ensure_typesense_deployments_table(client, deployments_table)
    state_counts = get_variant_state_count(client, state_table)
    if state_counts["row_count"] != state_counts["source_variant_key_count"]:
        raise RuntimeError(f"VariantState stable identity validation failed: {state_counts}")
    if state_counts["missing_source_variant_keys"]:
        raise RuntimeError(f"VariantState contains missing stable identities: {state_counts}")

    source_batch = get_latest_variant_batch(client, changes_table)
    started_at = datetime.now(timezone.utc)
    deployment = {
        "deployment_id": uuid4().hex,
        "collection_name": resolved_collection,
        "alias_name": alias_name,
        "schema_version": RELEASE_SCHEMA_VERSION,
        "status": "BUILDING",
        "source_table": state_table,
        "source_batch_run_id": source_batch["batch_run_id"],
        "source_batch_run_ts": source_batch["batch_run_ts"],
        "source_row_count": state_counts["row_count"],
        "source_identity_count": state_counts["source_variant_key_count"],
        "typesense_document_count": None,
        "started_at": started_at,
        "completed_at": None,
        "validation_json": None,
        "error_message": "",
    }
    upsert_typesense_deployment(client, deployments_table, deployment)

    session = requests.Session()
    rows_seen = 0
    batches_sent = 0
    pending_documents = []
    try:
        ensure_normalized_collection(
            host,
            admin_key,
            resolved_collection,
            session=session,
            fields=RELEASE_COLLECTION_FIELDS,
        )
        logging.info(
            "Building Typesense release %s from %s with schema %s.",
            resolved_collection,
            state_table,
            RELEASE_SCHEMA_VERSION,
        )
        for row in iterate_variant_state(client, state_table):
            pending_documents.append(build_document(row, use_source_variant_key=True))
            rows_seen += 1
            if len(pending_documents) < batch_size:
                continue
            batches_sent += 1
            result = send_upsert_batch(
                session,
                host,
                admin_key,
                resolved_collection,
                pending_documents,
                batches_sent,
            )
            if not result.get("ok"):
                raise RuntimeError(f"Typesense release failed in batch {batches_sent}: {result}")
            pending_documents = []

        if pending_documents:
            batches_sent += 1
            result = send_upsert_batch(
                session,
                host,
                admin_key,
                resolved_collection,
                pending_documents,
                batches_sent,
            )
            if not result.get("ok"):
                raise RuntimeError(f"Typesense release failed in batch {batches_sent}: {result}")

        final_source_batch = get_latest_variant_batch(client, changes_table)
        if final_source_batch["batch_run_id"] != source_batch["batch_run_id"]:
            raise RuntimeError(
                "VariantChanges advanced during the release build; the release will not be validated."
            )
        validation = validate_normalized_collection(
            client,
            state_table,
            host,
            admin_key,
            resolved_collection,
            session=session,
            expected_fields=RELEASE_COLLECTION_FIELDS,
            use_source_variant_key=True,
        )
        completed_at = datetime.now(timezone.utc)
        deployment.update(
            {
                "status": "VALIDATED",
                "typesense_document_count": validation["typesense_documents"],
                "completed_at": completed_at,
                "validation_json": json.dumps(validation, sort_keys=True),
                "error_message": "",
            }
        )
        upsert_typesense_deployment(client, deployments_table, deployment)
        summary = {
            "deployment_id": deployment["deployment_id"],
            "collection": resolved_collection,
            "status": deployment["status"],
            "schema_version": RELEASE_SCHEMA_VERSION,
            "source_batch_run_id": source_batch["batch_run_id"],
            "rows_seen": rows_seen,
            "batches_sent": batches_sent,
            "validation": validation,
            "alias_changed": False,
        }
        logging.info("Typesense release build completed: %s", summary)
        return summary
    except BaseException as exc:
        deployment.update(
            {
                "status": "FAILED",
                "completed_at": datetime.now(timezone.utc),
                "error_message": f"{type(exc).__name__}: {exc}",
            }
        )
        try:
            upsert_typesense_deployment(client, deployments_table, deployment)
        except Exception:
            logging.exception("Failed to record Typesense release failure in %s", deployments_table)
        raise
    finally:
        session.close()


def get_typesense_alias(
    session: requests.Session,
    host: str,
    admin_key: str,
    alias_name: str,
) -> Optional[Dict]:
    response = session.get(
        f"{host.rstrip('/')}/aliases/{alias_name}",
        headers={"X-TYPESENSE-API-KEY": admin_key},
        timeout=30,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def point_typesense_alias(
    session: requests.Session,
    host: str,
    admin_key: str,
    alias_name: str,
    collection_name: str,
) -> Dict:
    response = session.put(
        f"{host.rstrip('/')}/aliases/{alias_name}",
        headers={
            "Content-Type": "application/json",
            "X-TYPESENSE-API-KEY": admin_key,
        },
        json={"collection_name": collection_name},
        timeout=30,
    )
    response.raise_for_status()
    alias_data = response.json()
    if alias_data.get("collection_name") != collection_name:
        raise RuntimeError(
            f"Typesense alias {alias_name} did not resolve to {collection_name}: {alias_data}"
        )
    return alias_data


def get_typesense_aliases(
    session: requests.Session,
    host: str,
    admin_key: str,
) -> List[Dict]:
    response = session.get(
        f"{host.rstrip('/')}/aliases",
        headers={"X-TYPESENSE-API-KEY": admin_key},
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("aliases", [])


def get_typesense_collection(
    session: requests.Session,
    host: str,
    admin_key: str,
    collection_name: str,
) -> Optional[Dict]:
    response = session.get(
        f"{host.rstrip('/')}/collections/{collection_name}",
        headers={"X-TYPESENSE-API-KEY": admin_key},
        timeout=30,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def delete_typesense_collection(
    session: requests.Session,
    host: str,
    admin_key: str,
    collection_name: str,
) -> Dict:
    response = session.delete(
        f"{host.rstrip('/')}/collections/{collection_name}",
        headers={"X-TYPESENSE-API-KEY": admin_key},
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()
    if get_typesense_collection(session, host, admin_key, collection_name) is not None:
        raise RuntimeError(
            f"Typesense collection {collection_name} still exists after deletion."
        )
    return result


def get_typesense_deployment(
    client: bigquery.Client,
    table_name: str,
    alias_name: str,
    statuses: List[str],
    deployment_id: Optional[str] = None,
) -> Optional[Dict]:
    query = f"""
        SELECT *
        FROM `{table_name}`
        WHERE alias_name = @alias_name
          AND status IN UNNEST(@statuses)
          AND (@deployment_id IS NULL OR deployment_id = @deployment_id)
        ORDER BY COALESCE(activated_at, completed_at, started_at) DESC
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("alias_name", "STRING", alias_name),
            bigquery.ArrayQueryParameter("statuses", "STRING", statuses),
            bigquery.ScalarQueryParameter("deployment_id", "STRING", deployment_id),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return dict(rows[0].items()) if rows else None


def update_deployment_statuses(
    client: bigquery.Client,
    table_name: str,
    alias_name: str,
    from_status: str,
    to_status: str,
    excluded_deployment_id: Optional[str] = None,
    collection_name: Optional[str] = None,
) -> None:
    query = f"""
        UPDATE `{table_name}`
        SET status = @to_status, updated_at = CURRENT_TIMESTAMP()
        WHERE alias_name = @alias_name
          AND status = @from_status
          AND (@excluded_deployment_id IS NULL OR deployment_id != @excluded_deployment_id)
          AND (@collection_name IS NULL OR collection_name = @collection_name)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("alias_name", "STRING", alias_name),
            bigquery.ScalarQueryParameter("from_status", "STRING", from_status),
            bigquery.ScalarQueryParameter("to_status", "STRING", to_status),
            bigquery.ScalarQueryParameter(
                "excluded_deployment_id", "STRING", excluded_deployment_id
            ),
            bigquery.ScalarQueryParameter("collection_name", "STRING", collection_name),
        ]
    )
    client.query(query, job_config=job_config).result()


def get_release_runtime() -> Dict:
    host = os.getenv("TYPESENSE_HOST", "").strip()
    admin_key = os.getenv("TYPESENSE_ADMIN_KEY", "").strip()
    production_collection = os.getenv("TYPESENSE_COLLECTION", DEFAULT_COLLECTION).strip() or DEFAULT_COLLECTION
    alias_name = os.getenv("TYPESENSE_PRODUCTION_ALIAS", "discs_prod").strip() or "discs_prod"
    changes_table = os.getenv("BQ_VARIANT_CHANGES_TABLE", "").strip()
    state_table = os.getenv("BQ_VARIANT_STATE_TABLE", "").strip() or derive_variant_state_table_name(
        changes_table
    )
    deployments_table = os.getenv("TYPESENSE_DEPLOYMENTS_TABLE", "").strip() or (
        derive_typesense_deployments_table_name(state_table)
    )
    validate_required_env(host, admin_key, changes_table)
    if not state_table:
        raise ValueError(
            "Missing env var BQ_VARIANT_STATE_TABLE and it could not be derived from "
            "BQ_VARIANT_CHANGES_TABLE."
        )
    return {
        "host": host,
        "admin_key": admin_key,
        "production_collection": production_collection,
        "alias_name": alias_name,
        "changes_table": changes_table,
        "state_table": state_table,
        "deployments_table": deployments_table,
    }


def validate_release_deployment(
    client: bigquery.Client,
    runtime: Dict,
    deployment: Dict,
    session: requests.Session,
) -> Dict:
    if deployment.get("schema_version") != RELEASE_SCHEMA_VERSION:
        raise RuntimeError(
            f"Deployment schema {deployment.get('schema_version')} is not {RELEASE_SCHEMA_VERSION}."
        )
    latest_batch = get_latest_variant_batch(client, runtime["changes_table"])
    if latest_batch["batch_run_id"] != deployment.get("source_batch_run_id"):
        raise RuntimeError(
            "The release source batch is no longer current; build a new release before activation."
        )
    return validate_normalized_collection(
        client,
        runtime["state_table"],
        runtime["host"],
        runtime["admin_key"],
        deployment["collection_name"],
        session=session,
        expected_fields=RELEASE_COLLECTION_FIELDS,
        use_source_variant_key=True,
    )


def revalidate_latest_typesense_release() -> Dict:
    runtime = get_release_runtime()
    client = get_bigquery_client()
    ensure_typesense_deployments_table(client, runtime["deployments_table"])
    deployment = get_typesense_deployment(
        client,
        runtime["deployments_table"],
        runtime["alias_name"],
        ["VALIDATED", "ACTIVE"],
    )
    if deployment is None:
        raise RuntimeError("No validated Typesense release is available.")
    with requests.Session() as session:
        validation = validate_release_deployment(client, runtime, deployment, session)
    deployment["validation_json"] = json.dumps(validation, sort_keys=True)
    deployment["error_message"] = ""
    upsert_typesense_deployment(client, runtime["deployments_table"], deployment)
    return {
        "deployment_id": deployment["deployment_id"],
        "collection": deployment["collection_name"],
        "status": deployment["status"],
        "validation": validation,
        "alias_changed": False,
    }


def activate_latest_typesense_release(deployment_id: Optional[str] = None) -> Dict:
    runtime = get_release_runtime()
    client = get_bigquery_client()
    ensure_typesense_deployments_table(client, runtime["deployments_table"])
    deployment = get_typesense_deployment(
        client,
        runtime["deployments_table"],
        runtime["alias_name"],
        ["VALIDATED"],
        deployment_id=deployment_id,
    )
    if deployment is None:
        raise RuntimeError("No VALIDATED Typesense release is available for activation.")

    with requests.Session() as session:
        validation = validate_release_deployment(client, runtime, deployment, session)
        current_alias = get_typesense_alias(
            session,
            runtime["host"],
            runtime["admin_key"],
            runtime["alias_name"],
        )
        previous_collection = (
            current_alias.get("collection_name")
            if current_alias
            else runtime["production_collection"]
        )
        deployment.update(
            {
                "status": "ACTIVATING",
                "previous_collection": previous_collection,
                "previous_collection_deleted_at": None,
                "validation_json": json.dumps(validation, sort_keys=True),
                "error_message": "",
            }
        )
        upsert_typesense_deployment(client, runtime["deployments_table"], deployment)
        try:
            alias_data = point_typesense_alias(
                session,
                runtime["host"],
                runtime["admin_key"],
                runtime["alias_name"],
                deployment["collection_name"],
            )
        except BaseException as exc:
            deployment.update(
                {
                    "status": "VALIDATED",
                    "error_message": f"Alias activation failed: {type(exc).__name__}: {exc}",
                }
            )
            upsert_typesense_deployment(client, runtime["deployments_table"], deployment)
            raise

    deployment.update(
        {
            "status": "ACTIVE",
            "activated_at": datetime.now(timezone.utc),
            "error_message": "",
        }
    )
    update_deployment_statuses(
        client,
        runtime["deployments_table"],
        runtime["alias_name"],
        "ACTIVE",
        "SUPERSEDED",
        excluded_deployment_id=deployment["deployment_id"],
    )
    upsert_typesense_deployment(client, runtime["deployments_table"], deployment)
    return {
        "deployment_id": deployment["deployment_id"],
        "alias": alias_data["name"],
        "collection": alias_data["collection_name"],
        "previous_collection": previous_collection,
        "status": "ACTIVE",
    }


def publish_typesense_release(collection_name: Optional[str] = None) -> Dict:
    build_summary = run_typesense_release_build(collection_name=collection_name)
    activation_summary = activate_latest_typesense_release(
        deployment_id=build_summary["deployment_id"]
    )
    return {
        "deployment_id": build_summary["deployment_id"],
        "collection": build_summary["collection"],
        "status": activation_summary["status"],
        "schema_version": build_summary["schema_version"],
        "source_batch_run_id": build_summary["source_batch_run_id"],
        "rows_seen": build_summary["rows_seen"],
        "batches_sent": build_summary["batches_sent"],
        "validation": build_summary["validation"],
        "alias": activation_summary["alias"],
        "previous_collection": activation_summary["previous_collection"],
        "alias_changed": True,
    }


def rollback_active_typesense_release() -> Dict:
    runtime = get_release_runtime()
    client = get_bigquery_client()
    ensure_typesense_deployments_table(client, runtime["deployments_table"])
    deployment = get_typesense_deployment(
        client,
        runtime["deployments_table"],
        runtime["alias_name"],
        ["ACTIVE"],
    )
    if deployment is None:
        raise RuntimeError("No ACTIVE Typesense release is available to roll back.")
    previous_collection = safe_string(deployment.get("previous_collection"))
    if not previous_collection:
        raise RuntimeError("The active deployment has no previous collection recorded.")
    if deployment.get("previous_collection_deleted_at") is not None:
        raise RuntimeError(
            f"Cannot roll back because previous collection {previous_collection} was deleted at "
            f"{deployment['previous_collection_deleted_at']}."
        )

    with requests.Session() as session:
        alias_data = point_typesense_alias(
            session,
            runtime["host"],
            runtime["admin_key"],
            runtime["alias_name"],
            previous_collection,
        )
    deployment.update(
        {
            "status": "ROLLED_BACK",
            "completed_at": datetime.now(timezone.utc),
            "error_message": "",
        }
    )
    upsert_typesense_deployment(client, runtime["deployments_table"], deployment)
    update_deployment_statuses(
        client,
        runtime["deployments_table"],
        runtime["alias_name"],
        "SUPERSEDED",
        "ACTIVE",
        collection_name=previous_collection,
    )
    return {
        "deployment_id": deployment["deployment_id"],
        "alias": alias_data["name"],
        "collection": alias_data["collection_name"],
        "rolled_back_from": deployment["collection_name"],
        "status": "ROLLED_BACK",
    }


def build_previous_collection_cleanup_status(
    deployment: Optional[Dict],
    alias_data: Optional[Dict],
    aliases: List[Dict],
    previous_collection_exists: bool,
) -> Dict:
    reasons = []
    active_collection = safe_string(deployment.get("collection_name")) if deployment else ""
    previous_collection = safe_string(deployment.get("previous_collection")) if deployment else ""
    alias_collection = safe_string(alias_data.get("collection_name")) if alias_data else ""
    deleted_at = deployment.get("previous_collection_deleted_at") if deployment else None

    if deployment is None:
        reasons.append("No ACTIVE deployment is recorded.")
    if not alias_data:
        reasons.append("The production alias does not exist.")
    elif alias_collection != active_collection:
        reasons.append("The production alias does not point to the recorded ACTIVE deployment.")
    if not previous_collection:
        reasons.append("The active deployment has no previous collection recorded.")
    if previous_collection and previous_collection == active_collection:
        reasons.append("The previous collection is also the active collection.")
    if deleted_at is not None:
        reasons.append("The previous collection is already recorded as deleted.")
    if previous_collection and not previous_collection_exists:
        reasons.append("The previous collection does not exist in Typesense.")

    referencing_aliases = sorted(
        safe_string(alias.get("name"))
        for alias in aliases
        if safe_string(alias.get("collection_name")) == previous_collection
    )
    if referencing_aliases:
        reasons.append(
            "The previous collection is still targeted by alias(es): "
            + ", ".join(referencing_aliases)
        )

    return {
        "deployment_id": deployment.get("deployment_id") if deployment else None,
        "production_alias": alias_data.get("name") if alias_data else None,
        "active_collection": active_collection or None,
        "previous_collection": previous_collection or None,
        "previous_collection_exists": previous_collection_exists,
        "previous_collection_deleted_at": deleted_at,
        "referencing_aliases": referencing_aliases,
        "eligible_for_deletion": not reasons,
        "blocking_reasons": reasons,
    }


def get_previous_typesense_collection_cleanup_status() -> Dict:
    runtime = get_release_runtime()
    client = get_bigquery_client()
    ensure_typesense_deployments_table(client, runtime["deployments_table"])
    deployment = get_typesense_deployment(
        client,
        runtime["deployments_table"],
        runtime["alias_name"],
        ["ACTIVE"],
    )
    with requests.Session() as session:
        alias_data = get_typesense_alias(
            session,
            runtime["host"],
            runtime["admin_key"],
            runtime["alias_name"],
        )
        aliases = get_typesense_aliases(
            session,
            runtime["host"],
            runtime["admin_key"],
        )
        previous_collection = (
            safe_string(deployment.get("previous_collection")) if deployment else ""
        )
        previous_collection_exists = bool(
            previous_collection
            and get_typesense_collection(
                session,
                runtime["host"],
                runtime["admin_key"],
                previous_collection,
            )
        )
    return build_previous_collection_cleanup_status(
        deployment,
        alias_data,
        aliases,
        previous_collection_exists,
    )


def delete_previous_typesense_collection(confirm_collection: str) -> Dict:
    runtime = get_release_runtime()
    client = get_bigquery_client()
    ensure_typesense_deployments_table(client, runtime["deployments_table"])
    deployment = get_typesense_deployment(
        client,
        runtime["deployments_table"],
        runtime["alias_name"],
        ["ACTIVE"],
    )

    with requests.Session() as session:
        alias_data = get_typesense_alias(
            session,
            runtime["host"],
            runtime["admin_key"],
            runtime["alias_name"],
        )
        aliases = get_typesense_aliases(
            session,
            runtime["host"],
            runtime["admin_key"],
        )
        previous_collection = (
            safe_string(deployment.get("previous_collection")) if deployment else ""
        )
        previous_collection_exists = bool(
            previous_collection
            and get_typesense_collection(
                session,
                runtime["host"],
                runtime["admin_key"],
                previous_collection,
            )
        )
        cleanup_status = build_previous_collection_cleanup_status(
            deployment,
            alias_data,
            aliases,
            previous_collection_exists,
        )
        if confirm_collection != previous_collection:
            raise ValueError(
                "Deletion confirmation does not match the audited previous collection: "
                f"expected {previous_collection!r}, received {confirm_collection!r}."
            )
        if not cleanup_status["eligible_for_deletion"]:
            raise RuntimeError(
                "Previous collection is not eligible for deletion: "
                + "; ".join(cleanup_status["blocking_reasons"])
            )

        # Re-read the production alias immediately before the destructive call.
        current_alias = get_typesense_alias(
            session,
            runtime["host"],
            runtime["admin_key"],
            runtime["alias_name"],
        )
        if not current_alias or current_alias.get("collection_name") != deployment["collection_name"]:
            raise RuntimeError("The production alias changed during cleanup; deletion was cancelled.")
        deletion_result = delete_typesense_collection(
            session,
            runtime["host"],
            runtime["admin_key"],
            previous_collection,
        )

    deleted_at = datetime.now(timezone.utc)
    deployment["previous_collection_deleted_at"] = deleted_at
    deployment["error_message"] = ""
    upsert_typesense_deployment(client, runtime["deployments_table"], deployment)
    update_deployment_statuses(
        client,
        runtime["deployments_table"],
        runtime["alias_name"],
        "SUPERSEDED",
        "DELETED",
        collection_name=previous_collection,
    )
    return {
        "deployment_id": deployment["deployment_id"],
        "active_collection": deployment["collection_name"],
        "deleted_collection": previous_collection,
        "deleted_at": deleted_at.isoformat(),
        "rollback_available": False,
        "typesense_response": deletion_result,
    }


def get_typesense_release_status() -> Dict:
    runtime = get_release_runtime()
    client = get_bigquery_client()
    ensure_typesense_deployments_table(client, runtime["deployments_table"])
    with requests.Session() as session:
        alias_data = get_typesense_alias(
            session,
            runtime["host"],
            runtime["admin_key"],
            runtime["alias_name"],
        )
    deployment = get_typesense_deployment(
        client,
        runtime["deployments_table"],
        runtime["alias_name"],
        ["ACTIVE", "VALIDATED", "ROLLED_BACK", "FAILED", "BUILDING", "ACTIVATING"],
    )
    return {
        "alias": runtime["alias_name"],
        "alias_collection": alias_data.get("collection_name") if alias_data else None,
        "latest_deployment": deployment,
    }


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
                        raise RuntimeError(
                            f"DELETE batch {delete_batch_index} failed: {result}"
                        )
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
                    raise RuntimeError(
                        f"UPSERT batch {upsert_batch_index} failed: {result}"
                    )
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
                raise RuntimeError(
                    f"Final UPSERT batch {upsert_batch_index} failed: {result}"
                )

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
                raise RuntimeError(
                    f"Final DELETE batch {delete_batch_index} failed: {result}"
                )
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
