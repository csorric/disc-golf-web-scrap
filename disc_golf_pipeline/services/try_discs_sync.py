"""Cache unique Try Discs flight matches for normalized disc models."""

import json
import math

from google.cloud import bigquery

from disc_golf_pipeline.services.try_discs import (
    ATTRIBUTION,
    BRAND_ALIASES,
    build_catalog_index,
    fetch_catalog,
    match_model,
    match_key,
)


MATCH_FIELDS = (
    ("manufacturer", "STRING"),
    ("model", "STRING"),
    ("speed", "FLOAT64"),
    ("glide", "FLOAT64"),
    ("turn", "FLOAT64"),
    ("fade", "FLOAT64"),
    ("match_type", "STRING"),
    ("disc_url", "STRING"),
    ("dataset_version", "STRING"),
    ("attribution", "STRING"),
    ("raw_flight_json", "STRING"),
    ("invalid_fields_json", "STRING"),
    ("flight_conflict_unresolved", "BOOL"),
    ("catalog_category", "STRING"),
)


def build_model_pairs_sql(project_id, dataset):
    snapshot = f"`{project_id}.{dataset}.NormalizedVariantSnapshot`"
    return f"""
SELECT DISTINCT normalized_manufacturer AS manufacturer, normalized_model AS model
FROM {snapshot}
WHERE item_type = 'disc'
  AND NULLIF(TRIM(normalized_manufacturer), '') IS NOT NULL
  AND NULLIF(TRIM(normalized_model), '') IS NOT NULL
"""


def parse_flight_record(disc):
    """Preserve partial/out-of-domain records without making booleans numeric."""
    numbers, invalid = {}, []
    for field in ("speed", "glide", "turn", "fade"):
        raw = disc.get(field)
        value = None
        if raw is not None:
            try:
                value = float(raw) if not isinstance(raw, bool) else None
            except (ValueError, TypeError, OverflowError):
                pass
            if value is None or not math.isfinite(value):
                invalid.append(field)
                value = None
        numbers[field] = value
    return numbers, invalid


def build_match_rows(catalog, model_pairs, dataset_version):
    index = build_catalog_index(catalog, include_incomplete=True)
    rows = []
    for pair in model_pairs:
        manufacturer, model = pair["manufacturer"], pair["model"]
        brand_key = match_key(manufacturer)
        matches = index.get((BRAND_ALIASES.get(brand_key, brand_key), match_key(model)), [])
        if not matches:
            continue
        ambiguous = len(matches) != 1
        disc, match_type = match_model(manufacturer, model, index)
        disc = disc or {}
        numbers, invalid = parse_flight_record(disc)
        rows.append({
            "manufacturer": manufacturer,
            "model": model,
            **numbers,
            "match_type": "ambiguous" if ambiguous else match_type,
            "disc_url": disc.get("url"),
            "dataset_version": dataset_version,
            "attribution": ATTRIBUTION,
            "raw_flight_json": json.dumps(matches if ambiguous else disc, default=str),
            "invalid_fields_json": json.dumps(invalid),
            "flight_conflict_unresolved": ambiguous,
            "catalog_category": (disc.get("category")
                                 if isinstance(disc.get("category"), str) else None),
        })
    return rows


def ensure_match_schema(client, project_id, dataset):
    """Allow classifications to refresh from an older cache without an API call."""
    table = client.get_table(f"{project_id}.{dataset}.TryDiscsModelMatches")
    existing = {field.name.lower() for field in table.schema}
    missing = [bigquery.SchemaField(name, kind) for name, kind in MATCH_FIELDS
               if name.lower() not in existing]
    if missing:
        table.schema = list(table.schema) + missing
        client.update_table(table, ["schema"])


def sync_try_discs_matches(client, project_id, dataset):
    """Replace the small model-match cache after a complete API fetch."""
    catalog, meta = fetch_catalog()
    pairs = [dict(row.items()) for row in client.query(
        build_model_pairs_sql(project_id, dataset)
    ).result()]
    rows = build_match_rows(catalog, pairs, meta.get("dataset_version", "unknown"))
    table_ref = f"{project_id}.{dataset}.TryDiscsModelMatches"
    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(name, kind) for name, kind in MATCH_FIELDS],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_json(rows, table_ref, job_config=job_config).result()
    return {"catalog_entries": len(catalog), "model_matches": len(rows),
            "dataset_version": meta.get("dataset_version")}
