"""Cache unique Try Discs flight matches for normalized disc models."""

from google.cloud import bigquery

from disc_golf_pipeline.services.try_discs import (
    ATTRIBUTION,
    build_catalog_index,
    fetch_catalog,
    flight_numbers,
    match_model,
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


def build_match_rows(catalog, model_pairs, dataset_version):
    index = build_catalog_index(catalog)
    rows = []
    for pair in model_pairs:
        manufacturer, model = pair["manufacturer"], pair["model"]
        disc, match_type = match_model(manufacturer, model, index)
        if disc is None:
            continue
        speed, glide, turn, fade = flight_numbers(disc)
        rows.append({
            "manufacturer": manufacturer,
            "model": model,
            "speed": speed,
            "glide": glide,
            "turn": turn,
            "fade": fade,
            "match_type": match_type,
            "disc_url": disc.get("url"),
            "dataset_version": dataset_version,
            "attribution": ATTRIBUTION,
        })
    return rows


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
