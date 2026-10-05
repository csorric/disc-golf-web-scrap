"""Cached model/attribute repair and isolated BigQuery regression checks."""

import json
import re

from google.cloud import bigquery

from disc_golf_pipeline.common.runtime import PROJECT_ROOT
from disc_golf_pipeline.services.disc_attributes import build_disc_attributes_view_sql
from disc_golf_pipeline.services.disc_weight_llm import REVIEW_FIELDS
from disc_golf_pipeline.services.model_normalization import (
    build_apply_product_model_decisions_sql, build_disc_model_catalog_sql,
    build_product_model_candidates_sql, get_model_rules_version,
)
from disc_golf_pipeline.services.try_discs_sync import MATCH_FIELDS, build_cached_match_aliases_sql


def build_missing_model_repair_sql(project, dataset, version, decision_columns):
    """Apply only newly accepted decisions to previously unresolved products."""
    products = f"`{project}.{dataset}.NormalizedProducts`"
    decisions = f"`{project}.{dataset}.ProductDiscModelDecisions`"
    sql = build_product_model_candidates_sql(project, dataset, version)
    sql = sql.replace(f"FROM {products} AS product",
                      f"FROM (SELECT * FROM {products} WHERE normalized_model IS NULL "
                      "AND normalized_manufacturer IS NOT NULL) AS product")
    for table in ("ProductDiscModelCandidates", "ProductDiscModelDecisions"):
        sql = sql.replace(f"`{project}.{dataset}.{table}`", table + "Repair")
    sql = sql.replace("CREATE OR REPLACE TABLE", "CREATE TEMP TABLE")
    apply_sql = build_apply_product_model_decisions_sql(project, dataset, version)
    apply_sql = apply_sql.replace(decisions, "ProductDiscModelDecisionsRepair")
    apply_sql = apply_sql.replace("AND product.item_type = 'disc';",
                                  "AND product.item_type = 'disc' AND product.normalized_model IS NULL "
                                  "AND decision.decision_bucket = 'ACCEPT';")
    updates = ", ".join(f"target.{name} = source.{name}" for name in decision_columns if name != "product_key")
    columns = ", ".join(decision_columns)
    values = ", ".join(f"source.{name}" for name in decision_columns)
    return sql + f"""
BEGIN TRANSACTION;
{apply_sql}
MERGE {decisions} target
USING (SELECT * FROM ProductDiscModelDecisionsRepair WHERE decision_bucket = 'ACCEPT') source
ON target.product_key = source.product_key
WHEN MATCHED THEN UPDATE SET {updates}
WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values});
DELETE FROM `{project}.{dataset}.ProductDiscModelCandidates`
WHERE product_key IN (SELECT product_key FROM DesiredProductModels);
INSERT INTO `{project}.{dataset}.ProductDiscModelCandidates`
SELECT * FROM ProductDiscModelCandidatesRepair
WHERE product_key IN (SELECT product_key FROM DesiredProductModels);
COMMIT TRANSACTION;
SELECT COUNT(*) AS repaired_products FROM DesiredProductModels;
"""


def repair_missing_models(client, project, dataset):
    version = get_model_rules_version()
    client.query(build_disc_model_catalog_sql(project, dataset, version)).result()
    columns = [field.name for field in client.get_table(f"{project}.{dataset}.ProductDiscModelDecisions").schema]
    result = [dict(row.items()) for row in client.query(
        build_missing_model_repair_sql(project, dataset, version, columns)).result()]
    print(json.dumps({"cached_model_repair": result}), flush=True)
    return result


def _fixture_sql(sql):
    """Redirect generated builders to temporary tables in one isolated script."""
    sql = re.sub(r"`fixture\.repair\.([A-Za-z0-9_]+)`", r"\1", sql)
    return sql.replace("CREATE OR REPLACE TABLE", "CREATE TEMP TABLE")


def _run_fixture_query(client, sql, label):
    """Bound isolated validation so stalled queries cannot hold the repair open."""
    job = client.query(sql, job_config=bigquery.QueryJobConfig(job_timeout_ms=300_000))
    print(f"Validating {label} in BigQuery job {job.job_id} (5-minute limit).", flush=True)
    try:
        return job.result(timeout=330)
    except TimeoutError as exc:
        job.cancel()
        raise RuntimeError(f"Validation timed out: {label}; canceled BigQuery job {job.job_id}") from exc


def validate_attribute_repair(client):
    """Use the real model decision, cache recovery and attribute SQL."""
    model_script = """
CREATE TEMP TABLE PdgaDiscCanonical AS
SELECT CAST(i AS STRING) AS canonical_id, 'Fixture' AS pdga_manufacturer,
  model AS pdga_model, 'fixture' AS manufacturer_match_key, TRUE AS is_current
FROM UNNEST(['Teebird 3', 'TeeBird', 'M4', 'Ambiguous 1', 'Ambiguous1']) model WITH OFFSET i;
CREATE TEMP TABLE DiscManufacturerAliases AS
SELECT 'fixture' AS alias_match_key, 'Fixture' AS canonical_manufacturer,
  TRUE AS is_active, 'curated' AS alias_source;
CREATE TEMP TABLE NormalizedProducts AS
SELECT CONCAT('shopify:fixture:', CAST(i AS STRING)) AS product_key, CAST(i AS STRING) AS product_id,
  'fixture' AS store, 'Fixture' AS raw_vendor, 'shopify' AS source, 'disc' AS item_type,
  fixture.title, fixture.manufacturer AS normalized_manufacturer,
  CAST(NULL AS STRING) AS normalized_model,
  'recognized_vendor_alias' AS manufacturer_source, 1.0 AS manufacturer_confidence,
  CAST(NULL AS FLOAT64) AS model_confidence, 'unresolved' AS model_source,
  JSON '{"model_decision":{"model_rules_version":"old"}}' AS normalization_evidence,
  CAST(NULL AS TIMESTAMP) AS normalized_at
FROM UNNEST([
  STRUCT('Halo Star TeeBird3 8/4/0/2' AS title, 'Fixture' AS manufacturer),
  STRUCT('Halo Star TeeBird 3' AS title, 'Fixture' AS manufacturer),
  STRUCT('Fixture M 4' AS title, 'Fixture' AS manufacturer),
  STRUCT('Halo Star TeeBird3' AS title, 'Other' AS manufacturer),
  STRUCT('Halo Star TeeBird3' AS title, CAST(NULL AS STRING) AS manufacturer),
  STRUCT('Ambiguous1' AS title, 'Fixture' AS manufacturer),
  STRUCT('TeeBird' AS title, 'Fixture' AS manufacturer)
]) fixture WITH OFFSET i;
CREATE TEMP TABLE v_ShopifyVariants AS
SELECT product_id AS id, product_id, store, '173g Blue' AS variant_title FROM NormalizedProducts;
"""
    model_script += _fixture_sql(build_disc_model_catalog_sql("fixture", "repair", "fixture"))
    model_script += _fixture_sql(build_product_model_candidates_sql("fixture", "repair", "fixture"))
    model_script += """
ASSERT (SELECT COUNT(*) = 4 FROM ProductDiscModelDecisions WHERE decision_bucket = 'ACCEPT')
  AS 'Model spacing must retain manufacturer and ambiguity guards';
ASSERT (SELECT COUNT(*) = 2 FROM ProductDiscModelDecisions
  WHERE product_id IN ('0', '1') AND normalized_model = 'Teebird 3' AND decision_bucket = 'ACCEPT')
  AS 'Joined and spaced Teebird3 should identify the same model';
ASSERT (SELECT COUNT(*) = 1 FROM ProductDiscModelDecisions
  WHERE product_id = '2' AND normalized_model = 'M4' AND decision_bucket = 'ACCEPT')
  AS 'Spaced M4 should match the compact canonical name';
ASSERT (SELECT COUNT(*) = 1 FROM ProductDiscModelDecisions
  WHERE product_id = '6' AND normalized_model = 'TeeBird' AND decision_bucket = 'ACCEPT')
  AS 'Base TeeBird must remain distinct from TeeBird3';
"""
    model_script += """
UPDATE NormalizedProducts SET normalized_model = 'Preserve existing', model_source = 'cached_llm'
WHERE product_id = '1';
CREATE TEMP TABLE ProductNormalizationAudit (
  product_key STRING, variant_id STRING, source STRING, store STRING, change_type STRING,
  prior_item_type STRING, new_item_type STRING, prior_normalized_manufacturer STRING,
  new_normalized_manufacturer STRING, prior_normalized_model STRING, new_normalized_model STRING,
  decision_source STRING, rules_version STRING, event_timestamp TIMESTAMP
);
"""
    model_script += _fixture_sql(build_missing_model_repair_sql(
        "fixture", "repair", "fixture", ["product_key", "normalized_model", "normalized_manufacturer",
                                         "decision_bucket", "model_rules_version"]))
    model_script += """
ASSERT (SELECT normalized_model = 'Teebird 3'
  AND JSON_VALUE(normalization_evidence, '$.model_decision.model_rules_version') = 'fixture'
  FROM NormalizedProducts WHERE product_id = '0') AS 'Missing model not repaired';
ASSERT (SELECT normalized_model = 'Preserve existing' AND model_source = 'cached_llm'
  AND JSON_VALUE(normalization_evidence, '$.model_decision.model_rules_version') = 'old'
  FROM NormalizedProducts WHERE product_id = '1') AS 'Existing identity was overwritten';
ASSERT (SELECT COUNT(*) = 3 FROM ProductNormalizationAudit) AS 'Unexpected model audit changes';
"""
    _run_fixture_query(client, model_script, "model spacing and guarded repair")
    print("Passed 7 BigQuery model-spacing decision fixtures.", flush=True)

    cases = [
        ("title_flights", None, "8/4/0/2", "Blue / 173-5 grams", None, 173, 175),
        ("cached_flights", "Teebird 3", "", "Blue 165-70g", None, 165, 170),
        ("exact", "Teebird 3", "", "Blue 173g", 173, None, None),
        ("leading_range", "Teebird 3", "", "173-5 Blue", None, 173, 175),
        ("full_range", "Teebird 3", "", "Blue 173-175g", None, 173, 175),
        ("reversed", "Teebird 3", "", "Blue 175-3g", None, None, None),
        ("open_range", "Teebird 3", "", "Blue 175+g", None, None, None),
        ("no_rollover", "Teebird 3", "", "Blue 168-2g", None, None, None),
        ("cache_conflict", "Conflict 1", "", "Blue 173g", 173, None, None),
    ]
    def literal(value):
        return "CAST(NULL AS STRING)" if value is None else "'" + str(value).replace("'", "\\'") + "'"

    source = " UNION ALL ".join(
        f"SELECT {literal(name)} AS id, 'disc' AS item_type, 'Fixture' AS normalized_manufacturer, "
        f"{literal(model)} AS normalized_model, {literal('Fixture TeeBird3 ' + flight)} AS title, "
        f"{literal(variant)} AS variant_title, 255.0 AS weight_g, '' AS tags, '' AS BodyHtml"
        for name, model, flight, variant, _, _, _ in cases
    )
    script = f"CREATE TEMP TABLE NormalizedVariantSnapshot AS {source};\n"
    script += "CREATE TEMP TABLE TryDiscsModelMatches (" + ",".join(f"{n} {t}" for n, t in MATCH_FIELDS) + ");\n"
    script += "CREATE TEMP TABLE DiscWeightLlmReviews (" + ",".join(f"{n} {t}" for n, t in REVIEW_FIELDS) + ");\n"
    script += """
INSERT INTO TryDiscsModelMatches(manufacturer, model, speed, glide, turn, fade, match_type)
VALUES ('Fixture', 'TeeBird3', 8, 4, 0, 2, 'exact'),
  ('Fixture', 'Conflict1', 8, 4, 0, 2, 'exact'), ('Fixture', 'Conflict-1', 9, 4, 0, 2, 'exact');
"""
    script += _fixture_sql(build_cached_match_aliases_sql("fixture", "repair")) + ";\n"
    # The repair must be idempotent and must not choose among conflicting cache rows.
    script += _fixture_sql(build_cached_match_aliases_sql("fixture", "repair")) + ";\n"
    script += "ASSERT (SELECT COUNT(*) = 1 FROM TryDiscsModelMatches WHERE model = 'Teebird 3');\n"
    script += "ASSERT (SELECT COUNT(*) = 0 FROM TryDiscsModelMatches WHERE model = 'Conflict 1');\n"
    query = build_disc_attributes_view_sql("fixture", "repair").partition(" AS\n")[2]
    script += _fixture_sql(query)
    rows = {row["id"]: row for row in _run_fixture_query(client, script, "cached flights and weight ranges")}
    if len(rows) != len(cases):
        raise AssertionError("Attribute fixture row count mismatch")
    for name, _, _, _, exact, low, high in cases:
        row = rows[name]
        actual_weight = (row["normalized_weight_g"], row["normalized_weight_min_g"], row["normalized_weight_max_g"])
        if actual_weight != (exact, low, high):
            raise AssertionError(f"Weight range regression {name}: {actual_weight}")
        actual_flights = tuple(row[field] for field in ("speed", "glide", "turn", "fade"))
        expected_flights = (None,) * 4 if name == "cache_conflict" else (8, 4, 0, 2)
        if actual_flights != expected_flights:
            raise AssertionError(f"Flight regression {name}: {actual_flights}")
    print(f"Passed {len(cases)} BigQuery flight/cache/weight-range fixtures.", flush=True)


def summarize_attribute_coverage(client, project, dataset, label):
    query = f"""
SELECT COUNT(*) AS disc_variants,
  COUNTIF(normalized_manufacturer IS NULL) AS missing_manufacturer,
  COUNTIF(normalized_model IS NULL) AS missing_model,
  COUNTIF(speed IS NULL AND glide IS NULL AND turn IS NULL AND fade IS NULL) AS missing_all_flights,
  COUNTIF(data_status = 'complete') AS complete_flight_variants,
  COUNTIF(weight_status = 'range') AS weight_range_variants,
  COUNTIF(weight_status IN ('missing', 'invalid')) AS unknown_weight_variants
FROM `{project}.{dataset}.VariantState` WHERE item_type = 'disc'
"""
    summary = dict(next(iter(client.query(query).result())).items())
    path = PROJECT_ROOT / "output" / f"disc-attribute-repair-{label}.json"
    path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"attribute_coverage": label, **summary}), flush=True)
    return summary


def validate_repaired_attributes(client, project, dataset):
    query = f"""
CREATE TEMP TABLE target AS SELECT * FROM `{project}.{dataset}.VariantState`
WHERE source_variant_key = 'shopify:titandiscgolf.com:42746797555934';
ASSERT (SELECT COUNT(*) = 1 FROM target) AS 'TeeBird3 regression variant missing';
ASSERT (SELECT COUNTIF(normalized_manufacturer = 'Innova Champion Discs'
  AND normalized_model = 'Teebird 3' AND speed = 8 AND glide = 4 AND turn = 0 AND fade = 2
  AND weight_status = 'range' AND weight_g IS NULL AND weight_min_g = 173 AND weight_max_g = 175) = 1
  FROM target) AS 'TeeBird3 identity/flight/weight regression';
"""
    client.query(query).result()
    print("Validated reported TeeBird3 variant: identified model, 8/4/0/2, 173-175g range.", flush=True)
