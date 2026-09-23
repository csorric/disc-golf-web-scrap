import os

from google.cloud import bigquery

from disc_golf_pipeline.services.normalization import run_normalization
from disc_golf_pipeline.services.source_views import build_source_view_sqls

DEFAULT_PROJECT_ID = "disc-golf-price-compare"
DEFAULT_DATASET = "DiscGolfProducts"

NORMALIZATION_SCHEMA_FIELDS = (
    ("source_variant_key", "STRING"),
    ("source", "STRING"),
    ("retailer", "STRING"),
    ("raw_vendor", "STRING"),
    ("normalized_manufacturer", "STRING"),
    ("normalized_model", "STRING"),
    ("item_type", "STRING"),
    ("is_disc", "BOOL"),
    ("item_type_confidence", "FLOAT64"),
    ("manufacturer_confidence", "FLOAT64"),
    ("model_confidence", "FLOAT64"),
    ("normalization_confidence", "FLOAT64"),
    ("normalization_source", "STRING"),
    ("model_decision_level", "STRING"),
    ("normalization_version", "STRING"),
    ("model_rules_version", "STRING"),
)


def get_gcp_project_id():
    return os.getenv("GCP_PROJECT_ID", DEFAULT_PROJECT_ID).strip()


def get_bigquery_dataset():
    return os.getenv("BIGQUERY_DATASET", DEFAULT_DATASET).strip()


def build_table_ref(project_id, dataset, table_name):
    return f"`{project_id}.{dataset}.{table_name}`"


def build_shopify_variant_id_expr(alias):
    return f"""CONCAT(
  'shopify:',
  LOWER(TRIM(COALESCE(CAST({alias}.store AS STRING), CAST({alias}.store_url AS STRING), 'unknown-store'))),
  ':',
  COALESCE(
    NULLIF(TRIM(CAST({alias}.variant_id AS STRING)), ''),
    NULLIF(TRIM(CAST({alias}.id AS STRING)), ''),
    NULLIF(TRIM(CAST({alias}.product_id AS STRING)), '')
  )
)"""


def build_infinite_variant_id_expr(alias):
    return f"""TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
  'infinite' AS source,
  LOWER(TRIM(COALESCE(CAST({alias}.store AS STRING), CAST({alias}.store_url AS STRING), 'infinitediscs'))) AS store,
  COALESCE(NULLIF(TRIM(CAST({alias}.product_link AS STRING)), ''), '') AS product_link,
  COALESCE(NULLIF(TRIM(CAST({alias}.title AS STRING)), ''), '') AS title,
  COALESCE(NULLIF(TRIM(CAST({alias}.variant_title AS STRING)), ''), '') AS variant_title,
  COALESCE(NULLIF(TRIM(CAST({alias}.weight_g AS STRING)), ''), '') AS weight_g
))))"""


def build_product_id_expr(alias, fallback_id_expr):
    return f"""COALESCE(
  NULLIF(TRIM(CAST({alias}.product_id AS STRING)), ''),
  NULLIF(TRIM(CAST({alias}.id AS STRING)), ''),
  {fallback_id_expr}
)"""


def build_variant_id_expr(alias, fallback_id_expr):
    return f"""COALESCE(
  NULLIF(TRIM(CAST({alias}.variant_id AS STRING)), ''),
  NULLIF(TRIM(CAST({alias}.id AS STRING)), ''),
  NULLIF(TRIM(CAST({alias}.product_id AS STRING)), ''),
  {fallback_id_expr}
)"""


def build_variant_snapshot_view_sql(project_id, dataset):
    normalized_snapshot = build_table_ref(project_id, dataset, "NormalizedVariantSnapshot")
    destination_view = build_table_ref(project_id, dataset, "v_VariantSnapshot")
    infinite_fallback_id = build_infinite_variant_id_expr("src")
    shopify_fallback_id = build_shopify_variant_id_expr("src")

    return f"""
CREATE OR REPLACE VIEW {destination_view} AS
SELECT
  CAST(CASE
    WHEN src.source = 'shopify' THEN {shopify_fallback_id}
    ELSE {infinite_fallback_id}
  END AS STRING) AS id,
  CAST(src.product_id AS STRING) AS product_id,
  CAST(src.variant_id AS STRING) AS variant_id,
  CAST(src.source_variant_key AS STRING) AS source_variant_key,
  CAST(src.title AS STRING) AS title,
  CAST(src.vendor AS STRING) AS vendor,
  CAST(src.product_link AS STRING) AS product_link,
  CAST(src.store AS STRING) AS store,
  CAST(src.store_url AS STRING) AS store_url,
  CAST(src.image AS STRING) AS image,
  CAST(src.variant_title AS STRING) AS variant_title,
  CAST(src.price AS FLOAT64) AS price,
  CAST(src.weight_g AS INT64) AS weight_g,
  CAST(src.in_stock AS BOOL) AS in_stock,
  CAST(src.variant_image AS STRING) AS variant_image,
  CAST(src.high_price AS FLOAT64) AS high_price,
  CAST(src.low_price AS FLOAT64) AS low_price,
  CAST(src.tags AS STRING) AS tags,
  CAST(src.IsDistanceDriver AS INT64) AS IsDistanceDriver,
  CAST(src.IsFairwayDriver AS INT64) AS IsFairwayDriver,
  CAST(src.IsMidrange AS INT64) AS IsMidrange,
  CAST(src.IsPutter AS INT64) AS IsPutter,
  CAST(src.BodyHtml AS STRING) AS BodyHtml,
  CAST(src.product_type AS STRING) AS product_type,
  CAST(src.source AS STRING) AS source,
  CAST(src.retailer AS STRING) AS retailer,
  CAST(src.raw_vendor AS STRING) AS raw_vendor,
  CAST(src.normalized_manufacturer AS STRING) AS normalized_manufacturer,
  CAST(src.normalized_model AS STRING) AS normalized_model,
  CAST(src.item_type AS STRING) AS item_type,
  CAST(src.is_disc AS BOOL) AS is_disc,
  CAST(src.item_type_confidence AS FLOAT64) AS item_type_confidence,
  CAST(src.manufacturer_confidence AS FLOAT64) AS manufacturer_confidence,
  CAST(src.model_confidence AS FLOAT64) AS model_confidence,
  CAST(src.normalization_confidence AS FLOAT64) AS normalization_confidence,
  CAST(src.normalization_source AS STRING) AS normalization_source,
  CAST(src.model_decision_level AS STRING) AS model_decision_level,
  CAST(src.normalization_version AS STRING) AS normalization_version,
  CAST(src.model_rules_version AS STRING) AS model_rules_version
FROM {normalized_snapshot} AS src
WHERE COALESCE(
  NULLIF(TRIM(CAST(src.variant_id AS STRING)), ''),
  NULLIF(TRIM(CAST(src.id AS STRING)), ''),
  NULLIF(TRIM(CAST(src.product_id AS STRING)), ''),
  NULLIF(TRIM(CAST(src.product_link AS STRING)), ''),
  NULLIF(TRIM(CAST(src.title AS STRING)), '')
) IS NOT NULL
"""


def build_derived_product_type_sql(project_id, dataset):
    product_info_table = build_table_ref(project_id, dataset, "ProductInfo")
    infinite_discs_table = build_table_ref(project_id, dataset, "InfiniteDiscs")
    destination_table = build_table_ref(project_id, dataset, "DerivedProductType")

    return f"""
CREATE OR REPLACE TABLE {destination_table} AS
WITH source_rows AS (
  SELECT
    SAFE_CAST(MainProductId AS INT64) AS MainProductId,
    CAST(ProductType AS STRING) AS ProductType,
    CAST(Tags AS STRING) AS Tags,
    COALESCE(CAST(BodyHtml AS STRING), '') AS BodyHtml,
    'Shopify' AS Source
  FROM {product_info_table}

  UNION ALL

  SELECT
    SAFE_CAST(Id AS INT64) AS MainProductId,
    '' AS ProductType,
    '' AS Tags,
    COALESCE(CAST(ModelDescription AS STRING), '') AS BodyHtml,
    'Infinite' AS Source
  FROM {infinite_discs_table}
),
classified AS (
  SELECT
    MainProductId,
    ProductType,
    Tags,
    BodyHtml,
    Source,
    CASE
      WHEN REGEXP_CONTAINS(LOWER(BodyHtml), r'distance driver|long range driver') THEN 'DistanceDriver'
      WHEN REGEXP_CONTAINS(LOWER(BodyHtml), r'fairway driver|long range driver') THEN 'FairwayDriver'
      WHEN REGEXP_CONTAINS(LOWER(BodyHtml), r'putter|approach') THEN 'Putter'
      WHEN REGEXP_CONTAINS(LOWER(BodyHtml), r'midrange') THEN 'Midrange'
      ELSE 'Unknown'
    END AS DiscType
  FROM source_rows
)
SELECT
  MainProductId,
  ProductType,
  Tags,
  DiscType,
  CAST(DiscType = 'DistanceDriver' AS INT64) AS IsDistanceDriver,
  CAST(DiscType = 'FairwayDriver' AS INT64) AS IsFairwayDriver,
  CAST(DiscType = 'Midrange' AS INT64) AS IsMidrange,
  CAST(DiscType = 'Putter' AS INT64) AS IsPutter,
  Source,
  BodyHtml
FROM classified
"""


def build_variant_state_sql(project_id, dataset):
    variant_changes_table = build_table_ref(project_id, dataset, "VariantChanges")
    variant_state_table = build_table_ref(project_id, dataset, "VariantState")
    variant_snapshot_view = build_table_ref(project_id, dataset, "v_VariantSnapshot")

    return f"""
DECLARE batch_run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE batch_run_id STRING DEFAULT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', batch_run_ts);

CREATE TEMP TABLE NormalizedSnapshotSource AS
SELECT
  CAST(src.id AS STRING) AS id,
  CAST(src.product_id AS STRING) AS product_id,
  CAST(src.variant_id AS STRING) AS variant_id,
  CAST(src.source_variant_key AS STRING) AS source_variant_key,
  CAST(src.title AS STRING) AS title,
  CAST(src.vendor AS STRING) AS vendor,
  CAST(src.product_link AS STRING) AS product_link,
  CAST(src.store AS STRING) AS store,
  CAST(src.store_url AS STRING) AS store_url,
  CAST(src.image AS STRING) AS image,
  CAST(src.variant_title AS STRING) AS variant_title,
  CAST(src.price AS STRING) AS price,
  CAST(src.weight_g AS STRING) AS weight_g,
  CAST(src.in_stock AS BOOL) AS in_stock,
  CAST(src.variant_image AS STRING) AS variant_image,
  CAST(src.high_price AS STRING) AS high_price,
  CAST(src.low_price AS STRING) AS low_price,
  CAST(src.tags AS STRING) AS tags,
  CAST(src.IsDistanceDriver AS BOOL) AS IsDistanceDriver,
  CAST(src.IsFairwayDriver AS BOOL) AS IsFairwayDriver,
  CAST(src.IsMidrange AS BOOL) AS IsMidrange,
  CAST(src.IsPutter AS BOOL) AS IsPutter,
  CAST(src.BodyHtml AS STRING) AS BodyHtml,
  CAST(src.product_type AS STRING) AS product_type,
  CAST(src.source AS STRING) AS source,
  CAST(src.retailer AS STRING) AS retailer,
  CAST(src.raw_vendor AS STRING) AS raw_vendor,
  CAST(src.normalized_manufacturer AS STRING) AS normalized_manufacturer,
  CAST(src.normalized_model AS STRING) AS normalized_model,
  CAST(src.item_type AS STRING) AS item_type,
  CAST(src.is_disc AS BOOL) AS is_disc,
  CAST(src.item_type_confidence AS FLOAT64) AS item_type_confidence,
  CAST(src.manufacturer_confidence AS FLOAT64) AS manufacturer_confidence,
  CAST(src.model_confidence AS FLOAT64) AS model_confidence,
  CAST(src.normalization_confidence AS FLOAT64) AS normalization_confidence,
  CAST(src.normalization_source AS STRING) AS normalization_source,
  CAST(src.model_decision_level AS STRING) AS model_decision_level,
  CAST(src.normalization_version AS STRING) AS normalization_version,
  CAST(src.model_rules_version AS STRING) AS model_rules_version
FROM {variant_snapshot_view} AS src
WHERE NULLIF(TRIM(CAST(src.id AS STRING)), '') IS NOT NULL;

CREATE TEMP TABLE NewSnapshotRaw AS
SELECT
  v.id,
  v.product_id,
  v.variant_id,
  v.source_variant_key,
  v.title,
  v.vendor,
  v.product_link,
  v.store,
  v.store_url,
  v.image,
  v.variant_title,
  CAST(v.price AS STRING) AS price,
  CAST(v.weight_g AS STRING) AS weight_g,
  CAST(v.in_stock AS BOOL) AS in_stock,
  v.variant_image,
  CAST(v.high_price AS STRING) AS high_price,
  CAST(v.low_price AS STRING) AS low_price,
  v.tags,
  CAST(v.IsDistanceDriver AS BOOL) AS IsDistanceDriver,
  CAST(v.IsFairwayDriver AS BOOL) AS IsFairwayDriver,
  CAST(v.IsMidrange AS BOOL) AS IsMidrange,
  CAST(v.IsPutter AS BOOL) AS IsPutter,
  v.BodyHtml,
  v.product_type,
  v.source,
  v.retailer,
  v.raw_vendor,
  v.normalized_manufacturer,
  v.normalized_model,
  v.item_type,
  v.is_disc,
  v.item_type_confidence,
  v.manufacturer_confidence,
  v.model_confidence,
  v.normalization_confidence,
  v.normalization_source,
  v.model_decision_level,
  v.normalization_version,
  v.model_rules_version,
  TO_HEX(SHA256(
    TO_JSON_STRING(STRUCT(
      v.title,
      v.vendor,
      v.product_link,
      v.store,
      v.store_url,
      v.image,
      v.variant_title,
      CAST(v.price AS STRING),
      CAST(v.weight_g AS STRING),
      CAST(v.in_stock AS BOOL),
      v.variant_image,
      CAST(v.high_price AS STRING),
      CAST(v.low_price AS STRING),
      v.tags,
      CAST(v.IsDistanceDriver AS BOOL),
      CAST(v.IsFairwayDriver AS BOOL),
      CAST(v.IsMidrange AS BOOL),
      CAST(v.IsPutter AS BOOL),
      v.BodyHtml,
      v.product_type,
      v.source_variant_key,
      v.source,
      v.retailer,
      v.raw_vendor,
      v.normalized_manufacturer,
      v.normalized_model,
      v.item_type,
      v.is_disc,
      v.item_type_confidence,
      v.manufacturer_confidence,
      v.model_confidence,
      v.normalization_confidence,
      v.normalization_source,
      v.model_decision_level,
      v.normalization_version,
      v.model_rules_version
    ))
  )) AS row_hash
FROM NormalizedSnapshotSource AS v;

CREATE TEMP TABLE NewSnapshot AS
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
  model_rules_version,
  row_hash
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY row_hash DESC) AS rn
  FROM NewSnapshotRaw
)
WHERE rn = 1;

CREATE TEMP TABLE OldState AS
SELECT *
FROM {variant_state_table}
WHERE CAST(id AS STRING) IS NOT NULL
  AND TRIM(CAST(id AS STRING)) != '';

INSERT INTO {variant_changes_table} (
  batch_run_id,
  batch_run_ts,
  operation,
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
  model_rules_version,
  row_hash,
  change_ts
)
SELECT
  batch_run_id,
  batch_run_ts,
  'UPSERT' AS operation,
  ns.id,
  ns.product_id,
  ns.variant_id,
  ns.source_variant_key,
  ns.title,
  ns.vendor,
  ns.product_link,
  ns.store,
  ns.store_url,
  ns.image,
  ns.variant_title,
  ns.price,
  ns.weight_g,
  ns.in_stock,
  ns.variant_image,
  ns.high_price,
  ns.low_price,
  ns.tags,
  ns.IsDistanceDriver,
  ns.IsFairwayDriver,
  ns.IsMidrange,
  ns.IsPutter,
  ns.BodyHtml,
  ns.product_type,
  ns.source,
  ns.retailer,
  ns.raw_vendor,
  ns.normalized_manufacturer,
  ns.normalized_model,
  ns.item_type,
  ns.is_disc,
  ns.item_type_confidence,
  ns.manufacturer_confidence,
  ns.model_confidence,
  ns.normalization_confidence,
  ns.normalization_source,
  ns.model_decision_level,
  ns.normalization_version,
  ns.model_rules_version,
  ns.row_hash,
  CURRENT_TIMESTAMP() AS change_ts
FROM NewSnapshot AS ns
LEFT JOIN OldState AS os
ON ns.id = os.id
WHERE ns.id IS NOT NULL
  AND TRIM(ns.id) != ''
  AND (os.id IS NULL OR ns.row_hash != os.row_hash);

INSERT INTO {variant_changes_table} (
  batch_run_id,
  batch_run_ts,
  operation,
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
  model_rules_version,
  row_hash,
  change_ts
)
SELECT
  batch_run_id,
  batch_run_ts,
  'DELETE' AS operation,
  os.id,
  os.product_id,
  os.variant_id,
  os.source_variant_key,
  NULL AS title,
  NULL AS vendor,
  NULL AS product_link,
  NULL AS store,
  NULL AS store_url,
  NULL AS image,
  NULL AS variant_title,
  NULL AS price,
  NULL AS weight_g,
  NULL AS in_stock,
  NULL AS variant_image,
  NULL AS high_price,
  NULL AS low_price,
  NULL AS tags,
  NULL AS IsDistanceDriver,
  NULL AS IsFairwayDriver,
  NULL AS IsMidrange,
  NULL AS IsPutter,
  NULL AS BodyHtml,
  NULL AS product_type,
  os.source,
  os.retailer,
  os.raw_vendor,
  os.normalized_manufacturer,
  os.normalized_model,
  os.item_type,
  os.is_disc,
  os.item_type_confidence,
  os.manufacturer_confidence,
  os.model_confidence,
  os.normalization_confidence,
  os.normalization_source,
  os.model_decision_level,
  os.normalization_version,
  os.model_rules_version,
  os.row_hash,
  CURRENT_TIMESTAMP() AS change_ts
FROM OldState AS os
LEFT JOIN NewSnapshot AS ns
ON os.id = ns.id
WHERE os.id IS NOT NULL
  AND TRIM(os.id) != ''
  AND ns.id IS NULL;

MERGE {variant_state_table} AS T
USING NewSnapshot AS S
ON T.id = S.id
WHEN MATCHED THEN
  UPDATE SET
    T.product_id = S.product_id,
    T.variant_id = S.variant_id,
    T.source_variant_key = S.source_variant_key,
    T.title = S.title,
    T.vendor = S.vendor,
    T.product_link = S.product_link,
    T.store = S.store,
    T.store_url = S.store_url,
    T.image = S.image,
    T.variant_title = S.variant_title,
    T.price = S.price,
    T.weight_g = S.weight_g,
    T.in_stock = S.in_stock,
    T.variant_image = S.variant_image,
    T.high_price = S.high_price,
    T.low_price = S.low_price,
    T.tags = S.tags,
    T.IsDistanceDriver = S.IsDistanceDriver,
    T.IsFairwayDriver = S.IsFairwayDriver,
    T.IsMidrange = S.IsMidrange,
    T.IsPutter = S.IsPutter,
    T.BodyHtml = S.BodyHtml,
    T.product_type = S.product_type,
    T.source = S.source,
    T.retailer = S.retailer,
    T.raw_vendor = S.raw_vendor,
    T.normalized_manufacturer = S.normalized_manufacturer,
    T.normalized_model = S.normalized_model,
    T.item_type = S.item_type,
    T.is_disc = S.is_disc,
    T.item_type_confidence = S.item_type_confidence,
    T.manufacturer_confidence = S.manufacturer_confidence,
    T.model_confidence = S.model_confidence,
    T.normalization_confidence = S.normalization_confidence,
    T.normalization_source = S.normalization_source,
    T.model_decision_level = S.model_decision_level,
    T.normalization_version = S.normalization_version,
    T.model_rules_version = S.model_rules_version,
    T.row_hash = S.row_hash,
    T.last_seen_at = batch_run_ts
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
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
    model_rules_version,
    row_hash,
    last_seen_at
  )
  VALUES (
    S.id,
    S.product_id,
    S.variant_id,
    S.source_variant_key,
    S.title,
    S.vendor,
    S.product_link,
    S.store,
    S.store_url,
    S.image,
    S.variant_title,
    S.price,
    S.weight_g,
    S.in_stock,
    S.variant_image,
    S.high_price,
    S.low_price,
    S.tags,
    S.IsDistanceDriver,
    S.IsFairwayDriver,
    S.IsMidrange,
    S.IsPutter,
    S.BodyHtml,
    S.product_type,
    S.source,
    S.retailer,
    S.raw_vendor,
    S.normalized_manufacturer,
    S.normalized_model,
    S.item_type,
    S.is_disc,
    S.item_type_confidence,
    S.manufacturer_confidence,
    S.model_confidence,
    S.normalization_confidence,
    S.normalization_source,
    S.model_decision_level,
    S.normalization_version,
    S.model_rules_version,
    S.row_hash,
    batch_run_ts
  )
WHEN NOT MATCHED BY SOURCE THEN
  DELETE;
"""


def build_variant_identity_map_sql(project_id, dataset):
    identity_map_table = build_table_ref(project_id, dataset, "VariantIdentityMap")
    variant_snapshot_view = build_table_ref(project_id, dataset, "v_VariantSnapshot")

    return f"""
CREATE TABLE IF NOT EXISTS {identity_map_table} (
  source_variant_key STRING NOT NULL,
  legacy_id STRING NOT NULL,
  source STRING,
  first_seen_at TIMESTAMP NOT NULL,
  last_seen_at TIMESTAMP NOT NULL,
  is_current BOOL NOT NULL
)
CLUSTER BY source, source_variant_key;

ASSERT (
  SELECT COUNTIF(NULLIF(TRIM(source_variant_key), '') IS NULL)
  FROM {variant_snapshot_view}
) = 0 AS 'v_VariantSnapshot contains missing source_variant_key values';

ASSERT (
  SELECT COUNT(*) = COUNT(DISTINCT source_variant_key)
  FROM {variant_snapshot_view}
) AS 'v_VariantSnapshot contains duplicate source_variant_key values';

MERGE {identity_map_table} AS target
USING (
  SELECT source_variant_key, id AS legacy_id, source
  FROM {variant_snapshot_view}
) AS source
ON target.source_variant_key = source.source_variant_key
AND target.legacy_id = source.legacy_id
WHEN MATCHED THEN
  UPDATE SET
    target.source = source.source,
    target.last_seen_at = CURRENT_TIMESTAMP(),
    target.is_current = TRUE
WHEN NOT MATCHED THEN
  INSERT (
    source_variant_key,
    legacy_id,
    source,
    first_seen_at,
    last_seen_at,
    is_current
  )
  VALUES (
    source.source_variant_key,
    source.legacy_id,
    source.source,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    TRUE
  );

UPDATE {identity_map_table} AS target
SET is_current = FALSE
WHERE target.is_current
  AND NOT EXISTS (
    SELECT 1
    FROM {variant_snapshot_view} AS current_snapshot
    WHERE current_snapshot.source_variant_key = target.source_variant_key
      AND current_snapshot.id = target.legacy_id
  );
"""


def ensure_variant_table_schemas(client, project_id, dataset):
    table_fields = {
        "VariantChanges": (("change_ts", "TIMESTAMP"),) + NORMALIZATION_SCHEMA_FIELDS,
        "VariantState": NORMALIZATION_SCHEMA_FIELDS,
    }

    for table_name, required_fields in table_fields.items():
        table_ref = f"{project_id}.{dataset}.{table_name}"
        table = client.get_table(table_ref)
        existing_fields = {field.name.lower() for field in table.schema}
        missing_fields = [
            bigquery.SchemaField(field_name, field_type)
            for field_name, field_type in required_fields
            if field_name.lower() not in existing_fields
        ]
        if not missing_fields:
            print(f"Schema is current for {table_ref}")
            continue

        table.schema = list(table.schema) + missing_fields
        client.update_table(table, ["schema"])
        added_names = ", ".join(field.name for field in missing_fields)
        print(f"Added schema fields to {table_ref}: {added_names}")


def prepare_source_views(client, project_id, dataset):
    print(f"Building {project_id}.{dataset}.DerivedProductType")
    derived_job = client.query(build_derived_product_type_sql(project_id, dataset))
    derived_job.result()

    for source_view_sql in build_source_view_sqls(project_id, dataset):
        source_view_job = client.query(source_view_sql)
        source_view_job.result()
    print(f"Refreshed code-managed source views in {project_id}.{dataset}")


def run_normalize_data(project_id=None, dataset=None):
    resolved_project_id = (project_id or get_gcp_project_id()).strip()
    resolved_dataset = (dataset or get_bigquery_dataset()).strip()
    client = bigquery.Client(project=resolved_project_id)

    prepare_source_views(client, resolved_project_id, resolved_dataset)
    summary = run_normalization(client, resolved_project_id, resolved_dataset)
    print("normalize-data completed successfully.")
    return summary


def run_process_data(project_id=None, dataset=None, include_normalization=True):
    resolved_project_id = (project_id or get_gcp_project_id()).strip()
    resolved_dataset = (dataset or get_bigquery_dataset()).strip()
    client = bigquery.Client(project=resolved_project_id)

    if include_normalization:
        prepare_source_views(client, resolved_project_id, resolved_dataset)
        run_normalization(client, resolved_project_id, resolved_dataset)

    print(f"Refreshing {resolved_project_id}.{resolved_dataset}.v_VariantSnapshot")
    snapshot_view_job = client.query(build_variant_snapshot_view_sql(resolved_project_id, resolved_dataset))
    snapshot_view_job.result()

    ensure_variant_table_schemas(client, resolved_project_id, resolved_dataset)

    print(f"Refreshing {resolved_project_id}.{resolved_dataset}.VariantState and VariantChanges")
    variant_job = client.query(build_variant_state_sql(resolved_project_id, resolved_dataset))
    variant_job.result()

    print(f"Refreshing {resolved_project_id}.{resolved_dataset}.VariantIdentityMap")
    identity_map_job = client.query(
        build_variant_identity_map_sql(resolved_project_id, resolved_dataset)
    )
    identity_map_job.result()

    print("processData completed successfully.")


def main():
    run_process_data()


if __name__ == "__main__":
    main()
