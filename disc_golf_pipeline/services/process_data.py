import os

from google.cloud import bigquery

DEFAULT_PROJECT_ID = "disc-golf-price-compare"
DEFAULT_DATASET = "DiscGolfProducts"


def get_gcp_project_id():
    return os.getenv("GCP_PROJECT_ID", DEFAULT_PROJECT_ID).strip()


def get_bigquery_dataset():
    return os.getenv("BIGQUERY_DATASET", DEFAULT_DATASET).strip()


def build_table_ref(project_id, dataset, table_name):
    return f"`{project_id}.{dataset}.{table_name}`"


def build_variant_snapshot_view_sql(project_id, dataset):
    infinite_variants_view = build_table_ref(project_id, dataset, "v_InfiniteVariants")
    shopify_variants_view = build_table_ref(project_id, dataset, "v_ShopifyVariants")
    destination_view = build_table_ref(project_id, dataset, "v_VariantSnapshot")

    return f"""
CREATE OR REPLACE VIEW {destination_view} AS
SELECT
  CAST(id AS STRING) AS id,
  CAST(product_id AS STRING) AS product_id,
  CAST(variant_id AS STRING) AS variant_id,
  CAST(title AS STRING) AS title,
  CAST(vendor AS STRING) AS vendor,
  CAST(product_link AS STRING) AS product_link,
  CAST(store AS STRING) AS store,
  CAST(store_url AS STRING) AS store_url,
  CAST(image AS STRING) AS image,
  CAST(variant_title AS STRING) AS variant_title,
  CAST(price AS FLOAT64) AS price,
  CAST(weight_g AS INT64) AS weight_g,
  CAST(in_stock AS BOOL) AS in_stock,
  CAST(variant_image AS STRING) AS variant_image,
  CAST(high_price AS FLOAT64) AS high_price,
  CAST(low_price AS FLOAT64) AS low_price,
  CAST(tags AS STRING) AS tags,
  CAST(IsDistanceDriver AS INT64) AS IsDistanceDriver,
  CAST(IsFairwayDriver AS INT64) AS IsFairwayDriver,
  CAST(IsMidrange AS INT64) AS IsMidrange,
  CAST(IsPutter AS INT64) AS IsPutter,
  CAST(BodyHtml AS STRING) AS BodyHtml,
  CAST(product_type AS STRING) AS product_type
FROM {infinite_variants_view}

UNION ALL

SELECT
  CAST(id AS STRING) AS id,
  CAST(product_id AS STRING) AS product_id,
  CAST(variant_id AS STRING) AS variant_id,
  CAST(title AS STRING) AS title,
  CAST(vendor AS STRING) AS vendor,
  CAST(product_link AS STRING) AS product_link,
  CAST(store AS STRING) AS store,
  CAST(store_url AS STRING) AS store_url,
  CAST(image AS STRING) AS image,
  CAST(variant_title AS STRING) AS variant_title,
  CAST(price AS FLOAT64) AS price,
  CAST(weight_g AS INT64) AS weight_g,
  CAST(in_stock AS BOOL) AS in_stock,
  CAST(variant_image AS STRING) AS variant_image,
  CAST(high_price AS FLOAT64) AS high_price,
  CAST(low_price AS FLOAT64) AS low_price,
  CAST(tags AS STRING) AS tags,
  CAST(IsDistanceDriver AS INT64) AS IsDistanceDriver,
  CAST(IsFairwayDriver AS INT64) AS IsFairwayDriver,
  CAST(IsMidrange AS INT64) AS IsMidrange,
  CAST(IsPutter AS INT64) AS IsPutter,
  CAST(BodyHtml AS STRING) AS BodyHtml,
  CAST(product_type AS STRING) AS product_type
FROM {shopify_variants_view}
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
    infinite_variants_view = build_table_ref(project_id, dataset, "v_InfiniteVariants")
    shopify_variants_view = build_table_ref(project_id, dataset, "v_ShopifyVariants")

    return f"""
DECLARE batch_run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE batch_run_id STRING DEFAULT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', batch_run_ts);

ALTER TABLE {variant_changes_table}
ADD COLUMN IF NOT EXISTS change_ts TIMESTAMP;

CREATE TEMP TABLE NormalizedSnapshotSource AS
SELECT
  CAST(id AS STRING) AS id,
  CAST(product_id AS STRING) AS product_id,
  CAST(variant_id AS STRING) AS variant_id,
  CAST(title AS STRING) AS title,
  CAST(vendor AS STRING) AS vendor,
  CAST(product_link AS STRING) AS product_link,
  CAST(store AS STRING) AS store,
  CAST(store_url AS STRING) AS store_url,
  CAST(image AS STRING) AS image,
  CAST(variant_title AS STRING) AS variant_title,
  CAST(price AS STRING) AS price,
  CAST(weight_g AS STRING) AS weight_g,
  CAST(in_stock AS BOOL) AS in_stock,
  CAST(variant_image AS STRING) AS variant_image,
  CAST(high_price AS STRING) AS high_price,
  CAST(low_price AS STRING) AS low_price,
  CAST(tags AS STRING) AS tags,
  CAST(IsDistanceDriver AS BOOL) AS IsDistanceDriver,
  CAST(IsFairwayDriver AS BOOL) AS IsFairwayDriver,
  CAST(IsMidrange AS BOOL) AS IsMidrange,
  CAST(IsPutter AS BOOL) AS IsPutter,
  CAST(BodyHtml AS STRING) AS BodyHtml,
  CAST(product_type AS STRING) AS product_type
FROM {infinite_variants_view}
WHERE CAST(id AS STRING) IS NOT NULL
  AND TRIM(CAST(id AS STRING)) != ''

UNION ALL

SELECT
  CAST(id AS STRING) AS id,
  CAST(product_id AS STRING) AS product_id,
  CAST(variant_id AS STRING) AS variant_id,
  CAST(title AS STRING) AS title,
  CAST(vendor AS STRING) AS vendor,
  CAST(product_link AS STRING) AS product_link,
  CAST(store AS STRING) AS store,
  CAST(store_url AS STRING) AS store_url,
  CAST(image AS STRING) AS image,
  CAST(variant_title AS STRING) AS variant_title,
  CAST(price AS STRING) AS price,
  CAST(weight_g AS STRING) AS weight_g,
  CAST(in_stock AS BOOL) AS in_stock,
  CAST(variant_image AS STRING) AS variant_image,
  CAST(high_price AS STRING) AS high_price,
  CAST(low_price AS STRING) AS low_price,
  CAST(tags AS STRING) AS tags,
  CAST(IsDistanceDriver AS BOOL) AS IsDistanceDriver,
  CAST(IsFairwayDriver AS BOOL) AS IsFairwayDriver,
  CAST(IsMidrange AS BOOL) AS IsMidrange,
  CAST(IsPutter AS BOOL) AS IsPutter,
  CAST(BodyHtml AS STRING) AS BodyHtml,
  CAST(product_type AS STRING) AS product_type
FROM {shopify_variants_view}
WHERE CAST(id AS STRING) IS NOT NULL
  AND TRIM(CAST(id AS STRING)) != '';

CREATE TEMP TABLE NewSnapshotRaw AS
SELECT
  v.id,
  v.product_id,
  v.variant_id,
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
      v.product_type
    ))
  )) AS row_hash
FROM NormalizedSnapshotSource AS v;

CREATE TEMP TABLE NewSnapshot AS
SELECT
  id,
  product_id,
  variant_id,
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
)
SELECT
  batch_run_id,
  batch_run_ts,
  'UPSERT' AS operation,
  ns.id,
  ns.product_id,
  ns.variant_id,
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
)
SELECT
  batch_run_id,
  batch_run_ts,
  'DELETE' AS operation,
  os.id,
  os.product_id,
  os.variant_id,
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
    T.row_hash = S.row_hash,
    T.last_seen_at = batch_run_ts
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    id,
    product_id,
    variant_id,
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
    last_seen_at
  )
  VALUES (
    S.id,
    S.product_id,
    S.variant_id,
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
    S.row_hash,
    batch_run_ts
  )
WHEN NOT MATCHED BY SOURCE THEN
  DELETE;
"""


def run_process_data(project_id=None, dataset=None):
    resolved_project_id = (project_id or get_gcp_project_id()).strip()
    resolved_dataset = (dataset or get_bigquery_dataset()).strip()
    client = bigquery.Client(project=resolved_project_id)

    print(f"Building {resolved_project_id}.{resolved_dataset}.DerivedProductType")
    derived_job = client.query(build_derived_product_type_sql(resolved_project_id, resolved_dataset))
    derived_job.result()

    print(f"Refreshing {resolved_project_id}.{resolved_dataset}.v_VariantSnapshot")
    snapshot_view_job = client.query(build_variant_snapshot_view_sql(resolved_project_id, resolved_dataset))
    snapshot_view_job.result()

    print(f"Refreshing {resolved_project_id}.{resolved_dataset}.VariantState and VariantChanges")
    variant_job = client.query(build_variant_state_sql(resolved_project_id, resolved_dataset))
    variant_job.result()

    print("processData completed successfully.")


def main():
    run_process_data()


if __name__ == "__main__":
    main()
