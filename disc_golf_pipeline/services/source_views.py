"""Code-managed BigQuery source views for Shopify and Infinite Discs."""


def build_table_ref(project_id, dataset, table_name):
    return f"`{project_id}.{dataset}.{table_name}`"


def build_shopify_variants_view_sql(project_id, dataset):
    products_table = build_table_ref(project_id, dataset, "Products")
    product_info_table = build_table_ref(project_id, dataset, "ProductInfo")
    stores_table = build_table_ref(project_id, dataset, "Stores")
    derived_product_type_table = build_table_ref(project_id, dataset, "DerivedProductType")
    destination_view = build_table_ref(project_id, dataset, "v_ShopifyVariants")

    return f"""
CREATE OR REPLACE VIEW {destination_view} AS
WITH FeaturedImage AS (
  SELECT DISTINCT
    MainProductId,
    VariantId,
    FIRST_VALUE(VarFeaturedImage) OVER (
      PARTITION BY MainProductId
      ORDER BY VariantUpdatedAt DESC
    ) AS Image
  FROM {products_table}
  WHERE VariantAvailable = TRUE
    AND VarFeaturedImage IS NOT NULL
),
base AS (
  SELECT
    CAST(FARM_FINGERPRINT(CONCAT(
      CAST(i.MainProductId AS STRING),
      CAST(i.Store AS STRING)
    )) AS STRING) AS product_id,
    i.Title AS title,
    COALESCE(CONCAT(s.URL, 'products/', i.Handle), i.ProductLink) AS product_link,
    s.URL AS store_url,
    i.Store AS store,
    i.Vendor AS vendor,
    fi.Image AS product_image,
    p.VariantId,
    p.VariantTitle,
    p.VariantPrice,
    p.VariantGrams,
    p.VariantAvailable,
    p.VarFeaturedImage,
    i.Tags AS tags,
    dpt.IsDistanceDriver,
    dpt.IsFairwayDriver,
    dpt.IsMidrange,
    dpt.IsPutter,
    dpt.BodyHtml,
    i.ProductType AS product_type,
    MAX(p.VariantPrice) OVER (
      PARTITION BY i.MainProductId, i.Store
    ) AS high_price,
    MIN(p.VariantPrice) OVER (
      PARTITION BY i.MainProductId, i.Store
    ) AS low_price
  FROM {product_info_table} AS i
  INNER JOIN {products_table} AS p
    ON i.MainProductId = p.MainProductId
  LEFT JOIN FeaturedImage AS fi
    ON i.MainProductId = fi.MainProductId
   AND p.VariantId = fi.VariantId
  LEFT JOIN {stores_table} AS s
    ON i.Store = s.StoreName
  LEFT JOIN {derived_product_type_table} AS dpt
    ON i.MainProductId = dpt.MainProductId
  WHERE i.Store <> 'unitedsport'
    AND fi.Image IS NOT NULL
)
SELECT DISTINCT
  CONCAT(product_id, '-', CAST(VariantId AS STRING)) AS id,
  product_id,
  CAST(VariantId AS STRING) AS variant_id,
  title,
  vendor,
  product_link,
  store,
  store_url,
  product_image AS image,
  VariantTitle AS variant_title,
  VariantPrice AS price,
  VariantGrams AS weight_g,
  VariantAvailable AS in_stock,
  VarFeaturedImage AS variant_image,
  high_price,
  low_price,
  tags,
  IsDistanceDriver,
  IsFairwayDriver,
  IsMidrange,
  IsPutter,
  BodyHtml,
  product_type
FROM base
"""


def build_infinite_variants_view_sql(project_id, dataset):
    infinite_discs_table = build_table_ref(project_id, dataset, "InfiniteDiscs")
    derived_product_type_table = build_table_ref(project_id, dataset, "DerivedProductType")
    destination_view = build_table_ref(project_id, dataset, "v_InfiniteVariants")

    return f"""
CREATE OR REPLACE VIEW {destination_view} AS
WITH img AS (
  SELECT DISTINCT
    p.ManufacturerName,
    p.PlasticName,
    p.ModelName,
    COALESCE(p.AdditionalInputTitle, '1') AS AdditionalInputTitle,
    FIRST_VALUE(p.StockImage) OVER (
      PARTITION BY
        p.ManufacturerName,
        p.PlasticName,
        p.ModelName,
        COALESCE(p.AdditionalInputTitle, '1')
      ORDER BY p.Id
    ) AS FirstStockImage
  FROM {infinite_discs_table} AS p
),
base AS (
  SELECT
    CAST(FARM_FINGERPRINT(CONCAT(
      CAST(p.ManufacturerName AS STRING),
      CAST(p.PlasticName AS STRING),
      CAST(p.ModelName AS STRING),
      'infinitediscs'
    )) AS STRING) AS product_id,
    p.ManufacturerName,
    p.PlasticName,
    p.ModelName,
    COALESCE(p.AdditionalInputTitle, '1') AS AdditionalInputTitle,
    p.ColorName,
    p.StockPrice,
    p.StockWeight,
    p.AvailableStock,
    p.StockImage AS variant_image,
    p.ModelLink,
    CONCAT(
      COALESCE(p.ManufacturerName, ''), ' ',
      COALESCE(p.PlasticName, ''), ' ',
      COALESCE(p.ModelName, '')
    ) AS title,
    'https://www.infinitediscs.com/' AS store_url,
    'infinitediscs' AS store,
    p.ManufacturerName AS vendor,
    img.FirstStockImage AS product_image,
    MAX(p.StockPrice) OVER (
      PARTITION BY
        p.ManufacturerName,
        p.PlasticName,
        p.ModelName,
        COALESCE(p.AdditionalInputTitle, '1')
    ) AS high_price,
    MIN(p.StockPrice) OVER (
      PARTITION BY
        p.ManufacturerName,
        p.PlasticName,
        p.ModelName,
        COALESCE(p.AdditionalInputTitle, '1')
    ) AS low_price,
    dpt.IsDistanceDriver,
    dpt.IsFairwayDriver,
    dpt.IsMidrange,
    dpt.IsPutter,
    dpt.BodyHtml,
    'Discs' AS product_type
  FROM {infinite_discs_table} AS p
  LEFT JOIN img
    ON p.ManufacturerName = img.ManufacturerName
   AND p.PlasticName = img.PlasticName
   AND p.ModelName = img.ModelName
   AND COALESCE(p.AdditionalInputTitle, '1') = img.AdditionalInputTitle
  LEFT JOIN {derived_product_type_table} AS dpt
    ON p.Id = dpt.MainProductId
  WHERE p.AvailableStock > 0
)
SELECT DISTINCT
  CONCAT(
    product_id,
    '-',
    CAST(FARM_FINGERPRINT(CONCAT(
      'infinitediscs', '|',
      COALESCE(LOWER(TRIM(ColorName)), ''), '|',
      COALESCE(LOWER(TRIM(PlasticName)), ''), '|',
      COALESCE(LOWER(TRIM(ModelName)), ''), '|',
      COALESCE(LOWER(TRIM(AdditionalInputTitle)), '1'), '|',
      CAST(StockWeight AS STRING)
    )) AS STRING)
  ) AS id,
  product_id,
  CAST(FARM_FINGERPRINT(CONCAT(
    'infinitediscs', '|',
    COALESCE(LOWER(TRIM(ColorName)), ''), '|',
    COALESCE(LOWER(TRIM(PlasticName)), ''), '|',
    COALESCE(LOWER(TRIM(ModelName)), ''), '|',
    COALESCE(LOWER(TRIM(AdditionalInputTitle)), '1'), '|',
    CAST(StockWeight AS STRING)
  )) AS STRING) AS variant_id,
  title,
  vendor,
  CONCAT(ModelLink, '/', PlasticName) AS product_link,
  store,
  store_url,
  product_image AS image,
  CONCAT(
    COALESCE(ColorName, ''), ' ',
    COALESCE(PlasticName, ''), ' ',
    COALESCE(ModelName, ''),
    COALESCE(AdditionalInputTitle, '')
  ) AS variant_title,
  StockPrice AS price,
  StockWeight AS weight_g,
  AvailableStock > 0 AS in_stock,
  variant_image,
  high_price,
  low_price,
  CAST(NULL AS STRING) AS tags,
  IsDistanceDriver,
  IsFairwayDriver,
  IsMidrange,
  IsPutter,
  BodyHtml,
  product_type
FROM base
"""


def build_source_view_sqls(project_id, dataset):
    return [
        build_shopify_variants_view_sql(project_id, dataset),
        build_infinite_variants_view_sql(project_id, dataset),
    ]
