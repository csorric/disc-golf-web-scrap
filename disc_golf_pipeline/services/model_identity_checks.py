"""Regression checks for cached variant identities overriding resolved products."""

from disc_golf_pipeline.services.normalization import build_normalized_variant_snapshot_sql


def build_model_identity_fixture_query():
    # Run the complete production projection, including identity/provenance joins.
    raw_strings = ('title', 'vendor', 'product_link', 'store_url', 'image', 'variant_title',
                   'variant_image', 'tags', 'BodyHtml', 'product_type')
    raw_defaults = ', '.join(f"'' AS {name}" for name in raw_strings)
    query = build_normalized_variant_snapshot_sql('fixture', 'identity').partition(' AS\n')[2]
    for table in ('v_ShopifyVariants', 'v_InfiniteVariants', 'NormalizedProducts', 'VariantDiscModelDecisions'):
        query = query.replace(f'`fixture.identity.{table}`', table)
    return f"""
WITH cases AS (
  SELECT * FROM UNNEST([
    STRUCT('resolved' AS id, 'Destroyer' AS product_model, 'disc' AS item_type,
      'resolved' AS override_product, 'ACCEPT' AS bucket, 'Alien' AS variant_model),
    STRUCT('multi_alien', NULL, 'disc', 'multi_alien', 'ACCEPT', 'Alien'),
    STRUCT('multi_destroyer', NULL, 'disc', 'multi_destroyer', 'ACCEPT', 'Destroyer'),
    STRUCT('wrong_product', NULL, 'disc', 'other', 'ACCEPT', 'Alien'),
    STRUCT('unaccepted', NULL, 'disc', 'unaccepted', 'POSSIBLE', 'Alien'),
    STRUCT('non_disc', NULL, 'bag', 'non_disc', 'ACCEPT', 'Alien')
  ])
), v_ShopifyVariants AS (
  SELECT id, id AS variant_id, id AS product_id, 'fixture' AS store, {raw_defaults},
    20.0 AS price, 175 AS weight_g, TRUE AS in_stock, 20.0 AS high_price, 20.0 AS low_price,
    FALSE AS IsPutter, FALSE AS IsMidrange, FALSE AS IsFairwayDriver, TRUE AS IsDistanceDriver
  FROM cases
), v_InfiniteVariants AS (SELECT * FROM v_ShopifyVariants WHERE FALSE),
NormalizedProducts AS (
  SELECT CONCAT('shopify:fixture:', id) AS product_key, product_model AS normalized_model,
    'Innova Champion Discs' AS normalized_manufacturer, 'fixture' AS retailer,
    item_type, item_type = 'disc' AS is_disc, 1.0 AS item_type_confidence,
    1.0 AS manufacturer_confidence, 1.0 AS model_confidence,
    'fixture' AS item_type_source, 'fixture' AS manufacturer_source, 'product_fixture' AS model_source,
    'fixture' AS normalization_version, JSON '{{}}' AS normalization_evidence,
    'fixture' AS source_row_hash
  FROM cases
), VariantDiscModelDecisions AS (
  SELECT id AS variant_id, CONCAT('shopify:fixture:', override_product) AS product_key,
    bucket AS decision_bucket, variant_model AS normalized_model,
    'Innova Champion Discs' AS normalized_manufacturer, 0.9 AS decision_confidence,
    'cached_variant_fixture' AS decision_source, 'fixture' AS disc_entity_id,
    'fixture' AS model_rules_version
  FROM cases
)
SELECT id, normalized_model, model_decision_level, normalization_source,
  variant_disc_entity_id FROM ({query})
"""


def validate_model_identity_regressions(client):
    rows = {row['id']: row for row in client.query(build_model_identity_fixture_query()).result()}
    expected = {'resolved': ('Destroyer', 'product'), 'multi_alien': ('Alien', 'variant'),
                'multi_destroyer': ('Destroyer', 'variant'), 'wrong_product': (None, 'product'),
                'unaccepted': (None, 'product'), 'non_disc': (None, 'product')}
    actual = {key: (row['normalized_model'], row['model_decision_level']) for key, row in rows.items()}
    if actual != expected:
        raise AssertionError(f'Cached model identity regression: {actual}')
    for key, (_, level) in expected.items():
        row = rows[key]
        if level == 'product' and (row['variant_disc_entity_id'] is not None
                                  or 'cached_variant_fixture' in row['normalization_source']):
            raise AssertionError(f'Stale variant provenance retained: {key}')
    print('Passed 6 cached model identity and provenance fixtures.', flush=True)


def validate_production_model_identity(client, project, dataset):
    sql = f"""
ASSERT (SELECT COUNT(*) = 0
  FROM `{project}.{dataset}.NormalizedVariantSnapshot` snapshot
  JOIN `{project}.{dataset}.NormalizedProducts` product
    ON snapshot.source = product.source AND snapshot.store = product.store
    AND snapshot.product_id = product.product_id
  WHERE product.normalized_model IS NOT NULL
    AND (snapshot.normalized_model IS DISTINCT FROM product.normalized_model
      OR snapshot.normalized_manufacturer IS DISTINCT FROM product.normalized_manufacturer
      OR snapshot.model_decision_level != 'product')) AS 'Variant overrides resolved product identity';
CREATE TEMP TABLE destroyer_target AS
SELECT * FROM `{project}.{dataset}.VariantState`
WHERE source_variant_key = 'shopify:armorydiscgolf.com:49798863749362';
ASSERT (SELECT COUNT(*) = 1 FROM destroyer_target) AS 'Reported Armory variant missing';
ASSERT (SELECT COUNTIF(normalized_model = 'Destroyer'
  AND normalized_manufacturer = 'Innova Champion Discs'
  AND speed = 12 AND glide = 5 AND turn = -1 AND fade = 3
  AND disc_category = 'distance_driver'
  AND ENDS_WITH(flight_evidence, '/innova/destroyer')) = 1 FROM destroyer_target)
  AS 'Armory Destroyer identity/flight/evidence regression';
"""
    client.query(sql).result()
    print('Validated Armory Destroyer: Destroyer identity, 12/5/-1/3, Destroyer evidence.', flush=True)
