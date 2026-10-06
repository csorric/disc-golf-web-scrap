"""Opt-in BigQuery regressions using production SQL and temporary fixture tables.

Set RUN_BIGQUERY_MODEL_TESTS=1 to execute; no production tables are written.
"""

import json
import os
import unittest

from disc_golf_pipeline.services.model_normalization import (
    DEFAULT_MODEL_RULES_VERSION,
    build_product_model_candidates_sql,
    build_variant_model_decisions_sql,
)


CASES = [
    ('buzzz', 'Discraft ESP Buzzz', ['passion fruit/white 176.4 11'], 'Buzzz'),
    ('venom', 'Discraft ESP Venom', ['169.8 storm cloud/blue'], 'Venom'),
    ('passion', 'Discraft ESP Passion', ['passion fruit/white 170'], 'Passion'),
    ('storm', 'Discraft Storm', ['storm cloud/blue 170'], 'Storm'),
    ('mixed', 'Discraft Buzzz / Venom', ['Buzzz / passion fruit', 'Venom / storm cloud'], None),
    ('variant_models', 'Discraft assorted discs', ['Passion / blue', 'Storm / white'], None),
    ('color_only', 'Discraft assorted discs', ['passion fruit/white', 'storm cloud/blue'], None),
    ('real_conflict', 'Discraft ESP Buzzz', ['Passion / white'], None),
    ('repeat', 'Discraft assorted discs', ['Passion / passion fruit', 'Storm / storm cloud'], None),
    ('hyphen', 'Discraft ESP Venom', ['STORM-CLOUD/Blue 170'], 'Venom'),
]


def build_fixture_sql():
    products = []
    variants = []
    for key, title, titles, _ in CASES:
        products.append({'id': key, 'title': title})
        variants.extend({'id': f'{key}-{i}', 'product_id': key, 'title': value}
                        for i, value in enumerate(titles))
    sql = """
CREATE TEMP TABLE NormalizedProducts AS
SELECT CONCAT('shopify:fixture:', JSON_VALUE(row, '$.id')) AS product_key,
  JSON_VALUE(row, '$.id') AS product_id, JSON_VALUE(row, '$.title') AS title,
  'shopify' AS source, 'fixture' AS store, 'disc' AS item_type,
  'Discraft' AS raw_vendor, 'Discraft' AS normalized_manufacturer,
  CAST(NULL AS STRING) AS normalized_model
FROM UNNEST(JSON_QUERY_ARRAY(@products)) row;
CREATE TEMP TABLE v_ShopifyVariants AS
SELECT JSON_VALUE(row, '$.id') AS id, JSON_VALUE(row, '$.product_id') AS product_id,
  JSON_VALUE(row, '$.title') AS variant_title, 'fixture' AS store
FROM UNNEST(JSON_QUERY_ARRAY(@variants)) row;
CREATE TEMP TABLE DiscModelEntities AS
SELECT model AS disc_entity_id, model AS canonical_model,
  'Discraft' AS manufacturer, 'discraft' AS manufacturer_key
FROM UNNEST(['Buzzz', 'Venom', 'Passion', 'Storm']) model;
CREATE TEMP TABLE DiscModelAliases AS
SELECT *, canonical_model AS alias, LOWER(canonical_model) AS normalized_alias_text,
  'canonical' AS alias_type, 1 AS alias_token_count, 10 AS alias_priority,
  FALSE AS is_generic, FALSE AS is_context_only, FALSE AS requires_manufacturer,
  1 AS manufacturer_count, TRUE AS is_active
FROM DiscModelEntities;
"""
    for builder in (build_product_model_candidates_sql, build_variant_model_decisions_sql):
        built = builder('fixture', 'colors', DEFAULT_MODEL_RULES_VERSION)
        for table in ('NormalizedProducts', 'v_ShopifyVariants', 'DiscModelEntities',
                      'DiscModelAliases', 'ProductDiscModelCandidates', 'ProductDiscModelDecisions',
                      'VariantDiscModelCandidates', 'VariantDiscModelDecisions'):
            built = built.replace(f'`fixture.colors.{table}`', table)
        sql += built.replace('CREATE OR REPLACE TABLE', 'CREATE TEMP TABLE')
    sql += """
SELECT 'product' AS level, product_id AS id, normalized_model, decision_bucket
FROM ProductDiscModelDecisions
UNION ALL
SELECT 'variant', variant_id, normalized_model, decision_bucket
FROM VariantDiscModelDecisions;
"""
    return sql, products, variants


@unittest.skipUnless(os.getenv('RUN_BIGQUERY_MODEL_TESTS') == '1', 'Opt-in BigQuery fixtures')
class VariantColorBigQueryTests(unittest.TestCase):
    def test_colors_do_not_compete_with_titles_or_become_variant_models(self):
        from google.cloud import bigquery
        from disc_golf_pipeline.common.runtime import load_env_file

        load_env_file()
        client = bigquery.Client(project=os.getenv('GCP_PROJECT_ID', 'disc-golf-price-compare'))
        sql, products, variants = build_fixture_sql()
        config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter('products', 'STRING', json.dumps(products)),
            bigquery.ScalarQueryParameter('variants', 'STRING', json.dumps(variants)),
        ])
        rows = list(client.query(sql, job_config=config).result(timeout=120))
        product_rows = {row['id']: row for row in rows if row['level'] == 'product'}
        variant_rows = {row['id']: row for row in rows if row['level'] == 'variant'}
        for key, _, _, model in CASES:
            with self.subTest(product=key):
                if model:
                    self.assertEqual((model, 'ACCEPT'),
                                     (product_rows[key]['normalized_model'], product_rows[key]['decision_bucket']))
                    self.assertFalse(any(id.startswith(key + '-') for id in variant_rows))
                elif key == 'color_only':
                    self.assertNotIn(key, product_rows)
                    self.assertFalse(any(id.startswith(key + '-') for id in variant_rows))
                else:
                    self.assertNotEqual('ACCEPT', product_rows[key]['decision_bucket'])
        for key, models in [('mixed', ['Buzzz', 'Venom']),
                            ('variant_models', ['Passion', 'Storm']),
                            ('repeat', ['Passion', 'Storm'])]:
            for i, model in enumerate(models):
                row = variant_rows[f'{key}-{i}']
                self.assertEqual((model, 'ACCEPT'), (row['normalized_model'], row['decision_bucket']))


if __name__ == '__main__':
    unittest.main()
