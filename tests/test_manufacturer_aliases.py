"""Run temporary-table BigQuery regressions with RUN_BIGQUERY_MODEL_TESTS=1."""

import json
import os
import unittest

from disc_golf_pipeline.services.manufacturer_aliases import (
    CURATED_VENDOR_ALIASES,
    build_curated_manufacturer_aliases_sql,
)
from disc_golf_pipeline.services.normalization import build_normalized_products_sql


def alias_fixture_sql():
    return """
CREATE TEMP TABLE DiscManufacturerAliases AS
SELECT REGEXP_REPLACE(LOWER(name), r'[^a-z0-9]+', '') AS alias_match_key,
  name AS alias_display, name AS canonical_manufacturer, 'fixture' AS alias_source,
  TRUE AS is_active, CURRENT_TIMESTAMP() AS created_at, CURRENT_TIMESTAMP() AS updated_at
FROM UNNEST(@manufacturers) name;
"""


def seed_sql():
    return build_curated_manufacturer_aliases_sql('fixture', 'aliases').replace(
        '`fixture.aliases.DiscManufacturerAliases`', 'DiscManufacturerAliases')


@unittest.skipUnless(os.getenv('RUN_BIGQUERY_MODEL_TESTS') == '1', 'Opt-in BigQuery fixtures')
class ManufacturerAliasBigQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from google.cloud import bigquery
        from disc_golf_pipeline.common.runtime import load_env_file

        load_env_file()
        cls.bigquery = bigquery
        cls.client = bigquery.Client(project=os.getenv('GCP_PROJECT_ID', 'disc-golf-price-compare'))

    def run_sql(self, sql, cases=None):
        config = self.bigquery.QueryJobConfig(query_parameters=[
            self.bigquery.ArrayQueryParameter('manufacturers', 'STRING',
                                             sorted(set(CURATED_VENDOR_ALIASES.values()))),
            self.bigquery.ScalarQueryParameter('cases', 'STRING', json.dumps(cases or [])),
        ])
        return list(self.client.query(sql, job_config=config).result(timeout=120))

    def test_exact_vendor_aliases_normalize_without_guessing_retailer_brands(self):
        cases = [{'vendor': vendor, 'expected': canonical, 'store': 'fixture'}
                 for vendor, canonical in CURATED_VENDOR_ALIASES.items()]
        cases.extend({'vendor': vendor, 'expected': None, 'store': 'fixture'} for vendor in (
            'Russell Disc Golf', 'Ledgestone', 'Dynamic Distribution', 'Trilogy',
            'MVP Disc Sports,LLC', 'Discraft Disc Golf Outlet',
        ))
        cases.append({'vendor': 'Discraft Disc Golf', 'expected': None, 'store': 'retailer'})
        sql = alias_fixture_sql() + seed_sql() + """
CREATE TEMP TABLE ProductInfo AS
SELECT position AS MainProductId, JSON_VALUE(row, '$.store') AS Store,
  JSON_VALUE(row, '$.vendor') AS Vendor, 'Discraft ESP Buzzz' AS Title,
  'Discs' AS ProductType, '' AS Tags, '' AS BodyHtml
FROM UNNEST(JSON_QUERY_ARRAY(@cases)) row WITH OFFSET position;
CREATE TEMP TABLE StorefrontNormalizationRules AS
SELECT store, store AS retailer, IF(store = 'retailer', 'RETAILER', 'MIXED') AS vendor_mode,
  ARRAY<STRING>[] AS retailer_vendor_values, ARRAY<STRING>[] AS trusted_vendor_values,
  ['discs'] AS disc_product_types, 'fixture' AS rules_version, TRUE AS is_active
FROM UNNEST(['fixture', 'retailer']) store;
CREATE TEMP TABLE InfiniteDiscs AS
SELECT '' AS ManufacturerName, '' AS PlasticName, '' AS ModelName, '' AS ModelLink,
  '' AS ModelDescription, CURRENT_TIMESTAMP() AS record_timestamp, Id
FROM UNNEST([1]) Id WHERE FALSE;
"""
        normalization = build_normalized_products_sql('fixture', 'aliases', 'fixture')
        for table in ('ProductInfo', 'InfiniteDiscs', 'StorefrontNormalizationRules',
                      'DiscManufacturerAliases', 'NormalizedProducts', 'ProductNormalizationAudit'):
            normalization = normalization.replace(f'`fixture.aliases.{table}`', table)
        sql += normalization.replace('CREATE TABLE IF NOT EXISTS', 'CREATE TEMP TABLE')
        sql += 'SELECT source_product_id, normalized_manufacturer, normalized_model FROM NormalizedProducts;'
        rows = self.run_sql(sql, cases)
        self.assertEqual(len(cases), len(rows))
        for row in rows:
            case = cases[int(row['source_product_id'])]
            with self.subTest(case=case):
                self.assertEqual(case['expected'], row['normalized_manufacturer'])
                self.assertIsNone(row['normalized_model'])

    def test_seed_is_idempotent_and_preserves_disabled_aliases(self):
        sql = alias_fixture_sql() + seed_sql() + """
UPDATE DiscManufacturerAliases SET is_active = FALSE WHERE alias_match_key = 'discraftdiscgolf';
CREATE TEMP TABLE before_seed AS SELECT * FROM DiscManufacturerAliases;
DROP TABLE CuratedVendorAliases;
""" + seed_sql() + """
ASSERT (SELECT COUNT(*) FROM DiscManufacturerAliases) = (SELECT COUNT(*) FROM before_seed)
  AS 'Alias seeding inserted duplicates';
ASSERT NOT EXISTS (SELECT * FROM DiscManufacturerAliases EXCEPT DISTINCT SELECT * FROM before_seed)
  AS 'Alias seeding changed existing records';
SELECT TRUE AS passed;
"""
        self.assertTrue(self.run_sql(sql)[0]['passed'])

    def test_conflicting_alias_fails_before_inserting(self):
        from google.api_core.exceptions import BadRequest

        sql = alias_fixture_sql() + """
INSERT INTO DiscManufacturerAliases VALUES
('discraftdiscgolf', 'Discraft Disc Golf', 'Wrong brand', 'curated', TRUE,
 CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
""" + seed_sql()
        with self.assertRaisesRegex(BadRequest, 'conflicts with an existing manufacturer'):
            self.run_sql(sql)


if __name__ == '__main__':
    unittest.main()
