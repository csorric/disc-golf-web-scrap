import unittest
from types import SimpleNamespace

from google.cloud import bigquery

from disc_golf_pipeline.services.process_data import (
    build_variant_identity_map_sql,
    build_variant_snapshot_view_sql,
    build_variant_state_sql,
    ensure_variant_table_schemas,
)


NORMALIZATION_FIELDS = (
    "source_variant_key",
    "source",
    "retailer",
    "raw_vendor",
    "normalized_manufacturer",
    "normalized_model",
    "item_type",
    "is_disc",
    "item_type_confidence",
    "manufacturer_confidence",
    "model_confidence",
    "normalization_confidence",
    "normalization_source",
    "model_decision_level",
    "normalization_version",
    "model_rules_version",
)


class VariantPropagationSqlTests(unittest.TestCase):
    def test_variant_snapshot_reads_normalized_snapshot_and_preserves_ids(self):
        sql = build_variant_snapshot_view_sql("project", "dataset")

        self.assertIn("`project.dataset.NormalizedVariantSnapshot`", sql)
        self.assertNotIn("`project.dataset.v_ShopifyVariants`", sql)
        self.assertNotIn("`project.dataset.v_InfiniteVariants`", sql)
        self.assertIn("WHEN src.source = 'shopify'", sql)
        self.assertIn("'shopify:'", sql)
        self.assertIn("'infinite' AS source", sql)
        for field in NORMALIZATION_FIELDS:
            self.assertIn(f"src.{field}", sql)

    def test_state_and_changes_include_normalization_in_rows_and_hash(self):
        sql = build_variant_state_sql("project", "dataset")

        self.assertIn("`project.dataset.v_VariantSnapshot`", sql)
        self.assertNotIn("`project.dataset.v_ShopifyVariants`", sql)
        self.assertNotIn("`project.dataset.v_InfiniteVariants`", sql)
        self.assertNotIn("ALTER TABLE", sql)
        self.assertIn("TO_JSON_STRING(STRUCT", sql)
        for field in NORMALIZATION_FIELDS:
            self.assertIn(f"v.{field}", sql)
            self.assertIn(f"T.{field} = S.{field}", sql)

    def test_model_rules_version_is_part_of_row_hash(self):
        sql = build_variant_state_sql("project", "dataset")
        hash_start = sql.index("TO_JSON_STRING(STRUCT")
        hash_end = sql.index("))\n  )) AS row_hash", hash_start)
        hash_sql = sql[hash_start:hash_end]

        self.assertIn("v.normalization_version", hash_sql)
        self.assertIn("v.model_rules_version", hash_sql)
        self.assertIn("v.normalized_manufacturer", hash_sql)
        self.assertIn("v.normalized_model", hash_sql)

    def test_source_variant_key_is_part_of_row_hash(self):
        sql = build_variant_state_sql("project", "dataset")
        hash_start = sql.index("TO_JSON_STRING(STRUCT")
        hash_end = sql.index("))\n  )) AS row_hash", hash_start)

        self.assertIn("v.source_variant_key", sql[hash_start:hash_end])

    def test_identity_map_preserves_legacy_ids_and_checks_unique_stable_keys(self):
        sql = build_variant_identity_map_sql("project", "dataset")

        self.assertIn("`project.dataset.VariantIdentityMap`", sql)
        self.assertIn("`project.dataset.v_VariantSnapshot`", sql)
        self.assertIn("source_variant_key STRING NOT NULL", sql)
        self.assertIn("legacy_id STRING NOT NULL", sql)
        self.assertIn("COUNT(DISTINCT source_variant_key)", sql)
        self.assertIn("target.legacy_id = source.legacy_id", sql)
        self.assertIn("SET is_current = FALSE", sql)

    def test_schema_migration_updates_each_table_once_with_only_missing_fields(self):
        class FakeClient:
            def __init__(self):
                self.tables = {
                    "project.dataset.VariantChanges": SimpleNamespace(
                        schema=[
                            bigquery.SchemaField("change_ts", "TIMESTAMP"),
                            bigquery.SchemaField("source", "STRING"),
                        ]
                    ),
                    "project.dataset.VariantState": SimpleNamespace(
                        schema=[bigquery.SchemaField("source", "STRING")]
                    ),
                }
                self.updates = []

            def get_table(self, table_ref):
                return self.tables[table_ref]

            def update_table(self, table, fields):
                self.updates.append((table, fields))
                return table

        client = FakeClient()
        ensure_variant_table_schemas(client, "project", "dataset")

        self.assertEqual(2, len(client.updates))
        self.assertTrue(all(fields == ["schema"] for _, fields in client.updates))
        changes_names = [field.name for field in client.tables["project.dataset.VariantChanges"].schema]
        state_names = [field.name for field in client.tables["project.dataset.VariantState"].schema]
        self.assertEqual(1, changes_names.count("source"))
        self.assertEqual(1, changes_names.count("change_ts"))
        self.assertEqual(1, state_names.count("source"))
        for field in NORMALIZATION_FIELDS:
            self.assertIn(field, changes_names)
            self.assertIn(field, state_names)


if __name__ == "__main__":
    unittest.main()
