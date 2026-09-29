import math
import unittest
from unittest.mock import MagicMock

from disc_golf_pipeline.services.disc_classification import CLASSIFICATION_FIELDS
from disc_golf_pipeline.services.indexer import (
    NORMALIZED_COLLECTION_FIELDS, assert_disc_fields_match, build_document,
    iterate_variant_state, iterate_changes_for_batch,
)
from disc_golf_pipeline.services.process_data import (
    build_variant_state_sql, ensure_variant_table_schemas,
)
from tests.disc_classification_cases import EXAMPLES, reference


class ClassificationContractTests(unittest.TestCase):
    def test_reference_agrees_with_pdf_examples(self):
        for name, speed, glide, turn, fade, weight, score, role in EXAMPLES:
            with self.subTest(name=name):
                result = reference(dict(speed=speed, glide=glide, turn=turn, fade=fade,
                                        normalized_weight_g=weight))
                self.assertAlmostEqual(score, result["access_variant"])
                self.assertEqual(role, result["beginner_role"])

    def test_missing_and_invalid_weights_and_scores_never_become_zero(self):
        for value in (None, "", "false", False, True, math.nan, math.inf, -math.inf):
            with self.subTest(value=value):
                doc = build_document({"id": "disc", "weight_g": value,
                                      "access_variant": value, "speed": value})
                self.assertNotIn("weight_g", doc)
                self.assertNotIn("access_variant", doc)
                self.assertNotIn("speed", doc)
        doc = build_document({"id": "disc", "turn": 0, "access_model": 0,
                              "has_exact_weight": "false", "in_stock": "false"})
        self.assertEqual(0, doc["turn"])
        self.assertEqual(0, doc["access_model"])
        self.assertFalse(doc["has_exact_weight"])
        self.assertFalse(doc["in_stock"])

    def test_all_classification_fields_reach_document_and_queries(self):
        row = {"id": "9007199254740993", "source_variant_key": "shopify:example:9007199254740993"}
        values = {"STRING": "fixture", "FLOAT64": 1.5, "BOOL": False,
                  "ARRAY<STRING>": ["high_turn", "weight_not_exact"]}
        for name, kind in CLASSIFICATION_FIELDS:
            row[name] = values[kind]
        doc = build_document(row, use_source_variant_key=True)
        self.assertEqual(row["id"], doc["legacy_id"])
        for name, _ in CLASSIFICATION_FIELDS:
            self.assertEqual(row[name], doc[name])
        client = MagicMock()
        iterate_variant_state(client, "project.dataset.VariantState")
        iterate_changes_for_batch(client, "project.dataset.VariantChanges", "batch")
        for call in client.query.call_args_list:
            for name, _ in CLASSIFICATION_FIELDS:
                self.assertIn(name, call.args[0])

    def test_schema_and_migration_preserve_optional_values_and_reason_arrays(self):
        fields = {field["name"]: field for field in NORMALIZED_COLLECTION_FIELDS}
        self.assertEqual(len(fields), len(NORMALIZED_COLLECTION_FIELDS))
        self.assertTrue(fields["weight_g"]["optional"])
        for name, _ in CLASSIFICATION_FIELDS:
            self.assertTrue(fields[name]["optional"])
            if name.startswith("access_"):
                self.assertTrue(fields[name]["sort"])
        client = MagicMock()
        client.get_table.side_effect = [MagicMock(schema=[]), MagicMock(schema=[])]
        ensure_variant_table_schemas(client, "project", "dataset")
        for call in client.update_table.call_args_list:
            reasons = next(f for f in call.args[0].schema if f.name == "reason_codes")
            self.assertEqual("REPEATED", reasons.mode)

    def test_field_validation_catches_dropped_and_stale_data(self):
        row = {"id": "disc", "access_model": 39.833333333, "weight_g": None,
               "reason_codes": ["higher_speed"], "has_exact_weight": False}
        doc = build_document(row)
        assert_disc_fields_match(row, doc)
        del doc["access_model"]
        with self.assertRaisesRegex(RuntimeError, "access_model"):
            assert_disc_fields_match(row, doc)
        doc = build_document(row)
        doc["weight_g"] = 0
        with self.assertRaisesRegex(RuntimeError, "weight_g"):
            assert_disc_fields_match(row, doc)

    def test_state_hash_and_merge_include_classification_updates(self):
        sql = build_variant_state_sql("project", "dataset")
        hash_sql = sql.split("TO_JSON_STRING(STRUCT", 1)[1].split("AS row_hash", 1)[0]
        for name, _ in CLASSIFICATION_FIELDS:
            self.assertIn(f"v.{name}", hash_sql)
            self.assertIn(f"T.{name} = S.{name}", sql)
            self.assertIn(f"ns.{name}", sql)

    def test_legacy_invalid_weights_are_omitted_and_validate(self):
        for weight in (0, -1, 175.5, False, "NaN"):
            with self.subTest(weight=weight):
                row = {"id": "legacy", "weight_g": weight}
                doc = build_document(row)
                self.assertNotIn("weight_g", doc)
                assert_disc_fields_match(row, doc)


if __name__ == "__main__":
    unittest.main()
