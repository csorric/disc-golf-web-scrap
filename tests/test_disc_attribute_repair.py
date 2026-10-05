import re
import unittest
from unittest.mock import MagicMock, patch

from disc_golf_pipeline.common.model_keys import model_match_key
from disc_golf_pipeline.services.disc_attributes import TITLE_WEIGHT_RANGE_PATTERN, LEADING_TITLE_WEIGHT_RANGE_PATTERN
from disc_golf_pipeline.cli.main import build_parser, repair_disc_attributes_from_cache
from disc_golf_pipeline.services.disc_attribute_repair import build_missing_model_repair_sql, _run_fixture_query


class AttributeFormatTests(unittest.TestCase):
    def test_stalled_fixture_is_canceled_and_stops_repair(self):
        client = MagicMock()
        job = client.query.return_value
        job.result.side_effect = TimeoutError("stalled")
        with self.assertRaisesRegex(RuntimeError, "Validation timed out"):
            _run_fixture_query(client, "SELECT 1", "synthetic fixture")
        job.cancel.assert_called_once_with()
        self.assertEqual(300_000, int(client.query.call_args.kwargs["job_config"].job_timeout_ms))

    def test_fixture_sql_failure_is_preserved(self):
        client = MagicMock()
        client.query.return_value.result.side_effect = ValueError("invalid SQL")
        with self.assertRaisesRegex(ValueError, "invalid SQL"):
            _run_fixture_query(client, "invalid SQL", "synthetic fixture")

    def test_repair_changes_only_missing_models_with_accepted_evidence(self):
        sql = build_missing_model_repair_sql("project", "dataset", "fixture", ["product_key", "normalized_model"])
        self.assertIn("WHERE normalized_model IS NULL AND normalized_manufacturer IS NOT NULL", sql)
        self.assertIn("AND product.normalized_model IS NULL AND decision.decision_bucket = 'ACCEPT'", sql)
        self.assertIn("BEGIN TRANSACTION;", sql)
        self.assertIn("COMMIT TRANSACTION;", sql)
        self.assertIn("CREATE TEMP TABLE ProductDiscModelDecisionsRepair", sql)
        self.assertNotIn("CREATE OR REPLACE TABLE `project.dataset.ProductDiscModelDecisions`", sql)

    def test_model_equivalence_is_limited_to_numeric_boundaries(self):
        for left, right in (("TeeBird3", "Teebird 3"), ("M4", "M 4"), ("F 3", "F3"), ("PA 3", "PA3")):
            self.assertEqual(model_match_key(left), model_match_key(right))
        for left, right in (("TeeBird", "TeeBird3"), ("TeeBird3", "TeeBird L"),
                            ("Big Z", "BigZ"), ("M4", "M40")):
            self.assertNotEqual(model_match_key(left), model_match_key(right))

    def test_product_version_is_written_inside_evidence(self):
        sql = build_missing_model_repair_sql("project", "dataset", "fixture", ["product_key", "normalized_model"])
        update = sql.split("UPDATE `project.dataset.NormalizedProducts` AS product", 1)[1].split(";", 1)[0]
        self.assertNotIn("model_rules_version =", update)
        self.assertIn("normalization_evidence = desired.normalization_evidence", update)
        self.assertIn("'fixture' AS model_rules_version", sql)
        self.assertIn("'$.model_decision'", sql)

    def test_abbreviated_weight_ranges_retain_full_evidence(self):
        for text, expected in (("Blue / 173-5 grams", "173-5 grams"), ("Red 165-70g", "165-70g"),
                               ("Blue 173-175g", "173-175g"), ("Blue 173–5g", "173–5g")):
            self.assertEqual(expected, re.search(TITLE_WEIGHT_RANGE_PATTERN, text).group(1))
        self.assertEqual("173-5", re.search(LEADING_TITLE_WEIGHT_RANGE_PATTERN, "173-5 Blue").group(1))
        self.assertIsNone(re.search(TITLE_WEIGHT_RANGE_PATTERN, "Blue 175+g"))
        self.assertIsNone(re.search(TITLE_WEIGHT_RANGE_PATTERN, "Edition 24 / 5 grams"))


class AttributeRepairOrchestrationTests(unittest.TestCase):
    def setUp(self):
        prefix = "disc_golf_pipeline.cli.main."
        self.mocks = {}
        self.events = []
        values = {"get_gcp_project_id": "project", "get_bigquery_dataset": "dataset",
                  "get_release_runtime": {"state_table": "project.dataset.VariantState",
                                          "changes_table": "project.dataset.VariantChanges"},
                  "bigquery.Client": MagicMock()}
        for name, value in values.items():
            patcher = patch(prefix + name, return_value=value)
            self.mocks[name] = patcher.start()
            self.addCleanup(patcher.stop)
        for name in ("validate_attribute_repair", "repair_missing_models", "ensure_match_schema",
                     "refresh_model_quality_views", "run_process_data", "validate_repaired_attributes",
                     "validate_production_categories", "summarize_attribute_coverage", "publish_typesense_release"):
            patcher = patch(prefix + name)
            self.mocks[name] = patcher.start()
            self.addCleanup(patcher.stop)
            self.mocks[name].side_effect = lambda *a, _name=name, **kw: self.events.append(_name)
        self.coverage = {"disc_variants": 100, "missing_all_flights": 10, "complete_flight_variants": 90}
        self.mocks["summarize_attribute_coverage"].side_effect = [self.coverage, self.coverage]

    def test_validated_cached_repair_publishes(self):
        repair_disc_attributes_from_cache()
        self.assertEqual("validate_attribute_repair", self.events[0])
        self.assertEqual("publish_typesense_release", self.events[-1])
        self.mocks["run_process_data"].assert_called_once_with(
            project_id="project", dataset="dataset", include_normalization=False)

    def test_regression_stops_publication(self):
        self.mocks["summarize_attribute_coverage"].side_effect = [
            self.coverage, {**self.coverage, "missing_all_flights": 11}]
        with self.assertRaisesRegex(RuntimeError, "coverage"):
            repair_disc_attributes_from_cache()
        self.mocks["publish_typesense_release"].assert_not_called()

    def test_fixture_failure_stops_before_model_changes(self):
        self.mocks["validate_attribute_repair"].side_effect = RuntimeError("fixture failed")
        with self.assertRaisesRegex(RuntimeError, "fixture failed"):
            repair_disc_attributes_from_cache()
        self.mocks["repair_missing_models"].assert_not_called()
        self.mocks["publish_typesense_release"].assert_not_called()

    def test_wrong_dataset_stops_before_changes(self):
        self.mocks["get_release_runtime"].return_value["state_table"] = "different.dataset.VariantState"
        with self.assertRaisesRegex(ValueError, "processing dataset"):
            repair_disc_attributes_from_cache()
        self.mocks["repair_missing_models"].assert_not_called()

    def test_commands_are_available(self):
        parser = build_parser()
        for command in ("validate-disc-attribute-repair", "repair-disc-attributes-from-cache", "start-disc-attribute-repair-job"):
            self.assertEqual(command, parser.parse_args([command]).command)
