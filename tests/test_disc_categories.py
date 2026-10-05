import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from disc_golf_pipeline.cli.main import build_parser, main, refresh_typesense_from_cache
from disc_golf_pipeline.services.disc_categories import LEGACY_CATEGORY_FLAGS, extract_category
from disc_golf_pipeline.services.disc_category_checks import category_regression_cases, review_disc_categories
from disc_golf_pipeline.services.indexer import assert_disc_fields_match, build_document
from disc_golf_pipeline.services.normalization_job import SUPPORTED_PIPELINE_COMMANDS


class CategoryRegressionTests(unittest.TestCase):
    def test_catalog_review_reports_listing_and_variant_counts(self):
        client = MagicMock()
        client.query.return_value.result.return_value = [
            {"title": "Example", "variants": 9,
             "review_reasons": ["putter_above_speed_4", "conflicting_mold_categories"]},
            {"title": "Other", "variants": 3, "review_reasons": ["putter_above_speed_4"]},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "review.json"
            summary = review_disc_categories(client, "project", "dataset", path)
            report = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(2, summary["listing_counts_by_reason"]["putter_above_speed_4"])
        self.assertEqual(12, summary["variant_counts_by_reason"]["putter_above_speed_4"])
        self.assertEqual(9, summary["variant_counts_by_reason"]["conflicting_mold_categories"])
        self.assertEqual(2, len(report["listings"]))
        self.assertEqual("project.dataset.VariantState", report["source"])

    def test_retailer_evidence_regressions(self):
        for row, category, source in category_regression_cases():
            if source in ("flight_speed", "try_discs_category", "retailer_model_consensus"):
                continue
            with self.subTest(name=row["id"]):
                result = extract_category(row["product_type"], row["tags"], row["title"], row["BodyHtml"])
                self.assertEqual((category, source), result[:2])

    def test_exported_flags_agree_with_category_including_unknown(self):
        for category in (None, *LEGACY_CATEGORY_FLAGS.values()):
            row = {"id": "fixture", "disc_category": category}
            row.update({flag: category == value for flag, value in LEGACY_CATEGORY_FLAGS.items()})
            doc = build_document(row)
            assert_disc_fields_match(row, doc)
            for flag in LEGACY_CATEGORY_FLAGS:
                with self.subTest(category=category, flag=flag):
                    corrupt = dict(doc, **{flag: not doc[flag]})
                    with self.assertRaisesRegex(RuntimeError, flag):
                        assert_disc_fields_match(row, corrupt)
                    stale_row = dict(row, **{flag: not row[flag]})
                    with self.assertRaisesRegex(RuntimeError, flag):
                        assert_disc_fields_match(stale_row, build_document(stale_row))


class CategoryRebuildTests(unittest.TestCase):
    def setUp(self):
        self.prefix = "disc_golf_pipeline.cli.main."
        self.observed = []
        self.mocks = {}
        patcher = patch(self.prefix + 'validate_production_model_identity')
        self.identity_validation = patcher.start()
        self.addCleanup(patcher.stop)
        settings = {
            "get_gcp_project_id": "project", "get_bigquery_dataset": "dataset",
            "get_release_runtime": {"state_table": "project.dataset.VariantState",
                                    "changes_table": "project.dataset.VariantChanges"},
        }
        for name, value in settings.items():
            patcher = patch(self.prefix + name, return_value=value)
            self.mocks[name] = patcher.start()
            self.addCleanup(patcher.stop)
        for name in ("bigquery.Client", "validate_category_regressions", "validate_category_source_joins",
                     "prepare_source_views", "run_process_data", "validate_production_categories",
                     "review_disc_categories", "publish_typesense_release"):
            patcher = patch(self.prefix + name)
            mock = patcher.start()
            self.addCleanup(patcher.stop)
            if name != "bigquery.Client":
                mock.side_effect = lambda *a, _name=name, **kw: self.observed.append(_name)
            self.mocks[name] = mock

    def test_rebuild_checks_before_mutating_and_before_publishing(self):
        refresh_typesense_from_cache(rebuild_categories=True)
        self.assertEqual([
            "validate_category_regressions", "validate_category_source_joins", "review_disc_categories", "prepare_source_views",
            "run_process_data", "validate_production_categories", "review_disc_categories", "publish_typesense_release",
        ], self.observed)
        self.mocks["run_process_data"].assert_called_once_with(
            project_id="project", dataset="dataset", include_normalization=False)

    def test_any_failed_stage_prevents_publication(self):
        for stage in ("validate_category_regressions", "validate_category_source_joins",
                      "prepare_source_views", "run_process_data", "validate_production_categories", "review_disc_categories"):
            with self.subTest(stage=stage):
                saved = self.mocks[stage].side_effect
                self.mocks[stage].side_effect = RuntimeError(stage)
                with self.assertRaisesRegex(RuntimeError, stage):
                    refresh_typesense_from_cache(rebuild_categories=True)
                self.mocks["publish_typesense_release"].assert_not_called()
                self.mocks[stage].side_effect = saved

    def test_identity_failure_prevents_publication(self):
        self.identity_validation.side_effect = RuntimeError('wrong model')
        with self.assertRaisesRegex(RuntimeError, 'wrong model'):
            refresh_typesense_from_cache(rebuild_categories=True)
        self.mocks['publish_typesense_release'].assert_not_called()

    def test_dataset_mismatch_prevents_all_mutation(self):
        self.mocks["get_release_runtime"].return_value["state_table"] = "other.dataset.VariantState"
        with self.assertRaisesRegex(ValueError, "processing dataset"):
            refresh_typesense_from_cache(rebuild_categories=True)
        self.assertEqual([], self.observed)

    def test_commands_and_worker_support(self):
        parser = build_parser()
        for command in ("rebuild-disc-categories", "start-disc-category-rebuild-job", "validate-disc-categories",
                        "review-disc-categories", "start-disc-category-review-job"):
            self.assertEqual(command, parser.parse_args([command]).command)
        self.assertIn("rebuild-disc-categories", SUPPORTED_PIPELINE_COMMANDS)

    def test_cli_dispatches_rebuild(self):
        with patch("sys.argv", ["main.py", "rebuild-disc-categories"]):
            main()
        self.mocks["publish_typesense_release"].assert_called_once()

    def test_cli_dispatches_detached_worker(self):
        with patch("sys.argv", ["main.py", "start-disc-category-rebuild-job"]), patch(
            self.prefix + "start_pipeline_job", return_value={"job_id": "fixture"}
        ) as start:
            main()
        start.assert_called_once_with("rebuild-disc-categories")
        self.mocks["run_process_data"].assert_not_called()


if __name__ == "__main__":
    unittest.main()
