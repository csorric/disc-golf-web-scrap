import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from disc_golf_pipeline.cli.main import build_parser
from disc_golf_pipeline.services.disc_classification_audit import (
    INPUT_TABLES, _score, build_audit_rows_sql, build_samples_sql, build_summary_sql,
    generate_classification_report, render_classification_audit, run_classification_review,
)
from disc_golf_pipeline.services.process_data import run_process_data


SAMPLE = {
    "sample_group": "Independent mold review", "title": "Example disc", "id": "9007199254740993",
    "product_link": "https://example.com/disc", "variant_title": "170-175g / Blue",
    "normalized_manufacturer": "Example", "normalized_model": "Model", "weight_status": "range",
    "weight_min_g": 170, "weight_max_g": 175, "raw_weight_g": 227,
    "speed": 7, "glide": 5, "turn": 0, "fade": 1, "data_status": "complete",
    "access_model": 65, "access_variant": None, "access_low": 62.5, "access_high": 65,
    "beginner_role": "conditional_candidate", "reason_codes": ["weight_not_exact"],
}
SUMMARY = [{"dimension": "total", "value": "All disc variants", "variants": 1, "identified_molds": 1}]


class ClassificationAuditTests(unittest.TestCase):
    def test_render_preserves_ranges_nulls_and_escapes_evidence_and_links(self):
        row = {**SAMPLE, "title": '<script>alert("title")</script>',
               "flight_evidence": "</dd><script>alert('evidence')</script>",
               "product_link": "javascript:alert('link')"}
        rendered = render_classification_audit(SUMMARY, [row], {})
        self.assertNotIn("<script>alert", rendered)
        self.assertNotIn('href="javascript:', rendered)
        self.assertIn("&lt;script&gt;", rendered)
        self.assertIn("170–175 g", rendered)
        self.assertIn("Source field: 227", rendered)
        self.assertIn("7 / 5 / 0 / 1", rendered)
        self.assertIn("Variant —", rendered)
        self.assertIn("weight_not_exact", rendered)
        self.assertIn("disc data by try discs", rendered.lower())

    def test_display_uses_half_up_and_preserves_real_zero(self):
        self.assertEqual("1.01", _score(1.005))
        self.assertEqual("0.00", _score(0))
        self.assertEqual("—", _score(None))
        self.assertEqual("—", _score(float("nan")))

    def test_sampling_is_per_mold_and_counts_use_full_catalog(self):
        samples = build_samples_sql("project", "dataset")
        self.assertIn("PARTITION BY sample_group, COALESCE(mold_key", samples)
        self.assertIn("'Independent mold review', 50, 100", samples)
        self.assertNotIn("LIMIT", build_summary_sql("project", "dataset"))
        self.assertIn("COUNT(DISTINCT mold_key)", build_summary_sql("project", "dataset"))
        self.assertIn("ABS(c.access_low - 70) <= 2", build_audit_rows_sql("project", "dataset"))
        with self.assertRaises(ValueError):
            build_samples_sql("project", "dataset", 0)
        with self.assertRaises(ValueError):
            build_audit_rows_sql("project", "dataset`; DELETE")

    def test_report_writes_complete_source_query_and_metadata(self):
        client = MagicMock()
        jobs = [MagicMock(), MagicMock(), MagicMock()]
        jobs[1].result.return_value = SUMMARY
        jobs[2].result.return_value = [SAMPLE]
        client.query.side_effect = jobs
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.html"
            result = generate_classification_report(client, "project", "dataset", output)
            self.assertEqual(1, result["sample_count"])
            sql = output.with_suffix(".source.sql").read_text(encoding="utf-8")
            self.assertIn("classification.access_variant AS access_variant", sql)
            self.assertIn("`project.dataset.NormalizedDiscClassifications`", sql)
            self.assertNotIn("CREATE OR REPLACE", sql)
            metadata = json.loads(output.with_suffix(".summary.json").read_text())
            self.assertEqual(1, metadata["coverage"][0]["variants"])
            self.assertTrue(output.exists())

    def test_review_only_copies_and_mutates_new_dataset(self):
        client = MagicMock()
        client.get_dataset.return_value.location = "US"
        with tempfile.TemporaryDirectory() as directory, patch(
            "disc_golf_pipeline.services.disc_classification_audit.build_job_id", return_value="test-123"
        ), patch(
            "disc_golf_pipeline.services.disc_classification_audit.run_disc_classification"
        ) as classify, patch(
            "disc_golf_pipeline.services.disc_classification_audit.generate_classification_report", return_value={}
        ) as report:
            state = run_classification_review(client, "project", "production", directory)
            self.assertEqual("succeeded", state["state"])
            self.assertFalse(state["production_changed"])
            classify.assert_called_once_with(client, "project", "ClassificationReview_test_123")
            self.assertEqual("ClassificationReview_test_123", report.call_args.args[2])
            queries = [call.args[0] for call in client.query.call_args_list]
            for table, query in zip(INPUT_TABLES, queries):
                self.assertEqual(f"CREATE TABLE `project.ClassificationReview_test_123.{table}` AS "
                                 f"SELECT * FROM `project.production.{table}`", query)
            self.assertTrue(queries[-1].startswith("CREATE OR REPLACE VIEW `project.ClassificationReview_"))
            dataset = client.create_dataset.call_args.args[0]
            self.assertEqual(7 * 86_400_000, dataset.default_table_expiration_ms)

    def test_startup_and_classification_failures_are_recorded_and_stop_reporting(self):
        for stage in ("startup", "classify"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory, patch(
                "disc_golf_pipeline.services.disc_classification_audit.run_disc_classification"
            ) as classify, patch(
                "disc_golf_pipeline.services.disc_classification_audit.generate_classification_report"
            ) as report:
                client = MagicMock()
                client.get_dataset.return_value.location = "US"
                target = client.get_dataset if stage == "startup" else classify
                target.side_effect = RuntimeError("test failure")
                with self.assertRaisesRegex(RuntimeError, "test failure"):
                    run_classification_review(client, "project", "production", directory)
                state = json.loads((Path(directory) / "review-status.json").read_text())
                self.assertEqual("failed", state["state"])
                self.assertIn("test failure", state["error"])
                report.assert_not_called()

    def test_new_cli_commands_parse(self):
        parser = build_parser()
        for command in ("classify-discs", "start-classify-discs-job", "review-disc-classifications",
                        "start-disc-classification-review-job", "disc-classification-job-status",
                        "generate-disc-classification-report"):
            self.assertEqual(command, parser.parse_args([command]).command)
        args = parser.parse_args(["generate-disc-classification-report", "--dataset", "review"])
        self.assertEqual("review", args.dataset)


class ClassificationPipelineOrderingTests(unittest.TestCase):
    def test_final_normalization_then_classification_then_state(self):
        observed = []
        client = MagicMock()
        client.query.side_effect = lambda sql: observed.append(sql) or MagicMock()
        prefix = "disc_golf_pipeline.services.process_data."
        with patch(prefix + "bigquery.Client", return_value=client), patch(
            prefix + "prepare_source_views"
        ), patch(prefix + "run_normalization", side_effect=lambda *a: observed.append("normalize")), patch(
            prefix + "run_disc_classification", side_effect=lambda *a: observed.append("classify")
        ), patch(prefix + "ensure_variant_table_schemas"):
            run_process_data("project", "dataset")
        self.assertEqual(["normalize", "classify"], observed[:2])
        self.assertIn("v_VariantSnapshot", observed[2])
        self.assertIn("MERGE `project.dataset.VariantState`", observed[3])

    def test_classification_failure_prevents_downstream_state_changes(self):
        client = MagicMock()
        prefix = "disc_golf_pipeline.services.process_data."
        with patch(prefix + "bigquery.Client", return_value=client), patch(
            prefix + "run_disc_classification", side_effect=RuntimeError("Duplicate classification IDs")
        ), patch(prefix + "ensure_variant_table_schemas") as migrate:
            with self.assertRaisesRegex(RuntimeError, "Duplicate classification"):
                run_process_data("project", "dataset", include_normalization=False)
        migrate.assert_not_called()
        client.query.assert_not_called()


if __name__ == "__main__":
    unittest.main()
