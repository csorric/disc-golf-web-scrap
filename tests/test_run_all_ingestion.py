import os
import unittest
from unittest.mock import patch

from disc_golf_pipeline.cli.main import run_all_ingestion


class RunAllIngestionTests(unittest.TestCase):
    def test_full_pipeline_reviews_promotes_then_publishes(self):
        observed_modes = []

        def record_normalize(*args, **kwargs):
            observed_modes.append(("normalize", os.environ.get("LLM_RESOLUTION_MODE")))

        def record_process(*args, **kwargs):
            observed_modes.append(("process", os.environ.get("LLM_RESOLUTION_MODE")))

        llm_summary = {
            "eligible": 2,
            "cached_before": 8,
            "attempted": 2,
            "accepted": 1,
            "none": 1,
            "invalid": 0,
            "failed": 0,
        }
        promotion_summary = {
            "promoted": 9,
            "product": 8,
            "variant": 1,
            "unapplied": 0,
        }
        release_summary = {
            "collection": "discs_release",
            "deployment_id": "deployment-1",
            "alias": "discs_prod",
        }

        with patch.dict(os.environ, {}, clear=True), patch(
            "disc_golf_pipeline.cli.main.scrape_all", return_value=["shopify"]
        ), patch(
            "disc_golf_pipeline.cli.main.scrape_infinite_discs",
            return_value=["infinite"],
        ), patch(
            "disc_golf_pipeline.cli.main.parse_all", return_value=["shopify"]
        ), patch(
            "disc_golf_pipeline.cli.main.parse_infinite_discs",
            return_value=["infinite"],
        ), patch(
            "disc_golf_pipeline.cli.main.load_all", return_value=["shopify"]
        ), patch(
            "disc_golf_pipeline.cli.main.load_infinite_discs",
            return_value=["infinite"],
        ), patch(
            "disc_golf_pipeline.cli.main.get_gcp_project_id", return_value="project"
        ), patch(
            "disc_golf_pipeline.cli.main.get_bigquery_dataset", return_value="dataset"
        ), patch(
            "disc_golf_pipeline.cli.main.run_normalize_data",
            side_effect=record_normalize,
        ), patch(
            "disc_golf_pipeline.cli.main.bigquery.Client", return_value=object()
        ), patch(
            "disc_golf_pipeline.cli.main.run_full_ingestion_llm_stage",
            return_value=llm_summary,
        ), patch(
            "disc_golf_pipeline.cli.main.run_process_data",
            side_effect=record_process,
        ) as process_data, patch(
            "disc_golf_pipeline.cli.main.get_llm_promotion_summary",
            return_value=promotion_summary,
        ), patch(
            "disc_golf_pipeline.cli.main.publish_typesense_release",
            return_value=release_summary,
        ) as publish:
            summary = run_all_ingestion()

        self.assertEqual([("normalize", "off"), ("process", "promote")], observed_modes)
        process_data.assert_called_once_with(
            project_id="project",
            dataset="dataset",
            include_normalization=True,
        )
        publish.assert_called_once_with()
        self.assertEqual(2, summary["llm_attempted"])
        self.assertEqual(9, summary["llm_promoted"])
        self.assertEqual("discs_release", summary["release_collection"])
        self.assertIsNone(os.environ.get("LLM_RESOLUTION_MODE"))

    def test_llm_failure_prevents_typesense_publication(self):
        with patch.dict(os.environ, {}, clear=True), patch(
            "disc_golf_pipeline.cli.main.scrape_all", return_value=[]
        ), patch(
            "disc_golf_pipeline.cli.main.scrape_infinite_discs", return_value=[]
        ), patch(
            "disc_golf_pipeline.cli.main.parse_all", return_value=[]
        ), patch(
            "disc_golf_pipeline.cli.main.parse_infinite_discs", return_value=[]
        ), patch(
            "disc_golf_pipeline.cli.main.load_all", return_value=[]
        ), patch(
            "disc_golf_pipeline.cli.main.load_infinite_discs", return_value=[]
        ), patch(
            "disc_golf_pipeline.cli.main.get_gcp_project_id", return_value="project"
        ), patch(
            "disc_golf_pipeline.cli.main.get_bigquery_dataset", return_value="dataset"
        ), patch(
            "disc_golf_pipeline.cli.main.run_normalize_data"
        ), patch(
            "disc_golf_pipeline.cli.main.bigquery.Client", return_value=object()
        ), patch(
            "disc_golf_pipeline.cli.main.run_full_ingestion_llm_stage",
            side_effect=RuntimeError("LLM failed"),
        ), patch(
            "disc_golf_pipeline.cli.main.publish_typesense_release"
        ) as publish:
            with self.assertRaisesRegex(RuntimeError, "LLM failed"):
                run_all_ingestion()

        publish.assert_not_called()
        self.assertIsNone(os.environ.get("LLM_RESOLUTION_MODE"))


if __name__ == "__main__":
    unittest.main()
