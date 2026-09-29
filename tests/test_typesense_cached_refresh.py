import unittest
from unittest.mock import MagicMock, patch

from disc_golf_pipeline.cli.main import build_parser, refresh_typesense_from_cache
from disc_golf_pipeline.services.process_data import run_process_data


class CachedTypesenseRefreshTests(unittest.TestCase):
    def setUp(self):
        self.prefix = "disc_golf_pipeline.cli.main."
        self.patchers = [
            patch(self.prefix + "get_gcp_project_id", return_value="project"),
            patch(self.prefix + "get_bigquery_dataset", return_value="dataset"),
            patch(self.prefix + "get_release_runtime", return_value={
                "state_table": "project.dataset.VariantState",
                "changes_table": "project.dataset.VariantChanges",
            }),
        ]
        for patcher in self.patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_refresh_processes_cached_inputs_before_publishing(self):
        observed = []
        release = {"collection": "release", "alias_changed": True}
        with patch(self.prefix + "run_process_data", side_effect=lambda **kw: observed.append(("process", kw))), patch(
            self.prefix + "publish_typesense_release", side_effect=lambda: observed.append(("publish", {})) or release
        ):
            self.assertEqual(release, refresh_typesense_from_cache())
        self.assertEqual([
            ("process", {"project_id": "project", "dataset": "dataset", "include_normalization": False}),
            ("publish", {}),
        ], observed)

    def test_processing_failure_prevents_publication(self):
        with patch(self.prefix + "run_process_data", side_effect=RuntimeError("classification failed")), patch(
            self.prefix + "publish_typesense_release"
        ) as publish:
            with self.assertRaisesRegex(RuntimeError, "classification failed"):
                refresh_typesense_from_cache()
        publish.assert_not_called()

    def test_mismatched_release_source_stops_before_mutating_data(self):
        with patch(self.prefix + "get_release_runtime", return_value={
            "state_table": "project.other.VariantState", "changes_table": "project.dataset.VariantChanges",
        }), patch(self.prefix + "run_process_data") as process, patch(
            self.prefix + "publish_typesense_release"
        ) as publish:
            with self.assertRaisesRegex(ValueError, "processing dataset"):
                refresh_typesense_from_cache()
        process.assert_not_called()
        publish.assert_not_called()

    def test_cached_processing_skips_source_preparation_and_normalization(self):
        prefix = "disc_golf_pipeline.services.process_data."
        client = MagicMock()
        with patch(prefix + "bigquery.Client", return_value=client), patch(
            prefix + "prepare_source_views"
        ) as sources, patch(prefix + "run_normalization") as normalize, patch(
            prefix + "run_disc_classification"
        ) as classify, patch(prefix + "ensure_variant_table_schemas"):
            run_process_data("project", "dataset", include_normalization=False)
        sources.assert_not_called()
        normalize.assert_not_called()
        classify.assert_called_once_with(client, "project", "dataset")
        self.assertTrue(any("MERGE `project.dataset.VariantState`" in c.args[0]
                            for c in client.query.call_args_list))

    def test_refresh_commands_are_available(self):
        parser = build_parser()
        for command in ("refresh-typesense-from-cache", "start-typesense-refresh-job"):
            self.assertEqual(command, parser.parse_args([command]).command)


if __name__ == "__main__":
    unittest.main()
