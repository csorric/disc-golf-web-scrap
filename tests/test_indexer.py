import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from disc_golf_pipeline.services.indexer import (
    NORMALIZED_COLLECTION_FIELDS,
    RELEASE_COLLECTION_FIELDS,
    build_previous_collection_cleanup_status,
    build_document,
    build_normalized_collection_schema,
    build_release_collection_name,
    derive_typesense_deployments_table_name,
    derive_variant_state_table_name,
    delete_typesense_collection,
    ensure_typesense_deployments_table,
    get_typesense_alias,
    point_typesense_alias,
    publish_typesense_release,
    send_upsert_batch,
    validate_collection_schema,
    validate_release_deployment,
)


class FakeResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code
        self.ok = 200 <= status_code < 300


class FakeSession:
    def __init__(self, response):
        self.response = response

    def post(self, *args, **kwargs):
        return self.response


class FakeAliasResponse:
    def __init__(self, payload=None, status_code=200):
        self.payload = payload or {}
        self.status_code = status_code

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeAliasSession:
    def __init__(self, get_response=None, put_response=None):
        self.get_response = get_response
        self.put_response = put_response
        self.put_call = None
        self.delete_call = None

    def get(self, *args, **kwargs):
        return self.get_response

    def put(self, *args, **kwargs):
        self.put_call = (args, kwargs)
        return self.put_response

    def delete(self, *args, **kwargs):
        self.delete_call = (args, kwargs)
        return self.put_response


class NormalizedIndexerTests(unittest.TestCase):
    def test_document_contains_normalized_search_and_faceting_fields(self):
        row = {
            "id": "variant-1",
            "product_id": "product-1",
            "variant_id": "variant-1",
            "source_variant_key": "shopify:example.com:variant-1",
            "title": "Proto Glow Champion Destroyer",
            "vendor": "Innova",
            "store": "example.com",
            "variant_title": "Blue 175g",
            "tags": "distance driver, glow",
            "normalized_manufacturer": "Innova Champion Discs",
            "normalized_model": "Destroyer",
            "source": "shopify",
            "retailer": "Example Store",
            "raw_vendor": "Innova",
            "item_type": "disc",
            "is_disc": True,
            "item_type_confidence": 0.98,
            "manufacturer_confidence": 0.97,
            "model_confidence": 0.97,
            "normalization_confidence": 0.97,
            "normalization_source": "product_type_disc_rule|deterministic_v2",
            "model_decision_level": "product",
            "normalization_version": "normalization-v1",
            "model_rules_version": "model-v2-5",
        }

        with patch("disc_golf_pipeline.services.indexer.time.time", return_value=1000):
            document = build_document(row)

        self.assertEqual("Innova Champion Discs", document["normalized_manufacturer"])
        self.assertEqual("Destroyer", document["normalized_model"])
        self.assertEqual("disc", document["item_type"])
        self.assertTrue(document["is_disc"])
        self.assertIn("innova champion discs", document["search_text"])
        self.assertIn("destroyer", document["search_text"])
        self.assertEqual(1000000, document["last_indexed_at"])

    def test_release_document_uses_stable_key_and_preserves_legacy_id(self):
        document = build_document(
            {
                "id": "legacy-1",
                "source_variant_key": "shopify:example.com:variant-1",
            },
            use_source_variant_key=True,
        )

        self.assertEqual("shopify:example.com:variant-1", document["id"])
        self.assertEqual("shopify:example.com:variant-1", document["source_variant_key"])
        self.assertEqual("legacy-1", document["legacy_id"])

    def test_nullable_normalization_values_are_omitted_not_coerced(self):
        document = build_document(
            {
                "id": "variant-2",
                "title": "Unknown item",
                "normalized_manufacturer": None,
                "normalized_model": None,
                "is_disc": None,
                "manufacturer_confidence": None,
                "model_confidence": None,
            }
        )

        self.assertNotIn("normalized_manufacturer", document)
        self.assertNotIn("normalized_model", document)
        self.assertNotIn("is_disc", document)
        self.assertNotIn("manufacturer_confidence", document)
        self.assertNotIn("model_confidence", document)

    def test_v5_schema_has_normalized_facets(self):
        schema = build_normalized_collection_schema("discs_v5")
        fields = {field["name"]: field for field in schema["fields"]}

        self.assertEqual("discs_v5", schema["name"])
        self.assertEqual("price", schema["default_sorting_field"])
        for field_name in (
            "source",
            "retailer",
            "normalized_manufacturer",
            "normalized_model",
            "item_type",
            "is_disc",
        ):
            self.assertTrue(fields[field_name]["facet"])

    def test_release_schema_contains_stable_and_legacy_identity_fields(self):
        schema = build_normalized_collection_schema(
            "discs_20260921_120000_abcdef",
            RELEASE_COLLECTION_FIELDS,
        )
        fields = {field["name"]: field for field in schema["fields"]}

        self.assertIn("source_variant_key", fields)
        self.assertIn("legacy_id", fields)

    def test_release_collection_name_is_timestamped_and_sanitized(self):
        name = build_release_collection_name(
            "disc releases",
            now=datetime(2026, 9, 21, 12, 30, 45, tzinfo=timezone.utc),
            suffix="abcdef",
        )

        self.assertEqual("disc_releases_20260921_123045_abcdef", name)

    def test_schema_validator_accepts_server_expanded_defaults(self):
        fields = []
        for field in NORMALIZED_COLLECTION_FIELDS:
            expanded = {
                "name": field["name"],
                "type": field["type"],
                "facet": field.get("facet", False),
                "optional": field.get("optional", False),
                "sort": field.get("sort", False),
            }
            fields.append(expanded)

        validate_collection_schema(
            {"fields": fields, "default_sorting_field": "price"}
        )

    def test_import_http_200_with_document_failure_is_rejected(self):
        response = FakeResponse(
            '{"success":true}\n{"success":false,"error":"bad document"}'
        )
        result = send_upsert_batch(
            session=FakeSession(response),
            host="https://typesense.example",
            admin_key="secret",
            collection="discs_v5",
            docs=[{"id": "1"}, {"id": "2"}],
            batch_index=1,
        )

        self.assertFalse(result["ok"])
        self.assertEqual("bad document", result["failures"][0]["error"])

    def test_variant_state_table_is_derived_from_changes_table(self):
        self.assertEqual(
            "project.dataset.VariantState",
            derive_variant_state_table_name("project.dataset.VariantChanges"),
        )

    def test_deployments_table_is_derived_from_state_table(self):
        self.assertEqual(
            "project.dataset.TypesenseDeployments",
            derive_typesense_deployments_table_name("project.dataset.VariantState"),
        )

    def test_deployments_table_tracks_release_lifecycle(self):
        class FakeJob:
            def result(self):
                return []

        class FakeClient:
            def __init__(self):
                self.sql = []

            def query(self, sql):
                self.sql.append(sql)
                return FakeJob()

        client = FakeClient()
        ensure_typesense_deployments_table(
            client,
            "project.dataset.TypesenseDeployments",
        )

        sql = "\n".join(client.sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS", sql)
        self.assertIn("schema_version STRING NOT NULL", sql)
        self.assertIn("source_batch_run_id STRING", sql)
        self.assertIn("activated_at TIMESTAMP", sql)
        self.assertIn("previous_collection_deleted_at TIMESTAMP", sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS", sql)

    def test_missing_typesense_alias_returns_none(self):
        session = FakeAliasSession(
            get_response=FakeAliasResponse(status_code=404),
        )

        alias = get_typesense_alias(
            session,
            "https://typesense.example",
            "secret",
            "discs_prod",
        )

        self.assertIsNone(alias)

    def test_point_typesense_alias_verifies_target(self):
        session = FakeAliasSession(
            put_response=FakeAliasResponse(
                {"name": "discs_prod", "collection_name": "discs_20260921_abcdef"}
            ),
        )

        alias = point_typesense_alias(
            session,
            "https://typesense.example",
            "secret",
            "discs_prod",
            "discs_20260921_abcdef",
        )

        self.assertEqual("discs_20260921_abcdef", alias["collection_name"])
        _, kwargs = session.put_call
        self.assertEqual(
            {"collection_name": "discs_20260921_abcdef"},
            kwargs["json"],
        )

    def test_release_validation_rejects_stale_source_batch(self):
        runtime = {
            "changes_table": "project.dataset.VariantChanges",
            "state_table": "project.dataset.VariantState",
            "host": "https://typesense.example",
            "admin_key": "secret",
        }
        deployment = {
            "schema_version": "search-v2-source-identity",
            "source_batch_run_id": "old-batch",
            "collection_name": "discs_20260921_abcdef",
        }

        with patch(
            "disc_golf_pipeline.services.indexer.get_latest_variant_batch",
            return_value={"batch_run_id": "new-batch", "batch_run_ts": None},
        ):
            with self.assertRaisesRegex(RuntimeError, "no longer current"):
                validate_release_deployment(
                    client=object(),
                    runtime=runtime,
                    deployment=deployment,
                    session=object(),
                )

    def test_publish_activates_the_exact_release_it_built(self):
        build_summary = {
            "deployment_id": "deployment-1",
            "collection": "discs_20260922_abcdef",
            "schema_version": "search-v2-source-identity",
            "source_batch_run_id": "batch-1",
            "rows_seen": 100,
            "batches_sent": 1,
            "validation": {"typesense_documents": 100},
        }
        activation_summary = {
            "status": "ACTIVE",
            "alias": "discs_prod",
            "previous_collection": "discs_v4",
        }
        with patch(
            "disc_golf_pipeline.services.indexer.run_typesense_release_build",
            return_value=build_summary,
        ) as build_mock:
            with patch(
                "disc_golf_pipeline.services.indexer.activate_latest_typesense_release",
                return_value=activation_summary,
            ) as activate_mock:
                result = publish_typesense_release()

        build_mock.assert_called_once_with(collection_name=None)
        activate_mock.assert_called_once_with(deployment_id="deployment-1")
        self.assertEqual("ACTIVE", result["status"])
        self.assertTrue(result["alias_changed"])

    def test_cleanup_status_allows_only_unreferenced_previous_collection(self):
        status = build_previous_collection_cleanup_status(
            deployment={
                "deployment_id": "deployment-1",
                "collection_name": "discs_new",
                "previous_collection": "discs_old",
                "previous_collection_deleted_at": None,
            },
            alias_data={"name": "discs_prod", "collection_name": "discs_new"},
            aliases=[{"name": "discs_prod", "collection_name": "discs_new"}],
            previous_collection_exists=True,
        )

        self.assertTrue(status["eligible_for_deletion"])
        self.assertEqual([], status["blocking_reasons"])

    def test_cleanup_status_blocks_collection_targeted_by_any_alias(self):
        status = build_previous_collection_cleanup_status(
            deployment={
                "deployment_id": "deployment-1",
                "collection_name": "discs_new",
                "previous_collection": "discs_old",
                "previous_collection_deleted_at": None,
            },
            alias_data={"name": "discs_prod", "collection_name": "discs_new"},
            aliases=[
                {"name": "discs_prod", "collection_name": "discs_new"},
                {"name": "discs_backup", "collection_name": "discs_old"},
            ],
            previous_collection_exists=True,
        )

        self.assertFalse(status["eligible_for_deletion"])
        self.assertEqual(["discs_backup"], status["referencing_aliases"])

    def test_delete_collection_verifies_that_it_is_gone(self):
        session = FakeAliasSession(
            get_response=FakeAliasResponse(status_code=404),
            put_response=FakeAliasResponse({"name": "discs_old"}),
        )

        result = delete_typesense_collection(
            session,
            "https://typesense.example",
            "secret",
            "discs_old",
        )

        self.assertEqual("discs_old", result["name"])
        self.assertIsNotNone(session.delete_call)


if __name__ == "__main__":
    unittest.main()
