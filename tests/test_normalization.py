import unittest

from disc_golf_pipeline.services.model_normalization import (
    DEFAULT_MODEL_RULES_VERSION,
    build_apply_product_model_decisions_sql,
    build_disc_model_catalog_sql,
    build_model_quality_views_sql,
    build_product_model_candidates_sql,
    build_variant_model_decisions_sql,
    model_alias_requires_manufacturer,
    normalize_model_text,
    validate_model_quality_checks,
)
from disc_golf_pipeline.services.normalization import (
    DEFAULT_NORMALIZATION_VERSION,
    STOREFRONT_RETAILERS,
    build_normalized_products_sql,
    build_normalized_variant_snapshot_sql,
    build_quality_views_sql,
    build_storefront_rules_sql,
    classify_item_type,
    normalize_match_key,
    storefront_vendor_mode,
    validate_quality_checks,
)
from disc_golf_pipeline.services.source_views import (
    build_infinite_variants_view_sql,
    build_shopify_variants_view_sql,
)


class ItemTypeClassificationTests(unittest.TestCase):
    def test_infinite_is_always_a_disc(self):
        self.assertEqual(
            classify_item_type("", "Heat", source="infinite"),
            ("disc", 1.0, "infinite_source"),
        )

    def test_explicit_disc_product_type_is_high_confidence(self):
        self.assertEqual(
            classify_item_type("Discs", "Build Your Game Z Zone"),
            ("disc", 0.98, "product_type_disc_rule"),
        )

    def test_negative_title_overrides_weak_disc_product_type(self):
        item_type, confidence, source = classify_item_type(
            "Disc Golf", "Foundation Coffee Mug"
        )
        self.assertEqual(item_type, "accessory")
        self.assertEqual(confidence, 0.95)
        self.assertEqual(source, "title_tags_negative_rule")

    def test_bag_is_not_promoted_by_disc_words(self):
        item_type, _, _ = classify_item_type("Disc Golf Bag", "Disc Golf Backpack")
        self.assertEqual(item_type, "bag")

    def test_apparel_is_not_promoted_by_disc_words(self):
        item_type, _, _ = classify_item_type("Disc Golf Shirt", "Tour Series Shirt")
        self.assertEqual(item_type, "apparel")

    def test_model_only_title_remains_unknown(self):
        item_type, confidence, source = classify_item_type(
            "", "2026 Tour Series Hanna Huynh Heat"
        )
        self.assertEqual(item_type, "unknown")
        self.assertEqual(confidence, 0.0)
        self.assertEqual(source, "insufficient_evidence")

    def test_phone_case_overrides_disc_words(self):
        item_type, confidence, source = classify_item_type(
            "Disc Golf Disc", "Trash Panda Phone Case Inner Core"
        )
        self.assertEqual(item_type, "accessory")
        self.assertEqual(confidence, 0.95)
        self.assertEqual(source, "title_tags_negative_rule")


class StorefrontRuleTests(unittest.TestCase):
    def test_representative_vendor_modes(self):
        self.assertEqual(storefront_vendor_mode("foundationdiscs.com"), "MIXED")
        self.assertEqual(storefront_vendor_mode("shopledgestone.com"), "RETAILER")
        self.assertEqual(storefront_vendor_mode("dynamicdiscs.com"), "BRAND")

    def test_storefront_seed_covers_live_audit_set(self):
        self.assertGreaterEqual(len(STOREFRONT_RETAILERS), 40)
        self.assertIn("foundationdiscs.com", STOREFRONT_RETAILERS)
        self.assertIn("shopledgestone.com", STOREFRONT_RETAILERS)

    def test_match_keys_ignore_punctuation_and_case(self):
        self.assertEqual(normalize_match_key("MVP Disc Sports, LLC"), "mvpdiscsportsllc")
        self.assertEqual(normalize_match_key("Latitude 64°"), "latitude64")


class ModelAliasTests(unittest.TestCase):
    def test_model_text_normalization_preserves_token_boundaries(self):
        self.assertEqual(normalize_model_text("Time-Lapse (Retooled)"), "time lapse retooled")

    def test_generic_models_require_manufacturer(self):
        self.assertTrue(model_alias_requires_manufacturer("Wave"))
        self.assertTrue(model_alias_requires_manufacturer("Money"))
        self.assertTrue(model_alias_requires_manufacturer("Phoenix"))
        self.assertTrue(model_alias_requires_manufacturer("Orbit"))
        self.assertTrue(model_alias_requires_manufacturer("Magma"))
        self.assertTrue(model_alias_requires_manufacturer("Chameleon"))
        self.assertTrue(model_alias_requires_manufacturer("Viking"))
        self.assertTrue(model_alias_requires_manufacturer("Z"))

    def test_reviewed_short_models_can_be_retrieved_without_manufacturer(self):
        self.assertFalse(model_alias_requires_manufacturer("Heat"))
        self.assertFalse(model_alias_requires_manufacturer("Zone"))
        self.assertTrue(model_alias_requires_manufacturer("DD"))

    def test_cross_manufacturer_alias_requires_manufacturer(self):
        self.assertTrue(model_alias_requires_manufacturer("Vector", manufacturer_count=2))


class SqlBuilderTests(unittest.TestCase):
    def test_source_views_are_fully_qualified_and_preserve_contract(self):
        shopify_sql = build_shopify_variants_view_sql("project", "dataset")
        infinite_sql = build_infinite_variants_view_sql("project", "dataset")
        self.assertIn("`project.dataset.Products`", shopify_sql)
        self.assertIn("`project.dataset.ProductInfo`", shopify_sql)
        self.assertIn("`project.dataset.InfiniteDiscs`", infinite_sql)
        self.assertIn("ManufacturerName", infinite_sql)
        self.assertIn("ModelName", infinite_sql)

    def test_rules_sql_seeds_special_storefront_modes(self):
        sql = build_storefront_rules_sql(
            "project", "dataset", DEFAULT_NORMALIZATION_VERSION
        )
        self.assertIn("StorefrontNormalizationRules", sql)
        self.assertIn("foundationdiscs.com", sql)
        self.assertIn("shopledgestone.com", sql)
        self.assertIn("'RETAILER' AS vendor_mode", sql)
        self.assertIn("'BRAND' AS vendor_mode", sql)

    def test_normalized_products_sql_is_versioned_audited_and_idempotent(self):
        sql = build_normalized_products_sql(
            "project", "dataset", DEFAULT_NORMALIZATION_VERSION
        )
        self.assertIn("NormalizedProducts", sql)
        self.assertIn("ProductNormalizationAudit", sql)
        self.assertIn("source_row_hash", sql)
        self.assertIn("normalization_version", sql)
        self.assertIn("WHEN NOT MATCHED BY SOURCE THEN DELETE", sql)
        self.assertIn("retailer_vendor_ignored", sql)
        self.assertEqual(7, sql.count("base.item_type = 'disc'"))
        self.assertEqual(
            7,
            sql.count("$.model_decision.model_rules_version"),
        )

    def test_snapshot_and_quality_sql_include_phase_one_fields(self):
        snapshot_sql = build_normalized_variant_snapshot_sql("project", "dataset")
        quality_sql = build_quality_views_sql("project", "dataset")
        for field in (
            "retailer",
            "raw_vendor",
            "normalized_manufacturer",
            "normalized_model",
            "item_type",
            "is_disc",
            "source_variant_key",
            "normalization_version",
        ):
            self.assertIn(field, snapshot_sql)
        self.assertIn("ROW_NUMBER() OVER", snapshot_sql)
        self.assertIn("PARTITION BY raw.id", snapshot_sql)
        self.assertIn("v_NormalizationQualityReport", quality_sql)
        self.assertIn("v_NormalizationQualityChecks", quality_sql)
        self.assertIn("infinite_model_coverage", quality_sql)
        self.assertIn("missing_source_variant_keys", quality_sql)
        self.assertIn("duplicate_source_variant_keys", quality_sql)

    def test_model_quality_report_tracks_llm_promotions(self):
        sql = build_model_quality_views_sql("project", "dataset")

        self.assertIn("llm_v2_accepts", sql)
        self.assertIn("STARTS_WITH(model_source, 'llm_v2_')", sql)

    def test_model_catalog_is_search_oriented_and_reviewed(self):
        sql = build_disc_model_catalog_sql(
            "project", "dataset", DEFAULT_MODEL_RULES_VERSION
        )
        self.assertIn("DiscModelEntities", sql)
        self.assertIn("DiscModelAliases", sql)
        self.assertIn("DiscModelNormalizationOverrides", sql)
        self.assertIn("derived_parenthetical", sql)
        self.assertIn("Cloud Breaker", sql)
        self.assertIn("requires_manufacturer", sql)
        self.assertIn("is_context_only", sql)
        self.assertIn("ANY_VALUE(TRIM(canonical_model))", sql)

    def test_product_candidates_use_token_phrases_and_conflict_suppression(self):
        sql = build_product_model_candidates_sql(
            "project", "dataset", DEFAULT_MODEL_RULES_VERSION
        )
        self.assertIn("product_ngrams", sql)
        self.assertIn("variant_ngrams", sql)
        self.assertIn("stronger.alias_token_count", sql)
        self.assertIn("manufacturer_compatible", sql)
        self.assertIn("credible_candidate_count = 1", sql)
        self.assertIn("'ACCEPT'", sql)
        self.assertEqual(2, sql.count("THEN 'ACCEPT'"))
        self.assertEqual(2, sql.count("ngram_start + ngram_size - 1"))
        self.assertNotIn("ngram_start + ngram_size)", sql.replace("ngram_start + ngram_size - 1", ""))

    def test_variant_accepts_require_a_multi_model_product(self):
        sql = build_variant_model_decisions_sql(
            "project", "dataset", DEFAULT_MODEL_RULES_VERSION
        )
        self.assertIn("ngram_start + ngram_size - 1", sql)
        self.assertIn("product_entity_count >= 2", sql)
        self.assertIn("AND manufacturer_compatible THEN 'ACCEPT'", sql)
        self.assertIn("is_non_disc_variant", sql)
        self.assertIn("key ?chains?", sql)
        self.assertIn("credible_variant_candidate_count = 1", sql)
        self.assertIn("deterministic_v2_multi_model_variant", sql)

    def test_product_evidence_records_decision_source(self):
        sql = build_apply_product_model_decisions_sql(
            "project", "dataset", DEFAULT_MODEL_RULES_VERSION
        )

        self.assertIn("decision.decision_source AS decision_source", sql)

    def test_model_updates_are_audited_and_idempotent(self):
        sql = build_apply_product_model_decisions_sql(
            "project", "dataset", DEFAULT_MODEL_RULES_VERSION
        )
        self.assertIn("DesiredProductModels", sql)
        self.assertIn("ProductNormalizationAudit", sql)
        self.assertIn("IS DISTINCT FROM", sql)
        self.assertIn("JSON_SET", sql)

    def test_model_reports_compare_v1_and_create_review_samples(self):
        sql = build_model_quality_views_sql("project", "dataset")
        self.assertIn("v_ModelNormalizationQualityReport", sql)
        self.assertIn("v_ModelNormalizationQualityChecks", sql)
        self.assertIn("v_ModelNormalizationComparison", sql)
        self.assertIn("v_ModelNormalizationReviewSample", sql)
        self.assertIn("accepted_generic_without_manufacturer", sql)
        self.assertIn("accepted_context_only_aliases", sql)
        self.assertIn("accepted_non_disc_variants", sql)
        self.assertIn("CAST(snapshot.product_id AS STRING)", sql)


class QualityCheckTests(unittest.TestCase):
    def test_quality_validation_accepts_passing_rows(self):
        validate_quality_checks(
            [
                {
                    "check_name": "duplicates",
                    "observed_value": 0.0,
                    "required_value": 0.0,
                    "passed": True,
                }
            ]
        )

    def test_quality_validation_reports_failures(self):
        with self.assertRaisesRegex(RuntimeError, "duplicate_variant_ids"):
            validate_quality_checks(
                [
                    {
                        "check_name": "duplicate_variant_ids",
                        "observed_value": 2.0,
                        "required_value": 0.0,
                        "passed": False,
                    }
                ]
            )

    def test_model_quality_validation_reports_failures(self):
        with self.assertRaisesRegex(RuntimeError, "generic_alias"):
            validate_model_quality_checks(
                [
                    {
                        "check_name": "generic_alias",
                        "observed_value": 1.0,
                        "required_value": 0.0,
                        "passed": False,
                    }
                ]
            )


if __name__ == "__main__":
    unittest.main()
