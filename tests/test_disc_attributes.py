import re
import unittest

from disc_golf_pipeline.services.disc_attributes import (
    BARE_FLIGHT_SEQUENCE_PATTERN,
    FLIGHT_SEQUENCE_PATTERN,
    NON_VARIANT_WEIGHT_PATTERN,
    VARIANT_WEIGHT_PATTERN,
    VARIANT_WEIGHT_BARE_PATTERN,
    TITLE_WEIGHT_RANGE_PATTERN,
    LEADING_TITLE_WEIGHT_RANGE_PATTERN,
    WEIGHT_RANGE_PATTERN,
    WEIGHT_PATTERN,
    build_disc_attributes_view_sql,
)
from disc_golf_pipeline.services.process_data import build_variant_snapshot_view_sql


class DiscAttributeTests(unittest.TestCase):
    def test_flight_sequence_handles_negative_turn(self):
        match = re.search(FLIGHT_SEQUENCE_PATTERN, "Flight Numbers: 12 / 5 / -1 / 3")
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "12 / 5 / -1 / 3")
        bare_match = re.search(BARE_FLIGHT_SEQUENCE_PATTERN, "Destroyer 12/5/-1/3")
        self.assertEqual(bare_match.group(1), "12/5/-1/3")
        html_match = re.search(BARE_FLIGHT_SEQUENCE_PATTERN, "Destroyer 12 | 5 | -1 | 3")
        self.assertEqual(html_match.group(1), "12 | 5 | -1 | 3")

    def test_weight_requires_a_specific_gram_value(self):
        self.assertEqual(
            re.search(VARIANT_WEIGHT_PATTERN, "Blue / 173g").group(1), "173"
        )
        self.assertEqual(
            re.search(WEIGHT_PATTERN, "Actual Weight: 172 grams").group(1), "172"
        )
        self.assertIsNone(re.search(WEIGHT_PATTERN, "Weight: 170-172g"))
        self.assertEqual(
            "  Actual Weight: 172g",
            re.sub(NON_VARIANT_WEIGHT_PATTERN, " ",
                   "Max Weight: 175g Actual Weight: 172g"),
        )
        self.assertEqual(
            re.findall(VARIANT_WEIGHT_BARE_PATTERN, "Hard 173 green/orange"), ["173"]
        )
        self.assertTrue(re.search(WEIGHT_RANGE_PATTERN, "Pink 173-174g"))
        self.assertEqual(
            re.search(TITLE_WEIGHT_RANGE_PATTERN, "#1 173-174g | Red").group(1),
            "173-174g",
        )
        self.assertTrue(re.search(WEIGHT_RANGE_PATTERN, "Pink 173-5g"))
        self.assertTrue(re.search(WEIGHT_RANGE_PATTERN, "Pink 173–174g"))
        self.assertEqual(
            re.search(LEADING_TITLE_WEIGHT_RANGE_PATTERN,
                      "167-169 Marigold Siver prisms").group(1),
            "167-169",
        )
        self.assertIsNone(re.search(LEADING_TITLE_WEIGHT_RANGE_PATTERN,
                                    "Blue 167-169 Marigold"))
        self.assertIsNone(re.search(WEIGHT_RANGE_PATTERN, "Bio Gold - 181 - Blue"))

    def test_view_is_disc_only_and_retains_evidence(self):
        sql = build_disc_attributes_view_sql("project", "dataset")
        self.assertIn("WHERE item_type = 'disc'", sql)
        self.assertIn("NormalizedVariantSnapshot", sql)
        self.assertIn("flight_evidence", sql)
        self.assertIn("weight_evidence", sql)
        self.assertIn("BETWEEN 100 AND 190", sql)
        self.assertIn("BETWEEN -5 AND 2", sql)
        self.assertIn("ARRAY_LENGTH(html_weight_texts) = 1", sql)
        self.assertIn("TryDiscsModelMatches", sql)
        self.assertIn("catalog_speed, flight_numbers", sql)
        self.assertIn("local_speed", sql)
        self.assertIn("flight_conflict", sql)
        self.assertIn("flight_attribution", sql)
        self.assertIn("DiscWeightLlmReviews", sql)
        self.assertIn("'variant_title_range'", sql)
        self.assertIn("normalized_weight_min_g", sql)
        self.assertIn("normalized_weight_max_g", sql)
        self.assertLess(
            sql.index("WHEN weight_source = 'variant_title' THEN 0.97"),
            sql.index("AND ABS(normalized_weight_value - raw_weight_g) >= 2 THEN 0.55"),
        )

    def test_snapshot_uses_normalized_weight_only_for_discs(self):
        sql = build_variant_snapshot_view_sql("project", "dataset")
        self.assertIn("LEFT JOIN `project.dataset.NormalizedDiscAttributes`", sql)
        self.assertIn("WHEN src.item_type = 'disc' THEN attributes.normalized_weight_g", sql)
        self.assertIn("attributes.speed AS speed", sql)
        self.assertIn("attributes.weight_source AS weight_source", sql)
        self.assertIn("attributes.normalized_weight_min_g AS weight_min_g", sql)


if __name__ == "__main__":
    unittest.main()
