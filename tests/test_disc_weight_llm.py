import unittest

from disc_golf_pipeline.services.disc_weight_llm import (
    build_weight_queue_sql,
    parse_weight_response,
)


class DiscWeightLlmTests(unittest.TestCase):
    def test_accepts_weight_only_with_matching_variant_evidence(self):
        row = {"variant_title": "Blue 173g", "body_html": "Max weight 180g"}
        self.assertEqual(
            ("FOUND", 173, "173g", "variant label"),
            parse_weight_response(
                '{"weight_g":"173","evidence":"173g","reason":"variant label"}',
                row,
            ),
        )
        status, *_ = parse_weight_response(
            '{"weight_g":"200","evidence":"200g","reason":""}', row
        )
        self.assertEqual("INVALID", status)
        status, *_ = parse_weight_response(
            '{"weight_g":"175","evidence":"Max Weight: 175g","reason":""}',
            {"variant_title": "#2", "body_html": "Max Weight: 175g"},
        )
        self.assertEqual("INVALID", status)
        status, *_ = parse_weight_response(
            '{"weight_g":"180","evidence":"180g","reason":""}',
            {"variant_title": "Blue", "body_html": "Max weight: 180g"},
        )
        self.assertEqual("INVALID", status)
        status, *_ = parse_weight_response(
            '{"weight_g":"180","evidence":"180g","reason":""}',
            {"variant_title": "Blue 173g", "body_html": ""},
        )
        self.assertEqual("INVALID", status)

    def test_queue_is_disc_only_cached_and_capped(self):
        sql = build_weight_queue_sql("project", "dataset")
        self.assertIn("s.item_type = 'disc'", sql)
        self.assertIn("review.status IN ('FOUND', 'NONE')", sql)
        self.assertIn("LIMIT @batch_limit", sql)
        self.assertIn("a.weight_source, '') != 'variant_title_range'", sql)


if __name__ == "__main__":
    unittest.main()
