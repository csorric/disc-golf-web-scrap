import unittest
from datetime import datetime, timezone

from disc_golf_pipeline.services.llm_audit_html import (
    build_llm_review_records_sql,
    render_llm_audit_html,
)


class LlmAuditHtmlTests(unittest.TestCase):
    def test_records_query_is_limited_to_current_prompt_hash(self):
        sql = build_llm_review_records_sql("project", "dataset")

        self.assertIn("DiscModelLlmReviews", sql)
        self.assertIn("DiscModelLlmQueue", sql)
        self.assertIn("DiscModelLlmResolutions", sql)
        self.assertIn("resolution.promotion_policy_version", sql)
        self.assertIn("review.prompt_hash =", sql)
        self.assertIn("TO_JSON_STRING(queue.candidates)", sql)

    def test_report_renders_counts_filters_and_escapes_catalog_text(self):
        records = [
            {
                "status": "INVALID",
                "audit_stratum": "multi_candidate",
                "decision_level": "product",
                "title": "<script>alert(1)</script>",
                "store": "Example Store",
                "raw_vendor": "Vendor",
                "candidates_json": '[{"manufacturer":"MVP","model":"Wave","disc_entity_id":"entity-1"}]',
                "rationale": "Ambiguous",
                "error_message": "Unexpected response",
                "raw_response": "{}",
                "input_tokens": 100,
                "output_tokens": 20,
                "review_key": "product:1",
                "resolution_id": "resolution-1",
                "promotion_source": "llm_v2_product_candidate",
            }
        ]
        report = render_llm_audit_html(
            {"runs": []},
            records,
            generated_at=datetime(2026, 9, 22, tzinfo=timezone.utc),
        )

        self.assertIn("Total reviewed", report)
        self.assertIn("Invalid response", report)
        self.assertIn("PROMOTED", report)
        self.assertIn("multi_candidate", report)
        self.assertIn("entity-1", report)
        self.assertIn("id=\"search\"", report)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", report)
        self.assertNotIn("<script>alert(1)</script>", report)


if __name__ == "__main__":
    unittest.main()
