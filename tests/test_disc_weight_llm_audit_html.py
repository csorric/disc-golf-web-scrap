import unittest

from disc_golf_pipeline.services.disc_weight_llm_audit_html import (
    build_review_details_sql,
    build_review_scope_sql,
    render_disc_weight_llm_audit_html,
)


class DiscWeightLlmAuditHtmlTests(unittest.TestCase):
    def test_scope_and_details_use_reviews_and_current_attributes(self):
        scope = build_review_scope_sql("project", "dataset")
        details = build_review_details_sql("project", "dataset", limit=10)
        self.assertIn("`project.dataset.DiscWeightLlmReviews`", scope)
        self.assertIn("COUNTIF(NOT completed) AS pending", scope)
        self.assertIn("`project.dataset.NormalizedDiscAttributes`", details)
        self.assertIn("LIMIT 10", details)

    def test_render_shows_outcome_and_escapes_untrusted_evidence(self):
        rendered = render_disc_weight_llm_audit_html(
            {"attempted": 1, "found": 1}, {"eligible": 2, "pending": 1},
            [{"status": "FOUND", "title": "<script>", "evidence": "<b>173g</b>",
              "product_link": "javascript:alert(1)", "reviewed_weight_g": 173}],
        )
        self.assertIn("&lt;script&gt;", rendered)
        self.assertIn("&lt;b&gt;173g&lt;/b&gt;", rendered)
        self.assertIn("173 g", rendered)
        self.assertNotIn('href="javascript:', rendered)


if __name__ == "__main__":
    unittest.main()
