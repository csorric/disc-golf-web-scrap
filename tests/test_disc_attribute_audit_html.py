import unittest

from disc_golf_pipeline.services.disc_attribute_audit_html import (
    build_disc_attribute_samples_sql,
    build_disc_attribute_summary_sql,
    render_disc_attribute_audit_html,
)


class DiscAttributeAuditHtmlTests(unittest.TestCase):
    def test_queries_use_disc_view_and_sample_each_category(self):
        summary_sql = build_disc_attribute_summary_sql("project", "dataset")
        samples_sql = build_disc_attribute_samples_sql("project", "dataset", 12)
        self.assertIn("`project.dataset.NormalizedDiscAttributes`", summary_sql)
        self.assertIn("COUNTIF(weight_conflict)", summary_sql)
        self.assertIn("PARTITION BY audit_category", samples_sql)
        self.assertIn("<= 12", samples_sql)
        self.assertIn("rejected weight", samples_sql)
        self.assertIn("title weight range", samples_sql)

    def test_render_escapes_product_and_evidence(self):
        summary = [{"source": "shopify", "store": "example", "discs": 1}]
        samples = [{
            "audit_category": "rejected weight",
            "title": "<script>alert(1)</script>",
            "product_link": "javascript:alert(1)",
            "flight_evidence": "<b>untrusted</b>",
            "weight_evidence": "200",
        }]
        rendered = render_disc_attribute_audit_html(summary, samples)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", rendered)
        self.assertIn("&lt;b&gt;untrusted&lt;/b&gt;", rendered)
        self.assertNotIn('href="javascript:', rendered)

    def test_render_preserves_a_title_weight_range(self):
        summary = [{"source": "shopify", "store": "example", "discs": 1,
                    "weight_ranges": 1}]
        samples = [{"audit_category": "title weight range",
                    "raw_weight_g": 227,
                    "normalized_weight_min_g": 173,
                    "normalized_weight_max_g": 174}]
        rendered = render_disc_attribute_audit_html(summary, samples)
        self.assertIn("173–174 g range", rendered)
        self.assertIn("Title weight ranges", rendered)
        self.assertIn("Raw source (rejected): 227 g", rendered)

    def test_render_distinguishes_selected_weight_from_rejected_raw_weight(self):
        samples = [{"audit_category": "corrected weight",
                    "raw_weight_g": 227,
                    "normalized_weight_g": 176,
                    "weight_source": "variant_title"}]
        rendered = render_disc_attribute_audit_html([], samples)
        self.assertIn("<b>176 g</b>", rendered)
        self.assertIn("Selected · variant_title", rendered)
        self.assertIn("Raw source (rejected): 227 g", rendered)


if __name__ == "__main__":
    unittest.main()
