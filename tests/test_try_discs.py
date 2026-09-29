import unittest

from disc_golf_pipeline.services.try_discs import (
    build_catalog_index,
    fetch_catalog,
    flight_numbers,
    match_model,
)
from disc_golf_pipeline.services.try_discs_audit import (
    build_comparison_sql,
    compare_catalog,
    render_try_discs_audit_html,
)
from disc_golf_pipeline.services.try_discs_sync import build_match_rows, parse_flight_record


DESTROYER = {
    "brand": "Innova",
    "name": "Destroyer",
    "url": "https://trydiscs.com/product/innova/destroyer",
    "speed": 12,
    "glide": 5,
    "turn": -1,
    "fade": 3,
}


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, params, headers, timeout):
        self.calls.append((url, params, headers, timeout))
        data = [DESTROYER] if params["offset"] == 0 else [{**DESTROYER, "name": "Wraith"}]
        return FakeResponse({"meta": {"total": 2, "dataset_version": "test"}, "data": data})


class TryDiscsTests(unittest.TestCase):
    def test_fetch_uses_key_header_and_paginates_without_exposing_key(self):
        session = FakeSession()
        catalog, meta = fetch_catalog(api_key="secret", session=session)
        self.assertEqual(2, len(catalog))
        self.assertEqual("test", meta["dataset_version"])
        self.assertEqual([0, 1], [call[1]["offset"] for call in session.calls])
        self.assertTrue(all(call[2] == {"X-API-Key": "secret"} for call in session.calls))

    def test_match_requires_unique_brand_and_model(self):
        index = build_catalog_index([DESTROYER])
        disc, match_type = match_model("Innova Champion Discs", "Destroyer", index)
        self.assertEqual(DESTROYER, disc)
        self.assertEqual("brand_alias", match_type)
        self.assertEqual((None, None), match_model("Axiom Discs", "Destroyer", index))
        duplicate_index = build_catalog_index([DESTROYER, DESTROYER])
        self.assertEqual((None, None), match_model("Innova", "Destroyer", duplicate_index))

    def test_sync_rows_preserve_partial_unique_matches_without_mixing(self):
        rows = build_match_rows(
            [DESTROYER, {**DESTROYER, "name": "Incomplete", "fade": None}],
            [{"manufacturer": "Innova Champion Discs", "model": "Destroyer"},
             {"manufacturer": "Innova", "model": "Incomplete"}],
            "test-version",
        )
        self.assertEqual(2, len(rows))
        self.assertIsNone(rows[1]["fade"])
        self.assertEqual(12, rows[1]["speed"])
        self.assertEqual("brand_alias", rows[0]["match_type"])
        self.assertEqual((12, 5, -1, 3), tuple(rows[0][field] for field in
                         ("speed", "glide", "turn", "fade")))

    def test_parser_preserves_unsupported_values_and_rejects_non_numbers(self):
        values, invalid = parse_flight_record(
            {"speed": 15, "glide": True, "turn": "-1.5", "fade": "NaN"})
        self.assertEqual({"speed": 15.0, "glide": None, "turn": -1.5, "fade": None}, values)
        self.assertEqual(["glide", "fade"], invalid)

    def test_complete_flight_parser_rejects_boolean_and_nonfinite_values(self):
        for value in (True, False, float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                self.assertIsNone(flight_numbers({**DESTROYER, "speed": value}))
        self.assertEqual((12, 5, 0, 3), flight_numbers({**DESTROYER, "turn": 0}))

    def test_ambiguous_catalog_records_block_scoring_and_preserve_evidence(self):
        rows = build_match_rows([DESTROYER, {**DESTROYER, "turn": -2}],
                               [{"manufacturer": "Innova", "model": "Destroyer"}], "test")
        self.assertEqual(1, len(rows))
        self.assertTrue(rows[0]["flight_conflict_unresolved"])
        self.assertIsNone(rows[0]["speed"])
        self.assertIn('"turn": -2', rows[0]["raw_flight_json"])

    def test_comparison_counts_fill_only_missing_flights(self):
        rows = [
            {"manufacturer": "Innova Champion Discs", "model": "Destroyer", "speed": None,
             "glide": None, "turn": None, "fade": None, "variants": 3, "missing_flight": 3},
            {"manufacturer": "Innova Champion Discs", "model": "Destroyer", "speed": 12,
             "glide": 5, "turn": 0, "fade": 3, "variants": 2, "missing_flight": 0},
            {"manufacturer": None, "model": None, "speed": None,
             "glide": None, "turn": None, "fade": None, "variants": 1, "missing_flight": 1},
        ]
        report = compare_catalog([DESTROYER], rows)
        self.assertEqual(3, report["totals"]["fillable"])
        self.assertEqual(2, report["totals"]["existing_disagreements"])
        self.assertEqual(1, report["totals"]["missing_without_model"])
        self.assertEqual(5, report["totals"]["projected_flight_resolved"])

    def test_report_is_attributed_and_escapes_untrusted_text(self):
        sql = build_comparison_sql("project", "dataset")
        self.assertIn("`project.dataset.NormalizedDiscAttributes`", sql)
        report = compare_catalog([DESTROYER], [])
        report["unmatched"] = [(("<script>", "Model"), 1)]
        rendered = render_try_discs_audit_html(report, "2026-09-21")
        self.assertIn("Disc data by Try Discs", rendered)
        self.assertIn("&lt;script&gt;", rendered)


if __name__ == "__main__":
    unittest.main()
