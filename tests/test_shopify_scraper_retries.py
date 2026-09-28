import unittest
from unittest.mock import Mock, patch

from disc_golf_pipeline.scrapers.shopify import ShopifyScraper


class ShopifyScraperRetryTests(unittest.TestCase):
    def test_recovers_after_four_temporary_server_errors(self):
        failed = Mock(status_code=500, headers={}, text="")
        recovered = Mock(status_code=200)
        recovered.json.return_value = {"products": [{"id": 1}]}
        events = []
        scraper = ShopifyScraper(
            "https://example.com/", event_callback=events.append,
        )

        with patch(
            "disc_golf_pipeline.scrapers.shopify.requests.get",
            side_effect=[failed] * 4 + [recovered],
        ) as get, patch("disc_golf_pipeline.scrapers.shopify.time.sleep"):
            products = scraper.downloadJson(14)

        self.assertEqual([{"id": 1}], products)
        self.assertEqual(5, get.call_count)
        self.assertEqual("retry_success", events[-1]["event_type"])


if __name__ == "__main__":
    unittest.main()
