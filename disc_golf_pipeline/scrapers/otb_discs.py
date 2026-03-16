import re
import time
from datetime import datetime, timezone

import requests


class OTBDiscsScraper:
    BASE_URL = "https://otbdiscs.com"
    PRODUCT_SITEMAP_URL = f"{BASE_URL}/product-sitemap.xml"
    PRODUCT_URL_PATTERN = re.compile(r"<loc>(.*?)</loc>", re.IGNORECASE)
    MIN_DELAY_SECONDS = 5.0

    def __init__(self, timeout=30, delay_seconds=None):
        self.timeout = timeout
        requested_delay = self.MIN_DELAY_SECONDS if delay_seconds is None else float(delay_seconds)
        self.delay_seconds = max(self.MIN_DELAY_SECONDS, requested_delay)
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(
            {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "user-agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
            }
        )

    def fetch_text(self, url):
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.text

    def fetch_product_urls(self):
        xml_text = self.fetch_text(self.PRODUCT_SITEMAP_URL)
        urls = []
        seen = set()
        for url in self.PRODUCT_URL_PATTERN.findall(xml_text):
            if "/product/" not in url:
                continue
            if url in seen:
                continue
            seen.add(url)
            urls.append(url)
        return urls

    def crawl(self, max_products=None):
        product_urls = self.fetch_product_urls()
        if max_products is not None:
            product_urls = product_urls[:max_products]

        products = []
        skipped_urls = []

        for index, product_url in enumerate(product_urls):
            try:
                html = self.fetch_text(product_url)
                products.append(
                    {
                        "url": product_url,
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "html": html,
                    }
                )
            except requests.RequestException as exc:
                skipped_urls.append(
                    {
                        "url": product_url,
                        "error": str(exc),
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                    }
                )

            if index < len(product_urls) - 1:
                time.sleep(self.delay_seconds)

        return {
            "source": "otb-discs",
            "sitemap_url": self.PRODUCT_SITEMAP_URL,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "delay_seconds": self.delay_seconds,
            "product_url_count": len(product_urls),
            "product_count": len(products),
            "skipped_count": len(skipped_urls),
            "products": products,
            "skipped_urls": skipped_urls,
        }
