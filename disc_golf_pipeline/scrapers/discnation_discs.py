import re
import time
from datetime import datetime, timezone
from html import unescape
from urllib.parse import urljoin

import requests


class DiscNationDiscsScraper:
    BASE_URL = "https://discnation.com"
    SITEMAP_INDEX_URL = f"{BASE_URL}/xmlsitemap.php"
    URL_PATTERN = re.compile(r"<loc>(.*?)</loc>", re.IGNORECASE)
    MIN_DELAY_SECONDS = 10.0

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

    def extract_urls(self, xml_text):
        return [unescape(url).strip() for url in self.URL_PATTERN.findall(xml_text) if url.strip()]

    def fetch_product_sitemap_urls(self):
        sitemap_index_xml = self.fetch_text(self.SITEMAP_INDEX_URL)
        sitemap_urls = []
        seen = set()

        for url in self.extract_urls(sitemap_index_xml):
            normalized_url = urljoin(self.BASE_URL, url)
            if "xmlsitemap.php?type=products" not in normalized_url:
                continue
            if normalized_url in seen:
                continue
            seen.add(normalized_url)
            sitemap_urls.append(normalized_url)

        return sitemap_urls

    def fetch_product_urls(self):
        product_sitemap_urls = self.fetch_product_sitemap_urls()
        product_urls = []
        seen = set()

        for index, sitemap_url in enumerate(product_sitemap_urls):
            sitemap_xml = self.fetch_text(sitemap_url)
            for url in self.extract_urls(sitemap_xml):
                normalized_url = urljoin(self.BASE_URL, url)
                if normalized_url in seen:
                    continue
                seen.add(normalized_url)
                product_urls.append(normalized_url)

            if index < len(product_sitemap_urls) - 1:
                time.sleep(self.delay_seconds)

        return product_sitemap_urls, product_urls

    def crawl(self, max_products=None):
        product_sitemap_urls, product_urls = self.fetch_product_urls()
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
            "source": "discnation-discs",
            "sitemap_index_url": self.SITEMAP_INDEX_URL,
            "product_sitemap_urls": product_sitemap_urls,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "delay_seconds": self.delay_seconds,
            "product_sitemap_count": len(product_sitemap_urls),
            "product_url_count": len(product_urls),
            "product_count": len(products),
            "skipped_count": len(skipped_urls),
            "products": products,
            "skipped_urls": skipped_urls,
        }
