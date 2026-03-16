import re
import time
from collections import deque
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import requests


class SunKingDiscsScraper:
    BASE_URL = "https://www.sunkingdiscs.com/"
    START_URL = urljoin(BASE_URL, "all-flying-discs/")
    SAME_SECTION_PREFIX = "/all-flying-discs/"
    HREF_PATTERN = re.compile(r'href="([^"]+)"', re.IGNORECASE)
    PRODUCT_BLOCK_HREF_PATTERN = re.compile(
        r'<div class="product-photo">\s*<a\s+href="([^"]+)"\s+class="product-thumbnail">',
        re.IGNORECASE,
    )

    def __init__(self, timeout=30, delay_seconds=0.0):
        self.timeout = timeout
        self.delay_seconds = delay_seconds
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

    def fetch_html(self, url):
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.text

    def is_same_section_url(self, url):
        parsed = urlparse(url)
        return parsed.netloc == urlparse(self.BASE_URL).netloc and parsed.path.startswith(self.SAME_SECTION_PREFIX)

    def normalize_url(self, base_url, href):
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            return None
        normalized = urljoin(base_url, href)
        parsed = urlparse(normalized)
        cleaned = parsed._replace(query="", fragment="").geturl().rstrip("/")
        return cleaned

    def is_asset_url(self, url):
        path = urlparse(url).path.lower()
        return any(path.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".css", ".js", ".ico", ".pdf"))

    def is_product_page(self, html):
        return 'property="og:type" content="product"' in html.lower()

    def extract_same_section_links(self, html, base_url):
        links = []
        seen = set()
        for href in self.HREF_PATTERN.findall(html):
            normalized = self.normalize_url(base_url, href)
            if not normalized or normalized in seen:
                continue
            if not self.is_same_section_url(normalized):
                continue
            if self.is_asset_url(normalized):
                continue
            seen.add(normalized)
            links.append(normalized)
        return links

    def extract_product_links(self, html, base_url):
        links = []
        seen = set()
        for href in self.PRODUCT_BLOCK_HREF_PATTERN.findall(html):
            normalized = self.normalize_url(base_url, href)
            if not normalized or normalized in seen:
                continue
            if not self.is_same_section_url(normalized):
                continue
            seen.add(normalized)
            links.append(normalized)
        return links

    def crawl(self, max_pages=None, max_products=None):
        category_queue = deque([self.START_URL.rstrip("/")])
        product_queue = deque()
        visited = set()
        queued = {self.START_URL.rstrip("/")}
        category_pages = []
        products = []
        product_urls_seen = set()
        skipped_urls = []
        next_kind = "category"

        while category_queue or product_queue:
            if next_kind == "category" and category_queue:
                current_url = category_queue.popleft()
                current_kind = "category"
                next_kind = "product" if product_queue else "category"
            elif next_kind == "product" and product_queue:
                current_url = product_queue.popleft()
                current_kind = "product"
                next_kind = "category" if category_queue else "product"
            elif category_queue:
                current_url = category_queue.popleft()
                current_kind = "category"
            else:
                current_url = product_queue.popleft()
                current_kind = "product"
            if current_url in visited:
                continue
            if max_pages is not None and len(visited) >= max_pages:
                break

            try:
                html = self.fetch_html(current_url)
            except requests.RequestException as exc:
                visited.add(current_url)
                skipped_urls.append(
                    {
                        "url": current_url,
                        "kind": current_kind,
                        "error": str(exc),
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                continue

            visited.add(current_url)

            if current_kind == "product" or self.is_product_page(html):
                products.append(
                    {
                        "url": current_url,
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "html": html,
                    }
                )
                if max_products is not None and len(products) >= max_products:
                    break
            else:
                product_links = self.extract_product_links(html, current_url)
                for product_link in product_links:
                    if product_link not in visited and product_link not in queued and product_link not in product_urls_seen:
                        product_queue.append(product_link)
                        queued.add(product_link)
                        product_urls_seen.add(product_link)

                links = self.extract_same_section_links(html, current_url)
                category_pages.append(
                    {
                        "url": current_url,
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "discovered_link_count": len(links),
                        "discovered_product_count": len(product_links),
                    }
                )
                for link in links:
                    if link not in visited and link not in queued:
                        category_queue.append(link)
                        queued.add(link)

            if self.delay_seconds:
                time.sleep(self.delay_seconds)

        return {
            "source": "sunking-discs",
            "start_url": self.START_URL,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "visited_count": len(visited),
            "category_count": len(category_pages),
            "product_count": len(products),
            "skipped_count": len(skipped_urls),
            "category_pages": category_pages,
            "products": products,
            "skipped_urls": skipped_urls,
        }
