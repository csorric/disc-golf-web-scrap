import time

import requests


class ShopifyScrapeError(RuntimeError):
    pass


class ShopifyScraper:
    def __init__(self, baseurl, max_retries=3, retry_delay_seconds=2.0):
        self.baseurl = baseurl
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds

    def downloadJson(self, page):
        url = self.baseurl + f"products.json?limit=250&page={page}"
        last_status_code = None

        for attempt in range(1, self.max_retries + 1):
            response = requests.get(url, timeout=10)
            last_status_code = response.status_code
            if response.status_code == 200:
                data = response.json()
                products = data.get("products") or []
                if products:
                    return products

                print(f"No products found on page: {page}")
                return None

            print(f"Error downloading page: {response.status_code} {page} attempt {attempt}")
            if attempt < self.max_retries:
                time.sleep(self.retry_delay_seconds)

        raise ShopifyScrapeError(
            f"Failed to download Shopify page {page} after {self.max_retries} attempts. Last status: {last_status_code}."
        )
