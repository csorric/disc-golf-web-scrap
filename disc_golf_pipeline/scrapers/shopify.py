import logging
import time

import requests


def _response_body_snippet(response, limit=300):
    try:
        text = response.text or ""
    except Exception:
        return ""
    return " ".join(text.split())[:limit]


def _response_headers_snippet(response):
    header_names = ("content-type", "server", "cf-ray", "retry-after", "x-request-id")
    return {name: response.headers.get(name) for name in header_names if response.headers.get(name)}


class ShopifyScrapeError(RuntimeError):
    def __init__(self, message, page=None, status_code=None, attempts=None):
        super().__init__(message)
        self.page = page
        self.status_code = status_code
        self.attempts = attempts


class ShopifyScraper:
    def __init__(self, baseurl, max_retries=3, retry_delay_seconds=2.0, event_callback=None):
        self.baseurl = baseurl
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.event_callback = event_callback

    def emit_event(self, event_type, **payload):
        if not self.event_callback:
            return
        try:
            self.event_callback({"event_type": event_type, **payload})
        except Exception:
            logging.exception("Failed recording Shopify scrape event: %s", event_type)

    def downloadJson(self, page):
        url = self.baseurl + f"products.json?limit=250&page={page}"
        last_status_code = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, timeout=10)
            except requests.RequestException as exc:
                logging.warning(
                    "Shopify request exception page=%s attempt=%s url=%s error=%s",
                    page,
                    attempt,
                    url,
                    exc,
                )
                self.emit_event(
                    "request_exception",
                    page=page,
                    attempt=attempt,
                    url=url,
                    error=str(exc),
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay_seconds)
                    continue
                raise ShopifyScrapeError(
                    f"Failed to download Shopify page {page} after {self.max_retries} attempts due to request errors.",
                    page=page,
                    attempts=attempt,
                ) from exc

            last_status_code = response.status_code
            if response.status_code == 200:
                data = response.json()
                products = data.get("products") or []
                if products:
                    if attempt > 1:
                        logging.info(
                            "Shopify request recovered page=%s attempt=%s url=%s products=%s",
                            page,
                            attempt,
                            url,
                            len(products),
                        )
                        self.emit_event(
                            "retry_success",
                            page=page,
                            attempt=attempt,
                            url=url,
                            product_count=len(products),
                        )
                    return products

                logging.info("No Shopify products found page=%s url=%s", page, url)
                self.emit_event("page_empty", page=page, url=url)
                return None

            headers_snippet = _response_headers_snippet(response)
            body_snippet = _response_body_snippet(response)
            logging.warning(
                "Shopify request failed status=%s page=%s attempt=%s url=%s headers=%s body=%s",
                response.status_code,
                page,
                attempt,
                url,
                headers_snippet,
                body_snippet,
            )
            self.emit_event(
                "http_error",
                page=page,
                attempt=attempt,
                url=url,
                status_code=response.status_code,
                headers=headers_snippet,
                body=body_snippet,
            )
            if attempt < self.max_retries:
                time.sleep(self.retry_delay_seconds)

        raise ShopifyScrapeError(
            f"Failed to download Shopify page {page} after {self.max_retries} attempts. Last status: {last_status_code}.",
            page=page,
            status_code=last_status_code,
            attempts=self.max_retries,
        )
