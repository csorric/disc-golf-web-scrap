import json
import logging
import time

import requests


class InfiniteDiscsScraper:
    SEARCH_URL = "https://infinitediscs.com/Search/SearchedData"

    DEFAULT_HEADERS = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "content-type": "application/json; charset=UTF-8",
        "origin": "https://infinitediscs.com",
        "referer": "https://infinitediscs.com/advanced-search-results",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "x-requested-with": "XMLHttpRequest",
    }

    DEFAULT_FILTER = {
        "DiscBrand": None,
        "DiscType": None,
        "DiscStability": None,
        "DiscColor": None,
        "DiscWeight": None,
        "DiscWeight2": None,
        "PlasticGrade": None,
        "DiscPlastic": None,
        "DiscDiameter": None,
        "DiscHeight": None,
        "DiscDepth": None,
        "DiscWidth": None,
        "DiscSpeed": None,
        "DiscGlide": None,
        "DiscTurn": None,
        "DiscFade": None,
        "PlasticGlow": None,
        "DiscExtra": None,
        "SkillLevel": None,
        "DiscStamp": None,
        "ResultType": "I",
    }

    def __init__(self, timeout=30, max_retries=5, retry_delay_seconds=2.0, backoff_multiplier=2.0):
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.backoff_multiplier = backoff_multiplier

    def download_json(self, start, length, draw):
        payload = {
            "Draw": draw,
            "Start": start,
            "Length": length,
            "Type": "Advanced",
            "Filter": json.dumps(self.DEFAULT_FILTER),
            "DiscBrand": "",
            "DiscModel": "",
            "Weight1": "",
            "Weight2": "",
        }

        delay_seconds = self.retry_delay_seconds
        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.post(
                    self.SEARCH_URL,
                    headers=self.DEFAULT_HEADERS,
                    json=payload,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as exc:
                last_error = exc

                should_retry = False
                if isinstance(exc, requests.exceptions.HTTPError):
                    status_code = exc.response.status_code if exc.response is not None else None
                    should_retry = status_code is not None and status_code >= 500
                else:
                    should_retry = True

                if not should_retry or attempt == self.max_retries:
                    raise

                logging.warning(
                    "Infinite Discs request failed for start=%s length=%s draw=%s on attempt %s/%s: %s. Retrying in %.1fs",
                    start,
                    length,
                    draw,
                    attempt,
                    self.max_retries,
                    exc,
                    delay_seconds,
                )
                time.sleep(delay_seconds)
                delay_seconds *= self.backoff_multiplier

        raise last_error
