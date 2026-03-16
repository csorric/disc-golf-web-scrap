import json

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

    def __init__(self, timeout=30):
        self.timeout = timeout

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

        response = requests.post(
            self.SEARCH_URL,
            headers=self.DEFAULT_HEADERS,
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()
