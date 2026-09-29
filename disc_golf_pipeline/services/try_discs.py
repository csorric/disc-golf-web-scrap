"""Read-only Try Discs catalog client and conservative model matching."""

import os
import re
import unicodedata
from collections import defaultdict

import requests


API_URL = "https://api.trydiscs.com/v1/discs"
PAGE_LIMIT = 2000
ATTRIBUTION = "Disc data by Try Discs (https://trydiscs.com)"

# Canonical manufacturers in this pipeline that use a different catalog label.
BRAND_ALIASES = {
    "innova champion discs": "innova",
    "mvp disc sports": "mvp",
    "axiom discs": "axiom",
    "prodigy disc": "prodigy",
    "disc golf association": "dga",
    "streamline discs": "streamline",
    "gateway disc sports": "gateway",
    "westside golf discs": "westside",
    "yikun discs": "yikun",
    "millennium golf discs": "millennium",
}


def match_key(value):
    normalized = unicodedata.normalize("NFKD", value or "").casefold()
    return re.sub(r"[^a-z0-9]+", " ", normalized).strip()


def flight_numbers(disc):
    fields = ("speed", "glide", "turn", "fade")
    if any(disc.get(field) is None or isinstance(disc.get(field), bool) for field in fields):
        return None
    try:
        numbers = tuple(float(disc[field]) for field in fields)
    except (TypeError, ValueError, OverflowError):
        return None
    if all(low <= value <= high for value, (low, high) in zip(
        numbers, ((1, 15), (0, 7), (-5, 2), (0, 5))
    )):
        return numbers
    return None


def fetch_catalog(api_key=None, session=None):
    """Fetch all pages in memory; never log or persist the API key."""
    resolved_key = (api_key or os.getenv("TRY_DISCS_API_KEY") or "").strip()
    if not resolved_key:
        raise ValueError("TRY_DISCS_API_KEY is not configured")

    own_session = session is None
    client = session or requests.Session()
    try:
        catalog = []
        meta = {}
        offset = 0
        while True:
            response = client.get(
                API_URL,
                params={"limit": PAGE_LIMIT, "offset": offset},
                headers={"X-API-Key": resolved_key},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            page = payload.get("data")
            meta = payload.get("meta") or {}
            if not isinstance(page, list) or not isinstance(meta.get("total"), int):
                raise ValueError("Try Discs returned an unexpected catalog response")
            catalog.extend(page)
            if len(catalog) >= meta["total"]:
                break
            if not page:
                raise ValueError("Try Discs pagination ended before the reported total")
            offset += len(page)
        return catalog, meta
    finally:
        if own_session:
            client.close()


def build_catalog_index(catalog, include_incomplete=False):
    index = defaultdict(list)
    for disc in catalog:
        if not include_incomplete and flight_numbers(disc) is None:
            continue
        index[(match_key(disc.get("brand")), match_key(disc.get("name")))].append(disc)
    return index


def match_model(manufacturer, model, catalog_index):
    """Return a unique manufacturer-and-model record from the supplied index."""
    brand_key = match_key(manufacturer)
    model_key = match_key(model)
    if not brand_key or not model_key:
        return None, None
    mapped_brand = BRAND_ALIASES.get(brand_key, brand_key)
    matches = catalog_index.get((mapped_brand, model_key), [])
    if len(matches) != 1:
        return None, None
    return matches[0], "brand_alias" if mapped_brand != brand_key else "exact"
