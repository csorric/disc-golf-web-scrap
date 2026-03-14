import json
import re
from html import unescape
from urllib.parse import urlparse


class DataParser:
    STORE_URL_MAP = {
        "discgolfstar": "https://discgolfstar.com/",
        "discgod": "https://discgod.com/",
        "discstore": "https://discstore.com/",
        "dzdiscs": "https://dzdiscs.com/",
        "fadegear": "https://fadegear.com/",
        "foundationdiscs": "https://foundationdiscs.com/",
        "jerseydiscs": "https://jerseydiscs.com/",
        "maverickdiscgolf": "https://maverickdiscgolf.com/",
        "shopledgestone": "https://shopledgestone.com/",
        "skybreed-discs": "https://skybreed-discs.com/",
        "throwshop": "https://throwshop.us/",
        "titandiscgolf": "https://titandiscgolf.com/",
        "unitedsport": "https://unitedsport.ca/",
    }

    def __init__(self, store_reference, store_url=None):
        self.store_reference = store_reference.replace("raw-data/", "")
        self.store_url = self._normalize_store_url(store_url or self._infer_store_url(self.store_reference))
        self.store_host = urlparse(self.store_url).netloc if self.store_url else self.store_reference

    def strip_html_tags(self, text):
        if not text:
            return ""

        plain_text = re.sub(r"<[^>]+>", " ", text)
        plain_text = re.sub(r"\s+", " ", plain_text).strip()
        return unescape(plain_text)

    def _normalize_store_url(self, store_url):
        if not store_url:
            return None
        if not store_url.startswith(("http://", "https://")):
            store_url = "https://" + store_url
        return store_url.rstrip("/") + "/"

    def _infer_store_url(self, store_reference):
        if not store_reference:
            return None

        decoded_reference = store_reference.replace("__", ".")
        if "." in decoded_reference:
            return f"https://{decoded_reference}/"

        return self.STORE_URL_MAP.get(store_reference)

    def _build_product_link(self, handle):
        if not handle or not self.store_url:
            return None
        return f"{self.store_url}products/{handle}"

    def parse_json(self, jsondata):
        products = []
        product_info = []

        for product in jsondata:
            product_info.append(
                {
                    "MainProductId": product.get("id"),
                    "Title": product.get("title"),
                    "Handle": product.get("handle"),
                    "Store": self.store_host,
                    "ProductLink": self._build_product_link(product.get("handle")),
                    "BodyHtml": self.strip_html_tags(product.get("body_html")),
                    "PublishedAt": product.get("published_at"),
                    "CreatedAt": product.get("created_at"),
                    "Vendor": product.get("vendor"),
                    "ProductType": product.get("product_type"),
                    "Tags": json.dumps(product.get("tags", [])),
                }
            )

            for variant in product.get("variants", []):
                featured_image = variant.get("featured_image") or {}
                products.append(
                    {
                        "VariantId": variant.get("id"),
                        "VariantTitle": variant.get("title"),
                        "VarFeaturedImage": featured_image.get("src"),
                        "VariantAvailable": variant.get("available"),
                        "VariantPrice": variant.get("price"),
                        "VariantCompareAtPrice": variant.get("compare_at_price"),
                        "VariantGrams": variant.get("grams"),
                        "MainProductId": product.get("id"),
                        "VariantCreatedAt": variant.get("created_at"),
                        "VariantUpdatedAt": variant.get("updated_at"),
                    }
                )

        return products, product_info

    def parseJson(self, jsondata):
        return self.parse_json(jsondata)
