from datetime import datetime, timezone
from html import unescape
from pathlib import Path
import json
import re
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

SUN_KING_BASE_URL = "https://www.sunkingdiscs.com"

META_TAG_PATTERN = re.compile(
    r'<meta\s+(?:property|name)="(?P<name>[^"]+)"\s+content="(?P<value>[^"]*)"\s*/?>',
    re.IGNORECASE,
)
TITLE_PATTERN = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL)
SKU_PATTERN = re.compile(
    r'<li class="identifier product-sku">.*?<span class="value">(.*?)</span>',
    re.IGNORECASE | re.DOTALL,
)
WEIGHT_PATTERN = re.compile(
    r'<li class="product-weight">.*?<span>(.*?)</span>',
    re.IGNORECASE | re.DOTALL,
)
STOCK_PATTERN = re.compile(
    r'<span class="stock-level ([^"]*)">(.*?)</span>',
    re.IGNORECASE | re.DOTALL,
)
PLAIN_FIELD_PATTERN = re.compile(
    r"<div><strong>(.*?)</strong></div>\s*<span class=\"plain-field\">(.*?)</span>",
    re.IGNORECASE | re.DOTALL,
)
ATTRIBUTE_BLOCK_PATTERN = re.compile(
    r'<li class="">\s*<label class="title">(.*?)</label>\s*<select[^>]*>(.*?)</select>',
    re.IGNORECASE | re.DOTALL,
)
OPTION_PATTERN = re.compile(r"<option[^>]*value=\"([^\"]+)\"[^>]*>(.*?)</option>", re.IGNORECASE | re.DOTALL)
PRODUCT_ID_PATTERN = re.compile(r'"product_id":(\d+)', re.IGNORECASE)


def strip_html_tags(text):
    if not text:
        return ""
    plain_text = re.sub(r"<[^>]+>", " ", text)
    plain_text = re.sub(r"\s+", " ", plain_text).strip()
    return unescape(plain_text)


def extract_meta_tags(html):
    values = {}
    for match in META_TAG_PATTERN.finditer(html):
        values[match.group("name").lower()] = unescape(match.group("value"))
    return values


def extract_first(pattern, html):
    match = pattern.search(html)
    if not match:
        return None
    return strip_html_tags(match.group(1))


def to_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_numeric_value(text):
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    return match.group(0) if match else None


def normalize_slug(value):
    if not value:
        return None
    return value.replace("-", " ").strip() or None


def titleize_slug(value):
    normalized = normalize_slug(value)
    if not normalized:
        return None
    return " ".join(part.capitalize() for part in normalized.split())


def choose_product_link(crawled_url, canonical_url):
    if canonical_url and canonical_url.startswith("http"):
        return canonical_url
    return crawled_url


def parse_url_path(crawled_url):
    parts = [part for part in urlparse(crawled_url).path.split("/") if part]
    if parts and parts[0] == "all-flying-discs":
        parts = parts[1:]

    manufacturer_slug = parts[0] if len(parts) >= 2 else None
    plastic_slug = parts[1] if len(parts) >= 3 else None
    product_slug = parts[-1] if parts else None
    category_path = " / ".join(normalize_slug(part) for part in parts[:-1]) if len(parts) > 1 else None

    return {
        "manufacturer_slug": manufacturer_slug,
        "plastic_slug": plastic_slug,
        "product_slug": product_slug,
        "category_path": category_path,
    }


def extract_stock_status(html):
    match = STOCK_PATTERN.search(html)
    if not match:
        return None, None
    status_class = match.group(1).lower()
    status_text = strip_html_tags(match.group(2))
    is_out_of_stock = "out-of-stock" in status_class or "out of stock" in status_text.lower()
    return status_text, is_out_of_stock


def extract_plain_fields(html):
    fields = {}
    for label, value in PLAIN_FIELD_PATTERN.findall(html):
        fields[strip_html_tags(label)] = strip_html_tags(value)
    return fields


def extract_attribute_options(html):
    attributes = []
    for label, options_html in ATTRIBUTE_BLOCK_PATTERN.findall(html):
        options = []
        for option_value, option_label in OPTION_PATTERN.findall(options_html):
            options.append(
                {
                    "option_id": option_value,
                    "option_label": strip_html_tags(option_label),
                }
            )
        attributes.append(
            {
                "label": strip_html_tags(label),
                "options": options,
            }
        )
    return attributes


def option_rows_from_attributes(attributes):
    if not attributes:
        return []

    primary_attribute = attributes[0]
    rows = []
    for option in primary_attribute["options"]:
        rows.append(
            {
                "VariantLabel": primary_attribute["label"],
                "VariantOptionId": option["option_id"],
                "VariantOptionLabel": option["option_label"],
            }
        )
    return rows


def extract_variant_weight_grams(text):
    if not text:
        return None
    match = re.search(r"(\d+(?:-\d+)?\+?)\s*g", text.lower())
    return match.group(1) if match else None


def parse_product(item):
    html = item.get("html") or ""
    crawled_url = item.get("url")
    if not html or not crawled_url:
        return []

    meta = extract_meta_tags(html)
    url_bits = parse_url_path(crawled_url)
    title = extract_first(TITLE_PATTERN, html)
    sku = extract_first(SKU_PATTERN, html) or meta.get("product:retailer_item_id")
    weight_text = extract_first(WEIGHT_PATTERN, html)
    stock_text, is_out_of_stock = extract_stock_status(html)
    plain_fields = extract_plain_fields(html)
    attributes = extract_attribute_options(html)
    option_rows = option_rows_from_attributes(attributes)
    product_id_match = PRODUCT_ID_PATTERN.search(html)
    timestamp = datetime.now(timezone.utc)
    canonical_url = meta.get("og:url")
    product_link = choose_product_link(crawled_url, canonical_url)

    base_record = {
        "ProductId": int(product_id_match.group(1)) if product_id_match else None,
        "ProductName": title or meta.get("og:title"),
        "Sku": sku,
        "ProductLink": product_link,
        "ProductUrl": crawled_url,
        "CanonicalUrl": canonical_url,
        "ImageUrl": meta.get("og:image"),
        "Description": meta.get("og:description") or meta.get("description"),
        "Availability": meta.get("product:availability"),
        "StockStatusText": stock_text,
        "IsOutOfStock": is_out_of_stock,
        "ListPrice": to_float(meta.get("product:price:amount")),
        "SalePrice": to_float(meta.get("product:sale_price:amount")),
        "Currency": meta.get("product:price:currency"),
        "ProductWeightValue": to_float(meta.get("product:weight:value")),
        "ProductWeightUnits": meta.get("product:weight:units"),
        "ProductWeightText": weight_text,
        "ManufacturerSlug": url_bits["manufacturer_slug"],
        "ManufacturerName": titleize_slug(url_bits["manufacturer_slug"]),
        "PlasticSlug": url_bits["plastic_slug"],
        "PlasticName": titleize_slug(url_bits["plastic_slug"]),
        "ProductSlug": url_bits["product_slug"],
        "CategoryPath": url_bits["category_path"],
        "CatalogWeightSummary": plain_fields.get("Catalog Number/Weight (See Pics)") or plain_fields.get("Catalog Number/Disc Weight"),
        "record_timestamp": timestamp,
    }

    if not option_rows:
        return [{**base_record, "VariantLabel": None, "VariantOptionId": None, "VariantOptionLabel": None, "VariantWeightGrams": None}]

    rows = []
    for option_row in option_rows:
        rows.append(
            {
                **base_record,
                **option_row,
                "VariantWeightGrams": extract_variant_weight_grams(option_row["VariantOptionLabel"]),
            }
        )
    return rows


def parse_sunking_discs_payload(payload):
    rows = []
    for item in payload.get("products", []):
        rows.extend(parse_product(item))
    return rows


def records_to_parquet_bytes(data):
    if not data:
        return None
    table = pa.Table.from_pylist(data)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue().to_pybytes()


def write_local_parquet(rows, destination_file):
    destination_path = Path(destination_file)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_bytes = records_to_parquet_bytes(rows)
    if parquet_bytes:
        destination_path.write_bytes(parquet_bytes)
    return destination_path


def write_gcs_parquet(rows, bucket_name, blob_name):
    parquet_bytes = records_to_parquet_bytes(rows)
    if not parquet_bytes:
        return None
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")
    return blob_name


def load_json_file(file_path):
    return json.loads(Path(file_path).read_text(encoding="utf-8"))


def load_json_data(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return json.loads(blob.download_as_string())


def parse_local_json_file(source_file, output_dir):
    source_path = Path(source_file)
    payload = load_json_file(source_path)
    rows = parse_sunking_discs_payload(payload)
    destination_file = Path(output_dir) / f"{source_path.stem}.parquet"
    written_file = write_local_parquet(rows, destination_file)
    source_path.unlink()
    print(f"Deleted original file {source_path}")
    return written_file


def parse_local_directory(input_dir, output_dir):
    input_path = Path(input_dir)
    written_files = []
    for json_file in sorted(input_path.glob("*.json")):
        written_files.append(parse_local_json_file(json_file, output_dir))
    return written_files


def list_gcs_json_objects(bucket_name, prefix=""):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs if blob.name.lower().endswith(".json")]


def parse_gcs_json_file(source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_name):
    payload = load_json_data(source_bucket_name, source_blob_name)
    rows = parse_sunking_discs_payload(payload)
    written_blob = write_gcs_parquet(rows, destination_bucket_name, destination_blob_name)
    client = storage.Client()
    source_bucket = client.bucket(source_bucket_name)
    source_bucket.blob(source_blob_name).delete()
    print(f"Deleted original file {source_blob_name} from bucket {source_bucket_name}.")
    return written_blob


def parse_gcs_prefix(source_bucket_name, source_prefix, destination_bucket_name, destination_prefix=""):
    written_files = []
    for blob_name in sorted(list_gcs_json_objects(source_bucket_name, prefix=source_prefix)):
        base_name = Path(blob_name).stem
        destination_blob_name = f"{destination_prefix.rstrip('/')}/{base_name}.parquet" if destination_prefix else f"{base_name}.parquet"
        written_blob = parse_gcs_json_file(source_bucket_name, blob_name, destination_bucket_name, destination_blob_name)
        if written_blob:
            written_files.append(written_blob)
    return written_files
