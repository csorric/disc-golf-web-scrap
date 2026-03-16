from datetime import datetime, timezone
from html import unescape
from pathlib import Path
import json
import re

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

TITLE_PATTERN = re.compile(r'<h1 class="product_title entry-title">(.*?)</h1>', re.IGNORECASE | re.DOTALL)
SKU_PATTERN = re.compile(r'<span class="sku">\s*(.*?)\s*</span>', re.IGNORECASE | re.DOTALL)
POSTED_IN_PATTERN = re.compile(r'<span class="posted_in">(.*?)</span>', re.IGNORECASE | re.DOTALL)
CATEGORY_LINK_PATTERN = re.compile(r'<a href="([^"]+)"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
META_TAG_PATTERN = re.compile(
    r'<meta\s+(?:property|name)="(?P<name>[^"]+)"\s+content="(?P<value>[^"]*)"\s*/?>',
    re.IGNORECASE,
)
DESCRIPTION_PARAGRAPH_PATTERN = re.compile(r'<p>(.*?)</p>', re.IGNORECASE | re.DOTALL)
FLIGHT_TEXT_PATTERN = re.compile(
    r"Speed:\s*([-\d.]+)\s*,?\s*Glide:\s*([-\d.]+)\s*,?\s*Turn:\s*([-\d.]+)\s*,?\s*Fade:\s*([-\d.]+)",
    re.IGNORECASE,
)
FLIGHT_TABLE_PATTERN = re.compile(
    r'<div class="woocommerce-product-gallery"[^>]*><table[^>]*class="round-table"[^>]*><tbody><tr>(.*?)</tr>',
    re.IGNORECASE | re.DOTALL,
)
FLIGHT_TABLE_CELL_PATTERN = re.compile(r"<td[^>]*>.*?<br>\s*([^<]+)</td>", re.IGNORECASE | re.DOTALL)
ROW_TABLE_PATTERN = re.compile(
    r'<table[^>]*class="[^"]*\bvartable\b[^"]*"[^>]*>.*?<tbody>(.*?)</tbody>.*?</table>',
    re.IGNORECASE | re.DOTALL,
)
ROW_PATTERN = re.compile(r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
THUMBNAIL_PATTERN = re.compile(r'<a href="([^"]+)"[^>]*class="[^"]*\bthumb\b', re.IGNORECASE)
OPTION_CELL_PATTERN = re.compile(
    r'<td class="optionscol\s+attribute_pa_([^"]+)"[^>]*data-label="([^"]+)">\s*(.*?)</td>',
    re.IGNORECASE | re.DOTALL,
)
PRICE_CELL_PATTERN = re.compile(
    r'<td class="pricecol [^"]*"[^>]*data-price="([^"]+)"',
    re.IGNORECASE | re.DOTALL,
)
STOCK_CELL_PATTERN = re.compile(r'<td class="stockcol [^"]*"[^>]*><span class="([^"]+)">(.*?)</span>', re.IGNORECASE | re.DOTALL)
FORM_PRODUCT_ID_PATTERN = re.compile(r'name="product_id"\s+value="(\d+)"', re.IGNORECASE)
FORM_VARIATION_ID_PATTERN = re.compile(r'name="variation_id"\s+value="(\d+)"', re.IGNORECASE)
ATTRIBUTE_JSON_PATTERN = re.compile(
    r'name="form_vartable_attribute_json"\s+value=\'([^\']+)\'',
    re.IGNORECASE | re.DOTALL,
)
PAGE_PRODUCT_ID_PATTERN = re.compile(r'mlw_pid%3D(\d+)', re.IGNORECASE)

OTB_PARQUET_SCHEMA = pa.schema(
    [
        ("ProductId", pa.int64()),
        ("VariationId", pa.int64()),
        ("ProductName", pa.string()),
        ("Sku", pa.string()),
        ("ProductLink", pa.string()),
        ("ProductUrl", pa.string()),
        ("CanonicalUrl", pa.string()),
        ("ImageUrl", pa.string()),
        ("Description", pa.string()),
        ("ManufacturerName", pa.string()),
        ("DiscTypeName", pa.string()),
        ("ColorName", pa.string()),
        ("PlasticName", pa.string()),
        ("StampFoil", pa.string()),
        ("Weight", pa.string()),
        ("ScaledWeight", pa.string()),
        ("Flatness", pa.string()),
        ("Stiffness", pa.string()),
        ("StockPrice", pa.float64()),
        ("Currency", pa.string()),
        ("StockText", pa.string()),
        ("AvailableStock", pa.int64()),
        ("IsOutOfStock", pa.bool_()),
        ("VariantAttributesJson", pa.string()),
        ("Speed", pa.float64()),
        ("Glide", pa.float64()),
        ("Turn", pa.float64()),
        ("Fade", pa.float64()),
        ("record_timestamp", pa.timestamp("us", tz="UTC")),
    ]
)


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


def to_int(value):
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


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


def extract_available_stock(stock_text):
    if not stock_text:
        return None
    lower_value = stock_text.lower()
    if "out of stock" in lower_value or "sold out" in lower_value:
        return 0
    number = extract_numeric_value(stock_text)
    return to_int(number) if number is not None else None


def parse_categories(html):
    posted_in_match = POSTED_IN_PATTERN.search(html)
    if not posted_in_match:
        return []

    categories = []
    for url, label in CATEGORY_LINK_PATTERN.findall(posted_in_match.group(1)):
        categories.append({"url": unescape(url), "label": strip_html_tags(label)})
    return categories


def extract_manufacturer_name(categories):
    for category in categories:
        if "/product-category/manufacturers/" in category["url"]:
            return category["label"]
    return None


def extract_disc_type_name(categories):
    for category in categories:
        if "/product-category/products/" in category["url"]:
            return category["label"]
    return None


def extract_description(html):
    paragraphs = [strip_html_tags(value) for value in DESCRIPTION_PARAGRAPH_PATTERN.findall(html)]
    cleaned = []
    for paragraph in paragraphs:
        if not paragraph:
            continue
        if paragraph.startswith("Speed:") or paragraph.startswith("Flight Numbers"):
            continue
        if paragraph == "Currently Sold Out!":
            continue
        cleaned.append(paragraph)
    return "\n".join(cleaned[:2]) if cleaned else None


def extract_flight_numbers(html):
    flight_match = FLIGHT_TEXT_PATTERN.search(html)
    if flight_match:
        speed, glide, turn, fade = flight_match.groups()
        return {
            "Speed": to_float(speed),
            "Glide": to_float(glide),
            "Turn": to_float(turn),
            "Fade": to_float(fade),
        }

    table_match = FLIGHT_TABLE_PATTERN.search(html)
    if not table_match:
        return {"Speed": None, "Glide": None, "Turn": None, "Fade": None}

    values = [to_float(extract_numeric_value(strip_html_tags(cell))) for cell in FLIGHT_TABLE_CELL_PATTERN.findall(table_match.group(1))]
    while len(values) < 4:
        values.append(None)
    return {"Speed": values[0], "Glide": values[1], "Turn": values[2], "Fade": values[3]}


def parse_attribute_json(row_html):
    match = ATTRIBUTE_JSON_PATTERN.search(row_html)
    if not match:
        return None
    return unescape(match.group(1))


def parse_table_rows(product_html):
    table_match = ROW_TABLE_PATTERN.search(product_html)
    if not table_match:
        return []

    rows = []
    for row_html in ROW_PATTERN.findall(table_match.group(1)):
        option_values = {}
        for slug, label, value in OPTION_CELL_PATTERN.findall(row_html):
            option_values[label] = strip_html_tags(value)

        price_match = PRICE_CELL_PATTERN.search(row_html)
        stock_match = STOCK_CELL_PATTERN.search(row_html)
        thumb_match = THUMBNAIL_PATTERN.search(row_html)
        product_id_match = FORM_PRODUCT_ID_PATTERN.search(row_html)
        variation_id_match = FORM_VARIATION_ID_PATTERN.search(row_html)
        stock_text = strip_html_tags(stock_match.group(2)) if stock_match else None

        rows.append(
            {
                "ProductId": to_int(product_id_match.group(1)) if product_id_match else None,
                "VariationId": to_int(variation_id_match.group(1)) if variation_id_match else None,
                "ImageUrl": thumb_match.group(1) if thumb_match else None,
                "StockText": stock_text,
                "AvailableStock": extract_available_stock(stock_text),
                "StockClass": stock_match.group(1) if stock_match else None,
                "StockPrice": to_float(price_match.group(1)) if price_match else None,
                "OptionValues": option_values,
                "OptionValuesJson": parse_attribute_json(row_html),
            }
        )
    return rows


def build_fallback_row(product_html, meta):
    product_id_match = PAGE_PRODUCT_ID_PATTERN.search(product_html)
    return [
        {
            "ProductId": to_int(product_id_match.group(1)) if product_id_match else None,
            "VariationId": None,
            "ImageUrl": meta.get("og:image"),
            "StockText": "Currently Sold Out!" if "Currently Sold Out!" in product_html else None,
            "AvailableStock": 0 if "Currently Sold Out!" in product_html else None,
            "StockClass": "sold-out" if "Currently Sold Out!" in product_html else None,
            "StockPrice": to_float(meta.get("product:price:amount")),
            "OptionValues": {},
            "OptionValuesJson": None,
        }
    ]


def parse_product(item):
    product_html = item.get("html") or ""
    product_url = item.get("url")
    if not product_html or not product_url:
        return []

    meta = extract_meta_tags(product_html)
    categories = parse_categories(product_html)
    manufacturer_name = extract_manufacturer_name(categories)
    disc_type_name = extract_disc_type_name(categories)
    flight_numbers = extract_flight_numbers(product_html)
    title = extract_first(TITLE_PATTERN, product_html)
    sku = extract_first(SKU_PATTERN, product_html)
    description = extract_description(product_html) or meta.get("og:description") or meta.get("description")
    rows = parse_table_rows(product_html) or build_fallback_row(product_html, meta)
    timestamp = datetime.now(timezone.utc)

    parsed_rows = []
    for row in rows:
        option_values = row["OptionValues"]
        parsed_rows.append(
            {
                "ProductId": row["ProductId"],
                "VariationId": row["VariationId"],
                "ProductName": title,
                "Sku": sku,
                "ProductLink": meta.get("og:url") or product_url,
                "ProductUrl": product_url,
                "CanonicalUrl": meta.get("og:url"),
                "ImageUrl": row["ImageUrl"] or meta.get("og:image"),
                "Description": description,
                "ManufacturerName": manufacturer_name,
                "DiscTypeName": disc_type_name,
                "ColorName": option_values.get("Color"),
                "PlasticName": option_values.get("Plastic") or option_values.get("Plastic-UK"),
                "StampFoil": option_values.get("Stamp Foil"),
                "Weight": option_values.get("Weight"),
                "ScaledWeight": option_values.get("Scaled Weight"),
                "Flatness": option_values.get("Flatness"),
                "Stiffness": option_values.get("Stiffness"),
                "StockPrice": row["StockPrice"],
                "Currency": meta.get("product:price:currency") or "USD",
                "StockText": row["StockText"],
                "AvailableStock": row["AvailableStock"],
                "IsOutOfStock": row["AvailableStock"] == 0 if row["AvailableStock"] is not None else None,
                "VariantAttributesJson": row["OptionValuesJson"],
                "Speed": flight_numbers["Speed"],
                "Glide": flight_numbers["Glide"],
                "Turn": flight_numbers["Turn"],
                "Fade": flight_numbers["Fade"],
                "record_timestamp": timestamp,
            }
        )
    return parsed_rows


def parse_otb_discs_payload(payload):
    rows = []
    for item in payload.get("products", []):
        rows.extend(parse_product(item))
    return rows


def records_to_parquet_bytes(data):
    if not data:
        return None
    table = pa.Table.from_pylist(data, schema=OTB_PARQUET_SCHEMA)
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
    rows = parse_otb_discs_payload(payload)
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
    rows = parse_otb_discs_payload(payload)
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
