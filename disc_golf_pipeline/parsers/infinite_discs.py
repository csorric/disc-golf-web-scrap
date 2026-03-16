from datetime import datetime, timezone
from html import unescape
from pathlib import Path
import json
import re

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

INFINITE_DISCS_BASE_URL = "https://infinitediscs.com"


def strip_html_tags(text):
    if not text:
        return ""
    plain_text = re.sub(r"<[^>]+>", " ", text)
    plain_text = re.sub(r"\s+", " ", plain_text).strip()
    return unescape(plain_text)


def to_int(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_link(link_value):
    if not link_value:
        return None
    if link_value.startswith("http://") or link_value.startswith("https://"):
        return link_value
    return f"{INFINITE_DISCS_BASE_URL}/{link_value.lstrip('/')}"


def normalize_image(image_value):
    if not image_value:
        return None
    if image_value.startswith("http://") or image_value.startswith("https://"):
        return image_value
    return f"{INFINITE_DISCS_BASE_URL}/{image_value.lstrip('/')}"


def parse_infinite_discs_records(payload):
    timestamp = datetime.now(timezone.utc)
    parsed_rows = []

    for item in payload.get("data", []):
        parsed_rows.append(
            {
                "Id": to_int(item.get("Id")),
                "ManufacturerName": item.get("ManufacturerName"),
                "PlasticName": item.get("PlasticName"),
                "ModelName": item.get("ModelName"),
                "AdditionalInputTitle": item.get("AdditionalInputTitle"),
                "ModelLink": normalize_link(item.get("ModelLink")),
                "StockWeight": to_int(item.get("StockWeight")),
                "AvailableStock": to_int(item.get("AvailableStock")),
                "ColorName": item.get("ColorName"),
                "StockPrice": to_float(item.get("StockPrice")),
                "StockImage": normalize_image(item.get("StockImage")),
                "ModelDescription": strip_html_tags(item.get("ModelDescription")),
                "record_timestamp": timestamp,
            }
        )

    return parsed_rows


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
    rows = parse_infinite_discs_records(payload)
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
    rows = parse_infinite_discs_records(payload)
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
