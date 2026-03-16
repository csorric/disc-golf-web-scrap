import argparse
import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from disc_golf_pipeline.parsers.shopify_json import DataParser

DEFAULT_PARSED_BUCKET = "disc-golf-parsed-files"


def get_store_reference_from_name(name):
    filename_without_extension = name.rsplit(".", 1)[0]
    return re.sub(r"_\d+_\d+$", "", filename_without_extension)


def build_parsed_output_names(base_name):
    return (
        f"{base_name}_products.parquet",
        f"{base_name}_product_info.parquet",
    )


def records_to_parquet_bytes(data):
    if not data:
        return None

    table = pa.Table.from_pylist(data)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue().to_pybytes()


def load_json_data(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return json.loads(blob.download_as_string())


def load_json_file(file_path):
    return json.loads(Path(file_path).read_text(encoding="utf-8"))


def parse_json_records(json_data, store_reference, store_url=None):
    parser = DataParser(store_reference, store_url=store_url)
    return parser.parse_json(json_data)


def write_local_parquet(products, product_info, destination_base):
    destination_base = Path(destination_base)
    destination_base.parent.mkdir(parents=True, exist_ok=True)

    products_name, product_info_name = build_parsed_output_names(destination_base.name)
    written_files = []

    for data, file_name in (
        (products, products_name),
        (product_info, product_info_name),
    ):
        parquet_bytes = records_to_parquet_bytes(data)
        if not parquet_bytes:
            continue
        file_path = destination_base.parent / file_name
        file_path.write_bytes(parquet_bytes)
        written_files.append(file_path)

    return written_files


def write_gcs_parquet(products, product_info, destination_blob_base, destination_bucket):
    products_name, product_info_name = build_parsed_output_names(destination_blob_base)

    for data, file_name in (
        (products, products_name),
        (product_info, product_info_name),
    ):
        parquet_bytes = records_to_parquet_bytes(data)
        if not parquet_bytes:
            continue
        blob = destination_bucket.blob(file_name)
        blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")
        print(f"Parquet file {file_name} created in bucket {destination_bucket.name}.")


def list_gcs_json_objects(bucket_name, prefix=""):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs if blob.name.lower().endswith(".json")]


def json_to_parquet(source_bucket_name, source_blob_name, destination_blob_base, destination_bucket_name, store_reference):
    client = storage.Client()
    source_bucket = client.bucket(source_bucket_name)
    destination_bucket = client.bucket(destination_bucket_name)
    json_data = load_json_data(source_bucket_name, source_blob_name)
    products, product_info = parse_json_records(json_data, store_reference)

    write_gcs_parquet(products, product_info, destination_blob_base, destination_bucket)

    source_blob = source_bucket.blob(source_blob_name)
    source_blob.delete()
    print(f"Deleted original file {source_blob_name} from bucket {source_bucket_name}.")


def parse_gcs_json_file(source_bucket_name, source_blob_name, destination_bucket_name, destination_blob_base=None):
    client = storage.Client()
    source_bucket = client.bucket(source_bucket_name)
    destination_bucket = client.bucket(destination_bucket_name)
    json_data = load_json_data(source_bucket_name, source_blob_name)
    store_reference = get_store_reference_from_name(Path(source_blob_name).name)
    destination_blob_base = destination_blob_base or source_blob_name.replace(".json", "")
    products, product_info = parse_json_records(json_data, store_reference)
    write_gcs_parquet(products, product_info, destination_blob_base, destination_bucket)
    source_bucket.blob(source_blob_name).delete()
    print(f"Deleted original file {source_blob_name} from bucket {source_bucket_name}.")


def parse_gcs_prefix(source_bucket_name, source_prefix, destination_bucket_name, destination_prefix=""):
    written_targets = []
    for blob_name in sorted(list_gcs_json_objects(source_bucket_name, prefix=source_prefix)):
        base_name = Path(blob_name).stem
        destination_blob_base = f"{destination_prefix.rstrip('/')}/{base_name}" if destination_prefix else base_name
        parse_gcs_json_file(source_bucket_name, blob_name, destination_bucket_name, destination_blob_base=destination_blob_base)
        written_targets.extend(build_parsed_output_names(destination_blob_base))
    return written_targets


def parse_local_json_file(source_file, output_dir, store_url=None):
    source_path = Path(source_file)
    json_data = load_json_file(source_path)
    store_reference = get_store_reference_from_name(source_path.name)
    output_base = Path(output_dir) / source_path.stem
    products, product_info = parse_json_records(json_data, store_reference, store_url=store_url)
    written_files = write_local_parquet(products, product_info, output_base)
    source_path.unlink()
    print(f"Deleted original file {source_path}")
    return written_files


def parse_local_directory(input_dir, output_dir):
    input_path = Path(input_dir)
    written_files = []
    for json_file in sorted(input_path.glob("*.json")):
        written_files.extend(parse_local_json_file(json_file, output_dir))
    return written_files


def hello_gcs(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    blob_name = data["name"]
    destination_blob_base = blob_name.replace(".json", "")
    store_reference = get_store_reference_from_name(blob_name)

    json_to_parquet(
        bucket_name,
        blob_name,
        destination_blob_base,
        DEFAULT_PARSED_BUCKET,
        store_reference,
    )


try:
    import functions_framework
except Exception:
    functions_framework = None
else:
    hello_gcs = functions_framework.cloud_event(hello_gcs)


def main():
    parser = argparse.ArgumentParser(description="Parse raw Shopify JSON into Parquet files.")
    parser.add_argument("--input", required=True, help="JSON file or directory to parse.")
    parser.add_argument("--output", required=True, help="Directory for parsed Parquet output.")
    parser.add_argument("--store-url", help="Optional explicit store URL for single-file parsing.")
    args = parser.parse_args()

    input_path = Path(args.input)
    if input_path.is_dir():
        written_files = parse_local_directory(input_path, args.output)
    else:
        written_files = parse_local_json_file(input_path, args.output, store_url=args.store_url)

    for file_path in written_files:
        print(f"Wrote {file_path}")


if __name__ == "__main__":
    main()
