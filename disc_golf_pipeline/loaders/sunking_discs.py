from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.cloud import storage

DEFAULT_PROJECT_ID = "disc-golf-price-compare"
DEFAULT_DATASET = "DiscGolfProducts"
DEFAULT_TABLE = "SunKingDiscs"

SUN_KING_DISCS_SCHEMA = [
    bigquery.SchemaField("ProductId", "INT64"),
    bigquery.SchemaField("ProductName", "STRING"),
    bigquery.SchemaField("Sku", "STRING"),
    bigquery.SchemaField("ProductLink", "STRING"),
    bigquery.SchemaField("ProductUrl", "STRING"),
    bigquery.SchemaField("CanonicalUrl", "STRING"),
    bigquery.SchemaField("ImageUrl", "STRING"),
    bigquery.SchemaField("Description", "STRING"),
    bigquery.SchemaField("Availability", "STRING"),
    bigquery.SchemaField("StockStatusText", "STRING"),
    bigquery.SchemaField("IsOutOfStock", "BOOL"),
    bigquery.SchemaField("ListPrice", "FLOAT64"),
    bigquery.SchemaField("SalePrice", "FLOAT64"),
    bigquery.SchemaField("Currency", "STRING"),
    bigquery.SchemaField("ProductWeightValue", "FLOAT64"),
    bigquery.SchemaField("ProductWeightUnits", "STRING"),
    bigquery.SchemaField("ProductWeightText", "STRING"),
    bigquery.SchemaField("ManufacturerSlug", "STRING"),
    bigquery.SchemaField("ManufacturerName", "STRING"),
    bigquery.SchemaField("PlasticSlug", "STRING"),
    bigquery.SchemaField("PlasticName", "STRING"),
    bigquery.SchemaField("ProductSlug", "STRING"),
    bigquery.SchemaField("CategoryPath", "STRING"),
    bigquery.SchemaField("CatalogWeightSummary", "STRING"),
    bigquery.SchemaField("record_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("VariantLabel", "STRING"),
    bigquery.SchemaField("VariantOptionId", "STRING"),
    bigquery.SchemaField("VariantOptionLabel", "STRING"),
    bigquery.SchemaField("VariantWeightGrams", "STRING"),
]


def build_table_id(project_id, dataset, table_name=DEFAULT_TABLE):
    return f"{project_id}.{dataset}.{table_name}"


def create_parquet_job_config():
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=SUN_KING_DISCS_SCHEMA,
    )


def aggregate_local_parquet(input_dir, output_file):
    input_path = Path(input_dir)
    parquet_files = sorted(input_path.glob("*.parquet"))
    if not parquet_files:
        return None, 0

    tables = [pq.read_table(file_path) for file_path in parquet_files]
    combined_table = pa.concat_tables(tables, promote_options="default")
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(combined_table, output_path, compression="snappy")
    return output_path, combined_table.num_rows


def load_local_parquet_to_bigquery(file_path, project_id=DEFAULT_PROJECT_ID, dataset=DEFAULT_DATASET):
    client = bigquery.Client(project=project_id)
    table_id = build_table_id(project_id, dataset)
    with Path(file_path).open("rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=create_parquet_job_config())
    job.result()
    return table_id


def list_gcs_parquet_objects(bucket_name, prefix=""):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs if blob.name.lower().endswith(".parquet")]


def load_gcs_parquet_to_bigquery(bucket_name, blob_names, project_id=DEFAULT_PROJECT_ID, dataset=DEFAULT_DATASET):
    names = [blob_names] if isinstance(blob_names, str) else list(blob_names)
    if not names:
        return None

    client = bigquery.Client(project=project_id)
    table_id = build_table_id(project_id, dataset)
    uris = [f"gs://{bucket_name}/{blob_name}" for blob_name in names]
    job = client.load_table_from_uri(uris, table_id, job_config=create_parquet_job_config())
    job.result()
    return table_id


def archive_local_file(file_path, archive_dir):
    source_path = Path(file_path)
    archive_path = Path(archive_dir)
    archive_path.mkdir(parents=True, exist_ok=True)
    destination = archive_path / source_path.name
    source_path.replace(destination)
    return destination


def archive_gcs_object(bucket_name, blob_name, archive_prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(blob_name)
    archive_name = f"{archive_prefix.strip('/')}/{Path(blob_name).name}" if archive_prefix else Path(blob_name).name
    bucket.copy_blob(source_blob, bucket, archive_name)
    source_blob.delete()
    return archive_name
