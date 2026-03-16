from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery
from google.cloud import storage

DEFAULT_PROJECT_ID = "disc-golf-price-compare"
DEFAULT_DATASET = "DiscGolfProducts"
DEFAULT_TABLE = "OTBDiscs"

OTB_DISCS_SCHEMA = [
    bigquery.SchemaField("ProductId", "INT64"),
    bigquery.SchemaField("VariationId", "INT64"),
    bigquery.SchemaField("ProductName", "STRING"),
    bigquery.SchemaField("Sku", "STRING"),
    bigquery.SchemaField("ProductLink", "STRING"),
    bigquery.SchemaField("ProductUrl", "STRING"),
    bigquery.SchemaField("CanonicalUrl", "STRING"),
    bigquery.SchemaField("ImageUrl", "STRING"),
    bigquery.SchemaField("Description", "STRING"),
    bigquery.SchemaField("ManufacturerName", "STRING"),
    bigquery.SchemaField("DiscTypeName", "STRING"),
    bigquery.SchemaField("ColorName", "STRING"),
    bigquery.SchemaField("PlasticName", "STRING"),
    bigquery.SchemaField("StampFoil", "STRING"),
    bigquery.SchemaField("Weight", "STRING"),
    bigquery.SchemaField("ScaledWeight", "STRING"),
    bigquery.SchemaField("Flatness", "STRING"),
    bigquery.SchemaField("Stiffness", "STRING"),
    bigquery.SchemaField("StockPrice", "FLOAT64"),
    bigquery.SchemaField("Currency", "STRING"),
    bigquery.SchemaField("StockText", "STRING"),
    bigquery.SchemaField("AvailableStock", "INT64"),
    bigquery.SchemaField("IsOutOfStock", "BOOL"),
    bigquery.SchemaField("VariantAttributesJson", "STRING"),
    bigquery.SchemaField("Speed", "FLOAT64"),
    bigquery.SchemaField("Glide", "FLOAT64"),
    bigquery.SchemaField("Turn", "FLOAT64"),
    bigquery.SchemaField("Fade", "FLOAT64"),
    bigquery.SchemaField("record_timestamp", "TIMESTAMP"),
]

OTB_ARROW_SCHEMA = pa.schema(
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


def build_table_id(project_id, dataset, table_name=DEFAULT_TABLE):
    return f"{project_id}.{dataset}.{table_name}"


def create_parquet_job_config():
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=OTB_DISCS_SCHEMA,
    )


def aggregate_local_parquet(input_dir, output_file):
    input_path = Path(input_dir)
    parquet_files = sorted(input_path.glob("*.parquet"))
    if not parquet_files:
        return None, 0

    tables = [pq.read_table(file_path) for file_path in parquet_files]
    combined_table = pa.concat_tables(tables, promote_options="default").cast(OTB_ARROW_SCHEMA)
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
