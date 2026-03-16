from pathlib import Path

from google.cloud import bigquery
from google.cloud import storage

DEFAULT_PROJECT_ID = "disc-golf-price-compare"
DEFAULT_DATASET = "DiscGolfProducts"
DEFAULT_PRODUCTS_TABLE = "Products"
DEFAULT_PRODUCT_INFO_TABLE = "ProductInfo"


def get_table_name_for_file(file_name):
    lower_name = file_name.lower()
    if lower_name.endswith("_products.parquet") or lower_name == "products.parquet":
        return DEFAULT_PRODUCTS_TABLE
    if lower_name.endswith("_product_info.parquet") or lower_name == "productinfo.parquet":
        return DEFAULT_PRODUCT_INFO_TABLE
    return None


def build_table_id(project_id, dataset, table_name):
    return f"{project_id}.{dataset}.{table_name}"


def create_parquet_job_config():
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )


def load_local_parquet_to_bigquery(file_path, project_id=DEFAULT_PROJECT_ID, dataset=DEFAULT_DATASET):
    path = Path(file_path)
    table_name = get_table_name_for_file(path.name)
    if not table_name:
        return None

    client = bigquery.Client(project=project_id)
    table_id = build_table_id(project_id, dataset, table_name)
    with path.open("rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=create_parquet_job_config())
    job.result()
    return table_id


def load_local_directory_to_bigquery(input_dir, project_id=DEFAULT_PROJECT_ID, dataset=DEFAULT_DATASET):
    input_path = Path(input_dir)
    loaded_tables = []

    aggregated_products = input_path / f"{DEFAULT_PRODUCTS_TABLE}.parquet"
    aggregated_product_info = input_path / f"{DEFAULT_PRODUCT_INFO_TABLE}.parquet"

    for file_path in (aggregated_products, aggregated_product_info):
        if not file_path.exists():
            continue
        table_id = load_local_parquet_to_bigquery(file_path, project_id=project_id, dataset=dataset)
        if table_id:
            loaded_tables.append((file_path, table_id))

    return loaded_tables


def archive_local_file(file_path, archive_dir):
    source_path = Path(file_path)
    archive_path = Path(archive_dir)
    archive_path.mkdir(parents=True, exist_ok=True)
    destination = archive_path / source_path.name
    source_path.replace(destination)
    return destination


def list_gcs_parquet_objects(bucket_name, prefix=""):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs if blob.name.lower().endswith(".parquet")]


def archive_gcs_object(bucket_name, blob_name, archive_prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(blob_name)
    archive_name = f"{archive_prefix.strip('/')}/{Path(blob_name).name}" if archive_prefix else Path(blob_name).name
    bucket.copy_blob(source_blob, bucket, archive_name)
    source_blob.delete()
    return archive_name


def load_gcs_parquet_to_bigquery(bucket_name, blob_names, table_name, project_id=DEFAULT_PROJECT_ID, dataset=DEFAULT_DATASET):
    names = [blob_names] if isinstance(blob_names, str) else list(blob_names)
    if not names:
        return None

    client = bigquery.Client(project=project_id)
    table_id = build_table_id(project_id, dataset, table_name)
    uris = [f"gs://{bucket_name}/{blob_name}" for blob_name in names]
    job = client.load_table_from_uri(uris, table_id, job_config=create_parquet_job_config())
    job.result()
    return table_id


def load_gcs_prefix_to_bigquery(bucket_name, prefix="", project_id=DEFAULT_PROJECT_ID, dataset=DEFAULT_DATASET):
    loaded_tables = []

    parquet_objects = list_gcs_parquet_objects(bucket_name, prefix=prefix)
    products_files = sorted(name for name in parquet_objects if name.lower().endswith("_products.parquet"))
    product_info_files = sorted(name for name in parquet_objects if name.lower().endswith("_product_info.parquet"))

    for blob_names, table_name in (
        (products_files, DEFAULT_PRODUCTS_TABLE),
        (product_info_files, DEFAULT_PRODUCT_INFO_TABLE),
    ):
        table_id = load_gcs_parquet_to_bigquery(
            bucket_name,
            blob_names,
            table_name,
            project_id=project_id,
            dataset=dataset,
        )
        if table_id:
            loaded_tables.append((table_name, table_id))

    return loaded_tables


def hello_gcs(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    blob_name = data["name"]
    table_name = get_table_name_for_file(Path(blob_name).name)
    table_id = load_gcs_parquet_to_bigquery(bucket_name, blob_name, table_name) if table_name else None
    if table_id:
        print(f"Loaded {blob_name} into {table_id}")


try:
    import functions_framework
except Exception:
    functions_framework = None
else:
    hello_gcs = functions_framework.cloud_event(hello_gcs)
