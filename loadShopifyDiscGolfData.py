from pathlib import Path

from google.cloud import bigquery
from google.cloud import storage

DEFAULT_PROJECT_ID = "disc-golf-price-compare"
DEFAULT_DATASET = "DiscGolfProducts"
DEFAULT_PRODUCTS_TABLE = "Products"
DEFAULT_PRODUCT_INFO_TABLE = "ProductInfo"


def get_table_name_for_file(file_name):
    lower_name = file_name.lower()
    if lower_name.endswith("_products.csv"):
        return DEFAULT_PRODUCTS_TABLE
    if lower_name.endswith("_product_info.csv"):
        return DEFAULT_PRODUCT_INFO_TABLE
    return None


def build_table_id(project_id, dataset, table_name):
    return f"{project_id}.{dataset}.{table_name}"


def create_csv_job_config():
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        quote_character='"',
        allow_quoted_newlines=True,
    )


def load_local_csv_to_bigquery(file_path, project_id=DEFAULT_PROJECT_ID, dataset=DEFAULT_DATASET):
    path = Path(file_path)
    table_name = get_table_name_for_file(path.name)
    if not table_name:
        return None

    client = bigquery.Client(project=project_id)
    table_id = build_table_id(project_id, dataset, table_name)
    with path.open("rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=create_csv_job_config())
    job.result()
    return table_id


def load_local_directory_to_bigquery(input_dir, project_id=DEFAULT_PROJECT_ID, dataset=DEFAULT_DATASET):
    input_path = Path(input_dir)
    loaded_tables = []

    products_files = sorted(input_path.glob("*_products.csv"))
    product_info_files = sorted(input_path.glob("*_product_info.csv"))

    for file_path in products_files + product_info_files:
        table_id = load_local_csv_to_bigquery(file_path, project_id=project_id, dataset=dataset)
        if table_id:
            loaded_tables.append((file_path, table_id))

    return loaded_tables


def list_gcs_csv_objects(bucket_name, prefix=""):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs if blob.name.lower().endswith(".csv")]


def load_gcs_csv_to_bigquery(bucket_name, blob_name, project_id=DEFAULT_PROJECT_ID, dataset=DEFAULT_DATASET):
    table_name = get_table_name_for_file(Path(blob_name).name)
    if not table_name:
        return None

    client = bigquery.Client(project=project_id)
    table_id = build_table_id(project_id, dataset, table_name)
    uri = f"gs://{bucket_name}/{blob_name}"
    job = client.load_table_from_uri(uri, table_id, job_config=create_csv_job_config())
    job.result()
    return table_id


def load_gcs_prefix_to_bigquery(bucket_name, prefix="", project_id=DEFAULT_PROJECT_ID, dataset=DEFAULT_DATASET):
    loaded_tables = []

    csv_objects = list_gcs_csv_objects(bucket_name, prefix=prefix)
    products_files = sorted(name for name in csv_objects if name.lower().endswith("_products.csv"))
    product_info_files = sorted(name for name in csv_objects if name.lower().endswith("_product_info.csv"))

    for blob_name in products_files + product_info_files:
        table_id = load_gcs_csv_to_bigquery(bucket_name, blob_name, project_id=project_id, dataset=dataset)
        if table_id:
            loaded_tables.append((blob_name, table_id))

    return loaded_tables


def hello_gcs(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    blob_name = data["name"]
    table_id = load_gcs_csv_to_bigquery(bucket_name, blob_name)
    if table_id:
        print(f"Loaded {blob_name} into {table_id}")


try:
    import functions_framework
except Exception:
    functions_framework = None
else:
    hello_gcs = functions_framework.cloud_event(hello_gcs)
