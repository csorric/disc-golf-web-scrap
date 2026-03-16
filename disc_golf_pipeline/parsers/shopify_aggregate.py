import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from disc_golf_pipeline.loaders.shopify import DEFAULT_PRODUCT_INFO_TABLE, DEFAULT_PRODUCTS_TABLE


def get_grouped_parquet_files(input_dir):
    input_path = Path(input_dir)
    return {
        DEFAULT_PRODUCTS_TABLE: sorted(input_path.glob("*_products.parquet")),
        DEFAULT_PRODUCT_INFO_TABLE: sorted(input_path.glob("*_product_info.parquet")),
    }


def aggregate_parquet_files(source_files, output_file):
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    tables = [pq.read_table(source_file) for source_file in source_files]
    if not tables:
        return 0

    combined_table = pa.concat_tables(tables, promote_options="default")
    pq.write_table(combined_table, output_path, compression="snappy")
    return combined_table.num_rows


def aggregate_directory(input_dir, output_dir):
    grouped_files = get_grouped_parquet_files(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    results = []
    for table_name, source_files in grouped_files.items():
        if not source_files:
            continue
        output_file = output_path / f"{table_name}.parquet"
        row_count = aggregate_parquet_files(source_files, output_file)
        results.append((table_name, output_file, row_count))

    return results


def main():
    parser = argparse.ArgumentParser(description="Aggregate parsed Parquet files by table.")
    parser.add_argument("--input", required=True, help="Directory containing parsed Parquet files.")
    parser.add_argument("--output", required=True, help="Directory for aggregated Parquet files.")
    args = parser.parse_args()

    for table_name, output_file, row_count in aggregate_directory(args.input, args.output):
        print(f"Wrote {output_file} ({table_name}, {row_count} rows)")


if __name__ == "__main__":
    main()
