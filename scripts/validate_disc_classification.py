"""Detached integration checks using isolated BigQuery/Typesense fixtures.

Run: python -m scripts.validate_disc_classification --start
Writes status/logs under output/classification-checks. Never changes an alias
or production table. Temporary test resources are removed after success.
"""

import argparse
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import traceback
from urllib.parse import quote

import requests
from google.cloud import bigquery

from disc_golf_pipeline.common.runtime import PROJECT_ROOT, load_env_file
from disc_golf_pipeline.services.disc_attributes import build_disc_attributes_view_sql
from disc_golf_pipeline.services.disc_classification import (
    build_model_classification_query, build_variant_classification_query,
    refresh_disc_classifications,
)
from disc_golf_pipeline.services.disc_weight_llm import build_review_table_sql
from disc_golf_pipeline.services.indexer import (
    RELEASE_COLLECTION_FIELDS, assert_disc_fields_match, build_document,
    ensure_normalized_collection, iterate_variant_state, send_upsert_batch,
    validate_normalized_collection,
)
from disc_golf_pipeline.services.normalization_job import build_job_id, utc_now, write_json_atomic
from disc_golf_pipeline.services.process_data import (
    build_variant_snapshot_view_sql, build_variant_state_sql,
    ensure_variant_table_schemas, get_gcp_project_id, get_bigquery_dataset,
)
from disc_golf_pipeline.services.try_discs_sync import MATCH_FIELDS, build_match_rows
from tests.disc_classification_cases import EXAMPLES, formula_cases, reference


def load_rows(client, table, rows, schema):
    client.load_table_from_json(rows, table, job_config=bigquery.LoadJobConfig(
        schema=schema, write_disposition="WRITE_TRUNCATE",
    )).result()


def assert_value(expected, actual, label):
    if isinstance(expected, float):
        if actual is None or not math.isclose(expected, actual, abs_tol=1e-8, rel_tol=1e-9):
            raise AssertionError(f"{label}: expected {expected}, got {actual}")
    elif expected != actual:
        raise AssertionError(f"{label}: expected {expected}, got {actual}")


def check_formula_sql(client, project, dataset):
    cases = formula_cases()
    numeric_strings = {"speed", "glide", "turn", "fade", "normalized_weight_g",
                       "normalized_weight_min_g", "normalized_weight_max_g"}
    bools = {"flight_conflict", "flight_conflict_unresolved", "IsDistanceDriver",
             "IsFairwayDriver", "IsMidrange", "IsPutter"}
    schema = [bigquery.SchemaField(name,
              "BOOL" if name in bools else "FLOAT64" if name == "weight_confidence" else "STRING",
              mode="REPEATED" if name == "flight_invalid_fields" else "NULLABLE")
              for name in cases[0]]
    rows = [{key: (str(value) if key in numeric_strings and value is not None else value)
             for key, value in row.items()} for row in cases]
    prefix = f"{project}.{dataset}"
    load_rows(client, f"{prefix}.FormulaInputs", rows, schema)
    client.query(f"CREATE OR REPLACE TABLE `{prefix}.FormulaModels` AS "
                 + build_model_classification_query(f"`{prefix}.FormulaInputs`")).result()
    joined = f"""(SELECT src.*, model.* EXCEPT(flight_record_key, normalized_manufacturer,
        normalized_model, flight_scope, speed, glide, turn, fade, flight_source,
        flight_evidence, flight_conflict_unresolved)
        FROM `{prefix}.FormulaInputs` src JOIN `{prefix}.FormulaModels` model USING(flight_record_key))"""
    actual_rows = {r["id"]: dict(r.items()) for r in client.query(
        build_variant_classification_query(joined)).result()}
    assert len(actual_rows) == len(cases)
    for row in cases:
        for name, expected in reference(row).items():
            assert_value(expected, actual_rows[row["id"]][name], f"case {row['id']} {name}")
    # Six-case groups at the end vary exactly one input from the same baseline.
    random_start = len(cases) - 360
    for offset in range(random_start, len(cases), 6):
        baseline = actual_rows[str(offset)]["access_variant"]
        for shift, direction in ((1, -1), (2, 1), (3, 1), (4, -1), (5, 1)):
            changed = actual_rows[str(offset + shift)]["access_variant"]
            assert direction * (changed - baseline) >= -1e-9
    print(f"SQL/reference parity passed for {len(cases)} cases and 300 monotonic comparisons.", flush=True)
    return len(cases)


def pipeline_fixtures(client, project, production_dataset, dataset):
    catalog, rows = [], []

    def variant(model, weight=None, title="Blue", disc=True, body="", tags=""):
        key = str(9007199254740993 + len(rows))
        row = dict(id=f"fixture-{key}", source_variant_key=f"shopify:fixture.example:{key}",
                   source="shopify", product_id=key, variant_id=key, title=model,
                   vendor="Fixture", store="fixture.example", store_url="https://fixture.example",
                   product_link=f"https://fixture.example/products/{key}", image="", variant_image="",
                   variant_title=title, price=19.99, high_price=19.99, low_price=19.99,
                   weight_g=weight, in_stock=True, tags=tags, BodyHtml=body,
                   product_type="disc" if disc else "bag", item_type="disc" if disc else "bag",
                   is_disc=disc, normalized_manufacturer="Fixture", normalized_model=model,
                   IsDistanceDriver=False, IsFairwayDriver=False, IsMidrange=False, IsPutter=False)
        rows.append(row)

    for name, speed, glide, turn, fade, weight, _, _ in EXAMPLES:
        catalog.append(dict(brand="Fixture", name=name, speed=speed, glide=glide, turn=turn, fade=fade,
                            category="Fairway Driver" if name == "Anax" else None))
        variant(name, weight, f"Blue {weight}g", body="Flight Numbers: 10/6/-1/3" if name == "Anax" else "")
    variant("Anax", 227, "170-175g / Blue")
    variant("Anax", None)
    variant("Anax", 227)
    catalog.extend([
        dict(brand="Fixture", name="Partial", speed=7, glide=5, turn=-1, fade=None),
        dict(brand="Fixture", name="Unsupported", speed=15, glide=5, turn=-1, fade=2),
        dict(brand="Fixture", name="Invalid", speed=True, glide=5, turn=-1, fade=2),
        dict(brand="Fixture", name="Ambiguous", speed=7, glide=5, turn=-1, fade=2),
        dict(brand="Fixture", name="Ambiguous", speed=9, glide=5, turn=-1, fade=2),
    ])
    for name in ("Partial", "Unsupported", "Invalid", "Ambiguous"):
        variant(name, 170, "170g", body="Flight Numbers: 7/5/-1/2")
    variant("LocalConflict", 170, "170g", body="Flight Numbers: 7/5/-1/2", tags="7/5/-3/2")
    variant("Missing", None)
    variant("Bag", 225, disc=False)
    schema = client.get_table(f"{project}.{production_dataset}.NormalizedVariantSnapshot").schema

    def adapt(row):
        converted = {}
        for field in schema:
            value = row.get(field.name)
            if value is not None and field.field_type == "STRING":
                value = str(value)
            elif value is not None and field.field_type in ("INTEGER", "INT64"):
                value = int(value)
            converted[field.name] = [] if field.mode == "REPEATED" and value is None else value
        return converted

    prefix = f"{project}.{dataset}"
    load_rows(client, f"{prefix}.NormalizedVariantSnapshot", [adapt(r) for r in rows], schema)
    pairs = [{"manufacturer": "Fixture", "model": name} for name in sorted({r["normalized_model"] for r in rows})]
    load_rows(client, f"{prefix}.TryDiscsModelMatches", build_match_rows(catalog, pairs, "test"),
              [bigquery.SchemaField(n, t) for n, t in MATCH_FIELDS])
    client.query(build_review_table_sql(project, dataset)).result()
    for name in ("VariantState", "VariantChanges"):
        table_schema = client.get_table(f"{project}.{production_dataset}.{name}").schema
        client.create_table(bigquery.Table(f"{prefix}.{name}", schema=table_schema))
    return rows


def refresh_pipeline(client, project, dataset):
    client.query(build_disc_attributes_view_sql(project, dataset)).result()
    refresh_disc_classifications(client, project, dataset)
    client.query(build_variant_snapshot_view_sql(project, dataset)).result()
    ensure_variant_table_schemas(client, project, dataset)
    client.query(build_variant_state_sql(project, dataset)).result()
    return [dict(r.items()) for r in iterate_variant_state(client, f"{project}.{dataset}.VariantState")]


def validate(job_directory):
    load_env_file()
    project, production_dataset = get_gcp_project_id(), get_bigquery_dataset()
    suffix = job_directory.name.replace("-", "_")
    dataset = f"ClassificationCheck_{suffix}"
    collection = f"classification_check_{suffix.lower()}"
    client = bigquery.Client(project=project, default_query_job_config=bigquery.QueryJobConfig(
        maximum_bytes_billed=1_000_000_000))
    source_dataset = client.get_dataset(f"{project}.{production_dataset}")
    staging = bigquery.Dataset(f"{project}.{dataset}")
    staging.location = source_dataset.location
    staging.default_table_expiration_ms = 86_400_000
    client.create_dataset(staging)
    state = {"state": "running", "started_at": utc_now(), "dataset": staging.dataset_id,
             "collection": collection, "pid": os.getpid(), "production_changed": False}
    write_json_atomic(job_directory / "status.json", state)
    try:
        count = check_formula_sql(client, project, dataset)
        pipeline_fixtures(client, project, production_dataset, dataset)
        rows = refresh_pipeline(client, project, dataset)
        for name, _, _, _, _, _, score, role in EXAMPLES:
            row = next(r for r in rows if r["title"] == name and r["has_exact_weight"])
            assert_value(score, row["access_variant"], name)
            assert row["beginner_role"] == role
        anax = next(r for r in rows if r["title"] == "Anax" and r["has_exact_weight"])
        assert anax["disc_category"] == "fairway_driver" and anax["power_band"] == "high"
        assert anax["flight_conflict"] and not anax["flight_conflict_unresolved"]
        ranged = next(r for r in rows if r["weight_status"] == "range")
        assert ranged["weight_g"] is None and ranged["weight_min_g"] == 170 and ranged["weight_max_g"] == 175
        for name, status in (("Partial", "partial_flight"), ("Unsupported", "unsupported_flight"),
                             ("Invalid", "unsupported_flight"), ("Ambiguous", "unresolved_conflict"),
                             ("LocalConflict", "unresolved_conflict"), ("Missing", "missing_flight"),
                             ("Bag", "not_applicable")):
            row = next(r for r in rows if r["title"] == name)
            assert row["data_status"] == status, (name, row["data_status"])
            assert row["access_model"] is None
        assert next(r for r in rows if r["title"] == "Partial")["fade"] is None
        print(f"Pipeline fixture checks passed for {len(rows)} variants.", flush=True)
        host, key = os.environ["TYPESENSE_HOST"], os.environ["TYPESENSE_ADMIN_KEY"]
        with requests.Session() as session:
            ensure_normalized_collection(host, key, collection, session, RELEASE_COLLECTION_FIELDS)
            docs = [build_document(r, use_source_variant_key=True) for r in rows]
            result = send_upsert_batch(session, host, key, collection, docs, 1)
            assert result["ok"], result
            check = validate_normalized_collection(client, f"{project}.{dataset}.VariantState",
                host, key, collection, session, RELEASE_COLLECTION_FIELDS, True)
            headers = {"X-TYPESENSE-API-KEY": key}
            for row in rows:
                url = f"{host.rstrip('/')}/collections/{collection}/documents/{quote(row['source_variant_key'], safe='')}"
                response = session.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                assert_disc_fields_match(row, response.json())
            # Replace a formerly scored disc with missing flights, then run the
            # same merge and full document conversion to verify null clearing.
            client.query(f"UPDATE `{project}.{dataset}.TryDiscsModelMatches` "
                         "SET speed=NULL, glide=NULL, turn=NULL, fade=NULL WHERE model='Mako3'").result()
            refreshed = refresh_pipeline(client, project, dataset)
            changed = next(r for r in refreshed if r["title"] == "Mako3")
            assert changed["access_model"] is None and changed["power_band"] is None
            result = send_upsert_batch(session, host, key, collection,
                                      [build_document(changed, use_source_variant_key=True)], 2)
            assert result["ok"]
            response = session.get(f"{host.rstrip('/')}/collections/{collection}/documents/"
                                   + quote(changed["source_variant_key"], safe=""), headers=headers, timeout=30)
            response.raise_for_status()
            assert_disc_fields_match(changed, response.json())
            session.delete(f"{host.rstrip('/')}/collections/{collection}", headers=headers, timeout=30).raise_for_status()
        client.delete_dataset(staging, delete_contents=True)
        state.update(state="succeeded", finished_at=utc_now(), formula_cases=count,
                     variant_fixtures=len(rows), typesense_validation=check, test_resources_removed=True)
    except BaseException as exc:
        state.update(state="failed", finished_at=utc_now(), error=f"{type(exc).__name__}: {exc}")
        traceback.print_exc()
    write_json_atomic(job_directory / "status.json", state)
    print(json.dumps(state, indent=2), flush=True)
    return 0 if state["state"] == "succeeded" else 1


def start():
    directory = PROJECT_ROOT / "output" / "classification-checks" / build_job_id()
    directory.mkdir(parents=True)
    write_json_atomic(directory / "status.json", {"state": "queued", "queued_at": utc_now()})
    flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    with (directory / "stdout.log").open("w", encoding="utf-8") as out, (directory / "stderr.log").open("w", encoding="utf-8") as err:
        worker = subprocess.Popen([sys.executable, "-u", "-m", "scripts.validate_disc_classification",
                                   "--worker", str(directory)], cwd=PROJECT_ROOT,
                                  stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                  creationflags=flags, close_fds=True, start_new_session=os.name != "nt")
    print(json.dumps({"pid": worker.pid, "job_directory": str(directory)}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--start", action="store_true")
    mode.add_argument("--worker", type=Path)
    args = parser.parse_args()
    if args.start:
        start()
    else:
        raise SystemExit(validate(args.worker))
