"""Executable category regressions and publication gates; no external APIs."""

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery
from disc_golf_pipeline.common.runtime import PROJECT_ROOT

from disc_golf_pipeline.services.disc_categories import LEGACY_CATEGORY_FLAGS, extract_category
from disc_golf_pipeline.services.disc_classification import (
    build_model_classification_query, build_variant_classification_query,
)
from disc_golf_pipeline.services.model_identity_checks import validate_model_identity_regressions


DEFAULT_CATEGORY_REPORT = PROJECT_ROOT / "output" / "disc-category-review.json"


def build_category_audit_query(project, dataset):
    """Catalog-wide evidence review, including driver speed/category mismatches."""
    return f"""
      WITH listings AS (
        SELECT *,
          NULLIF(TRIM(normalized_manufacturer), '') IS NOT NULL
          AND NULLIF(TRIM(normalized_model), '') IS NOT NULL
          AND STRPOS(CONCAT(' ', REGEXP_REPLACE(LOWER(title), r'[^a-z0-9]+', ' '), ' '),
            CONCAT(' ', REGEXP_REPLACE(LOWER(normalized_model), r'[^a-z0-9]+', ' '), ' ')) > 0
          AS title_matches_model
        FROM `{project}.{dataset}.VariantState` WHERE item_type = 'disc'
      ), mold_categories AS (
        SELECT normalized_manufacturer, normalized_model,
          ARRAY_AGG(DISTINCT disc_category ORDER BY disc_category) AS observed_categories
        FROM listings WHERE title_matches_model AND disc_category IS NOT NULL
        GROUP BY normalized_manufacturer, normalized_model
        HAVING COUNT(DISTINCT disc_category) > 1
      ), flagged AS (
        SELECT src.*, observed_categories,
          ARRAY(SELECT reason FROM UNNEST([
            IF(disc_category = 'putter' AND speed > 4 AND speed <= 14,
              'putter_above_speed_4', NULL),
            IF(disc_category = 'fairway_driver' AND speed >= 1 AND speed < 6,
              'fairway_below_speed_6', NULL),
            IF(disc_category = 'distance_driver' AND speed >= 6 AND speed < 10,
              'distance_below_speed_10', NULL),
            IF(disc_category = 'fairway_driver' AND speed >= 10 AND speed <= 14,
              'fairway_at_or_above_speed_10', NULL),
            IF(title_matches_model AND ARRAY_LENGTH(observed_categories) > 1,
              'conflicting_mold_categories', NULL),
            IF(disc_category IS NULL, 'missing_category', NULL)
          ]) reason WHERE reason IS NOT NULL) AS review_reasons
        FROM listings src LEFT JOIN mold_categories USING(normalized_manufacturer, normalized_model)
      )
      SELECT store, title, product_link, normalized_manufacturer, normalized_model,
        title_matches_model, disc_category, category_source, category_evidence,
        product_type, tags, speed, flight_source, flight_evidence,
        SUBSTR(BodyHtml, 1, 1500) AS description_excerpt,
        observed_categories, review_reasons, COUNT(*) AS variants
      FROM flagged WHERE ARRAY_LENGTH(review_reasons) > 0
      GROUP BY ALL
      ORDER BY normalized_manufacturer, normalized_model, store, title
    """


def review_disc_categories(client, project, dataset, report_path=DEFAULT_CATEGORY_REPORT):
    """Save actionable listing evidence for all molds, without changing categories."""
    rows = [dict(row.items()) for row in client.query(build_category_audit_query(project, dataset)).result()]
    listing_counts = Counter(reason for row in rows for reason in row["review_reasons"])
    variant_counts = Counter()
    for row in rows:
        for reason in row["review_reasons"]:
            variant_counts[reason] += row["variants"]
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": f"{project}.{dataset}.VariantState",
        "policy": "Resolved supported speeds 6 to below 10 imply fairway; 10+ imply distance. Other categories use sourced evidence.",
        "listing_counts_by_reason": dict(listing_counts),
        "variant_counts_by_reason": dict(variant_counts),
        "listings": rows,
    }
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    summary = {"report_path": str(report_path), "listing_counts_by_reason": dict(listing_counts),
               "variant_counts_by_reason": dict(variant_counts)}
    print(json.dumps({"catalog_category_review": summary}), flush=True)
    return summary


def validate_category_audit(client):
    """Check speed boundaries and mold conflicts on isolated synthetic listings."""
    cases = [
        ("Putter boundary", "putter", 4, "Putter boundary", "disc", []),
        ("Fast putter", "putter", 4.1, "Fast putter", "disc", ["putter_above_speed_4"]),
        ("Slow fairway", "fairway_driver", 5.9, "Slow fairway", "disc", ["fairway_below_speed_6"]),
        ("Fairway boundary", "fairway_driver", 6, "Fairway boundary", "disc", []),
        ("Seven speed distance", "distance_driver", 7, "Seven speed distance", "disc", ["distance_below_speed_10"]),
        ("Ten speed fairway", "fairway_driver", 10, "Ten speed fairway", "disc", ["fairway_at_or_above_speed_10"]),
        ("Nine speed fairway", "fairway_driver", 9, "Nine speed fairway", "disc", []),
        ("Ten speed distance", "distance_driver", 10, "Ten speed distance", "disc", []),
        ("Missing speed", "putter", None, "Missing speed", "disc", []),
        ("Unsupported speed", "putter", 15, "Unsupported speed", "disc", []),
        ("Conflict A", "midrange", 5, "Conflict", "disc", ["conflicting_mold_categories"]),
        ("Conflict B", "fairway_driver", 7, "Conflict", "disc", ["conflicting_mold_categories"]),
        ("Other listing", "putter", 3, "Conflict", "disc", []),
        ("Unknown", None, 5, "Unknown", "disc", ["missing_category"]),
        ("Bag", "putter", 10, "Bag", "bag", []),
    ]
    rows = [dict(title=title, disc_category=category, speed=speed, normalized_model=model, item_type=item_type)
            for title, category, speed, model, item_type, _ in cases]
    source = """SELECT JSON_VALUE(row, '$.title') AS title,
      JSON_VALUE(row, '$.normalized_model') AS normalized_model,
      JSON_VALUE(row, '$.disc_category') AS disc_category,
      JSON_VALUE(row, '$.item_type') AS item_type,
      SAFE_CAST(JSON_VALUE(row, '$.speed') AS FLOAT64) AS speed,
      'Fixture' AS normalized_manufacturer, 'fixture' AS store, 'https://fixture' AS product_link,
      'fixture' AS category_source, 'fixture' AS category_evidence, 'disc' AS product_type,
      '' AS tags, 'fixture' AS flight_source, 'fixture' AS flight_evidence, '' AS BodyHtml
      FROM UNNEST(JSON_QUERY_ARRAY(@audit_fixtures)) row"""
    query = build_category_audit_query("fixture", "category").replace(
        "`fixture.category.VariantState`", "audit_inputs")
    query = f"WITH audit_inputs AS ({source}) SELECT * FROM ({query})"
    config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter(
        "audit_fixtures", "STRING", json.dumps(rows))])
    actual = {row["title"]: list(row["review_reasons"]) for row in client.query(query, job_config=config).result()}
    expected = {title: reasons for title, _, _, _, _, reasons in cases if reasons}
    if actual != expected:
        raise AssertionError(f"Catalog category audit regression: {actual}, expected {expected}")
    print(f"Passed {len(cases)} catalog-wide category audit fixtures.", flush=True)


def category_regression_cases():
    cases = []

    def add(name, category, source=None, **inputs):
        row = dict(id=name, title=name, product_type="Disc Golf", tags="", BodyHtml="",
                   normalized_manufacturer="Fixture", normalized_model=name, store=name,
                   catalog_category=None)
        row.update(inputs)
        cases.append((row, category, source))

    add("Dart", "putter", "product_tags", title="Innova XT Dart",
        tags='["approach", "putter", "wind"]',
        BodyHtml="Shop our putters, midranges, fairway drivers and distance drivers.")
    add("Fox", "midrange", "product_type", title="Innova DX Proto Glow Fox",
        product_type="Mid-Range Drivers", BodyHtml="This midrange flies like a fairway driver.")
    add("Fox definition", "midrange", "description_definition",
        BodyHtml="The Fox is a midrange that feels like a fairway driver.")
    add("Dart definition", "putter", "description_definition",
        BodyHtml="The Dart is a putt-and-approach disc.")
    for tag in ("Type_Mid-Range", "disc_type_Midrange", "Disc Type_Midrange", "category: Midrange"):
        add(tag, "midrange", "product_tags", tags=json.dumps([tag]),
            BodyHtml="This is a fairway driver.")
    add("ambiguous tags", None, "ambiguous_retailer_category",
        tags='["putter", "distance driver"]', BodyHtml="This is a putter.")
    add("negative", None, BodyHtml="This is not a distance driver.")
    add("navigation", None, BodyHtml="Browse our distance drivers, fairway drivers and putters.")
    add("HTML navigation", None, BodyHtml="<nav>This is a distance driver.</nav>")
    add("comparison only", None, BodyHtml="It flies like a fairway driver.")
    add("distance", "distance_driver", "description_definition", BodyHtml="This is a distance driver.")
    add("control", "fairway_driver", "description_definition", BodyHtml="This is a control driver.")
    add("ambiguous definition", None, "ambiguous_retailer_category",
        BodyHtml="This is a putter and a midrange.")
    add("plastic", None, title="Putter Line Buzzz")
    add("title", "midrange", "product_title", title="Fixture Mid-Range Driver")
    add("alias", "putter", "product_type", product_type="Putt & Approach Discs")
    add("catalog", "putter", "try_discs_category", catalog_category="Putt & Approach",
        product_type="Distance Driver")
    add("legacy only", None)  # SQL fixture deliberately supplies a stale driver flag.
    add("Bobcat shot tag", "midrange", "product_tags", title="Mint Bobcat",
        tags='["approach", "midrange", "overstable"]')
    add("approach shot only", None, tags='["approach"]')
    add("approach taxonomy", "putter", "product_tags", tags='["Type_Approach"]')
    add("Buzzz prose", "midrange", "description_definition",
        BodyHtml="The Buzzz is a dependable midrange for controlled drives and approach shots.")
    for i in range(4):
        add(f"strong majority {i}", "distance_driver", "product_type", title="Fixture Guld",
            normalized_model="Guld", store=f"majority-{i}", product_type="Distance Driver")
    add("incorrect structured category", "distance_driver", "retailer_model_consensus",
        title="Fixture Guld", normalized_model="Guld", store="outlier", product_type="Putt & Approach")
    add("majority identity mismatch", "putter", "product_type", title="Different Disc",
        normalized_model="Guld", product_type="putter")
    for i in range(3):
        add(f"weak majority {i}", "midrange", "product_type", title="Fixture Weak",
            normalized_model="Weak", store=f"weak-{i}", product_type="Midrange")
    add("insufficient majority", "putter", "product_type", title="Fixture Weak",
        normalized_model="Weak", store="weak-outlier", product_type="Putter")
    for i in range(6):
        add(f"duplicate store vote {i}", "midrange", "product_type", title="Fixture Weak",
            normalized_model="Weak", store="weak-0", product_type="Midrange")
    for i in range(4):
        add(f"mixed store majority {i}", "midrange", "product_type", title="Fixture Mixed",
            normalized_model="Mixed", store=f"mixed-{i}", product_type="Midrange")
    for category in ("midrange", "putter"):
        add(f"mixed store {category}", category, "product_type", title="Fixture Mixed",
            normalized_model="Mixed", store="mixed-vote", product_type=category)
    add("mixed stores count against majority", "putter", "product_type", title="Fixture Mixed",
        normalized_model="Mixed", store="mixed-outlier", product_type="putter")
    for store in ("one", "two"):
        add(f"consensus {store}", "midrange", "product_tags", title="Fixture Fox",
            normalized_model="Fox", store=store, tags='["midrange"]')
    add("consensus prose", "midrange", "retailer_model_consensus", title="Fixture Fox",
        normalized_model="Fox", BodyHtml="This is a fairway driver.")
    add("identity mismatch", "fairway_driver", "description_definition", title="Fixture Racer",
        normalized_model="Fox", BodyHtml="This is a fairway driver.")
    add("consensus ambiguity", None, "ambiguous_retailer_category", title="Fixture Fox",
        normalized_model="Fox", tags='["putter", "midrange"]')
    add("consensus catalog", "putter", "try_discs_category", title="Fixture Fox",
        normalized_model="Fox", catalog_category="putter")
    add("other manufacturer", None, title="Other Fox", normalized_model="Fox",
        normalized_manufacturer="Other")
    for store, category, product_type in (("a", "putter", "putter"), ("b", "midrange", "midrange")):
        add(f"conflict {store}", category, "product_type", normalized_model="Conflict",
            title="Conflict", store=store, product_type=product_type)
    add("conflicting consensus", None, title="Conflict", normalized_model="Conflict")
    for i in range(2):
        add(f"same store {i}", "midrange", "product_type", normalized_model="Single",
            title="Single", store="same-store", product_type="midrange")
    add("single retailer insufficient", None, title="Single", normalized_model="Single")
    for i in range(2):
        add(f"wrong identity {i}", "putter", "product_type", normalized_model="Unverified",
            title="Different mold", product_type="putter")
    add("unverified contributor", None, title="Unverified", normalized_model="Unverified")
    for speed in (6, 7, 9, 9.5, 10, 12, 14):
        category = 'fairway_driver' if speed < 10 else 'distance_driver'
        add(f"speed override {speed}", category, 'flight_speed', speed=speed,
            product_type='Distance Driver' if speed < 10 else 'Fairway Driver',
            catalog_category='Distance Driver' if speed < 10 else 'Fairway Driver')
    add('speed without category', 'fairway_driver', 'flight_speed', speed=7)
    for speed in (None, 'NaN', 'Infinity', 'false', -1, 5, 15):
        add(f"fallback speed {speed}", 'midrange', 'product_type', speed=speed, product_type='Midrange')
    add('unresolved speed conflict', 'putter', 'product_type', speed=7,
        flight_conflict_unresolved=True, product_type='Putter')
    add('invalid speed field', 'putter', 'product_type', speed=7,
        flight_invalid_fields=['speed'], product_type='Putter')
    add('resolved speed disagreement', 'fairway_driver', 'flight_speed', speed=7,
        flight_conflict=True, product_type='Distance Driver')
    return cases


def classify_category_fixtures(client, rows):
    """Run production classification SQL on a small isolated set of listings."""
    columns = ", ".join(f"JSON_VALUE(row, '$.{name}') AS {name}" for name in category_regression_cases()[0][0])
    source = f"""(SELECT {columns}, JSON_VALUE(row, '$.id') AS source_variant_key,
      JSON_VALUE(row, '$.id') AS flight_record_key, 'fixture' AS flight_scope,
      SAFE_CAST(JSON_VALUE(row, '$.speed') AS FLOAT64) AS speed,
      5.0 AS glide, -1.0 AS turn, 2.0 AS fade,
      'fixture' AS flight_source, 'fixture' AS flight_evidence,
      COALESCE(SAFE_CAST(JSON_VALUE(row, '$.flight_conflict') AS BOOL), FALSE) AS flight_conflict,
      COALESCE(SAFE_CAST(JSON_VALUE(row, '$.flight_conflict_unresolved') AS BOOL), FALSE) AS flight_conflict_unresolved,
      COALESCE(JSON_VALUE_ARRAY(row, '$.flight_invalid_fields'), ARRAY<STRING>[]) AS flight_invalid_fields,
      170.0 AS normalized_weight_g, CAST(NULL AS FLOAT64) AS normalized_weight_min_g,
      CAST(NULL AS FLOAT64) AS normalized_weight_max_g, 'fixture' AS weight_source,
      'fixture' AS weight_evidence, 1.0 AS weight_confidence,
      TRUE AS IsDistanceDriver, FALSE AS IsFairwayDriver, FALSE AS IsMidrange, FALSE AS IsPutter
      FROM UNNEST(JSON_QUERY_ARRAY(@category_fixtures)) row)"""
    model = build_model_classification_query("fixture_inputs")
    joined = """(SELECT inputs.*, model.* EXCEPT(flight_record_key, normalized_manufacturer,
      normalized_model, flight_scope, speed, glide, turn, fade, flight_source, flight_evidence,
      flight_conflict_unresolved) FROM fixture_inputs inputs
      JOIN fixture_models model USING(flight_record_key))"""
    sql = f"WITH fixture_inputs AS (SELECT * FROM {source}), fixture_models AS ({model})\n"
    sql += "SELECT * FROM (" + build_variant_classification_query(joined) + ")"
    config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter(
        "category_fixtures", "STRING", json.dumps(rows))])
    return {row["id"]: row for row in client.query(sql, job_config=config).result()}


def validate_category_regressions(client):
    """Run the real classification SQL on synthetic inputs before any rebuild."""
    cases = category_regression_cases()
    actual = classify_category_fixtures(client, [row for row, _, _ in cases])
    if len(actual) != len(cases):
        raise AssertionError("Category fixture row count mismatch")
    for row, category, source_name in cases:
        result = actual[row["id"]]
        if (result["disc_category"], result["category_source"]) != (category, source_name):
            raise AssertionError(f"Category regression {row['id']}: "
                                 f"{result['disc_category']}/{result['category_source']}, "
                                 f"expected {category}/{source_name}")
        if source_name not in ("flight_speed", "try_discs_category", "retailer_model_consensus"):
            local = extract_category(row["product_type"], row["tags"], row["title"], row["BodyHtml"])
            if local[:2] != (category, source_name):
                raise AssertionError(f"Python/SQL category mismatch: {row['id']}")
    print(f"Passed {len(cases)} BigQuery category regression cases.", flush=True)
    validate_category_audit(client)
    return len(cases)


def validate_production_categories(client, project, dataset):
    """Fail closed before publication if flags or title-confirmed Dart/Fox regress."""
    mismatches = " OR ".join(
        f"{flag} IS DISTINCT FROM COALESCE(disc_category = '{category}', FALSE)"
        for flag, category in LEGACY_CATEGORY_FLAGS.items()
    )
    sql = f"""
      ASSERT (SELECT COUNTIF({mismatches}) = 0 FROM `{project}.{dataset}.VariantState`)
        AS 'Legacy flags disagree with final disc_category';
      ASSERT (SELECT COUNTIF(item_type != 'disc' AND disc_category IS NOT NULL) = 0
        FROM `{project}.{dataset}.VariantState`) AS 'Non-disc has a disc category';
      ASSERT (SELECT COUNTIF(
          (speed >= 6 AND speed < 10 AND disc_category IS DISTINCT FROM 'fairway_driver')
          OR (speed >= 10 AND speed <= 14 AND disc_category IS DISTINCT FROM 'distance_driver')) = 0
        FROM `{project}.{dataset}.VariantState`
        WHERE item_type = 'disc' AND NOT COALESCE(flight_conflict_unresolved, FALSE)
          AND data_status != 'unsupported_flight') AS 'Driver category disagrees with flight speed';
      CREATE TEMP TABLE category_targets AS
      SELECT *, IF(LOWER(normalized_model) = 'dart', 'putter', 'midrange') AS expected_category
      FROM `{project}.{dataset}.VariantState`
      WHERE LOWER(normalized_manufacturer) IN ('innova', 'innova champion discs')
        AND LOWER(normalized_model) IN ('dart', 'fox') AND item_type = 'disc'
        AND REGEXP_CONTAINS(LOWER(title), CONCAT(r'\\b', LOWER(normalized_model), r'\\b'));
      ASSERT (SELECT COUNT(DISTINCT LOWER(normalized_model)) = 2 FROM category_targets)
        AS 'Dart/Fox category regression targets are missing';
      ASSERT (SELECT COUNTIF(disc_category IS DISTINCT FROM expected_category) = 0 FROM category_targets)
        AS 'Title-confirmed Dart/Fox category regression';
      SELECT normalized_model, disc_category, COUNT(*) AS variants
      FROM category_targets GROUP BY 1, 2 ORDER BY 1;
    """
    result = [dict(row.items()) for row in client.query(sql).result()]
    print(json.dumps({"category_validation": result}), flush=True)
    return result


def validate_category_source_joins(client):
    """Exercise identical IDs across two Shopify stores and Infinite in BigQuery."""
    from disc_golf_pipeline.services.process_data import build_derived_product_type_sql, build_variant_snapshot_query
    from disc_golf_pipeline.services.source_views import build_source_view_sqls
    from disc_golf_pipeline.services.disc_classification import CLASSIFICATION_FIELDS
    from disc_golf_pipeline.services.indexer import assert_disc_fields_match, build_document

    prefix = "fixture.category"
    fixtures = """
      ProductInfo AS (
        SELECT 1 AS MainProductId, 'A' AS Store, 'Mid-Range Drivers' AS ProductType,
          '[]' AS Tags, 'A body' AS BodyHtml, 'Fox' AS Title, 'fox' AS Handle,
          'https://a/products/fox' AS ProductLink, 'Innova' AS Vendor
        UNION ALL SELECT 1, 'B', 'Putt & Approach', '[]', 'B body', 'Dart', 'dart',
          'https://b/products/dart', 'Innova'
      ), InfiniteDiscs AS (
        SELECT 1 AS Id, 'Fixture' AS ManufacturerName, 'Plastic' AS PlasticName,
          'Model' AS ModelName, 'This is a distance driver.' AS ModelDescription,
          'Stamp' AS AdditionalInputTitle, 'Blue' AS ColorName, 19.99 AS StockPrice,
          170 AS StockWeight, 1 AS AvailableStock, 'https://image' AS StockImage,
          'https://infinite/model' AS ModelLink
      ), Products AS (
        SELECT 1 AS MainProductId, 2 AS VariantId, 'https://image' AS VarFeaturedImage,
          CURRENT_TIMESTAMP() AS VariantUpdatedAt, TRUE AS VariantAvailable,
          'Blue' AS VariantTitle, 19.99 AS VariantPrice, 170 AS VariantGrams
      ), Stores AS (
        SELECT 'A' AS StoreName, 'https://a/' AS URL UNION ALL SELECT 'B', 'https://b/'
      )
    """

    def query_part(statement):
        query = statement.partition(" AS\n")[2]
        for table in ("ProductInfo", "InfiniteDiscs", "Products", "Stores", "DerivedProductType"):
            query = query.replace(f"`{prefix}.{table}`", table)
        return query

    derived = query_part(build_derived_product_type_sql("fixture", "category"))
    shopify, infinite = [query_part(sql) for sql in build_source_view_sqls("fixture", "category")]
    sql = (f"WITH {fixtures}, DerivedProductType AS ({derived}), "
           f"shopify AS ({shopify}), infinite AS ({infinite}) "
           "SELECT store, BodyHtml, IsMidrange, IsPutter, IsDistanceDriver, IsFairwayDriver FROM shopify "
           "UNION ALL SELECT store, BodyHtml, IsMidrange, IsPutter, IsDistanceDriver, IsFairwayDriver FROM infinite")
    rows = [dict(row.items()) for row in client.query(sql).result()]
    expected = {
        "A": ("A body", 1, 0, 0, 0),
        "B": ("B body", 0, 1, 0, 0),
        "infinitediscs": ("This is a distance driver.", 0, 0, 1, 0),
    }
    actual = {row["store"]: tuple(row[key] for key in
              ("BodyHtml", "IsMidrange", "IsPutter", "IsDistanceDriver", "IsFairwayDriver")) for row in rows}
    if len(rows) != 3 or actual != expected:
        raise AssertionError(f"Category source/store isolation failed: {rows}")
    print("Passed BigQuery source/store ID collision regression.", flush=True)

    # Exercise the real snapshot projection with deliberately contaminated flags,
    # including unknown and non-disc rows, then the Typesense transport contract.
    fields = []
    for name, kind in CLASSIFICATION_FIELDS:
        value = ("NULLIF(test_category, 'unknown')" if name == "disc_category" else
                 "ARRAY<STRING>[]" if kind == "ARRAY<STRING>" else f"CAST(NULL AS {kind})")
        fields.append(f"{value} AS {name}")
    snapshot = build_variant_snapshot_query("fixture", "category")
    for table in ("NormalizedVariantSnapshot", "NormalizedDiscAttributes", "NormalizedDiscClassifications"):
        snapshot = snapshot.replace(f"`{prefix}.{table}`", table)
    sql = f"""WITH {fixtures}, DerivedProductType AS ({derived}), shopify AS ({shopify}),
      NormalizedVariantSnapshot AS (
        SELECT raw.* EXCEPT(id, variant_id, IsPutter, IsMidrange, IsFairwayDriver, IsDistanceDriver),
          CONCAT(id, ':', test_category) AS id, CONCAT(id, ':', test_category) AS source_variant_key,
          CONCAT(variant_id, ':', test_category) AS variant_id,
          1 AS IsPutter, 1 AS IsMidrange, 1 AS IsFairwayDriver, 1 AS IsDistanceDriver,
          'shopify' AS source, 'fixture' AS retailer, 'Fixture' AS raw_vendor,
          'Fixture' AS normalized_manufacturer, 'Fixture' AS normalized_model,
          IF(test_category = 'non-disc', 'bag', 'disc') AS item_type,
          test_category != 'non-disc' AS is_disc, 1.0 AS item_type_confidence,
          1.0 AS manufacturer_confidence, 1.0 AS model_confidence, 1.0 AS normalization_confidence,
          'fixture' AS normalization_source, 'fixture' AS model_decision_level,
          'fixture' AS normalization_version, 'fixture' AS model_rules_version, test_category
        FROM shopify raw CROSS JOIN UNNEST(
          ['putter', 'midrange', 'fairway_driver', 'distance_driver', 'unknown', 'non-disc']) test_category
      ), NormalizedDiscAttributes AS (
        SELECT id, 170 AS normalized_weight_g, 7.0 AS speed, 5.0 AS glide, -1.0 AS turn,
          2.0 AS fade, 1.0 AS flight_confidence, 'fixture' AS flight_source,
          'fixture' AS flight_evidence, 'fixture' AS flight_attribution
        FROM NormalizedVariantSnapshot WHERE item_type = 'disc'
      ), NormalizedDiscClassifications AS (
        SELECT id, {', '.join(fields)} FROM NormalizedVariantSnapshot WHERE item_type = 'disc'
      ) SELECT * FROM ({snapshot})
    """
    rows = list(client.query(sql).result())
    if len(rows) != 12:
        raise AssertionError("Category snapshot fixture row count mismatch")
    for row in rows:
        expected_category = row["id"].rsplit(":", 1)[1]
        if expected_category in ("unknown", "non-disc"):
            expected_category = None
        if row["disc_category"] != expected_category:
            raise AssertionError(f"Snapshot category changed for {row['id']}")
        assert_disc_fields_match(row, build_document(row))
    print("Passed 12 BigQuery snapshot/Typesense category flag fixtures.", flush=True)
    validate_model_identity_regressions(client)
