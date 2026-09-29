"""Versioned, deterministic SQL classifications over accepted disc attributes.

Scores are application heuristics for low-power throwing, not probabilities or
exclusive player skill levels. No API or LLM is called by this module.
"""

ALGORITHM_VERSION = "disc-classification-0.1"

# One transport contract for snapshot/state and indexer projections. The SQL
# array type is converted to a REPEATED field when migrating BigQuery tables.
CLASSIFICATION_FIELDS = (
    ("power_band", "STRING"), ("turn_band", "STRING"),
    ("fade_band", "STRING"), ("glide_band", "STRING"),
    ("approx_stability", "STRING"),
    ("access_model", "FLOAT64"), ("access_variant", "FLOAT64"),
    ("access_low", "FLOAT64"), ("access_high", "FLOAT64"),
    ("beginner_role", "STRING"), ("has_exact_weight", "BOOL"),
    ("weight_status", "STRING"), ("weight_min_g", "FLOAT64"),
    ("weight_max_g", "FLOAT64"), ("weight_confidence", "FLOAT64"),
    ("weight_source", "STRING"), ("weight_evidence", "STRING"),
    ("disc_category", "STRING"), ("category_source", "STRING"),
    ("category_evidence", "STRING"), ("algorithm_version", "STRING"),
    ("data_status", "STRING"), ("reason_codes", "ARRAY<STRING>"),
    ("classification_input_hash", "STRING"),
    ("flight_conflict", "BOOL"), ("flight_conflict_unresolved", "BOOL"),
)


def classification_columns(alias="", indent=2, casts=False):
    prefix = f"{alias}." if alias else ""
    expressions = []
    for name, kind in CLASSIFICATION_FIELDS:
        value = f"{prefix}{name}"
        if casts:
            value = (f"COALESCE({value}, ARRAY<STRING>[]) AS {name}"
                     if kind == "ARRAY<STRING>" else f"CAST({value} AS {kind}) AS {name}")
        expressions.append(" " * indent + value)
    return ",\n".join(expressions)


def _clip(expression, low=0, high=1):
    return f"LEAST({high}, GREATEST({low}, {expression}))"


def _number(expression):
    # Casting via STRING prevents BOOL -> 1/0 conversion by BigQuery.
    return f"SAFE_CAST(CAST({expression} AS STRING) AS FLOAT64)"


def _valid(expression, low, high):
    return (f"({expression} BETWEEN {low} AND {high} "
            f"AND NOT IS_NAN({expression}) AND NOT IS_INF({expression}))")


def build_classification_inputs_sql(project_id, dataset):
    return f"""
CREATE OR REPLACE VIEW `{project_id}.{dataset}.DiscClassificationInputs` AS
WITH inputs AS (
  SELECT attrs.*, src.source_variant_key, src.normalized_manufacturer,
    src.normalized_model, src.product_type,
    src.IsDistanceDriver, src.IsFairwayDriver, src.IsMidrange, src.IsPutter,
    CASE
      WHEN STARTS_WITH(COALESCE(attrs.flight_source, ''), 'try_discs_')
        AND NULLIF(TRIM(src.normalized_manufacturer), '') IS NOT NULL
        AND NULLIF(TRIM(src.normalized_model), '') IS NOT NULL
        THEN CONCAT('catalog:', src.normalized_manufacturer, ':', src.normalized_model)
      ELSE CONCAT('source:', COALESCE(NULLIF(src.source_variant_key, ''), attrs.id))
    END AS flight_scope
  FROM `{project_id}.{dataset}.NormalizedDiscAttributes` attrs
  JOIN `{project_id}.{dataset}.NormalizedVariantSnapshot` src USING (id)
)
SELECT *, TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
  normalized_manufacturer, normalized_model, flight_scope,
  speed, glide, turn, fade, flight_source, flight_evidence,
  flight_conflict_unresolved, flight_invalid_fields
)))) AS flight_record_key
FROM inputs
"""


def build_model_classification_query(source):
    """SELECT over a relation containing one row per rating record."""
    fields = (("speed", 1, 14), ("glide", 1, 7), ("turn", -5, 1), ("fade", 0, 5))
    parsing = ",\n    ".join(f"{_number(name)} AS n_{name}" for name, _, _ in fields)
    validation = ",\n    ".join(
        f"IF({_valid('n_' + name, low, high)}, n_{name}, NULL) AS v_{name}"
        for name, low, high in fields
    )
    unsupported = " OR ".join(
        f"(({name} IS NOT NULL AND v_{name} IS NULL) OR '{name}' IN UNNEST(flight_invalid_fields))"
        for name, _, _ in fields
    )
    missing = " + ".join(f"IF(v_{name} IS NULL, 1, 0)" for name, _, _ in fields)
    reasons = ",\n      ".join(
        f"IF('{name}' IN UNNEST(flight_invalid_fields), 'unsupported_{name}', "
        f"IF({name} IS NULL, 'missing_{name}', "
        f"IF(v_{name} IS NULL, 'unsupported_{name}', NULL)))"
        for name, _, _ in fields
    )
    return f"""
WITH parsed AS (
  SELECT *, {parsing} FROM {source}
), validated AS (
  SELECT *, {validation} FROM parsed
), descriptors AS (
  SELECT *,
    CASE
      WHEN flight_conflict_unresolved THEN 'unresolved_conflict'
      WHEN {unsupported} THEN 'unsupported_flight'
      WHEN ({missing}) = 4 THEN 'missing_flight'
      WHEN ({missing}) > 0 THEN 'partial_flight'
      ELSE 'complete'
    END AS data_status,
    CASE WHEN v_speed IS NULL THEN NULL WHEN v_speed <= 5 THEN 'low'
      WHEN v_speed <= 9 THEN 'moderate' WHEN v_speed <= 12 THEN 'high'
      ELSE 'very_high' END AS raw_power_band,
    CASE WHEN v_turn IS NULL THEN NULL WHEN v_turn <= -3 THEN 'high_turn'
      WHEN v_turn <= -1 THEN 'moderate_turn' WHEN v_turn < 0 THEN 'mild_turn'
      ELSE 'turn_resistant' END AS raw_turn_band,
    CASE WHEN v_fade IS NULL THEN NULL WHEN v_fade <= 1 THEN 'gentle'
      WHEN v_fade < 3 THEN 'moderate' ELSE 'strong' END AS raw_fade_band,
    CASE WHEN v_glide IS NULL THEN NULL WHEN v_glide <= 3 THEN 'low'
      WHEN v_glide < 5 THEN 'moderate' ELSE 'high' END AS raw_glide_band,
    CASE WHEN v_turn IS NULL OR v_fade IS NULL THEN NULL
      WHEN v_fade >= 2 AND v_turn < -1 THEN 'turn_and_fade'
      WHEN v_fade >= 4 AND v_turn >= -1 THEN 'very_overstable'
      WHEN v_fade >= 2 AND v_turn >= -1 THEN 'overstable'
      WHEN v_fade < 2 AND v_turn <= -3 THEN 'very_understable'
      WHEN v_fade < 2 AND v_turn <= -1.5 THEN 'understable'
      ELSE 'neutral' END AS raw_stability,
    {_clip('(v_speed - 5) / 2.0')} AS driver_blend,
    {_clip('(13 - v_speed) / 9.0')} AS speed_access,
    {_clip('(v_glide - 2) / 4.0')} AS glide_support,
    {_clip('(3 - v_fade) / 3.0')} AS gentle_finish,
    {_clip('1 - GREATEST(v_turn, 0)')} AS slow_turn_support,
    {_clip('(1 - v_turn) / 3.0')} AS driver_turn_support
  FROM validated
), components AS (
  SELECT *, (1 - driver_blend) * slow_turn_support
      + driver_blend * driver_turn_support AS turn_support
  FROM descriptors
)
SELECT flight_record_key, normalized_manufacturer, normalized_model, flight_scope,
  speed, glide, turn, fade, flight_source, flight_evidence,
  flight_conflict_unresolved, data_status,
  IF(flight_conflict_unresolved, NULL, raw_power_band) AS power_band,
  IF(flight_conflict_unresolved, NULL, raw_turn_band) AS turn_band,
  IF(flight_conflict_unresolved, NULL, raw_fade_band) AS fade_band,
  IF(flight_conflict_unresolved, NULL, raw_glide_band) AS glide_band,
  IF(flight_conflict_unresolved, NULL, raw_stability) AS approx_stability,
  driver_blend, speed_access, glide_support, gentle_finish, turn_support,
  IF(data_status = 'complete',
    50 * speed_access + 20 * turn_support + 15 * gentle_finish + 15 * glide_support,
    NULL) AS access_model,
  ARRAY(SELECT reason FROM UNNEST([
      {reasons},
      IF(flight_conflict_unresolved, 'unresolved_flight_conflict', NULL)
    ]) reason WHERE reason IS NOT NULL) AS model_reason_codes,
  '{ALGORITHM_VERSION}' AS algorithm_version
FROM components
"""


def build_model_classifications_sql(project_id, dataset):
    relation = f"`{project_id}.{dataset}.DiscClassificationInputs`"
    source = f"""(SELECT DISTINCT flight_record_key, normalized_manufacturer,
      normalized_model, flight_scope, speed, glide, turn, fade, flight_source,
      flight_evidence, flight_conflict_unresolved, flight_invalid_fields FROM {relation})"""
    return (f"CREATE OR REPLACE TABLE `{project_id}.{dataset}.DiscModelClassifications` AS\n"
            + build_model_classification_query(source))


def _category_case(expression):
    return f"""CASE REGEXP_REPLACE(LOWER(TRIM(COALESCE({expression}, ''))), r'[^a-z]', '')
      WHEN 'putter' THEN 'putter' WHEN 'putters' THEN 'putter'
      WHEN 'puttapproach' THEN 'putter' WHEN 'approachdisc' THEN 'putter'
      WHEN 'midrange' THEN 'midrange' WHEN 'midrangediscs' THEN 'midrange'
      WHEN 'fairwaydriver' THEN 'fairway_driver' WHEN 'fairwaydrivers' THEN 'fairway_driver'
      WHEN 'controldriver' THEN 'fairway_driver'
      WHEN 'distancedriver' THEN 'distance_driver' WHEN 'distancedrivers' THEN 'distance_driver'
    END"""


def build_variant_classification_query(source):
    """Apply variant weights and policy to a relation with model outputs."""
    adjust_low = _clip("0.5 * (170 - weight_max_g)", -5, 10)
    adjust_high = _clip("0.5 * (170 - weight_min_g)", -5, 10)
    return f"""
WITH weights AS (
  SELECT *, {_number('normalized_weight_g')} AS w,
    {_number('normalized_weight_min_g')} AS w_min,
    {_number('normalized_weight_max_g')} AS w_max,
    {_category_case('catalog_category')} AS catalog_disc_category,
    {_category_case('product_type')} AS product_disc_category,
    ARRAY(SELECT category FROM UNNEST([
      IF(LOWER(CAST(IsDistanceDriver AS STRING)) IN ('true', '1'), 'distance_driver', NULL),
      IF(LOWER(CAST(IsFairwayDriver AS STRING)) IN ('true', '1'), 'fairway_driver', NULL),
      IF(LOWER(CAST(IsMidrange AS STRING)) IN ('true', '1'), 'midrange', NULL),
      IF(LOWER(CAST(IsPutter AS STRING)) IN ('true', '1'), 'putter', NULL)
    ]) category WHERE category IS NOT NULL) AS flag_categories
  FROM {source}
), weight_states AS (
  SELECT *, CASE
    WHEN w_min IS NOT NULL OR w_max IS NOT NULL
      OR normalized_weight_min_g IS NOT NULL OR normalized_weight_max_g IS NOT NULL THEN
      IF({_valid('w_min', 80, 220)} AND {_valid('w_max', 80, 220)}
        AND w_min <= w_max AND normalized_weight_g IS NULL, 'range', 'invalid')
    WHEN {_valid('w', 80, 220)} THEN 'exact'
    WHEN normalized_weight_g IS NOT NULL OR STARTS_WITH(COALESCE(weight_source, ''), 'rejected_')
      THEN 'invalid'
    ELSE 'missing' END AS weight_status
  FROM weights
), bounds AS (
  SELECT *, weight_status = 'exact' AS has_exact_weight,
    CASE weight_status WHEN 'exact' THEN w WHEN 'range' THEN w_min END AS weight_min_g,
    CASE weight_status WHEN 'exact' THEN w WHEN 'range' THEN w_max END AS weight_max_g,
    COALESCE(catalog_disc_category, product_disc_category,
      IF(ARRAY_LENGTH(flag_categories) = 1, flag_categories[SAFE_OFFSET(0)], NULL)) AS disc_category,
    CASE WHEN catalog_disc_category IS NOT NULL THEN 'try_discs_category'
      WHEN product_disc_category IS NOT NULL THEN 'product_type'
      WHEN ARRAY_LENGTH(flag_categories) = 1 THEN 'source_type_flag' END AS category_source,
    CASE WHEN catalog_disc_category IS NOT NULL THEN catalog_category
      WHEN product_disc_category IS NOT NULL THEN product_type
      WHEN ARRAY_LENGTH(flag_categories) = 1 THEN flag_categories[SAFE_OFFSET(0)] END AS category_evidence
  FROM weight_states
), scores AS (
  SELECT *,
    IF(has_exact_weight, {_clip('access_model + driver_blend * ' + adjust_low, 0, 100)}, NULL)
      AS access_variant,
    {_clip('access_model + driver_blend * IF(weight_min_g IS NULL, -5, ' + adjust_low + ')', 0, 100)}
      AS access_low,
    {_clip('access_model + driver_blend * IF(weight_min_g IS NULL, 10, ' + adjust_high + ')', 0, 100)}
      AS access_high,
    IF(has_exact_weight, driver_blend * {adjust_low}, NULL) AS weight_adjustment
  FROM bounds
)
SELECT id, source_variant_key, flight_record_key,
  power_band, turn_band, fade_band, glide_band, approx_stability,
  access_model, access_variant, access_low, access_high,
  CASE WHEN access_model IS NULL OR flight_conflict_unresolved THEN 'unknown'
    WHEN access_low >= 70 AND {_number('speed')} <= 9
      AND {_number('turn')} BETWEEN -2.5 AND 0 AND {_number('fade')} <= 2
      THEN 'general_candidate'
    WHEN access_high >= 55 AND {_number('turn')} <= 0 AND {_number('fade')} <= 2 THEN 'conditional_candidate'
    ELSE 'not_default' END AS beginner_role,
  has_exact_weight, weight_status, weight_min_g, weight_max_g,
  weight_confidence, weight_source, weight_evidence,
  disc_category, category_source, category_evidence, algorithm_version, data_status,
  ARRAY_CONCAT(model_reason_codes, ARRAY(SELECT reason FROM UNNEST([
    IF({_number('speed')} > 9, 'higher_speed', NULL), IF({_number('turn')} < -2.5, 'high_turn', NULL),
    IF({_number('fade')} > 2, 'stronger_finish', NULL),
    IF(NOT has_exact_weight, 'weight_not_exact', NULL),
    IF(weight_status = 'invalid', 'invalid_weight', NULL),
    IF(flight_conflict AND NOT flight_conflict_unresolved, 'resolved_source_disagreement', NULL),
    IF(disc_category IS NULL, 'category_unknown', NULL)
  ]) reason WHERE reason IS NOT NULL)) AS reason_codes,
  TO_HEX(SHA256(TO_JSON_STRING(STRUCT(flight_record_key, algorithm_version,
    normalized_weight_g, normalized_weight_min_g, normalized_weight_max_g,
    weight_source, weight_evidence, flight_conflict, disc_category,
    category_source, category_evidence)))) AS classification_input_hash,
  flight_conflict, flight_conflict_unresolved,
  driver_blend, speed_access, glide_support, gentle_finish, turn_support, weight_adjustment
FROM scores
"""


def build_variant_classifications_sql(project_id, dataset):
    source = f"""(
      SELECT attrs.*, model.* EXCEPT(flight_record_key, normalized_manufacturer,
        normalized_model, flight_scope, speed, glide, turn, fade,
        flight_source, flight_evidence, flight_conflict_unresolved)
      FROM `{project_id}.{dataset}.DiscClassificationInputs` attrs
      JOIN `{project_id}.{dataset}.DiscModelClassifications` model USING (flight_record_key)
    )"""
    return (f"CREATE OR REPLACE VIEW `{project_id}.{dataset}.NormalizedDiscClassifications` AS\n"
            + build_variant_classification_query(source))


def refresh_disc_classifications(client, project_id, dataset):
    """Refresh classifications from accepted attributes, without external calls."""
    for builder in (build_classification_inputs_sql, build_model_classifications_sql,
                    build_variant_classifications_sql):
        client.query(builder(project_id, dataset)).result()
    client.query(f"""
      ASSERT (SELECT COUNT(*) = COUNT(DISTINCT id)
        FROM `{project_id}.{dataset}.NormalizedDiscClassifications`)
        AS 'Duplicate disc classification variants';
      ASSERT (SELECT COUNT(*) FROM `{project_id}.{dataset}.NormalizedDiscClassifications`)
        = (SELECT COUNT(*) FROM `{project_id}.{dataset}.NormalizedVariantSnapshot`
           WHERE item_type = 'disc') AS 'Disc classification coverage mismatch';
    """).result()


def run_disc_classification(client, project_id, dataset):
    """Refresh from normalized inputs and cached decisions; no API/LLM calls."""
    from disc_golf_pipeline.services.disc_attributes import build_disc_attributes_view_sql
    from disc_golf_pipeline.services.disc_weight_llm import build_review_table_sql
    from disc_golf_pipeline.services.try_discs_sync import ensure_match_schema

    ensure_match_schema(client, project_id, dataset)
    client.query(build_review_table_sql(project_id, dataset)).result()
    client.query(build_disc_attributes_view_sql(project_id, dataset)).result()
    refresh_disc_classifications(client, project_id, dataset)
    return {"dataset": f"{project_id}.{dataset}", "algorithm_version": ALGORITHM_VERSION}
