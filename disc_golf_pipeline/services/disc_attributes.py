"""Build the disc-only attribute layer over normalized variant rows."""

from disc_golf_pipeline.services.disc_weight_llm import PROMPT_VERSION


FLIGHT_SEQUENCE_PATTERN = (
    r"(?i)\bflight(?:\s+(?:numbers|ratings))?\s*[:=-]?\s*"
    r"(-?\d+(?:\.\d+)?\s*[/,|]\s*-?\d+(?:\.\d+)?\s*[/,|]\s*"
    r"-?\d+(?:\.\d+)?\s*[/,|]\s*-?\d+(?:\.\d+)?)"
)
BARE_FLIGHT_SEQUENCE_PATTERN = (
    r"(?i)\b(-?\d+(?:\.\d+)?\s*[/,|]\s*-?\d+(?:\.\d+)?\s*[/,|]\s*"
    r"-?\d+(?:\.\d+)?\s*[/,|]\s*-?\d+(?:\.\d+)?)\b"
)
WEIGHT_PATTERN = r"(?i)\bweight\s*[:=-]\s*(\d+(?:\.\d+)?)\s*(?:g|grams?)\b"
NON_VARIANT_WEIGHT_PATTERN = (
    r"(?i)\b(?:max(?:imum)?|legal|approved)\.?\s+(?:disc\s+)?"
    r"weight\s*[:=-]\s*\d+(?:\.\d+)?\s*(?:g|grams?)\b"
)
VARIANT_WEIGHT_PATTERN = r"(?i)(?:^|[^\d])(\d{3}(?:\.\d+)?)\s*(?:g|grams?)\b"
VARIANT_WEIGHT_BARE_PATTERN = (
    r"(?i)(?:^|[^0-9])((?:1[0-8][0-9]|190)(?:\.\d+)?)\b"
)
WEIGHT_RANGE_PATTERN = (
    r"(?i)(?:^|[^0-9])(?:1[0-8][0-9]|190)(?:\.\d+)?"
    r"(?:\s*[-–]\s*(?:(?:1[0-8][0-9]|190)|[0-9]{1,2})|\s*\+)"
)
TITLE_WEIGHT_RANGE_PATTERN = (
    r"(?i)(?:^|[^0-9])((?:1[0-8][0-9]|190)(?:\.\d+)?\s*[-–]\s*"
    r"(?:(?:1[0-8][0-9]|190)(?:\.\d+)?|[0-9]{1,2})\s*(?:g|grams?)\b)"
)
LEADING_TITLE_WEIGHT_RANGE_PATTERN = (
    r"(?i)^\s*(?:#\d+\s+)?((?:1[0-8][0-9]|190)(?:\.\d+)?\s*[-–]\s*"
    r"(?:(?:1[0-8][0-9]|190)(?:\.\d+)?|[0-9]{1,2}))(?:\s|$)"
)


def _weight_range_max_sql(expression):
    # Interpret 173-5 as 173-175 and 165-70 as 165-170. Do not guess a
    # rollover for reversed endpoints (e.g. 175-3); normal validation rejects it.
    lower = f"SAFE_CAST(REGEXP_EXTRACT({expression}, r'^(\\d+(?:\\.\\d+)?)') AS FLOAT64)"
    upper_text = f"REGEXP_EXTRACT({expression}, r'[-–]\\s*(\\d+(?:\\.\\d+)?)')"
    upper = f"SAFE_CAST({upper_text} AS FLOAT64)"
    scale = f"POW(10, LENGTH({upper_text}))"
    return f"IF(REGEXP_CONTAINS({upper_text}, r'^\\d{{1,2}}$'), FLOOR({lower} / {scale}) * {scale} + {upper}, {upper})"


def _flight_is_valid(array_name):
    values = [f"{array_name}[SAFE_OFFSET({index})]" for index in range(4)]
    return (
        f"ARRAY_LENGTH({array_name}) = 4 "
        f"AND {values[0]} BETWEEN 1 AND 15 "
        f"AND {values[1]} BETWEEN 0 AND 7 "
        f"AND {values[2]} BETWEEN -5 AND 2 "
        f"AND {values[3]} BETWEEN 0 AND 5"
    )


def _flight_has_values(array_name):
    return f"EXISTS(SELECT 1 FROM UNNEST({array_name}) value WHERE value IS NOT NULL)"


def _arrays_disagree(left, right):
    return "(" + " OR ".join(
        f"({left}[SAFE_OFFSET({index})] IS NOT NULL "
        f"AND {right}[SAFE_OFFSET({index})] IS NOT NULL "
        f"AND {left}[SAFE_OFFSET({index})] != {right}[SAFE_OFFSET({index})])"
        for index in range(4)
    ) + ")"


def _label_array(text_name):
    fields = ("speed", "glide", "turn", "fade")
    return "[" + ", ".join(
        f"SAFE_CAST(REGEXP_EXTRACT({text_name}, "
        f"r'(?i)\\b{field}\\s*[:=-]\\s*(-?\\d+(?:\\.\\d+)?)') AS FLOAT64)"
        for field in fields
    ) + "]"


def _sequence_array(text_name):
    return (
        "ARRAY(SELECT SAFE_CAST(token AS FLOAT64) "
        f"FROM UNNEST(REGEXP_EXTRACT_ALL({text_name}, "
        "r'-?\\d+(?:\\.\\d+)?')) AS token WITH OFFSET AS position "
        "ORDER BY position)"
    )


def _label_evidence(text_name):
    matches = (
        f"REGEXP_EXTRACT({text_name}, "
        f"r'(?i)\\b{field}\\s*[:=-]\\s*-?\\d+(?:\\.\\d+)?')"
        for field in ("speed", "glide", "turn", "fade")
    )
    return "ARRAY_TO_STRING([" + ", ".join(matches) + "], '; ')"


def build_disc_attributes_view_sql(project_id, dataset):
    """Choose validated flight and weight evidence for classified discs."""
    snapshot = f"`{project_id}.{dataset}.NormalizedVariantSnapshot`"
    destination = f"`{project_id}.{dataset}.NormalizedDiscAttributes`"
    try_discs_matches = f"`{project_id}.{dataset}.TryDiscsModelMatches`"
    weight_reviews = f"`{project_id}.{dataset}.DiscWeightLlmReviews`"
    candidates = (
        ("html_labels", _label_evidence("html_text"), 0.95),
        ("html_sequence", "html_flight_text", 0.92),
        ("html_bare_sequence", "html_bare_flight_text", 0.80),
        ("tags_labels", _label_evidence("tags"), 0.85),
        ("tags_sequence", "tags_flight_text", 0.82),
        ("title_sequence", "title_flight_text", 0.75),
    )
    valid_cases = "\n".join(
        f"    WHEN {_flight_is_valid(name + '_numbers')} THEN '{name}'"
        for name, _, _ in candidates
    )
    # Keep a single partial/unsupported record if no complete conventional set
    # exists. Never fill individual gaps from a different candidate.
    valid_cases += "\n" + "\n".join(
        f"    WHEN {_flight_has_values(name + '_numbers')} THEN '{name}'"
        for name, _, _ in candidates
    )
    flight_array_cases = "\n".join(
        f"    WHEN flight_source = '{name}' THEN {name}_numbers"
        for name, _, _ in candidates
    )
    flight_evidence_cases = "\n".join(
        f"    WHEN flight_source = '{name}' THEN {evidence}"
        for name, evidence, _ in candidates
    )
    flight_confidence_cases = "\n".join(
        f"    WHEN flight_source = '{name}' THEN {confidence}"
        for name, _, confidence in candidates
    )
    flight_disagreements = "\n      OR ".join(
        _arrays_disagree(name + "_numbers", "flight_numbers")
        for name, _, _ in candidates
    )
    catalog_disagreement = _arrays_disagree(
        "[catalog_speed, catalog_glide, catalog_turn, catalog_fade]", "flight_numbers")
    local_records = ",\n      ".join(
        f"STRUCT('{name}' AS source, {name}_numbers AS numbers, {evidence} AS evidence)"
        for name, evidence, _ in candidates
    )

    return f"""
CREATE OR REPLACE VIEW {destination} AS
WITH source_rows AS (
  SELECT
    id,
    item_type,
    normalized_manufacturer,
    normalized_model,
    SAFE_CAST(weight_g AS FLOAT64) AS raw_weight_g,
    COALESCE(variant_title, '') AS variant_title,
    COALESCE(title, '') AS title,
    COALESCE(tags, '') AS tags,
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(COALESCE(BodyHtml, ''), r'<[^>]*>', ' '),
      r'(?i)&nbsp;|&#160;', ' '
    )) AS html_text
  FROM {snapshot}
  WHERE item_type = 'disc'
),
text_matches AS (
  SELECT *,
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
      variant_title, html_text, raw_weight_g
    )))) AS weight_evidence_hash,
    REGEXP_EXTRACT(html_text, r'{FLIGHT_SEQUENCE_PATTERN}') AS html_flight_text,
    REGEXP_EXTRACT(html_text, r'{BARE_FLIGHT_SEQUENCE_PATTERN}') AS html_bare_flight_text,
    COALESCE(
      REGEXP_EXTRACT(tags, r'{FLIGHT_SEQUENCE_PATTERN}'),
      REGEXP_EXTRACT(tags, r'{BARE_FLIGHT_SEQUENCE_PATTERN}')
    ) AS tags_flight_text,
    COALESCE(
      REGEXP_EXTRACT(CONCAT(title, ' ', variant_title),
        r'{FLIGHT_SEQUENCE_PATTERN}'),
      REGEXP_EXTRACT(CONCAT(title, ' ', variant_title),
        r'{BARE_FLIGHT_SEQUENCE_PATTERN}')
    ) AS title_flight_text,
    REGEXP_EXTRACT(variant_title, r'{VARIANT_WEIGHT_PATTERN}') AS variant_weight_text,
    COALESCE(
      REGEXP_EXTRACT(variant_title, r'{TITLE_WEIGHT_RANGE_PATTERN}'),
      REGEXP_EXTRACT(variant_title, r'{LEADING_TITLE_WEIGHT_RANGE_PATTERN}')
    ) AS title_weight_range_text,
    REGEXP_EXTRACT_ALL(variant_title, r'{VARIANT_WEIGHT_BARE_PATTERN}') AS bare_weight_texts,
    REGEXP_EXTRACT_ALL(
      REGEXP_REPLACE(html_text, r'{NON_VARIANT_WEIGHT_PATTERN}', ' '),
      r'{WEIGHT_PATTERN}') AS html_weight_texts
  FROM source_rows
),
parsed AS (
  SELECT *,
    {_label_array('html_text')} AS html_labels_numbers,
    {_sequence_array('html_flight_text')} AS html_sequence_numbers,
    {_sequence_array('html_bare_flight_text')} AS html_bare_sequence_numbers,
    {_label_array('tags')} AS tags_labels_numbers,
    {_sequence_array('tags_flight_text')} AS tags_sequence_numbers,
    {_sequence_array('title_flight_text')} AS title_sequence_numbers,
    IF(NOT REGEXP_CONTAINS(variant_title, r'{WEIGHT_RANGE_PATTERN}'),
      SAFE_CAST(variant_weight_text AS FLOAT64), NULL) AS variant_weight_g,
    IF(ARRAY_LENGTH(bare_weight_texts) = 1
      AND NOT REGEXP_CONTAINS(variant_title, r'{WEIGHT_RANGE_PATTERN}'),
      SAFE_CAST(bare_weight_texts[SAFE_OFFSET(0)] AS FLOAT64), NULL) AS bare_variant_weight_g,
    IF(ARRAY_LENGTH(html_weight_texts) = 1,
      SAFE_CAST(html_weight_texts[SAFE_OFFSET(0)] AS FLOAT64), NULL) AS html_weight_g,
    SAFE_CAST(REGEXP_EXTRACT(title_weight_range_text,
      r'^(\\d+(?:\\.\\d+)?)') AS FLOAT64) AS title_weight_min_g,
    {_weight_range_max_sql('title_weight_range_text')} AS title_weight_max_g
  FROM text_matches
),
selected AS (
  SELECT *,
    CASE
{valid_cases}
    END AS flight_source,
    CASE
      WHEN variant_weight_g BETWEEN 100 AND 190 THEN 'variant_title'
      WHEN title_weight_min_g BETWEEN 100 AND 190
        AND title_weight_max_g BETWEEN title_weight_min_g AND 190
        THEN 'variant_title_range'
      WHEN bare_variant_weight_g BETWEEN 100 AND 190 THEN 'variant_title_unlabelled'
      WHEN html_weight_g BETWEEN 100 AND 190 THEN 'body_html'
      WHEN raw_weight_g BETWEEN 100 AND 190 THEN 'source_weight_g'
      WHEN raw_weight_g IS NOT NULL THEN 'rejected_source_weight_g'
    END AS weight_source
  FROM parsed
),
resolved AS (
  SELECT *,
    CASE
{flight_array_cases}
    END AS flight_numbers,
    CASE
{flight_evidence_cases}
    END AS flight_evidence,
    CASE
{flight_confidence_cases}
    END AS flight_confidence,
    CASE weight_source
      WHEN 'variant_title' THEN variant_weight_g
      WHEN 'variant_title_unlabelled' THEN bare_variant_weight_g
      WHEN 'body_html' THEN html_weight_g
      WHEN 'source_weight_g' THEN raw_weight_g
    END AS normalized_weight_value,
    CASE weight_source
      WHEN 'variant_title' THEN variant_weight_text
      WHEN 'variant_title_range' THEN CONCAT(
        title_weight_range_text,
        CASE
          WHEN raw_weight_g IS NOT NULL
            AND NOT raw_weight_g BETWEEN 100 AND 190 THEN
            CONCAT('; rejected source_weight_g=', CAST(raw_weight_g AS STRING))
          WHEN raw_weight_g IS NOT NULL
            AND (raw_weight_g < title_weight_min_g
              OR raw_weight_g > title_weight_max_g) THEN
            CONCAT('; source_weight_g=', CAST(raw_weight_g AS STRING),
              ' outside title range')
          ELSE ''
        END)
      WHEN 'variant_title_unlabelled' THEN bare_weight_texts[SAFE_OFFSET(0)]
      WHEN 'body_html' THEN html_weight_texts[SAFE_OFFSET(0)]
      WHEN 'source_weight_g' THEN CAST(raw_weight_g AS STRING)
      WHEN 'rejected_source_weight_g' THEN CAST(raw_weight_g AS STRING)
    END AS weight_evidence
  FROM selected
),
active_weight_reviews AS (
  SELECT id, evidence_hash, status, weight_g, evidence
  FROM {weight_reviews}
  WHERE prompt_version = '{PROMPT_VERSION}'
    AND status IN ('FOUND', 'NONE')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id, evidence_hash ORDER BY reviewed_at DESC
  ) = 1
),
matched AS (
  SELECT
    resolved.*,
    catalog.speed AS catalog_speed,
    catalog.glide AS catalog_glide,
    catalog.turn AS catalog_turn,
    catalog.fade AS catalog_fade,
    catalog.match_type AS catalog_match_type,
    catalog.disc_url AS catalog_url,
    catalog.dataset_version AS catalog_version,
    catalog.attribution AS catalog_attribution,
    catalog.raw_flight_json AS catalog_raw_flight_json,
    catalog.invalid_fields_json AS catalog_invalid_fields_json,
    COALESCE(catalog.flight_conflict_unresolved, FALSE) AS catalog_unresolved,
    catalog.catalog_category,
    review.status AS weight_review_status,
    review.weight_g AS reviewed_weight_g,
    review.evidence AS reviewed_weight_evidence
  FROM resolved
  LEFT JOIN {try_discs_matches} AS catalog
    ON resolved.normalized_manufacturer = catalog.manufacturer
   AND resolved.normalized_model = catalog.model
  LEFT JOIN active_weight_reviews AS review
    ON resolved.id = review.id
   AND resolved.weight_evidence_hash = review.evidence_hash
), conflicts AS (
  SELECT *, COALESCE(({flight_disagreements}), FALSE) AS local_has_conflict
  FROM matched
)
SELECT
  id,
  IF(catalog_match_type IS NOT NULL, catalog_speed, flight_numbers[SAFE_OFFSET(0)]) AS speed,
  IF(catalog_match_type IS NOT NULL, catalog_glide, flight_numbers[SAFE_OFFSET(1)]) AS glide,
  IF(catalog_match_type IS NOT NULL, catalog_turn, flight_numbers[SAFE_OFFSET(2)]) AS turn,
  IF(catalog_match_type IS NOT NULL, catalog_fade, flight_numbers[SAFE_OFFSET(3)]) AS fade,
  flight_numbers[SAFE_OFFSET(0)] AS local_speed,
  flight_numbers[SAFE_OFFSET(1)] AS local_glide,
  flight_numbers[SAFE_OFFSET(2)] AS local_turn,
  flight_numbers[SAFE_OFFSET(3)] AS local_fade,
  flight_source AS local_flight_source,
  flight_evidence AS local_flight_evidence,
  catalog_unresolved OR local_has_conflict OR COALESCE(
    catalog_match_type IS NOT NULL AND {catalog_disagreement}, FALSE) AS flight_conflict,
  catalog_unresolved OR (catalog_match_type IS NULL AND local_has_conflict)
    AS flight_conflict_unresolved,
  COALESCE(JSON_VALUE_ARRAY(catalog_invalid_fields_json), ARRAY<STRING>[])
    AS flight_invalid_fields,
  catalog_category,
  catalog_raw_flight_json,
  TO_JSON_STRING([{local_records}]) AS local_flight_records_json,
  CASE
    WHEN catalog_unresolved THEN 0.0
    WHEN catalog_match_type IS NOT NULL THEN
      IF(catalog_match_type = 'exact', 0.98, 0.95)
    WHEN flight_source IS NOT NULL AND (
      {flight_disagreements}
    ) THEN GREATEST(flight_confidence - 0.20, 0.0)
    ELSE flight_confidence
  END AS flight_confidence,
  IF(catalog_match_type IS NOT NULL,
    CONCAT('try_discs_', catalog_match_type), flight_source) AS flight_source,
  IF(catalog_match_type IS NOT NULL,
    CONCAT('Try Discs dataset ', COALESCE(catalog_version, 'unknown'), ': ',
      COALESCE(catalog_url, 'https://trydiscs.com')),
    flight_evidence) AS flight_evidence,
  IF(catalog_match_type IS NOT NULL, catalog_attribution, NULL) AS flight_attribution,
  CASE weight_review_status
    WHEN 'FOUND' THEN reviewed_weight_g
    WHEN 'NONE' THEN NULL
    ELSE SAFE_CAST(ROUND(normalized_weight_value) AS INT64)
  END AS normalized_weight_g,
  IF(weight_source = 'variant_title_range' AND weight_review_status IS NULL, title_weight_min_g, NULL)
    AS normalized_weight_min_g,
  IF(weight_source = 'variant_title_range' AND weight_review_status IS NULL, title_weight_max_g, NULL)
    AS normalized_weight_max_g,
  weight_evidence_hash,
  weight_review_status,
  CASE
    WHEN weight_review_status = 'FOUND' THEN 0.92
    WHEN weight_review_status = 'NONE' THEN 0.0
    WHEN weight_source = 'variant_title_range' THEN 0.90
    WHEN weight_source = 'variant_title' THEN 0.97
    WHEN normalized_weight_value IS NOT NULL
      AND raw_weight_g BETWEEN 100 AND 190
      AND ABS(normalized_weight_value - raw_weight_g) >= 2 THEN 0.55
    WHEN weight_source = 'variant_title_unlabelled' THEN 0.82
    WHEN weight_source = 'body_html' THEN 0.88
    WHEN weight_source = 'source_weight_g' THEN 0.70
    WHEN weight_source = 'rejected_source_weight_g' THEN 0.0
  END AS weight_confidence,
  CASE
    WHEN weight_review_status = 'FOUND' THEN 'llm_variant_evidence'
    WHEN weight_review_status = 'NONE' THEN 'llm_no_specific_weight'
    WHEN weight_source = 'variant_title_range' THEN 'variant_title_range'
    ELSE weight_source
  END AS weight_source,
  CASE
    WHEN weight_review_status = 'FOUND' THEN reviewed_weight_evidence
    WHEN weight_review_status = 'NONE' THEN NULL
    WHEN weight_source = 'variant_title_range' THEN weight_evidence
    ELSE IF(normalized_weight_value IS NOT NULL
      AND raw_weight_g BETWEEN 100 AND 190
      AND ABS(normalized_weight_value - raw_weight_g) >= 2,
      CONCAT(weight_evidence, '; source_weight_g=', CAST(raw_weight_g AS STRING)),
      weight_evidence)
  END AS weight_evidence
FROM conflicts
"""
