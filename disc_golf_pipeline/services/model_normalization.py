"""Deterministic search-oriented disc model normalization."""

import os
import re
import unicodedata


DEFAULT_MODEL_RULES_VERSION = "model-v2-5"

VARIANT_NON_DISC_PATTERN = (
    r"(?:^| )(?:key ?chains?|markers?|mini|miniature)(?: |$)"
)

# Parenthetical PDGA rows are collapsed only when the shortened pair is unique or
# when the retail identity has been reviewed here.
ENTITY_COLLAPSE_OVERRIDES = (
    ("Axiom Discs", "Simon Line Balance", "Simon Line Balance", "prototype approvals share one retail model"),
    ("Clash Discs", "Lotus", "Lotus", "tone and retooled approvals share one retail model"),
    ("Disc Golf Association", "Hurricane", "Hurricane", "plastic-line approvals share one retail model"),
    ("Discmania", "DD", "DD", "approval revisions share one retail model"),
    ("Discmania", "DD2", "DD2", "approval revisions share one retail model"),
    ("Discmania", "DD3", "DD3", "approval revisions share one retail model"),
    ("Discmania", "FD", "FD", "approval revisions share one retail model"),
    ("Discmania", "FD2", "FD2", "approval revisions share one retail model"),
    ("Discmania", "FD3", "FD3", "approval revisions share one retail model"),
    ("Discmania", "MD3", "MD3", "approval revisions share one retail model"),
    ("Discmania", "MD4", "MD4", "approval revisions share one retail model"),
    ("Discmania", "MD5", "MD5", "approval revisions share one retail model"),
    ("Discmania", "P1", "P1", "approval revisions share one retail model"),
    ("Discmania", "P1x", "P1x", "approval revisions share one retail model"),
    ("Discmania", "P2", "P2", "approval revisions share one retail model"),
    ("Discmania", "P3x", "P3x", "approval revisions share one retail model"),
    ("Gateway Disc Sports", "Assassin", "Assassin", "retools share one retail model"),
    ("Innova Champion Discs", "Eagle", "Eagle", "old and new approvals share one retail model"),
    ("RPM Discs/Disc Golf Aotearoa", "Kiwi", "Kiwi", "approval revisions share one retail model"),
    ("RPM Discs/Disc Golf Aotearoa", "Kotare", "Kotare", "approval revisions share one retail model"),
)

CURATED_MODEL_ALIASES = (
    ("Discmania", "DD3", "Cloud Breaker", "retail series name"),
    ("Prodigy Disc", "Good Boy", "P Model S", "previous retail model name"),
)

# These terms can be legitimate models but are unsafe without compatible
# manufacturer evidence because they commonly occur as descriptions, stamps,
# colors, release labels, or ordinary product language.
GENERIC_MODEL_ALIASES = {
    "ace",
    "air",
    "alpha",
    "animal",
    "arrow",
    "atom",
    "balance",
    "bear",
    "blade",
    "boatman",
    "breeze",
    "bullet",
    "cannon",
    "catapult",
    "crown",
    "defender",
    "destiny",
    "diamond",
    "driver",
    "escape",
    "fairway driver",
    "felon",
    "fireball",
    "fly",
    "fortress",
    "fuse",
    "gatekeeper",
    "giant",
    "glory",
    "grace",
    "harp",
    "hatchet",
    "hope",
    "justice",
    "king",
    "maiden",
    "money",
    "mystery box",
    "magma",
    "chameleon",
    "orbit",
    "phoenix",
    "pure",
    "queen",
    "river",
    "saint",
    "seer",
    "shield",
    "sword",
    "thief",
    "truth",
    "viking",
    "warship",
    "wave",
    "world",
}

# These aliases are useful audit evidence, but in ordinary retail titles they
# overwhelmingly describe plastic, stamps, or release metadata. They never
# participate in an automatic acceptance decision.
CONTEXT_ONLY_MODEL_ALIASES = {
    "big z",
    "d",
    "elite",
    "elite z",
    "esp",
    "first run",
    "flx",
    "glo",
    "glow",
    "limited edition",
    "pro d",
    "proto",
    "prototype",
    "series",
    "special edition",
    "stamp",
    "stock",
    "ti",
    "tour series",
    "x",
    "z",
}

# Reviewed short models that may be accepted without manufacturer evidence when
# the alias maps to exactly one manufacturer and no stronger candidate conflicts.
SAFE_SHORT_MODEL_ALIASES = {"heat", "zone"}


def get_model_rules_version():
    return os.getenv("MODEL_NORMALIZATION_RULES_VERSION", DEFAULT_MODEL_RULES_VERSION).strip()


def build_table_ref(project_id, dataset, table_name):
    return f"`{project_id}.{dataset}.{table_name}`"


def normalize_model_text(value):
    normalized = unicodedata.normalize("NFKD", value or "").casefold()
    return re.sub(r"[^a-z0-9]+", " ", normalized).strip()


def model_alias_requires_manufacturer(alias, manufacturer_count=1):
    normalized = normalize_model_text(alias)
    compact = normalized.replace(" ", "")
    return (
        normalized in GENERIC_MODEL_ALIASES
        or normalized in CONTEXT_ONLY_MODEL_ALIASES
        or manufacturer_count > 1
        or (len(compact) < 5 and normalized not in SAFE_SHORT_MODEL_ALIASES)
    )


def _sql_quote(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _sql_array(values):
    return "[" + ", ".join(_sql_quote(value) for value in sorted(values)) + "]"


def _override_structs(rules_version):
    rows = []
    for manufacturer, base_model, canonical_model, reason in ENTITY_COLLAPSE_OVERRIDES:
        rows.append(
            "STRUCT("
            "'collapse' AS override_type, "
            f"{_sql_quote(manufacturer)} AS manufacturer, "
            f"{_sql_quote(base_model)} AS base_model, "
            f"{_sql_quote(canonical_model)} AS canonical_model, "
            "CAST(NULL AS STRING) AS alias, "
            f"{_sql_quote(reason)} AS reason, "
            f"{_sql_quote(rules_version)} AS rules_version"
            ")"
        )
    for manufacturer, canonical_model, alias, reason in CURATED_MODEL_ALIASES:
        rows.append(
            "STRUCT("
            "'alias' AS override_type, "
            f"{_sql_quote(manufacturer)} AS manufacturer, "
            "CAST(NULL AS STRING) AS base_model, "
            f"{_sql_quote(canonical_model)} AS canonical_model, "
            f"{_sql_quote(alias)} AS alias, "
            f"{_sql_quote(reason)} AS reason, "
            f"{_sql_quote(rules_version)} AS rules_version"
            ")"
        )
    return ",\n    ".join(rows)


def build_disc_model_catalog_sql(project_id, dataset, rules_version):
    pdga_table = build_table_ref(project_id, dataset, "PdgaDiscCanonical")
    manufacturer_aliases = build_table_ref(project_id, dataset, "DiscManufacturerAliases")
    overrides_table = build_table_ref(project_id, dataset, "DiscModelNormalizationOverrides")
    entities_table = build_table_ref(project_id, dataset, "DiscModelEntities")
    aliases_table = build_table_ref(project_id, dataset, "DiscModelAliases")
    generic_aliases = _sql_array(GENERIC_MODEL_ALIASES)
    context_only_aliases = _sql_array(CONTEXT_ONLY_MODEL_ALIASES)
    safe_short_aliases = _sql_array(SAFE_SHORT_MODEL_ALIASES)

    return f"""
CREATE OR REPLACE TABLE {overrides_table} AS
SELECT
  *,
  REGEXP_REPLACE(
    NORMALIZE_AND_CASEFOLD(manufacturer, NFKD),
    r'[^a-z0-9]+',
    ''
  ) AS manufacturer_key,
  TRIM(REGEXP_REPLACE(
    NORMALIZE_AND_CASEFOLD(COALESCE(base_model, ''), NFKD),
    r'[^a-z0-9]+',
    ' '
  )) AS base_model_key,
  TRIM(REGEXP_REPLACE(
    NORMALIZE_AND_CASEFOLD(canonical_model, NFKD),
    r'[^a-z0-9]+',
    ' '
  )) AS canonical_model_key,
  CURRENT_TIMESTAMP() AS updated_at
FROM UNNEST([
    {_override_structs(rules_version)}
]);

CREATE OR REPLACE TABLE {entities_table}
CLUSTER BY manufacturer_key, canonical_model_key AS
WITH active_manufacturer_aliases AS (
  SELECT alias_match_key, canonical_manufacturer
  FROM {manufacturer_aliases}
  WHERE is_active
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY alias_match_key
    ORDER BY IF(alias_source = 'curated', 0, 1), canonical_manufacturer
  ) = 1
),
prepared AS (
  SELECT
    pdga.canonical_id,
    pdga.pdga_manufacturer,
    pdga.pdga_model,
    COALESCE(alias.canonical_manufacturer, pdga.pdga_manufacturer) AS manufacturer,
    REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(
        COALESCE(alias.canonical_manufacturer, pdga.pdga_manufacturer),
        NFKD
      ),
      r'[^a-z0-9]+',
      ''
    ) AS manufacturer_key,
    TRIM(REGEXP_REPLACE(pdga.pdga_model, r'\s*\([^)]*\)\s*$', '')) AS shortened_model,
    REGEXP_CONTAINS(pdga.pdga_model, r'\([^)]*\)\s*$') AS has_parenthetical
  FROM {pdga_table} AS pdga
  LEFT JOIN active_manufacturer_aliases AS alias
    ON pdga.manufacturer_match_key = alias.alias_match_key
  WHERE pdga.is_current
    AND NULLIF(TRIM(pdga.pdga_model), '') IS NOT NULL
),
keyed AS (
  SELECT
    prepared.*,
    TRIM(REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(shortened_model, NFKD),
      r'[^a-z0-9]+',
      ' '
    )) AS shortened_model_key
  FROM prepared
),
with_counts AS (
  SELECT
    keyed.*,
    COUNT(*) OVER (
      PARTITION BY manufacturer_key, shortened_model_key
    ) AS shortened_pair_count
  FROM keyed
),
entity_rows AS (
  SELECT
    source.*,
    CASE
      WHEN NOT source.has_parenthetical THEN source.pdga_model
      WHEN source.shortened_pair_count = 1 THEN source.shortened_model
      WHEN override.canonical_model IS NOT NULL THEN override.canonical_model
      ELSE source.pdga_model
    END AS canonical_model
  FROM with_counts AS source
  LEFT JOIN {overrides_table} AS override
    ON override.override_type = 'collapse'
   AND source.manufacturer_key = override.manufacturer_key
   AND source.shortened_model_key = override.base_model_key
),
entity_keys AS (
  SELECT
    *,
    TRIM(REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(canonical_model, NFKD),
      r'[^a-z0-9]+',
      ' '
    )) AS canonical_model_key
  FROM entity_rows
)
SELECT
  TO_HEX(SHA256(CONCAT(manufacturer_key, '|', canonical_model_key))) AS disc_entity_id,
  ANY_VALUE(manufacturer) AS manufacturer,
  manufacturer_key,
  ANY_VALUE(TRIM(canonical_model)) AS canonical_model,
  canonical_model_key,
  ARRAY_AGG(DISTINCT canonical_id ORDER BY canonical_id) AS pdga_canonical_ids,
  ARRAY_AGG(DISTINCT pdga_model ORDER BY pdga_model) AS pdga_models,
  COUNT(DISTINCT canonical_id) AS approval_count,
  {_sql_quote(rules_version)} AS rules_version,
  CURRENT_TIMESTAMP() AS updated_at,
  TRUE AS is_active
FROM entity_keys
GROUP BY manufacturer_key, canonical_model_key;

CREATE OR REPLACE TABLE {aliases_table}
CLUSTER BY normalized_alias_text, manufacturer_key AS
WITH entity_aliases AS (
  SELECT
    entity.disc_entity_id,
    entity.manufacturer,
    entity.manufacturer_key,
    entity.canonical_model,
    entity.canonical_model AS alias,
    'canonical' AS alias_type,
    10 AS alias_priority
  FROM {entities_table} AS entity

  UNION ALL

  SELECT
    entity.disc_entity_id,
    entity.manufacturer,
    entity.manufacturer_key,
    entity.canonical_model,
    pdga.pdga_model AS alias,
    'official' AS alias_type,
    20 AS alias_priority
  FROM {entities_table} AS entity
  CROSS JOIN UNNEST(entity.pdga_canonical_ids) AS canonical_id
  INNER JOIN {pdga_table} AS pdga
    ON canonical_id = pdga.canonical_id

  UNION ALL

  SELECT
    entity.disc_entity_id,
    entity.manufacturer,
    entity.manufacturer_key,
    entity.canonical_model,
    TRIM(REGEXP_REPLACE(pdga.pdga_model, r'\s*\([^)]*\)\s*$', '')) AS alias,
    'derived_parenthetical' AS alias_type,
    30 AS alias_priority
  FROM {entities_table} AS entity
  CROSS JOIN UNNEST(entity.pdga_canonical_ids) AS canonical_id
  INNER JOIN {pdga_table} AS pdga
    ON canonical_id = pdga.canonical_id
  WHERE REGEXP_CONTAINS(pdga.pdga_model, r'\([^)]*\)\s*$')
    AND TRIM(REGEXP_REPLACE(pdga.pdga_model, r'\s*\([^)]*\)\s*$', ''))
      = entity.canonical_model

  UNION ALL

  SELECT
    entity.disc_entity_id,
    entity.manufacturer,
    entity.manufacturer_key,
    entity.canonical_model,
    override.alias,
    'curated' AS alias_type,
    0 AS alias_priority
  FROM {overrides_table} AS override
  INNER JOIN {entities_table} AS entity
    ON override.manufacturer_key = entity.manufacturer_key
   AND override.canonical_model_key = entity.canonical_model_key
  WHERE override.override_type = 'alias'
),
normalized AS (
  SELECT
    *,
    TRIM(REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(alias, NFKD),
      r'[^a-z0-9]+',
      ' '
    )) AS normalized_alias_text,
    REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(alias, NFKD),
      r'[^a-z0-9]+',
      ''
    ) AS normalized_alias_key
  FROM entity_aliases
  WHERE NULLIF(TRIM(alias), '') IS NOT NULL
),
deduplicated AS (
  SELECT *
  FROM normalized
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY disc_entity_id, normalized_alias_text
    ORDER BY alias_priority, alias
  ) = 1
),
alias_counts AS (
  SELECT
    normalized_alias_text,
    COUNT(DISTINCT disc_entity_id) AS entity_count,
    COUNT(DISTINCT manufacturer_key) AS manufacturer_count
  FROM deduplicated
  GROUP BY normalized_alias_text
)
SELECT
  alias.disc_entity_id,
  alias.manufacturer,
  alias.manufacturer_key,
  alias.canonical_model,
  alias.alias,
  alias.normalized_alias_key,
  alias.normalized_alias_text,
  alias.alias_type,
  alias.alias_priority,
  ARRAY_LENGTH(SPLIT(alias.normalized_alias_text, ' ')) AS alias_token_count,
  counts.entity_count,
  counts.manufacturer_count,
  alias.normalized_alias_text IN UNNEST({generic_aliases}) AS is_generic,
  alias.normalized_alias_text IN UNNEST({context_only_aliases}) AS is_context_only,
  (
    alias.normalized_alias_text IN UNNEST({generic_aliases})
    OR alias.normalized_alias_text IN UNNEST({context_only_aliases})
    OR counts.manufacturer_count > 1
    OR (
      LENGTH(alias.normalized_alias_key) < 5
      AND alias.normalized_alias_text NOT IN UNNEST({safe_short_aliases})
    )
  ) AS requires_manufacturer,
  {_sql_quote(rules_version)} AS rules_version,
  TRUE AS is_active,
  CURRENT_TIMESTAMP() AS updated_at
FROM deduplicated AS alias
INNER JOIN alias_counts AS counts
  USING (normalized_alias_text);
"""


def build_product_model_candidates_sql(project_id, dataset, rules_version):
    products_table = build_table_ref(project_id, dataset, "NormalizedProducts")
    variants_view = build_table_ref(project_id, dataset, "v_ShopifyVariants")
    aliases_table = build_table_ref(project_id, dataset, "DiscModelAliases")
    entities_table = build_table_ref(project_id, dataset, "DiscModelEntities")
    candidates_table = build_table_ref(project_id, dataset, "ProductDiscModelCandidates")
    decisions_table = build_table_ref(project_id, dataset, "ProductDiscModelDecisions")

    return f"""
CREATE OR REPLACE TABLE {candidates_table}
CLUSTER BY decision_bucket, store AS
WITH shopify_discs AS (
  SELECT
    product.*,
    SPLIT(TRIM(REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(COALESCE(product.title, ''), NFKD),
      r'[^a-z0-9]+',
      ' '
    )), ' ') AS title_tokens,
    REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(COALESCE(product.normalized_manufacturer, ''), NFKD),
      r'[^a-z0-9]+',
      ''
    ) AS normalized_manufacturer_key
  FROM {products_table} AS product
  WHERE product.source = 'shopify'
    AND product.item_type = 'disc'
),
product_ngrams AS (
  SELECT
    product.product_key,
    ARRAY_TO_STRING(
      ARRAY_SLICE(product.title_tokens, ngram_start, ngram_start + ngram_size - 1),
      ' '
    ) AS matched_phrase
  FROM shopify_discs AS product
  CROSS JOIN UNNEST(GENERATE_ARRAY(1, LEAST(8, ARRAY_LENGTH(product.title_tokens)))) AS ngram_size
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(product.title_tokens) - ngram_size)) AS ngram_start
),
raw_product_matches AS (
  SELECT
    ngram.product_key,
    alias.*
  FROM product_ngrams AS ngram
  INNER JOIN {aliases_table} AS alias
    ON ngram.matched_phrase = alias.normalized_alias_text
   AND alias.is_active
),
product_matches AS (
  SELECT *
  FROM raw_product_matches AS match
  WHERE NOT EXISTS (
    SELECT 1
    FROM raw_product_matches AS stronger
    WHERE stronger.product_key = match.product_key
      AND stronger.disc_entity_id != match.disc_entity_id
      AND stronger.alias_token_count > match.alias_token_count
      AND STRPOS(
        CONCAT(' ', stronger.normalized_alias_text, ' '),
        CONCAT(' ', match.normalized_alias_text, ' ')
      ) > 0
  )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_key, disc_entity_id
    ORDER BY alias_token_count DESC, alias_priority, normalized_alias_text
  ) = 1
),
variant_source AS (
  SELECT
    product.product_key,
    CAST(variant.id AS STRING) AS variant_id,
    REGEXP_CONTAINS(
      TRIM(REGEXP_REPLACE(
        NORMALIZE_AND_CASEFOLD(COALESCE(CAST(variant.variant_title AS STRING), ''), NFKD),
        r'[^a-z0-9]+',
        ' '
      )),
      r'{VARIANT_NON_DISC_PATTERN}'
    ) AS is_non_disc_variant,
    SPLIT(TRIM(REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(COALESCE(CAST(variant.variant_title AS STRING), ''), NFKD),
      r'[^a-z0-9]+',
      ' '
    )), ' ') AS variant_tokens
  FROM {variants_view} AS variant
  INNER JOIN shopify_discs AS product
    ON product.product_key = CONCAT(
      'shopify:',
      LOWER(TRIM(CAST(variant.store AS STRING))),
      ':',
      CAST(variant.product_id AS STRING)
    )
),
variant_counts AS (
  SELECT product_key, COUNT(DISTINCT variant_id) AS variant_count
  FROM variant_source
  GROUP BY product_key
),
variant_ngrams AS (
  SELECT
    variant.product_key,
    variant.variant_id,
    variant.is_non_disc_variant,
    ARRAY_TO_STRING(
      ARRAY_SLICE(variant.variant_tokens, ngram_start, ngram_start + ngram_size - 1),
      ' '
    ) AS matched_phrase
  FROM variant_source AS variant
  CROSS JOIN UNNEST(GENERATE_ARRAY(1, LEAST(8, ARRAY_LENGTH(variant.variant_tokens)))) AS ngram_size
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(variant.variant_tokens) - ngram_size)) AS ngram_start
),
raw_variant_matches AS (
  SELECT
    ngram.product_key,
    ngram.variant_id,
    alias.*
  FROM variant_ngrams AS ngram
  INNER JOIN {aliases_table} AS alias
    ON ngram.matched_phrase = alias.normalized_alias_text
   AND alias.is_active
  WHERE NOT ngram.is_non_disc_variant
),
variant_matches AS (
  SELECT *
  FROM raw_variant_matches AS match
  WHERE NOT EXISTS (
    SELECT 1
    FROM raw_variant_matches AS stronger
    WHERE stronger.product_key = match.product_key
      AND stronger.variant_id = match.variant_id
      AND stronger.disc_entity_id != match.disc_entity_id
      AND stronger.alias_token_count > match.alias_token_count
      AND STRPOS(
        CONCAT(' ', stronger.normalized_alias_text, ' '),
        CONCAT(' ', match.normalized_alias_text, ' ')
      ) > 0
  )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_key, variant_id, disc_entity_id
    ORDER BY alias_token_count DESC, alias_priority, normalized_alias_text
  ) = 1
),
variant_match_summary AS (
  SELECT
    product_key,
    disc_entity_id,
    COUNT(DISTINCT variant_id) AS matched_variant_count,
    ARRAY_AGG(
      STRUCT(
        alias,
        normalized_alias_text,
        alias_type,
        alias_token_count,
        is_generic,
        is_context_only,
        requires_manufacturer,
        manufacturer_count
      )
      ORDER BY alias_token_count DESC, alias_priority
      LIMIT 1
    )[OFFSET(0)] AS best_variant_alias
  FROM variant_matches
  GROUP BY product_key, disc_entity_id
),
candidate_keys AS (
  SELECT product_key, disc_entity_id FROM product_matches
  UNION DISTINCT
  SELECT product_key, disc_entity_id FROM variant_match_summary
),
candidate_evidence AS (
  SELECT
    product.product_key,
    product.product_id,
    product.store,
    product.title,
    product.raw_vendor,
    product.normalized_manufacturer AS source_normalized_manufacturer,
    entity.disc_entity_id,
    entity.manufacturer AS candidate_manufacturer,
    entity.canonical_model AS candidate_model,
    product_match.disc_entity_id IS NOT NULL AS matched_product_title,
    COALESCE(variant_match.matched_variant_count, 0) AS matched_variant_count,
    COALESCE(variant_count.variant_count, 0) AS variant_count,
    COALESCE(
      product_match.alias,
      variant_match.best_variant_alias.alias
    ) AS matched_alias,
    COALESCE(
      product_match.alias_type,
      variant_match.best_variant_alias.alias_type
    ) AS alias_type,
    COALESCE(
      product_match.alias_token_count,
      variant_match.best_variant_alias.alias_token_count
    ) AS alias_token_count,
    COALESCE(
      product_match.is_generic,
      variant_match.best_variant_alias.is_generic
    ) AS is_generic,
    COALESCE(
      product_match.is_context_only,
      variant_match.best_variant_alias.is_context_only
    ) AS is_context_only,
    COALESCE(
      product_match.requires_manufacturer,
      variant_match.best_variant_alias.requires_manufacturer
    ) AS requires_manufacturer,
    COALESCE(
      product_match.manufacturer_count,
      variant_match.best_variant_alias.manufacturer_count
    ) AS alias_manufacturer_count,
    product.normalized_manufacturer IS NOT NULL
      AND product.normalized_manufacturer_key = entity.manufacturer_key
      AS manufacturer_compatible,
    product.normalized_manufacturer IS NOT NULL
      AND product.normalized_manufacturer_key != entity.manufacturer_key
      AS manufacturer_conflict
  FROM candidate_keys AS candidate
  INNER JOIN shopify_discs AS product
    ON candidate.product_key = product.product_key
  INNER JOIN {entities_table} AS entity
    ON candidate.disc_entity_id = entity.disc_entity_id
  LEFT JOIN product_matches AS product_match
    ON candidate.product_key = product_match.product_key
   AND candidate.disc_entity_id = product_match.disc_entity_id
  LEFT JOIN variant_match_summary AS variant_match
    ON candidate.product_key = variant_match.product_key
   AND candidate.disc_entity_id = variant_match.disc_entity_id
  LEFT JOIN variant_counts AS variant_count
    ON candidate.product_key = variant_count.product_key
),
scored AS (
  SELECT
    *,
    (
      manufacturer_compatible
      OR (
        source_normalized_manufacturer IS NULL
        AND matched_product_title
        AND NOT requires_manufacturer
        AND alias_manufacturer_count = 1
      )
    ) AND NOT is_context_only AS base_credible,
    CASE
      WHEN is_context_only THEN 0
      WHEN manufacturer_compatible AND matched_product_title THEN 100
      WHEN manufacturer_compatible
        AND matched_variant_count = variant_count
        AND variant_count > 1 THEN 92
      WHEN source_normalized_manufacturer IS NULL
        AND matched_product_title
        AND NOT requires_manufacturer
        AND alias_manufacturer_count = 1 THEN 85
      WHEN manufacturer_compatible AND matched_variant_count > 0 THEN 65
      WHEN matched_product_title AND NOT manufacturer_conflict THEN 45
      ELSE 0
    END AS evidence_score
  FROM candidate_evidence
),
with_competition AS (
  SELECT
    *,
    COUNTIF(base_credible AND NOT is_generic) OVER (
      PARTITION BY product_key
    ) AS non_generic_credible_count
  FROM scored
),
contextualized AS (
  SELECT
    *,
    base_credible AND (NOT is_generic OR non_generic_credible_count = 0) AS is_credible
  FROM with_competition
),
ranked AS (
  SELECT
    *,
    COUNTIF(is_credible) OVER (PARTITION BY product_key) AS credible_candidate_count,
    ROW_NUMBER() OVER (
      PARTITION BY product_key
      ORDER BY is_credible DESC, evidence_score DESC, alias_token_count DESC,
        candidate_manufacturer, candidate_model
    ) AS candidate_rank
  FROM contextualized
)
SELECT
  *,
  CASE
    WHEN NOT is_credible THEN 'UNRESOLVED'
    WHEN credible_candidate_count = 1
      AND manufacturer_compatible
      AND matched_product_title THEN 'ACCEPT'
    WHEN credible_candidate_count = 1
      AND manufacturer_compatible
      AND matched_variant_count = variant_count
      AND variant_count > 1 THEN 'ACCEPT'
    ELSE 'POSSIBLE'
  END AS decision_bucket,
  CASE
    WHEN credible_candidate_count = 1
      AND manufacturer_compatible
      AND matched_product_title THEN 0.99
    WHEN credible_candidate_count = 1
      AND manufacturer_compatible
      AND matched_variant_count = variant_count
      AND variant_count > 1 THEN 0.96
    ELSE NULL
  END AS decision_confidence,
  {_sql_quote(rules_version)} AS model_rules_version,
  CURRENT_TIMESTAMP() AS decided_at
FROM ranked;

CREATE OR REPLACE TABLE {decisions_table}
CLUSTER BY decision_bucket, store AS
SELECT
  product_key,
  product_id,
  store,
  title,
  raw_vendor,
  source_normalized_manufacturer,
  disc_entity_id,
  candidate_manufacturer AS normalized_manufacturer,
  candidate_model AS normalized_model,
  matched_alias,
  alias_type,
  is_generic,
  is_context_only,
  requires_manufacturer,
  manufacturer_compatible,
  manufacturer_conflict,
  matched_product_title,
  matched_variant_count,
  variant_count,
  credible_candidate_count,
  evidence_score,
  decision_bucket,
  decision_confidence,
  CASE
    WHEN decision_bucket != 'ACCEPT' THEN 'deterministic_v2_unresolved'
    WHEN manufacturer_compatible AND matched_product_title
      THEN 'deterministic_v2_manufacturer_and_product_title'
    WHEN manufacturer_compatible AND matched_variant_count = variant_count
      THEN 'deterministic_v2_manufacturer_and_all_variants'
    ELSE 'deterministic_v2_unique_product_title'
  END AS decision_source,
  model_rules_version,
  decided_at
FROM {candidates_table}
WHERE candidate_rank = 1;
"""


def build_variant_model_decisions_sql(project_id, dataset, rules_version):
    products_table = build_table_ref(project_id, dataset, "NormalizedProducts")
    variants_view = build_table_ref(project_id, dataset, "v_ShopifyVariants")
    aliases_table = build_table_ref(project_id, dataset, "DiscModelAliases")
    entities_table = build_table_ref(project_id, dataset, "DiscModelEntities")
    product_decisions = build_table_ref(project_id, dataset, "ProductDiscModelDecisions")
    candidates_table = build_table_ref(project_id, dataset, "VariantDiscModelCandidates")
    decisions_table = build_table_ref(project_id, dataset, "VariantDiscModelDecisions")

    return f"""
CREATE OR REPLACE TABLE {candidates_table}
CLUSTER BY decision_bucket, store AS
WITH unresolved_products AS (
  SELECT
    product.*,
    REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(COALESCE(product.normalized_manufacturer, ''), NFKD),
      r'[^a-z0-9]+',
      ''
    ) AS normalized_manufacturer_key
  FROM {products_table} AS product
  LEFT JOIN {product_decisions} AS decision
    ON product.product_key = decision.product_key
   AND decision.decision_bucket = 'ACCEPT'
  WHERE product.source = 'shopify'
    AND product.item_type = 'disc'
    AND decision.product_key IS NULL
),
variant_source AS (
  SELECT
    product.product_key,
    product.store,
    product.title,
    product.raw_vendor,
    product.normalized_manufacturer AS source_normalized_manufacturer,
    product.normalized_manufacturer_key,
    CAST(variant.id AS STRING) AS variant_id,
    CAST(variant.variant_title AS STRING) AS variant_title,
    REGEXP_CONTAINS(
      TRIM(REGEXP_REPLACE(
        NORMALIZE_AND_CASEFOLD(COALESCE(CAST(variant.variant_title AS STRING), ''), NFKD),
        r'[^a-z0-9]+',
        ' '
      )),
      r'{VARIANT_NON_DISC_PATTERN}'
    ) AS is_non_disc_variant,
    SPLIT(TRIM(REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(COALESCE(CAST(variant.variant_title AS STRING), ''), NFKD),
      r'[^a-z0-9]+',
      ' '
    )), ' ') AS variant_tokens
  FROM {variants_view} AS variant
  INNER JOIN unresolved_products AS product
    ON product.product_key = CONCAT(
      'shopify:',
      LOWER(TRIM(CAST(variant.store AS STRING))),
      ':',
      CAST(variant.product_id AS STRING)
    )
),
variant_ngrams AS (
  SELECT
    variant.* EXCEPT(variant_tokens),
    ARRAY_TO_STRING(
      ARRAY_SLICE(variant.variant_tokens, ngram_start, ngram_start + ngram_size - 1),
      ' '
    ) AS matched_phrase
  FROM variant_source AS variant
  CROSS JOIN UNNEST(GENERATE_ARRAY(1, LEAST(8, ARRAY_LENGTH(variant.variant_tokens)))) AS ngram_size
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(variant.variant_tokens) - ngram_size)) AS ngram_start
),
raw_matches AS (
  SELECT ngram.*, alias.*
  FROM variant_ngrams AS ngram
  INNER JOIN {aliases_table} AS alias
    ON ngram.matched_phrase = alias.normalized_alias_text
   AND alias.is_active
  WHERE NOT ngram.is_non_disc_variant
),
matches AS (
  SELECT *
  FROM raw_matches AS match
  WHERE NOT EXISTS (
    SELECT 1
    FROM raw_matches AS stronger
    WHERE stronger.product_key = match.product_key
      AND stronger.variant_id = match.variant_id
      AND stronger.disc_entity_id != match.disc_entity_id
      AND stronger.alias_token_count > match.alias_token_count
      AND STRPOS(
        CONCAT(' ', stronger.normalized_alias_text, ' '),
        CONCAT(' ', match.normalized_alias_text, ' ')
      ) > 0
  )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_key, variant_id, disc_entity_id
    ORDER BY alias_token_count DESC, alias_priority, normalized_alias_text
  ) = 1
),
scored AS (
  SELECT
    match.product_key,
    match.store,
    match.title,
    match.raw_vendor,
    match.source_normalized_manufacturer,
    match.variant_id,
    match.variant_title,
    match.disc_entity_id,
    entity.manufacturer AS candidate_manufacturer,
    entity.canonical_model AS candidate_model,
    match.alias AS matched_alias,
    match.alias_type,
    match.alias_token_count,
    match.is_generic,
    match.is_context_only,
    match.requires_manufacturer,
    match.manufacturer_count AS alias_manufacturer_count,
    match.source_normalized_manufacturer IS NOT NULL
      AND match.normalized_manufacturer_key = entity.manufacturer_key
      AS manufacturer_compatible,
    match.source_normalized_manufacturer IS NULL
      AND NOT match.requires_manufacturer
      AND match.manufacturer_count = 1
      AS unique_without_manufacturer,
    (
      match.source_normalized_manufacturer IS NOT NULL
        AND match.normalized_manufacturer_key = entity.manufacturer_key
      OR (
        match.source_normalized_manufacturer IS NULL
        AND NOT match.requires_manufacturer
        AND match.manufacturer_count = 1
      )
    ) AND NOT match.is_context_only AS base_credible
  FROM matches AS match
  INNER JOIN {entities_table} AS entity
    ON match.disc_entity_id = entity.disc_entity_id
),
with_competition AS (
  SELECT
    *,
    COUNTIF(base_credible AND NOT is_generic) OVER (
      PARTITION BY product_key, variant_id
    ) AS non_generic_credible_count
  FROM scored
),
contextualized AS (
  SELECT
    *,
    base_credible AND (NOT is_generic OR non_generic_credible_count = 0) AS is_credible
  FROM with_competition
),
with_counts AS (
  SELECT
    *,
    COUNTIF(is_credible) OVER (
      PARTITION BY product_key, variant_id
    ) AS credible_variant_candidate_count,
    COUNT(DISTINCT IF(
      is_credible,
      disc_entity_id,
      NULL
    )) OVER (PARTITION BY product_key) AS product_entity_count
  FROM contextualized
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_key, variant_id
      ORDER BY is_credible DESC, manufacturer_compatible DESC,
        alias_token_count DESC, candidate_manufacturer, candidate_model
    ) AS candidate_rank
  FROM with_counts
)
SELECT
  *,
  CASE
    WHEN is_credible
      AND credible_variant_candidate_count = 1
      AND product_entity_count >= 2
      AND manufacturer_compatible THEN 'ACCEPT'
    WHEN is_credible THEN 'POSSIBLE'
    ELSE 'UNRESOLVED'
  END AS decision_bucket,
  CASE
    WHEN is_credible
      AND credible_variant_candidate_count = 1
      AND product_entity_count >= 2
      AND manufacturer_compatible THEN 0.96
    ELSE NULL
  END AS decision_confidence,
  {_sql_quote(rules_version)} AS model_rules_version,
  CURRENT_TIMESTAMP() AS decided_at
FROM ranked;

CREATE OR REPLACE TABLE {decisions_table}
CLUSTER BY decision_bucket, store AS
SELECT
  product_key,
  store,
  title,
  raw_vendor,
  source_normalized_manufacturer,
  variant_id,
  variant_title,
  disc_entity_id,
  candidate_manufacturer AS normalized_manufacturer,
  candidate_model AS normalized_model,
  matched_alias,
  alias_type,
  is_generic,
  is_context_only,
  requires_manufacturer,
  manufacturer_compatible,
  credible_variant_candidate_count,
  product_entity_count,
  decision_bucket,
  decision_confidence,
  IF(
    decision_bucket = 'ACCEPT',
    'deterministic_v2_multi_model_variant',
    'deterministic_v2_variant_unresolved'
  ) AS decision_source,
  model_rules_version,
  decided_at
FROM {candidates_table}
WHERE candidate_rank = 1;
"""


def build_apply_product_model_decisions_sql(project_id, dataset, rules_version):
    products_table = build_table_ref(project_id, dataset, "NormalizedProducts")
    decisions_table = build_table_ref(project_id, dataset, "ProductDiscModelDecisions")
    audit_table = build_table_ref(project_id, dataset, "ProductNormalizationAudit")

    return f"""
CREATE TEMP TABLE DesiredProductModels AS
SELECT
  product.product_key,
  CASE
    WHEN decision.decision_bucket = 'ACCEPT'
      THEN COALESCE(
        IF(STARTS_WITH(product.manufacturer_source, 'deterministic_v2'), NULL,
          product.normalized_manufacturer),
        decision.normalized_manufacturer
      )
    WHEN STARTS_WITH(product.manufacturer_source, 'deterministic_v2') THEN NULL
    ELSE product.normalized_manufacturer
  END AS normalized_manufacturer,
  CASE
    WHEN decision.decision_bucket = 'ACCEPT'
      AND (
        product.normalized_manufacturer IS NULL
        OR STARTS_WITH(product.manufacturer_source, 'deterministic_v2')
      ) THEN decision.decision_confidence
    WHEN STARTS_WITH(product.manufacturer_source, 'deterministic_v2') THEN NULL
    ELSE product.manufacturer_confidence
  END AS manufacturer_confidence,
  CASE
    WHEN decision.decision_bucket = 'ACCEPT'
      AND (
        product.normalized_manufacturer IS NULL
        OR STARTS_WITH(product.manufacturer_source, 'deterministic_v2')
      ) THEN decision.decision_source
    WHEN STARTS_WITH(product.manufacturer_source, 'deterministic_v2') THEN 'unresolved_vendor'
    ELSE product.manufacturer_source
  END AS manufacturer_source,
  IF(decision.decision_bucket = 'ACCEPT', decision.normalized_model, NULL)
    AS normalized_model,
  IF(decision.decision_bucket = 'ACCEPT', decision.decision_confidence, NULL)
    AS model_confidence,
  COALESCE(decision.decision_source, 'deterministic_v2_unresolved') AS model_source,
  JSON_SET(
    product.normalization_evidence,
    '$.model_decision',
    TO_JSON(STRUCT(
      COALESCE(decision.decision_bucket, 'UNRESOLVED') AS decision_bucket,
      decision.disc_entity_id AS disc_entity_id,
      decision.matched_alias AS matched_alias,
      decision.credible_candidate_count AS candidate_count,
      decision.decision_source AS decision_source,
      {_sql_quote(rules_version)} AS model_rules_version
    ))
  ) AS normalization_evidence,
  COALESCE(decision.decision_source, 'deterministic_v2_unresolved') AS decision_source
FROM {products_table} AS product
LEFT JOIN {decisions_table} AS decision
  ON product.product_key = decision.product_key
WHERE product.source = 'shopify'
  AND product.item_type = 'disc';

INSERT INTO {audit_table} (
  product_key,
  variant_id,
  source,
  store,
  change_type,
  prior_item_type,
  new_item_type,
  prior_normalized_manufacturer,
  new_normalized_manufacturer,
  prior_normalized_model,
  new_normalized_model,
  decision_source,
  rules_version,
  event_timestamp
)
SELECT
  product.product_key,
  NULL,
  product.source,
  product.store,
  'MODEL_UPDATE',
  product.item_type,
  product.item_type,
  product.normalized_manufacturer,
  desired.normalized_manufacturer,
  product.normalized_model,
  desired.normalized_model,
  desired.decision_source,
  {_sql_quote(rules_version)},
  CURRENT_TIMESTAMP()
FROM {products_table} AS product
INNER JOIN DesiredProductModels AS desired
  USING (product_key)
WHERE product.normalized_manufacturer IS DISTINCT FROM desired.normalized_manufacturer
   OR product.normalized_model IS DISTINCT FROM desired.normalized_model
   OR product.model_source IS DISTINCT FROM desired.model_source;

UPDATE {products_table} AS product
SET
  normalized_manufacturer = desired.normalized_manufacturer,
  manufacturer_confidence = desired.manufacturer_confidence,
  manufacturer_source = desired.manufacturer_source,
  normalized_model = desired.normalized_model,
  model_confidence = desired.model_confidence,
  model_source = desired.model_source,
  normalization_evidence = desired.normalization_evidence,
  normalized_at = CURRENT_TIMESTAMP()
FROM DesiredProductModels AS desired
WHERE product.product_key = desired.product_key
  AND (
    product.normalized_manufacturer IS DISTINCT FROM desired.normalized_manufacturer
    OR product.normalized_model IS DISTINCT FROM desired.normalized_model
    OR product.model_source IS DISTINCT FROM desired.model_source
    OR TO_JSON_STRING(product.normalization_evidence)
      IS DISTINCT FROM TO_JSON_STRING(desired.normalization_evidence)
  );
"""


def build_model_quality_views_sql(project_id, dataset):
    products_table = build_table_ref(project_id, dataset, "NormalizedProducts")
    snapshot_view = build_table_ref(project_id, dataset, "NormalizedVariantSnapshot")
    candidates_table = build_table_ref(project_id, dataset, "ProductDiscModelCandidates")
    decisions_table = build_table_ref(project_id, dataset, "ProductDiscModelDecisions")
    variant_decisions = build_table_ref(project_id, dataset, "VariantDiscModelDecisions")
    v1_matches = build_table_ref(project_id, dataset, "ProductDiscMatches")
    report_view = build_table_ref(project_id, dataset, "v_ModelNormalizationQualityReport")
    checks_view = build_table_ref(project_id, dataset, "v_ModelNormalizationQualityChecks")
    comparison_view = build_table_ref(project_id, dataset, "v_ModelNormalizationComparison")
    sample_view = build_table_ref(project_id, dataset, "v_ModelNormalizationReviewSample")

    return f"""
CREATE OR REPLACE VIEW {report_view} AS
SELECT
  source,
  store,
  COUNTIF(item_type = 'disc') AS disc_products,
  COUNTIF(item_type = 'disc' AND normalized_model IS NOT NULL) AS normalized_model_products,
  COUNTIF(item_type = 'disc' AND normalized_model IS NULL) AS unresolved_model_products,
  SAFE_DIVIDE(
    COUNTIF(item_type = 'disc' AND normalized_model IS NOT NULL),
    COUNTIF(item_type = 'disc')
  ) AS model_coverage,
  COUNTIF(
    item_type = 'disc'
    AND STARTS_WITH(model_source, 'deterministic_v2')
    AND normalized_model IS NOT NULL
  ) AS deterministic_v2_accepts,
  COUNTIF(
    item_type = 'disc'
    AND STARTS_WITH(model_source, 'llm_v2_')
    AND normalized_model IS NOT NULL
  ) AS llm_v2_accepts,
  MAX(normalized_at) AS report_as_of
FROM {products_table}
GROUP BY source, store;

CREATE OR REPLACE VIEW {checks_view} AS
WITH checks AS (
  SELECT
    'accepted_generic_without_manufacturer' AS check_name,
    CAST(COUNTIF(
      decision_bucket = 'ACCEPT'
      AND requires_manufacturer
      AND NOT manufacturer_compatible
    ) AS FLOAT64) AS observed_value,
    0.0 AS required_value,
    COUNTIF(
      decision_bucket = 'ACCEPT'
      AND requires_manufacturer
      AND NOT manufacturer_compatible
    ) = 0 AS passed,
    'Generic aliases require compatible manufacturer evidence' AS details
  FROM {decisions_table}

  UNION ALL

  SELECT
    'accepted_context_only_aliases',
    CAST(COUNT(*) AS FLOAT64),
    0.0,
    COUNT(*) = 0,
    'Plastic, stamp, and release metadata aliases cannot be auto-accepted'
  FROM (
    SELECT product_key AS decision_key
    FROM {decisions_table}
    WHERE decision_bucket = 'ACCEPT' AND is_context_only
    UNION ALL
    SELECT variant_id AS decision_key
    FROM {variant_decisions}
    WHERE decision_bucket = 'ACCEPT' AND is_context_only
  )

  UNION ALL

  SELECT
    'multiple_product_accepts',
    CAST(COUNT(*) AS FLOAT64),
    0.0,
    COUNT(*) = 0,
    'A product may have at most one accepted model'
  FROM (
    SELECT product_key
    FROM {candidates_table}
    WHERE decision_bucket = 'ACCEPT'
    GROUP BY product_key
    HAVING COUNT(*) > 1
  )

  UNION ALL

  SELECT
    'multiple_variant_accepts',
    CAST(COUNT(*) AS FLOAT64),
    0.0,
    COUNT(*) = 0,
    'A variant may have at most one accepted override'
  FROM (
    SELECT variant_id
    FROM {variant_decisions}
    WHERE decision_bucket = 'ACCEPT'
    GROUP BY variant_id
    HAVING COUNT(*) > 1
  )

  UNION ALL

  SELECT
    'accepted_non_disc_variants',
    CAST(COUNTIF(
      decision_bucket = 'ACCEPT'
      AND REGEXP_CONTAINS(
        TRIM(REGEXP_REPLACE(
          NORMALIZE_AND_CASEFOLD(COALESCE(variant_title, ''), NFKD),
          r'[^a-z0-9]+',
          ' '
        )),
        r'{VARIANT_NON_DISC_PATTERN}'
      )
    ) AS FLOAT64),
    0.0,
    COUNTIF(
      decision_bucket = 'ACCEPT'
      AND REGEXP_CONTAINS(
        TRIM(REGEXP_REPLACE(
          NORMALIZE_AND_CASEFOLD(COALESCE(variant_title, ''), NFKD),
          r'[^a-z0-9]+',
          ' '
        )),
        r'{VARIANT_NON_DISC_PATTERN}'
      )
    ) = 0,
    'Miniature, marker, and keychain variants cannot receive model overrides'
  FROM {variant_decisions}

  UNION ALL

  SELECT
    'models_on_non_disc_products',
    CAST(COUNTIF(item_type != 'disc' AND normalized_model IS NOT NULL) AS FLOAT64),
    0.0,
    COUNTIF(item_type != 'disc' AND normalized_model IS NOT NULL) = 0,
    'Only confirmed discs may receive normalized models'
  FROM {products_table}

  UNION ALL

  SELECT
    'infinite_model_coverage_after_v2',
    SAFE_DIVIDE(COUNTIF(normalized_model IS NOT NULL), COUNT(*)),
    0.99,
    SAFE_DIVIDE(COUNTIF(normalized_model IS NOT NULL), COUNT(*)) >= 0.99,
    'Infinite source model coverage must remain at least 99%'
  FROM {products_table}
  WHERE source = 'infinite'

  UNION ALL

  SELECT
    'accepted_decisions_applied',
    CAST(COUNTIF(
      decision.decision_bucket = 'ACCEPT'
      AND product.normalized_model IS DISTINCT FROM decision.normalized_model
    ) AS FLOAT64),
    0.0,
    COUNTIF(
      decision.decision_bucket = 'ACCEPT'
      AND product.normalized_model IS DISTINCT FROM decision.normalized_model
    ) = 0,
    'Every accepted product decision must be reflected in NormalizedProducts'
  FROM {decisions_table} AS decision
  INNER JOIN {products_table} AS product
    USING (product_key)
)
SELECT * FROM checks;

CREATE OR REPLACE VIEW {comparison_view} AS
SELECT
  snapshot.id AS product_variant_id,
  snapshot.store,
  snapshot.title,
  snapshot.variant_title,
  snapshot.raw_vendor,
  snapshot.normalized_manufacturer AS v2_manufacturer,
  snapshot.normalized_model AS v2_model,
  snapshot.normalization_source AS v2_source,
  v1.canonical_manufacturer AS v1_manufacturer,
  v1.canonical_model AS v1_model,
  v1.match_method AS v1_method,
  CASE
    WHEN snapshot.normalized_model IS NOT NULL
      AND v1.canonical_model IS NOT NULL
      AND REGEXP_REPLACE(
        NORMALIZE_AND_CASEFOLD(snapshot.normalized_manufacturer, NFKD),
        r'[^a-z0-9]+',
        ''
      ) = REGEXP_REPLACE(
        NORMALIZE_AND_CASEFOLD(v1.canonical_manufacturer, NFKD),
        r'[^a-z0-9]+',
        ''
      )
      AND TRIM(REGEXP_REPLACE(
        NORMALIZE_AND_CASEFOLD(snapshot.normalized_model, NFKD),
        r'[^a-z0-9]+',
        ' '
      )) = TRIM(REGEXP_REPLACE(
        NORMALIZE_AND_CASEFOLD(
          REGEXP_REPLACE(v1.canonical_model, r'\s*\([^)]*\)\s*$', ''),
          NFKD
        ),
        r'[^a-z0-9]+',
        ' '
      )) THEN 'AGREE'
    WHEN snapshot.normalized_model IS NOT NULL AND v1.canonical_model IS NULL THEN 'V2_ONLY'
    WHEN snapshot.normalized_model IS NULL AND v1.canonical_model IS NOT NULL THEN 'V1_ONLY'
    WHEN snapshot.normalized_model IS NOT NULL AND v1.canonical_model IS NOT NULL THEN 'DIFFER'
    ELSE 'NEITHER'
  END AS comparison_result
FROM {snapshot_view} AS snapshot
LEFT JOIN {v1_matches} AS v1
  ON CAST(snapshot.product_id AS STRING) = CAST(v1.product_id AS STRING)
 AND CAST(snapshot.variant_id AS STRING) = CAST(v1.variant_id AS STRING)
 AND LOWER(TRIM(snapshot.store)) = LOWER(TRIM(v1.store))
 AND v1.is_active
 AND v1.match_status = 'matched';

CREATE OR REPLACE VIEW {sample_view} AS
WITH samples AS (
  SELECT
    CASE
      WHEN variant.decision_bucket = 'ACCEPT' THEN 'variant_override'
      WHEN decision.credible_candidate_count > 1 THEN 'multi_candidate'
      WHEN decision.is_generic THEN 'generic_alias'
      WHEN LENGTH(REGEXP_REPLACE(
        NORMALIZE_AND_CASEFOLD(decision.matched_alias, NFKD),
        r'[^a-z0-9]+',
        ''
      )) <= 4 THEN 'short_alias'
      WHEN decision.alias_type = 'derived_parenthetical' THEN 'parenthetical_alias'
      WHEN decision.source_normalized_manufacturer IS NULL THEN 'retailer_without_manufacturer'
      ELSE 'ordinary_accept'
    END AS sample_category,
    product.product_key,
    variant.variant_id,
    product.store,
    product.title,
    variant.variant_title,
    product.raw_vendor,
    product.normalized_manufacturer,
    COALESCE(variant.normalized_model, product.normalized_model) AS normalized_model,
    COALESCE(variant.decision_bucket, decision.decision_bucket, 'UNRESOLVED') AS decision_bucket,
    COALESCE(variant.decision_source, decision.decision_source, product.model_source)
      AS decision_source,
    decision.matched_alias,
    decision.credible_candidate_count
  FROM {products_table} AS product
  LEFT JOIN {decisions_table} AS decision
    USING (product_key)
  LEFT JOIN {variant_decisions} AS variant
    ON product.product_key = variant.product_key
   AND variant.decision_bucket = 'ACCEPT'
  WHERE product.source = 'shopify'
    AND product.item_type = 'disc'
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY sample_category
      ORDER BY FARM_FINGERPRINT(CONCAT(product_key, COALESCE(variant_id, '')))
    ) AS sample_rank
  FROM samples
)
SELECT * EXCEPT(sample_rank)
FROM ranked
WHERE sample_rank <= 25;
"""


def validate_model_quality_checks(rows):
    failures = [row for row in rows if not row["passed"]]
    if not failures:
        return
    messages = [
        f"{row['check_name']} observed={row['observed_value']} required={row['required_value']}"
        for row in failures
    ]
    raise RuntimeError("Model normalization quality checks failed: " + "; ".join(messages))


def run_model_normalization(client, project_id, dataset, rules_version=None):
    resolved_version = (rules_version or get_model_rules_version()).strip()

    print(f"Refreshing disc model entities and aliases in {project_id}.{dataset}")
    client.query(build_disc_model_catalog_sql(project_id, dataset, resolved_version)).result()

    print(f"Refreshing product-level disc model candidates in {project_id}.{dataset}")
    client.query(build_product_model_candidates_sql(project_id, dataset, resolved_version)).result()

    print(f"Refreshing variant-level disc model decisions in {project_id}.{dataset}")
    client.query(build_variant_model_decisions_sql(project_id, dataset, resolved_version)).result()

    from disc_golf_pipeline.services.llm_resolution import (
        LlmResolutionConfig,
        promote_llm_resolutions,
    )

    llm_config = LlmResolutionConfig.from_env()
    if llm_config.mode == "promote":
        print(f"Promoting validated LLM model resolutions in {project_id}.{dataset}")
        promotion_summary = promote_llm_resolutions(
            client,
            project_id,
            dataset,
            config=llm_config,
        )
        print(f"LLM model promotion summary: {promotion_summary}")

    print(f"Applying accepted product model decisions in {project_id}.{dataset}")
    client.query(
        build_apply_product_model_decisions_sql(project_id, dataset, resolved_version)
    ).result()

    return resolved_version


def refresh_model_quality_views(client, project_id, dataset):
    client.query(build_model_quality_views_sql(project_id, dataset)).result()
    checks_table = build_table_ref(project_id, dataset, "v_ModelNormalizationQualityChecks")
    check_rows = list(client.query(f"SELECT * FROM {checks_table} ORDER BY check_name").result())
    validate_model_quality_checks(check_rows)

    report_table = build_table_ref(project_id, dataset, "v_ModelNormalizationQualityReport")
    summary_query = f"""
      SELECT
        source,
        SUM(disc_products) AS disc_products,
        SUM(normalized_model_products) AS normalized_model_products,
        SUM(unresolved_model_products) AS unresolved_model_products,
        SAFE_DIVIDE(SUM(normalized_model_products), SUM(disc_products)) AS model_coverage,
        SUM(deterministic_v2_accepts) AS deterministic_v2_accepts,
        SUM(llm_v2_accepts) AS llm_v2_accepts
      FROM {report_table}
      GROUP BY source
      ORDER BY source
    """
    summaries = [dict(row.items()) for row in client.query(summary_query).result()]
    for summary in summaries:
        print(
            "Model normalization summary "
            f"source={summary['source']} discs={summary['disc_products']} "
            f"models={summary['normalized_model_products']} "
            f"unresolved={summary['unresolved_model_products']} "
            f"coverage={summary['model_coverage']:.3f} "
            f"v2_accepts={summary['deterministic_v2_accepts']} "
            f"llm_v2_accepts={summary['llm_v2_accepts']}"
        )
    return {
        "checks": [dict(row.items()) for row in check_rows],
        "sources": summaries,
    }
