"""Search-grade product normalization for Shopify and Infinite Discs."""

import os
import re
import unicodedata

from google.cloud import bigquery

from disc_golf_pipeline.services.model_normalization import (
    get_model_rules_version,
    refresh_model_quality_views,
    run_model_normalization,
)


DEFAULT_NORMALIZATION_VERSION = "shopify-infinite-v1"

DISC_PRODUCT_TYPE_KEYS = (
    "approach disc",
    "control driver",
    "disc",
    "disc golf",
    "disc golf disc",
    "discs",
    "distance driver",
    "distance drivers",
    "driver",
    "fairway driver",
    "fairway drivers",
    "golf disc",
    "limited edition discs",
    "mid range",
    "mid range driver",
    "mid range drivers",
    "midrange",
    "new disc",
    "production discs",
    "putt approach",
    "putt approach disc",
    "putter",
    "seconds and misprint discs",
    "used disc",
)

STOREFRONT_RETAILERS = {
    "152f17.myshopify.com": "H13 Disc Golf",
    "armorydiscgolf.com": "Armory Disc Golf",
    "blackswampdiscs.com": "Black Swamp Discs",
    "chains-or-dye-disc-golf.myshopify.com": "Chains Or Dye Disc Golf",
    "climodiscgolf.com": "Climo Disc Golf",
    "daddydiscgolf.com": "Daddy Disc Golf",
    "discgod.com": "DiscGod",
    "discgolfdealsusa.com": "Disc Golf Deals USA",
    "discgolfstar.com": "Disc Golf Star",
    "discologydiscgolf.com": "Discology Disc Golf",
    "discstore.com": "Disc Store",
    "dynamicdiscs.com": "Dynamic Discs",
    "dzdiscs.com": "DZ Discs",
    "fadegear.com": "Fade Gear",
    "fishdiscgolf.com": "Fish's Disc Golf Store",
    "flightfactorydiscs.com": "Flight Factory Discs",
    "flydesertdiscgolf.myshopify.com": "Fly Desert Disc Golf",
    "foundationdiscs.com": "Foundation Disc Golf",
    "gatewaydiscsports.com": "Gateway Disc Sports",
    "golfdisco.com": "GolfDisco",
    "gottagogottathrow.com": "Gotta Go Gotta Throw",
    "gravitydiscgolf.com": "Gravity Disc Golf",
    "jerseydiscs.com": "Jersey Discs",
    "maverickdiscgolf.com": "Maverick Disc Golf",
    "nashvillediscgolfstore.com": "Nashville Disc Golf Store",
    "oldmendiscgolf.myshopify.com": "Old Men Disc Golf",
    "outofboundsdiscgolf.com": "Out of Bounds Disc Golf",
    "pounddiscgolf.com": "Pound Disc Golf",
    "prodigy-disc-store.myshopify.com": "Prodigy Disc Store",
    "reaperdiscs.com": "Reaper Disc Supply",
    "russelldiscgolf.com": "Russell Disc Golf",
    "scissortaildiscgolf.com": "Scissortail Disc Golf",
    "shop.dgpt.com": "DGPT Pro Shop",
    "shop.discountdiscgolf.com": "Discount Disc Golf",
    "shopledgestone.com": "Ledgestone",
    "skybreed-discs.com": "Skybreed Discs",
    "skylinediscs.com": "Skyline Discs",
    "throwshop.us": "Throw Shop",
    "titandiscgolf.com": "Titan Disc Golf",
    "treemagnets.com": "Treemagnets Disc Golf Supply",
    "trifoxdiscgolf.com": "Tri-Fox Disc Golf",
    "wanderdiscgolf.com": "Wander Disc Golf",
    "woodlandvalley.com": "Woodland Valley Disc Golf",
}

BRAND_STOREFRONTS = {
    "152f17.myshopify.com",
    "climodiscgolf.com",
    "dynamicdiscs.com",
    "gatewaydiscsports.com",
    "prodigy-disc-store.myshopify.com",
}

RETAILER_STOREFRONTS = {
    "fishdiscgolf.com",
    "flydesertdiscgolf.myshopify.com",
    "oldmendiscgolf.myshopify.com",
    "russelldiscgolf.com",
    "shopledgestone.com",
    "wanderdiscgolf.com",
}

RETAILER_VENDOR_EXTRAS = {
    "foundationdiscs.com": ("Foundation Disc Golf",),
    "shopledgestone.com": ("Ledgestone",),
    "flydesertdiscgolf.myshopify.com": ("Fly Desert Disc Golf", "My Store"),
    "oldmendiscgolf.myshopify.com": ("Old Men Disc Golf",),
    "russelldiscgolf.com": ("Russell Disc Golf", "Russell Disc Golf Supplies"),
    "wanderdiscgolf.com": ("Wander Disc Golf", "Flight Factory Discs"),
}

# Some mixed storefronts also sell discs under a house brand that intentionally
# matches the retailer label. Keep these stores in MIXED mode so ordinary
# retailer-label leakage is still rejected, and exempt only the exact canonical
# house-brand manufacturer after a model decision has been accepted.
STOREFRONT_HOUSE_BRANDS = {
    "shop.discountdiscgolf.com": ("Discount Disc Golf",),
}

NON_BLOCKING_QUALITY_CHECKS = frozenset(
    {
        "retailer_labels_as_manufacturers",
    }
)

PRODUCT_TYPE_ITEM_PATTERNS = (
    ("cart", r"(^| )(disc golf )?carts?( |$)"),
    ("basket", r"(^| )(disc golf )?baskets?( |$)"),
    ("bag", r"(^| )(disc golf )?(bags?|backpacks?|slings?)( |$)"),
    (
        "apparel",
        r"(^| )(apparel|shirts?|t shirts?|jerseys?|hoodies?|hats?|beanies?|socks?|pants?|shorts?|polos?|tank tops?|sweatshirts?)( |$)",
    ),
    (
        "accessory",
        r"(^| )(accessories|accessory|towels?|mini marker discs?|mini markers?|minis?|patches?|pins?|stickers?|chalk|grip aids?|retrievers?|rangefinders?|gift cards?|keychains?|umbrellas?|bottles?|mugs?|flags?|phone cases?)( |$)",
    ),
    ("other", r"(^| )(games?|home decor|simulator|miniature furniture)( |$)"),
)

TEXT_ITEM_PATTERNS = (
    ("cart", r"\bdisc golf cart\b|\bcart accessories?\b"),
    ("basket", r"\bdisc golf basket\b|\bpractice basket\b"),
    ("bag", r"\bdisc golf bags?\b|\bbackpacks?\b|\bshoulder bags?\b"),
    (
        "apparel",
        r"\b(t[ -]?shirts?|shirts?|jerseys?|hoodies?|hats?|beanies?|socks?|pants?|shorts?|polos?|tank tops?|sweatshirts?)\b",
    ),
    (
        "accessory",
        r"\b(towels?|mini markers?|patches?|pins?|stickers?|retrievers?|rangefinders?|gift cards?|keychains?|umbrellas?|water bottles?|mugs?|flags?|phone cases?)\b",
    ),
)

DISC_PRODUCT_TYPE_PATTERN = (
    r"(^| )(distance|fairway|control) drivers?( |$)|"
    r"(^| )mid range( drivers?)?( |$)|"
    r"(^| )(putters?|putt approach|approach discs?|golf discs?|discs?)( |$)"
)
DISC_TEXT_PATTERN = (
    r"\b(distance|fairway|control) driver\b|\bmid[ -]?range\b|"
    r"\bputt(er)?\b|\bapproach disc\b|\bdisc\b"
)


def get_normalization_version():
    return os.getenv("NORMALIZATION_RULES_VERSION", DEFAULT_NORMALIZATION_VERSION).strip()


def build_table_ref(project_id, dataset, table_name):
    return f"`{project_id}.{dataset}.{table_name}`"


def normalize_match_key(value):
    normalized = unicodedata.normalize("NFKD", value or "").casefold()
    return re.sub(r"[^a-z0-9]+", "", normalized)


def normalize_text_key(value):
    normalized = unicodedata.normalize("NFKD", value or "").casefold()
    return re.sub(r"[^a-z0-9]+", " ", normalized).strip()


def classify_item_type(product_type, title="", tags="", source="shopify"):
    """Mirror the ordered SQL rules for small local fixtures and diagnostics."""
    if source == "infinite":
        return "disc", 1.0, "infinite_source"

    product_type_key = normalize_text_key(product_type)
    combined_text = normalize_text_key(f"{title or ''} {tags or ''}")

    for item_type, pattern in PRODUCT_TYPE_ITEM_PATTERNS:
        if re.search(pattern, product_type_key):
            return item_type, 0.99, "product_type_negative_rule"

    for item_type, pattern in TEXT_ITEM_PATTERNS:
        if re.search(pattern, combined_text):
            return item_type, 0.95, "title_tags_negative_rule"

    if product_type_key in DISC_PRODUCT_TYPE_KEYS or re.search(
        DISC_PRODUCT_TYPE_PATTERN, product_type_key
    ):
        return "disc", 0.98, "product_type_disc_rule"

    if re.search(DISC_TEXT_PATTERN, combined_text):
        return "disc", 0.85, "title_tags_disc_rule"

    return "unknown", 0.0, "insufficient_evidence"


def storefront_vendor_mode(store):
    if store in BRAND_STOREFRONTS:
        return "BRAND"
    if store in RETAILER_STOREFRONTS:
        return "RETAILER"
    return "MIXED"


def _sql_quote(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _sql_string_array(values):
    return "[" + ", ".join(_sql_quote(value) for value in values) + "]"


def _house_brand_quality_exception_sql(product_alias="products"):
    exceptions = []
    for store, manufacturers in sorted(STOREFRONT_HOUSE_BRANDS.items()):
        for manufacturer in sorted(manufacturers):
            exceptions.append(
                "("
                f"{product_alias}.store = {_sql_quote(store)} "
                "AND REGEXP_REPLACE("
                "NORMALIZE_AND_CASEFOLD("
                f"COALESCE({product_alias}.normalized_manufacturer, ''), NFKD"
                "), r'[^a-z0-9]+', '') "
                f"= {_sql_quote(normalize_match_key(manufacturer))} "
                f"AND {product_alias}.normalized_model IS NOT NULL "
                "AND ("
                f"STARTS_WITH({product_alias}.model_source, 'deterministic_v2') "
                f"OR STARTS_WITH({product_alias}.model_source, 'llm_v2_')"
                ")"
                ")"
            )
    if not exceptions:
        return "FALSE"
    return "(\n        " + "\n        OR ".join(exceptions) + "\n      )"


def _retailer_label_collision_sql(product_alias="products", rules_alias="rules"):
    house_brand_exception = _house_brand_quality_exception_sql(product_alias)
    return f"""{rules_alias}.vendor_mode != 'BRAND'
      AND REGEXP_REPLACE(
        NORMALIZE_AND_CASEFOLD(
          COALESCE({product_alias}.normalized_manufacturer, ''), NFKD
        ),
        r'[^a-z0-9]+',
        ''
      ) IN UNNEST({rules_alias}.retailer_vendor_values)
      AND NOT {house_brand_exception}"""


def _storefront_seed_structs(rules_version):
    structs = []
    for store, retailer in sorted(STOREFRONT_RETAILERS.items()):
        retailer_vendor_values = {
            normalize_match_key(retailer),
            normalize_match_key(store.split(".", 1)[0]),
        }
        retailer_vendor_values.update(
            normalize_match_key(value) for value in RETAILER_VENDOR_EXTRAS.get(store, ())
        )
        retailer_vendor_values.discard("")
        structs.append(
            "STRUCT("
            f"{_sql_quote(store)} AS store, "
            f"{_sql_quote(retailer)} AS retailer, "
            f"{_sql_quote(storefront_vendor_mode(store))} AS vendor_mode, "
            f"{_sql_string_array(sorted(retailer_vendor_values))} AS retailer_vendor_values, "
            "ARRAY<STRING>[] AS trusted_vendor_values, "
            f"{_sql_string_array(DISC_PRODUCT_TYPE_KEYS)} AS disc_product_types, "
            "PARSE_JSON('{\"strategy\":\"ordered-evidence-v1\"}') AS item_type_rules, "
            f"{_sql_quote(rules_version)} AS rules_version"
            ")"
        )
    return ",\n    ".join(structs)


def build_storefront_rules_sql(project_id, dataset, rules_version):
    rules_table = build_table_ref(project_id, dataset, "StorefrontNormalizationRules")
    product_info_table = build_table_ref(project_id, dataset, "ProductInfo")
    seed_structs = _storefront_seed_structs(rules_version)
    disc_types = _sql_string_array(DISC_PRODUCT_TYPE_KEYS)

    return f"""
CREATE TABLE IF NOT EXISTS {rules_table} (
  store STRING NOT NULL,
  retailer STRING NOT NULL,
  vendor_mode STRING NOT NULL,
  retailer_vendor_values ARRAY<STRING>,
  trusted_vendor_values ARRAY<STRING>,
  disc_product_types ARRAY<STRING>,
  item_type_rules JSON,
  rules_version STRING NOT NULL,
  is_active BOOL NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
CLUSTER BY store;

MERGE {rules_table} AS target
USING UNNEST([
    {seed_structs}
]) AS source
ON target.store = source.store
WHEN MATCHED THEN UPDATE SET
  retailer = source.retailer,
  vendor_mode = source.vendor_mode,
  retailer_vendor_values = source.retailer_vendor_values,
  trusted_vendor_values = source.trusted_vendor_values,
  disc_product_types = source.disc_product_types,
  item_type_rules = source.item_type_rules,
  rules_version = source.rules_version,
  is_active = TRUE,
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  store,
  retailer,
  vendor_mode,
  retailer_vendor_values,
  trusted_vendor_values,
  disc_product_types,
  item_type_rules,
  rules_version,
  is_active,
  updated_at
) VALUES (
  source.store,
  source.retailer,
  source.vendor_mode,
  source.retailer_vendor_values,
  source.trusted_vendor_values,
  source.disc_product_types,
  source.item_type_rules,
  source.rules_version,
  TRUE,
  CURRENT_TIMESTAMP()
);

INSERT INTO {rules_table} (
  store,
  retailer,
  vendor_mode,
  retailer_vendor_values,
  trusted_vendor_values,
  disc_product_types,
  item_type_rules,
  rules_version,
  is_active,
  updated_at
)
SELECT
  observed.store,
  observed.store AS retailer,
  'MIXED' AS vendor_mode,
  [REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(observed.store, NFKD), r'[^a-z0-9]+', '')],
  ARRAY<STRING>[],
  {disc_types},
  PARSE_JSON('{{"strategy":"ordered-evidence-v1","seed":"automatic"}}'),
  {_sql_quote(rules_version)},
  TRUE,
  CURRENT_TIMESTAMP()
FROM (
  SELECT DISTINCT LOWER(TRIM(CAST(Store AS STRING))) AS store
  FROM {product_info_table}
  WHERE NULLIF(TRIM(CAST(Store AS STRING)), '') IS NOT NULL
) AS observed
LEFT JOIN {rules_table} AS existing
  ON observed.store = existing.store
WHERE existing.store IS NULL;
"""


def _build_item_classification_case(product_type_expr, combined_text_expr):
    lines = ["CASE"]
    for item_type, pattern in PRODUCT_TYPE_ITEM_PATTERNS:
        lines.append(
            f"  WHEN REGEXP_CONTAINS({product_type_expr}, r'{pattern}') THEN '{item_type}'"
        )
    for item_type, pattern in TEXT_ITEM_PATTERNS:
        lines.append(
            f"  WHEN REGEXP_CONTAINS({combined_text_expr}, r'{pattern}') THEN '{item_type}'"
        )
    lines.extend(
        [
            f"  WHEN {product_type_expr} IN UNNEST(disc_product_types) THEN 'disc'",
            f"  WHEN REGEXP_CONTAINS({product_type_expr}, r'{DISC_PRODUCT_TYPE_PATTERN}') THEN 'disc'",
            f"  WHEN REGEXP_CONTAINS({combined_text_expr}, r'{DISC_TEXT_PATTERN}') THEN 'disc'",
            "  ELSE 'unknown'",
            "END",
        ]
    )
    return "\n".join(lines)


def _build_item_source_case(product_type_expr, combined_text_expr):
    negative_product_patterns = "|".join(f"(?:{pattern})" for _, pattern in PRODUCT_TYPE_ITEM_PATTERNS)
    negative_text_patterns = "|".join(f"(?:{pattern})" for _, pattern in TEXT_ITEM_PATTERNS)
    return f"""CASE
  WHEN REGEXP_CONTAINS({product_type_expr}, r'{negative_product_patterns}')
    THEN 'product_type_negative_rule'
  WHEN REGEXP_CONTAINS({combined_text_expr}, r'{negative_text_patterns}')
    THEN 'title_tags_negative_rule'
  WHEN {product_type_expr} IN UNNEST(disc_product_types)
    OR REGEXP_CONTAINS({product_type_expr}, r'{DISC_PRODUCT_TYPE_PATTERN}')
    THEN 'product_type_disc_rule'
  WHEN REGEXP_CONTAINS({combined_text_expr}, r'{DISC_TEXT_PATTERN}')
    THEN 'title_tags_disc_rule'
  ELSE 'insufficient_evidence'
END"""


def build_normalized_products_sql(
    project_id,
    dataset,
    rules_version,
    model_rules_version=None,
):
    resolved_model_rules_version = (
        model_rules_version or get_model_rules_version()
    ).strip()
    product_info_table = build_table_ref(project_id, dataset, "ProductInfo")
    infinite_discs_table = build_table_ref(project_id, dataset, "InfiniteDiscs")
    aliases_table = build_table_ref(project_id, dataset, "DiscManufacturerAliases")
    rules_table = build_table_ref(project_id, dataset, "StorefrontNormalizationRules")
    normalized_table = build_table_ref(project_id, dataset, "NormalizedProducts")
    audit_table = build_table_ref(project_id, dataset, "ProductNormalizationAudit")

    product_type_expr = "product_type_key"
    combined_text_expr = "combined_text_key"
    item_type_case = _build_item_classification_case(product_type_expr, combined_text_expr)
    item_source_case = _build_item_source_case(product_type_expr, combined_text_expr)

    return f"""
CREATE TABLE IF NOT EXISTS {normalized_table} (
  product_key STRING NOT NULL,
  source STRING NOT NULL,
  store STRING NOT NULL,
  product_id STRING NOT NULL,
  source_product_id STRING,
  retailer STRING,
  raw_vendor STRING,
  normalized_manufacturer STRING,
  manufacturer_confidence FLOAT64,
  manufacturer_source STRING,
  normalized_model STRING,
  model_confidence FLOAT64,
  model_source STRING,
  item_type STRING NOT NULL,
  is_disc BOOL,
  item_type_confidence FLOAT64 NOT NULL,
  item_type_source STRING NOT NULL,
  title STRING,
  product_type STRING,
  tags STRING,
  normalization_version STRING NOT NULL,
  normalization_evidence JSON,
  source_row_hash STRING NOT NULL,
  normalized_at TIMESTAMP NOT NULL
)
CLUSTER BY source, store, item_type;

CREATE TABLE IF NOT EXISTS {audit_table} (
  product_key STRING NOT NULL,
  variant_id STRING,
  source STRING,
  store STRING,
  change_type STRING NOT NULL,
  prior_item_type STRING,
  new_item_type STRING,
  prior_normalized_manufacturer STRING,
  new_normalized_manufacturer STRING,
  prior_normalized_model STRING,
  new_normalized_model STRING,
  decision_source STRING,
  rules_version STRING NOT NULL,
  event_timestamp TIMESTAMP NOT NULL
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY source, store;

CREATE TEMP TABLE NewNormalizedProducts AS
WITH active_aliases AS (
  SELECT
    alias_match_key,
    canonical_manufacturer
  FROM {aliases_table}
  WHERE is_active
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY alias_match_key
    ORDER BY IF(alias_source = 'curated', 0, 1), canonical_manufacturer
  ) = 1
),
shopify_prepared AS (
  SELECT
    CONCAT(
      'shopify:',
      LOWER(TRIM(CAST(i.Store AS STRING))),
      ':',
      CAST(FARM_FINGERPRINT(CONCAT(
        CAST(i.MainProductId AS STRING),
        CAST(i.Store AS STRING)
      )) AS STRING)
    ) AS product_key,
    'shopify' AS source,
    LOWER(TRIM(CAST(i.Store AS STRING))) AS store,
    CAST(FARM_FINGERPRINT(CONCAT(
      CAST(i.MainProductId AS STRING),
      CAST(i.Store AS STRING)
    )) AS STRING) AS product_id,
    CAST(i.MainProductId AS STRING) AS source_product_id,
    rules.retailer,
    CAST(i.Vendor AS STRING) AS raw_vendor,
    REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(COALESCE(CAST(i.Vendor AS STRING), ''), NFKD),
      r'[^a-z0-9]+',
      ''
    ) AS vendor_key,
    rules.vendor_mode,
    rules.retailer_vendor_values,
    rules.trusted_vendor_values,
    rules.disc_product_types,
    CAST(i.Title AS STRING) AS title,
    CAST(i.ProductType AS STRING) AS product_type,
    CAST(i.Tags AS STRING) AS tags,
    CAST(i.BodyHtml AS STRING) AS body_html,
    TRIM(REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(COALESCE(CAST(i.ProductType AS STRING), ''), NFKD),
      r'[^a-z0-9]+',
      ' '
    )) AS product_type_key,
    TRIM(REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(CONCAT(
        COALESCE(CAST(i.Title AS STRING), ''), ' ',
        COALESCE(CAST(i.Tags AS STRING), '')
      ), NFKD),
      r'[^a-z0-9]+',
      ' '
    )) AS combined_text_key,
    rules.rules_version,
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
      i.MainProductId,
      i.Store,
      i.Title,
      i.Vendor,
      i.ProductType,
      i.Tags,
      i.BodyHtml
    )))) AS source_row_hash
  FROM {product_info_table} AS i
  INNER JOIN {rules_table} AS rules
    ON LOWER(TRIM(CAST(i.Store AS STRING))) = rules.store
   AND rules.is_active
),
shopify_classified AS (
  SELECT
    prepared.*,
    alias.canonical_manufacturer AS alias_manufacturer,
    {item_type_case} AS classified_item_type,
    {item_source_case} AS classified_item_source
  FROM shopify_prepared AS prepared
  LEFT JOIN active_aliases AS alias
    ON prepared.vendor_key = alias.alias_match_key
),
shopify_normalized_base AS (
  SELECT
    product_key,
    source,
    store,
    product_id,
    source_product_id,
    retailer,
    raw_vendor,
    CASE
      WHEN vendor_mode = 'RETAILER' THEN NULL
      WHEN vendor_mode != 'BRAND' AND vendor_key IN UNNEST(retailer_vendor_values) THEN NULL
      WHEN ARRAY_LENGTH(trusted_vendor_values) > 0
        AND vendor_key NOT IN UNNEST(trusted_vendor_values) THEN NULL
      ELSE alias_manufacturer
    END AS normalized_manufacturer,
    CASE
      WHEN alias_manufacturer IS NULL OR vendor_mode = 'RETAILER' THEN NULL
      WHEN vendor_mode != 'BRAND' AND vendor_key IN UNNEST(retailer_vendor_values) THEN NULL
      WHEN ARRAY_LENGTH(trusted_vendor_values) > 0
        AND vendor_key NOT IN UNNEST(trusted_vendor_values) THEN NULL
      ELSE 0.97
    END AS manufacturer_confidence,
    CASE
      WHEN alias_manufacturer IS NULL THEN 'unresolved_vendor'
      WHEN vendor_mode = 'RETAILER' THEN 'retailer_vendor_ignored'
      WHEN vendor_mode != 'BRAND' AND vendor_key IN UNNEST(retailer_vendor_values)
        THEN 'retailer_vendor_ignored'
      WHEN ARRAY_LENGTH(trusted_vendor_values) > 0
        AND vendor_key NOT IN UNNEST(trusted_vendor_values) THEN 'untrusted_vendor_ignored'
      ELSE 'recognized_vendor_alias'
    END AS manufacturer_source,
    CAST(NULL AS STRING) AS normalized_model,
    CAST(NULL AS FLOAT64) AS model_confidence,
    'not_attempted_phase1' AS model_source,
    classified_item_type AS item_type,
    CASE
      WHEN classified_item_type = 'disc' THEN TRUE
      WHEN classified_item_type = 'unknown' THEN NULL
      ELSE FALSE
    END AS is_disc,
    CASE
      WHEN classified_item_source = 'product_type_negative_rule' THEN 0.99
      WHEN classified_item_source = 'title_tags_negative_rule' THEN 0.95
      WHEN classified_item_source = 'product_type_disc_rule' THEN 0.98
      WHEN classified_item_source = 'title_tags_disc_rule' THEN 0.85
      ELSE 0.0
    END AS item_type_confidence,
    classified_item_source AS item_type_source,
    title,
    product_type,
    tags,
    rules_version AS normalization_version,
    TO_JSON(STRUCT(
      vendor_mode,
      vendor_key,
      classified_item_source AS item_type_decision,
      product_type_key,
      alias_manufacturer AS matched_vendor_alias
    )) AS normalization_evidence,
    source_row_hash
  FROM shopify_classified
),
shopify_normalized AS (
  SELECT
    base.* REPLACE (
      COALESCE(
        base.normalized_manufacturer,
        IF(
          base.item_type = 'disc'
          AND existing.source_row_hash = base.source_row_hash
          AND STARTS_WITH(existing.model_source, 'deterministic_v2')
          AND JSON_VALUE(existing.normalization_evidence, '$.model_decision.model_rules_version')
            = {_sql_quote(resolved_model_rules_version)},
          existing.normalized_manufacturer,
          NULL
        )
      ) AS normalized_manufacturer,
      IF(
        base.normalized_manufacturer IS NOT NULL,
        base.manufacturer_confidence,
        IF(
          base.item_type = 'disc'
          AND existing.source_row_hash = base.source_row_hash
          AND STARTS_WITH(existing.model_source, 'deterministic_v2')
          AND JSON_VALUE(existing.normalization_evidence, '$.model_decision.model_rules_version')
            = {_sql_quote(resolved_model_rules_version)},
          existing.manufacturer_confidence,
          NULL
        )
      ) AS manufacturer_confidence,
      IF(
        base.normalized_manufacturer IS NOT NULL,
        base.manufacturer_source,
        IF(
          base.item_type = 'disc'
          AND existing.source_row_hash = base.source_row_hash
          AND STARTS_WITH(existing.model_source, 'deterministic_v2')
          AND JSON_VALUE(existing.normalization_evidence, '$.model_decision.model_rules_version')
            = {_sql_quote(resolved_model_rules_version)},
          existing.manufacturer_source,
          base.manufacturer_source
        )
      ) AS manufacturer_source,
      IF(
        base.item_type = 'disc'
        AND existing.source_row_hash = base.source_row_hash
        AND STARTS_WITH(existing.model_source, 'deterministic_v2')
        AND JSON_VALUE(existing.normalization_evidence, '$.model_decision.model_rules_version')
          = {_sql_quote(resolved_model_rules_version)},
        existing.normalized_model,
        NULL
      ) AS normalized_model,
      IF(
        base.item_type = 'disc'
        AND existing.source_row_hash = base.source_row_hash
        AND STARTS_WITH(existing.model_source, 'deterministic_v2')
        AND JSON_VALUE(existing.normalization_evidence, '$.model_decision.model_rules_version')
          = {_sql_quote(resolved_model_rules_version)},
        existing.model_confidence,
        NULL
      ) AS model_confidence,
      IF(
        base.item_type = 'disc'
        AND existing.source_row_hash = base.source_row_hash
        AND STARTS_WITH(existing.model_source, 'deterministic_v2')
        AND JSON_VALUE(existing.normalization_evidence, '$.model_decision.model_rules_version')
          = {_sql_quote(resolved_model_rules_version)},
        existing.model_source,
        base.model_source
      ) AS model_source,
      IF(
        base.item_type = 'disc'
        AND existing.source_row_hash = base.source_row_hash
        AND STARTS_WITH(existing.model_source, 'deterministic_v2')
        AND JSON_VALUE(existing.normalization_evidence, '$.model_decision.model_rules_version')
          = {_sql_quote(resolved_model_rules_version)},
        existing.normalization_evidence,
        base.normalization_evidence
      ) AS normalization_evidence
    )
  FROM shopify_normalized_base AS base
  LEFT JOIN {normalized_table} AS existing
    ON base.product_key = existing.product_key
),
infinite_ranked AS (
  SELECT
    p.*,
    CAST(FARM_FINGERPRINT(CONCAT(
      CAST(p.ManufacturerName AS STRING),
      CAST(p.PlasticName AS STRING),
      CAST(p.ModelName AS STRING),
      'infinitediscs'
    )) AS STRING) AS normalized_product_id,
    REGEXP_REPLACE(
      NORMALIZE_AND_CASEFOLD(COALESCE(CAST(p.ManufacturerName AS STRING), ''), NFKD),
      r'[^a-z0-9]+',
      ''
    ) AS manufacturer_key,
    ROW_NUMBER() OVER (
      PARTITION BY
        p.ManufacturerName,
        p.PlasticName,
        p.ModelName
      ORDER BY p.record_timestamp DESC, p.Id DESC
    ) AS product_rank
  FROM {infinite_discs_table} AS p
),
infinite_normalized AS (
  SELECT
    CONCAT('infinite:infinitediscs:', p.normalized_product_id) AS product_key,
    'infinite' AS source,
    'infinitediscs' AS store,
    p.normalized_product_id AS product_id,
    CAST(p.Id AS STRING) AS source_product_id,
    'Infinite Discs' AS retailer,
    CAST(p.ManufacturerName AS STRING) AS raw_vendor,
    COALESCE(alias.canonical_manufacturer, NULLIF(TRIM(CAST(p.ManufacturerName AS STRING)), ''))
      AS normalized_manufacturer,
    1.0 AS manufacturer_confidence,
    IF(alias.canonical_manufacturer IS NULL, 'infinite_source', 'infinite_source_alias')
      AS manufacturer_source,
    NULLIF(TRIM(CAST(p.ModelName AS STRING)), '') AS normalized_model,
    1.0 AS model_confidence,
    'infinite_source' AS model_source,
    'disc' AS item_type,
    TRUE AS is_disc,
    1.0 AS item_type_confidence,
    'infinite_source' AS item_type_source,
    CONCAT(
      COALESCE(p.ManufacturerName, ''), ' ',
      COALESCE(p.PlasticName, ''), ' ',
      COALESCE(p.ModelName, '')
    ) AS title,
    'Discs' AS product_type,
    CAST(NULL AS STRING) AS tags,
    {_sql_quote(rules_version)} AS normalization_version,
    TO_JSON(STRUCT(
      p.ManufacturerName AS source_manufacturer,
      p.ModelName AS source_model,
      alias.canonical_manufacturer AS matched_manufacturer_alias
    )) AS normalization_evidence,
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
      p.ManufacturerName,
      p.PlasticName,
      p.ModelName,
      p.ModelLink,
      p.ModelDescription
    )))) AS source_row_hash
  FROM infinite_ranked AS p
  LEFT JOIN active_aliases AS alias
    ON p.manufacturer_key = alias.alias_match_key
  WHERE p.product_rank = 1
)
SELECT
  *,
  CURRENT_TIMESTAMP() AS normalized_at
FROM shopify_normalized
UNION ALL
SELECT
  *,
  CURRENT_TIMESTAMP() AS normalized_at
FROM infinite_normalized;

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
  COALESCE(current_product.product_key, prior_product.product_key),
  NULL AS variant_id,
  COALESCE(current_product.source, prior_product.source),
  COALESCE(current_product.store, prior_product.store),
  CASE
    WHEN prior_product.product_key IS NULL THEN 'INSERT'
    WHEN current_product.product_key IS NULL THEN 'DELETE'
    ELSE 'UPDATE'
  END AS change_type,
  prior_product.item_type,
  current_product.item_type,
  prior_product.normalized_manufacturer,
  current_product.normalized_manufacturer,
  prior_product.normalized_model,
  current_product.normalized_model,
  CONCAT(
    COALESCE(current_product.item_type_source, prior_product.item_type_source, ''), '|',
    COALESCE(current_product.manufacturer_source, prior_product.manufacturer_source, ''), '|',
    COALESCE(current_product.model_source, prior_product.model_source, '')
  ) AS decision_source,
  COALESCE(current_product.normalization_version, prior_product.normalization_version),
  CURRENT_TIMESTAMP()
FROM {normalized_table} AS prior_product
FULL OUTER JOIN NewNormalizedProducts AS current_product
  ON prior_product.product_key = current_product.product_key
WHERE prior_product.product_key IS NULL
   OR current_product.product_key IS NULL
   OR prior_product.source_row_hash IS DISTINCT FROM current_product.source_row_hash
   OR prior_product.item_type IS DISTINCT FROM current_product.item_type
   OR prior_product.is_disc IS DISTINCT FROM current_product.is_disc
   OR prior_product.normalized_manufacturer IS DISTINCT FROM current_product.normalized_manufacturer
   OR prior_product.normalized_model IS DISTINCT FROM current_product.normalized_model
   OR prior_product.normalization_version IS DISTINCT FROM current_product.normalization_version;

MERGE {normalized_table} AS target
USING NewNormalizedProducts AS source
ON target.product_key = source.product_key
WHEN MATCHED AND (
  target.source_row_hash IS DISTINCT FROM source.source_row_hash
  OR target.item_type IS DISTINCT FROM source.item_type
  OR target.is_disc IS DISTINCT FROM source.is_disc
  OR target.normalized_manufacturer IS DISTINCT FROM source.normalized_manufacturer
  OR target.normalized_model IS DISTINCT FROM source.normalized_model
  OR target.normalization_version IS DISTINCT FROM source.normalization_version
) THEN UPDATE SET
  source = source.source,
  store = source.store,
  product_id = source.product_id,
  source_product_id = source.source_product_id,
  retailer = source.retailer,
  raw_vendor = source.raw_vendor,
  normalized_manufacturer = source.normalized_manufacturer,
  manufacturer_confidence = source.manufacturer_confidence,
  manufacturer_source = source.manufacturer_source,
  normalized_model = source.normalized_model,
  model_confidence = source.model_confidence,
  model_source = source.model_source,
  item_type = source.item_type,
  is_disc = source.is_disc,
  item_type_confidence = source.item_type_confidence,
  item_type_source = source.item_type_source,
  title = source.title,
  product_type = source.product_type,
  tags = source.tags,
  normalization_version = source.normalization_version,
  normalization_evidence = source.normalization_evidence,
  source_row_hash = source.source_row_hash,
  normalized_at = source.normalized_at
WHEN NOT MATCHED BY TARGET THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE THEN DELETE;
"""


def build_normalized_variant_snapshot_sql(project_id, dataset):
    shopify_view = build_table_ref(project_id, dataset, "v_ShopifyVariants")
    infinite_view = build_table_ref(project_id, dataset, "v_InfiniteVariants")
    normalized_products = build_table_ref(project_id, dataset, "NormalizedProducts")
    variant_model_decisions = build_table_ref(project_id, dataset, "VariantDiscModelDecisions")
    destination_view = build_table_ref(project_id, dataset, "NormalizedVariantSnapshot")

    return f"""
CREATE OR REPLACE VIEW {destination_view} AS
WITH raw_variants AS (
  SELECT
    'shopify' AS source,
    CONCAT('shopify:', LOWER(TRIM(CAST(src.store AS STRING))), ':', CAST(src.product_id AS STRING))
      AS product_key,
    CAST(src.id AS STRING) AS id,
    CAST(src.product_id AS STRING) AS product_id,
    CAST(src.variant_id AS STRING) AS variant_id,
    CONCAT(
      'shopify:',
      LOWER(TRIM(CAST(src.store AS STRING))),
      ':',
      CAST(src.variant_id AS STRING)
    ) AS source_variant_key,
    CAST(src.title AS STRING) AS title,
    CAST(src.vendor AS STRING) AS vendor,
    CAST(src.product_link AS STRING) AS product_link,
    CAST(src.store AS STRING) AS store,
    CAST(src.store_url AS STRING) AS store_url,
    CAST(src.image AS STRING) AS image,
    CAST(src.variant_title AS STRING) AS variant_title,
    SAFE_CAST(src.price AS FLOAT64) AS price,
    SAFE_CAST(src.weight_g AS INT64) AS weight_g,
    CAST(src.in_stock AS BOOL) AS in_stock,
    CAST(src.variant_image AS STRING) AS variant_image,
    SAFE_CAST(src.high_price AS FLOAT64) AS high_price,
    SAFE_CAST(src.low_price AS FLOAT64) AS low_price,
    CAST(src.tags AS STRING) AS tags,
    CAST(src.IsDistanceDriver AS BOOL) AS IsDistanceDriver,
    CAST(src.IsFairwayDriver AS BOOL) AS IsFairwayDriver,
    CAST(src.IsMidrange AS BOOL) AS IsMidrange,
    CAST(src.IsPutter AS BOOL) AS IsPutter,
    CAST(src.BodyHtml AS STRING) AS BodyHtml,
    CAST(src.product_type AS STRING) AS product_type
  FROM {shopify_view} AS src

  UNION ALL

  SELECT
    'infinite' AS source,
    CONCAT('infinite:infinitediscs:', CAST(src.product_id AS STRING)) AS product_key,
    CAST(src.id AS STRING) AS id,
    CAST(src.product_id AS STRING) AS product_id,
    CAST(src.variant_id AS STRING) AS variant_id,
    CONCAT(
      'infinite:',
      CAST(src.product_id AS STRING),
      ':',
      CAST(src.variant_id AS STRING)
    ) AS source_variant_key,
    CAST(src.title AS STRING) AS title,
    CAST(src.vendor AS STRING) AS vendor,
    CAST(src.product_link AS STRING) AS product_link,
    CAST(src.store AS STRING) AS store,
    CAST(src.store_url AS STRING) AS store_url,
    CAST(src.image AS STRING) AS image,
    CAST(src.variant_title AS STRING) AS variant_title,
    SAFE_CAST(src.price AS FLOAT64) AS price,
    SAFE_CAST(src.weight_g AS INT64) AS weight_g,
    CAST(src.in_stock AS BOOL) AS in_stock,
    CAST(src.variant_image AS STRING) AS variant_image,
    SAFE_CAST(src.high_price AS FLOAT64) AS high_price,
    SAFE_CAST(src.low_price AS FLOAT64) AS low_price,
    CAST(src.tags AS STRING) AS tags,
    CAST(src.IsDistanceDriver AS BOOL) AS IsDistanceDriver,
    CAST(src.IsFairwayDriver AS BOOL) AS IsFairwayDriver,
    CAST(src.IsMidrange AS BOOL) AS IsMidrange,
    CAST(src.IsPutter AS BOOL) AS IsPutter,
    CAST(src.BodyHtml AS STRING) AS BodyHtml,
    CAST(src.product_type AS STRING) AS product_type
  FROM {infinite_view} AS src
),
deduplicated_raw_variants AS (
  SELECT * EXCEPT(snapshot_rank)
  FROM (
    SELECT
      raw.*,
      ROW_NUMBER() OVER (
        PARTITION BY raw.id
        ORDER BY TO_HEX(SHA256(TO_JSON_STRING(raw))) DESC
      ) AS snapshot_rank
    FROM raw_variants AS raw
  )
  WHERE snapshot_rank = 1
)
SELECT
  raw.id,
  raw.product_id,
  raw.variant_id,
  raw.source_variant_key,
  raw.source,
  raw.title,
  raw.vendor,
  raw.vendor AS raw_vendor,
  raw.product_link,
  raw.store,
  raw.store_url,
  raw.image,
  raw.variant_title,
  raw.price,
  raw.weight_g,
  raw.in_stock,
  raw.variant_image,
  raw.high_price,
  raw.low_price,
  raw.tags,
  raw.IsDistanceDriver,
  raw.IsFairwayDriver,
  raw.IsMidrange,
  raw.IsPutter,
  raw.BodyHtml,
  raw.product_type,
  normalized.retailer,
  COALESCE(variant_model.normalized_manufacturer, normalized.normalized_manufacturer)
    AS normalized_manufacturer,
  COALESCE(variant_model.normalized_model, normalized.normalized_model) AS normalized_model,
  normalized.item_type,
  normalized.is_disc,
  normalized.item_type_confidence,
  COALESCE(variant_model.decision_confidence, normalized.manufacturer_confidence)
    AS manufacturer_confidence,
  COALESCE(variant_model.decision_confidence, normalized.model_confidence) AS model_confidence,
  CASE
    WHEN variant_model.decision_bucket = 'ACCEPT'
      THEN LEAST(normalized.item_type_confidence, variant_model.decision_confidence)
    WHEN normalized.normalized_manufacturer IS NULL THEN normalized.item_type_confidence
    ELSE LEAST(normalized.item_type_confidence, normalized.manufacturer_confidence)
  END AS normalization_confidence,
  IF(
    variant_model.decision_bucket = 'ACCEPT',
    CONCAT(normalized.item_type_source, '|', variant_model.decision_source),
    CONCAT(
      normalized.item_type_source, '|',
      normalized.manufacturer_source, '|',
      normalized.model_source
    )
  ) AS normalization_source,
  IF(variant_model.decision_bucket = 'ACCEPT', 'variant', 'product') AS model_decision_level,
  variant_model.disc_entity_id AS variant_disc_entity_id,
  normalized.normalization_version,
  COALESCE(
    variant_model.model_rules_version,
    JSON_VALUE(normalized.normalization_evidence, '$.model_decision.model_rules_version')
  ) AS model_rules_version,
  normalized.normalization_evidence,
  normalized.source_row_hash AS product_source_row_hash
FROM deduplicated_raw_variants AS raw
INNER JOIN {normalized_products} AS normalized
  ON raw.product_key = normalized.product_key
LEFT JOIN {variant_model_decisions} AS variant_model
  ON raw.id = variant_model.variant_id
 AND variant_model.decision_bucket = 'ACCEPT'
"""


def build_quality_views_sql(project_id, dataset):
    product_info = build_table_ref(project_id, dataset, "ProductInfo")
    normalized_products = build_table_ref(project_id, dataset, "NormalizedProducts")
    normalized_snapshot = build_table_ref(project_id, dataset, "NormalizedVariantSnapshot")
    rules_table = build_table_ref(project_id, dataset, "StorefrontNormalizationRules")
    shopify_view = build_table_ref(project_id, dataset, "v_ShopifyVariants")
    infinite_view = build_table_ref(project_id, dataset, "v_InfiniteVariants")
    report_view = build_table_ref(project_id, dataset, "v_NormalizationQualityReport")
    checks_view = build_table_ref(project_id, dataset, "v_NormalizationQualityChecks")
    retailer_label_collision = _retailer_label_collision_sql("products", "rules")

    return f"""
CREATE OR REPLACE VIEW {report_view} AS
SELECT
  source,
  store,
  COUNT(*) AS total_products,
  COUNTIF(item_type = 'disc') AS discs,
  COUNTIF(item_type NOT IN ('disc', 'unknown')) AS non_discs,
  COUNTIF(item_type = 'unknown') AS unknown_item_type,
  COUNTIF(normalized_manufacturer IS NOT NULL) AS normalized_manufacturer,
  COUNTIF(normalized_model IS NOT NULL) AS normalized_model,
  SAFE_DIVIDE(COUNTIF(item_type = 'unknown'), COUNT(*)) AS unknown_item_rate,
  SAFE_DIVIDE(COUNTIF(normalized_manufacturer IS NOT NULL), COUNT(*))
    AS manufacturer_coverage,
  SAFE_DIVIDE(COUNTIF(normalized_model IS NOT NULL), COUNT(*)) AS model_coverage,
  ANY_VALUE(normalization_version) AS normalization_version,
  MAX(normalized_at) AS report_as_of
FROM {normalized_products}
GROUP BY source, store;

CREATE OR REPLACE VIEW {checks_view} AS
WITH source_variant_ids AS (
  SELECT CAST(id AS STRING) AS id FROM {shopify_view}
  UNION ALL
  SELECT CAST(id AS STRING) AS id FROM {infinite_view}
),
checks AS (
  SELECT
    'duplicate_product_keys' AS check_name,
    CAST(COUNT(*) - COUNT(DISTINCT product_key) AS FLOAT64) AS observed_value,
    0.0 AS required_value,
    COUNT(*) = COUNT(DISTINCT product_key) AS passed,
    'NormalizedProducts product_key must be unique' AS details
  FROM {normalized_products}

  UNION ALL

  SELECT
    'duplicate_variant_ids',
    CAST(COUNT(*) - COUNT(DISTINCT id) AS FLOAT64),
    0.0,
    COUNT(*) = COUNT(DISTINCT id),
    'NormalizedVariantSnapshot id must be unique'
  FROM {normalized_snapshot}

  UNION ALL

  SELECT
    'missing_source_variant_keys',
    CAST(COUNTIF(NULLIF(TRIM(source_variant_key), '') IS NULL) AS FLOAT64),
    0.0,
    COUNTIF(NULLIF(TRIM(source_variant_key), '') IS NULL) = 0,
    'NormalizedVariantSnapshot source_variant_key must be populated'
  FROM {normalized_snapshot}

  UNION ALL

  SELECT
    'duplicate_source_variant_keys',
    CAST(COUNT(*) - COUNT(DISTINCT source_variant_key) AS FLOAT64),
    0.0,
    COUNT(*) = COUNT(DISTINCT source_variant_key),
    'NormalizedVariantSnapshot source_variant_key must be unique'
  FROM {normalized_snapshot}

  UNION ALL

  SELECT
    'retailer_labels_as_manufacturers',
    CAST(COUNTIF(
      {retailer_label_collision}
    ) AS FLOAT64),
    0.0,
    COUNTIF(
      {retailer_label_collision}
    ) = 0,
    'Non-brand storefront labels must not become normalized manufacturers'
  FROM {normalized_products} AS products
  INNER JOIN {rules_table} AS rules
    ON products.store = rules.store
  WHERE products.source = 'shopify'

  UNION ALL

  SELECT
    'infinite_manufacturer_coverage',
    SAFE_DIVIDE(COUNTIF(normalized_manufacturer IS NOT NULL), COUNT(*)),
    0.99,
    SAFE_DIVIDE(COUNTIF(normalized_manufacturer IS NOT NULL), COUNT(*)) >= 0.99,
    'Infinite Discs manufacturer coverage must remain at least 99%'
  FROM {normalized_products}
  WHERE source = 'infinite'

  UNION ALL

  SELECT
    'infinite_model_coverage',
    SAFE_DIVIDE(COUNTIF(normalized_model IS NOT NULL), COUNT(*)),
    0.99,
    SAFE_DIVIDE(COUNTIF(normalized_model IS NOT NULL), COUNT(*)) >= 0.99,
    'Infinite Discs model coverage must remain at least 99%'
  FROM {normalized_products}
  WHERE source = 'infinite'

  UNION ALL

  SELECT
    'shopify_product_coverage',
    SAFE_DIVIDE(
      (SELECT COUNT(*) FROM {normalized_products} WHERE source = 'shopify'),
      COUNT(DISTINCT CONCAT(CAST(MainProductId AS STRING), '|', CAST(Store AS STRING)))
    ),
    0.9999,
    (SELECT COUNT(*) FROM {normalized_products} WHERE source = 'shopify')
      >= COUNT(DISTINCT CONCAT(CAST(MainProductId AS STRING), '|', CAST(Store AS STRING))),
    'Every raw Shopify product must have a normalized product row'
  FROM {product_info}

  UNION ALL

  SELECT
    'normalized_variant_coverage',
    SAFE_DIVIDE(
      (SELECT COUNT(*) FROM {normalized_snapshot}),
      (SELECT COUNT(DISTINCT id) FROM source_variant_ids)
    ),
    0.9999,
    (SELECT COUNT(*) FROM {normalized_snapshot})
      = (SELECT COUNT(DISTINCT id) FROM source_variant_ids),
    'Normalized snapshot must retain one deterministic row per source-view variant id'
)
SELECT * FROM checks;
"""


def build_quality_audit_sql(project_id, dataset):
    normalized_products = build_table_ref(project_id, dataset, "NormalizedProducts")
    rules_table = build_table_ref(project_id, dataset, "StorefrontNormalizationRules")
    audit_table = build_table_ref(project_id, dataset, "NormalizationQualityAudit")
    retailer_label_collision = _retailer_label_collision_sql("products", "rules")

    return f"""
CREATE TABLE IF NOT EXISTS {audit_table} (
  finding_id STRING NOT NULL,
  check_name STRING NOT NULL,
  status STRING NOT NULL,
  source STRING,
  product_key STRING,
  store STRING,
  retailer STRING,
  title STRING,
  raw_vendor STRING,
  normalized_manufacturer STRING,
  normalized_model STRING,
  manufacturer_source STRING,
  model_source STRING,
  normalization_version STRING,
  first_seen_at TIMESTAMP NOT NULL,
  last_seen_at TIMESTAMP NOT NULL,
  observation_count INT64 NOT NULL,
  resolved_at TIMESTAMP
)
PARTITION BY DATE(first_seen_at)
CLUSTER BY check_name, status, store;

CREATE TEMP TABLE CurrentNormalizationQualityFindings AS
SELECT
  TO_HEX(SHA256(CONCAT(
    'retailer_labels_as_manufacturers|', products.product_key
  ))) AS finding_id,
  'retailer_labels_as_manufacturers' AS check_name,
  'OPEN' AS status,
  products.source,
  products.product_key,
  products.store,
  products.retailer,
  products.title,
  products.raw_vendor,
  products.normalized_manufacturer,
  products.normalized_model,
  products.manufacturer_source,
  products.model_source,
  products.normalization_version
FROM {normalized_products} AS products
INNER JOIN {rules_table} AS rules
  ON products.store = rules.store
WHERE products.source = 'shopify'
  AND {retailer_label_collision};

MERGE {audit_table} AS target
USING CurrentNormalizationQualityFindings AS source
  ON target.finding_id = source.finding_id
WHEN MATCHED THEN UPDATE SET
  status = 'OPEN',
  source = source.source,
  product_key = source.product_key,
  store = source.store,
  retailer = source.retailer,
  title = source.title,
  raw_vendor = source.raw_vendor,
  normalized_manufacturer = source.normalized_manufacturer,
  normalized_model = source.normalized_model,
  manufacturer_source = source.manufacturer_source,
  model_source = source.model_source,
  normalization_version = source.normalization_version,
  last_seen_at = CURRENT_TIMESTAMP(),
  observation_count = target.observation_count + 1,
  resolved_at = NULL
WHEN NOT MATCHED THEN INSERT (
  finding_id,
  check_name,
  status,
  source,
  product_key,
  store,
  retailer,
  title,
  raw_vendor,
  normalized_manufacturer,
  normalized_model,
  manufacturer_source,
  model_source,
  normalization_version,
  first_seen_at,
  last_seen_at,
  observation_count,
  resolved_at
)
VALUES (
  source.finding_id,
  source.check_name,
  source.status,
  source.source,
  source.product_key,
  source.store,
  source.retailer,
  source.title,
  source.raw_vendor,
  source.normalized_manufacturer,
  source.normalized_model,
  source.manufacturer_source,
  source.model_source,
  source.normalization_version,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  1,
  NULL
);

UPDATE {audit_table} AS target
SET
  status = 'RESOLVED',
  resolved_at = CURRENT_TIMESTAMP()
WHERE target.check_name = 'retailer_labels_as_manufacturers'
  AND target.status = 'OPEN'
  AND NOT EXISTS (
    SELECT 1
    FROM CurrentNormalizationQualityFindings AS current_finding
    WHERE current_finding.finding_id = target.finding_id
  );
"""


def validate_quality_checks(rows):
    warnings = [
        row
        for row in rows
        if not row["passed"] and row["check_name"] in NON_BLOCKING_QUALITY_CHECKS
    ]
    for row in warnings:
        print(
            "Normalization quality warning "
            f"check={row['check_name']} observed={row['observed_value']} "
            f"required={row['required_value']} "
            "(recorded in NormalizationQualityAudit; pipeline will continue)"
        )

    failures = [
        row
        for row in rows
        if not row["passed"] and row["check_name"] not in NON_BLOCKING_QUALITY_CHECKS
    ]
    if not failures:
        return warnings
    messages = [
        f"{row['check_name']} observed={row['observed_value']} required={row['required_value']}"
        for row in failures
    ]
    raise RuntimeError("Normalization quality checks failed: " + "; ".join(messages))


def run_normalization(client, project_id, dataset, rules_version=None):
    resolved_version = (rules_version or get_normalization_version()).strip()
    model_rules_version = get_model_rules_version()
    print(f"Seeding {project_id}.{dataset}.StorefrontNormalizationRules")
    client.query(build_storefront_rules_sql(project_id, dataset, resolved_version)).result()

    print(f"Refreshing {project_id}.{dataset}.NormalizedProducts")
    client.query(
        build_normalized_products_sql(
            project_id,
            dataset,
            resolved_version,
            model_rules_version=model_rules_version,
        )
    ).result()

    run_model_normalization(
        client,
        project_id,
        dataset,
        rules_version=model_rules_version,
    )

    print(f"Refreshing {project_id}.{dataset}.NormalizedVariantSnapshot")
    client.query(build_normalized_variant_snapshot_sql(project_id, dataset)).result()

    print(f"Refreshing normalization quality views in {project_id}.{dataset}")
    client.query(build_quality_views_sql(project_id, dataset)).result()

    print(f"Refreshing {project_id}.{dataset}.NormalizationQualityAudit")
    client.query(build_quality_audit_sql(project_id, dataset)).result()

    checks_table = build_table_ref(project_id, dataset, "v_NormalizationQualityChecks")
    check_rows = list(client.query(f"SELECT * FROM {checks_table} ORDER BY check_name").result())
    quality_warnings = validate_quality_checks(check_rows)
    model_quality = refresh_model_quality_views(client, project_id, dataset)

    report_table = build_table_ref(project_id, dataset, "v_NormalizationQualityReport")
    summary_query = f"""
      SELECT
        source,
        SUM(total_products) AS total_products,
        SUM(discs) AS discs,
        SUM(non_discs) AS non_discs,
        SUM(unknown_item_type) AS unknown_item_type,
        SUM(normalized_manufacturer) AS normalized_manufacturer,
        SUM(normalized_model) AS normalized_model
      FROM {report_table}
      GROUP BY source
      ORDER BY source
    """
    summaries = [dict(row.items()) for row in client.query(summary_query).result()]
    for summary in summaries:
        print(
            "Normalization summary "
            f"source={summary['source']} products={summary['total_products']} "
            f"discs={summary['discs']} non_discs={summary['non_discs']} "
            f"unknown={summary['unknown_item_type']} "
            f"manufacturers={summary['normalized_manufacturer']} "
            f"models={summary['normalized_model']}"
        )

    return {
        "normalization_version": resolved_version,
        "model_rules_version": model_rules_version,
        "checks": [dict(row.items()) for row in check_rows],
        "warnings": [dict(row.items()) for row in quality_warnings],
        "sources": summaries,
        "model_quality": model_quality,
    }
