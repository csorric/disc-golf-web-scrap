"""Conservative category evidence shared by source flags and classifications."""

import json
import re


CATEGORY_ALIASES = {
    "putter": ("putter", "putters", "puttapproach", "puttandapproach", "puttapproachdisc",
               "puttandapproachdiscs", "puttapproachdiscs", "puttandapproachdisc", "approach",
               "approachdisc", "approachdiscs", "puttingputter", "throwingputter", "putterdiscs"),
    "midrange": ("midrange", "midranges", "midrangedisc", "midrangediscs", "midrangedriver", "midrangedrivers"),
    "fairway_driver": ("fairway", "fairwaydriver", "fairwaydrivers", "fairwaydisc", "fairwaydiscs",
                       "controldriver", "controldrivers", "fairwaycontroldriver", "fairwaycontroldrivers"),
    "distance_driver": ("distancedriver", "distancedrivers", "distancedisc", "distancediscs",
                        "longrangedriver", "longrangedrivers", "highspeeddriver", "highspeeddrivers"),
}
LEGACY_CATEGORY_FLAGS = {
    "IsPutter": "putter",
    "IsMidrange": "midrange",
    "IsFairwayDriver": "fairway_driver",
    "IsDistanceDriver": "distance_driver",
}
CATEGORY_PATTERN = (r"\b(?:distance[ -]+drivers?|long[ -]+range[ -]+drivers?|fairway[ -]+drivers?|"
                    r"control[ -]+drivers?|mid[ -]?ranges?(?:[ -]+drivers?)?|putters?|"
                    r"putt[ -]+(?:(?:and|&)[ -]+)?approach(?:[ -]+discs?)?)\b")
TAG_PREFIX = r"^(?:disc[ _-]*type|type|category)[ _:-]+"
COMPARISON = (r"\b(?:like|unlike|than|compared\s+(?:to|with)|similar\s+to|reminiscent\s+of|"
              r"between|that\s+(?:thinks|flies|feels)|thinks?\s+it|as\s+if)\b.*$")
BOILERPLATE = r"\b(?:browse|shop\s+(?:all|our)|see\s+all|our\s+(?:selection|collection)|other\s+products)\b"
NEGATION = r"\b(?:not|never|isn.t|aren.t|without\s+being)\b"
DEFINITION = (r"\b(?:is|are|introducing|meet|this|our)\b.{0,120}" + CATEGORY_PATTERN
              + r"|\b(?:disc\s+type|type|category|mold)\s*[:=-]")


def canonical_category(value):
    key = re.sub(r"[^a-z]", "", str(value or "").lower())
    return next((category for category, aliases in CATEGORY_ALIASES.items() if key in aliases), None)


def canonical_category_sql(expression):
    cases = "\n".join(f"WHEN '{alias}' THEN '{category}'"
                      for category, aliases in CATEGORY_ALIASES.items() for alias in aliases)
    return f"CASE REGEXP_REPLACE(LOWER(COALESCE(CAST({expression} AS STRING), '')), r'[^a-z]', '')\n{cases}\nEND"


def extract_category(product_type="", tags="", title="", body=""):
    """Local evaluator for diagnostics; the same rules run in BigQuery below."""
    try:
        tag_values = json.loads(tags) if isinstance(tags, str) else tags
    except (ValueError, TypeError):
        tag_values = None
    if not isinstance(tag_values, list):
        tag_values = str(tags or "").split(",")
    text = re.sub(r"(?is)<(?:script|style|nav)\b[^>]*>.*?</(?:script|style|nav)>", " ", str(body or ""))
    text = re.sub(r"(?i)</(?:p|div|li|h[1-6]|tr)>|<br\s*/?>", ". ", text)
    text = re.sub(r"<[^>]*>", " ", text)
    text = re.sub(r"&[^;\s]+;", " ", text).lower().replace("\u2013", "-").replace("\u2014", "-")
    candidates = []
    direct = canonical_category(product_type)
    if direct:
        candidates.append((1, direct, "product_type", str(product_type)))
    for tag in tag_values:
        # An unqualified "approach" tag describes a shot, including midrange shots.
        # Explicit taxonomy such as Type_Approach and product types remains valid.
        if str(tag).strip().lower() == "approach":
            continue
        value = re.sub(TAG_PREFIX, "", str(tag).strip().lower())
        category = canonical_category(value)
        if category:
            candidates.append((2, category, "product_tags", str(tag)))
    title_text = re.sub(r"\bputter\s+(?:line|blend)\b", "", str(title or "").lower())
    for value in re.findall(CATEGORY_PATTERN, title_text):
        candidates.append((3, canonical_category(value), "product_title", value))
    for sentence in re.findall(r"[^.!?;\n]+", text):
        if re.search(BOILERPLATE, sentence) or re.search(NEGATION, sentence):
            continue
        claim = re.sub(COMPARISON, "", sentence)
        if not re.search(DEFINITION, claim):
            continue
        for value in re.findall(CATEGORY_PATTERN, claim):
            candidates.append((4, canonical_category(value), "description_definition", claim.strip()))
    candidates = [c for c in candidates if c[1] is not None]
    if not candidates:
        return None, None, None
    best = [c for c in candidates if c[0] == min(c[0] for c in candidates)]
    if len({c[1] for c in best}) != 1:
        return None, "ambiguous_retailer_category", json.dumps(sorted({c[1] for c in best}))
    chosen = min(best, key=lambda c: c[3])
    return chosen[1:]


def build_category_query(source):
    """SELECT source.* plus extracted_category/source/evidence. No legacy flags."""
    clean_html = "REGEXP_REPLACE(COALESCE(BodyHtml, ''), r'(?is)<(?:script|style|nav)\\b[^>]*>.*?</(?:script|style|nav)>', ' ')"
    clean_html = f"REGEXP_REPLACE({clean_html}, r'(?i)</(?:p|div|li|h[1-6]|tr)>|<br\\s*/?>', '. ')"
    clean_html = f"REGEXP_REPLACE({clean_html}, r'<[^>]*>', ' ')"
    clean_html = f"REGEXP_REPLACE({clean_html}, r'&[^;\\s]+;', ' ')"
    return f"""
WITH category_text AS (
  SELECT src.*, LOWER(REPLACE(REPLACE({clean_html}, '–', '-'), '—', '-')) AS category_body,
    REGEXP_REPLACE(LOWER(COALESCE(title, '')), r'\\bputter\\s+(?:line|blend)\\b', '') AS category_title
  FROM {source} src
), category_candidate_rows AS (
  SELECT *, ARRAY(
    SELECT candidate FROM UNNEST(ARRAY_CONCAT(
      [STRUCT(1 AS priority, {canonical_category_sql('product_type')} AS category,
        'product_type' AS source, CAST(product_type AS STRING) AS evidence)],
      ARRAY(SELECT AS STRUCT 2, {canonical_category_sql("REGEXP_REPLACE(LOWER(TRIM(tag)), r'" + TAG_PREFIX + "', '')")},
          'product_tags', tag
        FROM UNNEST(COALESCE(JSON_VALUE_ARRAY(tags), SPLIT(COALESCE(tags, ''), ','))) tag
        WHERE LOWER(TRIM(tag)) != 'approach'),
      ARRAY(SELECT AS STRUCT 3, {canonical_category_sql('term')}, 'product_title', term
        FROM UNNEST(REGEXP_EXTRACT_ALL(category_title, r'{CATEGORY_PATTERN}')) term),
      ARRAY(SELECT AS STRUCT 4, {canonical_category_sql('term')}, 'description_definition', TRIM(claim)
        FROM (
          SELECT REGEXP_REPLACE(sentence, r'{COMPARISON}', '') AS claim
          FROM UNNEST(REGEXP_EXTRACT_ALL(category_body, r'[^.!?;\\n]+')) sentence
          WHERE NOT REGEXP_CONTAINS(sentence, r'{BOILERPLATE}')
            AND NOT REGEXP_CONTAINS(sentence, r'{NEGATION}')
        ) CROSS JOIN UNNEST(REGEXP_EXTRACT_ALL(claim, r'{CATEGORY_PATTERN}')) term
        WHERE REGEXP_CONTAINS(claim, r'{DEFINITION}'))
    )) candidate WHERE candidate.category IS NOT NULL
  ) AS category_candidates
  FROM category_text
), category_best_rows AS (
  SELECT *, ARRAY(SELECT candidate FROM UNNEST(category_candidates) candidate
    WHERE candidate.priority = (SELECT MIN(priority) FROM UNNEST(category_candidates))) AS category_best
  FROM category_candidate_rows
), category_decision AS (
  SELECT *, (SELECT COUNT(DISTINCT category) FROM UNNEST(category_best)) AS category_count,
    (SELECT candidate FROM UNNEST(category_best) candidate ORDER BY evidence LIMIT 1) AS category_choice
  FROM category_best_rows
)
SELECT * EXCEPT(category_body, category_title, category_candidates, category_best, category_choice, category_count),
  IF(category_count = 1, category_choice.category, NULL) AS extracted_category,
  IF(category_count > 1, 'ambiguous_retailer_category', category_choice.source) AS extracted_category_source,
  IF(category_count > 1,
    TO_JSON_STRING(ARRAY(SELECT DISTINCT category FROM UNNEST(category_best) ORDER BY category)),
    category_choice.evidence) AS extracted_category_evidence
FROM category_decision
"""
