"""Conservative equivalence for spacing at letter/number model boundaries."""

import re
import unicodedata


def model_match_key(value):
    text = unicodedata.normalize("NFKD", value or "").casefold()
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    text = re.sub(r"([a-z]) +([0-9])", r"\1\2", text)
    return re.sub(r"([0-9]) +([a-z])", r"\1\2", text)


def model_match_key_sql(expression):
    text = f"TRIM(REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(COALESCE({expression}, ''), NFKD), r'[^a-z0-9]+', ' '))"
    return (f"REGEXP_REPLACE(REGEXP_REPLACE({text}, r'([a-z]) +([0-9])', r'\\1\\2'), "
            "r'([0-9]) +([a-z])', r'\\1\\2')")


def model_spacing_aliases_sql(expression):
    compact = model_match_key_sql(expression)
    spaced = (f"REGEXP_REPLACE(REGEXP_REPLACE({compact}, r'([a-z])([0-9])', r'\\1 \\2'), "
              "r'([0-9])([a-z])', r'\\1 \\2')")
    return f"[{compact}, {spaced}]"
