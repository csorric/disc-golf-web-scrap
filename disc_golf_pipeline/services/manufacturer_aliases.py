"""Reviewed exact vendor names for existing canonical manufacturers."""

import re
import unicodedata


# Exact aliases only. Retailers, distributors, and umbrella vendor labels such
# as MVP Disc Sports,LLC are deliberately excluded: they can span multiple brands.
CURATED_VENDOR_ALIASES = {
    "Above Ground Level Discs": "Above Ground Level",
    "Discmania Active": "Discmania",
    "Discmania Evolution": "Discmania",
    "Discraft Disc Golf": "Discraft",
    "Innova Disc Golf": "Innova Champion Discs",
    "Innova Discs": "Innova Champion Discs",
    "Latitude 64 Golf Discs": "Latitude 64",
    "Lightning Golf Discs": "Lightning Discs",
    "Lone Star Discs": "Lone Star Disc",
    "MVP Discs": "MVP Disc Sports",
    "Prodigy Ace Line": "Prodigy Disc",
    "Prodigy Discs": "Prodigy Disc",
    "RPM Discs": "RPM Discs/Disc Golf Aotearoa",
    "Trash Panda": "Trash Panda Disc Golf",
    "Westside Discs": "Westside Golf Discs",
}


def build_curated_manufacturer_aliases_sql(project_id, dataset):
    table = f"`{project_id}.{dataset}.DiscManufacturerAliases`"

    def quote(value):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    rows = []
    for display, canonical in sorted(CURATED_VENDOR_ALIASES.items()):
        key = re.sub(r"[^a-z0-9]+", "", unicodedata.normalize("NFKD", display).casefold())
        rows.append(f"STRUCT({quote(key)} AS alias_match_key, "
                    f"{quote(display)} AS alias_display, "
                    f"{quote(canonical)} AS canonical_manufacturer)")
    values = ",\n  ".join(rows)
    # Existing aliases (including manually disabled ones) are preserved. A
    # conflicting mapping requires review instead of silently changing identity.
    return f"""
CREATE TEMP TABLE CuratedVendorAliases AS
SELECT * FROM UNNEST([
  {values}
]);
ASSERT NOT EXISTS (
  SELECT 1 FROM CuratedVendorAliases proposed
  LEFT JOIN {table} existing
    ON proposed.canonical_manufacturer = existing.canonical_manufacturer
   AND existing.is_active
  WHERE existing.alias_match_key IS NULL
) AS 'Curated vendor alias refers to an unknown canonical manufacturer';
ASSERT NOT EXISTS (
  SELECT 1 FROM CuratedVendorAliases proposed
  JOIN {table} existing USING (alias_match_key)
  WHERE proposed.canonical_manufacturer != existing.canonical_manufacturer
) AS 'Curated vendor alias conflicts with an existing manufacturer mapping';
MERGE {table} target
USING CuratedVendorAliases source
ON target.alias_match_key = source.alias_match_key
WHEN NOT MATCHED THEN INSERT (
  alias_match_key, alias_display, canonical_manufacturer, alias_source,
  is_active, created_at, updated_at
) VALUES (
  source.alias_match_key, source.alias_display, source.canonical_manufacturer,
  'curated', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);
"""
