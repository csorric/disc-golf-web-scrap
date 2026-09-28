"""Compare primary Try Discs flights with retained store-extracted values."""

import html
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from disc_golf_pipeline.common.runtime import PROJECT_ROOT
from disc_golf_pipeline.services.try_discs import (
    ATTRIBUTION,
    build_catalog_index,
    fetch_catalog,
    flight_numbers,
    match_model,
)


DEFAULT_REPORT_PATH = PROJECT_ROOT / "reports" / "try_discs_api_audit.html"


def build_comparison_sql(project_id, dataset):
    snapshot = f"`{project_id}.{dataset}.NormalizedVariantSnapshot`"
    attributes = f"`{project_id}.{dataset}.NormalizedDiscAttributes`"
    return f"""
SELECT
  s.normalized_manufacturer AS manufacturer,
  s.normalized_model AS model,
  a.local_speed AS speed, a.local_glide AS glide,
  a.local_turn AS turn, a.local_fade AS fade,
  COUNT(*) AS variants,
  COUNTIF(a.local_speed IS NULL) AS missing_flight
FROM {snapshot} AS s
JOIN {attributes} AS a ON s.id = a.id
WHERE s.item_type = 'disc'
GROUP BY 1, 2, 3, 4, 5, 6
"""


def compare_catalog(catalog, rows):
    index = build_catalog_index(catalog)
    pairs = set()
    matched_pairs = {"exact": set(), "brand_alias": set()}
    unmatched = Counter()
    alias_counts = Counter()
    disagreements = Counter()
    totals = Counter()
    totals["catalog_entries"] = len(catalog)
    totals["catalog_complete_flight"] = sum(flight_numbers(disc) is not None for disc in catalog)

    for row in rows:
        manufacturer = row.get("manufacturer")
        model = row.get("model")
        count = int(row.get("variants") or 0)
        missing = int(row.get("missing_flight") or 0)
        totals["disc_variants"] += count
        totals["missing_flight"] += missing
        if manufacturer and model:
            pairs.add((manufacturer, model))
        disc, match_type = match_model(manufacturer, model, index)
        if disc is None:
            if manufacturer and model:
                unmatched[(manufacturer, model)] += missing
            else:
                totals["missing_without_model"] += missing
            continue

        matched_pairs[match_type].add((manufacturer, model))
        totals[f"{match_type}_variants"] += count
        totals[f"{match_type}_fillable"] += missing
        if match_type == "brand_alias":
            alias_counts[(manufacturer, disc["brand"])] += missing
        if row.get("speed") is None:
            continue
        observed = tuple(float(row[field]) for field in ("speed", "glide", "turn", "fade"))
        expected = flight_numbers(disc)
        if observed == expected:
            totals["existing_agreements"] += count
        else:
            totals["existing_disagreements"] += count
            disagreements[(manufacturer, model, observed, expected, disc.get("url"))] += count

    totals["normalized_pairs"] = len(pairs)
    totals["exact_pairs"] = len(matched_pairs["exact"])
    totals["alias_pairs"] = len(matched_pairs["brand_alias"])
    totals["fillable"] = totals["exact_fillable"] + totals["brand_alias_fillable"]
    totals["projected_flight_resolved"] = (
        totals["disc_variants"] - totals["missing_flight"] + totals["fillable"]
    )
    return {
        "totals": dict(totals),
        "aliases": alias_counts.most_common(30),
        "unmatched": unmatched.most_common(40),
        "disagreements": disagreements.most_common(40),
    }


def _escape(value):
    return html.escape("" if value is None else str(value), quote=True)


def _count(value):
    return f"{int(value or 0):,}"


def _flight(value):
    return " / ".join(f"{number:g}" for number in value)


def _source_link(url):
    parsed = urlparse(str(url or ""))
    if parsed.scheme == "https" and parsed.netloc == "trydiscs.com":
        return f'<a href="{_escape(url)}">Try Discs</a>'
    return ""


def render_try_discs_audit_html(report, dataset_version, generated_at=None):
    generated_at = generated_at or datetime.now(timezone.utc)
    totals = report["totals"]
    cards = (
        ("Disc variants", "disc_variants"),
        ("Local flight missing", "missing_flight"),
        ("Exact matches filling gaps", "exact_fillable"),
        ("Brand aliases filling gaps", "brand_alias_fillable"),
        ("Resolved with Try Discs", "projected_flight_resolved"),
        ("Local agreements", "existing_agreements"),
        ("Local disagreements", "existing_disagreements"),
    )
    card_html = "".join(
        f'<div class="card"><span>{label}</span><strong>{_count(totals.get(key))}</strong></div>'
        for label, key in cards
    )
    alias_rows = "".join(
        f"<tr><td>{_escape(source)}</td><td>{_escape(target)}</td><td>{_count(count)}</td></tr>"
        for (source, target), count in report["aliases"]
    )
    unmatched_rows = "".join(
        f"<tr><td>{_escape(manufacturer)}</td><td>{_escape(model)}</td><td>{_count(count)}</td></tr>"
        for (manufacturer, model), count in report["unmatched"]
    )
    disagreement_rows = "".join(
        "<tr>"
        f"<td>{_escape(manufacturer)} / {_escape(model)}</td>"
        f"<td>{_escape(_flight(observed))}</td>"
        f"<td>{_escape(_flight(expected))}</td>"
        f"<td>{_count(count)}</td>"
        f"<td>{_source_link(url)}</td>"
        "</tr>"
        for (manufacturer, model, observed, expected, url), count in report["disagreements"]
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Try Discs API Flight Audit</title>
<style>
:root {{ color-scheme:light; --ink:#17202a; --muted:#64748b; --line:#dbe3ea; --panel:#f8fafc; }}
* {{ box-sizing:border-box; }} body {{ margin:0; color:var(--ink); background:#eef2f6; font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif; }}
main {{ width:min(1500px,96vw); margin:24px auto 60px; }} h1 {{ margin-bottom:4px; }} .muted {{ color:var(--muted); }}
.cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(165px,1fr)); gap:12px; margin:20px 0; }}
.card,.panel {{ background:white; border:1px solid var(--line); border-radius:10px; padding:15px; box-shadow:0 2px 8px #0f172a0a; }}
.card strong {{ display:block; font-size:26px; }} .panel {{ margin:14px 0; overflow:auto; }} table {{ width:100%; border-collapse:collapse; }}
th,td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }} th {{ position:sticky; top:0; background:var(--panel); }}
a {{ color:#1d4ed8; }}
</style></head><body><main>
<h1>Try Discs API Flight Audit</h1>
<p class="muted">Generated {_escape(generated_at.isoformat())}. Try Discs dataset {_escape(dataset_version)}. {_escape(ATTRIBUTION)}.</p>
<p>Try Discs is the primary flight source when manufacturer and model identify one catalog disc with all four valid flight numbers. The stored local flight values remain available for disagreement review and for models without a catalog match. Brand aliases are limited to the explicit mappings in the pipeline code.</p>
<div class="cards">{card_html}</div>
<p>{_count(totals.get('catalog_complete_flight'))} of {_count(totals.get('catalog_entries'))} catalog discs have complete flight numbers. {_count(totals.get('normalized_pairs'))} distinct normalized manufacturer/model pairs were checked; {_count(totals.get('exact_pairs'))} matched exactly and {_count(totals.get('alias_pairs'))} matched through the explicit brand aliases. {_count(totals.get('missing_without_model'))} missing-flight variants have no normalized manufacturer/model pair to match.</p>
<h2>Brand aliases with the most potential fills</h2><div class="panel"><table><thead><tr><th>Pipeline manufacturer</th><th>Try Discs brand</th><th>Missing-flight variants</th></tr></thead><tbody>{alias_rows}</tbody></table></div>
<h2>Local flight disagreements</h2><p>Top 40 by variant count. Different editions and retailer descriptions may explain some differences.</p>
<div class="panel"><table><thead><tr><th>Disc</th><th>Store-extracted flight</th><th>Try Discs flight</th><th>Variants</th><th>Source</th></tr></thead><tbody>{disagreement_rows}</tbody></table></div>
<h2>Unmatched normalized models</h2><p>Top 40 by missing-flight variants. A name-only match is not used.</p>
<div class="panel"><table><thead><tr><th>Manufacturer</th><th>Model</th><th>Missing-flight variants</th></tr></thead><tbody>{unmatched_rows}</tbody></table></div>
<p class="muted">Attribution for any public use of Try Discs values: <a href="https://trydiscs.com">Disc data by Try Discs</a>. This report contains aggregate counts and limited comparisons, not the bulk catalog.</p>
</main></body></html>"""


def generate_try_discs_audit_report(client, project_id, dataset, output_path=DEFAULT_REPORT_PATH):
    catalog, meta = fetch_catalog()
    rows = [dict(row.items()) for row in client.query(build_comparison_sql(project_id, dataset)).result()]
    report = compare_catalog(catalog, rows)
    rendered = render_try_discs_audit_html(report, meta.get("dataset_version", "unknown"))
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary_path.write_text(rendered, encoding="utf-8")
    temporary_path.replace(output_path)
    return {"output_path": str(output_path), "dataset_version": meta.get("dataset_version"), **report["totals"]}
