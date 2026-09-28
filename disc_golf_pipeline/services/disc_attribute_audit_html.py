"""HTML audit of normalized flight numbers and disc weights in BigQuery."""

import html
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from disc_golf_pipeline.common.runtime import PROJECT_ROOT


DEFAULT_REPORT_PATH = PROJECT_ROOT / "reports" / "disc_attribute_audit_report.html"
SAMPLE_LIMIT_PER_CATEGORY = 75


def _table(project_id, dataset, name):
    return f"`{project_id}.{dataset}.{name}`"


def _joined_rows_sql(project_id, dataset):
    snapshot = _table(project_id, dataset, "NormalizedVariantSnapshot")
    attributes = _table(project_id, dataset, "NormalizedDiscAttributes")
    return f"""
SELECT
  s.id, s.source, s.store, s.title, s.variant_title, s.product_link,
  SAFE_CAST(s.weight_g AS INT64) AS raw_weight_g,
  SUBSTR(REGEXP_REPLACE(COALESCE(s.BodyHtml, ''), r'<[^>]*>', ' '), 1, 700) AS html_excerpt,
  a.speed, a.glide, a.turn, a.fade,
  a.local_speed, a.local_glide, a.local_turn, a.local_fade,
  a.flight_confidence, a.flight_source, a.flight_evidence, a.flight_attribution,
  a.normalized_weight_g, a.normalized_weight_min_g, a.normalized_weight_max_g,
  a.weight_confidence, a.weight_source, a.weight_evidence,
  a.flight_conflict,
  (a.normalized_weight_g IS NOT NULL
    AND SAFE_CAST(s.weight_g AS INT64) BETWEEN 100 AND 190
    AND ABS(a.normalized_weight_g - SAFE_CAST(s.weight_g AS INT64)) >= 2)
    AS weight_conflict
FROM {snapshot} AS s
INNER JOIN {attributes} AS a ON s.id = a.id
"""


def build_disc_attribute_summary_sql(project_id, dataset):
    return f"""
WITH disc_rows AS ({_joined_rows_sql(project_id, dataset)})
SELECT
  source, store,
  COUNT(*) AS discs,
  COUNTIF(speed IS NOT NULL) AS flight_resolved,
  COUNTIF(normalized_weight_g IS NOT NULL) AS weight_resolved,
  COUNTIF(normalized_weight_min_g IS NOT NULL) AS weight_ranges,
  COUNTIF(raw_weight_g < 100 OR raw_weight_g > 190) AS invalid_raw_weight,
  COUNTIF(weight_source = 'rejected_source_weight_g') AS rejected_weight,
  COUNTIF(normalized_weight_g IS NOT NULL
    AND raw_weight_g IS DISTINCT FROM normalized_weight_g) AS corrected_weight,
  COUNTIF(flight_conflict) AS store_api_differences,
  COUNTIF(flight_source = 'try_discs_exact') AS try_discs_exact_flights,
  COUNTIF(flight_source = 'try_discs_brand_alias') AS try_discs_alias_flights,
  COUNTIF(weight_source = 'llm_variant_evidence') AS llm_weights,
  COUNTIF(weight_source = 'llm_no_specific_weight') AS llm_no_weight,
  COUNTIF(weight_conflict) AS weight_conflicts,
  COUNTIF(flight_source = 'html_labels') AS html_label_flights,
  COUNTIF(flight_source = 'html_sequence') AS html_sequence_flights,
  COUNTIF(flight_source = 'html_bare_sequence') AS html_bare_flights,
  COUNTIF(flight_source IN ('tags_labels', 'tags_sequence')) AS tag_flights,
  COUNTIF(flight_source = 'title_sequence') AS title_flights,
  COUNTIF(weight_source = 'variant_title') AS variant_title_weights,
  COUNTIF(weight_source = 'variant_title_unlabelled') AS unlabelled_title_weights,
  COUNTIF(weight_source = 'body_html') AS html_weights,
  COUNTIF(weight_source = 'source_weight_g') AS source_weights
FROM disc_rows
GROUP BY source, store
ORDER BY discs DESC, source, store
"""


def build_disc_attribute_samples_sql(project_id, dataset, limit_per_category=SAMPLE_LIMIT_PER_CATEGORY):
    if limit_per_category < 1:
        raise ValueError("limit_per_category must be positive")
    return f"""
WITH disc_rows AS ({_joined_rows_sql(project_id, dataset)}),
categorized AS (
  SELECT disc_rows.*, category.name AS audit_category
  FROM disc_rows
  CROSS JOIN UNNEST([
    STRUCT('rejected weight' AS name, weight_source = 'rejected_source_weight_g' AS selected),
    STRUCT('title weight range' AS name, weight_source = 'variant_title_range' AS selected),
    STRUCT('corrected weight' AS name,
      normalized_weight_g IS NOT NULL AND raw_weight_g IS DISTINCT FROM normalized_weight_g AS selected),
    STRUCT('store/API difference' AS name, flight_conflict AS selected),
    STRUCT('source/selected weight difference' AS name, weight_conflict AS selected),
    STRUCT('missing flight' AS name, speed IS NULL AS selected),
    STRUCT('missing weight' AS name,
      normalized_weight_g IS NULL AND normalized_weight_min_g IS NULL AS selected),
    STRUCT('resolved sample' AS name,
      speed IS NOT NULL AND normalized_weight_g IS NOT NULL
      AND NOT flight_conflict AND NOT weight_conflict AS selected)
  ]) AS category
  WHERE category.selected
)
SELECT *
FROM categorized
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY audit_category
  ORDER BY FARM_FINGERPRINT(CAST(id AS STRING))
) <= {int(limit_per_category)}
ORDER BY audit_category, store, title, id
"""


def _as_dicts(rows):
    return [dict(row.items()) for row in rows]


def fetch_disc_attribute_audit(client, project_id, dataset):
    summary = _as_dicts(client.query(build_disc_attribute_summary_sql(project_id, dataset)).result())
    samples = _as_dicts(client.query(build_disc_attribute_samples_sql(project_id, dataset)).result())
    return summary, samples


def _escape(value):
    return html.escape("" if value is None else str(value), quote=True)


def _number(value):
    return f"{int(value or 0):,}"


def _score(value):
    return "—" if value is None else f"{float(value):.2f}"


def _selected_weight(row):
    exact = row.get("normalized_weight_g")
    if exact is not None:
        return f"{int(exact)} g"
    minimum = row.get("normalized_weight_min_g")
    maximum = row.get("normalized_weight_max_g")
    if minimum is not None and maximum is not None:
        return f"{float(minimum):g}–{float(maximum):g} g range"
    return "—"


def _raw_weight_label(row):
    raw_weight = row.get("raw_weight_g")
    if raw_weight is None:
        return "Raw source: —"
    status = " (rejected)" if not 100 <= int(raw_weight) <= 190 else ""
    return f"Raw source{status}: {int(raw_weight)} g"


def _link(url, label):
    parsed = urlparse(str(url or ""))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return _escape(label)
    return f'<a href="{_escape(url)}" target="_blank" rel="noopener noreferrer">{_escape(label)}</a>'


def render_disc_attribute_audit_html(summary, samples, generated_at=None):
    generated_at = generated_at or datetime.now(timezone.utc)
    totals = {
        key: sum(int(row.get(key) or 0) for row in summary)
        for key in (
            "discs", "flight_resolved", "weight_resolved", "weight_ranges",
            "invalid_raw_weight",
            "rejected_weight", "corrected_weight", "store_api_differences", "weight_conflicts",
            "html_bare_flights", "unlabelled_title_weights",
            "try_discs_exact_flights", "try_discs_alias_flights",
            "llm_weights", "llm_no_weight",
        )
    }
    cards = (
        ("Disc variants", "discs"),
        ("Flight resolved", "flight_resolved"),
        ("Try Discs exact", "try_discs_exact_flights"),
        ("Try Discs alias", "try_discs_alias_flights"),
        ("Weight resolved", "weight_resolved"),
        ("Title weight ranges", "weight_ranges"),
        ("LLM weight selected", "llm_weights"),
        ("Invalid source weight", "invalid_raw_weight"),
        ("Rejected weight", "rejected_weight"),
        ("Corrected weight", "corrected_weight"),
        ("Store/API differences", "store_api_differences"),
        ("Source weight differences", "weight_conflicts"),
        ("Unlabelled HTML flights", "html_bare_flights"),
        ("Unlabelled title weights", "unlabelled_title_weights"),
    )
    card_html = "".join(
        f'<div class="card"><span>{label}</span><strong>{_number(totals[key])}</strong></div>'
        for label, key in cards
    )
    store_html = "".join(
        "<tr>"
        f"<td>{_escape(row.get('source'))}</td><td>{_escape(row.get('store'))}</td>"
        f"<td>{_number(row.get('discs'))}</td>"
        f"<td>{_number(row.get('flight_resolved'))}</td>"
        f"<td>{_number(row.get('weight_resolved'))}</td>"
        f"<td>{_number(row.get('weight_ranges'))}</td>"
        f"<td>{_number(row.get('rejected_weight'))}</td>"
        f"<td>{_number(row.get('corrected_weight'))}</td>"
        f"<td>{_number(row.get('store_api_differences'))}</td>"
        f"<td>{_number(row.get('weight_conflicts'))}</td>"
        "</tr>"
        for row in summary
    )
    sample_html = []
    categories = sorted({str(row.get("audit_category") or "") for row in samples})
    for row in samples:
        category = str(row.get("audit_category") or "")
        search_text = " ".join(str(row.get(field) or "") for field in (
            "source", "store", "title", "variant_title", "flight_source", "weight_source",
            "flight_evidence", "weight_evidence", "html_excerpt",
        )).lower()
        flight_values = " / ".join(
            "—" if row.get(field) is None else str(row[field])
            for field in ("speed", "glide", "turn", "fade")
        )
        details = (
            "<details><summary>Evidence and HTML excerpt</summary>"
            f"<p><b>Flight:</b> {_escape(row.get('flight_evidence'))}</p>"
            f"<p><b>Store flight:</b> {_escape(' / '.join('—' if row.get(field) is None else str(row[field]) for field in ('local_speed', 'local_glide', 'local_turn', 'local_fade')))}</p>"
            f"<p><b>Weight:</b> {_escape(row.get('weight_evidence'))}</p>"
            f"<pre>{_escape(row.get('html_excerpt'))}</pre></details>"
        )
        sample_html.append(
            f'<tr data-category="{_escape(category)}" data-search="{_escape(search_text)}">'
            f'<td><span class="badge">{_escape(category)}</span></td>'
            f"<td>{_link(row.get('product_link'), row.get('title') or 'Product')}"
            f"<br>{_escape(row.get('variant_title'))}<br><small>{_escape(row.get('source'))} · {_escape(row.get('store'))}</small></td>"
            f"<td>{_escape(flight_values)}<br><small>{_escape(row.get('flight_source'))} · {_score(row.get('flight_confidence'))}</small></td>"
            f"<td><b>{_escape(_selected_weight(row))}</b>"
            f"<br><small>Selected · {_escape(row.get('weight_source'))} · {_score(row.get('weight_confidence'))}</small>"
            f"<br><small>{_escape(_raw_weight_label(row))}</small></td>"
            f"<td>{details}</td></tr>"
        )
    options = "".join(f'<option value="{_escape(category)}">{_escape(category)}</option>' for category in categories)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Disc Attribute Audit Report</title>
<style>
:root {{ color-scheme:light; --ink:#17202a; --muted:#64748b; --line:#dbe3ea; --panel:#f8fafc; }}
* {{ box-sizing:border-box; }} body {{ margin:0; color:var(--ink); background:#eef2f6; font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif; }}
main {{ width:min(1700px,96vw); margin:24px auto 60px; }} h1 {{ margin-bottom:4px; }} h2 {{ margin-top:30px; }}
.muted,small {{ color:var(--muted); }} .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(155px,1fr)); gap:12px; margin:20px 0; }}
.card,.panel {{ background:white; border:1px solid var(--line); border-radius:10px; padding:15px; box-shadow:0 2px 8px #0f172a0a; }}
.card strong {{ display:block; font-size:26px; }} .panel {{ margin:14px 0; overflow:auto; }} table {{ width:100%; border-collapse:collapse; }}
th,td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }} th {{ position:sticky; top:0; background:var(--panel); z-index:1; }}
input,select {{ padding:8px 10px; border:1px solid #94a3b8; border-radius:6px; background:white; }} input {{ min-width:300px; }}
.controls {{ display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:12px; }} .badge {{ display:inline-block; border-radius:999px; padding:2px 8px; font-size:11px; font-weight:700; background:#dbeafe; color:#1e3a8a; }}
pre {{ max-width:650px; white-space:pre-wrap; overflow-wrap:anywhere; font-size:12px; }} details {{ min-width:180px; }} a {{ color:#1d4ed8; }}
</style></head><body><main>
<h1>Disc Attribute Audit Report</h1>
<p class="muted">Generated {_escape(generated_at.isoformat())}. Shopify and Infinite Discs variants classified as discs. Try Discs supplies primary flight values for unique model matches; store text supplies the fallback. Weights outside 100–190 g are rejected, and ambiguous weights can receive an evidence-bound LLM review. <a href="https://trydiscs.com">Disc data by Try Discs</a>.</p>
<div class="cards">{card_html}</div>
<h2>Coverage by store</h2>
<div class="panel"><table><thead><tr><th>Source</th><th>Store</th><th>Discs</th><th>Flight resolved</th><th>Exact weight</th><th>Weight range</th><th>Weight rejected</th><th>Weight corrected</th><th>Store/API differences</th><th>Source weight differences</th></tr></thead><tbody>{store_html}</tbody></table></div>
<h2>Evidence samples</h2>
<p>Up to {SAMPLE_LIMIT_PER_CATEGORY} deterministic rows per category. A variant can appear in more than one category. Counts above cover the full dataset.</p>
<div class="panel"><div class="controls"><input id="search" type="search" placeholder="Search listing, store, or evidence…"><select id="category"><option value="">All categories</option>{options}</select><span id="visible"></span></div>
<table id="samples"><thead><tr><th>Category</th><th>Listing</th><th>Flight</th><th>Selected weight / raw source</th><th>Evidence</th></tr></thead><tbody>{''.join(sample_html)}</tbody></table></div>
<script>
const search=document.querySelector('#search'), category=document.querySelector('#category'), rows=[...document.querySelectorAll('#samples tbody tr')], visible=document.querySelector('#visible');
function filterRows() {{ const q=search.value.trim().toLowerCase(), c=category.value; let count=0; for(const row of rows) {{ const show=(!c||row.dataset.category===c)&&(!q||row.dataset.search.includes(q)); row.hidden=!show; if(show) count++; }} visible.textContent=`${{count.toLocaleString()}} of ${{rows.length.toLocaleString()}} samples`; }}
search.addEventListener('input',filterRows); category.addEventListener('change',filterRows); filterRows();
</script></main></body></html>"""


def generate_disc_attribute_audit_html_report(client, project_id, dataset, output_path=DEFAULT_REPORT_PATH):
    summary, samples = fetch_disc_attribute_audit(client, project_id, dataset)
    rendered = render_disc_attribute_audit_html(summary, samples)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary_path.write_text(rendered, encoding="utf-8")
    temporary_path.replace(output_path)
    return {
        "output_path": str(output_path),
        "disc_count": sum(int(row.get("discs") or 0) for row in summary),
        "weight_ranges": sum(int(row.get("weight_ranges") or 0) for row in summary),
        "flight_resolved": sum(int(row.get("flight_resolved") or 0) for row in summary),
        "try_discs_exact": sum(int(row.get("try_discs_exact_flights") or 0) for row in summary),
        "try_discs_alias": sum(int(row.get("try_discs_alias_flights") or 0) for row in summary),
        "store_api_differences": sum(int(row.get("store_api_differences") or 0) for row in summary),
        "sample_count": len(samples),
        "store_count": len(summary),
    }
