"""Local HTML audit for evidence-bound disc weight LLM reviews."""

import html
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from disc_golf_pipeline.common.runtime import PROJECT_ROOT
from disc_golf_pipeline.services.disc_weight_llm import PROMPT_VERSION


DEFAULT_REPORT_PATH = PROJECT_ROOT / "reports" / "disc_weight_llm_review.html"
DETAIL_LIMIT = 500


def _table(project_id, dataset, table_name):
    return f"`{project_id}.{dataset}.{table_name}`"


def build_review_summary_sql(project_id, dataset):
    reviews = _table(project_id, dataset, "DiscWeightLlmReviews")
    snapshot = _table(project_id, dataset, "NormalizedVariantSnapshot")
    attributes = _table(project_id, dataset, "NormalizedDiscAttributes")
    return f"""
SELECT
  COUNT(*) AS attempted,
  COUNTIF(r.status = 'FOUND') AS found,
  COUNTIF(r.status = 'NONE') AS no_specific_weight,
  COUNTIF(r.status = 'INVALID') AS invalid,
  COUNTIF(a.weight_evidence_hash = r.evidence_hash) AS current_evidence,
  COUNTIF(a.weight_evidence_hash IS DISTINCT FROM r.evidence_hash) AS stale_evidence,
  MAX(r.reviewed_at) AS latest_review_at
FROM {reviews} AS r
LEFT JOIN {snapshot} AS s ON r.id = s.id AND s.item_type = 'disc'
LEFT JOIN {attributes} AS a ON r.id = a.id
WHERE r.prompt_version = '{PROMPT_VERSION}'
"""


def build_review_scope_sql(project_id, dataset):
    reviews = _table(project_id, dataset, "DiscWeightLlmReviews")
    snapshot = _table(project_id, dataset, "NormalizedVariantSnapshot")
    attributes = _table(project_id, dataset, "NormalizedDiscAttributes")
    return f"""
WITH candidate_rows AS (
  SELECT
    s.id, a.weight_evidence_hash,
    CASE
      WHEN a.normalized_weight_g IS NULL THEN 'missing'
      WHEN a.weight_confidence <= 0.55 THEN 'conflicting'
      ELSE 'html_only'
    END AS queue_reason,
    EXISTS (
      SELECT 1 FROM {reviews} AS r
      WHERE r.id = s.id
        AND r.evidence_hash = a.weight_evidence_hash
        AND r.prompt_version = '{PROMPT_VERSION}'
        AND r.status IN ('FOUND', 'NONE')
    ) AS completed
  FROM {snapshot} AS s
  JOIN {attributes} AS a ON s.id = a.id
  WHERE s.item_type = 'disc'
    AND COALESCE(a.weight_source, '') != 'variant_title_range'
    AND (a.normalized_weight_g IS NULL OR a.weight_confidence <= 0.55
         OR a.weight_source = 'body_html')
)
SELECT COUNT(*) AS eligible, COUNTIF(completed) AS completed,
       COUNTIF(NOT completed) AS pending,
       COUNTIF(queue_reason = 'missing' AND NOT completed) AS pending_missing,
       COUNTIF(queue_reason = 'conflicting' AND NOT completed) AS pending_conflicting,
       COUNTIF(queue_reason = 'html_only' AND NOT completed) AS pending_html_only
FROM candidate_rows
"""


def build_review_details_sql(project_id, dataset, limit=DETAIL_LIMIT):
    if limit < 1:
        raise ValueError("limit must be positive")
    reviews = _table(project_id, dataset, "DiscWeightLlmReviews")
    snapshot = _table(project_id, dataset, "NormalizedVariantSnapshot")
    attributes = _table(project_id, dataset, "NormalizedDiscAttributes")
    return f"""
SELECT
  r.id, r.status, r.weight_g AS reviewed_weight_g, r.evidence, r.reason,
  r.model_name, r.reviewed_at,
  s.source, s.store, s.title, s.variant_title, s.product_link,
  SAFE_CAST(s.weight_g AS INT64) AS source_weight_g,
  a.normalized_weight_g AS current_weight_g,
  a.weight_source AS current_weight_source,
  a.weight_evidence_hash = r.evidence_hash AS current_evidence
FROM {reviews} AS r
LEFT JOIN {snapshot} AS s ON r.id = s.id AND s.item_type = 'disc'
LEFT JOIN {attributes} AS a ON r.id = a.id
WHERE r.prompt_version = '{PROMPT_VERSION}'
ORDER BY r.reviewed_at DESC, r.id
LIMIT {int(limit)}
"""


def _escape(value):
    return html.escape("" if value is None else str(value), quote=True)


def _count(value):
    return f"{int(value or 0):,}"


def _weight(value):
    return "—" if value is None else f"{int(value)} g"


def _listing_link(url, title):
    parsed = urlparse(str(url or ""))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return _escape(title or "Unknown listing")
    return f'<a href="{_escape(url)}" target="_blank" rel="noopener noreferrer">{_escape(title or "Listing")}</a>'


def render_disc_weight_llm_audit_html(summary, scope, details, generated_at=None):
    generated_at = generated_at or datetime.now(timezone.utc)
    cards = (
        ("Reviewed", summary.get("attempted")),
        ("Evidence-backed weights", summary.get("found")),
        ("No specific weight", summary.get("no_specific_weight")),
        ("Invalid response", summary.get("invalid")),
        ("Current evidence", summary.get("current_evidence")),
        ("Pending candidates", scope.get("pending")),
    )
    card_html = "".join(
        f'<div class="card"><span>{_escape(label)}</span><strong>{_count(value)}</strong></div>'
        for label, value in cards
    )
    rows = []
    for row in details:
        status = str(row.get("status") or "UNKNOWN")
        reviewed_at = row.get("reviewed_at")
        if hasattr(reviewed_at, "isoformat"):
            reviewed_at = reviewed_at.isoformat()
        search_text = " ".join(str(row.get(key) or "") for key in (
            "status", "store", "title", "variant_title", "evidence", "reason"
        )).lower()
        rows.append(
            f'<tr data-status="{_escape(status)}" data-search="{_escape(search_text)}">'
            f'<td><span class="badge {_escape(status.lower())}">{_escape(status)}</span>'
            f'<br><small>{_escape(reviewed_at)}</small></td>'
            f'<td>{_listing_link(row.get("product_link"), row.get("title"))}'
            f'<br>{_escape(row.get("variant_title"))}'
            f'<br><small>{_escape(row.get("source"))} · {_escape(row.get("store"))}</small></td>'
            f'<td>{_escape(_weight(row.get("source_weight_g")))}</td>'
            f'<td>{_escape(_weight(row.get("reviewed_weight_g")))}</td>'
            f'<td>{_escape(_weight(row.get("current_weight_g")))}'
            f'<br><small>{_escape(row.get("current_weight_source"))}</small></td>'
            f'<td>{_escape(row.get("evidence"))}'
            f'<br><small>{_escape(row.get("reason"))}</small></td>'
            f'<td>{"Yes" if row.get("current_evidence") else "No"}</td></tr>'
        )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Disc Weight LLM Review</title>
<style>
:root {{ color-scheme:light; --ink:#17202a; --muted:#64748b; --line:#dbe3ea; --panel:#f8fafc; }}
* {{ box-sizing:border-box; }} body {{ margin:0; color:var(--ink); background:#eef2f6; font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif; }}
main {{ width:min(1700px,96vw); margin:24px auto 60px; }} .muted,small {{ color:var(--muted); }}
.cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:12px; margin:20px 0; }}
.card,.panel {{ background:white; border:1px solid var(--line); border-radius:10px; padding:15px; box-shadow:0 2px 8px #0f172a0a; }}
.card strong {{ display:block; font-size:26px; }} .panel {{ overflow:auto; }} table {{ width:100%; border-collapse:collapse; }}
th,td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }} th {{ position:sticky; top:0; background:var(--panel); }}
input,select {{ padding:8px 10px; border:1px solid #94a3b8; border-radius:6px; background:white; }} input {{ min-width:300px; }}
.controls {{ display:flex; flex-wrap:wrap; gap:10px; margin-bottom:12px; }} .badge {{ display:inline-block; border-radius:999px; padding:2px 8px; font-size:11px; font-weight:700; background:#e2e8f0; }}
.found {{ background:#dcfce7; color:#166534; }} .none {{ background:#fef3c7; color:#92400e; }} .invalid {{ background:#fee2e2; color:#991b1b; }}
a {{ color:#1d4ed8; }}
</style></head><body><main>
<h1>Disc Weight LLM Review</h1>
<p class="muted">Generated {_escape(generated_at.isoformat())}. Prompt version {_escape(PROMPT_VERSION)}. This report reads stored reviews; generating it makes no LLM calls.</p>
<div class="cards">{card_html}</div>
<p>Of {_count(scope.get('eligible'))} disc variants in the current exception queue, {_count(scope.get('completed'))} have a cached final review and {_count(scope.get('pending'))} remain pending: {_count(scope.get('pending_missing'))} missing, {_count(scope.get('pending_conflicting'))} with conflicting evidence, and {_count(scope.get('pending_html_only'))} using only HTML evidence. A <b>FOUND</b> weight has a valid gram value and matching variant-title or HTML evidence. <b>NONE</b> means no defensible variant-specific weight was found. <b>INVALID</b> failed response or evidence validation and remains eligible for retry. Reviews with changed source evidence are marked non-current.</p>
<p>{_count(summary.get('stale_evidence'))} stored reviews have evidence that no longer matches the current variant. Showing the latest {min(len(details), DETAIL_LIMIT):,} review rows.</p>
<h2>Review details</h2><div class="panel"><div class="controls"><input id="search" type="search" placeholder="Search listing or evidence…"><select id="status"><option value="">All statuses</option><option>FOUND</option><option>NONE</option><option>INVALID</option></select><span id="visible"></span></div>
<table><thead><tr><th>Decision</th><th>Listing and variant</th><th>Source weight</th><th>LLM weight</th><th>Current weight</th><th>Evidence and reason</th><th>Current evidence?</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
<script>
const search=document.querySelector('#search'), status=document.querySelector('#status'), rows=[...document.querySelectorAll('tbody tr')], visible=document.querySelector('#visible');
function filterRows() {{ const q=search.value.trim().toLowerCase(), s=status.value; let count=0; for(const row of rows) {{ const show=(!s||row.dataset.status===s)&&(!q||row.dataset.search.includes(q)); row.hidden=!show; if(show) count++; }} visible.textContent=`${{count.toLocaleString()}} of ${{rows.length.toLocaleString()}} shown`; }}
search.addEventListener('input',filterRows); status.addEventListener('change',filterRows); filterRows();
</script></main></body></html>"""


def generate_disc_weight_llm_audit_report(client, project_id, dataset,
                                          output_path=DEFAULT_REPORT_PATH):
    summary = dict(next(iter(client.query(build_review_summary_sql(project_id, dataset)).result())).items())
    scope = dict(next(iter(client.query(build_review_scope_sql(project_id, dataset)).result())).items())
    details = [dict(row.items()) for row in client.query(
        build_review_details_sql(project_id, dataset)
    ).result()]
    rendered = render_disc_weight_llm_audit_html(summary, scope, details)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary_path.write_text(rendered, encoding="utf-8")
    temporary_path.replace(output_path)
    return {"output_path": str(output_path),
            "attempted": int(summary.get("attempted") or 0),
            "found": int(summary.get("found") or 0),
            "no_specific_weight": int(summary.get("no_specific_weight") or 0),
            "invalid": int(summary.get("invalid") or 0),
            "pending": int(scope.get("pending") or 0),
            "detail_rows": len(details)}
