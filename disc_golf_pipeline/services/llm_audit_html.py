import html
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from disc_golf_pipeline.common.runtime import PROJECT_ROOT
from disc_golf_pipeline.services.llm_resolution import (
    DEFAULT_LLM_PROMOTION_POLICY_VERSION,
    build_table_ref,
    get_llm_audit_report,
    get_prompt_contract_hash,
)


DEFAULT_REPORT_PATH = PROJECT_ROOT / "reports" / "pdga_llm_review_report.html"


def build_llm_review_records_sql(project_id: str, dataset: str) -> str:
    reviews_table = build_table_ref(project_id, dataset, "DiscModelLlmReviews")
    queue_table = build_table_ref(project_id, dataset, "DiscModelLlmQueue")
    resolutions_table = build_table_ref(
        project_id, dataset, "DiscModelLlmResolutions"
    )
    prompt_hash = get_prompt_contract_hash()
    return f"""
SELECT
  review.review_id,
  review.run_id,
  review.review_key,
  review.evidence_hash,
  review.decision_level,
  review.audit_stratum,
  review.status,
  review.selected_entity_id,
  review.rationale,
  review.error_message,
  review.raw_response,
  review.input_tokens,
  review.output_tokens,
  review.created_at,
  resolution.resolution_id,
  resolution.decision_source AS promotion_source,
  resolution.promoted_at,
  queue.store,
  queue.title,
  queue.variant_title,
  queue.raw_vendor,
  queue.source_normalized_manufacturer,
  TO_JSON_STRING(queue.candidates) AS candidates_json
FROM {reviews_table} AS review
LEFT JOIN {queue_table} AS queue
  ON review.review_key = queue.review_key
 AND review.evidence_hash = queue.evidence_hash
LEFT JOIN {resolutions_table} AS resolution
  ON review.review_key = resolution.review_key
 AND review.evidence_hash = resolution.evidence_hash
 AND review.review_id = resolution.review_id
 AND resolution.promotion_policy_version = '{DEFAULT_LLM_PROMOTION_POLICY_VERSION}'
WHERE review.prompt_hash = '{prompt_hash}'
ORDER BY review.created_at, review.review_key
"""


def _row_to_dict(row):
    return dict(row.items())


def fetch_llm_review_records(client, project_id: str, dataset: str):
    rows = client.query(build_llm_review_records_sql(project_id, dataset)).result()
    return [_row_to_dict(row) for row in rows]


def _escape(value):
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


def _format_candidates(candidates_json):
    try:
        candidates = json.loads(candidates_json or "[]")
    except (TypeError, json.JSONDecodeError):
        return _escape(candidates_json)
    values = []
    for candidate in candidates:
        manufacturer = candidate.get("manufacturer") or "Unknown manufacturer"
        model = candidate.get("model") or "Unknown model"
        entity_id = candidate.get("disc_entity_id") or ""
        values.append(
            f"<li><strong>{_escape(manufacturer)} / {_escape(model)}</strong>"
            f"<br><code>{_escape(entity_id)}</code></li>"
        )
    return f"<ol>{''.join(values)}</ol>" if values else ""


def _status_badge(status):
    normalized = str(status or "UNKNOWN").upper()
    return f'<span class="badge badge-{_escape(normalized.lower())}">{_escape(normalized)}</span>'


def _render_review_rows(records):
    rendered = []
    for record in records:
        searchable = " ".join(
            str(record.get(key) or "")
            for key in (
                "status",
                "audit_stratum",
                "decision_level",
                "store",
                "title",
                "variant_title",
                "raw_vendor",
                "source_normalized_manufacturer",
                "review_key",
                "selected_entity_id",
                "rationale",
                "error_message",
                "candidates_json",
                "promotion_source",
            )
        ).lower()
        raw_details = ""
        if record.get("raw_response") or record.get("error_message"):
            raw_details = (
                "<details><summary>Raw response / error</summary>"
                f"<pre>{_escape(record.get('raw_response'))}</pre>"
                f"<p class=\"error\">{_escape(record.get('error_message'))}</p>"
                "</details>"
            )
        promotion_badge = (
            '<span class="badge badge-promoted">PROMOTED</span><br>'
            if record.get("resolution_id")
            else ""
        )
        rendered.append(
            "<tr "
            f'data-status="{_escape(str(record.get("status") or "").upper())}" '
            f'data-search="{_escape(searchable)}">'
            f"<td>{_status_badge(record.get('status'))}</td>"
            f"<td>{_escape(record.get('audit_stratum') or 'unstratified')}"
            f"<br><small>{_escape(record.get('decision_level'))}</small></td>"
            f"<td><strong>{_escape(record.get('title'))}</strong>"
            f"<br>{_escape(record.get('variant_title'))}"
            f"<br><small>{_escape(record.get('store'))}</small></td>"
            f"<td>{_escape(record.get('raw_vendor'))}"
            f"<br><small>Normalized: {_escape(record.get('source_normalized_manufacturer'))}</small></td>"
            f"<td>{_format_candidates(record.get('candidates_json'))}</td>"
            f"<td>{promotion_badge}"
            f"<code>{_escape(record.get('selected_entity_id') or 'NONE')}</code>"
            f"<p>{_escape(record.get('rationale'))}</p>{raw_details}</td>"
            f"<td>{_escape(record.get('input_tokens'))} / {_escape(record.get('output_tokens'))}"
            f"<br><small>{_escape(record.get('review_key'))}</small></td>"
            "</tr>"
        )
    return "".join(rendered)


def render_llm_audit_html(audit_report, records, generated_at=None):
    generated_at = generated_at or datetime.now(timezone.utc)
    status_counts = Counter(str(row.get("status") or "UNKNOWN").upper() for row in records)
    stratum_counts = defaultdict(Counter)
    for row in records:
        stratum_counts[row.get("audit_stratum") or "unstratified"][
            str(row.get("status") or "UNKNOWN").upper()
        ] += 1

    total = len(records)
    accepted = status_counts["ACCEPT"]
    none_count = status_counts["NONE"]
    invalid = status_counts["INVALID"]
    failed = status_counts["ERROR"]
    promoted = sum(bool(row.get("resolution_id")) for row in records)
    input_tokens = sum(int(row.get("input_tokens") or 0) for row in records)
    output_tokens = sum(int(row.get("output_tokens") or 0) for row in records)

    stratum_rows = []
    for stratum, counts in sorted(stratum_counts.items()):
        stratum_total = sum(counts.values())
        stratum_rows.append(
            "<tr>"
            f"<td>{_escape(stratum)}</td><td>{stratum_total:,}</td>"
            f"<td>{counts['ACCEPT']:,}</td><td>{counts['NONE']:,}</td>"
            f"<td>{counts['INVALID']:,}</td><td>{counts['ERROR']:,}</td>"
            "</tr>"
        )

    run_rows = []
    for run in audit_report.get("runs", []):
        run_rows.append(
            "<tr>"
            f"<td><code>{_escape(run.get('run_id'))}</code></td>"
            f"<td>{_status_badge(run.get('status'))}</td>"
            f"<td>{_escape(run.get('prompt_version'))}</td>"
            f"<td>{int(run.get('attempted_count') or 0):,}</td>"
            f"<td>{int(run.get('accept_count') or 0):,}</td>"
            f"<td>{int(run.get('none_count') or 0):,}</td>"
            f"<td>{int(run.get('invalid_count') or 0):,}</td>"
            f"<td>{int(run.get('failed_count') or 0):,}</td>"
            f"<td>{int(run.get('input_tokens') or 0):,} / {int(run.get('output_tokens') or 0):,}</td>"
            f"<td>{_escape(run.get('started_at'))}</td>"
            "</tr>"
        )

    prompt_hash = get_prompt_contract_hash()
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Disc Model LLM v2 Audit Report</title>
<style>
:root {{ color-scheme: light; --ink:#17202a; --muted:#64748b; --line:#dbe3ea; --panel:#f8fafc; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; color:var(--ink); background:#eef2f6; font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif; }}
main {{ width:min(1800px,96vw); margin:24px auto 60px; }}
h1 {{ margin-bottom:4px; }} h2 {{ margin-top:30px; }} p {{ max-width:1000px; }}
.muted, small {{ color:var(--muted); }}
.cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin:20px 0; }}
.card {{ background:white; border:1px solid var(--line); border-radius:10px; padding:15px; box-shadow:0 2px 8px #0f172a0a; }}
.card strong {{ display:block; font-size:26px; }}
.panel {{ background:white; border:1px solid var(--line); border-radius:10px; padding:16px; margin:14px 0; overflow:auto; }}
table {{ width:100%; border-collapse:collapse; }} th,td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }}
th {{ position:sticky; top:0; background:var(--panel); z-index:1; }}
code,pre {{ white-space:pre-wrap; overflow-wrap:anywhere; font-size:12px; }} ol {{ margin:0; padding-left:20px; }}
.badge {{ display:inline-block; border-radius:999px; padding:2px 8px; font-weight:700; font-size:11px; }}
.badge-accept,.badge-succeeded {{ color:#166534; background:#dcfce7; }} .badge-none {{ color:#854d0e; background:#fef9c3; }}
.badge-invalid {{ color:#9a3412; background:#ffedd5; }} .badge-error,.badge-failed {{ color:#991b1b; background:#fee2e2; }}
.badge-promoted {{ color:#1e3a8a; background:#dbeafe; }}
.controls {{ display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:12px; }}
input,select {{ padding:8px 10px; border:1px solid #94a3b8; border-radius:6px; background:white; }} input {{ min-width:320px; }}
.error {{ color:#991b1b; }} details {{ margin-top:8px; }}
</style>
</head>
<body><main>
<h1>Disc Model LLM v2 Audit Report</h1>
<p class="muted">Generated {_escape(generated_at.isoformat())}. This report preserves raw audit outcomes and identifies decisions materialized by the versioned promotion policy.</p>
<div class="cards">
  <div class="card"><span>Total reviewed</span><strong>{total:,}</strong></div>
  <div class="card"><span>Accepted candidate</span><strong>{accepted:,}</strong></div>
  <div class="card"><span>Returned NONE</span><strong>{none_count:,}</strong></div>
  <div class="card"><span>Invalid response</span><strong>{invalid:,}</strong></div>
  <div class="card"><span>Remote failure</span><strong>{failed:,}</strong></div>
  <div class="card"><span>Promoted</span><strong>{promoted:,}</strong></div>
  <div class="card"><span>Input / output tokens</span><strong>{input_tokens:,} / {output_tokens:,}</strong></div>
</div>
<div class="panel"><strong>Current prompt contract</strong><br><code>{_escape(prompt_hash)}</code></div>
<h2>Outcome by audit stratum</h2>
<div class="panel"><table><thead><tr><th>Stratum</th><th>Total</th><th>Accept</th><th>None</th><th>Invalid</th><th>Error</th></tr></thead>
<tbody>{''.join(stratum_rows)}</tbody></table></div>
<h2>Recent audit runs</h2>
<div class="panel"><table><thead><tr><th>Run ID</th><th>Status</th><th>Prompt</th><th>Attempted</th><th>Accept</th><th>None</th><th>Invalid</th><th>Failed</th><th>Input / output tokens</th><th>Started</th></tr></thead>
<tbody>{''.join(run_rows)}</tbody></table></div>
<h2>Review records</h2>
<p>The table contains every review for the current prompt contract. Invalid rows remain unresolved and can be isolated with the status filter.</p>
<div class="panel">
  <div class="controls"><input id="search" type="search" placeholder="Search title, vendor, model, store, ID…"><select id="status"><option value="">All statuses</option><option>ACCEPT</option><option>NONE</option><option>INVALID</option><option>ERROR</option></select><span id="visible"></span></div>
  <table id="reviews"><thead><tr><th>Status</th><th>Stratum</th><th>Listing</th><th>Vendor</th><th>Supplied candidates</th><th>Decision and rationale</th><th>Tokens and key</th></tr></thead>
  <tbody>{_render_review_rows(records)}</tbody></table>
</div>
<script>
const search=document.querySelector('#search'), status=document.querySelector('#status'), rows=[...document.querySelectorAll('#reviews tbody tr')], visible=document.querySelector('#visible');
function filterRows() {{ const q=search.value.trim().toLowerCase(), s=status.value; let count=0; for(const row of rows) {{ const show=(!s||row.dataset.status===s)&&(!q||row.dataset.search.includes(q)); row.hidden=!show; if(show) count++; }} visible.textContent=`${{count.toLocaleString()}} of ${{rows.length.toLocaleString()}} records`; }}
search.addEventListener('input',filterRows); status.addEventListener('change',filterRows); filterRows();
</script>
</main></body></html>
"""


def generate_llm_audit_html_report(
    client,
    project_id: str,
    dataset: str,
    output_path=DEFAULT_REPORT_PATH,
):
    audit_report = get_llm_audit_report(client, project_id, dataset)
    records = fetch_llm_review_records(client, project_id, dataset)
    rendered = render_llm_audit_html(audit_report, records)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary_path.write_text(rendered, encoding="utf-8")
    temporary_path.replace(output_path)
    return {
        "output_path": str(output_path),
        "record_count": len(records),
        "invalid_count": sum(row.get("status") == "INVALID" for row in records),
    }
