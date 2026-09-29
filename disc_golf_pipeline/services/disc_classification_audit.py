"""Catalog classification review in an isolated dataset, with a local HTML audit."""

import html
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from urllib.parse import urlparse

from google.cloud import bigquery

from disc_golf_pipeline.common.runtime import PROJECT_ROOT
from disc_golf_pipeline.services.disc_classification import ALGORITHM_VERSION, run_disc_classification
from disc_golf_pipeline.services.normalization_job import build_job_id, write_json_atomic, write_text_atomic
from disc_golf_pipeline.services.process_data import build_variant_snapshot_query, build_variant_snapshot_view_sql


REVIEW_DIRECTORY = PROJECT_ROOT / "output" / "classification-review"
DEFAULT_REPORT_PATH = REVIEW_DIRECTORY / "report.html"
JOBS_DIRECTORY = PROJECT_ROOT / "output" / "classification-jobs"
SAMPLE_LIMIT = 100
INPUT_TABLES = ("NormalizedVariantSnapshot", "TryDiscsModelMatches", "DiscWeightLlmReviews")


def _table(project, dataset, name):
    # CLI dataset overrides become identifiers, never SQL fragments.
    if not re.fullmatch(r"[A-Za-z0-9_-]+", project) or not re.fullmatch(r"[A-Za-z0-9_]+", dataset):
        raise ValueError("Invalid BigQuery project or dataset identifier")
    return f"`{project}.{dataset}.{name}`"


def build_audit_rows_sql(project, dataset):
    return f"""
SELECT c.*, s.source, s.store, s.title, s.variant_title, s.product_link,
  s.normalized_manufacturer, s.normalized_model, s.product_type,
  s.IsDistanceDriver, s.IsFairwayDriver, s.IsMidrange, s.IsPutter,
  s.weight_g AS raw_weight_g,
  SUBSTR(REGEXP_REPLACE(COALESCE(s.BodyHtml, ''), r'<[^>]*>', ' '), 1, 1200) AS html_excerpt,
  a.speed, a.glide, a.turn, a.fade,
  a.local_speed, a.local_glide, a.local_turn, a.local_fade,
  a.flight_source, a.flight_confidence, a.flight_evidence, a.flight_attribution,
  a.catalog_raw_flight_json, a.local_flight_records_json, a.flight_invalid_fields,
  a.normalized_weight_g, a.weight_review_status,
  CASE WHEN NULLIF(TRIM(s.normalized_manufacturer), '') IS NOT NULL
    AND NULLIF(TRIM(s.normalized_model), '') IS NOT NULL
    THEN TO_JSON_STRING(STRUCT(s.normalized_manufacturer, s.normalized_model)) END AS mold_key,
  (ABS(c.access_low - 70) <= 2 OR ABS(c.access_high - 55) <= 2) AS near_cutoff
FROM {_table(project, dataset, 'NormalizedDiscClassifications')} c
JOIN {_table(project, dataset, 'NormalizedVariantSnapshot')} s USING (id)
JOIN {_table(project, dataset, 'NormalizedDiscAttributes')} a USING (id)
"""


def build_summary_sql(project, dataset):
    return f"""
SELECT dimension.name AS dimension, dimension.value AS value,
  COUNT(*) AS variants, COUNT(DISTINCT mold_key) AS identified_molds,
  COUNT(DISTINCT IF(data_status = 'complete', mold_key, NULL)) AS molds_with_complete_flight,
  COUNT(DISTINCT flight_record_key) AS flight_records,
  COUNTIF(mold_key IS NULL) AS unidentified_variants,
  COUNTIF(data_status = 'complete') AS complete_flight,
  COUNTIF(access_variant IS NOT NULL) AS exact_scores,
  COUNTIF(weight_status = 'range') AS ranged_weights,
  COUNTIF(weight_status IN ('missing', 'invalid')) AS unknown_weights,
  COUNTIF(disc_category IS NULL) AS missing_categories,
  COUNTIF(flight_conflict AND NOT flight_conflict_unresolved) AS resolved_disagreements,
  COUNTIF(flight_conflict_unresolved) AS unresolved_conflicts,
  COUNTIF(near_cutoff) AS near_cutoff
FROM {_table(project, dataset, 'DiscClassificationAuditRows')}
CROSS JOIN UNNEST([
  STRUCT('total' AS name, 'All disc variants' AS value),
  STRUCT('brand', COALESCE(NULLIF(normalized_manufacturer, ''), 'Unknown')),
  STRUCT('store', CONCAT(COALESCE(source, 'Unknown'), ' / ', COALESCE(store, 'Unknown'))),
  STRUCT('model', IF(mold_key IS NULL, 'Unknown identity',
    CONCAT(normalized_manufacturer, ' / ', normalized_model))),
  STRUCT('status', data_status), STRUCT('weight', weight_status),
  STRUCT('profile', COALESCE(approx_stability, 'Unknown')),
  STRUCT('beginner role', beginner_role), STRUCT('category', COALESCE(disc_category, 'Unknown'))
]) dimension
GROUP BY 1, 2
ORDER BY dimension, variants DESC, value
"""


def build_samples_sql(project, dataset, limit=SAMPLE_LIMIT):
    if not 1 <= limit <= 1000:
        raise ValueError("Sample limit must be between 1 and 1000")
    return f"""
WITH categorized AS (
  SELECT a.*, sample_group
  FROM {_table(project, dataset, 'DiscClassificationAuditRows')} a
  CROSS JOIN UNNEST([
    CONCAT('Status: ', data_status), CONCAT('Weight: ', weight_status),
    CONCAT('Role: ', beginner_role),
    CONCAT('Profile: ', COALESCE(approx_stability, 'unknown')),
    IF(disc_category IS NULL, 'Missing category', NULL),
    IF(flight_conflict AND NOT flight_conflict_unresolved, 'Resolved source disagreement', NULL),
    IF(near_cutoff, 'Near 55 or 70 cutoff', NULL),
    IF(mold_key IS NOT NULL, 'Independent mold review', NULL)
  ]) sample_group
  WHERE sample_group IS NOT NULL
), distinct_molds AS (
  SELECT * FROM categorized
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sample_group, COALESCE(mold_key, CONCAT('variant:', id))
    ORDER BY FARM_FINGERPRINT(id), id
  ) = 1
)
SELECT * FROM distinct_molds
QUALIFY ROW_NUMBER() OVER (PARTITION BY sample_group
  ORDER BY FARM_FINGERPRINT(COALESCE(mold_key, id)), id)
  <= IF(sample_group = 'Independent mold review', 50, {int(limit)})
ORDER BY sample_group, normalized_manufacturer, normalized_model, id
"""


def _escape(value):
    return html.escape("" if value is None else str(value), quote=True)


def _score(value):
    if value is None:
        return "—"
    number = Decimal(str(value))
    return str(number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)) if number.is_finite() else "—"


def _link(url, label):
    parsed = urlparse(str(url or ""))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return _escape(label)
    return f'<a href="{_escape(url)}" target="_blank" rel="noopener noreferrer">{_escape(label)}</a>'


def _flights(row, prefix=""):
    return " / ".join("—" if row.get(prefix + key) is None else str(row[prefix + key])
                      for key in ("speed", "glide", "turn", "fade"))


def _weight(row):
    if row.get("weight_status") == "exact":
        return f"{row['weight_min_g']:g} g"
    if row.get("weight_status") == "range":
        return f"{row['weight_min_g']:g}–{row['weight_max_g']:g} g"
    return row.get("weight_status") or "missing"


def render_classification_audit(summary, samples, metadata):
    total = next((r for r in summary if r["dimension"] == "total"), {})
    cards = [("Disc variants", "variants"), ("Identified molds", "identified_molds"),
             ("Molds with complete flights", "molds_with_complete_flight"),
             ("Complete flight variants", "complete_flight"), ("Exact variant scores", "exact_scores"),
             ("Weight ranges", "ranged_weights"), ("Missing categories", "missing_categories"),
             ("Unresolved conflicts", "unresolved_conflicts")]
    card_html = "".join(f'<div class="card">{label}<strong>{int(total.get(key) or 0):,}</strong></div>'
                        for label, key in cards)
    coverage = "".join(
        f'<tr data-dimension="{_escape(r["dimension"])}" data-search="{_escape(r["value"]).lower()}">'
        f'<td>{_escape(r["dimension"])}</td><td>{_escape(r["value"])}</td>'
        + "".join(f'<td>{int(r.get(key) or 0):,}</td>' for key in (
            "variants", "identified_molds", "molds_with_complete_flight", "complete_flight",
            "exact_scores", "ranged_weights", "unknown_weights", "resolved_disagreements",
            "unresolved_conflicts", "missing_categories", "near_cutoff")) + '</tr>'
        for r in summary
    )
    rows = []
    for row in samples:
        search = " ".join(str(v) for v in row.values() if v is not None).lower()
        reasons = ", ".join(row.get("reason_codes") or []) or "None"
        detail_fields = (
            ("Flight evidence", "flight_evidence"), ("Weight evidence", "weight_evidence"),
            ("Category evidence", "category_evidence"), ("Try Discs raw record", "catalog_raw_flight_json"),
            ("Retailer flight records", "local_flight_records_json"),
            ("Unsupported fields", "flight_invalid_fields"), ("HTML excerpt", "html_excerpt"),
            ("Flight record key", "flight_record_key"), ("Input hash", "classification_input_hash"),
            ("Variant key", "source_variant_key"), ("Algorithm", "algorithm_version"),
        )
        evidence = "".join(f'<dt>{label}</dt><dd>{_escape(row.get(key)) or "—"}</dd>'
                           for label, key in detail_fields)
        components = " · ".join(f'{label}: {_score(row.get(key))}' for label, key in (
            ("Speed", "speed_access"), ("Turn", "turn_support"), ("Fade", "gentle_finish"),
            ("Glide", "glide_support"), ("Driver blend", "driver_blend"), ("Weight adjustment", "weight_adjustment")))
        rows.append(
            f'<tr data-group="{_escape(row["sample_group"])}" data-search="{_escape(search)}">'
            f'<td>{_escape(row["sample_group"])}<br><span class="badge">{_escape(row.get("data_status"))}</span></td>'
            f'<td>{_link(row.get("product_link"), row.get("title") or "Product")}<br>{_escape(row.get("variant_title"))}'
            f'<br><small>{_escape(row.get("normalized_manufacturer"))} / {_escape(row.get("normalized_model"))}'
            f'<br>{_escape(row.get("store"))}</small></td>'
            f'<td>{_escape(_flights(row))}<br><small>{_escape(row.get("flight_source"))}'
            f'<br>Confidence {_score(row.get("flight_confidence"))}</small></td>'
            f'<td>{_escape(_weight(row))}<br><small>Source field: {_escape(row.get("raw_weight_g")) or "—"}'
            f'<br>{_escape(row.get("weight_source"))}<br>Confidence {_score(row.get("weight_confidence"))}</small></td>'
            f'<td>{_escape(row.get("disc_category")) or "Unknown category"}<br>{_escape(row.get("approx_stability")) or "—"}'
            f'<br><small>Power {_escape(row.get("power_band")) or "—"}; turn {_escape(row.get("turn_band")) or "—"}; '
            f'fade {_escape(row.get("fade_band")) or "—"}; glide {_escape(row.get("glide_band")) or "—"}'
            f'<br>Category source: {_escape(row.get("category_source")) or "—"}</small></td>'
            f'<td>Model {_score(row.get("access_model"))}<br>Variant {_score(row.get("access_variant"))}'
            f'<br>Bounds {_score(row.get("access_low"))}–{_score(row.get("access_high"))}'
            f'<br><b>{_escape(row.get("beginner_role"))}</b></td>'
            f'<td>{_escape(reasons)}<details><summary>Evidence and calculation</summary>'
            f'<p>{_escape(components)}</p><p>Retailer flights: {_escape(_flights(row, "local_"))}</p>'
            f'<p>Source disagreement: {_escape(row.get("flight_conflict"))}; unresolved: '
            f'{_escape(row.get("flight_conflict_unresolved"))}</p><dl>{evidence}</dl></details></td></tr>'
        )
    dimensions = sorted({r["dimension"] for r in summary})
    groups = sorted({r["sample_group"] for r in samples})

    def options(values):
        return "".join(f'<option value="{_escape(v)}">{_escape(v)}</option>' for v in values)

    scope = ("Isolated catalog review; production data and Typesense were not changed."
             if metadata.get("isolated_review") else "Report from the selected classification dataset.")
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Disc classification audit</title><style>
*{{box-sizing:border-box}} body{{margin:0;background:#edf2f5;color:#172d35;font:14px/1.5 system-ui,sans-serif}}
main{{width:96%;max-width:1900px;margin:28px auto 60px}}h1{{margin-bottom:4px}}small,.muted{{color:#526971}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(165px,1fr));gap:12px;margin:24px 0}}
.card,.panel{{background:white;border:1px solid #d4e0e5;border-radius:10px;padding:16px}}.card strong{{display:block;font-size:28px}}
.panel{{overflow:auto;margin-bottom:24px}}table{{width:100%;border-collapse:collapse}}th,td{{text-align:left;padding:10px;border-bottom:1px solid #d4e0e5;vertical-align:top}}
th{{background:#eef5f6;white-space:nowrap}}a{{color:#086884}}input,select,button{{padding:8px;border:1px solid #8da7b2;border-radius:5px;background:white}}
.controls{{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:14px}}input{{min-width:280px}}button{{cursor:pointer}}
.badge{{display:inline-block;background:#e2eef0;border-radius:4px;padding:2px 5px;font-size:12px}}details{{min-width:200px;max-width:540px}}
dt{{font-weight:bold}}dd{{margin:0 0 10px;white-space:pre-wrap;overflow-wrap:anywhere}}#samples td:nth-child(2){{min-width:240px}}
</style></head><body><main>
<h1>Disc classification audit</h1><p class="muted">Generated {_escape(metadata.get('generated_at'))} · {_escape(ALGORITHM_VERSION)}</p>
<p>{scope} Source: {_escape(metadata.get('source_dataset'))}. Review: {_escape(metadata.get('dataset'))}.</p>
<p>Access scores are throwing heuristics, not probabilities or player skill levels. Beginner roles are for review.
Missing scores are shown as —. Weight ranges retain bounds without an exact variant score. Display rounds half up to two decimals; calculations keep full precision.
<a href="https://trydiscs.com">Disc data by Try Discs</a>.</p>
<p><a href="{_escape(metadata.get('source_query_file', 'source.sql'))}" download>Download complete proposed Typesense source query</a> ·
<a href="{_escape(metadata.get('summary_file', 'summary.json'))}" download>Download audit counts</a></p>
<div class="cards">{card_html}</div>
<h2>Full catalog coverage</h2><p>Counts cover all disc variants. An identified mold is a distinct normalized manufacturer/model pair.
Molds with complete flights have at least one complete variant; other variants of the same mold may still be incomplete.
Unknown identities are excluded from mold counts ({int(total.get('unidentified_variants') or 0):,} variants).
Counts across dimensions overlap. Cutoff cases are within two points of a lower bound of 70 or an upper bound of 55.</p>
<div class="panel"><div class="controls"><select id="dimension"><option value="">All breakdowns</option>{options(dimensions)}</select>
<input id="coverage-search" type="search" placeholder="Search brand, model, or store"><span id="coverage-count"></span></div>
<table id="coverage"><thead><tr><th>Breakdown</th><th>Value</th><th>Variants</th><th>Molds</th><th>Molds with complete flight</th><th>Complete flights</th><th>Exact scores</th><th>Ranges</th><th>Unknown weights</th><th>Resolved differences</th><th>Unresolved conflicts</th><th>Missing category</th><th>Near cutoff</th></tr></thead><tbody>{coverage}</tbody></table></div>
<h2>Evidence samples</h2><p>Up to {SAMPLE_LIMIT} distinct molds per group (or separate variants when identity is unknown).
The independent review group contains up to 50 molds. One representative variant per mold per group; a variant may appear in multiple groups.
Search filters these samples. Use the source query for a listing that is absent here.</p>
<div class="panel"><div class="controls"><select id="group"><option value="">All sample groups</option>{options(groups)}</select>
<input id="search" type="search" placeholder="Search title, model, store, or evidence"><button id="previous">Previous</button><button id="next">Next</button><span id="sample-count"></span></div>
<table id="samples"><thead><tr><th>Review group / status</th><th>Listing</th><th>Speed / glide / turn / fade</th><th>Weight</th><th>Classification</th><th>Scores / role</th><th>Reasons</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
<script>
const coverageRows=[...document.querySelectorAll('#coverage tbody tr')],sampleRows=[...document.querySelectorAll('#samples tbody tr')];
const dimension=document.querySelector('#dimension'),coverageSearch=document.querySelector('#coverage-search'),group=document.querySelector('#group'),search=document.querySelector('#search');
let page=0;const pageSize=50;
function filterCoverage(){{let count=0;const q=coverageSearch.value.trim().toLowerCase();for(const row of coverageRows){{const show=(!dimension.value||row.dataset.dimension===dimension.value)&&(!q||row.dataset.search.includes(q));row.hidden=!show;if(show)count++;}}document.querySelector('#coverage-count').textContent=count+' rows';}}
function filterSamples(reset=false){{if(reset)page=0;const q=search.value.trim().toLowerCase(),matching=sampleRows.filter(r=>(!group.value||r.dataset.group===group.value)&&(!q||r.dataset.search.includes(q)));page=Math.max(0,Math.min(page,Math.ceil(matching.length/pageSize)-1));sampleRows.forEach(r=>r.hidden=true);matching.slice(page*pageSize,(page+1)*pageSize).forEach(r=>r.hidden=false);document.querySelector('#sample-count').textContent=matching.length+' matches · page '+(page+1)+' of '+Math.max(1,Math.ceil(matching.length/pageSize));document.querySelector('#previous').disabled=page===0;document.querySelector('#next').disabled=(page+1)*pageSize>=matching.length;}}
dimension.addEventListener('change',filterCoverage);coverageSearch.addEventListener('input',filterCoverage);group.addEventListener('change',()=>filterSamples(true));search.addEventListener('input',()=>filterSamples(true));document.querySelector('#previous').addEventListener('click',()=>{{page--;filterSamples();}});document.querySelector('#next').addEventListener('click',()=>{{page++;filterSamples();}});dimension.value='status';filterCoverage();filterSamples();
</script></main></body></html>"""


def generate_classification_report(client, project, dataset, output_path=DEFAULT_REPORT_PATH, metadata=None):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Freeze joined results once: coverage and samples must describe the same rows.
    client.query(f"CREATE OR REPLACE TABLE {_table(project, dataset, 'DiscClassificationAuditRows')} AS\n"
                 + build_audit_rows_sql(project, dataset)).result()
    summary = [dict(r.items()) for r in client.query(build_summary_sql(project, dataset)).result()]
    samples = [dict(r.items()) for r in client.query(build_samples_sql(project, dataset)).result()]
    metadata = {key: value for key, value in (metadata or {}).items()
                if key not in {"state", "stage", "error", "finished_at"}}
    metadata.update(generated_at=datetime.now(timezone.utc).isoformat(), dataset=f"{project}.{dataset}",
                    algorithm_version=ALGORITHM_VERSION, sample_count=len(samples), output_path=str(output_path))
    metadata.setdefault("source_dataset", f"{project}.{dataset}")
    query_path = output_path.with_suffix(".source.sql")
    summary_path = output_path.with_suffix(".summary.json")
    metadata.update(source_query_file=query_path.name, summary_file=summary_path.name)
    write_text_atomic(query_path,
        "-- Proposed Typesense source rows, including non-disc products.\n"
        "-- This does not query the active Typesense collection.\n"
        + build_variant_snapshot_query(project, dataset).strip() + ";\n")
    write_json_atomic(summary_path, {"metadata": metadata, "coverage": summary})
    write_text_atomic(output_path, render_classification_audit(summary, samples, metadata))
    return metadata


def run_classification_review(client, project, source_dataset, output_directory=REVIEW_DIRECTORY):
    """Materialize existing inputs into a new expiring dataset; never publish."""
    output_directory = Path(output_directory)
    run_id = build_job_id()
    dataset = f"ClassificationReview_{run_id.replace('-', '_')}"
    _table(project, source_dataset, "NormalizedVariantSnapshot")
    state = {"state": "running", "run_id": run_id, "started_at": datetime.now(timezone.utc).isoformat(),
             "source_dataset": f"{project}.{source_dataset}", "dataset": f"{project}.{dataset}",
             "isolated_review": True, "production_changed": False, "stage": "create review dataset",
             "table_expiration_days": 7}
    status_path = output_directory / "review-status.json"

    def progress(stage):
        state["stage"] = stage
        write_json_atomic(status_path, state)
        logging.info("Classification review: %s", stage)

    try:
        progress("create review dataset")
        origin = client.get_dataset(f"{project}.{source_dataset}")
        destination = bigquery.Dataset(f"{project}.{dataset}")
        destination.location = origin.location
        destination.default_table_expiration_ms = 7 * 86_400_000
        client.create_dataset(destination)  # Never replace/reuse a production dataset.
        for table in INPUT_TABLES:
            progress(f"copy {table}")
            client.query(f"CREATE TABLE {_table(project, dataset, table)} AS SELECT * FROM "
                         f"{_table(project, source_dataset, table)}").result()
        progress("classify copied inputs")
        run_disc_classification(client, project, dataset)
        progress("build proposed search snapshot")
        client.query(build_variant_snapshot_view_sql(project, dataset)).result()
        progress("generate HTML audit")
        metadata = generate_classification_report(client, project, dataset,
            output_path=output_directory / "report.html", metadata=state)
        state.update(metadata, state="succeeded", stage="complete")
    except BaseException as exc:
        state.update(state="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        state["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_json_atomic(status_path, state)
    return state
