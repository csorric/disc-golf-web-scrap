# Disc classification implementation plan

Status: steps 1-4 and the HTML audit tooling from step 6 are implemented on `disc-classification`. Steps 1-3 passed isolated BigQuery/Typesense integration validation. The catalog review completed successfully, and the user accepted the sample review on 29 September 2026. Production classifications, state, and Typesense were refreshed from cached inputs on 29 September 2026. Frontend integration belongs to the user's separate project.

Production rollout: job `20260929T184142Z-f01cada6` succeeded at 18:59:50 UTC, publishing collection `discs_20260929_184517_f02e65` under alias `discs_prod` with schema `search-v4-disc-classification-0.1`. Validation matched 227,987 documents to BigQuery and compared classification values in 24 representative documents. A subsequent read-only check confirmed the active alias, all 26 classification fields, and 145,778 complete exact-weight classification documents. No stores were scraped and no new Try Discs or LLM calls were made. The prior collection `discs_20260928_193924_752d13` was retained for rollback.

Review record: isolated run `20260929T013209Z-f37f259d` covered 177,488 disc variants and produced 2,257 evidence samples. After reviewing the independent mold sample group, the user reported, "I think they all look good." No corrections were requested. This records acceptance of the reviewed samples; catalog-wide exceptions remain visible in the audit. The HTML is at `output/classification-review/report.html`, and job completion is recorded under `output/classification-jobs/` and `output/classification-review/review-status.json`.

Source: [disc_classification_algorithm_v0_1.pdf](../disc_classification_algorithm_v0_1.pdf), version 0.1, 28 September 2026. All 12 pages were reviewed. An independent arithmetic check reproduced all ten example scores and beginner roles on page 8. That confirms arithmetic consistency, not real-world recommendation accuracy. The PDF's 50-row sample is not a catalog-wide coverage measurement.

The backend will own every classification and score. The frontend will display stored values and submit filters; AI search will translate a shopper's intent into those fields. Classification adds no per-product LLM calls or new AI credentials.

The field dictionary and AI-search guidance for the separate frontend project are documented in [disc_classification_frontend_handoff.md](disc_classification_frontend_handoff.md). Frontend endpoints, model configuration, and reference-disc similarity search remain responsibilities of that project.

## Intended data path

```text
Accepted flight records + normalized disc variant weights
    -> DiscModelClassifications (baseline per distinct flight record)
    -> NormalizedDiscClassifications (variant weights, bounds, policy)
    -> v_VariantSnapshot
    -> VariantState
    -> new full Typesense collection
    -> frontend filters and AI search
```

All new BigQuery objects belong in `DiscGolfProducts`. `VariantState` remains the source of the full Typesense release. Keep `VariantChanges` compatible with state/change tracking, but do not add incremental Typesense indexing to the production workflow.

## Decisions required by the existing pipeline

| Issue | Proposed treatment |
| --- | --- |
| Existing flight validation accepts speed 15, glide 0, and turn 2; the PDF supports speed 1-14, glide 1-7, turn -5 to 1, and fade 0-5. | Preserve source values. Apply the narrower limits only to v0.1 classifications and scores. Flag unsupported fields; do not clamp or rewrite them. Independent supported bands can still be computed. |
| `flight_conflict` currently means the accepted Try Discs tuple differs from the retailer tuple. | Retain that audit flag. Add an explicit unresolved-conflict state so an accepted Try Discs match continues to score under the existing source priority. Block complete scores only when no reliable tuple has been selected. Local fallback disagreements must also be visible. |
| The source matcher currently retains only complete tuples. | Retain partial or unsupported records and their provenance for review; preserve usable single-field descriptors from an unambiguous record. Never combine individual numbers from unrelated records to manufacture a complete score. |
| Current weight extraction accepts 100-190 g; the PDF proposes 80-220 g as a broad review range. | Keep the current trusted extraction policy for this release. The classifier can validate the broader PDF domain, but must consume accepted normalized weights and must not revive rejected raw shipping weights. Broadening extraction is a separate policy change. |
| `weight_g` is currently required in Typesense and missing values become zero. Range endpoints stop at `v_VariantSnapshot`. | Make exact weight optional in the new release schema. Propagate range endpoints and weight status. For exact weights, searchable endpoints both equal the exact weight; for ranges, retain the two stated endpoints; for missing/invalid weights, omit them. |
| Category flags can all be false for a known disc. | Classify records identified as discs independently of the four subtype flags. Keep a sourced category separate from the speed-derived `power_band`; speed 10 must not automatically become distance driver. Unknown categories remain explicit and auditable. |
| The PDF only specifies `data_status = complete` and examples of reason codes. | Define status vocabulary before implementation: `complete`, `partial_flight`, `missing_flight`, `unsupported_flight`, `unresolved_conflict`, and `not_applicable`. Use `weight_status` separately for `exact`, `range`, `missing`, and `invalid`. Here, complete refers to supported, resolved flight inputs; exact weight is optional. Preserve all applicable reasons even when one primary status is selected. |
| The prose mentions a stronger finish as a conditional use, but the formal conditional gate requires fade <= 2. | Follow the formal rule order and gates exactly. Do not silently expand conditional eligibility beyond the numeric rule. |

## 1. Implement the deterministic classification rules

Add `disc_golf_pipeline/services/disc_classification.py` with the versioned BigQuery SQL transform. Keep production arithmetic in one implementation, following the repository's existing SQL-based normalization approach. A small independent reference implementation in tests will check the published examples and SQL results.

- Implement every descriptor boundary and the six stability cases on page 3 exactly.
- Implement the speed-dependent blend, four component contributions, and `access_model` on page 4.
- Implement exact-weight adjustment and missing/range bounds on page 5. A missing or ranged weight leaves `access_variant` null, including slow discs whose two bounds happen to coincide.
- Implement the ordered beginner policy on page 6. Use the lower bound for a general candidate and the upper bound for a conditional candidate.
- Preserve decimals, reject booleans as numeric inputs, and handle null, NaN, infinity, and invalid strings deliberately. A genuine zero remains valid where the specified domain permits it.
- Keep all computation precision. Round only display/report values using the PDF's half-up convention.
- Store components, input hash, rule version, and reason codes for diagnosis. Do not multiply scores by extraction confidence.

## 2. Create the backend storage layer

Build `DiscModelClassifications` as a table with one baseline per distinct accepted rating record. Its key must include normalized identity, full flight tuple, and source scope; manufacturer and model alone must not merge different reliable plastic/run ratings. When identity or scope is unknown, retain the source-record identity rather than grouping unrelated unknown records.

Create `NormalizedDiscClassifications` as a view joining the baseline to normalized disc variants and their accepted exact/ranged weights. Compute the variant adjustment, bounds, and policy there so current cached weight reviews are reflected. Keep the existing normalized ID and `source_variant_key`; no new product identity scheme is needed.

Retain the original flight tuple, provenance, Try Discs attribution, accepted-source decision, and disagreements. Source prose such as reported stability or intended use remains separate from rule outputs. Any future manual override must preserve the computed result and include scope, reason, reviewer, date, and version; v0.1 will not automatically infer overrides or skill claims from prose.

## 3. Carry the complete contract to Typesense

| Fields | Purpose and representation |
| --- | --- |
| `power_band`, `turn_band`, `fade_band`, `glide_band`, `approx_stability` | Optional string facets for filters and explanations. |
| `access_model`, `access_variant`, `access_low`, `access_high` | Optional numeric fields, sortable. Missing values remain absent/null. |
| `beginner_role` | String facet: general candidate, conditional candidate, not default, or unknown, using the exact snake_case enum values in the PDF. |
| `has_exact_weight`, `weight_status`, `weight_min_g`, `weight_max_g` | Exact/range/missing handling and strict variant weight filtering. |
| `disc_category`, `category_source`, `category_evidence` | Optional category based on explicit sourced evidence or unambiguous existing type fields; never inferred solely from speed. |
| `algorithm_version`, `data_status`, `reason_codes` | Version, calculation status, and explanation codes; reason codes are a string array. |
| `flight_conflict`, `flight_conflict_unresolved` | Preserve source disagreement separately from a conflict that actually prevents selecting a tuple. |
| Existing flight, weight, source and evidence fields | Preserve raw accepted facts separately from calculated values. |

Use `algorithm_version = disc-classification-0.1`. Non-disc products receive no disc scores; expose `not_applicable` rather than making them look like unscored discs. A separate beginner boolean is unnecessary because the richer role is available.

Extend the snapshot, schema migration, state projection, row hash, and compatible changes projection together. Add fields to the full-release query and document builder. Explicitly specify schema options, including optional and sort behavior, so server defaults cannot cause the release schema validator to disagree.

Use a new Typesense release schema version and the existing full-collection build/validate/alias workflow. Validate actual field presence and representative document values against BigQuery as well as document counts. This must catch a field disappearing between normalization and indexing.

## 4. Integrate unattended pipeline execution

Refresh the baseline and variant classification view after accepted flight/weight normalization, including the final normalization pass after LLM review, and before `VariantState` is rebuilt. `python main.py run-all-ingestion` remains the complete unattended command.

Add a standalone `classify-discs` command to refresh classifications from existing normalized data. It should not scrape stores, refetch the API, run LLM review, or publish Typesense. Add a separate report command for reviewing the stored results.

Structural failures such as duplicate variant joins, broken field propagation, or SQL errors must stop publication. Missing or unsupported flight data is a normal data status, not a reason to discard products or fail the entire run.

## 5. Define the frontend and AI-search contract

Provide a field dictionary, allowed values, example queries, and prompt guidance alongside the backend changes. Typesense natural-language search can translate requests into `q`, `filter_by`, and `sort_by` and supports additional system-prompt instructions; actual integration must use the deployed server's supported version. See [the official Typesense documentation](https://typesense.org/docs/29.0/api/natural-language-search.html).

- Understable: filter the two understable stability classes; do not automatically include `turn_and_fade`.
- Beginner fairway: honor the independently sourced category and prioritize `general_candidate`; any expansion to conditional candidates must be explicit.
- Straight: use the documented low-turn/low-fade numeric constraints, with a suitable category/speed context and no promise of a straight shot.
- Strict shopper weight interval: require both stored endpoints to fit inside it. Overlap is a separate possible-match mode. Missing weights never satisfy the constraint.
- Apply price, stock, and weight constraints to the same variant. Group only matching variants and display their matching minimum price. Retain interpreted parameters across pagination.
- AI must not recalculate access scores, invent missing flight values, turn scores into probabilities, or assign exclusive beginner/intermediate/advanced skill labels.

The similarity formula on page 9 is relative to a selected reference mold, so it is not another fixed per-disc field. Specify a deterministic backend helper with the speed window, requested category, directional constraints, and weighted distance. Full shopper constraints must be applied before selecting/grouping candidates. Connecting that helper to a search endpoint is a separate integration task because this repository contains ingestion/indexing, not the frontend/search application. Avoid adding a large all-pairs neighbor table to the initial classification change.

## 6. Test, audit, and roll out

Add focused tests for all ten published examples; every exact boundary; partial inputs; non-finite/boolean values; supported-domain limits; unresolved versus resolved source disagreement; exact, ranged, rejected, and missing weights; and missing category flags. Check the weight caps, bound containment, saturation, stability separation, and monotonic properties with a fixed reproducible input set.

Validate generated SQL in a temporary/staging BigQuery dataset against the independent example fixtures, then verify the complete path through a new test Typesense collection. Test removal of formerly valid values as well as additions so stale classifications cannot survive a rebuild. Keep existing normalization, weight, and ingestion tests passing.

Produce a searchable HTML audit with counts by model, brand, store, status, profile, and beginner role. Include inputs, evidence, components, scores/bounds, reasons, source disagreements, unsupported domains, missing categories, and cases near the 55/70 cutoffs. Report coverage at both model and variant levels. Provide a SELECT query for the complete proposed Typesense source rows.

Follow the PDF's rollout recommendation: make descriptive fields ready for normal filtering, and calculate access scores and beginner roles in review mode first. Use a separate review collection or search policy that does not expose unreviewed scores as default public recommendations. Evaluate approximately 30-50 independent molds, reserve whole molds for evaluation, and inspect sensitivity to the proposed coefficient/cutoff changes before enabling default beginner recommendations.

Implemented review commands: `classify-discs`, `start-classify-discs-job`, `review-disc-classifications`, `start-disc-classification-review-job`, `disc-classification-job-status`, and `generate-disc-classification-report`. The isolated review copies current normalized inputs and caches, builds classifications and the proposed search snapshot, and writes `output/classification-review/report.html` with full coverage counts and representative evidence samples. It makes no new API/LLM calls and does not change the active collection or production tables. Review tables expire after seven days; downloaded HTML and summaries persist locally. Publishing remains a distinct rollout step.
