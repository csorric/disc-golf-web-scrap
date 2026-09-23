import os
import unittest
from unittest.mock import patch

from disc_golf_pipeline.services.llm_resolution import (
    DEFAULT_LLM_CANDIDATE_LIMIT,
    DEFAULT_LLM_MAX_CALLS_PER_RUN,
    DEFAULT_LLM_MODE,
    LlmResolutionConfig,
    build_apply_llm_resolutions_sql,
    build_llm_audit_report_sql,
    build_llm_audit_tables_sql,
    build_llm_batch_input_sql,
    build_llm_generate_sql,
    build_llm_promotion_summary_sql,
    build_llm_queue_sql,
    build_llm_resolution_table_sql,
    build_promote_llm_resolutions_sql,
    build_review_prompt,
    calculate_evidence_hash,
    get_prompt_contract_hash,
    insert_llm_review_rows,
    parse_generation_statistics,
    parse_llm_review_response,
    run_full_ingestion_llm_stage,
)


class LlmResolutionFoundationTests(unittest.TestCase):
    def test_configuration_defaults_to_off_and_bounded_values(self):
        with patch.dict(os.environ, {}, clear=True):
            config = LlmResolutionConfig.from_env()

        self.assertEqual(DEFAULT_LLM_MODE, config.mode)
        self.assertEqual("DiscStandardizationLlm", config.bigquery_model)
        self.assertEqual(DEFAULT_LLM_CANDIDATE_LIMIT, config.candidate_limit)
        self.assertEqual(DEFAULT_LLM_MAX_CALLS_PER_RUN, config.max_calls_per_run)

    def test_configuration_rejects_unknown_mode(self):
        with patch.dict(
            os.environ,
            {"LLM_RESOLUTION_MODE": "unsafe-auto"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "LLM_RESOLUTION_MODE"):
                LlmResolutionConfig.from_env()

    def test_configuration_rejects_candidate_limit_above_five(self):
        with patch.dict(
            os.environ,
            {"LLM_CANDIDATE_LIMIT": "6"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "between 1 and 5"):
                LlmResolutionConfig.from_env()

    def test_evidence_hash_is_stable_for_dictionary_key_order(self):
        first = {
            "review_key": "product:1",
            "title": "Innova Destroyer",
            "candidate_ids": ["entity-1", "entity-2"],
        }
        second = {
            "candidate_ids": ["entity-1", "entity-2"],
            "title": "Innova Destroyer",
            "review_key": "product:1",
        }

        self.assertEqual(
            calculate_evidence_hash(first),
            calculate_evidence_hash(second),
        )

    def test_evidence_hash_changes_when_candidates_change(self):
        first = {"review_key": "product:1", "candidate_ids": ["entity-1"]}
        second = {"review_key": "product:1", "candidate_ids": ["entity-2"]}

        self.assertNotEqual(
            calculate_evidence_hash(first),
            calculate_evidence_hash(second),
        )

    def test_audit_tables_track_runs_reviews_budgets_and_raw_responses(self):
        sql = "\n".join(build_llm_audit_tables_sql("project", "dataset"))

        self.assertIn("DiscModelLlmReviews", sql)
        self.assertIn("DiscModelLlmRuns", sql)
        self.assertIn("evidence_hash STRING NOT NULL", sql)
        self.assertIn("candidate_ids ARRAY<STRING>", sql)
        self.assertIn("raw_response STRING", sql)
        self.assertIn("lock_expires_at TIMESTAMP", sql)
        self.assertIn("max_calls INT64 NOT NULL", sql)
        self.assertIn("prompt_hash STRING", sql)
        self.assertIn("audit_stratum STRING", sql)

    def test_prompt_contract_hash_changes_with_prompt_instructions(self):
        original_hash = get_prompt_contract_hash()

        with patch(
            "disc_golf_pipeline.services.llm_resolution.LLM_PROMPT_INSTRUCTIONS",
            "changed prompt contract",
        ):
            changed_hash = get_prompt_contract_hash()

        self.assertNotEqual(original_hash, changed_hash)

    def test_queue_contains_only_possible_decisions_and_supplied_candidates(self):
        config = LlmResolutionConfig(
            mode="off",
            model_name="gemini-test",
            bigquery_model="DiscStandardizationLlm",
            prompt_version="prompt-test",
            candidate_limit=3,
            max_calls_per_run=100,
        )

        sql = build_llm_queue_sql("project", "dataset", config)

        self.assertEqual(2, sql.count("decision.decision_bucket = 'POSSIBLE'"))
        self.assertEqual(2, sql.count("candidate.is_credible"))
        self.assertIn("LIMIT 3", sql)
        self.assertIn("candidate.disc_entity_id AS disc_entity_id", sql)
        self.assertIn("TO_HEX(SHA256", sql)
        self.assertIn("review.status IN ('ACCEPT', 'NONE')", sql)
        self.assertIn("'CACHED'", sql)
        self.assertIn("'PENDING'", sql)
        self.assertIn("gemini-test", sql)
        self.assertIn("prompt-test", sql)
        self.assertIn("review.prompt_hash = hashed.prompt_hash", sql)

    def test_prompt_contains_only_supplied_candidate_contract(self):
        prompt = build_review_prompt(
            {
                "review_key": "product:1",
                "decision_level": "product",
                "title": "Innova Destroyer",
                "raw_vendor": "Innova",
                "candidates": [
                    {
                        "disc_entity_id": "entity-1",
                        "manufacturer": "Innova Champion Discs",
                        "model": "Destroyer",
                        "candidate_rank": 1,
                    }
                ],
            }
        )

        self.assertIn("one supplied disc_entity_id or the literal string NONE", prompt)
        self.assertIn("untrusted catalog data", prompt)
        self.assertIn('"disc_entity_id":"entity-1"', prompt)
        self.assertIn('"model":"Destroyer"', prompt)

    def test_response_accepts_only_a_supplied_entity_id(self):
        parsed = parse_llm_review_response(
            '{"selected_entity_id":"entity-1","reason":"Exact title match"}',
            ["entity-1", "entity-2"],
        )

        self.assertEqual("ACCEPT", parsed["status"])
        self.assertEqual("entity-1", parsed["selected_entity_id"])

    def test_response_accepts_none(self):
        parsed = parse_llm_review_response(
            '{"selected_entity_id":"NONE","reason":"Ambiguous"}',
            ["entity-1"],
        )

        self.assertEqual("NONE", parsed["status"])
        self.assertIsNone(parsed["selected_entity_id"])

    def test_response_rejects_an_invented_entity_id(self):
        parsed = parse_llm_review_response(
            '{"selected_entity_id":"invented","reason":"Guess"}',
            ["entity-1"],
        )

        self.assertEqual("INVALID", parsed["status"])
        self.assertIn("was not supplied", parsed["error_message"])

    def test_response_rejects_extra_free_form_fields(self):
        parsed = parse_llm_review_response(
            '{"selected_entity_id":"entity-1","reason":"Match","model":"Invented"}',
            ["entity-1"],
        )

        self.assertEqual("INVALID", parsed["status"])
        self.assertIn("Unexpected response keys", parsed["error_message"])

    def test_batch_sql_caps_staged_rows_before_remote_inference(self):
        config = LlmResolutionConfig(
            mode="audit",
            model_name="gemini-test",
            bigquery_model="DiscStandardizationLlm",
            prompt_version="prompt-test",
            candidate_limit=3,
            max_calls_per_run=5,
        )

        batch_sql = build_llm_batch_input_sql("project", "dataset")
        generate_sql = build_llm_generate_sql("project", "dataset", config)

        self.assertIn("CREATE OR REPLACE TABLE", batch_sql)
        self.assertIn("WHERE queue_status = 'PENDING'", batch_sql)
        self.assertIn("LIMIT @batch_limit", batch_sql)
        self.assertIn("PARTITION BY audit_stratum", batch_sql)
        self.assertIn("'variant'", batch_sql)
        self.assertIn("'multi_candidate'", batch_sql)
        self.assertIn("'generic_alias'", batch_sql)
        self.assertIn("'parenthetical_alias'", batch_sql)
        self.assertIn("'missing_manufacturer'", batch_sql)
        self.assertIn("ORDER BY stratum_rank, audit_stratum", batch_sql)
        self.assertIn("AI.GENERATE_TEXT", generate_sql)
        self.assertIn("DiscModelLlmBatchInput", generate_sql)
        self.assertIn('"response_mime_type":"application/json"', generate_sql)
        self.assertIn('"thinking_budget":0', generate_sql)

    def test_audit_report_covers_contracts_strata_runs_and_tokens(self):
        sql_by_section = build_llm_audit_report_sql("project", "dataset")

        self.assertEqual({"contracts", "strata", "runs"}, set(sql_by_section))
        self.assertIn("prompt_hash", sql_by_section["contracts"])
        self.assertIn("SUM(input_tokens)", sql_by_section["contracts"])
        self.assertIn("audit_stratum", sql_by_section["strata"])
        self.assertIn("LIMIT 20", sql_by_section["runs"])

    def test_promotion_is_versioned_and_revalidates_current_candidates(self):
        config = LlmResolutionConfig(
            mode="promote",
            model_name="gemini-test",
            bigquery_model="DiscStandardizationLlm",
            prompt_version="prompt-test",
            candidate_limit=3,
            max_calls_per_run=100,
        )

        ddl = build_llm_resolution_table_sql("project", "dataset")
        promote_sql = build_promote_llm_resolutions_sql(
            "project", "dataset", config
        )
        apply_sql = build_apply_llm_resolutions_sql(
            "project", "dataset", config
        )
        summary_sql = build_llm_promotion_summary_sql(
            "project", "dataset", config
        )

        self.assertIn("DiscModelLlmResolutions", ddl)
        self.assertIn("promotion_policy_version STRING NOT NULL", ddl)
        self.assertIn("review.selected_entity_id = candidate.disc_entity_id", promote_sql)
        self.assertIn("review.evidence_hash = queue.evidence_hash", promote_sql)
        self.assertIn("candidate.is_context_only = FALSE", promote_sql)
        self.assertIn("decision.decision_bucket != 'ACCEPT'", apply_sql)
        self.assertIn("llm_v2_product_candidate", promote_sql)
        self.assertIn("llm_v2_variant_candidate", promote_sql)
        self.assertIn("AS unapplied", summary_sql)

    def test_generation_statistics_maps_prompt_and_candidate_tokens(self):
        counts = parse_generation_statistics(
            {
                "prompt_token_count": 322,
                "candidates_token_count": 93,
                "total_token_count": 415,
            }
        )

        self.assertEqual(322, counts["input_tokens"])
        self.assertEqual(93, counts["output_tokens"])

    def test_review_rows_are_inserted_in_bounded_batches(self):
        class FakeClient:
            def __init__(self):
                self.batches = []

            def insert_rows_json(self, table, rows):
                self.batches.append((table, list(rows)))
                return []

        client = FakeClient()
        rows = [{"review_id": str(index)} for index in range(5)]

        insert_llm_review_rows(client, "project.dataset.reviews", rows, batch_size=2)

        self.assertEqual([2, 2, 1], [len(batch) for _, batch in client.batches])

    def test_review_insert_stops_on_a_failed_batch(self):
        class FakeClient:
            def __init__(self):
                self.calls = 0

            def insert_rows_json(self, table, rows):
                self.calls += 1
                return [] if self.calls == 1 else [{"error": "bad row"}]

        client = FakeClient()

        with self.assertRaisesRegex(RuntimeError, "batch 2"):
            insert_llm_review_rows(
                client,
                "project.dataset.reviews",
                [{"review_id": str(index)} for index in range(5)],
                batch_size=2,
            )

        self.assertEqual(2, client.calls)

    def test_full_ingestion_llm_stage_processes_every_pending_row(self):
        config = LlmResolutionConfig(
            mode="off",
            model_name="gemini-test",
            bigquery_model="DiscStandardizationLlm",
            prompt_version="prompt-test",
            candidate_limit=3,
            max_calls_per_run=100,
        )
        before = {
            "pending": 2,
            "cached": 8,
        }
        after = {
            "pending": 1,
            "cached": 9,
        }
        batch = {
            "run_id": "run-1",
            "status": "SUCCEEDED",
            "attempted": 2,
            "accept": 1,
            "none": 0,
            "invalid": 1,
            "failed": 0,
            "input_tokens": 200,
            "output_tokens": 40,
        }

        with patch(
            "disc_golf_pipeline.services.llm_resolution.prepare_llm_review_queue",
            side_effect=[before, after],
        ), patch(
            "disc_golf_pipeline.services.llm_resolution.run_llm_audit_batch",
            return_value=batch,
        ) as run_batch:
            summary = run_full_ingestion_llm_stage(
                object(),
                "project",
                "dataset",
                config=config,
                max_calls=10,
            )

        run_batch.assert_called_once()
        self.assertEqual(2, summary["attempted"])
        self.assertEqual(1, summary["accepted"])
        self.assertEqual(1, summary["pending_after"])

    def test_full_ingestion_llm_stage_fails_closed_over_budget(self):
        config = LlmResolutionConfig(
            mode="off",
            model_name="gemini-test",
            bigquery_model="DiscStandardizationLlm",
            prompt_version="prompt-test",
            candidate_limit=3,
            max_calls_per_run=100,
        )

        with patch(
            "disc_golf_pipeline.services.llm_resolution.prepare_llm_review_queue",
            return_value={"pending": 11, "cached": 0},
        ), patch(
            "disc_golf_pipeline.services.llm_resolution.run_llm_audit_batch"
        ) as run_batch:
            with self.assertRaisesRegex(RuntimeError, "paid-call cap"):
                run_full_ingestion_llm_stage(
                    object(),
                    "project",
                    "dataset",
                    config=config,
                    max_calls=10,
                )

        run_batch.assert_not_called()

    def test_full_ingestion_llm_stage_blocks_remote_failures(self):
        config = LlmResolutionConfig(
            mode="off",
            model_name="gemini-test",
            bigquery_model="DiscStandardizationLlm",
            prompt_version="prompt-test",
            candidate_limit=3,
            max_calls_per_run=100,
        )
        batch = {
            "run_id": "run-2",
            "status": "SUCCEEDED",
            "attempted": 1,
            "accept": 0,
            "none": 0,
            "invalid": 0,
            "failed": 1,
        }

        with patch(
            "disc_golf_pipeline.services.llm_resolution.prepare_llm_review_queue",
            return_value={"pending": 1, "cached": 0},
        ), patch(
            "disc_golf_pipeline.services.llm_resolution.run_llm_audit_batch",
            return_value=batch,
        ):
            with self.assertRaisesRegex(RuntimeError, "remote failures"):
                run_full_ingestion_llm_stage(
                    object(),
                    "project",
                    "dataset",
                    config=config,
                    max_calls=10,
                )


if __name__ == "__main__":
    unittest.main()
