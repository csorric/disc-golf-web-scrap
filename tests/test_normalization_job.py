import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from disc_golf_pipeline.services.normalization_job import (
    STATUS_FILE,
    get_job_status,
    get_latest_job_directory,
    run_worker,
    start_llm_review_audit_job,
    start_llm_promotion_job,
    start_pipeline_job,
    tail_file,
    write_json_atomic,
    write_text_atomic,
)


class NormalizationJobTests(unittest.TestCase):
    def test_atomic_status_and_latest_job_files_round_trip(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            jobs_directory = Path(temporary_directory)
            job_directory = jobs_directory / "job-1"
            write_json_atomic(job_directory / STATUS_FILE, {"job_id": "job-1", "state": "queued"})
            write_text_atomic(jobs_directory / "latest-job.txt", "job-1\n")

            self.assertEqual(job_directory, get_latest_job_directory(jobs_directory))
            status = get_job_status(jobs_directory=jobs_directory)
            self.assertEqual("job-1", status["job_id"])
            self.assertEqual("queued", status["state"])

    def test_worker_records_success_and_captures_output(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            job_directory = Path(temporary_directory) / "job-1"
            write_json_atomic(
                job_directory / STATUS_FILE,
                {
                    "job_id": "job-1",
                    "state": "queued",
                    "stdout_file": str(job_directory / "stdout.log"),
                    "stderr_file": str(job_directory / "stderr.log"),
                },
            )

            exit_code = run_worker(job_directory, dry_run=True)

            status = json.loads((job_directory / STATUS_FILE).read_text(encoding="utf-8"))
            self.assertEqual(0, exit_code)
            self.assertEqual("succeeded", status["state"])
            self.assertEqual(0, status["exit_code"])
            self.assertIsNotNone(status["started_at"])
            self.assertIsNotNone(status["finished_at"])
            self.assertIn(
                "dry run succeeded",
                (job_directory / "stdout.log").read_text(encoding="utf-8"),
            )

    def test_unknown_pipeline_command_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported pipeline command"):
            start_pipeline_job("delete-everything")

    def test_llm_audit_job_sets_bounded_audit_environment_for_worker(self):
        captured_environment = {}

        def fake_start_pipeline_job(command, jobs_directory, dry_run):
            captured_environment.update(
                {
                    "command": command,
                    "mode": os.environ.get("LLM_RESOLUTION_MODE"),
                    "max_calls": os.environ.get("LLM_MAX_CALLS_PER_RUN"),
                }
            )
            return {"job_id": "job-1", "state": "running"}

        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "disc_golf_pipeline.services.normalization_job.start_pipeline_job",
                side_effect=fake_start_pipeline_job,
            ):
                start_llm_review_audit_job(jobs_directory=Path("jobs"))

            self.assertIsNone(os.environ.get("LLM_RESOLUTION_MODE"))
            self.assertIsNone(os.environ.get("LLM_MAX_CALLS_PER_RUN"))

        self.assertEqual("run-llm-review-audit", captured_environment["command"])
        self.assertEqual("audit", captured_environment["mode"])
        self.assertEqual("10000", captured_environment["max_calls"])

    def test_llm_promotion_uses_supported_detached_pipeline_command(self):
        with patch(
            "disc_golf_pipeline.services.normalization_job.start_pipeline_job",
            return_value={"job_id": "job-2", "state": "running"},
        ) as start_job:
            start_llm_promotion_job(jobs_directory=Path("jobs"))

        start_job.assert_called_once_with(
            "promote-llm-resolutions",
            jobs_directory=Path("jobs"),
            dry_run=False,
        )

    def test_dead_running_worker_is_marked_interrupted(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            job_directory = Path(temporary_directory) / "job-1"
            write_json_atomic(
                job_directory / STATUS_FILE,
                {
                    "job_id": "job-1",
                    "state": "running",
                    "worker_pid": 12345,
                },
            )

            with patch(
                "disc_golf_pipeline.services.normalization_job.process_is_running",
                return_value=False,
            ):
                status = get_job_status(job_directory=job_directory)

            self.assertEqual("interrupted", status["state"])
            self.assertIn("exited before recording", status["error"])

    def test_tail_file_returns_only_requested_lines(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            log_path = Path(temporary_directory) / "worker.log"
            log_path.write_text("one\ntwo\nthree\n", encoding="utf-8")
            self.assertEqual(["two\n", "three\n"], tail_file(log_path, line_count=2))


if __name__ == "__main__":
    unittest.main()
