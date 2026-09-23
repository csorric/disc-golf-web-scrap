import argparse
import ctypes
from ctypes import wintypes
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from disc_golf_pipeline.common.runtime import PROJECT_ROOT


DEFAULT_JOBS_DIRECTORY = PROJECT_ROOT / "output" / "normalization-jobs"
LATEST_JOB_FILE = "latest-job.txt"
STATUS_FILE = "status.json"
STDOUT_FILE = "stdout.log"
STDERR_FILE = "stderr.log"
ACTIVE_STATES = {"queued", "starting", "running"}
SUPPORTED_PIPELINE_COMMANDS = {
    "normalize-data",
    "process-data",
    "backfill-typesense-v5",
    "build-typesense-release",
    "publish-typesense-release",
    "promote-llm-resolutions",
    "run-llm-review-audit",
    "run-all-ingestion",
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_json_atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    temporary_path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary_path, path)


def write_text_atomic(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    temporary_path.write_text(value, encoding="utf-8")
    os.replace(temporary_path, path)


def read_status(job_directory):
    status_path = Path(job_directory) / STATUS_FILE
    if not status_path.exists():
        return None
    return json.loads(status_path.read_text(encoding="utf-8"))


def process_is_running(process_id):
    if not process_id:
        return False

    if os.name == "nt":
        process_query_limited_information = 0x1000
        still_active = 259
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel32.GetExitCodeProcess.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(
            process_query_limited_information,
            False,
            int(process_id),
        )
        if not handle:
            return False
        try:
            exit_code = wintypes.DWORD()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return exit_code.value == still_active
        finally:
            kernel32.CloseHandle(handle)

    try:
        os.kill(int(process_id), 0)
    except (OSError, ValueError):
        return False
    return True


def get_latest_job_directory(jobs_directory=DEFAULT_JOBS_DIRECTORY):
    jobs_directory = Path(jobs_directory)
    latest_path = jobs_directory / LATEST_JOB_FILE
    if latest_path.exists():
        job_id = latest_path.read_text(encoding="utf-8").strip()
        if job_id:
            job_directory = jobs_directory / job_id
            if job_directory.is_dir():
                return job_directory

    if not jobs_directory.exists():
        return None
    job_directories = sorted(
        (path for path in jobs_directory.iterdir() if path.is_dir()),
        reverse=True,
    )
    return job_directories[0] if job_directories else None


def get_job_status(job_directory=None, jobs_directory=DEFAULT_JOBS_DIRECTORY, reconcile=True):
    resolved_job_directory = (
        Path(job_directory)
        if job_directory is not None
        else get_latest_job_directory(jobs_directory)
    )
    if resolved_job_directory is None:
        return None

    status = read_status(resolved_job_directory)
    if status is None:
        return None

    if (
        reconcile
        and status.get("state") in ACTIVE_STATES
        and status.get("worker_pid")
        and not process_is_running(status["worker_pid"])
    ):
        status.update(
            {
                "state": "interrupted",
                "finished_at": utc_now(),
                "exit_code": None,
                "error": "The detached worker exited before recording a final status.",
            }
        )
        write_json_atomic(resolved_job_directory / STATUS_FILE, status)

    status["job_directory"] = str(resolved_job_directory)
    return status


def build_job_id():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{uuid4().hex[:8]}"


def _worker_command(job_directory, dry_run):
    command = [
        sys.executable,
        "-m",
        "disc_golf_pipeline.services.normalization_job",
        "worker",
        "--job-directory",
        str(job_directory),
    ]
    if dry_run:
        command.append("--dry-run")
    return command


def _detached_creation_flags():
    if os.name != "nt":
        return 0
    return subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP


def start_pipeline_job(
    pipeline_command,
    jobs_directory=DEFAULT_JOBS_DIRECTORY,
    dry_run=False,
):
    if pipeline_command not in SUPPORTED_PIPELINE_COMMANDS:
        raise ValueError(f"Unsupported pipeline command: {pipeline_command}")

    jobs_directory = Path(jobs_directory).resolve()
    jobs_directory.mkdir(parents=True, exist_ok=True)

    latest_status = get_job_status(jobs_directory=jobs_directory)
    if latest_status and latest_status.get("state") in ACTIVE_STATES:
        raise RuntimeError(
            "A pipeline job is already active: "
            f"{latest_status['job_id']} (PID {latest_status.get('worker_pid', 'starting')})"
        )

    job_id = build_job_id()
    job_directory = jobs_directory / job_id
    job_directory.mkdir(parents=True, exist_ok=False)
    command = (
        [sys.executable, "-c", "print('Detached normalization job dry run succeeded.')"]
        if dry_run
        else [sys.executable, str(PROJECT_ROOT / "main.py"), pipeline_command]
    )
    status = {
        "job_id": job_id,
        "job_type": "pipeline-dry-run" if dry_run else pipeline_command,
        "state": "queued",
        "queued_at": utc_now(),
        "started_at": None,
        "finished_at": None,
        "worker_pid": None,
        "exit_code": None,
        "command": command,
        "stdout_file": str(job_directory / STDOUT_FILE),
        "stderr_file": str(job_directory / STDERR_FILE),
        "error": None,
    }
    write_json_atomic(job_directory / STATUS_FILE, status)
    write_text_atomic(jobs_directory / LATEST_JOB_FILE, job_id + "\n")

    popen_kwargs = {
        "cwd": str(PROJECT_ROOT),
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "close_fds": True,
        "creationflags": _detached_creation_flags(),
    }
    if os.name != "nt":
        popen_kwargs["start_new_session"] = True

    worker_process = subprocess.Popen(
        _worker_command(job_directory, dry_run),
        **popen_kwargs,
    )

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        current_status = read_status(job_directory)
        if current_status and current_status.get("state") != "queued":
            break
        if worker_process.poll() is not None:
            break
        time.sleep(0.05)

    current_status = read_status(job_directory) or status
    if current_status.get("state") == "queued":
        if worker_process.poll() is not None:
            current_status.update(
                {
                    "state": "failed",
                    "finished_at": utc_now(),
                    "exit_code": worker_process.returncode,
                    "error": "The detached worker failed before it initialized.",
                }
            )
        else:
            current_status.update(
                {
                    "state": "starting",
                    "worker_pid": worker_process.pid,
                }
            )
        write_json_atomic(job_directory / STATUS_FILE, current_status)

    current_status["job_directory"] = str(job_directory)
    return current_status


def start_normalization_job(jobs_directory=DEFAULT_JOBS_DIRECTORY, dry_run=False):
    return start_pipeline_job(
        "normalize-data",
        jobs_directory=jobs_directory,
        dry_run=dry_run,
    )


def start_process_data_job(jobs_directory=DEFAULT_JOBS_DIRECTORY, dry_run=False):
    return start_pipeline_job(
        "process-data",
        jobs_directory=jobs_directory,
        dry_run=dry_run,
    )


def start_typesense_v5_backfill_job(jobs_directory=DEFAULT_JOBS_DIRECTORY, dry_run=False):
    return start_pipeline_job(
        "backfill-typesense-v5",
        jobs_directory=jobs_directory,
        dry_run=dry_run,
    )


def start_typesense_release_job(jobs_directory=DEFAULT_JOBS_DIRECTORY, dry_run=False):
    return start_pipeline_job(
        "build-typesense-release",
        jobs_directory=jobs_directory,
        dry_run=dry_run,
    )


def start_typesense_publish_job(jobs_directory=DEFAULT_JOBS_DIRECTORY, dry_run=False):
    return start_pipeline_job(
        "publish-typesense-release",
        jobs_directory=jobs_directory,
        dry_run=dry_run,
    )


def start_full_ingestion_job(jobs_directory=DEFAULT_JOBS_DIRECTORY, dry_run=False):
    return start_pipeline_job(
        "run-all-ingestion",
        jobs_directory=jobs_directory,
        dry_run=dry_run,
    )


def start_llm_review_audit_job(
    jobs_directory=DEFAULT_JOBS_DIRECTORY,
    dry_run=False,
):
    environment_updates = {
        "LLM_RESOLUTION_MODE": "audit",
        "LLM_MAX_CALLS_PER_RUN": "10000",
    }
    previous_values = {
        name: os.environ.get(name) for name in environment_updates
    }
    try:
        os.environ.update(environment_updates)
        return start_pipeline_job(
            "run-llm-review-audit",
            jobs_directory=jobs_directory,
            dry_run=dry_run,
        )
    finally:
        for name, previous_value in previous_values.items():
            if previous_value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous_value


def start_llm_promotion_job(
    jobs_directory=DEFAULT_JOBS_DIRECTORY,
    dry_run=False,
):
    return start_pipeline_job(
        "promote-llm-resolutions",
        jobs_directory=jobs_directory,
        dry_run=dry_run,
    )


def run_worker(job_directory, dry_run=False):
    job_directory = Path(job_directory).resolve()
    status = read_status(job_directory)
    if status is None:
        raise RuntimeError(f"Missing job status file in {job_directory}")

    stdout_path = job_directory / STDOUT_FILE
    stderr_path = job_directory / STDERR_FILE
    status.update(
        {
            "state": "running",
            "started_at": utc_now(),
            "worker_pid": os.getpid(),
            "error": None,
        }
    )
    write_json_atomic(job_directory / STATUS_FILE, status)

    command = (
        [sys.executable, "-c", "print('Detached normalization job dry run succeeded.')"]
        if dry_run
        else status["command"]
    )
    exit_code = 1
    try:
        with stdout_path.open("a", encoding="utf-8", buffering=1) as stdout_file:
            with stderr_path.open("a", encoding="utf-8", buffering=1) as stderr_file:
                stdout_file.write(
                    f"[{utc_now()}] Starting: {' '.join(command)}\n"
                )
                completed = subprocess.run(
                    command,
                    cwd=PROJECT_ROOT,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    check=False,
                )
                exit_code = completed.returncode
                stdout_file.write(
                    f"[{utc_now()}] Finished with exit code {exit_code}.\n"
                )
        status.update(
            {
                "state": "succeeded" if exit_code == 0 else "failed",
                "finished_at": utc_now(),
                "exit_code": exit_code,
                "error": None if exit_code == 0 else "The pipeline command returned a non-zero exit code.",
            }
        )
    except BaseException as exc:
        status.update(
            {
                "state": "failed",
                "finished_at": utc_now(),
                "exit_code": exit_code,
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        with stderr_path.open("a", encoding="utf-8") as stderr_file:
            traceback.print_exc(file=stderr_file)
    finally:
        write_json_atomic(job_directory / STATUS_FILE, status)

    return exit_code


def tail_file(path, line_count=30):
    path = Path(path)
    if not path.exists() or line_count <= 0:
        return []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return handle.readlines()[-line_count:]


def print_normalization_job_status(
    job_directory=None,
    jobs_directory=DEFAULT_JOBS_DIRECTORY,
    line_count=30,
):
    status = get_job_status(
        job_directory=job_directory,
        jobs_directory=jobs_directory,
    )
    if status is None:
        print("No normalization job has been recorded.")
        return None

    print(
        f"Pipeline job {status['job_id']} ({status.get('job_type', 'normalization')}): "
        f"{status['state']} "
        f"(PID {status.get('worker_pid') or 'not started'})"
    )
    print(f"Started:  {status.get('started_at') or '-'}")
    print(f"Finished: {status.get('finished_at') or '-'}")
    print(f"Exit code: {status.get('exit_code') if status.get('exit_code') is not None else '-'}")
    print(f"Job directory: {status['job_directory']}")
    if status.get("error"):
        print(f"Error: {status['error']}")

    for label, key in (("stdout", "stdout_file"), ("stderr", "stderr_file")):
        lines = tail_file(status[key], line_count=line_count)
        if lines:
            print(f"\nLast {len(lines)} {label} lines:")
            print("".join(lines).rstrip())
    return status


def build_parser():
    parser = argparse.ArgumentParser(description="Detached normalization job helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    start_parser = subparsers.add_parser("start")
    start_parser.add_argument("--dry-run", action="store_true")
    start_parser.add_argument(
        "--pipeline-command",
        choices=sorted(SUPPORTED_PIPELINE_COMMANDS),
        default="normalize-data",
    )
    start_parser.add_argument("--jobs-directory", type=Path, default=DEFAULT_JOBS_DIRECTORY)

    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--job-directory", type=Path)
    status_parser.add_argument("--jobs-directory", type=Path, default=DEFAULT_JOBS_DIRECTORY)
    status_parser.add_argument("--lines", type=int, default=30)

    worker_parser = subparsers.add_parser("worker", help=argparse.SUPPRESS)
    worker_parser.add_argument("--job-directory", type=Path, required=True)
    worker_parser.add_argument("--dry-run", action="store_true")
    return parser


def main():
    args = build_parser().parse_args()
    if args.command == "start":
        status = start_pipeline_job(
            args.pipeline_command,
            jobs_directory=args.jobs_directory,
            dry_run=args.dry_run,
        )
        print(f"Started normalization job {status['job_id']} ({status['state']}).")
        print(f"Job directory: {status['job_directory']}")
        print("Check it with: py main.py normalization-job-status")
        return 0
    if args.command == "status":
        print_normalization_job_status(
            job_directory=args.job_directory,
            jobs_directory=args.jobs_directory,
            line_count=args.lines,
        )
        return 0
    return run_worker(args.job_directory, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
