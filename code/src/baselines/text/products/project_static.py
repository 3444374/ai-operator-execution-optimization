"""Project frozen-best static-K AI_COMPLETE arm (the paper method, run via profiler).

This is the ``project_static`` arm of the SQuAD database-E2E runner. Unlike the
``duckdb_ai`` and ``direct_client`` arms (in-process adapters that take a
``tuple[ChatRequest, ...]`` and return ``BaselineRequestResult``), this arm is a
THIN WRAPPER that subprocess-calls ``postgres_ai_operator_profile.py`` with the
project's frozen-best static-K configuration. The profiler owns the full chain
end-to-end -- PG scan of the workload -> Daft token-budget organizer -> Ray actor
executor -> static per-endpoint K admission -> vLLM -> unified sink
(``document_completions``). The wrapper does NOT scan or sink; it only invokes,
then assembles evidence. It is connection-free: all per-doc evidence comes from
profiler output files.

Why shell out (codex ruling + codebase precedent): the gate runner deliberately
BLOCKS an inline ``project_profiler`` ("requires_existing_project_profiler") rather
than approximate the project's execution inline. The project's frozen-best static
method IS the profiler path; re-implementing its Ray init + actor pool + organizer
+ scheduler inline would risk diverging from the real method under test. So
``run_project_static`` runs the real profiler.

Effective K (contract): the profiler's per-endpoint in-flight ceiling is
``min(max_inflight, actor_workers_per_endpoint * ray_actor_max_concurrency)``. The
config REQUIRES ``actor_workers_per_endpoint * ray_actor_max_concurrency >=
max_inflight`` so the declared K is the EFFECTIVE K (no silent clamp) -- the argv
records both and the run surfaces ``effective_k``.

Request semantics (frozen for parity with the direct arm): ``temperature=0``,
``http_transport=httpx_async`` (default urllib would break request parity), and the
``service-prefix-caching`` label. The cryptographically pinned request-set manifest
guard is a 2-endpoint pinned-comparison mechanism (it requires endpoint_count >= 2)
and is NOT applicable to this single-endpoint arm; workload integrity is verified
by the runner via an independent content hash of the scanned workload.

Evidence (non-circular readback): the profiler emits a run-scoped completion
evidence CSV (``--completion-evidence-output``) carrying per-doc ``output_text``
flattened from in-process ``operator_results`` -- INDEPENDENT of the
``document_completions`` sink. The wrapper builds ``BaselineRequestResult`` +
``sunk_pairs`` from this file; the runner then compares ``sunk_pairs`` against
``document_completions`` (two independent sources) so a stale residual row with
the same doc_id is caught instead of self-proving.

Timing: segment timing lives in the profiler ``--output`` CSV row (one formal row).
``database_e2e_wall_s`` is the profiler's ``e2e_s`` -- which is a STRICTLY BROADER
boundary than the in-process arms' runner-measured wall (it includes the post-loop
vLLM metrics scrape + trace-CSV IO + finish_job, and excludes actor-ready/Ray-init
which the in-process arms do not perform). It is therefore NOT directly comparable
to the in-process arms' ``database_e2e_wall_s``; the report records this asymmetry
and also surfaces ``operator_wall_s`` (the tighter adapter-equivalent span) +
``wrapper_wall_s`` (the subprocess wall). Cross-arm absolute-wall comparison
requires a future unified boundary.

Success detection: subprocess exit 0 AND a formal CSV row with ``status=="ok"``.
Per-doc failures become individual ``BaselineRequestResult(status="failed")``; they
do NOT fail the whole run.
"""

from __future__ import annotations

import csv
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from src.baselines.common.contracts import BaselineRequestResult


@dataclass(frozen=True)
class ProjectStaticConfig:
    """Frozen project static-K pipeline config (paper method, run via profiler).

    ``token_budget``, ``max_inflight``, ``actor_workers_per_endpoint``, and
    ``ray_actor_max_concurrency`` are the frozen text-track static values. They are
    REQUIRED (no default) so a caller can never silently run an un-frozen guess.
    ``actor_workers_per_endpoint * ray_actor_max_concurrency`` must be >=
    ``max_inflight`` so the declared K is the EFFECTIVE K (the profiler would
    otherwise silently clamp per-endpoint inflight to the actor-pool slot count).
    """

    database_url: str
    workload_name: str
    endpoint_url: str
    model: str
    max_tokens: int
    token_budget: int
    max_inflight: int
    actor_workers_per_endpoint: int
    ray_actor_max_concurrency: int
    api_key: str = "EMPTY"
    writeback_mode: str = "json_text"
    write_batch_rows: int = 500
    total_rows: int = 0
    ray_batch_rows: int = 64
    request_timeout_s: float = 120.0
    completion_temperature: float = 0.0
    completion_http_transport: str = "httpx_async"
    service_prefix_caching: str = "enabled"
    python_executable: str = ""
    profiler_script: str = (
        "code/scripts/profiling/postgres_ai_operator_profile.py"
    )
    scenario_id: str = "project_static"

    def __post_init__(self) -> None:
        if not self.database_url:
            raise ValueError("database_url must be non-empty")
        if not self.workload_name:
            raise ValueError("workload_name must be non-empty")
        if not self.endpoint_url:
            raise ValueError("endpoint_url must be non-empty")
        if not self.model:
            raise ValueError("model must be non-empty")
        if self.max_tokens <= 0:
            raise ValueError("max_tokens must be positive")
        if self.token_budget <= 0:
            raise ValueError("token_budget must be positive (frozen static value)")
        if self.max_inflight <= 0:
            raise ValueError("max_inflight must be positive (frozen per-endpoint K)")
        if self.actor_workers_per_endpoint <= 0:
            raise ValueError("actor_workers_per_endpoint must be positive")
        if self.ray_actor_max_concurrency <= 0:
            raise ValueError("ray_actor_max_concurrency must be positive")
        slots = self.actor_workers_per_endpoint * self.ray_actor_max_concurrency
        if slots < self.max_inflight:
            raise ValueError(
                f"actor_workers_per_endpoint * ray_actor_max_concurrency "
                f"({slots}) < max_inflight ({self.max_inflight}): the profiler "
                f"would silently clamp effective per-endpoint K to {slots}. Raise "
                f"the actor topology so effective K == declared K."
            )
        if self.writeback_mode not in ("none", "json_text"):
            raise ValueError("project_static writeback_mode must be none or json_text")
        if self.completion_http_transport not in ("urllib", "httpx_async"):
            raise ValueError("completion_http_transport must be urllib or httpx_async")
        if self.service_prefix_caching not in ("enabled", "disabled", "unknown"):
            raise ValueError("service_prefix_caching must be enabled/disabled/unknown")
        if not self.scenario_id:
            raise ValueError("scenario_id must be non-empty (profiler requires it)")

    @property
    def effective_k(self) -> int:
        """The profiler's actual per-endpoint in-flight ceiling."""

        return min(
            self.max_inflight,
            self.actor_workers_per_endpoint * self.ray_actor_max_concurrency,
        )


@dataclass(frozen=True)
class ProjectStaticRun:
    """What ``run_project_static`` returns to the runner for report assembly."""

    results: tuple[BaselineRequestResult, ...]
    sunk_pairs: tuple[tuple[int, str], ...]
    timing: dict[str, float]
    effective_k: int
    actor_workers_per_endpoint: int
    ray_actor_max_concurrency: int
    exit_code: int
    formal_row_found: bool
    stderr_tail: str


def _to_float(value: str) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: str, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def build_profiler_argv(
    config: ProjectStaticConfig,
    trace_path: str,
    evidence_path: str,
    summary_path: str,
) -> list[str]:
    """The frozen static-K argv for ``postgres_ai_operator_profile.py``.

    Pure (no I/O) so a unit test can assert the exact flags + that effective K is
    self-consistent. Single formal run (``--run-phase formal --run-repeat-index 1``)
    emits exactly one formal row. ``--request-trace-output`` is required to populate
    the lifecycle events the completion-evidence emit joins on.
    """

    argv = [
        "--operator", "ai_complete",
        "--database-url", config.database_url,
        "--source-workload-name", config.workload_name,
        "--source-order", "doc_id",
        "--data-source", "daft_postgres",
        "--organizer", "daft",
        "--model-backend", "compatible_http",
        "--completion-endpoint-url", config.endpoint_url,
        "--completion-model", config.model,
        "--completion-api-key", config.api_key,
        "--completion-max-tokens", str(config.max_tokens),
        "--completion-protocol", "chat_completions",
        "--completion-request-timeout-s", str(config.request_timeout_s),
        "--completion-temperature", str(config.completion_temperature),
        "--completion-http-transport", config.completion_http_transport,
        "--service-prefix-caching", config.service_prefix_caching,
        "--batching-policy", "token_budget",
        "--token-budget", str(config.token_budget),
        "--token-budget-policy", "static",
        "--ray-batch-rows", str(config.ray_batch_rows),
        "--scheduling-policy", "static",
        "--admission-scope", "per_endpoint",
        "--max-inflight", str(config.max_inflight),
        "--actor-workers-per-endpoint", str(config.actor_workers_per_endpoint),
        "--ray-actor-max-concurrency", str(config.ray_actor_max_concurrency),
        "--executor", "ray_actor",
        "--submission-granularity", "request",
        "--writeback-mode", config.writeback_mode,
        "--write-batch-rows", str(config.write_batch_rows),
        "--run-phase", "formal",
        "--run-repeat-index", "1",
        "--warmup-runs", "0",
        "--repeats", "1",
        "--scenario-id", config.scenario_id,
        "--experiment-id", "project_static_squad",
        "--request-trace-output", trace_path,
        "--completion-evidence-output", evidence_path,
        "--output", summary_path,
    ]
    if config.total_rows > 0:
        argv.extend(["--total-rows", str(config.total_rows)])
    return argv


def read_completion_evidence(path: Path) -> dict[int, dict]:
    """Read the completion-evidence CSV -> ``{doc_id: row}``.

    Pure (file read). The evidence file is the single per-doc source: it carries
    output_text (independent of the sink) plus the timestamps/status/finish_reason/
    tokens the wrapper needs to build ``BaselineRequestResult``.
    """

    by_doc: dict[int, dict] = {}
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            doc_id = _to_int(row.get("doc_id", ""), default=-1)
            if doc_id < 0:
                continue
            by_doc[doc_id] = row
    return by_doc


def read_summary_timing(path: Path, scenario_id: str) -> tuple[dict, bool]:
    """Find the formal ``status=='ok'`` row in the profiler --output CSV.

    Returns ``(timing_fields, formal_row_found)``. The profiler appends exactly
    one ``status=='ok'``/``phase=='formal'`` row per invocation (a dry_run
    preflight pass runs for schema validation but its row is discarded, never
    appended). ``run_project_static`` clears the work dir per invocation so the
    CSV holds exactly one row; this returns the LAST matching formal-ok row as
    defense-in-depth against any future append behavior on a reused dir.
    """

    timing_fields = (
        "e2e_s", "db_fetch_s", "arrow_build_s", "source_fetch_s",
        "organizer_from_arrow_s", "organizer_plan_s", "organizer_collect_s",
        "submit_s", "model_service_s", "model_request_wall_s", "operator_wall_s",
        "bounded_wait_s", "fanin_s", "writeback_s",
    )
    found: dict = {}
    found_row = False
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("status") != "ok" or row.get("phase") != "formal":
                continue
            if row.get("scenario_id", "") != scenario_id:
                continue
            found = {k: _to_float(row.get(k, "")) for k in timing_fields}
            found_row = True
    return (found, found_row)


def merge_results(evidence_by_doc: dict[int, dict]) -> tuple[BaselineRequestResult, ...]:
    """Build ``BaselineRequestResult`` from the completion-evidence rows.

    Pure. ``started_at_s`` falls back to ``completed`` for failed rows (the
    evidence leaves ``service_start_epoch_s`` empty when a submission never
    reached the service) so a failed row's queue time does not pull down
    ``min(started)`` and overstate the runner's operator span. ``endpoint_index``
    is 0 (this is a single-endpoint arm).
    """

    results: list[BaselineRequestResult] = []
    for doc_id, row in sorted(evidence_by_doc.items()):
        submitted = _to_float(row.get("submit_epoch_s", ""))
        completed = _to_float(row.get("completion_epoch_s", "")) or submitted
        started = _to_float(row.get("service_start_epoch_s", "")) or completed
        status = row.get("status") or "failed"
        output_text = row.get("output_text")
        results.append(BaselineRequestResult(
            doc_id=doc_id,
            endpoint_index=0,
            status="completed" if status == "completed" else "failed",
            error=(row.get("error_type") or None),
            submitted_at_s=submitted,
            started_at_s=started,
            completed_at_s=completed,
            input_tokens=_to_int(row.get("prompt_tokens", "")),
            output_tokens=_to_int(row.get("output_tokens", "")),
            output_text=(output_text if output_text else None),
            finish_reason=(row.get("finish_reason") or None),
        ))
    return tuple(results)


def run_project_static(
    config: ProjectStaticConfig, work_dir: Path,
) -> ProjectStaticRun:
    """Invoke the profiler (frozen static-K) and assemble BaselineRequestResult.

    Connection-free: all per-doc evidence comes from the profiler's completion
    -evidence CSV (output_text, independent of the sink) + the summary CSV (timing).
    ``work_dir`` is CLEARED per invocation so a ``--force`` re-run can never merge
    fresh results with stale append-mode profiler output. Raises (returns a failed
    ``ProjectStaticRun``) if the profiler exits non-zero or no formal ok row was
    produced.
    """

    shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    trace_path = work_dir / "project_static_request_trace.csv"
    evidence_path = work_dir / "project_static_completion_evidence.csv"
    summary_path = work_dir / "project_static_summary.csv"

    python = config.python_executable or "python"
    cmd = [python, config.profiler_script] + build_profiler_argv(
        config, str(trace_path), str(evidence_path), str(summary_path),
    )
    completed = subprocess.run(cmd, capture_output=True, text=True)
    stderr_tail = (completed.stderr or "")[-1600:]

    def _failed(timing: dict, formal_ok: bool, note: str = "") -> ProjectStaticRun:
        return ProjectStaticRun(
            results=(), sunk_pairs=(), timing=timing, formal_row_found=formal_ok,
            effective_k=config.effective_k,
            actor_workers_per_endpoint=config.actor_workers_per_endpoint,
            ray_actor_max_concurrency=config.ray_actor_max_concurrency,
            exit_code=completed.returncode,
            stderr_tail=stderr_tail + (("\n" + note) if note else ""),
        )

    if completed.returncode != 0:
        return _failed({}, False)
    if not evidence_path.exists() or not summary_path.exists():
        return _failed({}, False, "profiler did not emit evidence/summary files")

    timing, formal_ok = read_summary_timing(summary_path, config.scenario_id)
    if not formal_ok:
        return _failed(timing, False, "no formal status==ok row in profiler summary")

    evidence_by_doc = read_completion_evidence(evidence_path)
    results = merge_results(evidence_by_doc)
    sunk_pairs = tuple(
        (doc_id, (row.get("output_text") or ""))
        for doc_id, row in sorted(evidence_by_doc.items())
    )
    return ProjectStaticRun(
        results=results, sunk_pairs=sunk_pairs, timing=timing, formal_row_found=True,
        effective_k=config.effective_k,
        actor_workers_per_endpoint=config.actor_workers_per_endpoint,
        ray_actor_max_concurrency=config.ray_actor_max_concurrency,
        exit_code=completed.returncode, stderr_tail=stderr_tail,
    )
