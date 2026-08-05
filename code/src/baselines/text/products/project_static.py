"""Project frozen-best static-K AI_COMPLETE arm (the paper method, run via profiler).

This is the ``project_static`` arm of the SQuAD database-E2E runner. Unlike the
``duckdb_ai`` and ``direct_client`` arms (in-process adapters that take a
``tuple[ChatRequest, ...]`` and return ``BaselineRequestResult``), this arm is a
THIN WRAPPER that subprocess-calls ``postgres_ai_operator_profile.py`` with the
project's frozen-best static-K configuration. The profiler owns the full chain
end-to-end -- PG scan of the workload -> Daft token-budget organizer -> Ray actor
executor -> static per-endpoint K admission + active-work -> vLLM -> unified sink
(``document_completions``). The wrapper does NOT scan or sink; it only invokes,
then assembles evidence.

Why shell out (codex ruling + codebase precedent): the gate runner deliberately
BLOCKS an inline ``project_profiler`` ("requires_existing_project_profiler") rather
than approximate the project's execution inline. The project's frozen-best static
method IS the profiler path; re-implementing its Ray init + actor pool + organizer
+ scheduler inline would risk diverging from the real method under test. So
``run_project_static`` runs the real profiler.

Evidence merge (BaselineRequestResult has no single profiler file source):
* The request-trace CSV (``--request-trace-output``) carries per-doc timestamps,
  status, error, finish_reason, output tokens -- but NOT output_text.
* ``output_text`` is written only to ``document_completions.completion_text`` by
  the profiler's sink. The wrapper reads it back by ``doc_id`` (the profiler just
  sank it) and merges it with the request-trace row by ``doc_id``.

Timing: segment timing lives in the profiler ``--output`` CSV row (one formal row).
The wrapper surfaces the raw profiler timing fields; the runner maps them to its
report timing block (``e2e_s``->``database_e2e_wall_s`` etc.). Note the
``construct_s`` field has no clean profiler analog (the runner's construct_s is
ChatRequest-object building; the profiler has organizer stages instead) -- the
runner synthesizes/notes this asymmetry, and the primary headline
``correct_rows_per_s`` divides EM rows by the comparable ``e2e_s`` wall.

Success detection: subprocess exit 0 AND a formal CSV row with ``status=="ok"``.
Per-doc failures (status=="failed" in the request trace) become individual
``BaselineRequestResult(status="failed")``; they do NOT fail the whole run.
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

    ``token_budget`` and ``max_inflight`` are the frozen text-track static values
    (sequential token-budget organizer + static per-endpoint K). They are REQUIRED
    (no default) so a caller can never silently run an un-frozen guess.
    """

    database_url: str
    workload_name: str
    endpoint_url: str
    model: str
    max_tokens: int
    token_budget: int
    max_inflight: int
    api_key: str = "EMPTY"
    writeback_mode: str = "json_text"
    write_batch_rows: int = 500
    sink_category: str = "squad"
    total_rows: int = 0
    ray_batch_rows: int = 64
    request_timeout_s: float = 120.0
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
        if self.writeback_mode not in ("none", "json_text"):
            raise ValueError("project_static writeback_mode must be none or json_text")
        if not self.scenario_id:
            raise ValueError("scenario_id must be non-empty (profiler requires it)")


@dataclass(frozen=True)
class ProjectStaticRun:
    """What ``run_project_static`` returns to the runner for report assembly."""

    results: tuple[BaselineRequestResult, ...]
    sunk_pairs: tuple[tuple[int, str], ...]
    timing: dict[str, float]
    exit_code: int
    formal_row_found: bool
    stderr_tail: str


def _to_float(value: str) -> float:
    """Parse a profiler CSV numeric cell that may be empty for failed rows."""

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


def _parse_endpoint_index(value: str) -> int:
    """Profiler request-trace ``endpoint_id`` is ``endpoint-{i}``; parse to int."""

    if not value:
        return 0
    text = value.strip()
    if text.startswith("endpoint-"):
        text = text[len("endpoint-"):]
    return _to_int(text, 0)


def build_profiler_argv(
    config: ProjectStaticConfig, trace_path: str, summary_path: str,
) -> list[str]:
    """The frozen static-K argv for ``postgres_ai_operator_profile.py``.

    Pure (no I/O) so a unit test can assert the exact flags. Single formal run
    (``--run-phase formal --run-repeat-index 1``) emits exactly one formal row.
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
        "--batching-policy", "token_budget",
        "--token-budget", str(config.token_budget),
        "--token-budget-policy", "static",
        "--ray-batch-rows", str(config.ray_batch_rows),
        "--scheduling-policy", "static",
        "--admission-scope", "per_endpoint",
        "--max-inflight", str(config.max_inflight),
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
        "--output", summary_path,
    ]
    if config.total_rows > 0:
        argv.extend(["--total-rows", str(config.total_rows)])
    return argv


def read_request_trace(path: Path) -> dict[int, dict]:
    """Read the request-trace CSV -> ``{doc_id: row}`` (last row wins per doc_id).

    Pure (file read). Returns only the fields the merge needs, keyed by doc_id.
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
            # Empty scenario_id is a non-match (defensive; the wrapper always
            # passes a non-empty scenario_id and the profiler writes it through).
            if row.get("scenario_id", "") != scenario_id:
                continue
            found = {k: _to_float(row.get(k, "")) for k in timing_fields}
            found_row = True
    return (found, found_row)


def read_output_text(conn, doc_ids: list[int], category: str) -> dict[int, str]:
    """Read ``document_completions.completion_text`` back by doc_id.

    The profiler's own sink wrote these rows; this is the only source of per-doc
    ``output_text`` (no profiler file carries it). Failed rows have empty
    completion_text (NULL->"" at sink). If a doc_id appears more than once
    (stale residual from a prior run), the sink readback in the runner catches
    the content-digest mismatch and fails closed.
    """

    if not doc_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT doc_id, completion_text FROM document_completions "
            "WHERE doc_id = ANY(%s) AND category = %s",
            (doc_ids, category),
        )
        fetched = cur.fetchall()
    out: dict[int, str] = {}
    for doc_id, text in fetched:
        out[int(doc_id)] = "" if text is None else str(text)
    return out


def merge_results(
    trace_by_doc: dict[int, dict], output_text_by_doc: dict[int, str],
) -> tuple[BaselineRequestResult, ...]:
    """Merge request-trace rows + output_text -> ``BaselineRequestResult`` tuple.

    Pure. ``started_at_s`` falls back to ``submitted`` for failed rows (the
    profiler leaves ``service_start_epoch_s`` empty when a submission never
    reached the service). ``output_tokens`` falls back to 0 when the backend did
    not return per-choice token counts.
    """

    results: list[BaselineRequestResult] = []
    for doc_id, row in sorted(trace_by_doc.items()):
        submitted = _to_float(row.get("submit_epoch_s", ""))
        completed = _to_float(row.get("completion_epoch_s", "")) or submitted
        # Failed rows have empty service_start_epoch_s (the submission never
        # reached the service). Fall back to ``completed`` (NOT ``submitted``) so
        # a failed row's queue time does not pull down min(started) and overstate
        # the runner's operator span; the row still contributes its completion.
        started = _to_float(row.get("service_start_epoch_s", "")) or completed
        status = row.get("status") or "failed"
        results.append(BaselineRequestResult(
            doc_id=doc_id,
            endpoint_index=_parse_endpoint_index(row.get("endpoint_id", "")),
            status="completed" if status == "completed" else "failed",
            error=(row.get("error_type") or None),
            submitted_at_s=submitted,
            started_at_s=started,
            completed_at_s=completed,
            input_tokens=_to_int(row.get("prompt_tokens", "")),
            output_tokens=_to_int(row.get("actual_output_tokens", "")),
            output_text=output_text_by_doc.get(doc_id),
            finish_reason=(row.get("finish_reason") or None),
        ))
    return tuple(results)


def run_project_static(
    config: ProjectStaticConfig, work_dir: Path, conn,
) -> ProjectStaticRun:
    """Invoke the profiler (frozen static-K) and assemble BaselineRequestResult.

    ``work_dir`` receives the profiler's request-trace + summary CSV (temporary,
    caller-managed) and is CLEARED per invocation so a ``--force`` re-run into the
    same dir can never merge fresh results with stale append-mode profiler output.
    ``conn`` is used ONLY for the post-run ``output_text`` readback from
    ``document_completions`` (the profiler's sink output); the profiler owns scan
    + sink. Raises if the profiler exits non-zero or no formal ok row was produced.
    """

    # Clear stale append-mode output from any prior run into this dir: the
    # profiler's --output/--request-trace CSVs open in append mode, and
    # read_summary_timing/read_request_trace must see ONLY this invocation's rows.
    shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    trace_path = work_dir / "project_static_request_trace.csv"
    summary_path = work_dir / "project_static_summary.csv"

    python = config.python_executable or "python"
    cmd = [python, config.profiler_script] + build_profiler_argv(
        config, str(trace_path), str(summary_path),
    )
    completed = subprocess.run(
        cmd, capture_output=True, text=True,
    )
    stderr_tail = (completed.stderr or "")[-1600:]

    if completed.returncode != 0:
        return ProjectStaticRun(
            results=(), sunk_pairs=(), timing={}, exit_code=completed.returncode,
            formal_row_found=False, stderr_tail=stderr_tail,
        )
    if not trace_path.exists() or not summary_path.exists():
        return ProjectStaticRun(
            results=(), sunk_pairs=(), timing={}, exit_code=completed.returncode,
            formal_row_found=False,
            stderr_tail=stderr_tail + "\nprofiler did not emit trace/summary files",
        )

    timing, formal_ok = read_summary_timing(summary_path, config.scenario_id)
    if not formal_ok:
        return ProjectStaticRun(
            results=(), sunk_pairs=(), timing=timing, exit_code=completed.returncode,
            formal_row_found=False,
            stderr_tail=stderr_tail + "\nno formal status==ok row in profiler summary",
        )

    trace_by_doc = read_request_trace(trace_path)
    doc_ids = sorted(trace_by_doc)
    output_text_by_doc = (
        read_output_text(conn, doc_ids, config.sink_category)
        if config.writeback_mode != "none" else {}
    )
    results = merge_results(trace_by_doc, output_text_by_doc)
    sunk_pairs = tuple(
        (doc_id, output_text_by_doc.get(doc_id, ""))
        for doc_id in doc_ids
    )
    return ProjectStaticRun(
        results=results, sunk_pairs=sunk_pairs, timing=timing,
        exit_code=completed.returncode, formal_row_found=True,
        stderr_tail=stderr_tail,
    )
