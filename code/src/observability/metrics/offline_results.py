"""Bounded, read-only adapters for retained experiment observations.

The adapters describe file layouts, never dataset, model or query names. They
do not execute experiment scripts or infer unrecorded clocks/counters.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import gzip
import hashlib
import io
import json
import math
from pathlib import Path

from .statistics import latency_observations


PUBLIC = "semloom.public_query_observations.v1"
FIFO = "semloom.fifo_summary.v1"
SCHEMAS = (PUBLIC, FIFO)
MANIFEST = "semloom.offline_inputs.v1"
MAX_BYTES = 32 * 1024 * 1024  # Per explicit file, compressed and decoded.


def na(reason, *, status="NA"):
    return dict(status=status, value=None, reason=reason)


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def _count(value):
    if value is not None and (type(value) is not int or value < 0):
        raise ValueError("counts must be nonnegative integers or missing")
    return value


def _sum_known(values):
    values = list(values)
    return sum(values) if values and all(v is not None for v in values) else None


def _empty_successor(q):
    map_calls = (q["stages"] or {}).get("Map")
    result_rows = q["result_rows"]
    if q["status"] != "succeeded" or map_calls is None or result_rows is None:
        return None
    return _count(map_calls) == 0 and _count(result_rows) == 0


def _distribution(rows, *, complete=True):
    # Reuse the existing rank, small-sample and incomplete-observation rules.
    result = latency_observations(rows)
    keys = ("observations", "completed_units", "median_s", "maximum_s",
            "empirical_p99_s", "empirical_p99_rank", "quantile_method",
            "population_P99_established", "p99_unavailable_reason")
    result = {key: result[key] for key in keys}
    if not complete:
        result.update(empirical_p99_s=None, empirical_p99_rank=None,
                      p99_unavailable_reason="incomplete_or_unverified_API_accounting")
    result["status"] = "observed" if result["completed_units"] else "NA"
    result["sample_independence"] = "not established by receipts"
    return result


def _status(q):
    if q.get("status") in ("succeeded", "failed", "unknown", "not_executed"):
        return q["status"]
    if "status" in q:
        raise ValueError("unsupported query status")
    success, failed, unknown, total = (q.get(k) for k in (
        "successful_records", "failed_records", "unknown_records", "records"))
    for value in (success, failed, unknown, total):
        _count(value)
    if None in (success, failed, unknown, total):
        return "unknown"
    if unknown:
        return "unknown"
    if failed:
        return "failed"
    return "succeeded" if success == total else "unknown"


def _api(q, values):
    counts = q.get("API_counts") or {}
    if not isinstance(counts, dict):
        raise ValueError("invalid API accounting: counters must be a mapping")
    for name in ("attempted", "completed", "timed", "successful_timed", "missing_clock", "unresolved_identity_records"):
        _count(counts.get(name))
    statuses = counts.get("statuses")
    if statuses is not None:
        if not isinstance(statuses, dict):
            raise ValueError("invalid API accounting: statuses must be a sparse counter")
        if set(statuses) - {"succeeded", "failed", "unknown"}:
            raise ValueError("unsupported API status; preserve it via a new schema")
        for value in statuses.values():
            _count(value)
    batches = q.get("actual_batch_sizes")
    if batches is not None:
        if any(not str(k).isdigit() or int(k) < 1 for k in batches):
            raise ValueError("batch sizes must be positive integers")
        for value in batches.values():
            _count(value)
        if values is not None and sum(batches.values()) != len(values):
            raise ValueError("API samples and actual batch histogram disagree")
        records = q.get("successful_records")
        if records is not None and sum(int(k) * n for k, n in batches.items()) != records:
            raise ValueError("batch histogram and successful record count disagree")
    if values is not None and any(not _number(v) for v in values):
        raise ValueError("API durations must be finite nonnegative numbers")
    timed_n = counts.get("successful_timed")
    if timed_n is not None and values is not None and timed_n != len(values):
        raise ValueError("invalid API accounting: successful_timed disagrees with retained samples")
    declared_n = (q.get("model_API") or {}).get("n")
    if declared_n is not None and values is not None and declared_n != len(values):
        raise ValueError("declared API n disagrees with retained samples")
    attempted = counts.get("attempted")
    known_status_total = sum(v for v in (statuses or {}).values() if v is not None)
    if attempted is not None and known_status_total > attempted:
        raise ValueError("invalid API accounting: status counts exceed attempted")
    status_counter_known = statuses is not None and all(v is not None for v in statuses.values())
    if status_counter_known and attempted is not None and known_status_total != attempted:
        raise ValueError("invalid API accounting: sparse status total differs from attempted")
    # Omitted Counter categories mean zero only when the recorded categories
    # account for every explicit attempted call. Missing evidence stays None.
    statuses_closed = status_counter_known and attempted is not None
    status_counts = {name: (statuses or {}).get(name, 0 if statuses_closed else None)
                     for name in ("succeeded", "failed", "unknown")}
    for lower, upper in (("completed", "attempted"), ("timed", "attempted"),
                         ("missing_clock", "attempted"), ("successful_timed", "completed"),
                         ("successful_timed", "timed")):
        if counts.get(lower) is not None and counts.get(upper) is not None and counts[lower] > counts[upper]:
            raise ValueError("invalid API accounting: " + lower + " exceeds " + upper)
    completed, timed, missing_clock = (counts.get(k) for k in ("completed", "timed", "missing_clock"))
    if None not in (attempted, timed, missing_clock) and timed + missing_clock != attempted:
        raise ValueError("invalid API accounting: timed plus missing_clock differs from attempted")
    known_terminal = sum(status_counts[k] for k in ("succeeded", "failed") if status_counts[k] is not None)
    terminal_counts_known = all(status_counts[k] is not None for k in ("succeeded", "failed"))
    if completed is not None and (known_terminal > completed or terminal_counts_known and known_terminal != completed):
        raise ValueError("invalid API accounting: completed disagrees with succeeded plus failed")
    succeeded = status_counts["succeeded"]
    if timed_n is not None and succeeded is not None and timed_n > succeeded:
        raise ValueError("invalid API accounting: successful_timed exceeds succeeded")
    if values is not None:
        for label, upper in (("timed", timed), ("completed", completed), ("succeeded", succeeded)):
            if upper is not None and len(values) > upper:
                raise ValueError("invalid API accounting: retained successful samples exceed " + label)
    verified_n = timed_n if values is not None and timed_n is not None and succeeded is not None else None
    complete = (values is not None and statuses_closed and batches is not None
                and all(counts.get(k) == len(values) for k in ("attempted", "completed", "timed", "successful_timed"))
                and succeeded == len(values)
                and status_counts["failed"] == status_counts["unknown"] == 0
                and counts.get("missing_clock") == 0
                and counts.get("unresolved_identity_records") == 0)
    missing = [name for name, value in dict(API_counts=q.get("API_counts"), statuses=statuses,
        actual_batch_sizes=batches, completed=completed, timed=timed, successful_timed=timed_n,
        attempted=counts.get("attempted"), missing_clock=counts.get("missing_clock"),
        unresolved_identity_records=counts.get("unresolved_identity_records"), samples=values).items() if value is None]
    return dict(values=values, counts=counts or None, complete=complete,
                batches=batches, units=q.get("API_units"), missing=missing,
                status_counts=status_counts, verified_successful_timed=verified_n)


def _public(queries, samples, lifecycles):
    ids = [q.get("query_id") for q in queries]
    if any(not isinstance(i, str) or not i for i in ids) or len(ids) != len(set(ids)):
        raise ValueError("unique nonempty query IDs are required")
    samples_by_id = {}
    for row in samples:
        key = row["query_id"]
        if key in samples_by_id or key not in set(ids):
            raise ValueError("duplicate or orphan API sample collection")
        samples_by_id[key] = row["elapsed_s"]
    result = []
    for q in queries:
        times = q.get("timing") or {}
        full, ready = times.get("API_return_s"), times.get("answer_ready_s")
        if _number(full) and _number(ready) and ready > full:
            raise ValueError("query-ready duration exceeds complete-query duration")
        result.append(dict(id=q["query_id"], configuration=q.get("configuration"),
            task=q.get("task"), phase=q.get("phase"), role=q.get("role"),
            status=_status(q), full_s=times.get("API_return_s"),
            ready_s=times.get("answer_ready_s"),
            full_clock_complete=times.get("API_timing_complete") is True,
            records={k: q.get(k) for k in ("records", "successful_records", "failed_records", "unknown_records")},
            api=_api(q, samples_by_id.get(q["query_id"])), GPU=q.get("GPU"),
            quality=q.get("quality"), returned_result=q.get("result"),
            stages=q.get("stage_counts"), result_rows=q.get("result_rows")))
    return result, list(lifecycles)


def _fifo(data):
    if not isinstance(data.get("queries"), list):
        raise ValueError("fifo_summary.v1 requires queries[] with retained API.values")
    qs = data["queries"]
    counts = [data.get(k) for k in ("actual_success", "actual_failure", "actual_unknown")]
    for count in counts:
        _count(count)
    reconciled = (None not in counts and counts[1:] == [0, 0]
                  and all(q.get("actual_success") is not None for q in qs)
                  and sum(q["actual_success"] for q in qs) == counts[0])
    queries, lifecycles = [], []
    for index, q in enumerate(qs):
        # Old summaries have no query ID. This ordinal identifies the retained
        # query receipt, never an invented row/request or independent repeat.
        key = q.get("query_id", "summary-query-" + str(index))
        values = (q.get("API") or {}).get("values")
        n = _count(q.get("actual_success"))
        public = dict(model_API=q.get("API"), successful_records=n,
                      API_units=[data["API_unit"]] if data.get("API_unit") else None)
        # This layout declares one complete single-call interval per API value.
        # Other unit shapes need a different adapter; do not expand records.
        if data.get("API_unit") == "single_generate":
            public["actual_batch_sizes"] = {"1": n} if n is not None else None
            if reconciled:
                public["API_counts"] = dict(successful_timed=n, statuses={"succeeded": n})
        queries.append(dict(id=key, configuration=q.get("mode"), task=q.get("task"),
            phase=q.get("phase"), role=q.get("role"),
            status="succeeded" if reconciled else "unknown",
            full_s=q.get("caller_return_s"), ready_s=q.get("query_result_ready_s"),
            full_clock_complete=reconciled, api=_api(public, values),
            # actual_success counts model calls across stages in this layout;
            # it is not the number of input records or returned rows.
            records=dict(records=None, successful_records=None,
                         failed_records=None, unknown_records=None),
            GPU=q.get("GPU_caller_window"), quality=q.get("quality"),
            returned_result=q.get("result"), stages=q.get("stage_counts"),
            result_rows=q.get("result_rows")))
        lifecycles.append(dict(query_id=key, configuration=q.get("mode"),
            startup_s=q.get("startup_s"), cleanup_s=q.get("close_s"),
            application_s=q.get("owner_lifecycle_s"), external_caller_s=q.get("caller_return_s")))
    counter_names = ("actual_success", "actual_failure", "actual_unknown", "automatic_model_retries",
                     "warmup_calls", "actual_model_call_authorization_cap", "historical_allocated",
                     "historical_claimed_cap", "old_expired_unclaimed_FIFO_cap")
    source_counters = {name: _count(data.get(name)) for name in counter_names}
    failures_or_unknown = (True if any(v is not None and v > 0 for v in counts[1:])
                          else False if None not in counts[1:] else None)
    source_accounting = dict(scope="entire_input_summary; not allocated to individual queries",
        source_status=data.get("status"), counters=source_counters,
        failures_or_unknown_present=failures_or_unknown, success_counts_reconciled=reconciled)
    return queries, lifecycles, source_accounting


def _gpu(queries):
    # Same count-weighted sample means as the existing retained-result
    # recompute.py: this is a sample mean, never a time-coverage estimate.
    names = {name for q in queries for name in (q.get("GPU") or {}).get("metrics", {})}
    result = {}
    for name in sorted(names):
        parts = [(q.get("GPU") or {}).get("metrics", {}).get(name, {}) for q in queries]
        valid = _sum_known(_count(p.get("valid_samples")) for p in parts)
        attempted = _sum_known(_count(p.get("attempted_samples")) for p in parts)
        means_complete = all(p.get("sample_mean") is not None or p.get("valid_samples") == 0 for p in parts)
        maxima = [p["observed_max"] for p in parts if p.get("observed_max") is not None]
        result[name] = dict(valid_samples=valid, attempted_samples=attempted,
            sample_mean=(math.fsum(p["sample_mean"] * p["valid_samples"] for p in parts
                         if p.get("sample_mean") is not None) / valid
                         if valid and means_complete else None),
            observed_max=max(maxima) if maxima else None,
            summaries_missing=sum(not p for p in parts),
            missing_reasons=sorted({r for p in parts for r in p.get("missing_reasons", [])}))
    counts = [(q.get("GPU") or {}).get("samples") for q in queries]
    return dict(n_samples=_sum_known(_count(n) for n in counts), metrics=result,
        status="observed" if result else "NA", aggregation="valid-sample-count weighted mean",
        continuous_time_coverage=na("retained summaries do not provide verified time segments"),
        query_sample_counts=counts, query_summaries_missing=sum(q.get("GPU") is None for q in queries))


def summarize(*, input_schema, comparison_scope, queries=None, api_samples=None,
              lifecycles=None, results=None, metadata=None):
    """Return JSON-compatible summaries; do not mutate caller-owned objects.

    comparison_scope is an explicit caller label (e.g. records_e2e versus
    token_diagnostic). No grouping or metric uses dataset/model names.
    """
    if input_schema not in SCHEMAS:
        raise ValueError("unsupported input schema: " + str(input_schema))
    if not isinstance(comparison_scope, str) or not comparison_scope:
        raise ValueError("an explicit comparison_scope is required")
    if input_schema == PUBLIC:
        normalized, lives = _public(queries or [], api_samples or [], lifecycles or [])
        source_accounting = na("no source ledger in query rows; optional historical ledger remains in metadata")
    else:
        normalized, lives, source_accounting = _fifo(results or {})
    if len({q["id"] for q in normalized}) != len(normalized):
        raise ValueError("duplicate query identity")
    groups = defaultdict(list)
    for q in normalized:
        groups[tuple(q[k] for k in ("configuration", "task", "phase", "role"))].append(q)
    output = []
    for key, items in sorted(groups.items(), key=lambda kv: json.dumps(kv[0])):
        items = sorted(items, key=lambda q: q["id"])
        distributions = {}
        for name, field in (("complete_query", "full_s"), ("query_ready", "ready_s")):
            rows = [dict(id=q["id"], elapsed_s=q[field], status=q["status"])
                    for q in items if q["status"] != "not_executed"]
            distributions[name] = _distribution(rows, complete=all(q["full_clock_complete"] for q in items))
        api_rows = [dict(id=str(i) + ":" + str(j), status="succeeded", elapsed_s=v)
                    for i, q in enumerate(items) for j, v in enumerate(q["api"]["values"] or [])]
        api_complete = all(q["api"]["complete"] for q in items)
        api_statuses = [q["api"]["status_counts"] for q in items]
        batches = Counter()
        for q in items:
            batches.update(q["api"]["batches"] or {})
        counts = {name: _sum_known(s[name] for s in api_statuses)
                  for name in ("succeeded", "failed", "unknown")}
        output.append(dict(group=dict(zip(("configuration", "task", "phase", "role"), key)),
            comparison_scope=comparison_scope, query_ids=[q["id"] for q in items],
            n_query=len(items), query_statuses=dict(Counter(q["status"] for q in items)),
            clocks=distributions,
            records={k: _sum_known(_count(q["records"].get(k)) for q in items)
                     for k in ("records", "successful_records", "failed_records", "unknown_records")},
            model_API=dict(n_retained_samples=len(api_rows),
                n_successful_timed=_sum_known(q["api"]["verified_successful_timed"] for q in items),
                sample_success_verified=all(q["api"]["verified_successful_timed"] is not None for q in items),
                distribution_scope="retained API intervals; success counts require explicit counters, never array length",
                status_counts=counts,
                accounting_complete=api_complete, distribution=_distribution(api_rows, complete=api_complete),
                sample_units=sorted({unit for q in items for unit in q["api"]["units"] or []}),
                actual_batch_sizes=dict(batches) if all(q["api"]["batches"] is not None for q in items) else None,
                samples_missing_queries=[q["id"] for q in items if q["api"]["values"] is None],
                accounting_missing_fields={q["id"]: q["api"]["missing"] for q in items if q["api"]["missing"]},
                batch_intervals_expanded=False), GPU=_gpu(items),
            quality=[dict(query_id=q["id"], metrics=q["quality"] or na("quality audit not retained"),
                returned_result=q["returned_result"], stage_counts=q["stages"],
                zero_Map=(q["stages"].get("Map") == 0 if q["stages"] is not None and "Map" in q["stages"] else None),
                empty_successor=_empty_successor(q)) for q in items]))
    return dict(schema="semloom.offline_summary.v1", input_schema=input_schema,
        comparison_scope=comparison_scope, n_query=len(normalized), n_lifecycle=len(lives),
        groups=output, lifecycles=lives, metadata=metadata or {}, source_accounting=source_accounting,
        unavailable=dict(
            MFU=na("closed native FLOPs and matching clocks are not retained in these layouts; saved estimates are not recertified", status="unsupported"),
            API_window_MFU=na("terminal calls/tokens do not seal FLOPs; close-tail deltas cannot be moved into an earlier API window", status="unsupported"),
            ready_to_admit=na("verified absolute ready/admit clocks absent"),
            critical_path=na("consumer/dependency events absent; overlapping intervals are not summed"),
            physical_livebuffer=na("RSS/logical bytes do not establish physical buffer lifetimes"),
            kernel_time=na("API wall time includes host work and waits")),
        accounting_note="retained query/API successes are separate from historical ledger statuses, retries and unused reservations in metadata; missing counts stay null",
        lifecycle_note="original nested clocks retained separately; startup/close are not added to query or caller clocks")


def _read(path, *, jsonl=False, expected_sha=None, with_digest=False):
    path = Path(path)
    with path.open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("input exceeds per-file byte limit")
    digest = hashlib.sha256(raw).hexdigest()
    if expected_sha is not None and expected_sha != digest:
        raise ValueError("input SHA-256 mismatch")
    if path.suffix == ".gz":
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
            payload = stream.read(MAX_BYTES + 1)
    else:
        payload = raw
    if len(payload) > MAX_BYTES:
        raise ValueError("decoded input exceeds per-file byte limit")
    def no_constant(value):
        raise ValueError("nonfinite JSON number: " + value)
    def unique(pairs):
        obj = {}
        for k, v in pairs:
            if k in obj:
                raise ValueError("duplicate JSON key: " + k)
            obj[k] = v
        return obj
    def parse(value):
        return json.loads(value, parse_constant=no_constant, object_pairs_hook=unique)
    result = [parse(line) for line in payload.splitlines() if line.strip()] if jsonl else parse(payload)
    return (result, dict(sha256=digest, bytes=len(raw))) if with_digest else result


def analyze(*, manifest=None, input_schema=None, comparison_scope=None, root=None, **paths):
    """Read only explicit local files and return a summary with input digests.

    A semloom.offline_inputs.v1 manifest supplies input_schema,
    comparison_scope, files:{role:{path,sha256?}}, and optional metadata.
    Path arguments override paths but retain manifest hashes when present.
    """
    config = _read(manifest) if manifest else {}
    if config and config.get("schema") != MANIFEST:
        raise ValueError("unsupported manifest schema")
    if input_schema and config.get("input_schema", input_schema) != input_schema:
        raise ValueError("CLI and manifest schemas disagree")
    schema = input_schema or config.get("input_schema")
    if schema not in SCHEMAS:
        raise ValueError("unsupported input schema: " + str(schema))
    files = dict(config.get("files", {}))
    for role, path in paths.items():
        if path is not None:
            files[role] = dict(files.get(role, {}), path=str(path))
    allowed = {"queries", "api_samples", "lifecycles"} if schema == PUBLIC else {"results"}
    if set(files) - allowed:
        raise ValueError("unsupported file roles for schema")
    required = {"queries"} if schema == PUBLIC else {"results"}
    if not required <= set(files):
        raise ValueError("missing required input path")
    base = Path(root) if root else Path(manifest).resolve().parent if manifest else Path.cwd()
    loaded, digests = {}, {}
    for role, entry in files.items():
        path = (base / entry["path"]).resolve()
        # Hash only an explicitly selected compact file, never walk a directory.
        loaded[role], digests[role] = _read(path, jsonl=role != "results",
            expected_sha=entry.get("sha256"), with_digest=True)
    result = summarize(input_schema=schema,
        comparison_scope=comparison_scope or config.get("comparison_scope"),
        metadata=config.get("metadata"), **loaded)
    result["inputs"] = digests
    return result
