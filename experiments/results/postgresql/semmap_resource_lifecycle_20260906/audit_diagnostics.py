"""Read-only, allowlisted audit of private diagnostic roots; never export log text.

Run with PYTHONPATH=<source>/code and pass one or more run directories. Source
logs, payloads, FD paths, environment values and host/user names stay private.
The output is a derived audit, not a byte-identical copy of the original raw.
"""
import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path

from src.baselines.common.redact import redact_text


def sha256(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def records(path):
    with gzip.open(path, "rt") as handle:
        return [json.loads(line) for line in handle]


def identity(row):
    return row["pid"], row["process_start_time_ticks"]


def fds(row):
    return {(f["fd"], f["inode"], f["target"]) for f in row["fds"]}


def trace_audit(path):
    rows = records(path)
    baseline = {r["role"]: r for r in rows if r["kind"] == "baseline"}
    ticks = [r for r in rows if r["kind"] == "tick"]
    roles = {}
    for role, base in baseline.items():
        values = [r for r in ticks if r["role"] == role]
        last = values[-1] if values else None
        anon = defaultdict(list)
        for row in values:
            for fd in row["fds"] or []:
                if fd["target"] == "anon_inode:[eventpoll]" and (fd["fd"], fd["inode"], fd["target"]) not in fds(base):
                    anon[(fd["fd"], fd["inode"])].append(row["monotonic_ns"])
        roles[role] = {
            "baseline_identity": identity(base),
            "all_ticks_same_process": all(identity(r) == identity(base) for r in values),
            "observations": len(values),
            "end_fd_identity_matches_baseline": last is not None and fds(last) == fds(base),
            "end_total_fd_delta": None if last is None else last["total_fd_count"] - base["total_fd_count"],
            "end_thread_delta": None if last is None else last["thread_count"] - base["thread_count"],
            "end_rss_delta": None if last is None else last["rss_bytes"] - base["rss_bytes"],
            "baseline_rss_bytes": base["rss_bytes"],
            "new_eventpoll_observations": [{"fd": key[0], "inode": key[1], "samples": len(times),
                                            "first_ns": min(times), "last_ns": max(times)}
                                           for key, times in anon.items()],
            "observed_anon_kinds": sorted({f["target"] for r in values for f in (r["fds"] or [])
                                           if f["target"] in {"anon_inode:[eventpoll]", "anon_inode:[eventfd]", "anon_inode:[signalfd]"}}),
        }
    return {
        "sha256": sha256(path), "process_record_statuses": dict(Counter(r["status"] for r in ticks)),
        "empty_ticks": sum(r["kind"] == "empty_tick" for r in rows),
        "invalid_unix_table_records": sum(not r["unix_table_valid"] for r in ticks),
        "retry_attempts_with_errors": sum(bool(a["errors"]) for r in ticks for a in r.get("fd_attempts", [])),
        "roles": roles,
    }


def audit(root):
    summary = json.loads((root / "summary.json").read_text())
    source = json.loads((root / "source_identity.json").read_text())
    checks = []
    # Check all original hash lists, including private database files, in place.
    for path in sorted(root.rglob("SHA256SUMS.json")):
        entries = json.loads(path.read_text())
        mismatches = [name for name, expected in entries.items()
                      if not (path.parent / name).is_file() or sha256(path.parent / name) != expected]
        checks.append({"path": str(path.relative_to(root)), "sha256": sha256(path),
                       "entries": len(entries), "mismatch_count": len(mismatches)})
    phases, processes = {}, set()
    for path in sorted(root.glob("*/**/phase_report.json")):
        phase = path.parent
        report = json.loads(path.read_text())
        output = {key: report.get(key) for key in ("phase", "state", "assessment", "safe")}
        output["failure_metrics"] = [f.get("metric") for f in report.get("failures", [])]
        output["problem_count"] = len(report.get("problems", []))
        for window in ("baseline", "operation", "cleanup"):
            raw = phase / window / "process_samples.jsonl.gz"
            if raw.exists():
                output[window] = trace_audit(raw)
                processes.update(tuple(v["baseline_identity"]) for v in output[window]["roles"].values())
        derived = phase / "attributed_operation/process_samples.jsonl.gz"
        if derived.exists():
            rows = records(derived)
            base = {r["role"]: r for r in rows if r["kind"] == "baseline"}
            pairs = defaultdict(dict)
            for row in rows:
                if row["kind"] != "tick":
                    continue
                role = row["role"]
                count = lambda r: sum(f["kind"] == "provider_uds_connected" for f in (r["fds"] or []))
                pairs[row["tick_start_ns"]][role] = count(row) - count(base[role])
            output["recomputed_same_tick_socket_peak"] = max((sum(v.values()) for v in pairs.values()), default=None)
            output["derived_sha256"] = sha256(derived)
        event_file = phase / "session_events.json"
        if event_file.exists():
            events = json.loads(event_file.read_text())
            output["event_counts"] = dict(Counter(e["event"] for e in events))
            output["all_session_ends_explicitly_closed"] = all(e.get("connection_closed") is True for e in events if e["event"] == "session_end")
        outcome = phase / "operation/operation_outcome.json"
        if outcome.exists():
            value = json.loads(outcome.read_text())
            output["sqlstate"] = value.get("operation_error", {}).get("sqlstate")
            output["sampling_error_count"] = len(value.get("sampling_errors", []))
        phases[str(phase.relative_to(root))] = output
    live = []
    for pid, start in sorted(processes):
        try:
            value = Path(f"/proc/{pid}/stat").read_text()
            if int(value[value.rfind(")") + 2:].split()[19]) == start:
                live.append([pid, start])
        except FileNotFoundError:
            pass
    client = []
    client_file = root / "stress_large_payload/client/client.log"
    if client_file.exists():
        for line in client_file.read_text().splitlines():
            try:
                value = json.loads(line)
            except ValueError:
                continue
            client.append({k: v for k, v in value.items()
                           if (k in {"round", "rounds", "rows", "rows_per_round", "completed", "total_rows", "backend_pid"}
                               and isinstance(v, (int, bool)))
                           or (k == "event" and v in {"warmup_complete", "round_complete", "all_complete"})})
    selected_hashes = {str(p.relative_to(root)): sha256(p) for p in root.rglob("*")
                       if p.is_file() and p.relative_to(root).parts[0] not in {"data", "socket"}}
    return {
        "source": {k: source[k] for k in ("commit", "postgresql", "source_clean")},
        "mode": summary["mode"], "implementation_revision": summary["implementation_revision"],
        "assessment": summary["assessment"], "diagnostic_status": summary["diagnostic_status"],
        "qualification_status": summary["qualification_status"], "exit_code": summary["exit_code"],
        "model_requests": summary["model_requests"],
        "workload": {k: summary["workload"][k] for k in ("rounds", "rows_per_round", "input_bytes", "output_bytes", "sample_seconds")},
        "client_events_allowlisted": client, "phases": phases, "manifest_checks": checks,
        "original_selected_file_sha256": selected_hashes,
        "observed_process_identities": sorted(processes), "same_identities_still_running": live,
        "postmaster_pid_file_exists": (root / "data/postmaster.pid").exists(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("roots", nargs="+", type=Path)
    parser.add_argument("--toolchain", type=Path)
    parser.add_argument("--preflight", type=Path)
    args = parser.parse_args()
    result = {"producer": "read_only_allowlisted_audit", "original_raw_exported": False,
              "runs": {root.name: audit(root) for root in args.roots}}
    if args.toolchain:
        result["toolchain_sha256"] = {name: sha256(args.toolchain / name)
                                      for name in ("bin/postgres", "bin/pg_config", "lib/semloom_pg.so")}
    if args.preflight:
        result["preflight_sha256"] = sha256(args.preflight)
    print(redact_text(json.dumps(result, indent=2, sort_keys=True)))
