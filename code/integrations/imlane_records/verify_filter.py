"""Finite CPython ordinary Movie Filter qualification; the owner supplies its slot.

No public backend selector is changed. The no-model entry uses original LOTUS
and an explicit CPU subprocess. local-model must be scheduled by the slot owner.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

OWNED = Path(__file__).resolve().parent
CODE = OWNED.parents[1]
sys.path.insert(0, str(CODE))

from integrations.imlane_records import owner, verify
from integrations.imlane_records.client import RecordsBridge
from integrations.imlane_records.filter_host import FilterResults
from integrations.imlane_records.filter_workload import PROTOCOL
from src.baselines.common.private_artifacts import require_outside_git, write_private_json
from src.baselines.common.redact import redact_json_values, redact_text
from src.baselines.text.movie_records import MovieRecordSource
from src.experiments.lotus_records_movie import run_movie_query
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


class FilterScope(verify.RecordScope):
    def workload_environment(self):
        return dict(IMLANE_FILTER_CONFIG=str(self.args.filter_config),
                    CUDA_VISIBLE_DEVICES=self.args.cuda_visible_devices,
                    IMLANE_FILTER_FIXTURE="normal", HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
                    LITELLM_LOCAL_MODEL_COST_MAP="True")


def configuration(value):
    if value.get("schema") != "imlane.filter.run.v1" or value.get("mode") not in ("no-model", "local-model"):
        raise ValueError("an explicit versioned no-model or local-model run is required")
    method = FilterMethod.from_dict(value["method"])
    engine = EngineSpec.from_dict(value["engine"])
    generation = GenerationSpec.from_dict(value["generation"])
    resources = RecordsResources(**value["resources"])
    if (resources.cpu_workers != 1 or resources.model_batch_rows > 16 or resources.prepare_batch_rows > 16
            or resources.max_active_batches > 2):
        raise ValueError("this CPython slice requires one worker and the existing finite bridge capacities")
    queries = value["query_ids"]
    if (type(queries) is not list or not 1 <= len(queries) <= 2 or len(set(queries)) != len(queries)
            or any(type(q) is not str or not q.strip() for q in queries)
            or type(value["max_total_calls"]) is not int or not 0 < value["max_total_calls"] <= 4096
            or engine.max_model_calls is None or engine.max_model_calls * len(queries) > value["max_total_calls"]):
        raise ValueError("qualification needs distinct query IDs and a finite total permanent allowance")
    source = MovieRecordSource.from_manifest(value["source_manifest"])
    source.preflight(method)
    from src.semantic_methods.lotus.engine import validate_generation
    validate_generation(method, generation)
    if any(len(source.expected(q)) > engine.max_model_calls for q in queries):
        raise ValueError("source occurrences exceed this model instance's request allowance")
    if value["mode"] == "local-model" and any(not source.expected(q) for q in queries):
        raise ValueError("model qualification requires at least one eligible occurrence in each query")
    preflight = json.loads(Path(value["preflight_report"]).read_text())
    if preflight.get("status") != "ok":
        raise ValueError("the owner must supply a successful current runtime preflight")
    method_python, author = Path(value["method_python"]), Path(value["author_root"])
    probe = "import json,sys,pyarrow; print(json.dumps(dict(base=sys.base_prefix,version=list(sys.version_info[:2]),arrow=pyarrow.__version__)))"
    probes = [json.loads(subprocess.check_output([str(p), "-I", "-B", "-c", probe], text=True, timeout=15))
              for p in (author / "venv/bin/python", method_python)]
    if (any(p["arrow"] != "16.1.0" or p["version"] != [3, 12] for p in probes)
            or Path(probes[0]["base"]).resolve() != Path(probes[1]["base"]).resolve()):
        raise ValueError("the isolated method copy must retain the author's Python and Arrow ABI")
    subprocess.run([str(method_python), "-I", "-B", "-c",
        "import sys;sys.path.insert(0,sys.argv[1]);from src.semantic_methods.lotus.engine import validate_filter_source;validate_filter_source()",
        str(CODE)], check=True, capture_output=True, timeout=15)
    binary = Path(value["bridge_binary"])
    author_build = json.loads((author / "artifacts/build-patched/build-identity.json").read_text())
    if author_build.get("upstream_commit") != owner.AUTHOR or author_build.get("mode") != "patched":
        raise ValueError("the checked author build identity differs")
    expected = dict(author_build["original_sha256"], **author_build["patched_sha256"])
    if (any(owner.digest(author / "source" / path) != sha for path, sha in expected.items())
            or any(owner.digest(author / "prefix-patched" / path) != sha
                   for path, sha in author_build["binaries_sha256"].items())):
        raise ValueError("the author's checked source or binaries changed")
    built = json.loads(binary.with_suffix(".build.json").read_text())
    if (built.get("upstream_commit") != owner.AUTHOR or built.get("binary_sha256") != owner.digest(binary)
            or built.get("sources_sha256") != {p.name: owner.digest(p) for p in (
                OWNED / "imlane_records.cpp", OWNED / "record_dbend_adapter.hpp")}):
        raise ValueError("the bridge binary is not bound to these reviewed v2 source bytes")
    if value["mode"] == "local-model":
        engine.validate_driver_environment()
        if (not value["model_environment"].get("CUDA_VISIBLE_DEVICES")
                or "," in value["model_environment"]["CUDA_VISIBLE_DEVICES"]):
            raise ValueError("the slot owner must explicitly assign one model GPU")
    elif value.get("model_environment"):
        raise ValueError("no-model qualification must keep its GPU mask empty")
    return method, engine, generation, resources, source


def gpu_snapshot(device):
    def query(kind, fields):
        result = subprocess.run(["nvidia-smi", "-i", device, "--query-" + kind + "=" + fields,
                                 "--format=csv,noheader,nounits"], check=True,
                                capture_output=True, text=True, timeout=5)
        return [[field.strip() for field in line.split(",")] for line in result.stdout.splitlines() if line.strip()]
    devices = query("gpu", "uuid,memory.used")
    if len(devices) != 1 or len(devices[0]) != 2:
        raise ValueError("the assigned GPU must resolve to exactly one physical device")
    processes = query("compute-apps", "pid,gpu_uuid,used_memory")
    if any(len(row) != 3 or row[1] != devices[0][0] for row in processes):
        raise ValueError("GPU compute process observation is incomplete")
    return dict(uuid=devices[0][0], memory_mib=int(devices[0][1]), compute_processes=processes)


def settle_lease(lease, scopes, report):
    if report.get("model_launch_possible") and report.get("gpu_exit_confirmed") is not True:
        report["passed"] = False
        report["host_lease"] = dict(status="held_unresolved", reason="independent GPU exit remains unconfirmed",
                                    scopes=[owner.resource_state(scope) for scope in scopes])
        return
    verify._settle_host_lease(lease, scopes, report)


def apply_final_verdict(report, *, stopped):
    if (stopped or report.get("sampler_errors") or report.get("stderr_capture_errors")
            or report.get("cleanup_errors") or report.get("final_cleanup_notices")
            or any(not item.get("passed", False) for item in report.get("cleanup", []))):
        report["passed"] = False


def run_one(args, value, report, components, query_id):
    method, engine, generation, resources, source = components
    deadline = time.monotonic() + resources.query_timeout_s
    args.filter_config = args.output / ("worker-" + str(len(report["queries"])) + ".json")
    args.cuda_visible_devices = value.get("model_environment", {}).get("CUDA_VISIBLE_DEVICES", "")
    udf = (OWNED / "filter_udf.py" if value["mode"] == "local-model"
           else CODE / "tests/integrations/imlane_filter_fixture_udf.py")
    scope = FilterScope(args, "cpython-filter", owner, report, kernel=udf)
    control = scope.root / "query-control"
    control.mkdir()
    write_private_json(args.filter_config, dict(method=asdict(method), engine=asdict(engine),
        generation=asdict(generation), resources=asdict(resources), query_id=query_id,
        deadline_monotonic=deadline, control_dir=str(control), model_environment=value.get("model_environment", {})))
    iterator = None
    try:
        scope.__enter__()
        def execute():
            nonlocal iterator
            process = subprocess.Popen([str(args.bridge_binary), str(scope.port), str(udf), "infer_records", PROTOCOL],
                env=scope.env, cwd=scope.root, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, start_new_session=True)
            scope.track_root(process)
            writer = verify._capture_stderr(process.stderr, scope.root / "bridge-stderr.log", report)
            scope.children.append((process, writer))
            def finish_owner():
                scope.close()
                return owner.resource_state(scope)
            def abort_owner():
                state = finish_owner()
                process.wait(timeout=3)
                if not state["resources_confirmed"]:
                    raise RuntimeError("Filter scope resources remain unconfirmed")
            client = RecordsBridge(process, abort_owner=abort_owner, timeout_s=min(60, resources.query_timeout_s),
                                   protocol=PROTOCOL)
            rows = (dict(identity(query_id, row["row_id"], row["source_position"]), reviewText=row["reviewText"])
                    for row in source.rows() if row["id"] == "taken_3")
            iterator = FilterResults(rows, client, query_id=query_id, max_calls=engine.max_model_calls,
                control_dir=control, deadline=deadline, close_owner=finish_owner,
                block_rows=resources.prepare_batch_rows, max_pending=resources.max_active_batches)
            return iterator
        result = run_movie_query(source, value["sembench_checkout"], method, generation, execute, query_id=query_id)
        report["queries"].append(result)
    except BaseException as error:
        report["queries"].append(getattr(error, "query_report", dict(query_id=query_id, status="failed",
            error_type=type(error).__name__, error=redact_text(str(error)))))
        raise
    finally:
        scope.close()
        report.setdefault("scope_resources", []).append(owner.resource_state(scope))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    require_outside_git(args.config)
    require_outside_git(args.output)
    value = json.loads(args.config.read_text())
    components = configuration(value)  # No owner/model construction before preflight.
    args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
    for name in ("author_root", "method_python", "bridge_binary", "artifact_root"):
        path = Path(value[name]).resolve()
        require_outside_git(path)
        setattr(args, name, path)
    args.scopes, args.stop_budget = [], owner.StopBudget(threading.Event())
    signal.signal(signal.SIGTERM, args.stop_budget.signal)
    signal.signal(signal.SIGINT, args.stop_budget.signal)
    report = dict(schema="imlane.filter.qualification.v1", mode=value["mode"], runtime="author-cpython",
                  author_commit=owner.AUTHOR, model_lifetime="one private engine per query owner",
                  author_scheduling_modified=False, public_cli_changed=False, queries=[], cleanup=[], passed=False,
                  sources_sha256={p.name: owner.digest(p) for p in OWNED.iterdir() if p.suffix in (".py", ".cpp", ".hpp")})
    if value["mode"] == "no-model":
        report.update(real_model_requests=0, gpu_requests=0)
    engine = components[1]
    reservation = dict(schema="imlane.filter.reservation.v1", max_total_calls=value["max_total_calls"],
                       reserved_by_query={q: engine.max_model_calls for q in value["query_ids"]},
                       retry=False, refund_failed_or_unknown=False, one_new_engine_per_query=True)
    write_private_json(args.output / "reservation.json", reservation)
    report["reservation"] = reservation
    lease = verify._host_lease(args.artifact_root, "imlane-filter-staged-source")
    try:
        if value["mode"] == "local-model":
            report["gpu_before"] = gpu_snapshot(value["model_environment"]["CUDA_VISIBLE_DEVICES"])
            if report["gpu_before"]["compute_processes"]:
                raise RuntimeError("the owner's assigned GPU is not idle before this run")
        for query_id in value["query_ids"]:
            args.stop_budget.check()
            if value["mode"] == "local-model":
                report["model_launch_possible"] = True
            run_one(args, value, report, components, query_id)
        report["passed"] = all(q["status"] == "completed" for q in report["queries"])
    except BaseException as error:
        report.update(passed=False, error_type=type(error).__name__, error=redact_text(str(error)))
    finally:
        for scope in reversed(args.scopes):
            try:
                scope.close()
            except BaseException as error:
                report.setdefault("cleanup_errors", []).append(redact_text(type(error).__name__ + ": " + str(error)))
        if report.get("model_launch_possible"):
            try:
                report["gpu_after"] = gpu_snapshot(value["model_environment"]["CUDA_VISIBLE_DEVICES"])
                before, after = report["gpu_before"], report["gpu_after"]
                report["gpu_exit_confirmed"] = (after["uuid"] == before["uuid"] and not after["compute_processes"]
                                                 and after["memory_mib"] <= before["memory_mib"] + 1)
            except BaseException as error:
                report["gpu_exit_confirmed"] = None
                report["gpu_cleanup_error"] = redact_text(type(error).__name__ + ": " + str(error))
        apply_final_verdict(report, stopped=args.stop_budget.stopped.is_set())
        settle_lease(lease, args.scopes, report)
        if any(not owner.resource_state(scope)["resources_confirmed"] for scope in args.scopes):
            report["passed"] = False
        write_private_json(args.output / "report.json", redact_json_values(report))
    print(json.dumps(dict(passed=report["passed"], queries=len(report["queries"]), mode=value["mode"])))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
