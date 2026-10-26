"""Finite one-executor CPython records echo check using the existing owner supervisor."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time

OWNED = Path(__file__).resolve().parent
CODE = OWNED.parents[1]
sys.path.insert(0, str(CODE))
from integrations.imlane_records import owner as tools
from integrations.imlane_records.client import RecordsBridge, RecordsRejected
from integrations.imlane_records.resource_limits import check_resources, owner_limits
from src.baselines.common.private_artifacts import require_outside_git, write_private_json
from src.baselines.common.redact import redact_text
from src.infrastructure.runner_lease import (
    HOST_SCOPE_DIR_NAME, HOST_SCOPE_FINGERPRINT, acquire_runner_lease,
)


class RecordScope(tools.AuthorScope):
    def workload_environment(self):
        return {}

    def sample(self):
        import psutil
        try:
            limits = owner_limits(getattr(self.args, "owner_resource_limits", None))
            while not self.stop_sampler.wait(.1):
                self.snapshot()
                rss = 0
                for pid, ticks in self.survivors():
                    try:
                        process = psutil.Process(pid)
                        if tools.process_start_ticks(pid) == ticks:
                            rss += process.memory_info().rss
                    except (psutil.Error, OSError):
                        pass
                shm = sum(self.helper.resources(self.namespace).values())
                self.report["rss_peak_bytes"] = max(self.report.get("rss_peak_bytes", 0), rss)
                self.report["shm_peak_bytes"] = max(self.report.get("shm_peak_bytes", 0), shm)
                try:
                    check_resources(rss, shm, limits)
                except RuntimeError:
                    self.report["resource_allowance_exceeded"] = True
                    raise
        except BaseException as error:
            self.report.setdefault("sampler_errors", []).append(redact_text(type(error).__name__ + ": " + str(error)))
            self.args.stop_budget.stopped.set()
            self.abort_owned()

    def __enter__(self):
        self.args.stop_budget.check()
        author = self.args.author_root
        python = getattr(self.args, "method_python", None) or author / "venv/bin/python"
        prefix = author / "prefix-patched"
        info = json.loads(subprocess.check_output([str(python), "-B", "-c",
            "import sys,sysconfig,pyarrow,json;print(json.dumps(dict(site=sysconfig.get_path('purelib'),lib=pyarrow.get_library_dirs()[0],base=sys.base_prefix)))"], text=True, timeout=15))
        cpp = Path(subprocess.check_output(["g++", "-print-file-name=libstdc++.so.6"], text=True).strip()).resolve()
        temporary = self.root / "tmp"
        temporary.mkdir()
        self.env = dict(os.environ, CUDA_VISIBLE_DEVICES="", OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1",
            PYTHONDONTWRITEBYTECODE="1", PATH=str(python.parent) + os.pathsep + os.environ["PATH"],
            PYTHONPATH=os.pathsep.join([info["site"], str(CODE), str(OWNED)]),
            LD_LIBRARY_PATH=os.pathsep.join([str(cpp.parent), str(prefix / "lib"), info["lib"],
                str(author / "source/third_party/fmt/dist/lib"), str(author / "source/third_party/boost/dist/lib"), info["base"] + "/lib"]),
            TMPDIR=str(temporary), IMLANE_NAMESPACE=self.namespace, IMLANE_GATEWAY_HOST="127.0.0.1")
        self.env.update(self.workload_environment())
        self.env.pop("RAY_ADDRESS", None)
        self.env.pop("PYTHONHOME", None)
        self.sampler = threading.Thread(target=self.sample, daemon=True)
        self.sampler.start()
        self.port = self.helper.free_port()
        command = [str(prefix / "bin/imlane"), "--working_dir", str(self.root / "state"),
                   "--n_executors", "1", "--runtime", "cpython", "--gateway_port", str(self.port)]
        try:
            self.service, self.writer = self.helper.spawn(command, self.env, self.root / "coordinator.log")
            self.track_root(self.service)
            until = time.monotonic() + 30
            while True:
                self.args.stop_budget.check()
                if self.service.poll() is not None:
                    raise RuntimeError("records Coordinator exited before ready")
                try:
                    with socket.create_connection(("127.0.0.1", self.port), timeout=.1):
                        break
                except OSError:
                    if time.monotonic() >= until:
                        raise TimeoutError("records Coordinator startup deadline exceeded")
                    time.sleep(.05)
            return self
        except BaseException:
            self.close()
            raise


def rows(query, *ids):
    return [dict(call_id=value, row_id=7, query_id=query, stage_id="filter",
                 record_json=json.dumps({"text": "same source\n\0中文"}, ensure_ascii=False)) for value in ids]


def reject(operation):
    try:
        operation()
    except RecordsRejected:
        return
    raise AssertionError("invalid records command was accepted")


def collect_all(client, expected):
    result = {}
    deadline = time.monotonic() + 8
    while len(result) < expected:
        for block in client.wait_ready(timeout_s=max(.01, deadline - time.monotonic())):
            reply = client.request("collect", block_id=block)
            result[block] = reply["rows"]
            client.native_retained_output_checks = reply["retained_output_checks"]
    return result


def healthy(client):
    client.begin("query-one")
    first, second = rows("query-one", "a", "b"), rows("query-one", "c", "d")
    client.submit("block-one", first)
    accepted = client.submit("block-two", second)
    assert accepted["pending_blocks"] == 2
    reject(lambda: client.end())
    result = collect_all(client, 2)
    before = json.dumps(result, sort_keys=True)
    reject(lambda: client.collect("block-one"))
    reject(lambda: client.submit("new-block", rows("query-one", "a")))
    reject(lambda: client.submit("block-one", rows("query-one", "new")))
    invalid = [[], rows("wrong-query", "x"), rows("query-one", "x", "x")]
    null = rows("query-one", "x"); null[0]["record_json"] = None; invalid.append(null)
    negative = rows("query-one", "x"); negative[0]["row_id"] = -1; invalid.append(negative)
    wrong_type = rows("query-one", "x"); wrong_type[0]["row_id"] = "7"; invalid.append(wrong_type)
    mixed = rows("query-one", "x", "y"); mixed[1]["stage_id"] = "another-stage"; invalid.append(mixed)
    for i, values in enumerate(invalid):
        reject(lambda i=i, values=values: client.submit("invalid-" + str(i), values))
    client.submit("block-three", rows("query-one", "e"))
    third = collect_all(client, 1)
    assert before == json.dumps(result, sort_keys=True)
    client.end()
    client.begin("query-two")
    reject(lambda: client.submit("wrong-query", rows("query-one", "q")))
    client.submit("block-one", rows("query-two", "a", "b"))
    fourth = collect_all(client, 1)
    assert client.native_retained_output_checks >= 3
    client.end()
    all_rows = [r for block in (*result.values(), *third.values(), *fourth.values()) for r in block]
    envelopes = [json.loads(row["outcome_json"]) for row in all_rows]
    for row, envelope in zip(all_rows, envelopes, strict=True):
        assert envelope["status"] == "ok" and not envelope["model_entry_attempted"]
        assert envelope["worker"]["cuda_visible_devices"] == ""
        assert envelope["result"]["_occurrence_id"] == row["call_id"] and row["row_id"] == 7
        assert envelope["result"]["record"]["text"] == "same source\n\0中文"
    workers = {(value["worker"]["worker_pid"], value["worker"]["worker_uid"]) for value in envelopes}
    assert len(workers) == 1
    assert sorted({value["worker"]["completed_batches"] for value in envelopes}) == [1, 2, 3, 4]
    assert {json.loads(row["outcome_json"])["query_id"] for row in fourth["block-one"]} == {"query-two"}
    return dict(physical_blocks=4, rows=len(all_rows), pending_blocks_before_collect=2,
                worker_pids=[item[0] for item in workers], worker_instances=len(workers),
                python_result_unchanged=True, native_retained_output_checks=client.native_retained_output_checks,
                query_isolation=True, rejected_input_cases=len(invalid) + 3)


def _capture_stderr(pipe, path, report):
    def capture():
        try:
            with path.open("w", encoding="utf-8") as stream:
                for raw in iter(pipe.readline, b""):
                    stream.write(redact_text(raw.decode("utf-8", errors="replace")))
                    stream.flush()
        except BaseException as error:
            report.setdefault("stderr_capture_errors", []).append(
                redact_text(type(error).__name__ + ": " + str(error))
            )
        finally:
            pipe.close()
    writer = threading.Thread(target=capture, name="imlane-records-stderr", daemon=True)
    writer.start()
    return writer


def run_case(args, helper, report, function):
    scope = RecordScope(args, "cpython", helper, report, kernel=OWNED / "echo_udf.py")
    case = dict(function=function, real_model_requests=0)
    report["cases"].append(case)
    client = None
    try:
        scope.__enter__()
        process = subprocess.Popen([str(args.bridge_binary), str(scope.port), str(OWNED / "echo_udf.py"), function],
            env=scope.env, cwd=scope.root, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, start_new_session=True)
        scope.track_root(process)
        stderr_writer = _capture_stderr(process.stderr, scope.root / "bridge-stderr.log", report)
        scope.children.append((process, stderr_writer))
        def abort_owner():
            try:
                scope.close()
            finally:
                process.wait(timeout=3)
        def save_line(line):
            with (scope.root / "bridge-stdout.log").open("a") as stream:
                stream.write(redact_text(line) + "\n")
        client = RecordsBridge(process, abort_owner=abort_owner, timeout_s=5,
                               log=save_line)
        assert 0 < client.meta["native_task_threads"] <= 8
        if function == "infer_records":
            case.update(healthy(client))
            scope.snapshot()
            for pid in case["worker_pids"]:
                assert (pid, tools.process_start_ticks(pid)) in scope.known
            client.close()
            case["passed"] = True
        else:
            client.begin("fault-query")
            client.submit("fault-block", rows("fault-query", "first", "second"))
            try:
                for block in client.wait_ready(timeout_s=3):
                    client.collect(block)
            except (RuntimeError, TimeoutError) as error:
                case["observed_error"] = redact_text(str(error))
                case["deadline_triggered"] = isinstance(error, TimeoutError)
                assert case["deadline_triggered"] == (function == "raw_exception")
                expected_error = {
                    "reversed_records": "records occurrence/order differs",
                    "missing_record": "records result cardinality differs",
                    "malformed_records": "records success/error fields are inconsistent",
                    "error_records": "records worker reported an error; owner must stop",
                    "oversized_records": "records buffers exceed byte allowance before copy",
                    "raw_exception": "records native task deadline exceeded; owner must stop",
                }[function]
                assert str(error) == expected_error, "fault did not reach its intended records check"
                case["expected_check_observed"] = True
                if function == "raw_exception":
                    logs = list((scope.root / "state").rglob("*.log")) + [scope.root / "coordinator.log"]
                    assert any("injected raw Python exception before output semaphore" in path.read_text(errors="replace")
                               for path in logs if path.is_file()), "raw Python fault was not observed in author logs"
                    case["author_raw_exception_observed"] = True
                assert client._terminal
                try:
                    client.begin("must-not-reuse")
                except RuntimeError:
                    case["terminal_reuse_rejected"] = True
                else:
                    raise AssertionError("failed records owner was reused")
            else:
                raise AssertionError("fault UDF was delivered as a successful result")
            case["passed"] = True
    except BaseException as error:
        case["passed"] = False
        case["unexpected_error"] = redact_text(type(error).__name__ + ": " + str(error))
    finally:
        try:
            if client is not None and not client._closed:
                client.abort()
            scope.close()
        except BaseException as error:
            case["cleanup_notice"] = redact_text(type(error).__name__ + ": " + str(error))
        cleanup = next((item for item in reversed(report["cleanup"])
                        if item.get("namespace") == scope.namespace), {})
        case["cleanup_confirmed"] = tools.resource_state(scope)["resources_confirmed"]
        case["author_normal_exit"] = cleanup.get("passed", False)
        if (not case["cleanup_confirmed"] or cleanup.get("errors") or report.get("sampler_errors")
                or report.get("stderr_capture_errors")
                or (function == "infer_records" and not case["author_normal_exit"])):
            case["passed"] = False
        if function != "infer_records" and not case.get("expected_check_observed"):
            case["passed"] = False
    return case["passed"]


def _host_lease(artifact_root, commit):
    return acquire_runner_lease(
        artifact_root / HOST_SCOPE_DIR_NAME,
        config_fingerprint=HOST_SCOPE_FINGERPRINT,
        repository_commit=commit,
        recover_stale=False,
    )


def _settle_host_lease(lease, scopes, report):
    states = [tools.resource_state(scope) for scope in scopes]
    disposition = dict(status="held_unresolved", lease_path=str(lease.path), scope_resources=states,
                       recover_stale=False)
    report["host_lease"] = disposition
    if not all(item["resources_confirmed"] for item in states):
        report["passed"] = False
        return
    try:
        # RunnerLease validates its owner token; never remove a replacement lease.
        lease.release()
    except BaseException as error:
        disposition["release_error"] = redact_text(type(error).__name__ + ": " + str(error))
        report["passed"] = False
    else:
        disposition["status"] = "released"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    functions = ("infer_records", "reversed_records", "missing_record", "malformed_records",
                 "error_records", "oversized_records", "raw_exception")
    for name in ("author-root", "bridge-binary", "output", "artifact-root"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--case", action="append", choices=functions)
    args = parser.parse_args(argv)
    for name in ("author_root", "bridge_binary", "output", "artifact_root"):
        value = getattr(args, name).resolve()
        require_outside_git(value)
        setattr(args, name, value)
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    args.scopes = []
    args.stop_budget = tools.StopBudget(threading.Event())
    signal.signal(signal.SIGTERM, args.stop_budget.signal)
    signal.signal(signal.SIGINT, args.stop_budget.signal)
    helper = tools
    selected = args.case or list(functions)
    report = dict(schema="imlane.records.echo.v1", real_model_requests=0, gpu_requests=0,
                  author_commit=tools.AUTHOR, executor_count=1, runtime="cpython", selected_cases=selected,
                  cpu_affinity=sorted(os.sched_getaffinity(0)), cases=[], cleanup=[],
                  sources_sha256={path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                  for path in OWNED.iterdir() if path.suffix in (".py", ".cpp", ".hpp")},
                  supervisor_sha256=tools.digest(OWNED / "owner.py"),
                  bridge_sha256=tools.digest(args.bridge_binary))
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=CODE.parent, text=True).strip() if (CODE.parent / ".git").exists() else "staged-public-source"
    lease = _host_lease(args.artifact_root, commit)
    try:
        results = []
        for function in selected:
            args.stop_budget.check()
            results.append(run_case(args, helper, report, function))
            if not results[-1]:
                break
        report["passed"] = len(results) == len(selected) and all(results)
    except BaseException as error:
        report["passed"] = False
        report["unexpected_error"] = redact_text(type(error).__name__ + ": " + str(error))
    finally:
        with args.stop_budget.cleanup():
            for scope in reversed(args.scopes):
                try:
                    scope.close()
                except BaseException as error:
                    report.setdefault("final_cleanup_notices", []).append(
                        redact_text(type(error).__name__ + ": " + str(error))
                    )
            if report.get("stderr_capture_errors"):
                report["passed"] = False
            _settle_host_lease(lease, args.scopes, report)
            write_private_json(args.output / "report.json", report)
    print(json.dumps(dict(passed=report["passed"], cases=len(report["cases"]), real_model_requests=0)))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
