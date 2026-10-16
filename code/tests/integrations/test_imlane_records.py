"""No-model records worker and supervisor-client behavior checks."""
import importlib.util
import io
import itertools
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

CODE = Path(__file__).resolve().parents[2]
DIRECTORY = CODE / "integrations/imlane_records"
sys.path.insert(0, str(CODE))
from integrations.imlane_records.client import RecordsBridge, RecordsRejected
from integrations.imlane_records import owner as owner_tools, verify
from src.infrastructure.runner_lease import (
    HOST_SCOPE_DIR_NAME, HOST_SCOPE_FINGERPRINT, LEASE_NAME, RunnerOwner, acquire_runner_lease,
)

try:
    import pyarrow as pa
except ImportError:
    pa = None

SERVER = r'''
import json,sys,time
def frame(value): print("IMLANE_RECORDS " + json.dumps(value), flush=True)
frame(dict(event="ready", protocol="imlane.records.v1"))
slow=False
for line in sys.stdin:
    op=json.loads(line)["op"]
    if op=="slow_poll":slow=True
    if op=="poll" and slow:time.sleep(10)
    if op=="partial":
        print("IMLANE_RECORDS {",end="",flush=True);time.sleep(10)
    elif op=="invalid":frame(dict(event="error",ok=False,owner_terminal=False,error="rejected before author submission"))
    else:frame(dict(event=op,ok=True,ready=[]))
    if op=="close":break
'''


class ClientChecks(unittest.TestCase):
    def setup_client(self, callback=None):
        process = subprocess.Popen([sys.executable, "-u", "-c", SERVER], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def cleanup():
            if process.poll() is None:
                process.kill()
            process.wait(timeout=2)
            for stream in (process.stdin, process.stdout, process.stderr):
                stream.close()
        self.addCleanup(cleanup)
        def abort():
            if callback:
                callback()
            if process.poll() is None:
                process.kill()
            process.wait(timeout=2)
        client = RecordsBridge(process, abort_owner=abort, timeout_s=.2)
        return client

    def test_pre_submission_rejection_allows_next_command(self):
        client = self.setup_client()
        with self.assertRaises(RecordsRejected):
            client.request("invalid")
        client.begin("query")
        client.close()
        client.close()

    def test_empty_ready_polls_do_not_hide_native_task_deadline(self):
        client = self.setup_client()
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            client.wait_ready(timeout_s=.1)
        self.assertLess(time.monotonic() - start, 1)
        self.assertTrue(client._closed)
        with self.assertRaises(RuntimeError):
            client.begin("another")

    def test_partial_response_cannot_block_past_rpc_deadline(self):
        client = self.setup_client()
        with self.assertRaises(TimeoutError):
            client.request("partial")
        self.assertIsNotNone(client.process.poll())

    def test_native_task_deadline_caps_a_blocked_poll_response(self):
        client = self.setup_client()
        client.request("slow_poll")
        client.timeout_s = 2
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            client.wait_ready(timeout_s=.05)
        self.assertLess(time.monotonic() - start, .5)

    def test_failed_owner_cleanup_is_not_marked_closed_and_can_retry(self):
        calls = []
        def callback():
            calls.append(True)
            if len(calls) == 1:
                raise RuntimeError("cleanup failure")
        client = self.setup_client(callback)
        with self.assertRaises(TimeoutError) as caught:
            client.wait_ready(timeout_s=.05)
        self.assertIn("cleanup remains unconfirmed", caught.exception.__notes__[0])
        self.assertFalse(client._closed)
        client.close()
        self.assertTrue(client._closed)
        self.assertEqual(len(calls), 2)


class OwnerChecks(unittest.TestCase):
    def test_import_needs_no_removed_tree_or_runtime_start(self):
        program = '''
import json,subprocess,sys
sys.path.insert(0, sys.argv[1])
def forbidden(*args, **kwargs):
    raise AssertionError("import attempted a runtime start")
subprocess.Popen = forbidden
from integrations.imlane_records import owner, verify
assert verify.tools is owner
assert not any(name in sys.modules for name in (
    'verify_imlane_async_calls', 'src.baselines.text.frameworks.imlane_calls',
    'src.execution_provider.adapters.sema_imlane', 'ray', 'lotus', 'vllm'))
print(json.dumps({'standalone_import': True}))
'''
        completed = subprocess.run(
            [sys.executable, "-I", "-B", "-c", program, str(CODE)],
            capture_output=True, text=True, timeout=5, check=True,
        )
        self.assertTrue(json.loads(completed.stdout)["standalone_import"])

    def test_start_ticks_unavailable_is_not_a_zero_identity(self):
        with patch.object(owner_tools, "process_start_time_ticks", return_value=123):
            self.assertEqual(owner_tools.process_start_ticks(42), 123)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "42").mkdir()
            (root / "42/stat").write_text("unreadable or malformed identity")
            with patch.object(owner_tools, "process_start_time_ticks", return_value=None), patch.object(owner_tools, "PROC_ROOT", root):
                with self.assertRaises(owner_tools.ProcessIdentityUnknown):
                    owner_tools.process_start_ticks(42)
                (root / "42/stat").unlink()
                with self.assertRaises(ProcessLookupError):
                    owner_tools.process_start_ticks(42)

    def test_reused_pid_cannot_signal_a_foreign_process(self):
        with patch.object(owner_tools, "process_start_ticks", return_value=124), patch.object(owner_tools.os, "kill") as kill:
            owner_tools.AuthorScope.signal_identity((42, 123), signal.SIGTERM)
            kill.assert_not_called()
        with patch.object(owner_tools, "process_start_ticks", return_value=123), patch.object(owner_tools.os, "kill") as kill:
            owner_tools.AuthorScope.signal_identity((42, 123), signal.SIGTERM)
            kill.assert_called_once_with(42, signal.SIGTERM)
        with patch.object(owner_tools, "process_start_ticks", side_effect=owner_tools.ProcessIdentityUnknown("fixture identity is inaccessible")), patch.object(owner_tools.os, "kill") as kill:
            error = owner_tools.AuthorScope.signal_identity((42, 123), signal.SIGTERM)
            self.assertEqual(error["type"], "ProcessIdentityUnknown")
            kill.assert_not_called()

    def test_shared_memory_scan_excludes_foreign_namespaces(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("ia-cpython-fixture-lane", ".ia-cpython-fixture-sem", "foreign-lane"):
                (root / name).write_bytes(b"scope")
            with patch.object(owner_tools, "SHM_ROOT", root):
                self.assertEqual(set(owner_tools.resources("ia-cpython-fixture")), {
                    "ia-cpython-fixture-lane", ".ia-cpython-fixture-sem",
                })
            self.assertEqual((root / "foreign-lane").read_bytes(), b"scope")

    def test_unconfirmed_shm_cleanup_remains_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(output=Path(directory), scopes=[],
                                   stop_budget=owner_tools.StopBudget(threading.Event()))
            helper = SimpleNamespace(resources=lambda _namespace: {})
            report = {"cleanup": []}
            scope = owner_tools.AuthorScope(args, "cpython", helper, report, kernel=DIRECTORY / "echo_udf.py")
            with patch.object(scope, "snapshot"), patch.object(scope, "stop_survivors", return_value=([], [], [])):
                with patch.object(helper, "resources", side_effect=RuntimeError("fixture scan failure")):
                    with self.assertRaises(RuntimeError):
                        scope.close()
                self.assertFalse(scope.closed)
                scope.close()
                self.assertTrue(scope.closed)
            self.assertEqual(scope.cleanup_attempts, 2)
            self.assertTrue(report["cleanup"][0]["errors"])
            self.assertEqual(report["cleanup"][1]["remaining_shm"], {})

    def test_host_lease_does_not_recover_an_existing_stale_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lease = acquire_runner_lease(
                root / HOST_SCOPE_DIR_NAME,
                config_fingerprint=HOST_SCOPE_FINGERPRINT, repository_commit="fixture-before",
                owner=RunnerOwner(socket.gethostname(), 2_000_000_000, "fixture-start", "fixture-token"),
                process_alive=lambda _pid: False,
            )
            path = root / HOST_SCOPE_DIR_NAME / LEASE_NAME
            original = path.read_bytes()
            try:
                with self.assertRaisesRegex(RuntimeError, "stale"):
                    verify._host_lease(root, "fixture-after")
                self.assertEqual(path.read_bytes(), original)
            finally:
                lease.release()

    def test_unknown_identity_preserves_shm_and_lease_until_retry(self):
        class Error(Exception):
            pass
        class NoSuchProcess(Error):
            pass
        state = {"unknown": True}
        def status():
            if state["unknown"]:
                raise Error("fixture status is inaccessible")
            return "zombie"
        psutil = SimpleNamespace(Error=Error, NoSuchProcess=NoSuchProcess,
                                 STATUS_ZOMBIE="zombie", STATUS_DEAD="dead",
                                 Process=lambda _pid: SimpleNamespace(is_running=lambda: True, status=status))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(output=root, scopes=[], stop_budget=owner_tools.StopBudget(threading.Event()))
            report = {"cleanup": [], "passed": False}
            scope = owner_tools.AuthorScope(args, "cpython", owner_tools, report, kernel=DIRECTORY / "echo_udf.py")
            scope.known.add((42, 123))
            shm = root / "shm"
            shm.mkdir()
            shared = shm / (scope.namespace + "-lane")
            shared.write_bytes(b"owned fixture")
            lease = verify._host_lease(root, "fixture")
            original_lease = lease.path.read_bytes()
            clock = itertools.count(step=3)
            with patch.dict(sys.modules, {"psutil": psutil}), patch.object(scope, "snapshot"), patch.object(owner_tools, "process_start_ticks", return_value=123), patch.object(owner_tools, "SHM_ROOT", shm), patch.object(owner_tools.time, "monotonic", side_effect=lambda: next(clock)), patch.object(owner_tools.os, "kill") as kill:
                with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
                    scope.close()
                self.assertFalse(scope.closed)
                self.assertIsNone(report["cleanup"][-1]["owned_survivors"])
                self.assertTrue(shared.exists())
                verify._settle_host_lease(lease, [scope], report)
                self.assertEqual(report["host_lease"]["status"], "held_unresolved")
                self.assertEqual(lease.path.read_bytes(), original_lease)
                state["unknown"] = False
                # Confirmed exit now permits namespaced SHM fallback. A forced
                # resource cleanup is a failed normal exit with settled resources.
                with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
                    scope.close()
                self.assertTrue(scope.closed)
                self.assertFalse(shared.exists())
                verify._settle_host_lease(lease, [scope], report)
                self.assertEqual(report["host_lease"]["status"], "released")
                self.assertFalse(lease.path.exists())
                kill.assert_not_called()

    def test_unknown_scope_resources_keep_the_owned_host_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            lease = verify._host_lease(Path(directory), "fixture")
            original = lease.path.read_bytes()
            scope = SimpleNamespace(namespace="fixture", closed=False, report={"cleanup": [
                dict(namespace="fixture", owned_survivors=None, remaining_shm=None, owned_threads_stopped=False),
            ]})
            report = {"passed": True}
            verify._settle_host_lease(lease, [scope], report)
            self.assertFalse(report["passed"])
            self.assertEqual(report["host_lease"]["status"], "held_unresolved")
            self.assertEqual(lease.path.read_bytes(), original)
            lease.release()  # This test owns no processes or shared memory.

    def test_failed_first_root_identity_is_not_an_empty_owned_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(output=root, scopes=[], stop_budget=owner_tools.StopBudget(threading.Event()))
            report = {"cleanup": [], "passed": False}
            scope = owner_tools.AuthorScope(args, "cpython", owner_tools, report, kernel=DIRECTORY / "echo_udf.py")
            process = SimpleNamespace(pid=42)
            with patch.object(owner_tools, "process_start_ticks", side_effect=owner_tools.ProcessIdentityUnknown("fixture first identity is inaccessible")), patch.object(scope, "snapshot"), patch.object(scope, "stop_survivors", return_value=([], [], [])), patch.object(owner_tools, "resources", return_value={}):
                with self.assertRaises(owner_tools.ProcessIdentityUnknown):
                    scope.track_root(process)
                with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
                    scope.close()
            self.assertFalse(scope.closed)
            self.assertEqual(report["cleanup"][-1]["unconfirmed_root_pids"], [42])
            self.assertIsNone(report["cleanup"][-1]["owned_survivors"])
            self.assertFalse(owner_tools.resource_state(scope)["resources_confirmed"])

    def test_failed_query_with_confirmed_resources_releases_host_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            lease = verify._host_lease(Path(directory), "fixture")
            scope = SimpleNamespace(namespace="fixture", closed=True, report={"cleanup": [
                dict(namespace="fixture", passed=False, owned_survivors=[], remaining_shm={}, owned_threads_stopped=True),
            ]})
            report = {"passed": False}
            verify._settle_host_lease(lease, [scope], report)
            self.assertEqual(report["host_lease"]["status"], "released")
            self.assertFalse(report["passed"])
            self.assertFalse(lease.path.exists())

    def test_success_with_confirmed_resources_releases_host_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            lease = verify._host_lease(Path(directory), "fixture")
            scope = SimpleNamespace(namespace="fixture", closed=True, report={"cleanup": [
                dict(namespace="fixture", passed=True, owned_survivors=[], remaining_shm={}, owned_threads_stopped=True),
            ]})
            report = {"passed": True}
            verify._settle_host_lease(lease, [scope], report)
            self.assertEqual(report["host_lease"]["status"], "released")
            self.assertTrue(report["passed"])
            self.assertFalse(lease.path.exists())

    def test_release_cannot_remove_another_owners_replacement_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            lease = verify._host_lease(Path(directory), "fixture")
            value = json.loads(lease.path.read_text())
            value["owner_token"] = "foreign-fixture-owner"
            lease.path.write_text(json.dumps(value))
            original = lease.path.read_bytes()
            report = {"passed": True}
            verify._settle_host_lease(lease, [], report)
            self.assertEqual(report["host_lease"]["status"], "held_unresolved")
            self.assertFalse(report["passed"])
            self.assertEqual(lease.path.read_bytes(), original)

    def test_bridge_stderr_is_redacted_before_it_reaches_disk(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bridge-stderr.log"
            pipe = io.BytesIO(b"worker detail\napi_key=fixture-sensitive-value\n")
            report = {}
            writer = verify._capture_stderr(pipe, path, report)
            writer.join(timeout=1)
            self.assertFalse(writer.is_alive())
            self.assertTrue(pipe.closed)
            self.assertNotIn("fixture-sensitive-value", path.read_text())
            self.assertIn("api_key=***", path.read_text())
            self.assertFalse(report.get("stderr_capture_errors"))


@unittest.skipIf(pa is None, "uses an explicitly supplied existing Arrow environment")
class EchoChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("records_echo_test", DIRECTORY / "echo_udf.py")
        cls.worker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.worker)

    def source(self):
        return pa.Table.from_pylist([dict(call_id=name, row_id=7, query_id="q", stage_id="s",
            record_json=json.dumps(dict(text="repeat\n\0中文"))) for name in ("a", "b")], schema=self.worker.INPUT)

    def test_repeat_source_has_distinct_occurrences_and_persistent_worker(self):
        source = self.source()
        first, second = self.worker.infer_records(source), self.worker.infer_records(source)
        a, b = [json.loads(table["outcome_json"][0].as_py()) for table in (first, second)]
        self.assertEqual(first["call_id"].to_pylist(), ["a", "b"])
        self.assertEqual(a["worker"]["worker_uid"], b["worker"]["worker_uid"])
        self.assertEqual(a["worker"]["completed_batches"] + 1, b["worker"]["completed_batches"])
        self.assertEqual(a["result"]["record"]["text"], "repeat\n\0中文")
        self.assertFalse(a["model_entry_attempted"])

    def test_nonnullable_arrow_fields_match_the_host_input_policy(self):
        source = self.source()
        schema = pa.schema([pa.field(field.name, field.type, nullable=False) for field in source.schema])
        table = pa.Table.from_arrays(source.columns, schema=schema)
        self.assertEqual(len(self.worker.infer_records(table)), 2)

    def test_input_types_nulls_query_and_occurrences_are_checked(self):
        source = self.source()
        invalid = [source.slice(0, 0), source.rename_columns(["different", *source.column_names[1:]])]
        for key, value in (("call_id", "a"), ("query_id", "other"), ("row_id", -1), ("record_json", None)):
            rows = source.to_pylist(); rows[1][key] = value
            invalid.append(pa.Table.from_pylist(rows, schema=self.worker.INPUT))
        for table in invalid:
            with self.subTest(table=table), self.assertRaises(ValueError):
                self.worker.infer_records(table)

    def test_error_envelope_is_explicit_and_disjoint_from_success(self):
        failed = self.worker.error_records(self.source())
        for row in failed.to_pylist():
            value = json.loads(row["outcome_json"])
            self.assertEqual(value["status"], "error")
            self.assertIsNone(value["result"])
            self.assertFalse(value["completion_known"])
            self.assertEqual(value["error_type"], "EchoFailure")


if __name__ == "__main__":
    unittest.main()
