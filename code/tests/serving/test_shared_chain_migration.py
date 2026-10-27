"""Shared worker/IPC relocation: identity, isolation, pipes and finite lifecycle."""

import base64
from copy import deepcopy
import importlib
import json
import multiprocessing
import os
from pathlib import Path
import pickle
import socket
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.method_workers import lotus_chain_batch as worker
from src.serving.backends import staged_generation_process as staged
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


CODE = Path(__file__).resolve().parents[2]
LEGACY_PROCESS = "src.baselines.text.frameworks.lotus_chain_engine_process"
PROCESS = "src.serving.backends.staged_generation_process"
LEGACY_WORKER = "src.baselines.text.frameworks.lotus_chain_batch"
WORKER = "src.execution_provider.method_workers.lotus_chain_batch"


def spawn_identity(queue, process_type, worker_type):
    queue.put((process_type is staged.ChainEngineProcess, worker_type is worker.FusedChainBatch,
               str(staged.EngineProcess.__module__)))


class MigrationTests(unittest.TestCase):
    def test_legacy_functions_types_and_pickle_globals_have_one_implementation(self):
        for old, new, names in (
            (LEGACY_PROCESS, PROCESS, ("ChainEngineProcess", "serve", "EngineProcess", "EngineSpec",
                                      "GenerationSpec", "MAX_MESSAGE_BYTES", "_encode", "_prepared")),
            (LEGACY_WORKER, WORKER, ("FusedChainBatch", "_check", "_phase_values", "_source_binding",
                                    "LotusRecordChain", "RecordOccurrence", "EngineSpec", "IDENTITY_FIELDS")),
        ):
            legacy, canonical = importlib.import_module(old), importlib.import_module(new)
            for name in names:
                with self.subTest(name=name):
                    value = getattr(canonical, name)
                    self.assertIs(getattr(legacy, name), value)
                    if callable(value):
                        self.assertIs(pickle.loads(("c" + old + "\n" + name + "\n.").encode()), value)
                        self.assertIs(pickle.loads(pickle.dumps(value)), value)

    def test_cold_import_orders_do_not_load_baseline_workers_core_tests_or_models(self):
        program = '''
import importlib, importlib.abc, pathlib, sys
sys.path.insert(0, sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        forbidden = ('src.scheduling', 'src.execution_provider.adapters', 'src.experiments',
                     'tests', 'scripts', 'ray', 'daft', 'vllm', 'torch', 'lotus')
        if any(fullname == p or fullname.startswith(p + '.') for p in forbidden):
            raise AssertionError('unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Guard())
first = importlib.import_module(sys.argv[2])
if sys.argv[2] in ('src.serving.backends.staged_generation_process',
                   'src.execution_provider.method_workers.lotus_chain_batch'):
    assert not any(n.startswith('src.baselines.text.frameworks') for n in sys.modules)
second = importlib.import_module(sys.argv[3])
assert getattr(first, sys.argv[4]) is getattr(second, sys.argv[4])
sync = importlib.import_module('src.serving.backends.vllm_sync_process')
assert sync.CODE_ROOT == pathlib.Path(sys.argv[1])
'''
        for old, new, name in ((LEGACY_PROCESS, PROCESS, "ChainEngineProcess"),
                               (LEGACY_WORKER, WORKER, "FusedChainBatch")):
            for order in ((old, new), (new, old)):
                with self.subTest(order=order):
                    run = subprocess.run([sys.executable, "-I", "-B", "-c", program,
                                          str(CODE), *order, name], capture_output=True, text=True, timeout=15)
                    self.assertEqual(run.returncode, 0, run.stderr)

    def test_native_adapters_share_worker_without_importing_core_policy(self):
        from src.baselines.text.frameworks import daft_lotus_chain, ray_data_lotus_chain
        self.assertIs(daft_lotus_chain.FusedChainBatch, worker.FusedChainBatch)
        self.assertIs(ray_data_lotus_chain.FusedChainBatch, worker.FusedChainBatch)

    def test_spawn_unpickles_the_same_shared_types(self):
        context = multiprocessing.get_context("spawn")
        queue = context.Queue()
        legacy = importlib.import_module(LEGACY_WORKER)
        child = context.Process(target=spawn_identity,
                                args=(queue, staged.ChainEngineProcess, legacy.FusedChainBatch))
        child.start()
        try:
            self.assertEqual(queue.get(timeout=10), (True, True, "src.serving.backends.vllm_sync_process"))
            child.join(timeout=5)
            self.assertEqual(child.exitcode, 0)
        finally:
            if child.is_alive():
                child.terminate()
                child.join(timeout=5)
            queue.close()
            queue.join_thread()

    def test_old_and_new_cli_preserve_error_and_descriptor_eof(self):
        environment = dict(os.environ, PYTHONPATH=str(CODE), PYTHONDONTWRITEBYTECODE="1",
                           CUDA_VISIBLE_DEVICES="", HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1")
        for module in (LEGACY_PROCESS, PROCESS):
            with self.subTest(module=module):
                invalid = subprocess.run([sys.executable, "-B", "-m", module], env=environment,
                                         capture_output=True, text=True, timeout=15)
                self.assertEqual(invalid.returncode, 1)
                self.assertEqual(invalid.stderr.strip(),
                                 "private engine worker requires an inherited result descriptor")
                read_fd, write_fd = os.pipe()
                try:
                    with subprocess.Popen([sys.executable, "-B", "-m", module, "--serve", str(write_fd)],
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            pass_fds=(write_fd,), env=environment) as child:
                        os.close(write_fd)
                        write_fd = None
                        try:
                            _, error = child.communicate(timeout=15)
                        except BaseException:
                            child.kill()
                            child.communicate()
                            raise
                    self.assertEqual(child.returncode, 0, error.decode())
                    self.assertEqual(os.read(read_fd, 1), b"")
                finally:
                    os.close(read_fd)
                    if write_fd is not None:
                        os.close(write_fd)

    def test_new_worker_state_pickle_is_readable_in_a_fresh_interpreter(self):
        value = object.__new__(worker.FusedChainBatch)
        value.control = Path("/tmp/CPU-fixture-control")
        value.received = {"occurrence-0"}
        payload = base64.b64encode(pickle.dumps(value)).decode()
        program = '''
import base64, pickle, sys
sys.path.insert(0, sys.argv[1])
from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch
value = pickle.loads(base64.b64decode(sys.argv[2]))
assert type(value) is FusedChainBatch
assert value.received == {'occurrence-0'}
'''
        run = subprocess.run([sys.executable, "-I", "-B", "-c", program, str(CODE), payload],
                             capture_output=True, text=True, timeout=15)
        self.assertEqual(run.returncode, 0, run.stderr)


class StageTransportTests(unittest.TestCase):
    def test_real_cpu_pipe_keeps_per_call_generation_identity_usage_and_close(self):
        from src.semantic_methods.lotus.records import identity
        class CPUProcess(staged.ChainEngineProcess):
            @staticmethod
            def _command(engine, result_fd):
                return [engine.model_python, "-m", "tests.serving.staged_generation_cpu_engine", str(result_fd)]
        with tempfile.TemporaryDirectory() as directory:
            process = CPUProcess(EngineSpec("/tmp/CPU-fixture-model", sys.executable, sys.executable),
                                 GenerationSpec(max_tokens=2), Path(directory), time.monotonic() + 8, 3)
            try:
                request = dict(identity("fixture-query", "occurrence-0", 0),
                               messages_json=json.dumps([dict(role="user", content="CPU fixture")]))
                for tokens in (4, 7):
                    result, = process.run_prepared([request], generation=GenerationSpec(max_tokens=tokens))
                    self.assertEqual(result["text"], str(tokens))
                    self.assertEqual(result["call_id"], request["call_id"])
                    self.assertEqual(result["total_tokens"], 4)
                report = process.close(2)
                self.assertTrue(report["process_exit_confirmed"])
                self.assertTrue(report["engine_api"]["engine_api_shutdown_confirmed"])
                self.assertIs(process.close(2), report)
            finally:
                process.close(2)

    def test_bad_reply_keeps_unknown_call_identity_and_cleanup_error(self):
        from src.semantic_methods.lotus.records import identity
        request = dict(identity("fixture-query", "occurrence-0", 0),
                       messages_json=json.dumps([dict(role="user", content="CPU fixture")]))
        process = object.__new__(staged.ChainEngineProcess)
        import threading
        process._lock = threading.RLock()
        process._broken = process._closed = False
        process.deadline = time.monotonic() + 1
        process.ready_report = {"engine_uid": "CPU-fixture"}
        with patch.object(process, "_exchange", return_value={"kind": "error", "error_type": "FixtureFailure"}), \
             patch.object(process, "close", side_effect=RuntimeError("CPU cleanup")) as close:
            result, = process.run_prepared([request], generation=GenerationSpec())
        self.assertTrue(process._broken)
        self.assertEqual(result["call_id"], request["call_id"])
        self.assertEqual(result["status"], "unknown")
        self.assertFalse(result["submission_confirmed"])
        self.assertIsNone(result["total_tokens"])
        self.assertEqual(result["engine_uid"], "CPU-fixture")
        close.assert_called_once()


class FixtureChain:
    """Finite method protocol; production method parsing/algorithms stay outside this fixture."""
    def __init__(self, stage_id="stage-0"):
        self.query_id = "fixture-query"
        self.stages = (SimpleNamespace(stage_id=stage_id, kind="map",
                                      method=SimpleNamespace(model_name="CPU-fixture"),
                                      generation=GenerationSpec()),)

    def run_batch(self, records, infer, **kwargs):
        from src.semantic_methods.lotus.record_chain import stage_identity
        prepared = [dict(stage_identity(self.query_id, row, self.stages[0].stage_id),
                         messages_json=json.dumps([dict(role="user", content="CPU fixture")])) for row in records]
        calls = infer(prepared, generation=self.stages[0].generation)
        if calls and calls[0].get("status") != "succeeded":
            failure = RuntimeError("CPU method failure")
            failure.chain_calls = calls
            raise failure
        return dict(records=records, calls=calls, stage_stats={}, columns=["text"])


class FixtureProcess:
    def __init__(self, *args):
        self.closes = 0
        self.calls = []

    def run_prepared(self, rows, *, generation):
        self.calls.extend(deepcopy(rows))
        return [dict({key: row[key] for key in worker.IDENTITY_FIELDS}, status="succeeded",
                     submission_confirmed=True, text="CPU result", prompt_tokens=3,
                     completion_tokens=1, total_tokens=4) for row in rows]

    def close(self, timeout=None):
        self.closes += 1
        return dict(status="closed", process_exit_confirmed=True, gpu_exit_confirmed=None)


class WorkerLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.control = Path(self.temporary.name)
        (self.control / "owner").write_text("CPU-token")
        self.record = worker.RecordOccurrence("occurrence-0", 0, "label", {"text": "CPU fixture"})
        self.chain = FixtureChain()
        self.decoder = patch.object(worker.LotusRecordChain, "from_dict", side_effect=lambda value: value)
        self.decoder.start()
        self.addCleanup(self.decoder.stop)

    def owner(self, **kwargs):
        value = worker.FusedChainBatch(self.chain,
                dict(model_path="/tmp/CPU-fixture-model", driver_python=sys.executable, model_python=sys.executable),
                [worker._source_binding(self.record)], self.control, time.monotonic() + 10, 3,
                socket.gethostname(), "CPU-token", **kwargs)
        value._engine_type = FixtureProcess
        return value

    def run_rows(self, owner):
        return owner.run_rows([dict(record_json=json.dumps(self.record.describe()))])

    def test_no_model_close_is_confirmed_without_constructing_a_process(self):
        owner = self.owner()
        self.assertIsNone(owner.engine)
        self.assertEqual(owner.close(), dict(status="closed", engine_api_shutdown_confirmed=True,
                                             process_exit_confirmed=True, gpu_exit_confirmed=True))
        result = self.run_rows(owner)
        self.assertEqual(result["error_type"], "RuntimeError")
        self.assertIsNone(owner.engine)

    def test_completed_source_keeps_calls_audit_and_closes_model_once(self):
        owner = self.owner()
        result = self.run_rows(owner)
        self.assertIsNone(result["error_type"])
        self.assertEqual(result["calls"][0]["total_tokens"], 4)
        self.assertEqual(result["records"], [self.record.describe()])
        self.assertEqual(owner.engine.closes, 1)
        audit = json.loads(owner._audit_path.read_text())
        self.assertTrue(audit["calls"][0]["submission_confirmed"])
        self.assertEqual(audit["calls"][0]["total_tokens"], 4)
        self.assertEqual(audit["phase_stage_ids"], ["stage-0"])

    def test_open_relation_phase_reuses_one_process_and_rejects_stage_reuse(self):
        owner = self.owner(close_on_source_complete=False)
        self.assertIsNone(self.run_rows(owner)["error_type"])
        process = owner.engine
        parents = {(self.record.row_id, self.record.source_position): (process.calls[0]["call_id"],)}
        with self.assertRaisesRegex(ValueError, "stage IDs"):
            owner.begin_relation_phase(self.chain, [worker._source_binding(self.record)],
                initial_parents=parents, columns=["text"], allow_nulls=False, source_exhausted=True)
        owner.begin_relation_phase(FixtureChain("stage-1"), [worker._source_binding(self.record)],
            initial_parents=parents, columns=["text"], allow_nulls=False, source_exhausted=True)
        self.assertIsNone(self.run_rows(owner)["error_type"])
        self.assertIs(owner.engine, process)
        self.assertEqual(len(process.calls), 2)
        self.assertEqual(process.closes, 0)
        self.assertEqual(json.loads(owner._audit_path.read_text())["calls"][0]["parent_call_ids"], list(next(iter(parents.values()))))
        owner.close()
        self.assertEqual(process.closes, 1)

    def test_failed_method_preserves_usage_cleanup_and_does_not_submit_queued_batch(self):
        owner = self.owner()
        class FailedProcess(FixtureProcess):
            def run_prepared(self, rows, *, generation):
                return [dict(row, status="failed", error_type="CPU failure", submission_confirmed=True,
                             prompt_tokens=3, completion_tokens=1, total_tokens=4)
                        for row in super().run_prepared(rows, generation=generation)]
        owner._engine_type = FailedProcess
        result = self.run_rows(owner)
        self.assertEqual(result["error_type"], "RuntimeError")
        self.assertEqual(result["calls"][0]["total_tokens"], 4)
        self.assertEqual(owner.engine.closes, 1)
        self.assertEqual(self.run_rows(owner), result)
        self.assertEqual(len(owner.engine.calls), 1)
        self.assertEqual(owner.engine.closes, 1)

    def test_cancellation_before_run_does_not_construct_a_process(self):
        owner = self.owner()
        (self.control / "stop").touch()
        result = self.run_rows(owner)
        self.assertEqual(result["error_type"], "InterruptedError")
        self.assertIsNone(owner.engine)

    def test_failed_allocation_keeps_cleanup_unknown(self):
        owner = self.owner()
        owner._engine_type = lambda *args: (_ for _ in ()).throw(RuntimeError("CPU allocation failed"))
        self.assertEqual(self.run_rows(owner)["error_type"], "RuntimeError")
        report = owner.close()
        self.assertEqual(report["status"], "unknown")
        self.assertIsNone(report["process_exit_confirmed"])
        self.assertIsNone(report["gpu_exit_confirmed"])
