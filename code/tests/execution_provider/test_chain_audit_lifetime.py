"""Shared chain audit lifetime: real methods/Core, CPU inference and native graph seams."""
from dataclasses import asdict
import json
from pathlib import Path
import socket
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import daft_lotus_chain as daft_chain
from src.baselines.text.frameworks import ray_data_lotus_chain as ray_chain
from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.method_workers import lotus_chain_batch as worker
from src.semantic_methods.lotus.record_results import ChainBatchResults, _source_binding, record_row
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.semantic_methods import test_lotus_chain_composition as fixture
from tests.semantic_methods import test_lotus_chain_relation_handoff as relation


class AuditLifetimeTests(unittest.TestCase):
    def setUp(self):
        import lotus
        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)
        temp = tempfile.TemporaryDirectory(prefix="chain-audit-cpu-")
        self.addCleanup(temp.cleanup)
        self.control = Path(temp.name)
        (self.control / "owner").write_text("cpu-owner")
        self.plan = fixture.chain_for("FM")
        self.rows = fixture.source_rows(size=3)
        self.spec = EngineSpec("CPU-fixture", sys.executable, sys.executable)
        self.resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=2, max_active_batches=1)
        self.created = []

    def batch_type(self, plan=None, close_report=None):
        plan, created = plan or self.plan, self.created
        class Engine:
            def __init__(self, *_args):
                self.infer = fixture.ResponseFixture(plan)
            def run_prepared(self, rows, *, generation):
                return self.infer(rows, generation=generation)
            def close(self, timeout=None):
                return close_report or dict(status="closed", cleanup_errors=[], process_exit_confirmed=True,
                    engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None)
        class Batch(worker.FusedChainBatch):
            _engine_type = Engine
            def __init__(inner, *args, **kwargs):
                super().__init__(*args, **kwargs)
                created.append(inner)
        return Batch

    def make_worker(self, enabled=False, **options):
        return self.batch_type()(self.plan.describe(), asdict(self.spec),
            [_source_binding(r) for r in self.rows], self.control, time.monotonic() + 30, 2,
            socket.gethostname(), "cpu-owner", release_completed_audit=enabled, **options)

    def test_success_rebind_keeps_envelope_disk_and_two_consumers(self):
        outcomes = []
        for enabled in (False, True):
            batch = self.make_worker(enabled, close_on_source_complete=False)
            retained = []
            encode = worker._json
            def encoding(value):
                retained.append(batch._audit_calls)
                return encode(value)
            with patch.object(worker, "_json", encoding):
                out = batch.run_rows([record_row(r) for r in self.rows])
            # Encoding and both consumers keep their own full data.
            consumers = [ChainBatchResults(self.rows, self.plan) for _ in range(2)]
            for consumer in consumers:
                consumer.accept(out)
                consumer.verify_success(source_exhausted=True)
                self.assertEqual(len(consumer.calls), len(out["calls"]))
            self.assertEqual(len(retained[0]), len(out["calls"]))
            disk = json.loads(batch._audit_path.read_text())
            self.assertEqual(len(disk["calls"]), len(out["calls"]))
            self.assertEqual(batch._audit_calls, [] if enabled else retained[0])
            outcomes.append(fixture.normalized(out))
        self.assertEqual(*outcomes)

    def test_dirty_unknown_close_keeps_audit_but_clean_close_releases(self):
        reports = [dict(status="unknown", cleanup_errors=[]),
                   dict(status="closed", cleanup_errors=["CPU cleanup failure"]),
                   dict(status="closed", cleanup_errors=[])]
        for report in reports:
            with self.subTest(report=report):
                batch = self.batch_type(close_report=report)(self.plan.describe(), asdict(self.spec),
                    [_source_binding(r) for r in self.rows], self.control, time.monotonic() + 30, 2,
                    socket.gethostname(), "cpu-owner", release_completed_audit=True)
                out = batch.run_rows([record_row(r) for r in self.rows])
                self.assertEqual(out["engine_close"], report)
                self.assertEqual(bool(batch._audit_calls), report != reports[-1])
                self.assertTrue(all(c["status"] == "succeeded" for c in out["calls"]))

    def test_encoding_cancel_expiry_or_failure_keeps_audit(self):
        for event in ("cancel", "expiry", "encode_error"):
            with self.subTest(event=event):
                (self.control / "stop").unlink(missing_ok=True)
                batch = self.make_worker(True, close_on_source_complete=False)
                encode = worker._json
                def encoding(value):
                    if event == "cancel":
                        (self.control / "stop").touch()
                    elif event == "expiry":
                        batch.deadline = time.monotonic() - 1
                    else:
                        raise ValueError("CPU encoding failure")
                    return encode(value)
                with patch.object(worker, "_json", encoding):
                    if event == "encode_error":
                        with self.assertRaisesRegex(ValueError, "encoding failure"):
                            batch.run_rows([record_row(r) for r in self.rows])
                    else:
                        batch.run_rows([record_row(r) for r in self.rows])
                self.assertGreater(len(batch._audit_calls), 0)

    def test_unknown_keeps_usage_and_failure_receipt_without_retry(self):
        batch = self.make_worker(True)
        infer = fixture.ResponseFixture(self.plan)
        def unknown(prepared, *, generation):
            values = infer(prepared, generation=generation)
            values[0].update(status="unknown", error_type="CPUUnconfirmed")
            return values
        engine = SimpleNamespace(run_prepared=unknown,
            close=lambda: dict(status="unknown", cleanup_errors=[]))
        batch.engine = engine
        first = batch.run_rows([record_row(r) for r in self.rows])
        self.assertIsNotNone(first["error_type"])
        self.assertTrue(batch.failed)
        self.assertTrue(any(c["status"] == "unknown" for c in batch._audit_calls))
        self.assertTrue(all(c["total_tokens"] == 14 for c in batch._audit_calls))
        self.assertEqual(batch.run_rows([record_row(r) for r in self.rows]), first)
        self.assertTrue(batch._audit_path.exists())

    def test_core_public_entry_keeps_results_usage_parents_and_relation_phases(self):
        for pattern in ("FM", "MFEFM"):
            outcomes = []
            for enabled in (False, True):
                with self.subTest(pattern=pattern, enabled=enabled):
                    plan = relation.relation_chain(pattern) if "E" in pattern else fixture.chain_for(pattern)
                    Batch = self.batch_type(plan)
                    Owner = type("CPUOwner", (SemLoomRecordChainCore,), {"_batch_type": Batch})
                    owner = Owner(self.spec, self.resources, control_dir=self.control,
                                  release_completed_audit=enabled)
                    with patch.object(EngineSpec, "validate_driver_environment", return_value={}):
                        stream = owner.evaluate(iter(self.rows), plan, expected=self.rows, query_id=plan.query_id)
                        try:
                            records = list(stream)
                        finally:
                            report = owner.close(timeout=3)
                    self.assertEqual(report["status"], "closed")
                    self.assertTrue(report["query_completed"])
                    self.assertEqual(report["core_usage"]["held_tasks"], 0)
                    batch = self.created[-1]
                    self.assertEqual(batch.release_completed_audit, enabled)
                    self.assertEqual(bool(batch._audit_calls), not enabled)
                    outcomes.append(fixture.normalized(dict(records=records, calls=owner.calls, columns=stream.columns)))
            self.assertEqual(*outcomes)

    def test_invalid_option_fails_at_all_public_boundaries(self):
        for value in (None, 0, 1, "true"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(TypeError, "release_completed_audit"):
                    SemLoomRecordChainCore(self.spec, self.resources, release_completed_audit=value)
                for adapter in (ray_chain, daft_chain):
                    with self.assertRaisesRegex(TypeError, "release_completed_audit"):
                        adapter.evaluate(None, self.plan, self.spec, self.resources,
                            expected=self.rows, query_id=self.plan.query_id, release_completed_audit=value)
        with self.assertRaisesRegex(ValueError, "fused worker"):
            SemLoomRecordChainCore(self.spec, self.resources, execution="record_local_sections",
                                  release_completed_audit=True)

    def test_ray_public_graph_receives_option_and_shared_worker(self):
        import ray
        import ray.data
        for enabled in (False, True):
            captured = []
            class Dataset:
                def map_batches(inner, cls, **kwargs):
                    captured.append((cls, kwargs))
                    return inner
            with patch.object(ray, "__version__", ray_chain.RAY_VERSION), \
                 patch.object(ray, "is_initialized", return_value=True), \
                 patch.object(ray, "nodes", return_value=[{"Alive": True}]), \
                 patch.object(EngineSpec, "validate_driver_environment", return_value={}):
                stream = ray_chain.evaluate(Dataset(), self.plan, self.spec, self.resources,
                    expected=self.rows, query_id=self.plan.query_id, control_dir=self.control,
                    release_completed_audit=enabled)
                cls, kwargs = captured[0]
                self.assertIs(cls, worker.FusedChainBatch)
                self.assertEqual(kwargs["fn_constructor_kwargs"]["release_completed_audit"], enabled)
                # Instantiate the captured native constructor with only inference replaced.
                batch = self.batch_type()(**kwargs["fn_constructor_kwargs"])
                out = batch.run_rows([record_row(r) for r in self.rows])
                self.assertEqual(bool(batch._audit_calls), not enabled)
                self.assertTrue(out["calls"])
                self.assertEqual(kwargs["max_concurrency"], 1)
                self.assertEqual(kwargs["num_gpus"], 1)
                stream.close()

    def test_daft_public_graph_receives_option_and_shared_worker(self):
        import daft
        for enabled in (False, True):
            native_types = []
            class Expression:
                def alias(self, _name): return self
                def not_null(self): return self
            class Frame:
                column_names = ["record_json"]
                def schema(self): return {"record_json": SimpleNamespace(dtype=daft.DataType.string())}
                def select(self, _value): return self
                def where(self, _value): return self
            def native_class(**options):
                self.assertEqual(options["gpus"], 1)
                self.assertTrue(options["use_process"])
                self.assertEqual(options["max_concurrency"], 1)
                def decorate(cls):
                    native_types.append(cls)
                    return lambda: SimpleNamespace(run=lambda _col: Expression())
                return decorate
            with patch.object(daft_chain, "_native_runner"), \
                 patch.object(daft, "__version__", daft_chain.DAFT_VERSION), \
                 patch.object(daft, "cls", native_class), \
                 patch.object(daft.method, "batch", lambda **_: lambda fn: fn), \
                 patch.object(daft, "col", lambda _: Expression()), \
                 patch.object(daft_chain, "FusedChainBatch", self.batch_type()), \
                 patch.object(EngineSpec, "validate_driver_environment", return_value={}):
                stream = daft_chain.evaluate(Frame(), self.plan, self.spec, self.resources,
                    expected=self.rows, query_id=self.plan.query_id, control_dir=self.control,
                    release_completed_audit=enabled)
                native = native_types[0]()
                self.assertEqual(native.kernel.release_completed_audit, enabled)
                values = daft.Series.from_pylist([json.dumps(r.describe()) for r in self.rows])
                out = json.loads(native.run(values).to_pylist()[0])
                self.assertEqual(bool(native.kernel._audit_calls), not enabled)
                self.assertTrue(out["calls"])
                stream.close()

    def test_native_relation_phase_callbacks_keep_option(self):
        import daft
        import ray
        import ray.data
        plan = relation.relation_chain("FEM")
        segments = plan.relation_segments()
        for adapter in (ray_chain, daft_chain):
            captured = []
            sentinel = object()
            def relation_rows(chain, rows, open_phase, control, deadline, cancelled, **options):
                try:
                    for i, segment in enumerate(segments):
                        open_phase(i, segment, self.rows, initial_parents={(r.row_id, r.source_position): () for r in self.rows},
                            columns=["passage"], allow_nulls=True)
                finally:
                    import shutil
                    shutil.rmtree(control)
                return sentinel
            def graph(*args, **kwargs):
                captured.append(kwargs["release_completed_audit"])
                return None
            class Frame:
                column_names = ["record_json"]
                def schema(self): return {"record_json": SimpleNamespace(dtype=daft.DataType.string())}
            with patch.object(EngineSpec, "validate_driver_environment", return_value={}), \
                 patch.object(ray, "__version__", ray_chain.RAY_VERSION), \
                 patch.object(ray, "is_initialized", return_value=True), \
                 patch.object(ray, "nodes", return_value=[{"Alive": True}]), \
                 patch.object(ray.data, "from_items", return_value=Frame()), \
                 patch.object(daft, "__version__", daft_chain.DAFT_VERSION), \
                 patch.object(daft, "from_pylist", return_value=Frame()), \
                 patch.object(adapter, "_graph", graph), \
                 patch("src.baselines.text.frameworks.lotus_chain_native_relation.NativeExtractRelationRows", relation_rows):
                result = adapter.evaluate(Frame(), plan, self.spec, self.resources,
                    expected=self.rows, query_id=plan.query_id, control_dir=self.control,
                    release_completed_audit=True)
                self.assertIs(result, sentinel)
                self.assertEqual(captured, [True, True])
