"""Runtime dependency direction and shared helper state; no models or services."""

import ast
import importlib
import json
import os
from pathlib import Path
import pickle
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


CODE = Path(__file__).resolve().parents[2]


class DependencyDirectionTests(unittest.TestCase):
    def test_runtime_sources_do_not_import_tests_scripts_or_experiment_entry_points(self):
        # The baseline orchestration package and two explicit legacy comparison
        # entry points are harnesses; their callers choose a comparison run.
        violations = []
        for path in (CODE / "src").rglob("*.py"):
            relative = str(path.relative_to(CODE / "src"))
            if relative.startswith(("experiments/", "baselines/text/orchestration/")):
                continue
            tree = ast.parse(path.read_text())
            cli_compatibility = set()
            if relative == "baselines/text/frameworks/kalypso_native/adapter.py":
                cli_compatibility = {id(n) for fn in tree.body if isinstance(fn, ast.FunctionDef)
                                     and fn.name in {"main", "run_attached"} for n in ast.walk(fn)}
            for node in ast.walk(tree):
                if id(node) in cli_compatibility:
                    continue
                names = ([node.module or ""] if isinstance(node, ast.ImportFrom)
                         else [item.name for item in node.names] if isinstance(node, ast.Import) else [])
                for name in names:
                    if name.startswith(("src.experiments", "experiments", "tests", "scripts", "code.tests", "code.scripts")):
                        violations.append((relative, node.lineno, name))
                if isinstance(node, ast.Call):
                    call = node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id if isinstance(node.func, ast.Name) else ""
                    if call in {"import_module", "__import__", "run_module", "run_path", "spec_from_file_location", "Popen", "run", "check_output"}:
                        for value in (n.value for arg in node.args for n in ast.walk(arg)
                                      if isinstance(n, ast.Constant) and isinstance(n.value, str)):
                            if re.search(r"(?:^|/)(?:tests|scripts)/|^(?:tests|scripts|src\.experiments)\.", value):
                                violations.append((relative, node.lineno, value))
        self.assertEqual(violations, [])

    def test_request_claim_errors_and_old_pickle_globals_keep_the_same_identity(self):
        canonical = importlib.import_module("src.infrastructure.request_budget")
        cell = importlib.import_module("src.experiments.cell_budget")
        ledger = importlib.import_module("src.experiments.attempt_ledger")
        for owner, name in ((cell, "Reservation"), (cell, "ClaimedUnit"),
                            (ledger, "BudgetError"), (ledger, "BudgetExhausted")):
            with self.subTest(name=name):
                self.assertIs(getattr(owner, name), getattr(canonical, name))
                value = pickle.loads(("c" + owner.__name__ + "\n" + name + "\n.").encode())
                self.assertIs(value, getattr(canonical, name))
        claim = canonical.ClaimedUnit(canonical.Reservation("fixture", 7, 1), None)
        self.assertEqual(claim.reserve("a" * 64), 7)
        with self.assertRaises(canonical.BudgetExhausted):
            claim.reserve("b" * 64)

    def test_shared_timing_uses_one_recorder_and_preserves_its_event_schema(self):
        timing = importlib.import_module("src.observability.execution_timing")
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ, {"SEMLOOM_DETAILED_TIMING_DIR": directory}), \
             patch.object(timing, "_shared", None):
            recorder = timing.get_timing()
            try:
                self.assertIs(timing.get_timing(), recorder)
                with timing.timing_span("fixture.operation", query_id="fixture"):
                    pass
            finally:
                timing.close_timing()
            events = [json.loads(line) for line in recorder.path.read_text().splitlines()]
        self.assertEqual([item["event"] for item in events], ["span_started", "span_ended"])
        self.assertTrue(all(item["schema"] == "semloom.execution_timing.v1" for item in events))
        self.assertEqual(events[0]["span_id"], events[1]["span_id"])

    def test_cold_runtime_imports_and_httpx_fixture_work_with_experiment_imports_blocked(self):
        program = '''
import importlib, importlib.abc, os, sys
sys.path.insert(0, sys.argv[1])
os.environ.pop('SEMLOOM_DETAILED_TIMING_DIR', None)
os.environ.pop('SEMLOOM_IMAGE_REUSE_DIR', None)
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        forbidden = ('src.experiments', 'tests', 'scripts', 'torch', 'vllm', 'ray', 'daft', 'lotus')
        if any(fullname == p or fullname.startswith(p + '.') for p in forbidden):
            raise AssertionError('unexpected runtime dependency: ' + fullname)
sys.meta_path.insert(0, Guard())
for module in ('src.observability.execution_timing', 'src.observability.model_reuse',
               'src.observability.native_query_timing',
               'src.observability.buffered_events', 'src.observability.httpx_observer',
               'src.infrastructure.request_budget', 'src.modalities.image.clip',
               'src.semantic_methods.lotus.image_rm',
               'src.semantic_methods.lotus.image_backend', 'src.baselines.image.frameworks.daft_clip',
               'src.modalities.image.clip_batch_workers'):
    importlib.import_module(module)
from src.observability.execution_timing import timing_span
with timing_span('disabled.fixture'):
    pass
from src.observability.model_reuse import observe_model
assert observe_model(object(), role='fixture', device='cpu') is None
import httpx
from src.observability.httpx_observer import _observe_native_httpx_client
class Budget:
    def reserve(self, digest): return 1
events = []
endpoint = 'http://localhost/v1/chat/completions'
with _observe_native_httpx_client(Budget(), events.append, endpoint):
    with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={})),
                      trust_env=False) as client:
        assert client.post(endpoint, json={'messages': ['fixture']}).status_code == 200
assert [e['event'] for e in events] == ['request', 'http_started', 'http_response', 'http_finished']
'''
        run = subprocess.run([sys.executable, "-I", "-B", "-c", program, str(CODE)],
                             capture_output=True, text=True, timeout=20)
        self.assertEqual(run.returncode, 0, run.stderr)

    def test_old_and_new_kalypso_cli_help_do_not_load_the_author_runtime(self):
        environment = dict(os.environ, PYTHONPATH=str(CODE), PYTHONDONTWRITEBYTECODE="1")
        results = []
        for module in ("src.baselines.text.frameworks.kalypso_native.adapter",
                       "src.experiments.kalypso_query_lifecycle"):
            run = subprocess.run([sys.executable, "-B", "-m", module, "--help"],
                                 env=environment, text=True, capture_output=True, timeout=15)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertIn("--config", run.stdout)
            self.assertIn("--unit", run.stdout)
            results.append(run.stdout)
        self.assertEqual(results[0].split("\n\n", 1)[1], results[1].split("\n\n", 1)[1])

    def test_kalypso_dispatch_keeps_default_backends_on_success_or_error(self):
        from src.baselines.text.frameworks.kalypso_native import adapter
        from src.experiments import native_query_lifecycle as sessions
        original = sessions.BACKENDS, sessions.RESIDENT
        cfg = dict(native_launch='author_defaults', model='CPU-fixture', port=18103)
        class Driver:
            def __init__(self, cfg, unit, path):
                self.cfg, self.unit, self.path = cfg, unit, path
        def run(queries, factory, output, **kwargs):
            self.assertNotIn("kalypso-native", sessions.BACKENDS)
            self.assertIsInstance(factory(output, None), Driver)
            return {"queries": [], "status": "passed"}
        with patch.object(adapter, "load_dependencies"), \
             patch.object(adapter, "NativeQueryDriver", Driver), \
             patch.object(sessions, "run_sequence", side_effect=run):
            result = adapter.run_attached(cfg, {"queries": []}, Path("unused-fixture-output"))
        self.assertEqual(result["status"], "passed")
        error = ValueError("fixture comparison failure")
        with patch.object(adapter, "load_dependencies"), \
             patch.object(adapter, "NativeQueryDriver", Driver), \
             patch.object(sessions, "run_sequence", side_effect=error):
            with self.assertRaises(ValueError) as caught:
                adapter.run_attached(cfg, {"queries": []}, Path("unused-fixture-output"))
        self.assertIs(caught.exception, error)
        self.assertIs(sessions.BACKENDS, original[0])
        self.assertIs(sessions.RESIDENT, original[1])

    def test_kalypso_observer_setup_failure_does_not_register_a_backend(self):
        from src.baselines.text.frameworks.kalypso_native import adapter
        from src.experiments import native_query_lifecycle as sessions
        from src.observability import native_query_timing
        original = sessions.BACKENDS, sessions.RESIDENT
        error = ValueError('fixture observer setup failure')
        cfg = dict(native_launch='author_defaults', model='CPU-fixture', port=18103)
        with patch.object(sessions, 'BACKENDS', original[0]), \
             patch.object(sessions, 'RESIDENT', original[1]), \
             patch.object(adapter, 'load_dependencies'), \
             patch.object(native_query_timing, 'ObservationStore', side_effect=error):
            with self.assertRaises(ValueError) as caught:
                adapter.run_attached(cfg, {'queries': []}, Path('unused-fixture-output'))
            self.assertIs(caught.exception, error)
            self.assertEqual((sessions.BACKENDS, sessions.RESIDENT), original)

    def test_overlapping_kalypso_sessions_keep_default_backend_support(self):
        from concurrent.futures import ThreadPoolExecutor
        import threading
        from src.baselines.text.frameworks.kalypso_native import adapter
        from src.experiments import native_query_lifecycle as sessions
        original = sessions.BACKENDS, sessions.RESIDENT
        entered = {name: threading.Event() for name in ('first', 'second')}
        release = {name: threading.Event() for name in entered}
        class Driver:
            def __init__(self, cfg, unit, directory):
                self.name = unit['name']
            def start(self):
                entered[self.name].set()
                if not release[self.name].wait(5):
                    raise TimeoutError('fixture session was not released')
                return {'engine_uid': self.name, 'physical_model': False}
            def query(self, spec, emit):
                emit('result_ready')
                return {'status': 'completed'}
            def close(self):
                return {'status': 'closed'}
        cfg = dict(native_launch='author_defaults', model='CPU-fixture', port=18103)
        query = dict(query_id='fixture', task='fixture', phase='measured')
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(sessions, 'BACKENDS', original[0]), \
             patch.object(sessions, 'RESIDENT', original[1]), \
             patch.object(adapter, 'load_dependencies'), \
             patch.object(adapter, 'NativeQueryDriver', Driver), \
             ThreadPoolExecutor(max_workers=2) as pool:
            try:
                first = pool.submit(adapter.run_attached, cfg,
                    {'name': 'first', 'queries': [query]}, Path(directory) / 'first')
                self.assertTrue(entered['first'].wait(5))
                second = pool.submit(adapter.run_attached, cfg,
                    {'name': 'second', 'queries': [query]}, Path(directory) / 'second')
                self.assertTrue(entered['second'].wait(5))
                release['first'].set()
                self.assertEqual(first.result(timeout=5)['status'], 'completed')
                release['second'].set()
                self.assertEqual(second.result(timeout=5)['status'], 'completed')
            finally:
                for event in release.values():
                    event.set()
            self.assertEqual((sessions.BACKENDS, sessions.RESIDENT), original)
