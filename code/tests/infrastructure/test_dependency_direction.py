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
        compatibility = "baselines/text/products/project_static.py"
        violations = []
        for path in (CODE / "src").rglob("*.py"):
            relative = str(path.relative_to(CODE / "src"))
            if relative.startswith(("experiments/", "baselines/text/orchestration/")) or relative == compatibility:
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

    def test_observation_compatibility_imports_share_one_module_and_mutable_state(self):
        for old, new in (
            ("src.experiments.execution_timing", "src.observability.execution_timing"),
            ("src.experiments.image_reuse", "src.observability.model_reuse"),
            ("src.experiments.buffered_events", "src.observability.buffered_events"),
        ):
            with self.subTest(module=old):
                legacy, canonical = importlib.import_module(old), importlib.import_module(new)
                self.assertIs(legacy, canonical)
                with patch.object(legacy, "_migration_marker", "shared", create=True):
                    self.assertEqual(canonical._migration_marker, "shared")

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

    def test_request_identity_http_observation_and_map_values_are_not_duplicated(self):
        from src.experiments import request_identity as legacy_identity
        from src.observability import request_identity
        from src.experiments import native_http_observer
        from src.observability.httpx_observer import _observe_native_httpx_client
        from src.experiments.postgresql import map_direct
        from src.execution_provider.semantic_map import request_body
        self.assertIs(legacy_identity.request_identity, request_identity.request_identity)
        self.assertIs(native_http_observer._observe_native_httpx_client, _observe_native_httpx_client)
        self.assertIs(map_direct.request_body, request_body)

    def test_shared_timing_uses_one_recorder_and_preserves_its_event_schema(self):
        timing = importlib.import_module("src.observability.execution_timing")
        legacy = importlib.import_module("src.experiments.execution_timing")
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ, {"SEMLOOM_DETAILED_TIMING_DIR": directory}), \
             patch.object(timing, "_shared", None):
            recorder = timing.get_timing()
            try:
                self.assertIs(legacy.get_timing(), recorder)
                with legacy.timing_span("fixture.operation", query_id="fixture"):
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
               'src.observability.buffered_events', 'src.observability.httpx_observer',
               'src.infrastructure.request_budget', 'src.modalities.image.clip',
               'src.observability.request_gateway', 'src.semantic_methods.lotus.image_rm',
               'src.semantic_methods.lotus.image_backend', 'src.baselines.image.frameworks.daft_clip',
               'src.baselines.text.frameworks.native_calls',
               'src.baselines.text.frameworks.lotus_native_local_model',
               'src.baselines.text.frameworks.ray_data_pg_http'):
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

    def test_kalypso_compatibility_dispatch_restores_comparison_registry_after_success_or_error(self):
        from src.baselines.text.frameworks.kalypso_native import adapter
        from src.experiments import native_query_lifecycle as sessions
        original = sessions.BACKENDS, sessions.RESIDENT
        class Driver:
            def __init__(self, cfg, unit, path):
                self.cfg, self.unit, self.path = cfg, unit, path
        def run(queries, factory, output, **kwargs):
            self.assertIn("kalypso-native", sessions.BACKENDS)
            self.assertIsInstance(factory(output, None), Driver)
            return {"queries": [], "status": "passed"}
        with patch.object(adapter, "load_dependencies"), \
             patch.object(adapter, "NativeQueryDriver", Driver), \
             patch.object(sessions, "run_sequence", side_effect=run):
            result = adapter.run_attached({}, {"queries": []}, Path("unused-fixture-output"))
        self.assertEqual(result["status"], "passed")
        error = ValueError("fixture comparison failure")
        with patch.object(adapter, "load_dependencies"), \
             patch.object(adapter, "NativeQueryDriver", Driver), \
             patch.object(sessions, "run_sequence", side_effect=error):
            with self.assertRaises(ValueError) as caught:
                adapter.run_attached({}, {"queries": []}, Path("unused-fixture-output"))
        self.assertIs(caught.exception, error)
        self.assertIs(sessions.BACKENDS, original[0])
        self.assertIs(sessions.RESIDENT, original[1])
