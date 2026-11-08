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
        # The baseline orchestration package contains comparison harnesses.
        violations = []
        for path in (CODE / "src").rglob("*.py"):
            relative = str(path.relative_to(CODE / "src"))
            if relative.startswith(("experiments/", "baselines/text/orchestration/")):
                continue
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
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
               'src.modalities.image.clip_batch_workers',
               'src.baselines.text.frameworks.kalypso_native.adapter'):
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

    def test_kalypso_query_script_and_experiment_cli_keep_help_and_required_arguments(self):
        environment = dict(os.environ, PYTHONPATH=str(CODE), PYTHONDONTWRITEBYTECODE="1")
        entries = ([sys.executable, "-B", str(CODE / "scripts/experiments/run_kalypso_sembench_query.py")],
                   [sys.executable, "-B", "-m", "src.experiments.kalypso_query_lifecycle"])
        results = []
        for entry in entries:
            with self.subTest(entry=entry[-1]):
                help_run = subprocess.run(entry + ["--help"], env=environment, text=True,
                                          capture_output=True, timeout=15)
                self.assertEqual(help_run.returncode, 0, help_run.stderr)
                for flag in ("--config", "--unit", "--output"):
                    self.assertIn(flag, help_run.stdout)
                missing = subprocess.run(entry, env=environment, text=True,
                                         capture_output=True, timeout=15)
                self.assertEqual(missing.returncode, 2, missing.stderr)
                self.assertIn("required: --config, --unit, --output", missing.stderr)
                results.append(help_run.stdout)
        self.assertEqual(results[0].split("\n\n", 1)[1], results[1].split("\n\n", 1)[1])
