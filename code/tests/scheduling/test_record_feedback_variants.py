"""CPU differential contracts against both retained historical feedback sources.

The target is the current exported source. Both oracles are resolved file by
file through native_baselines/MANIFEST.json, and every loaded oracle file is
SHA256 checked. No model, Ray, vLLM, database or network is instantiated.

The core delivery/profile/sampling nodes are compiled verbatim from their AST
to avoid importing unrelated runtime dependencies. These are method contracts;
the existing CPU IPC/lifecycle tests remain necessary integration coverage.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from contextlib import nullcontext
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import importlib
import importlib.abc
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import time
from types import ModuleType, SimpleNamespace
from typing import Any
import unittest


ROOT = Path(__file__).resolve().parents[3]
TARGET_SOURCE = ORACLE_SOURCE = ROOT


class _NoBytecodeLoader(importlib.abc.Loader):
    def __init__(self, path):
        self.path = path

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        module.__file__ = str(self.path)
        exec(compile(self.path.read_bytes(), str(self.path), "exec"), module.__dict__)


class SourceView(importlib.abc.MetaPathFinder):
    """Isolate ordinary Python classes; never overlay or mutate the source tree."""

    def __init__(self, source, label, variant=None):
        self.source, self.label = source, "_legacy_feedback_draft_" + label
        self.root = source / "experiments/reproduction/native_baselines"
        self.manifest = (None if variant is None else
                         json.loads((self.root / "MANIFEST.json").read_text())["variants"][variant])
        package = ModuleType(self.label)
        package.__path__ = [str(source / "code/src")]
        sys.modules[self.label] = package
        sys.meta_path.insert(0, self)
        self.adaptive = self.module("scheduling.submission_control.adaptive")
        self.models = self.module("scheduling.core.models")
        self.calibration = self.module("scheduling.runtime.calibration")

    def path(self, logical):
        if self.manifest is None:
            return self.source / logical
        relative = self.manifest["overrides"].get(logical)
        path = self.root / relative if relative is not None else self.source / logical
        expected = self.manifest["code_files_sha256"].get(logical)
        if expected is None or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise AssertionError("Frozen oracle bytes unavailable or changed: " + logical)
        return path

    def find_spec(self, fullname, path=None, target=None):
        if not fullname.startswith(self.label + "."):
            return None
        tail = fullname[len(self.label) + 1:].replace(".", "/")
        if (self.source / "code/src" / tail).is_dir():
            # Empty package shells avoid unrelated package initializers.
            spec = importlib.util.spec_from_loader(fullname, loader=None, is_package=True)
            spec.submodule_search_locations = [str(self.source / "code/src" / tail)]
            return spec
        location = self.path("code/src/" + tail + ".py")
        return importlib.util.spec_from_loader(fullname, _NoBytecodeLoader(location))

    def module(self, name):
        return importlib.import_module(self.label + "." + name)

    def core_contract_nodes(self):
        logical = "code/src/execution_provider/adapters/lotus_records_core.py"
        location = self.path(logical)
        tree = ast.parse(location.read_text(), filename=str(location))
        names = {"_measure", "resolve_core_profile", "_CoreRows", "_CoreFeedback"}
        optional = {"expand_core_profile"}
        nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))
                 and node.name in names | optional]
        if not names <= {node.name for node in nodes}:
            raise AssertionError("Production core node moved: adapt extraction explicitly")
        records = self.module("semantic_methods.lotus.records")
        observations = self.module("scheduling.runtime.observations")
        namespace = dict(__name__=self.label + "._core_nodes", Iterator=Iterator, Any=Any,
                         asdict=asdict, replace=replace, json=json, time=time,
                         nullcontext=nullcontext, CallLedger=records.CallLedger,
                         ResourceFeedbackConfig=self.adaptive.ResourceFeedbackConfig,
                         ResourceFeedbackController=self.adaptive.ResourceFeedbackController,
                         ServiceMetricsSnapshot=observations.ServiceMetricsSnapshot)
        selected = ast.Module(body=[ast.ImportFrom(module="__future__", names=[
            ast.alias(name="annotations")], level=0), *nodes], type_ignores=[])
        exec(compile(ast.fix_missing_locations(selected), str(location), "exec"), namespace)
        return SimpleNamespace(**namespace)


class Trace:
    """Scripted source observations, not a replacement admission algorithm."""

    def __init__(self, view, *, initial=3, maximum=15, total=100000,
                 computed=False, pairs=False, hint=None, options=None):
        self.view = view
        self.now = self.count = self.inputs = self.outputs = self.computed = self.cached = 0
        kw = dict(search_mode="geometric", window_s=1, min_samples=2,
                  min_remaining=4, exploration_s=100, max_changes=8, allow_exploration=True)
        if computed:
            kw.update(work_basis="computed", work_comparison="local_pairs" if pairs else "envelope")
        self.options = options or view.adaptive.ResourceFeedbackConfig(**kw)
        self.effective = getattr(self.options, "work_basis", "delivered") == "computed"
        self.c = view.adaptive.ResourceFeedbackController(maximum, initial, self.options,
                                                        clock=lambda: self.now)
        self.c.reset(initial, total, search_hint=hint)
        self.decisions = []

    def tick(self, n=10, *, iw=100, ow=4, cached=40, fresh=True, transition=0,
             waiting=0, kv=.1, pending=90000, pressure=None, backpressure=False):
        self.now += 1
        self.count += n
        self.inputs += n * iw
        self.outputs += n * ow
        if cached is not None:
            self.cached += n * cached
            self.computed += n * (iw - cached)
        fields = dict(completed_items_total=self.count, pending_items=pending,
                      memory_pressure=pressure, transition_pending=transition,
                      consumer_backpressure=backpressure,
                      completed_input_work_total=self.inputs,
                      completed_output_work_total=self.outputs)
        if self.effective:
            fields.update(completed_computed_input_work_total=None if cached is None else self.computed,
                          completed_cached_input_work_total=None if cached is None else self.cached)
        observation = self.view.models.AdmissionObservation(
            self.now, fresh, self.c.current_window, self.c.current_window, waiting, kv, **fields)
        decision = self.c.update(observation)
        self.decisions.append(asdict(decision))
        return decision

    def run(self, mode="gain", limit=200):
        counts = {}
        for _ in range(limit):
            window = self.c.current_window
            index = counts.get(window, 0)
            counts[window] = index + 1
            n = {1: 10, 2: 20, 3: 30, 6: 60, 12: 90, 15: 90}.get(window, 10)
            iw, cached = 100, 40
            if mode == "regression":
                n = 30 if window == 3 else 10
            elif mode == "noise":
                n = 10 if window == 3 else (8, 12, 9, 11)[index % 4]
            elif mode == "cache_only":
                n, cached = (10, 40) if window == 3 else (30, 90)
            elif mode == "pair_gain":
                iw, cached, n = ((100, 40, 20), (140, 60, 14), (120, 50, 16))[self.now % 3]
                if window == 6:
                    n *= 2
            elif mode == "pair_order_conflict":
                n = 30 if self.c._probe_candidate is not None else 20 if window == 6 else 10
            elif mode != "gain":
                raise AssertionError(mode)
            self.tick(n, iw=iw, cached=cached)
            if self.c._growth_stopped:
                return self.c.report()
        raise AssertionError("Original bounded exploration did not stop")


def project_legacy(actual, expected):
    """cfdc retains every existing field; ee39 adds explicit audit fields."""
    if isinstance(expected, dict):
        missing = set(expected) - set(actual)
        if missing:
            raise AssertionError("Removed legacy fields: " + repr(missing))
        return {key: project_legacy(actual[key], value) for key, value in expected.items()}
    if isinstance(expected, list):
        if len(actual) != len(expected):
            raise AssertionError("Different legacy list length")
        return [project_legacy(a, b) for a, b in zip(actual, expected)]
    return actual


class LegacyFeedbackContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.target = SourceView(TARGET_SOURCE, "target")
        cls.cfdc = SourceView(ORACLE_SOURCE, "cfdc", "core_original_dynamic")
        cls.ee39 = SourceView(ORACLE_SOURCE, "ee39", "core_local_window_pairs")

    @classmethod
    def tearDownClass(cls):
        for view in (cls.target, cls.cfdc, cls.ee39):
            sys.meta_path.remove(view)
            for name in tuple(sys.modules):
                if name == view.label or name.startswith(view.label + "."):
                    del sys.modules[name]

    def assert_trace_equal(self, target, oracle, *, legacy=False):
        self.assertEqual(target.decisions, oracle.decisions)
        expected, actual = oracle.c.report(), target.c.report()
        self.assertEqual(project_legacy(actual, expected) if legacy else actual, expected)

    def test_work_comparison_and_candidate_search_nodes_are_exact_ee39_ast(self):
        logical = "code/src/scheduling/submission_control/adaptive.py"
        names = {"_local_pair_evidence", "_GeometricSearch"}
        def nodes(view):
            tree = ast.parse(view.path(logical).read_text())
            return {node.name: ast.dump(node, include_attributes=False) for node in tree.body
                    if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names}
        self.assertEqual(set(nodes(self.target)), names)
        self.assertEqual(nodes(self.target), nodes(self.ee39))

    def test_current_feedback_defaults_have_no_implicit_exploration_cutoffs(self):
        cfg = self.target.adaptive.ResourceFeedbackConfig()
        self.assertEqual((cfg.exploration_s, cfg.min_remaining, cfg.max_changes), (None, 0, None))
        self.assertFalse(cfg.allow_exploration)
        for invalid in (dict(exploration_s=0), dict(exploration_s=float('inf')),
                        dict(min_remaining=-1), dict(min_remaining=True), dict(max_changes=0)):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.target.adaptive.ResourceFeedbackConfig(**invalid)

    def test_unbudgeted_scan_can_start_after_ten_seconds_with_fewer_than_sixty_four_pending(self):
        options = self.target.adaptive.ResourceFeedbackConfig(search_mode='geometric',
            window_s=1, min_samples=2, allow_exploration=True)
        trace = Trace(self.target, initial=4, maximum=8, total=60, options=options)
        trace.tick(6, pending=50)
        for _ in range(20):
            trace.tick(0, pending=50)
        for _ in range(8):
            trace.tick(2, pending=60-trace.count-2-trace.c.current_window)
            if trace.c.current_window == 8:
                break
        self.assertEqual(trace.c.current_window, 8)
        self.assertGreater(trace.now, 10)
        self.assertNotIn('exploration_budget', trace.c.reasons)
        self.assertNotIn('short_tail_or_fixed', trace.c.reasons)

    def test_unrequested_change_budget_does_not_stop_measured_growth(self):
        options = self.target.adaptive.ResourceFeedbackConfig(search_mode='geometric',
            window_s=1, min_samples=2, allow_exploration=True)
        trace = Trace(self.target, initial=1, maximum=2048, total=1000000000, options=options)
        for _ in range(200):
            window = trace.c.current_window
            trace.tick(window*2, pending=trace.c.total_items-trace.count-window*3)
            if trace.c._growth_stopped:
                break
        report = trace.c.report()
        self.assertEqual(report['recommended_window'], 2048)
        self.assertGreater(report['changes'], 8)
        self.assertNotIn('exploration_budget', report['reasons'])
        self.assertEqual(report['stop_reason'], 'sampled_range_complete')
        self.assertFalse(report['learned_global_optimum'])

    def test_short_work_without_comparison_is_censored_even_without_a_probe(self):
        for mode in ('local', 'geometric'):
            options = self.target.adaptive.ResourceFeedbackConfig(search_mode=mode,
                min_remaining=32, allow_exploration=True)
            trace = Trace(self.target, initial=3, maximum=6, total=3, options=options)
            trace.tick(0, pending=0)
            report = trace.c.report()
            self.assertEqual(report['changes'], 0)
            self.assertTrue(report['exploration_censored'])
            self.assertFalse(report['learned_global_optimum'])
            if mode == 'geometric':
                self.assertFalse(report['can_continue'])

    def test_missing_pending_work_is_unknown_and_zero_pending_is_not_a_platform(self):
        options = self.target.adaptive.ResourceFeedbackConfig(search_mode='geometric',
            window_s=1, min_samples=2, allow_exploration=True)
        trace = Trace(self.target, initial=3, maximum=6, total=3, options=options)
        self.assertEqual(trace.tick(0, pending=None).reason, 'missing_pending_feedback')
        self.assertFalse(trace.c._growth_stopped)
        self.assertEqual(trace.tick(0, pending=0).reason, 'no_pending_work')
        report = trace.c.report()
        self.assertTrue(report['exploration_censored'])
        self.assertEqual(report['incomplete_reason'], 'no_pending_work')
        self.assertFalse(report['learned_global_optimum'])

    def test_old_budgeted_hint_does_not_alias_unbudgeted_policy(self):
        old = self.target.adaptive.ResourceFeedbackConfig(search_mode='geometric', window_s=1,
            min_samples=2, min_remaining=64, exploration_s=10, max_changes=8, allow_exploration=True)
        current = replace(old, min_remaining=0, exploration_s=None, max_changes=None)
        identity = {name: dict(fixture=name) for name in
                    ('hardware', 'model', 'generation', 'execution', 'workload', 'measurement')}
        identity['measurement']['work_unit'] = 'rows'
        with tempfile.TemporaryDirectory() as directory:
            reports = []
            for options in (old, current, current):
                def execute(window, *, allow_exploration, search_hint):
                    trace = Trace(self.target, initial=window, maximum=6,
                        options=replace(options, allow_exploration=allow_exploration), hint=search_hint)
                    if allow_exploration:
                        trace.run()
                    return dict(status='completed', reuse_eligible=True, admission_feedback=trace.c.report())
                reports.append(self.target.calibration.run_admission_query(identity, execute,
                    initial_window=3, maximum=6, cache_dir=directory, options=options))
            self.assertEqual([report['cache_hit'] for report in reports], [False, False, True])
            self.assertNotEqual(reports[0]['saved_hint']['path'], reports[1]['saved_hint']['path'])

    def test_delivered_defaults_preserve_cfdc_decisions_existing_fields_and_state(self):
        for mode in ("gain", "regression", "noise", "cache_only"):
            for search_mode in ("local", "geometric"):
                with self.subTest(mode=mode, search_mode=search_mode):
                    traces = [Trace(v, maximum=15) for v in (self.target, self.cfdc)]
                    if search_mode == "local":
                        traces = [Trace(t.view, maximum=15, options=replace(
                            t.options, search_mode="local")) for t in traces]
                    for trace in traces:
                        trace.run(mode)
                    self.assert_trace_equal(*traces, legacy=True)
        self.assertEqual(self.target.adaptive.ResourceFeedbackConfig().work_basis, "delivered")
        self.assertEqual(self.target.adaptive.ResourceFeedbackConfig().work_comparison, "envelope")

    def test_computed_and_pairs_match_ee39_full_report_and_discriminating_outcomes(self):
        for mode, pairs, selected, accepted in (
            ("gain", False, 6, 1), ("regression", False, 3, 0),
            ("noise", False, 3, 0), ("cache_only", False, 3, 0),
            ("pair_gain", True, 6, 1), ("pair_order_conflict", True, 3, 0),
        ):
            with self.subTest(mode=mode, pairs=pairs):
                traces = [Trace(v, maximum=6, computed=True, pairs=pairs)
                          for v in (self.target, self.ee39)]
                reports = [trace.run(mode) for trace in traces]
                self.assert_trace_equal(*traces)
                self.assertEqual(reports[0]["recommended_window"], selected)
                self.assertEqual(reports[0]["accepted_steps"], accepted)
                self.assertFalse(reports[0]["learned_global_optimum"])

    def test_stale_missing_transition_backpressure_tail_and_fixed_controls_match(self):
        for pairs in (False, True):
            for mode in ("stale", "missing", "transition", "backpressure", "tail", "fixed"):
                with self.subTest(mode=mode, pairs=pairs):
                    traces = []
                    for view in (self.target, self.ee39):
                        trace = Trace(view, maximum=6, computed=True, pairs=pairs)
                        if mode == "fixed":
                            trace = Trace(view, maximum=6, options=replace(
                                trace.options, allow_exploration=False))
                        for _ in range(12):
                            trace.tick(cached=None if mode == "missing" else 40,
                                       fresh=mode != "stale", transition=int(mode == "transition"),
                                       pending=1 if mode == "tail" else 90000,
                                       backpressure=mode == "backpressure")
                        traces.append(trace)
                    self.assert_trace_equal(*traces)
                    self.assertEqual(traces[0].c.report()["accepted_steps"], 0)

    def test_continuation_context_drift_pressure_recovery_and_attempt_bounds(self):
        traces = []
        for view in (self.target, self.ee39):
            first = Trace(view, computed=True)
            first.run()
            self.assertEqual(first.c.report()["recommended_window"], 12)
            state = deepcopy(first.c.report()["search_state"])
            trace = Trace(view, initial=12, computed=True, hint=state)
            started = trace.c.started
            trace.tick(0, kv=.99)
            trace.tick(0, kv=.99)
            self.assertEqual(trace.c.current_window, 6)
            self.assertIsNotNone(trace.c._search.pressure_recovery)
            trace.run()
            self.assertEqual(trace.c.started, started)
            self.assertEqual(trace.c.report()["recommended_window"], 12)
            probes = [h["window"] for h in trace.c.history if h["reason"] == "performance_probe"]
            self.assertEqual(len(probes), len(set(probes)))
            self.assertLessEqual(trace.c.report()["changes"], trace.options.max_changes)
            traces.append(trace)
        self.assert_trace_equal(*traces)
        drifted = []
        for view in (self.target, self.ee39):
            first = Trace(view, maximum=6, computed=True)
            first.run()
            trace = Trace(view, initial=6, maximum=6, computed=True,
                          hint=deepcopy(first.c.report()["search_state"]))
            for _ in range(15):
                trace.tick(10 if trace.c.current_window == 3 else 30, cached=80)
                if trace.c._growth_stopped:
                    break
            self.assertIn("cached_work_changed", [h["reason"] for h in trace.c.history])
            drifted.append(trace)
        self.assert_trace_equal(*drifted)

    def test_local_pairs_match_exact_frozen_work_only_matching_and_prefix_rules(self):
        def point(work, rates, phase):
            return dict(control_phase_id=phase, windows=[dict(input=i, computed=c, output=o, rate=r)
                        for (i, c, o), r in zip(work, rates)])
        cases = [
            (point([(100, 60, 4), (140, 80, 4), (120, 70, 4)], [20, 14, 16], 0),
             point([(140, 80, 4), (120, 70, 4), (100, 60, 4)], [28, 32, 40], 1), "gain"),
            (point([(100, 80, 4)] * 3, [10] * 3, 0),
             point([(100, 20, 4)] * 3, [40] * 3, 1), None),
            (point([(100, 80, 4)] * 3, [10] * 3, 0),
             point([(100, 20, 4)] * 3, [5] * 3, 1), "regression"),
        ]
        for before, after, outcome in cases:
            expected = self.ee39.adaptive._local_pair_evidence(before, after, 0)
            actual = self.target.adaptive._local_pair_evidence(before, after, 0)
            self.assertEqual(actual, expected)
            self.assertEqual(actual["outcome"], outcome)
        before = point([(100, 60, 4)] * 3, [10] * 3, 0)
        after = point([(100, 60, 4)] * 3, [20] * 3, 1)
        for i, (a, b) in enumerate(zip(before["windows"], after["windows"])):
            a.update(completed_begin=i * 20, completed_end=(i + 1) * 20)
            b.update(completed_begin=i * 20, completed_end=(i + 1) * 20)
        expected = self.ee39.adaptive._local_pair_evidence(before, after, 0)
        self.assertEqual(self.target.adaptive._local_pair_evidence(before, after, 0), expected)
        self.assertEqual(expected["support_pairs"], 0)

    def test_profile_options_and_invalid_combinations_match_ee39(self):
        for view in (self.target, self.ee39):
            core = view.core_contract_nodes()
            engine, resources = SimpleNamespace(max_num_seqs=12), SimpleNamespace(max_active_batches=8)
            options = dict(kind="resource_feedback", search_mode="geometric",
                           work_basis="computed", work_comparison="local_pairs")
            profile = core.resolve_core_profile(engine, resources,
                                                dict(mode="async", max_inflight=3, admission=options))
            self.assertEqual(profile["max_inflight"], 3)
            self.assertEqual(profile["owner_max_inflight"], 8)
            self.assertEqual(profile["admission"]["work_comparison"], "local_pairs")
            for invalid in (dict(work_basis="computed"), dict(work_comparison="local_pairs"),
                            dict(work_basis="invented"), dict(work_comparison="invented")):
                with self.assertRaises(ValueError):
                    view.adaptive.ResourceFeedbackConfig(**invalid)

    def test_public_profile_names_expand_to_the_retained_experiment_options(self):
        core = self.target.core_contract_nodes()
        engine, resources = SimpleNamespace(max_num_seqs=64), SimpleNamespace(max_active_batches=64)
        for name, basis, comparison in (("delivered-work-scan", "delivered", "envelope"),
                                        ("computed-work-local-pairs", "computed", "local_pairs")):
            result = core.resolve_core_profile(engine, resources, name)
            self.assertEqual((result["mode"], result["initial_window"], result["owner_max_inflight"]),
                             ("async", 8, 64))
            self.assertTrue(result["stage_timing"])
            options = result["admission"]
            self.assertEqual({key: options[key] for key in (
                "poll_interval_s", "window_s", "min_samples", "min_remaining", "exploration_s",
                "max_changes", "max_candidate_attempts", "work_basis", "work_comparison")}, dict(
                poll_interval_s=.1, window_s=.25, min_samples=16, min_remaining=64,
                exploration_s=10., max_changes=8, max_candidate_attempts=2,
                work_basis=basis, work_comparison=comparison))
            self.assertFalse(result["performance_tuned"])
        self.assertNotIn("admission", core.resolve_core_profile(engine, resources))
        with self.assertRaises(ValueError):
            core.expand_core_profile("invented")

    def test_per_call_request_output_measurement_does_not_fabricate_cache_zero(self):
        for cached in (6, 0, None, -1, 11, True, 1.5, "absent"):
            for transfer in (None, {}, {"remote": True}):
                results = []
                for view in (self.target, self.ee39):
                    backend = view.module("serving.backends.vllm_async_engine")
                    common = view.module("serving.backends.vllm_engine")
                    records = view.module("semantic_methods.lotus.records")
                    converter = object.__new__(backend.VllmAsyncEngine)
                    converter.engine_uid = "CPU-no-model"
                    converter._llm = SimpleNamespace(get_tokenizer=lambda: None)
                    output = SimpleNamespace(request_id="request", finished=True,
                        prompt_token_ids=list(range(10)), num_cached_tokens=cached,
                        kv_transfer_params=transfer, outputs=[SimpleNamespace(
                            index=0, text="True", token_ids=[3], finish_reason="stop")])
                    if cached == "absent":
                        del output.num_cached_tokens
                    value = converter._convert(records.identity("cpu", "one", 0), output,
                        "request", list(range(10)), common.GenerationSpec(),
                        time.monotonic(), time.monotonic() + 60)
                    results.append({key: value.get(key) for key in (
                        "status", "prompt_tokens", "completion_tokens", "total_tokens",
                        "prompt_cached_tokens", "prompt_computed_tokens", "input_work_source")})
                with self.subTest(cached=cached, transfer=transfer):
                    self.assertEqual(*results)
                    valid = type(cached) is int and 0 <= cached <= 10 and not transfer
                    self.assertEqual(results[0]["prompt_cached_tokens"], cached if valid else None)
                    self.assertEqual(results[0]["prompt_computed_tokens"], 10 - cached if valid else None)
                    self.assertEqual(results[0]["status"], "succeeded")
                    self.assertEqual(results[0]["total_tokens"], 11)

    def test_delivery_prefix_counts_out_of_order_duplicates_and_invalid_work(self):
        for view in (self.target, self.ee39):
            nodes = view.core_contract_nodes()
            records = view.module("semantic_methods.lotus.records")
            for bad in ("unknown", "missing", "negative", "boolean", "sum"):
                stream = object.__new__(nodes._CoreRows)
                identities = [records.identity("cpu", "r" + str(i), i) for i in range(3)]
                stream.expected = {v["call_id"]: v for v in identities}
                stream.received = set()
                stream.completed_work = stream.completed_effective_work = (0, 0, 0)
                stream.owner = SimpleNamespace(_feedback=SimpleNamespace(
                    options=SimpleNamespace(work_basis="computed"), observe=lambda _: None))
                value = dict(identities[1], status="succeeded", prompt_tokens=10,
                             completion_tokens=1, prompt_computed_tokens=4, prompt_cached_tokens=6)
                stream._value(json.dumps(value))
                self.assertEqual(stream.completed_effective_work, (1, 4, 6))
                with self.assertRaises(ValueError):
                    stream._value(json.dumps(value))
                self.assertEqual(stream.completed_effective_work, (1, 4, 6))
                value.update(identities[0])
                if bad == "unknown":
                    value["status"] = "unknown"
                elif bad == "missing":
                    value.pop("prompt_computed_tokens")
                elif bad == "negative":
                    value["prompt_computed_tokens"] = -1
                elif bad == "boolean":
                    value["prompt_computed_tokens"] = True
                elif bad == "sum":
                    value["prompt_computed_tokens"] = 5
                stream._value(json.dumps(value))
                self.assertEqual(stream.completed_effective_work, (2, None, None))
                value.update(identities[2], status="succeeded", prompt_computed_tokens=4,
                             prompt_cached_tokens=6)
                stream._value(json.dumps(value))
                self.assertEqual(stream.completed_effective_work, (3, None, None))

    def test_sampler_requires_the_exact_same_delivered_prefix_and_generation(self):
        for view in (self.target, self.ee39):
            nodes = view.core_contract_nodes()
            for effective_count, session_id, generation_changed in (
                    (10, "session", False), (11, "session", False),
                    (10, "foreign", False), (10, "session", True)):
                feedback = object.__new__(nodes._CoreFeedback)
                feedback.generation = 1
                feedback.stream = SimpleNamespace(session=SimpleNamespace(session_id="session"))
                feedback.owner = SimpleNamespace(transport=None, _actors=SimpleNamespace(
                    model_ready=dict(engine_uid="CPU")))
                feedback.options = SimpleNamespace(poll_interval_s=.1)
                feedback.gpu_ids = []
                feedback.started = 0
                feedback.ref, feedback.ref_expired = object(), False
                feedback.progress = (10, 90, 0, False)
                feedback.completed_work = (10, 100, 10)
                feedback.completed_effective_work = (effective_count, 40, 60)
                feedback.samples = feedback.sample_errors = feedback.peak_native_running = 0
                feedback.provider = None
                snapshot = dict(engine_uid="CPU", observed_at_s=1, running=3, waiting=0,
                                kv_usage=.1, generation_tokens_total=10, sequence=1,
                                preemptions_total=0)
                def result():
                    if generation_changed:
                        feedback.generation += 1
                    return dict(session_id=session_id, ipc=dict(native_feedback=snapshot))
                feedback.future = SimpleNamespace(done=lambda: True, result=result)
                value = feedback.sample()
                self.assertEqual(feedback.sample_errors, 0)
                if session_id != "session" or generation_changed:
                    self.assertIsNone(value)
                else:
                    self.assertIsNotNone(value)
                    self.assertEqual(value.completed_computed_input_work_total,
                                     40 if effective_count == 10 else None)

    def test_admission_feature_identity_and_cross_query_hints_match_ee39(self):
        summaries = []
        for view in (self.target, self.ee39):
            identity = {key: {"CPU": "v1"} for key in (
                "hardware", "model", "generation", "execution", "workload", "measurement")}
            identity["measurement"]["work_unit"] = "model_calls"
            features = dict(work_unit="tokens", input_work=dict(mean=100, p95=100),
                            output_work=dict(limit=4), cache=dict(enabled=True, scope="model_owner"))
            summary = []
            with tempfile.TemporaryDirectory() as folder:
                for pairs in (False, True):
                    # Each policy iteration starts with a distinct raw task identity;
                    # the prior iteration intentionally renamed its workload.
                    identity["workload"] = dict(query_id="original-" + str(pairs),
                                                path="path-a", rows=137)
                    base = Trace(view, maximum=6, computed=True, pairs=pairs)
                    calls = []
                    def invoke(*, features_value=features, failed=False, pressure=False):
                        def execute(window, *, allow_exploration, search_hint):
                            calls.append((window, allow_exploration, deepcopy(search_hint)))
                            trace = Trace(view, initial=window, maximum=6,
                                          hint=search_hint, options=replace(
                                              base.options, allow_exploration=allow_exploration))
                            if pressure:
                                trace.tick(0, kv=.99)
                                trace.tick(0, kv=.99)
                            else:
                                trace.run()
                            return dict(status="failed" if failed else "completed",
                                        reuse_eligible=not failed, admission_feedback=trace.c.report())
                        return view.calibration.run_admission_query(identity, execute, initial_window=3,
                            maximum=6, cache_dir=Path(folder) / str(pairs), options=base.options,
                            workload_features=features_value)
                    original = deepcopy(identity)
                    changed = deepcopy(identity)
                    changed["workload"] = dict(query_id="new", path="other", rows=19)
                    self.assertNotEqual(view.calibration.identity_signature(identity),
                                        view.calibration.identity_signature(changed))
                    self.assertNotEqual(view.calibration.identity_signature(identity),
                        view.calibration.identity_signature(view.calibration.admission_identity(
                            identity, workload_features=features)))
                    self.assertEqual(view.calibration.admission_identity(identity, workload_features=features),
                                     view.calibration.admission_identity(changed, workload_features=features))
                    self.assertEqual(identity, original)
                    first = invoke()
                    identity["workload"] = changed["workload"]
                    second = invoke()
                    self.assertEqual([first["cache_hit"], second["cache_hit"]], [False, True])
                    self.assertEqual(second["result"]["admission_feedback"]["changes"], 0)
                    self.assertTrue(calls[-1][1])  # Fresh work validation even for a complete hint.
                    pressure_report = invoke(pressure=True)
                    third = invoke()
                    self.assertTrue(third["revalidate_prior_pressure"])
                    self.assertEqual(third["historical_evidence"]["search_state"],
                                     pressure_report["result"]["admission_feedback"]["search_state"])
                    different = deepcopy(features)
                    different["input_work"]["mean"] = 200
                    self.assertFalse(invoke(features_value=different)["cache_hit"])
                    for group in ("hardware", "model", "generation", "execution", "measurement"):
                        previous = deepcopy(identity[group])
                        identity[group]["version"] = "changed"
                        self.assertFalse(invoke()["cache_hit"])
                        identity[group] = previous
                    summary.append(dict(first=first["result"]["admission_feedback"],
                                        second=second["result"]["admission_feedback"],
                                        third=third["result"]["admission_feedback"], calls=calls))
                    with self.assertRaises(ValueError):
                        view.calibration.run_admission_query(identity, lambda *_: None,
                            initial_window=3, maximum=6, cache_dir=folder, options=base.options)
                with tempfile.TemporaryDirectory() as failed_folder:
                    def failed_execute(window, *, allow_exploration, search_hint):
                        return dict(status="failed", reuse_eligible=False)
                    view.calibration.run_admission_query(identity, failed_execute,
                        initial_window=3, maximum=6, cache_dir=failed_folder,
                        options=base.options, workload_features=features)
                    self.assertEqual(list(Path(failed_folder).iterdir()), [])
            summaries.append(summary)
        self.assertEqual(*summaries)


if __name__ == "__main__":
    unittest.main()
