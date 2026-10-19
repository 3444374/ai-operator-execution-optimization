"""Cascade owner callbacks over real private JSON and permanent CPU ledgers.

Semantic-bound, author, GPU and query callbacks are explicit fixtures here.
Original cascade semantics are checked separately; no child or socket is used.
"""

from contextlib import ExitStack
from copy import deepcopy
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


_GUARDS = []
OPTIONAL_PROBE_DENIALS = []


def setUpModule():
    class NoSocket(socket.socket):
        def __init__(self, *args, **kwargs):
            raise AssertionError("NoSocket: owned cascade tests cannot create sockets")

    def no_run(command, *args, **kwargs):
        if command == "lscpu":
            OPTIONAL_PROBE_DENIALS.append("optional NumPy lscpu denied without a child")
            raise OSError("optional CPU probe denied")
        raise AssertionError("NoPopen: owned cascade tests cannot run subprocesses")

    for guard in (
        patch.dict(os.environ, HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
                   LITELLM_LOCAL_MODEL_COST_MAP="True", CUDA_VISIBLE_DEVICES="-1"),
        patch("subprocess.Popen", side_effect=AssertionError("NoPopen: no author/model child")),
        patch("subprocess.run", side_effect=no_run),
        patch("socket.socket", NoSocket),
        patch("socket.socketpair", side_effect=AssertionError("NoSocket: no socketpair")),
        patch("socket.create_connection", side_effect=AssertionError("NoSocket: no connection")),
    ):
        guard.start()
        _GUARDS.append(guard)


def tearDownModule():
    for guard in reversed(_GUARDS):
        guard.stop()


class OwnedCascadeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from integrations.imlane_records.owner import StopBudget
        from src.baselines.common.private_artifacts import write_private_json
        from src.experiments import lotus_records_cascade
        from src.experiments.attempt_ledger import AttemptBudget, BudgetError, BudgetExhausted
        from src.experiments.cell_budget import CellBudgetLedger

        cls.runner, cls.StopBudget = lotus_records_cascade, StopBudget
        cls.AttemptBudget, cls.BudgetError, cls.BudgetExhausted = AttemptBudget, BudgetError, BudgetExhausted
        cls.Ledger, cls.private_write = CellBudgetLedger, staticmethod(write_private_json)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="owned-cascade-cpu-", dir="/tmp")
        self.addCleanup(temporary.cleanup)
        self.root, self.sequence = Path(temporary.name), 0
        self.events, self.guards = [], []
        self.required = 10  # The method fixture declares N=4, S=2, Q=2N+S.
        self.config = dict(query_id="fixture-cascade", helper_model_name="fixture/helper", max_calls=128)
        self.relation = SimpleNamespace(records=tuple(object() for _ in range(4)))
        self.method, self.generations, self.resources, self.cascade_args = object(), object(), object(), object()
        self.engines = dict(main=object(), helper=SimpleNamespace(
            validate_driver_environment=Mock(side_effect=lambda: self.events.append("helper_driver"))))
        self.owner = dict(
            **{key: str(self.root / key) for key in ("author_root", "bridge_binary", "artifact_root", "method_python")},
            owner_resource_limits=dict(cpu_cores=2, rss_bytes=1024**3, shm_bytes=1024**2),
            model_environment=dict(CUDA_VISIBLE_DEVICES="fixture-device"),
            budget=dict(path=str(self.root / "budget.sqlite"), id="fixture.cascade", limit=128, unit_id="cascade"),
        )
        self.budget = self.AttemptBudget("fixture.cascade", 128)
        self.deadline = time.time() + 3600
        self.ledger = self.Ledger.create(Path(self.owner["budget"]["path"]), self.budget, deadline_utc=self.deadline)
        self.ledger.reserve_unit("cascade", self.required)
        self.snapshot = self.Ledger.snapshot
        self.calls = dict(
            schema="semloom.lotus_cascade_calls.v1", max_calls=128,
            records=[dict(call_id="fixture-helper", model_role="helper", algorithm_stage="helper_score",
                          status="succeeded", prompt_tokens=5, completion_tokens=1, total_tokens=6)],
            usage=dict(prompt_tokens=5, completion_tokens=1, total_tokens=6,
                       known_prompt_tokens=5, known_completion_tokens=1,
                       unknown_prompt_tokens_calls=0, unknown_completion_tokens_calls=0),
            stages=[dict(name="helper_score", expected=1, recorded=1)], events=[],
        )
        self.outcome = dict(schema="semloom.lotus_helper_cascade.v2", query_id="fixture-cascade",
                            status="completed", calls=self.calls)
        self.primary = self.finish_error = self.write_error = None
        self.gpu_confirmed = self.lease_valid = True
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.method_preflight = self.stack.enter_context(patch.object(
            self.runner, "_method_preflight", side_effect=self.fixture_method_bound))
        self.preflight = self.stack.enter_context(patch.object(self.runner, "preflight", wraps=self.runner.preflight))
        self.owner_preflight = self.stack.enter_context(patch.object(
            self.runner, "preflight_owner", side_effect=self.fixture_owner_preflight))
        self.guard_factory = self.stack.enter_context(patch.object(
            self.runner, "AuthorRunGuard", side_effect=self.fixture_guard))
        self.query = self.stack.enter_context(patch.object(self.runner, "run_query", side_effect=self.fixture_query))
        self.factory = self.stack.enter_context(patch.object(
            self.runner, "_make_backend", side_effect=lambda **_: self.events.append("backend_factory")))
        self.write = self.stack.enter_context(patch.object(
            self.runner, "write_private_json", side_effect=self.fixture_write))

    def fixture_method_bound(self, *args):
        self.events.append("method_bound")
        return dict(max_possible_calls=self.required, fixture_pure_method_bound=True)

    def fixture_owner_preflight(self, *args, **kwargs):
        self.events.append("owner_preflight")
        return dict(fixture=True)

    def fixture_guard(self, owner, args):
        self.events.append("guard")
        self.assertIs(owner, self.current_owner)
        self.assertEqual(args.output, self.output)
        self.assertEqual(args.scopes, [])
        self.assertIsInstance(args.stop_budget, self.StopBudget)
        self.assertEqual(args.owner_resource_limits, owner["owner_resource_limits"])
        for key in ("author_root", "bridge_binary", "artifact_root", "method_python"):
            self.assertEqual(getattr(args, key), Path(owner[key]))
        guard = Mock()

        def finish():
            self.events.append("gpu_final")
            if self.finish_error is not None:
                raise self.finish_error
            return dict(fixture=True, gpu_exit_confirmed=self.gpu_confirmed,
                        external_lease_valid=self.lease_valid, external_host_lease_released_by_worker=False)

        guard.finish.side_effect = finish
        self.guards.append(guard)
        return guard

    def fixture_query(self, config, relation, method, generations, resources, engines, args,
                      author_args, model_environment, *, cancelled):
        self.events.append("query")
        for actual, expected in ((config, self.config), (relation, self.relation), (method, self.method),
                                 (generations, self.generations), (resources, self.resources),
                                 (engines, self.engines), (args, self.cascade_args),
                                 (model_environment, self.current_owner["model_environment"])):
            self.assertIs(actual, expected)
        self.assertEqual(author_args.output, self.output)
        self.assertIs(cancelled, self.guards[-1])
        state = self.snapshot(self.ledger)
        unit = next(row for row in state["units"] if row["unit_id"] == self.current_owner["budget"]["unit_id"])
        self.assertEqual(unit["claimed"], 1)
        self.runner._make_backend(fixture=True)
        if self.primary is not None:
            self.primary.query_report = deepcopy(self.outcome)
            raise self.primary
        return deepcopy(self.outcome)

    def fixture_write(self, path, report):
        self.events.append("write_report")
        if self.write_error is not None:
            raise self.write_error
        self.private_write(path, report)

    def run_worker(self, owner=None):
        self.sequence += 1
        self.output = self.root / ("run-" + str(self.sequence))
        self.output.mkdir(mode=0o700)
        self.current_owner = self.owner if owner is None else owner
        handlers = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        try:
            return self.runner.run_worker(self.config, self.relation, self.method, self.generations,
                self.resources, self.engines, self.cascade_args, self.output, self.current_owner)
        finally:
            self.assertEqual({number: signal.getsignal(number) for number in handlers}, handlers)

    def read_report(self):
        from integrations.imlane_records.chain_runtime import read_json

        path = self.output / "query.json"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        return read_json(path)

    def fail_query(self):
        self.outcome["status"] = "failed"
        self.calls["records"] += [
            dict(call_id="fixture-main-failed", model_role="main", algorithm_stage="threshold_sample",
                 status="failed", prompt_tokens=3, completion_tokens=1, total_tokens=4),
            dict(call_id="fixture-main-unknown", model_role="main", algorithm_stage="fallback",
                 status="unknown", prompt_tokens=None, completion_tokens=None, total_tokens=None),
        ]
        self.calls["usage"].update(prompt_tokens=None, completion_tokens=None, total_tokens=None,
            known_prompt_tokens=8, known_completion_tokens=2, unknown_prompt_tokens_calls=1,
            unknown_completion_tokens_calls=1)
        self.primary = RuntimeError("declared cascade query fixture failure")

    def assert_original_calls(self, report):
        self.assertIs(type(report["calls"]), dict)
        self.assertEqual(report["calls"], self.calls)
        self.assertFalse(report["external_host_lease_released_by_worker"])

    def test_cascade_q_is_claimed_once_between_public_preflight_and_backend_factory(self):
        report = self.run_worker()
        self.assertEqual(self.events, ["method_bound", "method_bound", "owner_preflight", "helper_driver",
                                      "guard", "query", "backend_factory", "gpu_final", "write_report"])
        self.assertEqual(self.method_preflight.call_count, 2)
        self.preflight.assert_called_once_with(self.config, self.relation, self.method, self.generations,
            self.resources, self.engines, self.cascade_args, self.owner)
        self.owner_preflight.assert_called_once_with(self.owner, self.engines["main"], chain=False, cascade=True)
        self.assertEqual(report["permanent_budget"]["units"], [
            dict(unit_id="cascade", first_attempt=1, requests=10, claimed=1)])
        self.assert_original_calls(report)
        self.assertEqual(report, self.read_report())
        with self.assertRaises(self.BudgetError):
            self.run_worker()
        self.factory.assert_called_once_with(fixture=True)
        self.assertEqual(self.snapshot(self.ledger)["allocated_requests"], 10)

    def test_unit_covering_two_roles_but_not_sample_is_rejected_and_remains_charged(self):
        self.ledger.reserve_unit("two-roles-only", 8)
        owner = deepcopy(self.owner)
        owner["budget"]["unit_id"] = "two-roles-only"
        with self.assertRaisesRegex(ValueError, "cannot cover this complete query"):
            self.run_worker(owner)
        self.factory.assert_not_called()
        self.query.assert_not_called()
        state = self.snapshot(self.ledger)
        self.assertEqual(state["allocated_requests"], 18)
        self.assertEqual(state["units"][-1]["claimed"], 1)
        with self.assertRaises(self.BudgetError):
            self.ledger.claim_unit("two-roles-only")
        self.assertEqual(self.read_report()["status"], "failed")

    def test_predefined_threshold_bound_without_sample_claims_exact_two_n(self):
        self.required = 8  # N=4, predefined thresholds make S=0.
        self.ledger.reserve_unit("fixed-thresholds", 8)
        owner = deepcopy(self.owner)
        owner["budget"]["unit_id"] = "fixed-thresholds"
        report = self.run_worker(owner)
        self.assertEqual(report["permanent_budget"]["units"][-1]["requests"], 8)
        self.assertEqual(report["permanent_budget"]["units"][-1]["claimed"], 1)

    def test_empty_cascade_still_claims_one_existing_unit(self):
        self.required, self.relation.records = 0, ()
        self.calls["records"], self.calls["stages"] = [], []
        self.calls["usage"] = dict(prompt_tokens=0, completion_tokens=0, total_tokens=0)
        self.ledger.reserve_unit("empty", 1)
        owner = deepcopy(self.owner)
        owner["budget"]["unit_id"] = "empty"
        report = self.run_worker(owner)
        self.assert_original_calls(report)
        self.assertEqual(report["permanent_budget"]["units"][-1],
                         dict(unit_id="empty", first_attempt=11, requests=1, claimed=1))
        with self.assertRaises(self.BudgetError):
            self.ledger.claim_unit("empty")

    def test_missing_wrong_or_unreserved_unit_never_reaches_backend(self):
        for changes in (dict(path=str(self.root / "missing.sqlite")), dict(id="wrong.identity"),
                        dict(limit=129), dict(unit_id="absent")):
            owner = deepcopy(self.owner)
            owner["budget"].update(changes)
            with self.subTest(changes=changes), self.assertRaises(self.BudgetError):
                self.run_worker(owner)
            self.assertEqual(self.read_report()["status"], "failed")
        self.assertFalse((self.root / "missing.sqlite").exists())
        self.query.assert_not_called()
        self.factory.assert_not_called()
        self.assertEqual(self.snapshot(self.ledger)["units"][0]["claimed"], 0)
        self.assertTrue(all(guard.finish.call_count == 1 for guard in self.guards))

    def test_used_unit_cannot_restart_a_cascade_or_refund_failed_attempts(self):
        self.ledger.claim_unit("cascade")
        with self.assertRaises(self.BudgetError):
            self.run_worker()
        self.query.assert_not_called()
        self.factory.assert_not_called()
        self.assertEqual(self.read_report()["permanent_budget"]["allocated_requests"], 10)

    def test_expired_unit_never_reaches_backend(self):
        with patch("src.experiments.cell_budget.time.time", return_value=self.deadline + 1), \
             self.assertRaises(self.BudgetExhausted):
            self.run_worker()
        self.query.assert_not_called()
        self.factory.assert_not_called()
        self.assertEqual(self.snapshot(self.ledger)["units"][0]["claimed"], 0)
        self.assertEqual(self.read_report()["status"], "failed")

    def test_method_rejection_before_owner_does_not_claim_unit_or_construct_backend(self):
        self.method_preflight.side_effect = ValueError("declared pure method rejection")
        with self.assertRaisesRegex(ValueError, "pure method rejection"):
            self.run_worker()
        self.preflight.assert_not_called()
        self.guard_factory.assert_not_called()
        self.query.assert_not_called()
        self.assertEqual(self.snapshot(self.ledger)["units"][0]["claimed"], 0)

    def test_repeated_public_preflight_rejection_reports_failure_without_claim(self):
        self.preflight.side_effect = ValueError("declared public preflight rejection")
        with self.assertRaisesRegex(ValueError, "public preflight rejection"):
            self.run_worker()
        self.guard_factory.assert_not_called()
        self.factory.assert_not_called()
        self.assertEqual(self.snapshot(self.ledger)["units"][0]["claimed"], 0)
        self.assertEqual(self.read_report()["status"], "failed")

    def test_guard_rejection_cannot_claim_or_start_backend(self):
        self.guard_factory.side_effect = RuntimeError("declared owner guard rejection")
        with self.assertRaisesRegex(RuntimeError, "owner guard rejection"):
            self.run_worker()
        self.query.assert_not_called()
        self.factory.assert_not_called()
        self.assertEqual(self.snapshot(self.ledger)["units"][0]["claimed"], 0)
        self.assertEqual(self.read_report()["status"], "failed")

    def test_full_cascade_bound_must_fit_global_budget_before_owner_preflight(self):
        owner = deepcopy(self.owner)
        owner["budget"]["limit"] = 9
        with self.assertRaisesRegex(ValueError, "global cascade exceeds"):
            self.run_worker(owner)
        self.owner_preflight.assert_not_called()
        self.guard_factory.assert_not_called()
        self.factory.assert_not_called()
        self.assertEqual(self.snapshot(self.ledger)["units"][0]["claimed"], 0)
        self.assertEqual(self.read_report()["status"], "failed")

    def test_failed_unknown_cascade_calls_keep_known_usage_primary_and_permanent_charge(self):
        self.fail_query()
        with self.assertRaises(RuntimeError) as caught:
            self.run_worker()
        self.assertIs(caught.exception, self.primary)
        report = self.read_report()
        self.assert_original_calls(report)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 8)
        self.assertIsNone(report["calls"]["usage"]["prompt_tokens"])
        self.assertEqual({row["status"] for row in report["calls"]["records"]},
                         {"succeeded", "failed", "unknown"})
        self.assertEqual(report["permanent_budget"]["allocated_requests"], 10)
        with self.assertRaises(self.BudgetError):
            self.ledger.claim_unit("cascade")

    def test_final_gpu_or_lease_unknown_fails_completed_query_with_calls_and_budget(self):
        for name in ("gpu_confirmed", "lease_valid"):
            self.gpu_confirmed = self.lease_valid = True
            setattr(self, name, False)
            unit_id = "final-" + name
            self.ledger.reserve_unit(unit_id, 10)
            owner = deepcopy(self.owner)
            owner["budget"]["unit_id"] = unit_id
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, "remains unconfirmed"):
                self.run_worker(owner)
            report = self.read_report()
            self.assert_original_calls(report)
            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["permanent_budget"]["units"][-1]["claimed"], 1)

    def test_final_gpu_error_keeps_primary_calls_and_budget_snapshot(self):
        self.fail_query()
        self.finish_error = OSError("declared GPU final fixture failure")
        with self.assertRaises(RuntimeError) as caught:
            self.run_worker()
        self.assertIs(caught.exception, self.primary)
        report = self.read_report()
        self.assert_original_calls(report)
        self.assertEqual(report["permanent_budget"]["allocated_requests"], 10)
        self.assertTrue(any("physical exit check also failed" in note for note in self.primary.__notes__))

    def test_budget_snapshot_error_keeps_primary_and_never_skips_gpu_or_report(self):
        self.fail_query()
        with patch.object(self.Ledger, "snapshot", side_effect=OSError("declared snapshot fixture failure")), \
             self.assertRaises(RuntimeError) as caught:
            self.run_worker()
        self.assertIs(caught.exception, self.primary)
        self.guards[0].finish.assert_called_once_with()
        report = self.read_report()
        self.assert_original_calls(report)
        self.assertTrue(report["physical_exit"]["gpu_exit_confirmed"])
        self.assertEqual(self.snapshot(self.ledger)["allocated_requests"], 10)
        self.assertTrue(any("budget receipt also failed" in note for note in self.primary.__notes__))

    def test_snapshot_error_after_success_attaches_cascade_calls_and_gpu_receipt(self):
        with patch.object(self.Ledger, "snapshot", side_effect=OSError("declared snapshot fixture failure")), \
             self.assertRaisesRegex(OSError, "snapshot fixture failure") as caught:
            self.run_worker()
        report = self.read_report()
        self.assert_original_calls(report)
        self.assertEqual(report, caught.exception.query_report)
        self.assertTrue(report["physical_exit"]["gpu_exit_confirmed"])
        self.assertEqual(report["status"], "failed")

    def test_report_write_failure_keeps_primary_calls_and_both_final_receipts(self):
        self.fail_query()
        self.write_error = OSError("declared report fixture failure")
        with self.assertRaises(RuntimeError) as caught:
            self.run_worker()
        self.assertIs(caught.exception, self.primary)
        report = self.primary.query_report
        self.assert_original_calls(report)
        self.assertTrue(report["physical_exit"]["gpu_exit_confirmed"])
        self.assertEqual(report["permanent_budget"]["allocated_requests"], 10)
        self.assertEqual(report["report_error_type"], "OSError")
        self.write.assert_called_once_with(self.output / "query.json", report)

    def test_all_final_receipt_failures_keep_primary_and_still_attempt_report(self):
        self.fail_query()
        self.finish_error = OSError("declared GPU final fixture failure")
        self.write_error = OSError("declared report fixture failure")

        def broken_snapshot(*args):
            self.events.append("snapshot_failure")
            raise OSError("declared snapshot fixture failure")

        with patch.object(self.Ledger, "snapshot", side_effect=broken_snapshot), \
             self.assertRaises(RuntimeError) as caught:
            self.run_worker()
        self.assertIs(caught.exception, self.primary)
        self.assert_original_calls(self.primary.query_report)
        self.assertEqual(self.events[-3:], ["gpu_final", "snapshot_failure", "write_report"])
        for label in ("physical exit check also failed", "budget receipt also failed", "query report also failed"):
            self.assertTrue(any(label in note for note in self.primary.__notes__), label)


class CascadeABIRoutingTests(unittest.TestCase):
    def test_cascade_profile_probes_filter_and_cascade_sources_after_one_abi_pair(self):
        from integrations.imlane_records import method_environment

        probe = dict(base="/tmp/fixture-base", version=[3, 12], arrow="16.1.0")
        with patch.object(method_environment.subprocess, "check_output", return_value=json.dumps(probe)) as check, \
             patch.object(method_environment.subprocess, "run") as run:
            result = method_environment.preflight_method("/tmp/fixture-method-python", "/tmp/fixture-author",
                                                          "/tmp/fixture-code", cascade=True)
        self.assertEqual(result, [probe, probe])
        self.assertEqual(check.call_count, 2)
        self.assertEqual(run.call_count, 2)
        for call, profile, validator in zip(run.call_args_list, ("engine", "cascade"),
                                            ("validate_filter_source", "validate_cascade_source"), strict=True):
            command = call.args[0]
            self.assertEqual(command[:4], ["/tmp/fixture-method-python", "-I", "-B", "-c"])
            self.assertIn("lotus." + profile + " import " + validator, command[4])
            self.assertEqual(command[-1], "/tmp/fixture-code")
            self.assertEqual(call.kwargs, dict(check=True, capture_output=True, timeout=15))

    def test_ambiguous_chain_and_cascade_profile_rejects_before_any_probe(self):
        from integrations.imlane_records import method_environment

        with patch.object(method_environment.subprocess, "check_output") as check, \
             patch.object(method_environment.subprocess, "run") as run, \
             self.assertRaisesRegex(ValueError, "choose one finite method source profile"):
            method_environment.preflight_method("/tmp/fixture-method", "/tmp/fixture-author", "/tmp/fixture-code",
                                                 chain=True, cascade=True)
        check.assert_not_called()
        run.assert_not_called()

    def test_owner_preflight_forwards_cascade_profile_to_isolated_method_probe(self):
        from integrations.imlane_records import chain_runtime

        owner = dict(author_root="/tmp/fixture-author", bridge_binary="/tmp/fixture-bridge",
                     method_python="/tmp/fixture-method", preflight_report="/tmp/fixture-preflight.json",
                     owner_resource_limits=dict(cpu_cores=1, rss_bytes=1024**3, shm_bytes=1024**2))
        engine = SimpleNamespace(validate_driver_environment=Mock(return_value={"fixture": True}))
        with patch.object(chain_runtime, "check_external_lease", return_value={"fixture": True}), \
             patch.object(chain_runtime, "read_json", return_value=dict(status="ok")), \
             patch.object(chain_runtime.os, "sched_getaffinity", create=True, return_value={0}), \
             patch.object(chain_runtime, "validate_author_build", return_value={"fixture": True}), \
             patch.object(chain_runtime, "preflight_method", return_value={"fixture": True}) as abi:
            chain_runtime.preflight_owner(owner, engine, chain=False, cascade=True)
        abi.assert_called_once_with(owner["method_python"], owner["author_root"],
                                    Path(chain_runtime.__file__).parents[2], chain=False, cascade=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
