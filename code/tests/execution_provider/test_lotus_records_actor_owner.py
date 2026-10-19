"""Actor ownership contracts; vendor/model runtimes are software substitutes."""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
import subprocess
import sys
import time
from types import ModuleType
import unittest
from unittest.mock import patch


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
sys.path.insert(0, str(CODE_ROOT))

from src.execution_provider.adapters import lotus_records_actor_owner as actors
from src.execution_provider.adapters import lotus_records_static as static
from src.semantic_methods.lotus import engine as lotus_engine
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources, identity
from src.serving.backends import vllm_engine
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


class Ref:
    def __init__(self, name, operation):
        self.name, self.operation = name, operation
        self.resolved = False
        self.value = None

    def resolve(self):
        if not self.resolved:
            self.value = self.operation()
            self.resolved = True
        return self.value


class FakeRuntime:
    def __init__(self, *, close_reports=None, ready_error=None, constructor_error=None):
        self.events = []
        self.engines = []
        self.formatters = []
        runtime = self

        class Method:
            def __init__(self, actor, name):
                self.actor, self.name = actor, name

            def remote(self, *args):
                name = type(self.actor).__name__ + "." + self.name
                runtime.events.append(("submit", name, args))
                return Ref(name, lambda: getattr(self.actor, self.name)(
                    *[x.resolve() if isinstance(x, Ref) else x for x in args]
                ))

        class Handle:
            def __init__(self, value):
                self.value = value

            def __getattr__(self, name):
                return Method(self.value, name)

        class ActorClass:
            def __init__(self, cls):
                self.cls, self.settings = cls, None

            def options(self, **kwargs):
                self.settings = kwargs
                return self

            def remote(self, *args):
                runtime.events.append(("actor", self.cls.__name__, self.settings, args))
                return Handle(self.cls(*args))

        class Formatter:
            def __init__(self, method):
                assert method.resolved_input_columns == ("reviewText",)
                runtime.formatters.append(self)

            def prepare_rows(self, rows):
                return [dict(row, messages_json="fixture prepared bytes") for row in rows]

        class Engine:
            def __init__(self, engine, generation):
                self.runs, self.closes = [], 0
                runtime.engines.append(self)
                if constructor_error is not None:
                    raise constructor_error

            def ready(self):
                if ready_error is not None:
                    raise ready_error
                return {"engine_uid": "fixture-one-engine", "physical_gpu_work": False}

            def run_prepared(self, rows, *, deadline_monotonic):
                self.runs.append((rows, deadline_monotonic))
                return [dict(row, status="succeeded", prompt_tokens=3,
                             completion_tokens=2, total_tokens=5) for row in rows]

            def close(self, timeout):
                self.closes += 1
                if close_reports:
                    return close_reports[min(self.closes, len(close_reports)) - 1]
                return {"status": "closed", "engine_api_shutdown_confirmed": True,
                        "gpu_exit_confirmed": None}

        self.ray = ModuleType("ray")
        self.ray.__version__ = "2.56.1"
        self.ray.is_initialized = lambda: True
        self.ray.remote = ActorClass
        self.ray.wait = lambda refs, **kwargs: (refs[:1], refs[1:])

        def get(ref):
            runtime.events.append(("get", ref.name))
            return ref.resolve()

        self.ray.get = get
        self.ray.kill = lambda actor, **kwargs: self.events.append(("kill", actor, kwargs))
        self.stack = ExitStack()
        self.stack.enter_context(patch.dict(sys.modules, {"ray": self.ray}))
        self.stack.enter_context(patch.object(lotus_engine, "validate_filter_source", return_value={}))

        def fixture_binding(method):
            # These graph tests substitute the formatter; real binding has its own library checks.
            lotus_engine.validate_filter_source()
            return replace(method, resolved_input_columns=("reviewText",))

        self.stack.enter_context(patch.object(lotus_engine, "bind_filter_method", side_effect=fixture_binding))
        self.stack.enter_context(patch.object(lotus_engine, "LotusFilterMethod", Formatter))
        self.stack.enter_context(patch.object(EngineSpec, "validate_driver_environment", return_value={}))
        self.stack.enter_context(patch.object(vllm_engine, "VllmEngine", Engine))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self.stack.__exit__(*args)


class RecordsActorOwnerTests(unittest.TestCase):
    def owner(self, **resources):
        return actors.RecordsActorOwner(
            FilterMethod("openai/fixture"),
            EngineSpec("/tmp/fixture-model", "/tmp/model python", "/tmp/driver python"),
            GenerationSpec(), RecordsResources(**resources),
        )

    def test_import_and_model_actor_construction_need_no_vendor_or_method_runtime(self):
        program = '''
import importlib.abc,sys
sys.path.insert(0,sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'ray','daft','lotus','vllm'} or fullname == 'src.semantic_methods.lotus.engine':
            raise AssertionError('unexpected runtime import: '+fullname)
sys.meta_path.insert(0,Guard())
from src.execution_provider.adapters.lotus_records_actor_owner import _ModelActor
_ModelActor(dict(model_path='/tmp/model',model_python='/tmp/model-python',driver_python='/tmp/driver-python'),{},1)
'''
        subprocess.run([sys.executable, "-I", "-B", "-c", program, str(CODE_ROOT)],
                       check=True, capture_output=True, text=True, timeout=10)

    def test_static_reexports_the_same_actor_types(self):
        self.assertIs(static._PrepareActor, actors._PrepareActor)
        self.assertIs(static._ModelActor, actors._ModelActor)

    def test_empty_owner_closes_without_allocating_or_importing_ray(self):
        owner = self.owner()
        report = owner.close()
        self.assertEqual(report["status"], "closed")
        self.assertTrue(report["gpu_exit_confirmed"])
        self.assertIsNone(owner.ray)
        self.assertIsNone(owner.model_actor)
        with self.assertRaisesRegex(RuntimeError, "closing"):
            owner.start(time.monotonic() + 1)

    def test_start_reuses_one_engine_and_preserves_fixed_worker_settings(self):
        with FakeRuntime() as runtime:
            owner = self.owner(cpu_workers=2, model_batch_rows=1)
            deadline = time.monotonic() + 3
            ready = owner.start(deadline)
            self.assertIs(ready, owner.model_ready)
            self.assertIs(owner.start(deadline), ready)
            self.assertEqual(len(runtime.engines), 1)
            self.assertEqual(len(owner.prepare_actors), 2)
            allocations = [x for x in runtime.events if x[0] == "actor"]
            self.assertEqual(len(allocations), 3)
            for event in allocations:
                settings = event[2]
                self.assertEqual([settings[k] for k in ["max_restarts", "max_task_retries", "max_concurrency"]], [0, 0, 1])
                self.assertEqual(settings["runtime_env"]["env_vars"], {"PYTHONPATH": str(CODE_ROOT)})
                self.assertTrue(all(type(x) in (dict, int) for x in event[3]))
            self.assertEqual(allocations[0][2]["runtime_env"]["py_executable"], "'/tmp/driver python'")
            self.assertEqual(allocations[-1][2]["runtime_env"]["py_executable"], "'/tmp/model python'")
            self.assertEqual(allocations[-1][2]["num_gpus"], 1)
            owner.close()

    def test_model_actor_factory_selects_trusted_override_and_dynamic_default(self):
        class TrustedActor(actors._ModelActor):
            pass

        class TrustedOwner(actors.RecordsActorOwner):
            def _model_actor_type(self):
                return TrustedActor

        with FakeRuntime():
            default = self.owner()
            deadline = time.monotonic() + 3
            default.start(deadline)
            self.assertIs(type(default.model_actor.value), actors._ModelActor)
            specialized = TrustedOwner(default.method, default.engine, default.generation, default.resources)
            specialized.start(deadline)
            self.assertIs(type(specialized.model_actor.value), TrustedActor)
            with patch.object(actors, "_ModelActor", TrustedActor):
                patched_default = self.owner()
                patched_default.start(deadline)
                self.assertIs(type(patched_default.model_actor.value), TrustedActor)
            for owner in [default, specialized, patched_default]:
                owner.close()

    def test_all_preflight_failures_precede_actor_allocation(self):
        for failure in ["method", "driver", "generation", "ray-version", "ray-uninitialized"]:
            with self.subTest(failure=failure), FakeRuntime() as runtime:
                owner = self.owner()
                with ExitStack() as stack:
                    if failure == "method":
                        stack.enter_context(patch.object(lotus_engine, "validate_filter_source", side_effect=ValueError("source")))
                    elif failure == "driver":
                        stack.enter_context(patch.object(EngineSpec, "validate_driver_environment", side_effect=ValueError("driver")))
                    elif failure == "generation":
                        owner.generation = GenerationSpec(repetition_penalty=1.2)
                    elif failure == "ray-version":
                        runtime.ray.__version__ = "unsupported"
                    else:
                        runtime.ray.is_initialized = lambda: False
                    with self.assertRaises((ValueError, RuntimeError)):
                        owner.start(time.monotonic() + 3)
                self.assertEqual(runtime.events, [])

    def test_cancelled_expired_and_nonfinite_deadlines_allocate_nothing(self):
        with FakeRuntime() as runtime:
            for deadline in [True, 0, -1, float("inf"), float("nan")]:
                with self.subTest(deadline=deadline), self.assertRaises(ValueError):
                    self.owner().start(deadline)
            with self.assertRaises(TimeoutError):
                self.owner().start(time.monotonic() - 1)
            with self.assertRaises(InterruptedError):
                self.owner().start(time.monotonic() + 3, lambda: True)
            self.assertEqual(runtime.events, [])

    def test_prepared_objectref_is_not_fetched_on_driver(self):
        with FakeRuntime() as runtime:
            owner = self.owner()
            deadline = time.monotonic() + 3
            owner.start(deadline)
            rows = [dict(identity("q", "row", 0), reviewText="text")]
            prepared = owner.prepare_actors[0].prepare.remote(rows, deadline)
            results = owner.model_actor.run.remote(prepared, deadline)
            terminal = runtime.ray.get(results)
            self.assertEqual(terminal[0]["total_tokens"], 5)
            self.assertNotIn(("get", "_PrepareActor.prepare"), runtime.events)
            self.assertEqual(runtime.engines[0].runs[0][1], deadline)
            owner.close()

    def test_start_failure_retains_ownership_for_explicit_close(self):
        with FakeRuntime(ready_error=RuntimeError("startup failed")) as runtime:
            owner = self.owner()
            with self.assertRaisesRegex(RuntimeError, "startup failed"):
                owner.start(time.monotonic() + 3)
            self.assertIsNotNone(owner.model_actor)
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                owner.start(time.monotonic() + 3)
            report = owner.close()
            self.assertTrue(report["model_actor_handle_retained"])
            self.assertIsNone(report["gpu_exit_confirmed"])
            self.assertEqual(runtime.engines[0].closes, 1)

    def test_constructor_failure_stays_unknown_without_reconstruction_or_model_kill(self):
        with FakeRuntime(constructor_error=RuntimeError("allocated then failed")) as runtime:
            owner = self.owner()
            deadline = time.monotonic() + 3
            with self.assertRaisesRegex(RuntimeError, "allocated then failed"):
                owner.start(deadline)
            actor = owner.model_actor.value
            self.assertTrue(actor._engine_start_attempted)
            self.assertTrue(actor._engine_start_failed)
            self.assertIsNone(actor.engine)
            with self.assertRaisesRegex(RuntimeError, "startup is unconfirmed"):
                actor.ready(deadline)
            self.assertEqual(len(runtime.engines), 1)
            for _ in range(2):
                report = owner.close()
                self.assertEqual(report["status"], "unknown")
                self.assertFalse(report["engine_close_report"]["engine_api_shutdown_confirmed"])
                self.assertIsNone(report["gpu_exit_confirmed"])
                self.assertTrue(report["model_actor_handle_retained"])
                self.assertFalse(report["actor_kill_requests_confirmed"])
            self.assertFalse(any(event[0] == "kill" and event[1] is owner.model_actor
                                 for event in runtime.events))
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                owner.start(deadline)

    def test_never_started_model_actor_keeps_empty_close_behavior(self):
        actor = actors._ModelActor(dict(model_path="/tmp/model", model_python="/tmp/model-python",
                                        driver_python="/tmp/driver-python"), {}, 1)
        with self.assertRaises(TimeoutError):
            actor.ready(time.monotonic() - 1)
        self.assertFalse(actor._engine_start_attempted)
        self.assertFalse(actor._engine_start_failed)
        self.assertEqual(actor.close(1), dict(status="closed", engine_api_shutdown_confirmed=True,
                                             gpu_exit_confirmed=True))

    def test_unconfirmed_engine_close_can_retry_without_claiming_gpu_exit(self):
        reports = [dict(engine_api_shutdown_confirmed=False, gpu_exit_confirmed=None),
                   dict(engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None)]
        with FakeRuntime(close_reports=reports) as runtime:
            owner = self.owner()
            owner.start(time.monotonic() + 3)
            first = owner.close()
            self.assertEqual(first["cleanup_errors"], ["model_engine_shutdown_unconfirmed"])
            self.assertFalse(any(x[0] == "kill" and x[1] is owner.model_actor for x in runtime.events))
            second = owner.close()
            self.assertTrue(second["engine_close_report"]["engine_api_shutdown_confirmed"])
            self.assertTrue(second["actor_kill_requests_confirmed"])
            self.assertIsNone(second["gpu_exit_confirmed"])
            self.assertEqual(second["status"], "unknown")
            owner.close()
            self.assertEqual(runtime.engines[0].closes, 2)

    def test_timed_out_close_waits_on_the_same_ref_on_retry(self):
        with FakeRuntime() as runtime:
            owner = self.owner()
            owner.start(time.monotonic() + 3)
            wait = owner.wait_ready
            with patch.object(owner, "wait_ready", side_effect=TimeoutError("still running")):
                first = owner.close()
            pending = owner._model_close_ref
            self.assertIsNotNone(pending)
            self.assertEqual(first["cleanup_errors"], ["model_close: TimeoutError"])
            with patch.object(owner, "wait_ready", wraps=wait) as retry:
                owner.close()
            self.assertIs(retry.call_args.args[0], pending)
            self.assertEqual(sum(x[:2] == ("submit", "_ModelActor.close") for x in runtime.events), 1)
            self.assertEqual(runtime.engines[0].closes, 1)

    def test_actor_kill_failure_is_retryable_without_reclosing_engine(self):
        with FakeRuntime() as runtime:
            owner = self.owner()
            owner.start(time.monotonic() + 3)
            with patch.object(runtime.ray, "kill", side_effect=OSError("kill unavailable")):
                first = owner.close()
            self.assertFalse(first["actor_kill_requests_confirmed"])
            self.assertEqual(first["status"], "unknown")
            second = owner.close()
            self.assertTrue(second["actor_kill_requests_confirmed"])
            self.assertEqual(runtime.engines[0].closes, 1)

    def test_shared_owner_does_not_own_query_or_session_reports(self):
        owner = self.owner()
        self.assertFalse(hasattr(owner, "last_query_report"))
        self.assertFalse(hasattr(owner, "_active"))
        self.assertFalse(hasattr(owner, "pending"))
        self.assertNotIn("last_query_report", owner.close())


if __name__ == "__main__":
    unittest.main()
