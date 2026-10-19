"""Opt-in real Core/Ray/Daft/LOTUS composition with a zero-GPU CPU fixture.

Both Core and its single-row static reference run two queries. Only the model
kernel and its GPU allocation/interpreter preflight are substituted. Fixed text
and synthetic token IDs/usage establish wiring, never model quality or speed.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
sys.path.insert(0, str(CODE_ROOT))


@unittest.skipUnless(os.environ.get("RUN_LOTUS_RECORDS_CPU_LIBRARIES") == "1"
                     and os.environ.get("SEMBENCH_CHECKOUT"),
                     "explicit CPU-library qualification and fixed SemBench checkout required")
class CoreRecordsLibrariesTests(unittest.TestCase):
    def test_two_core_queries_match_static_method_results_and_keep_core_ownership(self):
        import daft
        from daft.runners import _get_runner
        import ray
        from src.baselines.common.private_artifacts import write_private_json
        from src.baselines.common.redact import redact_json_values
        from src.execution_provider.adapters import lotus_records_actor_owner as actors
        from src.execution_provider.adapters import lotus_records_core as core
        from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
        from src.experiments.lotus_records_movie import run_movie_query
        from src.scheduling.core.session import SessionEngine
        from src.scheduling.runtime.async_backend import BoundedAsyncBackend
        from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        from tests.experiments.test_lotus_records_movie import fixture_source

        class CpuEngine:
            def __init__(self):
                import uuid

                self.uid, self.calls = "explicit-cpu-fixture-" + uuid.uuid4().hex, 0

            def ready(self):
                import sys

                assert "lotus" not in sys.modules and "vllm" not in sys.modules
                return dict(engine_uid=self.uid, physical_gpu_work=False,
                            inference_kind="fixed CPU output and synthetic token usage")

            def run_prepared(self, rows, *, deadline_monotonic):
                import hashlib
                import json
                import time

                assert time.monotonic() < deadline_monotonic and len(rows) == 1
                self.calls += len(rows)
                assert self.calls <= 8
                values = []
                for row in rows:
                    message_hash = hashlib.sha256(json.dumps(
                        json.loads(row["messages_json"]), sort_keys=True,
                        ensure_ascii=False, separators=(",", ":"),
                    ).encode()).hexdigest()
                    values.append(dict(
                        **{k: v for k, v in row.items() if k != "messages_json"},
                        messages_sha256=message_hash, status="succeeded",
                        text="Answer: True" if row["source_position"] == 0 else "Answer: False",
                        prompt_token_ids_json="[1, 2, 3]", output_token_ids_json="[4]",
                        logprobs_json="null", prompt_tokens=3, completion_tokens=1, total_tokens=4,
                        finish_reason="stop", engine_uid=self.uid, engine_request_id=row["call_id"],
                        submission_confirmed=True, usage_source="synthetic CPU fixture; no inference",
                    ))
                return values

            def close(self, timeout):
                return dict(status="closed", engine_api_shutdown_confirmed=True,
                            gpu_exit_confirmed=None, reserved_model_calls=self.calls)

        def cpu_ready(actor, deadline):
            from src.execution_provider.adapters.lotus_records_actor_owner import _check

            _check(deadline)
            if actor.engine is None:
                actor.engine = CpuEngine()
            return actor.engine.ready()

        class CpuStaticActor(actors._ModelActor):
            ready = cpu_ready

        class CpuCoreActor(core._CoreModelActor):
            ready = cpu_ready

        self.assertEqual((ray.__version__, daft.__version__), ("2.56.1", "0.7.21"))
        self.assertFalse(ray.is_initialized())
        self.assertIsNone(_get_runner(), "run this opt-in check in a fresh process")
        original_remote = ray.remote
        allocations = []

        class CpuOptions:
            def __init__(self, replacement, kind):
                self.replacement, self.kind = replacement, kind

            def options(self, **kwargs):
                assert kwargs["num_gpus"] == 1 and kwargs["max_concurrency"] == 1
                assert kwargs["max_restarts"] == kwargs["max_task_retries"] == 0
                self.settings = dict(kwargs)
                allocations.append(dict(kind=self.kind, original_options=dict(kwargs), actual_num_gpus=0))
                return self

            def remote(self, *args):
                return original_remote(self.replacement).options(
                    **dict(self.settings, num_gpus=0),
                ).remote(*args)

        def cpu_remote(cls):
            if cls is actors._ModelActor:
                return CpuOptions(CpuStaticActor, "static")
            if cls is core._CoreModelActor:
                return CpuOptions(CpuCoreActor, "core")
            return original_remote(cls)

        output = os.environ.get("RECORDS_CORE_CPU_ARTIFACTS")

        def save(name, value):
            if output:
                write_private_json(Path(output) / name, redact_json_values(value))

        reports, session_ids, close_reports = {}, [], {}
        with tempfile.TemporaryDirectory(prefix="core-input-") as inputs, \
                tempfile.TemporaryDirectory(prefix="coreray-", dir="/tmp") as runtime:
            source = fixture_source(Path(inputs))
            method, generation = FilterMethod("openai/local-fixture"), GenerationSpec()
            spec = EngineSpec(inputs, sys.executable, sys.executable, max_model_calls=8)
            resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                                         max_active_batches=2, query_timeout_s=90, startup_timeout_s=60)
            try:
                ray.init(address="local", num_cpus=3, num_gpus=0, include_dashboard=False,
                         object_store_memory=80 * 1024**2, _temp_dir=runtime,
                         _node_ip_address="127.0.0.1")
                with patch.object(ray, "remote", side_effect=cpu_remote), patch.object(
                    EngineSpec, "validate_driver_environment", return_value={"scope": "CPU fixture"},
                ):
                    for kind, owner_type in [("static", SemLoomRecordsStaticReference),
                                             ("core", core.SemLoomRecordsCore)]:
                        owner = owner_type(method, spec, generation, resources)
                        reports[kind] = []
                        actor_owner = owner._actor_owner if kind == "static" else owner._actors
                        self.assertIsInstance(actor_owner, actors.RecordsActorOwner)
                        try:
                            for index in [1, 2]:
                                query_id = kind + "-cpu-" + str(index)
                                value = run_movie_query(
                                    source, os.environ["SEMBENCH_CHECKOUT"], method, generation,
                                    lambda: owner.evaluate(source, query_id=query_id), query_id=query_id,
                                )
                                reports[kind].append(value)
                                save(query_id + ".json", value)
                                self.assertEqual(value["status"], "completed")
                                self.assertEqual(value["result"], {"positive_review_cnt": 1})
                                self.assertEqual(value["calls"]["usage"]["total_tokens"], 12)
                                audit = value["quality"]["classification_audit"]
                                self.assertEqual((audit["true_positive"], audit["true_negative"]), (1, 2))
                                if kind == "core":
                                    self.assertIsInstance(owner.core, SessionEngine)
                                    self.assertIsInstance(owner.transport.backend, BoundedAsyncBackend)
                                    self.assertEqual(owner.core.capacity.usage().held_tasks, 0)
                                    self.assertEqual(owner.transport.retained_refs(), 0)
                                    self.assertLessEqual(owner.last_query_report["peak_core_usage"]["held_tasks"], 2)
                                    session_ids.append(owner._active.session.session_id)
                                self.assertIs(actor_owner, owner._actor_owner if kind == "static" else owner._actors)
                        finally:
                            close_reports[kind] = owner.close()
                            save(kind + "-close.json", close_reports[kind])
                        close = close_reports[kind]
                        model_close = close["engine_close_report"] if kind == "static" else close["model_owner"]["engine_close_report"]
                        self.assertTrue(model_close["engine_api_shutdown_confirmed"])
                        self.assertEqual(model_close["reserved_model_calls"], 6)
                        self.assertIsNone(close["gpu_exit_confirmed"])
                        if kind == "core":
                            self.assertTrue(close["transport_closed"])
                            self.assertEqual(close["last_query_report"]["unconsumed_call_results"], [])
                self.assertEqual([x["kind"] for x in allocations], ["static", "core"])
                self.assertEqual(_get_runner().name, "native")
                self.assertEqual(len(set(session_ids)), 2)
                for kind in ["static", "core"]:
                    first, second = [q["calls"]["records"] for q in reports[kind]]
                    self.assertEqual(len({r["engine_uid"] for r in first + second}), 1)
                    self.assertTrue({r["call_id"] for r in first}.isdisjoint(r["call_id"] for r in second))
                fields = ["messages_sha256", "text", "prompt_token_ids_json", "output_token_ids_json",
                          "prompt_tokens", "completion_tokens", "total_tokens", "finish_reason"]
                for reference, actual in zip(reports["static"], reports["core"], strict=True):
                    normalize = lambda q: {r["row_id"]: {k: r[k] for k in fields} for r in q["calls"]["records"]}
                    self.assertEqual(normalize(reference), normalize(actual))
                    self.assertEqual(reference["quality"], actual["quality"])
                    self.assertEqual(reference["lm"], actual["lm"])
                save("composition.json", dict(
                    status="passed", ray=ray.__version__, daft=daft.__version__, actual_model_calls=0,
                    logical_ray_gpus=0, configured_cpu_workers=3,
                    inference_kind="fixed CPU output; synthetic token IDs and usage",
                    fixture_request_rows=12, arms=list(reports), queries_per_arm=2,
                    same_actor_owner_per_arm=True, same_engine_per_arm=True,
                    distinct_query_call_ids=True, distinct_core_session_ids=True,
                    original_method_results_equal=True, original_evaluation_equal=True,
                    requested_and_substituted_model_allocations=allocations,
                    production_core_connected=close_reports["core"]["production_core_connected"],
                    shared_runner_production_core_used=reports["core"][0]["production_core_used"],
                    core_identity_scope="Actual SessionEngine/BoundedAsyncBackend and Core close report; shared runner labeling is integrated separately",
                    core_usage=close_reports["core"]["core_usage"],
                    transport_closed=close_reports["core"]["transport_closed"],
                    native_gpu_exit_qualified=False,
                ))
            finally:
                ray.shutdown()


if __name__ == "__main__":
    unittest.main()
