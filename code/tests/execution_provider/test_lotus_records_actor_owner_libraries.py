"""Opt-in real Ray/Daft graph with a CPU model substitute; no GPU or vLLM.

The preparation actor uses the pinned LOTUS formatter and both queries use the
original parser/evaluator. The inference actor returns explicitly fixed output
and synthetic usage. This does not qualify a model interpreter or GPU lifecycle.
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
class ActorOwnerLibrariesTests(unittest.TestCase):
    def test_static_queries_use_extracted_actors_and_preserve_method_results(self):
        import daft
        from daft.runners import _get_runner
        import ray
        from src.execution_provider.adapters import lotus_records_actor_owner as actors
        from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
        from src.experiments.lotus_records_movie import run_movie_query
        from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        from tests.experiments.test_lotus_records_movie import fixture_source

        class CpuEngine:
            def __init__(self):
                import uuid

                self.uid = "explicit-cpu-fixture-" + uuid.uuid4().hex
                self.calls = 0

            def ready(self):
                import sys

                assert "lotus" not in sys.modules and "vllm" not in sys.modules
                return {"engine_uid": self.uid, "physical_gpu_work": False,
                        "inference_kind": "fixed CPU output and synthetic token usage"}

            def run_prepared(self, rows, *, deadline_monotonic):
                import hashlib
                import json
                import time

                assert time.monotonic() < deadline_monotonic
                self.calls += len(rows)
                assert self.calls <= 8
                result = []
                for row in rows:
                    messages = json.loads(row["messages_json"])
                    message_hash = hashlib.sha256(json.dumps(
                        messages, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                    ).encode()).hexdigest()
                    result.append(dict(
                        **{k: v for k, v in row.items() if k != "messages_json"},
                        messages_sha256=message_hash, status="succeeded",
                        text="Answer: True" if row["source_position"] == 0 else "Answer: False",
                        prompt_token_ids_json="[1, 2, 3]", output_token_ids_json="[4]",
                        logprobs_json="null", prompt_tokens=3, completion_tokens=1, total_tokens=4,
                        finish_reason="stop", engine_uid=self.uid, engine_request_id=row["call_id"],
                        submission_confirmed=True, usage_source="synthetic CPU fixture; no inference",
                    ))
                return result

            def close(self, timeout):
                return dict(status="closed", engine_api_shutdown_confirmed=True,
                            gpu_exit_confirmed=None, reserved_model_calls=self.calls)

        class CpuModelActor(actors._ModelActor):
            def ready(self, deadline):
                from src.execution_provider.adapters.lotus_records_actor_owner import _check

                _check(deadline)
                if self.engine is None:
                    self.engine = CpuEngine()
                return self.engine.ready()

        self.assertEqual(ray.__version__, "2.56.1")
        self.assertEqual(daft.__version__, "0.7.21")
        self.assertFalse(ray.is_initialized())
        self.assertIsNone(_get_runner(), "run the opt-in check in its own process")
        original_remote = ray.remote
        model_allocations = []

        class CpuOptions:
            def options(self, **kwargs):
                self.settings = dict(kwargs)
                self.assert_settings(kwargs)
                model_allocations.append(dict(kwargs))
                return self

            @staticmethod
            def assert_settings(settings):
                assert settings["num_gpus"] == 1 and settings["max_concurrency"] == 1
                assert settings["max_restarts"] == settings["max_task_retries"] == 0

            def remote(self, *args):
                return original_remote(CpuModelActor).options(
                    **dict(self.settings, num_gpus=0),
                ).remote(*args)

        def cpu_remote(cls):
            return CpuOptions() if cls is actors._ModelActor else original_remote(cls)

        with tempfile.TemporaryDirectory(prefix="owner-input-") as inputs, \
                tempfile.TemporaryDirectory(prefix="ownray-", dir="/tmp") as runtime:
            source = fixture_source(Path(inputs))
            method, generation = FilterMethod("openai/local-fixture"), GenerationSpec()
            spec = EngineSpec(inputs, sys.executable, sys.executable, max_model_calls=8)
            owner = SemLoomRecordsStaticReference(method, spec, generation, RecordsResources(
                prepare_batch_rows=2, model_batch_rows=1, max_active_batches=1,
                query_timeout_s=90, startup_timeout_s=60,
            ))
            try:
                ray.init(address="local", num_cpus=3, num_gpus=0, include_dashboard=False,
                         object_store_memory=80 * 1024**2, _temp_dir=runtime,
                         _node_ip_address="127.0.0.1")
                with patch.object(ray, "remote", side_effect=cpu_remote), patch.object(
                    EngineSpec, "validate_driver_environment", return_value={"scope": "CPU fixture"},
                ):
                    reports = []
                    for query_id in ["owner-first", "owner-second"]:
                        reports.append(run_movie_query(
                            source, os.environ["SEMBENCH_CHECKOUT"], method, generation,
                            lambda: owner.evaluate(source, query_id=query_id), query_id=query_id,
                        ))
                    close = owner.close()
                self.assertEqual(len(model_allocations), 1)
                self.assertTrue(close["engine_close_report"]["engine_api_shutdown_confirmed"])
                self.assertIsNone(close["gpu_exit_confirmed"])
                self.assertEqual(close["engine_close_report"]["reserved_model_calls"], 6)
                self.assertEqual(_get_runner().name, "native")
                self.assertIs(owner._actor_owner.__class__, actors.RecordsActorOwner)
                for report in reports:
                    self.assertEqual(report["status"], "completed")
                    self.assertEqual(report["result"], {"positive_review_cnt": 1})
                    self.assertEqual(report["calls"]["usage"]["total_tokens"], 12)
                    audit = report["quality"]["classification_audit"]
                    self.assertEqual((audit["true_positive"], audit["true_negative"]), (1, 2))
                first, second = [q["calls"]["records"] for q in reports]
                self.assertTrue({r["call_id"] for r in first}.isdisjoint(r["call_id"] for r in second))
                self.assertEqual(len({r["engine_uid"] for r in first + second}), 1)
            finally:
                owner.close()
                ray.shutdown()


if __name__ == "__main__":
    unittest.main()
