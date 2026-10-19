"""Real Core/Ray/Daft/LOTUS cascade composition, synthetic child model only."""

import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


@unittest.skipUnless(os.environ.get("RUN_LOTUS_CASCADE_CORE_CPU") == "1",
                     "explicit real-library Core CPU qualification")
class CoreCascadeLibrariesTests(unittest.TestCase):
    def test_q3_and_distinct_helper_columns_match_the_pinned_original_controller(self):
        offline = patch.dict(os.environ, {
            "LITELLM_LOCAL_MODEL_COST_MAP": "True", "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
        })
        offline.start()
        self.addCleanup(offline.stop)
        import daft
        from daft.runners import _get_runner
        import lotus
        import pandas as pd
        import ray
        from lotus.types import CascadeArgs
        from src.baselines.common.private_artifacts import write_private_json
        from src.baselines.common.redact import redact_json_values
        from src.execution_provider.adapters import lotus_cascade_core as core
        from src.semantic_methods.lotus.cascade import run_cascade
        from src.semantic_methods.lotus.engine import bind_filter_method
        from src.semantic_methods.lotus.records import FilterMethod, MOVIE_Q3_INSTRUCTION, RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec
        from tests.execution_provider.cascade_core_fixture import CpuCascadeEngineProcess
        from tests.execution_provider.test_lotus_cascade_core import Source
        from tests.semantic_methods.test_lotus_cascade import CascadeDifferentialTests, models

        self.assertEqual((ray.__version__, daft.__version__), ("2.56.1", "0.7.21"))
        self.assertFalse(ray.is_initialized())
        self.assertIsNone(_get_runner(), "qualification must run in its own fresh process")
        reports, allocations = [], []
        original_remote = ray.remote

        class CpuOptions:
            def __init__(self, bound):
                class CpuStageActor(bound):
                    def _process_type(self):
                        return CpuCascadeEngineProcess
                self.actor = CpuStageActor

            def options(self, **settings):
                assert settings["num_gpus"] == settings["max_concurrency"] == 1
                assert settings["max_restarts"] == settings["max_task_retries"] == 0
                allocations.append(dict(requested_num_gpus=1, actual_num_gpus=0,
                                        reason="explicit synthetic-model CPU qualification"))
                self.settings = dict(settings, num_gpus=0)
                return self

            def remote(self, *args):
                return original_remote(self.actor).options(**self.settings).remote(*args)

        def cpu_remote(actor):
            if isinstance(actor, type) and issubclass(actor, core._CascadeCoreModelActor):
                return CpuOptions(actor)
            return original_remote(actor)

        def args(**options):
            return CascadeArgs(sampling_percentage=0.5, cascade_IS_random_seed=5,
                               cascade_num_calibration_quantiles=5, **options)

        with tempfile.TemporaryDirectory(prefix="cascade-core-input-") as directory, \
                tempfile.TemporaryDirectory(prefix="cascoreray-", dir="/tmp") as runtime:
            root = Path(directory)
            for role in ("main", "helper"):
                (root / role).mkdir()
            main, helper = models()
            main.tokenizer.save(str(root / "main" / "tokenizer.json"))
            helper.tokenizer.save(str(root / "helper" / "tokenizer.json"))
            try:
                ray.init(address="local", num_cpus=3, num_gpus=0, include_dashboard=False,
                         object_store_memory=80 * 1024**2, _temp_dir=runtime,
                         _node_ip_address="127.0.0.1")
                with lotus.settings.context(enable_cache=False), \
                        patch.object(ray, "remote", side_effect=cpu_remote), \
                        patch.object(EngineSpec, "validate_driver_environment", return_value={
                            "scope": "CPU fixture; model and environment validation substituted",
                        }):
                    for column, instruction, helper_instruction in (
                        ("reviewText", MOVIE_Q3_INSTRUCTION, None),
                        ("body", "Check whether this article supports the claim: {body}",
                         "Use this headline as a proxy for the claim: {heading}"),
                    ):
                        frame = pd.DataFrame([dict(row_id="row-" + str(index), source_position=index,
                                                   **{column: "unique input " + str(index) + ";"},
                                                   **({"heading": "headline " + str(index)} if column == "body" else {}))
                                              for index in range(10)])
                        main, helper = models()
                        method = bind_filter_method(FilterMethod(main.model, instruction))
                        source = Source(root, frame.to_dict("records"), method,
                                        payload=(column, "heading") if column == "body" else (column,))
                        backend = core.CoreCascadeBackend(source, engines={role: EngineSpec(
                            str(root / role), sys.executable, sys.executable, max_model_calls=25,
                        ) for role in ("main", "helper")}, resources=RecordsResources(
                            prepare_batch_rows=1, model_batch_rows=1, max_active_batches=2,
                            query_timeout_s=180, startup_timeout_s=60,
                        ))
                        original = CascadeDifferentialTests()
                        original.frame = frame
                        configuration = args(**({"helper_filter_instruction": helper_instruction}
                                                if helper_instruction else {}))
                        native, stats, native_calls, sampled, _, _ = original.reference(
                            configuration, instruction=instruction,
                        )
                        try:
                            result = run_cascade(
                                frame, main, helper, configuration, backend, instruction=instruction,
                                query_id="core-" + column, max_rows=10, max_calls=25,
                            )
                        finally:
                            backend.close()
                        self.assertTrue(backend.production_core_connected)
                        self.assertEqual([row["decision"] for row in result["decisions"]], native["_filter"].tolist())
                        self.assertEqual([row["raw_output"] for row in result["decisions"]], native["raw_output_filter"].tolist())
                        self.assertEqual(result["stats"], {key: value.model_dump(mode="json")
                            if hasattr(value, "model_dump") else value for key, value in stats.items()})
                        event = next(item for item in result["calls"]["events"] if item["event"] == "sample_selected")
                        self.assertEqual(event["indices"], sampled)
                        self.assertLess(len(set(sampled)), len(sampled))
                        expected_hashes = [hashlib.sha256(json.dumps(messages, ensure_ascii=False,
                            sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                            for _, messages, _ in native_calls]
                        calls = result["calls"]["records"]
                        self.assertEqual([row["messages_sha256"] for row in calls], expected_hashes)
                        self.assertEqual(len({row["call_id"] for row in calls}), len(calls))
                        self.assertEqual(len({row["provider_call_id"] for row in calls}), len(calls))
                        self.assertTrue(all(row["engine_request_id"] == row["call_id"] for row in calls))
                        self.assertTrue(all(row["prepare_worker_pid"] != os.getpid() for row in calls))
                        self.assertTrue(all(row["model_fixture_pid"] != os.getpid() for row in calls))
                        self.assertEqual(result["calls"]["usage"]["total_tokens"], sum(row["total_tokens"] for row in calls))
                        self.assertEqual(len(backend.close_reports), 3)
                        for close in backend.close_reports:
                            self.assertTrue(close["production_core_connected"])
                            self.assertTrue(close["stage_process_exit_confirmed"])
                            self.assertEqual(close["core_usage"]["held_tasks"], 0)
                            self.assertEqual(close["retained_remote_inputs"], 0)
                            self.assertIsNone(close["gpu_exit_confirmed"])
                            self.assertTrue(close["registered_engine_cleanup"]["process_exit_confirmed"])
                        reports.append(dict(column=column, query=result, close_reports=backend.close_reports))
                self.assertEqual(len(allocations), 6)
            finally:
                ray.shutdown()
        output = os.environ.get("CASCADE_CORE_CPU_ARTIFACTS")
        if output:
            target = Path(output)
            target.mkdir(mode=0o700, parents=True, exist_ok=True)
            write_private_json(target / "qualification.json", redact_json_values(dict(
                status="passed", reports=reports, allocations=allocations, actual_model_calls=0,
                gpu_requests=0, evidence_scope="real Core/Ray/Daft/LOTUS; synthetic model child process",
            )))


if __name__ == "__main__":
    unittest.main()
