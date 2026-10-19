"""Opt-in native Ray/Daft graphs with original LOTUS and private CPU fixtures."""

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

from src.baselines.text.frameworks import daft_lotus_chain, ray_data_lotus_chain
from src.baselines.text.frameworks.lotus_chain_results import record_row
from src.semantic_methods.lotus.record_chain import LotusRecordMethod, records_to_frame
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from tests.baselines.text.native_relation_cpu_fixture import CpuRelationBatch, UnknownRelationBatch
from tests.execution_provider.test_lotus_record_chain_core_relations import relation_chain, source_records


@unittest.skipUnless(os.environ.get("RUN_NATIVE_EXTRACT_CPU") in ("ray", "daft"),
                     "explicit pinned native CPU qualification required")
class NativeExtractRelationLibraryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.backend = os.environ["RUN_NATIVE_EXTRACT_CPU"]
        if cls.backend == "ray":
            import ray
            import ray.data

            assert ray.__version__ == "2.56.1"
            cls.ray_temp = tempfile.TemporaryDirectory(prefix="nxr-", dir="/tmp")
            ray.init(num_cpus=2, num_gpus=0, include_dashboard=False, log_to_driver=False,
                     _temp_dir=cls.ray_temp.name, object_store_memory=128 * 1024**2)
            ray.data.DataContext.get_current().execution_options.preserve_order = True
        else:
            import daft
            assert daft.__version__ == "0.7.21"

    @classmethod
    def tearDownClass(cls):
        if cls.backend == "ray":
            import ray
            ray.shutdown()
            cls.ray_temp.cleanup()

    def setUp(self):
        import lotus

        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)
        self.temp = tempfile.TemporaryDirectory(prefix="native-extract-library-")
        self.addCleanup(self.temp.cleanup)
        self.records = source_records()
        self.chain = relation_chain("EMF", query_id="native-" + self.backend + "-" + uuid.uuid4().hex)
        self.stream = None

    def tearDown(self):
        if self.stream is not None:
            self.stream.close()

    def evaluate(self, batch_type=CpuRelationBatch):
        import pyarrow as pa

        if self.backend == "ray":
            import ray.data
            native = ray_data_lotus_chain
            frame = ray.data.from_items([record_row(row) for row in self.records], override_num_blocks=4)
            options = {}
        else:
            import daft
            native = daft_lotus_chain
            native._native_runner(2)
            frame = daft.from_pylist([record_row(row) for row in self.records])
            options = {"runner_threads": 2}
        graph = native._graph
        # The relation owner starts graphs lazily. Keep this explicit CPU fixture
        # injection alive until all phases and owner cleanup have finished.
        probe = patch.object(EngineSpec, "validate_driver_environment", return_value={"CPU fixture": True})
        probe.start()
        self.addCleanup(probe.stop)
        kernels = patch.object(native, "_graph", side_effect=lambda *args, **kwargs:
            graph(*args, **dict(kwargs, batch_type=batch_type, model_gpus=0)))
        kernels.start()
        self.addCleanup(kernels.stop)
        self.stream = native.evaluate(frame, self.chain,
            EngineSpec(self.temp.name, sys.executable, sys.executable, max_model_calls=12),
            RecordsResources(model_batch_rows=2, max_active_batches=1,
                query_timeout_s=90, startup_timeout_s=30),
            expected=self.records, query_id=self.chain.query_id, control_dir=self.temp.name, **options)
        return self.stream

    def save(self, suffix, output):
        folder = os.environ.get("NATIVE_EXTRACT_CPU_EVIDENCE")
        if folder:
            from src.baselines.common.private_artifacts import write_private_json
            from src.baselines.common.redact import redact_json_values

            write_private_json(Path(folder) / (self.backend + "-" + suffix + ".json"), redact_json_values(dict(
                output=[row.describe() for row in output], calls=self.stream.calls,
                columns=self.stream.columns, stage_stats=self.stream.batch_stage_stats,
                close=self.stream.last_close_report, phase_history=self.stream.phase_history,
                actual_model_calls=0, logical_gpus=0, source="CPU fixture responses with actual native graphs")))

    def original(self, output):
        import lotus
        import pandas as pd
        from src.semantic_methods.lotus.engine import engine_response, lm_report

        frame = pd.DataFrame([row.values for row in self.records], index=[row.source_index for row in self.records])
        for stage in self.chain.stages:
            method = LotusRecordMethod(stage)
            calls = sorted((call for call in self.stream.calls if call["algorithm_stage"] == stage.stage_id),
                           key=lambda call: call["source_position"])
            captured = []

            def answer(data, options, *_args, method=method, calls=calls):
                self.assertEqual(len(data), len(calls))
                captured.extend(message for message, _media in data)
                return [engine_response(call, method.lm) for call in calls]

            method.lm._process_uncached_messages = answer
            with lotus.settings.context(lm=method.lm):
                if stage.kind == "extract":
                    frame = frame.sem_extract(list(stage.method.input_columns), stage.method.output_columns)
                elif stage.kind == "map":
                    frame = frame.sem_map(stage.method.instruction, suffix=stage.method.output_column)
                else:
                    frame = frame.sem_filter(stage.method.instruction, default=stage.method.default)
            hashes = [hashlib.sha256(json.dumps(message, ensure_ascii=False, sort_keys=True,
                separators=(",", ":")).encode()).hexdigest() for message in captured]
            self.assertEqual(hashes, [call["messages_sha256"] for call in calls])
            self.assertIn(lm_report(method.lm)["stats"], [stats[stage.stage_id]["stats"]
                for stats in self.stream.batch_stage_stats if stage.stage_id in stats])
        pd.testing.assert_frame_equal(records_to_frame(output, columns=self.stream.columns), frame)

    def test_complete_native_relation_matches_original_with_distinct_engine_lifetimes(self):
        stream = self.evaluate()
        output = []
        try:
            output.extend(stream)
            self.assertTrue(stream.completed)
            self.original(output)
            self.assertEqual([row.row_id for row in output], ["occ-0", "occ-2"])
            self.assertEqual(len(stream.calls), 12)
            self.assertEqual(sum(call["total_tokens"] for call in stream.calls), 48)
            self.assertTrue(all(call["actual_model_call"] is False for call in stream.calls))
            prefix = {call["source_position"]: call for call in stream.calls if call["algorithm_stage"] == "e0"}
            self.assertNotEqual(prefix[0]["fixture_batch_positions"], prefix[2]["fixture_batch_positions"])
            self.assertEqual(len({call["engine_uid"] for call in stream.calls}), 2)
            for call in stream.calls:
                if call["algorithm_stage"] == "m1" and call["source_position"] in (0, 2):
                    self.assertEqual(call["parent_call_ids"], [prefix[0]["call_id"], prefix[2]["call_id"]])
            report = stream.close()
            self.assertEqual(report["status"], "closed")
            self.assertFalse(report["engine_reuse_across_segments"])
            self.assertIsNone(report["gpu_exit_confirmed"])
            self.assertEqual(list(stream), [])
            self.assertEqual(len(stream.calls), 12)
        finally:
            self.save("complete", output)

    def test_unknown_prefix_preserves_usage_and_allocates_no_suffix_engine(self):
        stream = self.evaluate(UnknownRelationBatch)
        output = []
        try:
            with self.assertRaises(RuntimeError):
                output.extend(stream)
            self.assertFalse(stream.completed)
            self.assertEqual(output, [])
            self.assertTrue(stream.calls)
            self.assertTrue(all(call["algorithm_stage"] == "e0" for call in stream.calls))
            self.assertEqual(sum(call["status"] == "unknown" for call in stream.calls), 1)
            self.assertEqual(len({call["engine_uid"] for call in stream.calls}), 1)
            self.assertEqual(len(stream._phases), 1)
            self.assertFalse(stream.last_close_report["query_completed"])
            self.assertEqual(list(stream), [])
        finally:
            self.save("unknown", output)


if __name__ == "__main__":
    unittest.main()
