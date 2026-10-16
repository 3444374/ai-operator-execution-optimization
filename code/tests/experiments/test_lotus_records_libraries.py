"""Opt-in CPU checks with real Ray/Daft and explicitly fixed model responses.

RUN_LOTUS_RECORDS_CPU_LIBRARIES=1 and SEMBENCH_CHECKOUT enable these checks.
They never construct vLLM or load weights. The inference stage is a CPU fixture;
the production GPU stage and separate model interpreter require qualification.
"""

import csv
from dataclasses import asdict
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest


class FixedResponses:
    def __call__(self, batch):
        import pyarrow as pa

        return pa.Table.from_pylist([
            dict(row, text="Answer: True" if row["source_position"] == 0 else "Answer: False",
                 status="succeeded", prompt_tokens=12, completion_tokens=2, total_tokens=14,
                 finish_reason="stop", engine_request_id=row["call_id"],
                 engine_uid="cpu-fixed-responses", submission_confirmed=True,
                 usage_source="configured software fixture; no inference")
            for row in batch.to_pylist()
        ])


@unittest.skipUnless(os.environ.get("RUN_LOTUS_RECORDS_CPU_LIBRARIES") == "1"
                     and os.environ.get("SEMBENCH_CHECKOUT"),
                     "explicit CPU-library qualification and fixed SemBench checkout required")
class CpuLibrariesTests(unittest.TestCase):
    def test_daft_preserves_empty_and_null_looking_text(self):
        import daft

        self.assertEqual(daft.__version__, "0.7.21")
        texts = ["", "NULL", "null", "None", "NaN", "same", "line\nbreak"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "records.csv"
            with path.open("w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["row_id", "source_position", "reviewId", "id", "reviewText"])
                for index, text in enumerate(texts):
                    writer.writerow(["row-" + str(index), index, "duplicate", "taken_3", text])
            frame = daft.read_csv(str(path), schema={
                "row_id": daft.DataType.string(), "source_position": daft.DataType.int64(),
                "reviewId": daft.DataType.string(), "id": daft.DataType.string(),
                "reviewText": daft.DataType.string(),
            })
            rows = list(frame.iter_rows())
        self.assertEqual([row["reviewText"] for row in rows], texts)

    def test_ray_csv_worker_formatter_and_original_evaluator(self):
        import pyarrow as pa
        import pyarrow.csv as pacsv
        import ray
        import ray.data
        from src.baselines.text.frameworks.ray_data_lotus_records import PrepareBatch, _add_identity, _runtime_env
        from src.experiments.lotus_records_movie import run_movie_query
        from src.semantic_methods.lotus.records import FilterMethod
        from src.serving.backends.vllm_engine import GenerationSpec
        from tests.experiments.test_lotus_records_movie import fixture_source

        self.assertEqual(ray.__version__, "2.56.1")
        self.assertFalse(ray.is_initialized(), "qualification must own its local Ray runtime")
        with tempfile.TemporaryDirectory(prefix="lr-input-") as inputs, \
             tempfile.TemporaryDirectory(prefix="lr-ray-", dir="/tmp") as runtime:
            source = fixture_source(Path(inputs))
            method = FilterMethod("openai/local-fixture")
            try:
                ray.init(address="local", num_cpus=4, num_gpus=0, include_dashboard=False,
                         object_store_memory=80 * 1024**2, _temp_dir=runtime,
                         _node_ip_address="127.0.0.1")
                def execute():
                    deadline = time.monotonic() + 90
                    dataset = ray.data.read_csv(str(source.csv_path), override_num_blocks=2,
                        convert_options=pacsv.ConvertOptions(column_types={
                            "row_id": pa.string(), "source_position": pa.int64(),
                            "reviewId": pa.string(), "id": pa.string(), "reviewText": pa.string(),
                        }, strings_can_be_null=False, null_values=[]))
                    dataset = dataset.filter(lambda row: row["id"] == "taken_3", max_retries=0)
                    dataset = dataset.select_columns(["row_id", "source_position", "reviewText"])
                    dataset = dataset.map(_add_identity, fn_kwargs={"query_id": "ray-cpu-q3"}, max_retries=0)
                    dataset = dataset.map_batches(PrepareBatch, batch_format="pyarrow", batch_size=1,
                        compute=ray.data.ActorPoolStrategy(size=1), num_cpus=1,
                        fn_constructor_kwargs={"method": asdict(method), "deadline": deadline},
                        runtime_env=_runtime_env(sys.executable), max_restarts=0, max_task_retries=0)
                    dataset = dataset.map_batches(FixedResponses, batch_format="pyarrow", batch_size=2,
                        compute=ray.data.ActorPoolStrategy(size=1), num_cpus=1,
                        max_restarts=0, max_task_retries=0)
                    return dataset.iter_rows()
                report = run_movie_query(source, os.environ["SEMBENCH_CHECKOUT"], method,
                                         GenerationSpec(), execute, query_id="ray-cpu-q3")
                self.assertEqual(report["status"], "completed")
                self.assertEqual(report["result"], {"positive_review_cnt": 1})
                self.assertEqual(report["calls"]["counts"]["succeeded"], 3)
                self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 36)
                self.assertEqual(report["quality"]["classification_audit"]["true_positive"], 1)
                self.assertEqual(report["quality"]["classification_audit"]["true_negative"], 2)
            finally:
                ray.shutdown()


if __name__ == "__main__":
    unittest.main()
