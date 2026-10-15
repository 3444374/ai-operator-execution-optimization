"""Release image evidence only after its own successful persistent export."""
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
import asyncio
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import numpy as np
from PIL import Image

from src.semantic_methods.lotus.image_rm import RMObservation, observe_stock_rm
from src.semantic_methods.lotus import image_workflow


def add_call(observation, *, image_rows=1, status="succeeded"):
    matrix = np.zeros((1, 512), dtype=np.float32)
    matrix[:, 0] = 1
    observation.retain(matrix)
    record = dict(call_index=observation.call_count, image_rows=image_rows,
                  text_rows=0, status=status)
    observation.calls.append(record)
    return record


class ObservationReleaseTests(unittest.TestCase):
    def test_default_retains_success_failure_and_vectors(self):
        observation = RMObservation(capture_vectors=True)
        records = [add_call(observation), add_call(observation, status="failed")]
        retained = observation.calls
        observation.release_exported()
        self.assertIs(observation.calls, retained)
        self.assertEqual(observation.calls, records)
        self.assertEqual(set(observation.vectors), {"call_0", "call_1"})

    def test_release_prefix_preserves_suffix_total_rows_and_monotonic_keys(self):
        observation = RMObservation(capture_vectors=True)
        exported = add_call(observation, image_rows=3)
        unexported = add_call(observation, image_rows=2, status="failed")
        vector = observation.vectors["call_0"]
        retained_vector = observation.vectors["call_1"]
        observation.release_exported(calls=[exported], vectors={"call_0": vector})
        self.assertEqual(observation.calls, [unexported])
        self.assertEqual(observation.call_count, 2)
        self.assertEqual(observation.image_rows, 5)
        latest = add_call(observation, image_rows=4)
        self.assertEqual(latest["call_index"], 2)
        self.assertEqual(set(observation.vectors), {"call_1", "call_2"})
        self.assertEqual(observation.image_rows, 9)
        self.assertIs(observation.vectors["call_1"], retained_vector)

    def test_equal_or_nonprefix_records_are_rejected_without_releasing_anything(self):
        observation = RMObservation(capture_vectors=True)
        first, second = add_call(observation), add_call(observation)
        for calls in ([dict(first)], [second], [first, second, first]):
            with self.subTest(calls=calls):
                with self.assertRaisesRegex(ValueError, "retained prefix"):
                    observation.release_exported(calls=calls)
                self.assertEqual(observation.calls, [first, second])
                self.assertEqual(observation.call_count, 2)

    def test_wrong_vector_identity_prevents_call_release_atomically(self):
        observation = RMObservation(capture_vectors=True)
        record = add_call(observation)
        for vectors in ({"call_0": observation.vectors["call_0"].copy()},
                        {"missing": observation.vectors["call_0"]}):
            with self.subTest(keys=tuple(vectors)):
                with self.assertRaisesRegex(ValueError, "retained identities"):
                    observation.release_exported(calls=[record], vectors=vectors)
                self.assertEqual(observation.calls, [record])
                self.assertEqual(set(observation.vectors), {"call_0"})

    def test_vector_evidence_cannot_silently_overwrite_an_existing_key(self):
        observation = RMObservation(capture_vectors=True)
        matrix = np.zeros((1, 512), np.float32)
        observation.retain(matrix)
        first = observation.vectors["call_0"]
        with self.assertRaisesRegex(ValueError, "already exists"):
            observation.retain(matrix + 1)
        self.assertIs(observation.vectors["call_0"], first)

    def test_stock_call_number_continues_after_export_and_failure(self):
        class RM:
            def __call__(self, docs):
                return self._embed(docs)
        values = np.zeros((1, 512), np.float32)
        values[:, 0] = 1
        observation = RMObservation(capture_vectors=True)
        with patch.dict(sys.modules, {"lotus.models.rm": SimpleNamespace(RM=RM)}):
            rm = observe_stock_rm(lambda docs: values, observation=observation)
            rm([Image.new("RGB", (2, 2))])
            observation.release_exported(calls=list(observation.calls))
            rm([Image.new("RGB", (2, 2))])
            self.assertEqual(observation.calls[0]["call_index"], 1)
            self.assertEqual(observation.image_rows, 2)
            self.assertEqual(set(observation.vectors), {"call_0", "call_1"})


class WorkflowSnapshotTests(unittest.TestCase):
    def run_fixture(self, root, *, fail=False, phase_error=None, settings_error=None):
        observation = RMObservation()
        image = Image.new("RGB", (2, 2))
        class Frame:
            index = SimpleNamespace(equals=lambda value: True)
            iloc = [{"doc_id": "a"}]
            def __len__(self):
                return 1
            def __getitem__(self, name):
                return [image] if name == "image" else ["a"]
            def itertuples(self, **kwargs):
                return iter([SimpleNamespace(doc_id="a", image=image)])
            def sem_index(self, column, directory):
                add_call(observation)
                directory = Path(directory)
                directory.mkdir()
                (directory / "vecs").write_bytes(b"matrix fixture")
                (directory / "index").write_bytes(b"index fixture")
                return self
            def sem_search(self, *args, **kwargs):
                add_call(observation, status="failed" if fail else "succeeded")
                if fail:
                    raise RuntimeError("query fixture failed")
                class Result:
                    index = [0]
                    def __getitem__(self, key):
                        return SimpleNamespace(tolist=lambda: ["a"]) if key == "doc_id" else [1.0]
                return Result()
        class Calls(list):
            copies = 0
            def __iter__(self):
                self.copies += 1
                return super().__iter__()
        observation.calls = Calls()
        report = {}
        @contextmanager
        def settings(*args):
            yield
            if settings_error is not None:
                raise settings_error
        def observer(phase):
            if phase == "after_queries" and phase_error is not None:
                raise phase_error
        with ExitStack() as stack:
            stack.enter_context(patch.dict(sys.modules, {
                "pandas": SimpleNamespace(RangeIndex=lambda n: object()),
                "lotus.vector_store": SimpleNamespace(FaissVS=lambda: object()),
            }))
            stack.enter_context(patch.object(image_workflow, "native_settings", settings))
            kwargs = dict(index_dir=root / "index", embedding_identity={}, k=1,
                          query_repeats=3, report=report, phase_observer=observer)
            queries = [image_workflow.RetrievalQuery("q", "query")]
            if fail:
                with self.assertRaisesRegex(RuntimeError, "query fixture failed"):
                    image_workflow.run_workflow(Frame(), SimpleNamespace(observation=observation), queries, **kwargs)
            elif phase_error is not None or settings_error is not None:
                error = phase_error if phase_error is not None else settings_error
                with self.assertRaises(type(error)) as caught:
                    image_workflow.run_workflow(Frame(), SimpleNamespace(observation=observation), queries, **kwargs)
                self.assertIs(caught.exception, error)
            else:
                image_workflow.run_workflow(Frame(), SimpleNamespace(observation=observation), queries, **kwargs)
        return observation, report

    def test_success_has_one_final_owned_history_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            observation, report = self.run_fixture(Path(directory))
        self.assertIsNot(report["rm_calls"], observation.calls)
        self.assertEqual(observation.calls.copies, 1)
        self.assertEqual(len(report["queries"]), 3)
        observation.release_exported(calls=report["rm_calls"])
        add_call(observation)
        self.assertEqual(len(report["rm_calls"]), 4)
        self.assertEqual([x["rm_calls"][0]["call_index"] for x in report["queries"]], [1, 2, 3])

    def test_failure_snapshots_failed_query_before_returning_error(self):
        with tempfile.TemporaryDirectory() as directory:
            observation, report = self.run_fixture(Path(directory), fail=True)
        self.assertIsNot(report["rm_calls"], observation.calls)
        self.assertEqual(observation.calls.copies, 1)
        self.assertEqual(report["queries"][0]["rm_calls"][0]["status"], "failed")
        observation.calls.append(dict(call_index=2, image_rows=1))
        self.assertEqual(len(report["rm_calls"]), 2)

    def test_after_queries_error_or_cancellation_keeps_one_owned_snapshot(self):
        for error in (RuntimeError("phase observer failed"), asyncio.CancelledError("phase cancelled")):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as directory:
                observation, report = self.run_fixture(Path(directory), phase_error=error)
                self.assertEqual(len(report["rm_calls"]), 4)
                self.assertEqual(observation.calls.copies, 1)
                self.assertIsNot(report["rm_calls"], observation.calls)
                observation.calls.append(dict(call_index=4, image_rows=1))
                self.assertEqual(len(report["rm_calls"]), 4)

    def test_settings_exit_error_keeps_one_owned_snapshot_after_successful_queries(self):
        error = RuntimeError("native settings exit failed")
        with tempfile.TemporaryDirectory() as directory:
            observation, report = self.run_fixture(Path(directory), settings_error=error)
        self.assertEqual(len(report["rm_calls"]), 4)
        self.assertEqual(observation.calls.copies, 1)
        self.assertIsNot(report["rm_calls"], observation.calls)
        self.assertTrue(all(query["status"] == "succeeded" for query in report["queries"]))


class CLIExportTests(unittest.TestCase):
    def run_fixture(self, root, *, vector_failure=False, result_failure=False,
                    receipt_and_update_failure=False, append_during_write=False,
                    append_during_close=False):
        path = Path(__file__).parents[2] / "scripts/experiments/run_lotus_image_retrieval.py"
        spec = importlib.util.spec_from_file_location("lotus_image_release_cli_fixture", path)
        cli = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cli)
        observation = RMObservation(capture_vectors=True)
        initial = add_call(observation, image_rows=3)
        executor = SimpleNamespace(execution_observations=[dict(batches=[dict(rows=3)])], close=lambda: None)
        if append_during_close:
            executor.close = lambda: executor.execution_observations.append(dict(cleanup=True))
        rm = SimpleNamespace(observation=observation)
        raw = b"bounded CLI source fixture"
        (root / "image").write_bytes(raw)
        (root / "identity.json").write_text(json.dumps(dict(qualified=True, model_dtype="float32")))
        manifest = SimpleNamespace(root=root, sha256="0" * 64, entries=(SimpleNamespace(
            path="image", sha256=hashlib.sha256(raw).hexdigest(), doc_id="a"),))
        reports = []
        class Stock:
            def __init__(self, *a, **k):
                self.transformer = SimpleNamespace(to=lambda **k: None)
        def workflow(*a, **kwargs):
            report = kwargs["report"]
            report.update(status="succeeded", index_s=0.0, queries=[])
            reports.append(report)
        original_write = Path.write_text
        result_writes = []
        def write(path, text, *args, **kwargs):
            if path.name == "evidence-write.json" and receipt_and_update_failure:
                raise OSError("receipt fixture failed")
            if path.name == "result.json":
                result_writes.append(text)
                if result_failure or (receipt_and_update_failure and len(result_writes) > 1):
                    raise OSError("result fixture failed")
                self.assertIsNot(reports[0]["rm_calls"], observation.calls)
                self.assertIsNot(reports[0]["execution_observations"], executor.execution_observations)
                if append_during_write:
                    add_call(observation, image_rows=2, status="failed")
                    executor.execution_observations.append(dict(later=True))
            return original_write(path, text, *args, **kwargs)
        with ExitStack() as stack:
            for name in ("validate_image_source", "validate_embedding_identity"):
                stack.enter_context(patch.object(cli, name, lambda *a, **k: {}))
            for name, value in {
                    "version": lambda name: "fixture", "configure_torch_thread_pools": lambda *a: {},
                    "DirectImageExecutor": lambda **k: executor, "make_image_rm": lambda *a, **k: rm,
                    "load_image_file_manifest": lambda *a: manifest,
                    "decode_rgb_image": lambda raw: Image.new("RGB", (2, 3)),
                    "image_frame": lambda *a: object(), "run_workflow": workflow}.items():
                stack.enter_context(patch.object(cli, name, value))
            stack.enter_context(patch.dict(sys.modules, {
                "lotus.models": SimpleNamespace(SentenceTransformersRM=Stock),
                "torch": SimpleNamespace(float32="float32")}))
            stack.enter_context(patch.object(Path, "write_text", write))
            if vector_failure:
                stack.enter_context(patch.object(np, "savez_compressed", side_effect=OSError("vector fixture failed")))
            stack.enter_context(redirect_stdout(io.StringIO()))
            stack.enter_context(redirect_stderr(io.StringIO()))
            args = ["--manifest", str(root / "manifest"), "--image-root", str(root),
                    "--stock-model", str(root), "--model", str(root), "--arm", "direct",
                    "--embedding-identity", str(root / "identity.json"), "--rows", "1",
                    "--index-dir", str(root / "index"), "--output-dir", str(root / "output"),
                    "--retain-query-vectors", "--device", "cpu"]
            code = cli.main(args)
        return code, observation, executor, reports[0], initial

    def test_success_releases_only_exported_calls_vectors_and_execution_records(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code, observation, executor, report, initial = self.run_fixture(root)
            saved = json.loads((root / "output/result.json").read_text())
            with np.load(root / "output/rm-vectors.npz") as vectors:
                self.assertEqual(vectors.files, ["call_0"])
        self.assertEqual(code, 0)
        self.assertEqual(observation.calls, [])
        self.assertEqual(observation.vectors, {})
        self.assertEqual(executor.execution_observations, [])
        self.assertEqual(observation.image_rows, 3)
        self.assertEqual(observation.call_count, 1)
        self.assertEqual(report["rm_calls"], saved["rm_calls"])
        self.assertIs(report["rm_calls"][0], initial)
        self.assertEqual(len(report["execution_observations"]), 1)

    def test_failed_json_retains_calls_and_execution_but_releases_persisted_vectors(self):
        with tempfile.TemporaryDirectory() as directory:
            code, observation, executor, report, initial = self.run_fixture(Path(directory), result_failure=True)
        self.assertEqual(code, 1)
        self.assertEqual(observation.calls, [initial])
        self.assertEqual(len(executor.execution_observations), 1)
        self.assertEqual(observation.vectors, {})
        self.assertEqual(report["evidence_write_errors"], ["result: OSError"])

    def test_failed_npz_keeps_vectors_while_persisted_json_releases_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            code, observation, executor, report, _ = self.run_fixture(Path(directory), vector_failure=True)
        self.assertEqual(code, 1)
        self.assertEqual(observation.calls, [])
        self.assertEqual(executor.execution_observations, [])
        self.assertEqual(set(observation.vectors), {"call_0"})
        self.assertEqual(report["evidence_write_errors"], ["vectors: OSError"])

    def test_failed_receipt_and_result_rewrite_preserve_call_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            code, observation, executor, report, initial = self.run_fixture(
                Path(directory), receipt_and_update_failure=True)
        self.assertEqual(code, 1)
        self.assertEqual(observation.calls, [initial])
        self.assertEqual(len(executor.execution_observations), 1)
        self.assertEqual(report["evidence_write_errors"], ["receipt: OSError"])

    def test_records_added_after_snapshot_are_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            code, observation, executor, report, _ = self.run_fixture(Path(directory), append_during_write=True)
        self.assertEqual(code, 0)
        self.assertEqual([x["call_index"] for x in observation.calls], [1])
        self.assertEqual(observation.calls[0]["status"], "failed")
        self.assertEqual(observation.image_rows, 5)
        self.assertEqual(set(observation.vectors), {"call_1"})
        self.assertEqual(executor.execution_observations, [dict(later=True)])
        self.assertEqual(len(report["rm_calls"]), 1)
        self.assertEqual(len(report["execution_observations"]), 1)

    def test_cleanup_records_are_included_in_the_final_export_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code, observation, executor, report, _ = self.run_fixture(root, append_during_close=True)
            saved = json.loads((root / "output/result.json").read_text())
        self.assertEqual(code, 0)
        self.assertEqual(len(report["execution_observations"]), 2)
        self.assertEqual(saved["execution_observations"][-1], dict(cleanup=True))
        self.assertEqual(executor.execution_observations, [])
        self.assertEqual(observation.calls, [])


if __name__ == "__main__":
    unittest.main()
