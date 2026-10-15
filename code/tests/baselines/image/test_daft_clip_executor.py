"""Encoded image / ID behavior and opt-in real Daft graph with a CPU kernel.

The stand-in never loads CLIP or uses a GPU. Daft decode, stateful UDF execution,
batching and result consumption are real when SEMLOOM_DAFT_IMAGE_SDK_TEST=1.
"""
import io
import gc
import json
import os
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from src.baselines.image.frameworks import daft_clip as native
from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult


def trace(event, **values):
    line = json.dumps(dict(event=event, pid=os.getpid(), **values)) + "\n"
    descriptor = os.open(os.environ["SEMLOOM_DAFT_IMAGE_TRACE"], os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, line.encode())
    finally:
        os.close(descriptor)


class Processor:
    def __call__(self, *, images, return_tensors, input_data_format):
        if return_tensors != "np" or input_data_format != "channels_last":
            raise AssertionError("processor input layout changed")
        means = [np.asarray(image, dtype=np.float32).mean(axis=(0, 1)) for image in images]
        return {"pixel_values": np.asarray(means, dtype=np.float32)[:, :, None, None]}


def load_processor(path):
    trace("processor_load")
    return Processor()


def expected_vectors(colors):
    matrix = np.ones((len(colors), 512), dtype=np.float32)
    matrix[:, :3] = np.asarray(colors, dtype=np.float32) + 1
    return matrix / np.linalg.norm(matrix, axis=1, keepdims=True)


class Actor:
    def __init__(self, model_path, **options):
        trace("model_load", options=options)
        if options["device"] != "cpu" or options["detailed_stage_timing"]:
            raise AssertionError("CPU SDK kernel must not load a GPU or detailed timing")
        self.semantics = EmbeddingSemantics("fixture", "fixture", 512)

    def __del__(self):
        if os.environ.get("SEMLOOM_DAFT_IMAGE_TRACE"):
            trace("model_destroy")

    def embed(self, batch):
        trace("embed", ids=batch.doc_ids, rows=len(batch.doc_ids))
        if os.environ.get("SEMLOOM_DAFT_IMAGE_FAKE_ERROR") == "model":
            raise ValueError("fixture model failure")
        colors = batch.payload[:, :, 0, 0]
        matrix = expected_vectors(colors)
        if os.environ.get("SEMLOOM_DAFT_IMAGE_FAKE_ERROR") == "norm":
            matrix *= 2
        trace("embed_done", ids=batch.doc_ids)
        return ImageEmbeddingResult(batch.doc_ids, matrix, self.semantics, 0)

    def synchronize(self):
        trace("synchronize")


class DaftClipOptionsTests(unittest.TestCase):
    def test_invalid_resources_rejected_without_sdk_or_model_access(self):
        for options in (dict(batch_size=True), dict(batch_size=0), dict(model_workers=0),
                        dict(gpus_per_worker=float("nan")), dict(gpus_per_worker=2),
                        dict(device="cpu"), dict(gpus_per_worker=0), dict(dtype="int8"),
                        dict(max_batch_rows=1, batch_size=2)):
            with self.subTest(options=options), self.assertRaises(ValueError):
                native.DaftClipExecutor(model_path="missing", **options)

    def test_batch_rejects_ids_payloads_and_limit_before_native_graph(self):
        executor = object.__new__(native.DaftClipExecutor)
        executor.input_kind = "encoded_bytes"
        executor.identity = dict(max_batch_rows=2)
        for ids, images in ((["a"], [b"x"]), (("a", "a"), [b"x", b"y"]),
                            ((1,), [b"x"]), (("",), [b"x"]),
                            (("a",), ["path.jpg"]), (("a",), [b""]),
                            (("a",), []), (("a", "b", "c"), [b"x"] * 3)):
            with self.subTest(ids=ids), self.assertRaises(ValueError):
                executor.embed_batch(ids, images)


    def test_verifier_saves_completed_results_when_lease_release_fails(self):
        from tests.baselines.image import verify_daft_clip_gpu as verifier

        class Lease:
            def release(self):
                raise RuntimeError("fixture lease ownership changed")

        def qualified(args, report):
            report.update(status="passed", comparison=[dict(max_abs=0)])

        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "new-run"
            argv = ["verify", "--model", "fixture", "--processor", "fixture",
                    "--image-root", "fixture", "--artifact-root", temporary,
                    "--output", str(output), "--source-hash", "fixture", "--base-sha", "fixture"]
            with patch.object(sys, "argv", argv), patch.object(verifier, "run", qualified), \
                    patch.object(verifier, "acquire_runner_lease", return_value=Lease()):
                with self.assertRaisesRegex(RuntimeError, "ownership changed"):
                    verifier.main()
            report = json.loads((output / "report.json").read_text())
            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["shared_lease_released"], "unconfirmed")
            self.assertEqual(report["comparison"], [dict(max_abs=0)])
            self.assertIn("ownership changed", report["lease_release_error"])


    def test_native_release_checks_model_parameters_and_buffers_without_gpu(self):
        from tests.baselines.image import verify_daft_clip_gpu as verifier

        class Tensor:
            pass

        class Model:
            def __init__(self):
                self.p = [Tensor() for _ in range(3)]
                self.b = [Tensor()]

            def parameters(self):
                return iter(self.p)

            def buffers(self):
                return iter(self.b)

        def initialize(actor, *args, **kwargs):
            actor._model, actor._device, actor._dtype = Model(), "cpu", "fixture"

        for retained in (None, "model", "parameter", "buffer"):
            events = []

            def observe(event, **fields):
                events.append(dict(event=event, pid=os.getpid(), **fields))

            with self.subTest(retained=retained), \
                    patch.object(verifier.ClipTensorActor, "__init__", initialize), \
                    patch.object(verifier, "record", side_effect=observe):
                actor = verifier.ObservedActor("fixture")
                hold = (actor._model if retained == "model" else
                        actor._model.p[0] if retained == "parameter" else
                        actor._model.b[0] if retained == "buffer" else None)
                del actor
                gc.collect()
                if retained is not None:
                    self.assertIsNotNone(hold)
                    with self.assertRaisesRegex(RuntimeError, "destruction unconfirmed"):
                        verifier.check_model_release(events)
                else:
                    self.assertEqual(verifier.check_model_release(events)["parameters_destroyed"], 3)
                hold = None
                gc.collect()
                self.assertEqual(verifier.check_model_release(events)["buffers_destroyed"], 1)

    def test_native_release_rejects_missing_load_or_duplicate_finalization(self):
        from tests.baselines.image import verify_daft_clip_gpu as verifier
        with self.assertRaisesRegex(RuntimeError, "load identity"):
            verifier.check_model_release([])
        event = dict(event="model_loaded", pid=1, model_instance_id="fixture", parameters=0, buffers=0)
        closed = [dict(event=name, pid=1, model_instance_id="fixture")
                  for name in ["model_destroyed", "model_module_destroyed"]]
        with self.assertRaisesRegex(RuntimeError, "destruction unconfirmed"):
            verifier.check_model_release([event, *closed, closed[0]])


@unittest.skipUnless(os.environ.get("SEMLOOM_DAFT_IMAGE_SDK_TEST") == "1",
                     "requires fixed Daft 0.7.21; set SEMLOOM_DAFT_IMAGE_SDK_TEST=1")
class DaftClipSdkTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        for name in ("pytorch_model.bin", "config.json", "preprocessor_config.json"):
            (root / name).write_bytes(b"fixture-" + name.encode())
        self.trace_path = root / "trace.jsonl"
        self.environment = patch.dict(os.environ, {"SEMLOOM_DAFT_IMAGE_TRACE": str(self.trace_path)})
        self.environment.start()
        self.patches = [patch.object(native, "ClipTensorActor", Actor),
                        patch.object(native, "_load_processor", load_processor)]
        for item in self.patches:
            item.start()
        self.executor = native.DaftClipExecutor(model_path=str(root), batch_size=2,
                                               gpus_per_worker=0, device="cpu", input_kind="encoded_bytes")

    def tearDown(self):
        if not self.executor._unconfirmed_work:
            self.executor.close()
        for item in reversed(self.patches):
            item.stop()
        self.environment.stop()
        self.directory.cleanup()

    def png(self, mode="RGB", color=(25, 80, 120)):
        from PIL import Image

        image = Image.new(mode, (13, 9), color)
        output = io.BytesIO()
        image.save(output, format="PNG")
        return output.getvalue(), np.asarray(image.convert("RGB")).mean(axis=(0, 1))

    def events(self):
        return [json.loads(line) for line in self.trace_path.read_text().splitlines()]

    def test_native_rgb_decode_batches_tail_ids_and_full_output(self):
        fixtures = [self.png(color=(index * 20, 80, 120)) for index in range(3)]
        fixtures += [self.png("L", 17), self.png("RGBA", (6, 7, 8, 110))]
        ids = ("duplicate-image/4", "unicode-图", "0", "tail/L", "tail/RGBA")
        result = self.executor.embed_batch(ids, [item[0] for item in fixtures])
        self.assertEqual(result.doc_ids, ids)
        self.assertEqual(result.embeddings.dtype, np.dtype("float32"))
        self.assertEqual(result.embeddings.shape, (5, 512))
        np.testing.assert_allclose(result.embeddings, expected_vectors([x[1] for x in fixtures]), atol=1e-7)
        events = self.events()
        batches = [x for x in events if x["event"] == "embed"]
        self.assertEqual(sorted(x["rows"] for x in batches), [1, 2, 2])
        self.assertEqual([x["event"] for x in events].count("model_load"), 1)
        self.assertEqual(sum(x["event"] == "embed_done" for x in events), 3)
        self.assertFalse(self.executor._unconfirmed_work)
        self.executor.close()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            self.executor.embed_batch(("a",), [fixtures[0][0]])

    def test_native_stateful_instance_reused_across_graphs_and_released_on_close(self):
        image, color = self.png()
        for index in range(2):
            self.executor.embed_batch(tuple(f"{index}:{row}" for row in range(5)), [image] * 5)
        events = self.events()
        self.assertEqual(sum(x["event"] == "model_load" for x in events), 1)
        self.assertEqual(sum(x["event"] == "embed" for x in events), 6)
        self.executor.close()
        self.assertEqual(sum(x["event"] == "model_destroy" for x in self.events()), 1)

    def test_empty_batch_does_not_load_a_model(self):
        result = self.executor.embed_batch((), [])
        self.assertEqual(result.embeddings.shape, (0, 512))
        self.assertEqual(result.service_s, 0)
        self.assertFalse(self.trace_path.exists())

    def test_invalid_norm_is_reported_after_complete_graph_consumption(self):
        image, color = self.png()
        with patch.dict(os.environ, {"SEMLOOM_DAFT_IMAGE_FAKE_ERROR": "norm"}):
            with self.assertRaisesRegex(ValueError, "unit normalized"):
                self.executor.embed_batch(("a", "b", "c"), [image] * 3)
        self.assertEqual(sum(x["event"] == "embed_done" for x in self.events()), 2)
        self.assertFalse(self.executor._unconfirmed_work)

    def test_model_error_preserves_failure_and_refuses_unknown_worker_exit(self):
        image, color = self.png()
        with patch.dict(os.environ, {"SEMLOOM_DAFT_IMAGE_FAKE_ERROR": "model"}):
            with self.assertRaisesRegex(Exception, "fixture model failure"):
                self.executor.embed_batch(("a",), [image])
        self.assertTrue(self.executor._unconfirmed_work)
        self.assertEqual(sum(x["event"] == "synchronize" for x in self.events()), 1)
        with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
            self.executor.embed_batch(("b",), [image])
        with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
            self.executor.close()

    def test_decode_error_is_preserved_and_never_treated_as_a_missing_row(self):
        with self.assertRaises(Exception):
            self.executor.embed_batch(("broken",), [b"not an image"])
        self.assertTrue(self.executor._unconfirmed_work)

    def test_close_rejects_an_active_batch(self):
        self.executor._lock.acquire()
        try:
            with self.assertRaisesRegex(RuntimeError, "active"):
                self.executor.close()
        finally:
            self.executor._lock.release()

    def test_pinned_version_and_native_runner_required(self):
        import daft
        with patch.object(daft, "__version__", "other"):
            with self.assertRaisesRegex(RuntimeError, "0.7.21"):
                native.DaftClipExecutor(model_path=self.directory.name, input_kind="encoded_bytes")


if __name__ == "__main__":
    unittest.main()
