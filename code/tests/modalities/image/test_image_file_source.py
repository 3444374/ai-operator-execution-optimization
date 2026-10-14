import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch
from urllib.parse import unquote, urlparse

import numpy as np
from PIL import Image

from src.baselines.image.frameworks.ray_data import _bind_file_rows, _read_ray_image_files
from src.modalities.image.execution import ExecutionResult
from src.modalities.image.file_source import (
    build_daft_image_file_source, build_image_file_manifest, load_image_file_manifest,
)
from src.modalities.image.source import ImageSourceConfig

SCRIPT = Path(__file__).resolve().parents[3] / "scripts/experiments/run_image_clip_e2e.py"
SPEC = importlib.util.spec_from_file_location("image_file_runner", SCRIPT)
RUNNER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RUNNER
SPEC.loader.exec_module(RUNNER)


class ImageFileSourceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "images"
        self.root.mkdir()
        for source_id in (139, 285, 632):
            Image.new("RGB", (3, 2), (source_id % 255, 80, 160)).save(
                self.root / f"{source_id:012d}.jpg", format="JPEG")
        self.path = Path(self.temp.name) / "manifest.json"
        self.manifest = build_image_file_manifest(root=self.root, workload_name="coco_val2017", output=self.path)

    def rewrite(self, mutate):
        value = json.loads(self.path.read_text())
        mutate(value)
        self.path.write_text(json.dumps(value))

    def test_real_jpeg_manifest_records_content_and_pass_identities(self):
        selected = self.manifest.entries
        self.assertEqual([entry.doc_id for entry in selected], ["139", "285", "632"])
        for entry in selected:
            self.assertEqual(entry.sha256, hashlib.sha256((self.root / entry.path).read_bytes()).hexdigest())
        ids, metadata = self.manifest.metadata(ImageSourceConfig("coco_val2017", 2, 1, 2))
        self.assertEqual(ids, {"285#pass=1", "632#pass=1", "285#pass=2", "632#pass=2"})
        self.assertEqual(metadata["input_encoded_bytes"], 2 * sum(e.image_bytes for e in selected[1:]))
        self.assertEqual(self.manifest.preparation["content_hash_bytes"], sum(e.image_bytes for e in selected))
        with self.assertRaises(FileExistsError):
            build_image_file_manifest(root=self.root, workload_name="coco_val2017", output=self.path)

    def test_metadata_preflight_does_not_read_or_rehash_jpeg_content(self):
        original = Path.open
        def guarded(path, *args, **kwargs):
            if path.suffix == ".jpg":
                raise AssertionError("preflight read an image payload")
            return original(path, *args, **kwargs)
        with patch.object(Path, "open", guarded):
            load_image_file_manifest(self.path, self.root)
            args = self.args()
            processor, capture, sidecar, provenance = RUNNER._validate_image_run_args(args)
            RUNNER._load_image_run_setup(args, processor=processor, embedding_target=capture,
                                         embedding_sidecar=sidecar, provenance=provenance)

    def test_changed_file_is_rejected_even_when_size_and_mtime_are_preserved(self):
        entry = self.manifest.entries[0]
        path = self.root / entry.path
        before = path.stat()
        content = path.read_bytes()
        path.write_bytes(content[:-1] + bytes([content[-1] ^ 1]))
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        with self.assertRaisesRegex(ValueError, "metadata changed"):
            load_image_file_manifest(self.path, self.root)

    def test_empty_duplicate_wrong_identity_and_invalid_manifest_are_rejected(self):
        original = self.path.read_text()
        cases = [
            lambda p: p.update(entries=[]),
            lambda p: p["entries"].append(p["entries"][0]),
            lambda p: p["entries"][0].update(doc_id="999"),
            lambda p: p["entries"][0].update(sha256="bad"),
            lambda p: p["entries"][0].update(image_bytes=True),
            lambda p: p["preparation"].update(content_hash_s=float("nan")),
        ]
        for mutate in cases:
            with self.subTest(mutate=mutate):
                self.path.write_text(original)
                self.rewrite(mutate)
                with self.assertRaises(ValueError):
                    load_image_file_manifest(self.path, self.root)
        self.path.write_text("[]")
        with self.assertRaises(ValueError):
            load_image_file_manifest(self.path, self.root)

    def test_paths_outside_root_symlinks_and_invalid_suffixes_are_rejected(self):
        original = self.path.read_text()
        for path in ("../outside.jpg", "/absolute.jpg", "./000000000139.jpg", "image.png", "a\\b.jpg"):
            self.path.write_text(original)
            self.rewrite(lambda p: p["entries"][0].update(path=path))
            with self.assertRaises(ValueError):
                load_image_file_manifest(self.path, self.root)
        self.path.write_text(original)
        target = self.root / self.manifest.entries[0].path
        target.unlink()
        target.symlink_to(self.root / self.manifest.entries[1].path)
        with self.assertRaisesRegex(ValueError, "symlink"):
            load_image_file_manifest(self.path, self.root)

    def test_build_rejects_empty_directory_duplicate_ids_and_non_jpeg_payload(self):
        empty = Path(self.temp.name) / "empty"
        empty.mkdir()
        with self.assertRaisesRegex(ValueError, "empty"):
            build_image_file_manifest(root=empty, workload_name="test", output=empty / "new.json")
        (empty / "1.jpg").write_bytes((self.root / self.manifest.entries[0].path).read_bytes())
        (empty / "000000000001.jpg").write_bytes((empty / "1.jpg").read_bytes())
        with self.assertRaisesRegex(ValueError, "duplicate"):
            build_image_file_manifest(root=empty, workload_name="test", output=empty / "new.json")
        (empty / "000000000001.jpg").unlink()
        (empty / "1.jpg").write_bytes(b"not a jpeg")
        with self.assertRaisesRegex(ValueError, "JPEG header"):
            build_image_file_manifest(root=empty, workload_name="test", output=empty / "new.json")

    def test_wrong_workload_missing_rows_and_missing_files_fail(self):
        for config in (ImageSourceConfig("other", 1), ImageSourceConfig("coco_val2017", 4)):
            with self.assertRaises(ValueError):
                self.manifest.metadata(config)
        (self.root / self.manifest.entries[0].path).unlink()
        with self.assertRaises(FileNotFoundError):
            load_image_file_manifest(self.path, self.root)

    def test_daft_and_native_ray_source_protocols_read_same_real_jpegs_lazily(self):
        # These SDK doubles exercise adapter protocols, not Ray/Daft runtime scheduling.
        reads = []
        native_batches = []
        class Frame:
            def __init__(self, columns): self.columns = columns
            def into_batches(self, batch_size): native_batches.append(batch_size); return self
            def with_column(self, name, expression): return self
            def rows(self):
                return [(doc_id, Path(unquote(urlparse(uri).path)).read_bytes())
                        for doc_id, uri in zip(self.columns["doc_id"], self.columns["path"], strict=True)]
        daft = ModuleType("daft")
        daft.from_pydict = Frame
        daft.col = lambda name: name
        functions = ModuleType("daft.functions")
        url = ModuleType("daft.functions.url")
        def download(expression, **kwargs):
            self.assertEqual(kwargs, {"max_connections": 2, "on_error": "raise"})
            return expression
        url.download = download
        class Dataset:
            def __init__(self, paths):
                self.parts = [(paths, [])]
                self.context = SimpleNamespace(execution_options=SimpleNamespace(preserve_order=False))
            def map_batches(self, fn, **kwargs):
                self.parts[0][1].append(fn)
                return self
            def union(self, other): self.parts.extend(other.parts); return self
            def rows(self):
                result = []
                for paths, transforms in self.parts:
                    reads.extend(paths)
                    batch = {"path": np.asarray(paths), "bytes": np.asarray([Path(p).read_bytes() for p in paths], dtype=object)}
                    for fn in transforms: batch = fn(batch)
                    result.extend(zip(batch["doc_id"], batch["image"], strict=True))
                return result
        ray = ModuleType("ray")
        ray.data = ModuleType("ray.data")
        def native_reader(paths, **kwargs):
            self.assertTrue(kwargs["include_paths"])
            self.assertFalse(kwargs["ignore_missing_paths"])
            self.assertIsNone(kwargs["shuffle"])
            self.assertEqual(kwargs["num_cpus"], 1)
            return Dataset(paths)
        ray.data.read_binary_files = native_reader
        config = ImageSourceConfig("coco_val2017", 2, 1, 2)
        with patch.dict(sys.modules, {"daft": daft, "daft.functions": functions, "daft.functions.url": url,
                                      "ray": ray, "ray.data": ray.data}):
            frame = build_daft_image_file_source(self.manifest, config, batch_size=2, max_connections=2)
            dataset = _read_ray_image_files(self.manifest, config, 2)
            self.assertEqual(reads, [])
            self.assertNotIn("image", frame.columns)
            self.assertEqual(native_batches, [2])
            self.assertTrue(dataset.context.execution_options.preserve_order)
            self.assertEqual(frame.rows(), dataset.rows())
        self.assertEqual([str(doc_id) for doc_id, _ in frame.rows()],
                         ["285#pass=1", "632#pass=1", "285#pass=2", "632#pass=2"])

    def test_native_reader_rejects_unexpected_path_and_changed_size(self):
        arguments = dict(rows_by_path={"/allowed/1.jpg": ("1", 4)}, pass_index=1, passes=1)
        for path, data in (("/unexpected/1.jpg", b"abcd"), ("/allowed/1.jpg", b"short")):
            with self.assertRaises(ValueError):
                _bind_file_rows({"path": [path], "bytes": [data]}, **arguments)

    def test_file_cli_and_setup_do_not_require_or_query_postgres(self):
        args = self.args()
        processor, target, sidecar, provenance = RUNNER._validate_image_run_args(args)
        with patch.object(RUNNER, "read_database_metadata", side_effect=AssertionError("PG query")):
            setup = RUNNER._load_image_run_setup(args, processor=processor, embedding_target=target,
                                                embedding_sidecar=sidecar, provenance=provenance)
        self.assertEqual(setup.formal_ids, {"139", "285", "632"})
        self.assertEqual(setup.database_metadata["server_version"], "not_applicable_file_source")
        for field in ("source_manifest", "image_root"):
            invalid = self.args()
            setattr(invalid, field, "")
            with self.assertRaises(SystemExit): RUNNER._validate_image_run_args(invalid)

    def test_file_ready_and_other_outputs_cannot_overwrite_manifest_or_jpeg_inputs(self):
        for field in ("out_csv", "out_manifest", "formal_ready_file"):
            for target in (self.path, self.root / self.manifest.entries[0].path):
                with self.subTest(field=field, target=target):
                    args = self.args()
                    setattr(args, field, str(target))
                    if field == "formal_ready_file":
                        args.formal_start_barrier_file = str(Path(self.temp.name) / "barrier.json")
                    with self.assertRaises(SystemExit):
                        processor, capture, sidecar, provenance = RUNNER._validate_image_run_args(args)
                        RUNNER._load_image_run_setup(args, processor=processor, embedding_target=capture,
                                                     embedding_sidecar=sidecar, provenance=provenance)
        fixed = Path(self.temp.name) / "fixed.json.tmp"
        fixed.write_bytes(self.path.read_bytes())
        args = self.args()
        args.source_manifest = str(fixed)
        args.formal_ready_file = str(fixed.with_suffix(""))
        args.formal_start_barrier_file = str(Path(self.temp.name) / "barrier.json")
        with self.assertRaises(SystemExit): RUNNER._validate_image_run_args(args)
        self.assertEqual(fixed.read_bytes(), self.path.read_bytes())

    def test_existing_output_hardlinks_cannot_alias_fixed_manifest_or_jpeg(self):
        ready = Path(self.temp.name) / "ready.json"
        temporary = ready.with_suffix(".json.tmp")
        image = self.root / self.manifest.entries[0].path
        original = image.read_bytes()
        for source in ("jpeg", "manifest"):
            with self.subTest(source=source):
                if temporary.exists(): temporary.unlink()
                path = Path(self.temp.name) / f"fixed-{source}.json"
                if source == "jpeg": os.link(image, temporary)
                build_image_file_manifest(root=self.root, workload_name="coco_val2017", output=path)
                if source == "manifest": os.link(path, temporary)
                args = self.args()
                args.source_manifest = str(path)
                args.formal_ready_file = str(ready)
                args.formal_start_barrier_file = str(Path(self.temp.name) / "barrier.json")
                with self.assertRaisesRegex(SystemExit, "alias fixed inputs"):
                    processor, capture, sidecar, provenance = RUNNER._validate_image_run_args(args)
                    RUNNER._load_image_run_setup(args, processor=processor, embedding_target=capture,
                                                 embedding_sidecar=sidecar, provenance=provenance)
                self.assertEqual(image.read_bytes(), original)

    def test_result_identity_records_actual_reader_and_cache_preparation_boundary(self):
        modules = SimpleNamespace(__version__="fixture")
        torch = SimpleNamespace(__version__="fixture", cuda=SimpleNamespace(get_device_name=lambda _: "fixture"))
        cpu = dict.fromkeys(("cpu_busy_cores_mean", "host_disk_read_bytes", "host_disk_write_bytes",
                             "host_net_recv_bytes", "host_net_sent_bytes"), 0)
        observed = RUNNER._ObservedImageRun(
            ExecutionResult(6, 2, {"exactly_once": True}), 1, 0, 0, cpu,
            {"gpu_energy_estimate_j": 0}, {}, 0.5,
        )
        for arm in ("project_ray", "ray_data_staged"):
            args = self.args()
            args.arm = arm
            args.source_shards = 2
            processor, capture, sidecar, provenance = RUNNER._validate_image_run_args(args)
            setup = RUNNER._load_image_run_setup(args, processor=processor, embedding_target=capture,
                                                embedding_sidecar=sidecar, provenance=provenance)
            with patch.object(RUNNER, "git_commit", return_value="fixture"):
                row = RUNNER._build_image_result_row(args, setup, observed, daft_module=modules,
                                                     ray_module=modules, torch_module=torch,
                                                     transformers_module=modules)
            reader = json.loads(row["source_reader_config_json"])
            self.assertEqual(reader["source_shards_requested"], 2)
            self.assertEqual(row["source_shards"], 2 if arm == "ray_data_staged" else "")
            if arm == "project_ray":
                self.assertEqual(reader["download_max_connections_requested"], 2)
                self.assertEqual(reader["download_max_connections_documented_scope"], "per_thread_daft_0.7.21")
                self.assertNotIn("max_connections_per_batch", reader)
            self.assertNotIn("sql_readers", row["resource_budget_semantics"])
            self.assertEqual(row["operator_e2e_s"], 7)
            contract = RUNNER._build_image_manifest(args, setup, row)["file_source_contract"]
            self.assertTrue(contract["manifest_preparation_may_warm_page_cache"])
            self.assertFalse(contract["adapter_payload_cache"])
            self.assertEqual(contract["os_page_cache_condition"], "uncontrolled")

    def test_file_run_outputs_cannot_overwrite_the_fixed_source_or_mix_source_kinds(self):
        args = self.args()
        args.out_manifest = str(self.path)
        with self.assertRaises(SystemExit): RUNNER._validate_image_run_args(args)
        args = self.args()
        args.out_csv = str(self.path)
        with self.assertRaises(SystemExit): RUNNER._validate_image_run_args(args)
        output = Path(self.temp.name) / "runs.csv"
        row = dict.fromkeys(RUNNER.CSV_FIELDS, "")
        row.update(source_kind="files", file_manifest_sha256=self.manifest.sha256)
        RUNNER.append_csv(output, row)
        before = output.read_bytes()
        row["source_kind"] = "postgres"
        with self.assertRaisesRegex(ValueError, "separate output"):
            RUNNER.append_csv(output, row)
        self.assertEqual(output.read_bytes(), before)

    def args(self):
        argv = ["runner", "--arm", "project_ray", "--model", "unused-local-model", "--pg-dsn", "",
                "--source-kind", "files", "--source-manifest", str(self.path), "--image-root", str(self.root),
                "--limit", "3", "--cpu-workers", "1", "--gpu-workers", "1", "--source-cpu-threads", "1",
                "--source-shards", "1", "--out-csv", str(Path(self.temp.name) / "runs.csv"),
                "--out-manifest", str(Path(self.temp.name) / "run.json")]
        with patch.object(sys, "argv", argv): return RUNNER.parse_args()

    def test_file_timing_includes_source_construction_and_final_metadata_audit(self):
        clock = SimpleNamespace(now=0.0)
        def load(*args): clock.now += 1; return self.manifest
        def source(*args, **kwargs): clock.now += 4; return "lazy-source"
        def kernel(*args, **kwargs):
            clock.now += 6
            return ExecutionResult(6, 2, {"exactly_once": True})
        obj = object.__new__(RUNNER._ImageExecution)
        obj._failed = obj._closed = False
        obj.args = self.args()
        obj.setup = SimpleNamespace(file_manifest=self.manifest, source_shards=1, processor="unused",
                                    database_metadata={"max_encoded_bytes": 1000}, hse_limits=None)
        obj.worker_pool = object()
        for mode in ("direct_dependency", "hse_static"):
            obj.args.project_execution_mode = mode
            clock.now = 0
            with patch.object(RUNNER.time, "perf_counter", side_effect=lambda: clock.now), \
                 patch.object(RUNNER, "load_image_file_manifest", side_effect=load), \
                 patch.object(RUNNER, "build_daft_image_file_source", side_effect=source), \
                 patch.object(RUNNER, "run_semloom_ray_pipeline", side_effect=kernel), \
                 patch.object(RUNNER, "run_semloom_ray_hse_pipeline", side_effect=kernel):
                result = obj.execute(3, frozenset({"139", "285", "632"}))
            self.assertEqual(result.total_s, 12)
            self.assertEqual(result.first_output_s, 7)
            self.assertEqual(obj.source_open_s, 5)
        obj.args.arm = "ray_data_staged"
        clock.now = 0
        obj.args.ray_data_autoscaling_actor_pools = False
        with patch.object(RUNNER.time, "perf_counter", side_effect=lambda: clock.now), \
             patch.object(RUNNER, "load_image_file_manifest", side_effect=load), \
             patch.object(RUNNER, "build_ray_data_clip_pipeline", side_effect=lambda **kwargs: source()), \
             patch.object(RUNNER, "run_ray_data_clip_baseline", side_effect=kernel):
            result = obj.execute(3, frozenset({"139", "285", "632"}))
        self.assertEqual(result.total_s, 12)
        self.assertEqual(result.first_output_s, 7)
        original = ExecutionResult(6, 2, {"exactly_once": True})
        obj.setup.file_manifest = None
        with patch.object(obj, "_execute_source", return_value=original):
            self.assertIs(obj.execute(3, frozenset()), original)

    def test_changed_manifest_is_rejected_before_the_execution_kernel(self):
        obj = object.__new__(RUNNER._ImageExecution)
        obj._failed = obj._closed = False
        obj.args = self.args()
        obj.setup = SimpleNamespace(file_manifest=self.manifest)
        self.path.write_text(self.path.read_text() + "\n")
        with patch.object(obj, "_execute_source", side_effect=AssertionError("kernel started")):
            with self.assertRaisesRegex(ValueError, "manifest changed"):
                obj.execute(3, frozenset({"139", "285", "632"}))

    @unittest.skipUnless(os.environ.get("SEMLOOM_IMAGE_FILE_SDK_TESTS") == "1",
                         "opt-in fixed-SDK CPU file-reader check; no server window used")
    def test_real_daft_file_reader_without_model_or_ray(self):
        import daft

        daft.set_runner_native(num_threads=1)
        config = ImageSourceConfig("coco_val2017", 2, 1, 2)
        frame = build_daft_image_file_source(self.manifest, config, batch_size=2, max_connections=1)
        rows = []
        # Bounded four-row fixture only, never a formal-workload collect.
        for batch in frame.into_batches(2).to_arrow_iter(results_buffer_size=1):
            rows.extend((row["doc_id"], bytes(row["image"])) for row in batch.to_pylist())
        self.assertEqual([doc_id for doc_id, _ in rows],
                         ["285#pass=1", "632#pass=1", "285#pass=2", "632#pass=2"])
        self.assertEqual([data for _, data in rows],
                         [(self.root / entry.path).read_bytes() for _ in range(2) for entry in self.manifest.entries[1:]])

    @unittest.skipUnless(os.environ.get("SEMLOOM_IMAGE_FILE_SDK_TESTS") == "1",
                         "opt-in fixed-SDK CPU file-reader check; no server window used")
    def test_real_ray_binary_reader_without_model_or_gpu(self):
        import ray

        if ray.is_initialized():
            self.skipTest("requires an isolated Ray process; preserve an existing connection")
        ray.init(num_cpus=2, num_gpus=0, include_dashboard=False)
        try:
            config = ImageSourceConfig("coco_val2017", 2, 1, 2)
            dataset = _read_ray_image_files(self.manifest, config, 1)
            rows = []
            for batch in dataset.iter_batches(batch_format="numpy", prefetch_batches=1):
                rows.extend(zip(batch["doc_id"], batch["image"], strict=True))
            self.assertEqual([str(doc_id) for doc_id, _ in rows],
                             ["285#pass=1", "632#pass=1", "285#pass=2", "632#pass=2"])
            self.assertEqual([bytes(data) for _, data in rows],
                             [(self.root / entry.path).read_bytes() for _ in range(2) for entry in self.manifest.entries[1:]])
        finally:
            ray.shutdown()


if __name__ == "__main__":
    unittest.main()
