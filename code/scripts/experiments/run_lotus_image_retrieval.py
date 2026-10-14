#!/usr/bin/env python3
"""Bounded native LOTUS image retrieval qualification; no VLM map/filter."""
from __future__ import annotations

import argparse
import hashlib
import importlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import sys
import time

CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.private_artifacts import require_outside_git
from src.baselines.common.redact import redact_text
from src.modalities.image.clip import decode_rgb_image, configure_torch_thread_pools
from src.modalities.image.file_source import load_image_file_manifest
from src.semantic_methods.lotus.image_backend import DirectImageExecutor, SemLoomImageExecutor, RayDataImageExecutor
from src.semantic_methods.lotus.image_rm import make_image_rm, observe_stock_rm, RMObservation
from src.semantic_methods.lotus.image_source import validate_image_source
from src.semantic_methods.lotus.image_identity import validate_embedding_identity, validate_executor_identity
from src.semantic_methods.lotus.image_workflow import RetrievalQuery, image_frame, run_workflow


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--image-root", type=Path, required=True)
    p.add_argument("--stock-model", type=Path, required=True,
                   help="local pinned SentenceTransformers CLIP snapshot")
    p.add_argument("--model", type=Path, help="local candidate CLIP checkpoint")
    p.add_argument("--processor", type=Path)
    p.add_argument("--embedding-identity", type=Path, required=True,
                   help="qualification identity including actual model/processor hashes")
    p.add_argument("--arm", choices=("stock", "direct", "semloom_static", "semloom_hse", "ray_data", "daft_native", "external"), default="stock")
    p.add_argument("--executor-factory", help="module:factory implementing ImageBatchExecutor")
    p.add_argument("--executor-config", type=Path)
    p.add_argument("--mode", choices=("combined", "index", "search"), default="combined")
    p.add_argument("--index-dir", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--rows", type=int, default=64)
    p.add_argument("--max-gallery-rows", type=int, default=5000)
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--batch-size", type=int, default=32)
    p.add_argument("--stock-outer-batch-size", type=int, default=64)
    p.add_argument("--max-active-batches", type=int, default=2)
    p.add_argument("--ray-temp-dir", type=Path)
    p.add_argument("--device", choices=("cpu", "cuda"), default="cuda")
    p.add_argument("--dtype", choices=("float32",), default="float32")
    p.add_argument("--query-doc-id", action="append", default=[])
    p.add_argument("--text-query", action="append", default=[])
    p.add_argument("--retain-query-vectors", action="store_true",
                   help="private numeric qualification only, maximum 256 gallery rows")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    if (not 0 < args.rows <= args.max_gallery_rows <= 5000
            or args.k < 1 or min(args.batch_size, args.stock_outer_batch_size) < 1):
        raise ValueError("invalid bounded qualification dimensions")
    if args.retain_query_vectors and args.rows > 256:
        raise ValueError("private vector capture is limited to 256 qualification rows")
    require_outside_git(args.output_dir)
    require_outside_git(args.index_dir)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    report = dict(arm=args.arm, status="preparing", task="native_clip_image_retrieval",
                  input_kind="RGB_PIL", model_dtype=args.dtype,
                  stock_outer_batch_size=args.stock_outer_batch_size,
                  stock_encode_batch_size=32, direct_model_batch_size=args.batch_size,
                  private_vector_capture=args.retain_query_vectors,
                  detailed_timing=bool(os.environ.get("SEMLOOM_DETAILED_TIMING_DIR")))
    executor = None
    rm = None
    owned_ray = None
    error = None
    started = time.monotonic_ns()
    try:
        report["lotus_source"] = validate_image_source()
        report["versions"] = {x: version(x) for x in ("lotus-ai", "sentence-transformers", "transformers", "torch", "pandas", "numpy", "pillow", "faiss-cpu")}
        identity = json.loads(args.embedding_identity.read_text())
        if identity.get("model_dtype") != args.dtype or identity.get("qualified") is not True:
            raise ValueError("a qualified embedding identity matching model dtype is required")
        validate_embedding_identity(identity, stock_model=args.stock_model,
                                    candidate_model=args.model, processor=args.processor)
        report["embedding_identity"] = identity
        report["torch_thread_pools"] = configure_torch_thread_pools(1, 1)
        from lotus.models import SentenceTransformersRM
        stock_device = args.device if args.arm == "stock" else "cpu"
        stock = SentenceTransformersRM(str(args.stock_model),
            max_batch_size=args.stock_outer_batch_size, normalize_embeddings=True, device=stock_device)
        stock.transformer.to(dtype=__import__("torch").__dict__[args.dtype])
        report["stock_rm_device"] = stock_device
        report["image_model_lifecycle"] = ("cold_native_graph_per_rm_call" if args.arm == "ray_data"
            else "resident_for_workflow" if args.arm != "external" else "declared_by_external_executor")
        if args.arm == "stock":
            rm = observe_stock_rm(stock, observation=RMObservation(capture_vectors=args.retain_query_vectors))
        else:
            if args.arm in ("direct", "semloom_static", "semloom_hse", "ray_data"):
                if args.model is None:
                    raise ValueError("direct arm requires its qualified CLIP checkpoint")
                settings = dict(model=str(args.model), processor=str(args.processor) if args.processor else None,
                                dtype=args.dtype, batch_size=args.batch_size)
                if args.arm == "direct":
                    executor = DirectImageExecutor(**settings, device=args.device)
                else:
                    if args.device != "cuda" or args.ray_temp_dir is None:
                        raise ValueError("Ray image qualification requires CUDA and its own temp directory")
                    require_outside_git(args.ray_temp_dir)
                    import ray
                    if ray.is_initialized():
                        raise ValueError("CLI requires an exclusively owned Ray runtime")
                    ray.init(num_cpus=4, num_gpus=1, include_dashboard=False,
                             _temp_dir=str(args.ray_temp_dir), log_to_driver=False)
                    owned_ray = ray
                    if args.arm == "ray_data":
                        executor = RayDataImageExecutor(**settings)
                    else:
                        executor = SemLoomImageExecutor(**settings, hse=args.arm == "semloom_hse",
                                                       max_active_batches=args.max_active_batches)
            elif args.arm == "daft_native":
                if args.model is None:
                    raise ValueError("Daft arm requires its qualified CLIP checkpoint")
                from src.baselines.image.frameworks.daft_clip import DaftClipExecutor
                executor = DaftClipExecutor(model_path=str(args.model),
                    processor_path=str(args.processor) if args.processor else None,
                    dtype=args.dtype, batch_size=args.batch_size, device=args.device,
                    gpus_per_worker=1 if args.device == "cuda" else 0,
                    max_batch_rows=args.max_gallery_rows)
                report["executor_identity"] = executor.identity
                report["image_model_lifecycle"] = "native_stateful_wrapper_across_graphs"
            else:
                if not args.executor_factory or not args.executor_config or args.model is None:
                    raise ValueError("external arm requires a factory, config and qualified model")
                module, name = args.executor_factory.split(":")
                executor = getattr(importlib.import_module(module), name)(**json.loads(args.executor_config.read_text()))
                report["executor_config_sha256"] = hashlib.sha256(args.executor_config.read_bytes()).hexdigest()
            if args.arm in ("external", "daft_native"):
                report["executor_identity"] = validate_executor_identity(executor, identity,
                    model=args.model, processor=args.processor, dtype=args.dtype, device=args.device)
            if args.arm in ("semloom_static", "semloom_hse", "ray_data", "daft_native"):
                report["versions"].update({x: version(x) for x in ("ray", "daft", "pyarrow")})
            rm = make_image_rm(executor, stock, max_rows=args.max_gallery_rows,
                               observation=RMObservation(capture_vectors=args.retain_query_vectors))
        report["model_startup_s"] = (time.monotonic_ns() - started) / 1e9
        application_started = time.monotonic_ns()
        report["stage"] = "gallery_source_decode"
        manifest = load_image_file_manifest(args.manifest, args.image_root)
        if args.rows > len(manifest.entries):
            raise ValueError("selected rows exceed the source manifest")
        entries = manifest.entries[:args.rows]
        images = []
        for entry in entries:
            raw = (manifest.root / entry.path).read_bytes()
            if hashlib.sha256(raw).hexdigest() != entry.sha256:
                raise ValueError("gallery content differs from the pinned source manifest")
            images.append(decode_rgb_image(raw))
        report["source_decode_s"] = (time.monotonic_ns() - application_started) / 1e9
        report["manifest_sha256"] = manifest.sha256
        ids = tuple(x.doc_id for x in entries)
        queries = []
        for doc_id in (args.query_doc_id or [ids[0]]):
            if doc_id not in ids:
                raise ValueError("query image ID is not in this gallery")
            queries.append(RetrievalQuery("image-" + doc_id, images[ids.index(doc_id)], doc_id))
        queries.extend(RetrievalQuery("text-" + str(i), text) for i, text in enumerate(args.text_query))
        run_workflow(image_frame(ids, images), rm, queries, index_dir=args.index_dir,
                     embedding_identity=identity, k=args.k, mode=args.mode,
                     max_gallery_rows=args.max_gallery_rows, report=report)
        report["application_e2e_s"] = (time.monotonic_ns() - application_started) / 1e9
        report["application_scope"] = "source content check/decode through native index/search and output validation; model startup and evidence serialization separate"
    except BaseException as exc:
        error = exc
        report.update(status="failed", error_type=type(exc).__name__, reason=redact_text(str(exc)))
    finally:
        if rm is not None:
            report["rm_calls"] = rm.observation.calls
        if executor is not None:
            try:
                executor.close()
            except BaseException as exc:
                report["cleanup_error"] = type(exc).__name__
                if error is None:
                    error = exc
                    report.update(status="failed", error_type=type(exc).__name__, reason="executor cleanup failed")
        if owned_ray is not None:
            try:
                owned_ray.shutdown()
                report["ray_shutdown_returned"] = True
            except BaseException as exc:
                report["ray_cleanup_error"] = type(exc).__name__
                if error is None:
                    error = exc
                    report.update(status="failed", error_type=type(exc).__name__, reason="Ray shutdown failed")
        report["process_wall_s"] = (time.monotonic_ns() - started) / 1e9
        sink = time.monotonic_ns()
        if rm is not None and rm.observation.vectors:
            import numpy as np
            np.savez_compressed(args.output_dir / "rm-vectors.npz", **rm.observation.vectors)
        report_path = args.output_dir / "result.json"
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        (args.output_dir / "evidence-write.json").write_text(json.dumps({"elapsed_s": (time.monotonic_ns() - sink) / 1e9}) + "\n")
    if error is not None:
        print("native LOTUS qualification failed: " + type(error).__name__, file=sys.stderr)
        return 1
    print(json.dumps({"status": "succeeded", "arm": args.arm, "rows": args.rows,
                      "index_s": report["index_s"], "query_count": len(report["queries"])}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
