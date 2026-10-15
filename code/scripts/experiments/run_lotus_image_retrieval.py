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


def _retain_failure(report, error, failure, scope, *, cleanup=False):
    from src.experiments.execution_timing import is_timing_error
    if error is None or (cleanup and is_timing_error(error) and not is_timing_error(failure)):
        if error is not None:
            failure.add_note("Prior image timing failure: " + type(error).__name__)
        report.update(status="failed", error_type=type(failure).__name__, reason=redact_text(str(failure)))
        return failure
    error.add_note(scope + " also raised " + type(failure).__name__)
    return error


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
    p.add_argument("--ray-image-pipeline", choices=("fused", "staged"),
                   help="ray_data only: native RGB callable (default fused) or CPU/GPU stages")
    p.add_argument("--hse-encoded-bytes", type=int)
    p.add_argument("--hse-encoded-block-bytes-upper-bound", type=int)
    p.add_argument("--ray-temp-dir", type=Path)
    p.add_argument("--device", choices=("cpu", "cuda"), default="cuda")
    p.add_argument("--dtype", choices=("float32",), default="float32")
    p.add_argument("--query-doc-id", action="append", default=[])
    p.add_argument("--text-query", action="append", default=[])
    p.add_argument("--query-repeats", type=int, default=1,
                   help="repeat the same ordered queries in one native owner; all first calls are retained")
    p.add_argument("--observe-model-reuse", action="store_true",
                   help="opt-in actual model/worker identities and integer invocation counters")
    p.add_argument("--retain-query-vectors", action="store_true",
                   help="private numeric qualification only, maximum 256 gallery rows")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    if args.ray_image_pipeline is not None and args.arm != "ray_data":
        raise ValueError("ray-image-pipeline requires ray_data")
    hse_bytes = (args.hse_encoded_bytes, args.hse_encoded_block_bytes_upper_bound)
    if any(x is not None for x in hse_bytes):
        if args.arm != "semloom_hse" or any(x is not None and x <= 0 for x in hse_bytes):
            raise ValueError("positive HSE encoded byte settings require semloom_hse")
    if (not 0 < args.rows <= args.max_gallery_rows <= 5000
            or args.k < 1 or min(args.batch_size, args.stock_outer_batch_size, args.query_repeats) < 1):
        raise ValueError("invalid bounded qualification dimensions")
    if args.mode == "index" and args.query_repeats != 1:
        raise ValueError("query repeats require search or combined mode")
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
                  input_policy="backend_native",
                  hse_encoded_bytes=args.hse_encoded_bytes,
                  hse_encoded_block_bytes_upper_bound=args.hse_encoded_block_bytes_upper_bound,
                  detailed_timing=bool(os.environ.get("SEMLOOM_DETAILED_TIMING_DIR")))
    report["model_load_scope"] = (
        "identity, stock text RM and eager CLIP/worker setup are in model_startup_s"
        if args.arm in ("stock", "direct", "semloom_static", "semloom_hse") else
        "identity, stock CPU text RM and Ray owner setup are in model_startup_s; this adapter's per-RM native callable CLIP loads remain in index/query/application time"
        if args.arm == "ray_data" else
        "identity, stock CPU text RM and Daft wrapper setup are in model_startup_s; first native UDF processor/CLIP load remains in index/query/application time"
        if args.arm == "daft_native" else
        "eager identity/text RM/factory setup is in model_startup_s; external lazy loads remain in index/query/application time")
    if args.arm == "ray_data":
        report["ray_image_pipeline"] = "native_image_arrays_" + (args.ray_image_pipeline or "fused") + "_slow_preprocess_clip"
    executor = None
    source_images = {} if args.arm in ("semloom_static", "semloom_hse") else None
    rm = None
    owned_ray = None
    startup_failure = None
    error = None
    reuse_enabled = args.observe_model_reuse
    previous_reuse_directory = os.environ.get("SEMLOOM_IMAGE_REUSE_DIR")
    if reuse_enabled:
        os.environ["SEMLOOM_IMAGE_REUSE_DIR"] = str(args.output_dir / "reuse")
        report["model_reuse_observations"] = []

    def observe_phase(phase, *, allow_workers=True):
        from src.experiments.image_reuse import snapshot_models
        at = time.monotonic_ns()
        snapshot = dict(phase=phase, observed_monotonic_ns=at, local_models=snapshot_models(), worker_models=[])
        if args.arm in ("semloom_static", "semloom_hse") and executor is not None and executor.pool is not None:
            if allow_workers and not executor._unconfirmed_work:
                snapshot["worker_models"] = owned_ray.get(
                    [actor.ready.remote() for actor in executor.pool.gpu_actors])
            else:
                snapshot["worker_models_unavailable"] = "failed_or_cancelled_workflow; do not wait before cleanup"
        snapshot["observation_wall_s"] = (time.monotonic_ns() - at) / 1e9
        report["model_reuse_observations"].append(snapshot)

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
        stock_probe = None
        if reuse_enabled:
            from src.experiments.image_reuse import observe_stock_model
            stock_probe = observe_stock_model(stock.transformer[0].model, device=stock_device)
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
                if args.arm == "ray_data":
                    settings["pipeline"] = args.ray_image_pipeline or "fused"
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
                                                       max_active_batches=args.max_active_batches,
                            encoded_bytes_limit=args.hse_encoded_bytes,
                            encoded_block_bytes_upper_bound=args.hse_encoded_block_bytes_upper_bound)
            elif args.arm == "daft_native":
                if args.model is None:
                    raise ValueError("Daft arm requires its qualified CLIP checkpoint")
                from src.baselines.image.frameworks.daft_clip import DaftClipExecutor
                executor = DaftClipExecutor(model_path=str(args.model),
                    processor_path=str(args.processor) if args.processor else None,
                    dtype=args.dtype, batch_size=args.batch_size, device=args.device,
                    gpus_per_worker=1 if args.device == "cuda" else 0,
                    max_batch_rows=args.max_gallery_rows, input_kind="rgb_pil")
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
            rm_options = {} if source_images is None else {"source_images": source_images}
            report["executor_input_kind"] = getattr(executor, "input_kind", "encoded_bytes")
            rm = make_image_rm(executor, stock, max_rows=args.max_gallery_rows,
                               observation=RMObservation(capture_vectors=args.retain_query_vectors),
                               **rm_options)
        if reuse_enabled:
            observe_phase("startup_complete")
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
            image = decode_rgb_image(raw)
            images.append(image)
            if source_images is not None:
                source_images[id(image)] = (image, raw, image.size, hashlib.sha256(image.tobytes()).hexdigest())
        report["source_decode_s"] = (time.monotonic_ns() - application_started) / 1e9
        report["manifest_sha256"] = manifest.sha256
        ids = tuple(x.doc_id for x in entries)
        queries = []
        for doc_id in (args.query_doc_id or [ids[0]]):
            if doc_id not in ids:
                raise ValueError("query image ID is not in this gallery")
            queries.append(RetrievalQuery("image-" + doc_id, images[ids.index(doc_id)], doc_id))
        queries.extend(RetrievalQuery("text-" + str(i), text) for i, text in enumerate(args.text_query))
        workflow_options = {}
        if args.query_repeats != 1:
            workflow_options["query_repeats"] = args.query_repeats
            report["query_policy"] = dict(repeats=args.query_repeats,
                payload_count=len(queries), retained_first_round=True, discarded_warmup_queries=0,
                input_ready="RGB PIL images and text prepared before submission",
                arrival="synchronous closed loop; one outstanding query", native_cache=False)
        if reuse_enabled:
            workflow_options["phase_observer"] = observe_phase
        run_workflow(image_frame(ids, images), rm, queries, index_dir=args.index_dir,
                     embedding_identity=identity, k=args.k, mode=args.mode,
                     max_gallery_rows=args.max_gallery_rows, report=report, **workflow_options)
        report["application_e2e_s"] = (time.monotonic_ns() - application_started) / 1e9
        report["application_scope"] = ("source content check/decode through native index/search and output validation; "
            "eager owner/identity setup and evidence serialization separate; per-call or lazy model loads remain included where performed")
    except BaseException as exc:
        error = exc
        startup_failure = getattr(exc, "image_worker_pool_startup", None)
        report.update(status="failed", error_type=type(exc).__name__, reason=redact_text(str(exc)))
    finally:
        cleanup_at = time.monotonic_ns()
        if rm is not None:
            report["rm_calls"] = rm.observation.calls
        if reuse_enabled:
            try:
                observe_phase("before_close", allow_workers=error is None)
                report["native_batch_model_reuse"] = getattr(executor, "reuse_observations", [])
            except BaseException as exc:
                report["reuse_observation_error"] = type(exc).__name__
                error = _retain_failure(report, error, exc, "model reuse observations", cleanup=True)
        if startup_failure is not None:
            try:
                startup_failure.request_termination()
            except BaseException as exc:
                report["startup_cleanup_error"] = type(exc).__name__
                error = _retain_failure(report, error, exc, "partial startup cleanup", cleanup=True)
            report["startup_actor_count"] = len(startup_failure.actor_handles)
            report["startup_termination_requests"] = startup_failure.termination_request_outcomes
            report["startup_physical_exit"] = startup_failure.physical_exit_status
        if executor is not None:
            try:
                report["execution_observations"] = getattr(executor, "execution_observations", [])
            except BaseException as exc:
                report["execution_observation_error"] = type(exc).__name__
                if not isinstance(exc, Exception):
                    error = _retain_failure(report, error, exc, "execution observations", cleanup=True)
            try:
                close_at = time.monotonic_ns()
                executor.close()
            except BaseException as exc:
                report["cleanup_error"] = type(exc).__name__
                error = _retain_failure(report, error, exc, "executor cleanup", cleanup=True)
            finally:
                report["executor_close_s"] = (time.monotonic_ns() - close_at) / 1e9
        if owned_ray is not None:
            try:
                shutdown_at = time.monotonic_ns()
                owned_ray.shutdown()
                report["ray_shutdown_returned"] = True
            except BaseException as exc:
                report["ray_cleanup_error"] = type(exc).__name__
                error = _retain_failure(report, error, exc, "Ray shutdown", cleanup=True)
            finally:
                report["owned_ray_shutdown_s"] = (time.monotonic_ns() - shutdown_at) / 1e9
        report["cleanup_s"] = (time.monotonic_ns() - cleanup_at) / 1e9
        report["process_wall_s"] = (time.monotonic_ns() - started) / 1e9
        sink = time.monotonic_ns()
        if rm is not None and rm.observation.vectors:
            import numpy as np
            try:
                np.savez_compressed(args.output_dir / "rm-vectors.npz", **rm.observation.vectors)
            except BaseException as exc:
                report.setdefault("evidence_write_errors", []).append("vectors: " + type(exc).__name__)
                error = _retain_failure(report, error, exc, "vector evidence write")
        report_path = args.output_dir / "result.json"
        try:
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        except BaseException as exc:
            report.setdefault("evidence_write_errors", []).append("result: " + type(exc).__name__)
            error = _retain_failure(report, error, exc, "result evidence write")
        try:
            (args.output_dir / "evidence-write.json").write_text(json.dumps({
                "elapsed_s": (time.monotonic_ns() - sink) / 1e9,
                "errors": report.get("evidence_write_errors", []),
            }) + "\n")
        except BaseException as exc:
            report.setdefault("evidence_write_errors", []).append("receipt: " + type(exc).__name__)
            error = _retain_failure(report, error, exc, "receipt evidence write")
            try:
                report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            except BaseException as update_error:
                error.add_note("updated result evidence write also raised " + type(update_error).__name__)
        if reuse_enabled:
            if previous_reuse_directory is None:
                os.environ.pop("SEMLOOM_IMAGE_REUSE_DIR", None)
            else:
                os.environ["SEMLOOM_IMAGE_REUSE_DIR"] = previous_reuse_directory
    if error is not None:
        print("native LOTUS qualification failed: " + type(error).__name__, file=sys.stderr)
        return 1
    print(json.dumps({"status": "succeeded", "arm": args.arm, "rows": args.rows,
                      "index_s": report["index_s"], "query_count": len(report["queries"])}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
