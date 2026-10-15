"""Ray Data batches using an independently owned native Ray Serve CLIP replica.

The project supplies a workload UDF (diagnostic_reference). Ray Data schedules
the finite graphs; Ray Serve owns routing, queues and replica lifetime. The
original graph-owned CLIP configuration remains a separate baseline.
"""
from __future__ import annotations

import hashlib
from importlib.metadata import version
import math
import os
from pathlib import Path
import threading
import time
import uuid

import numpy as np

from src.serving.ray_data_clip_client import clip_service_batch, rgb_batch


RAY_VERSION = "2.56.1"


def _artifacts(model, processor):
    weights = model / "model.safetensors"
    if not weights.is_file():
        weights = model / "pytorch_model.bin"
    paths = {"weights": weights, "model_config": model / "config.json",
             "processor_config": processor / "preprocessor_config.json"}
    result = {}
    for name, path in paths.items():
        with path.open("rb") as stream:
            result[name] = hashlib.file_digest(stream, "sha256").hexdigest()
    return result


def _software():
    return {name: version(name) for name in
            ("ray", "torch", "transformers", "numpy", "pyarrow", "Pillow")}


class _ClipReplica:
    """One native deployment instance; no input or result history is retained."""

    def __init__(self, model, processor, device, max_batch_rows):
        from src.modalities.image.clip import ClipImagePreprocessor, ClipTensorActor

        self.processor = ClipImagePreprocessor(processor, local_files_only=True)
        self.actor = ClipTensorActor(model, processor_revision=processor, device=device,
            dtype="float32", normalize=True, local_files_only=True,
            detailed_stage_timing=False, torch_intraop_threads=1, torch_interop_threads=1)
        if self.actor.semantics.dimension != 512:
            raise ValueError("CLIP service requires a 512-dimensional projection")
        self.uid = uuid.uuid4().hex
        self.artifacts = _artifacts(Path(model), Path(processor))
        self.software = _software()
        self._failed = False
        self.max_batch_rows = max_batch_rows
        self.forward_attempts = self.forward_returns = self.rows = 0

    def ready(self):
        return dict(service_uid=self.uid, pid=os.getpid(), model=self.actor.ready(),
            artifacts_sha256=self.artifacts, software=self.software,
            forward_attempts=self.forward_attempts, forward_returns=self.forward_returns,
            returned_rows=self.rows, failed=self._failed)

    def embed(self, request):
        from src.modalities.image.contracts import ImageEmbeddingBatch

        if self._failed or request["service_uid"] != self.uid:
            raise RuntimeError("CLIP service failed or its instance changed")
        ids, images = rgb_batch(request["doc_ids"], request["images"])
        if len(ids) > self.max_batch_rows:
            raise ValueError("CLIP service request exceeds its native model batch size")
        try:
            pixels = self.processor.preprocess_decoded(images)
            batch = ImageEmbeddingBatch(ids, pixels, "preprocessed_tensor",
                                        len(ids) * 224 * 224, "pixels")
            self.forward_attempts += 1
            result = self.actor.embed(batch)
            self.forward_returns += 1
            self.rows += len(ids)
            reply = dict(doc_id=np.asarray(result.doc_ids), embedding=result.embeddings,
                service_uid=self.uid, request_id=request["request_id"])
            probe = self.actor.ready().get("model_reuse")
            if probe is not None:
                reply["model_reuse"] = probe
            return reply
        except BaseException as error:
            self._failed = True
            try:
                self.actor.synchronize()
            except BaseException as cleanup:
                error.add_note("CLIP synchronize also failed: " + type(cleanup).__name__)
            raise

    def synchronize(self):
        self.actor.synchronize()
        return self.ready()


class RayServeClipExecutor:
    """Bounded RGB RM interface owning a fresh runtime and one Serve app.

    A complete result confirms successful service calls. Any native failure or
    timeout forbids reuse. Final application/runtime exit still needs physical
    process checks by the existing experiment supervisor.
    """

    input_kind = "rgb_pil"

    def __init__(self, *, model_path, processor_path=None, device="cuda",
                 dtype="float32", batch_size=32, max_batch_rows=5000,
                 ray_temp_dir, request_timeout_s=60):
        if dtype != "float32" or device not in ("cpu", "cuda"):
            raise ValueError("this CLIP configuration requires float32 on cpu or cuda")
        if (any(type(x) is not int or x <= 0 for x in (batch_size, max_batch_rows))
                or batch_size > max_batch_rows or max_batch_rows > 5000):
            raise ValueError("invalid bounded CLIP batch size")
        if (isinstance(request_timeout_s, bool) or not isinstance(request_timeout_s, (int, float))
                or not math.isfinite(request_timeout_s) or request_timeout_s <= 0):
            raise ValueError("service timeout must be finite and positive")
        model = Path(model_path).resolve(strict=True)
        processor = Path(processor_path or model_path).resolve(strict=True)
        from src.baselines.common.private_artifacts import require_outside_git
        require_outside_git(Path(ray_temp_dir))
        artifacts, software = _artifacts(model, processor), _software()
        if software["ray"] != RAY_VERSION:
            raise RuntimeError("Ray CLIP service requires Ray " + RAY_VERSION)
        from src.modalities.image.contracts import EmbeddingSemantics
        self.semantics = EmbeddingSemantics(str(model), str(processor), 512)
        self.identity = dict(scheduler_owner="ray_data+ray_serve", implementation="diagnostic_reference",
            model_revision=str(model), processor_revision=str(processor), artifacts_sha256=artifacts,
            compute_dtype=dtype, output_dtype="float32", dimension=512, projected=True, normalized=True,
            device=device, processor="slow CLIPProcessor", input_kind=self.input_kind, software=software,
            batch_size=batch_size, max_batch_rows=max_batch_rows, data_task_cpus=1, data_task_gpus=0,
            cluster_cpus=4, cluster_gpus=int(device == "cuda"), model_replicas=1,
            replica_cpus=1, replica_gpus=int(device == "cuda"), torch_threads=1,
            model_lifetime="owned_serve_application_across_data_graphs", data_task_max_retries=0,
            max_ongoing_requests=1, max_queued_requests_per_caller=1,
            native_serve_rerouting="possible; Data max_retries does not disable it",
            data_retried_map_errors=False, max_constructor_retry_count=1,
            request_timeout_s=request_timeout_s)
        self.batch_size, self.max_batch_rows = batch_size, max_batch_rows
        self.request_timeout_s = request_timeout_s
        self._lock = threading.Lock()
        self._closed = self._unconfirmed_work = self._owns_runtime = False
        self._handle = None
        self._close_error = None
        self._app_name = "lotus-clip-" + uuid.uuid4().hex
        self.reuse_observations = []
        import ray
        from ray import serve
        from ray.data import DataContext
        self._ray, self._serve = ray, serve
        if ray.is_initialized():
            raise ValueError("CLIP service requires its own previously uninitialized Ray runtime")
        if DataContext.get_current().retried_map_errors is not False:
            raise ValueError("CLIP service requires Data retried_map_errors=False")
        # address='local' prevents RAY_ADDRESS from silently borrowing a cluster.
        try:
            self._owns_runtime = True
            ray.init(address="local", num_cpus=4, num_gpus=int(device == "cuda"),
                include_dashboard=False, _temp_dir=str(Path(ray_temp_dir).resolve()), log_to_driver=False)
            serve.start(proxy_location="Disabled")
            application = serve.deployment(num_replicas=1, ray_actor_options={"num_cpus": 1,
                "num_gpus": int(device == "cuda")}, max_ongoing_requests=1,
                max_queued_requests=1, max_constructor_retry_count=1)(_ClipReplica).bind(
                    str(model), str(processor), device, batch_size)
            self._handle = serve.run(application, name=self._app_name, route_prefix=None)
            ready = self._handle.ready.remote().result(timeout_s=request_timeout_s)
            if ready["artifacts_sha256"] != artifacts or ready["software"] != software:
                raise ValueError("CLIP service artifact or software identity differs from its owner")
            self._service_uid = ready["service_uid"]
            self.identity["service_ready"] = ready
        except BaseException as error:
            # A readiness failure may leave the deployment busy or unavailable.
            # Do not add a second waiting RPC before deleting our owned app.
            self._unconfirmed_work = True
            try:
                self.close()
            except BaseException as cleanup:
                error.add_note("CLIP startup cleanup also failed: " + type(cleanup).__name__)
            raise

    def embed_batch(self, doc_ids, images):
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("CLIP executor already has an active batch")
        try:
            if self._closed or self._unconfirmed_work:
                raise RuntimeError("CLIP executor closed or prior work completion unknown")
            from PIL import Image
            if (not isinstance(doc_ids, tuple) or not doc_ids or len(doc_ids) > self.max_batch_rows
                    or any(type(x) is not str or not x for x in doc_ids)
                    or len(set(doc_ids)) != len(doc_ids) or len(images) != len(doc_ids)):
                raise ValueError("CLIP input must have bounded, unique aligned string IDs")
            # Validate the entire caller batch before native work can accept a prefix.
            for image in images:
                if not isinstance(image, Image.Image) or image.mode != "RGB" or min(image.size) <= 0:
                    raise TypeError("CLIP input requires nonempty RGB PIL images")
                image.load()
            return self._embed_batch(doc_ids, images)
        finally:
            self._lock.release()

    def _embed_batch(self, doc_ids, images):
        import ray.data
        from src.semantic_methods.lotus.image_backend import OrderedMatrix, RayDataImageExecutor
        from src.modalities.image.contracts import ImageEmbeddingResult

        started = time.perf_counter()
        collector = OrderedMatrix(doc_ids, self.semantics)
        data = ray.data.from_items([dict(doc_id=x, image=np.asarray(y))
                                   for x, y in zip(doc_ids, images, strict=True)])
        data = data.map_batches(clip_service_batch, batch_format="numpy", batch_size=self.batch_size,
            compute=ray.data.TaskPoolStrategy(size=1), num_cpus=1, num_gpus=0, max_retries=0,
            fn_kwargs=dict(handle=self._handle, service_uid=self._service_uid,
                call_id=uuid.uuid4().hex, timeout_s=self.request_timeout_s))
        iterator = None
        error = None
        self._unconfirmed_work = True
        try:
            iterator = iter(data.iter_batches(batch_format="numpy", prefetch_batches=1))
            for batch in iterator:
                collector.add(tuple(batch["doc_id"]), batch["embedding"])
                RayDataImageExecutor._observe_reuse_batch(self, batch)
            matrix = collector.finish()
        except BaseException as exc:
            error = exc
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                try:
                    close()
                except BaseException as exc:
                    if error is None:
                        error = exc
                    else:
                        error.add_note("Data iterator close also failed: " + type(exc).__name__)
        if error is not None:
            raise error
        self._unconfirmed_work = False
        return ImageEmbeddingResult(doc_ids, matrix, self.semantics, time.perf_counter() - started)

    def close(self):
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("cannot close CLIP executor while a batch is active")
        try:
            if self._closed:
                if self._close_error is not None:
                    raise RuntimeError("Prior CLIP close failed; confirm owned physical process exit") from self._close_error
                return
            self._closed = True
            error = None
            try:
                if self._handle is not None and not self._unconfirmed_work:
                    ready = self._handle.synchronize.remote().result(timeout_s=self.request_timeout_s)
                    self.identity["service_before_close"] = ready
                    if ready.get("service_uid") != self._service_uid or ready.get("failed") is True:
                        raise RuntimeError("CLIP service instance changed or failed before close")
            except BaseException as exc:
                error = exc
            try:
                if self._owns_runtime and self._ray.is_initialized():
                    self._serve.delete(self._app_name)
            except BaseException as exc:
                if error is None:
                    error = exc
                else:
                    error.add_note("owned Serve application delete also failed: " + type(exc).__name__)
            finally:
                self._handle = None
                if self._owns_runtime:
                    try:
                        self._ray.shutdown()
                    except BaseException as exc:
                        if error is None:
                            error = exc
                        else:
                            error.add_note("owned Ray shutdown also failed: " + type(exc).__name__)
            if self._unconfirmed_work:
                unknown = RuntimeError("CLIP work completion unknown; confirm owned physical process exit")
                if error is not None:
                    unknown.add_note("owned cleanup also raised " + type(error).__name__)
                self._close_error = unknown
                raise unknown from error
            if error is not None:
                self._close_error = error
                raise error
        finally:
            self._lock.release()
