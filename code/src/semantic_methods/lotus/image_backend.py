"""Direct reference reusing the project's slow CLIP preprocessing and actor."""
from __future__ import annotations

import time
import threading

from src.modalities.image.clip import ClipImagePreprocessor, ClipTensorActor
from src.modalities.image.contracts import ImageEmbeddingBatch, ImageEmbeddingResult, ImageBatchTelemetry


class DirectImageExecutor:
    def __init__(self, *, model, processor=None, device="cuda", dtype="float32", batch_size=32):
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("image model batch size must be positive")
        self.batch_size = batch_size
        self.processor = ClipImagePreprocessor(processor or model, local_files_only=True)
        self.actor = ClipTensorActor(model, processor_revision=processor or model,
                                    device=device, dtype=dtype, local_files_only=True,
                                    normalize=True, detailed_stage_timing=False)
        self.semantics = self.actor.semantics
        self.execution_observations = []

    def embed_batch(self, doc_ids, images):
        if not doc_ids or len(set(doc_ids)) != len(doc_ids) or len(images) != len(doc_ids):
            raise ValueError("image IDs and inputs must be nonempty, unique and aligned")
        import numpy as np

        started = time.perf_counter()
        chunks = []
        costs = []
        for offset in range(0, len(doc_ids), self.batch_size):
            ids = doc_ids[offset:offset + self.batch_size]
            at = time.perf_counter()
            pixels = self.processor.preprocess(images[offset:offset + self.batch_size])
            preprocess_s = time.perf_counter() - at
            batch = ImageEmbeddingBatch(ids, pixels, "preprocessed_tensor",
                                        len(ids) * 224 * 224, "pixels")
            result = self.actor.embed(batch)
            chunks.append(result.embeddings)
            costs.append(dict(decode_resize_normalize_s=preprocess_s, actor_service_s=result.service_s,
                host_copy_s=result.telemetry.host_copy_s, device_input_bytes=result.telemetry.device_input_bytes,
                output_bytes=result.telemetry.output_bytes))
        self.execution_observations.append(dict(batches=costs, detailed_cuda_timing=False,
            transport_time_s=None))
        return ImageEmbeddingResult(tuple(doc_ids), np.concatenate(chunks), self.semantics,
                                    time.perf_counter() - started)

    def close(self):
        self.actor.synchronize()


class SlowClipPrepare:
    """A workload preprocessor; execution and queues belong to the chosen engine."""
    def __init__(self, processor_revision, torch_intraop_threads=1, torch_interop_threads=1):
        from src.modalities.image.clip import configure_torch_thread_pools
        configure_torch_thread_pools(torch_intraop_threads, torch_interop_threads)
        self.processor = ClipImagePreprocessor(processor_revision, local_files_only=True)

    def ready(self):
        return {"input_kind": "encoded_bytes", "processor_backend": "PIL"}

    def preprocess(self, doc_ids, encoded_images):
        pixels = self.processor.preprocess(encoded_images)
        return ImageEmbeddingBatch(tuple(doc_ids), pixels, "preprocessed_tensor",
                                   len(doc_ids) * 224 * 224, "pixels")

    def preprocess_staged(self, descriptor, encoded_images):
        from src.modalities.image.staged import build_prepared_image_block_descriptor
        at = time.perf_counter()
        pixels = self.processor.preprocess(encoded_images)
        preprocess_s = time.perf_counter() - at
        prepared = build_prepared_image_block_descriptor(descriptor, pixels, ready_at_s=time.perf_counter())
        batch = ImageEmbeddingBatch(descriptor.row_ids, pixels, "preprocessed_tensor",
                                   descriptor.model_work_units, "pixels", work_descriptor=descriptor.work,
                                   telemetry=ImageBatchTelemetry(preprocess_s=preprocess_s,
                                       encoded_bytes=sum(len(x) for x in encoded_images), input_tensor_bytes=pixels.nbytes))
        return prepared, batch

    def __call__(self, batch):
        import numpy as np
        return {"doc_id": np.asarray(batch["doc_id"]),
                "pixel_values": self.processor.preprocess([bytes(x) for x in batch["image"]])}


class OrderedMatrix:
    """Collect the complete explicitly bounded matrix required by native sem_index."""
    def __init__(self, ids, semantics):
        if len(ids) > 5000:
            raise ValueError("native index collection exceeds 5000 rows")
        self.ids, self.semantics, self.rows = tuple(ids), semantics, {}

    def add(self, ids, matrix):
        from .image_rm import checked_matrix
        values = checked_matrix(matrix, len(ids), self.semantics)
        for doc_id, row in zip(ids, values, strict=True):
            if doc_id not in self.ids or doc_id in self.rows:
                raise ValueError("native index collection returned an unexpected or duplicate ID")
            self.rows[doc_id] = row.copy()

    def finish(self):
        import numpy as np
        if len(self.rows) != len(self.ids):
            raise ValueError("native index collection is incomplete")
        return np.stack([self.rows[x] for x in self.ids])


class SemLoomImageExecutor:
    """Adapt the existing SemLoom driver/HSE to the public image batch interface."""
    def __init__(self, *, model, processor=None, dtype="float32", batch_size=32,
                 max_active_batches=2, hse=True, encoded_bytes_limit=None,
                 encoded_block_bytes_upper_bound=None):
        for value in (encoded_bytes_limit, encoded_block_bytes_upper_bound):
            if value is not None and (type(value) is not int or value <= 0):
                raise ValueError("HSE encoded byte settings must be positive integers")
        if not hse and (encoded_bytes_limit is not None or encoded_block_bytes_upper_bound is not None):
            raise ValueError("encoded byte settings require HSE")
        import ray
        from src.modalities.image.contracts import EmbeddingSemantics
        from src.modalities.image.execution import SemLoomRayWorkerPool, RayWorkerPoolStartupFailure
        if not ray.is_initialized():
            raise ValueError("caller must initialize an exclusively owned Ray runtime")
        if min(batch_size, max_active_batches) < 1:
            raise ValueError("positive SemLoom batch and activity settings are required")
        self.batch_size, self.window, self.hse = batch_size, max_active_batches, hse
        self.model, self.processor, self.dtype = model, processor or model, dtype
        self.encoded_bytes_limit = encoded_bytes_limit
        self.encoded_block_bytes_upper_bound = encoded_block_bytes_upper_bound
        self.execution_observations = []
        self.semantics = EmbeddingSemantics(model, self.processor, 512)
        self.pool = None
        self._closed = self._unconfirmed_work = False
        self._lock = threading.Lock()
        handles = []
        try:
            cpu = ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)(SlowClipPrepare).remote(self.processor)
            handles.append(cpu)
            gpu = ray.remote(num_cpus=1, num_gpus=1, max_restarts=0, max_task_retries=0)(ClipTensorActor).remote(
                model, processor_revision=self.processor, dtype=dtype, normalize=True,
                local_files_only=True, torch_intraop_threads=1, torch_interop_threads=1)
            handles.append(gpu)
            ray.get([cpu.ready.remote(), gpu.ready.remote()])
            self.pool = SemLoomRayWorkerPool((cpu,), (gpu,))
        except BaseException as error:
            failure = RayWorkerPoolStartupFailure(tuple(handles))
            error.image_worker_pool_startup = failure
            try:
                failure.request_termination()
            except BaseException as cleanup_error:
                error.add_note("worker startup termination raised " + type(cleanup_error).__name__)
            error.add_note("retained startup actors are cleanup-only; physical exit is unconfirmed")
            raise

    def embed_batch(self, doc_ids, images):
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("SemLoom image executor already has an active batch")
        try:
            if self._closed or self.pool is None:
                raise RuntimeError("SemLoom image executor is closed")
            if self._unconfirmed_work:
                raise RuntimeError("SemLoom image worker exit is unconfirmed")
            return self._embed_batch(doc_ids, images)
        finally:
            self._lock.release()

    def _embed_batch(self, doc_ids, images):
        import daft
        from src.modalities.image.execution import run_semloom_ray_pipeline
        from src.modalities.image.staged_execution import run_semloom_ray_hse_pipeline
        from src.scheduling.runtime.stage_broker import StageBrokerLimits
        if not doc_ids or len(set(doc_ids)) != len(doc_ids) or len(images) != len(doc_ids):
            raise ValueError("image IDs and inputs must be nonempty, unique and aligned")
        source = daft.from_pydict({"doc_id": list(doc_ids), "image": list(images)})
        collector = OrderedMatrix(doc_ids, self.semantics)
        kwargs = dict(worker_pool=self.pool, expected_doc_ids=frozenset(doc_ids),
                      batch_size=self.batch_size, max_active_batches=self.window,
                      embedding_dimension=512, embedding_capture=collector)
        started = time.perf_counter()
        self._unconfirmed_work = True
        if self.hse:
            encoded = max(len(x) for x in images) * self.batch_size
            block_upper = self.encoded_block_bytes_upper_bound if self.encoded_block_bytes_upper_bound is not None else encoded
            capacity = self.encoded_bytes_limit if self.encoded_bytes_limit is not None else encoded * self.window
            prepared = self.batch_size * 3 * 224 * 224 * 4
            execution = run_semloom_ray_hse_pipeline(source, **kwargs,
                encoded_block_bytes_upper_bound=block_upper,
                limits=StageBrokerLimits(encoded_bytes=capacity,
                    prepare_inflight=1, ready_bytes=prepared * self.window,
                    ready_work=self.batch_size * 224 * 224 * self.window,
                    model_inflight=1),
                model_revision=self.model, processor_revision=self.processor, model_dtype=self.dtype)
            self.execution_observations.append(dict(encoded_bytes_limit=capacity,
                encoded_block_bytes_upper_bound=block_upper, encoded_bytes_peak=execution.encoded_bytes_peak,
                ready_bytes_peak=execution.ready_bytes_peak, batch_preprocess_s=execution.batch_preprocess_s,
                batch_host_copy_s=execution.batch_host_copy_s, batch_actor_service_s=execution.batch_actor_service_s,
                batch_submit_s=execution.batch_submit_s, device_input_bytes=execution.device_input_bytes,
                detailed_cuda_timing=False, transport_time_s=None))
        else:
            run_semloom_ray_pipeline(source, **kwargs)
        result = ImageEmbeddingResult(tuple(doc_ids), collector.finish(), self.semantics,
                                      time.perf_counter() - started)
        self._unconfirmed_work = False
        return result

    def close(self):
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("cannot close SemLoom image executor while a batch is active")
        try:
            self._closed = True
            if self.pool is not None:
                from src.modalities.image.execution import stop_semloom_ray_worker_pool
                stop_semloom_ray_worker_pool(self.pool)
                self.pool = None
        finally:
            self._lock.release()


class RayDataImageExecutor:
    """Native Ray Data graph reusing the existing tensor predictor."""
    def __init__(self, *, model, processor=None, dtype="float32", batch_size=32):
        from src.modalities.image.contracts import EmbeddingSemantics
        self.model, self.processor, self.dtype = model, processor or model, dtype
        self.batch_size = batch_size
        self.semantics = EmbeddingSemantics(model, self.processor, 512)
        self._closed = self._unconfirmed_work = False
        self._lock = threading.Lock()

    def embed_batch(self, doc_ids, images):
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("Ray Data image executor already has an active batch")
        try:
            if self._closed:
                raise RuntimeError("Ray Data image executor is closed")
            if self._unconfirmed_work:
                raise RuntimeError("Ray Data image worker exit is unconfirmed")
            return self._embed_batch(doc_ids, images)
        finally:
            self._lock.release()

    def _embed_batch(self, doc_ids, images):
        import ray
        import ray.data
        from src.baselines.image.frameworks.ray_data import RayDataClipPredictor
        if not ray.is_initialized():
            raise ValueError("caller must initialize an exclusively owned Ray runtime")
        if not doc_ids or len(set(doc_ids)) != len(doc_ids) or len(images) != len(doc_ids):
            raise ValueError("image IDs and inputs must be nonempty, unique and aligned")
        collector = OrderedMatrix(doc_ids, self.semantics)
        started = time.perf_counter()
        data = ray.data.from_items([{"doc_id": x, "image": y} for x, y in zip(doc_ids, images, strict=True)])
        data = data.map_batches(SlowClipPrepare, batch_format="numpy", batch_size=self.batch_size,
            compute=ray.data.ActorPoolStrategy(size=1), num_cpus=1,
            fn_constructor_kwargs={"processor_revision": self.processor})
        data = data.map_batches(RayDataClipPredictor, batch_format="numpy", batch_size=self.batch_size,
            compute=ray.data.ActorPoolStrategy(size=1), num_cpus=1, num_gpus=1,
            fn_constructor_kwargs={"model_revision": self.model, "processor_revision": self.processor,
                "dtype": self.dtype, "torch_intraop_threads": 1, "torch_interop_threads": 1})
        iterator = None
        error = None
        self._unconfirmed_work = True
        try:
            iterator = iter(data.iter_batches(batch_format="numpy", prefetch_batches=1))
            for batch in iterator:
                collector.add(tuple(str(x) for x in batch["doc_id"]), __import__("numpy").asarray(batch["embedding"], dtype="float32"))
            result = ImageEmbeddingResult(tuple(doc_ids), collector.finish(), self.semantics,
                                          time.perf_counter() - started)
        except BaseException as exc:
            error = exc
        finally:
            actions = []
            close = getattr(iterator, "close", None)
            if close is not None:
                actions.append(("iterator close", close))
            executor = getattr(data, "_current_executor", None)
            if executor is not None:
                actions.append(("graph shutdown", lambda: executor.shutdown(force=True)))
            for name, action in actions:
                try:
                    action()
                except BaseException as cleanup_error:
                    from src.experiments.execution_timing import is_timing_error
                    if error is None:
                        error = cleanup_error
                    elif is_timing_error(error) and not is_timing_error(cleanup_error):
                        cleanup_error.add_note("Prior image timing failure: " + type(error).__name__)
                        error = cleanup_error
                    else:
                        error.add_note(name + " also raised " + type(cleanup_error).__name__)
        if error is not None:
            raise error
        self._unconfirmed_work = False
        return result

    def close(self):
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("cannot close Ray Data image executor while a batch is active")
        try:
            self._closed = True
            if self._unconfirmed_work:
                raise RuntimeError("Ray Data image worker exit is unconfirmed")
            # Caller-owned Ray shutdown and physical process-exit checks remain separate.
        finally:
            self._lock.release()
