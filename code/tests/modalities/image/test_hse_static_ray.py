"""Opt-in actual Ray/Arrow stage checks, with no model, HTTP or GPU use."""

import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import numpy as np

from src.modalities.image import staged_execution
from src.modalities.image.contracts import (
    EmbeddingSemantics, ImageBatchTelemetry, ImageEmbeddingBatch, ImageEmbeddingResult,
)
from src.modalities.image.execution import ProjectRayWorkerPool
from src.modalities.image.staged import build_prepared_image_block_descriptor
from src.scheduling.runtime.stage_broker import BoundedStageBroker, StageBrokerLimits


_OPERATION_TIMEOUT_S = 60.0
_CLEANUP_TIMEOUT_S = 30.0


class _ArrowSource:
    def __init__(self, rows, batch_size):
        self.rows, self.batch_size = rows, batch_size

    def into_batches(self, batch_size):
        assert batch_size == self.batch_size
        return self

    def to_arrow_iter(self, *, results_buffer_size):
        import pyarrow as pa
        assert results_buffer_size == 2
        for start in range(0, len(self.rows), self.batch_size):
            rows = self.rows[start:start + self.batch_size]
            yield pa.record_batch([pa.array(rows), pa.array([row.encode() for row in rows])],
                                  names=["doc_id", "image"])


class _CpuFixture:
    def __init__(self, fail_prepare=False, fail_fence=False):
        self.fail_prepare, self.fail_fence = fail_prepare, fail_fence
        self.events = []

    def boot(self):
        import psutil
        import ray
        return dict(pid=os.getpid(), birth=psutil.Process().create_time(),
                    node=ray.get_runtime_context().get_node_id())

    def preprocess_staged(self, descriptor, encoded):
        self.events.append("prepare_started")
        if self.fail_prepare:
            self.events.append("prepare_failed")
            raise RuntimeError("fixture prepare failed")
        pixels = np.zeros((len(encoded), 3, 2, 2), dtype=np.float32)
        prepared = build_prepared_image_block_descriptor(
            descriptor, pixels, ready_at_s=descriptor.created_at_s)
        payload = ImageEmbeddingBatch(
            doc_ids=descriptor.row_ids, payload=pixels, input_kind="preprocessed_tensor",
            work_units=descriptor.model_work_units, work_unit="pixels", work_descriptor=descriptor.work,
            telemetry=ImageBatchTelemetry(encoded_bytes=descriptor.physical_bytes,
                                          input_tensor_bytes=pixels.nbytes),
        )
        self.events.append("prepare_finished")
        return prepared, payload

    def ready(self):
        self.events.append("prepare_fence")
        if self.fail_fence:
            raise RuntimeError("fixture prepare confirmation unavailable")
        return {"fixture_fence": "prepare"}

    def history(self):
        return tuple(self.events)


class _ModelFixture:
    boot = _CpuFixture.boot

    def __init__(self, fail_model=False):
        self.fail_model = fail_model
        self.events = []

    def embed(self, batch):
        self.events.append("model_started")
        assert type(batch) is ImageEmbeddingBatch
        assert isinstance(batch.payload, np.ndarray)
        if self.fail_model:
            self.events.append("model_failed")
            raise RuntimeError("fixture model failed")
        embeddings = np.zeros((len(batch.doc_ids), 2), dtype=np.float32)
        embeddings[:, 0] = 1
        self.events.append("model_finished")
        return ImageEmbeddingResult(
            doc_ids=batch.doc_ids, embeddings=embeddings,
            semantics=EmbeddingSemantics("fixture", "fixture", 2), service_s=0,
            telemetry=batch.telemetry,
        )

    def synchronize(self):
        self.events.append("model_fence")
        return {"fixture_fence": "model"}

    def history(self):
        return tuple(self.events)


class _ObservedRay:
    def __init__(self, api, events):
        self.api, self.events, self.get_types = api, events, []

    def __getattr__(self, name):
        return getattr(self.api, name)

    def wait(self, refs, *, num_returns, timeout=None):
        # Timeout covers one wait without completion, never total healthy wall.
        ready, rest = self.api.wait(refs, num_returns=num_returns,
                                    timeout=_OPERATION_TIMEOUT_S if timeout is None else timeout)
        if not ready and timeout is None:
            raise TimeoutError("fixture Ray call produced no completion")
        return ready, rest

    def get(self, reference, *, timeout=None):
        value = self.api.get(reference, timeout=_OPERATION_TIMEOUT_S if timeout is None else timeout)
        self.get_types.append(type(value).__name__)
        assert not isinstance(value, ImageEmbeddingBatch), "driver fetched the prepared payload"
        if isinstance(value, dict) and "fixture_fence" in value:
            self.events.append("confirmed_" + value["fixture_fence"])
        return value


class _LostSubmitActor:
    """Drop one driver acknowledgement after the real actor accepted work."""

    def __init__(self, actor):
        self.actor, self.inject, self.refs = actor, True, []
        self.preprocess_staged = self

    def options(self, **kwargs):
        self.method = self.actor.preprocess_staged.options(**kwargs)
        return self

    def remote(self, *args):
        refs = self.method.remote(*args)
        self.refs.extend(refs)
        if self.inject:
            self.inject = False
            raise RuntimeError("fixture submit acknowledgement lost")
        return refs

    def __getattr__(self, name):
        return getattr(self.actor, name)


@unittest.skipUnless(os.environ.get("SEMLOOM_RUN_HSE_RAY") == "1",
                     "requires an explicitly scheduled CPU Ray window")
class HSEStaticActualRayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import pyarrow  # noqa: F401 -- explicit opt-in must fail if dependencies are absent
        import psutil
        import ray
        cls.ray, cls.psutil = ray, psutil
        cls.cleanup_unconfirmed = False
        cls.cleanup_incidents = []
        if ray.is_initialized():
            raise RuntimeError("HSE fixture must own its driver connection")
        env = {"PYTHONPATH": str(Path(__file__).resolve().parents[3]),
               "CUDA_VISIBLE_DEVICES": "", "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}
        try:
            # Retain raw runtime logs here for the owner's private evidence.
            cls.ray_temp = tempfile.mkdtemp(prefix="hs", dir="/tmp")
            ray.init(address="local", num_cpus=4, num_gpus=0,
                     object_store_memory=512 * 1024 ** 2, include_dashboard=False,
                     _temp_dir=cls.ray_temp, runtime_env={"env_vars": env})
            cls.cpu_type = ray.remote(num_cpus=1, num_gpus=0, max_restarts=0,
                                      max_task_retries=0)(_CpuFixture)
            cls.model_type = ray.remote(num_cpus=1, num_gpus=0, max_restarts=0,
                                        max_task_retries=0)(_ModelFixture)
        except BaseException as error:
            try:
                ray.shutdown()
            except BaseException as cleanup_error:
                error.add_note(f"fixture shutdown raised {type(cleanup_error).__name__}")
            raise

    @classmethod
    def tearDownClass(cls):
        cls.ray.shutdown()

    def setUp(self):
        if type(self).cleanup_unconfirmed:
            self.skipTest("previous fixture cleanup is unconfirmed; no later actor creation")
        self.actors, self.identities, self.events, self.brokers = [], [], [], []
        self.observed_ray = _ObservedRay(self.ray, self.events)

    def pool(self, *, cpu_options=({},), model_options=None):
        cpus = []
        for options in cpu_options:
            actor = self.cpu_type.remote(**options)
            self.actors.append(actor)
            cpus.append(actor)
        model = self.model_type.remote(**(model_options or {}))
        self.actors.append(model)
        for actor in (*cpus, model):
            identity = self.ray.get(actor.boot.remote(), timeout=_OPERATION_TIMEOUT_S)
            self.assertEqual(identity["node"], self.ray.get_runtime_context().get_node_id(),
                             "PID exit proof requires a single-node Ray fixture")
            self.identities.append(identity)
        return ProjectRayWorkerPool(tuple(cpus), (model,))

    def run_pipeline(self, pool, *, rows=("a",), batch_size=1):
        test = self

        class ObservedBroker(BoundedStageBroker):
            def __init__(self, limits):
                super().__init__(limits)
                test.brokers.append(self)

            def fail_prepare(self, lease_id, *, requeue):
                test.events.append("released_prepare")
                return super().fail_prepare(lease_id, requeue=requeue)

            def fail_model(self, lease_id, *, requeue):
                test.events.append("released_model")
                return super().fail_model(lease_id, requeue=requeue)

        with patch.dict("sys.modules", {"ray": self.observed_ray}), patch.object(
                staged_execution, "BoundedStageBroker", ObservedBroker):
            return staged_execution.run_project_ray_hse_pipeline(
                _ArrowSource(rows, batch_size), worker_pool=pool,
                expected_doc_ids=frozenset(rows), batch_size=batch_size, max_active_batches=3,
                encoded_block_bytes_upper_bound=batch_size,
                limits=StageBrokerLimits(6, 192, 16, len(pool.preprocessors), 1),
                model_revision="fixture", processor_revision="fixture", model_dtype="float32",
                input_size=2, embedding_dimension=2,
            )

    def tearDown(self):
        # Any further interruption must also prevent the next case creating actors.
        type(self).cleanup_unconfirmed = True
        kill_errors, proof_errors = [], []
        for index, actor in enumerate(self.actors):
            try:
                self.ray.kill(actor, no_restart=True)
            except BaseException as error:
                kill_errors.append(dict(actor_index=index, error_type=type(error).__name__))
        deadline = time.monotonic() + _CLEANUP_TIMEOUT_S
        remaining = list(self.identities)
        while remaining:
            live = []
            for identity in remaining:
                try:
                    process = self.psutil.Process(identity["pid"])
                    if (process.create_time() == identity["birth"]
                            and process.is_running() and process.status() != self.psutil.STATUS_ZOMBIE):
                        live.append(identity)
                except self.psutil.NoSuchProcess:
                    pass
                except BaseException as error:
                    proof_errors.append(dict(error_type=type(error).__name__))
                    live.append(identity)
            remaining = live
            if remaining and time.monotonic() >= deadline:
                break
            if remaining:
                time.sleep(0.05)
        missing_identities = len(self.actors) - len(self.identities)
        if kill_errors or proof_errors or remaining or missing_identities:
            incident = dict(case=self.id(), kill_errors=kill_errors, proof_errors=proof_errors,
                            unconfirmed_processes=len(remaining), missing_identities=missing_identities)
            type(self).cleanup_incidents.append(incident)
            raise RuntimeError(f"fixture cleanup unconfirmed: {incident}")
        type(self).cleanup_unconfirmed = False

    def test_real_multi_return_dependency_and_tail_batch(self):
        result = self.run_pipeline(self.pool(cpu_options=({}, {})),
                                   rows=("a", "b", "c", "d", "e"), batch_size=2)
        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual((result.audit["output_rows"], result.submitted_batches), (5, 3))
        self.assertEqual(self.observed_ray.get_types.count("StageBlockDescriptor"), 3)
        self.assertEqual(self.observed_ray.get_types.count("ImageEmbeddingResult"), 3)
        self.assertLessEqual(result.ready_bytes_peak, 192)

    def test_real_prepare_error_confirms_actor_before_release(self):
        pool = self.pool(cpu_options=({"fail_prepare": True},))
        with self.assertRaisesRegex(self.ray.exceptions.RayTaskError, "fixture prepare failed"):
            self.run_pipeline(pool)
        self.assertLess(self.events.index("confirmed_prepare"), self.events.index("released_prepare"))
        self.assertTrue(self.brokers[-1].is_drained())

    def test_real_model_error_confirms_serial_model_fence_before_release(self):
        pool = self.pool(model_options={"fail_model": True})
        with self.assertRaisesRegex(self.ray.exceptions.RayTaskError, "fixture model failed"):
            self.run_pipeline(pool)
        self.assertLess(self.events.index("confirmed_model"), self.events.index("released_model"))
        self.assertEqual(self.brokers[-1].snapshot().ready_held_bytes, 0)

    def test_real_failed_confirmation_retains_unknown_and_blocks_reuse(self):
        pool = self.pool(cpu_options=({"fail_prepare": True, "fail_fence": True}, {}))
        with self.assertRaisesRegex(self.ray.exceptions.RayTaskError, "fixture prepare failed") as caught:
            self.run_pipeline(pool, rows=("a", "b"))
        snapshot = caught.exception.hse_unconfirmed_work.broker.snapshot()
        self.assertEqual((snapshot.active_blocks, snapshot.prepare_inflight, snapshot.ready_held_bytes),
                         (1, 1, 48))
        self.assertIsInstance(caught.exception.hse_unconfirmed_work.calls[0].payload_ref,
                              self.ray.ObjectRef)
        with self.assertRaisesRegex(RuntimeError, "unconfirmed work"):
            self.run_pipeline(pool, rows=("c",))

    def test_real_submit_ack_loss_uses_ordered_actor_fence(self):
        pool = self.pool()
        proxy = _LostSubmitActor(pool.preprocessors[0])
        borrowed = ProjectRayWorkerPool((proxy,), pool.gpu_actors)
        with self.assertRaisesRegex(RuntimeError, "fixture submit acknowledgement lost"):
            self.run_pipeline(borrowed)
        self.assertTrue(self.brokers[-1].is_drained())
        history = self.ray.get(pool.preprocessors[0].history.remote(), timeout=_OPERATION_TIMEOUT_S)
        self.assertLess(history.index("prepare_finished"), history.index("prepare_fence"))
        self.assertTrue(self.run_pipeline(borrowed, rows=("b",)).audit["exactly_once"])
        proxy.refs.clear()


if __name__ == "__main__":
    unittest.main()
