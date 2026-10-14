"""Opt-in actual Ray software checks; CPU fixtures execute every model role.

These exercise real Arrow, ObjectRef dependencies, serial actors and HSE/broker
code. Controlled CPU delays test decisions, not GPU speed or tail performance.
"""

import dataclasses
from pathlib import Path
import time
from unittest.mock import patch

from src.modalities.image import staged_execution
from src.modalities.image.execution import ProjectRayWorkerPool
from src.modalities.image.hse_dispatch import observe_ray_stage_destination
from src.scheduling.runtime.stage_broker import BoundedStageBroker, StageBrokerLimits
from src.scheduling.runtime.stage_dispatch import (
    StageActorCapacity, StageCapacity, StageExecutionGroupCapacity, StageModelDispatcher,
)
from tests.modalities.image import test_hse_static_ray as base


class _DispatchModelFixture(base._ModelFixture):
    def __init__(self, actor_index, delay_s=0.01, zero_service=False,
                 fail_model=False, fail_fence=False, invalid_output=False,
                 await_release=None, signal_started=None):
        super().__init__(fail_model=fail_model)
        self.actor_index, self.delay_s, self.zero_service = actor_index, delay_s, zero_service
        self.fail_fence, self.invalid_output, self.calls = fail_fence, invalid_output, []
        self.await_release, self.signal_started = await_release, signal_started

    def boot(self):
        identity = super().boot()
        identity["destination"] = observe_ray_stage_destination(
            endpoint_id=f"model-{self.actor_index}", execution_group_id=f"group-{self.actor_index}")
        return identity

    def embed(self, batch):
        record = {"row_ids": batch.doc_ids, "started_epoch_s": time.time(),
                  "payload_readonly": not batch.payload.flags.writeable}
        self.calls.append(record)
        started = time.perf_counter()
        try:
            if self.signal_started is not None:
                Path(self.signal_started).touch()
                record["start_signalled"] = True
            if self.await_release is not None:
                # Single-node fixture handshake establishes overlap without
                # assuming the driver or second actor runs within a short sleep.
                deadline = time.monotonic() + base._OPERATION_TIMEOUT_S
                while not Path(self.await_release).exists():
                    if time.monotonic() >= deadline:
                        raise TimeoutError("fixture model release was not confirmed")
                    time.sleep(0.005)
                record["release_confirmed"] = True
            time.sleep(self.delay_s)
            result = super().embed(batch)
            if self.invalid_output:
                result.embeddings[0, 0] = float("nan")
            service_s = 0.0 if self.zero_service else time.perf_counter() - started
            record["service_s"] = service_s
            return dataclasses.replace(result, service_s=service_s)
        finally:
            record["finished_epoch_s"] = time.time()

    def synchronize(self):
        self.events.append("model_fence")
        if self.fail_fence:
            raise RuntimeError("fixture model confirmation unavailable")
        return {"fixture_fence": "model", "actor_index": self.actor_index}

    def trace(self):
        return {"actor_index": self.actor_index, "calls": self.calls, "events": self.events}


class _RecordedDispatcher(StageModelDispatcher):
    def __init__(self, *args, downshift_index=None, require_active=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.records, self.downshift_index, self.require_active = [], downshift_index, require_active
        self.downshift_at_s = None

    def choose(self, descriptor, request, *, available_actor_indices, active, now_s):
        if (self.downshift_index is not None and self.downshift_at_s is None
                and (active or not self.require_active)):
            self.observe_actor_capacity(self.downshift_index, StageCapacity(0, 0), observed_at_s=now_s)
            self.downshift_at_s = now_s
        decision = super().choose(descriptor, request, available_actor_indices=available_actor_indices,
                                  active=active, now_s=now_s)
        self.records.append({"block_id": descriptor.block_id, "row_ids": descriptor.row_ids,
            "retry_count": descriptor.retry_count, "now_s": now_s,
            "active": [dataclasses.asdict(item) for item in active],
            "available_actor_indices": sorted(available_actor_indices),
            "decision": dataclasses.asdict(decision),
            "observed_rates": {str(index): dataclasses.asdict(value) for index, value in self._rates.items()}})
        return decision


class _ErrorReleaseRay(base._ObservedRay):
    """Keep the peer pending until the driver observes the original model error."""

    def __init__(self, api, events, release_marker, diagnostic_errors):
        super().__init__(api, events)
        self.release_marker = release_marker
        self.diagnostic_errors = diagnostic_errors

    def get(self, reference, *, timeout=None):
        try:
            return super().get(reference, timeout=timeout)
        except self.api.exceptions.RayTaskError as error:
            if "fixture model failed" in str(error):
                try:
                    self.events.append("model_error_observed")
                    Path(self.release_marker).touch()
                except BaseException as diagnostic_error:
                    self.diagnostic_errors.append({"error_type": type(diagnostic_error).__name__})
                    error.add_note(f"fixture release raised {type(diagnostic_error).__name__}")
            raise


class HSEDeviceDispatchActualRayTests(base.HSEStaticActualRayTests):
    """Include the five published default-path cases in the same CPU Ray run."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        try:
            cls.dispatch_model_type = cls.ray.remote(num_cpus=1, num_gpus=0, max_restarts=0,
                                                     max_task_retries=0)(_DispatchModelFixture)
        except BaseException as error:
            try:
                cls.ray.shutdown()
            except BaseException as cleanup_error:
                error.add_note(f"fixture shutdown raised {type(cleanup_error).__name__}")
            raise

    def setUp(self):
        super().setUp()
        self.dispatchers, self.dispatch_traces, self.model_leases = [], [], []
        self.dispatch_diagnostic_errors = []

    def marker(self, name):
        directory = base.tempfile.mkdtemp(prefix="dispatch-gate-", dir=self.ray_temp)
        return str(Path(directory) / name)

    def tearDown(self):
        super().tearDown()
        self.assertFalse(self.dispatch_diagnostic_errors, "fixture diagnostics failed")

    def dispatch_pool(self, *, model_options=({}, {})):
        cpus, models, destinations = [], [], []
        for _index in range(2):
            actor = self.cpu_type.remote()
            self.actors.append(actor)
            cpus.append(actor)
        for index, options in enumerate(model_options):
            actor = self.dispatch_model_type.remote(actor_index=index, **options)
            self.actors.append(actor)
            models.append(actor)
        for actor in (*cpus, *models):
            identity = self.ray.get(actor.boot.remote(), timeout=base._OPERATION_TIMEOUT_S)
            self.assertEqual(identity["node"], self.ray.get_runtime_context().get_node_id())
            destination = identity.pop("destination", None)
            if destination is not None:
                self.assertEqual(destination.accelerator_ids, ())
                destinations.append(destination)
                identity["runtime_actor_id"] = destination.runtime_actor_id
            self.identities.append(identity)
        self.assertEqual(len({item.runtime_actor_id for item in destinations}), len(models))
        return ProjectRayWorkerPool(tuple(cpus), tuple(models)), tuple(destinations)

    def policy(self, destinations, *, group_requests=None, downshift_index=None, require_active=False):
        if group_requests is not None:
            destinations = tuple(dataclasses.replace(item, execution_group_id="shared") for item in destinations)
        actors = tuple(StageActorCapacity(index, item, StageCapacity(1, 8))
                       for index, item in enumerate(destinations))
        groups = ((StageExecutionGroupCapacity("shared", StageCapacity(group_requests, 8)),)
                  if group_requests is not None else tuple(StageExecutionGroupCapacity(
                      item.execution_group_id, StageCapacity(1, 8)) for item in destinations))
        policy = _RecordedDispatcher(actors, groups, feedback_ttl_s=0.15,
            downshift_index=downshift_index, require_active=require_active)
        self.dispatchers.append(policy)
        return policy

    def run_dispatch(self, pool, policy, *, rows=("a", "b", "c", "d"), model_slots=2):
        test = self

        class ObservedBroker(BoundedStageBroker):
            def __init__(self, limits):
                super().__init__(limits)
                test.brokers.append(self)

            def lease_model(self, **kwargs):
                lease = super().lease_model(**kwargs)
                if lease is not None:
                    test.model_leases.append({"block_id": lease.descriptor.block_id,
                        "issued_at_s": lease.issued_at_s, "retry_count": lease.descriptor.retry_count,
                        "model_inflight": self.snapshot().model_inflight})
                return lease

            def fail_model(self, lease_id, *, requeue):
                test.events.append("released_model")
                return super().fail_model(lease_id, requeue=requeue)

        primary_error = None
        try:
            with patch.dict("sys.modules", {"ray": self.observed_ray}), patch.object(
                    staged_execution, "BoundedStageBroker", ObservedBroker):
                return staged_execution.run_semloom_ray_hse_pipeline(
                    base._ArrowSource(rows, 1), worker_pool=pool, expected_doc_ids=frozenset(rows),
                    batch_size=1, max_active_batches=4, encoded_block_bytes_upper_bound=1,
                    limits=StageBrokerLimits(8, 192, 16, 2, model_slots),
                    model_revision="fixture", processor_revision="fixture", model_dtype="float32",
                    input_size=2, embedding_dimension=2, model_dispatch=policy,
                )
        except BaseException as error:
            primary_error = error
            raise
        finally:
            try:
                self.dispatch_traces = [self.ray.get(actor.trace.remote(), timeout=base._OPERATION_TIMEOUT_S)
                                        for actor in pool.gpu_actors]
            except BaseException as diagnostic_error:
                self.dispatch_diagnostic_errors.append({"error_type": type(diagnostic_error).__name__})
                if primary_error is None:
                    raise
                primary_error.add_note(f"fixture trace raised {type(diagnostic_error).__name__}")

    def test_actual_completed_feedback_and_shared_group_capacity(self):
        pool, destinations = self.dispatch_pool(model_options=({"delay_s": 0.08}, {"delay_s": 0.005}))
        policy = self.policy(destinations, group_requests=1)
        result = self.run_dispatch(pool, policy)
        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual(result.model_inflight_peak, 1)  # Broker permits two; the group permits one.
        self.assertEqual(policy.snapshot()["sampled_actor_indices"], [0, 1])
        selected = [record for record in policy.records if record["decision"]["actor_index"] is not None]
        self.assertEqual([item["decision"]["actor_index"] for item in selected[:2]], [0, 1])
        for item in selected[2:]:
            self.assertEqual(item["decision"]["reason"], "least_work")
            rates = item["observed_rates"]
            expected = min(policy.actors, key=lambda actor: (
                4 / rates[str(actor.actor_index)]["work_units_s"], actor.destination.endpoint_id))
            self.assertEqual(item["decision"]["actor_index"], expected.actor_index)
        self.assertNotIn("ImageEmbeddingBatch", self.observed_ray.get_types)
        self.assertEqual(sum(len(item["calls"]) for item in self.dispatch_traces), 4)

    def test_actual_capacity_expiry_with_no_pending_model(self):
        pool, destinations = self.dispatch_pool(model_options=({},))
        policy = self.policy(destinations, downshift_index=0)
        result = self.run_dispatch(pool, policy, rows=("a",), model_slots=1)
        self.assertTrue(result.audit["exactly_once"])
        self.assertTrue(any(item["decision"]["reason"] == "capacity_wait" for item in policy.records))
        self.assertEqual(len(self.model_leases), 1)
        self.assertGreaterEqual(self.model_leases[0]["issued_at_s"], policy.downshift_at_s + policy.feedback_ttl_s)
        self.assertEqual(self.model_leases[0]["retry_count"], 0)

    def test_actual_expiry_dispatches_free_actor_while_other_model_is_pending(self):
        marker = self.marker("second-model-started")
        pool, destinations = self.dispatch_pool(model_options=(
            {"await_release": marker}, {"signal_started": marker}))
        policy = self.policy(destinations, downshift_index=1, require_active=True)
        result = self.run_dispatch(pool, policy, rows=("a", "b"))
        self.assertTrue(result.audit["exactly_once"])
        self.assertTrue(any(item["decision"]["reason"] == "capacity_wait" and item["active"]
                            for item in policy.records))
        self.assertEqual(result.model_inflight_peak, 2)
        first, second = [item["calls"][0] for item in self.dispatch_traces]
        self.assertLess(second["started_epoch_s"], first["finished_epoch_s"])
        self.assertTrue(first["release_confirmed"] and second["start_signalled"])
        self.assertTrue(all(item["retry_count"] == 0 for item in self.model_leases))

    def test_actual_zero_service_rotates_without_training_invalid_rate(self):
        pool, destinations = self.dispatch_pool(model_options=({"zero_service": True}, {}))
        policy = self.policy(destinations)
        result = self.run_dispatch(pool, policy, model_slots=1)
        self.assertTrue(result.audit["exactly_once"])
        selected = [item["decision"]["actor_index"] for item in policy.records
                    if item["decision"]["actor_index"] is not None]
        self.assertEqual(selected, [0, 1, 0, 1])
        self.assertEqual(policy.snapshot()["sampled_actor_indices"], [1])

    def test_actual_model_error_and_failed_fence_retain_only_unknown_owner(self):
        marker = self.marker("second-model-started")
        release = self.marker("driver-observed-model-error")
        pool, destinations = self.dispatch_pool(model_options=(
            {"fail_model": True, "fail_fence": True, "await_release": marker},
            {"signal_started": marker, "await_release": release}))
        policy = self.policy(destinations)
        self.observed_ray = _ErrorReleaseRay(self.ray, self.events, release, self.dispatch_diagnostic_errors)
        with self.assertRaisesRegex(self.ray.exceptions.RayTaskError, "fixture model failed") as caught:
            self.run_dispatch(pool, policy, rows=("a", "b"))
        retained = caught.exception.hse_unconfirmed_work
        self.assertEqual([call.actor_index for call in retained.calls], [0])
        snapshot = retained.broker.snapshot()
        self.assertEqual((snapshot.model_inflight, snapshot.ready_held_bytes, snapshot.ready_held_work), (1, 48, 4))
        self.assertIsInstance(retained.calls[0].payload_ref, self.ray.ObjectRef)
        self.assertNotIn(0, policy.snapshot()["sampled_actor_indices"])
        self.assertEqual(len(self.model_leases), 2)
        self.assertLess(self.events.index("model_error_observed"), self.events.index("confirmed_model"))
        self.assertLess(self.events.index("confirmed_model"), self.events.index("released_model"))
        with self.assertRaisesRegex(RuntimeError, "unconfirmed work"):
            self.run_dispatch(pool, policy, rows=("c",))

    def test_actual_invalid_output_is_completed_but_does_not_train_rate(self):
        pool, destinations = self.dispatch_pool(model_options=({"invalid_output": True},))
        policy = self.policy(destinations)
        with self.assertRaisesRegex(ValueError, "non-finite"):
            self.run_dispatch(pool, policy, rows=("a",), model_slots=1)
        self.assertEqual(policy.snapshot()["sampled_actor_indices"], [])
        self.assertTrue(self.brokers[-1].is_drained())
        self.assertEqual(self.brokers[-1].snapshot().completed, 1)
