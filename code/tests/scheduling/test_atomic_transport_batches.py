"""Atomic local handoff retains ordinary per-member Core outcomes."""
from dataclasses import replace
import unittest
from src.scheduling.core.session_contract import Acceptance, BatchSubmission
from tests.scheduling.test_incremental_session import setup, task


class AtomicTransportTests(unittest.TestCase):
    def engine(self):
        engine, session, backend, clock = setup(held_tasks=3, active_requests=3, active_work=3,
            input_bytes=24, result_bytes=24, metadata_bytes=1024)
        backend.supports_batch_submit = True
        backend.groups = []
        backend.accept_batch = True
        def bulk(tasks, endpoint):
            if not backend.accept_batch:
                return BatchSubmission(Acceptance.NOT_ACCEPTED)
            backend.groups.append(tasks)
            replies = [backend.try_submit(t, endpoint) for t in tasks]
            return BatchSubmission(Acceptance.ACCEPTED, tuple(r.handle for r in replies))
        backend.try_submit_batch = bulk
        engine.policies = replace(engine.policies, organize=lambda rows: tuple(r.key for r in rows),
            topology=replace(engine.policies.topology, endpoints=engine.policies.topology.endpoints[:1]))
        return engine, session, backend, clock

    def test_one_action_dispatches_whole_batch_and_a_fast_member_delivers_independently(self):
        engine, session, backend, _ = self.engine()
        session.offer(tuple(task(i) for i in range(3)))
        session.advance(1)
        self.assertEqual(len(backend.groups), 1)
        self.assertEqual(len(backend.pending), 3)
        fast = backend.groups[0][1].key
        backend.complete(fast)
        result = session.advance(1)
        self.assertEqual(result.deliveries[0].key, fast)
        self.assertEqual(engine.capacity.usage().active_requests, 2)
        session.release((result.deliveries[0].lease_id,))
        session.close_consumer(clean=False)
        for key in tuple(backend.pending):
            backend.complete(key)
        engine.advance()

    def test_zero_acceptance_restores_all_compute_and_original_queue_clocks(self):
        engine, session, backend, clock = self.engine()
        session.offer(tuple(task(i) for i in range(3)))
        stamps = {k:r.since for k,r in engine.capacity.records.items()}
        clock.now = .5
        backend.accept_batch = False
        session.advance(1)
        self.assertEqual(engine.capacity.usage().active_requests, 0)
        self.assertEqual({k:r.since for k,r in engine.capacity.records.items()}, stamps)
        self.assertEqual(backend.pending, {})
        backend.accept_batch = True
        clock.now = 1
        session.advance(1)
        self.assertEqual(engine.capacity.usage().active_requests, 3)
        session.close_consumer(clean=False)
        for key in tuple(backend.pending): backend.complete(key)
        engine.advance()
