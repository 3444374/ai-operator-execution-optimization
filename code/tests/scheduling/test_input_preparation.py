"""Accepted input borrowing never acquires model capacity or loses ownership."""

import unittest

from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import SessionSpec, TaskKey
from src.scheduling.core.session_jobs import JobBudget
from tests.scheduling.test_incremental_session import setup, task


class Preparation:
    def __init__(self, maximum=8):
        self.maximum = maximum
        self.held, self.ready, self.releasable = set(), set(), set()
        self.batches, self.cancelled = [], []

    def try_prepare(self, tasks):
        accepted = min(len(tasks), self.maximum)
        self.batches.append(tasks[:accepted])
        self.held.update(t.key for t in tasks[:accepted])
        return accepted

    def is_ready(self, key):
        return key in self.ready

    def release(self, key):
        if key not in self.held:
            return True
        self.cancelled.append(key)
        if key not in self.releasable:
            return False
        self.held.remove(key)
        return True


def prepared_setup(*, registered=False, step_actions=8):
    template, _, backend, clock = setup(held_tasks=4, input_bytes=16, result_bytes=16,
                                       offer_tasks=4, step_actions=step_actions)
    preparation = Preparation()
    engine = SessionEngine(template.capacity.limits, backend, template.policies,
                           clock=clock, preparation=preparation)
    if registered:
        job = engine.register_job('fixture', JobBudget(4, 16, 16, 1, 2))
        session = engine.open(SessionSpec(job.job_id, 'flow', 'fixture'), job=job)
    else:
        session = engine.open(SessionSpec('job', 'flow', 'fixture'))
    return engine, session, backend, preparation


class InputPreparationTests(unittest.TestCase):
    def test_only_accepted_prefix_can_be_borrowed_without_compute(self):
        engine, session, backend, preparation = prepared_setup()
        offered = [task(i) for i in range(6)]
        # Offer length is separately validated; use a storage-limited prefix.
        session.offer(offered[:4])
        self.assertEqual(session.offer([task(4)]).accepted_prefix_count, 0)
        session.advance(4)
        keys = {t.key.sequence for batch in preparation.batches for t in batch}
        self.assertEqual(keys, {0, 1, 2, 3})
        self.assertFalse(backend.pending)
        usage = engine.capacity.usage()
        self.assertEqual((usage.held_tasks, usage.input_bytes, usage.active_requests, usage.active_work),
                         (4, 16, 0, 0))
        self.assertFalse(session.advance(4).has_immediate_work)

    def test_preparation_advances_while_model_capacity_is_full(self):
        for registered in (False, True):
            with self.subTest(registered=registered):
                engine, session, backend, preparation = prepared_setup(registered=registered)
                session.offer([task(0), task(1)])
                tick = engine.advance if registered else lambda: session.advance(4)
                tick()
                preparation.ready.update(preparation.held)
                tick()
                self.assertEqual(len(backend.pending), 1)
                session.offer([task(2), task(3)])
                tick()
                self.assertTrue({TaskKey(session.session_id, 2), TaskKey(session.session_id, 3)}
                                <= preparation.held)
                self.assertEqual(engine.capacity.usage().active_requests, 1)

    def test_partial_preparation_prefix_is_retried_without_duplicate_borrowing(self):
        engine, session, _, preparation = prepared_setup()
        preparation.maximum = 1
        session.offer([task(0), task(1)])
        session.advance(2)
        session.advance(2)
        self.assertEqual([t.key.sequence for batch in preparation.batches for t in batch], [0, 1])
        self.assertTrue(all(r.preparation_held for r in engine.capacity.records.values()))

    def test_closed_consumer_retains_local_input_until_reap_confirms_release(self):
        engine, session, backend, preparation = prepared_setup()
        session.offer([task(0)])
        session.advance(1)
        key = TaskKey(session.session_id, 0)
        report = session.close()
        self.assertEqual(report.uncertain_requests, 0)
        self.assertEqual(engine.capacity.usage().input_bytes, 4)
        self.assertFalse(backend.cancelled)
        engine.reap(8)
        self.assertEqual(engine.capacity.usage().held_tasks, 1)
        preparation.releasable.add(key)
        engine.reap(8)
        self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_confirmed_model_result_waits_for_transient_preparation_release(self):
        engine, session, backend, preparation = prepared_setup()
        session.offer([task(0)])
        session.advance(1)
        key = TaskKey(session.session_id, 0)
        preparation.ready.add(key)
        session.advance(1)
        backend.complete(key)
        result = session.advance(1)
        self.assertFalse(result.deliveries)
        self.assertEqual(engine.capacity.usage().active_requests, 0)
        self.assertEqual(engine.capacity.usage().input_bytes, 4)
        self.assertIsNone(engine.error)
        preparation.releasable.add(key)
        result = session.advance(1)
        if not result.deliveries:
            result = session.advance(1)
        self.assertEqual(result.deliveries[0].result, b'done')
        self.assertEqual(engine.capacity.usage().input_bytes, 0)
        session.release([result.deliveries[0].lease_id])
        self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_one_action_tick_does_not_starve_a_ready_model_request(self):
        _, session, backend, preparation = prepared_setup(step_actions=1)
        session.offer([task(0), task(1)])
        session.advance(2)
        preparation.ready.update(preparation.held)
        session.advance(2)
        self.assertEqual(len(backend.pending), 1)

    def test_invalid_or_raised_preparation_acceptance_keeps_input_until_confirmed(self):
        for acceptance in ('raised', True, 5):
            with self.subTest(acceptance=acceptance):
                engine, session, backend, preparation = prepared_setup()
                def reject(tasks):
                    preparation.held.update(t.key for t in tasks)
                    if acceptance == 'raised':
                        raise RuntimeError('fixture uncertain preparation')
                    return acceptance
                preparation.try_prepare = reject
                session.offer([task(0)])
                session.advance(1)
                self.assertEqual(engine.capacity.usage().input_bytes, 4)
                self.assertFalse(backend.pending)
                preparation.releasable.update(preparation.held)
                session.advance(1)
                self.assertEqual(engine.capacity.usage().held_tasks, 0)

    def test_dispatch_gate_also_prevents_early_preparation(self):
        _, session, _, preparation = prepared_setup()
        session.set_dispatch_enabled(False)
        session.offer([task(0)])
        session.advance(1)
        self.assertFalse(preparation.held)
