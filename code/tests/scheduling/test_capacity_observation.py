"""Capacity refusal observation preserves the existing dispatch and wake decisions."""
from dataclasses import asdict
import unittest

from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import SessionSpec, TaskKey
from src.scheduling.core.session_jobs import JobBudget
from tests.scheduling.test_incremental_session import setup, task


class CapacityObservationTests(unittest.TestCase):
    def run_case(self, registered, observe):
        original, _, backend, clock=setup(active_requests=2,active_work=4)
        events=[]
        def record(kind,key):
            events.append(dict(event=kind,key=asdict(key),usage=asdict(engine.capacity.usage())))
        engine=SessionEngine(original.capacity.limits,backend,original.policies,clock=clock,sink=record,
                             observe_capacity_blocks=observe)
        if registered:
            job=engine.register_job('job',JobBudget(2,8,8,2,4))
            session=engine.open(SessionSpec(job.job_id,'flow','fixture'),job=job)
        else:
            session=engine.open(SessionSpec('job','flow','fixture'))
        def advance():
            if registered:engine.advance()
            result=session.advance(2)
            session.release([d.lease_id for d in result.deliveries])
        session.offer([task(0,estimated_work=3),task(1,estimated_work=3)])
        advance()
        first=list(backend.pending)
        self.assertEqual(first,[TaskKey(0,0)])
        self.assertEqual(engine.capacity.usage().active_work,3)
        blocks=[e for e in events if e['event'].endswith('capacity_blocked')]
        if observe:
            self.assertTrue(blocks)
            expected='flow_capacity_blocked' if registered else 'dispatch_capacity_blocked'
            self.assertEqual({e['event'] for e in blocks},{expected})
            self.assertTrue(all(e['key']['sequence']==1 and e['usage']['active_requests']==1 for e in blocks))
        else:self.assertFalse(blocks)
        backend.complete(first[0]);clock.now+=.2;advance()
        self.assertEqual(list(backend.pending),[TaskKey(0,1)])
        backend.complete(TaskKey(0,1));advance()
        session.request_cancel();session.close();engine.reap(8)
        self.assertEqual(engine.capacity.usage().held_tasks,0)
        return [(e['event'],e['key']) for e in events if not e['event'].endswith('capacity_blocked')]

    def test_legacy_and_registered_paths_observe_only_explicit_refusals(self):
        for registered in (False,True):
            with self.subTest(registered=registered):
                self.assertEqual(self.run_case(registered,False),self.run_case(registered,True))
