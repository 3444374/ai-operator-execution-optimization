"""Shared compute ceilings never lend Job storage or settle unknown work."""
from dataclasses import replace
import unittest

from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import SessionSpec, TaskKey, Uncertain
from src.scheduling.core.session_jobs import shared_compute_job_budget, equal_share_job_budget
from tests.scheduling.test_incremental_session import setup, limits, Backend, Clock, task


def build(jobs=4, capacity=4):
    previous, old, _, _ = setup()
    old.close()
    backend, clock = Backend(), Clock()
    core = SessionEngine(limits(held_tasks=jobs*capacity, input_bytes=jobs*capacity*4,
        result_bytes=jobs*capacity*4, item_input_bytes=4, item_result_bytes=4,
        active_requests=capacity, active_work=capacity),
        backend, previous.policies, clock=clock, max_jobs=jobs)
    return core, backend, clock


def join(core, name, policy=shared_compute_job_budget):
    job = core.register_job(name, policy(core))
    return job, core.open(SessionSpec('ignored',name,'fixture'),job=job)


class SharedComputeTests(unittest.TestCase):
    def test_idle_registered_jobs_do_not_strand_compute(self):
        for count in (2,4):
            with self.subTest(jobs=count):
                core, backend, _ = build(count)
                jobs=[join(core,str(i)) for i in range(count)]
                active=jobs[0][1]
                active.offer(tuple(task(i) for i in range(4)))
                core.advance()
                self.assertEqual(len(backend.pending),4)
                self.assertEqual(core.capacity.usage().active_requests,4)
                self.assertEqual(core.capacity.usage(job_id=jobs[-1][0].job_id).held_tasks,0)

    def test_original_equal_share_policy_remains_fixed(self):
        core, backend, _ = build()
        _, stream=join(core,'A',equal_share_job_budget)
        stream.offer(tuple(task(i) for i in range(4)))
        core.advance()
        self.assertEqual(len(backend.pending),1)

    def test_competing_jobs_rotate_and_resume_at_next_release(self):
        core, backend, _ = build()
        jobs=[join(core,str(i)) for i in range(4)]
        for _, s in jobs:s.offer((task(0),task(1)))
        core.advance()
        self.assertEqual(set(backend.pending),{TaskKey(s.session_id,0) for _,s in jobs})
        # All four slots are occupied; an arriving next item cannot create a fifth.
        core.advance();self.assertEqual(len(backend.pending),4)
        backend.complete(TaskKey(jobs[0][1].session_id,0))
        core.advance()
        self.assertEqual(len(backend.pending),4)
        self.assertIn(TaskKey(jobs[0][1].session_id,1),backend.pending)

    def test_slow_consumer_retains_only_its_storage(self):
        core, backend, _ = build(2,2)
        a,sa=join(core,'A');b,sb=join(core,'B')
        sa.offer((task(0),task(1)));core.advance()
        for i in range(2):backend.complete(TaskKey(sa.session_id,i))
        core.advance()
        self.assertEqual(core.capacity.usage(job_id=a.job_id).held_tasks,2)
        self.assertEqual(core.capacity.usage().active_requests,0)
        sb.offer((task(0),task(1)));core.advance()
        self.assertEqual(len(backend.pending),2)
        self.assertEqual(sa.offer((task(2),)).status,'BACKPRESSURE')
        self.assertEqual(core.capacity.usage(job_id=b.job_id).held_tasks,2)

    def test_unknown_compute_is_not_reborrowed_after_cancel(self):
        core,backend,_=build(2,2)
        a,sa=join(core,'A');_,sb=join(core,'B')
        sa.offer((task(0),));core.advance()
        key=TaskKey(sa.session_id,0)
        # The backend reports uncertainty, which is not a terminal receipt.
        backend.events.append(Uncertain(key,backend.pending[key][0]))
        core.advance();core.close_job(a)
        sb.offer((task(0),task(1)));core.advance()
        self.assertEqual(core.capacity.usage().active_requests,2)
        self.assertTrue(core.capacity.records[key].compute)

    def test_shared_work_limit_binds_before_request_count_and_recovers(self):
        core, backend, _ = build(2, 4)
        _, a = join(core, 'A')
        _, b = join(core, 'B')
        a.offer((task(0, estimated_work=3),))
        b.offer((task(0, estimated_work=3),))
        core.advance()
        self.assertEqual(core.capacity.usage().active_requests, 1)
        self.assertEqual(core.capacity.usage().active_work, 3)
        backend.complete(TaskKey(a.session_id, 0))
        core.advance()
        self.assertIn(TaskKey(b.session_id, 0), backend.pending)
        self.assertEqual(core.capacity.usage().active_work, 3)

    def test_retired_jobs_free_membership_for_more_than_maximum_over_time(self):
        core,backend,_=build(2,2)
        identities=[]
        for i in range(7):
            job,s=join(core,str(i));identities.append(job.job_id)
            s.offer((task(0),));core.advance();backend.complete(TaskKey(s.session_id,0));core.advance()
            delivery=s.advance(1).deliveries[0];s.release((delivery.lease_id,));s.close_consumer();core.close_job(job);core.advance()
            self.assertEqual(core.capacity.usage().held_tasks,0)
            self.assertEqual(len(core.jobs.jobs),0)
        self.assertEqual(len(set(identities)),7)

    def test_storage_must_support_full_declared_request_capacity(self):
        core,_,_=build(4,4)
        for field in ('held_tasks','input_bytes','result_bytes'):
            original=core.capacity.limits
            core.capacity.limits=replace(original,**{field:getattr(original,field)//2})
            with self.subTest(field=field),self.assertRaises(ValueError):shared_compute_job_budget(core)
            core.capacity.limits=original

    def test_flow_partition_does_not_multiply_job_compute_limit(self):
        core,backend,_=build(2,2)
        job=core.register_job('dependent',replace(shared_compute_job_budget(core),max_sessions=2))
        a=core.open(SessionSpec('ignored','filter','fixture'),job=job)
        b=core.open(SessionSpec('ignored','map','fixture'),job=job)
        a.offer((task(0),));b.offer((task(0),));core.advance()
        self.assertEqual(core.capacity.usage().active_requests,2)
        self.assertEqual(core.capacity.job_limits[job.job_id].active_requests,2)
