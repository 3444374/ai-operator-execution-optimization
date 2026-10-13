"""No models: finite Job knowledge, opportunity protection and remote ownership."""

from dataclasses import replace
import unittest

from src.scheduling.core.session_jobs import (
    ReadyJob, JobWorkView, JobSelectionHistory, round_robin_flow,
)
from src.scheduling.core.session_contract import Acceptance, TaskKey
from src.scheduling.submission_control.query_sharing import BoundedShortJobBurst, RemainingWorkPriority
from tests.scheduling.test_multisession import engine, budget, flow
from tests.scheduling.test_incremental_session import task


def view(name, remaining, dispatch=1, unit='work_units'):
    return ReadyJob(name, (int(name),), JobWorkView(unit, remaining, dispatch))


def policy():
    return BoundedShortJobBurst(work_unit='work_units', short_work_limit=4, bypass_work_limit=2)


class QuerySharingTests(unittest.TestCase):
    def test_only_sealed_small_job_gets_completion_preference(self):
        history = JobSelectionHistory(None, ())
        ready = (view('0', None), view('1', 4))
        self.assertEqual(policy()(ready, history).job_id, '1')
        self.assertEqual(policy()((view('0', None), view('1', None)), history),
                         round_robin_flow(ready, history))
        self.assertEqual(policy()((view('0', 8), view('1', 8)), history).job_id, '0')

    def test_waiting_peer_is_selected_after_finite_bypass_work(self):
        choose = policy()
        ready = (view('0', None), view('1', 4))
        history = JobSelectionHistory(None, ())
        choices = []
        for _ in range(3):
            selected = choose(ready, history)
            choices.append(selected.job_id)
            history = replace(history, last_job=selected.job_id)
        self.assertEqual(choices, ['1', '1', '0'])

    def test_large_grant_forces_peer_at_next_eligible_opportunity(self):
        choose = policy()
        ready = (view('0', None), view('1', 4, dispatch=4))
        first = choose(ready, JobSelectionHistory(None, ()))
        self.assertEqual(first.job_id, '1')
        self.assertEqual(choose(ready, JobSelectionHistory('1', ())).job_id, '0')

    def test_unknown_or_mixed_units_use_existing_round_robin(self):
        history = JobSelectionHistory('0', ())
        for ready in ((view('0', None), ReadyJob('1', (1,))),
                      (view('0', None), view('1', 1, unit='tokens'))):
            self.assertEqual(policy()(ready, history), round_robin_flow(ready, history))

    def test_departed_jobs_do_not_accumulate_policy_history(self):
        choose = policy()
        for i in range(50):
            choose((view(str(i), 1),), JobSelectionHistory(None, ()))
            self.assertEqual(len(choose._bypassed), 1)

    def test_remaining_priority_includes_known_jobs_above_candidate_threshold(self):
        ready = (view('0', 40), view('1', 20), view('2', None))
        history = JobSelectionHistory(None, ())
        self.assertEqual(RemainingWorkPriority(work_unit='work_units')(ready, history).job_id, '1')
        self.assertEqual(policy()(ready, history).job_id, '0')
        self.assertEqual(RemainingWorkPriority(work_unit='work_units')(
            ready, JobSelectionHistory('1', ())).job_id, '1')

    def test_remaining_ties_rotate_jobs_then_flows(self):
        choose = RemainingWorkPriority(work_unit='work_units')
        ready = (ReadyJob('0', (0, 2), JobWorkView('work_units', 8, 1)), view('1', 8))
        selected = choose(ready, JobSelectionHistory('1', (('0', 0),)))
        self.assertEqual((selected.job_id, selected.session_id), ('0', 2))

    def test_controls_share_compensation_even_when_attempts_do_not_succeed(self):
        ready = (view('0', None), view('1', 4, dispatch=3))
        for choose in (policy(), RemainingWorkPriority(work_unit='work_units', bypass_work_limit=2)):
            # Core history has not advanced: both submissions were rejected.
            history = JobSelectionHistory(None, ())
            self.assertEqual(choose(ready, history).job_id, '1')
            self.assertEqual(choose(ready, history).job_id, '0')
            self.assertEqual(choose(ready, history).job_id, '1')

    def test_all_unknown_or_incomparable_views_reset_compensation_to_rr(self):
        for choose in (policy(), RemainingWorkPriority(work_unit='work_units', bypass_work_limit=2)):
            known = (view('0', None), view('1', 4, dispatch=3))
            choose(known, JobSelectionHistory(None, ()))
            for ready in ((view('0', None), view('1', None)),
                          (view('0', 5), view('1', 1, unit='tokens')),
                          (view('0', 5), ReadyJob('1', (1,)))):
                history = JobSelectionHistory('0', ())
                self.assertEqual(choose(ready, history), round_robin_flow(ready, history))
                self.assertFalse(choose._bypassed)

    def test_remaining_compensation_discards_departed_identities(self):
        choose = RemainingWorkPriority(work_unit='work_units', bypass_work_limit=2)
        for i in range(50):
            choose((view(str(i), 1),), JobSelectionHistory(None, ()))
            self.assertEqual(tuple(choose._bypassed), (str(i),))

    def test_sealing_requires_finished_inputs_and_prevents_new_flows(self):
        core, _, _ = engine(requests=2)
        job = core.register_job('short', budget(requests=2))
        session = flow(core, job, 'map')
        session.offer((task(0), task(1)))
        with self.assertRaisesRegex(ValueError, 'finish all Job inputs'):
            core.seal_job(job)
        session.seal()
        core.seal_job(job)
        core.seal_job(job)
        with self.assertRaisesRegex(ValueError, 'sealed'):
            flow(core, job, 'new-map')
        self.assertEqual(session.offer((task(2),)).status, 'REJECTED')

    def test_work_view_counts_inflight_until_terminal_and_not_deliveries(self):
        core, backend, _ = engine(requests=2)
        core.observe_job_work = True
        job = core.register_job('short', budget(requests=2))
        session = flow(core, job, 'map')
        session.offer((task(0), task(1)))
        self.assertIsNone(core._job_work(job.job_id).remaining_work)
        session.seal()
        core.seal_job(job)
        core.advance(1)
        self.assertEqual(core._job_work(job.job_id).remaining_work, 2)
        self.assertEqual(core._job_work(job.job_id).active_work, 1)
        backend.complete(TaskKey(session.session_id, 0))
        core.advance(1)
        self.assertEqual(core._job_work(job.job_id).remaining_work, 1)

    def test_cancel_does_not_make_remote_capacity_available(self):
        core, backend, _ = engine(requests=1)
        core.observe_job_work = True
        core.choose_flow = policy()
        long = core.register_job('long', budget(requests=1))
        short = core.register_job('short', budget(requests=1))
        a, b = flow(core, long, 'a'), flow(core, short, 'b')
        a.offer((task(0),))
        core.advance()
        b.offer((task(0),))
        b.seal()
        core.seal_job(short)
        a.request_cancel()
        core.advance()
        self.assertEqual(core.capacity.usage().active_requests, 1)
        self.assertNotIn(TaskKey(b.session_id, 0), backend.pending)
        backend.complete(TaskKey(a.session_id, 0), status='cancelled')
        core.advance()
        self.assertIn(TaskKey(b.session_id, 0), backend.pending)

    def test_unknown_submission_keeps_compute_and_rejected_attempt_does_not(self):
        for acceptance, requests in ((Acceptance.UNKNOWN, 1), (Acceptance.NOT_ACCEPTED, 0)):
            with self.subTest(acceptance=acceptance):
                core, backend, _ = engine()
                core.observe_job_work = True
                core.choose_flow = policy()
                job = core.register_job('short', budget())
                session = flow(core, job, 'map')
                session.offer((task(0),))
                session.seal()
                core.seal_job(job)
                backend.acceptance = acceptance
                core.advance()
                self.assertEqual(core.capacity.usage().active_requests, requests)


if __name__ == '__main__':
    unittest.main()
