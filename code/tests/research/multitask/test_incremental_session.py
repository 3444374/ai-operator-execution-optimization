"""Optional local-coordinator integration; ordinary Session tests use no research policy."""

import unittest
from unittest.mock import patch

from src.scheduling.core.session_contract import Acceptance, SessionSpec, TaskKey
from tests.scheduling.test_incremental_session import setup, task


class SharedCreditSessionTests(unittest.TestCase):
    def test_existing_local_credit_waits_for_remote_terminal_before_finish(self):
        from src.research.multitask.shared_credit import FairEndpointCreditCoordinator

        e, s, b, _ = setup()
        credit = FairEndpointCreditCoordinator(
            {"e1": (1, 2), "e2": (1, 2)}, quantum=1, policy="fifo"
        )
        e.credit = credit
        s.offer([task(0)])
        s.advance(1)
        s.close()
        self.assertEqual(credit.snapshot("e1").active_requests, 1)
        self.assertIn("job", e._retired_jobs)
        b.complete(TaskKey(0, 0), status="cancelled")
        self.assertEqual(e.reap(1).usage.active_requests, 0)
        self.assertEqual(credit.snapshot("e1").active_requests, 0)
        self.assertNotIn("job", e._retired_jobs)


    def test_credit_retirement_errors_keep_finally_cleanup(self):
        from src.scheduling.core.session_jobs import equal_share_job_budget
        from src.research.multitask.shared_credit import FairEndpointCreditCoordinator

        for registered in (False, True):
            for failing in ("finish", "forget"):
                with self.subTest(registered=registered, failing=failing):
                    e, s, b, _ = setup()
                    if registered:
                        s.close()
                        job = e.register_job("registered", equal_share_job_budget(e))
                        s = e.open(SessionSpec("ignored", "flow", "fixture"), job=job)
                    credit = FairEndpointCreditCoordinator(
                        {"e1": (1, 2), "e2": (1, 2)}, quantum=1, policy="fifo"
                    )
                    e.credit = credit
                    s.offer([task(0)])
                    (e.advance if registered else s.advance)(1)
                    calls = []
                    original_finish = credit.finish_job
                    original_forget = credit.forget_finished_job

                    def finish(job_id):
                        calls.append(("finish", job_id, e.capacity.usage(job_id=job_id).held_tasks))
                        if failing == "finish":
                            raise RuntimeError("fixture finish failure")
                        return original_finish(job_id)

                    def forget(job_id):
                        calls.append(("forget", job_id, e.capacity.usage(job_id=job_id).held_tasks))
                        if failing == "forget":
                            raise RuntimeError("fixture forget failure")
                        return original_forget(job_id)

                    with patch.object(credit, "finish_job", side_effect=finish), patch.object(
                        credit, "forget_finished_job", side_effect=forget
                    ):
                        if registered:
                            e.close_job(job)
                        s.close()
                        self.assertEqual(calls, [])
                        b.complete(TaskKey(s.session_id, 0), status="cancelled")
                        e.reap(1)
                        expected = [("finish", s.spec.job_id, 0)]
                        if failing == "forget":
                            expected.append(("forget", s.spec.job_id, 0))
                        self.assertEqual(calls, expected)
                        self.assertEqual(e.error, "credit finish failed")
                        self.assertEqual(e.capacity.usage().held_tasks, 0)
                        self.assertEqual(credit.snapshot("e1").active_requests, 0)
                        if registered:
                            with self.assertRaises(ValueError):
                                e.jobs.require(job)
                        e.reap(1)
                        self.assertEqual(calls, expected)


    def test_not_accepted_returns_existing_credit_and_preserves_task(self):
        from src.research.multitask.shared_credit import FairEndpointCreditCoordinator

        e, s, b, _ = setup()
        e.credit = FairEndpointCreditCoordinator(
            {"e1": (1, 2), "e2": (1, 2)}, quantum=1, policy="fifo"
        )
        b.acceptance = Acceptance.NOT_ACCEPTED
        s.offer([task(0)])
        s.advance(1)
        self.assertEqual(e.credit.snapshot("e1").active_requests, 0)
        self.assertEqual(e.capacity.usage().held_tasks, 1)
        self.assertEqual(s.close().status, "CLOSED")


    def test_retired_local_fifo_credit_does_not_accumulate_job_history(self):
        from src.research.multitask.shared_credit import FairEndpointCreditCoordinator

        e, s, b, _ = setup()
        e.credit = FairEndpointCreditCoordinator(
            {"e1": (1, 2), "e2": (1, 2)}, quantum=1, policy="fifo"
        )
        for i in range(100):
            if i:
                s = e.open(SessionSpec(f"job-{i}", "flow", "fixture"))
            s.offer([task(0)])
            s.advance(1)
            b.complete(TaskKey(i, 0))
            d = s.advance(1).deliveries[0]
            s.release([d.lease_id])
            s.close()
            self.assertFalse(e.credit._finished_jobs)
            self.assertFalse(e.credit._weights)
            self.assertTrue(all(not jobs for jobs in e.credit._granted_requests.values()))


    def test_retirement_cannot_erase_live_credit_or_legacy_history(self):
        from src.research.multitask.shared_credit import FairEndpointCreditCoordinator

        credit = FairEndpointCreditCoordinator({"e1": (1, 2)}, quantum=1, policy="fifo")
        credit.try_acquire(request_id="r", job_id="j", endpoint_id="e1", estimated_work=1)
        with self.assertRaises(ValueError):
            credit.forget_finished_job("j")
        credit.release("r", job_id="j")
        credit.finish_job("j")
        self.assertIn("j", credit._finished_jobs)
        credit.forget_finished_job("j")
        self.assertNotIn("j", credit._finished_jobs)
