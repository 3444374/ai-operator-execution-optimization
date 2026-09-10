"""Owned worker termination, partial evidence and late POST rejection."""
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from src.experiments.attempt_ledger import AttemptBudget,BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.query_supervisor import supervise


class QuerySupervisorTests(unittest.TestCase):
    def test_hung_query_is_stopped_and_queued_posts_are_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            ledger=CellBudgetLedger.create(root/'budget.sqlite',AttemptBudget('timeout',2),deadline_utc=time.time()+30)
            ledger.reserve_unit('unit',2)
            shared=ledger.claim_shared_unit('unit')
            script="""import time,json,signal
from pathlib import Path
root=Path(__import__('sys').argv[1]);(root/'unit/q0').mkdir(parents=True)
(root/'unit/unit-reserved.json').write_text('{}')
(root/'unit/q0/started.json').write_text(json.dumps({'started_ns':time.monotonic_ns()}))
signal.signal(signal.SIGTERM,signal.SIG_IGN)
while True:time.sleep(.1)
"""
            with self.assertRaises(TimeoutError):
                supervise([sys.executable,'-c',script,str(root)],root,lambda:ledger.close_shared_unit('unit'),
                          query_timeout_s=.15,preparation_s=3,finish_s=3,grace_s=.1)
            report=json.loads((root/'supervisor.json').read_text())
            self.assertEqual(report['remaining_owned_pids'],[])
            self.assertEqual(report['status'],'failed')
            with self.assertRaises(BudgetExhausted):shared.reserve('a'*64)

    def test_normal_worker_exits_without_cancellation(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            result=supervise([sys.executable,'-c','pass'],root,lambda:self.fail('no unit to close'),query_timeout_s=1)
            self.assertEqual(result['status'],'passed')
            self.assertEqual(result['worker_exit'],0)

    def test_budget_close_failure_still_stops_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'unit').mkdir();(root/'unit/unit-reserved.json').write_text('{}')
            def broken():raise OSError('injected ledger error')
            with self.assertRaises(TimeoutError):
                supervise([sys.executable,'-c','import time;time.sleep(20)'],root,broken,
                          query_timeout_s=.1,preparation_s=.1,finish_s=.1,grace_s=.1)
            report=json.loads((root/'supervisor.json').read_text())
            self.assertEqual(report['remaining_owned_pids'],[])
            self.assertEqual(report['budget_close_error_type'],'OSError')

    def test_report_failure_does_not_replace_worker_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch('src.experiments.postgresql.query_supervisor.write_private_json',side_effect=OSError('disk full')):
                with self.assertRaisesRegex(RuntimeError,'worker failed') as caught:
                    supervise([sys.executable,'-c','raise SystemExit(3)'],Path(directory),lambda:None,query_timeout_s=1)
            self.assertIn('Supervisor report also failed: OSError',caught.exception.__notes__)
