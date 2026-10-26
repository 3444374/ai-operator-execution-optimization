"""Exact one-pass hints reuse existing cache identity/storage, not scan work."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from src.scheduling.runtime.calibration import run_admission_query
from tests.scheduling import test_online_performance_control as traces


class AdmissionReuseTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.identity={k:{'version':'CPU-v1'} for k in ('hardware','model','generation','execution','workload','measurement')}
        self.identity['measurement']['work_unit']='model_calls';self.calls=[]

    def execute(self,window,*,allow_exploration):
        self.calls.append((window,allow_exploration))
        case=traces.OnlinePerformanceTests();case.setUp()
        if allow_exploration:
            case.probe();case.tick(20);case.tick(20)
            # Complete the next trial at a plateau before claiming zero-explore
            # reuse; an unfinished frontier is deliberately resumable.
            for _ in range(4):case.tick(20)
        else:
            case.c.reset(window,100)
        return dict(status='completed',reuse_eligible=True,admission_feedback=case.c.report())

    def run_query(self,**kw):
        return run_admission_query(self.identity,self.execute,initial_window=4,maximum=16,cache_dir=self.temp.name,**kw)

    def test_first_then_two_reuses_apply_confirmed_window_without_reexploring(self):
        first=self.run_query();path=Path(first['saved_hint']['path']);original=path.read_bytes()
        second=self.run_query();third=self.run_query()
        self.assertEqual(self.calls,[(4,True),(6,False),(6,False)])
        self.assertFalse(first['cache_hit']);self.assertTrue(second['cache_hit']);self.assertTrue(third['cache_hit'])
        self.assertEqual(second['scan_cost'],dict(wall_s=0.,samples=0));self.assertEqual(path.read_bytes(),original)
        self.assertTrue(second['reused_hint_unmodified'])
        self.assertIn('not a fixed full-query',second['historical_evidence']['source'])

    def test_model_hardware_or_workload_change_does_not_match_old_hint(self):
        self.run_query()
        for key in ('model','hardware','workload'):
            old=deepcopy(self.identity);self.identity[key]['version']='changed-'+key
            self.assertFalse(self.run_query()['cache_hit']);self.identity=old

    def test_bad_optional_cache_does_not_repeat_or_prevent_business_execution(self):
        first=self.run_query();Path(first['saved_hint']['path']).write_text('{bad')
        before=len(self.calls);value=self.run_query()
        self.assertEqual(len(self.calls),before+1);self.assertFalse(value['cache_hit'])
        self.assertIsNotNone(value['lookup_error'])

    def test_cache_existence_error_executes_once_without_a_hint(self):
        with patch.object(Path,'exists',side_effect=PermissionError('CPU unreadable cache')):
            value=self.run_query()
        self.assertEqual(self.calls,[(4,True)])
        self.assertFalse(value['cache_hit'])
        self.assertEqual(value['lookup_error'],'PermissionError')

    def test_earlier_pressure_cannot_publish_a_later_unconfirmed_probe(self):
        case=traces.OnlinePerformanceTests();case.setUp()
        case.tick(kv=.99);case.tick(kv=.99)
        for _ in range(5):case.tick()
        feedback=case.c.report()
        self.assertEqual((feedback['window'],feedback['recommended_window'],feedback['unconfirmed_trial']),(4,2,True))
        def execute(window,**kw):
            return dict(status='completed',reuse_eligible=True,admission_feedback=feedback)
        value=run_admission_query(self.identity,execute,initial_window=4,maximum=16,cache_dir=self.temp.name)
        self.assertEqual(value['saved_hint']['window'],2)
        self.run_query()
        self.assertEqual(self.calls,[(2,True)])

    def test_pressure_only_reduction_can_replace_an_existing_hint(self):
        self.run_query()
        def execute(window,**kw):
            case=traces.OnlinePerformanceTests();case.setUp()
            case.c.reset(window,10000)
            case.tick(kv=.99);case.tick(kv=.99)
            return dict(status='completed',reuse_eligible=True,admission_feedback=case.c.report())
        value=run_admission_query(self.identity,execute,initial_window=4,maximum=16,cache_dir=self.temp.name)
        self.assertTrue(value['cache_hit'])
        self.assertEqual(value['saved_hint']['window'],3)

    def test_failed_or_ineligible_query_does_not_publish_a_hint(self):
        def failed(window,**kw):return dict(status='failed',reuse_eligible=False)
        value=run_admission_query(self.identity,failed,initial_window=4,maximum=16,cache_dir=self.temp.name)
        self.assertNotIn('saved_hint',value);self.assertEqual(list(Path(self.temp.name).iterdir()),[])
        calls=[]
        def raises(window,**kw):
            calls.append(window);raise RuntimeError('CPU failure')
        with self.assertRaises(RuntimeError):run_admission_query(self.identity,raises,initial_window=4,maximum=16,cache_dir=self.temp.name)
        self.assertEqual(calls,[4])

    def test_caller_wait_is_not_added_again_to_total(self):
        now=[10.]
        def execute(window,**kw):
            now[0]+=4;return dict(status='completed',reuse_eligible=False,admission_feedback=dict(exploration_wall_s=2))
        r=run_admission_query(self.identity,execute,initial_window=4,maximum=16,cache_dir=self.temp.name,
            clock=lambda:now[0],request_started_monotonic=7.)
        self.assertEqual((r['total_wall_s'],r['request_wait_s'],r['online_exploration_s']),(4,7,2))
