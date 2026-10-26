"""Existing pure CPU return-clock regressions, independent of Arrow/Ray."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from src.experiments.native_query_lifecycle import run_sequence
from src.experiments import movie_records_session as session
from tests.experiments import test_movie_records_session as lifecycle_fixture

class ComparableLifecycleTimingTests(unittest.TestCase):
    def fixture(self):
        case=lifecycle_fixture.LifecycleTests();case.setUp();self.addCleanup(case.doCleanups)
        return case

    def test_resident_startup_is_once_and_driver_return_is_after_evaluation(self):
        case=self.fixture();report=case.run_session('resident');t=report['lifecycle_timing']
        self.assertEqual([q['full_query_seconds'] for q in report['queries']],[36]*3)
        self.assertEqual([q['query_api_return_seconds'] for q in report['queries']],[53]*3)
        self.assertEqual(t['owner_startup_seconds_sum'],100)
        self.assertEqual(t['query_api_return_seconds_sum'],159)
        self.assertEqual(t['owner_shutdown_seconds_sum'],29)
        self.assertEqual(t['owner_startup_already_in_query_api_seconds'],0)
        self.assertEqual(t['owners'][0]['cold_first_api_return_seconds'],153)
        self.assertEqual(t['owners'][0]['cold_first_result_seconds'],136)
        self.assertIsNone(report['model_load_count_observed'])

    def test_isolated_startup_is_already_inside_each_driver_return(self):
        case=self.fixture();report=case.run_session('isolated');t=report['lifecycle_timing']
        self.assertEqual([q['full_query_seconds'] for q in report['queries']],[136]*3)
        self.assertEqual([q['query_api_return_seconds'] for q in report['queries']],[153]*3)
        self.assertEqual(t['owner_startup_seconds_sum'],300)
        self.assertEqual(t['query_api_return_seconds_sum'],459)
        self.assertEqual(t['owner_startup_already_in_query_api_seconds'],300)
        self.assertEqual(t['owner_shutdown_seconds_sum'],87)
        self.assertEqual(report['application_e2e_s'],546)
        self.assertEqual(len(t['owners']),3)

    def test_checkpoint_and_final_owner_close_are_not_model_or_query_api_time(self):
        case=self.fixture()
        report=run_sequence(case.queries,case.factory,case.output,backend='semloom-core',
            lifecycle='resident',clock=case.clock,checkpoint=lambda event:case.clock.advance(41))
        self.assertEqual([q['query_api_return_seconds'] for q in report['queries']],[53]*3)
        self.assertEqual(report['application_e2e_s'],411)
        self.assertEqual(report['lifecycle_timing']['owner_shutdown_seconds_sum'],29)

    def test_failed_call_has_no_invented_normal_return_or_zero_cost(self):
        case=self.fixture();case.query_error=ValueError('CPU query failure')
        with self.assertRaises(ValueError) as caught:case.run_session('resident')
        report=caught.exception.session_report
        self.assertIsNone(report['queries'][1]['query_api_return_ns'])
        self.assertIsNone(report['queries'][1]['query_api_return_seconds'])
        self.assertIsNone(report['lifecycle_timing']['query_api_return_seconds_sum'])
        self.assertEqual(report['lifecycle_timing']['query_api_timing_count'],1)
        self.assertEqual(report['skipped_queries'],['q2'])

    def test_partial_start_keeps_unknown_start_duration_and_cleanup_evidence(self):
        case=self.fixture();case.start_error=RuntimeError('CPU start failure')
        with self.assertRaises(RuntimeError) as caught:case.run_session('resident')
        report=caught.exception.session_report
        self.assertIsNone(report['lifecycle_timing']['owner_startup_seconds_sum'])
        self.assertIsNone(report['lifecycle_timing']['owners'][0]['cold_first_api_return_seconds'])
        self.assertEqual(case.drivers[0].closes,1)

    def test_runtime_only_readiness_does_not_claim_model_load_or_residency(self):
        case=self.fixture();original=case.factory
        class RuntimeOnly(original):
            def start(self):
                super().start()
                return dict(no_session_model=True,model_startup_scope='inside existing per-query adapter')
        report=run_sequence(case.queries,RuntimeOnly,case.output,backend='ray-data',
            lifecycle='isolated',clock=case.clock)
        owners=report['lifecycle_timing']['owners']
        self.assertTrue(all(o['owner_startup_scope']=='driver_runtime_only' for o in owners))
        self.assertTrue(all(o['model_initialization_scope']=='inside existing per-query adapter' for o in owners))
        self.assertIsNone(report['model_load_count_observed'])

    def test_resident_rejection_is_about_this_adapter_not_a_vendor_capability_claim(self):
        for backend in ('ray-data','daft'):
            case=self.fixture()
            with self.subTest(backend=backend),self.assertRaisesRegex(ValueError,'this adapter'):
                run_sequence(case.queries,case.factory,case.output,backend=backend,
                    lifecycle='resident',clock=case.clock)
            self.assertEqual(case.drivers,[])

    def test_existing_native_constructor_receipt_is_separate_from_runtime_start(self):
        case=self.fixture();original=case.factory
        class NativeReceipts(original):
            def start(self):
                super().start()
                return dict(no_session_model=True,model_startup_scope='inside native graph')
            def query(self,spec,emit):
                result=super().query(spec,emit)
                result['backend_close']=dict(engine_reports=[dict(
                    engine_ready=dict(engine_uid='CPU-'+spec['query_id']),startup_seconds=11)])
                return result
        report=run_sequence(case.queries,NativeReceipts,case.output,backend='daft',
            lifecycle='isolated',clock=case.clock)
        starts=report['lifecycle_timing']['native_query_model_startup']
        self.assertEqual([q['reports'][0]['startup_seconds'] for q in starts],[11]*3)
        self.assertEqual(len({q['reports'][0]['engine_uid'] for q in starts}),3)
        self.assertTrue(all(o['owner_startup_scope']=='driver_runtime_only'
                            for o in report['lifecycle_timing']['owners']))
        self.assertEqual(report['lifecycle_timing']['query_api_return_seconds_sum'],459)

    def test_missing_native_model_duration_is_not_backfilled_from_query_time(self):
        case=self.fixture();report=case.run_session()
        starts=report['lifecycle_timing']['native_query_model_startup']
        self.assertTrue(all(q['reports']==[] and q['missing_reason'] for q in starts))

    def test_generic_clock_keeps_existing_imlane_result_timing_semantics(self):
        case=self.fixture()
        report=run_sequence(case.queries,case.factory,case.output,backend='imlane',
            lifecycle='resident',clock=case.clock)
        self.assertEqual(report['resident_timing']['cold_first_result_seconds'],136)
        self.assertEqual(report['lifecycle_timing']['owners'][0]['cold_first_result_seconds'],136)
        self.assertEqual(report['lifecycle_timing']['owners'][0]['cold_first_api_return_seconds'],153)

    def test_invalid_backend_report_is_not_masked_by_optional_lifetime_metrics(self):
        case=self.fixture();original=case.factory
        class BadResult(original):
            def query(self,spec,emit):return None
        with self.assertRaises(AttributeError) as caught:
            run_sequence(case.queries,BadResult,case.output,backend='semloom-core',
                lifecycle='resident',clock=case.clock)
        self.assertEqual(caught.exception.session_report['status'],'failed')
        self.assertEqual(case.drivers[0].closes,1)
