"""Original SDK/operator paths over real local CPU IPC; no inference or network."""
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
import csv
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import litellm
import lotus
import pandas as pd

from src.baselines.text.frameworks import lotus_native_local as native
from src.infrastructure.request_budget import ClaimedUnit, Reservation
from src.semantic_methods.lotus.engine import make_lm, observe_filter_rows
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources, identity
from src.serving.backends.async_engine_process import AsyncEngineProcess, SubmitResult, TerminalEvent
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec, _prepared, result_record


class FixtureProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        return [sys.executable, str(Path(__file__).with_name('lotus_native_ipc_fixture.py')), *map(str, descriptors)]


class FixtureCase(unittest.TestCase):
    def setUp(self):
        self.engine = EngineSpec('/tmp/CPU-native-fixture', sys.executable, '/tmp/driver', max_model_calls=20)
        self.generation = GenerationSpec(seed=0)
        self.resources = RecordsResources(query_timeout_s=2, startup_timeout_s=10)
        self.method = FilterMethod('openai/local-fixture', 'Keep {reviewText}')
        self.owners = []
        scope = patch.object(EngineSpec, 'validate_driver_environment', return_value={})
        scope.start(); self.addCleanup(scope.stop)
        scope = patch.object(native, 'AsyncEngineProcess', FixtureProcess)
        scope.start(); self.addCleanup(scope.stop)
        self.addCleanup(self.close_owners)

    def close_owners(self):
        for owner in self.owners:
            owner.close(timeout=2)

    def owner(self, concurrency=4, **options):
        owner = native.LocalNativeModel(self.engine, self.generation, self.resources, concurrency, **options)
        self.owners.append(owner)
        return owner


class NativeLocalProviderTests(FixtureCase):

    def execute(self, texts, owner=None, query='q'):
        from lotus.models import LM
        import lotus.models.lm as lm_module
        owner = owner or self.owner()
        lm = make_lm(self.method, self.generation)
        lm.max_batch_size = owner.concurrency
        expected = [identity(query, str(i), i) for i in range(len(texts))]
        budget = ClaimedUnit(Reservation(query, 1, len(texts)), None)
        frame = pd.DataFrame(dict(reviewText=texts, _execution_row_id=[str(i) for i in range(len(texts))]))
        old = LM.__call__, LM._process_uncached_messages, lm_module.batch_completion
        decisions, prompts = [], []
        with owner.query(lm, expected, budget, query_id=query) as binding:
            with lotus.settings.context(lm=lm, enable_cache=False):
                with observe_filter_rows(decisions.extend,
                        lambda rows:(prompts.extend(rows),binding.capture_prompts(rows))):
                    result = frame.sem_filter(self.method.instruction)
            report = binding.report()
            self.assertIs(LM.__call__, old[0])
            self.assertIs(LM._process_uncached_messages, old[1])
        self.assertIs(lm_module.batch_completion, old[2])
        return result, decisions, prompts, report, lm, budget, owner

    def test_original_filter_and_LM_return_the_original_order_with_no_HTTP(self):
        original = socket.socket.connect
        def connect(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                self.fail('native baseline must not open a TCP connection')
            return original(sock, address)
        with patch.object(socket.socket, 'connect', connect):
            result, decisions, prompts, report, lm, budget, owner = self.execute(['slow good', 'bad', 'good', 'bad'])
        self.assertEqual(list(result['_execution_row_id']), ['0','2'])
        self.assertEqual(decisions, [('0',True),('1',False),('2',True),('3',False)])
        self.assertEqual(report['calls']['counts']['succeeded'], 4)
        self.assertEqual(report['calls']['usage']['total_tokens'], 52)
        self.assertEqual(budget.attempts, 4)
        self.assertEqual(lm.model, self.method.model_name)
        self.assertEqual(lm.stats.physical_usage.total_tokens, 52)
        self.assertEqual(owner.ready_report['protocol'], 'local_async_model_IPC')
        self.assertEqual(owner.transport.snapshot()['held_requests'], 0)
        by_row={r['row_id']:r for r in report['calls']['records']}
        self.assertEqual([by_row[str(i)]['messages_sha256'] for i in range(4)],
            [hashlib.sha256(json.dumps(p['messages'],ensure_ascii=False,sort_keys=True,
                separators=(',',':'),allow_nan=False).encode()).hexdigest()
             for p in prompts])
        for row in report['native_transport_receipts']:
            self.assertEqual(row['fixture_generation']['max_tokens'], 8)
            self.assertEqual(row['fixture_generation']['seed'], 0)

    def test_duplicate_prompts_bind_by_native_response_order_not_completion_order(self):
        result, decisions, prompts, report, *_ = self.execute(['duplicate','slow good','duplicate','duplicate'])
        by_row={r['row_id']:r for r in report['calls']['records']}
        self.assertEqual(len(by_row), 4)
        self.assertEqual(len({r['engine_request_id'] for r in by_row.values()}), 4)
        self.assertEqual([row_id for row_id,keep in decisions if keep], list(result['_execution_row_id']))
        for row_id, keep in decisions:
            self.assertEqual('True' in by_row[row_id]['text'], keep)
            self.assertEqual(by_row[row_id]['source_binding'], 'original LOTUS batch_completion ordered return')

    def test_native_concurrency_is_retained_and_model_owner_can_be_reused(self):
        owner=self.owner(concurrency=2)
        *_, first=self.execute(['good','bad'], owner=owner, query='first')
        uid=owner.ready_report['engine_uid']
        result, _, _, report, _, _, _=self.execute(['bad','good'], owner=owner, query='second')
        self.assertEqual(owner.ready_report['engine_uid'], uid)
        self.assertEqual(owner.limits.max_inflight, 2)
        self.assertEqual(owner.ready_report['transport_limits']['max_inflight'],2)
        self.assertEqual(list(result['_execution_row_id']), ['1'])
        self.assertEqual(report['calls']['counts']['succeeded'], 2)
        closed=owner.close(timeout=2)
        self.assertEqual(closed['status'],'closed')
        self.assertIsNone(closed['native_final_accounting'])
        self.assertIsNone(owner._last_native_binding)

    def test_registration_and_kwargs_restore_after_an_operator_exception(self):
        owner=self.owner();lm=make_lm(self.method,self.generation);kwargs=dict(lm.kwargs)
        maps=list(litellm.custom_provider_map);providers=list(litellm.provider_list);custom=list(litellm._custom_providers)
        original=RuntimeError('original operator exception')
        with self.assertRaises(RuntimeError) as caught:
            with owner.query(lm,[identity('q','0',0)],ClaimedUnit(Reservation('q',1,1),None),query_id='q'):
                raise original
        self.assertIs(caught.exception, original)
        self.assertEqual(lm.kwargs, kwargs)
        self.assertEqual(litellm.custom_provider_map, maps)
        self.assertEqual(litellm.provider_list, providers)
        self.assertEqual(litellm._custom_providers, custom)
        with self.assertRaises(RuntimeError):owner.start()

    def test_unknown_receipt_does_not_become_a_native_success_or_release_ownership(self):
        owner=self.owner();lm=make_lm(self.method,self.generation)
        expected=[identity('q','0',0)]
        budget=ClaimedUnit(Reservation('q',1,1),None)
        with owner.query(lm,expected,budget,query_id='q') as binding:
            binding.capture_prompts([dict(row_id='0',messages=[dict(role='user',content='unknown')])])
            with lotus.settings.context(lm=lm,enable_cache=False),self.assertRaises(Exception):
                lm([[dict(role='user',content='unknown')]],show_progress_bar=False)
            report=binding.report()
        self.assertEqual(report['calls']['counts']['succeeded'], 0)
        self.assertEqual(report['calls']['counts']['unknown'], 1)
        self.assertEqual(report['native_transport_receipts'][0]['prompt_tokens'], 11)
        self.assertEqual(owner.transport.snapshot()['unknown_requests'], 1)
        self.assertEqual(budget.attempts,1)
        with self.assertRaises(RuntimeError):owner.start()
        close=owner.close(timeout=2)
        self.assertEqual(close['retired_unknown_requests'],1)

    def test_empty_native_query_does_not_start_a_model(self):
        owner=self.owner();lm=make_lm(self.method,self.generation)
        # There are no SDK model calls and no serving allocation for an empty relation.
        with patch.object(native,'AsyncEngineProcess',side_effect=AssertionError('no empty model')):
            with owner.query(lm,[],object(),query_id='empty') as binding:
                self.assertEqual(binding.report()['calls']['counts']['succeeded'],0)
        self.assertIsNone(owner.transport)
        self.assertEqual(owner.close()['status'],'closed')

    def test_native_workers_retry_only_backpressure_without_recounting_attempts(self):
        limits=replace(native.AsyncEngineLimits(),max_inflight=1,result_slots=1,command_slots=1)
        owner=self.owner(4,limits=limits)
        result,_,_,report,_,budget,_=self.execute(['slow good','bad','good','bad'],owner)
        self.assertEqual(list(result['_execution_row_id']),['0','2'])
        self.assertEqual(budget.attempts,4)
        self.assertEqual(report['calls']['counts']['succeeded'],4)
        self.assertEqual(len(report['native_transport_receipts']),4)
        self.assertEqual(owner.transport.snapshot()['held_requests'],0)

    def test_explicit_input_byte_limit_rejects_without_false_success_or_usage(self):
        limits=replace(native.AsyncEngineLimits(),item_input_bytes=1024)
        owner=self.owner(limits=limits)
        lm=make_lm(self.method,self.generation)
        budget=ClaimedUnit(Reservation('bytes',1,1),None)
        with owner.query(lm,[identity('bytes','0',0)],budget,query_id='bytes') as binding:
            with self.assertRaises(Exception):
                lm([[dict(role='user',content='x'*2048)]],show_progress_bar=False)
            report=binding.report()
        self.assertEqual(budget.attempts,1)
        self.assertEqual(report['calls']['counts']['unknown'],1)
        self.assertEqual(report['native_transport_receipts'][0]['status'],'failed')
        self.assertEqual(report['native_transport_receipts'][0]['total_tokens'],0)
        self.assertFalse(report['native_transport_receipts'][0]['submission_confirmed'])
        self.assertEqual(owner.transport.snapshot()['held_requests'],0)

    def test_model_failure_usage_is_retained_when_native_batch_raises(self):
        owner=self.owner();lm=make_lm(self.method,self.generation)
        with owner.query(lm,[identity('failure','0',0)],
                ClaimedUnit(Reservation('failure',1,1),None),query_id='failure') as binding:
            with self.assertRaises(Exception):
                lm([[dict(role='user',content='fail-model')]],show_progress_bar=False)
            report=binding.report()
        self.assertEqual(report['calls']['counts']['succeeded'],0)
        self.assertEqual(report['native_transport_receipts'][0]['status'],'failed')
        self.assertEqual(report['native_transport_receipts'][0]['total_tokens'],13)
        self.assertEqual(owner.transport.snapshot()['held_requests'],0)

    def test_protocol_failure_is_accounting_evidence_without_a_native_answer(self):
        owner=self.owner();lm=make_lm(self.method,self.generation)
        with owner.query(lm,[identity('protocol','0',0)],
                ClaimedUnit(Reservation('protocol',1,1),None),query_id='protocol') as binding:
            with self.assertRaises(Exception):
                lm([[dict(role='user',content='protocol-failure good')]],show_progress_bar=False)
            report=binding.report()
        self.assertEqual(report['calls']['counts']['succeeded'],0)
        self.assertEqual(report['native_transport_receipts'][0]['total_tokens'],13)
        self.assertEqual(report['native_transport_receipts'][0]['engine_protocol_error'],'CPUProtocolFailure')

    def test_close_aborts_active_native_call_and_retains_unreturned_known_usage(self):
        owner=self.owner();lm=make_lm(self.method,self.generation)
        with owner.query(lm,[identity('cancel','0',0)],
                ClaimedUnit(Reservation('cancel',1,1),None),query_id='cancel') as binding:
            with tempfile.TemporaryDirectory() as directory,ThreadPoolExecutor(max_workers=1) as executor:
                marker=Path(directory)/'fixture-started'
                future=executor.submit(lm,[[dict(role='user',content='hold good\nfixture_start_path='+str(marker))]],
                    show_progress_bar=False)
                deadline=time.monotonic()+2
                while not marker.exists() and time.monotonic()<deadline:
                    owner.transport.wait(.01)
                self.assertTrue(marker.exists(),'fixture must be actively generating before cancellation')
                self.assertEqual(owner.transport.snapshot()['held_requests'],1)
                close=owner.close(timeout=2)
                with self.assertRaises(Exception):future.result(timeout=2)
            report=binding.report()
        self.assertEqual(close['status'],'closed')
        self.assertEqual(report['calls']['counts']['succeeded'],0)
        self.assertEqual(report['calls']['counts']['unknown'],1)
        retained=report['native_transport_receipts']+report['late_native_receipts']
        self.assertTrue(any(r['total_tokens']==13 for r in retained))
        self.assertEqual(owner.close(timeout=.1)['status'],'closed')

    def test_startup_failure_retains_the_existing_serving_owner_for_cleanup(self):
        class Retained:
            closes=0
            def close(self,timeout):
                self.closes+=1
                return dict(status='unknown' if self.closes==1 else 'closed')
        retained=Retained();error=RuntimeError('startup failed');error.async_engine_process=retained
        owner=self.owner()
        with patch.object(native,'AsyncEngineProcess',side_effect=error),self.assertRaises(RuntimeError) as caught:
            owner.start()
        self.assertIs(caught.exception,error)
        self.assertIs(owner.transport,retained)
        self.assertEqual(owner.close()['status'],'unknown')
        self.assertEqual(owner.close()['status'],'closed')

    def test_invalid_occurrence_ids_fail_before_model_start(self):
        owner=self.owner();lm=make_lm(self.method,self.generation)
        with patch.object(native,'AsyncEngineProcess',side_effect=AssertionError('invalid input allocated')):
            with self.assertRaises(ValueError):
                with owner.query(lm,[identity('q','0',0),identity('q','0',1)],object(),query_id='q'):
                    self.fail('duplicate IDs accepted')
        self.assertIsNone(owner.transport)


@unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT'),
                     'select an existing pinned SemBench checkout for official query/evaluator CPU tests')
class OriginalMovieQueryTests(FixtureCase):
    @classmethod
    def setUpClass(cls):
        missing=[name for name in ('overrides','cdlib','duckdb','scipy') if importlib.util.find_spec(name) is None]
        if missing:
            raise unittest.SkipTest('official SemBench runner/evaluator dependencies unavailable: '+', '.join(missing))

    def source(self,root,query,rows=None):
        from src.baselines.text.movie_records import MovieRecordSource,prepare_movie_source
        path=root/'Reviews.csv'
        rows=rows if rows is not None else [
            ('r0','other','good','POSITIVE'),('r1','taken_3','slow good','POSITIVE'),
            ('r2','taken_3','bad','NEGATIVE'),('r1','taken_3','good','POSITIVE')]
        with path.open('w',newline='') as stream:
            writer=csv.writer(stream)
            writer.writerow(['reviewId','id','reviewText','scoreSentiment']);writer.writerows(rows)
        manifest=prepare_movie_source(path,root/'prepared',
            expected_csv_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            max_rows=max(1,len(rows)),task='movie-q'+str(query))
        return MovieRecordSource.from_manifest(manifest)

    def run_query(self,root,query,*,owner=None,rows=None):
        source=self.source(root,query,rows)
        expected=source.expected('movie-'+str(query))
        events=[]
        report=native.run(source,os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'],
            source.task.method(self.method.model_name),self.generation,self.engine,self.resources,
            query_id='movie-'+str(query),profile=dict(native_lm_max_batch_size=4),
            context=dict(claimed_budget=ClaimedUnit(Reservation('movie-'+str(query),1,len(expected)),None)),
            observer=events.append,shared_model=owner)
        self.assertLess(events.index('result_ready'),events.index('evaluation_start'))
        self.assertLess(events.index('evaluation_start'),events.index('evaluation_end'))
        self.assertEqual(report['status'],'completed')
        self.assertEqual(report['calls']['counts']['succeeded'],len(expected))
        self.assertEqual(report['inference_calls'],0)
        self.assertFalse(report['production_core_used'])
        self.assertFalse(report['cache']['result_cache_enabled'])
        self.assertEqual(report['quality']['classification_audit']['false_positive'],0)
        self.assertEqual(report['quality']['classification_audit']['false_negative'],0)
        return report,events

    def test_original_pinned_Q1_Q2_Q3_query_and_gold_evaluator(self):
        from src.baselines.text.sembench_movie import verify_checkout
        verify_checkout(os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'])
        for query in (1,2,3):
            with self.subTest(query=query),tempfile.TemporaryDirectory() as name:
                report,events=self.run_query(Path(name),query)
                self.assertEqual(report['result'],dict(positive_review_cnt=2) if query==3 else
                    dict(reviewId=['r0','r1','r1'] if query==1 else ['r1','r1']))
                self.assertEqual(report['backend_close']['status'],'closed')
                self.assertLess(events.index('result_ready'),events.index('owner_closed'))

    def test_original_queries_reuse_exact_model_owner_without_closing_it(self):
        owner=self.owner()
        owner.start();uid=owner.ready_report['engine_uid']
        for query in (1,2,3):
            with tempfile.TemporaryDirectory() as name:
                report,events=self.run_query(Path(name),query,owner=owner)
                self.assertEqual(report['owner_ready']['engine_uid'],uid)
                self.assertEqual(report['backend_close']['status'],'retained')
                self.assertNotIn('owner_closed',events)
        self.assertEqual(owner.close(timeout=2)['status'],'closed')

    def test_original_empty_predicate_uses_no_model(self):
        # A nonempty table with no eligible taken_3 records still uses the
        # original empty-relation query. No fallback model call is introduced.
        with tempfile.TemporaryDirectory() as name:
            with patch.object(native,'AsyncEngineProcess',side_effect=AssertionError('empty model')):
                source=self.source(Path(name),3,[('r0','other','good','POSITIVE')])
                report=native.run(source,os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'],
                    source.task.method(self.method.model_name),self.generation,self.engine,self.resources,
                    query_id='empty',profile=dict(native_lm_max_batch_size=4),
                    context=dict(claimed_budget=ClaimedUnit(Reservation('empty',1,0),None)))
        self.assertEqual(report['result'],dict(positive_review_cnt=0))
        self.assertEqual(report['calls']['counts']['succeeded'],0)
        self.assertEqual(report['backend_close']['cleanup_scope'],'no model process allocated')


class RunLifecycleContractTests(FixtureCase):
    """Wrapper lifecycle tests, not qualification of the official SemBench runner.

    The query/evaluator entry points alone are test callbacks; LOTUS Filter,
    provider, original SDK batch_completion and existing IPC are exercised.
    """
    source=OriginalMovieQueryTests.source
    run_query=OriginalMovieQueryTests.run_query

    def setUp(self):
        super().setUp()
        def query(_checkout,number,load):
            from src.baselines.text.movie_tasks import MovieTask
            task=MovieTask(number);frame=load()
            if number!=1:frame=frame[frame['id']=='taken_3']
            result=frame.sem_filter(task.instruction)
            return pd.DataFrame(dict(positive_review_cnt=[len(result)])) if number==3 else result[['reviewId']].head(5)
        for name,value in (('run_original_lotus_query',query),
                ('evaluate_original',lambda *_args:dict(original_metric={})),
                ('verify_checkout',lambda path:Path(path))):
            scope=patch('src.baselines.text.sembench_movie.'+name,value)
            scope.start();self.addCleanup(scope.stop)
        scope=patch.dict(os.environ,SEMLOOM_NATIVE_TEST_CHECKOUT='/tmp/CPU-query-callback')
        scope.start();self.addCleanup(scope.stop)

    def test_run_preserves_result_evaluation_and_cold_close_order(self):
        for query in (1,2,3):
            with self.subTest(query=query),tempfile.TemporaryDirectory() as name:
                report,events=self.run_query(Path(name),query)
                self.assertEqual(report['backend_close']['status'],'closed')
                self.assertLess(events.index('evaluation_end'),events.index('owner_closed'))

    def test_run_reuses_model_owner_and_keeps_shutdown_with_caller(self):
        owner=self.owner();owner.start();uid=owner.ready_report['engine_uid']
        for query in (1,2,3):
            with tempfile.TemporaryDirectory() as name:
                report,events=self.run_query(Path(name),query,owner=owner)
                self.assertEqual(report['owner_ready']['engine_uid'],uid)
                self.assertEqual(report['backend_close']['status'],'retained')
                self.assertNotIn('owner_closed',events)

    def test_run_empty_relation_does_not_allocate_a_model(self):
        OriginalMovieQueryTests.test_original_empty_predicate_uses_no_model(self)

    def test_invalid_native_profile_and_checkout_fail_before_model_start(self):
        with tempfile.TemporaryDirectory() as name:
            source=self.source(Path(name),1)
            options=dict(query_id='q',profile=dict(native_lm_max_batch_size=False),
                context=dict(claimed_budget=ClaimedUnit(Reservation('q',1,4),None)))
            with patch.object(native,'AsyncEngineProcess',side_effect=AssertionError('invalid model')):
                with self.assertRaises(ValueError):
                    native.run(source,'/tmp/CPU',source.task.method(self.method.model_name),
                        self.generation,self.engine,self.resources,**options)
                options['profile']=dict(native_lm_max_batch_size=4)
                with patch('src.baselines.text.sembench_movie.verify_checkout',side_effect=ValueError('bad source')):
                    with self.assertRaises(ValueError):
                        native.run(source,'/tmp/CPU',source.task.method(self.method.model_name),
                            self.generation,self.engine,self.resources,**options)

    def failed_resident_query_with_late_transport(self,*,close_actions=(),terminal='succeeded',
                                                  resident=True,emit_during_unknown_close=False):
        """The SDK times out first; serving returns its receipt only at close."""
        class LateTransport:
            ready_report=dict(engine_uid='CPU-late-owner')
            def __init__(self):
                self.row=None;self.events=[];self.emitted=False;self.actions=list(close_actions)
            def submit(self,row,**_kwargs):
                self.row=dict(row)
                return SubmitResult('queued','CPU-late-request')
            def poll(self,maximum):
                events,self.events=self.events[:maximum],self.events[maximum:]
                return tuple(events)
            def abort(self,request_id):
                return True
            def wait(self,timeout):
                raise TimeoutError('CPU SDK stops before serving returns its terminal')
            def close(self,timeout):
                action=self.actions.pop(0) if self.actions else 'closed'
                if isinstance(action,BaseException):raise action
                if (action=='closed' or emit_during_unknown_close) and not self.emitted:
                    self.emitted=True
                    identity=_prepared([self.row])[0][0]
                    result=result_record(identity,engine_uid='CPU-late-owner',start=1.,end=2.,
                        status=terminal,submitted=True)
                    result.update(text='Answer: True',prompt_tokens=11,completion_tokens=2,total_tokens=13,
                        engine_request_id='CPU-late-request',finish_reason='stop')
                    self.events.append(TerminalEvent('CPU-late-request',identity,result,terminal!='unknown',None))
                return dict(status=action,cleanup_errors=[] if action=='closed' else ['CPU cleanup incomplete'])
        transport=LateTransport();owner=self.owner()
        with tempfile.TemporaryDirectory() as name:
            source=self.source(Path(name),1,[('r0','taken_3','good','POSITIVE')])
            budget=ClaimedUnit(Reservation('late',1,1),None)
            with patch.object(native,'AsyncEngineProcess',return_value=transport):
                with self.assertRaises(Exception) as caught:
                    native.run(source,'/tmp/CPU-query-callback',source.task.method(self.method.model_name),
                        self.generation,self.engine,self.resources,query_id='late',
                        profile=dict(native_lm_max_batch_size=4),context=dict(claimed_budget=budget),
                        shared_model=owner if resident else None)
        initial=caught.exception.query_report
        self.assertEqual(initial['calls']['counts']['unknown'],1)
        self.assertEqual(initial['calls']['counts']['succeeded'],0)
        self.assertEqual(initial['native_transport_receipts'][0]['total_tokens'],None)
        self.assertEqual(budget.attempts,1)
        return owner,initial,budget

    def assert_late_accounting(self,receipt):
        accounting=receipt['native_final_accounting']
        self.assertEqual(accounting['query_id'],'late')
        self.assertEqual(accounting['calls']['counts']['unknown'],1)
        self.assertEqual(accounting['calls']['counts']['succeeded'],0)
        self.assertEqual(len(accounting['native_transport_receipts']),1)
        self.assertEqual(len(accounting['late_native_receipts']),1)
        self.assertEqual(accounting['late_native_receipts'][0]['total_tokens'],13)
        return accounting

    def test_resident_close_transfers_late_usage_without_changing_source_unknown(self):
        import weakref
        owner,initial,budget=self.failed_resident_query_with_late_transport()
        retained=weakref.ref(owner._last_native_binding)
        receipt=owner.close(timeout=2)
        self.assertEqual(receipt['status'],'closed')
        self.assert_late_accounting(receipt)
        self.assertEqual(initial['calls']['counts']['unknown'],1)
        self.assertIsNone(owner.last_native_query_report)
        self.assertIsNone(owner._last_native_binding)
        self.assertNotIn('native_final_accounting',owner.last_close_report)
        # The error object may retain a frame for caller debugging; the binding
        # itself has explicitly released all original prompt/result containers.
        if retained() is not None:
            self.assertFalse(retained().expected)
            self.assertFalse(retained().receipts)
            self.assertFalse(retained().bound)
            self.assertFalse(retained().late_receipts)
            self.assertFalse(retained()._pending)
            self.assertFalse(retained()._prompts)
        self.assertEqual(budget.attempts,1)
        repeated=owner.close(timeout=2)
        self.assertEqual(repeated['status'],'closed')
        self.assertIsNone(repeated['native_final_accounting'])

    def test_resident_unknown_terminal_stays_unknown_with_known_late_usage(self):
        owner,_,_=self.failed_resident_query_with_late_transport(terminal='unknown')
        receipt=owner.close(timeout=2)
        accounting=self.assert_late_accounting(receipt)
        self.assertEqual(accounting['late_native_receipts'][0]['status'],'unknown')
        self.assertEqual(receipt['status'],'closed')

    def test_resident_unknown_close_retains_binding_and_retry_transfers_once(self):
        for late_before_close_confirmation in (False,True):
            with self.subTest(late=late_before_close_confirmation):
                owner,_,_=self.failed_resident_query_with_late_transport(close_actions=('unknown','closed'),
                    emit_during_unknown_close=late_before_close_confirmation)
                first=owner.close(timeout=2)
                self.assertEqual(first['status'],'unknown')
                self.assertIsNotNone(owner._last_native_binding)
                self.assertIsNotNone(owner.last_native_query_report)
                self.assertEqual(len(first['native_final_accounting']['late_native_receipts']),
                                 int(late_before_close_confirmation))
                second=owner.close(timeout=2)
                self.assertEqual(second['status'],'closed')
                self.assert_late_accounting(second)
                self.assertIsNone(owner._last_native_binding)

    def test_resident_close_exception_or_interrupt_keeps_accounting_for_retry(self):
        for error in (OSError('CPU close I/O failure'),KeyboardInterrupt('CPU close interrupted')):
            with self.subTest(error=type(error).__name__):
                owner,_,_=self.failed_resident_query_with_late_transport(close_actions=(error,'closed'))
                with self.assertRaises(type(error)) as caught:owner.close(timeout=2)
                self.assertIs(caught.exception,error)
                self.assertIsNotNone(owner._last_native_binding)
                self.assert_late_accounting(owner.close(timeout=2))

    def test_resident_accounting_error_after_physical_close_keeps_original_binding(self):
        owner,_,_=self.failed_resident_query_with_late_transport()
        binding=owner._last_native_binding
        with patch.object(binding,'report',side_effect=OSError('CPU audit snapshot failure')):
            with self.assertRaises(OSError) as caught:owner.close(timeout=2)
        self.assertEqual(caught.exception.native_close_report['model_close_status'],'closed')
        self.assertEqual(owner.last_close_report['status'],'unknown')
        self.assertIs(owner._last_native_binding,binding)
        self.assert_late_accounting(owner.close(timeout=2))

    def test_cold_failure_returns_close_accounting_without_masking_sdk_exception(self):
        _,report,budget=self.failed_resident_query_with_late_transport(resident=False)
        self.assertEqual(report['status'],'failed')
        self.assert_late_accounting(report['backend_close'])
        self.assertEqual(report['late_native_receipts'][0]['total_tokens'],13)
        self.assertEqual(budget.attempts,1)

    def test_concurrent_close_transfers_the_retained_accounting_once(self):
        owner,_,budget=self.failed_resident_query_with_late_transport()
        with ThreadPoolExecutor(max_workers=2) as executor:
            receipts=list(executor.map(lambda _:owner.close(timeout=2),range(2)))
        self.assertTrue(all(r['status']=='closed' for r in receipts))
        transferred=[r for r in receipts if r['native_final_accounting'] is not None]
        self.assertEqual(len(transferred),1)
        self.assert_late_accounting(transferred[0])
        self.assertIsNone(owner._last_native_binding)
        self.assertEqual(budget.attempts,1)
