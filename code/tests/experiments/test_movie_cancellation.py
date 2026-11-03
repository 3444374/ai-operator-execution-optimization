"""Native-frontier replay and real CPU IPC; no real model, Ray or GPU."""
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from types import ModuleType
import asyncio
import importlib.util
import json
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from src.experiments import movie_cancellation as subject
from src.experiments import movie_cancellation_probe as probe
from src.experiments.attempt_ledger import AttemptBudget, BudgetError
from src.experiments.cell_budget import CellBudgetLedger
from src.serving.backends.vllm_engine import EngineSpec
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor, AsyncRecordsActorOwner
from tests.experiments.movie_token_resident_fixture import MovieTokenResidentFixture, with_actual_cpu_pid
from tests.execution_provider.test_lotus_record_local_query import Process


def expected():
    return [dict(query_id='q', row_id='r'+str(i), source_position=i, call_index=0, call_id='ordinary-'+str(i)) for i in range(2)]


def identity(key, *, present=True):
    return dict(lookup_internal=False, mapped_internal_ids=['internal-'+key],
        present_internal_ids=['internal-'+key] if present else [], external_id_by_internal={'internal-'+key:key})


def events(*, complete=False, aborted=False, no_op=False):
    rows=[]
    for i,row in enumerate(expected()):
        key='owner:'+str(i)
        rows.append(dict(event='qualification_request', request_id=key, **dict(row,call_id='chain-'+str(i))))
        rows.append(dict(event='native_add_return',request_id=key,native_identity=identity(key)))
    if complete:
        rows += [dict(event='native_final_observed',request_id='internal-owner:'+str(i),finished=True) for i in range(2)]
    if aborted:
        for i in range(2):
            key='owner:'+str(i)
            rows += [dict(event='native_abort_enter',request_id=key,internal=False,native_identities={key:identity(key,present=not no_op)}),
                     dict(event='native_abort_return',request_id=key)]
    return [dict(row,pid=123,monotonic_ns=i+1) for i,row in enumerate(rows)]


class NativeGateTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name)
        self.gate=subject.NativeGate(self.root,expected(),'q',max_bytes=65536,max_events=100)
        self.gate.ready=lambda:dict(pid=123,producer_owner_uid='owner')

    def write(self,rows):
        (self.root/'native-123.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows))

    def test_true_native_registration_triggers_once_and_joins_chain_to_ordinary_IDs(self):
        self.write(events())
        self.assertTrue(self.gate())
        tick=self.gate.trigger_ns
        self.write(events(aborted=True))
        self.assertTrue(self.gate())
        self.assertEqual(self.gate.trigger_ns,tick)
        state=self.gate.state(complete=True)
        self.assertEqual(set(state['binding'].values()),{'ordinary-0','ordinary-1'})
        self.assertEqual(len(state['aborted']),2)

    def test_natural_completion_or_noop_abort_does_not_qualify(self):
        for rows in (events(complete=True),events(complete=True,aborted=True),events(aborted=True,no_op=True)):
            with self.subTest(rows=rows):
                self.write(rows)
                self.assertEqual(self.gate.state(complete=True)['aborted'],[])
        self.write(events(complete=True))
        self.assertFalse(self.gate())

    def test_foreign_binding_changed_alias_or_duplicate_occurrence_is_not_guessed(self):
        for defect in ('owner','call','alias','duplicate'):
            rows=events()
            if defect=='owner':rows[0]['request_id']='foreign:0'
            elif defect=='call':rows[0]['row_id']='other-row'
            elif defect=='alias':
                rows[3]['native_identity']['mapped_internal_ids']=['internal-owner:0']
                rows[3]['native_identity']['present_internal_ids']=['internal-owner:0']
                rows[3]['native_identity']['external_id_by_internal']={'internal-owner:0':'owner:1'}
            else:rows[2].update(row_id='r0',source_position=0)
            self.write(rows)
            with self.subTest(defect=defect),self.assertRaises(ValueError):self.gate.state(complete=True)

    def test_missing_partial_malformed_and_over_bound_evidence_cannot_pass(self):
        self.assertFalse(self.gate())
        self.write(events())
        path=self.root/'native-123.jsonl'
        path.write_bytes(path.read_bytes()+b'{')
        with self.assertRaises(ValueError):self.gate.state(complete=True)
        path.write_text('{bad}\n')
        with self.assertRaises(ValueError):self.gate()
        self.write(events())
        self.gate.max_bytes=1
        with self.assertRaises(ValueError):self.gate()

    def test_native_abort_error_and_missing_native_identity_remain_unqualified(self):
        rows=events(aborted=True)
        rows.append(dict(event='native_abort_error',request_id='owner:0',pid=123))
        self.write(rows)
        with self.assertRaises(ValueError):self.gate.state(complete=True)
        rows=events()
        for row in rows:
            if row['event']=='native_add_return':row['native_identity']=None
        self.write(rows)
        self.assertFalse(self.gate())


class CancellationEntryTests(unittest.TestCase):
    def fixture(self):
        fixture=MovieTokenResidentFixture(self)
        plan=fixture.plan('RF')
        for query in plan['queries']:query['phase']='measured'
        context=fixture.context(plan)
        context['cancellation_probe']=dict(max_bytes=1_000_000,max_events=1000)
        return fixture,plan,context

    @contextmanager
    def cpu_boundaries(self,fixture,*,mode='hold'):
        from src.experiments import lotus_records_movie as movie
        @contextmanager
        def instrument(directory):
            class CPUProcess(Process):
                @staticmethod
                def _command(spec,descriptors):
                    return [sys.executable,str(Path(__file__).with_name('cancellation_child_fixture.py')),str(directory),mode,*map(str,descriptors)]
            with patch.object(AsyncCoreModelActor,'_process_type',return_value=with_actual_cpu_pid(CPUProcess)):
                yield
        with patch('integrations.imlane_records.chain_runtime.check_external_lease',return_value={}), \
                patch.object(EngineSpec,'validate_driver_environment',return_value={}), \
                patch.object(movie,'model_preflight',return_value=dict(inference_calls=0)), \
                patch.object(probe,'instrument',instrument):
            yield

    def test_actual_CPU_IPC_cancel_then_fresh_recovery_preserves_unknowns_and_budgets(self):
        for bypass in (False,True):
            with self.subTest(bypass=bypass):
                fixture,plan,context=self.fixture()
                plan['experimental_skip_token_packing']=bypass
                output=fixture.root/'session'
                with self.cpu_boundaries(fixture):
                    result=subject.run_movie_cancellation(plan,context,output)
                self.assertEqual(result['status'],'completed',result)
                self.assertEqual(result['cancel_phase']['status'],'failed')
                self.assertEqual(result['cancellation_qualification']['status'],'passed')
                self.assertEqual(len(result['cancellation_qualification']['expected_unknown_call_ids']),2)
                self.assertTrue(result['recovery_owner_new'])
                self.assertEqual([r['status'] for r in subject._records(result['cancel_phase'])],['unknown','unknown'])
                self.assertEqual(sum(r['total_tokens'] for r in subject._records(result['cancel_phase'])),28)
                self.assertEqual([r['status'] for r in subject._records(result['recovery_phase'])],['succeeded','succeeded'])
                for claim in context['claims'].values():
                    with self.assertRaises(BudgetError):claim.bind_execution()
                config=dict(movie_plan=plan,workload=dict(kind='movie_session',qualification=subject.QUALIFICATION),
                            observation=dict(mode='off',max_events_per_process=1000,max_event_bytes=1000))
                counts=dict(observed_records=4,missing_claimed=0,failed=0,unknown_records=2,succeeded=2)
                receipt=subject.verify_cancellation_cell(config,output.parent,counts)
                self.assertEqual(receipt['expected_unknown_call_ids'],result['cancellation_qualification']['expected_unknown_call_ids'])
                original=(output/'session.json').read_text()
                for defect in ('method_storage','RPC','unknown_error','published','counts'):
                    bad=deepcopy(result)
                    owners=bad['cancel_phase']['owners']
                    if defect=='method_storage':owners[0]['close']['qualification_method_storage']['runs']=1
                    elif defect=='RPC':owners[0]['close']['receipts']['model']['model_owner']['data_rpc_pending']=1
                    elif defect=='unknown_error':subject._records(bad['cancel_phase'])[0]['error_type']='UnexpectedOOM'
                    elif defect=='published':bad['cancel_phase']['queries'][0]['query_id']='other'
                    (output/'session.json').write_text(json.dumps(bad))
                    with self.subTest(defect=defect),self.assertRaises(ValueError):
                        subject.verify_cancellation_cell(config,output.parent,dict(counts,unknown_records=0) if defect=='counts' else counts)
                (output/'session.json').write_text(original)

    def prepaid_context(self, fixture, plan, context):
        ledger=CellBudgetLedger.create(fixture.root/'budget.sqlite',AttemptBudget('cancel-fixture',4),deadline_utc=None)
        for query in plan['queries']:ledger.reserve_unit(query['query_id'],2)
        context['claims']={query['query_id']:ledger.claim_unit(query['query_id']) for query in plan['queries']}
        return ledger

    def test_cancel_close_recovers_actor_receipts_after_bulk_result_readers_failed(self):
        from src.execution_provider.adapters.lotus_record_chain_core import _SectionTransport
        fixture,plan,context=self.fixture()
        ledger=self.prepaid_context(fixture,plan,context);before=ledger.snapshot()
        poll=AsyncCoreModelActor.poll_prepared_batch
        execute=_SectionTransport._execute
        recover=_SectionTransport.recover_actor_receipts
        failed_readers=set();lock=threading.Lock();readers_done=threading.Event();bridges=[]
        def failed_close_poll(actor,*args):
            with actor._condition:closing=actor._closing
            if closing:
                raise RuntimeError('CPU injected lost bulk result reply during close')
            return poll(actor,*args)
        async def observe_failed_reader(transport,task,*args):
            try:return await execute(transport,task,*args)
            except RuntimeError:
                with lock:
                    failed_readers.add(task.key)
                    if len(failed_readers)==2:readers_done.set()
                raise
        def after_failed_readers(transport,receipts):
            if receipts:
                self.assertTrue(readers_done.wait(3),'CPU close did not reach the failed local-reader boundary')
                self.assertEqual([r['result']['status'] for r in receipts],['unknown','unknown'])
            errors=recover(transport,receipts)
            if receipts:
                bridges.append(dict(actor_receipts=len(receipts),retained=len(transport.failure_receipts()),
                                    owner_calls=len(transport.owner.calls)))
            return errors
        output=fixture.root/'session'
        with self.cpu_boundaries(fixture),patch.object(AsyncCoreModelActor,'poll_prepared_batch',failed_close_poll), \
                patch.object(_SectionTransport,'_execute',observe_failed_reader), \
                patch.object(_SectionTransport,'recover_actor_receipts',after_failed_readers):
            result=subject.run_movie_cancellation(plan,context,output)
        self.assertEqual(result['status'],'completed',dict(result=result,bridge=bridges))
        self.assertTrue(bridges)
        self.assertEqual(bridges[0],dict(actor_receipts=2,retained=2,owner_calls=2))
        records=subject._records(result['cancel_phase'])
        self.assertEqual([r['status'] for r in records],['unknown','unknown'])
        self.assertTrue(all(r['submission_confirmed'] and r['engine_terminal_confirmed'] is False for r in records))
        self.assertTrue(all(r['error_type']=='AbortNotConfirmed' and r.get('chain_call_id') for r in records))
        self.assertEqual(sum(r['total_tokens'] for r in records),28)
        self.assertTrue(all(r.get('core_task_key') and r.get('transport_batch') for r in records))
        self.assertEqual(result['cancel_phase']['queries'][0]['backend_report']['runner_unconfirmed_call_ids'],[])
        closed=result['cancel_phase']['owners'][0]['close']['receipts']['model']
        self.assertFalse(closed['query_completed'])
        self.assertFalse(closed['producer_retirement']['outcome_confirmation'])
        self.assertTrue(all(value==0 for value in closed['core_usage'].values()))
        self.assertEqual(closed['retained_remote_inputs'],0)
        self.assertEqual([r['status'] for r in subject._records(result['recovery_phase'])],['succeeded','succeeded'])
        self.assertEqual(sum(e['event']=='construct' for e in fixture.events()),2)
        self.assertEqual(ledger.snapshot(),before)
        config=dict(movie_plan=plan,workload=dict(kind='movie_session',qualification=subject.QUALIFICATION),
                    observation=dict(mode='off',max_events_per_process=1000,max_event_bytes=1000))
        counts=dict(observed_records=4,missing_claimed=0,failed=0,unknown_records=2,succeeded=2)
        self.assertEqual(subject.verify_cancellation_cell(config,output.parent,counts)['status'],'passed')

    def assert_stopped_before_recovery(self, fixture, plan, context, ledger, before, result):
        self.assertEqual(result['status'],'failed',result)
        self.assertNotIn('recovery_phase',result)
        self.assertEqual(sum(e['event']=='construct' for e in fixture.events()),1)
        records=subject._records(result['cancel_phase'])
        self.assertEqual([r['status'] for r in records],['unknown','unknown'])
        self.assertEqual(sum(r['total_tokens'] for r in records),28)
        self.assertEqual(ledger.snapshot(),before)  # Neither unknowns nor unexecuted recovery refund the claim.
        with self.assertRaises(BudgetError):context['claims'][plan['queries'][0]['query_id']].bind_execution()
        context['claims'][plan['queries'][1]['query_id']].bind_execution()

    def test_audit_sink_failure_survives_retirement_until_retry_succeeds(self):
        from src.execution_provider.adapters.lotus_record_chain_core import _SectionTransport, SemLoomRecordChainCore
        for failed_positions,error_type in (((0,),OSError),((0,1),OSError),((0,1),KeyboardInterrupt)):
            with self.subTest(failed_positions=failed_positions,error_type=error_type.__name__):
                fixture,plan,context=self.fixture()
                ledger=self.prepaid_context(fixture,plan,context);before=ledger.snapshot()
                poll=AsyncCoreModelActor.poll_prepared_batch;execute=_SectionTransport._execute
                initialize=_SectionTransport.__init__;recover=_SectionTransport.recover_actor_receipts
                close=SemLoomRecordChainCore._close_record_local
                failed_readers=set();lock=threading.Lock();readers_done=threading.Event()
                fault=[True];transports=[];closes=[];accepted={}
                def failed_close_poll(actor,*args):
                    with actor._condition:closing=actor._closing
                    if closing:raise RuntimeError('CPU injected ended bulk result reader')
                    return poll(actor,*args)
                async def observe_reader(transport,task,*args):
                    try:return await execute(transport,task,*args)
                    except RuntimeError:
                        with lock:
                            failed_readers.add(task.key)
                            if len(failed_readers)==2:readers_done.set()
                        raise
                def initialize_sink(transport,*args):
                    initialize(transport,*args);transports.append(transport)
                    sink=transport.on_result
                    def submit(key,value):
                        if fault[0] and value['source_position'] in failed_positions:
                            raise error_type('CPU audit sink rejects this member')
                        sink(key,value);accepted[key]=accepted.get(key,0)+1
                    transport.on_result=submit
                def recover_after_readers(transport,receipts):
                    if receipts:self.assertTrue(readers_done.wait(3),'CPU readers did not stop')
                    return recover(transport,receipts)
                def capture_close(owner,*args,**kwargs):
                    report=close(owner,*args,**kwargs)
                    closes.append(dict(status=report['status'],cleanup_errors=list(report['cleanup_errors'])))
                    return report
                with self.cpu_boundaries(fixture), \
                        patch.object(AsyncCoreModelActor,'poll_prepared_batch',failed_close_poll), \
                        patch.object(_SectionTransport,'_execute',observe_reader), \
                        patch.object(_SectionTransport,'__init__',initialize_sink), \
                        patch.object(_SectionTransport,'recover_actor_receipts',recover_after_readers), \
                        patch.object(SemLoomRecordChainCore,'_close_record_local',capture_close):
                    try:
                        result=subject.run_movie_cancellation(plan,context,fixture.root/'sink-session')
                        self.assertEqual(result['status'],'failed')
                        self.assertNotIn('recovery_phase',result)
                        self.assertEqual(len(transports),1)
                        transport=transports[0];owner=transport.owner;pending=len(failed_positions)
                        self.assertTrue(closes)
                        self.assertTrue(all(r['status']=='unknown' and r['cleanup_errors'] for r in closes),closes)
                        first=deepcopy(owner.last_close_report);partial=deepcopy(owner.calls)
                        self.assertEqual(len(transport.failure_receipts()),pending)
                        self.assertEqual(len(transport._requests),pending)
                        retained_requests=deepcopy(transport._requests)
                        retained_results=dict(transport.failure_receipts())
                        self.assertEqual(len(owner.calls),2-pending)
                        self.assertEqual(first['audit_handoff']['pending_receipts'],pending)
                        self.assertGreater(first['audit_handoff']['retained_result_bytes'],0)
                        self.assertEqual(first['audit_handoff']['deferred_core_acks'],pending)
                        self.assertEqual(first['audit_handoff']['registered_requests'],pending)
                        self.assertEqual(first['audit_handoff']['retained_result_bytes'],sum(map(len,retained_results.values())))
                        self.assertEqual(transport.retained_refs(),0)
                        self.assertTrue(all(v==0 for v in first['core_usage'].values()))
                        self.assertTrue(first['producer_retirement']['confirmed'])
                        self.assertFalse(first['producer_retirement']['outcome_confirmation'])
                        actor_receipts=deepcopy(first['model_owner']['unconsumed_core_results'])
                        wanted=next(r for r in actor_receipts if r['result']['source_position'] in failed_positions)
                        conflicting=deepcopy(wanted)
                        conflicting['result'].update(completion_tokens=4,total_tokens=15)
                        errors=transport.recover_actor_receipts([conflicting])
                        self.assertIn('actor_close_receipt: ValueError',errors)
                        self.assertEqual(owner.calls,partial)
                        for _ in range(2):
                            report=owner.close(timeout=1)
                            self.assertEqual(report['status'],'unknown')
                            self.assertTrue(report['cleanup_errors'])
                            self.assertEqual(report['audit_handoff']['pending_receipts'],pending)
                            self.assertEqual(len(transport._requests),pending)
                            self.assertEqual(transport._requests,retained_requests)
                            self.assertEqual(dict(transport.failure_receipts()),retained_results)
                        stats=deepcopy(owner._root_run.stats)
                        fault[0]=False
                        report=owner.close(timeout=1)
                        self.assertEqual(report['status'],'closed',report['cleanup_errors'])
                        self.assertEqual(report['audit_handoff']['pending_receipts'],0)
                        self.assertEqual(report['audit_handoff']['retained_result_bytes'],0)
                        self.assertEqual(report['cleanup_errors'],[])
                        calls=deepcopy(owner.calls)
                        self.assertEqual([c['status'] for c in calls],['unknown','unknown'])
                        self.assertTrue(all(c['error_type']=='AbortNotConfirmed' and c['engine_terminal_confirmed'] is False for c in calls))
                        self.assertEqual(sum(c['total_tokens'] for c in calls),28)
                        self.assertTrue(all(c['core_task_key'] and c['transport_batch'] for c in calls))
                        self.assertEqual(list(accepted.values()),[1,1])
                        self.assertEqual(transport.failure_receipts(),())
                        self.assertEqual(transport._requests,{})
                        self.assertEqual(transport.recover_actor_receipts(actor_receipts*2),[])
                        self.assertEqual(owner.close(timeout=1)['status'],'closed')
                        self.assertEqual(owner.calls,calls)
                        self.assertEqual(list(accepted.values()),[1,1])
                        self.assertEqual(owner._root_run.stats,stats)
                        self.assertFalse(report['query_completed'])
                        self.assertEqual(sum(e['event']=='construct' for e in fixture.events()),1)
                        self.assertEqual(ledger.snapshot(),before)
                        with self.assertRaises(BudgetError):context['claims'][plan['queries'][0]['query_id']].bind_execution()
                        context['claims'][plan['queries'][1]['query_id']].bind_execution()
                    finally:
                        fault[0]=False
                        for transport in transports:transport.owner.close(timeout=2)

    def test_cancel_receipt_write_failure_stops_before_recovery_without_refunding_unknowns(self):
        from src.experiments import native_query_lifecycle as lifecycle
        for receipt, writer in (('session.json','write_private_json'),('query-0.json','publish_private_json')):
            with self.subTest(receipt=receipt):
                fixture,plan,context=self.fixture()
                ledger=self.prepaid_context(fixture,plan,context);before=ledger.snapshot()
                original=getattr(lifecycle,writer)
                def fail(path,value):
                    if Path(path).name==receipt and Path(path).parent.name=='cancel':
                        raise OSError('CPU injected cancel phase receipt write failure')
                    return original(path,value)
                with self.cpu_boundaries(fixture),patch.object(lifecycle,writer,fail):
                    result=subject.run_movie_cancellation(plan,context,fixture.root/'session')
                self.assert_stopped_before_recovery(fixture,plan,context,ledger,before,result)
                self.assertFalse((fixture.root/'session/cancel'/receipt).exists())
                self.assertIn('OSError',' '.join(result['cancel_exception']['notes']))

    def test_secondary_cleanup_interrupt_stops_before_recovery_with_original_failure_evidence(self):
        fixture,plan,context=self.fixture()
        ledger=self.prepaid_context(fixture,plan,context);before=ledger.snapshot()
        interrupted=[]
        def observer(event):
            if event['event']=='session_owner_closed' and not interrupted:
                interrupted.append(event['query_id'])
                raise KeyboardInterrupt('CPU injected interrupt after requested cancellation')
        with self.cpu_boundaries(fixture):
            result=subject.run_movie_cancellation(plan,context,fixture.root/'session',observer=observer)
        self.assert_stopped_before_recovery(fixture,plan,context,ledger,before,result)
        self.assertEqual(result['cancel_phase']['cleanup_failure']['error_type'],'KeyboardInterrupt')
        self.assertEqual(result['cancel_exception']['error_type'],'InterruptedError')
        self.assertIn('KeyboardInterrupt',' '.join(result['cancel_exception']['notes']))

    def test_missing_original_cancel_receipt_stops_even_without_exception_notes(self):
        from src.experiments import movie_records_session as session
        for receipt in ('session.json','query-0.json','owner-ready-0.json','owner-0/query-0/query-accounting-final.json'):
            with self.subTest(receipt=receipt):
                fixture,plan,context=self.fixture()
                ledger=self.prepaid_context(fixture,plan,context);before=ledger.snapshot()
                run=session.run_movie_session
                def missing(value,context,output,**kwargs):
                    try:return run(value,context,output,**kwargs)
                    except InterruptedError:
                        (output/receipt).unlink()
                        raise
                with self.cpu_boundaries(fixture),patch.object(session,'run_movie_session',missing):
                    result=subject.run_movie_cancellation(plan,context,fixture.root/'session')
                self.assert_stopped_before_recovery(fixture,plan,context,ledger,before,result)
                self.assertEqual(result['cancel_exception']['notes'],[])

    def test_unexpected_primary_interrupt_is_not_an_expected_cancel_or_recovery(self):
        from src.experiments import movie_records_session as session
        fixture,plan,context=self.fixture()
        ledger=self.prepaid_context(fixture,plan,context);before=ledger.snapshot()
        run=session.run_movie_session
        def interrupted(*args,**kwargs):
            try:return run(*args,**kwargs)
            except InterruptedError as original:
                error=KeyboardInterrupt('CPU injected unrelated primary interrupt')
                error.session_report=original.session_report
                raise error from original
        output=fixture.root/'session'
        with self.cpu_boundaries(fixture),patch.object(session,'run_movie_session',interrupted):
            with self.assertRaises(KeyboardInterrupt):subject.run_movie_cancellation(plan,context,output)
        result=json.loads((output/'session.json').read_text())
        self.assert_stopped_before_recovery(fixture,plan,context,ledger,before,result)
        self.assertEqual(result['cancel_exception']['error_type'],'KeyboardInterrupt')

    def test_guard_requires_original_phase_receipts_and_no_secondary_cancellation_failures(self):
        fixture,plan,context=self.fixture();output=fixture.root/'session'
        with self.cpu_boundaries(fixture):result=subject.run_movie_cancellation(plan,context,output)
        self.assertEqual(result['status'],'completed',result)
        config=dict(movie_plan=plan,workload=dict(kind='movie_session',qualification=subject.QUALIFICATION),
                    observation=dict(mode='off',max_events_per_process=1000,max_event_bytes=1000))
        counts=dict(observed_records=4,missing_claimed=0,failed=0,unknown_records=2,succeeded=2)
        for phase in ('cancel','recovery'):
            for receipt in ('session.json','query-0.json','owner-ready-0.json','owner-0/query-0/query-accounting-final.json'):
                path=output/phase/receipt;hidden=path.with_name(path.name+'.held')
                path.rename(hidden)
                try:
                    with self.subTest(phase=phase,receipt=receipt),self.assertRaises(ValueError):
                        subject.verify_cancellation_cell(config,output.parent,counts)
                finally:hidden.rename(path)
        original=(output/'session.json').read_text()
        for defect in ('missing_exception','exception_notes','exception_type','exception_module',
                       'cleanup_failure','query_notes','observer_errors'):
            bad=deepcopy(result)
            if defect=='missing_exception':bad.pop('cancel_exception',None)
            elif defect=='exception_notes':bad.setdefault('cancel_exception',{})['notes']=['session recording also failed: OSError']
            elif defect=='exception_type':bad['cancel_exception']['type_qualname']='KeyboardInterrupt'
            elif defect=='exception_module':bad['cancel_exception']['type_module']='unrelated_module'
            elif defect=='cleanup_failure':bad['cancel_phase']['cleanup_failure']={'error_type':'KeyboardInterrupt'}
            elif defect=='query_notes':bad['cancel_phase']['queries'][0]['failure']['notes']=['additional failure']
            else:bad['cancel_phase']['observer_errors']=[{'error_type':'OSError'}]
            (output/'session.json').write_text(json.dumps(bad))
            with self.subTest(defect=defect),self.assertRaises(ValueError):
                subject.verify_cancellation_cell(config,output.parent,counts)
        (output/'session.json').write_text(original)

    def test_fast_natural_completion_is_not_cancellation_and_never_starts_recovery(self):
        fixture,plan,context=self.fixture()
        with self.cpu_boundaries(fixture,mode='fast'):
            result=subject.run_movie_cancellation(plan,context,fixture.root/'session')
        self.assertEqual(result['status'],'failed')
        self.assertNotIn('recovery_phase',result)
        self.assertEqual(result['cancel_phase']['status'],'completed')
        self.assertEqual(sum(e['event']=='construct' for e in fixture.events()),1)
        recovery=plan['queries'][1]['query_id']
        context['claims'][recovery].bind_execution()  # The recovery allowance was never executed.

    def test_recovery_failure_preserves_all_known_call_receipts(self):
        fixture,plan,context=self.fixture()
        from src.experiments import movie_records_session as session
        run=session.run_movie_session
        def fail_recovery(value,*args,**kwargs):
            result=run(value,*args,**kwargs)
            result['status']='failed'
            error=RuntimeError('explicit CPU failure after recovery results')
            error.session_report=result
            raise error
        invoked=[0]
        def phased(value,*args,**kwargs):
            invoked[0]+=1
            return (run if invoked[0]==1 else fail_recovery)(value,*args,**kwargs)
        with self.cpu_boundaries(fixture),patch.object(session,'run_movie_session',phased):
            result=subject.run_movie_cancellation(plan,context,fixture.root/'session')
        self.assertEqual(result['status'],'failed')
        self.assertEqual(len(subject._records(result['recovery_phase'])),2)
        self.assertTrue((fixture.root/'session/query-1.json').exists())
        self.assertEqual(invoked,[2])

    def test_plan_rejects_warmup_search_and_insufficient_C_without_owner_allocation(self):
        fixture,plan,_context=self.fixture()
        for defect in ('warmup','search','C','cap'):
            value=deepcopy(plan)
            if defect=='warmup':value['queries'][0]['phase']='warmup'
            elif defect=='search':value['profile']['admission']=dict(kind='resource_feedback')
            elif defect=='C':value['profile']['max_inflight']=1
            else:value['engine']['max_model_calls']=5
            with self.subTest(defect=defect),self.assertRaises(ValueError):subject.check_cancellation_plan(value)
        self.assertEqual(fixture.runtime.async_actors,[])

    def test_ordinary_workflow_stays_ordinary_and_qualification_requires_bounds(self):
        config=dict(workload=dict(kind='movie_session'),observation=dict(mode='off'))
        self.assertIsNone(subject.qualification_options(config))
        config['workload']['qualification']=subject.QUALIFICATION
        with self.assertRaises(ValueError):subject.qualification_options(config)
        config['observation'].update(max_events_per_process=10,max_event_bytes=100)
        self.assertEqual(subject.qualification_options(config),dict(max_bytes=1000,max_events=10))
        config['observation']['mode']='on'
        with self.assertRaises(ValueError):subject.qualification_options(config)

    def test_probe_keeps_original_actor_args_and_only_changes_the_child_command(self):
        fixture,plan,_context=self.fixture()
        from src.semantic_methods.lotus.records import RecordsResources
        from src.serving.backends.vllm_engine import GenerationSpec
        owner=AsyncRecordsActorOwner(None,EngineSpec.from_dict(plan['engine']),GenerationSpec.from_dict(plan['generation']),
                                    RecordsResources(**plan['resources']),_prepared_only=True)
        before=owner._model_actor_arguments()
        with probe.instrument(fixture.root):
            self.assertIs(owner._model_actor_type(),probe.CancellationActor)
            self.assertEqual(owner._model_actor_arguments(),(*before,str(fixture.root)))
        self.assertEqual(owner._model_actor_arguments(),before)
        process=object.__new__(probe.CancellationProcess);process.trace_directory=str(fixture.root)
        command=process._command(owner.engine,(5,6,7))
        self.assertEqual(command[0],owner.engine.model_python)
        self.assertEqual(command[-4:],[str(fixture.root),'5','6','7'])

    def test_worker_dispatch_uses_existing_claims_and_only_the_explicit_qualification(self):
        spec=importlib.util.spec_from_file_location('cancellation_worker_test',
            Path(__file__).resolve().parents[2]/'scripts/profiling/record_observation/run_resource_C2.py')
        worker=importlib.util.module_from_spec(spec);spec.loader.exec_module(worker)
        config=dict(artifact_root='/fixture/out',run_id='case',worker_entry_ns=1,
            workload=dict(kind='movie_session',expected_calls=4,qualification=subject.QUALIFICATION),
            movie_plan={},units=dict(cancel=2,recovery=2),runtime_binding={},external_owner={},
            sembench_checkout='/fixture/sembench',limits=dict(worker_timeout_s=5),
            observation=dict(mode='off',max_events_per_process=10,max_event_bytes=100))
        claims=[]
        class Ledger:
            def claim_unit(self,key):claims.append(key);return ('claim',key)
        checked=dict(expected_calls=4,queries=[dict(query_id=key,expected_calls=2) for key in config['units']])
        with patch.object(subject,'check_cancellation_plan',return_value=checked), \
                patch.object(subject,'run_movie_cancellation',return_value={'status':'completed'}) as execute:
            worker.movie_session(config,Ledger())
        self.assertEqual(claims,['cancel','recovery'])
        context=execute.call_args.args[1]
        self.assertEqual(context['cancellation_probe'],dict(max_events=10,max_bytes=1000))
        self.assertEqual(context['claims'],{'cancel':('claim','cancel'),'recovery':('claim','recovery')})


class BootstrapTests(unittest.TestCase):
    def test_sidecar_failure_never_changes_original_input_return_or_business_exception(self):
        from src.serving.backends import vllm_async_engine as serving
        from src.observability import native_model_api_trace as trace
        path=Path(__file__).resolve().parents[2]/'scripts/experiments/probe_movie_cancellation_child.py'
        spec=importlib.util.spec_from_file_location('cancellation_bootstrap_test',path)
        bootstrap=importlib.util.module_from_spec(spec);spec.loader.exec_module(bootstrap)
        modules={name:ModuleType(name) for name in ('vllm','vllm.v1','vllm.v1.engine','vllm.v1.engine.async_llm')}
        for value in modules.values():value.__path__=[]
        modules['vllm.v1.engine.async_llm'].AsyncLLM=object()
        for fail in (False,True):
            seen=[];returned=object();business_error=ValueError('original CPU business error')
            row=expected()[0];generation=object();owner=object()
            async def original(self,passed,*args,**kwargs):
                seen.append((self,passed,args,kwargs))
                if fail:raise business_error
                return returned
            def broken_emit(*args,**kwargs):raise OSError('CPU observer unavailable')
            with patch.dict(sys.modules,modules),patch.object(trace,'install'), \
                    patch.object(trace,'writer',return_value=broken_emit), \
                    patch.object(serving.VllmAsyncEngine,'generate_prepared',original):
                bootstrap.install('/fixture/sidecar')
                call=serving.VllmAsyncEngine.generate_prepared(owner,row,request_id='owner:0',
                    generation=generation,deadline_monotonic=123.)
                if fail:
                    with self.assertRaises(ValueError) as error:asyncio.run(call)
                    self.assertIs(error.exception,business_error)
                else:self.assertIs(asyncio.run(call),returned)
            self.assertIs(seen[0][0],owner);self.assertIs(seen[0][1],row)
            self.assertIs(seen[0][3]['generation'],generation)
            self.assertEqual(seen[0][3]['deadline_monotonic'],123.)


class GuardQualificationTests(unittest.TestCase):
    def test_only_verified_explicit_cancellation_can_pass_with_unknowns_without_refund(self):
        from tests.experiments import test_record_campaign_guard as fixtures
        import workflow
        import control_resource_C2 as controller
        for selection in ('ordinary','verified','bad-evidence'):
            helper=fixtures.CampaignEntryTests();helper.setUp();self.addCleanup(helper.doCleanups)
            guard,root,config=helper.entry('cancel-'+selection)
            helper.cpu_fake_ray_ready(guard)
            if selection!='ordinary':config['workload']['qualification']=subject.QUALIFICATION
            records=[helper.record('q:0',status='unknown',submission_confirmed=False),helper.record('q:1'),helper.record('q:2')]
            def controller_fixture(path):
                (path/'WORKER-RESULT.json').write_text(json.dumps(dict(calls=records)))
                (path/'RESULT.json').write_text(json.dumps(dict(status='completed',physical_cleanup_confirmed=True,lease_released=True)))
            verifier=(lambda *a: {'schema':'CPU-verifier-fixture','status':'passed'}) if selection=='verified' else (lambda *a: (_ for _ in ()).throw(ValueError('invalid native proof')))
            with patch.object(workflow,'check',return_value=config),patch.object(controller,'profiled',return_value=False), \
                    patch.object(controller,'worker_environment',return_value={}),patch.object(controller,'main',controller_fixture), \
                    patch.object(subject,'verify_cancellation_cell',side_effect=verifier) as verify:
                if selection=='verified':fixtures.guard_module.run_cell(guard,'first')
                else:
                    with self.assertRaises(fixtures.guard_module.GuardStop):fixtures.guard_module.run_cell(guard,'first')
            if selection=='ordinary':verify.assert_not_called()
            else:verify.assert_called_once()
            end=json.loads((guard.root/'first-GUARD-END.json').read_text())
            self.assertEqual(end['status'],'passed' if selection=='verified' else 'failed')
            self.assertEqual(end['counts']['unknown_records'],1)
            self.assertEqual(end['counts']['conservative_charge'],3)
            self.assertIsNone(end['counts']['actual_submitted_exact'])
            self.assertEqual(end['counts']['unknown_responsibilities'],1)
