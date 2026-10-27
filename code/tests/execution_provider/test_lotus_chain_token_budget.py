"""Real LOTUS/Core/actor/CPU IPC, tiny tokenizer; Ray RPC alone is substituted."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
from src.execution_provider.adapters.lotus_chain_token_budget import ChainTokenBudget, OutputTokenEstimate, verify_cost
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor, AsyncRecordsActorOwner
from src.execution_provider.adapters.lotus_record_chain_fifo import LotusChainFIFO
from src.execution_provider.adapters.lotus_chain_token_budget import fingerprint
from src.scheduling.core.session_contract import SessionSpec, TaskProfile, TaskKey
from src.semantic_methods.budget import MethodBudgetPool, MethodCapacity, row_reservation
from src.semantic_methods.continuation import MethodLimits
from src.semantic_methods.lotus.record_chain import LotusRecordChain, StageSpec, MapMethod, RecordOccurrence
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS
from tests.scheduling.test_incremental_session import setup
from tests.execution_provider.chain_tokenizer_fixture import tokenizer, TEMPLATE

ITEM = 65536
METHOD_LIMITS = MethodLimits(65536,131072,131072,4)

class Process(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        return [sys.executable,str(Path(__file__).with_name('chain_token_child_fixture.py')),*map(str,descriptors)]

class Actor(AsyncCoreModelActor):
    def _process_type(self):
        return Process

class Handle:
    """Only .remote/.future are mocked. Production actor and IPC stay intact."""
    def __init__(self, actor, pool):
        self.actor,self.pool = actor,pool
    def __getattr__(self,name):
        def remote(*args):
            future=self.pool.submit(getattr(self.actor,name),*args)
            return SimpleNamespace(future=lambda:future)
        return SimpleNamespace(remote=remote)

class ChainTokens(unittest.TestCase):
    def setUp(self):
        import lotus
        scope=lotus.settings.context(enable_cache=False)
        scope.__enter__();self.addCleanup(scope.__exit__,None,None,None)
        self.model=EngineSpec('/tmp/cpu-chain-tokenizer',sys.executable,sys.executable,max_model_len=4096)
        self.stages=(StageSpec('judge',FilterMethod('openai/cpu-fixture','Accept {body}'),GenerationSpec(max_tokens=8)),
                     StageSpec('compose',MapMethod('openai/cpu-fixture','Rewrite {body}','derived'),GenerationSpec(max_tokens=32,stop=())))
        self.chain=LotusRecordChain(self.stages,query_id='opaque-query')
        self.tok=tokenizer()

    def budget(self,W=1000,stages=None):
        stages=self.stages if stages is None else stages
        return ChainTokenBudget(self.tok,self.model,active_tokens=W,
            tokenizer_signature=fingerprint(self.tok.backend_tokenizer.to_str()),
            template_signature=fingerprint(TEMPLATE),
            expected_output={s.stage_id:OutputTokenEstimate(2,'declared CPU fixture; not measured native usage') for s in stages})

    def record(self,i,text):
        return RecordOccurrence('row-'+str(i),i,7,{'body':text,'business_id':'repeated'})

    def prepared(self,record=None):
        run=self.chain.start_batch([self.record(0,'plain text') if record is None else record])
        self.assertTrue(run.prepare())
        return run.prepared[0],run.method.stage.generation

    def make(self,*,W=1000,C=3,token=True,sink=None,chain=None):
        chain=chain or self.chain
        core,old,_,clock=setup(held_tasks=C,active_requests=C,active_work=W if token else C,
            input_bytes=C*65536,item_input_bytes=65536,result_bytes=C*ITEM,item_result_bytes=ITEM,
            metadata_bytes=4096)
        old.close()
        limits=AsyncEngineLimits(max_inflight=C,result_slots=C,command_slots=C,
            input_bytes=C*65536,item_input_bytes=65536,result_bytes=C*ITEM,item_result_bytes=ITEM)
        resources=RecordsResources(prepare_batch_rows=1,model_batch_rows=1,max_active_batches=C)
        actors=AsyncRecordsActorOwner(chain.stages[0].method,self.model,chain.stages[0].generation,resources,limits=limits)
        actor=Actor(asdict(self.model),asdict(chain.stages[0].generation),1,asdict(limits))
        with patch.object(EngineSpec,'validate_driver_environment',return_value={}):
            actors.model_ready=actor.ready(time.monotonic()+8)
        pool=ThreadPoolExecutor(max_workers=C+2)
        actors.model_actor=Handle(actor,pool)
        budget=self.budget(W,chain.stages) if token else None
        receipts=[]
        backend=LotusChainActorBackend(actors,C,core.wake.notify,deadline_monotonic=time.monotonic()+10,
            token_budget=budget,on_result=sink or (lambda k,v:receipts.append((k,v))))
        core.backend=backend
        session=core.open(SessionSpec('query','chain','fixture',work_unit='tokens' if token else 'model_calls',
            task_profiles=tuple(TaskProfile(s.stage_id,'fixture') for s in chain.stages)))
        amount=row_reservation(METHOD_LIMITS)
        mb=MethodBudgetPool(MethodCapacity(8,8*amount))
        driver=LotusChainFIFO(session,chain,METHOD_LIMITS,mb.allocate(mb.capacity),token_budget=budget)
        def cleanup():
            driver.close()
            actor.close(3)
            pool.shutdown(wait=True)
            core.reap(C)
            backend.close(2)
        self.addCleanup(cleanup)
        return SimpleNamespace(core=core,session=session,driver=driver,actor=actor,actors=actors,backend=backend,
                               receipts=receipts,pool=pool,budget=budget,method_budget=mb)

    def until(self,r,predicate,*,drive=True):
        deadline=time.monotonic()+6
        while not predicate():
            if drive:
                try:r.driver.advance(3)
                except RuntimeError:
                    if predicate():return
                    raise
            if time.monotonic()>deadline:
                self.fail('CPU path did not progress: '+str((r.core.capacity.usage(),r.actor.snapshot())))
            r.core.wake.wait(r.core.wake.generation,.005)

    def finish(self,r):
        out=[]
        deadline=time.monotonic()+7
        while not r.driver.finished:
            try:
                r.driver.advance(3)
            except BaseException:
                import traceback
                for slot in r.backend.backend._slots.values():
                    if slot.future.done() and slot.future.exception() is not None:
                        traceback.print_exception(slot.future.exception())
                raise
            for value in r.driver.results(8):
                out.append(json.loads(value.value));r.driver.release_result(value.row)
            if r.driver.error:raise r.driver.error
            if time.monotonic()>deadline:self.fail('chain not finished')
            r.core.wake.wait(r.core.wake.generation,.002)
        return out

    def test_actual_tokenizer_sees_complete_formatter_messages_once_per_stage(self):
        budget=self.budget()
        row,generation=self.prepared()
        before=deepcopy(row)
        cost=budget.estimate(row,generation)
        self.assertGreater(cost['input_tokens'],len('plain text'.split()))
        self.assertEqual(cost['expected_output_tokens'],2)
        self.assertEqual(cost['output_cap'],8)
        self.assertEqual(cost['reserved_tokens'],cost['input_tokens']+8)
        self.assertEqual(before,row)

    def test_changed_full_identity_messages_generation_and_config_rejected(self):
        budget=self.budget();row,generation=self.prepared();cost=budget.estimate(row,generation)
        for field in IDENTITY_FIELDS:
            wrong=dict(row)
            wrong[field]=wrong[field]+1 if type(wrong[field]) is int else wrong[field]+'changed'
            with self.subTest(field=field),self.assertRaises(ValueError):
                budget.validate(wrong,generation,cost)
        with self.assertRaises(ValueError):budget.validate(dict(row,messages_json='[]'),generation,cost)
        with self.assertRaises(ValueError):budget.validate(row,replace(generation,max_tokens=9),cost)
        with self.assertRaises(ValueError):verify_cost(row,generation,cost,engine=replace(self.model,model_path='/tmp/other-model'))

    def test_short_long_costs_do_not_use_row_count_or_labels(self):
        b=self.budget();a,g=self.prepared(self.record(0,'one'))
        z,_=self.prepared(self.record(1,'one '*80))
        self.assertGreater(b.estimate(z,g)['reserved_tokens'],b.estimate(a,g)['reserved_tokens'])
        self.assertEqual(b.estimate(a,g)['input_tokens'],b.estimate(dict(a,row_id='changed'),g)['input_tokens'])

    def test_oversized_singleton_rejected_before_Core_or_model_submit(self):
        r=self.make(W=10)
        with self.assertRaisesRegex(ValueError,'exceeds W'):
            r.driver.offer_record(self.record(0,'word '*100))
        self.assertEqual(r.core.capacity.usage().held_tasks,0)
        self.assertEqual(r.actor.engine._sequence,0)
        self.assertEqual(r.method_budget.used[0],0)

    def test_W_blocks_below_C_and_real_terminal_releases_then_allows_successor(self):
        large=self.record(0,'WAIT_RELEASE '+('word '*45));small=self.record(1,'plain')
        b=self.budget();a,g=self.prepared(large);z,_=self.prepared(small)
        W=b.estimate(a,g)['reserved_tokens']+b.estimate(z,g)['reserved_tokens']-1
        r=self.make(W=W)
        r.driver.offer_record(large);r.driver.offer_record(small);r.driver.end_input()
        self.until(r,lambda:r.actor.engine.snapshot()['accepted_requests']==1)
        for _ in range(4):r.driver.advance(3)
        self.assertEqual(r.actor.engine._sequence,1)
        self.assertEqual(r.core.capacity.usage().active_requests,1)
        self.assertEqual(r.core.capacity.usage().active_work,b.estimate(a,g)['reserved_tokens'])
        # Fake inference returns a known completion winning this abort race.
        r.actors.cancel_core(TaskKey(r.session.session_id,0)).future().result(2)
        output=self.finish(r)
        self.assertEqual(len(output),2)
        self.assertEqual(len(r.receipts),4)
        self.assertEqual(r.core.capacity.usage().active_work,0)
        self.assertEqual(r.core.capacity.usage().held_tasks,0)
        self.assertEqual(r.method_budget.used[0],0)
        for key,receipt in r.receipts:
            cost=receipt['token_estimate']
            self.assertEqual(receipt['prompt_tokens'],cost['input_tokens'])
            self.assertEqual(receipt['core_task_key'],asdict(key))
            self.assertLessEqual(cost['reserved_tokens'],W)
        maps=[v for k,v in r.receipts if v['algorithm_stage']=='compose']
        self.assertTrue(all(v['fixture_generation']['max_tokens']==32 for v in maps))
        self.assertTrue(all(len(v['parent_call_ids'])==1 for v in maps))

    def test_default_request_count_and_same_original_parser_results(self):
        r=self.make(token=False)
        for i,text in enumerate(['normal','STOP_ME','normal '*8]):r.driver.offer_record(self.record(i,text))
        r.driver.end_input();out=self.finish(r)
        self.assertEqual(len(r.receipts),5)
        self.assertTrue(all('token_estimate' not in v for k,v in r.receipts))
        self.assertEqual(sum(len(v['records']) for v in out),2)
        self.assertEqual(r.core.capacity.usage().active_work,0)

    def test_unknown_retains_Core_W_actor_receipt_and_original_known_usage(self):
        r=self.make()
        row=self.record(0,'OUTCOME_UNKNOWN')
        r.driver.offer_record(row);r.driver.end_input()
        self.until(r,lambda:bool(r.receipts))
        self.until(r,lambda:r.driver.error is not None)
        self.assertEqual(len(r.receipts),1)
        self.assertEqual(r.receipts[0][1]['status'],'unknown')
        self.assertGreater(r.receipts[0][1]['prompt_tokens'],0)
        expected=r.receipts[0][1]['token_estimate']['reserved_tokens']
        self.assertEqual(r.core.capacity.usage().active_work,expected)
        self.assertEqual(r.actor.snapshot()['held_requests'],1)
        self.assertEqual(len(r.backend.snapshot()['retained_results']),1)

    def test_cancel_does_not_refund_before_late_confirmed_receipt(self):
        r=self.make();r.driver.offer_record(self.record(0,'WAIT_CANCEL'));r.driver.end_input()
        self.until(r,lambda:r.actor.engine.snapshot()['accepted_requests']==1)
        work=r.core.capacity.usage().active_work
        r.driver.close()
        self.assertEqual(r.core.capacity.usage().active_work,work)
        self.until(r,lambda:bool(r.receipts),drive=False)
        deadline=time.monotonic()+4
        while r.core.capacity.usage().held_tasks and time.monotonic()<deadline:
            r.core.reap(3);r.core.wake.wait(r.core.wake.generation,.005)
        self.assertEqual(r.core.capacity.usage().active_work,0)
        self.assertEqual(len(r.receipts),1)
        self.assertEqual(r.actor.engine._sequence,1)

    def test_sink_failure_retains_receipt_and_does_not_ack_or_refund(self):
        def fail(key,value):raise OSError('CPU owner sink failed')
        r=self.make(sink=fail);r.driver.offer_record(self.record(0,'plain'));r.driver.end_input()
        self.until(r,lambda:r.driver.error is not None)
        self.assertGreater(r.core.capacity.usage().active_work,0)
        self.assertEqual(r.actor.snapshot()['held_requests'],1)
        self.assertEqual(len(r.backend.failure_receipts()),1)

    def test_confirmed_failure_releases_reservation_but_preserves_usage(self):
        r=self.make();r.driver.offer_record(self.record(0,'CONFIRMED_FAILURE'));r.driver.end_input()
        self.until(r,lambda:r.driver.error is not None)
        self.assertEqual(r.core.capacity.usage().active_work,0)
        self.assertEqual(r.receipts[0][1]['status'],'failed')
        self.assertGreater(r.receipts[0][1]['prompt_tokens'],0)
        self.assertEqual(r.actor.engine._sequence,1)

    def test_IPC_not_accepted_retries_same_generation_without_duplicate_call(self):
        from src.serving.backends.async_engine_process import SubmitResult
        r=self.make();original=r.actor.engine.submit;observed=[]
        def submit(row,**kwargs):
            observed.append((row['call_id'],kwargs['generation'].max_tokens))
            if len(observed)==1:return SubmitResult('backpressure')
            return original(row,**kwargs)
        with patch.object(r.actor.engine,'submit',side_effect=submit):
            r.driver.offer_record(self.record(0,'plain'));r.driver.end_input();out=self.finish(r)
        self.assertEqual(len(out),1)
        self.assertEqual(r.actor.engine._sequence,2)
        self.assertEqual(observed[0],observed[1])
        self.assertEqual([v for k,v in observed],[8,8,32])
        self.assertEqual(len(r.receipts),2)

    def test_complete_FM_matches_original_fused_formatter_parser_and_parent_identity(self):
        from src.modalities.text.tokenization import chat_token_count
        from tests.semantic_methods.test_lotus_record_chain import engine_result
        source=[self.record(0,'plain'),self.record(1,'STOP_ME'),self.record(2,'long '*12)]
        expected_messages={}
        def infer(rows,*,generation):
            values=[]
            for row in rows:
                expected_messages[row['call_id']]=(row['messages_json'],generation.kwargs())
                text=('False' if 'STOP_ME' in row['messages_json'] else 'True') if generation.max_tokens==8 else 'rewritten'
                value=engine_result(row,text)
                count=chat_token_count(self.tok,json.loads(row['messages_json']))
                value.update(prompt_tokens=count,completion_tokens=1,total_tokens=count+1)
                values.append(value)
            return values
        expected=self.chain.run_batch(source,infer)
        r=self.make()
        for record in source:r.driver.offer_record(record)
        r.driver.end_input();out=self.finish(r)
        actual=sorted([record for group in out for record in group['records']],key=lambda v:v['source_position'])
        self.assertEqual(actual,[record.describe() for record in expected['records']])
        calls={value['call_id']:value for key,value in r.receipts}
        self.assertEqual(set(calls),set(expected_messages))
        for wanted in expected['calls']:
            got=calls[wanted['call_id']]
            for name in (*IDENTITY_FIELDS,'parent_call_ids','text','status','prompt_tokens','completion_tokens','total_tokens'):
                self.assertEqual(got[name],wanted[name])
            self.assertEqual(got['messages_sha256'],hashlib.sha256(json.dumps(
                json.loads(expected_messages[got['call_id']][0]),sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest())
            self.assertEqual(got['fixture_generation'],expected_messages[got['call_id']][1])

    def test_capacity_preflight_rejects_long_stage_before_actor_or_IPC_ownership(self):
        from src.execution_provider.adapters.lotus_records_core import _check_result_capacity
        from src.semantic_methods.lotus.record_chain import stage_identity
        row=stage_identity('query',self.record(0,'plain'),'compose-with-'+'x'*50)
        with self.assertRaises(ValueError):_check_result_capacity(row,398)
        r=self.make()
        prepared=dict(row,messages_json=json.dumps([{'role':'user','content':'plain'}]))
        with self.assertRaises(ValueError):
            r.actor.run_prepared_core([prepared],time.monotonic()+2,398,(99,0),asdict(self.stages[1].generation))
        self.assertEqual(r.actor.snapshot()['held_requests'],0)
        self.assertEqual(r.actor.engine._sequence,0)

    def test_oversized_close_receipt_preserves_chain_identity_and_known_usage(self):
        from src.execution_provider.adapters.lotus_chain_token_budget import audit_chain_call
        from src.semantic_methods.lotus.record_chain import stage_identity
        import threading
        r=self.make()
        key=TaskKey(42,0)
        wanted=stage_identity('query',self.record(0,'plain'),'compose')
        value=dict(wanted,status='succeeded',text='x'*4000,prompt_tokens=11,completion_tokens=3,total_tokens=14)
        with r.backend._lock:r.backend._requests[key]=(wanted,1024)
        errors=r.backend.recover_actor_receipts([dict(request_key=[42,0],result=value)])
        self.assertEqual(errors,[])
        _,encoded=r.backend.failure_receipts()[0]
        got=json.loads(encoded)
        audit_chain_call(wanted,got)
        self.assertLessEqual(len(encoded),1024)
        self.assertEqual((got['status'],got['prompt_tokens'],got['completion_tokens']),('failed',11,3))
        self.assertEqual(got['call_id'],wanted['call_id'])

    def test_chain_outputs_feed_canonical_result_accounting_and_legacy_alias(self):
        from src.semantic_methods.lotus.record_results import ChainBatchResults, _source_binding
        from src.baselines.text.frameworks.lotus_chain_results import ChainBatchResults as Legacy
        self.assertIs(ChainBatchResults,Legacy)
        source=[self.record(0,'plain'),self.record(1,'STOP_ME')]
        r=self.make()
        for record in source:r.driver.offer_record(record)
        r.driver.end_input();out=self.finish(r)
        audit=ChainBatchResults(source,self.chain)
        by_id={record.row_id:record for record in source}
        for i,group in enumerate(out):
            input_id=group['calls'][0]['row_id']
            audit.accept(dict(batch_id='batch-'+str(i),inputs=[_source_binding(by_id[input_id])],
                records=group['records'],calls=group['calls'],columns=group['columns'],
                stage_stats=group['stage_stats']))
        audit.verify_success(source_exhausted=True)
        self.assertEqual(len(audit.calls),3)
        self.assertEqual(len(audit.pending),1)
        self.assertEqual(audit.pop().values['derived'],'rewritten')

    def test_stage_names_fields_scale_do_not_select_different_cost_policy(self):
        stages=(StageSpec('arbitrary/verify',FilterMethod('openai/cpu-fixture','Accept {caption}'),GenerationSpec(max_tokens=8)),
            StageSpec('arbitrary/write',MapMethod('openai/cpu-fixture','Rewrite {caption}','answer'),GenerationSpec(max_tokens=32,stop=())))
        chain=LotusRecordChain(stages,query_id='renamed')
        r=self.make(chain=chain)
        for i in range(5):r.driver.offer_record(RecordOccurrence('item-'+str(i),i,'index',{'caption':'free text '*i or 'plain'}))
        r.driver.end_input();out=self.finish(r)
        self.assertEqual(sum(len(v['records']) for v in out),5)
        self.assertEqual(len(r.receipts),10)
        self.assertEqual(r.core.capacity.usage().active_work,0)

if __name__=='__main__':unittest.main()
