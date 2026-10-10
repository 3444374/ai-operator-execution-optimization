import io
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from contextlib import nullcontext
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.full_response import FullModelResponse,encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.scheduling.core.session_contract import CloseReport,Usage
from src.semantic_methods.duckdb_ai import DuckDBCall,DuckDBResponse
from src.semantic_methods.continuation import Continue,Final,Request
from src.experiments.postgresql.native_adapter_metrics import summarize_calls
from src.experiments.postgresql.supplier_adapter_query import (
    MethodObservations,_LotusBatchObservation,_LotusMethodObservation,run_supplier_query,
)


class Writer:
    def __init__(self):self.events=[]
    def record(self,event):self.events.append(event)


class SupplierObservationTests(unittest.TestCase):
    def observations(self):
        writer=Writer();raw=io.StringIO()
        return MethodObservations(writer,raw,'fixture'),writer,raw

    def test_native_batch_caller_receipts_wait_for_original_batch_return(self):
        observations,writer,raw=self.observations()
        calls=[]
        def original(data,kwargs,show,description):
            self.assertEqual(sum(e['event']=='task_ready' for e in writer.events),2)
            self.assertFalse(any(e['event']=='caller_response' for e in writer.events))
            calls.append(data)
            return [SimpleNamespace(model_dump=lambda:dict(model='fixture',value='ok')) for _ in data]
        lm=SimpleNamespace(_process_uncached_messages=original)
        selected=_LotusBatchObservation(lm,[dict(row_id='a'),dict(row_id='b')],observations)
        payload=b'{"model":"fixture","messages":[]}'
        with patch('src.semantic_methods.lotus.sdk.prepare_call',return_value=SimpleNamespace(payload=payload)):
            selected(lm,[([],None),([],None)],{},False,'fixture')
        self.assertEqual(len(calls),1)
        receipts=[e for e in writer.events if e['event']=='caller_response']
        self.assertEqual(receipts[0]['monotonic_ns'],receipts[1]['monotonic_ns'])
        ready=[e for e in writer.events if e['event']=='task_ready']
        prepared=[e for e in writer.events if e['event']=='lotus_http_call_prepared']
        self.assertEqual(ready[0]['monotonic_ns'],ready[1]['monotonic_ns'])
        self.assertEqual(len(prepared),2)
        self.assertTrue(all(ready[0]['monotonic_ns']<=e['monotonic_ns']<=receipts[0]['monotonic_ns'] for e in prepared))
        batches=[e for e in writer.events if e['event']=='lotus_batch_return']
        self.assertEqual(len(batches),1)
        self.assertEqual(batches[0]['monotonic_ns'],receipts[0]['monotonic_ns'])
        self.assertEqual(batches[0]['response_count'],2)
        self.assertEqual(batches[0]['executor'],'native')
        saved=[json.loads(line) for line in raw.getvalue().splitlines()]
        self.assertEqual(len(saved),2)
        body=json.dumps(saved[0]['response_values'],sort_keys=True,ensure_ascii=False,allow_nan=False).encode()
        self.assertEqual(hashlib.sha256(body).hexdigest(),receipts[0]['response_bytes_sha256'])
        result=summarize_calls(writer.events,list(observations.calls.values()))
        self.assertEqual(result['request_e2e']['count'],2)
        self.assertEqual(result['calls'][0]['response_representation'],'complete native SDK ModelResponse values')

    def test_semloom_observation_uses_each_execution_payload_once_and_batch_entry_time(self):
        from tests.semantic_methods.test_lotus_control import LotusControlTests
        observations,writer,raw=self.observations()
        with LotusControlTests().batch(prevalidate=True,retain=False) as (executor,execution,state):
            lm=SimpleNamespace(_process_uncached_messages=lambda *args: (_ for _ in ()).throw(AssertionError('native pool called')))
            observer=_LotusBatchObservation(lm,[dict(row_id='a'),dict(row_id='b')],observations,executor)
            with patch('src.semantic_methods.lotus.batch.lotus_response',
                side_effect=lambda full:SimpleNamespace(model_dump=lambda:dict(value='ok'))):
                observer(lm,[([dict(role='user',content=str(i))],None) for i in range(2)],{},False,'fixture')
            self.assertEqual(state['events'].count('prepare'),2)
            self.assertEqual(executor.last_responses,())
            self.assertEqual(execution.engine.capacity.usage(),Usage())
        ready=[e for e in writer.events if e['event']=='task_ready']
        prepared=[e for e in writer.events if e['event']=='lotus_http_call_prepared']
        self.assertEqual(ready[0]['monotonic_ns'],ready[1]['monotonic_ns'])
        self.assertTrue(all(e['scope']=='HTTP encoding used by execution' for e in prepared))
        self.assertTrue(all(ready[0]['monotonic_ns']<=e['monotonic_ns'] for e in prepared))
        self.assertEqual([content['request_values_sha256'] for content in observations.calls.values()],
            [hashlib.sha256(json.dumps(json.loads(payload),sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':')).encode()).hexdigest() for payload in state['actual']])
        self.assertEqual(len(raw.getvalue().splitlines()),4)

    def test_two_stage_observation_keeps_supplier_state_and_real_successor(self):
        observations,writer,raw=self.observations()
        payload=b'{"model":"fixture","messages":[]}'
        class Method:
            def start(self,value):
                return Continue(Request('fixture',payload,1,256),
                    json.dumps(dict(row=json.loads(value),stage=0)).encode())
            def resume(self,state,result):
                current=json.loads(state)
                if current['stage']==0:
                    current['stage']=1
                    return Continue(Request('fixture',payload,1,256),json.dumps(current).encode())
                return Final(b'final')
        method=_LotusMethodObservation(Method(),observations)
        first=method.start(b'{"row_id":"a","text":"same"}')
        full=encode_full_response(FullModelResponse(200,(),b'{"value":"complete"}'))
        second=method.resume(first.state,full)
        final=method.resume(second.state,full)
        self.assertEqual(final.value,b'final')
        value=summarize_calls(writer.events,list(observations.calls.values()))
        self.assertEqual(value['request_e2e']['count'],2)
        self.assertEqual(len(value['successor_waits']),1)
        self.assertEqual(len(raw.getvalue().splitlines()),2)

    def test_invalid_source_retains_failure_before_supplier_or_budget_access(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'query'
            with self.assertRaisesRegex(ValueError,'unique bounded'):
                run_supplier_query('lotus-adapted-native',
                    load_source=lambda:iter([dict(row_id='a',text='same')]*2),
                    plan=SemanticMapPlan('instruction','fixture',16),
                    model=FixedModelConfig('http://127.0.0.1:1/v1/chat/completions','fixture',1000),
                    ledger=None,unit_id='fixture',root=root,options=NativeGraphOptions())
            summary=json.loads((root/'summary.json').read_text())
            self.assertEqual(summary['status'],'failed')
            self.assertEqual(summary['attempted_posts'],0)
            self.assertIn('query',summary['errors'])

    def test_duckdb_failure_summary_keeps_inner_and_outer_diagnostics_and_observation_first_error(self):
        for observation_failure,backend in ((False,'daft'),(True,'daft'),(False,'arrow')):
            with self.subTest(observation_failure=observation_failure,backend=backend), tempfile.TemporaryDirectory() as directory:
                primary=ValueError('observation failed' if observation_failure else 'decode failed')
                cleanup=RuntimeError('iterator close failed')
                report=CloseReport('closed',0,Usage(),None)
                class Native:
                    def __init__(self,*args):
                        self.last_close_report=None
                        self.last_cleanup_error=None
                        self.last_cleanup_errors=()
                    def __call__(self,calls,cancelled):
                        native=self
                        class Responses:
                            def __iter__(self):return self
                            def __next__(self):
                                if not observation_failure:
                                    native.last_cleanup_errors=('release: RuntimeError: release failed','close: RuntimeError: close failed')
                                    native.last_cleanup_error='\n'.join(native.last_cleanup_errors)
                                    raise primary
                                return DuckDBResponse(calls[0].call_id,b'{}',200,-1)
                            def close(self):
                                if observation_failure:
                                    native.last_close_report=report
                                    native.last_cleanup_errors=('close: RuntimeError: iterator close failed',)
                                    native.last_cleanup_error=native.last_cleanup_errors[0]
                                    raise cleanup
                        return Responses()
                class Bridge:
                    def __init__(self,library,callback):
                        self.callback=callback;self.last_error=self.last_cleanup_error=None
                    def __enter__(self):return self
                    def __exit__(self,*args):return False
                    def enable(self,connection):connection.bridge=self
                class Connection:
                    def execute(self,statement):
                        call=DuckDBCall(row=0,query_id='fixture',call_id='row-call',model='fixture',
                            endpoint='http://127.0.0.1:1/v1/chat/completions',
                            payload=b'{"model":"fixture","messages":[]}',headers=(),
                            estimated_tokens=1,timeout_seconds=1,connect_timeout_seconds=1,ready_ns=1)
                        try:list(self.bridge.callback((call,),lambda:False))
                        except BaseException as error:
                            self.bridge.last_error=str(error)
                            raise
                        raise AssertionError('controlled failure must stop SQL')
                connection=Connection()
                physical=SimpleNamespace(window_bytes=2**21+24,payload_backend=backend,workers=2,batch_rows=2)
                model=FixedModelConfig('http://127.0.0.1:1/v1/chat/completions','fixture',1000)
                owner=SimpleNamespace(physical=physical,group=SimpleNamespace(runtime={}),model=model,
                    execution=object(),query=lambda *args:nullcontext(),duckdb_connection=lambda values:connection)
                ledger=SimpleNamespace(reserve_unit=lambda *args:None,claim_shared_unit=lambda *args:None,
                    close_shared_unit=lambda *args:None)
                root=Path(directory)/'query'
                with patch('src.semantic_methods.duckdb_ai.DuckDBNativeTaskExecutor',Native), \
                     patch('src.semantic_methods.duckdb_ai.DuckDBSemLoomBridge',Bridge), \
                     patch.object(MethodObservations,'received',side_effect=primary if observation_failure else None):
                    with self.assertRaises(ValueError) as caught:
                        run_supplier_query('duckdb-method-semloom',load_source=lambda:iter([dict(row_id='row',text='fixture')]),
                            plan=SemanticMapPlan('Return ok.','fixture',16),model=model,ledger=ledger,
                            unit_id='fixture',root=root,options=NativeGraphOptions(concurrency=4),
                            physical=physical,duckdb_library=Path('fixture'),owner=owner)
                self.assertIs(caught.exception,primary)
                saved=json.loads((root/'summary.json').read_text())
                self.assertEqual(saved['status'],'failed')
                diagnostics=saved['identity']['diagnostics']
                self.assertEqual(diagnostics['bridge']['last_error'],str(primary))
                self.assertIsNone(diagnostics['bridge']['last_cleanup_error'])
                self.assertTrue(diagnostics['executor']['last_cleanup_errors'])
                if observation_failure:
                    self.assertIn('iterator close failed',diagnostics['iterator_close_error'])
                    self.assertEqual(diagnostics['executor']['last_close_report']['usage']['held_tasks'],0)
                    self.assertTrue(primary.__notes__)
                else:
                    self.assertEqual(len(diagnostics['executor']['last_cleanup_errors']),2)
                    self.assertIsNone(diagnostics['executor']['last_close_report'])


if __name__=='__main__':unittest.main()
