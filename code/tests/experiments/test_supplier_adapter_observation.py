import io
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.full_response import FullModelResponse,encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
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


if __name__=='__main__':unittest.main()
