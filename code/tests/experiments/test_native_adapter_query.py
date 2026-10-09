import base64
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

from src.baselines.text.frameworks.prepared_map import graph_input, graph_preprocess, graph_postprocess
from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.native_adapter_http import bind_native_http_events
from src.experiments.postgresql.native_adapter_query import prepare_fixed_calls, run_prepared_map_query,load_bounded_rows,resolve_adapter_limits
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.experiments.request_identity import RAY_IDENTITY_FIELD


@contextmanager
def fixture_server(*, http_status=200, delay=.005, content='ok'):
    requests=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*_):pass
        def do_POST(self):
            body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            requests.append(body)
            time.sleep(delay)
            value=dict(id='fixture-response',object='chat.completion',created=0,model='fixture',
                       choices=[dict(index=0,message=dict(role='assistant',content=content),finish_reason='stop')],
                       usage=dict(prompt_tokens=2,completion_tokens=1,total_tokens=3))
            raw=json.dumps(value).encode()
            self.send_response(http_status)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(raw)))
            self.end_headers();self.wfile.write(raw)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        yield 'http://127.0.0.1:'+str(server.server_port)+'/v1/chat/completions',requests
    finally:
        server.shutdown();server.server_close();thread.join(2)


class NativeAdapterQueryTests(unittest.TestCase):
    def test_method_prepares_duplicate_text_as_distinct_calls_before_graph(self):
        events=[]
        values=[dict(row_id='a',text='same'),dict(row_id='b',text='same')]
        calls=prepare_fixed_calls(values,SemanticMapPlan('instruction','fixture',16),events.append,
                                  clock=lambda:10,clock_domain='fixture')
        self.assertEqual(calls[0]['request_values_sha256'],calls[1]['request_values_sha256'])
        self.assertNotEqual(calls[0]['call_id'],calls[1]['call_id'])
        self.assertEqual([e['monotonic_ns'] for e in events],[10,10])
        row=graph_input(calls[0])
        request=graph_preprocess(row)
        self.assertNotIn(RAY_IDENTITY_FIELD,row['payload'])
        wire=encode_full_response(FullModelResponse(200,(),b'complete'))
        row['http_response']={RAY_IDENTITY_FIELD:request['payload'][RAY_IDENTITY_FIELD],
                             '__semloom_complete_response':base64.b64encode(wire).decode()}
        self.assertEqual(base64.b64decode(graph_postprocess(row)['response']),wire)
        row['http_response'][RAY_IDENTITY_FIELD]['row_id']='other'
        with self.assertRaises(ValueError):graph_postprocess(row)

    def test_native_binding_keeps_pre_send_clock_and_occurrence(self):
        events=[dict(event='http_started',attempt='native-attempt',monotonic_ns=10),
                dict(event='prepared_http_binding',attempt='native-attempt',call_id='a',row_id='a',
                     stage_id='model',clock_domain='fixture')]
        bound=bind_native_http_events(events)
        self.assertEqual(bound[0]['call_id'],'a')
        self.assertEqual(bound[0]['monotonic_ns'],10)
        with self.assertRaises(ValueError):bind_native_http_events(events[:1])

    def run_local(self, directory, endpoint, *, rows=3, **parameters):
        budget=AttemptBudget('adapter-fixture',rows)
        ledger=CellBudgetLedger.create(Path(directory)/'budget.sqlite',budget,deadline_utc=time.time()+10)
        return run_prepared_map_query('fixed-map-semloom-local-diagnostic',
            load_source=lambda:iter(dict(row_id=str(i),text='duplicate text') for i in range(rows)),
            plan=SemanticMapPlan('instruction','fixture',16),
            model=FixedModelConfig(endpoint,'fixture',2000),ledger=ledger,unit_id='query-fixture',
            root=Path(directory)/'query',query_timeout_s=3,max_rows=rows,**parameters)

    def test_more_held_tasks_keep_the_active_capacity_and_complete_every_input(self):
        with tempfile.TemporaryDirectory() as directory,fixture_server() as (endpoint,requests):
            value=self.run_local(directory,endpoint,rows=32,max_held_tasks=128)
            self.assertEqual(value['semloom_capacity'],dict(held_tasks=128,active_requests=4))
            self.assertEqual((value['actual_posts'],value['rows'],len(requests)),(32,32,32))
            self.assertFalse(any(value['core_cleanup']['final_core_usage'].values()))

    def test_adapter_limits_keep_legacy_defaults_and_reject_unreachable_capacity(self):
        options=NativeGraphOptions(concurrency=16)
        self.assertEqual(resolve_adapter_limits(options),(16,16))
        self.assertEqual(resolve_adapter_limits(options,128,4),(128,4))
        for held,threads in ((8,4),(True,4),(128,False),(0,4),(257,4),(128,257)):
            with self.subTest(held=held,threads=threads),self.assertRaises(ValueError):
                resolve_adapter_limits(options,held,threads)

    def test_real_core_and_fixture_http_keep_counts_full_response_and_release(self):
        with tempfile.TemporaryDirectory() as directory,fixture_server() as (endpoint,requests):
            value=self.run_local(directory,endpoint)
            self.assertEqual(value['status'],'passed')
            self.assertEqual(value['actual_posts'],3)
            self.assertEqual(len(requests),3)
            self.assertEqual(value['call_timing']['request_e2e']['count'],3)
            self.assertEqual(value['execution']['recorded_rows'],3)
            self.assertFalse(any(value['core_cleanup']['final_core_usage'].values()))
            rows=[json.loads(line) for line in (Path(directory)/'query/complete-responses.jsonl').read_text().splitlines()]
            self.assertEqual(len(rows),3)

    def test_http_failure_preserves_primary_failure_and_raw_protocol(self):
        with tempfile.TemporaryDirectory() as directory,fixture_server(http_status=500) as (endpoint,requests):
            with self.assertRaises(ValueError):self.run_local(directory,endpoint,rows=1)
            root=Path(directory)/'query'
            summary=json.loads((root/'summary.json').read_text())
            self.assertEqual(summary['status'],'failed')
            self.assertEqual(summary['attempted_posts'],1)
            self.assertEqual(len(requests),1)
            self.assertIn('http_failure',summary['errors'])
            self.assertTrue((root/'protocols.jsonl').read_text())

    def test_semloom_main_rejects_missing_physical_executor_before_model(self):
        with self.assertRaisesRegex(ValueError,'declared executor'):
            run_prepared_map_query('fixed-map-semloom',load_source=lambda:(),
                plan=SemanticMapPlan('instruction','fixture',16),
                model=FixedModelConfig('http://127.0.0.1:1/v1/chat/completions','fixture',1),
                ledger=None,unit_id='fixture',root=Path('/unused'))

    def test_source_row_limit_stops_the_producer_before_full_collection(self):
        visited=[]
        def source():
            for i in range(1000):
                visited.append(i)
                yield dict(row_id=str(i),text='raw')
        with self.assertRaisesRegex(ValueError,'row allowance'):
            load_bounded_rows(source,max_rows=2)
        self.assertEqual(visited,[0,1,2])
        with self.assertRaises(ValueError):
            load_bounded_rows(lambda:iter([dict(row_id='a',text='same')]*2))


if __name__=='__main__':unittest.main()
