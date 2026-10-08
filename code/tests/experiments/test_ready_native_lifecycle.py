from contextlib import nullcontext
import base64
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.query_workloads import prepare
from src.experiments.postgresql.semantic_system_query import run_native_query


class ReadyNativeLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        prepare(self.root/'inputs','movie',[('a','m','raw review','POSITIVE','original')],{},max_rows=1)
        self.model=self.root/'model.json'
        self.model.write_text(json.dumps(dict(endpoint_url='http://127.0.0.1:1/v1/chat/completions',model_id='fixture',timeout_ms=1000)))
        self.ledger=CellBudgetLedger.create(self.root/'budget.sqlite',AttemptBudget('ready.fixture',1),deadline_utc=time.time()+60)
        self.cancelled=threading.Event();self.source_cancels=0;self.rejected=[]
        test=self
        class Cursor:
            def __enter__(self):return self
            def __exit__(self,*_):pass
            def stream(self,*_):yield 0,'a','m','raw review','original'
        class Connection:
            info=SimpleNamespace(backend_pid=1)
            def __enter__(self):return self
            def __exit__(self,*_):pass
            def execute(self,*_):pass
            def transaction(self):return nullcontext()
            def cursor(self):return Cursor()
            def cancel_safe(self,*_,**__):test.source_cancels+=1
        class Gateway:
            def __init__(self,**kwargs):
                test.before_forward=kwargs['before_forward']
                test.after_forward=kwargs['after_forward']
            def __enter__(self):return self
            def __exit__(self,*_):pass
            def endpoint_url(self,*_):return 'http://127.0.0.1:1/v1/chat/completions'
        class Sampler:
            def __init__(self,*_,**__):pass
            def __enter__(self):return self
            def __exit__(self,*_):pass
            def summary(self):return {}
        self.patchers=(
            patch.dict('sys.modules',{'psycopg':SimpleNamespace(connect=lambda *a,**k:Connection())}),
            patch('src.experiments.postgresql.semantic_system_query.ObservationGateway',Gateway),
            patch('src.experiments.postgresql.semantic_system_query.ProcessSampler',Sampler),
            patch('src.experiments.postgresql.semantic_system_query.QueryInputs.select_sql',return_value=('raw-select',None)),
        )
        for p in self.patchers:p.start();self.addCleanup(p.stop)

    def query(self,prepared,timeout=1):
        with patch('src.experiments.postgresql.semantic_system_query.prepare_rows',return_value=nullcontext(prepared)):
            return run_native_query('sema-map',manifest_path=self.root/'inputs/manifest.json',table='raw_input',
                model_path=self.model,ledger=self.ledger,unit_id='native.unit',root=self.root/'query',dsn='fixture',
                sema_binary=self.root/'unused',query_timeout_s=timeout,timing_mode='ready')

    def test_deadline_cancels_native_session_and_refuses_later_forwarding(self):
        test=self
        class Prepared:
            def cancel(self):test.cancelled.set()
            def execute(self):
                if not test.cancelled.wait(.5):raise AssertionError('native cancel was not called')
                with test.assertRaisesRegex(RuntimeError,'stopped further'):
                    test.before_forward(None,json.dumps(dict(model='fixture',temperature=0)).encode())
                test.rejected.append(True)
                raise RuntimeError('native session cancelled')
                yield
        with self.assertRaisesRegex(RuntimeError,'native session cancelled'):
            self.query(Prepared(),timeout=.03)
        value=json.loads((self.root/'query/summary.json').read_text())
        recording=json.loads((self.root/'query/q0/execution.json').read_text())
        self.assertEqual(self.source_cancels,0)
        self.assertTrue(value['query_stopped_observed'])
        self.assertEqual(value['actual_posts'],0)
        self.assertIsNotNone(recording['t_cancel_triggered_ns'])
        self.assertEqual(self.rejected,[True])

    def test_budget_close_failure_keeps_native_error_and_other_evidence(self):
        class Prepared:
            def execute(self):
                raise ValueError('original native error')
                yield
        with patch.object(CellBudgetLedger,'close_shared_unit',side_effect=OSError('budget close failed')):
            with self.assertRaisesRegex(ValueError,'original native error'):
                self.query(Prepared())
        value=json.loads((self.root/'query/summary.json').read_text())
        self.assertEqual(value['status'],'failed')
        self.assertEqual(value['error']['message'],'original native error')
        self.assertEqual(value['errors']['budget_close']['message'],'budget close failed')
        self.assertEqual(json.loads((self.root/'query/request-reservations.json').read_text()),[])

    def test_cancel_failure_does_not_replace_the_triggering_http_error(self):
        test=self
        class Prepared:
            def cancel(self):raise OSError('native cancel failed')
            def execute(self):
                test.after_forward(None,json.dumps(dict(model='fixture')).encode(),b'{"error":"upstream failed"}',500)
                yield
        with self.assertRaisesRegex(ValueError,'HTTP failure'):
            self.query(Prepared())
        value=json.loads((self.root/'query/summary.json').read_text())
        self.assertEqual(value['error']['type'],'ValueError')
        self.assertEqual(value['errors']['http_failure_cancel']['message'],'native cancel failed')

    def test_malformed_response_is_saved_before_cancel_and_further_requests_are_refused(self):
        test=self
        body=json.dumps(dict(model='fixture',temperature=0)).encode()
        response=b'\xff'
        class Prepared:
            def cancel(self):test.cancelled.set()
            def execute(self):
                test.before_forward(None,body)
                try:
                    test.after_forward(None,body,response,200)
                finally:
                    with test.assertRaisesRegex(RuntimeError,'stopped further'):
                        test.before_forward(None,body)
                yield
        with self.assertRaises(UnicodeDecodeError):
            self.query(Prepared())
        self.assertTrue(self.cancelled.is_set())
        value=json.loads((self.root/'query/summary.json').read_text())
        protocol=json.loads((self.root/'query/protocols.jsonl').read_text())
        self.assertEqual(value['actual_posts'],1)
        self.assertEqual(value['error']['type'],'UnicodeDecodeError')
        self.assertEqual(protocol['http_status'],200)
        self.assertIsNone(protocol['response'])
        self.assertEqual(base64.b64decode(protocol['response_bytes_base64']),response)


if __name__=='__main__':unittest.main()
