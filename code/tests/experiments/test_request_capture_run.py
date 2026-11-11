import json
from pathlib import Path
import tempfile
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.experiments.request_capture_run import read_capture, run_capture
from src.infrastructure.private_artifacts import file_identity


class CaptureRunTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        generation = dict(max_tokens=8, stop=[])
        self.requests = [dict(row=dict(query_id='q', model_role='main', algorithm_stage='map',
            row_id=i, source_position=i, call_index=0, call_id='call-'+str(i),
            messages_json=json.dumps([dict(role='user', content='complete input '+str(i))])),
            generation=generation, token_ids=[10,i]) for i in range(3)]
        path=self.root/'capture.json'
        path.write_text(json.dumps(dict(schema='semloom.semantic_request_capture.v1', requests=self.requests)))
        self.config=dict(workload=dict(kind='request_capture', expected_calls=3,
            input=dict(capture=str(path), **file_identity(path))), generation=generation,
            engine=dict(model_path='/fixture/model', model_python='/fixture/model/bin/python',
                        driver_python='/fixture/driver/bin/python',max_model_calls=3),
            ipc_limits=dict(max_inflight=2), request_group_rows=2, artifact_root=str(self.root),
            units=dict(capture=3), resources=dict(startup_timeout_s=1, query_timeout_s=1),
            limits=dict(owner_close_s=1))
        self.claim=SimpleNamespace(bind_execution=lambda:None)
        self.ledger=SimpleNamespace(claim_unit=lambda key:self.claim)

    def process(self, *, changed_tokens=False, fail_second=False):
        parent=self
        class Process:
            def __init__(self,*args,**kwargs):
                self.ready_report={'engine_uid':'fixture'}
                self.pending=[];self.groups=[];parent.owner=self
            def submit_batch(self, rows, **kwargs):
                self.groups.append(rows)
                for row in rows:
                    i=row['row_id']
                    self.pending.append(SimpleNamespace(request_id=row['call_id'], engine_terminal_confirmed=True,
                        identity={k:v for k,v in row.items()if k!='messages_json'},error_type=None,
                        result=dict(row,status='failed'if fail_second and i==1 else 'succeeded',
                                    prompt_token_ids_json=json.dumps([11 if changed_tokens else 10,i]))))
                return SimpleNamespace(status='queued',request_ids=tuple(r['call_id']for r in rows))
            def poll(self, **kwargs):
                values=self.pending[::-1];self.pending=[];return values
            def close(self, **kwargs):
                parent.closed=True;return dict(status='closed')
        return Process

    def test_complete_requests_and_generation_survive_out_of_order_results(self):
        result=run_capture(self.config,self.ledger,process_type=self.process())
        self.assertEqual(result['status'],'completed')
        self.assertEqual([len(g)for g in self.owner.groups],[2,1])
        self.assertEqual([r for g in self.owner.groups for r in g],[r['row']for r in self.requests])
        self.assertTrue(self.closed)

    def test_tokenizer_mismatch_retains_results_and_closes(self):
        result=run_capture(self.config,self.ledger,process_type=self.process(changed_tokens=True))
        self.assertEqual(result['status'],'failed')
        self.assertEqual(len(result['calls']),3)
        self.assertTrue(self.closed)
        self.assertTrue((self.root/'REQUEST-RESULTS.json').exists())

    def test_failed_native_request_stops_later_groups(self):
        result=run_capture(self.config,self.ledger,process_type=self.process(fail_second=True))
        self.assertEqual(result['status'],'failed')
        self.assertEqual(len(self.owner.groups),1)
        self.assertEqual(len(result['calls']),2)
        self.assertEqual(result['terminal_failures'][0]['request_id'],'call-1')
        self.assertTrue(self.closed)

    def test_failure_preserves_same_batch_terminal_delivered_during_close(self):
        from src.serving.backends.vllm_protocol import EngineSpec
        from tests.serving.test_async_engine_process import FixtureProcess, row
        for i, mode in enumerate(('unknown', 'hold')):
            self.requests[i].update(row=row(i, mode), token_ids=list(range(11)))
        path = self.root/'capture.json'
        path.write_text(json.dumps(dict(schema='semloom.semantic_request_capture.v1', requests=self.requests)))
        self.config['workload']['input'].update(file_identity(path))
        self.config['engine'].update(model_python=sys.executable)
        self.config['resources'].update(startup_timeout_s=3, query_timeout_s=3)
        self.config['limits']['owner_close_s'] = 2
        owners = []
        def factory(*args, **kwargs):
            owner = FixtureProcess(*args, **kwargs)
            owners.append(owner)
            return owner
        try:
            with patch.object(EngineSpec, 'validate_driver_environment', return_value={}):
                result = run_capture(self.config, self.ledger, process_type=factory)
            self.assertEqual(result['status'], 'failed')
            self.assertEqual(result['error_type'], 'RuntimeError')
            self.assertEqual(result['actual_calls'], 2)
            self.assertEqual({r['call_id'] for r in result['calls']}, {'q:0', 'q:1'})
            self.assertEqual(result['submitted_requests'], 2)
            self.assertEqual(result['received_terminal_events'], 2)
            self.assertEqual(result['confirmed_terminal_events'], 1)
            self.assertEqual(result['unreceived_request_ids'], [])
            self.assertEqual(owners[0].poll(max_events=2), ())
            saved = json.loads((self.root/'REQUEST-RESULTS.json').read_text())
            self.assertEqual(saved['calls'], result['calls'])
        finally:
            for owner in owners:
                owner.close(timeout=2)

    def test_generation_rewrite_is_rejected_before_start(self):
        self.config['generation']=dict(max_tokens=4,stop=[])
        with self.assertRaisesRegex(ValueError,'unchanged'):
            read_capture(self.config)
