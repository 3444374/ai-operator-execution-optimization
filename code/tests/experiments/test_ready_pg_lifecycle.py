import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from src.baselines.common.private_artifacts import write_private_json
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.postgresql.query_config import QueryConfig
from src.experiments.postgresql.query_workloads import prepare
from src.experiments.postgresql.ready_semantic_query import run_ready_pg_query,run_ready_database_query


class ReadyPgLifecycleTests(unittest.TestCase):
    def native_fixture(self, arm, *, early_http=False):
        test=self
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            prepare(root/'inputs','movie',[('a','m','review','POSITIVE','original')],{},max_rows=1)
            model=root/'model.json'
            model.write_text(json.dumps(dict(endpoint_url='http://127.0.0.1:1/v1/chat/completions',model_id='fixture',timeout_ms=1000)))
            tick=time.monotonic_ns()+1_000_000
            class Gateway:
                def __init__(self,**options):self.trace=options['trace_path']
                def __enter__(self):return self
                def __exit__(self,*_):
                    self.trace.write_text(json.dumps(dict(status='completed',request_body_sha256='same',
                        forwarded_body_sha256='same',received_monotonic_ns=tick-1 if early_http else tick+1,
                        upstream_dispatch_started_monotonic_ns=tick+2,
                        upstream_response_body_read_completed_monotonic_ns=tick+3))+'\n')
                def endpoint_url(self,*_):return 'http://127.0.0.1:1/v1/chat/completions'
            def run(config,**options):
                test.assertEqual(config.arm,arm)
                test.assertEqual(options['timing_mode'],'ready')
                test.assertEqual(options.get('ray_temp_root'),root/'ray-temp')
                test.assertEqual(json.loads(Path(options['model_path']).read_text())['model_id'],'fixture')
                Path(options['root']).mkdir()
                write_private_json(Path(options['root'])/'summary.json',dict(status='passed'))
                return dict(execution=dict(t_submit_ns=tick,t_query_terminal_ns=tick+4),
                    evaluation=dict(actual_posts=1,quality=dict(invalid=0)))
            with patch('src.experiments.postgresql.ready_semantic_query.ObservationGateway',Gateway), \
                 patch('src.experiments.postgresql.ready_semantic_query.run_query',side_effect=run):
                call=lambda:run_ready_database_query(QueryConfig('fixture',arm,'map','raw_input'),
                    manifest_path=root/'inputs/manifest.json',model_path=model,
                    budget_path=root/'unused-budget.sqlite',budget=AttemptBudget('ready.fixture',1),
                    root=root/'query',dsn='fixture',ray_temp_root=root/'ray-temp')
                if early_http:
                    with self.assertRaisesRegex(ValueError,'preparation unexpectedly'):
                        call()
                    value=json.loads((root/'query/summary.json').read_text())
                    self.assertEqual(value['status'],'failed')
                else:
                    value=call()
                    self.assertEqual(value['status'],'passed')
                    self.assertEqual(value['schema'],'semloom.ready_database_query.v1')
                    self.assertEqual(value['observed_peak_http'],1)
                    self.assertEqual(value['preparation_model_posts'],0)
                    self.assertEqual(value['client_http_observation'],{})

    def test_native_database_entries_share_proxy_clock_and_keep_their_attempt_guard(self):
        for arm in ('pg-source-direct','ray-data','daft-native'):
            with self.subTest(arm=arm):self.native_fixture(arm)

    def test_native_database_entry_rejects_model_work_before_submission(self):
        self.native_fixture('daft-native',early_http=True)

    def test_http_failure_stops_further_forwarding_and_retains_raw_response_and_first_error(self):
        for arm in ('pg-source-direct','ray-data','daft-native'):
            with self.subTest(arm=arm),tempfile.TemporaryDirectory() as temp:
                root=Path(temp);callbacks={}
                prepare(root/'inputs','movie',[('a','m','review','POSITIVE','original')],{},max_rows=1)
                model=root/'model.json'
                model.write_text(json.dumps(dict(endpoint_url='http://127.0.0.1:1/v1/chat/completions',model_id='fixture',timeout_ms=1000)))
                class Gateway:
                    def __init__(self,**options):callbacks.update(options)
                    def __enter__(self):return self
                    def __exit__(self,*_):pass
                    def endpoint_url(self,*_):return 'http://127.0.0.1:1/v1/chat/completions'
                def run(*args,**options):
                    callbacks['before_forward'](None,b'{}')
                    with self.assertRaises(json.JSONDecodeError):
                        callbacks['after_forward'](None,b'{}',b'broken response',200)
                    with self.assertRaisesRegex(RuntimeError,'stopped further'):
                        callbacks['before_forward'](None,b'{}')
                    raise RuntimeError('later execution failure')
                with patch('src.experiments.postgresql.ready_semantic_query.ObservationGateway',Gateway), \
                     patch('src.experiments.postgresql.ready_semantic_query.run_query',side_effect=run):
                    with self.assertRaises(json.JSONDecodeError):
                        run_ready_database_query(QueryConfig('fixture',arm,'map','raw_input'),
                            manifest_path=root/'inputs/manifest.json',model_path=model,
                            budget_path=root/'unused-budget.sqlite',budget=AttemptBudget('ready.fixture',1),
                            root=root/'query',dsn='fixture')
                summary=json.loads((root/'query/summary.json').read_text())
                self.assertEqual(summary['error']['type'],'JSONDecodeError')
                self.assertEqual(set(summary['errors']),{'http_failure','query'})
                response=json.loads((root/'query/protocols.jsonl').read_text())
                self.assertEqual(response['response_bytes_base64'],'YnJva2VuIHJlc3BvbnNl')

    def test_summary_write_error_does_not_replace_original_pg_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            prepare(root/'inputs','movie',[('a','m','review','POSITIVE','original')],{},max_rows=1)
            model=root/'model.json'
            model.write_text(json.dumps(dict(endpoint_url='http://127.0.0.1:1/v1/chat/completions',model_id='fixture',timeout_ms=1000)))
            class Gateway:
                def __init__(self,**_):pass
                def __enter__(self):return self
                def __exit__(self,*_):pass
                def endpoint_url(self,*_):return 'http://127.0.0.1:1/v1/chat/completions'
            def write(path,value):
                if Path(path).name=='summary.json':raise OSError('summary write failed')
                return write_private_json(path,value)
            with patch('src.experiments.postgresql.ready_semantic_query.ObservationGateway',Gateway), \
                 patch('src.experiments.postgresql.ready_semantic_query.run_query',side_effect=RuntimeError('original PG failure')), \
                 patch('src.experiments.postgresql.ready_semantic_query.write_private_json',side_effect=write):
                with self.assertRaisesRegex(RuntimeError,'original PG failure'):
                    run_ready_pg_query(QueryConfig('fixture','pg','map','raw_input'),
                        manifest_path=root/'inputs/manifest.json',model_path=model,
                        budget_path=root/'unused-budget.sqlite',budget=AttemptBudget('ready.fixture',1),
                        root=root/'query',dsn='fixture',pg_log=root/'unused.log')


if __name__=='__main__':unittest.main()
