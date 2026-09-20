"""Actual isolated PG and HTTP fixture checks for M1 stages and M2 source controls."""
from contextlib import suppress
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import hashlib
import json
import os
from pathlib import Path
import threading
import time
import unittest

from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.query_inputs import QueryInputs
from src.experiments.postgresql.query_tables import install_input_table
from src.experiments.postgresql.query_workloads import prepare,read_prepared
from src.experiments.postgresql.m1_campaign import run_campaign,SCHEMA
from src.experiments.postgresql.m2_runner import run_information_query
from src.execution_provider.adapters.map_organization import tokenizer_fingerprint


class Tokenizer:
    def apply_chat_template(self,messages,**_):return list(range(len(messages[-1]['content'])+10))


@unittest.skipUnless(all(os.environ.get(k) for k in ('SEMLOOM_INFO_PG_DSN','SEMLOOM_INFO_PG_LOG','SEMLOOM_INFO_TEST_ROOT')),
                     'requires an owned PG18.3 diagnostic instance')
class InformationPathIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root=Path(os.environ['SEMLOOM_INFO_TEST_ROOT']);cls.root.mkdir(mode=0o700)
        cls.count=0;cls.lock=threading.Lock();cls.invalid=False
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                with cls.lock:cls.count+=1
                text=body['messages'][-1]['content'];tokens=len(text)+10
                value='bad-format' if cls.invalid else ('POSITIVE' if '[GOOD]' in text else 'NEGATIVE')
                payload=json.dumps(dict(model='fixture',choices=[dict(message=dict(content=value),finish_reason='stop')],
                    usage=dict(prompt_tokens=tokens,completion_tokens=1))).encode()
                time.sleep(.002+(len(text)%3)*.001)
                self.send_response(200);self.send_header('Content-Length',str(len(payload)));self.end_headers()
                with suppress(BrokenPipeError,ConnectionResetError):self.wfile.write(payload)
            def log_message(self,*_):pass
        cls.server=ThreadingHTTPServer(('127.0.0.1',0),Handler,bind_and_activate=False)
        cls.server.request_queue_size=64;cls.server.server_bind();cls.server.server_activate()
        cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True);cls.thread.start()
        cls.addClassCleanup(cls.server.server_close);cls.addClassCleanup(cls.thread.join,5);cls.addClassCleanup(cls.server.shutdown)
        cls.model=cls.root/'model.json'
        cls.model.write_text(json.dumps(dict(endpoint_url=f'http://127.0.0.1:{cls.server.server_port}/v1/chat/completions',
            model_id='fixture',timeout_ms=5000)))
        cls.inputs=cls.root/'inputs';cls.inputs.mkdir()
        cls.manifests={}
        for split in ('tuning','evaluation'):
            cls.manifests[split]=prepare(cls.inputs/split,'movie',
                ((split+str(i),'movie-'+split,('[GOOD]' if i%2 else '[BAD]')+' '+('x'*(9-i)),
                  'POSITIVE' if i%2 else 'NEGATIVE') for i in range(8)),{},max_rows=8)
        cls.tokenizer=cls.root/'tokenizer';cls.tokenizer.mkdir();(cls.tokenizer/'tokenizer.json').write_text('{}')
        (cls.tokenizer/'tokenizer_config.json').write_text('{}')
        cls.fingerprint=tokenizer_fingerprint(cls.tokenizer)

    def test_m1_screening_actual_pg_and_direct(self):
        groups=[dict(id=prefix+str(c),arm=arm,control='request',capacity=c,event_content='full')
            for prefix,arm in [('p','pg'),('d','pg-source-direct')] for c in (1,2,4)]
        keys=[g['id'] for g in groups]
        budget=AttemptBudget('info-m1',192);path=self.root/'m1-budget.sqlite'
        CellBudgetLedger.create(path,budget,deadline_utc=time.time()+110)
        config=dict(schema=SCHEMA,stage='screening',split='tuning',inputs_root=str(self.inputs),
            output_root=str(self.root/'m1'),model_config=str(self.model),pg_log=os.environ['SEMLOOM_INFO_PG_LOG'],
            model_id='fixture',service_signature='isolated-fixture',
            manifest_sha256={k:v['sha256'] for k,v in self.manifests.items()},
            resources=dict(window=8,input_bytes=8*1048576,result_bytes=8*1048576,
                pg_window_bytes=16*1048576,pg_staging_bytes=4*1048576),groups=groups,
            orders=[keys,keys[1:]+keys[:1],list(reversed(keys)),keys[2:]+keys[:2]],
            selection_policy=dict(epsilon=.03,max_relative_spread=.1,min_repeats=3,
                supply_level=.9,min_supply_fraction=.8,middle_fraction=.6),
            max_posts=192,max_seconds=120,query_timeout_s=10,budget_id=budget.budget_id,budget_path=str(path))
        file=self.root/'m1.json';file.write_text(json.dumps(config))
        from unittest.mock import patch
        before=self.count
        with patch.dict(os.environ,{'SEMLOOM_TEST_PG_DSN':os.environ['SEMLOOM_INFO_PG_DSN']}):run_campaign(file)
        result=json.loads((self.root/'m1/comparison.json').read_text())
        self.assertEqual(result['status'],'completed');self.assertEqual(result['actual_posts'],192)
        self.assertEqual(type(self).count-before,192);self.assertFalse(result['next_stage_started'])
        self.assertEqual(len(result['rows']),24)

    def test_m2_modes_share_actual_snapshot_source_and_preserve_results(self):
        import psycopg
        table='info_'+hashlib.sha256(str(self.root).encode()).hexdigest()[:12]
        with psycopg.connect(os.environ['SEMLOOM_INFO_PG_DSN'],autocommit=True) as connection:
            install_input_table(connection,QueryInputs('movie',table,8),read_prepared(self.inputs/'evaluation/manifest.json','raw.jsonl'))
        budget=AttemptBudget('info-m2',48);ledger_path=self.root/'m2-budget.sqlite'
        CellBudgetLedger.create(ledger_path,budget,deadline_utc=time.time()+120)
        results={};before=self.count
        for mode in ('stream_fifo','window_fifo','window_length','global_length','global_reuse'):
            config=dict(unit_id=mode,table=table,mode=mode,concurrency=3,query_timeout_s=10,
                tokenizer_path=str(self.tokenizer),tokenizer_sha256=self.fingerprint,
                source_limits=dict(candidate_rows=4,payload_bytes=1048576,metadata_bytes=1048576,
                    result_disk_bytes=4*1048576,context_tokens=4096))
            if mode=='global_reuse':config['reuse_metadata']=str(self.root/'global_length/metadata.sqlite')
            result=run_information_query(config,manifest_path=self.inputs/'evaluation/manifest.json',model_path=self.model,
                budget_path=ledger_path,budget=budget,root=self.root/mode,dsn=os.environ['SEMLOOM_INFO_PG_DSN'],tokenizer=Tokenizer())
            self.assertEqual(result['status'],'passed');self.assertEqual(result['evaluation']['actual_posts'],8)
            self.assertTrue(result['output_order_verified']);results[mode]=result['output_values']
        self.assertEqual(type(self).count-before,40)
        self.assertTrue(all(v==results['stream_fifo'] for v in results.values()))
        self.assertEqual(json.loads((self.root/'global_length/source.json').read_text())['source_reads'],16)
        self.assertEqual(json.loads((self.root/'global_reuse/source.json').read_text())['source_reads'],8)
        type(self).invalid=True
        config.update(unit_id='invalid',mode='stream_fifo');config.pop('reuse_metadata')
        try:
            with self.assertRaisesRegex(ValueError,'invalid classification'):
                run_information_query(config,manifest_path=self.inputs/'evaluation/manifest.json',model_path=self.model,
                    budget_path=ledger_path,budget=budget,root=self.root/'invalid',dsn=os.environ['SEMLOOM_INFO_PG_DSN'],tokenizer=Tokenizer())
        finally:type(self).invalid=False
        self.assertEqual(json.loads((self.root/'invalid/summary.json').read_text())['status'],'failed')
