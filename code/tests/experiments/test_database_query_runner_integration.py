"""Full query runner on real PG/Ray/LOTUS and a controlled local model endpoint."""
from collections import Counter
from contextlib import suppress
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import hashlib
import json
import os
from pathlib import Path
import threading
import unittest
import time
import subprocess
import sys
from dataclasses import asdict

from src.baselines.common.private_artifacts import write_private_json,content_digest
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.query_config import QueryConfig
from src.experiments.postgresql.query_inputs import QueryInputs
from src.experiments.postgresql.query_tables import install_input_table
from src.experiments.postgresql.query_workloads import prepare,read_prepared
from src.experiments.postgresql.query_runner import run_query


@unittest.skipUnless(all(os.environ.get(k) for k in ('SEMLOOM_TEST_PG_DSN','SEMLOOM_TEST_PG_LOG',
    'SEMLOOM_TEST_ARTIFACT_ROOT','SEMLOOM_TEST_SEMBENCH_ROOT','SEMLOOM_TEST_RAY_TMPDIR')),
    'requires an isolated PG, pinned SemBench and owned Ray runtime directory')
class DatabaseQueryRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from tokenizers import Tokenizer,models
        cls.root=Path(os.environ['SEMLOOM_TEST_ARTIFACT_ROOT']);cls.root.mkdir(mode=0o700)
        cls.posts=[];cls.lock=threading.Lock();cls.active=0
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                with cls.lock:
                    cls.posts.append(body);cls.active+=1
                try:
                    text=body['messages'][-1]['content']
                    if 'FORCE_ERROR' in text:
                        self.send_response(500);self.send_header('Content-Length','0');self.end_headers();return
                    good='[GOOD]' in text
                    limit=body.get('max_tokens',body.get('max_completion_tokens'))
                    if limit not in (8,128):
                        raise ValueError('fixture received an undeclared generation limit')
                    output=('POSITIVE' if good else 'NEGATIVE') if limit==128 else ('TRUE' if good else 'FALSE')
                    tokenizer = getattr(cls, 'organization_tokenizer', None)
                    prompt_tokens = (len(tokenizer.apply_chat_template(body['messages'], tokenize=True,
                        add_generation_prompt=True)) if tokenizer else 10)
                    payload=json.dumps(dict(id='fixture',object='chat.completion',created=0,model='fixture-model',
                        choices=[dict(index=0,message=dict(role='assistant',content=output),finish_reason='stop')],
                        usage=dict(prompt_tokens=prompt_tokens,completion_tokens=1,total_tokens=prompt_tokens+1))).encode()
                    time.sleep(.02+prompt_tokens*.0001 if tokenizer else .001)
                    self.send_response(200);self.send_header('Content-Type','application/json')
                    self.send_header('Content-Length',str(len(payload)));self.end_headers()
                    with suppress(BrokenPipeError,ConnectionResetError):self.wfile.write(payload)
                finally:
                    with cls.lock:cls.active-=1
            def log_message(self,*_):pass
        cls.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        cls.thread=threading.Thread(target=cls.server.serve_forever);cls.thread.start()
        cls.addClassCleanup(cls.close_server)
        cls.model=cls.root/'model.json'
        write_private_json(cls.model,dict(endpoint_url=f'http://127.0.0.1:{cls.server.server_port}/v1/chat/completions',
                                         model_id='fixture-model',timeout_ms=3000))
        cls.tokenizer=cls.root/'tokenizer.json';Tokenizer(models.WordLevel({'[UNK]':0},unk_token='[UNK]')).save(str(cls.tokenizer))
        cls.budget=AttemptBudget('fixture.database-query-suite',2000)
        cls.ledger=CellBudgetLedger.create(cls.root/'budget.sqlite',cls.budget,deadline_utc=time.time()+600)
        cls.table='runner_'+hashlib.sha256(str(cls.root).encode()).hexdigest()[:16]
        examples=[(str(i),'taken_3' if i<90 else 'other',('[GOOD]' if i%3==0 else '[BAD]')+f' review {i}',
                   'POSITIVE' if i%3==0 else 'NEGATIVE','original-'+str(i//2)) for i in range(120)]
        prepare(cls.root/'workload','movie',iter(examples),{'source':'synthetic'},max_rows=120)
        cls.manifest=cls.root/'workload/manifest.json'
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
            connection.execute('CREATE EXTENSION IF NOT EXISTS semloom_pg')
            install_input_table(connection,QueryInputs('movie',cls.table,120),read_prepared(cls.manifest,'raw.jsonl'))

    @classmethod
    def close_server(cls):
        cls.server.shutdown();cls.server.server_close();cls.thread.join(3)
        write_private_json(cls.root/'fixture-summary.json',dict(fixture_posts=len(cls.posts),actual_model_posts=0,
            active_http=cls.active,server_stopped=not cls.thread.is_alive(),ledger=cls.ledger.snapshot()))
        if cls.active or cls.thread.is_alive():raise RuntimeError('fixture did not settle')

    def run_arm(self, arm, task, *, manifest=None, table=None, unit=None, total_budget=False, **changes):
        options=dict(concurrency=2,window=4,ray_batch_rows=16,pg_total_budget=total_budget)
        options.update(changes)
        config=QueryConfig(unit or arm+'-'+task,arm,task,table or self.table,**options)
        return run_query(config,manifest_path=manifest or self.manifest,model_path=self.model,
            budget_path=self.ledger.path,budget=self.budget,root=self.root/config.unit_id,dsn=os.environ['SEMLOOM_TEST_PG_DSN'],
            pg_log=os.environ['SEMLOOM_TEST_PG_LOG'],checkout=os.environ['SEMLOOM_TEST_SEMBENCH_ROOT'],
            tokenizer_path=self.tokenizer,ray_temp_root=os.environ['SEMLOOM_TEST_RAY_TMPDIR'])

    def test_map_three_execution_owners(self):
        for arm in ('pg','pg-source-direct','ray-data'):
            with self.subTest(arm=arm):
                result=self.run_arm(arm,'map')
                self.assertEqual(result['status'],'passed')
                self.assertEqual(result['evaluation']['actual_posts'],120)
                self.assertEqual(result['evaluation']['quality']['invalid'],0)
                self.assertEqual(result['evaluation']['quality']['false_positive'],0)
                self.assertEqual(result['evaluation']['quality']['false_negative'],0)
                self.assertLessEqual(result['evaluation']['http']['peak_http'],2)

    def test_total_budget_query_reports_independent_pg_memory(self):
        result=self.run_arm('pg','map',unit='pg-total-map',total_budget=True)
        self.assertEqual(result['status'],'passed')
        self.assertEqual(result['evaluation']['actual_posts'],120)
        memory=result['evaluation']['pg_memory']
        self.assertEqual(memory['operators'],1)
        self.assertLessEqual(memory['sum_retained_peaks'],memory['sum_retained_limits'])

    def test_organization_controls_bind_tokens_groups_and_actual_pg_rows(self):
        import psycopg
        from tokenizers import Tokenizer, models, pre_tokenizers
        from transformers import PreTrainedTokenizerFast
        from src.execution_provider.adapters.map_organization import MapOrganizationConfig, tokenizer_fingerprint
        table=self.table+'_org';root=self.root/'organization-workload'
        examples=[(str(i),'taken_3','[GOOD] '+('word '*(220,4,40,80)[i%4])+str(i),'POSITIVE') for i in range(36)]
        prepare(root,'movie',examples,{'source':'synthetic heterogeneous work'},max_rows=36)
        manifest=root/'manifest.json'
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
            install_input_table(connection,QueryInputs('movie',table,36),read_prepared(manifest,'raw.jsonl'))
        tokenizer=Tokenizer(models.WordLevel({'[UNK]':0},unk_token='[UNK]'))
        tokenizer.pre_tokenizer=pre_tokenizers.Whitespace()
        fast=PreTrainedTokenizerFast(tokenizer_object=tokenizer,unk_token='[UNK]')
        fast.chat_template="{% for message in messages %}{{ message['role'] + ' ' + message['content'] + '\\n' }}{% endfor %}{% if add_generation_prompt %}assistant{% endif %}"
        path=self.root/'organization-tokenizer';fast.save_pretrained(path)
        config=MapOrganizationConfig('rows',16,4,400,512,'fixture-model','fixture-model-v1',
                                    'controlled-http-v1',str(path),tokenizer_fingerprint(path),512)
        type(self).organization_tokenizer=fast
        histories=[]
        try:
            from dataclasses import replace
            for mode in ('rows','work','length'):
                declared=self.root/(mode+'-organization.json')
                write_private_json(declared,asdict(replace(config,mode=mode)))
                result=self.run_arm('pg','map',manifest=manifest,table=table,unit='org-'+mode,total_budget=True,
                    window=16,pg_window_bytes=2097152,pg_staging_bytes=4194304,
                    organization_config=str(declared),organization_sha256=hashlib.sha256(declared.read_bytes()).hexdigest())
                audit=result['evaluation']['organization'];histories.append(audit)
                self.assertEqual(audit['rows'],36)
                self.assertEqual(len(set(audit['http_sequences'])),36)
                self.assertLessEqual(audit['peak_active_work'],512)
                self.assertLessEqual(audit['max_candidate_rows'],16)
                self.assertTrue(audit['server_prompt_tokens_verified'])
                self.assertTrue(audit['declared_generation_budget_verified'])
                self.assertIsNotNone(audit['compute_lifecycle']['capacity_block_counts'])
                self.assertGreater(audit['compute_lifecycle']['work_only_block_count'],0)
                self.assertEqual(result['evaluation']['quality']['false_negative'],0)
            self.assertEqual(histories[0]['submitted_sequences'],list(range(36)))
            self.assertEqual(histories[1]['submitted_sequences'],list(range(36)))
            self.assertNotEqual(histories[2]['submitted_sequences'],list(range(36)))
            write_private_json(self.root/'organization-comparison.json',histories)
        finally:
            type(self).organization_tokenizer=None

    def test_supervised_native_cli(self):
        for arm,task in (('pg-source-direct','map'),('lotus','movie-q3'),('ray-data','map')):
            with self.subTest(arm=arm):
                unit='cli-'+arm
                config=QueryConfig(unit,arm,task,self.table,query_timeout_s=60,ray_batch_rows=16)
                path=self.root/(unit+'.json');write_private_json(path,asdict(config))
                command=[sys.executable,'-m','src.experiments.postgresql.query_cli','run',
                    '--config',str(path),'--manifest',str(self.manifest),'--model',str(self.model),
                    '--budget',str(self.ledger.path),'--budget-id',self.budget.budget_id,
                    '--max-attempts',str(self.budget.limit),'--output',str(self.root/unit),
                    '--dsn-env','SEMLOOM_TEST_PG_DSN','--sembench-checkout',os.environ['SEMLOOM_TEST_SEMBENCH_ROOT'],
                    '--tokenizer',str(self.tokenizer),'--ray-temp-root',os.environ['SEMLOOM_TEST_RAY_TMPDIR']+'c']
                result=subprocess.run(command,capture_output=True,text=True,timeout=180)
                self.assertEqual(result.returncode,0,result.stderr)
                report=json.loads((self.root/unit/'supervisor.json').read_text())
                self.assertEqual(report['status'],'passed')
                self.assertEqual(report['remaining_owned_pids'],[])

    def test_original_count_then_limit_tasks(self):
        for task,posts in (('movie-q3',90),('movie-q1',120),('movie-q2',90)):
            for arm in ('pg','lotus'):
                with self.subTest(arm=arm,task=task):
                    result=self.run_arm(arm,task)
                    audit=result['evaluation']
                    self.assertEqual(result['status'],'passed')
                    self.assertEqual(audit['actual_posts'],13 if arm=='pg' and task!='movie-q3' else posts)
                    self.assertEqual(audit['row_audit']['false_positive'],0)
                    self.assertEqual(audit['row_audit']['false_negative'],0)
                    metric=audit['quality']['original_metric']
                    self.assertEqual(metric['relative_error'] if task=='movie-q3' else metric['precision'],0 if task=='movie-q3' else 1)

    def test_native_and_pg_failures_do_not_retry_a_row(self):
        import psycopg
        table=self.table+'_bad'
        root=self.root/'bad-workload'
        prepare(root,'movie',((str(i),'taken_3',f'FORCE_ERROR row {i}','NEGATIVE') for i in range(4)),
                {'source':'synthetic failure'},max_rows=4)
        manifest=root/'manifest.json'
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
            install_input_table(connection,QueryInputs('movie',table,4),read_prepared(manifest,'raw.jsonl'))
        for arm in ('pg','lotus'):
            before=len(self.posts);unit=arm+'-failure'
            with self.subTest(arm=arm),self.assertRaises(Exception):
                self.run_arm(arm,'movie-q3',manifest=manifest,table=table,unit=unit)
            sent=self.posts[before:]
            self.assertGreater(len(sent),0)
            self.assertTrue(all(n==1 for n in Counter(content_digest(v['messages']) for v in sent).values()))
            summary=json.loads((self.root/unit/'summary.json').read_text())
            self.assertEqual(summary['status'],'failed')
            self.assertNotIn('evaluation',summary)
            execution=json.loads((self.root/unit/'q0/execution.json').read_text())
            self.assertIsNotNone(execution['query_error'])

    def test_zero_eligible_rows_are_successful_without_post(self):
        import psycopg
        table=self.table+'_empty';root=self.root/'empty-workload'
        prepare(root,'movie',[('z','other','[BAD] zero eligible rows','NEGATIVE')],{'source':'synthetic empty selection'},max_rows=1)
        manifest=root/'manifest.json'
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
            install_input_table(connection,QueryInputs('movie',table,1),read_prepared(manifest,'raw.jsonl'))
        for arm in ('pg','lotus'):
            with self.subTest(arm=arm):
                result=self.run_arm(arm,'movie-q3',manifest=manifest,table=table,unit=arm+'-zero')
                self.assertEqual(result['evaluation']['actual_posts'],0)
                self.assertEqual(result['evaluation']['quality']['original_metric']['absolute_error'],0)
