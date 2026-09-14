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
        cls.posts=[];cls.lock=threading.Lock();cls.active=0;cls.http_timings=[]
        cls.delay_by_text={}
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                timing=dict(request_values_sha256=content_digest(body),handler_received_ns=time.monotonic_ns())
                with cls.lock:
                    cls.posts.append(body);cls.active+=1
                    cls.http_timings.append(timing)
                capacity = getattr(cls, 'fixture_execution_capacity', None)
                if capacity is not None:
                    capacity.acquire()
                timing['service_started_ns']=time.monotonic_ns()
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
                    time.sleep(cls.delay_by_text.get(text,.02+prompt_tokens*.0001 if tokenizer else .001))
                    self.send_response(200);self.send_header('Content-Type','application/json')
                    self.send_header('Content-Length',str(len(payload)));self.end_headers()
                    with suppress(BrokenPipeError,ConnectionResetError):self.wfile.write(payload)
                finally:
                    timing['handler_finished_ns']=time.monotonic_ns()
                    if capacity is not None:
                        capacity.release()
                    with cls.lock:cls.active-=1
            def log_message(self,*_):pass
        cls.server=ThreadingHTTPServer(('127.0.0.1',0),Handler,bind_and_activate=False)
        cls.addClassCleanup(cls.server.server_close)
        backlog=os.environ.get('SEMLOOM_TEST_HTTP_BACKLOG')
        if backlog is not None:
            cls.server.request_queue_size=int(backlog)
            if cls.server.request_queue_size<1:
                cls.server.server_close()
                raise ValueError('positive fixture listen backlog required')
        cls.server.server_bind();cls.server.server_activate()
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
        write_private_json(cls.root/'fixture-http-timing.json',dict(listen_backlog=cls.server.request_queue_size,
            rows=cls.http_timings,scope='handler input parsed, execution semaphore acquired, handler response finished'))
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

    def make_organization_fixture(self):
        from tokenizers import Tokenizer, models, pre_tokenizers
        from transformers import PreTrainedTokenizerFast
        from src.execution_provider.adapters.map_organization import MapOrganizationConfig, tokenizer_fingerprint
        tokenizer=Tokenizer(models.WordLevel({'[UNK]':0},unk_token='[UNK]'))
        tokenizer.pre_tokenizer=pre_tokenizers.Whitespace()
        fast=PreTrainedTokenizerFast(tokenizer_object=tokenizer,unk_token='[UNK]')
        fast.chat_template="{% for message in messages %}{{ message['role'] + ' ' + message['content'] + '\\n' }}{% endfor %}{% if add_generation_prompt %}assistant{% endif %}"
        path=self.root/'organization-tokenizer';fast.save_pretrained(path)
        config=MapOrganizationConfig('rows',16,4,400,512,'fixture-model','fixture-model-v1',
                                    'controlled-http-v1',str(path),tokenizer_fingerprint(path),512)
        return config,fast

    def test_organized_empty_selection_keeps_provider_lazy_open(self):
        from dataclasses import replace
        config,fast=self.make_organization_fixture()
        type(self).organization_tokenizer=fast
        try:
            for mode in ('rows','work','length'):
                declared=self.root/('empty-'+mode+'-organization.json')
                write_private_json(declared,asdict(replace(config,mode=mode)))
                unit='org-empty-'+mode
                result=self.run_arm('pg','map',unit=unit,total_budget=True,movie_id='absent',window=16,
                    organization_config=str(declared),organization_sha256=hashlib.sha256(declared.read_bytes()).hexdigest())
                self.assertEqual(result['status'],'passed')
                self.assertEqual(result['evaluation']['actual_posts'],0)
                self.assertEqual(result['evaluation']['organization']['rows'],0)
                self.assertEqual(result['evaluation']['drained_jobs'],0)
                events=[json.loads(line) for line in (self.root/unit/'events.jsonl').read_text().splitlines()]
                self.assertFalse(any(e['event'] in ('core_job_opened','core_submitted','request') for e in events))
                sessions=(self.root/unit/'sessions.jsonl').read_text().splitlines()
                self.assertEqual(sessions,[])
        finally:
            type(self).organization_tokenizer=None

    def test_organization_controls_bind_tokens_groups_and_actual_pg_rows(self):
        import psycopg
        table=self.table+'_org';root=self.root/'organization-workload'
        examples=[(str(i),'taken_3','[GOOD] '+('word '*(220,4,40,80)[i%4])+str(i),'POSITIVE') for i in range(36)]
        prepare(root,'movie',examples,{'source':'synthetic heterogeneous work'},max_rows=36)
        manifest=root/'manifest.json'
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
            install_input_table(connection,QueryInputs('movie',table,36),read_prepared(manifest,'raw.jsonl'))
        config,fast=self.make_organization_fixture()
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

    @unittest.skipUnless(os.environ.get('SEMLOOM_TEST_FLOW_DIAGNOSTIC') == '1',
                         'requires the explicitly instrumented PG test build')
    def test_controlled_input_and_ready_delivery_timeline(self):
        from dataclasses import replace
        from unittest.mock import patch
        import psycopg
        from src.experiments.postgresql import query_runner
        from src.experiments.postgresql.flow_timing import analyze_flow
        table=self.table+'_flow';root=self.root/'flow-workload'
        texts=['[GOOD] flow row '+str(i) for i in range(8)]
        prepare(root,'movie',[(str(i),'taken_3',text,'POSITIVE') for i,text in enumerate(texts)],
                {'source':'synthetic input/delivery flow diagnostic'},max_rows=8)
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM pg_settings WHERE name LIKE 'semloom_pg.test_flow_%'").fetchone()[0],4)
            install_input_table(connection,QueryInputs('movie',table,8),read_prepared(root/'manifest.json','raw.jsonl'))
        config,fast=self.make_organization_fixture()
        config=replace(config,window_rows=4,batch_rows=4,batch_work=2048,active_work=2048)
        declared=self.root/'flow-organization.json';write_private_json(declared,asdict(config))
        original=query_runner.run_pg
        reports=[];before=len(self.posts)
        type(self).organization_tokenizer=fast
        type(self).delay_by_text={text:.12 if i==0 else .002 for i,text in enumerate(texts)}
        try:
            for repeat in range(2):
                for pause in (-1,3,4):
                    for ready_first in ((False,True) if repeat==0 else (True,False)):
                        unit=f'flow-{repeat}-{pause}-'+('ready' if ready_first else 'current')
                        settings=dict(pause_input=pause,pause_ms=80,ready_first=ready_first,
                                      clock_source=time.get_clock_info('monotonic').implementation)
                        def run_pg(configuration,inputs,plan,connection,pg_log,model_path,ledger,output,errors):
                            for name,value in (('test_flow_trace','on'),('test_flow_ready_first','on' if ready_first else 'off'),
                                               ('test_flow_pause_input',str(pause)),('test_flow_pause_ms','80')):
                                connection.execute('SELECT set_config(%s,%s,false)',('semloom_pg.'+name,value))
                            write_private_json(output/'flow-settings.json',settings)
                            return original(configuration,inputs,plan,connection,pg_log,model_path,ledger,output,errors)
                        with patch.object(query_runner,'run_pg',run_pg):
                            self.run_arm('pg','map',unit=unit,table=table,manifest=root/'manifest.json',
                                total_budget=True,window=4,concurrency=4,max_posts=8,query_timeout_s=10,
                                pg_window_bytes=8388608,pg_staging_bytes=4194304,
                                organization_config=str(declared),organization_sha256=hashlib.sha256(declared.read_bytes()).hexdigest())
                        result=analyze_flow(self.root/unit,expected_rows=8,window=4)
                        self.assertEqual(result['startup_candidate_rows'],[4])
                        write_private_json(self.root/unit/'flow-timing.json',result)
                        reports.append(dict(unit=unit,repeat=repeat,**result))
            self.assertEqual(len(self.posts)-before,96)
            write_private_json(self.root/'flow-comparison.json',reports)
        finally:
            type(self).organization_tokenizer=None
            type(self).delay_by_text={}

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

    @unittest.skipUnless(os.environ.get('SEMLOOM_TEST_FLOW_DIAGNOSTIC') == '1',
                         'requires the explicitly instrumented PG test build')
    def test_waiting_positions_pilot(self):
        from dataclasses import replace
        from statistics import median
        from unittest.mock import patch
        import psycopg
        from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION
        from src.execution_provider.semantic_map import canonical_messages
        from src.experiments.postgresql import query_runner
        from src.experiments.postgresql.waiting_positions import analyze_waiting_positions
        config, tokenizer = self.make_organization_fixture()
        type(self).organization_tokenizer = tokenizer
        type(self).fixture_execution_capacity = threading.BoundedSemaphore(4)
        prepared = {}
        for role, count in (('tuning',16),('evaluation',32)):
            root = self.root / ('waiting-'+role)
            texts = ['[GOOD] '+role+' '+('word '*(24,80,144,224)[i%4])+str(i) for i in range(count)]
            prepare(root,'movie',[(role+'-'+str(i),'taken_3',text,'POSITIVE') for i,text in enumerate(texts)],
                    {'source':'synthetic waiting fixture; not independent semantic evaluation'},max_rows=count)
            table = self.table + '_' + role
            with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
                install_input_table(connection,QueryInputs('movie',table,count),read_prepared(root/'manifest.json','raw.jsonl'))
            work = [len(tokenizer.apply_chat_template(json.loads(canonical_messages(MOVIE_MAP_INSTRUCTION,text)),
                        tokenize=True,add_generation_prompt=True))+128 for text in texts]
            self.assertLessEqual(max(work),config.context_tokens)
            prepared[role] = dict(manifest=root/'manifest.json',table=table,work=work,count=count)
        window, capacities = 8, (2,4,8)
        wide = max(config.context_tokens, sum(sorted(prepared['evaluation']['work'],reverse=True)[:max(capacities)]))
        reports = []
        original = query_runner.run_pg

        def trace_pg(configuration,inputs,plan,connection,pg_log,model_path,ledger,output,errors):
            connection.execute('SET semloom_pg.test_flow_trace=on')
            write_private_json(output/'flow-settings.json',dict(pause_input=-1,pause_ms=0,ready_first=False,
                clock_source=time.get_clock_info('monotonic').implementation))
            return original(configuration,inputs,plan,connection,pg_log,model_path,ledger,output,errors)

        def run(role, arm, capacity, repeat):
            data = prepared[role]
            unit = f'waiting-{role}-{arm}-{capacity}-{repeat}'
            options = {}
            if arm != 'request':
                organization = replace(config, mode='rows',window_rows=window,batch_rows=window,
                    active_work=wide if arm=='token-wide' else config.context_tokens,batch_work=wide)
                declared = self.root/(unit+'-organization.json')
                write_private_json(declared,asdict(organization))
                options.update(organization_config=str(declared),organization_sha256=hashlib.sha256(declared.read_bytes()).hexdigest())
            with patch.object(query_runner,'run_pg',trace_pg):
                result = self.run_arm('pg','map',unit=unit,table=data['table'],manifest=data['manifest'],
                    total_budget=True,window=window,concurrency=capacity,max_posts=data['count'],query_timeout_s=15,
                    input_bytes=8388608,result_bytes=8388608,pg_window_bytes=8388608,pg_staging_bytes=4194304,**options)
            write_private_json(self.root/unit/'waiting-contract.json',dict(
                eligibility='sealed-immutable-full-scan-at-invocation',manifest_sha256=result['manifest_sha256'],
                role=role,arm=arm,capacity=capacity,repeat=repeat,driver='persistent',gateway='new-per-query'))
            timing = analyze_waiting_positions(self.root/unit,expected_rows=data['count'],window=window)
            audit = result['evaluation'].get('organization')
            if audit:
                self.assertEqual(audit['submitted_sequences'],list(range(data['count'])))
                blocks = audit['compute_lifecycle']['work_only_block_count']
                self.assertEqual(blocks == 0, arm == 'token-wide')
            self.assertEqual(result['evaluation']['quality']['false_negative'],0)
            write_private_json(self.root/unit/'waiting-positions.json',timing)
            report = dict(unit=unit,role=role,arm=arm,capacity=capacity,repeat=repeat,timing=timing,
                          evaluation=result['evaluation'])
            reports.append(report)
            return report

        before = len(self.posts)
        try:
            prior_selection = os.environ.get('SEMLOOM_TEST_WAITING_SELECTION')
            if prior_selection:
                prior = json.loads(Path(prior_selection).read_text())
                self.assertEqual(prior['evaluation_work'],prepared['evaluation']['work'])
                self.assertEqual(prior['wide_work'],wide)
                self.assertEqual(prior['tight_work'],config.context_tokens)
                selected = prior['selected']
                self.assertIn(selected,capacities)
            else:
                for repeat, order in enumerate((capacities, tuple(reversed(capacities)))):
                    for capacity in order:
                        run('tuning','request',capacity,repeat)
                selected = min(capacities,key=lambda c:median(r['timing']['preparation_to_eof_seconds']
                    for r in reports if r['capacity']==c))
            # A smaller work cap must admit each item and still be able to refuse a feasible C-set.
            self.assertGreater(sum(sorted(prepared['evaluation']['work'],reverse=True)[:selected]),config.context_tokens)
            write_private_json(self.root/'waiting-selection.json',dict(capacities=list(capacities),selected=selected,
                rule='preserved prior selection for transport diagnostic' if prior_selection else 'minimum median invocation-to-EOF of two tuning queries',
                wide_work=wide,tight_work=config.context_tokens,listen_backlog=self.server.request_queue_size,
                evaluation_work=prepared['evaluation']['work'],performance_qualified=False))
            orders = (('request','token-wide','token-tight'),('token-tight','request','token-wide'),
                      ('token-wide','token-tight','request'))
            for repeat, order in enumerate(orders):
                for arm in order:
                    run('evaluation',arm,selected,repeat)
            self.assertEqual(len(self.posts)-before,288 if prior_selection else 384)
            write_private_json(self.root/'waiting-comparison.json',reports)
        finally:
            type(self).organization_tokenizer = None
            type(self).fixture_execution_capacity = None

    def test_waiting_positions_persistent_gateway(self):
        from contextlib import ExitStack
        from dataclasses import replace
        import psycopg
        from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION
        from src.execution_provider.semantic_map import SemanticMapPlan
        from src.experiments.postgresql.persistent_gateway import PersistentMapGateway
        from src.experiments.postgresql.waiting_positions import analyze_waiting_positions
        organization, tokenizer = self.make_organization_fixture()
        type(self).organization_tokenizer = tokenizer
        type(self).fixture_execution_capacity = threading.BoundedSemaphore(4)
        workload = self.root/'persistent-input'
        texts = ['[GOOD] evaluation '+('word '*(24,80,144,224)[i%4])+str(i) for i in range(32)]
        prepare(workload,'movie',[(f'evaluation-{i}','taken_3',text,'POSITIVE') for i,text in enumerate(texts)],
                {'source':'synthetic waiting fixture; not independent semantic evaluation'},max_rows=32)
        table = self.table+'_warm'
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
            install_input_table(connection,QueryInputs('movie',table,32),read_prepared(workload/'manifest.json','raw.jsonl'))
        arms = (('request-c4',4,None),('request-c8',8,None),('wide-c8',8,3096),('tight-c8',8,512))
        orders = ((0,1,2,3),(0,1,2,3),(1,2,3,0),(2,3,0,1),(3,0,1,2),(0,1,2,3))
        write_private_json(self.root/'persistent-design.json',dict(arms=arms,orders=orders,warmup_round=0,
            queries_per_group=6,fixture_posts=768,actual_model_posts=0,service_slots=4,backlog=self.server.request_queue_size))
        reports = []
        before = len(self.posts)
        try:
            with ExitStack() as stack:
                groups = []
                for name, capacity, work in arms:
                    options = {}
                    if work is not None:
                        declared = self.root/(name+'-organization.json')
                        write_private_json(declared,asdict(replace(organization,mode='rows',window_rows=8,
                            batch_rows=8,active_work=work,batch_work=3096)))
                        options.update(organization_config=str(declared),organization_sha256=hashlib.sha256(declared.read_bytes()).hexdigest())
                    config = QueryConfig(name,'pg','map',table,concurrency=capacity,window=8,pg_total_budget=True,
                        input_bytes=8388608,result_bytes=8388608,pg_window_bytes=8388608,pg_staging_bytes=4194304,
                        query_timeout_s=15,**options)
                    groups.append(stack.enter_context(PersistentMapGateway(config,
                        plan=SemanticMapPlan(MOVIE_MAP_INSTRUCTION,'fixture-model',128),manifest_path=workload/'manifest.json',
                        model_path=self.model,ledger=self.ledger,root=self.root/name,query_count=6)))
                pids = {arms[i][0]:group.gateway.pid for i,group in enumerate(groups)}
                for repeat, order in enumerate(orders):
                    for index in order:
                        group = groups[index]
                        unit = f'query-{repeat}'
                        result = group.run_query(unit,dsn=os.environ['SEMLOOM_TEST_PG_DSN'],
                            pg_log=os.environ['SEMLOOM_TEST_PG_LOG'],trace_flow=True,peer_pids=pids)
                        output = group.root/unit
                        write_private_json(output/'waiting-contract.json',dict(
                            eligibility='sealed-immutable-full-scan-at-invocation',manifest_sha256=result['manifest_sha256'],
                            role='warmup' if repeat==0 else 'measurement',arm=arms[index][0],repeat=repeat,
                            driver='persistent',gateway='persistent'))
                        timing = analyze_waiting_positions(output,expected_rows=32,window=8)
                        write_private_json(output/'waiting-positions.json',timing)
                        self.assertEqual(result['evaluation']['quality']['false_negative'],0)
                        audit = result['evaluation'].get('organization')
                        if audit:
                            self.assertEqual(audit['submitted_sequences'],list(range(32)))
                            self.assertEqual(audit['compute_lifecycle']['work_only_block_count']==0,index==2)
                        reports.append(dict(group=arms[index][0],unit=unit,repeat=repeat,warmup=repeat==0,
                            timing=timing,evaluation=result['evaluation']))
            self.assertEqual(len(self.posts)-before,768)
            write_private_json(self.root/'persistent-comparison.json',reports)
        finally:
            type(self).organization_tokenizer = None
            type(self).fixture_execution_capacity = None

    def test_persistent_compact_observation(self):
        from dataclasses import replace
        import psycopg
        from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION
        from src.execution_provider.semantic_map import SemanticMapPlan
        from src.experiments.postgresql.persistent_gateway import PersistentMapGateway
        from src.experiments.postgresql.query_evaluation import _evaluate_rows
        organization, tokenizer = self.make_organization_fixture()
        type(self).organization_tokenizer = tokenizer
        data = self.root/'observation-input'
        prepare(data,'movie',[(str(i),'fixture-movie',('[GOOD]' if i%2 else '[BAD]')+f' review {i}',
            'POSITIVE' if i%2 else 'NEGATIVE') for i in range(16)],{'source':'controlled observation comparison'},max_rows=16)
        table=self.table+'_observation'
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
            install_input_table(connection,QueryInputs('movie',table,16),read_prepared(data/'manifest.json','raw.jsonl'))
        before=len(self.posts)
        outputs=[]
        try:
            for organized in (False,True):
                options={}
                if organized:
                    path=self.root/'compact-organization.json'
                    write_private_json(path,asdict(replace(organization,window_rows=8,batch_rows=8,active_work=512,batch_work=2048)))
                    options.update(organization_config=str(path),organization_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
                for mode in ('full','compact'):
                    config=QueryConfig(f'observation-{organized}-{mode}','pg','map',table,concurrency=4,window=8,
                        pg_total_budget=True,event_content=mode,query_timeout_s=15,**options)
                    plan=SemanticMapPlan(MOVIE_MAP_INSTRUCTION,'fixture-model',128)
                    with PersistentMapGateway(config,plan=plan,manifest_path=data/'manifest.json',model_path=self.model,
                            ledger=self.ledger,root=self.root/config.unit_id,query_count=2) as group:
                        for repeat in range(2):
                            summary=group.run_query(f'q-{repeat}',dsn=os.environ['SEMLOOM_TEST_PG_DSN'],
                                pg_log=os.environ['SEMLOOM_TEST_PG_LOG'],trace_flow=mode=='full')
                            output=group.root/f'q-{repeat}'
                            predictions=[json.loads(line)['row'] for line in (output/'q0/results.jsonl').read_text().splitlines()]
                            outputs.append(predictions)
                            self.assertEqual(summary['evaluation']['association']['matched_rows'],16)
                            self.assertEqual(summary['evaluation']['quality']['invalid'],0)
                            if mode=='compact':
                                events=[json.loads(line) for line in (output/'events.jsonl').read_text().splitlines()]
                                self.assertTrue(all('body' not in e and 'raw_output' not in e for e in events))
                                if organized:self.assertEqual(summary['evaluation']['organization_observation']['status'],'unavailable')
                                # A fresh evaluation cannot accept a changed actual request digest.
                                request=next(e for e in events if e['event']=='request')
                                request['request_values_sha256']='0'*64
                                from unittest.mock import patch
                                with patch('src.experiments.postgresql.query_evaluation.read_events',return_value=events):
                                    with self.assertRaisesRegex(ValueError,'request multiset'):
                                        _evaluate_rows(config,QueryInputs('movie',table,16),plan,data/'manifest.json',output,None,predictions)
            self.assertEqual(len(self.posts)-before,128)
            self.assertTrue(all(value==outputs[0] for value in outputs))
        finally:
            type(self).organization_tokenizer=None

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
