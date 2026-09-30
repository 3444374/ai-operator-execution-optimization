"""Actual native candidate supply on a held local HTTP fixture, never a model."""
from contextlib import suppress
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time
import unittest

from src.baselines.common.private_artifacts import write_private_json
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.query_inputs import QueryInputs
from src.experiments.postgresql.query_tables import install_input_table
from src.experiments.postgresql.query_workloads import prepare,read_prepared,file_identity
from src.experiments.postgresql.query_supervisor import supervise
from src.experiments.postgresql.text_map_candidates import CAPACITIES,native_candidate
from src.experiments.postgresql.text_map_comparison import read_run


@unittest.skipUnless(os.environ.get('SEMLOOM_TEST_TEXT_MAP_ROOT'),'requires explicit isolated PG/Ray fixture')
class NativeSupplyTests(unittest.TestCase):
    def test_declared_native_candidates_reach_capacity_and_settle(self):
        import psycopg
        root=Path(os.environ['SEMLOOM_TEST_TEXT_MAP_ROOT']);root.mkdir(mode=0o700)
        roles=tuple(os.environ.get('SEMLOOM_TEST_NATIVE_ROLES','ray-data,daft-native').split(','))
        if not roles or len(set(roles))!=len(roles) or any(r not in ('ray-data','daft-native') for r in roles):
            raise ValueError('explicit native fixture roles required')
        budget=AttemptBudget('native-supply-fixture',len(roles)*1536)
        ledger=CellBudgetLedger.create(root/'budget.sqlite',budget,deadline_utc=time.time()+720)
        manifest=prepare(root/'inputs','movie',[(str(i),'fixture-movie','[GOOD] 评论' if i%2==0 else '[BAD] review',
            'POSITIVE' if i%2==0 else 'NEGATIVE','original-'+str(i//2)) for i in range(512)],
            dict(source='synthetic held HTTP fixture; no real model'),max_rows=512)
        path=root/'inputs/manifest.json';table='native_'+hashlib.sha256(str(root).encode()).hexdigest()[:12]
        with psycopg.connect(os.environ['SEMLOOM_TEST_PG_DSN'],autocommit=True) as connection:
            install_input_table(connection,QueryInputs('movie',table,512),read_prepared(path,'raw.jsonl'))
        lock=threading.Lock();release=threading.Event()
        state=dict(active=0,peak=0,target=16,posts=0,expired=0)

        class Handler(BaseHTTPRequestHandler):
            def do_POST(handler):
                body=json.loads(handler.rfile.read(int(handler.headers['Content-Length'])))
                with lock:
                    state['posts']+=1;state['active']+=1
                    state['peak']=max(state['peak'],state['active'])
                    if state['active']>=state['target']:release.set()
                try:
                    if not release.wait(25):
                        with lock:state['expired']+=1
                        handler.send_error(504,'candidate did not reach its declared native concurrency')
                        return
                    output='POSITIVE' if '[GOOD]' in body['messages'][-1]['content'] else 'NEGATIVE'
                    payload=json.dumps(dict(model='fixture-model',choices=[dict(message=dict(content=output),finish_reason='stop')],
                        usage=dict(prompt_tokens=10,completion_tokens=1))).encode()
                    handler.send_response(200);handler.send_header('Content-Type','application/json')
                    handler.send_header('Content-Length',str(len(payload)));handler.end_headers()
                    with suppress(BrokenPipeError,ConnectionResetError):handler.wfile.write(payload)
                finally:
                    with lock:state['active']-=1

            def log_message(self,*_):pass

        class Server(ThreadingHTTPServer):request_queue_size=512
        server=Server(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        model=root/'model.json';write_private_json(model,dict(model_id='fixture-model',
            endpoint_url=f'http://127.0.0.1:{server.server_port}/v1/chat/completions',timeout_ms=30000))
        completed=[]
        try:
            for role in roles:
                for capacity in CAPACITIES:
                    with self.subTest(role=role,capacity=capacity):
                        release.clear()
                        with lock:
                            self.assertEqual(state['active'],0)
                            state.update(peak=0,target=capacity);before=state['posts']
                        candidate=native_candidate(role,capacity,table,512,ray_address=os.environ['SEMLOOM_TEST_RAY_ADDRESS'])
                        cfg=candidate['config'];unit=candidate['id'];cfg_path=root/(unit+'.json');write_private_json(cfg_path,cfg)
                        output=root/unit;output.mkdir(mode=0o700)
                        command=[sys.executable,'-m','src.experiments.postgresql.query_cli','run','--worker',
                            '--config',str(cfg_path),'--manifest',str(path),'--model',str(model),
                            '--budget',str(ledger.path),'--budget-id',budget.budget_id,'--max-attempts',str(budget.limit),
                            '--output',str(output),'--dsn-env','SEMLOOM_TEST_PG_DSN']
                        try:
                            supervise(command,output,lambda:ledger.close_shared_unit(unit),query_timeout_s=120,max_duration_s=150)
                        finally:
                            write_private_json(output/'fixture-state.json',dict(state))
                        summary=output/'unit/summary.json';record=read_run(dict(path=str(summary),sha256=file_identity(summary)['sha256']),role)
                        self.assertEqual(state['peak'],capacity);self.assertEqual(state['expired'],0)
                        self.assertEqual(state['posts']-before,512)
                        self.assertEqual(record['http']['peak_http'],capacity)
                        self.assertEqual(record['quality']['true_positive']+record['quality']['true_negative'],512)
                        completed.append(dict(role=role,capacity=capacity,peak_http=state['peak'],posts=512,
                            config=cfg,summary_sha256=file_identity(summary)['sha256']))
                        write_private_json(root/f'completed-{len(completed)}.json',completed[-1])
                    if len(completed)<roles.index(role)*3+CAPACITIES.index(capacity)+1:
                        self.fail('first failed candidate stops the fixture campaign')
        finally:
            release.set();server.shutdown();server.server_close();thread.join(5)
            write_private_json(root/'result.json',dict(completed=completed,fixture=state,model_requests=0,
                budget=ledger.snapshot(),status='passed' if len(completed)==len(roles)*3 and not state['active'] else 'failed'))
