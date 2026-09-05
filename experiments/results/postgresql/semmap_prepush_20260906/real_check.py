"""One bounded pre-push check using only current shared experiment interfaces.

All machine/service/ledger inputs come from a private settings file. This is a
single-run driver; it neither launches the model nor changes the request budget.
"""
import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from urllib.parse import urlsplit,urlunsplit

import psycopg
from psycopg import sql
from transformers import AutoTokenizer

from src.experiments.attempt_ledger import AttemptBudget, AttemptLedger
from src.experiments.postgresql.resource_lifecycle import RunSpec
from src.experiments.postgresql.resource_phase import execute_phase, hashes, save_json
from src.experiments.postgresql.runtime_helpers import isolated_pg18_cluster, owned_child_process, wait_for_path
from src.experiments.postgresql.provider_session_attribution import load_session_events
from src.observability.process_resources.model import PgFileClassificationContext
from src.observability.process_resources.recorder import ProcfsTickSampler

INSTRUCTION = 'Return only the input text exactly as received. Do not add, remove, translate, normalize, or explain anything.'
CANCEL_INSTRUCTION = 'Write exactly 128 numbered one-word items and do not stop early.'
CASES = [('warmup','warmup',INSTRUCTION), ('unicode','数据库与人工智能',INSTRUCTION),
         ('empty','',INSTRUCTION), ('insert','Hello, SemLoom.',INSTRUCTION),
         ('cancel','cancel this generation',CANCEL_INSTRUCTION),
         ('cancel_recovery','after cancel',INSTRUCTION), ('reject','token '*18000,INSTRUCTION),
         ('reject_recovery','after model error',INSTRUCTION)]
REQUIRED = ['select','insert','cancel','cancel_recovery','reject','reject_recovery']


def events(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def map_node(plan):
    if isinstance(plan,list):
        for item in plan:
            answer=map_node(item)
            if answer:return answer
    if isinstance(plan,dict):
        if plan.get('Semantic Spec') == 'semloom.semantic.sem_map.generate.v1': return plan
        for value in plan.values():
            if isinstance(value,(list,dict)):
                answer=map_node(value)
                if answer:return answer
    return None


def model_idle(endpoint, timeout=60):
    url=urlsplit(endpoint)
    url=urlunsplit((url.scheme,url.netloc,'/metrics','',''))
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        with urllib.request.urlopen(url,timeout=5) as response:
            text=response.read().decode()
        counts={}
        for line in text.splitlines():
            match=re.fullmatch(r'vllm:num_requests_(running|waiting)(?:\{.*\})? ([0-9.e+-]+)',line)
            if match:counts[match[1]]=counts.get(match[1],0)+float(match[2])
        assert set(counts)=={'running','waiting'}, 'missing_queue_metrics'
        if all(value==0 for value in counts.values()):return counts
        time.sleep(.1)
    raise RuntimeError('model_queue_not_idle')


def assert_service(settings):
    identity=json.loads(Path(settings['service_verification']).read_text())
    configuration=json.loads(Path(settings['config']).read_text())
    assert configuration['endpoint_url']==identity['endpoint_url']==f"http://127.0.0.1:{settings['model_port']}/v1/chat/completions", 'model_endpoint_binding'
    assert configuration['timeout_ms']==120000,'model_timeout'
    process=Path('/proc')/str(identity['pid'])
    stat=(process/'stat').read_text()
    assert int(stat[stat.rfind(')')+2:].split()[19])==identity['start_time_ticks'],'model_process_changed'
    assert hashlib.sha256((process/'cmdline').read_bytes()).hexdigest()==identity['cmdline_sha256'],'model_command_changed'
    assert hashlib.sha256(Path(settings['config']).read_bytes()).hexdigest()==identity['config_sha256'],'model_config_changed'
    assert identity['verified'] and identity['model_revision']==settings['model_revision'],'unverified_service'
    return identity


def run(settings):
    root=Path(settings['root'])
    root.mkdir()
    summary={'status':'incomplete','runtime_commit':settings['source_commit'], 'required_phases':REQUIRED,
             'phases':{},'model_requests':None,'request_limit':settings['budget_limit'],
             'quality_evaluated':False,'performance_evaluated':False,'response_delay_ms':100}
    ledger=None
    try:
        repo=Path(settings['source'])
        assert subprocess.check_output(['git','-C',str(repo),'rev-parse','HEAD'],text=True).strip()==settings['source_commit'],'source_commit'
        assert not subprocess.check_output(['git','-C',str(repo),'status','--porcelain'],text=True).strip(),'source_dirty'
        ledger=AttemptLedger(Path(settings['ledger']),AttemptBudget(settings['budget_id'],settings['budget_limit']))
        assert ledger.attempts==0 and settings['budget_limit']==len(CASES),'fresh_bounded_budget'
        config=json.loads(Path(settings['config']).read_text())
        identity=assert_service(settings)
        save_json(root/'service-identity.json',identity)
        save_json(root/'manifest.json',{'runtime_commit':settings['source_commit'], 'budget_id':settings['budget_id'],
            'limit':settings['budget_limit'],'pg_port':settings['pg_port'],'config_sha256':identity['config_sha256'],
            'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'response_delay_ms':100})
        tokenizer=AutoTokenizer.from_pretrained(settings['model_root'],local_files_only=True)
        token_counts={}
        for name,text,instruction in CASES:
            messages=[{'role':'system','content':instruction},{'role':'user','content':text}]
            count=len(tokenizer.apply_chat_template(messages,tokenize=True,add_generation_prompt=True,return_dict=False))
            token_counts[name]=count
            assert (count+128>4096) if name=='reject' else (count+128<=4096),'context_budget'
        save_json(root/'token-preflight.json',token_counts)
        model_idle(config['endpoint_url'])
        user=pwd.getpwuid(os.getuid())
        options={'model':config['model_id'],'temperature':0,'max_tokens':128}
        def expression(value, instruction=INSTRUCTION):
            return sql.SQL('ai_semantic.map({},{},{}::jsonb)').format(value,sql.Literal(instruction),sql.Literal(json.dumps(options)))
        with isolated_pg18_cluster(Path(settings['prefix']),root,user,port=settings['pg_port']) as connection, \
             psycopg.connect(host=str(root/'socket'),port=connection.info.port,user=connection.info.user,
                             dbname=connection.info.dbname,autocommit=True) as audit:
            for table in ('resource_rows','insert_rows','map_sink'):
                connection.execute(sql.SQL('CREATE TABLE {} (id integer, payload text) WITH (autovacuum_enabled=false, toast.autovacuum_enabled=false)').format(sql.Identifier(table)))
            with connection.cursor() as cursor:
                cursor.executemany('INSERT INTO resource_rows VALUES (%s,%s)',[(1,'数据库与人工智能'),(2,''),(3,None)])
                cursor.executemany('INSERT INTO insert_rows VALUES (%s,%s)',[(4,'Hello, SemLoom.'),(5,None)])
            connection.execute('INSERT INTO map_sink VALUES (-1,NULL)')
            connection.execute('DELETE FROM map_sink')
            for table in ('resource_rows','insert_rows','map_sink'):
                connection.execute(sql.SQL('SELECT * FROM {}').format(sql.Identifier(table))).fetchall()
            filenodes=connection.execute("SELECT relfilenode FROM pg_class WHERE relnamespace='public'::regnamespace AND relfilenode<>0").fetchall()
            context=PgFileClassificationContext(str(root/'data'),frozenset(row[0] for row in filenodes),frozenset())
            socket_path=root/'socket/provider.sock'
            sessions,http_events=root/'sessions.jsonl',root/'http-events.jsonl'
            command=[sys.executable,'-m','src.experiments.choice_gateway_observer','--events',str(http_events),
                '--session-events',str(sessions),'--ledger',settings['ledger'],'--budget-id',settings['budget_id'],
                '--max-attempts',str(settings['budget_limit']),'--','--socket',str(socket_path),
                '--fixed-model-config',settings['config'],'--test-response-delay-ms','100']
            with owned_child_process(command,root,'gateway',dict(os.environ,PYTHONPATH=str(repo/'code')),user) as gateway:
                connection.execute("SET semloom_pg.provider_execution_profile='openai-compatible-fixed'")
                connection.execute("SET statement_timeout='120s'")
                wait_for_path(socket_path,gateway)
                connection.execute("SELECT set_config('semloom_pg.gateway_socket',%s,false)",(str(socket_path),))
                expr=expression(sql.Identifier('payload'))
                insert_sql=sql.SQL('INSERT INTO map_sink SELECT id,{} FROM insert_rows').format(expr)
                connection.execute(sql.SQL('EXPLAIN SELECT {} FROM resource_rows').format(expr)).fetchall()
                connection.execute(sql.SQL('EXPLAIN {}').format(insert_sql)).fetchall()
                assert connection.execute(sql.SQL('SELECT {} FROM resource_rows LIMIT 0').format(expr)).fetchall()==[]
                assert connection.execute(sql.SQL('SELECT {} FROM resource_rows WHERE id=3').format(expr)).fetchall()==[(None,)]
                assert ledger.attempts==0 and events(http_events)==[],'zero_task_calls'
                save_json(root/'zero-task.json',{'plain_explain_limit0_null':True,'model_requests':0})

                def one(text,instruction=INSTRUCTION):
                    output=connection.execute(sql.SQL('SELECT {} FROM ONLY resource_rows WHERE id=1').format(expression(sql.Literal(text),instruction))).fetchone()[0]
                    return [(1,text,output)]

                def verify_http(observed, inputs, instruction, rows=None):
                    requests=[item for item in observed if item['event']=='request']
                    assert len(requests)==len(inputs) and len(observed)==2*len(inputs),'request_count'
                    assert all(observed[i]['event']=='request' and observed[i+1]['event'] in ('completion','error') for i in range(0,len(observed),2)), 'http_event_order'
                    for request,text in zip(requests,inputs):
                        assert request['body']=={'model':config['model_id'],
                            'messages':[{'role':'system','content':instruction},{'role':'user','content':text}],
                            'temperature':0,'top_p':1,'max_tokens':128,'n':1,'stream':False,'stop':None},'request_fields'
                    if rows is not None:
                        completions=[item for item in observed if item['event']=='completion']
                        nonnull=[row for row in rows if row[1] is not None]
                        assert len(completions)==len(nonnull)==len(inputs),'completion_count'
                        assert all(row[2] is None for row in rows if row[1] is None),'null_result'
                        for request,completed,row in zip(requests,completions,nonnull):
                            assert row[1]==request['body']['messages'][1]['content'],'row_order'
                            assert row[2].encode()==completed['raw_output'].encode(),'output_bytes'
                            assert completed['response_model_id']==config['model_id'] and completed['finish_reason']=='stop','completion_identity'
                            count=len(tokenizer.apply_chat_template(request['body']['messages'],tokenize=True,add_generation_prompt=True,return_dict=False))
                            assert count==completed['prompt_tokens'] and 0<=completed['output_tokens']<=128,'usage'

                warmup=one('warmup')
                model_idle(config['endpoint_url'])
                deadline=time.monotonic()+10
                while not any(e['event']=='session_end' for e in events(sessions)) and time.monotonic()<deadline:time.sleep(.01)
                verify_http(events(http_events),['warmup'],INSTRUCTION,warmup)
                assert ledger.attempts==1,'warmup_calls'
                save_json(root/'warmup.json',{'model_requests':1,'rows':warmup})

                def phase(name,query,inputs,expected_state=None,instruction=INSTRUCTION):
                    cursor=len(events(http_events)); attempts=ledger.attempts; actual={}
                    sampler=ProcfsTickSampler({'backend':connection.info.backend_pid,'gateway':gateway.pid},str(socket_path),context)
                    def invoke():
                        done=threading.Event(); canceler=None
                        if name=='cancel':
                            def cancel_after_request():
                                deadline=time.monotonic()+10
                                while not done.is_set() and time.monotonic()<deadline:
                                    if any(e['event']=='request' for e in events(http_events)[cursor:]):
                                        if not done.wait(.1):connection.cancel()
                                        return
                                    done.wait(.005)
                            canceler=threading.Thread(target=cancel_after_request)
                            canceler.start()
                        try:
                            key='plan' if name=='insert' else 'rows'
                            actual[key]=query()
                            save_json(root/name/(key+'.json'),actual[key])
                            return {'sql_completed':True}
                        except psycopg.Error as error:
                            actual['sqlstate']=error.sqlstate
                            raise
                        finally:
                            done.set()
                            if canceler:
                                canceler.join(timeout=15)
                                assert not canceler.is_alive(),'canceler_survived'
                    def verify():
                        observed=events(http_events)[cursor:]
                        save_json(root/name/'http.json',observed)
                        model_idle(config['endpoint_url'])
                        if name=='insert':
                            actual['rows']=audit.execute('SELECT i.id,i.payload,s.payload FROM insert_rows i JOIN map_sink s USING(id) ORDER BY i.id').fetchall()
                            save_json(root/name/'rows.json',actual['rows'])
                            save_json(root/name/'readback.json',{'measured_backend_pid':connection.info.backend_pid,'audit_backend_pid':audit.info.backend_pid})
                            assert audit.info.backend_pid!=connection.info.backend_pid,'audit_connection_not_independent'
                            node=map_node(actual['plan'])
                            completed=[e for e in observed if e['event']=='completion']
                            assert node is not None,'missing_map_plan'
                            assert node['Model Calls']==node['Accepted Rows']==node['Emitted Rows']==1,'plan_counts'
                            assert node['Prompt Tokens']==sum(e['prompt_tokens'] for e in completed),'plan_prompt_usage'
                            assert node['Output Tokens']==sum(e['output_tokens'] for e in completed),'plan_output_usage'
                        rows=actual.get('rows') if expected_state is None else None
                        if rows is not None:
                            assert all(len(row)==3 for row in rows),'row_shape'
                            assert sorted(row[0] for row in rows)==([1,2,3] if name=='select' else [4,5] if name=='insert' else [1]),'row_ids'
                        verify_http(observed,inputs,instruction,rows)
                        assert ledger.attempts==attempts+len(inputs),'phase_ledger_count'
                        if name=='reject':assert observed[-1]['event']=='error' and observed[-1]['code']=='MODEL_REQUEST_REJECTED','rejection'
                        if name=='cancel':assert observed[-1]['event'] in ('completion','error'),'cancel_http_not_terminal'
                        save_json(root/name/'checks.json',{'request_count':len(inputs),'passed':True,'sqlstate':actual.get('sqlstate')})
                        return []
                    result=execute_phase(root=root/name,phase=name,spec=RunSpec('diagnostic'),sampler=sampler,
                        operation=invoke,events=lambda:load_session_events(sessions),expected_tasks=len(inputs),
                        expected_sqlstate=expected_state,extra_checks=verify)
                    summary['phases'][name]=asdict(result)
                    save_json(root/'progress.json',summary)
                    assert result.assessment==('valid','passed'), 'phase_failed_'+name

                phase('select',lambda:connection.execute(sql.SQL('SELECT id,payload,{} FROM resource_rows').format(expr)).fetchall(),['数据库与人工智能',''])
                phase('insert',lambda:connection.execute(sql.SQL('EXPLAIN (ANALYZE, FORMAT JSON) {}').format(insert_sql)).fetchone()[0],['Hello, SemLoom.'])
                phase('cancel',lambda:one('cancel this generation',CANCEL_INSTRUCTION),['cancel this generation'],'57014',CANCEL_INSTRUCTION)
                phase('cancel_recovery',lambda:one('after cancel'),['after cancel'])
                phase('reject',lambda:one('token '*18000),['token '*18000],'38000')
                phase('reject_recovery',lambda:one('after model error'),['after model error'])
                assert list(summary['phases'])==REQUIRED and ledger.attempts==len(CASES),'complete_scope'
                assert_service(settings)
                summary['status']='passed'
    except BaseException as error:
        summary['error_type']=type(error).__name__
        summary['error_code']=str(error) if isinstance(error,AssertionError) and re.fullmatch('[A-Za-z0-9_]+',str(error)) else 'prepush_check_failed'
        raise
    finally:
        if ledger:
            try:summary['model_requests']=ledger.attempts
            except Exception:
                summary['ledger_unavailable']=True
                summary['status']='incomplete'
        save_json(root/'summary.json',summary)
        save_json(root/'SHA256SUMS.json',hashes(root))


if __name__=='__main__':
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, interrupted)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--settings',type=Path,required=True)
    run(json.loads(parser.parse_args().settings.read_text()))
