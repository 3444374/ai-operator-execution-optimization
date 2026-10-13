"""Bounded localhost three-hop fixture for the archived private POST ledger."""
import ast
import argparse
import asyncio
import hashlib
import http.client
import importlib.util
import json
import multiprocessing as mp
import os
import sqlite3
import statistics
import tarfile
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

SOURCE_SHA = 'a32a7f57b7cc5b6733411e8518fa5ffbe980c713aff05014077042f691dba7a2'
WORKERS, PER_WORKER = 4, 24


class TimedConnection:
    def __init__(self, connection, records):
        self.connection, self.records = connection, records
        self.fact = dict(begin_wait_wall_ns=0, begin_wait_cpu_ns=0, scan_wall_ns=0,
                         scan_cpu_ns=0, commit_wall_ns=0, commit_cpu_ns=0, reservation=False)

    @property
    def in_transaction(self):
        return self.connection.in_transaction

    def __enter__(self):
        self.connection.__enter__()
        return self

    def execute(self, sql, *args):
        wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
        result = self.connection.execute(sql, *args)
        if sql.startswith('INSERT INTO sends'):
            self.fact['reservation'] = True
        if sql == 'BEGIN IMMEDIATE':
            key = 'begin_wait'
        elif sql.startswith('SELECT count(*) FROM sends'):
            key = 'scan'
        else:
            key = None
        if key:
            self.fact[key+'_wall_ns'] += time.perf_counter_ns()-wall
            self.fact[key+'_cpu_ns'] += time.thread_time_ns()-cpu
        return result

    def __exit__(self, *args):
        wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
        result = self.connection.__exit__(*args)
        self.fact['commit_wall_ns'] += time.perf_counter_ns()-wall
        self.fact['commit_cpu_ns'] += time.thread_time_ns()-cpu
        self.records.append(self.fact)
        return result


def context(root, records):
    fixed=os.environ.get('SEMLOOM_LEDGER_FIXED_HELPER')
    if fixed:
        source=Path(fixed).read_bytes()
        assert hashlib.sha256(source).hexdigest()==os.environ['SEMLOOM_LEDGER_FIXED_SHA']
    else:
        with tarfile.open(os.environ['SEMLOOM_LEDGER_SOURCE_ARCHIVE']) as bundle:
            source = bundle.extractfile('validation/pre_send_ledger.py').read()
        assert hashlib.sha256(source).hexdigest() == SOURCE_SHA
    functions = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef)
                 and node.name in ('reserve','assert_hooks','finish')]
    class Hook:
        def request(self):
            pass
    marker = Hook()
    def connect(*args, **kwargs):
        return TimedConnection(sqlite3.connect(*args, **kwargs), records)
    ctx = dict(ROOT=Path(root), DB=Path(root)/'sends.sqlite', time=time, os=os,
               sqlite3=SimpleNamespace(connect=connect), hashlib=hashlib, json=json,
               _installed=True, _hooks={'aiohttp':(marker,'request',marker.request)},
               STATE={'minimal_sync_public_cli_software': {'max_logical':100000,'max_posts':300000},
                      'matrix': {'stages': {'single': [100000,300000,90]},
                                 'hard_logical_total':100000,'hard_transmission_total':300000}})
    # Bound methods create new identities; use the registered function object itself.
    marker.request = marker.request
    ctx['_hooks']['aiohttp'] = (marker,'request',marker.request)
    if fixed:
        counts=load_counts()
        ctx['_send_counts']=counts
        ctx['_hooks'].update({'send_counts_'+name:(counts,name,getattr(counts,name)) for name in ['snapshot','verify_increment','initialize_counts','check_identity','mark_initialized']})
    exec(compile(ast.Module(body=functions,type_ignores=[]),'archived-private-ledger','exec'),ctx)
    return ctx


def load_counts():
    path=Path(os.environ['SEMLOOM_LEDGER_COUNTER_SOURCE'])
    assert hashlib.sha256(path.read_bytes()).hexdigest()==os.environ['SEMLOOM_LEDGER_COUNTER_SHA']
    spec=importlib.util.spec_from_file_location('fixture_send_counts',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def reserve(ctx, kind, url, body, result):
    wall,cpu=time.perf_counter_ns(),time.thread_time_ns()
    sequence=ctx['reserve'](kind,url,body)
    result.append(dict(kind=kind,reserve_wall_ns=time.perf_counter_ns()-wall,
                       reserve_cpu_ns=time.thread_time_ns()-cpu,sequence=sequence))
    return sequence


async def async_post(port, body):
    reader,writer=await asyncio.open_connection('127.0.0.1',port)
    writer.write(b'POST /fixture HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: '+str(len(body)).encode()+b'\r\n\r\n'+body)
    await writer.drain()
    response=await reader.read()
    writer.close()
    await writer.wait_closed()
    assert response.startswith(b'HTTP/1.1 200')
    return response.split(b'\r\n\r\n',1)[1]


def server(root, kind, upstream, stop, ready):
    transactions, calls = [],[]
    ctx=context(root,transactions) if kind else None
    async def run():
        async def handler(reader,writer):
            head=await reader.readuntil(b'\r\n\r\n')
            size=int(next(line.split(b':',1)[1] for line in head.split(b'\r\n') if line.lower().startswith(b'content-length:')))
            body=await reader.readexactly(size)
            if kind:
                seq=reserve(ctx,kind,'http://127.0.0.1/fixture',body,calls)
                response=await async_post(upstream,body)
                ctx['finish'](seq,SimpleNamespace(status=200))
            else:
                await asyncio.sleep(.025)
                response=b'{"fixture":true}'
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(response)).encode()+b'\r\nConnection: close\r\n\r\n'+response)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        listener=await asyncio.start_server(handler,'127.0.0.1',0)
        ready.put(listener.sockets[0].getsockname()[1])
        async with listener:
            while not stop.is_set():
                await asyncio.sleep(.025)
    asyncio.run(run())
    (Path(root)/('server-'+str(kind)+'.json')).write_text(json.dumps(dict(calls=calls,transactions=transactions)))


def caller(root,port,start,index,ready):
    transactions,calls,complete=[],[],[]
    ctx=context(root,transactions)
    body=b'{"fixture":true}'
    ready.put('caller-ready-'+str(index))
    start.wait(15)
    for _ in range(PER_WORKER):
        wall=time.perf_counter_ns()
        seq=reserve(ctx,'gateway','http://127.0.0.1/fixture',body,calls)
        connection=http.client.HTTPConnection('127.0.0.1',port,timeout=15)
        connection.request('POST','/fixture',body=body)
        response=connection.getresponse()
        status=response.status
        assert response.read()==b'{"fixture":true}' and status==200
        ctx['finish'](seq,SimpleNamespace(status=status))
        connection.close()
        complete.append(time.perf_counter_ns()-wall)
    (Path(root)/f'caller-{index}.json').write_text(json.dumps(dict(calls=calls,transactions=transactions,complete_ns=complete)))


def percentile(values,p):
    ordered=sorted(values);place=(len(ordered)-1)*p;low=int(place);high=min(low+1,len(ordered)-1)
    return ordered[low]+(ordered[high]-ordered[low])*(place-low)


def run_case(base,prefix,round_index):
    version='fixed' if os.environ.get('SEMLOOM_LEDGER_FIXED_HELPER') else 'original'
    root=base/version/f'round-{round_index}-rows-{prefix}';root.mkdir(parents=True)
    (root/'model-start.json').write_text(json.dumps({'deadline_monotonic_ns':time.monotonic_ns()+300_000_000_000}))
    with sqlite3.connect(root/'sends.sqlite') as db:
        db.execute('CREATE TABLE control (id INTEGER PRIMARY KEY,phase TEXT,slot TEXT,unit TEXT,started REAL,deadline REAL,reserve REAL,deadline_ns INTEGER)')
        db.execute('CREATE TABLE sends (sequence INTEGER PRIMARY KEY,phase TEXT,slot TEXT,unit TEXT,kind TEXT,pid INTEGER,url TEXT,at_epoch REAL,at_ns INTEGER,body_sha256 TEXT,values_sha256 TEXT,returned_ns INTEGER,status INTEGER,error_type TEXT)')
        db.execute('INSERT INTO control VALUES(1,?,?,?,?,?,?,?)',('single','fixture-slot','fixture-unit',time.time(),time.time()+90,0,time.monotonic_ns()+90_000_000_000))
        db.executemany('INSERT INTO sends(phase,kind,body_sha256,values_sha256) VALUES(?,?,?,?)',
                       [('single',('gateway','router','model')[i%3],'0'*64,'0'*64) for i in range(prefix)])
    if os.environ.get('SEMLOOM_LEDGER_FIXED_HELPER'):
        counts=load_counts()
        with sqlite3.connect(root/'sends.sqlite') as db:
            db.execute('BEGIN IMMEDIATE');counts.initialize_counts(db,migrate=True,expected_rows=prefix)
        counts.mark_initialized(root,root/'sends.sqlite')
    ctx=mp.get_context('spawn');stop=ctx.Event();start=ctx.Event();ready=ctx.Queue();children=[]
    try:
        fixture=ctx.Process(target=server,args=(str(root),None,None,stop,ready));fixture.start();children.append(fixture);fixture_port=ready.get(timeout=15)
        router=ctx.Process(target=server,args=(str(root),'model',fixture_port,stop,ready));router.start();children.append(router);router_port=ready.get(timeout=15)
        gateway=ctx.Process(target=server,args=(str(root),'router',router_port,stop,ready));gateway.start();children.append(gateway);gateway_port=ready.get(timeout=15)
        clients=[ctx.Process(target=caller,args=(str(root),gateway_port,start,i,ready)) for i in range(WORKERS)]
        for child in clients:child.start();children.append(child)
        assert {ready.get(timeout=15) for _ in clients} == {'caller-ready-'+str(i) for i in range(WORKERS)}
        began=time.perf_counter_ns();start.set()
        for child in clients:
            child.join(timeout=30)
            assert child.exitcode==0,(child.pid,child.exitcode)
        completed=time.perf_counter_ns();stop.set()
        for child in children[:3]:child.join(timeout=5);assert child.exitcode==0
    finally:
        stop.set()
        for child in children:
            if child.is_alive():child.terminate();child.join(timeout=5)
    records=[json.loads(p.read_text()) for p in root.glob('caller-*.json')]+[json.loads(p.read_text()) for p in root.glob('server-*.json')]
    calls=[call for record in records for call in record['calls']]
    transactions=[entry for record in records for entry in record['transactions']]
    complete=[value for record in records for value in record.get('complete_ns',[])]
    assert len(calls)==288 and len(complete)==96 and len(transactions)==576
    with sqlite3.connect('file:'+str(root/'sends.sqlite')+'?mode=ro',uri=True) as db:
        assert db.execute('SELECT count(*) FROM sends').fetchone()[0]==prefix+288
        assert db.execute('SELECT count(*) FROM sends WHERE sequence>? AND status=200 AND error_type=\'\'',(prefix,)).fetchone()[0]==288
        indexes=list(db.execute('PRAGMA index_list(sends)'))
    out=dict(ledger_version=version,round=round_index,prefix_rows=prefix,new_logical_requests=96,transmission_posts=288,
             concurrency=4,fixed_response_delay_ms=25,fixture_wall_s=(completed-began)/1e9,all_processes_exited=True,indexes=indexes)
    for key in ['reserve_wall_ns','reserve_cpu_ns']:
        values=[call[key]/1e6 for call in calls];out[key.replace('_ns','_mean_ms')]=statistics.mean(values)
    for key in ['begin_wait_wall_ns','begin_wait_cpu_ns','scan_wall_ns','scan_cpu_ns','commit_wall_ns','commit_cpu_ns']:
        values=[row[key]/1e6 for row in transactions if row['reservation']]
        assert len(values)==288
        out[key.replace('_ns','_mean_ms')]=statistics.mean(values)
    out['fixture_request_mean_ms']=statistics.mean(complete)/1e6
    out['fixture_request_p95_ms']=percentile(complete,.95)/1e6
    out['reserve_samples']=calls
    out['transaction_samples']=transactions
    out['fixture_request_values_ns']=complete
    return out


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--result-dir',type=Path,default=Path(__file__).resolve().parent)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--fixed-helper',type=Path)
    parser.add_argument('--counter-source',type=Path)
    parser.add_argument('--compare',action='store_true')
    args=parser.parse_args()
    source_archive=args.result_dir/'supplement-observations.tar.gz'
    assert hashlib.sha256(source_archive.read_bytes()).hexdigest()=='8450aeb1521d9142e3ee200088daa5d55e7fc3a410d9117c85c18a7d5d85335e'
    os.environ['SEMLOOM_LEDGER_SOURCE_ARCHIVE']=str(source_archive.resolve())
    for key in ['SEMLOOM_LEDGER_FIXED_HELPER','SEMLOOM_LEDGER_FIXED_SHA','SEMLOOM_LEDGER_COUNTER_SOURCE','SEMLOOM_LEDGER_COUNTER_SHA']:
        os.environ.pop(key,None)
    fixed_environment={}
    if args.fixed_helper:
        assert args.counter_source
        os.environ['SEMLOOM_LEDGER_FIXED_HELPER']=str(args.fixed_helper.resolve())
        os.environ['SEMLOOM_LEDGER_FIXED_SHA']=hashlib.sha256(args.fixed_helper.read_bytes()).hexdigest()
        os.environ['SEMLOOM_LEDGER_COUNTER_SOURCE']=str(args.counter_source.resolve())
        os.environ['SEMLOOM_LEDGER_COUNTER_SHA']=hashlib.sha256(args.counter_source.read_bytes()).hexdigest()
        fixed_environment={key:os.environ[key] for key in ['SEMLOOM_LEDGER_FIXED_HELPER','SEMLOOM_LEDGER_FIXED_SHA','SEMLOOM_LEDGER_COUNTER_SOURCE','SEMLOOM_LEDGER_COUNTER_SHA']}
    if args.compare:assert fixed_environment
    began=time.monotonic();cases=[]
    with tempfile.TemporaryDirectory(prefix='semloom-ledger-localhost-') as directory:
        for round_index,order in enumerate([(3000,15000,23000),(23000,15000,3000)]):
            for prefix in order:
                modes=(['original','fixed'] if round_index==0 else ['fixed','original']) if args.compare else ['fixed' if args.fixed_helper else 'original']
                for mode in modes:
                    for key in fixed_environment:os.environ.pop(key,None)
                    if mode=='fixed':os.environ.update(fixed_environment)
                    assert time.monotonic()-began<150
                    cases.append(run_case(Path(directory),prefix,round_index))
    out=dict(passed=True,source_sha256=os.environ.get('SEMLOOM_LEDGER_FIXED_SHA',SOURCE_SHA),
             counter_source_sha256=os.environ.get('SEMLOOM_LEDGER_COUNTER_SHA'),
             ledger_version='comparison' if args.compare else ('fixed' if args.fixed_helper else 'original'),
             source_signatures={'original':SOURCE_SHA,'fixed':fixed_environment.get('SEMLOOM_LEDGER_FIXED_SHA'),'counter':fixed_environment.get('SEMLOOM_LEDGER_COUNTER_SHA')},sqlite_version=sqlite3.sqlite_version,
             scope='local archived ledger, four caller processes and two async proxies; synthetic 25ms response, not model/GPU/server performance',
             logical_fixture_requests=96*len(cases),fixture_transmission_posts=288*len(cases),model_calls=0,gpu_calls=0,
             hard_fixture_seconds=180,elapsed_seconds=time.monotonic()-began,cases=cases)
    args.output.write_text(json.dumps(out,indent=2)+'\n')
    print(json.dumps({k:v for k,v in out.items() if k!='cases'}))
    for case in cases:print(json.dumps({k:v for k,v in case.items() if k not in ['reserve_samples','transaction_samples','fixture_request_values_ns']}))


if __name__=='__main__':main()
