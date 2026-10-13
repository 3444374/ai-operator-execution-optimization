"""Explicit finite author-async software check; never uploads, compiles or loads a model."""
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from contextlib import contextmanager
from dataclasses import replace
import argparse
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

CODE=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(CODE))
from src.baselines.common.private_artifacts import require_outside_git, write_private_json
from src.baselines.common.redact import redact_text
from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig, IMLaneCallExecutor, process_start_ticks
from src.baselines.text.frameworks.native_calls import NativeCall,_call_from_wire
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions

KERNEL=Path(__file__).with_name('imlane_async_complete_call.py')
REQUEST_SOURCE=CODE/'src/baselines/text/frameworks/native_calls.py'
AUTHOR='072f8257db80569e42c9f5727d839587df529652'
SOURCE_FILES=('code/integrations/sema/imlane_async_complete_call.py',
    'code/integrations/sema/imlane_async_q1_client.py',
    'code/integrations/sema/verify_imlane_async_calls.py','code/integrations/sema/imlane_http.cpp',
    'code/src/baselines/text/frameworks/native_calls.py','code/src/baselines/text/frameworks/imlane_calls.py',
    'code/src/baselines/text/frameworks/imlane_limits.py','code/src/baselines/text/frameworks/prepared_map.py',
    'code/src/execution_provider/adapters/sema_imlane.py','code/src/execution_provider/adapters/full_response.py',
    'code/src/semantic_methods/lotus/native.py','code/src/semantic_methods/lotus/batch.py',
    'code/src/semantic_methods/lotus/sdk.py','code/src/execution_provider/adapters/model_config.py',
    'code/src/experiments/postgresql/sembench_queries.py','code/src/experiments/postgresql/sembench_query_cli.py',
    'code/src/experiments/postgresql/imlane_query.py','code/src/baselines/text/sembench_movie.py',
    'code/src/baselines/text/frameworks/lotus_pg.py',
    'code/src/baselines/common/private_artifacts.py','code/src/baselines/common/redact.py')


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class StopBudget:
    """Deadline signals stop work while allowing each owned teardown to finish."""
    def __init__(self, stopped):
        self.stopped=stopped;self.cleanup_depth=0;self.reason=None

    def signal(self, number, _frame):
        self.stopped.set();self.reason=signal.Signals(number).name
        if not self.cleanup_depth:raise TimeoutError('software stop signal: '+self.reason)

    @contextmanager
    def cleanup(self):
        self.cleanup_depth+=1
        try:yield
        finally:self.cleanup_depth-=1

    def check(self):
        if self.stopped.is_set():raise TimeoutError('software work has stopped')


class PostAllowance:
    def __init__(self, maximum, stopped, abort, *, payload_identity=False):
        self.maximum=maximum;self.stopped=stopped;self.abort=abort;self.reserved=0
        self.available={};self.expected={};self.used={};self.violations=[];self.lock=threading.Lock()
        self.payload_identity=payload_identity

    def reserve(self, calls):
        entries=[(hashlib.sha256(call.payload).hexdigest() if self.payload_identity else call.call_id,
                  2 if call.endpoint.endswith('/cookie-307') else 1,
                  hashlib.sha256(call.payload).hexdigest()) for call in calls]
        with self.lock:
            cost=sum(amount for _identity,amount,_sha in entries)
            if self.stopped.is_set() or self.reserved+cost>self.maximum-8:
                raise ValueError('POST allowance would be exceeded before author submission')
            self.reserved+=cost
            for identity,amount,sha in entries:
                self.available[identity]=self.available.get(identity,0)+amount;self.expected[identity]=sha

    def receipt(self, row, posts):
        if row['method']!='POST':return
        with self.lock:
            identity=row['payload_sha256'] if self.payload_identity else row['call_id'];left=self.available.get(identity,0)
            invalid=(not left or self.expected.get(identity)!=row['payload_sha256'] or posts>self.maximum-8)
            if invalid:self.violations.append(dict(call_id=identity,posts=posts))
            else:self.available[identity]-=1;self.used[identity]=self.used.get(identity,0)+1
        if invalid:
            self.stopped.set()
            # Stop verified owned processes immediately, before waiting for a consumer poll.
            self.abort()


class Fixture:
    def __init__(self, maximum, stopped, abort, *, q1=False, journal_root=None):
        self.receipts=[];self.responses=[];self.connections=set();self.closed_connections=set()
        self.lock=threading.Lock();self.active=0;self.peak=0;self.slow=threading.Event()
        self.allowance=PostAllowance(maximum,stopped,abort,payload_identity=q1)
        self.stopped=stopped;self.abort=abort;self.journal_errors=[]
        self.journal=None if journal_root is None else os.fdopen(os.open(journal_root/'fixture-receipts.jsonl',
            os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w',buffering=1)
        owner=self
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            timeout=3
            def log_message(self,*_args):pass
            def setup(self):
                super().setup();self.identity=uuid.uuid4().hex
                with owner.lock:owner.connections.add(self.identity)
            def finish(self):
                try:super().finish()
                finally:
                    with owner.lock:owner.closed_connections.add(self.identity)
            def do_GET(self):self.respond()
            def do_POST(self):self.respond()
            def respond(self):
                raw=self.rfile.read(int(self.headers.get('Content-Length','0')))
                receipt=dict(method=self.command,path=self.path,
                    payload_sha256=hashlib.sha256(raw).hexdigest(),connection=self.identity,
                    call_id=self.headers.get('X-Call-ID'),cookie=self.headers.get('Cookie',''))
                with owner.lock:
                    owner.receipts.append(receipt)
                    posts=sum(r['method']=='POST' for r in owner.receipts)
                    owner.active+=1;owner.peak=max(owner.peak,owner.active)
                try:
                    if not owner.accept_receipt(receipt,posts):
                        self.send_error(503,'private receipt journal failed');return
                    if self.path=='/slow':owner.slow.set();time.sleep(.6)
                    time.sleep(float(self.headers.get('X-Delay','0')))
                    if self.path in ('/cookie-307','/cookie-302'):
                        status=307 if self.path.endswith('307') else 302
                        body=b''
                        self.send_response(status)
                        self.send_header('Set-Cookie','route_cookie='+self.headers['X-Cookie-Value']+'; Path=/')
                        self.send_header('Location','/cookie-check')
                    else:
                        status=401 if self.path=='/unauthorized' else 429 if self.path=='/rate' else 200
                        body=json.dumps(dict(id='fixture',object='chat.completion',model='Qwen2.5-1.5B-Instruct' if q1 else 'fixture',created=0,
                            cookie=self.headers.get('Cookie',''),method=self.command,
                            choices=[dict(index=0,message=dict(role='assistant',content='True' if q1 else 'ok'),
                                          finish_reason='length' if self.path=='/length' else 'stop')],
                            usage=dict(prompt_tokens=2,completion_tokens=1,total_tokens=3))).encode()
                        if self.path=='/bad-json':body=b'not-json\x00\n'
                        if self.path=='/gzip':body=gzip.compress(body,mtime=0)
                        self.send_response(status)
                        self.send_header('Content-Type','application/json')
                        if self.path=='/gzip':self.send_header('Content-Encoding','gzip')
                    self.send_header('X-Repeat','one');self.send_header('X-Repeat','two')
                    self.send_header('Content-Length',str(len(body)))
                    headers=tuple((name,value.lstrip()) for line in b''.join(self._headers_buffer).decode('latin1').split('\r\n')[1:]
                                  if line for name,_separator,value in (line.partition(':'),))
                    with owner.lock:owner.responses.append(dict(call_id=receipt['call_id'],path=self.path,
                        status_code=status,headers=headers,http_version=self.protocol_version,
                        body_sha256=hashlib.sha256(body).hexdigest()))
                    self.end_headers()
                    try:self.wfile.write(body);self.wfile.flush()
                    except (BrokenPipeError,ConnectionResetError):pass
                finally:
                    with owner.lock:owner.active-=1
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.server.daemon_threads=True
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        # Default CookieJar accepts localhost cookies, matching the original kernel.
        self.base='http://localhost:'+str(self.server.server_port)

    @property
    def posts(self):return sum(r['method']=='POST' for r in self.receipts)

    def accept_receipt(self, receipt, posts):
        self.allowance.receipt(receipt,posts)
        if self.journal is not None:
            try:
                with self.lock:self.journal.write(json.dumps(receipt)+'\n')
            except Exception as error:
                with self.lock:self.journal_errors.append(dict(type=type(error).__name__,error=redact_text(str(error))))
                self.stopped.set();self.abort();return False
        return True

    def settled(self):
        until=time.monotonic()+4
        while time.monotonic()<until:
            with self.lock:
                if self.active==0 and self.connections==self.closed_connections:return
            time.sleep(.02)
        raise AssertionError('fixture HTTP or connection closure is unconfirmed')

    def close(self):
        try:
            self.server.shutdown();self.server.server_close();self.thread.join(2)
            self.settled()
        finally:
            if self.journal is not None:self.journal.close()


class AuthorScope:
    def __init__(self,args,mode,helper,report):
        self.args=args;self.mode=mode;self.helper=helper;self.report=report
        self.namespace='ia-'+mode+'-'+uuid.uuid4().hex[:10]
        self.root=args.output/self.namespace;self.root.mkdir()
        self.service=self.owner=None;self.writer=self.owner_writer=None;self.short=None
        self.executors=[];self.children=[];self.known=set();self.roots={};self.identities_lock=threading.Lock()
        self.stop_sampler=threading.Event();self.closed=False;self.cleanup_attempts=0;args.scopes.append(self)

    def track_root(self, process):
        ticks=process_start_ticks(process.pid)
        with self.identities_lock:
            self.roots[process.pid]=ticks;self.known.add((process.pid,ticks))
        self.snapshot()

    def snapshot(self, *, include_orphans=False):
        import psutil
        with self.identities_lock:roots=list(self.roots.items())
        observed=[]
        for pid,ticks in roots:
            try:
                if process_start_ticks(pid)!=ticks:continue
                root=psutil.Process(pid);family=[root,*root.children(recursive=True)]
                if process_start_ticks(pid)!=ticks:continue
                observed.extend(family)
            except (psutil.Error,OSError):continue
        if include_orphans:
            # Exact random namespace also identifies our descendants after reparenting.
            for process in psutil.process_iter():
                try:
                    if process.environ().get('IMLANE_NAMESPACE')==self.namespace:observed.append(process)
                except psutil.Error:continue
        for process in observed:
            try:
                ticks=process_start_ticks(process.pid)
                if process.is_running():
                    with self.identities_lock:self.known.add((process.pid,ticks))
            except (psutil.Error,OSError):pass

    def sample(self):
        while not self.stop_sampler.wait(.1):
            self.snapshot()

    def __enter__(self):
        self.args.stop_budget.check()
        author=self.args.author_root;python=author/'venv/bin/python';prefix=author/'prefix-patched'
        info=json.loads(subprocess.check_output([str(python),'-B','-c',
            "import sys,sysconfig,pyarrow,json;print(json.dumps(dict(site=sysconfig.get_path('purelib'),lib=pyarrow.get_library_dirs()[0],base=sys.base_prefix)))"],text=True,timeout=15))
        cpp=Path(subprocess.check_output(['g++','-print-file-name=libstdc++.so.6'],text=True,timeout=10).strip()).resolve()
        temporary=self.root/'tmp';temporary.mkdir()
        self.env=dict(os.environ,CUDA_VISIBLE_DEVICES='',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',
            PYTHONDONTWRITEBYTECODE='1',
            PATH=str(python.parent)+os.pathsep+os.environ['PATH'],
            PYTHONPATH=os.pathsep.join([info['site'],str(CODE),str(KERNEL.parent)]),
            LD_LIBRARY_PATH=os.pathsep.join([str(cpp.parent),str(prefix/'lib'),info['lib'],
                str(author/'source/third_party/fmt/dist/lib'),str(author/'source/third_party/boost/dist/lib'),info['base']+'/lib']),
            TMPDIR=str(temporary),IMLANE_NAMESPACE=self.namespace,IMLANE_GATEWAY_HOST='127.0.0.1',
            IMLANE_HTTP_TRACE_DIR=str(self.root/'worker-traces'),RAY_USAGE_STATS_ENABLED='0',RAY_DEDUP_LOGS='0')
        self.env.pop('RAY_ADDRESS',None);self.env.pop('PYTHONHOME',None)
        self.sampler=threading.Thread(target=self.sample,daemon=True);self.sampler.start()
        port=self.helper.free_port()
        command=[str(prefix/'bin/imlane'),'--working_dir',str(self.root/'state'),'--n_executors','2',
                 '--runtime',self.mode,'--gateway_port',str(port)]
        try:
            if self.mode=='ray':
                ray_root=self.root/'ray';ray_root.mkdir()
                self.short=self.args.author_root.parents[1]/('ia-r-'+uuid.uuid4().hex[:8])
                if len(str(self.short).encode())>60:raise ValueError('owned Ray temporary path is too long')
                self.short.symlink_to(ray_root,target_is_directory=True);address=self.root/'ray-address.json'
                program="import sys,ray,json,os;ctx=ray.init(address='local',num_cpus=2,num_gpus=0,include_dashboard=False,object_store_memory=134217728,_node_ip_address='127.0.0.1',_temp_dir=sys.argv[1]);temporary=sys.argv[2]+'.tmp';open(temporary,'w').write(json.dumps(dict(address=ctx.address_info['gcs_address'])));os.replace(temporary,sys.argv[2]);sys.stdin.readline();ray.shutdown()"
                self.owner,self.owner_writer=self.helper.spawn([str(python),'-B','-c',program,str(self.short),str(address)],self.env,self.root/'ray-owner.log',stdin=True)
                self.track_root(self.owner)
                until=time.monotonic()+45
                while not address.is_file():
                    if self.owner.poll() is not None or time.monotonic()>=until:raise TimeoutError('owned Ray readiness failed')
                    time.sleep(.05)
                command+=['--ray_address',json.loads(address.read_text())['address']]
            self.service,self.writer=self.helper.spawn(command,self.env,self.root/'coordinator.log')
            self.track_root(self.service)
            until=time.monotonic()+45
            while True:
                if self.service.poll() is not None:raise RuntimeError('Coordinator exited before readiness')
                try:
                    with socket.create_connection(('127.0.0.1',port),timeout=.1):break
                except OSError:
                    if time.monotonic()>=until:raise TimeoutError('Coordinator readiness failed')
                    time.sleep(.05)
            probe=subprocess.run([str(self.args.bridge_binary),str(port),str(KERNEL),'0'],
                env=self.env,input='',text=True,capture_output=True,timeout=20)
            (self.root/'probe.log').write_text(redact_text(probe.stdout+probe.stderr))
            if probe.returncode:raise RuntimeError('async function registration failed')
            meta=json.loads(next(line.partition(' ')[2] for line in probe.stdout.splitlines() if line.startswith('SEMA_IMLANE_META ')))
            if meta['native_task_threads']>8:raise ValueError('native task threads exceed this software allowance')
            bridge=dict(author_commit=AUTHOR,bridge_binary=str(self.args.bridge_binary),bridge_sha256=digest(self.args.bridge_binary),
                gateway_port=port,udf_path=str(KERNEL),udf_sha256=digest(KERNEL),work_root=str(self.root/'batches'),
                namespace=self.namespace,native_task_threads=meta['native_task_threads'],
                env={k:self.env[k] for k in ('PATH','PYTHONPATH','LD_LIBRARY_PATH','TMPDIR','OMP_NUM_THREADS','OPENBLAS_NUM_THREADS')})
            path=self.root/'bridge-config.json';write_private_json(path,bridge)
            self.config=IMLaneCallConfig(self.mode,str(path),self.service.pid,process_start_ticks(self.service.pid),
                str(author/'artifacts/build-patched/build-identity.json'),max_batch_rows=2000)
            self.meta=meta
            return self
        except BaseException as error:
            try:self.close()
            except BaseException as cleanup:error.add_note('Author startup cleanup also failed: '+type(cleanup).__name__)
            raise

    def executor(self):
        self.args.stop_budget.check()
        value=IMLaneCallExecutor(self.config,NativeGraphOptions(concurrency=8));self.executors.append(value)
        return value

    def survivors(self):
        import psutil
        with self.identities_lock:identities=sorted(self.known)
        result=[]
        for pid,ticks in identities:
            try:
                process=psutil.Process(pid)
                if process_start_ticks(pid)==ticks and process.is_running() and process.status()!=psutil.STATUS_ZOMBIE:
                    result.append((pid,ticks))
            except (psutil.Error,OSError):pass
        return result

    @staticmethod
    def signal_identity(identity, number):
        pid,ticks=identity
        try:
            if process_start_ticks(pid)==ticks:os.kill(pid,number)
        except ProcessLookupError:pass
        except FileNotFoundError:pass
        except OSError as error:
            return dict(pid=pid,start_ticks=ticks,signal=signal.Signals(number).name,
                        type=type(error).__name__,error=redact_text(str(error)))

    def stop_root(self, process, writer, *, owner=False):
        forced=False
        if process.poll() is None:
            ticks=self.roots[process.pid]
            if owner:
                try:process.stdin.write('\n');process.stdin.flush()
                except (BrokenPipeError,OSError):pass
            else:self.signal_identity((process.pid,ticks),signal.SIGTERM)
            try:process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                forced=True;self.snapshot(include_orphans=True)
                for identity in self.survivors():self.signal_identity(identity,signal.SIGKILL)
                process.wait(timeout=2)
        writer.join(1)
        return dict(returncode=process.returncode,forced=forced)

    def stop_survivors(self):
        self.snapshot(include_orphans=True);remaining=self.survivors();forced=list(remaining);errors=[]
        for number in (signal.SIGTERM,signal.SIGKILL):
            for identity in remaining:
                error=self.signal_identity(identity,number)
                if error:errors.append(error)
            until=time.monotonic()+2
            while remaining and time.monotonic()<until:
                time.sleep(.02);remaining=self.survivors()
            remaining=self.survivors()
        return forced,remaining,errors

    def abort_owned(self):
        if self.closed:return
        errors=[]
        try:self.snapshot(include_orphans=True)
        except Exception as error:errors.append(dict(type=type(error).__name__,error=redact_text(str(error))))
        for identity in self.survivors():
            error=self.signal_identity(identity,signal.SIGKILL)
            if error:errors.append(error)
        return errors

    def close(self):
        if self.closed:return
        self.cleanup_attempts+=1
        cleanup=dict(runtime=self.mode,namespace=self.namespace,attempt=self.cleanup_attempts,errors=[])
        def attempt(label, operation):
            try:return operation()
            except BaseException as error:
                cleanup['errors'].append(dict(action=label,type=type(error).__name__,error=redact_text(str(error))))
        with self.args.stop_budget.cleanup():
            attempt('snapshot',lambda:self.snapshot(include_orphans=True))
            for index,value in enumerate(self.executors):attempt('executor-'+str(index),value.close)
            if self.service is not None:
                cleanup['coordinator']=attempt('coordinator',lambda:self.stop_root(self.service,self.writer))
            if self.owner is not None:
                cleanup['ray_owner']=attempt('ray_owner',lambda:self.stop_root(self.owner,self.owner_writer,owner=True))
            for index,(process,writer) in enumerate(self.children):
                cleanup['client-'+str(index)]=attempt('client-'+str(index),lambda:self.stop_root(process,writer))
            self.stop_sampler.set()
            if hasattr(self,'sampler'):attempt('sampler',lambda:self.sampler.join(1))
            fallback=attempt('owned-survivors',self.stop_survivors)
            cleanup['forced_identities']=None if fallback is None else [dict(pid=p,start_ticks=t) for p,t in fallback[0]]
            cleanup['owned_survivors']=None if fallback is None else [dict(pid=p,start_ticks=t) for p,t in fallback[1]]
            if fallback is not None:cleanup['errors'].extend(fallback[2])
            cleanup['remaining_shm']=attempt('shared-memory',lambda:self.helper.resources(self.namespace))
            cleanup['shared_memory_before_fallback']=cleanup['remaining_shm']
            cleanup['forced_shared_memory']=[]
            if cleanup['owned_survivors']==[] and cleanup['remaining_shm']:
                for name in cleanup['remaining_shm']:
                    if Path(name).name!=name or not name.startswith((self.namespace,'.'+self.namespace)):
                        cleanup['errors'].append(dict(action='shared-memory-name',error='unrecognized owned namespace member'))
                        continue
                    cleanup['forced_shared_memory'].append(name)
                    attempt('shared-memory-unlink',lambda:(Path('/dev/shm')/name).unlink(missing_ok=True))
                cleanup['remaining_shm']=attempt('shared-memory-recheck',lambda:self.helper.resources(self.namespace))
            writers=(self.writer,self.owner_writer,*(writer for _process,writer in self.children))
            for writer in writers:
                if writer is not None:attempt('log-writer',lambda:writer.join(1))
            cleanup['owned_threads_stopped']=all(not value.is_alive() for value in
                (*writers,getattr(self,'sampler',None)) if value is not None)
            cleanup['owned_process_identities']=[dict(pid=p,start_ticks=t) for p,t in sorted(self.known)]
            cleanup['passed']=(not cleanup['errors'] and cleanup['owned_survivors']==[]
                and cleanup['forced_identities']==[] and not cleanup['forced_shared_memory']
                and cleanup['remaining_shm']=={} and cleanup['owned_threads_stopped'] and all(
                    cleanup[k] and not cleanup[k]['forced'] and cleanup[k]['returncode']==0
                    for k in ('coordinator','ray_owner') if k in cleanup))
            if any(not row or row['forced'] or row['returncode']!=0 for key,row in cleanup.items() if key.startswith('client-')):cleanup['passed']=False
            if self.short is not None and self.short.is_symlink():attempt('temporary-link',self.short.unlink)
            if cleanup['errors']:cleanup['passed']=False
            self.report['cleanup'].append(cleanup)
            attempt('cleanup-evidence',lambda:write_private_json(self.root/f'cleanup-{self.cleanup_attempts:03}.json',cleanup))
            if cleanup['errors']:cleanup['passed']=False
            self.closed=(cleanup['owned_survivors']==[] and cleanup['remaining_shm']=={} and cleanup['owned_threads_stopped'])
        if not cleanup['passed']:raise RuntimeError('owned author cleanup failed')

    def __exit__(self,kind,error,traceback):
        try:self.close()
        except Exception as cleanup:
            if error is None:raise
            error.add_note('Author cleanup also failed: '+type(cleanup).__name__)


def run_mode(args,mode,helper,fixture,stopped,report):
    before=fixture.posts;cases=[]
    def calls(label,count,path='/ok',**options):
        return tuple(NativeCall(label+':'+str(i),i//3,b'{"text":"duplicate"}',fixture.base+path,
            (('X-Call-ID',label+':'+str(i)),('X-Delay','.04' if i==0 else '0')),1.,**options) for i in range(count))
    def batch(scope,values,*,allow_transport=False):
        receipt_start=len(fixture.receipts)
        fixture.allowance.reserve(values)
        executor=scope.executor();iterator=executor.iter_batch(values,cancelled=stopped.is_set,timeout_s=45)
        first=next(iterator);time.sleep(.02);outcomes=[first,*iterator]
        assert len(outcomes)==len(values) and len({o.call_id for o in outcomes})==len(values)
        expected={c.call_id:c.row_id for c in values}
        assert all(expected[o.call_id]==o.row_id for o in outcomes)
        assert allow_transport or all(o.response is not None for o in outcomes),'unexpected native transport outcome'
        assert executor.last_close_report['clean']
        cases.append(dict(label=values[0].call_id.split(':')[0],rows=len(values),close=executor.last_close_report))
        fixture.settled()
        expected_calls={c.call_id:c for c in values}
        receipts=fixture.receipts[receipt_start:]
        for receipt in receipts:
            assert receipt['call_id'] in expected_calls
            if receipt['method']=='POST':assert receipt['payload_sha256']==hashlib.sha256(expected_calls[receipt['call_id']].payload).hexdigest()
        for call in values:
            own=[r for r in receipts if r['call_id']==call.call_id]
            methods=['POST','POST'] if call.endpoint.endswith('/cookie-307') else ['POST','GET'] if call.endpoint.endswith('/cookie-302') else ['POST']
            assert [r['method'] for r in own]==methods
        for outcome in outcomes:
            if outcome.response is None:continue
            expected_response=next(row for row in reversed(fixture.responses) if row['call_id']==outcome.call_id)
            assert outcome.response.status_code==expected_response['status_code']
            assert outcome.response.headers==expected_response['headers']
            assert outcome.response.http_version==expected_response['http_version']
            assert hashlib.sha256(outcome.response.body).hexdigest()==expected_response['body_sha256']
        return outcomes
    with AuthorScope(args,mode,helper,report) as scope:
        empty=scope.executor()
        try:list(empty.iter_batch(calls('cancel-before',1),cancelled=lambda:True));raise AssertionError('cancel ignored')
        except InterruptedError:pass
        assert fixture.posts==before
        batch(scope,calls('normal-first',8));batch(scope,calls('normal-next',8))
        batch(scope,calls('window129',129))
        full=tuple(calls('http'+str(i),1,path)[0] for i,path in enumerate(('/ok','/unauthorized','/rate','/gzip','/bad-json','/length')))
        outcomes=batch(scope,full);mapped={v.call_id:v for v in outcomes}
        assert [mapped[c.call_id].response.status_code for c in full]==[200,401,429,200,200,200]
        assert mapped[full[4].call_id].response.body==b'not-json\x00\n'
        assert json.loads(gzip.decompress(mapped[full[3].call_id].response.body))['choices'][0]['message']['content']=='ok'
        assert all([v for k,v in o.response.headers if k=='X-Repeat']==['one','two'] for o in outcomes)
        def cookie(label,path,value):
            return NativeCall(label,0,b'{}',fixture.base+path,(('X-Call-ID',label),('X-Cookie-Value',value)),1.,follow_redirects=True)
        first,=batch(scope,(cookie('cookie-first','/cookie-307','first'),))
        assert json.loads(first.response.body)['cookie']=='route_cookie=first'
        plain,=batch(scope,calls('cookie-independent',1,'/cookie-check'))
        assert json.loads(plain.response.body)['cookie']==''
        parallel=batch(scope,(cookie('cookie-A','/cookie-307','A'),cookie('cookie-B','/cookie-307','B')))
        assert {v.call_id:json.loads(v.response.body)['cookie'] for v in parallel}=={'cookie-A':'route_cookie=A','cookie-B':'route_cookie=B'}
        changed,=batch(scope,(cookie('cookie-302','/cookie-302','changed'),))
        assert json.loads(changed.response.body)['method']=='GET'
        fixture.slow.clear();timeout_before=fixture.posts
        timed,=batch(scope,(replace(calls('timeout',1,'/slow')[0],timeout_s=.25,connect_timeout_s=.25),),allow_transport=True)
        assert timed.response is None and timed.transport_error
        assert fixture.slow.is_set() and fixture.posts==timeout_before+1
        fixture.settled();batch(scope,calls('after-timeout',2))
        errored=scope.executor()
        fixture.allowance.reserve(calls('capacity-error',1,max_response_bytes=128))
        try:list(errored.iter_batch(calls('capacity-error',1,max_response_bytes=128)));raise AssertionError('capacity error ignored')
        except ValueError as error:
            assert getattr(error,'native_execution_error','').startswith('ValueError:')
        assert errored.last_close_report['external_coordinator_requires_cleanup']
        another=scope.executor()
        try:list(another.iter_batch(calls('quarantined',1)));raise AssertionError('quarantine ignored')
        except RuntimeError as error:assert 'unsettled' in str(error)
    with AuthorScope(args,mode,helper,report) as scope:
        batch(scope,calls('after-error',2));fixture.slow.clear();cancelled=threading.Event();executor=scope.executor()
        fixture.allowance.reserve(calls('cancel-sent',1,'/slow'))
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(lambda:list(executor.iter_batch(calls('cancel-sent',1,'/slow'),cancelled=cancelled.is_set,timeout_s=10)))
            assert fixture.slow.wait(5);cancelled.set()
            try:future.result(timeout=5);raise AssertionError('sent cancel ignored')
            except InterruptedError:pass
        assert executor.last_close_report['external_coordinator_requires_cleanup'];fixture.settled()
    with AuthorScope(args,mode,helper,report) as scope:
        batch(scope,calls('after-cancel',2));executor=scope.executor()
        fixture.allowance.reserve(calls('early-close',4))
        iterator=executor.iter_batch(calls('early-close',4),timeout_s=10)
        next(iterator);iterator.close();assert executor.last_close_report['external_coordinator_requires_cleanup'];fixture.settled()
    with AuthorScope(args,mode,helper,report) as scope:batch(scope,calls('after-early-close',2))
    traces=[]
    for p in args.output.glob('ia-'+mode+'-*/worker-traces/*.jsonl'):
        traces.extend(json.loads(line) for line in p.read_text().splitlines())
    normal=[r for r in traces if r['call_id'].startswith(('normal-first:','normal-next:','window129:'))]
    assert len(normal)==145 and len({r['worker_pid'] for r in normal})==2
    loops={}
    for row in normal:
        assert (row['loop_running'] and not row['loop_closed'] and row['kernel_sha256']==digest(KERNEL)
            and row['native_calls_sha256']==digest(REQUEST_SOURCE))
        assert row['loaded_http_code_matches_source'] is True and row['loaded_request_code_sha256']
        loops.setdefault(row['worker_pid'],set()).add((row['native_thread_id'],row['loop_generation']))
    assert all(len(values)==1 for values in loops.values())
    if mode=='ray':assert len({r['ray_actor_id'] for r in normal})==2
    assert fixture.posts-before<=174 and fixture.peak<=2
    report['modes'].append(dict(runtime=mode,passed=True,posts=fixture.posts-before,cases=cases,
        stable_author_loops={str(pid):list(values) for pid,values in loops.items()},
        loop_reused=True,http_client_reused=False,trace_rows=len(traces),kernel_sha256=digest(KERNEL),
        native_calls_sha256=digest(REQUEST_SOURCE)))


def run_q1(args,mode,helper,fixture,stopped,report):
    specification=json.loads(args.q1_spec.read_text());expected=specification['expected'];before=fixture.posts
    assert expected['total_calls']==2000
    for name in ('raw','csv','manifest','tokenizer','lotus_python'):
        if not Path(specification[name]).is_absolute() or not Path(specification[name]).is_file():
            raise ValueError('Q1 requires the existing absolute input/environment paths')
    checkout=Path(specification['checkout'])
    git=['git','-c','safe.directory='+str(checkout.resolve()),'-C',str(checkout)]
    for name,sha in specification['checkout_sources_sha256'].items():
        assert digest(checkout/name)==sha
        committed=subprocess.check_output(git+['show','HEAD:'+name],timeout=10)
        assert hashlib.sha256(committed).hexdigest()==sha
    assert subprocess.check_output(git+['rev-parse','HEAD'],text=True,timeout=10).strip()==specification['checkout_commit']
    with AuthorScope(args,mode,helper,report) as scope:
        config_path=scope.root/'call-config.json';write_private_json(config_path,scope.config.__dict__)
        client,writer=helper.spawn([specification['lotus_python'],'-B',str(KERNEL.with_name('imlane_async_q1_client.py')),
            str(config_path),str(args.q1_spec),fixture.base,str(scope.root)],scope.env,scope.root/'client.log')
        scope.children.append((client,writer));scope.track_root(client)
        until=time.monotonic()+60
        ready=scope.root/'prepared-ready.json'
        while not ready.is_file():
            args.stop_budget.check()
            if client.poll() is not None or time.monotonic()>=until:raise RuntimeError('original Q1 preparation failed before dispatch')
            time.sleep(.02)
        prepared=json.loads(ready.read_text())
        calls=tuple(_call_from_wire(line) for line in (scope.root/'prepared.jsonl').read_text().splitlines())
        assert len(calls)==2000 and len({c.call_id for c in calls})==2000
        assert all(call.endpoint==fixture.base+'/v1/chat/completions' for call in calls)
        assert prepared['payload_sequence_sha256']==expected['payload_sequence_sha256']
        assert prepared['original_wire_sequence_sha256']==expected['wire_sequence_sha256']
        assert prepared['payload_total_bytes']==expected['payload_total_bytes']
        fixture.allowance.reserve(calls)
        write_private_json(scope.root/'dispatch-approved.json',dict(rows=2000,max_posts=2000,models=0,gpu=0))
        if client.wait(timeout=180):raise RuntimeError('original Q1 client failed')
        fixture.settled();result=json.loads((scope.root/'client-result.json').read_text())
        assert result['passed'] and result['complete_responses']==result['distinct_occurrences']==2000
        assert result['original_query_result_rows']==5 and result['close']['clean']
        entry=result['public_entry'];identity=entry['coordinator_identity']
        assert entry['entry']=='MovieQueryResult.execute' and entry['factory']=='NativeCallExecutor'
        assert entry['delegate']=='IMLaneCallExecutor' and entry['bridge_delegate']=='SemaIMLaneExecutor'
        assert entry['configured_max_batch_rows']==entry['effective_max_batch_rows']==2000
        assert entry['bridge_sha256']==digest(args.bridge_binary) and entry['udf_sha256']==digest(KERNEL)
        assert identity['coordinator_pid']==scope.service.pid and identity['coordinator_start_ticks']==process_start_ticks(scope.service.pid)
        assert len(list((scope.root/'batches').glob('batch-*')))==1
        assert fixture.posts-before==2000 and fixture.peak<=2
        received=fixture.receipts[-2000:]
        assert all(row['method']=='POST' for row in received)
        assert Counter(row['payload_sha256'] for row in received)==Counter(hashlib.sha256(c.payload).hexdigest() for c in calls)
        def response_key(row):
            return row['status_code'],tuple(tuple(pair) for pair in row['headers']),row['http_version'],row['body_sha256']
        assert Counter(response_key(row) for row in result['responses'])==Counter(response_key(row) for row in fixture.responses[-2000:])
        traces=[json.loads(line) for path in (scope.root/'worker-traces').glob('*.jsonl') for line in path.read_text().splitlines()]
        assert len(traces)==2000 and len({row['call_id'] for row in traces})==2000
        original={call.call_id:call for call in calls};loops={}
        for row in traces:
            call=original[row['call_id']]
            assert row['row_id']==call.row_id and row['request_sha256']==hashlib.sha256(call.payload).hexdigest()
            assert row['kernel_sha256']==digest(KERNEL) and row['native_calls_sha256']==digest(REQUEST_SOURCE)
            assert row['loaded_http_code_matches_source'] and row['loop_running'] and not row['loop_closed']
            loops.setdefault(row['worker_pid'],set()).add((row['native_thread_id'],row['loop_generation']))
        assert len(loops)==2 and all(len(values)==1 for values in loops.values())
        if mode=='ray':assert len({row['ray_actor_id'] for row in traces})==2
        report['modes'].append(dict(runtime=mode,passed=True,posts=2000,original_query_result_rows=5,
            distinct_occurrences=2000,author_dbend_query_batches=1,prepared=prepared,close=result['close'],
            stable_author_loops={str(pid):list(values) for pid,values in loops.items()},
            complete_response_equality=True,request_multiset_identity=True,original_parser=result['original_parser']))
        report['modes'][-1]['public_entry']=entry


def parse_args(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true',help='start only after the separate software budget is approved')
    parser.add_argument('--scope',choices=('lifecycle','q1'),default='lifecycle')
    for name in ('author-root','bridge-binary','output','source-manifest'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--wall-limit-s',type=int,default=600)
    parser.add_argument('--post-limit',type=int,default=384)
    parser.add_argument('--q1-spec',type=Path)
    parser.add_argument('--lifecycle-evidence',type=Path)
    return parser.parse_args(argv)


def main(argv=None):
    args=parse_args(argv)
    if not args.execute:raise ValueError('explicit --execute required; this command does not upload or request authorization')
    if not __debug__:raise ValueError('verification requires Python assertions enabled')
    wall_max,post_min,post_max=(600,360,384) if args.scope=='lifecycle' else (900,4008,4096)
    if not 120<=args.wall_limit_s<=wall_max or not post_min<=args.post_limit<=post_max:raise ValueError('finite software limits differ from this matrix')
    if sys.platform!='linux' or set(os.sched_getaffinity(0))!=set(range(16,24)):raise ValueError('requires the assigned CPU16-23 affinity')
    args.author_root=args.author_root.resolve();args.bridge_binary=args.bridge_binary.resolve();args.output=args.output.resolve()
    args.source_manifest=args.source_manifest.resolve()
    for p in (args.author_root,args.bridge_binary,args.output,args.source_manifest):require_outside_git(p)
    manifest=json.loads(args.source_manifest.read_text())
    if set(manifest['files_sha256'])!=set(SOURCE_FILES):raise ValueError('candidate source manifest is incomplete')
    for name,sha in manifest['files_sha256'].items():
        if digest(CODE.parent/name)!=sha:raise ValueError('candidate source differs from delivered source manifest: '+name)
    if any(CODE.rglob('*.pyc')):raise ValueError('execute from a fresh source-only candidate directory; bytecode caches are unsupported')
    if args.scope=='q1':
        if args.q1_spec is None or args.lifecycle_evidence is None:raise ValueError('Q1 requires lifecycle evidence and original input specification')
        for path in (args.q1_spec,args.lifecycle_evidence):require_outside_git(path.resolve())
        lifecycle=json.loads(args.lifecycle_evidence.read_text())
        if (not lifecycle['passed'] or lifecycle['scope']!='lifecycle' or lifecycle['candidate_sha']!=manifest['candidate_sha']
            or lifecycle['files_sha256']!=manifest['files_sha256'] or lifecycle['models'] or lifecycle['gpu']
            or lifecycle['actual_posts']>384 or lifecycle['elapsed_seconds']>600):raise ValueError('same candidate must pass the separate lifecycle allowance first')
    identity=json.loads((args.author_root/'artifacts/build-patched/build-identity.json').read_text())
    if identity['upstream_commit']!=AUTHOR or identity['mode']!='patched':raise ValueError('checked author build required')
    for name,sha in dict(identity['original_sha256'],**identity['patched_sha256']).items():
        if digest(args.author_root/'source'/name)!=sha:raise ValueError('checked author source changed')
    for name,sha in identity['binaries_sha256'].items():
        if digest(args.author_root/'prefix-patched'/name)!=sha:raise ValueError('author binary changed')
    linked=json.loads(args.bridge_binary.with_suffix('.build.json').read_text())
    if linked['returncode'] or linked['binary_sha256']!=digest(args.bridge_binary) or linked['source_sha256']!=digest(KERNEL.with_name('imlane_http.cpp')):
        raise ValueError('bridge binary/source identity differs')
    import importlib.metadata as metadata
    versions={name:metadata.version(name) for name in ('pyarrow','ray','psutil','aiohttp')}
    if versions['pyarrow']!='16.1.0' or versions['ray']!='2.56.1':raise ValueError('existing author Python environment required')
    args.output.mkdir(parents=True,exist_ok=False)
    helper_path=args.author_root/'harness/code/integrations/imlane/verify.py'
    if digest(helper_path)!=identity['owned_sha256']['verify.py']:raise ValueError('checked author helper changed')
    spec=importlib.util.spec_from_file_location('owned_author_helpers',helper_path);helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
    started=time.monotonic();stopped=threading.Event();fixture=None
    args.stop_budget=StopBudget(stopped);args.scopes=[]
    report=dict(passed=False,models=0,gpu=0,started_monotonic=started,hard_deadline_monotonic=started+args.wall_limit_s,
        scope=args.scope,wall_limit_s=args.wall_limit_s,post_limit=args.post_limit,
        planned_max_posts=348 if args.scope=='lifecycle' else 4000,cpu_affinity=sorted(os.sched_getaffinity(0)),
        versions=versions,kernel_sha256=digest(KERNEL),native_calls_sha256=digest(REQUEST_SOURCE),
        candidate_sha=manifest['candidate_sha'],source_manifest_sha256=digest(args.source_manifest),
        files_sha256=manifest['files_sha256'],bridge_sha256=digest(args.bridge_binary),modes=[],cleanup=[],cleanup_errors=[])
    previous={number:signal.signal(number,args.stop_budget.signal) for number in (signal.SIGALRM,signal.SIGTERM,signal.SIGINT)}
    signal.setitimer(signal.ITIMER_REAL,max(1,args.wall_limit_s-90))
    try:
        fixture=Fixture(args.post_limit,stopped,lambda:[scope.abort_owned() for scope in args.scopes if not scope.closed],
            q1=args.scope=='q1',journal_root=args.output)
        for mode in ('cpython','ray'):
            (run_mode if args.scope=='lifecycle' else run_q1)(args,mode,helper,fixture,stopped,report)
        report['passed']=len(report['modes'])==2 and all(v['passed'] for v in report['cleanup'])
    except BaseException as error:report.update(error_type=type(error).__name__,error=redact_text(str(error)))
    finally:
        try:
            with args.stop_budget.cleanup():
                for scope in args.scopes:
                    try:scope.close()
                    except BaseException as error:report['cleanup_errors'].append(dict(type=type(error).__name__,error=redact_text(str(error))))
                if fixture is not None:
                    try:fixture.close()
                    except BaseException as error:report['cleanup_errors'].append(dict(type=type(error).__name__,error=redact_text(str(error))))
                    report.update(actual_posts=fixture.posts,fixture_receipts=fixture.receipts,
                        fixture_responses=fixture.responses,
                        reserved_max_posts=fixture.allowance.reserved,post_guard_violations=fixture.allowance.violations,
                        fixture_journal_errors=fixture.journal_errors,
                        fixture_peak_active=fixture.peak,fixture_active_after_cleanup=fixture.active,
                        fixture_connections_closed=fixture.connections==fixture.closed_connections)
                report['elapsed_seconds']=time.monotonic()-started;report['stop_signal']=args.stop_budget.reason
                report['passed']=(report['passed'] and not stopped.is_set() and not report['cleanup_errors']
                    and all(v['passed'] for v in report['cleanup']) and report.get('actual_posts',0)<=args.post_limit
                    and not report.get('post_guard_violations') and not report.get('fixture_journal_errors')
                    and report['elapsed_seconds']<=args.wall_limit_s and report.get('fixture_active_after_cleanup')==0
                    and report.get('fixture_connections_closed') is True and report.get('fixture_peak_active',3)<=2)
                write_private_json(args.output/'summary.json',report)
                print(json.dumps({k:v for k,v in report.items() if k not in ('modes','cleanup','fixture_receipts','fixture_responses')}))
        finally:
            signal.setitimer(signal.ITIMER_REAL,0)
            for number,handler in previous.items():signal.signal(number,handler)
    return 0 if report['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
