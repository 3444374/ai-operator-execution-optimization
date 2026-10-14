"""Bounded default/session qualification against separately owned author runtimes."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import argparse
import copy
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time
from urllib.parse import urlsplit

CODE=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(CODE))
sys.path.insert(0,str(Path(__file__).resolve().parent))
import verify_imlane_async_calls as tools
from imlane_session_sdk_client import verify_source_manifest
from src.baselines.common.private_artifacts import require_outside_git,write_private_json
from src.baselines.common.redact import redact_text
from src.baselines.text.sembench_movie import verify_checkout
from src.baselines.text.frameworks.native_calls import NativeCall
from src.execution_provider.adapters.full_response import encode_full_response

KERNEL=Path(__file__).with_name('imlane_complete_call.py')
SDK_CLIENT=Path(__file__).with_name('imlane_session_sdk_client.py')
REQUIRED_SOURCES=(
 'code/integrations/sema/verify_imlane_session.py','code/integrations/sema/imlane_session_sdk_client.py',
 'code/integrations/sema/verify_imlane_async_calls.py','code/integrations/sema/imlane_complete_call.py',
 'code/integrations/sema/imlane_http_udf.py','code/integrations/sema/imlane_http.cpp',
 'code/src/execution_provider/adapters/imlane_session.py','code/src/execution_provider/adapters/sema_imlane.py',
 'code/src/baselines/text/frameworks/imlane_calls.py','code/src/baselines/text/frameworks/imlane_limits.py',
 'code/src/baselines/text/frameworks/native_calls.py','code/src/baselines/text/frameworks/prepared_map.py',
 'code/src/execution_provider/adapters/full_response.py','code/src/semantic_methods/lotus/native.py',
 'code/src/semantic_methods/lotus/sdk.py','code/src/semantic_methods/lotus/batch.py',
 'code/src/experiments/postgresql/sembench_queries.py','code/src/baselines/text/sembench_movie.py')

def require(value,message):
    if not value:raise AssertionError(message)

def response_key(response):
    return (response.status_code,tuple(response.headers),response.http_version,
            tools.hashlib.sha256(response.body).hexdigest(),len(response.body))

def fixture_key(row):
    return (row['status_code'],tuple(tuple(v) for v in row['headers']),row['http_version'],
            row['body_sha256'],row['body_bytes'])

def check_complete(calls,outcomes,receipts,responses):
    """Check byte identities and header multiplicity, including repeated source rows."""
    original={c.call_id:c for c in calls}
    require(len(original)==len(calls),'input occurrence IDs repeat within one batch')
    require(len(outcomes)==len(calls) and {o.call_id for o in outcomes}==set(original),
            'complete outcomes do not cover each occurrence once')
    require(len({o.call_id for o in outcomes})==len(calls),'duplicate outcome')
    require(all(o.row_id==original[o.call_id].row_id and o.response is not None for o in outcomes),
            'unexpected transport/error value or changed source row')
    require(len(receipts)==len(calls) and len(responses)==len(calls),'HTTP receipt/result count differs')
    require(Counter(r['call_id'] for r in receipts)==Counter(original.keys()),'request occurrence count differs')
    for r in receipts:
        c=original[r['call_id']]
        require(r['method']=='POST' and r['path']==urlsplit(c.endpoint).path,'complete method/path changed')
        require(r['payload_sha256']==tools.hashlib.sha256(c.payload).hexdigest() and r['payload_bytes']==len(c.payload),
                'complete request body changed')
        for name in {k.lower() for k,_v in c.headers}:
            expected=[v for k,v in c.headers if k.lower()==name]
            actual=[v for k,v in r['headers'] if k.lower()==name]
            require(expected==actual,'complete request headers changed: '+name)
    by_id={o.call_id:o for o in outcomes}
    require(Counter(r['call_id'] for r in responses)==Counter(original.keys()),'response occurrence count differs')
    for r in responses:
        require(response_key(by_id[r['call_id']].response)==fixture_key(r),'complete HTTP response changed')

def fixture_body(_path,receipt,body):
    value=json.loads(body)
    value['id']=receipt['call_id'];value['fixture_cookie']=receipt['cookie']
    return json.dumps(value,ensure_ascii=False,separators=(',',':')).encode()

def calls(fixture,label,number=4,*,path='/ok',timeout_s=2.,delay=True):
    return tuple(NativeCall(label+':'+str(i),5,b' {"text":"same source","nested":{"n":1}} \n',
        fixture.base+path,(('Content-Type','application/json'),('X-Call-ID',label+':'+str(i)),
         ('X-Request-Repeat','one'),('X-Request-Repeat','two'),('Cookie','fixture='+label+'-'+str(i)),
         ('X-Delay','.3' if delay and i==0 else '0')),timeout_s,max_response_bytes=65536) for i in range(number))

def check_lifecycle(report,reuse,index,rows):
    require(report.get('clean') is True,'healthy batch did not finish cleanly')
    stats=report.get('bridge_stats') or {}
    require(stats.get('lifecycle')==dict(context_creations=int(not reuse or index==0),
        registrations=int(not reuse or index==0),author_calls=rows,output_copies=rows),'context/copy lifecycle differs')
    if reuse:
        require(report.get('bridge_lifecycle')=='session' and report.get('batch_callbacks_returned') is True,
                'session batch fence is incomplete')
        require(report.get('bridge_stopped') is False and stats.get('session_batches')==index+1,
                'healthy session stopped or batch sequence changed')
    else:
        require(report.get('bridge_lifecycle')=='per_batch' and report.get('bridge_stopped') is True
                and report.get('returncode')==0,'default bridge EOF/exit was not confirmed')

def healthy_batches(scope,fixture):
    """One executor object owns all three batches; first outputs remain retained."""
    executor=scope.executor();reports=[];retained=None;frozen=None
    for index,path in enumerate(('/ok','/rate','/gzip')):
        values=calls(fixture,'same-occurrence',path=path)
        start=len(fixture.receipts);response_start=len(fixture.responses)
        fixture.allowance.reserve(values)
        outcomes=list(executor.iter_batch(values,timeout_s=45))
        fixture.settled()
        check_complete(values,outcomes,fixture.receipts[start:],fixture.responses[response_start:])
        require([o.call_id for o in outcomes]!=[c.call_id for c in values],'controlled completion did not arrive out of order')
        close=copy.deepcopy(executor.last_close_report)
        check_lifecycle(close,scope.config.reuse_bridge,index,len(values))
        if index==0:
            retained=tuple(outcomes);frozen=tuple(encode_full_response(o.response) for o in retained)
        require(tuple(encode_full_response(o.response) for o in retained)==frozen,'retained first-batch output changed after Lane reuse')
        reports.append(close)
    if scope.config.reuse_bridge:
        require(len({r['bridge_pid'] for r in reports})==1,'three batches did not reuse one bridge process')
        require(len({r['batch_id'] for r in reports})==3,'batch IDs repeated')
        process=executor.delegate._process
        executor.close()
        require(process.poll()==0,'session close fence/zero exit was not confirmed')
    else:executor.close()
    return dict(passed=True,batches=reports,output_lifecycle=True,controlled_reordering=True,posts=12)

def check_quarantine(scope,executor,fixture):
    report=copy.deepcopy(executor.last_close_report)
    require(report.get('clean') is False and report.get('external_coordinator_requires_cleanup') is True,
            'unknown author work was accepted as clean')
    require(report.get('bridge_stopped') is True and report.get('remote_http_stopped')=='unavailable',
            'bridge stop was confused with remote HTTP cancellation')
    marker=executor._uncertain
    require(marker.is_file(),'unknown work lacks its persistent coordinator marker')
    first=marker.read_bytes()
    replacement=scope.executor()
    try:list(replacement.iter_batch(calls(fixture,'forbidden-replacement',number=1,delay=False)))
    except RuntimeError as error:require('unsettled' in str(error),'replacement failed for an unrelated reason')
    else:raise AssertionError('same coordinator identity accepted a new owner after uncertainty')
    require(marker.read_bytes()==first,'later replacement overwrote the first unknown record')
    fixture.settled()
    require(all(r['call_id']!='forbidden-replacement:0' for r in fixture.receipts),'replacement submitted HTTP')
    return dict(passed=True,close=report,marker_sha256=tools.hashlib.sha256(first).hexdigest(),new_owner_rejected=True)

def fault_batch(scope,fixture,case):
    executor=scope.executor()
    warm=calls(fixture,case+'-warm',number=1,delay=False)
    fixture.allowance.reserve(warm);list(executor.iter_batch(warm,timeout_s=45))
    require(executor.last_close_report.get('clean') is True,'fault probe warmup failed')
    fixture.settled();fixture.slow.clear()
    primary=None
    if case=='consumer':
        values=calls(fixture,case,delay=False);fixture.allowance.reserve(values)
        iterator=executor.iter_batch(values,timeout_s=45)
        expected=ValueError('fixture first consumer failure')
        cleanup_error=None
        try:next(iterator);raise expected
        except BaseException as error:primary=error
        finally:
            try:iterator.close()
            except BaseException as error:
                cleanup_error=error
                primary.add_note('Consumer iterator cleanup also failed: '+type(error).__name__)
        if primary is not expected or cleanup_error is not None:raise primary
    else:
        values=calls(fixture,case,number=1,path='/slow',timeout_s=.05 if case=='transport' else 2.,delay=False)
        fixture.allowance.reserve(values)
        if case=='transport':
            outcome,=executor.iter_batch(values,timeout_s=45)
            require(outcome.response is None and bool(outcome.transport_error),'transport fault became a model response')
        else:
            cancelled=threading.Event()
            with ThreadPoolExecutor(max_workers=1) as pool:
                future=pool.submit(lambda:list(executor.iter_batch(values,cancelled=cancelled.is_set,
                    timeout_s=.4 if case=='timeout' else 45)))
                if not fixture.slow.wait(3):
                    if future.done():future.result()
                    raise AssertionError('fault occurred before the fixture received the author HTTP request')
                if case=='cancel':cancelled.set()
                if case=='crash':
                    process=executor.delegate._process
                    require(process is not None,'no live owned bridge to crash')
                    scope.track_root(process)
                    error=scope.signal_identity((process.pid,tools.process_start_ticks(process.pid)),signal.SIGKILL)
                    require(error is None,'owned bridge crash signal failed')
                expected={'timeout':TimeoutError,'cancel':InterruptedError,'crash':RuntimeError}[case]
                try:future.result(timeout=8)
                except expected as error:
                    require(future.done(),'owned iterator remained alive after the fault wait bound')
                    primary=error
                else:raise AssertionError(case+' did not stop the batch')
    result=check_quarantine(scope,executor,fixture)
    result.update(case=case,first_error=None if primary is None else dict(type=type(primary).__name__,error=redact_text(str(primary))))
    return result

class SessionScope(tools.AuthorScope):
    """Use the existing author scope; add bounds for this smaller software window."""
    def __init__(self,args,mode,helper,report,*,reuse_bridge,expected_fault=False):
        super().__init__(args,mode,helper,report,kernel=KERNEL,reuse_bridge=reuse_bridge,max_batch_rows=16)
        self.expected_fault=expected_fault;self.rss_peak=0;self.shm_peak=0

    def sample(self):
        import psutil
        while not self.stop_sampler.wait(.1):
            try:
                for executor in self.executors:
                    p=executor.delegate._process
                    if p is not None and p.poll() is None and p.pid not in self.roots:self.track_root(p)
                self.snapshot()
                with self.identities_lock:identities=list(self.known)
                rss=0
                for pid,ticks in identities:
                    try:
                        if tools.process_start_ticks(pid)==ticks:rss+=psutil.Process(pid).memory_info().rss
                    except (psutil.Error,OSError):pass
                shm=sum(self.helper.resources(self.namespace).values())
                self.rss_peak=max(self.rss_peak,rss);self.shm_peak=max(self.shm_peak,shm)
                if rss>8*1024**3 or shm>2*1024**3:raise RuntimeError('software RSS/SHM bound exceeded')
                deadline=self.args.cell_deadline
                if deadline is not None and time.monotonic()>=deadline:raise TimeoutError('software cell deadline; cleanup reserve begins')
                self.args.check_lease()
            except Exception as error:
                self.args.resource_errors.append(dict(type=type(error).__name__,error=redact_text(str(error))))
                self.args.stop_budget.stopped.set();self.abort_owned();return

    def close(self):
        before=len(self.report['cleanup'])
        try:super().close()
        except RuntimeError:
            if len(self.report['cleanup'])==before:raise
            record=self.report['cleanup'][-1]
            verified=(self.expected_fault and not record['errors'] and self.closed
                and record['owned_survivors']==[] and record['remaining_shm']=={} and record['owned_threads_stopped'])
            if not verified:raise
            # Preserve the original non-clean teardown and its forced actions.
            record['expected_fault_owner_cleanup_verified']=True
            write_private_json(self.root/'expected-fault-cleanup.json',record)
        finally:
            if len(self.report['cleanup'])>before:
                self.report['cleanup'][-1].update(rss_peak_bytes=self.rss_peak,shared_memory_peak_bytes=self.shm_peak)

def check_workers(scope,expected):
    rows=[json.loads(line) for p in (scope.root/'worker-traces').glob('*.jsonl') for line in p.read_text().splitlines()]
    require(len(rows)==expected,'author HTTP trace does not cover complete calls')
    require(all(r['kernel_sha256']==tools.digest(KERNEL) for r in rows),'a different UDF handled calls')
    workers={r['worker_pid'] for r in rows}
    require(len(workers)==2,'fixture did not exercise both declared author workers')
    scope.snapshot()
    require(all((pid,tools.process_start_ticks(pid)) in scope.known for pid in workers),
            'HTTP work did not run in the owned author process tree')
    if scope.mode=='ray':require(len({r.get('ray_actor_id') for r in rows})==2 and all(r.get('ray_actor_id') for r in rows),
                               'author Ray actors were not separately verified')
    return dict(passed=True,workers=sorted(workers),rows=len(rows),runtime=scope.mode)

def run_sdk(args,scope,case):
    config=scope.root/'sdk-config.json';write_private_json(config,asdict(scope.config))
    output=scope.root/('sdk-'+case)
    env=dict(scope.env,PYTHONPATH=str(CODE),HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',
             LITELLM_LOCAL_MODEL_COST_MAP='True',CUDA_VISIBLE_DEVICES='')
    env['PATH']=str(args.lotus_python.parent)+os.pathsep+os.environ['PATH']
    command=[str(args.lotus_python),'-B',str(SDK_CLIENT),'--config',str(config),
        '--tokenizer',str(args.tokenizer),'--checkout',str(args.checkout),'--source-manifest',str(args.source_manifest),
        '--output',str(output),'--case',case]
    process,writer=scope.helper.spawn(command,env,scope.root/('sdk-'+case+'.log'))
    scope.children.append((process,writer));scope.track_root(process)
    timeout=None if case=='healthy' and getattr(args,'wall_limit_s',1200)==0 else 120
    require(process.wait(timeout=timeout)==0,'original SDK/query software client failed')
    writer.join(1)
    result=json.loads((output/'summary.json').read_text())
    require(result.get('passed') is True and result.get('runtime')==scope.mode
        and result.get('reuse_bridge') is scope.config.reuse_bridge and result.get('models')==result.get('gpu')==0,
        'SDK client did not qualify this exact software cell')
    expected=28 if case=='healthy' else 4
    require(result['fixture']['actual_posts']<=expected and result['fixture']['reserved']<=expected
            and not result['fixture']['violations'],'SDK child exceeded its finite POST reservation')
    require(case!='healthy' or result['fixture']['actual_posts']==28,'original SDK/query request count changed')
    require(result['source']['candidate_commit']==args.candidate_commit,'SDK child used another source candidate')
    return result

def run_cell(args,mode,reuse,helper,report):
    started=time.monotonic();stopped=args.stop_budget.stopped
    wall_limit=getattr(args,'wall_limit_s',1200)
    cell_limit=0 if wall_limit==0 else min(600,wall_limit)
    args.cell_deadline=None if cell_limit==0 else started+cell_limit-60
    result=dict(runtime=mode,reuse_bridge=reuse,passed=False,cases=[],models=0,gpu=0,post_limit=128,wall_limit_s=cell_limit)
    fixture=tools.Fixture(96,stopped,lambda:[s.abort_owned() for s in args.scopes if not s.closed],response_factory=fixture_body)
    primary=None
    try:
        with SessionScope(args,mode,helper,report,reuse_bridge=reuse) as scope:
            result['healthy']=healthy_batches(scope,fixture)
            result['sdk']=run_sdk(args,scope,'healthy')
            result['workers']=check_workers(scope,40)
        for case in ('transport','timeout','consumer','cancel','crash'):
            args.stop_budget.check();args.check_lease()
            with SessionScope(args,mode,helper,report,reuse_bridge=reuse,expected_fault=True) as scope:
                result['cases'].append(fault_batch(scope,fixture,case))
        with SessionScope(args,mode,helper,report,reuse_bridge=reuse,expected_fault=True) as scope:
            result['parser']=run_sdk(args,scope,'parser')
        result['passed']=True
    except BaseException as error:
        primary=error
        result['first_error']=dict(type=type(error).__name__,error=redact_text(str(error)))
        raise
    finally:
        cleanup_error=None
        try:
            with args.stop_budget.cleanup():fixture.close()
        except BaseException as error:
            cleanup_error=error
            result['fixture_cleanup_error']=dict(type=type(error).__name__,error=redact_text(str(error)))
            if primary is not None:primary.add_note('Fixture cleanup also failed: '+type(error).__name__)
        result.update(root_posts=fixture.posts,root_reserved=fixture.allowance.reserved,root_post_violations=fixture.allowance.violations,
            fixture_journal_errors=fixture.journal_errors,root_connections_closed=fixture.connections==fixture.closed_connections,
            elapsed_seconds=time.monotonic()-started)
        result['actual_posts']=fixture.posts+sum(result.get(k,{}).get('fixture',{}).get('actual_posts',0) for k in ('sdk','parser'))
        result['passed']=(result['passed'] and cleanup_error is None and result['actual_posts']<=128 and not fixture.allowance.violations
            and not fixture.journal_errors and result['root_connections_closed'] and not stopped.is_set()
            and (cell_limit==0 or result['elapsed_seconds']<=cell_limit))
        report['cells'].append(result)
        if primary is None and cleanup_error is not None:raise cleanup_error
    return result

def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute',action='store_true')
    p.add_argument('--runtime',choices=('cpython','ray'),required=True,help='one author runtime per separately scheduled window')
    p.add_argument('--path-mode',choices=('default','reuse','both'),default='both')
    for name in ('author-root','bridge-binary','lotus-python','tokenizer','checkout','source-manifest','output','lease-file'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--lease-sha256',required=True,help='main coordinator snapshot of its held shared-host lease; never acquire a lease here')
    p.add_argument('--cpu-ids',required=True,help='eight comma-separated CPU IDs assigned by the main coordinator')
    p.add_argument('--cpu-quota-cores',type=int,default=16,help='actual host quota confirmed by owner, never affinity length')
    p.add_argument('--wall-limit-s',type=int,default=1200,
        help='0 disables the healthy total window limit; otherwise 120-1200 seconds including cleanup')
    return p.parse_args(argv)

def resolve_inputs(args):
    for name in ('author_root','bridge_binary','lotus_python','tokenizer','checkout','source_manifest','output','lease_file'):
        # Resolving a venv interpreter symlink selects the base Python's environment.
        path=getattr(args,name).expanduser().absolute() if name=='lotus_python' else getattr(args,name).resolve()
        # SemBench is a read-only pinned Git source; artifacts/configuration stay outside Git.
        if name!='checkout':require_outside_git(path)
        setattr(args,name,path)

def verify_inputs(args):
    if not args.execute:raise ValueError('explicit --execute required; this verifier never uploads or builds')
    ids=[int(n) for n in args.cpu_ids.split(',')]
    if (sys.platform!='linux' or len(ids)!=len(set(ids)) or len(ids)!=8 or set(os.sched_getaffinity(0))!=set(ids)
            or not 8<=args.cpu_quota_cores<=16):raise ValueError('requires assigned eight CPU slots within the owner-confirmed <=16-core quota')
    if args.wall_limit_s!=0 and not 120<=args.wall_limit_s<=1200:
        raise ValueError('software window must be 0 or 120-1200 seconds including cleanup')
    resolve_inputs(args)
    def check_lease():
        if not args.lease_file.is_file() or tools.digest(args.lease_file)!=args.lease_sha256:
            raise RuntimeError('assigned shared-host lease changed or was released; stop this window')
    args.check_lease=check_lease;check_lease()
    # Use the SDK child's source validator before any author scope can submit HTTP.
    manifest=verify_source_manifest(args.source_manifest);files=manifest['files_sha256']
    require(set(REQUIRED_SOURCES)<=set(files),'source lock misses the session/SDK qualification inputs')
    args.candidate_commit=manifest['candidate_commit']
    verify_checkout(args.checkout)
    identity=json.loads((args.author_root/'artifacts/build-patched/build-identity.json').read_text())
    require(identity['upstream_commit']==tools.AUTHOR and identity['mode']=='patched','checked author build required')
    for name,sha in dict(identity['original_sha256'],**identity['patched_sha256']).items():
        require(tools.digest(args.author_root/'source'/name)==sha,'author source changed')
    for name,sha in identity['binaries_sha256'].items():
        require(tools.digest(args.author_root/'prefix-patched'/name)==sha,'author binary changed')
    built=json.loads(args.bridge_binary.with_suffix('.build.json').read_text())
    require(built['returncode']==0 and built['source_sha256']==tools.digest(KERNEL.with_name('imlane_http.cpp'))
        and built['binary_sha256']==tools.digest(args.bridge_binary),'new complete bridge build identity differs')
    versions={n:importlib.metadata.version(n) for n in ('pyarrow','ray','psutil','aiohttp')}
    require(versions==dict(pyarrow='16.1.0',ray='2.56.1',psutil='7.0.0',aiohttp='3.14.5'),'existing dedicated author interpreter required')
    helper_path=args.author_root/'harness/code/integrations/imlane/verify.py'
    require(tools.digest(helper_path)==identity['owned_sha256']['verify.py'],'author helper changed')
    spec=importlib.util.spec_from_file_location('fixed_session_author_helpers',helper_path)
    helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
    return helper,versions,ids

def main(argv=None):
    args=parse_args(argv);helper,versions,ids=verify_inputs(args)
    args.output.mkdir(parents=True,exist_ok=False)
    args.scopes=[];args.resource_errors=[];stopped=threading.Event();args.stop_budget=tools.StopBudget(stopped)
    report=dict(schema='semloom.imlane_session_author.v1',passed=False,runtime=args.runtime,candidate_commit=args.candidate_commit,
        cells=[],cleanup=[],resource_errors=args.resource_errors,models=0,gpu=0,retries=0,
        cpu_ids=ids,host_cpu_quota_cores=args.cpu_quota_cores,versions=versions,wall_limit_s=args.wall_limit_s,
        source_manifest_sha256=tools.digest(args.source_manifest),bridge_sha256=tools.digest(args.bridge_binary))
    started=time.monotonic();handlers={n:signal.signal(n,args.stop_budget.signal) for n in (signal.SIGALRM,signal.SIGTERM,signal.SIGINT)}
    if args.wall_limit_s:signal.setitimer(signal.ITIMER_REAL,args.wall_limit_s-60)
    try:
        modes=(False,True) if args.path_mode=='both' else (args.path_mode=='reuse',)
        for reuse in modes:
            args.stop_budget.check();args.check_lease()
            require(run_cell(args,args.runtime,reuse,helper,report)['passed'],'software cell failed')
        report['passed']=True
    except BaseException as error:report.update(first_error_type=type(error).__name__,first_error=redact_text(str(error)))
    finally:
        try:
            with args.stop_budget.cleanup():
                for scope in args.scopes:
                    if scope.closed:continue
                    try:scope.close()
                    except BaseException as error:
                        report.setdefault('cleanup_errors',[]).append(dict(type=type(error).__name__,error=redact_text(str(error))))
                report['elapsed_seconds']=time.monotonic()-started
                report['passed']=(report['passed'] and not stopped.is_set() and not report.get('cleanup_errors') and not args.resource_errors
                    and all(r['passed'] or r.get('expected_fault_owner_cleanup_verified') for r in report['cleanup'])
                    and (args.wall_limit_s==0 or report['elapsed_seconds']<=args.wall_limit_s))
                write_private_json(args.output/'summary.json',report)
        finally:
            signal.setitimer(signal.ITIMER_REAL,0)
            for n,h in handlers.items():signal.signal(n,h)
    print(json.dumps({k:report[k] for k in ('passed','runtime','candidate_commit','models','gpu','elapsed_seconds')}))
    return 0 if report['passed'] else 1

if __name__=='__main__':raise SystemExit(main())
