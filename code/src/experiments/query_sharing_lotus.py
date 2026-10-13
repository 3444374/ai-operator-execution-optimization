"""Two real LOTUS frontend processes; bounded localhost fixture, zero models.

The native arm retains LOTUS/LiteLLM's own two-worker pool per LM. Internal
controls use the same private bridge and Core; the fixture owns two service slots
in every arm. Fixture time is not a GPU performance prediction.
"""

from dataclasses import asdict, is_dataclass
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.metadata import version
import ipaddress
import json
import multiprocessing as mp
import os
from pathlib import Path
import re
import socket
from statistics import median
import threading
import time

from ..baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION
from ..execution_provider.adapters.model_config import FixedModelConfig
from ..execution_provider.adapters.native_query_bridge import NativeTaskBridge, NativeTaskClient
from ..execution_provider.adapters.native_tasks import build_native_execution
from ..scheduling.core.session_jobs import round_robin_flow, shared_compute_job_budget
from ..scheduling.submission_control.query_sharing import BoundedShortJobBurst
from .query_sharing_probe import least_active_work


ROW_PATTERN = re.compile(r'\[fixture:(long|short):(\d+):(POSITIVE|NEGATIVE)\]')
ARMS = ('native_shared', 'work_conserving', 'strong_static', 'candidate')


def row_text(query, sequence):
    label = 'POSITIVE' if sequence % 2 == 0 else 'NEGATIVE'
    return f'[fixture:{query}:{sequence}:{label}] ' + ('A wonderful movie.' if label == 'POSITIVE' else 'A terrible movie.')


def identity(body):
    matches = ROW_PATTERN.findall('\n'.join(m['content'] for m in body['messages']))
    if len(matches) != 1:
        raise ValueError('fixture request must contain exactly one row identity')
    query, sequence, label = matches[0]
    return query, int(sequence), label


def digest(body):
    return hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


class FixtureService:
    """Read complete requests; one nonpreemptive fixture delay per row, two slots."""

    def __init__(self, delay_s=.025, max_posts=5000):
        if not 0 <= delay_s <= .1 or not 1 <= max_posts <= 5000:
            raise ValueError('invalid finite fixture budget')
        self.delay_s, self.max_posts = delay_s, max_posts
        self.calls, self.errors = [], []
        self.lock, self.slots = threading.Lock(), threading.BoundedSemaphore(2)
        self.active, self.peak = 0, 0
        self.run_id = None
        service = self
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.connection.settimeout(5)
                record = None
                try:
                    length = int(self.headers['Content-Length'])
                    if self.path != '/v1/chat/completions' or not 0 < length <= 8 * 1024 * 1024:
                        raise ValueError('invalid fixture HTTP request')
                    body = json.loads(self.rfile.read(length))
                    query, sequence, label = identity(body)
                    record = dict(run_id=service.run_id, query_id=query, sequence=sequence,
                                  body=body, body_sha256=digest(body), arrival=time.monotonic())
                    with service.lock:
                        if len(service.calls) >= service.max_posts:
                            raise RuntimeError('cumulative fixture POST budget exhausted')
                        service.calls.append(record)
                    with service.slots:
                        with service.lock:
                            service.active += 1
                            service.peak = max(service.peak, service.active)
                            record['service_start'] = time.monotonic()
                            record['active'] = service.active
                        try:
                            time.sleep(service.delay_s)
                            raw = json.dumps(dict(id=f'fixture-{query}-{sequence}', object='chat.completion',
                                created=1, model=body['model'], choices=[dict(index=0,
                                    message=dict(role='assistant', content=label), finish_reason='stop')],
                                usage=dict(prompt_tokens=7, completion_tokens=1, total_tokens=8))).encode()
                            record['response_body'] = raw.decode()
                            record['response_sha256'] = hashlib.sha256(raw).hexdigest()
                            self.send_response(200)
                            self.send_header('Content-Type', 'application/json')
                            self.send_header('Content-Length', str(len(raw)))
                            self.end_headers()
                            self.wfile.write(raw)
                            record['http_finish'] = time.monotonic()
                        finally:
                            with service.lock:
                                record['service_finish'] = time.monotonic()
                                service.active -= 1
                except Exception as error:
                    with service.lock:
                        service.errors.append(dict(run_id=service.run_id, error_type=type(error).__name__))
                    self.close_connection = True
            def log_message(self, *args):
                pass
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def base(self):
        return f'http://127.0.0.1:{self.server.server_port}/v1'

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown(); self.server.server_close(); self.thread.join(2)
        if self.thread.is_alive() or self.active:
            raise RuntimeError('fixture service cleanup unconfirmed')


def _only_loopback():
    """Own child only: fail accidental package/tokenizer network downloads."""
    original = socket.socket.connect
    def connect(instance, address):
        if isinstance(address, tuple) and ipaddress.ip_address(address[0]).is_loopback:
            return original(instance, address)
        raise PermissionError('fixture process permits only loopback connections')
    socket.socket.connect = connect


def _frontend(channel, pipe, query, count, base, arm, release, cancelled):
    _only_loopback()
    os.environ.update(LITELLM_LOCAL_MODEL_COST_MAP='True', HF_HUB_OFFLINE='1',
                      TRANSFORMERS_OFFLINE='1', CUDA_VISIBLE_DEVICES='')
    flow_executor = None
    try:
        import lotus
        import pandas as pd
        from lotus.cache import InMemoryCache
        from lotus.models import LM
        from ..semantic_methods.lotus.batch import LotusBatchExecutor, lotus_executor
        from ..semantic_methods.lotus.sdk import validate_source, SOURCE_HASHES, LITELLM_SOURCE_HASHES
        validate_source()
        lm = LM('openai/lotus-fixture', api_base=base, api_key='local-fixture',
                max_batch_size=2, temperature=0, top_p=1, max_tokens=16, n=1,
                num_retries=0, max_retries=0, timeout=5, cache=InMemoryCache(max_size=512))
        rows = [(i, row_text(query, i)) for i in range(count)]
        frame = pd.DataFrame(rows, columns=['row_id', 'text'])
        if arm != 'native_shared':
            client = NativeTaskClient(pipe, query, timeout_s=10)
            flow_executor = LotusBatchExecutor(None,
                FixedModelConfig(base + '/chat/completions', 'lotus-fixture', 5000, 'local-fixture'),
                query_id=query, operator_id='lotus-map', session_factory=client.open_session,
                session_limits=client.describe_limits(),
                cancelled=cancelled.is_set, prevalidate_batch=True, retain_full_responses=False,
                timeout_s=30)
        channel.send(dict(event='ready', query_id=query, pid=os.getpid(),
            versions={p:version(p) for p in ('lotus-ai','litellm','openai','pandas','pyarrow')},
            lotus_source_sha256=SOURCE_HASHES, litellm_source_sha256=LITELLM_SOURCE_HASHES))
        if not release.wait(60):
            raise TimeoutError('frontend release was not supplied')
        scheduled = channel.recv()
        while time.monotonic() < scheduled:
            time.sleep(max(0,min(.005, scheduled - time.monotonic())))
        began, cpu = time.monotonic(), time.process_time()
        with lotus.settings.context(lm=lm, enable_cache=False), lotus_executor(lm, flow_executor):
            result = frame.sem_map(MOVIE_MAP_INSTRUCTION + '\nInput: {text}', suffix='_answer',
                                   return_raw_outputs=True)
        ended = time.monotonic()
        channel.send(dict(event='finished', query_id=query, pid=os.getpid(),
            scheduled=scheduled, began=began, ended=ended, jct_s=ended-scheduled,
            cpu_s=time.process_time()-cpu,
            rows=list(zip(result['row_id'].tolist(), result['_answer'].tolist())),
            lm_stats=asdict(lm.stats) if is_dataclass(lm.stats) else lm.stats.model_dump(),
            cleanup_errors=len(flow_executor.last_cleanup_errors) if flow_executor else 0))
    except BaseException as error:
        channel.send(dict(event='error', query_id=query, pid=os.getpid(),
                          error_type=type(error).__name__, error=str(error)[:1000]))
    finally:
        if pipe is not None:
            pipe.close()
        channel.close()


def _rss_bytes(pids):
    if not Path('/proc/self/status').exists():
        return None
    from multiprocessing import resource_tracker
    tracker_pid = resource_tracker._resource_tracker._pid
    total = 0
    for pid in set(pids+([tracker_pid] if tracker_pid else [])):
        try:
            text = Path(f'/proc/{pid}/status').read_text()
        except FileNotFoundError:
            continue
        match = re.search(r'^VmRSS:\s+(\d+) kB$', text, re.MULTILINE)
        if match: total += int(match[1]) * 1024
    return total


def run_pair(service, arm, *, long_rows=256, short_rows=8, delay_s=.5, active=2,
             cancel_at_s=None, run_id=None, window_end=None):
    """Common ready barrier + scheduled release; no future service info in policy."""
    if arm not in ARMS or not 1 <= long_rows <= 256 or not 0 <= short_rows <= 8 or active not in (2,4):
        raise ValueError('invalid finite paired workload')
    if not 0 <= delay_s <= 10 or (cancel_at_s is not None and (arm == 'native_shared' or not 0 < cancel_at_s <= 10)):
        raise ValueError('invalid fixture release or cancel')
    ctx = mp.get_context('spawn')
    counts = dict(long=long_rows, short=short_rows)
    if not short_rows: counts.pop('short')
    evidence = dict(run_id=run_id or arm, arm=arm, counts=counts, delay_s=delay_s,
                    active_requests=active if arm != 'native_shared' else None,
                    native_pool_per_lm=2, fixture_service_slots=2, fixture_delay_s=service.delay_s,
                    ready=[], frontends={}, bridge_events=[], core_events=[], rss_peak_bytes=0,
                    cancel_at_s=cancel_at_s, status='running')
    service.run_id = evidence['run_id']
    start_calls, start_errors = len(service.calls), len(service.errors)
    parent_cpu = time.process_time()
    bridge, children, controls = None, {}, {}
    release = ctx.Event()
    cancellations = {query:ctx.Event() for query in counts}
    if arm != 'native_shared':
        def factory():
            selector = (BoundedShortJobBurst(work_unit='work_units', short_work_limit=8, bypass_work_limit=8)
                        if arm == 'candidate' else least_active_work if arm == 'work_conserving' else round_robin_flow)
            def observe(event):
                evidence['core_events'].append(dict(event, t=time.monotonic()))
            execution = build_native_execution(
                FixedModelConfig(service.base+'/chat/completions','lotus-fixture',5000,'local-fixture'),
                physical=None, max_tasks=64, max_active_requests=active, max_jobs=len(counts),
                choose_flow=selector, allocate_job=shared_compute_job_budget, observer=observe)
            execution.engine.observe_job_work = True
            return execution
        bridge = NativeTaskBridge(factory, counts, observer=evidence['bridge_events'].append, context=ctx).start()
    try:
        for query, count in counts.items():
            parent, child = ctx.Pipe()
            connection = bridge.client_connection(query) if bridge else None
            process = ctx.Process(target=_frontend,
                args=(child,connection,query,count,service.base,arm,release,cancellations[query]))
            children[query], controls[query] = process, parent
            process.start(); child.close()
            if bridge:
                bridge.bind_process(query, process)
                connection.close()
        deadline = min(time.monotonic()+60,window_end or float('inf'))
        pending = set(counts)
        while pending:
            rss = _rss_bytes([os.getpid(),*(p.pid for p in children.values())])
            if rss is not None:
                evidence['rss_peak_bytes'] = max(evidence['rss_peak_bytes'],rss)
                if rss > 4*1024**3: raise RuntimeError('fixture aggregate driver RSS budget exhausted')
            for query in tuple(pending):
                if controls[query].poll(.01):
                    ready = controls[query].recv(); evidence['ready'].append(ready)
                    if ready['event'] != 'ready': raise RuntimeError('LOTUS frontend setup failed')
                    pending.remove(query)
                elif children[query].exitcode is not None:
                    raise RuntimeError('LOTUS frontend stopped before ready')
            if time.monotonic()>deadline: raise TimeoutError('LOTUS frontend setup timed out')
        epoch = time.monotonic()+.1
        evidence['release_epoch'] = epoch
        for query in counts:
            controls[query].send(epoch+(delay_s if query=='short' else 0))
        release.set()
        pending, cancelled = set(counts), False
        while pending:
            rss = _rss_bytes([os.getpid(),*(p.pid for p in children.values())])
            if rss is not None:
                evidence['rss_peak_bytes'] = max(evidence['rss_peak_bytes'],rss)
                if rss > 4*1024**3: raise RuntimeError('fixture aggregate driver RSS budget exhausted')
            if cancel_at_s is not None and not cancelled and time.monotonic() >= epoch+cancel_at_s:
                cancellations['long'].set(); bridge.request_cancel('long')
                evidence['cancel_observed'] = time.monotonic()
                evidence['at_cancel'] = bridge.snapshot()
                cancelled = True
            for query in tuple(pending):
                if controls[query].poll(.005):
                    evidence['frontends'][query] = controls[query].recv(); pending.remove(query)
                elif children[query].exitcode is not None:
                    raise RuntimeError('frontend disappeared without final result')
            if time.monotonic() > min(epoch+40,window_end or float('inf')): raise TimeoutError('LOTUS fixture pair timed out')
        for query, result in evidence['frontends'].items():
            expected_cancel = query=='long' and cancel_at_s is not None
            if expected_cancel:
                if (result['event'] != 'error' or result.get('error_type') != 'RuntimeError'
                        or result.get('error') not in ('LOTUS batch cancelled','LOTUS execution stopped')):
                    raise RuntimeError('cancelled frontend did not return its defined error')
            elif result['event'] != 'finished' or result['rows'] != [(i,'POSITIVE' if i%2==0 else 'NEGATIVE') for i in range(counts[query])]:
                raise RuntimeError('LOTUS row association/output differs')
        evidence['status'] = 'passed'
    except BaseException as error:
        evidence['status'], evidence['error_type'] = 'failed', type(error).__name__
        raise PairFailure(evidence) from error
    finally:
        for process in children.values():
            if process.pid is None: continue
            process.join(3)
            if process.is_alive(): process.terminate(); process.join(2)
        for control in controls.values(): control.close()
        if bridge:
            evidence['bridge_closed'] = bridge.close(7)
            evidence['final_core'] = bridge.snapshot()
        deadline = time.monotonic()+7
        while service.active and time.monotonic()<deadline: time.sleep(.01)
        evidence['calls'] = service.calls[start_calls:]
        evidence['http_errors'] = service.errors[start_errors:]
        evidence['exitcodes'] = {q:p.exitcode for q,p in children.items()}
        # Parent includes Core, fixture HTTP and orchestration threads; this is
        # an observed control cost, not an isolated scheduler CPU attribution.
        evidence['parent_cpu_s'] = time.process_time()-parent_cpu
        if (any(p.is_alive() or p.exitcode!=0 for p in children.values()) or service.active or evidence['http_errors']
                or (bridge and not evidence['bridge_closed'])):
            evidence['status'] = 'failed'
            raise PairFailure(evidence)
    return evidence


class PairFailure(RuntimeError):
    def __init__(self, evidence):
        super().__init__('LOTUS paired fixture failed; evidence retained')
        self.evidence = evidence


def audit(runs):
    """Rebuild associations/concurrency from fixture traces and client endpoints."""
    bodies, summaries = {}, []
    for run in runs:
        if run['status'] != 'passed': raise ValueError('failed paired run')
        identities = [(c['query_id'],c['sequence']) for c in run['calls']]
        if len(identities) != len(set(identities)): raise ValueError('duplicate physical request')
        expected = {(q,i) for q,n in run['counts'].items() for i in range(n)}
        if run['cancel_at_s'] is None and set(identities) != expected: raise ValueError('missing/extra physical request')
        if not set(identities) <= expected: raise ValueError('foreign physical request')
        timeline=[]
        for call in run['calls']:
            if call['arrival'] < run['release_epoch']+(run['delay_s'] if call['query_id']=='short' else 0):
                raise ValueError('HTTP request before query release')
            if call['body_sha256'] != digest(call['body']): raise ValueError('request body digest differs')
            for field,value in dict(model='lotus-fixture',temperature=0,top_p=1,n=1).items():
                if call['body'].get(field)!=value: raise ValueError('generation parameter differs')
            # The pinned SDK maps the requested max_tokens to the provider's
            # max_completion_tokens for this model. Compare its effective cap
            # and the complete body, without rewriting the supplier request.
            caps=[call['body'][k] for k in ('max_tokens','max_completion_tokens') if k in call['body']]
            if not caps or any(cap!=16 for cap in caps): raise ValueError('generation output cap differs')
            raw=call['response_body'].encode()
            if hashlib.sha256(raw).hexdigest()!=call['response_sha256']: raise ValueError('response body digest differs')
            response=json.loads(raw)
            if (response['model']!='lotus-fixture' or response['usage']['total_tokens']!=8
                    or response['choices'][0]['finish_reason']!='stop'
                    or response['choices'][0]['message']['content']!=identity(call['body'])[2]):
                raise ValueError('fixture complete response differs')
            key=(call['query_id'],call['sequence'])
            if key in bodies and bodies[key]!=call['body']: raise ValueError('paired complete request body differs')
            bodies[key]=call['body']
            timeline.extend(((call['service_start'],1),(call['service_finish'],-1)))
        current=peak=0
        for _,delta in sorted(timeline,key=lambda x:(x[0],x[1])):
            current+=delta;peak=max(peak,current)
            if current<0 or current>2: raise ValueError('fixture service slot accounting differs')
        if current: raise ValueError('fixture service slots retained')
        if len({r['pid'] for r in run['ready']}) != len(run['counts']): raise ValueError('frontend processes not independent')
        if run['http_errors'] or any(code!=0 for code in run['exitcodes'].values()):
            raise ValueError('HTTP/frontend exit failed')
        if run['arm']!='native_shared':
            if not run['bridge_closed'] or any(run['final_core']['usage'].values()) or run['final_core']['jobs']:
                raise ValueError('Core resources retained')
            owners={e['owner_thread'] for e in run['bridge_events']}
            if len(owners)!=1: raise ValueError('more than one Core owner')
            opened=[e for e in run['bridge_events'] if e['event']=='query_open']
            if (len({e['query_id'] for e in opened})!=len(run['counts'])
                    or len({e['job_id'] for e in opened})!=len(run['counts'])
                    or len({e['session_id'] for e in opened})!=len(run['counts'])):
                raise ValueError('missing independent query Job')
            for event in run['core_events']:
                if 'usage' not in event:
                    continue  # The shared factory observer also reports HTTP transport events.
                if (event['usage']['active_requests']>run['active_requests']
                        or event['usage']['active_work']>run['active_requests']
                        or event['usage']['held_tasks']>64):
                    raise ValueError('Core resource peak exceeds declared capacity')
        finished={q:r for q,r in run['frontends'].items() if r['event']=='finished'}
        for query,result in finished.items():
            stats=result['lm_stats']
            if (stats['cache_hits'] or stats['operator_cache_hits']
                    or stats['physical_usage']['total_tokens']!=run['counts'][query]*8):
                raise ValueError('LM cache/usage differs from necessary HTTP work')
        overlap=(len(finished)==2 and finished['long']['began']<finished['short']['began']<finished['long']['ended'])
        summaries.append(dict(run_id=run['run_id'],arm=run['arm'],count=len(identities),service_peak=peak,
            overlap=overlap,client_jct_s={q:r['jct_s'] for q,r in finished.items()},
            group_jct_s=max(r['ended'] for r in finished.values())-run['release_epoch'],
            cpu_s={q:r['cpu_s'] for q,r in finished.items()},rss_peak_bytes=run['rss_peak_bytes'],cancel_at_s=run['cancel_at_s']))
        summaries[-1]['parent_cpu_s']=run.get('parent_cpu_s')
    return summaries


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--repeats',type=int,choices=(1,2,3),default=3)
    parser.add_argument('--max-fixture-posts',type=int,default=4240)
    parser.add_argument('--window-seconds',type=int,default=1800)
    parser.add_argument('--remaining-checks',action='store_true',
        help='only the three unfinished no-contention and two cancellation checks; no calibration/performance rerun')
    parser.add_argument('--previous-fixture-posts',type=int,default=0)
    args=parser.parse_args()
    if args.output.exists(): parser.error('output already exists; preserve previous evidence')
    if not 1<=args.max_fixture_posts<=4240 or not 30<=args.window_seconds<=1800:
        parser.error('fixture budgets must stay within 4240 POST / 1800 seconds')
    if not 0<=args.previous_fixture_posts<=4240 or args.previous_fixture_posts+args.max_fixture_posts>4240:
        parser.error('previous and new fixture budgets must together stay within 4240 POST')
    if args.remaining_checks and (args.max_fixture_posts>360 or args.window_seconds>600
                                 or args.previous_fixture_posts<1604):
        parser.error('remaining checks require <=360 new POST / <=600 seconds and the previous >=1604 POST charge')
    report=dict(schema=1,scope='actual LOTUS processes, localhost fixture; model/GPU/SQL zero',
        source_commit=os.environ.get('SEMLOOM_SOURCE_COMMIT'),runs=[],status='running')
    code=Path(__file__).resolve().parents[2]
    report['source_sha256']={rel:hashlib.sha256((code/rel).read_bytes()).hexdigest() for rel in
        ('src/experiments/query_sharing_lotus.py','src/execution_provider/adapters/native_query_bridge.py',
         'src/semantic_methods/lotus/batch.py','src/scheduling/submission_control/query_sharing.py',
         'src/scheduling/core/session.py','src/scheduling/core/session_jobs.py')}
    report['cpu_affinity']=sorted(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else None
    began=time.monotonic()
    window_end=began+args.window_seconds-20
    report['budget']=dict(max_fixture_posts=args.max_fixture_posts,window_seconds=args.window_seconds,
        previous_fixture_posts=args.previous_fixture_posts,cumulative_max_fixture_posts=4240)
    report['protocol']='remaining lifecycle checks' if args.remaining_checks else 'paired fixture with calibration'
    try:
        with FixtureService(max_posts=args.max_fixture_posts) as service:
            if args.remaining_checks:
                # A fixed K is a compatibility configuration, not a newly
                # calibrated performance point for the merged source.
                for arm in ARMS[1:]:
                    report['runs'].append(run_pair(service,arm,long_rows=64,delay_s=3,active=4,
                        run_id='no-contention-'+arm,window_end=window_end))
                for arm in ('strong_static','candidate'):
                    report['runs'].append(run_pair(service,arm,long_rows=64,active=4,cancel_at_s=.6,
                        run_id='cancel-'+arm,window_end=window_end))
            else:
                # Long-only fixture calibration, no evaluation outcomes select K.
                for active in (2,4):
                    for repeat in range(2):
                        report['runs'].append(run_pair(service,'strong_static',long_rows=64,short_rows=0,
                            active=active,run_id=f'calibration-k{active}-r{repeat}',window_end=window_end))
                calibration={active:median(r['frontends']['long']['jct_s'] for r in report['runs']
                             if r['active_requests']==active) for active in (2,4)}
                selected=min(calibration,key=lambda active:(calibration[active],active))
                report['static_calibration']=dict(median_long_jct_s=calibration,selected_active_requests=selected,
                    rule='lowest median of the independent long64 two-repeat fixture calibration; tie uses smaller K')
                for repeat in range(args.repeats):
                    for arm in (ARMS if repeat%2==0 else tuple(reversed(ARMS))):
                        if time.monotonic()>window_end: raise TimeoutError('aggregate fixture window exhausted')
                        report['runs'].append(run_pair(service,arm,active=selected,
                            run_id=f'paired-r{repeat}-{arm}',window_end=window_end))
                for arm in ARMS:
                    report['runs'].append(run_pair(service,arm,long_rows=64,delay_s=3,active=selected,
                        run_id='no-contention-'+arm,window_end=window_end))
                for arm in ('strong_static','candidate'):
                    report['runs'].append(run_pair(service,arm,active=selected,cancel_at_s=.6,
                        run_id='cancel-'+arm,window_end=window_end))
        report['summary']=audit(report['runs']);report['status']='passed'
    except PairFailure as error:
        report['runs'].append(error.evidence)
        report['error_type']=error.evidence.get('error_type') or type(error.__cause__ or error).__name__
        report['status']='failed'
    except BaseException as error:
        report['error_type']=type(error).__name__;report['status']='failed'
    report['elapsed_s']=time.monotonic()-began
    report['budget']['fixture_posts']=sum(len(r.get('calls',())) for r in report['runs'])
    report['budget']['cumulative_charged_posts']=args.previous_fixture_posts+report['budget']['fixture_posts']
    with args.output.open('xb') as raw, gzip.GzipFile(fileobj=raw,mode='wb',mtime=0) as stream:
        stream.write(json.dumps(report,ensure_ascii=False,separators=(',',':')).encode())
    print(json.dumps(dict(status=report['status'],runs=len(report['runs']),
                         fixture_posts=sum(len(r.get('calls',())) for r in report['runs']),elapsed_s=report['elapsed_s'])))
    return 0 if report['status']=='passed' else 1


if __name__=='__main__':
    raise SystemExit(main())
