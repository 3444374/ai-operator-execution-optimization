"""Opt-in private record observer. No payload logging or execution policy."""
import atexit
from collections import defaultdict,deque
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict
import functools
import hashlib
import json
import math
import os
from pathlib import Path
import threading
import time
import uuid

ENABLED = os.environ.get('SEM_RECORD_OBSERVATION') == 'on'
ROOT = Path(os.environ['SEM_RECORD_OBSERVER_ROOT']) if ENABLED else None
FIELDS = ('query_id', 'model_role', 'algorithm_stage', 'row_id', 'source_position', 'call_index', 'call_id')
_env_names = ('SEM_RECORD_OBSERVATION', 'SEM_RECORD_OBSERVER_ROOT', 'SEM_RECORD_OBSERVER_CODE',
              'SEM_RECORD_RUN_ID', 'SEM_RECORD_SESSION_ID')
# A deliberately narrow metadata vocabulary; raw messages, text, options,
# exceptions, environments and readiness dictionaries are never persisted.
_ALLOWED = set(FIELDS) | set('span_id parent_span_id operation members members_complete membership_reason stage span_kind boundary phase status reason error_type marker iteration observer_execute_id observer_sample_id state_identity handoff_step step_identity_source scheduled_tokens actual_runner_module native_instance_id engine_uid session_id owner_uid producer_owner_uid request_id collector_request_id native_identity key usage retained_refs logical_input_bytes logical_messages_bytes source_values_utf8_bytes rows outputs call_ns lower_ns upper_ns start_ns end_ns thread_cpu_start_ns thread_cpu_end_ns channel direction socket_id frame_id encoded_frame_id actual_socket_bytes stream_start stream_end encoded_frame_bytes payload_bytes op frame_start frame_end collection_wall_ns collection_thread_CPU_ns processes unknown flush_confirmed events_written events_attempted dropped errors emit_wall_ns emit_thread_cpu_ns closed last_sequence post_return observed scope'.split())
_ALLOWED.update(('identities','outstanding','metrics','prompt_tokens','output_tokens'))
_ALLOWED.update(('rss','uss','pss','pid','process_birth'))
_ALLOWED.add('range_handle')
_ALLOWED.add('batch_key')
_ALLOWED.update('observation_ns accepted source_sequence source_observed_at_s received_at_s decision_at_s expected_engine_uid'.split())
_ALLOWED.update('control_request_index source_clock_domain received_clock_domain decision_clock_domain'.split())
_ALLOWED.update('running waiting kv_usage preemptions_total finished_total generation_tokens_total'.split())
_ALLOWED.update('native_capacity max_num_seqs max_num_batched_tokens max_model_len kv_cache_blocks kv_cache_block_tokens kv_free_bytes physical_free_bytes'.split())
_installed = set()
_parent = ContextVar('record_observer_parent', default=None)
_lock = threading.Lock()
_close_lock = threading.Lock()
_fd = None
_closed = False
_phase = 'online'
_n = _attempts = _errors = _dropped = _wall = _cpu = 0
_settings_cache = None
_buffer = deque()
_buffer_lock = threading.Lock()
_buffer_closed = False
_buffer_dropped = _buffer_errors = _buffer_drained = _buffer_peak = 0
_buffer_attempts = _buffer_enqueued = 0


def emit_feedback(event, **values):
    """Keep only bounded native identities, clocks, counters and reason codes."""
    uid_fields={'engine_uid','expected_engine_uid','owner_uid'}
    count_fields={'session_id','source_sequence','control_request_index','running','waiting','preemptions_total','finished_total','generation_tokens_total'}
    clock_fields={'source_observed_at_s','received_at_s','decision_at_s','kv_usage'}
    safe={}
    for key,value in values.items():
        if key in uid_fields:
            safe[key]=value if type(value)is str and len(value)==32 and all(c in '0123456789abcdef'for c in value)else None
        elif key in count_fields:
            safe[key]=value if type(value)is int and 0<=value<1<<64 else None
        elif key in clock_fields:
            safe[key]=value if type(value)in(int,float)and math.isfinite(value)and value>=0 else None
        elif key=='accepted':safe[key]=value if type(value)is bool else None
        elif key in ('stage','reason','scope','source_clock_domain','received_clock_domain','decision_clock_domain')and type(value)is str and len(value)<=128:safe[key]=value
    emit_buffered(event,**safe)


def emit_buffered(event, **values):
    """Metadata only; no file I/O or sink lock on a feedback/decision thread.

    Existing event/packet bounds also bound this buffer. close_events drains it
    through the original sink; observation_ns is the enqueue clock, while the
    ordinary event timestamp is the later sink-write clock.
    """
    global _buffer_dropped, _buffer_errors, _buffer_peak, _buffer_attempts, _buffer_enqueued
    if not ENABLED:return
    with _buffer_lock:_buffer_attempts+=1
    try:
        limits = _settings_cache
        if limits is None:raise RuntimeError('observer settings not initialized')
        raw = dict(event=event,observation_ns=time.monotonic_ns())
        raw.update({k:_safe(v) for k,v in values.items() if k in _ALLOWED})
        packet = json.dumps(raw,allow_nan=False,separators=(',',':')).encode()
        with _buffer_lock:
            if (_buffer_closed or len(_buffer)>=limits['max_events_per_process']
                    or len(packet)>limits['max_event_bytes']):
                _buffer_dropped += 1;return
            _buffer.append(packet);_buffer_enqueued+=1;_buffer_peak=max(_buffer_peak,len(_buffer))
    except Exception:
        with _buffer_lock:_buffer_errors += 1


def _drain_buffer():
    global _buffer_closed, _buffer_drained, _buffer_errors
    with _buffer_lock:
        _buffer_closed=True
    while True:
        with _buffer_lock:
            if not _buffer:break
            packet=_buffer.popleft()
        try:
            value=json.loads(packet);event=value.pop('event')
            emit(event,**value)
        except Exception:
            with _buffer_lock:_buffer_errors+=1
        _buffer_drained += 1


def settings():
    global _settings_cache
    if not ENABLED:
        return {}
    if _settings_cache is None:
        _settings_cache=json.loads((ROOT/'config.json').read_text()).get('observation', {})
    return _settings_cache


def handoff_profile():
    return ENABLED and settings().get('profile')=='handoff'


def core_key(value):
    if (type(value)in(list,tuple)and len(value)==2
            and all(type(v)is int and 0<=v<1<<64 for v in value)):
        return dict(session_id=value[0],sequence=value[1])


def observe_ref_ready(ref, key, identities, *, batch_key=None, scope=None):
    if not handoff_profile():return
    def completed(unused):
        emit('driver_batch_future_done'if key is None else 'driver_future_done',
             key=key,identities=identities,batch_key=batch_key,
             scope=scope or 'Python Future completion notification; original await determines outcome')
    try:ref.future().add_done_callback(completed)
    except Exception as error:
        emit('handoff_probe_unknown',key=key,error_type=type(error).__name__)


def _safe(value):
    if isinstance(value, dict):
        return {str(k): _safe(v) for k, v in value.items() if str(k) in _ALLOWED
                or str(k) in {'mapped_internal_ids', 'external_id_by_internal', 'present_internal_ids'}
                or type(v) in (int, float, bool)}
    if isinstance(value, (list, tuple)):
        return [_safe(v) for v in value]
    if value is None or type(value) in (str, int, float, bool):
        return value
    return None


def emit(event, **values):
    global _fd, _n, _attempts, _errors, _dropped, _wall, _cpu
    if not ENABLED:
        return
    try:started,cpu=time.monotonic_ns(),time.thread_time_ns()
    except Exception:
        with _lock:_attempts+=1;_errors+=1
        return  # No invented clock or successful coverage when the clock fails.
    with _lock:
        _attempts += 1
        try:
            limits = settings()
            raw = dict(event=event, phase=_phase, pid=os.getpid(), parent_pid=os.getppid(),
                       thread=threading.get_ident(), monotonic_ns=started, epoch_ns=time.time_ns(),
                       thread_cpu_ns=cpu, sequence=_attempts,
                       run_id=os.environ.get('SEM_RECORD_RUN_ID'), observer_session_id=os.environ.get('SEM_RECORD_SESSION_ID'))
            raw.update({k: _safe(v) for k, v in values.items() if k in _ALLOWED})
            packet = (json.dumps(raw, allow_nan=False, separators=(',', ':'))+'\n').encode()
            if _closed or _n >= limits['max_events_per_process'] or len(packet) > limits['max_event_bytes']:
                _dropped += 1
                return
            if _fd is None:
                (ROOT/'events').mkdir(mode=0o700, exist_ok=True)
                _fd = os.open(ROOT/'events'/('events-'+str(os.getpid())+'.jsonl'),
                              os.O_CREAT|os.O_EXCL|os.O_WRONLY, 0o600)
            if os.write(_fd, packet) != len(packet):
                raise OSError('short observer write')
            _n += 1
        except Exception:
            _errors += 1
        finally:
            try:
                _wall += time.monotonic_ns()-started
                _cpu += time.thread_time_ns()-cpu
            except Exception:_errors+=1


def close_events():
    if not ENABLED:
        return
    # Draining calls emit(), which takes _lock. A separate lock keeps the whole
    # drain/fsync/footer transaction owned by one closer without blocking enqueue.
    with _close_lock:
        if not _closed:
            _close_events()


def _close_events():
    global _fd, _closed, _errors
    _drain_buffer()
    with _lock:
        flushed = False
        if _fd is not None:
            descriptor=_fd;_fd=None
            try:os.fsync(descriptor);flushed=True
            except OSError:_errors+=1
            try:os.close(descriptor)
            except OSError:_errors+=1;flushed=False
        _closed = True
        status = dict(events_written=_n, events_attempted=_attempts+_buffer_dropped+_buffer_errors,
                       dropped=_dropped+_buffer_dropped, errors=_errors+_buffer_errors,
                      emit_wall_ns=_wall, emit_thread_cpu_ns=_cpu, closed=True,
                      flush_confirmed=flushed, last_sequence=_attempts,
                      scope='this observer sink only; not proof of complete CUPTI/process-tree capture')
        status['feedback_buffer']=dict(attempted=_buffer_attempts,enqueued=_buffer_enqueued,
            drained=_buffer_drained,dropped=_buffer_dropped,errors=_buffer_errors,
            peak_events=_buffer_peak,remaining=len(_buffer),closed=_buffer_closed,
            event_limit=settings()['max_events_per_process'],packet_byte_limit=settings()['max_event_bytes'],
            drain_complete=not _buffer and not _buffer_dropped and not _buffer_errors)
        try:
            path=ROOT/'events'/('events-'+str(os.getpid())+'.status.json')
            with path.open('x') as stream:json.dump(status, stream)
            path.chmod(0o600)
        except OSError:
            _errors += 1


if ENABLED:
    atexit.register(close_events)


def set_phase(phase):
    global _phase
    _phase=phase


def ids(rows):
    if not isinstance(rows, (list, tuple)):
        return None  # Never consume or materialize an iterable to observe it.
    return [{k: row[k] for k in FIELDS if k in row} for row in rows if isinstance(row, dict)]


@contextmanager
def cpu_span(operation, members=None, *, kind='synchronous'):
    span=uuid.uuid4().hex
    parent=_parent.get()
    token=_parent.set(span)
    start,cpu=time.monotonic_ns(),time.thread_time_ns()
    complete=members is not None and all('call_id' in m and 'algorithm_stage' in m for m in members)
    common=dict(span_id=span,parent_span_id=parent,operation=operation,span_kind=kind,
                members=members,members_complete=complete)
    emit('cpu_span_start',**common,start_ns=start,thread_cpu_start_ns=cpu)
    try:
        yield span
    finally:
        end,finished=time.monotonic_ns(),time.thread_time_ns()
        emit('cpu_span_end',**common,end_ns=end,thread_cpu_end_ns=finished)
        _parent.reset(token)


def observe_method(cls, name, operation, members):
    original=getattr(cls,name)
    @functools.wraps(original)
    def wrapped(self, rows, *args, **kwargs):
        identities=members(self,rows,args,kwargs)
        legacy='prepare' if operation.endswith('prepare') else 'parser' if operation.endswith('parser') else 'method_complete'
        emit(legacy+'_start',identities=identities)
        try:
            with cpu_span(operation,identities) as span:
                value=original(self,rows,*args,**kwargs)
                if operation.endswith('prepare') and ids(value) is not None:
                    emit('cpu_span_members',span_id=span,members=ids(value),members_complete=True)
                return value
        finally:emit(legacy+'_end',identities=identities)
    setattr(cls,name,wrapped)


def install_cpu_hooks():
    if not ENABLED or handoff_profile() or 'cpu' in _installed:return
    _installed.add('cpu')
    from src.semantic_methods.lotus.record_chain import LotusRecordMethod,ChainRun,stage_identity
    from src.semantic_methods.lotus import engine
    def chain_members(self,records,args,kwargs):
        if not isinstance(records,(list,tuple)):return None
        return [stage_identity(kwargs['query_id'],r,self.stage.stage_id,model_role=self.stage.model_role) for r in records]
    observe_method(LotusRecordMethod,'prepare_rows','chain_prepare',chain_members)
    observe_method(LotusRecordMethod,'interpret_results','chain_parser',lambda s,r,a,k:ids(r))
    observe_method(ChainRun,'complete','chain_complete',lambda s,r,a,k:ids(r))
    observe_method(engine.LotusFilterMethod,'prepare_rows','ordinary_prepare',lambda s,r,a,k:ids(r))
    original=engine.interpret_results
    @functools.wraps(original)
    def interpret(results,*args,**kwargs):
        identities=ids(results);emit('parser_start',identities=identities)
        try:
            with cpu_span('ordinary_parser',identities):return original(results,*args,**kwargs)
        finally:emit('parser_end',identities=identities)
    engine.interpret_results=interpret


def memory_snapshot(boundary):
    if handoff_profile():return
    start,cpu=time.monotonic_ns(),time.thread_time_ns()
    rows=[]
    try:
        import psutil
        parent=psutil.Process()
        for process in [parent]+parent.children(recursive=True):
            try:
                birth=process.create_time();value=process.memory_full_info()
                if process.create_time()!=birth:raise RuntimeError('process identity changed')
                rows.append(dict(pid=process.pid,process_birth=birth,rss=value.rss,
                                 uss=getattr(value,'uss',None),pss=getattr(value,'pss',None)))
            except Exception as error:
                rows.append(dict(pid=process.pid,unknown=type(error).__name__))
    except Exception as error:
        rows.append(dict(unknown=type(error).__name__))
    emit('private_memory_snapshot',boundary=boundary,processes=rows,
         collection_wall_ns=time.monotonic_ns()-start,collection_thread_CPU_ns=time.thread_time_ns()-cpu,
         scope='sparse observed process snapshots; not phase peaks or additive shared RSS')


def install_driver():
    if not ENABLED or 'driver' in _installed:return
    _installed.add('driver');install_cpu_hooks()
    install_feedback_observer()
    from src.execution_provider.adapters import lotus_records_actor_owner as owner,lotus_records_async_actor as async_owner
    from src.execution_provider.adapters.lotus_chain_actor_backend import LotusChainActorBackend
    from src.execution_provider.adapters import lotus_records_core
    from record_ipc_observer import ObservedAsyncCoreModelActor,ObservedPrepareActor
    async_owner.AsyncRecordsActorOwner._model_actor_type=lambda self:ObservedAsyncCoreModelActor
    owner._PrepareActor=ObservedPrepareActor
    original_env=owner._runtime_env
    def runtime_env(python):
        settings=original_env(python);variables=dict(settings['env_vars'])
        variables.update({key:os.environ[key] for key in _env_names if key in os.environ})
        variables['PYTHONPATH']=variables.get('PYTHONPATH','')+os.pathsep+str(Path(__file__).parent)
        return dict(settings,env_vars=variables)
    owner._runtime_env=async_owner._runtime_env=runtime_env
    def attach_core(core):
        previous=core.sink
        transport=core.backend
        if not callable(getattr(transport,'retained_refs',None)):
            # The ordinary Core holds the existing transport's bound coroutine.
            transport=getattr(getattr(transport,'_execute',None),'__self__',None)
        def sink(kind,key):
            if previous is not None:previous(kind,key)
            try:
                reference_reader=getattr(transport,'retained_refs',None)
                references=reference_reader() if callable(reference_reader) else None
                emit('Core_'+kind,key=asdict(key),usage=asdict(core.capacity.usage()),retained_refs=references)
            except Exception as error:emit('core_observation_unavailable',error_type=type(error).__name__)
        core.sink=sink
    # Both public owners create their Core lazily. Observe the real construction
    # once, before open/offer, rather than dereferencing owner.core in __init__.
    core_type=lotus_records_core.SessionEngine
    constructor=core_type.__init__
    @functools.wraps(constructor)
    def construct(self,*args,**kwargs):
        constructor(self,*args,**kwargs)
        try:attach_core(self)
        except Exception as error:emit('core_observation_unavailable',error_type=type(error).__name__)
    core_type.__init__=construct
    original=LotusChainActorBackend._submit_task
    @functools.wraps(original)
    def submit(self,task,*args,**kwargs):
        members=batch=None
        try:
            members=ids([json.loads(task.task.payload)['row']])
            member=getattr(task,'member',None)
            batch=None if member is None else asdict(member.batch_key)
            emit('Ray_submit_start',identities=members,key=asdict(task.key),batch_key=batch,
                 logical_input_bytes=len(task.task.payload),
                 scope='member transport entry; bulk data RPC is owned by the existing batch driver')
        except Exception as error:emit('handoff_probe_unknown',error_type=type(error).__name__)
        result=original(self,task,*args,**kwargs)
        try:
            emit('Ray_submit_return',key=asdict(task.key),batch_key=batch,retained_refs=self.retained_refs())
            if handoff_profile():observe_ref_ready(result,asdict(task.key),members,batch_key=batch,
                scope=('local member Future ready after native batch-page decode; not Ray RPC completion'
                       if batch is not None else None))
        except Exception as error:emit('handoff_probe_unknown',error_type=type(error).__name__)
        return result
    LotusChainActorBackend._submit_task=submit
    poll=getattr(async_owner.AsyncRecordsActorOwner,'poll_prepared_batch',None)
    if callable(poll):
        @functools.wraps(poll)
        def batch_poll(self,batch_key):
            result=poll(self,batch_key)
            observe_ref_ready(result,None,None,batch_key=core_key(batch_key),
                scope='Ray poll Future notification only; no result read, member count or unique page pairing')
            return result
        async_owner.AsyncRecordsActorOwner.poll_prepared_batch=batch_poll
    original_audit=LotusChainActorBackend._audit_result
    @functools.wraps(original_audit)
    def audit(self,expected,value):
        if handoff_profile():
            emit('driver_result_audit_enter',identities=ids([value]),
                 scope='after decode, before native audit; recovery may also enter this hook')
        return original_audit(self,expected,value)
    LotusChainActorBackend._audit_result=audit
    transferred=LotusChainActorBackend._result_transferred
    @functools.wraps(transferred)
    def transfer(self,task,value):
        member=getattr(task,'member',None)
        emit('Ray_result_to_Core',identities=ids([value]),key=asdict(task.key),
             batch_key=None if member is None else asdict(member.batch_key),retained_refs=self.retained_refs())
        return transferred(self,task,value)
    LotusChainActorBackend._result_transferred=transfer
    emit('driver_observer_installed')


def install_feedback_observer():
    # The tool can inspect older runtimes, but must not claim their consumer
    # freshness decisions are observed when the optional callback is absent.
    if 'feedback' in _installed:return
    _installed.add('feedback')
    try:
        import inspect
        from src.scheduling.runtime.observations import NonBlockingMetricsObservationProvider as Provider
        original=Provider.__init__
        if 'observer' not in inspect.signature(original).parameters:
            emit('native_feedback_audit_unavailable',reason='runtime_has_no_consumer_observer');return
        @functools.wraps(original)
        def construct(self,*args,**kwargs):
            previous=kwargs.get('observer')
            def record(value):
                emit_feedback('native_feedback_consumption',**value)
                if previous is not None:previous(value)
            kwargs['observer']=record
            return original(self,*args,**kwargs)
        Provider.__init__=construct
    except ImportError:
        emit('native_feedback_audit_unavailable',reason='runtime_has_no_consumer_provider')


def install_actor():
    if not ENABLED or 'actor' in _installed:return
    _installed.add('actor')
    install_cpu_hooks()
    from record_ipc_observer import install_transport
    install_transport(child=False)
    emit('actor_observer_installed')


def request_range_start(nvtx,marker):
    if not callable(getattr(nvtx,'range_start',None)) or not callable(getattr(nvtx,'range_end',None)):
        emit('request_clock_unknown',marker=marker,unknown='asynchronous NVTX ranges unavailable')
        return None,{}
    try:
        lower=time.monotonic_ns();handle=nvtx.range_start(marker);upper=time.monotonic_ns()
        return handle,dict(lower_ns=lower,upper_ns=upper)
    except Exception as error:
        emit('request_clock_unknown',marker=marker,error_type=type(error).__name__)
        return None,{}


def request_range_end(nvtx,handle):
    if handle is None:return {}
    try:
        lower=time.monotonic_ns();nvtx.range_end(handle);upper=time.monotonic_ns()
        return dict(lower_ns=lower,upper_ns=upper)
    except Exception as error:
        emit('request_clock_unknown',range_handle=handle,error_type=type(error).__name__)
        return {}


def install_model():
    if not ENABLED or handoff_profile() or 'model' in _installed:return
    _installed.add('model')
    from record_ipc_observer import install_transport
    install_transport(child=True)
    from vllm.engine.arg_utils import AsyncEngineArgs
    from src.serving.backends.vllm_async_engine import VllmAsyncEngine
    from src.serving.backends import vllm_engine as common
    from src.observability.native_model_api_trace import install as install_native_api
    from vllm.v1.engine.async_llm import AsyncLLM
    import torch
    install_native_api(AsyncLLM,common,lambda event,**values:emit('native_api_'+event,**values))
    original=AsyncEngineArgs.__init__
    @functools.wraps(original)
    def args(self,*a,**kw):
        kw.update(disable_log_stats=False,worker_cls='record_gpu_observer.ObservedWorker')
        return original(self,*a,**kw)
    AsyncEngineArgs.__init__=args
    init=VllmAsyncEngine.__init__
    @functools.wraps(init)
    def observed_init(self,*a,**kw):
        emit('async_model_init_start')
        try:return init(self,*a,**kw)
        finally:emit('async_model_init_end',engine_uid=getattr(self,'engine_uid',None))
    VllmAsyncEngine.__init__=observed_init
    generate,close=VllmAsyncEngine.generate_prepared,VllmAsyncEngine.close
    state={'capture_attempted':False,'capturing':False,'outstanding':0}
    @functools.wraps(generate)
    async def observed_generate(self,row,*a,**kw):
        identity=common._prepared([row])[0][0]
        if not state['capture_attempted']:
            state['capture_attempted']=True
            begin=time.monotonic_ns();status=int(torch.cuda.cudart().cudaProfilerStart())
            emit('profiler_start_return',status=str(status),call_ns=time.monotonic_ns()-begin)
            state['capturing']=status==0
        state['outstanding']+=1
        marker='record-native:'+hashlib.sha256(identity['call_id'].encode()).hexdigest()[:16]
        handle,anchors=request_range_start(torch.cuda.nvtx,marker)
        emit('native_open',identities=ids([identity]),request_id=kw['request_id'],marker=marker,
             **anchors,range_handle=handle,outstanding=state['outstanding'],engine_uid=self.engine_uid)
        try:return await generate(self,row,*a,**kw)
        finally:
            anchors=request_range_end(torch.cuda.nvtx,handle)
            emit('native_close',identities=ids([identity]),request_id=kw['request_id'],marker=marker,
                 **anchors,range_handle=handle,engine_uid=self.engine_uid)
            state['outstanding']-=1
    VllmAsyncEngine.generate_prepared=observed_generate
    convert=common.result_from_output
    @functools.wraps(convert)
    def converted(identity,output,*a,**kw):
        result=convert(identity,output,*a,**kw)
        metrics=getattr(output,'metrics',None)
        measured={key:getattr(metrics,key,None) for key in ('arrival_time','queued_ts','scheduled_ts','first_token_ts','last_token_ts','num_generation_tokens')}
        emit('native_result',identities=ids([identity]),request_id=output.request_id,engine_uid=result['engine_uid'],
             metrics=measured,prompt_tokens=result.get('prompt_tokens'),output_tokens=result.get('completion_tokens'))
        return result
    common.result_from_output=converted
    @functools.wraps(close)
    async def observed_close(self,*a,**kw):
        emit('native_engine_close_start')
        try:return await close(self,*a,**kw)
        finally:
            emit('native_engine_close_end',scope='API close; physical GPU tail/exit independently checked')
            emit('trace_finish_deferred_to_process_exit',scope='normal process-tree exit; no cudaProfilerStop in API close')
    VllmAsyncEngine.close=observed_close
    emit('async_model_observer_installed')


def summarize_cpu_spans(paths):
    """One physical span once. Parent inclusive CPU is never added to children."""
    groups=defaultdict(lambda:defaultdict(dict))
    for path in paths:
        for line in Path(path).read_text().splitlines():
            event=json.loads(line)
            if event.get('phase')=='online' and event['event'].startswith('cpu_span_'):
                groups[event['span_id']][event['event']][json.dumps(event,sort_keys=True)]=event
    spans=[]
    for identity,events in groups.items():
        start=list(events['cpu_span_start'].values());end=list(events['cpu_span_end'].values())
        if len(start)!=1 or len(end)!=1:
            boundary=start[0] if len(start)==1 else end[0] if len(end)==1 else {}
            spans.append(dict(span_id=identity,parent_span_id=boundary.get('parent_span_id'),
                              status='unknown',reason='missing_or_conflicting_span_boundary'));continue
        a,b=start[0],end[0]
        valid=(a['pid'],a['thread'])==(b['pid'],b['thread']) and a['start_ns']<=b['end_ns'] and a['thread_cpu_start_ns']<=b['thread_cpu_end_ns']
        members=list(events['cpu_span_members'].values())
        membership=members[0].get('members') if len(members)==1 else a.get('members') if not members else None
        spans.append(dict(span_id=identity,parent_span_id=a.get('parent_span_id'),members=membership,
                          operation=a['operation'],pid=a['pid'],thread=a['thread'],
                          start_ns=a['start_ns'],end_ns=b['end_ns'],status='observed' if valid else 'unknown',
                          wall_ns=b['end_ns']-a['start_ns'] if valid else None,
                          inclusive_thread_CPU_ns=b['thread_cpu_end_ns']-a['thread_cpu_start_ns'] if valid else None))
    for span in spans:
        children=[s for s in spans if s.get('parent_span_id')==span['span_id']]
        valid=span['status']=='observed' and all(c['status']=='observed' and
            (c['pid'],c['thread'])==(span['pid'],span['thread']) and span['start_ns']<=c['start_ns']<=c['end_ns']<=span['end_ns'] for c in children)
        exclusive=None if not valid else span['inclusive_thread_CPU_ns']-sum(c['inclusive_thread_CPU_ns'] for c in children)
        span['exclusive_thread_CPU_ns']=exclusive if exclusive is not None and exclusive>=0 else None
    return dict(spans=spans,scope='unique physical synchronous spans; shared members are not per-row costs; sum exclusive CPU only with known nesting')


if __name__=='__main__':
    import sys
    if len(sys.argv)!=3 or sys.argv[1]!='--summarize-spans':raise SystemExit('use --summarize-spans COPIED_CAPTURE_ROOT')
    directory=Path(sys.argv[2])
    (directory/'CPU-SPANS-SUMMARY.json').write_text(json.dumps(summarize_cpu_spans((directory/'events').glob('*.jsonl')),indent=2)+'\n')
