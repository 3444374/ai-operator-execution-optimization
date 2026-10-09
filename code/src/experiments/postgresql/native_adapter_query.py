"""Finite external-input Map method comparisons with distinct executor owners."""
import argparse
import base64
from collections import Counter
from contextlib import contextmanager, ExitStack
from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
import threading
import time
import uuid

from src.baselines.common.private_artifacts import (
    content_digest, new_private_directory, open_private_text, write_private_json,
)
from src.baselines.common.redact import redact_json_values
from src.baselines.text.frameworks.prepared_map import (
    NativeGraphOptions, open_daft_calls, open_ray_calls,
)
from src.execution_provider.adapters.completion_response import decode_backend_completion
from src.execution_provider.adapters.full_response import decode_full_response
from src.execution_provider.adapters.model_config import MAX_MODEL_RESPONSE_BYTES, load_fixed_model_config
from src.execution_provider.adapters.native_tasks import (
    NativeTaskSession, build_native_execution, prepare_native_task,
)
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, _clock_domain
from src.execution_provider.semantic_map import SemanticMapPlan, MapCompletionStatus, completion_status
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.buffered_events import BufferedEvents
from src.experiments.process_sampling import ProcessSampler
from src.experiments.shared_request_budget import CellBudgetLedger
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from src.scheduling.core.session_contract import State, Usage
from .cell_evidence import CellErrors
from .map_direct import request_body
from .native_adapter_http import PreparedSessionFactory, bind_native_http_events
from .native_adapter_metrics import summarize_calls, executor_phase_observations
from .query_config import QueryConfig
from .query_execution import native_ray_runtime
from .ready_query_recording import record_prepared_execution, proxy_http_peak
from .semantic_system_query import record_native_response


ARMS = ('fixed-map-native-daft', 'fixed-map-native-ray', 'fixed-map-semloom',
        'fixed-map-semloom-local-diagnostic')
_PROCESS_CLOCK = 'process-clock-'+uuid.uuid4().hex


def call_clock_domain():
    return _clock_domain() or _PROCESS_CLOCK


def load_bounded_rows(load_source,*,max_rows=4096,max_source_bytes=64*1024*1024):
    if type(max_rows) is not int or not 1 <= max_rows <= 4096:
        raise ValueError('external source row allowance must be bounded')
    if type(max_source_bytes) is not int or not 1 <= max_source_bytes <= 64*1024*1024:
        raise ValueError('external source byte allowance must be bounded')
    values,seen=[],set()
    size=0
    for row in load_source():
        if len(values)==max_rows:
            raise ValueError('external producer exceeded its declared row allowance')
        if (not isinstance(row,dict) or set(row)!={'row_id','text'}
                or not isinstance(row['row_id'],str) or not 1<=len(row['row_id'].encode())<=128
                or row['row_id'] in seen or '\x00' in row['row_id']
                or not isinstance(row['text'],str) or '\x00' in row['text']
                or len(row['text'].encode())>65536):
            raise ValueError('external source requires unique bounded row identities and raw text')
        size+=len(json.dumps(row,ensure_ascii=False).encode())
        if size>max_source_bytes:
            raise ValueError('external producer exceeded its declared byte allowance')
        seen.add(row['row_id']);values.append(row)
    if not values:
        raise ValueError('this comparison requires nonempty external input')
    return values


class ProxyAccountedAttempts:
    """Worker observation tags; the shared proxy owns the durable POST budget."""
    def reserve(self, digest):
        return uuid.uuid4().hex


def prepare_fixed_calls(values, plan, record, *, clock=time.monotonic_ns, clock_domain=None):
    calls = []
    seen = set()
    for position, row in enumerate(values):
        row_id, text = row['row_id'], row['text']
        if (not isinstance(row_id,str) or not 1 <= len(row_id.encode()) <= 128
                or row_id in seen or not isinstance(text,str)):
            raise ValueError('external input must have unique bounded row identities and text')
        seen.add(row_id)
        body = request_body(plan,text)
        call = dict(call_id='map:'+row_id, row_id=row_id, source_position=position,
                    stage_id='model', stage_ordinal=0, model_role='main',
                    request_values_sha256=content_digest(body),
                    payload=json.dumps(body,ensure_ascii=False,separators=(',',':')).encode())
        record(dict(event='task_ready',monotonic_ns=clock(),clock_domain=clock_domain,
            **{k:call[k] for k in ('call_id','row_id','stage_id','request_values_sha256')}))
        calls.append(call)
    return tuple(calls)


def parse_fixed_response(plan, response):
    if response.status_code != 200:
        raise RuntimeError('method received HTTP status '+str(response.status_code))
    completion = decode_backend_completion(response.body)
    if completion_status(plan,completion) != MapCompletionStatus.VALID:
        raise ValueError('complete response violates the selected Map method')
    return completion


@contextmanager
def open_semloom_calls(calls, execution, unit_id, record, *, cleanup_report):
    """Advance the existing flow; accepted-prefix ownership stays in Core."""
    flow = NativeTaskSession(execution,unit_id,'prepared-map')
    finished = False
    pending_index = 0
    sealed = False

    def rows():
        nonlocal pending_index,sealed,finished
        while True:
            if pending_index < len(calls):
                selected = calls[pending_index:pending_index+flow.limits.offer_tasks]
                tasks = tuple(prepare_native_task(c['payload'],pending_index+i,
                    row_sequence=c['source_position'],call_id=c['call_id'],stage_id=c['stage_id'])
                    for i,c in enumerate(selected))
                offered = flow.offer(tasks)
                if offered.status not in ('ACCEPTED','BACKPRESSURE'):
                    raise RuntimeError('the prepared task batch was rejected: '+offered.status)
                pending_index += offered.accepted_prefix_count
            if pending_index == len(calls) and not sealed:
                flow.end_input()
                sealed = True
            progress = flow.advance(flow.limits.offer_tasks)
            for delivery in progress.deliveries:
                call = calls[delivery.key.sequence]
                try:
                    if delivery.status != 'completed':
                        raise RuntimeError(progress.error or 'prepared model transport did not complete')
                    if (delivery.info.call_id,delivery.info.row_sequence,delivery.info.stage_id) != (
                            call['call_id'],call['source_position'],call['stage_id']):
                        raise ValueError('SemLoom delivery changed method call association')
                    yield call['call_id'],call['row_id'],delivery.result
                finally:
                    flow.release((delivery.lease_id,))
                    record(dict(event='result_release',call_id=call['call_id'],row_id=call['row_id'],
                        stage_id=call['stage_id'],monotonic_ns=time.monotonic_ns(),clock_domain=call_clock_domain()))
            if progress.state in (State.FAILED,State.CANCELLED):
                raise RuntimeError('the SemLoom prepared-task flow ended '+progress.state.value)
            if progress.state == State.FINISHED:
                finished = True
                break
            flow.wait(progress)

    stream = rows()
    error = None
    try:
        yield stream
    except BaseException as failure:
        error = failure
        raise
    finally:
        cleanup = CellErrors()
        cleanup.attempt('consumer_close',stream.close)
        if not finished:
            cleanup.attempt('query_cancel',flow.request_cancel)
        report = cleanup.attempt('flow_close',lambda:flow.close(clean=finished))
        until = time.monotonic() + execution.drain_timeout_s
        while execution.engine.capacity.usage() != Usage() and time.monotonic() < until:
            progress = execution.engine.advance()
            execution.engine.wake.wait(progress.generation,min(.01,max(0,until-time.monotonic())))
        cleanup_report.update(consumer=asdict(report) if report is not None else None,
                              final_core_usage=asdict(execution.engine.capacity.usage()))
        if execution.engine.capacity.usage() != Usage():
            cleanup.record('core_drain',RuntimeError('Core retains unconfirmed model responsibilities'))
        cleanup_report['errors'] = cleanup.details
        if cleanup.first is not None:
            if error is not None:
                error.add_note('Prepared flow cleanup also failed: '+type(cleanup.first).__name__)
            else:
                cleanup.raise_if_failed()


def _runtime(stack, arm, options, physical, ray_temp_root, ray_address):
    if arm == 'fixed-map-native-daft':
        from src.baselines.text.frameworks.daft_pg_http import prepare_runtime
        prepare_runtime(options.num_threads)
        return None,dict(owner='Daft Native',version='0.7.21',source='external finite prepared calls')
    if arm == 'fixed-map-semloom-local-diagnostic':
        return None,dict(owner='SemLoom local diagnostic',source='external finite prepared calls')
    os.environ['RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY']=str(options.ray_async_batches_per_actor)
    import ray
    config = QueryConfig(unit_id='prepared-runtime',arm='ray-data',task='map',table='unused',
        concurrency=options.concurrency,ray_num_cpus=options.num_threads,ray_actors=options.ray_actors,
        ray_async_batches_per_actor=options.ray_async_batches_per_actor,ray_address=ray_address)
    runtime = stack.enter_context(native_ray_runtime(ray,config,ray_temp_root))
    if arm == 'fixed-map-semloom':
        physical = replace(physical,address=ray.get_runtime_context().gcs_address,response_mode='full')
    return physical,dict(runtime,owner=('SemLoom Daft/Ray' if arm == 'fixed-map-semloom' else 'Ray Data'),
                         version=ray.__version__,source='external finite prepared calls')


def run_prepared_map_query(arm, *, load_source, plan, model, ledger, unit_id, root,
                           options=NativeGraphOptions(), physical=None,
                           ray_temp_root=None, ray_address=None, query_timeout_s=120,
                           max_rows=4096, max_source_bytes=64*1024*1024,
                           reference_outputs=None, allowed_outputs=None,
                           preparation_started_ns=None):
    """Keep method preparation timed, but outside every native execution graph."""
    if arm not in ARMS or (arm == 'fixed-map-semloom' and physical is None):
        raise ValueError('the selected Map arm requires its declared executor')
    if arm != 'fixed-map-semloom' and physical is not None:
        raise ValueError('Ray Map physical options belong to SemLoom only')
    if physical is not None and (physical.window_bytes < MAX_FRAME_BYTES+24
            or physical.payload_backend != 'daft'
            or max(physical.workers,physical.batch_rows) > options.concurrency):
        raise ValueError('SemLoom main requires declared Daft batches fitting a complete legal task')
    if type(max_rows) is not int or not 1 <= max_rows <= 4096:
        raise ValueError('external source row allowance must be bounded')
    if plan.model_id != model.model_id:
        raise ValueError('method and service model identities differ')
    invoked = time.monotonic_ns()
    started = invoked if preparation_started_ns is None else preparation_started_ns
    if type(started) is not int or not 0 < started <= invoked:
        raise ValueError('application preparation time must precede executor entry')
    root = Path(root)
    new_private_directory(root)
    errors = CellErrors()
    summary = dict(schema='semloom.native_adapter_query.v1',status='failed',arm=arm,unit_id=unit_id,
        source_identity='external finite raw-input producer; no SQL executor replacement',
        method='fixed SemanticMapPlan single-model complete Map',
        native_baseline_modified=False,performance_qualified=False,options=asdict(options),
        query_preparation_started_ns=started,request_budget_owner='common proxy before upstream POST',
        query_entry='external-input executor API', model_role='main', stages={})
    calls = ()
    responses, seen = {}, set()
    predictions = {}
    count = 0
    shared = None
    reserved = False
    stop = threading.Event()
    expected = Counter()
    actual = Counter()
    count_lock = threading.Lock()
    core_cleanup = {}
    execution = None

    def before(route, body):
        nonlocal count
        with count_lock:
            if stop.is_set():
                raise RuntimeError('this query stopped further model forwarding')
            digest = content_digest(json.loads(body))
            if actual[digest] >= expected[digest]:
                stop.set()
                raise ValueError('model POST is outside the method-selected task set')
            attempt = shared.reserve(hashlib.sha256(body).hexdigest())
            count += 1
            actual[digest] += 1
        return attempt

    def after(route, body, response, status):
        try:
            record_native_response(protocols,body,response,status,model.model_id)
        except BaseException as failure:
            stop.set()
            errors.record('http_failure',failure)
            raise

    try:
        source_started = time.monotonic_ns()
        values=load_bounded_rows(load_source,max_rows=max_rows,max_source_bytes=max_source_bytes)
        summary['stages']['source_seconds'] = (time.monotonic_ns()-source_started)/1e9
        if reference_outputs is not None and (not isinstance(reference_outputs,dict)
                or set(reference_outputs)!={row['row_id'] for row in values}
                or any(not isinstance(v,str) for v in reference_outputs.values())):
            raise ValueError('evaluation references must match all external row occurrences')
        ledger.reserve_unit(unit_id,len(values))
        reserved = True
        shared = ledger.claim_shared_unit(unit_id)
        event_root=root/'worker-events';event_root.mkdir()
        with ExitStack() as stack:
            writer=stack.enter_context(BufferedEvents(root/'method-events.jsonl'))
            raw=stack.enter_context(open_private_text(root/'complete-responses.jsonl'))
            protocols=stack.enter_context(open_private_text(root/'protocols.jsonl'))
            physical,runtime=_runtime(stack,arm,options,physical,ray_temp_root,ray_address)
            summary['runtime']=runtime
            summary['physical']=asdict(physical) if physical is not None else None
            summary['method_plan']=asdict(plan)
            summary['model_id']=model.model_id
            gateway=stack.enter_context(ObservationGateway(
                routes=(GatewayRoute(unit_id,'model',model.endpoint_url),),
                trace_path=root/'http-trace.jsonl',request_timeout_s=min(query_timeout_s,model.timeout_ms/1000),
                before_forward=before,after_forward=after))
            routed=replace(model,endpoint_url=gateway.endpoint_url(unit_id,'model'))
            domain=call_clock_domain()

            def record(event):
                value=dict(event)
                value.setdefault('monotonic_ns',time.monotonic_ns())
                writer.record(value)

            def core_record(event):
                record(dict(event, event='backend_'+event['event']) if event.get('event') in (
                    'http_started','http_finished') else event)
                key=event.get('key')
                if event.get('event')=='core_submit' and key is not None and key['sequence'] < len(calls):
                    call=calls[key['sequence']]
                    record(dict(event='executor_submit',call_id=call['call_id'],row_id=call['row_id'],
                                stage_id=call['stage_id'],clock_domain=domain))
                if event.get('event')=='ray_http_completed' and event.get('shared_clock'):
                    call=calls[key['sequence']]
                    fields=dict(call_id=call['call_id'],row_id=call['row_id'],stage_id=call['stage_id'],clock_domain=domain)
                    record(dict(event='worker_enter',monotonic_ns=event['worker_started_ns'],**fields))
                    record(dict(event='worker_response',monotonic_ns=event['worker_ended_ns'],**fields))

            if arm.startswith('fixed-map-semloom'):
                execution=build_native_execution(routed,physical=physical,observer=core_record,
                    max_tasks=options.concurrency,max_active_requests=options.concurrency,
                    input_bytes=options.concurrency*1048576,
                    result_bytes=options.concurrency*MAX_MODEL_RESPONSE_BYTES)
                def close_execution():
                    nonlocal execution
                    closed=errors.attempt('backend_close',lambda:execution.close(execution.drain_timeout_s))
                    if closed is not True:
                        errors.record('backend_close_unconfirmed',RuntimeError('SemLoom backend close remains unconfirmed'))
                    execution=None
                stack.callback(close_execution)
            ready=time.monotonic_ns()
            summary['preparation_model_posts']=count
            if count:
                raise ValueError('executor preparation unexpectedly called the model')
            headers={'Authorization':'Bearer '+model.bearer_token} if model.bearer_token else None

            @contextmanager
            def open_rows():
                def rows():
                    nonlocal calls,expected
                    method_started=time.monotonic_ns()
                    calls=prepare_fixed_calls(values,plan,record,clock_domain=domain)
                    summary['stages']['method_preparation_seconds']=(time.monotonic_ns()-method_started)/1e9
                    expected=Counter(c['request_values_sha256'] for c in calls)
                    lookup={c['call_id']:c for c in calls}
                    metadata=tuple({k:c[k] for k in ('row_id','source_position','call_id','stage_id',
                                                    'request_values_sha256')} for c in calls)
                    factory=PreparedSessionFactory(ProxyAccountedAttempts(),str(event_root),
                        routed.endpoint_url,model.timeout_ms/1000,metadata)
                    if arm=='fixed-map-native-daft':
                        context=open_daft_calls(calls,options,factory,headers=headers)
                    elif arm=='fixed-map-native-ray':
                        context=open_ray_calls(calls,options,factory,headers=headers)
                    else:
                        context=open_semloom_calls(calls,execution,unit_id,record,cleanup_report=core_cleanup)
                    with context as results:
                        for call_id,row_id,wire in results:
                            call=lookup.get(call_id)
                            if call is None or row_id!=call['row_id'] or call_id in seen:
                                raise ValueError('executor returned a missing, duplicate or unrelated call')
                            response=decode_full_response(wire)
                            record(dict(event='caller_response',call_id=call_id,row_id=row_id,
                                stage_id=call['stage_id'],clock_domain=domain,
                                response_bytes_sha256=hashlib.sha256(response.body).hexdigest(),
                                status=response.status_code))
                            raw.write(json.dumps(dict(call_id=call_id,response_wire=base64.b64encode(wire).decode()))+'\n')
                            completion=parse_fixed_response(plan,response)
                            if allowed_outputs is not None and completion.raw_output not in allowed_outputs:
                                stop.set()
                                raise ValueError('the application returned an undeclared output value')
                            seen.add(call_id)
                            predictions[row_id]=completion.raw_output
                            responses[call_id]=dict(prompt_tokens=completion.prompt_tokens,
                                                    output_tokens=completion.output_tokens)
                            yield row_id,completion.raw_output
                stream=rows()
                try:
                    yield stream
                finally:
                    stream.close()

            def cancel_query():
                stop.set()

            with ProcessSampler(root/'query-rss.jsonl',{'adapter_driver':os.getpid()},include_children=True) as sampler:
                summary['execution']=record_prepared_execution(root/'q0',open_rows,
                    backend_ready_ns=ready,preparation_started_ns=started,max_rows=len(values),
                    max_result_bytes=len(values)*70000,flush_rows=64,
                    query_timeout_s=query_timeout_s,cancel_query=cancel_query)
            summary['resources']=sampler.summary()
        events=[json.loads(line) for line in (root/'method-events.jsonl').read_text().splitlines()]
        for path in sorted(event_root.glob('*.jsonl')):
            events.extend(json.loads(line) for line in path.read_text().splitlines())
        events=bind_native_http_events(events)
        traces=[json.loads(line) for line in (root/'http-trace.jsonl').read_text().splitlines()]
        if (actual!=expected or count!=len(calls) or len(seen)!=len(calls) or len(traces)!=count
                or any(t['status']!='completed' or t['request_body_sha256']!=t['forwarded_body_sha256'] for t in traces)):
            raise ValueError('task, POST, complete response or result counts differ')
        call_metadata=[{k:v for k,v in c.items() if k!='payload'} for c in calls]
        write_private_json(root/'calls.json',call_metadata)
        summary.update(status='passed',rows=len(values),actual_posts=count,call_timing=summarize_calls(events,call_metadata),
            observed_peak_http=proxy_http_peak(traces),core_cleanup=core_cleanup,
            full_query_seconds=(summary['execution']['t_query_terminal_ns']-started)/1e9,
            method_calls=call_metadata,model_usage=list(responses.values()),
            quality=dict(status='unavailable',reason='no reference labels supplied; outputs and request semantics retained'))
        summary['stages']['model_inference_seconds']=dict(status='unavailable',reason='no server-side per-call inference clock')
        summary['stages']['raw_executor_events']='method-events.jsonl and worker-events/'
        summary['executor_phases']=executor_phase_observations(events)
        if reference_outputs is not None:
            correct=sum(predictions[row_id]==answer for row_id,answer in reference_outputs.items())
            summary['quality']=dict(status='observed',rows=len(predictions),correct=correct,
                                    accuracy=correct/len(predictions),matching='exact method-parsed output values')
    except BaseException as failure:
        stop.set()
        errors.record('query',failure)
    finally:
        if execution is not None:
            closed=errors.attempt('backend_close',lambda:execution.close(execution.drain_timeout_s))
            if closed is False:
                errors.record('backend_close_unconfirmed',RuntimeError('SemLoom backend close remains unconfirmed'))
        if reserved:
            errors.attempt('budget_close',lambda:ledger.close_shared_unit(unit_id))
        if errors.first is not None:
            summary['status']='failed'
        summary.update(errors=errors.details,ended_ns=time.monotonic_ns(),attempted_posts=count)
        errors.attempt('summary_write',lambda:write_private_json(root/'summary.json',redact_json_values(summary)))
    errors.raise_if_failed()
    return summary


def main(argv=None):
    started=time.monotonic_ns()
    from .supplier_adapter_query import SUPPLIER_ARMS,run_supplier_query
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--arm',choices=ARMS+SUPPLIER_ARMS,required=True)
    for name in ('input','plan','model','budget','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--unit-id',required=True)
    parser.add_argument('--budget-id',required=True)
    parser.add_argument('--max-attempts',type=int,required=True)
    parser.add_argument('--options',type=Path)
    parser.add_argument('--references',type=Path)
    parser.add_argument('--allowed-output',action='append')
    parser.add_argument('--stages',type=Path)
    parser.add_argument('--sema-binary',type=Path)
    parser.add_argument('--duckdb-library',type=Path)
    parser.add_argument('--tokenizer',type=Path)
    parser.add_argument('--ray-physical',type=Path)
    parser.add_argument('--ray-temp-root',type=Path)
    parser.add_argument('--ray-address')
    parser.add_argument('--query-timeout-s',type=float,default=120)
    args=parser.parse_args(argv)
    if args.input.stat().st_size>64*1024*1024:
        parser.error('raw input file exceeds its byte allowance')
    def load_source():
        with args.input.open() as stream:
            for line in stream:
                yield json.loads(line)
    options=NativeGraphOptions(**json.loads(args.options.read_text())) if args.options else NativeGraphOptions()
    physical=RayMapConfig.load(args.ray_physical) if args.ray_physical else None
    common=dict(load_source=load_source,
        plan=SemanticMapPlan(**json.loads(args.plan.read_text())),model=load_fixed_model_config(args.model),
        ledger=CellBudgetLedger(args.budget,AttemptBudget(args.budget_id,args.max_attempts)),
        unit_id=args.unit_id,root=args.output,options=options,physical=physical,
        ray_temp_root=args.ray_temp_root,ray_address=args.ray_address,query_timeout_s=args.query_timeout_s,
        reference_outputs=json.loads(args.references.read_text()) if args.references else None,
        allowed_outputs=tuple(args.allowed_output) if args.allowed_output else None)
    if args.arm in SUPPLIER_ARMS:
        run_supplier_query(args.arm,**common,stages=json.loads(args.stages.read_text()) if args.stages else None,
                           sema_binary=args.sema_binary,tokenizer_path=args.tokenizer,duckdb_library=args.duckdb_library,
                           preparation_started_ns=started)
    else:
        run_prepared_map_query(args.arm,**common,preparation_started_ns=started)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
