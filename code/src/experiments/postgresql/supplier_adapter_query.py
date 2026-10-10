"""Query recording around delivered supplier methods and their real executors."""
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

from src.baselines.common.private_artifacts import content_digest, new_private_directory, open_private_text, write_private_json
from src.baselines.common.redact import redact_json_values,redact_text
from src.execution_provider.adapters.full_response import decode_full_response, encode_full_response
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.experiments.buffered_events import BufferedEvents
from src.experiments.process_sampling import ProcessSampler
from src.scheduling.core.session_contract import Usage, SessionTimeouts
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from .cell_evidence import CellErrors
from .native_adapter_metrics import summarize_calls, executor_phase_observations, sample_distribution
from .native_adapter_query import call_clock_domain, _runtime,load_bounded_rows,resolve_adapter_limits
from .ready_query_recording import record_prepared_execution, proxy_http_peak
from .semantic_system_query import record_native_response


SUPPLIER_ARMS = (
    'lotus-adapted-native', 'lotus-method-semloom', 'lotus-method-semloom-local-diagnostic',
    'lotus-two-map-native-staged', 'lotus-two-map-semloom-staged',
    'lotus-two-map-semloom-incremental',
    'sema-native-direct', 'sema-native-transparent', 'sema-method-semloom-request-service',
    'duckdb-adapted-native', 'duckdb-method-semloom',
)


class MethodObservations:
    def __init__(self, writer, raw, unit_id):
        self.writer,self.raw,self.unit_id=writer,raw,unit_id
        self.calls={}
        self.domain=call_clock_domain()

    def record(self, event):
        value=dict(event)
        value.setdefault('monotonic_ns',time.monotonic_ns())
        self.writer.record(value)

    def ready(self,row_id,stage,payload,*,response_representation,call_id='lotus-map',native_ready_ns=None):
        identity=(row_id,str(stage),call_id)
        if identity in self.calls:
            raise ValueError('method generated a duplicate complete call')
        call=dict(row_id=row_id,stage_id=str(stage),stage_ordinal=stage,call_id=call_id,
                  model_role='main',request_values_sha256=content_digest(json.loads(payload)),
                  response_representation=response_representation)
        if native_ready_ns is not None:
            call['native_ready_ns']=native_ready_ns
            call['readiness_scope']='complete call received by the trusted batch callback; native steady clock retained separately'
        self.calls[identity]=call
        self.record(dict(event='task_ready',clock_domain=self.domain,**call))

    def received(self,row_id,stage,body,*,when=None,call_id='lotus-map'):
        self.record(dict(event='caller_response',row_id=row_id,stage_id=str(stage),call_id=call_id,
            monotonic_ns=time.monotonic_ns() if when is None else when,
            clock_domain=self.domain,response_bytes_sha256=hashlib.sha256(body).hexdigest()))

    def save_full(self,row_id,stage,full):
        self.raw.write(json.dumps(dict(row_id=row_id,stage_id=str(stage),
            response_wire=base64.b64encode(encode_full_response(full)).decode()))+'\n')


class _LotusBatchObservation:
    """Measure the original LM's complete batch return; native pools remain native."""
    def __init__(self,lm,rows,observations,executor=None):
        self.original=lm._process_uncached_messages
        self.executor,self.rows,self.observations=executor,rows,observations
        self.stage=0

    def __call__(self,lm,data,kwargs,show_progress_bar,description):
        from src.semantic_methods.lotus.sdk import prepare_call
        stage=self.stage;self.stage+=1
        if len(data)!=len(self.rows):
            raise ValueError('cache-disabled LOTUS batch differs from the source row set')
        for index,item in enumerate(data):
            call=prepare_call(lm,item[0],kwargs)
            self.observations.ready(self.rows[index]['row_id'],stage,call.payload,
                                    response_representation='complete native SDK ModelResponse values')
        if self.executor is None:
            results=self.original(data,kwargs,show_progress_bar,description)
        else:
            self.executor.on_response=lambda index,full:self.observations.save_full(self.rows[index]['row_id'],stage,full)
            results=self.executor(lm,data,kwargs,show_progress_bar,description)
        if len(results)!=len(self.rows):
            raise ValueError('LOTUS native batch returned an incomplete response list')
        returned=time.monotonic_ns()
        self.observations.record(dict(event='lotus_batch_return',monotonic_ns=returned,
            stage_id=str(stage),response_count=len(results),
            executor='native' if self.executor is None else 'semloom',
            scope='complete SDK response list returned before observation serialization and LOTUS statistics'))
        for row,response in zip(self.rows,results):
            if not callable(getattr(response,'model_dump',None)):
                raise ValueError('LOTUS returned a failed complete SDK response')
            response_values=response.model_dump()
            body=json.dumps(response_values,sort_keys=True,ensure_ascii=False,allow_nan=False).encode()
            self.observations.received(row['row_id'],stage,body,when=returned)
            self.observations.raw.write(json.dumps(dict(row_id=row['row_id'],stage_id=str(stage),
                response_representation='complete native SDK ModelResponse values',
                response_values=response_values),ensure_ascii=False,allow_nan=False)+'\n')
        return results


class _LotusMethodObservation:
    """Observe supplier callbacks without changing their state or continuation."""
    def __init__(self,method,observations):
        self.method,self.observations=method,observations

    def start(self,value):
        step=self.method.start(value)
        row=json.loads(value)
        self.observations.ready(row['row_id'],0,step.request.payload,
                                response_representation='complete HTTP response body')
        return step

    def resume(self,state,result):
        from src.semantic_methods.continuation import Continue
        current=json.loads(state);row_id=current['row']['row_id'];stage=current['stage']
        full=decode_full_response(result)
        self.observations.received(row_id,stage,full.body)
        self.observations.save_full(row_id,stage,full)
        step=self.method.resume(state,result)
        if isinstance(step,Continue):
            self.observations.ready(row_id,stage+1,step.request.payload,
                                    response_representation='complete HTTP response body')
        return step


def prepare_lotus_lm(plan, model, options, tokenizer_path):
    os.environ['LITELLM_LOCAL_MODEL_COST_MAP']='True'
    import lotus
    from lotus.models import LM
    from src.semantic_methods.lotus.sdk import validate_source
    validate_source()
    token=model.bearer_token or 'local-fixture'
    tokenizer=None
    if tokenizer_path is not None:
        from tokenizers import Tokenizer
        tokenizer=Tokenizer.from_file(str(tokenizer_path))
    return LM('openai/'+model.model_id,api_base=model.endpoint_url.removesuffix('/chat/completions'),
        api_key=token,max_batch_size=options.concurrency,temperature=0,top_p=1,max_tokens=plan.max_tokens,
        num_retries=0,max_retries=0,timeout=model.timeout_ms/1000,tokenizer=tokenizer)


@contextmanager
def _lotus_rows(stack,arm,values,plan,model,execution,observations,options,unit_id,stages,stop,tokenizer_path,*,lm=None,max_held_tasks=None):
    import lotus
    import pandas as pd
    from src.semantic_methods.lotus.batch import LotusBatchExecutor,lotus_executor
    from src.semantic_methods.lotus.maps import LotusMapStage,LotusTwoMapMethod,staged_two_map
    from src.semantic_methods.lotus.driver import iter_two_map_rows
    from src.semantic_methods.continuation import MethodLimits
    from src.semantic_methods.budget import MethodCapacity,row_reservation
    token=model.bearer_token or 'local-fixture'
    configured=replace(model,bearer_token=token)
    if lm is None:
        lm=prepare_lotus_lm(plan,model,options,tokenizer_path)
    frame=pd.DataFrame({'row_id':[v['row_id'] for v in values],'text':[v['text'] for v in values]})
    context=stack.enter_context(lotus.settings.context(lm=lm,enable_cache=False))
    selected=None if execution is None else LotusBatchExecutor(execution,configured,
        query_id=unit_id,operator_id='lotus-map',cancelled=stop.is_set)
    chain=arm.startswith('lotus-two-map')
    if chain:
        if stages is None:
            raise ValueError('the two Map comparison requires explicit supplier stages')
        stages=tuple(LotusMapStage(**s) for s in stages)
    if arm.endswith('incremental'):
        method=LotusTwoMapMethod(lm,stages,model_config=configured)
        observed=_LotusMethodObservation(method,observations)
        limits=MethodLimits(65536,262144,524288,2)
        held_tasks,_=resolve_adapter_limits(options,max_held_tasks)
        capacity=MethodCapacity(held_tasks,held_tasks*row_reservation(limits))
        def execute():
            source=(json.dumps(dict(row_id=v['row_id'],text=v['text']),ensure_ascii=False).encode() for v in values)
            iterator=iter_two_map_rows(execution,observed,source,query_id=unit_id,operator_id='lotus-map',
                limits=limits,capacity=capacity,max_rows=len(values),cancelled=stop.is_set)
            try:
                for result in iterator:
                    row=json.loads(result.value)['row']
                    yield row['row_id'],row[stages[-1].output_column]
            finally:
                iterator.close()
    else:
        observer=_LotusBatchObservation(lm,values,observations,selected)
        stack.enter_context(lotus_executor(lm,observer))
        def execute():
            if chain:
                result=staged_two_map(frame,lm,stages)
                column=stages[-1].output_column
            else:
                result=frame.sem_map(plan.instruction+'\nInput: {text}',suffix='_answer',return_raw_outputs=True)
                column='_answer'
            yield from zip(result['row_id'],result[column])
    yield execute,dict(supplier='LOTUS1.2.4',source_commit='b1a85fd7a66fabed8a1585d44d7597d592b4433f',
        cache_policy='disabled',method_roles=['main'],two_map=chain,
        caller_scope=('supplier resume callback receives complete HTTP response' if arm.endswith('incremental')
                      else 'original LM receives full uncached SDK response list; whole batch wait retained'),
        native_supply='DataFrame formatting and complete uncached batch; native pool retained only on native arm')


def _drain_execution(execution, *, close=True):
    until=time.monotonic()+execution.drain_timeout_s
    while execution.engine.capacity.usage()!=Usage() and time.monotonic()<until:
        execution.engine.advance();time.sleep(.01)
    if (execution.engine.capacity.usage()!=Usage() or execution.engine.jobs.jobs
            or (close and not execution.close(execution.drain_timeout_s))):
        raise RuntimeError('supplier execution retains unresolved model resources')


def prepare_duckdb_connection(stack,plan,model,options,library,*,semloom_batch=False):
    import ctypes
    import duckdb
    import _duckdb
    from src.baselines.text.products.duckdb_ai import DuckDBAiConfig,configure_ai_endpoint
    if library is None:raise ValueError('DuckDB adapter requires its verified patched extension')
    if model.timeout_ms%1000:raise ValueError('DuckDB requires an integral request timeout in seconds')
    runtime=ctypes.CDLL(_duckdb.__file__,mode=ctypes.RTLD_GLOBAL)
    connection=stack.enter_context(duckdb.connect(config={'allow_unsigned_extensions':'true','threads':'1'}))
    connection.execute("LOAD '"+str(Path(library).resolve()).replace("'","''")+"'")
    version=connection.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ai' AND loaded").fetchone()[0]
    if duckdb.__version__!='1.5.4' or version!='0.4.14-semloom1':
        raise ValueError('DuckDB adapter binary does not match its compiled source identity')
    token=model.bearer_token or 'EMPTY'
    # The whole-vector callback bypasses ProviderExecutorState and its native pool.
    native_capacity=min(options.concurrency,64) if semloom_batch else options.concurrency
    configure_ai_endpoint(connection,DuckDBAiConfig(model.endpoint_url.removesuffix('/chat/completions'),
        model.model_id,token,max_tokens=plan.max_tokens,max_concurrent_requests=native_capacity,
        timeout_seconds=model.timeout_ms//1000))
    environment_name='DUCKDB_AI_CONNECT_TIMEOUT_SECONDS'
    previous=os.environ.get(environment_name)
    os.environ[environment_name]=str(model.timeout_ms//1000)
    def restore_timeout():
        if previous is None:os.environ.pop(environment_name,None)
        else:os.environ[environment_name]=previous
    stack.callback(restore_timeout)
    connection.execute('CREATE TABLE adapter_inputs(source_position BIGINT,row_id VARCHAR,prompt VARCHAR)')
    return connection


def replace_duckdb_inputs(connection,values,plan):
    from src.baselines.text.frameworks.semantic_map import application_prompt
    connection.execute('DELETE FROM adapter_inputs')
    connection.executemany('INSERT INTO adapter_inputs VALUES (?,?,?)',[(i,v['row_id'],application_prompt(plan.instruction,v['text'])) for i,v in enumerate(values)])


@contextmanager
def _duckdb_rows(stack,arm,values,plan,model,execution,observations,options,library,*,connection=None):
    from src.semantic_methods.duckdb_ai import DuckDBSemLoomBridge,DuckDBNativeTaskExecutor
    token=model.bearer_token or 'EMPTY'
    if connection is None:
        connection=prepare_duckdb_connection(stack,plan,model,options,library,semloom_batch=execution is not None)
        replace_duckdb_inputs(connection,values,plan)
    if execution is not None:
        native=DuckDBNativeTaskExecutor(execution,replace(model,bearer_token=token))
        supplied_rows=0
        iterator_close_error=None
        def execute_batch(calls,cancelled):
            nonlocal supplied_rows,iterator_close_error
            iterator_close_error=None
            if sorted(c.row for c in calls)!=list(range(len(calls))) or supplied_rows+len(calls)>len(values):
                raise ValueError('DuckDB source vector differs from the ordered non-NULL input')
            row_ids={c.call_id:values[supplied_rows+c.row]['row_id'] for c in calls}
            supplied_rows+=len(calls)
            for call in calls:
                observations.ready(row_ids[call.call_id],0,call.payload,
                    call_id='duckdb-map',native_ready_ns=call.ready_ns,response_representation='complete native provider HTTP body')
            iterator=iter(native(calls,cancelled))
            primary_error=None
            try:
                for response in iterator:
                    observations.received(row_ids[response.call_id],0,response.body,call_id='duckdb-map')
                    observations.raw.write(json.dumps(dict(row_id=row_ids[response.call_id],status=response.http_status,
                        body_base64=base64.b64encode(response.body).decode()))+'\n')
                    yield response
            except GeneratorExit:
                raise
            except BaseException as error:
                primary_error=error
                raise
            finally:
                try:iterator.close()
                except BaseException as error:
                    iterator_close_error=redact_text(f'{type(error).__name__}: {error}')[:4096]
                    if primary_error is None:raise
                    primary_error.add_note('DuckDB observation iterator close also failed: '+type(error).__name__)
        bridge=stack.enter_context(DuckDBSemLoomBridge(library,execute_batch));bridge.enable(connection)
    statement=('WITH completed AS MATERIALIZED (SELECT row_id,ai_try_complete(prompt,max_tokens => '+str(plan.max_tokens)+
        ',temperature => 0.0) AS result FROM (SELECT * FROM adapter_inputs ORDER BY source_position)) '
        'SELECT row_id,result.response,result.error FROM completed ORDER BY row_id')
    def execute():
        for row_id,value,error in connection.execute(statement).fetchall():
            if error is not None or value is None:raise ValueError('native DuckDB parser reported a failed complete call')
            yield row_id,value
    identity=dict(supplier='DuckDB1.5.4/ai0.4.14-semloom1',source_commit='9b7b16a5d5bfa97180b8be48d69bd9a4a4106419',
        native_provider_capacity=min(options.concurrency,64) if execution is not None else options.concurrency,
        native_provider_pool_used=execution is None,
        sql_threads=1,method_roles=['main'],request_cache=False,retry_count=0,
        caller_scope='complete body returned by native batch callback immediately before C++ response consumer',
        native_supply='current SQL vector only; SQL results wait for complete vector return')
    try:
        yield execute,identity
    finally:
        if execution is not None:
            identity['diagnostics']=dict(
                bridge=dict(last_error=bridge.last_error,last_cleanup_error=bridge.last_cleanup_error),
                iterator_close_error=iterator_close_error,
                executor=dict(last_cleanup_error=native.last_cleanup_error,
                    last_cleanup_errors=list(native.last_cleanup_errors),
                    last_close_report=None if native.last_close_report is None else asdict(native.last_close_report)))


def run_supplier_query(arm, *, load_source,plan,model,ledger,unit_id,root,options,
                       physical=None,ray_temp_root=None,ray_address=None,query_timeout_s=120,
                       stages=None,sema_binary=None,tokenizer_path=None,
                       reference_outputs=None,allowed_outputs=None,duckdb_library=None,
                       preparation_started_ns=None, owner=None,max_held_tasks=None,sema_native_threads=None):
    if arm not in SUPPLIER_ARMS:
        raise ValueError('supplier arm has not been integrated')
    use_core='semloom' in arm
    local=arm=='lotus-method-semloom-local-diagnostic'
    if use_core and physical is None and not local:
        raise ValueError('supplier SemLoom arm requires its Daft/Ray transport')
    if (not use_core or local) and physical is not None:
        raise ValueError('Ray Map physical options belong to SemLoom only')
    if physical is not None and (physical.window_bytes < MAX_FRAME_BYTES+24
            or physical.payload_backend != 'daft'
            or max(physical.workers,physical.batch_rows) > options.concurrency):
        raise ValueError('supplier SemLoom arm requires declared Daft batches fitting a complete legal task')
    if plan.model_id != model.model_id:
        raise ValueError('method and service model identities differ')
    held_tasks,native_threads=resolve_adapter_limits(options,max_held_tasks,sema_native_threads)
    invoked=time.monotonic_ns()
    started=invoked if preparation_started_ns is None else preparation_started_ns
    if type(started) is not int or not 0 < started <= invoked:
        raise ValueError('application preparation time must precede executor entry')
    root=Path(root);new_private_directory(root)
    errors=CellErrors();stop=threading.Event();lock=threading.Lock()
    count=0;execution=None;reserved=False;prepared_native=None;service=None
    predictions={};summary=dict(schema='semloom.supplier_adapter_query.v1',status='failed',arm=arm,
        unit_id=unit_id,performance_qualified=False,query_preparation_started_ns=started,
        source_identity='finite external raw input; no PG multi-Map',options=asdict(options),
        semloom_capacity=dict(held_tasks=held_tasks,active_requests=options.concurrency) if use_core else None,
        sema_native_threads=native_threads if arm.startswith('sema-') else None,
        request_budget_owner='common proxy before upstream POST',stages={})
    values=[];maximum=0

    def before(route,body):
        nonlocal count
        with lock:
            if stop.is_set():raise RuntimeError('supplier query stopped further model forwarding')
            value=json.loads(body)
            if value.get('model')!=model.model_id or value.get('stream',False):
                raise ValueError('supplier changed single-model nonstreaming semantics')
            if count==maximum:raise ValueError('supplier exceeded its declared complete-call count')
            shared.reserve(hashlib.sha256(body).hexdigest());count+=1

    def after(route,body,response,status):
        try:
            with lock:
                upstream_raw.write(json.dumps(dict(request_values_sha256=content_digest(json.loads(body)),
                    request_body_sha256=hashlib.sha256(body).hexdigest(),http_status=status,
                    response_body_sha256=hashlib.sha256(response).hexdigest(),
                    response_body_base64=base64.b64encode(response).decode()))+'\n')
            record_native_response(protocols,body,response,status,model.model_id)
        except BaseException as failure:
            stop.set();errors.record('http_failure',failure)
            if prepared_native is not None:errors.attempt('native_cancel',prepared_native.cancel)
            raise

    try:
        source_started=time.monotonic_ns()
        values=load_bounded_rows(load_source)
        summary['stages']['source_seconds']=(time.monotonic_ns()-source_started)/1e9
        row_ids={v['row_id'] for v in values}
        if reference_outputs is not None and (not isinstance(reference_outputs,dict)
                or set(reference_outputs)!=row_ids
                or any(not isinstance(v,str) for v in reference_outputs.values())):
            raise ValueError('evaluation references must match all external row occurrences')
        maximum=len(values)*(2 if arm.startswith('lotus-two-map') else 1)
        ledger.reserve_unit(unit_id,maximum);reserved=True;shared=ledger.claim_shared_unit(unit_id)
        with ExitStack() as stack:
            writer=stack.enter_context(BufferedEvents(root/'method-events.jsonl'))
            raw=stack.enter_context(open_private_text(root/'complete-responses.jsonl'))
            upstream_raw=stack.enter_context(open_private_text(root/'upstream-response-bodies.jsonl'))
            protocols=stack.enter_context(open_private_text(root/'protocols.jsonl'))
            observations=MethodObservations(writer,raw,unit_id)
            if owner is not None:
                physical,runtime=owner.physical,owner.group.runtime
                summary['runtime']=runtime
            elif use_core and not local:
                physical,runtime=_runtime(stack,'fixed-map-semloom',options,physical,ray_temp_root,ray_address)
                summary['runtime']=runtime
            if owner is None:
                gateway=stack.enter_context(ObservationGateway(routes=(GatewayRoute(unit_id,'model',model.endpoint_url),),
                    trace_path=root/'http-trace.jsonl',request_timeout_s=min(query_timeout_s,model.timeout_ms/1000),
                    before_forward=before,after_forward=after))
                routed=replace(model,endpoint_url=gateway.endpoint_url(unit_id,'model'))
            else:
                routed=owner.model
            def core_record(event):
                observations.record(dict(event,event='backend_'+event['event']) if event.get('event') in (
                    'http_started','http_finished') else event)
            if owner is not None:
                stack.enter_context(owner.query(unit_id,root,before,after,core_record))
                execution=owner.execution
            elif use_core and not arm.startswith('sema-'):
                token=routed.bearer_token or ('EMPTY' if arm.startswith('duckdb-') else 'local-fixture')
                execution=build_native_execution(replace(routed,bearer_token=token),
                    physical=physical,max_tasks=held_tasks,max_active_requests=options.concurrency,
                    observer=core_record,timeouts=SessionTimeouts(backend_s=max(45,query_timeout_s)))
                def close_execution():
                    nonlocal execution
                    summary['core_cleanup']=dict(final_core_usage=asdict(execution.engine.capacity.usage()))
                    errors.attempt('execution_drain',lambda:_drain_execution(execution))
                    summary['core_cleanup']['final_core_usage']=asdict(execution.engine.capacity.usage())
                    execution=None
                stack.callback(close_execution)
            if arm.startswith('lotus-'):
                execute,identity=stack.enter_context(_lotus_rows(stack,arm,values,plan,routed,execution,
                    observations,options,unit_id,stages,stop,tokenizer_path,
                    lm=owner.lotus_lm() if owner is not None else None,max_held_tasks=held_tasks))
                if local:
                    identity.update(executor='SemLoom local diagnostic',payload_backend='no Daft or Ray')
                summary['identity']=identity
            elif arm.startswith('duckdb-'):
                execute,identity=stack.enter_context(_duckdb_rows(stack,arm,values,plan,routed,execution,
                    observations,options,duckdb_library,
                    connection=owner.duckdb_connection(values) if owner is not None else None))
                summary['identity']=identity
            else:
                from src.execution_provider.adapters.sema_service import SemaRequestService,SemaServiceLimits,prepare_sema_projection
                from src.execution_provider.adapters.sema_semloom import SemaSemLoomService
                if sema_binary is None:raise ValueError('Sema requires the verified author binary')
                limits=SemaServiceLimits(max_requests=maximum,request_bytes=1048576,
                    response_bytes=1048576-8204,timeout_s=query_timeout_s)
                if arm=='sema-native-transparent':
                    service=stack.enter_context(SemaRequestService(query_id=unit_id,upstream_url=routed.endpoint_url,
                        limits=limits,trace_path=root/'sema-service.jsonl'))
                elif use_core:
                    service=stack.enter_context(SemaSemLoomService(query_id=unit_id,model_config=routed,physical=physical,
                        limits=limits,trace_path=root/'sema-service.jsonl',max_held_tasks=held_tasks,
                        max_active_requests=options.concurrency,
                        executor_owner=owner.sema_executor if owner is not None else None))
                if owner is not None:
                    owner.service=service
                converted=[dict(source_example_id=v['row_id'],input_text=v['text'],source_position=i) for i,v in enumerate(values)]
                new_private_directory(root/'native')
                prepared_native=stack.enter_context(prepare_sema_projection(converted,plan,routed,binary=sema_binary,
                    root=root/'native',num_threads=native_threads,service=service,
                    native=owner.sema_native(converted) if owner is not None else None))
                execute=prepared_native.execute
                summary['identity']=dict(supplier='Sema author binary',integration='request service',
                    native_supply='author SQL, threads, request pool, prompt, parser and row association retained',
                    sema_executor_scope='query' if owner is None else owner.group.sema_executor_scope)
            ready=time.monotonic_ns()
            if count:raise ValueError('supplier preparation called the model before query submission')
            @contextmanager
            def open_rows():
                def rows():
                    for row_id,output in execute():
                        if row_id in predictions or row_id not in row_ids:
                            raise ValueError('supplier result lost source occurrence association')
                        if allowed_outputs is not None and output not in allowed_outputs:
                            stop.set();raise ValueError('supplier returned an undeclared application value')
                        predictions[row_id]=output
                        yield row_id,output
                stream=rows()
                try:yield stream
                finally:stream.close()
            def cancel():
                stop.set()
                if prepared_native is not None:prepared_native.cancel()
            with ProcessSampler(root/'query-rss.jsonl',{'supplier_driver':os.getpid()},include_children=True) as sampler:
                summary['execution']=record_prepared_execution(root/'q0',open_rows,backend_ready_ns=ready,
                    preparation_started_ns=started,max_rows=len(values),max_result_bytes=len(values)*70000,
                    flush_rows=64,query_timeout_s=query_timeout_s,cancel_query=cancel)
            summary['resources']=sampler.summary()
            if prepared_native is not None:summary['native_query']=prepared_native.summary
            if service is not None:summary['request_service']=service.summary
        traces=[json.loads(line) for line in (root/'http-trace.jsonl').read_text().splitlines()]
        if (count!=maximum or len(predictions)!=len(values) or len(traces)!=count
                or any(t['status']!='completed' or t['request_body_sha256']!=t['forwarded_body_sha256'] for t in traces)):
            raise ValueError('supplier task, POST or row counts differ')
        events=[json.loads(line) for line in (root/'method-events.jsonl').read_text().splitlines()]
        calls=list(observations.calls.values());write_private_json(root/'calls.json',calls)
        if calls:
            protocols=[json.loads(line) for line in (root/'protocols.jsonl').read_text().splitlines()]
            if Counter(c['request_values_sha256'] for c in calls)!=Counter(content_digest(p['request']) for p in protocols):
                raise ValueError('supplier actual HTTP values differ from method-prepared complete calls')
        summary.update(status='passed',rows=len(values),actual_posts=count,preparation_model_posts=0,
            full_query_seconds=(summary['execution']['t_query_terminal_ns']-started)/1e9,
            method_calls=calls,observed_peak_http=proxy_http_peak(traces),executor_phases=executor_phase_observations(events))
        if calls:
            summary['call_timing']=summarize_calls(events,calls)
        else:
            summary['call_timing']=dict(status='unavailable',reason=(
                'native C++ executor has no pre-pool call observer in the integrated runner' if arm.startswith('duckdb-')
                else 'author binary exposes no task readiness or per-row full response receipt'))
        if service is not None:
            summary['request_service']=service.summary
        if reference_outputs is not None:
            if set(reference_outputs)!=set(predictions):raise ValueError('reference rows differ from supplier results')
            correct=sum(predictions[k]==v for k,v in reference_outputs.items())
            summary['quality']=dict(rows=len(values),correct=correct,accuracy=correct/len(values))
        else:summary['quality']=dict(status='unavailable',reason='no reference labels supplied')
    except BaseException as failure:
        stop.set();errors.record('query',failure)
    finally:
        if execution is not None and owner is None:errors.attempt('execution_drain',lambda:_drain_execution(execution))
        if reserved:errors.attempt('budget_close',lambda:ledger.close_shared_unit(unit_id))
        if errors.first is not None:summary['status']='failed'
        summary.update(errors=errors.details,attempted_posts=count,ended_ns=time.monotonic_ns())
        errors.attempt('summary_write',lambda:write_private_json(root/'summary.json',redact_json_values(summary)))
    errors.raise_if_failed()
    return summary
