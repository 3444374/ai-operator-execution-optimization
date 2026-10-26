"""Optional native API sidecar; observes delegation without changing results."""
import functools
import json
import math
import os
from pathlib import Path
import time


def writer(directory):
    directory=Path(directory)
    def emit(event, **value):
        try:
            row=dict(event=event,pid=os.getpid(),monotonic_ns=time.monotonic_ns(),**value)
            raw=(json.dumps(row,separators=(',',':'),allow_nan=False)+'\n').encode()
            fd=os.open(directory/('native-'+str(os.getpid())+'.jsonl'),os.O_CREAT|os.O_WRONLY|os.O_APPEND,0o600)
            try:os.write(fd,raw)
            finally:os.close(fd)
        except Exception:
            pass  # Optional evidence failure never changes query/cancellation.
    return emit


def install(async_llm_class, common, emit):
    original_add=async_llm_class.add_request
    original_abort=async_llm_class.abort
    original_convert=common.result_from_output
    original_generate=getattr(async_llm_class,'generate',None)
    original_shutdown=getattr(async_llm_class,'shutdown',None)
    def native_identity(self,external,internal=False):
        processor=getattr(self,'output_processor',None)
        states=getattr(processor,'request_states',None)
        aliases=getattr(processor,'external_req_ids',None)
        if not isinstance(states,dict) or not isinstance(aliases,dict):return None
        ids=([external] if internal else list(aliases.get(external,())))
        if any(not isinstance(x,str) for x in ids):return None
        return dict(lookup_internal=internal,mapped_internal_ids=ids,
                    present_internal_ids=[x for x in ids if x in states],
                    external_id_by_internal={x:getattr(states.get(x),'external_req_id',None) for x in ids})
    def observe(event, **value):
        try:emit(event,**value)
        except Exception:pass

    @functools.wraps(original_add)
    async def add(self, request_id, *args, **kwargs):
        params=kwargs.get('sampling_params',args[1] if len(args)>1 else None)
        sampling={key:getattr(params,key,None) for key in ('max_tokens','temperature','top_p','seed','stop','presence_penalty','frequency_penalty','repetition_penalty')}
        observe('native_add_enter',request_id=request_id,sampling=sampling)
        try:collector=await original_add(self,request_id,*args,**kwargs)
        except BaseException as error:
            observe('native_add_error',request_id=request_id,error_type=type(error).__name__)
            raise
        try:
            observe('native_add_return',request_id=request_id,
                    collector_request_id=getattr(collector,'request_id',None),native_identity=native_identity(self,request_id))
        except Exception:pass
        return collector

    @functools.wraps(original_abort)
    async def abort(self, request_id, *args, **kwargs):
        internal=kwargs.get('internal',args[0] if args else False)
        identity=None
        try:
            identifiers=([request_id] if isinstance(request_id,str) else request_id if type(request_id) in (list,tuple) else None)
            if identifiers is not None:identity={key:native_identity(self,key,internal=internal) for key in identifiers}
        except Exception:pass
        observe('native_abort_enter',request_id=request_id,internal=internal,native_identities=identity)
        try:result=await original_abort(self,request_id,*args,**kwargs)
        except BaseException as error:
            observe('native_abort_error',request_id=request_id,error_type=type(error).__name__)
            raise
        observe('native_abort_return',request_id=request_id)
        return result

    @functools.wraps(original_convert)
    def convert(identity, output, *args, **kwargs):
        result=original_convert(identity,output,*args,**kwargs)
        try:
            metrics=getattr(output,'metrics',None)
            observed={}
            for name in ('arrival_time','first_scheduled_time','first_token_time','finished_time','time_in_queue'):
                value=getattr(metrics,name,None)
                observed[name]=value if type(value) in (int,float) and math.isfinite(value) else None
            observe('native_final_observed',request_id=getattr(output,'request_id',None),
                    call_id=identity['call_id'],query_id=identity['query_id'],
                    finished=getattr(output,'finished',None),status=result.get('status'),native_metrics=observed)
        except Exception:pass
        return result

    if original_generate is not None:
        @functools.wraps(original_generate)
        def generate(self,*args,**kwargs):
            rid=kwargs.get('request_id',args[2] if len(args)>2 else None)
            observe('native_generate_enter',request_id=rid,scope='native API returns original async iterator; not completion')
            try:value=original_generate(self,*args,**kwargs)
            except BaseException as error:
                observe('native_generate_error',request_id=rid,error_type=type(error).__name__);raise
            observe('native_generate_return',request_id=rid,scope='original async iterator returned without consuming or wrapping it')
            return value
        async_llm_class.generate=generate
    if original_shutdown is not None:
        @functools.wraps(original_shutdown)
        def shutdown(self,*args,**kwargs):
            observe('native_shutdown_enter')
            try:value=original_shutdown(self,*args,**kwargs)
            except BaseException as error:
                observe('native_shutdown_error',error_type=type(error).__name__);raise
            observe('native_shutdown_return');return value
        async_llm_class.shutdown=shutdown
    async_llm_class.add_request=add
    async_llm_class.abort=abort
    common.result_from_output=convert


def install_child_command(bootstrap):
    from src.serving.backends.async_engine_process import AsyncEngineProcess
    def command(spec,descriptors):
        return [spec.model_python,'-B',str(bootstrap),*map(str,descriptors)]
    AsyncEngineProcess._command=staticmethod(command)


def install_actor_runtime_env(packet):
    """Keep original actor settings; let optional hooks survive its explicit path."""
    from src.execution_provider.adapters import lotus_records_actor_owner as owner
    from src.execution_provider.adapters import lotus_records_async_actor as async_owner
    packet=Path(packet).resolve(strict=True)
    original=owner._runtime_env
    def runtime_env(python):
        settings=original(python)
        variables=dict(settings['env_vars'])
        variables['PYTHONPATH']=variables['PYTHONPATH']+os.pathsep+str(packet)
        return dict(settings,env_vars=variables)
    owner._runtime_env=runtime_env
    async_owner._runtime_env=runtime_env
