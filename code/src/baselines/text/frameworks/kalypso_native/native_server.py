"""Run the unchanged author CLI with finite call accounting and optional observations."""
import argparse
import contextvars
import hashlib
import importlib
import inspect
import json
import multiprocessing
import os
from pathlib import Path
import runpy
import subprocess
import sys
import time
import uuid

ROW = contextvars.ContextVar('observed_native_row',default=None)
CALL = contextvars.ContextVar('observed_native_call',default=None)
AUTHOR_SOURCE_COMMIT = '05024f027f932c0e398852e38302be30b9015481'


def check_author_source(cfg):
    if cfg['source_commit'] != AUTHOR_SOURCE_COMMIT:
        raise ValueError('Native binding requires the fixed author commit')
    source = Path(cfg['install_root']) / 'source'
    actual = subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip()
    if actual != AUTHOR_SOURCE_COMMIT:
        raise ValueError('Author checkout changed')
    clean = subprocess.run(['git','-C',str(source),'diff','HEAD','--exit-code','--','vllm'],capture_output=True)
    if clean.returncode:
        raise ValueError('Author implementation or native defaults changed')


def native_engine_configuration(runner):
    """Read existing worker config; never profile, allocate or change its values."""
    c = runner.vllm_config
    def values(obj, names):
        return {name: (value if value is None or type(value) in (str,int,float,bool)
                       else str(value)) for name in names for value in [getattr(obj,name,None)]}
    return dict(source='actual worker vllm_config after original load_model returned',
        allocation_scope='KV allocation and graph capture may finish later; retain native startup log',
        model=values(c.model_config,('max_model_len','dtype')),
        scheduler=values(c.scheduler_config,('max_num_seqs','max_num_batched_tokens','enable_chunked_prefill')),
        cache=values(c.cache_config,('gpu_memory_utilization','enable_prefix_caching','block_size','num_gpu_blocks')),
        parallel=values(c.parallel_config,('tensor_parallel_size','pipeline_parallel_size')),
        attention=values(getattr(c,'attention_config',None),('backend',)),
        compilation=values(c.compilation_config,('max_cudagraph_capture_size',)))


def _worker_status(directory,status,**fields):
    try:
        path=Path(directory)/'worker-observer-status.json'
        previous=json.loads(path.read_text()) if path.exists() else {'history':[]}
        entry=dict(status=status,pid=os.getpid(),at_ns=time.monotonic_ns(),**fields)
        history=[*previous['history'],entry]
        temp=path.with_name(path.name+'.'+str(os.getpid())+'.tmp')
        temp.write_text(json.dumps(dict(entry,history=history),indent=2)+'\n')
        os.replace(temp,path)
        return True
    except Exception as error:
        # Persisting an observation must not replace the original loader outcome.
        print('[kalypso-worker-observation] status unavailable: '+type(error).__name__,file=sys.stderr)
        return False


def install_worker_model_observation(cfg,directory,runner_class):
    """Install inside native init_worker, before the original loader runs."""
    directory=Path(directory)
    if getattr(runner_class.load_model,'_kalypso_observer_revision',None)==cfg['observer_revision']:
        return
    original=runner_class.load_model
    runner_path=str(Path(inspect.getfile(runner_class)).resolve())
    _worker_status(directory,'installed',observer_revision=cfg['observer_revision'],
        observer_module_path=str(Path(__file__).resolve()),
        observer_module_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        runner_module_path=runner_path,start_method=multiprocessing.get_start_method(allow_none=True),
        api_pid=int(os.environ.get('KALYPSO_OBSERVER_API_PID','0')))
    def load(self,*args,**kwargs):
        try:
            value=original(self,*args,**kwargs)
        except BaseException as error:
            _worker_status(directory,'original_load_failed',error_type=type(error).__name__)
            raise
        try:
            from adapter import read_gpu_uuid_hash
            visible=os.environ.get('CUDA_VISIBLE_DEVICES')
            if visible!=str(cfg['physical_gpu_index']):
                raise ValueError('Single physical GPU ordinal mapping differs from the fixed launch')
            actual_uuid=read_gpu_uuid_hash(cfg['physical_gpu_index'])
            if actual_uuid!=cfg['gpu_uuid_sha256']:raise ValueError('Actual worker physical GPU UUID differs')
            parameter=next(self.model.parameters());config=self.model_config
            proof=dict(model=config.model,hidden_size=config.hf_text_config.hidden_size,
                model_uid=uuid.uuid4().hex,model_class=type(self.model).__qualname__,dtype=str(parameter.dtype),
                first_parameter_shape=list(parameter.shape),first_parameter_device=str(parameter.device),
                parameter_numel=sum(p.numel() for p in self.model.parameters()),pid=os.getpid(),
                loaded_monotonic_ns=time.monotonic_ns(),observer_revision=cfg['observer_revision'],
                runner_module_path=runner_path,observer_module_path=str(Path(__file__).resolve()),
                physical_gpu_index=cfg['physical_gpu_index'],physical_gpu_uuid_sha256=actual_uuid,
                CUDA_VISIBLE_DEVICES=visible,weights_copied_to_CPU=False,
                source='actual worker GPUModelRunner.model metadata after original load_model returned',
                native_engine_configuration=native_engine_configuration(self))
            path=directory/'loaded-model.json';temp=path.with_name(path.name+'.'+str(os.getpid())+'.tmp')
            with temp.open('x') as stream:
                json.dump(proof,stream,indent=2);stream.flush();os.fsync(stream.fileno())
            os.link(temp,path);temp.unlink()  # Atomic publication, never overwrite an older proof.
            _worker_status(directory,'model_observed',model_uid=proof['model_uid'],
                observer_revision=cfg['observer_revision'],physical_gpu_uuid_sha256=actual_uuid)
        except Exception as error:
            _worker_status(directory,'observation_failed',error_type=type(error).__name__)
        return value
    load._kalypso_observer_revision=cfg['observer_revision']
    runner_class.load_model=load


def __getattr__(name):
    if name!='WorkerObservationExtension':raise AttributeError(name)
    # vLLM resolves this name in WorkerWrapperBase.init_worker, including spawn.
    config_path=os.environ.get('KALYPSO_OBSERVER_CONFIG')
    directory=os.environ.get('KALYPSO_OBSERVER_EVIDENCE')
    if not config_path or not directory:raise RuntimeError('Worker observation context was not propagated')
    cfg=json.loads(Path(config_path).read_text())
    if os.environ.get('KALYPSO_OBSERVER_UNIT') not in {u['unit_id'] for u in cfg['units']}:
        raise RuntimeError('Worker observation unit context differs')
    try:
        module=importlib.import_module('vllm.v1.worker.gpu_model_runner')
        install_worker_model_observation(cfg,directory,module.GPUModelRunner)
    except BaseException as error:
        _worker_status(directory,'bootstrap_failed',error_type=type(error).__name__)
        raise
    extension=type(name,(),{'__module__':__name__})
    globals()[name]=extension
    return extension


def install_hooks(cfg,unit,directory,executor_class,filter_class,model_runner_class=None):
    directory=Path(directory)
    allowed={q['query_id']:q['expected_calls'] for q in unit['queries']}
    counters=dict(reserved_calls=0,cap=unit['call_cap'],query_calls=dict.fromkeys(allowed,0),
                  observation_errors=[],model_uid=None)
    from src.baselines.common.redact import redact_json_values

    def event(name,**fields):
        try:
            row=dict(event=name,monotonic_ns=time.monotonic_ns(),pid=os.getpid())
            row.update(fields)
            with (directory/'native-trace.jsonl').open('a') as stream:
                stream.write(json.dumps(redact_json_values(row),ensure_ascii=False)+'\n')
        except Exception as error:
            counters['observation_errors'].append(type(error).__name__)

    def identity(raw):
        return raw.headers.get('X-Kalypso-Observation')

    old_chat=executor_class.chatcomplete
    async def chat(self,raw_request,prompt,max_tokens,pin=False,priority=0):
        qid=identity(raw_request)
        # This is the declared safety budget, separate from optional measurement.
        if (qid not in allowed or counters['reserved_calls']>=counters['cap']
                or counters['query_calls'][qid]>=allowed[qid]):
            raise RuntimeError('Declared native call budget or query identity exceeded; no next model invocation')
        counters['reserved_calls']+=1
        counters['query_calls'][qid]+=1
        sequence=counters['reserved_calls']
        started=time.monotonic_ns()
        if counters['model_uid'] is None:
            try:
                counters['model_uid']=json.loads((directory/'loaded-model.json').read_text())['model_uid']
            except Exception as error:
                counters['observation_errors'].append(type(error).__name__)
        fields=dict(query_id=qid,idx=ROW.get(),sequence=sequence,model=self.model,
            model_uid=counters['model_uid'],max_tokens=max_tokens,pin=pin,priority=priority)
        try:
            fields['messages_sha256']=hashlib.sha256(json.dumps(prompt,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
        except Exception as error:
            fields['messages_sha256']=None
            counters['observation_errors'].append(type(error).__name__)
        event('native_API_start',started_ns=started,**fields)
        token=CALL.set((qid,sequence))
        try:
            output=await old_chat(self,raw_request,prompt,max_tokens,pin=pin,priority=priority)
        except BaseException as error:
            ended=time.monotonic_ns()
            event('native_API_end',**fields,status='failed_or_unknown',error_type=type(error).__name__,
                started_ns=started,ended_ns=ended,elapsed_s=(ended-started)/1e9)
            raise
        finally:
            CALL.reset(token)
        ended=time.monotonic_ns()
        event('native_API_end',**fields,status='succeeded',request_id=output.request_id,
            finish_reason=output.finish_reason,started_ns=started,ended_ns=ended,elapsed_s=(ended-started)/1e9)
        return output

    old_filter=filter_class._run_single
    async def filtered(self,ctx,priority=0):
        token=ROW.set(ctx.state.idx)
        try:
            value=await old_filter(self,ctx,priority=priority)
            try:
                verdict=next(iter(ctx.output[-1].values()))
                event('filter_result',query_id=identity(ctx.state.raw_request),idx=ctx.state.idx,
                    decision=value,verdict=verdict,model_uid=counters['model_uid'])
            except Exception as error:
                counters['observation_errors'].append(type(error).__name__)
            return value
        finally:
            ROW.reset(token)

    executor_class.chatcomplete=chat
    filter_class._run_single=filtered
    if hasattr(executor_class,'_build_chat_request'):
        old_build=executor_class._build_chat_request
        def build(self,*args,**kwargs):
            value=old_build(self,*args,**kwargs)
            try:
                qid,sequence=CALL.get()
                event('native_sampling',query_id=qid,sequence=sequence,idx=ROW.get(),
                    model=value.model,sampling={name:getattr(value,name) for name in
                        ['max_tokens','temperature','seed','top_p','frequency_penalty','repetition_penalty','stop']})
            except Exception as error:
                counters['observation_errors'].append(type(error).__name__)
            return value
        executor_class._build_chat_request=build
    if model_runner_class is not None and 'observer_revision' in cfg:
        install_worker_model_observation(cfg,directory,model_runner_class)
    return counters


def server_argv(cfg):
    if cfg.get('native_launch') != 'author_defaults' or 'engine' in cfg:
        raise ValueError('Use a fresh native_launch=author_defaults binding without engine overrides')
    return ['serve',cfg['model'],'--host','127.0.0.1','--port',str(cfg['port']),
        '--worker-extension-cls','native_server.WorkerObservationExtension']


def native_launch_report(cfg, parsed):
    argv = server_argv(cfg)
    if parsed.model_tag != cfg['model'] or parsed.worker_extension_cls != 'native_server.WorkerObservationExtension':
        raise ValueError('Actual native CLI model or observation binding differs')
    names = ('max_num_seqs','max_num_batched_tokens','max_model_len','gpu_memory_utilization',
             'dtype','tensor_parallel_size','seed','attention_backend','enable_prefix_caching',
             'enable_chunked_prefill','kv_cache_metrics_sample')
    return dict(policy='author_defaults',author_commit=AUTHOR_SOURCE_COMMIT,argv=argv,
        parsed_options={name:getattr(parsed,name,None) for name in names},
        capacity_overrides=False,
        parameter_source='author CLI defaults; model and localhost endpoint are explicit bindings',
        observation_override='worker extension only; no scheduler or sampling override',
        effective_capacity_source='worker loaded-model.json.native_engine_configuration and native startup log; CPU parsing is not hardware resolution')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path,required=True);p.add_argument('--unit',required=True)
    p.add_argument('--evidence',type=Path,required=True);p.add_argument('--cpu-check',action='store_true')
    a=p.parse_args();cfg=json.loads(a.config.read_text())
    unit=next(u for u in cfg['units'] if u['unit_id']==a.unit)
    argv=server_argv(cfg)
    check_author_source(cfg)
    sys.path.insert(0,str(Path(cfg['baseline_source'])/'code'))
    import torch
    from vllm.kalypso.execution.vllm_executor import VLLMExecutor
    from vllm.kalypso.controller import SemanticPlan  # Preserve the validated native import order.
    from vllm.kalypso.sem_ops.filters import SemFilter
    from vllm.entrypoints.cli.serve import ServeSubcommand
    from vllm.utils.argparse_utils import FlexibleArgumentParser
    parser=FlexibleArgumentParser();ServeSubcommand().subparser_init(parser.add_subparsers(dest='command'))
    parsed=parser.parse_args(argv)
    launch=native_launch_report(cfg,parsed)
    if a.cpu_check:
        if os.environ.get('CUDA_VISIBLE_DEVICES')!='' or torch.cuda.is_initialized():
            raise ValueError('CPU check must not initialize CUDA')
        print(json.dumps(dict(status='native_hooks_imported_and_arguments_parsed_CPU_only',
            argv=argv,native_launch=launch,cuda_initialized=False)))
        return
    if not cfg['execution_enabled'] or cfg.get('CPU_fixture_only'):
        raise ValueError('GPU owner runtime config is not enabled')
    with (a.evidence/'native-launch.json').open('x') as stream:
        json.dump(launch,stream,indent=2)
    # Only environment and an existing-file import path cross the native process boundary.
    # No GPU runner import, forced fork/spawn, sitecustomize, or author source edit here.
    os.environ.update(KALYPSO_OBSERVER_CONFIG=str(a.config.resolve()),KALYPSO_OBSERVER_UNIT=a.unit,
        KALYPSO_OBSERVER_EVIDENCE=str(a.evidence.resolve()),KALYPSO_OBSERVER_API_PID=str(os.getpid()))
    sys.path.insert(0,str(Path(__file__).resolve().parent))
    counters=install_hooks(cfg,unit,a.evidence,VLLMExecutor,SemFilter)
    sys.argv=['vllm',*argv]
    try:
        runpy.run_module('vllm.entrypoints.cli.main',run_name='__main__')
    finally:
        (a.evidence/'native-call-summary.json').write_text(json.dumps(counters,indent=2)+'\n')


if __name__=='__main__':main()
