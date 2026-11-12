"""Observe model construction across two actual Daft AI materializations.

Only vLLM is a software dependency. Daft's expression, prefix operator, native
executor, submit/poll and DataFrame collect remain real. This is a lifecycle
diagnostic, not a TopK executor or model/performance qualification.
"""
import argparse
import asyncio
import importlib.abc
import importlib.metadata
import inspect
import json
import os
import platform
from pathlib import Path
import sys
from types import ModuleType,SimpleNamespace
from unittest.mock import patch

from src.infrastructure.private_artifacts import file_identity,new_private_directory,write_private_json
from src.infrastructure.redact import redact_text


class NoModels(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,*unused):
        if fullname.split('.')[0] in ('vllm','torch','transformers'):
            raise AssertionError('lifecycle diagnostic forbids real model imports: '+fullname)


def probe(output):
    output=Path(output);new_private_directory(output)
    if os.environ.get('CUDA_VISIBLE_DEVICES')!='':
        raise ValueError('zero-model lifecycle diagnostic requires CUDA_VISIBLE_DEVICES empty')
    if importlib.metadata.version('daft')!='0.7.21':
        raise ValueError('lifecycle evidence requires the fixed Daft0.7.21 SDK')
    models=[];executors=[];rounds=[];failure=None
    report=dict(status='failed',models=0,GPU=False,software_generate=0,rounds=rounds,
        SDK='Daft0.7.21',platform=sys.platform,machine=platform.machine(),
        native_Daft_graph_replaced=False,source_of_requests='two distinct finite two-row CPU fixtures',
        resource_profile=dict(gpus_per_actor=0,concurrency=1),
        interpretation='construction counts describe this actual SDK graph under software vLLM; not runtime reuse or timing of real models')
    fake=ModuleType('vllm')
    class Engine:
        def __init__(self):self.index=len(models);self.calls=[];self.closed=False;models.append(self)
        async def generate(self,prompt,sampling,request_id,**kwargs):
            self.calls.append(dict(prompt=prompt,request_id=request_id))
            yield SimpleNamespace(outputs=[SimpleNamespace(text='software-'+str(self.index)+'-'+str(request_id))])
        async def do_log_stats(self):pass
        def shutdown(self,*args,**kwargs):self.closed=True
    class Factory:
        @classmethod
        def from_engine_args(cls,*args,**kwargs):return Engine()
    fake.AsyncEngineArgs=lambda **kwargs:SimpleNamespace(**kwargs)
    fake.AsyncLLMEngine=Factory
    fake.SamplingParams=lambda **kwargs:SimpleNamespace(**kwargs)
    guard=NoModels();sys.meta_path.insert(0,guard)
    try:
        import daft
        from daft import daft as extension
        from daft.functions.ai import prompt
        from daft.execution.vllm import LocalVLLMExecutor
        report['native_sources']={name:file_identity(inspect.getfile(value)) for name,value in
            [('Daft',daft),('Daft_native_extension',extension),('LocalVLLMExecutor',LocalVLLMExecutor)]}
        original=LocalVLLMExecutor.__init__
        def constructed(current,*args,**kwargs):
            original(current,*args,**kwargs);executors.append(current)
        with patch.dict(sys.modules,{'vllm':fake}),patch.object(LocalVLLMExecutor,'__init__',constructed):
            expression=prompt(daft.col('prompt'),provider='vllm-prefix-caching',model='software-no-weights',
                gpus_per_actor=0,concurrency=1,
                generate_args=dict(sampling_params=fake.SamplingParams(max_tokens=1,temperature=0)))
            for index in range(2):
                ids=[str(index)+'-'+str(i) for i in range(2)]
                source=daft.from_pydict(dict(call_id=ids,prompt=['fixture-'+value for value in ids]))
                native=source.select('call_id',expression.alias('answer'))
                result=native.collect().to_pydict()
                if sorted(result['call_id'])!=ids or any(not text.startswith('software-') for text in result['answer']):
                    raise ValueError('native expression did not return the complete software-generated relation')
                rounds.append(dict(round=index,rows=2,result=result,
                    cumulative_model_constructors=len(models),cumulative_native_executors=len(executors),
                    cumulative_software_generate=sum(len(m.calls) for m in models)))
        if not executors or sum(len(m.calls) for m in models)!=4:
            raise ValueError('the actual native graph did not execute all four software calls')
        report.update(status='passed',native_executor_constructors=len(executors),
            software_model_constructors=len(models),software_generate=4,
            constructor_delta_second_round=rounds[1]['cumulative_model_constructors']-rounds[0]['cumulative_model_constructors'])
    except BaseException as error:
        failure=error;report.update(error_type=type(error).__name__,error=redact_text(str(error)))
    finally:
        sys.meta_path.remove(guard)
        cleanup=[]
        for executor in executors:
            try:
                executor.llm.shutdown()
                executor.loop.call_soon_threadsafe(executor.loop.stop)
                executor.loop_thread.join(timeout=5)
                if executor.loop_thread.is_alive():raise RuntimeError('software executor loop did not stop')
                pending=list(asyncio.all_tasks(executor.loop))
                for task in pending:task.cancel()
                if pending:executor.loop.run_until_complete(asyncio.gather(*pending,return_exceptions=True))
                executor.loop.close()
                cleanup.append(dict(loop_closed=executor.loop.is_closed(),thread_stopped=True))
            except BaseException as error:
                if failure is None:failure=error
                report.update(status='failed',cleanup_error=redact_text(str(error)))
        report['cleanup']=cleanup
        report['all_software_models_closed']=all(model.closed for model in models)
        write_private_json(output/'LIFECYCLE.json',report)
    if failure is not None:raise failure
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();print(json.dumps(probe(args.output)))
