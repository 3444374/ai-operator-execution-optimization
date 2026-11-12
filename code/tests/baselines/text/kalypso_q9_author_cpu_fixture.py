"""Test-only isolated interpreter: actual author CPU CLI with explicit native stubs."""
import argparse
import csv
import hashlib
import importlib.abc
import json
from pathlib import Path
import runpy
import sys
from types import ModuleType, SimpleNamespace as NS
from unittest.mock import patch

parser=argparse.ArgumentParser()
parser.add_argument('--repo',type=Path,required=True)
parser.add_argument('--config',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
assert sys.flags.isolated and sys.flags.no_site
assert not any('site-packages' in path for path in sys.path)
assert __import__('importlib.machinery',fromlist=['PathFinder']).PathFinder.find_spec('pandas') is None
sys.path.insert(0,str(args.repo/'code'))
blocked=[]
class NoDriverDependencies(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,*unused):
        if fullname.split('.')[0] in ('pandas','lotus','duckdb') or fullname.startswith('src.baselines.text.sembench'):
            blocked.append(fullname)
            raise AssertionError('author CPU step imported driver dependency: '+fullname)
sys.meta_path.insert(0,NoDriverDependencies())
from src.baselines.text.frameworks.kalypso_native import cpu_native
modules={}
def module(name,**values):
    m=ModuleType(name);m.__path__=[];m.__dict__.update(values);modules[name]=m;return m
for name in ('vllm','vllm.kalypso','vllm.kalypso.execution','vllm.kalypso.sem_ops','vllm.entrypoints',
             'vllm.entrypoints.cli','vllm.utils','vllm.engine','vllm.kalypso.controller'):
    module(name)
class Tokenizer:
    chat_template='CPU-isolated-native-stub-template'
    def apply_chat_template(self,prompt,**kwargs):return json.dumps(prompt,ensure_ascii=False)
    def encode(self,text,**kwargs):return list(text.encode())
class Manager:
    tokenizer=Tokenizer()
    @classmethod
    def init(cls,*a):pass
    @classmethod
    def get_instance(cls):return cls()
    def reset_stages(self):pass
    def apply_chat_template(self,prompt):return self.tokenizer.apply_chat_template(prompt)
    def token_length(self,text):return len(text.encode())
class Map:
    pin=False;unpin=False;position=0
    def __init__(self,instruction):self.instruction=instruction
    def _build_prompt(self,ctx):return ctx.input.data,ctx.input.data
    def _planned_max_tokens(self,ctx):return 75
    async def _run_single(self,*a,**kw):raise AssertionError('CPU planner executed a model task')
class Filter:
    pin=False;unpin=False;max_tokens=8
    def __init__(self,instruction):self.instruction=instruction
    def _build_prompts(self,ctx):return ctx.input.data,ctx.input.data
class Estimator:
    def reset(self):pass
class Plan:
    def __init__(self,*a,**kw):pass
    def parse_ops(self,ops):
        cls=Filter if ops[0]['op']=='sem_filter' else Map
        return [cls(ops[0]['args']['prompt'])]
    def build(self,contexts,ops):
        assert all(ctx.state.raw_request.headers['X-Kalypso-Observation'] for ctx in contexts)
        return [ops]
class Executor:
    def __init__(self,model):self.model=model
    def _build_chat_request(self,prompt,cap,pin,priority):
        return NS(model=self.model,messages=prompt,max_tokens=cap,add_generation_prompt=True,add_special_tokens=False)
class Serve:
    def subparser_init(self,subparsers):
        p=subparsers.add_parser('serve');p.add_argument('model_tag')
        for flag in ('host','port','worker-extension-cls'):p.add_argument('--'+flag)
class Args:
    @classmethod
    def from_cli_args(cls,parsed):return cls()
    def create_model_config(self):return NS(max_model_len=100000,dtype='CPU-stub')
def get_data_prompt(text):return [dict(role='user',content=text)]
def get_prompt(instruction,data,op):return data+[dict(role='user',content=instruction)]
def data_source(raw,path,executor):
    with Path(path).open() as stream:
        return [NS(state=NS(idx=i),input=NS(data=get_data_prompt(row['data'].strip()),right_data=[]))
                for i,row in enumerate(csv.DictReader(stream))]
module('torch',cuda=NS(is_initialized=lambda:False))
modules['vllm'].__version__='explicit-isolated-CPU-stub'
module('vllm.kalypso.budget',KVMemoryManager=Manager)
module('vllm.kalypso.execution.vllm_executor',VLLMExecutor=Executor)
module('vllm.kalypso.controller',SemanticPlan=Plan)
module('vllm.kalypso.controller.map_estimator',MapRatioEstimator=Estimator)
module('vllm.kalypso.sem_ops.map',SemMap=Map)
module('vllm.kalypso.sem_ops.filters',SemFilter=Filter)
module('vllm.kalypso.sem_ops.base',OpName=NS(SEM_MAP='sem_map',SEM_FILTER='sem_filter'))
module('vllm.kalypso.sem_ops.prompt_utils',get_data_prompt=get_data_prompt,get_prompt=get_prompt)
module('vllm.kalypso.context',RETRY_TASK=object())
module('vllm.kalypso.data',data=NS(_data_source=data_source))
module('vllm.entrypoints.cli.serve',ServeSubcommand=Serve)
module('vllm.engine.arg_utils',AsyncEngineArgs=Args)
module('vllm.utils.argparse_utils',FlexibleArgumentParser=argparse.ArgumentParser)
argv=['check_kalypso_native_cpu.py','--config',str(args.config),'--output',str(args.output)]
# Only the existing native environment is substituted. Handoff validation,
# maintained source checks, CSV/prompt loading and actual CPU CLI remain real.
with patch.dict(sys.modules,modules),patch.object(cpu_native,'check_author_source') as author_source,patch.object(sys,'argv',argv):
    runpy.run_path(str(args.repo/'code/scripts/experiments/check_kalypso_native_cpu.py'),run_name='__main__')
    assert author_source.call_count==1
assert not blocked
assert not any(name.split('.')[0] in ('pandas','lotus','duckdb') for name in sys.modules)
(args.output/'ISOLATED-INTERPRETER.json').write_text(json.dumps(dict(status='passed',isolated=True,no_site=True,
    driver_dependency_imports=blocked,pandas_available=False,sys_path=list(sys.path),real_model_calls=0,native_runtime='explicit stub; no author runtime qualification',
    actual_formal_CPU_CLI=True,actual_frozen_input_validation=True,actual_maintained_source_checks=True),indent=2)+'\n')
