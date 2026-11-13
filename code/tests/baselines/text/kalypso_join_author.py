"""Load pinned native method bodies with only explicit CPU dependencies."""
import ast
import asyncio
from contextlib import contextmanager
import csv
from dataclasses import dataclass,field,replace
from enum import Enum,auto
import os
import math
import random
from itertools import count
from pathlib import Path
import subprocess
import sys
from types import ModuleType,SimpleNamespace
from typing import Any,Dict,List
import uuid


@contextmanager
def methods(checkout,manager,*,planning=False,ranking=False,mapping=False,aggregation=False):
    name='_semloom_kalypso05024_'+uuid.uuid4().hex
    module=ModuleType(name);sys.modules[name]=module;scope=module.__dict__
    scope.update(asyncio=asyncio,csv=csv,os=os,math=math,random=random,count=count,Path=Path,dataclass=dataclass,field=field,replace=replace,
        Enum=Enum,auto=auto,Any=Any,Dict=Dict,List=List,LLMExecutor=object,Query=object,KVMemoryManager=type(
            'CPUManager',(),{'get_instance':staticmethod(lambda:manager)}))
    prefix='05024f027f932c0e398852e38302be30b9015481:vllm/kalypso/'
    def load(path,classes=None):
        source=subprocess.check_output(['git','-C',str(checkout),'show',prefix+path],
            env=dict(os.environ,GIT_NO_LAZY_FETCH='1'),text=True)
        tree=ast.parse(source)
        tree.body=[node for node in tree.body if not isinstance(node,(ast.Import,ast.ImportFrom))
            and (classes is None or isinstance(node,ast.ClassDef) and node.name in classes)]
        exec(compile(tree,'author05024/'+path,'exec'),scope)
    try:
        load('sem_ops/base.py');load('sem_ops/prompt_utils.py');load('context/context.py')
        load('sem_ops/joins.py',{'CartesianProduct'});load('sem_ops/filters.py',{'SemFilter'})
        if ranking:load('sem_ops/reducers.py',{'SemTopK'})
        if aggregation:
            load('execution/pipeline_execution.py',{'BlockingExecutor'})
            load('sem_ops/reducers.py',{'SemAgg'})
        if mapping:
            import numpy as np
            from collections import defaultdict
            scope.update(np=np,defaultdict=defaultdict)
            load('controller/map_estimator.py')
            load('sem_ops/map.py',{'SemMap'})
        load('data/data.py')
        if planning:
            scope.update(ops=SimpleNamespace(CartesianProduct=scope['CartesianProduct'],SemFilter=scope['SemFilter']),
                data=SimpleNamespace(_data_source=scope['_data_source']),PlanExecutor=type('NoModelPlanExecutor',(),{}))
            if ranking:scope['ops'].SemTopK=scope['SemTopK']
            if aggregation:scope['ops'].SemAgg=scope['SemAgg']
            if mapping:scope['ops'].SemMap=scope['SemMap']
            load('controller/stage.py');load('controller/plan.py')
        yield module
    finally:sys.modules.pop(name,None)


def empty_cpu_plan(checkout,cfg,output,*,kind):
    """Run the real CPU checker/planner with an empty source and no tokenizer calls."""
    from unittest.mock import patch
    from src.baselines.text.frameworks.kalypso_native import join_cpu,movie_map_cpu
    manager=SimpleNamespace(token_length=lambda value:len(str(value)),bytes_per_token=1,
        capacity=lambda:1024**3,reset_stages=lambda:None,register_stage=lambda *a,**k:None)
    with methods(checkout,manager,planning=True,mapping=kind=='map') as author:
        prompt=ModuleType('vllm.kalypso.sem_ops.prompt_utils');prompt.__dict__.update(vars(author))
        modules={'vllm.kalypso.sem_ops.prompt_utils':prompt}
        for name,symbol in [('base','OpName'),('filters','SemFilter'),('joins','CartesianProduct')]:
            module=ModuleType('vllm.kalypso.sem_ops.'+name);setattr(module,symbol,getattr(author,symbol))
            modules[module.__name__]=module
        if kind=='map':
            module=ModuleType('vllm.kalypso.sem_ops.map');module.SemMap=author.SemMap;modules[module.__name__]=module
            module=ModuleType('vllm.kalypso.controller.map_estimator');module.MapRatioEstimator=author.MapRatioEstimator
            modules[module.__name__]=module
            module=ModuleType('vllm.kalypso.context');module.RETRY_TASK=author.RETRY_TASK;modules[module.__name__]=module
        output.mkdir()
        with patch.dict(sys.modules,modules):
            args=(cfg,output,object(),manager,author.SemanticPlan,
                  SimpleNamespace(_data_source=author._data_source),32768)
            return join_cpu.check(*args) if kind=='join' else movie_map_cpu.check(*args,kind=kind)
