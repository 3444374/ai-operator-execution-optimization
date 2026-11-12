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
def methods(checkout,manager,*,planning=False,ranking=False):
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
        load('data/data.py')
        if planning:
            scope.update(ops=SimpleNamespace(CartesianProduct=scope['CartesianProduct'],SemFilter=scope['SemFilter']),
                data=SimpleNamespace(_data_source=scope['_data_source']),PlanExecutor=type('NoModelPlanExecutor',(),{}))
            if ranking:scope['ops'].SemTopK=scope['SemTopK']
            load('controller/stage.py');load('controller/plan.py')
        yield module
    finally:sys.modules.pop(name,None)
