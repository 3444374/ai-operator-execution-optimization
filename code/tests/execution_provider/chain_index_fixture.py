"""Load the actual frozen CPU method and replace only its model/publisher boundaries."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import sys
import time
from unittest.mock import patch

from types import SimpleNamespace
from src.execution_provider.method_workers import lotus_chain_batch as implementation
from src.serving.backends import vllm_sync_process as transport
from src.serving.backends.vllm_engine import IDENTITY_FIELDS

_ORIGINAL_INDEX = implementation._associate_declared_results

def _reference_associate(declared, results):
    for declaration in declared:
        matches = [row for row in results if type(row) is dict and all(
            row.get(key) == declaration[key] for key in IDENTITY_FIELDS)]
        if len(matches) == 1:
            parents = declaration["parent_call_ids"]
            declaration.clear()
            declaration.update(deepcopy(matches[0]), parent_call_ids=parents)
    raw = [row for row in results if type(row) is not dict or sum(
        type(other) is dict and all(other.get(key) == row.get(key) for key in IDENTITY_FIELDS)
        for other in results) != 1 or not any(
            all(row.get(key) == item[key] for key in IDENTITY_FIELDS) for item in declared)]
    return raw


def load(which):
    return SimpleNamespace(FusedChainBatch=implementation.FusedChainBatch,
                           __file__=implementation.__file__, indexed=which == 'candidate')


def fixtures(n, text_bytes=64, stage='s0'):
    prepared=[];results=[]
    for i in range(n):
        identity=dict(query_id='fixture-query',model_role='main',algorithm_stage=stage,
            row_id='row-'+str(i),source_position=i,call_index=0,call_id=stage+':call:'+str(i))
        prepared.append(dict(identity,messages_json='[{"role":"user","content":"fixture"}]'))
        results.append(dict(identity,status='succeeded',submission_confirmed=True,
            prompt_tokens=12,completion_tokens=4,total_tokens=16,
            text='text-'+str(i)+'-'+'x'*text_bytes,engine_uid='CPU-FAKE',engine_request_id=stage+':'+str(i),
            extra={'mutable':[i,{'child':[1,2,3]}]}))
    return prepared,results


class FakeEngine:
    def __init__(self,results,error=None):
        self.results,self.error,self.invocations=results,error,0
    def run_prepared(self,prepared,*,generation):
        self.invocations+=1
        if self.error:raise self.error
        return self.results


def worker(module,control,results,*,parents=None,error=None):
    obj=module.FusedChainBatch.__new__(module.FusedChainBatch)
    obj.control=Path(control);obj.deadline=time.monotonic()+60
    obj._audit_calls=[];obj._audit_errors=[];obj._batch_id='CPU-BATCH'
    obj._phase_stage_ids=['s0','s1','s2'];obj._audit_path=obj.control/'audit.json'
    obj.initial_parents=parents or {};obj.engine=FakeEngine(results,error)
    return obj


class CapturePublisher:
    def __init__(self,fail_at=None):self.snapshots=[];self.fail_at=fail_at;self.calls=0
    def __call__(self,path,value):
        self.calls+=1
        if self.calls==self.fail_at:raise OSError('CPU publisher failure')
        self.snapshots.append(deepcopy(value))


def exercise(module,prepared,results,control,*,parents=None,error=None,fail_at=None,profile=False):
    if parents is None:parents={(r['row_id'],r['source_position']):() for r in prepared}
    obj=worker(module,control,results,parents=parents,error=error)
    pub=CapturePublisher(fail_at)
    count=0
    def profiler(frame,event,arg):
        nonlocal count
        if (event=='c_call' and frame.f_code.co_filename in (module.__file__, __file__)
                and getattr(arg,'__name__',None)=='get' and isinstance(getattr(arg,'__self__',None),dict)):
            count+=1
    thrown=None;returned=None
    with patch.object(transport,'_publish',pub), patch.object(
            implementation, '_associate_declared_results',
            _ORIGINAL_INDEX if module.indexed else _reference_associate):
        previous=sys.getprofile()
        if profile:sys.setprofile(profiler)
        try:returned=obj._infer(prepared,generation='CPU-GENERATION')
        except BaseException as e:thrown=(type(e).__name__,str(e))
        finally:
            if profile:sys.setprofile(previous)
    return dict(error=thrown,returned=returned,audit=obj._audit_calls,snapshots=pub.snapshots,
        audit_errors=obj._audit_errors,model_invocations=obj.engine.invocations,dict_get_calls=count)


def no_runtime_loaded():
    return {name:name in sys.modules for name in ['torch','vllm','ray','daft','lotus']}
