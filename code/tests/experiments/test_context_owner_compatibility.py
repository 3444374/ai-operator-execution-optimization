"""Reviewed owner-lifetime and finite IMLane regressions with CPU fixtures only."""

from copy import deepcopy
from dataclasses import asdict, replace
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.experiments.calibration.core import core_session_model_ready
from src.semantic_methods.lotus.cascade import CascadeStage, cascade_identity
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.model_context import model_context_record
from src.serving.backends.vllm_engine import EngineSpec
from tests.experiments.movie_token_resident_fixture import MovieTokenResidentFixture, query_calls
from tests.execution_provider.test_lotus_async_token_batches import fixture as chain_fixture


class TokenOwnerContextTests(unittest.TestCase):
    def context(self, fixture, plan, *, length=None):
        context=fixture.context(plan)
        observed={}
        if length is not None:
            engine=EngineSpec.from_dict(plan['engine']); uid='CPU-prior-confirmation'
            observed['model_ready']=dict(engine_uid=uid,
                model_context=model_context_record(engine,uid,length))
        context['plan_binding']=dict(kind='scan',observed=observed,
            **(dict(resolve_model_context=True) if length is None else {}))
        return context

    def run_lifecycle(self, lifecycle):
        fixture=MovieTokenResidentFixture(self)
        plan=fixture.plan('RF',lifecycle=lifecycle)
        plan['engine']['max_model_len']=None
        context=self.context(fixture,plan)
        ready_counts=[]
        def observe(event):
            if event['event']=='session_owner_ready':
                ready_counts.append(sum(e['event']=='construct' for e in fixture.events()))
        report=fixture.run(plan,context=context,observer=observe)
        self.assertEqual(ready_counts,[1] if lifecycle=='resident' else [0,1])
        actual=core_session_model_ready(plan,report)
        self.assertEqual(actual['model_context']['max_model_len'],32768)
        self.assertEqual([len(query_calls(q)) for q in report['queries']],[2,2])
        self.assertEqual(sum(e['event']=='construct' for e in fixture.events()),1 if lifecycle=='resident' else 2)
        self.assertEqual(sum(e['event']=='close' for e in fixture.events()),1 if lifecycle=='resident' else 2)
        self.assertEqual(len({c['engine_uid'] for q in report['queries'] for c in query_calls(q)}),
            1 if lifecycle=='resident' else 2)
        for owner in report['owners']:
            self.assertEqual(owner['status'],'closed')
            if lifecycle=='isolated':
                self.assertTrue(owner['ready']['no_session_model'])
                self.assertIn('query_model_ready',owner['ready'])
                self.assertEqual(owner['ready']['model_startup_scope'],'inside finite record-chain evaluate')
        return fixture,plan,report

    def test_resident_token_wrapper_uses_actual_ready_without_reading_private_actor_layout(self):
        self.run_lifecycle('resident')

    def test_isolated_token_wrapper_keeps_model_allocation_inside_query(self):
        self.run_lifecycle('isolated')

    def test_context_mismatch_stops_before_any_token_model_request(self):
        for lifecycle in ('resident','isolated'):
            with self.subTest(lifecycle=lifecycle):
                fixture=MovieTokenResidentFixture(self)
                plan=fixture.plan('RF',lifecycle=lifecycle);plan['engine']['max_model_len']=None
                with self.assertRaisesRegex(ValueError,'executing model context differs'):
                    fixture.run(plan,context=self.context(fixture,plan,length=8192))
                self.assertFalse(any(e['event']=='start' for e in fixture.events()))
                self.assertEqual(sum(e['event']=='construct' for e in fixture.events()),1)
                self.assertEqual(sum(e['event']=='close' for e in fixture.events()),1)

    def test_public_ready_view_allocates_nothing_and_precedes_first_model_submission(self):
        from src.baselines.text.movie_records import MovieRecordSource
        fixture=MovieTokenResidentFixture(self)
        owner=fixture.owner('RF',resident=False)
        source=MovieRecordSource.from_manifest(fixture.manifests['movie-q1'])
        try:
            self.assertIsNone(owner.model_ready)
            self.assertTrue(owner.start()['no_session_model'])
            self.assertIsNone(owner.model_ready)
            self.assertEqual(fixture.events(),[])
            stream=owner.evaluate(source,query_id='CPU-readiness-before-consumption')
            observed=owner.model_ready
            self.assertTrue(observed['engine_uid'])
            self.assertEqual(sum(e['event']=='construct' for e in fixture.events()),1)
            self.assertFalse(any(e['event']=='start' for e in fixture.events()))
            observed['engine_uid']='CPU-unrelated-copy'
            self.assertNotEqual(owner.model_ready['engine_uid'],observed['engine_uid'])
            self.assertEqual(len(list(stream)),2)
        finally:self.assertEqual(owner.close()['status'],'closed')

    def test_empty_isolated_query_is_not_mistaken_for_unknown_model_readiness(self):
        fixture=MovieTokenResidentFixture(self)
        fixture.manifests['movie-q3']=fixture.source_manifest('empty-calibration',[],task='movie-q3')
        plan=fixture.plan('RF',lifecycle='isolated',tasks=('movie-q1','movie-q3','movie-q1'))
        plan['engine']['max_model_len']=None
        report=fixture.run(plan,context=self.context(fixture,plan))
        self.assertEqual([len(query_calls(q)) for q in report['queries']],[2,0,2])
        self.assertEqual(sum(e['event']=='construct' for e in fixture.events()),2)
        self.assertNotIn('query_model_ready',report['owners'][1]['ready'])
        self.assertEqual(core_session_model_ready(plan,report)['model_context']['max_model_len'],32768)
        changed=deepcopy(report)
        changed['queries'][1]['backend_report']['calls']['counts']['unknown']=1
        with self.assertRaises(RuntimeError):core_session_model_ready(plan,changed)


class IMLaneFiniteContextTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.root=Path(temp.name)
        self.chain=chain_fixture.chain_for('M');self.rows=chain_fixture.source_rows(size=2)
        self.resources=RecordsResources(cpu_workers=1,prepare_batch_rows=1,model_batch_rows=1,max_active_batches=1)

    def engine(self, extra=None, *, path='model'):
        return EngineSpec(str(self.root/path),sys.executable,sys.executable,max_model_calls=4,**(extra or {}))

    def test_chain_configuration_omitted_and_null_match_explicit_legacy_values(self):
        from integrations.imlane_records import chain_host
        configs=[]
        for index,extra in enumerate(({},dict(max_model_len=None),dict(max_model_len=4096),dict(max_model_len=8192))):
            engine=self.engine(extra);control=self.root/str(index);control.mkdir()
            with patch.object(chain_host,'assigned_environment',return_value={}), \
                    patch.object(EngineSpec,'validate_driver_environment',return_value={}):
                config=chain_host.configuration(self.chain,engine,self.resources,self.rows,
                    control_dir=control,deadline=time.monotonic()+5,model_environment={})
            self.assertEqual(config['engine']['max_model_len'],extra.get('max_model_len') or 4096)
            self.assertEqual(engine.max_model_len,extra.get('max_model_len'))
            configs.append({k:v for k,v in config.items() if k not in
                ('control_dir','deadline_monotonic','control_token','driver_host')})
        self.assertEqual(configs[0],configs[1]);self.assertEqual(configs[0],configs[2])
        self.assertEqual(configs[3]['engine']['max_model_len'],8192)

    def test_chain_backend_retains_finite_context_and_rejects_explicit_output_overflow(self):
        from integrations.imlane_records.chain_backend import IMLaneChainBackend
        args=SimpleNamespace(output=self.root,stop_budget=SimpleNamespace(stopped=threading.Event()))
        for extra in ({},dict(max_model_len=None),dict(max_model_len=8192)):
            with self.subTest(extra=extra):
                backend=IMLaneChainBackend(engine=self.engine(extra),resources=self.resources,author_args=args,
                    model_environment={'CUDA_VISIBLE_DEVICES':'0'})
                self.assertEqual(backend.engine.max_model_len,extra.get('max_model_len') or 4096)
        backend=IMLaneChainBackend(engine=self.engine(dict(max_model_len=1)),resources=self.resources,
            author_args=args,model_environment={'CUDA_VISIBLE_DEVICES':'0'})
        with patch.object(backend,'_open_phase',side_effect=AssertionError('author allocated')), \
                self.assertRaisesRegex(ValueError,'finite model context'):
            backend.evaluate(iter(self.rows),self.chain,expected=self.rows)

    def stage(self, *, output=8):
        row=dict(cascade_identity('CPU-cascade','row-0',0,0,model_role='main',stage='fallback'),reviewText='text')
        return CascadeStage('fallback','main','openai/CPU-main','Keep {reviewText}',False,None,
            ('True','False'),dict(max_completion_tokens=output,temperature=0,top_p=1,logprobs=False),
            (row,),('reviewText',))

    def test_cascade_worker_normalizes_finite_context_before_generation_check(self):
        from integrations.imlane_records.cascade_udf import CascadeWorker
        for extra in ({},dict(max_model_len=None),dict(max_model_len=8192)):
            with self.subTest(extra=extra):
                config=dict(schema='imlane.cascade.stage.v1',stage=asdict(self.stage()),
                    engine=asdict(replace(self.engine(extra),max_model_calls=1)),
                    resources=asdict(self.resources),query_id='CPU-cascade',deadline_monotonic=time.monotonic()+5,
                    control_dir=str(self.root),model_environment={'CUDA_VISIBLE_DEVICES':'0'})
                worker=CascadeWorker(config,prepare_type=lambda _:object())
                self.assertEqual(worker.engine.max_model_len,extra.get('max_model_len') or 4096)
                self.assertIsNone(worker.kernel)
        config['engine']=asdict(replace(self.engine(dict(max_model_len=1)),max_model_calls=1))
        with self.assertRaisesRegex(ValueError,'finite bound role'):
            CascadeWorker(config,prepare_type=lambda _:object())

    def test_cascade_backend_roles_keep_legacy_context_without_loading_models(self):
        from integrations.imlane_records.cascade_host import IMLaneCascadeBackend
        args=SimpleNamespace(output=self.root,stop_budget=SimpleNamespace(check=lambda:None))
        source=SimpleNamespace()
        for extra in ({},dict(max_model_len=None),dict(max_model_len=8192)):
            with self.subTest(extra=extra):
                backend=IMLaneCascadeBackend(source,engines=dict(main=self.engine(extra,path='main'),
                    helper=self.engine(extra,path='helper')),resources=self.resources,author_args=args,
                    model_environment={'CUDA_VISIBLE_DEVICES':'0'})
                self.assertEqual({e.max_model_len for e in backend.engines.values()},{extra.get('max_model_len') or 4096})
                self.assertFalse(backend._checked)


if __name__=='__main__':unittest.main()
