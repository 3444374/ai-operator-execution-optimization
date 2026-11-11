"""Q9 task binding, event identity and finite native retry contracts; no model."""
import asyncio
import copy
import json
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace as NS
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import adapter, movie_map, native_server
from src.experiments.kalypso_query_lifecycle import query_driver_type


def unit():
    return dict(call_cap=1024,queries=[dict(query_id=q,task='movie-q9',phase='measured',
        expected_calls=256,max_native_calls=512) for q in ('cold','warm')])


def trace(count=2,retry=False):
    events=[];sequence=0
    for idx in range(count):
        for attempt in range(2 if retry and idx==0 else 1):
            sequence+=1
            fields=dict(query_id='q',sequence=sequence,idx=idx,model='m',model_uid='u',
                max_tokens=8192 if attempt else 50,pin=False,priority=0,messages_sha256='digest')
            events.extend([dict(fields,event='native_API_start'),dict(fields,event='native_API_end',
                status='succeeded',request_id='r'+str(sequence),finish_reason='length' if retry and idx==0 else 'stop'),
                dict(event='native_sampling',query_id='q',sequence=sequence,idx=idx,model='m',
                     sampling=dict(max_tokens=fields['max_tokens'],temperature=0,top_p=1,seed=42,
                                   frequency_penalty=.5,repetition_penalty=1.3,stop=[]))])
        events.append(dict(event='map_result',query_id='q',idx=idx,text=str(idx)))
    response=dict(model_name='m',num_output_rows=count,
                  results=[dict(idx=i,outputs=[dict(value=str(i))]) for i in reversed(range(count))])
    return response,events


class MovieMapContracts(unittest.TestCase):
    def test_standard_query_selection_is_Map_and_does_not_accept_TopK_or_other_tasks(self):
        self.assertIs(query_driver_type(unit()),movie_map.MapDriver)
        self.assertIs(query_driver_type(dict(queries=[dict(task='movie-q4')])),adapter.NativeQueryDriver)
        for task in ('movie-q9-topk','movie-q10','medical-q4','fixture'):
            with self.assertRaisesRegex(ValueError,'not TopK'):
                query_driver_type(dict(queries=[dict(task=task)]))
        with self.assertRaises(ValueError):query_driver_type(dict(queries=[]))
        mixed=unit();mixed['queries'][1]['task']='movie-q1'
        with self.assertRaises(ValueError):query_driver_type(mixed)

    def test_logical_and_physical_budgets_stay_distinct(self):
        original=unit();before=copy.deepcopy(original)
        self.assertEqual(native_server.physical_call_caps(original),dict(cold=512,warm=512))
        self.assertEqual(original,before)
        for key,value in (('expected_calls',255),('max_native_calls',256),('max_native_calls',513)):
            changed=unit();changed['queries'][0][key]=value
            with self.assertRaises(ValueError):native_server.physical_call_caps(changed)
        with self.assertRaises(ValueError):native_server.physical_call_caps(dict(unit(),call_cap=float('inf')))

    def test_original_length_retry_at_8192_is_final_even_if_still_length(self):
        response,events=trace(retry=True)
        texts,ends=movie_map.reconcile(response,events,'q',2)
        self.assertEqual(texts,['0','1']);self.assertEqual(len(ends),3)
        self.assertEqual(ends[1]['finish_reason'],'length');self.assertEqual(ends[1]['max_tokens'],8192)

    def test_duplicate_missing_foreign_or_invalid_event_identity_is_rejected(self):
        mutations=[lambda e:e.append(copy.deepcopy(e[1])),
            lambda e:e[1].update(request_id='r2'),lambda e:e[1].update(idx=True),
            lambda e:e[1].update(sequence=999),lambda e:e[1].update(model_uid='other'),
            lambda e:e[2]['sampling'].update(seed=1),lambda e:e[1].update(status='unknown'),
            lambda e:e[3].update(idx=1),lambda e:e.pop(0)]
        for change in mutations:
            response,events=trace();change(events)
            with self.subTest(change=change),self.assertRaises(ValueError):
                movie_map.reconcile(response,events,'q',2)

    def test_retry_requires_native_length_branch_and_cannot_be_silently_disabled(self):
        response,events=trace()
        events[1]['finish_reason']='length'
        with self.assertRaisesRegex(ValueError,'retry'):
            movie_map.reconcile(response,events,'q',2)
        response,events=trace(retry=True);events[1]['finish_reason']='stop'
        with self.assertRaisesRegex(ValueError,'retry'):
            movie_map.reconcile(response,events,'q',2)

    def test_physical_budget_stops_before_native_API_and_keeps_query_declaration(self):
        from src.baselines.text.frameworks.kalypso_native import map_observation
        module=ModuleType('vllm.kalypso.sem_ops.map');module.SemMap=object
        estimator=ModuleType('vllm.kalypso.controller.map_estimator');estimator.MapRatioEstimator=object
        modules={module.__name__:module,estimator.__name__:estimator}
        with tempfile.TemporaryDirectory() as raw,patch.dict('sys.modules',modules), \
                patch.object(map_observation,'install',return_value={}), \
                patch.object(map_observation,'install_response_observation'):
            root=Path(raw);(root/'loaded-model.json').write_text(json.dumps(dict(model_uid='u')))
            class Executor:
                model='m';calls=0
                async def chatcomplete(self,*args,**kwargs):
                    self.calls+=1
                    return NS(request_id=str(self.calls),finish_reason='length')
            class Filter:
                async def _run_single(self,*args,**kwargs):return True
            bound=unit();before=copy.deepcopy(bound)
            counts=native_server.install_hooks({},bound,root,Executor,Filter);executor=Executor()
            async def calls():
                for qid in ('cold','warm'):
                    raw=NS(headers={'X-Kalypso-Observation':qid})
                    for _ in range(512):await executor.chatcomplete(raw,[],8192)
                    with self.assertRaisesRegex(RuntimeError,'budget'):
                        await executor.chatcomplete(raw,[],8192)
                with self.assertRaises(RuntimeError):
                    await executor.chatcomplete(NS(headers={'X-Kalypso-Observation':'foreign'}),[],8192)
            asyncio.run(calls())
            self.assertEqual(executor.calls,1024);self.assertEqual(counts['reserved_calls'],1024)
            self.assertEqual(counts['query_calls'],dict(cold=512,warm=512));self.assertEqual(bound,before)

    def test_CPU_Map_preflight_binds_headers_before_plan_and_checks_retry_context(self):
        from src.baselines.text.frameworks.kalypso_native import movie_map_cpu
        class Map:
            pin=False;unpin=False
            def _build_prompt(self,ctx):return [],[{'role':'user','content':'full prompt'}]
            def _planned_max_tokens(self,ctx):return 75
        module=ModuleType('vllm.kalypso.sem_ops.map');module.SemMap=Map
        estimator=ModuleType('vllm.kalypso.controller.map_estimator');estimator.MapRatioEstimator=object
        class Plan:
            def __init__(self,*a,**kw):pass
            def parse_ops(self,ops):
                self_test.assertEqual(ops,[dict(op='sem_map',args=dict(prompt='original instruction'))])
                return [Map()]
            def build(self,contexts,ops):
                self_test.assertEqual(len(contexts),256)
                self_test.assertTrue(all(ctx.state.raw_request.headers['X-Kalypso-Observation'] in ('cold','warm') for ctx in contexts))
                return [ops]
        self_test=self
        tokenizer=NS(apply_chat_template=lambda *a,**kw:'complete',encode=lambda *a,**kw:list(range(30)))
        manager=NS(reset_stages=lambda:None,tokenizer=tokenizer)
        executor=NS(_build_chat_request=lambda prompt,cap,pin,priority:NS(messages=prompt,max_tokens=cap,
            add_generation_prompt=True,add_special_tokens=False))
        data=NS(_data_source=lambda *a:[NS(state=NS(idx=i)) for i in range(256)])
        inputs=[dict(spec=spec,table=dict(instruction='original instruction',
            rows=[dict(row_id=str(i),source_position=i,source_index=i,reviewId='duplicate') for i in range(256)]),
            paths=dict(csv=Path('fixture.csv'),prompt=Path('prompts')/(spec['query_id']+'.json')))
            for spec in unit()['queries']]
        cfg=dict(units=[unit()],native_cpu_inputs=dict(sha256='CPU-fixture'))
        with patch.dict('sys.modules',{module.__name__:module,estimator.__name__:estimator}), \
                patch.object(movie_map_cpu,'install'), \
                patch.object(movie_map_cpu,'load_inputs',return_value=(dict(binding={}),inputs)):
            for limit in (8222,8221):
                with tempfile.TemporaryDirectory() as raw:
                    if limit==8221:
                        with self.assertRaisesRegex(ValueError,'retry cap'):
                            movie_map_cpu.check(cfg,Path(raw),executor,manager,Plan,data,limit)
                    else:
                        result=movie_map_cpu.check(cfg,Path(raw),executor,manager,Plan,data,limit)
                        self.assertEqual([p['logical_occurrences'] for p in result['plans']],[256,256])
                        self.assertTrue((Path(raw)/'cold-native-prompts.json').is_file())

    def test_response_observer_does_not_replace_original_outcome_on_setup_or_write_failure(self):
        from src.baselines.text.frameworks.kalypso_native import map_observation
        original_response=NS(body=b'{"id":"r"}')
        primary=LookupError('original native failure')
        for failure in (None,primary):
            async def original(**kwargs):
                if failure is not None:raise failure
                return original_response
            api=ModuleType('vllm.entrypoints.openai.api_server');api.create_chat_completion=original
            package=ModuleType('vllm.entrypoints.openai');package.api_server=api
            with tempfile.TemporaryDirectory() as raw,patch.dict('sys.modules',
                    {'vllm.entrypoints.openai':package,'vllm.entrypoints.openai.api_server':api}), \
                    patch.object(Path,'open',side_effect=OSError('observer write failed')):
                errors=map_observation.install_response_observation(native_server,raw)
                request=NS(app=NS(state=NS(engine_client=object())))
                if failure is None:
                    self.assertIs(asyncio.run(api.create_chat_completion(raw_request=request)),original_response)
                else:
                    with self.assertRaises(LookupError) as caught:
                        asyncio.run(api.create_chat_completion(raw_request=request))
                    self.assertIs(caught.exception,primary)
                self.assertIn('AttributeError',errors);self.assertIn('OSError',errors)

    def test_missing_or_changed_full_source_fails_before_original_query(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw);(root/'Reviews.csv').write_text('changed')
            for hashes in ({},{'Reviews.csv':'wrong'}):
                with self.assertRaises(ValueError):
                    movie_map.make_query(dict(data_dir=raw,source_data_sha256=hashes),'q')


if __name__=='__main__':unittest.main()
