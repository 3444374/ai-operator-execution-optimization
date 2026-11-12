"""Movie Map binding, event identity and finite native retry contracts; no model."""
import asyncio
import copy
import json
import os
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace as NS
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.kalypso_native import adapter, movie_map, native_server, sembench_inputs
from src.experiments.kalypso_query_lifecycle import query_driver_type


def unit(count=256, task='movie-q9'):
    return dict(call_cap=4*count,queries=[dict(query_id=q,task=task,phase='measured',
        expected_calls=count,max_native_calls=2*count) for q in ('cold','warm')])


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
    def test_movie_map_aliases_keep_map_and_other_tasks_need_explicit_bindings(self):
        for task in native_server.MOVIE_MAP_QUERIES:
            self.assertIs(query_driver_type(unit(task=task)),movie_map.MapDriver)
        self.assertIs(query_driver_type(dict(queries=[dict(task='movie-q4')])),adapter.NativeQueryDriver)
        for task in ('movie-q9-topk','movie-q7','medical-q4','fixture'):
            with self.assertRaisesRegex(ValueError,'supports'):
                query_driver_type(dict(queries=[dict(task=task)]))
        with self.assertRaises(ValueError):query_driver_type(dict(queries=[]))
        mixed=unit();mixed['queries'][1]['task']='movie-q1'
        with self.assertRaises(ValueError):query_driver_type(mixed)

    def test_logical_and_physical_budgets_stay_distinct(self):
        for task in native_server.MOVIE_MAP_QUERIES:
            for count in (1,7,13,256):
                original=unit(count,task);before=copy.deepcopy(original)
                self.assertEqual(native_server.physical_call_caps(original),dict(cold=2*count,warm=2*count))
                self.assertEqual(original,before)
        for key,value in (('expected_calls',255),('max_native_calls',256),('max_native_calls',513)):
            changed=unit();changed['queries'][0][key]=value
            with self.assertRaises(ValueError):native_server.physical_call_caps(changed)
        with self.assertRaises(ValueError):native_server.physical_call_caps(dict(unit(),call_cap=float('inf')))
        for count in (0,-1,True,7.0,float('inf')):
            with self.subTest(count=count),self.assertRaises(ValueError):
                native_server.physical_call_caps(unit(count))
        filtered=dict(call_cap=7,queries=[dict(query_id='q',task='movie-q4',expected_calls=7)])
        self.assertEqual(native_server.physical_call_caps(filtered),dict(q=7))
        filtered['queries'][0]['max_native_calls']=14
        with self.assertRaises(ValueError):native_server.physical_call_caps(filtered)

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
            bound=unit(7);before=copy.deepcopy(bound)
            counts=native_server.install_hooks({},bound,root,Executor,Filter);executor=Executor()
            async def calls():
                for qid in ('cold','warm'):
                    raw=NS(headers={'X-Kalypso-Observation':qid})
                    for _ in range(14):await executor.chatcomplete(raw,[],8192)
                    with self.assertRaisesRegex(RuntimeError,'budget'):
                        await executor.chatcomplete(raw,[],8192)
                with self.assertRaises(RuntimeError):
                    await executor.chatcomplete(NS(headers={'X-Kalypso-Observation':'foreign'}),[],8192)
            asyncio.run(calls())
            self.assertEqual(executor.calls,28);self.assertEqual(counts['reserved_calls'],28)
            self.assertEqual(counts['query_calls'],dict(cold=14,warm=14));self.assertEqual(bound,before)

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
                self_test.assertEqual(len(contexts),count)
                self_test.assertTrue(all(ctx.state.raw_request.headers['X-Kalypso-Observation'] in ('cold','warm') for ctx in contexts))
                return [ops]
        self_test=self
        tokenizer=NS(apply_chat_template=lambda *a,**kw:'complete',encode=lambda *a,**kw:list(range(30)))
        manager=NS(reset_stages=lambda:None,tokenizer=tokenizer)
        executor=NS(_build_chat_request=lambda prompt,cap,pin,priority:NS(messages=prompt,max_tokens=cap,
            add_generation_prompt=True,add_special_tokens=False))
        for task,count in [(task,count) for task in native_server.MOVIE_MAP_QUERIES for count in (1,7,13)]:
            data=NS(_data_source=lambda *a:[NS(state=NS(idx=i)) for i in range(count)])
            inputs=[dict(spec=spec,table=dict(instruction='original instruction',
                rows=[dict(row_id=str(i),source_position=i,source_index=i,reviewId='duplicate') for i in range(count)]),
                paths=dict(csv=Path('fixture.csv'),prompt=Path('prompts')/(spec['query_id']+'.json')))
                for spec in unit(count,task)['queries']]
            cfg=dict(units=[unit(count,task)],native_cpu_inputs=dict(sha256='CPU-fixture'))
            with patch.dict('sys.modules',{module.__name__:module,estimator.__name__:estimator}), \
                    patch.object(movie_map_cpu,'install'), \
                    patch.object(movie_map_cpu,'load_inputs',return_value=(dict(binding={}),inputs)):
                for limit in (8222,8221):
                    with self.subTest(task=task,count=count,limit=limit),tempfile.TemporaryDirectory() as raw:
                        if limit==8221:
                            with self.assertRaisesRegex(ValueError,'retry cap'):
                                movie_map_cpu.check(cfg,Path(raw),executor,manager,Plan,data,limit)
                        else:
                            result=movie_map_cpu.check(cfg,Path(raw),executor,manager,Plan,data,limit)
                            self.assertEqual([p['logical_occurrences'] for p in result['plans']],[count,count])
                            self.assertEqual([p['physical_call_cap'] for p in result['plans']],[2*count,2*count])
                            saved=json.loads((Path(raw)/'cold-native-prompts.json').read_text())
                            self.assertEqual([row['source_position'] for row in saved],list(range(count)))
                            self.assertTrue(all(row['token_ids']==list(range(30)) for row in saved))

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

    def test_q10_requires_frozen_movie_labels_before_original_query(self):
        with patch.object(sembench_inputs,'SemBenchTextQuery') as query:
            with self.assertRaisesRegex(ValueError,'evaluation file hashes'):
                movie_map.make_query(dict(source_data_sha256={'Reviews.csv':'hash'}),'q','movie-q10')
            with self.assertRaisesRegex(ValueError,'supported original text'):
                movie_map.make_query({},'q','movie-q9-topk')
        query.assert_not_called()


@unittest.skipUnless(os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT'),
    'requires the pinned public SemBench checkout and existing driver dependencies')
class OriginalMapSizeContracts(unittest.TestCase):
    def test_small_sources_keep_original_parser_relation_evaluator_and_memory_sink(self):
        """Real original query/parser/evaluator; only native HTTP/clock are fixtures."""
        import pandas as pd
        from lotus.sem_ops import postprocessors
        from src.baselines.text.frameworks.kalypso_native import movie_map_inputs
        from tests.baselines.text.test_kalypso_memory_sink import ControlledClock
        from tests.experiments.test_sembench_text_original import OriginalSemBenchTextTests

        helper=OriginalSemBenchTextTests();helper.setUp();self.addCleanup(helper.doCleanups)
        seed=pd.read_csv(helper.data/'Reviews.csv')
        for task,count in [(task,count) for task in native_server.MOVIE_MAP_QUERIES for count in (1,7,13)]:
            with self.subTest(task=task,count=count):
                # Explicit software input: include an ineligible row and repeated
                # business IDs while keeping every selected occurrence distinct.
                movie={'movie-q8':'taken_3','movie-q9':'ant_man_and_the_wasp_quantumania'}.get(task)
                selected=seed if movie is None else seed[seed['id']==movie]
                excluded=seed.iloc[:0] if movie is None else seed[seed['id']!=movie].head(1)
                sample=selected.iloc[[i%len(selected) for i in range(count)]]
                pd.concat([excluded,sample],ignore_index=True).to_csv(helper.data/'Reviews.csv',index=False)
                files=['Reviews.csv','Movies.csv'] if task=='movie-q10' else ['Reviews.csv']
                cfg=dict(data_dir=str(helper.data),sembench=os.environ['SEMLOOM_NATIVE_TEST_CHECKOUT'],
                    model='m',model_name='openai/local-fixture',native_trace=str(helper.data/'trace.jsonl'),
                    source_data_sha256={name:adapter.sha(helper.data/name) for name in files},
                    map_prompt_root=str(helper.data/f'prompts-{task}-{count}'),
                    execution_enabled=False,CPU_fixture_only=True)
                spec=dict(query_id='q',task=task,expected_calls=count,max_native_calls=2*count)
                bound=dict(call_cap=2*count,queries=[spec]);cfg['units']=[bound]
                query=movie_map.make_query(cfg,'q',task)
                values=(('POSITIVE','NEGATIVE','other',' NEGATIVE ','positive','POSITIVE\n','')
                        if task=='movie-q8' else ('5','1','not numeric','6','nan','Answer: 2','3'))
                answers=[values[i%7] for i in range(count)]
                expected,_=helper._reference(query,answers)
                expected_quality=query.evaluate(expected)
                if task=='movie-q9':
                    self.assertEqual(len(expected),count)
                    self.assertEqual(expected.reviewId.tolist(),sample.reviewId.tolist())
                with patch.object(movie_map_inputs,'binding',return_value={'CPU_fixture':True}):
                    frozen=movie_map_inputs.prepare_inputs(cfg,bound,helper.data/f'freeze-{task}-{count}')
                    cfg['native_cpu_inputs']={key:frozen[key] for key in ('manifest','sha256')}
                    _,loaded=movie_map_inputs.load_inputs(cfg)
                self.assertEqual([r['source_index'] for r in loaded[0]['table']['rows']],list(range(len(excluded),count+len(excluded))))
                self.assertEqual([r['reviewId'] for r in loaded[0]['table']['rows']],sample.reviewId.tolist())
                response,events=trace(count)
                response['latency_sec']=3
                for row in response['results']:row['outputs'][0]['value']=answers[row['idx']]
                for event in events:
                    if event['event']=='map_result':event['text']=answers[event['idx']]
                clock=ControlledClock();actions=[]
                def emit(name):actions.append((name,clock()));return clock()
                def transport(payload,qid,directory):
                    self.assertEqual(qid,'q')
                    self.assertEqual(payload['ops'],[dict(op='sem_map',args=dict(prompt=query.operation.stage.method.instruction))])
                    self.assertFalse(payload['blocking'])
                    self.assertEqual(pd.read_csv(payload['data_path'])['data'].tolist(),sample.reviewText.tolist())
                    clock.advance(3);return response
                original_prepare=movie_map.prepare
                original_make_query=movie_map.make_query
                original_parse=postprocessors.map_postprocess
                original_query=sembench_inputs.SemBenchTextQuery._original
                original_evaluate=sembench_inputs.SemBenchTextQuery.evaluate
                def prepare(*args):clock.advance(2);return original_prepare(*args)
                def make_query(*args):clock.advance(17);return original_make_query(*args)
                def parse(*args):clock.advance(5);return original_parse(*args)
                def original(owner,*args):
                    result=original_query(owner,*args);clock.advance(4);return result
                def read_trace(path,qid):
                    emit('trace');clock.advance(7)
                    return [] if Path(path).name=='map-trace.jsonl' else events
                def evaluate(owner,frame):
                    emit('evaluate');clock.advance(11);return original_evaluate(owner,frame)
                def save(path,value):
                    emit('persist');clock.advance(13);path.write_text(json.dumps(value))
                with patch.object(adapter,'load_dependencies',return_value=save), \
                        patch.object(movie_map.time,'monotonic_ns',clock), \
                        patch.object(movie_map,'prepare',prepare), \
                        patch.object(movie_map,'make_query',make_query), \
                        patch.object(postprocessors,'map_postprocess',parse), \
                        patch.object(sembench_inputs.SemBenchTextQuery,'_original',original), \
                        patch.object(sembench_inputs.SemBenchTextQuery,'evaluate',evaluate), \
                        patch.object(adapter,'read_trace',read_trace):
                    driver=movie_map.MapDriver(cfg,bound,helper.data/f'query-{task}-{count}',transport)
                    driver.model_uid='u'
                    report=driver.query(spec,emit)
                self.assertEqual(report['result'],expected.to_dict(orient='split'))
                self.assertEqual(json.dumps(report['quality'],sort_keys=True),json.dumps(expected_quality,sort_keys=True))
                self.assertEqual(report['physical_native_calls'],count)
                self.assertEqual(report['physical_call_cap'],2*count)
                timing=report['memory_sink_timing']
                self.assertEqual(timing['source_started_ns'],17_000_000_000)
                self.assertEqual(timing['memory_sink_ready_ns'],31_000_000_000)
                self.assertEqual(timing['phases']['original_query']['elapsed_s'],14)
                self.assertEqual(timing['phases']['source_preflight']['elapsed_s'],17)
                self.assertEqual(actions[0],('memory_sink_ready',31_000_000_000))
                self.assertTrue(all(at>=31_000_000_000 for name,at in actions if name in ('trace','evaluate','persist')))
                self.assertEqual(timing['trace_audit_status'],'passed')


if __name__=='__main__':unittest.main()
