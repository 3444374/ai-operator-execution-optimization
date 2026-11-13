"""Original hierarchy, real Arrow/provider/ACK logic; SQL/model are software."""

from collections import Counter
from copy import deepcopy
import csv
import json
import os
from pathlib import Path
import shutil
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa

from integrations.imlane_duckdb import model_provider, sembench_udf
from src.experiments import native_imlane_sembench as entry, native_imlane_relation as relation
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.shared_request_budget import SharedClaimedUnit
from src.infrastructure.private_artifacts import content_digest, file_identity, publish_private_json, read_json
from tests.baselines.text.test_imlane_duckdb_model_provider import SoftwareSocket
from tests.experiments import test_sembench_lotus_mmqa_aggregate as fixture


@unittest.skipUnless(fixture.CHECKOUT and fixture.HAS_SDK, 'requires pinned SemBench and native LOTUS SDK')
class NativeAggregateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.NativeMMQAAggregateTests.setUpClass()

    def setUp(self):
        self.source = fixture.NativeMMQAAggregateTests()
        self.source.setUp()
        self.addCleanup(self.source.doCleanups)
        self.model = self.source.directory('software-model')
        shutil.copyfile(self.source.tokenizer_path, self.model/'tokenizer.json')
        model_provider._configuration.cache_clear()
        self.addCleanup(model_provider._configuration.cache_clear)

    def config(self, *, multi=False, selected=16, allowance=40):
        service, _ = self.source.multi_config() if multi else self.source.config()
        data = self.source.source(selected=selected)
        counter = dict(service['token_counter'], path=str(self.model/'tokenizer.json'))
        config = dict(schema=entry.TEXT_SCHEMA, query_id='original-aggregate',
            task=dict(scenario='mmqa', query='Q5'), sembench_checkout=str(self.source.checkout),
            data_dir=str(data), input_sha256={'lizzy_caplan_text_data.csv':
                file_identity(data/'lizzy_caplan_text_data.csv')['sha256']},
            model_name=service['model_name'], tokenizer=str(self.model), token_counter=counter,
            generation=service['generation'], model_service=dict(model_python='CPU-no-model',
                model=str(self.model), served_model_name='local-aggregate-fixture', dtype='bfloat16',
                engine_options=dict(max_model_len=service['native_lm_max_ctx_len'], tokenizer_revision='fixture-v1')),
            max_model_calls=allowance, query_timeout_s=60, cleanup_s=30, stop_grace_s=2, request_timeout_s=60)
        return service, config, data

    def invoke(self, config, *, mode='compact', bad_ids=False, close_failure=False, audit_failure=False,
               ready_context=None, reply_batch=None):
        import httpx
        from src.experiments import native_imlane_topk as prediction

        output = self.source.root/('native-query-'+str(next(self.source.serial)))
        args = SimpleNamespace(output=output, build_record=self.source.root/'build.json',
            kind='thread', threads=None, executor_count=None)
        args.build_record.write_text('{}')
        build = dict(binary_path='/unused/driver', kernel_path=str(entry.CODE/'integrations/imlane_duckdb/sembench_udf.py'),
                     duckdb_library_path='/unused/libduckdb.so')
        events, services, current = [], [], []
        replies = fixture.NativeAggregateReplies(self, mode=mode)
        self.last = SimpleNamespace(output=output, events=events, services=services, replies=replies)
        case = self

        class Service:
            def __init__(self, cfg, directory, **kwargs):
                services.append(self)
                value = cfg['request_budget']
                self.unit = SharedClaimedUnit(Path(value['path']), AttemptBudget(value['budget_id'],value['limit']),value['unit_id'])
                self.socket_path, self.served_model_name = case.source.root/'no-socket', 'local-aggregate-fixture'
                context = cfg['engine_options']['max_model_len'] if ready_context is None else ready_context
                self.ready_info = dict(max_model_len=context,engine_instances=1)
                self.report = dict(status='configured',software_fixture=True)
                self.closed = False
            def __enter__(self):
                self.report['started_monotonic'] = time.monotonic()
                events.append(('model-ready',time.monotonic_ns()))
                return self
            def __exit__(self,*args): return entry.ModelService.__exit__(self,*args)
            def check(self): pass
            def begin_cleanup(self,deadline): pass
            def close(self,**kwargs):
                if self.closed: return
                self.closed = True
                events.append(('model-close',time.monotonic_ns()))
                if close_failure: raise RuntimeError('CPU model close failed')
                self.report['status'] = 'closed'

        def reply(request):
            if reply_batch is not None:
                return reply_batch(request, services[0].unit)
            calls = []
            for position,messages in enumerate(request['messages_batch']):
                attempt = services[0].unit.reserve(content_digest(messages))
                body = dict(request['sampling_params'],messages=messages,model='local-aggregate-fixture')
                response = replies.respond(httpx.Request('POST','http://localhost:18474/v1/chat/completions',
                    headers=dict(authorization='Bearer '+fixture.KEY_VALUE),json=body)).json()
                text = response['choices'][0]['message']['content']
                calls.append(dict(position=position,attempted=True,status='succeeded',generated_text=text,
                    request_id='software-'+str(attempt),budget_attempt=attempt,
                    requested_max_tokens=request['sampling_params']['max_tokens'],
                    resolved_max_tokens=request['sampling_params']['max_tokens'],rendered_prompt_token_ids=[7,8],
                    finish_reason='stop',usage=dict(prompt_tokens=2,completion_tokens=1,total_tokens=3)))
            return dict(schema=request['schema'],batch_id=request['batch_id'],model=request['model'],status='succeeded',calls=calls)

        class Scope:
            def __init__(self,stage_args,report,**kwargs):
                self.args = stage_args
                self.env = stage_args.workload_environment
                self.children = []
                self.track_root,self.check = Mock(),Mock()
            def __enter__(self):
                current.append(self)
                events.append(('sql-open',time.monotonic_ns()))
                return self
            def __exit__(self,*args):
                case.assertFalse((output/'result-ready.json').exists())
                events.append(('sql-close',time.monotonic_ns()))
                current.pop()

        def spawn(command,environment,log):
            stage = current[-1]
            with (stage.args.output/'input.csv').open() as stream:
                records = list(csv.DictReader(stream))
            table = pa.table({'c0':[row['call_id'] for row in records],
                              'c1':[row['messages_json'] for row in records]})
            with patch.dict(os.environ,environment), patch.object(model_provider,'socket',SimpleNamespace(
                    AF_UNIX=model_provider.socket.AF_UNIX,SOCK_STREAM=model_provider.socket.SOCK_STREAM,
                    socket=lambda *args:SoftwareSocket(reply))):
                texts = sembench_udf.infer_prepared(table).to_pylist()
            predictions = [dict(call_id=row['call_id'],**text) for row,text in zip(records,texts,strict=True)]
            if bad_ids: predictions[0]['call_id'] = 'foreign-occurrence'
            stamp = time.monotonic_ns()
            native = dict(status='query_returned',result_clock='CLOCK_MONOTONIC',result_ready_monotonic_ns=stamp,
                sink_handoff=dict(protocol=relation.ACK_PROTOCOL,release_path=str(stage.args.output/'sink-release.ack')),
                return_type='VARCHAR',full_projection_materialized=True,variant=entry.VARIANT,
                source_rows=len(records),converter_resolved_DSO=build['duckdb_library_path'],
                mapped_occurrences=list(reversed(predictions)))
            publish_private_json(stage.args.output/'query.json',native)
            events.append(('projection-ready',stamp))
            process = SimpleNamespace(returncode=None)
            def poll():
                if process.returncode is None and (stage.args.output/'sink-release.ack').exists():
                    events.append(('driver-ack-observed',time.monotonic_ns()))
                    process.returncode = 0
                return process.returncode
            process.poll = poll
            process.wait = lambda **kwargs:poll()
            return process,Mock()

        native_query = relation.run_relation_query
        def observe_query(*args,**kwargs):
            original = kwargs['sink']
            def sink(native):
                value = original(native)
                events.append(('batch-mapped',time.monotonic_ns()))
                return value
            kwargs['sink'] = sink
            return native_query(*args,**kwargs)
        release = relation.release_relation
        def observe_release(*args,**kwargs):
            release(*args,**kwargs)
            events.append(('ack-'+kwargs.get('outcome','complete'),time.monotonic_ns()))
        def tokens(*args,**kwargs):
            case.assertTrue((output/'result-ready.json').exists())
            events.append(('token-audit',time.monotonic_ns()))
            return [7,8]
        audit = entry.audit_ipc
        def observed_audit(*args,**kwargs):
            events.append(('ipc-audit',time.monotonic_ns()))
            if audit_failure: raise ValueError('CPU saved receipt audit failed')
            return audit(*args,**kwargs)
        with patch.object(entry,'verified_build',return_value=build), \
                patch('transformers.AutoTokenizer.from_pretrained',return_value=SimpleNamespace(apply_chat_template=tokens)), \
                patch.object(prediction,'run_relation_query',side_effect=observe_query), \
                patch.object(relation,'DuckDBScope',side_effect=Scope), \
                patch.object(relation.owner,'spawn',side_effect=spawn), \
                patch.object(relation.owner,'resource_state',return_value={'resources_confirmed':True}), \
                patch.object(relation,'release_relation',side_effect=observe_release), \
                patch.object(entry,'audit_ipc',side_effect=observed_audit):
            return entry.run_worker(args,config,service_factory=Service)

    def test_single_and_multilevel_match_original_sdk_batches_tokens_result_and_evaluator(self):
        from lotus.models import LM

        counts = []
        count = LM.count_tokens
        def observe(lm,value):
            result = count(lm,value)
            counts.append((time.monotonic_ns(),deepcopy(value),result))
            return result
        for multi,mode in ((False,'compact'),(True,'compact'),(True,'long_summaries'),(False,'wrong_actor')):
            with self.subTest(multi=multi,mode=mode):
                service,config,data = self.config(multi=multi)
                reference = fixture.NativeAggregateReplies(self,mode=mode)
                with patch.object(LM,'count_tokens',observe):
                    original,_,_ = self.source.invoke(service,data,reference,allowance=40)
                original_counts = [(value,total) for stamp,value,total in counts
                                   if original['source_started_ns']<=stamp<=original['sink_ready_ns']]
                counts.clear()
                with patch.object(LM,'count_tokens',observe):
                    report = self.invoke(config,mode=mode)
                actual_counts = [(value,total) for stamp,value,total in counts
                                 if report['query_started_monotonic_ns']<=stamp<=report['full_sink_observed_monotonic_ns']]
                counts.clear()
                self.assertEqual(report['result'],original['result'])
                self.assertEqual(report['quality'],original['quality'])
                self.assertEqual([s['occurrences'] for s in report['native_lm_batches']],
                                 [s['input_occurrences'] for s in original['native_lm_calls']])
                self.assertEqual(Counter(content_digest(r['body']['messages']) for r in self.last.replies.requests),
                                 Counter(content_digest(r['body']['messages']) for r in reference.requests))
                self.assertEqual(report['model_calls'],original['model_calls'])
                self.assertEqual(actual_counts,original_counts)
                self.assertEqual(report['budget']['consumed_requests'],original['model_calls'])
                self.assertEqual(report['query_clock_version'],'lotus_imlane.source_to_sink.v1')
                self.assertFalse(report['cost_available'])
                self.assertIsNone(report['cost'])
                kinds = [kind for kind,stamp in self.last.events]
                self.assertEqual(kinds.count('model-close'),1)
                self.assertEqual(kinds.count('sql-close'),len(report['native_lm_batches']))
                self.assertEqual(kinds.count('ack-complete'),len(report['native_lm_batches']))
                self.assertLess(max(stamp for kind,stamp in self.last.events if kind=='sql-close'),
                                report['full_sink_observed_monotonic_ns'])
                self.assertTrue(all(stamp>report['full_sink_observed_monotonic_ns'] for kind,stamp in self.last.events
                                    if kind in ('token-audit','ipc-audit','model-close')))
                for phase in report['prediction_batches']:
                    self.assertLessEqual(phase['full_sink_observed_monotonic_ns'],phase['sink_release']['published_monotonic_ns'])
                calls = read_json(self.last.output/'original-lm-batches.json')
                ids = [r['call_id'] for batch in calls for r in batch['requests']]
                self.assertEqual(len(ids),len(set(ids)))

    def test_unknown_total_uses_minimum_and_actual_budget_without_starting_a_fixed_frontier(self):
        service,config,data = self.config(multi=True)
        query,*frontier = entry.prepare_query(config)
        self.assertEqual(frontier,[None,None,None,None])
        self.assertIsNone(query.describe()['expected_model_calls'])
        path = self.source.root/'aggregate-config.json'
        entry.write_private_json(path,config)
        output = self.source.root/'aggregate-check'
        self.assertEqual(entry.main(['check','--config',str(path),'--output',str(output)]),0)
        checked = read_json(output/'CHECK.json')
        self.assertIsNone(checked['source']['expected_model_calls'])
        self.assertEqual(checked['model_calls'],0)
        self.assertFalse(checked['native_DSO_SQL_model_binding_qualified'])
        minimum = query.describe()['minimum_model_calls']
        config['max_model_calls'] = minimum
        with self.assertRaises(BudgetExhausted): self.invoke(config,mode='long_summaries')
        report = read_json(self.last.output/'author.json')
        self.assertEqual(report['status'],'failed')
        self.assertNotIn('quality',report)
        self.assertNotIn('full_sink_observed_monotonic_ns',report)
        self.assertLessEqual(report['budget']['consumed_requests'],minimum)
        self.assertEqual(sum(k=='model-close' for k,t in self.last.events),1)

    def test_empty_original_selection_keeps_sdk_error_before_model_or_native_sql(self):
        service,config,data = self.config(selected=0)
        with self.assertRaises(IndexError) as original:
            self.source.entry.check(service,self.source.checkout,data_dir=data)
        with self.assertRaises(IndexError) as native:self.invoke(config)
        self.assertEqual(str(native.exception),str(original.exception))
        self.assertEqual(self.last.services,[])
        self.assertFalse(self.last.output.exists())

    def test_foreign_native_call_id_aborts_handoff_without_a_complete_query(self):
        _,config,_ = self.config()
        with self.assertRaisesRegex(ValueError,'native prediction lost'):
            self.invoke(config,bad_ids=True)
        report = read_json(self.last.output/'author.json')
        self.assertNotIn('full_sink_observed_monotonic_ns',report)
        self.assertIn('ack-abort',[kind for kind,stamp in self.last.events])
        self.assertEqual(sum(k=='model-close' for k,t in self.last.events),1)

    def test_post_sink_audit_and_model_close_failures_keep_result_and_primary(self):
        _,config,_ = self.config()
        with self.assertRaisesRegex(ValueError,'saved receipt audit failed') as caught:
            self.invoke(config,audit_failure=True,close_failure=True)
        self.assertTrue(any('close failed' in note for note in caught.exception.__notes__))
        report = read_json(self.last.output/'author.json')
        self.assertIn('full_sink_observed_monotonic_ns',report)
        self.assertEqual(report['status'],'failed')
        self.assertTrue((self.last.output/'full-sink-observed.json').exists())

    def test_json_overrides_bad_counter_and_wrong_ready_context_cannot_qualify(self):
        _,config,_ = self.config()
        for change in ({'response_format':{'type':'json_object'}},{'logprobs':0},{'repetition_penalty':1.1}):
            with self.subTest(change=change),self.assertRaisesRegex(ValueError,'standard generation'):
                entry.configuration(dict(config,generation=dict(config['generation'],**change)))
        with self.assertRaises(ValueError):
            entry.prepare_query(dict(config,token_counter=dict(config['token_counter'],sha256='0'*64)))
        with self.assertRaisesRegex(ValueError,'actual native model context'):
            self.invoke(config,ready_context=config['model_service']['engine_options']['max_model_len']+1)
        self.assertEqual(sum(k=='sql-open' for k,t in self.last.events),0)
        self.assertEqual(sum(k=='model-close' for k,t in self.last.events),1)


if __name__=='__main__':unittest.main()
