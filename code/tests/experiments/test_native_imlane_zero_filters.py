"""Real original zero frontiers and Arrow/ACK; native SQL is a software fixture."""
import csv
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow as pa

from integrations.imlane_duckdb import mmqa_filter_udf, model_provider
from src.baselines.text.sembench_prepared import finish_relation, result_value
from src.experiments import native_imlane_filters as entry, native_imlane_relation as relation
from src.infrastructure.private_artifacts import json_metric, publish_private_json, read_json
from tests.experiments.test_native_imlane_filters import config
from tests.experiments.test_native_imlane_relation import CHECKOUT


@unittest.skipUnless(CHECKOUT,'requires pinned original SemBench/LOTUS')
class ZeroNativeFilterTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name)
        self.serial=0
        pd.DataFrame(dict(id=['unrelated'],reviewId=[1],reviewText=['source'],scoreSentiment=['NEGATIVE'],isPositive=[False])).to_csv(
            self.root/'Reviews.csv',index=False)
        pd.DataFrame(dict(id=[11],productDisplayName=['watch'],productDescriptors=['description'],price=[700],
            brandName=['brand'],masterCategory=[{'typeName':'Accessories'}],subCategory=[{'typeName':'Watches'}],
            articleType=[{'typeName':'Watches'}])).to_parquet(self.root/'styles_details.parquet',index=False)

    def frozen(self, *, join=False, query=None):
        value=config()
        for name in ('input_sha256','prompt_columns','max_model_calls'):value.pop(name)
        value.update(schema=entry.JOIN_SCHEMA if join else entry.MOVIE_SCHEMA,
            query_id=query or ('7' if join else '3'), execution_id='zero-original',
            sembench_checkout=CHECKOUT,data_dir=str(self.root),model_name='openai/local-fixture',
            tokenizer='/unused/tokenizer')
        if join:value['scenario']='ecomm' if query is None else 'movie'
        return entry.freeze_config(value)

    def worker(self, value, *, mismatched_rows=False, evaluation_failure=False):
        self.serial+=1
        args=NS(output=self.root/('worker-'+str(self.serial)),build_record=self.root/'build.json',
                kind='thread',threads=None,executor_count=None)
        args.build_record.write_text('{}')
        build=dict(binary_path='/unused/native-driver',kernel_path='/unused/filter-kernel',duckdb_library_path='/unused/lib.so')
        self.last=NS(output=args.output,events=[],spawns=[],args=args)
        case=self
        class Scope:
            def __init__(self,args,report,**kwargs):
                self.args=args;self.env=args.workload_environment;self.children=[]
                self.track_root,self.check=Mock(),Mock()
            def __enter__(self):case.last.events.append(('sql-open',time.monotonic_ns()));return self
            def __exit__(self,*args):case.last.events.append(('sql-close',time.monotonic_ns()))
        def spawn(command,environment,log):
            self.last.spawns.append(command)
            self.assertEqual(command[command.index('--expected-rows')+1],'0')
            self.assertNotIn(model_provider.CONFIG_ENV,environment)
            self.assertEqual(read_json(args.output/'messages.json'),{'rows':[]})
            with (args.output/'input.csv').open() as stream:
                reader=csv.DictReader(stream)
                self.assertEqual(reader.fieldnames,['occurrence_id','title','text','source_position'])
                self.assertEqual(list(reader),[])
            table=pa.table({name:pa.array([],type=pa.string()) for name in ['c0','c1','c2']})
            self.assertEqual(mmqa_filter_udf.infer_mmqa(table).num_rows,0)
            native=dict(status='query_returned',variant='original-author',expected_DSO_sha256=entry.DSO_SHA256,
                converter_resolved_DSO=build['duckdb_library_path'],return_type='TINYINT',
                full_filter_materialized_before_title_sink=True,source_rows=1 if mismatched_rows else 0,
                eligible_rows=0,accepted_occurrences=[],result_clock='CLOCK_MONOTONIC',
                result_ready_monotonic_ns=time.monotonic_ns(),
                sink_handoff=dict(protocol=relation.ACK_PROTOCOL,release_path=str(args.output/'sink-release.ack')))
            publish_private_json(args.output/'query.json',native)
            process=NS(returncode=None)
            def poll():
                if process.returncode is None and (args.output/'sink-release.ack').exists():
                    self.last.events.append(('driver-saw-ack',time.monotonic_ns()))
                    process.returncode=0
                return process.returncode
            process.poll=poll;process.wait=lambda **kwargs:poll()
            return process,Mock()
        service,tokenizer,provider=Mock(side_effect=AssertionError('zero input allocated a model')),Mock(),Mock()
        actual_release=relation.release_relation
        def released(*args,**kwargs):
            actual_release(*args,**kwargs)
            self.last.events.append(('ack-'+kwargs['outcome'],time.monotonic_ns()))
        evaluate=entry.SemBenchTextQuery.evaluate
        def evaluated(query,result):
            self.last.events.append(('evaluate',time.monotonic_ns()))
            if evaluation_failure:raise ValueError('zero original evaluator failed')
            return evaluate(query,result)
        with patch.object(entry,'verified_build',return_value=build), \
                patch('transformers.AutoTokenizer.from_pretrained',tokenizer), \
                patch.object(model_provider,'infer_batch',provider), \
                patch.object(relation,'DuckDBScope',side_effect=Scope), \
                patch.object(relation.owner,'spawn',side_effect=spawn), \
                patch.object(relation.owner,'resource_state',return_value=dict(
                    resources_confirmed=None,scope='software DuckDBScope; no author resources')), \
                patch.object(relation,'release_relation',side_effect=released), \
                patch.object(entry.SemBenchTextQuery,'evaluate',evaluated):
            try:return entry.run_worker(args,value,service_factory=service)
            finally:
                service.assert_not_called();tokenizer.assert_not_called();provider.assert_not_called()

    def test_actual_empty_filter_and_exact_join_keep_sql_ack_sink_and_zero_calls(self):
        for join in (False,True):
            with self.subTest(join=join):
                value=self.frozen(join=join)
                self.assertEqual(value['max_model_calls'],1)
                with patch('transformers.AutoTokenizer.from_pretrained') as tokenizer:
                    query,method,prepared,requests,tokens,rows=entry.prepare_query(value)
                    tokenizer.assert_not_called()
                self.assertEqual((prepared,requests,tokens,rows),([],[],[],[]))
                self.assertEqual(query.operation.describe()['kind'],'join' if join else 'filter')
                expected,_=finish_relation(query,method,prepared,[])
                report=self.worker(value)
                self.assertEqual(report['result'],result_value(expected))
                self.assertEqual(json_metric(report['quality']),json_metric(query.evaluate(expected)))
                self.assertEqual(report['budget']['consumed_requests'],0)
                self.assertEqual(report['ipc']['native_generate_terminal_count'],0)
                self.assertEqual(len(self.last.spawns),1)
                self.assertEqual(report['service_close']['status'],'not_allocated')
                for name in ('model_ready_before_source','model_ready_before_source_observed_ns',
                             'model_startup_seconds','ready_source_to_sink_seconds'):
                    self.assertIsNone(report[name])
                self.assertGreaterEqual(report['source_to_sink_seconds'],0)
                self.assertGreaterEqual(report['cold_worker_entry_to_sink_seconds'],report['source_to_sink_seconds'])
                events=dict(self.last.events)
                self.assertLessEqual(report['full_sink_observed_monotonic_ns'],events['ack-complete'])
                self.assertLessEqual(events['driver-saw-ack'],events['evaluate'])
                self.assertFalse((self.last.output/'model-provider.json').exists())

    def test_original_early_return_has_no_semantic_operator_sql_or_ready_clock(self):
        for join,query_id in ((False,'4'),(True,'5')):
            value=self.frozen(join=join,query=query_id)
            query,method,prepared,requests,tokens,rows=entry.prepare_query(value)
            self.assertIsNone(query.operation)
            expected,_=query.bind_execution()(None)
            report=self.worker(value)
            self.assertEqual(report['result'],result_value(expected))
            self.assertIsNone(report['semantic_operator'])
            self.assertEqual(report['native_ai_query_attempts'],0)
            self.assertEqual(self.last.spawns,[])
            self.assertEqual(report['budget']['consumed_requests'],0)
            self.assertIsNone(report['model_ready_before_source'])
            self.assertIsNone(report['ready_source_to_sink_seconds'])
            self.assertFalse((self.last.output/'input.csv').exists())
            self.assertFalse((self.last.output/'sink-release.ack').exists())
            self.assertTrue((self.last.output/'full-sink-observed.json').exists())

    def test_zero_sql_cardinality_corruption_aborts_ack_without_a_complete_sink(self):
        value=self.frozen()
        with self.assertRaisesRegex(ValueError,'cardinality') as caught:self.worker(value,mismatched_rows=True)
        report=read_json(self.last.output/'worker.json')
        self.assertEqual(report['status'],'failed')
        self.assertNotIn('full_sink_observed_monotonic_ns',report)
        self.assertTrue((self.last.output/'sink-release.ack').read_text().splitlines()[1].startswith('abort '))
        self.assertEqual(report['budget']['consumed_requests'],0)

    def test_early_return_post_sink_failure_preserves_the_original_result(self):
        value=self.frozen(query='4')
        with self.assertRaisesRegex(ValueError,'evaluator failed'):self.worker(value,evaluation_failure=True)
        report=read_json(self.last.output/'worker.json')
        self.assertEqual(report['status'],'failed')
        self.assertIsInstance(report['full_sink_observed_monotonic_ns'],int)
        self.assertNotIn('quality',report)
        self.assertTrue((self.last.output/'full-sink-observed.json').exists())
        self.assertEqual(self.last.spawns,[])

    def test_empty_budget_identity_is_exact_and_changed_source_fails_before_model(self):
        value=self.frozen()
        with self.assertRaisesRegex(ValueError,'every original occurrence'):
            entry.prepare_query(dict(value,max_model_calls=2))
        path=self.root/'Reviews.csv';path.write_text(path.read_text()+'other,2,new,False\n')
        with self.assertRaisesRegex(ValueError,'source files changed'):entry.prepare_query(value)


if __name__=='__main__':unittest.main()
