"""Original SemBench/LOTUS query parity with fixed CPU software answers.

Set SEMLOOM_NATIVE_TEST_CHECKOUT to the clean pinned public checkout. The real
query functions, formatter, parser and relational sinks run; no model runs.
"""

from copy import deepcopy
from dataclasses import asdict
import hashlib
import importlib
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
from contextlib import redirect_stdout
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from src.baselines.text.sembench_text import SemBenchTextQuery
from src.infrastructure.request_budget import ClaimedUnit, Reservation
from src.semantic_methods.lotus.record_chain import LotusRecordChain
from src.semantic_methods.lotus.record_method import LotusRecordMethod
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.async_engine_process import SubmitResult, TerminalEvent
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec, _prepared, result_record


CHECKOUT = os.environ.get("SEMLOOM_NATIVE_TEST_CHECKOUT")
REAL_MMQA_DATA = os.environ.get("SEMLOOM_NATIVE_TEST_MMQA_DATA")
REAL_MEDICAL_DATA = os.environ.get("SEMLOOM_NATIVE_TEST_MEDICAL_DATA")
HAS_SDK = all(importlib.util.find_spec(name) is not None for name in ("lotus", "litellm"))
READY = bool(CHECKOUT and Path(CHECKOUT).is_dir() and HAS_SDK)


def _reply(prepared, text):
    return dict(prepared, status="succeeded", text=text, prompt_tokens=11,
                completion_tokens=2, total_tokens=13, finish_reason="stop",
                engine_request_id="software-" + prepared["call_id"],
                submission_confirmed=True, logprobs_json="null")


def _messages_key(messages):
    return json.dumps(messages, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@unittest.skipUnless(READY, "requires LOTUS and SEMLOOM_NATIVE_TEST_CHECKOUT at the pinned public source")
class OriginalSemBenchTextTests(unittest.TestCase):
    def setUp(self):
        self.scope = patch.dict(os.environ, dict(HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
            HF_DATASETS_OFFLINE="1", LITELLM_LOCAL_MODEL_COST_MAP="True",
            TOKENIZERS_PARALLELISM="false", CUDA_VISIBLE_DEVICES=""))
        self.scope.start()
        self.addCleanup(self.scope.stop)
        connect = socket.socket.connect

        def forbid_network(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                self.fail("software query parity attempted a network connection")
            return connect(sock, address)

        scope = patch.object(socket.socket, "connect", forbid_network)
        scope.start()
        self.addCleanup(scope.stop)
        from src.serving.backends.vllm_async_engine import VllmAsyncEngine
        from src.serving.backends.vllm_engine import VllmEngine
        for engine in (VllmEngine, VllmAsyncEngine):
            scope = patch.object(engine, '__init__', side_effect=AssertionError(
                'software query parity must not construct a real model engine'))
            scope.start()
            self.addCleanup(scope.stop)
        import lotus
        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)
        temporary = tempfile.TemporaryDirectory(prefix="sembench-original-query-")
        self.addCleanup(temporary.cleanup)
        self.data = Path(temporary.name).resolve()
        self.generation = GenerationSpec(max_tokens=32, temperature=0, seed=0)
        self.aliases = ("src.runner", "src.runner.generic_lotus_runner",
                        "src.runner.generic_lotus_runner.generic_lotus_runner")
        self.absent = object()
        self.original_aliases = {name: sys.modules.get(name, self.absent) for name in self.aliases}
        self.addCleanup(self._assert_aliases_restored)
        self._write_inputs()

    def test_imlane_filter_budget_binds_actual_original_source_not_fixed_sizes(self):
        from src.experiments import native_imlane_filters as entry
        from src.infrastructure.private_artifacts import file_identity
        from transformers import AutoTokenizer

        value = json.loads((entry.CODE / 'configs/sembench_imlane_filter.example.json').read_text())
        value.update(schema=entry.MOVIE_SCHEMA, sembench_checkout=CHECKOUT, data_dir=str(self.data),
            input_sha256={'Reviews.csv': file_identity(self.data / 'Reviews.csv')['sha256']},
            query_id='1', max_model_calls=11, prompt_columns=['reviewText'])
        # Only tokenization is synthetic. Original source loading, query capture,
        # LOTUS formatting and occurrence-budget validation execute unchanged.
        tokenizer = Mock()
        tokenizer.apply_chat_template.return_value = [1, 2]
        with patch.object(AutoTokenizer, 'from_pretrained', return_value=tokenizer):
            query, method, prepared, requests, tokens, rows = entry.prepare_query(value)
            self.assertEqual(len(query.operation.records), 11)
            self.assertEqual(len(prepared), 11)
            self.assertEqual(len(rows), 11)
            self.assertEqual(list(method.columns), ['reviewText'])
            tokenizer.reset_mock()
            for wrong_budget in (10, 12, 2000):
                with self.subTest(budget=wrong_budget), self.assertRaisesRegex(ValueError, 'every original occurrence'):
                    entry.prepare_query(dict(value, max_model_calls=wrong_budget))
            tokenizer.apply_chat_template.assert_not_called()
            with self.assertRaisesRegex(ValueError, 'original prompt columns'):
                entry.prepare_query(dict(value, prompt_columns=['Reviewtext']))

    def _assert_aliases_restored(self):
        for name in self.aliases:
            self.assertIs(sys.modules.get(name, self.absent), self.original_aliases[name])

    def _write_inputs(self):
        reviews = pd.DataFrame([
            ("duplicate", "taken_3", "same review", "NEGATIVE", 1),
            ("other", "other_movie", "invalid numeric review", "POSITIVE", 5),
            ("duplicate", "taken_3", "same review", "POSITIVE", 5),
            ("third", "taken_3", "out of range review", "NEGATIVE", 2),
            ("fourth", "other_movie", "one star review", "NEUTRAL", 3),
            ("fifth", "other_movie", "nan numeric review", "POSITIVE", 4),
            ("ant-duplicate", "ant_man_and_the_wasp_quantumania", "antman positive review", "POSITIVE", 5),
            ("ant-negative", "ant_man_and_the_wasp_quantumania", "antman negative review", "NEGATIVE", 1),
            ("ant-duplicate", "ant_man_and_the_wasp_quantumania", "antman invalid review", "NEUTRAL", 3),
            ("ant-range", "ant_man_and_the_wasp_quantumania", "antman out of range review", "NEUTRAL", 3),
            ("ant-nan", "ant_man_and_the_wasp_quantumania", "antman nan review", "NEUTRAL", 3),
        ], columns=["reviewId", "id", "reviewText", "scoreSentiment", "score"])
        reviews['originalScore'] = reviews['score'].map(lambda value: str(value) + '/5')
        reviews.to_csv(self.data / "Reviews.csv", index=False)
        pd.DataFrame(dict(id=["taken_3", "other_movie", "ant_man_and_the_wasp_quantumania"],
            audienceScore=[10, 20, 30])).to_csv(
            self.data / "Movies.csv", index=False)
        pd.DataFrame(dict(Title=["Target Film", "Another Film"],
            Role=["Bob Whitewood", "Other Role"])).to_csv(self.data / "ben_piazza.csv", index=False)
        pd.DataFrame(dict(row_id=[700, 700, 99], title=["Unused Film", "Target Film", "Another Film"],
            url=["unused", "target", "another"], id=["u", "t", "a"],
            text=["unused film description", "target film description", "another film description"])).to_csv(
                self.data / "ben_piazza_text_data.csv", index=False)
        pd.DataFrame(dict(row_id=[0, 5, 5, 11, 11, 12],
            title=["Outside Fixture", "Mean Girls", "Outside Other", "Orange County", "Mean Girls", "Cobweb"],
            url=["u0", "u1", "u2", "u3", "u4", "u5"],
            text=["outside description", "same film description", "outside other description",
                  "different film description", "same film description", "horror film description"])).to_csv(
                self.data / "lizzy_caplan_text_data.csv", index=False)
        pd.DataFrame(dict(row_id=[1, 1, 2], Airlines=["Air A", "Air B", "Air A"],
            Destinations=["Frankfurt, Berlin", None, "Frankfurt, Berlin"],
            Airport=[None, "Unused airport", "Different unused airport"])).to_csv(
                self.data / "tampa_international_airport.csv", index=False)
        medical = self.data / 'data'
        medical.mkdir()
        pd.DataFrame(dict(patient_id=[10, 30, 30, 70], symptom_id=[0, 1, 2, 3],
            symptoms=['allergy fixture', 'acne fixture', 'acne fixture', None])).to_csv(
                medical / 'text_symptoms_data.csv', index=False)
        patients = pd.DataFrame(dict(patient_id=[900, 30, 10, 800, 70], age=[70, 52, 20, 65, 41],
            gender=['M', 'F', 'F', 'M', 'M'], smoking_history=['Never'] * 5,
            did_family_have_cancer=[0] * 5))
        patients.to_csv(medical / 'patient_data.csv', index=False)
        patients['text_diagnosis'] = ['none', 'acne', 'allergy', 'none', 'migraine']
        patients.to_csv(medical / 'patient_data_with_labels.csv', index=False)

    def _query(self, scenario, query):
        return SemBenchTextQuery(CHECKOUT, self.data, scenario, query,
            execution_id="software-" + scenario + "-" + query,
            model_name="openai/local-fixture", generation=self.generation)

    def _write_ecomm_inputs(self):
        frame = pd.DataFrame(dict(id=[11, 22, 33, 44, 55, 66],
            productDisplayName=['same shirt', 'same shirt', 'socks', 'watch', 'saree', 'nightwear'],
            productDescriptors=[{'description': {'value': 'Quoted "text", 汉字\nand details'}}] * 6,
            masterCategory=[{'typeName': value} for value in
                ['Apparel', 'Apparel', 'Apparel', 'Accessories', 'Apparel', 'Apparel']],
            subCategory=[{'typeName': value} for value in
                ['Topwear', 'Topwear', 'Socks', 'Watches', 'Saree', 'Loungewear and Nightwear']],
            articleType=[{'typeName': value} for value in
                ['Shirts', 'Shirts', 'Socks', 'Watches', 'Sarees', 'Nightwear']],
            price=[300, 300, 200, 700, 1000, 600],
            brandName=['Acme', 'Acme', 'Zen', 'Zen', 'Acme', 'Zen']))
        frame.to_parquet(self.data / 'styles_details.parquet', index=False)
        return frame

    def test_ecomm_original_parquet_selection_messages_relations_and_ari(self):
        source = self._write_ecomm_inputs()
        for key, answers, expected_ids in (
                ('3', [json.dumps({'category': value}) for value in source.brandName], source.id.tolist()),
                ('5', ['Topwear', 'Topwear', 'Socks'], [11, 22, 33])):
            with self.subTest(query=key):
                query = self._query('ecomm', key)
                self.assertEqual(set(query.operation.describe()['model_columns']),
                    {'productDisplayName', 'productDescriptors'})
                self.assertEqual(query.operation.frame.id.tolist(), expected_ids)
                result, _, _ = self._parity(query, answers)
                self.assertEqual(result.columns.tolist(), ['id', 'category'])
                self.assertEqual(result.id.tolist(), expected_ids)
                quality = query.evaluate(result)['original_metric']
                self.assertEqual(quality['metric_type'], 'adjusted-rand-index')
                self.assertEqual(quality['accuracy'], 1)
                # ARI evaluates grouping, not literal brand/category names.
                renamed = result.copy()
                renamed['category'] = renamed.category.map({value: str(i) for i, value in
                    enumerate(result.category.unique())})
                self.assertEqual(query.evaluate(renamed)['original_metric'], quality)

    def test_ecomm_prepared_reordered_outputs_reuse_parser_and_complete_original_sink(self):
        from src.baselines.text.sembench_prepared import prepare, finish_relation

        source = self._write_ecomm_inputs()
        for key, answers in (('3', [json.dumps({'category': value}) for value in source.brandName]),
                             ('5', ['Topwear', 'Topwear', 'Socks'])):
            with self.subTest(query=key):
                query = self._query('ecomm', key)
                expected, batches = self._reference(query, answers)
                method, prepared, requests = prepare(query)
                self.assertEqual([row['messages'] for row in requests],
                    [messages for batch, _ in batches for messages in batch])
                predictions = list(reversed([dict(call_id=row['call_id'], generated_text=text)
                    for row, text in zip(prepared, answers, strict=True)]))
                result, _ = finish_relation(query, method, prepared, predictions)
                self._assert_result_equal(result, expected)

    def test_ecomm_missing_changed_and_empty_parquet_have_distinct_outcomes(self):
        with self.assertRaises(FileNotFoundError):
            self._query('ecomm', '3')
        source = self._write_ecomm_inputs()
        query = self._query('ecomm', '3')
        source.iloc[:1].to_parquet(self.data / 'styles_details.parquet', index=False)
        execute = Mock()
        with self.assertRaisesRegex(ValueError, 'changed after preflight'):
            query.execute(execute)
        execute.assert_not_called()
        with self.assertRaisesRegex(ValueError, 'gold input changed'):
            query.evaluate(source[['id', 'brandName']].rename(columns={'brandName': 'category'}))
        source.iloc[[3, 4, 5]].to_parquet(self.data / 'styles_details.parquet', index=False)
        query = self._query('ecomm', '5')
        self.assertEqual(query.describe()['expected_model_calls'], 0)
        result, _, _ = self._parity(query, [])
        self.assertEqual(result.columns.tolist(), ['id', 'category'])
        self.assertTrue(result.empty)

    def test_bound_original_queries_scan_real_data_without_reloading_source_modules(self):
        from src.baselines.text import sembench_source

        self._write_ecomm_inputs()
        for scenario, key, text in (('movie', '8', 'POSITIVE'), ('mmqa', '6a', 'True'),
                ('medical', '10', '{"text_diagnosis":"ALLERGY"}'), ('ecomm', '5', 'Topwear')):
            with self.subTest(scenario=scenario):
                query = self._query(scenario, key)
                answers = [text] * len(query.operation.records)
                expected, _ = self._reference(query, answers)
                execute, _, _ = self._chain(query, answers)
                reader_name = 'read_parquet' if scenario == 'ecomm' else 'read_csv'
                with patch.object(sembench_source, 'verify_checkout', side_effect=AssertionError(
                        'source validation belongs to query binding, before execution')), \
                        patch.object(pd, reader_name, wraps=getattr(pd, reader_name)) as read:
                    result, _ = query.execute(execute)
                self.assertGreater(read.call_count, 0)
                self._assert_result_equal(result, expected)

    def test_bound_execution_reads_data_without_file_hashing_then_audits_after_sink(self):
        from src.baselines.text import sembench_text as source

        query=self._query('movie','8')
        answers=['POSITIVE']*len(query.operation.records)
        expected,_=self._reference(query,answers)
        execute,_,_=self._chain(query,answers)
        run=query.bind_execution()
        with patch.object(source,'file_identity',side_effect=AssertionError('query interval hashed a source file')):
            result,_=run(execute)
        self._assert_result_equal(result,expected)
        path=self.data/'Reviews.csv'
        path.write_text(path.read_text().replace('same review','changed after sink'))
        with self.assertRaisesRegex(ValueError,'changed after preflight'):
            query.verify_source()

    def test_ecomm_original_multi_column_join_reuses_native_owner_and_evaluator(self):
        import lotus
        from lotus.types import LMOutput
        from contextlib import contextmanager
        from src.baselines.text.frameworks.lotus_native_join import (
            audit, execute_native_join, finish_accounting, make_lm,
        )
        from src.infrastructure.private_artifacts import content_digest

        self._write_ecomm_inputs()
        query = self._query('ecomm', '7')
        operation = query.operation
        self.assertEqual(operation.describe()['expected_model_calls'], 9)
        self.assertEqual(operation.frame.id.tolist(), [11, 22, 33])
        self.assertEqual(set(operation.columns), {column + ':' + side for side in ('left', 'right')
            for column in ('productDisplayName', 'productDescriptors')})
        self.assertTrue(all(set(record.values) == set(operation.columns) for record in operation.records))
        replies = ['True', 'True', 'False', 'True', 'True', 'False', 'False', 'False', 'True']
        batches, captured, relations = [], [], []

        def answer(current, messages, **kwargs):
            batches.append(deepcopy(messages))
            self.assertEqual(len(messages), 9)
            self.assertNotIn('brandName', json.dumps(messages))
            self.assertNotIn('articleType', json.dumps(messages))
            return LMOutput(outputs=replies)

        lm = make_lm(operation, 2)
        with lotus.settings.context(lm=lm, enable_cache=False), patch.object(type(lm), '__call__', answer):
            expected = query._original()

        def accounting():
            return dict(calls=dict(counts=dict(succeeded=9, missing=0), records=[dict(
                row_id=prompt['row_id'], status='succeeded', text=text,
                messages_sha256=content_digest(prompt['messages']))
                for prompt, text in zip(captured, replies, strict=True)]))

        @contextmanager
        def owner_query(current_lm, expected_calls, budget, *, query_id):
            self.assertEqual(len(expected_calls), 9)
            yield SimpleNamespace(capture_prompts=lambda prompts: captured.extend(deepcopy(prompts)),
                                  report=accounting)

        owner = SimpleNamespace(generation=operation.generation, concurrency=2, query=owner_query)

        def execute(current, invoke):
            report = execute_native_join(current, invoke=invoke, shared_owner=owner,
                query_id=query.query_id, budget=SimpleNamespace(remaining=9))
            # Match the resident owner: the original query subsequently mutates
            # its returned Join frame when it constructs the final id column.
            relations.append(report['frame'].copy(deep=True))
            return report

        with patch.object(type(lm), '__call__', answer):
            result, report = query.execute(execute)
            finish_accounting(report)
            audit(query, report, result, relations[0])
        pd.testing.assert_series_equal(result, expected)
        self.assertEqual(result.tolist(), ['11-11', '11-22', '22-11', '22-22', '33-33'])
        self.assertEqual(batches[0], batches[1])
        self.assertEqual(len(report['native_join_inputs']), 1)
        observed = report['native_join_inputs'][0]
        self.assertEqual(len(observed['declared_columns']), 4)
        self.assertEqual(len(observed['selected_columns']), 2)
        self.assertFalse(observed['all_declared_columns_selected'])
        quality = query.evaluate(result)['original_metric']
        self.assertEqual(quality['metric_type'], 'f1-score')
        self.assertEqual(quality['accuracy'], 1)

    def _join_reference(self, query, answers):
        import lotus
        from lotus.types import LMOutput
        from src.baselines.text.frameworks.lotus_native_join import make_lm

        batches=[]
        lm=make_lm(query.operation,2)
        def respond(current,messages,**kwargs):
            self.assertIs(current,lm)
            batches.append(deepcopy(messages))
            return LMOutput(outputs=answers)
        with lotus.settings.context(lm=lm,enable_cache=False),patch.object(type(lm),'__call__',respond):
            result=query._original()
        return result,batches[0]

    def test_movie_join_retains_raw_source_columns_without_leaking_into_messages(self):
        from types import SimpleNamespace
        from src.baselines.text.sembench_movie import bind_original_lotus_query
        from src.baselines.text.sembench_prepared import prepare,finish_relation,result_value

        path=self.data/'Reviews.csv';raw=pd.read_csv(path)
        raw['unused_source_note']=['NOT_MODEL_METADATA_'+str(i) for i in range(len(raw))]
        raw.to_csv(path,index=False)
        query=self._query('movie','5');answers=['True']*len(query.operation.records)
        original=bind_original_lotus_query(CHECKOUT,5,lambda:pd.read_csv(path))
        accessor=type(pd.DataFrame().sem_join);original_join=accessor.__call__;relations=[]
        def capture(current,*args,**kwargs):
            frame=original_join(current,*args,**kwargs);relations.append(frame.copy(deep=True));return frame
        with patch.object(accessor,'__call__',capture):
            expected,messages=self._join_reference(SimpleNamespace(operation=query.operation,_original=original),answers)
        method,prepared,requests=prepare(query)
        self.assertEqual([row['messages'] for row in requests],messages)
        self.assertNotIn('NOT_MODEL_METADATA_',json.dumps(messages))
        result,relation=finish_relation(query,method,prepared,[dict(call_id=row['call_id'],generated_text=text)
            for row,text in zip(prepared,answers,strict=True)])
        self._assert_result_equal(result,expected)
        self.assertEqual(result_value(relation['operator']['frame']),result_value(relations[0]))
        self.assertIn('unused_source_note:left',relation['operator']['frame'].columns)

    def test_prepared_joins_reuse_native_messages_parser_and_complete_original_queries(self):
        from src.baselines.text.sembench_prepared import prepare,finish

        self._write_ecomm_inputs()
        for scenario,key in [('movie','5'),('movie','6'),('movie','7'),('ecomm','7')]:
            query=self._query(scenario,key)
            answers=['False' if i % 3 == 1 else 'True' for i in range(len(query.operation.records))]
            expected,messages=self._join_reference(query,answers)
            method,prepared,requests=prepare(query)
            predictions=[dict(call_id=row['call_id'],generated_text=text)
                         for row,text in zip(prepared,answers,strict=True)]
            actual,semantic=finish(query,method,prepared,predictions[::-1],execute_query=query.bind_execution())
            with self.subTest(scenario=scenario,query=key):
                self._assert_result_equal(actual,expected)
                self.assertEqual([row['messages'] for row in requests],messages)
                self.assertEqual(len({row['call_id'] for row in prepared}),len(answers))
                self.assertEqual(len(semantic['calls']),len(answers))
                self.assertEqual(semantic['native_join_inputs'][0]['all_declared_columns_selected'],scenario=='movie')

    def test_movie_filters_use_original_query_sink_through_the_same_native_ray_processor(self):
        from src.baselines.text.sembench_prepared import prepare,finish
        from src.baselines.text.frameworks.ray_data_sembench_native import execute_prepared
        from tests.baselines.text.ray_native_fixtures import recorded_processor
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger

        for key in ('1','2','3','4'):
            query=self._query('movie',key)
            answers=['Answer: False' if i%3==1 else 'Answer: True' for i in range(len(query.operation.records))]
            expected,batches=self._reference(query,answers)
            method,prepared,requests=prepare(query)
            self.assertEqual([row['messages'] for row in requests],
                [message for messages,_ in batches for message in messages])
            budget=dict(path=str(self.data/('ray-filter-'+key+'.sqlite')),id='CPU-filter-'+key,
                limit=len(answers),unit_id='query',output=str(self.data))
            ledger=CellBudgetLedger.create(Path(budget['path']),AttemptBudget(budget['id'],budget['limit']),deadline_utc=None)
            ledger.reserve_unit('query',budget['limit']);ledger.claim_shared_unit('query')
            ready=[]
            replies={row['call_id']:text for row,text in zip(prepared,answers,strict=True)}
            with self.subTest(query=key),recorded_processor(replies) as cpu:
                result=execute_prepared(requests,model='CPU-only',sampling_params=query.operation.generation.kwargs(),
                    engine_kwargs={},processor_options={},runtime_options={},
                    engine_runtime_env=dict(env_vars=dict(SEMLOOM_RAY_NATIVE_OBSERVER=json.dumps(budget))),
                    on_result_ready=lambda raw:ready.append(finish(query,method,prepared,raw['predictions'])[0]))
                self._assert_result_equal(ready[0],expected)
                self.assertEqual(len(result['predictions']),len(answers))
                self.assertEqual(len(cpu.requests),len(answers))
                self.assertEqual(cpu.events.count('materialize'),1)
                self.assertEqual(cpu.events[-1],'close')
            ledger.close_shared_unit('query')

    def test_prepared_join_native_ray_processor_keeps_pair_occurrences(self):
        from src.baselines.text.sembench_prepared import prepare,finish
        from src.baselines.text.frameworks.ray_data_sembench_native import execute_prepared
        from tests.baselines.text.ray_native_fixtures import recorded_processor
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger

        query=self._query('movie','5')
        answers=['False' if i%2 else 'True' for i in range(len(query.operation.records))]
        expected,messages=self._join_reference(query,answers)
        method,prepared,requests=prepare(query)
        replies={row['call_id']:text for row,text in zip(prepared,answers,strict=True)}
        ready=[]
        budget=dict(path=str(self.data/'ray-budget.sqlite'),id='CPU-join',limit=len(answers),
                    unit_id='query',output=str(self.data))
        ledger=CellBudgetLedger.create(Path(budget['path']),AttemptBudget(budget['id'],budget['limit']),deadline_utc=None)
        ledger.reserve_unit('query',budget['limit'])
        ledger.claim_shared_unit('query')
        with recorded_processor(replies) as cpu:
            report=execute_prepared(requests,model='CPU-only',sampling_params=query.operation.generation.kwargs(),
                engine_kwargs={},processor_options={},runtime_options={},
                engine_runtime_env=dict(env_vars=dict(SEMLOOM_RAY_NATIVE_OBSERVER=json.dumps(budget))),
                on_result_ready=lambda raw:ready.append(finish(query,method,prepared,raw['predictions'])[0]))
        self._assert_result_equal(ready[0],expected)
        self.assertEqual(len(report['predictions']),len(answers))

    @unittest.skipUnless(importlib.util.find_spec('daft'), 'Daft native SDK belongs to its separate engine environment')
    def test_prepared_join_native_daft_executor_keeps_pair_occurrences(self):
        from src.baselines.text.sembench_prepared import prepare,finish
        from src.baselines.text.frameworks.daft_sembench_native import execute_prepared_map
        from tests.baselines.text.native_sdk_fixtures import recorded_map_provider

        query=self._query('movie','5')
        answers=['False' if i%2 else 'True' for i in range(len(query.operation.records))]
        expected,messages=self._join_reference(query,answers)
        tokenizer=SimpleNamespace(apply_chat_template=lambda messages,**kwargs:json.dumps(messages))
        method,prepared,requests=prepare(query,tokenizer)
        ready=[]
        with recorded_map_provider(requests,answers) as cpu:
            report=execute_prepared_map(requests,model='CPU-only',sampling_params={'n':1},
                on_result_ready=lambda raw:ready.append(finish(query,method,prepared,raw['predictions'])[0]),
                before_generate=lambda *args:None)
        self._assert_result_equal(ready[0],expected)
        self.assertEqual([json.loads(row['prompt']) for row in requests],messages)

    def test_prepared_join_daft_driver_binds_native_packet_and_complete_sink(self):
        from src.baselines.text.sembench_prepared import result_value
        from src.experiments import native_daft_sembench as driver
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.shared_request_budget import SharedClaimedUnit
        from src.infrastructure.private_artifacts import read_json,publish_private_json

        query=self._query('movie','5')
        count=len(query.operation.records)
        answers=['False' if i%2 else 'True' for i in range(count)]
        expected,messages=self._join_reference(query,answers)
        config=dict(schema='semloom.native_daft_sembench.v1',sembench_checkout=str(query.checkout),
            data_dir=str(self.data),scenario='movie',query='5',query_id=query.query_id,
            model_name=query.model_name,model='CPU-only',sampling_params=query.generation.kwargs(),
            engine_kwargs={},provider_options={},query_timeout_s=60,cleanup_s=5,stop_grace_s=1,
            max_model_calls=count)
        def supervised(path,directory,**options):
            worker=read_json(path)
            requests=read_json(worker['requests_path'])['requests']
            self.assertEqual([json.loads(row['prompt']) for row in requests],messages)
            self.assertEqual(worker['sampling_params'],query.operation.generation.kwargs())
            value=worker['request_budget']
            unit=SharedClaimedUnit(Path(value['path']),AttemptBudget(value['budget_id'],value['limit']),value['unit_id'])
            for row in requests:unit.reserve(hashlib.sha256(row['call_id'].encode()).hexdigest())
            raw=dict(result_ready_monotonic_ns=time.monotonic_ns(),result=dict(predictions=[
                dict(call_id=row['call_id'],generated_text=text) for row,text in zip(requests,answers,strict=True)][::-1]))
            directory.mkdir()
            options['on_result_ready'](raw)
            publish_private_json(directory/'worker.json',dict(status='completed',native_api={},
                model_close_receipt=dict(status='closed')))
            return dict(status='CPU fixture')
        tokenizer=SimpleNamespace(apply_chat_template=lambda messages,**kwargs:json.dumps(messages))
        report=driver.run_query(config,self.data/'driver-result',model_python='CPU-only',
                                tokenizer=tokenizer,supervise=supervised)
        self.assertEqual(report['status'],'completed')
        self.assertEqual(report['result'],result_value(expected))
        self.assertEqual(report['logical_requests'],count)


    def test_prepared_join_empty_invalid_ids_and_native_hooks_are_preserved(self):
        import lotus
        from src.baselines.text.sembench_prepared import prepare,finish

        path=self.data/'Reviews.csv'
        original_csv=path.read_bytes()
        pd.DataFrame(columns=['reviewId','id','reviewText']).to_csv(path,index=False)
        query=self._query('movie','5')
        original=lotus.models.LM.__call__
        method,prepared,requests=prepare(query)
        self.assertIs(lotus.models.LM.__call__,original)
        actual,semantic=finish(query,method,prepared,[])
        self.assertTrue(actual.empty)
        self.assertEqual(requests,[])
        self.assertEqual(semantic['calls'],[])
        path.write_bytes(original_csv)
        query=self._query('movie','5')
        method,prepared,requests=prepare(query)
        predictions=[dict(call_id=row['call_id'],generated_text='True') for row in prepared]
        for invalid in (predictions[:-1],predictions+[predictions[0]],
                        [dict(row,call_id='unknown') for row in predictions]):
            with self.assertRaisesRegex(ValueError,'every unique original occurrence'):
                finish(query,method,prepared,invalid)
        self.assertIs(lotus.models.LM.__call__,original)

    def test_prepared_rectangular_join_keeps_different_answers_for_identical_pair_messages(self):
        from src.baselines.text.sembench_text import _operation
        from src.baselines.text.sembench_prepared import prepare,finish

        left=pd.DataFrame(dict(text=['same','same'],item=['L','L']),index=[10,20])
        right=pd.DataFrame(dict(text=['same']*3,item=['R']*3),index=[7,8,9])
        instruction='Compare {text:left} and {text:right}'
        operation=_operation('join',left,(right,instruction),{},model_name='openai/CPU-only',
                             generation=self.generation,query_id='rectangular')
        query=SimpleNamespace(operation=operation,query_id='rectangular',
            _original=lambda:left.sem_join(right,instruction))
        def execute(handler):
            report=handler(operation,query._original)
            return report['frame'],report
        query.execute=execute
        answers=['True','False','ambiguous','False','True','False']
        expected,messages=self._join_reference(query,answers)
        method,prepared,requests=prepare(query)
        actual,_=finish(query,method,prepared,[dict(call_id=row['call_id'],generated_text=text)
            for row,text in zip(prepared,answers,strict=True)][::-1])
        pd.testing.assert_frame_equal(actual,expected)
        self.assertEqual(len(prepared),6)
        self.assertEqual(len({row['call_id'] for row in prepared}),6)
        self.assertEqual(len({json.dumps(row['messages']) for row in requests}),1)

    def test_ecomm_text_filter_uses_the_same_task_and_original_score(self):
        from src.baselines.text.sembench_prepared import prepare,finish

        pd.DataFrame(dict(id=[1,2,3],productDisplayName=['Reebok backpack','other bag','other shoe'],
            productDescriptors=[{'description':{'value':text}} for text in ('Reebok backpack','other bag','other shoe')],
            articleType=[{'typeName':text} for text in ('Backpacks','Backpacks','Shoes')],
            brandName=['Reebok','Other','Other'])).to_parquet(self.data/'styles_details.parquet',index=False)
        query=self._query('ecomm','1')
        answers=['True','False','False']
        reference,messages=self._reference(query,answers)
        execute,_,_=self._chain(query,answers)
        result,_=query.execute(execute)
        method,prepared,_=prepare(query)
        restored,_=finish(query,method,prepared,[dict(call_id=row['call_id'],generated_text=text)
            for row,text in zip(prepared,answers,strict=True)])
        pd.testing.assert_series_equal(result,reference)
        pd.testing.assert_series_equal(restored,reference)
        self.assertEqual([json.loads(row['messages_json']) for row in prepared],messages[0][0])
        self.assertEqual(result.tolist(),[1])
        self.assertEqual(query.evaluate(result)['original_metric']['accuracy'],1)

    def _write_cars_inputs(self, scale=7):
        from src.baselines.text.sembench_cars import text_files

        cars, complaints, full_cars, full_complaints = text_files(scale)
        for name in (cars, complaints, full_cars, full_complaints):
            (self.data / name).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(dict(car_id=[1,2], year=[2010,2020])).to_csv(self.data / cars, index=False)
        pd.DataFrame(dict(car_id=[1,1,2,2], complaint_id=[101,102,103,104],
            summary=['crash and engine','engine second','engine fails','steering fails'])).to_csv(
                self.data / complaints, index=False)
        pd.DataFrame(dict(car_id=[1,2,3], year=[2010,2020,2000])).to_csv(self.data / full_cars, index=False)
        pd.DataFrame(dict(car_id=[1,1,2,2,3], complaint_id=[101,102,103,104,105],
            summary=['crash and engine','engine second','engine fails','steering fails','outside sample'],
            crash=[True,False,False,False,True], component_class=['ENGINE','ENGINE','ENGINE','STEERING','ENGINE'],
            private_label=['REFERENCE_ONLY_']*5)).to_csv(self.data / full_complaints, index=False)

    def test_cars_text_original_queries_reuse_record_native_and_prepared_semantics(self):
        from src.baselines.text.sembench_prepared import prepare, finish
        from src.baselines.text.sembench_cars import text_files
        from pandas.testing import assert_frame_equal, assert_series_equal

        self._write_cars_inputs()
        cases = [('1',['True','False','False','False']), ('4',['True','True','True','False']),
                 ('10',[json.dumps({'problem_category': value}) for value in ('ENGINE','ENGINE','ENGINE','STEERING')])]
        for key, answers in cases:
            with self.subTest(query=key):
                query = SemBenchTextQuery(CHECKOUT,self.data,'cars',key,execution_id='cars-'+key,
                    model_name='openai/local-fixture',generation=self.generation,scale_factor=7)
                expected, native_batches = self._reference(query, answers)
                execute, batches, _ = self._chain(query, answers)
                actual, _ = query.execute(execute)
                native_execute, requests, _, _, _ = self._native(query, answers, native_batches[0][0])
                native, _ = query.execute(native_execute)
                method, prepared, wire = prepare(query)
                predicted = [dict(call_id=row['call_id'],generated_text=text)
                    for row,text in zip(prepared,answers,strict=True)][::-1]
                restored, _ = finish(query,method,prepared,predicted)
                equal = assert_series_equal if isinstance(expected,pd.Series) else assert_frame_equal
                for result in (actual,native,restored):equal(result,expected)
                messages = [json.loads(row['messages_json']) for row in prepared]
                self.assertEqual(messages,native_batches[0][0])
                self.assertNotIn('REFERENCE_ONLY_',json.dumps(messages))
                self.assertNotIn('component_class',json.dumps(messages))
                self.assertEqual(len(wire),4)
                quality=query.evaluate(actual)['original_metric']
                if key == '1':
                    self.assertEqual(actual.tolist(),[1])
                    self.assertEqual(quality['f1_score'],1)
                elif key == '4':
                    self.assertAlmostEqual(actual.iloc[0,0],2026-(2010+2010+2020)/3)
                    self.assertAlmostEqual(quality['absolute_error'],abs(actual.iloc[0,0]-11))
                    self.assertGreater(quality['absolute_error'],0)
                else:
                    self.assertEqual(actual.car_id.tolist(),[1,1,2,2])
                    self.assertEqual(quality['f1_score'],1)
                execution_files=set(query.files)-query.evaluation_files
                self.assertNotIn(text_files(7)[-1],execution_files)

    def test_cars_authored_scale_changes_paths_not_model_cardinality_or_gold_labels(self):
        from src.baselines.text.sembench_cars import text_files

        for scale in (7,29):
            self._write_cars_inputs(scale)
            query=SemBenchTextQuery(CHECKOUT,self.data,'cars','4',execution_id='cars-scale',
                model_name='openai/local-fixture',generation=self.generation,scale_factor=scale)
            self.assertEqual(query.describe()['expected_model_calls'],4)
            self.assertIn(text_files(scale)[0],query.files)
            execute, _, _ = self._chain(query,['True']*4)
            original=query._read_file
            reads=[]
            def read(path):
                reads.append(path.relative_to(self.data).as_posix())
                return original(path)
            with patch.object(query,'_read_file',side_effect=read):
                query.execute(execute)
            self.assertEqual(reads,list(text_files(scale)[:2]))
        for scale in (0,-1,True,1.5):
            with self.subTest(scale=scale),self.assertRaisesRegex(ValueError,'Cars scale_factor'):
                text_files(scale)

    def test_cars_q1_needs_only_text_inputs_and_uses_selected_full_labels(self):
        from src.baselines.text.sembench_cars import text_files

        self._write_cars_inputs()
        cars, complaints, full_cars, full_complaints=text_files(7)
        (self.data/cars).unlink()
        (self.data/full_cars).unlink()
        query=SemBenchTextQuery(CHECKOUT,self.data,'cars','1',execution_id='cars-text-only',
            model_name='openai/local-fixture',generation=self.generation,scale_factor=7)
        self.assertEqual(set(query.files),{complaints,full_complaints})
        execute, _, _=self._chain(query,['True','False','False','False'])
        result,_=query.execute(execute)
        self.assertEqual(query.evaluate(result)['original_metric']['f1_score'],1)

    def test_cars_empty_extract_keeps_original_missing_output_column_error(self):
        from src.baselines.text.sembench_cars import text_files

        self._write_cars_inputs()
        complaints=self.data/text_files(7)[1]
        pd.read_csv(complaints).iloc[:0].to_csv(complaints,index=False)
        query=SemBenchTextQuery(CHECKOUT,self.data,'cars','10',execution_id='cars-empty',
            model_name='openai/local-fixture',generation=self.generation,scale_factor=7)
        execute,batches,_=self._chain(query,[])
        with self.assertRaises(KeyError):query.execute(execute)
        self.assertEqual(batches,[])

    def _reference(self, query, answers):
        """Run the complete original query and original accessor with no provider."""
        import lotus
        from src.semantic_methods.lotus.engine import engine_response

        method = LotusRecordMethod(query.operation.stage)
        batches, cursor = [], 0

        def respond(data, options, *_args):
            nonlocal cursor
            messages = [deepcopy(message) for message, _ in data]
            batches.append((messages, deepcopy(options)))
            replies = answers[cursor:cursor + len(messages)]
            self.assertEqual(len(replies), len(messages))
            cursor += len(messages)
            return [engine_response(_reply(dict(call_id=str(cursor - len(messages) + index)), text), method.lm)
                    for index, text in enumerate(replies)]

        method.lm._process_uncached_messages = respond
        with lotus.settings.context(lm=method.lm, enable_cache=False):
            result = query._original()
        self.assertEqual(cursor, len(answers))
        return result, batches

    def _chain(self, query, answers):
        batches, reports = [], []

        def execute(operation, invoke):
            def infer(prepared, *, generation):
                batches.append((deepcopy(prepared), asdict(generation)))
                return [_reply(row, answers[row["source_position"]]) for row in prepared]

            chain = LotusRecordChain((operation.stage,), query_id=query.query_id)
            output = chain.run_batch(list(operation.records), infer,
                columns=operation.describe()["model_columns"])
            report = dict(frame=operation.result_frame(output["records"]), calls=output["calls"])
            reports.append(report)
            return report

        return execute, batches, reports

    def _native(self, query, answers, expected_messages):
        from src.baselines.text.frameworks import lotus_native_local as native
        from src.baselines.text.frameworks.lotus_native_stage import execute_native

        by_message = {}
        for message, answer in zip(expected_messages, answers, strict=True):
            key = _messages_key(message)
            if key in by_message:
                self.assertEqual(by_message[key], answer, "duplicate conversations need consistent fixed replies")
            by_message[key] = answer
        requests, reports, transports = [], [], []

        class Transport:
            def __init__(self, *_args, **_kwargs):
                self.ready_report = dict(engine_uid="software-query", inference_calls=0)
                self.events, self.lock = [], threading.Lock()
                transports.append(self)

            def submit(self, row, *, generation, deadline_monotonic):
                with self.lock:
                    request_id = "request-" + str(len(requests))
                    requests.append((dict(row), generation))
                    identity = _prepared([row])[0][0]
                    receipt = result_record(identity, engine_uid="software-query", start=time.monotonic(),
                        end=time.monotonic(), status="succeeded", submitted=True)
                    receipt.update(engine_request_id=request_id,
                        text=by_message[_messages_key(json.loads(row["messages_json"]))],
                        finish_reason="stop", prompt_tokens=11, completion_tokens=2, total_tokens=13)
                    self.events.append(TerminalEvent(request_id, identity, receipt, True))
                    return SubmitResult("queued", request_id)

            def poll(self, maximum):
                with self.lock:
                    result, self.events = self.events[:maximum], self.events[maximum:]
                    return result

            def wait(self, timeout):
                return None

            def abort(self, request_id):
                return None

            def close(self, timeout):
                return dict(status="closed", process_exit_confirmed=True, cleanup_errors=[])

        allowance = ClaimedUnit(Reservation(query.query_id, 1, max(1, len(answers))), None)

        def execute(operation, invoke):
            with patch.object(native, "AsyncEngineProcess", Transport):
                report = execute_native(operation.frame, operation.stage, operation.records, invoke=invoke,
                    engine=EngineSpec("/tmp/software-query-fixture", sys.executable, "/tmp/driver",
                        max_model_calls=max(1, len(answers))),
                    resources=RecordsResources(query_timeout_s=3, startup_timeout_s=3),
                    query_id=query.query_id, profile=dict(native_lm_max_batch_size=2), budget=allowance)
            reports.append(report)
            return report

        return execute, requests, reports, allowance, transports

    def _assert_result_equal(self, actual, expected):
        if isinstance(expected, pd.Series):
            self.assertIsInstance(actual, pd.Series)
            pd.testing.assert_series_equal(actual, expected)
        else:
            pd.testing.assert_frame_equal(actual, expected)

    def _parity(self, query, answers, *, error=None):
        self.assertEqual(len(query.operation.records), len(answers))
        self.assertEqual([row.source_position for row in query.operation.records], list(range(len(answers))))
        self.assertEqual([row.source_index for row in query.operation.records], query.operation.frame.index.tolist())
        for record in query.operation.records:
            self.assertEqual(set(record.values), set(query.operation.describe()["model_columns"]))
        # The reference callback remains observable even when the original
        # relational sink raises after a successful Extract.
        reference_batches = []
        if error is None:
            expected, reference_batches = self._reference(query, answers)
        else:
            import lotus
            from src.semantic_methods.lotus.engine import engine_response
            method = LotusRecordMethod(query.operation.stage)

            def respond(data, options, *_args):
                messages = [deepcopy(message) for message, _ in data]
                reference_batches.append((messages, deepcopy(options)))
                return [engine_response(_reply(dict(call_id=str(i)), answer), method.lm)
                        for i, answer in enumerate(answers)]

            method.lm._process_uncached_messages = respond
            with lotus.settings.context(lm=method.lm, enable_cache=False), self.assertRaises(error) as caught:
                query._original()
            reference_error = str(caught.exception)
        messages = [message for batch, _ in reference_batches for message in batch]
        self.assertEqual(len(messages), len(answers))
        chain, chain_batches, chain_reports = self._chain(query, answers)
        if error is None:
            result, _ = query.execute(chain)
            self._assert_result_equal(result, expected)
        else:
            with self.assertRaises(error) as caught:
                query.execute(chain)
            self.assertEqual(str(caught.exception), reference_error)
            self.assertIs(caught.exception.operator_report, chain_reports[0])
        prepared = [row for batch, _ in chain_batches for row in batch]
        self.assertEqual([json.loads(row["messages_json"]) for row in prepared], messages)
        self.assertEqual([row["row_id"] for row in prepared], [row.row_id for row in query.operation.records])
        native, requests, reports, allowance, transports = self._native(query, answers, messages)
        if error is None:
            result, _ = query.execute(native)
            self._assert_result_equal(result, expected)
        else:
            with self.assertRaises(error) as caught:
                query.execute(native)
            self.assertEqual(str(caught.exception), reference_error)
            self.assertIs(caught.exception.operator_report, reports[0])
        self.assertEqual(len(requests), len(answers))
        self.assertEqual(allowance.attempts, len(answers))
        self.assertEqual(len(transports), int(bool(answers)))
        self.assertEqual(reports[0]["calls"]["counts"]["succeeded"], len(answers))
        self.assertEqual(reports[0]["backend_close"]["status"], "closed")
        pd.testing.assert_frame_equal(chain_reports[0]["frame"], reports[0]["frame"])
        calls = sorted(reports[0]["calls"]["records"], key=lambda row: row["source_position"])
        self.assertEqual([row["row_id"] for row in calls], [row.row_id for row in query.operation.records])
        self.assertEqual([row["messages_sha256"] for row in calls],
            [hashlib.sha256(_messages_key(message).encode()).hexdigest() for message in messages])
        self.assertTrue(all(generation.response_format == query.operation.stage.generation.response_format
                            for _, generation in requests))
        self.assertTrue(all(generation == query.operation.stage.generation for _, generation in requests))
        self.assertTrue(all(generation == asdict(query.operation.stage.generation)
                            for _, generation in chain_batches))
        for _, options in reference_batches:
            self.assertEqual(options["max_completion_tokens"], query.operation.stage.generation.max_tokens)
            self.assertEqual(options.get("response_format"), query.operation.stage.generation.response_format)
            self.assertEqual(options.get('stop'), list(query.operation.stage.generation.stop))
        return (expected if error is None else None), chain_reports[0], reports[0]

    def test_upstream_src_runner_aliases_are_scoped_and_original_bytes_stay_unchanged(self):
        from src.baselines.text.sembench_source import load_module

        source = Path(CHECKOUT) / "src/scenario/mmqa/runner/lotus_runner/lotus_runner.py"
        original_bytes = source.read_bytes()
        module = load_module(CHECKOUT, "sembench_mmqa_original_test",
                             "src/scenario/mmqa/runner/lotus_runner/lotus_runner.py")
        self._assert_aliases_restored()
        canonical = importlib.import_module("runner.generic_lotus_runner.generic_lotus_runner")
        self.assertIs(module.GenericLotusRunner, canonical.GenericLotusRunner)
        self.assertEqual(source.read_bytes(), original_bytes)

    def test_movie_q123_keep_full_filter_occurrences_before_limit_or_count(self):
        reviews = pd.read_csv(self.data / 'Reviews.csv')
        extra = reviews.iloc[[0, 2, 3, 0]].copy()
        reviews = pd.concat([reviews, extra], ignore_index=True)
        reviews.to_csv(self.data / 'Reviews.csv', index=False)
        instructions = {}
        for key in ('1', '2', '3'):
            with self.subTest(query=key), redirect_stdout(io.StringIO()):
                query = self._query('movie', key)
                selected = reviews if key == '1' else reviews[reviews.id == 'taken_3']
                self.assertEqual(len(query.operation.records), len(selected))
                self.assertGreater(len(selected), 5)
                self.assertEqual(query.operation.frame.index.tolist(), selected.index.tolist())
                self.assertGreater(len(selected), selected.reviewId.nunique())
                instructions[key] = query.operation.stage.method.instruction
                result, chain, native = self._parity(query, ['True'] * len(selected))
                self.assertEqual(len(chain['calls']), len(selected))
                self.assertEqual(len(native['calls']['records']), len(selected))
                if key in ('1', '2'):
                    self.assertEqual(result.reviewId.tolist(), selected.head(5).reviewId.tolist())
                    self.assertEqual(len(result), 5)
                else:
                    self.assertEqual(result.positive_review_cnt.tolist(), [len(selected)])
                if key in ('1', '2'):
                    self.assertEqual(query.evaluate(result)['supplement']['returned_rows'], 5)
        self.assertEqual(instructions['1'], instructions['2'])
        self.assertNotEqual(instructions['2'], instructions['3'])

    def test_movie_q123_all_negative_keep_original_empty_or_zero_sink(self):
        for key in ('1', '2', '3'):
            with self.subTest(query=key), redirect_stdout(io.StringIO()):
                query = self._query('movie', key)
                result, _, _ = self._parity(query, ['False'] * len(query.operation.records))
                if key in ('1', '2'):
                    self.assertEqual(result.columns.tolist(), ['reviewId', 'movieId', 'reviewText'])
                    self.assertTrue(result.empty)
                else:
                    self.assertEqual(result.positive_review_cnt.tolist(), [0])

    def test_movie_q9_topk_keeps_native_full_ranking_separate_from_default_map(self):
        import lotus
        import numpy as np
        from lotus.types import LMOutput
        from src.baselines.text.frameworks.lotus_native_topk import make_lm

        rng = np.random.get_state()
        self.addCleanup(np.random.set_state, rng)
        default = self._query('movie', '9')
        query = SemBenchTextQuery(CHECKOUT, self.data, 'movie', '9',
            execution_id='software-movie-topk', model_name='openai/local-fixture',
            generation=self.generation, ranking='topk')
        self.assertEqual(default.describe()['ranking'], 'map')
        self.assertEqual(default.operation.stage.kind, 'map')
        self.assertEqual(query.operation.describe()['kind'], 'topk')
        self.assertEqual(query.operation.frame.index.tolist(), default.operation.frame.index.tolist())
        lm = make_lm(query.operation, 2)
        prompts = []

        def answer(current, messages, **options):
            prompts.extend(messages)
            return LMOutput(outputs=['Document 1'] * len(messages))

        def execute(operation, invoke):
            with lotus.settings.context(lm=lm, enable_cache=False), patch.object(type(lm), '__call__', answer):
                frame, stats = invoke()
            return dict(frame=frame, native_stats=stats)

        with redirect_stdout(io.StringIO()):
            result, operator = query.execute(execute)
        count = len(query.operation.records)
        self.assertEqual(len(prompts), count * (count - 1) // 2)
        self.assertEqual(operator['native_stats']['total_llm_calls'], len(prompts))
        self.assertCountEqual(result.reviewId.tolist(), query.operation.frame.reviewId.tolist())
        self.assertEqual(result.reviewScore.tolist(), [5.0, 4.0, 3.0, 2.0, 1.0])

    def test_all_sixteen_original_queries_match_native_accessor_and_record_chain(self):
        movie = {
            "4": ["True", "True", "False"],
            "8": ["POSITIVE", "POSITIVE", "NEGATIVE"],
            "9": ["5", "1", "not-a-number", "9", "NaN"],
            "10": ["5", "not-a-number", "5", "9", "1", "NaN",
                   "5", "1", "not-a-number", "9", "NaN"],
        }
        mmqa = {"1": [json.dumps(dict(director=value)) for value in
                       ("Unused Director", "Expected Director", "Another Director")],
                "4": [json.dumps(dict(genres=value)) for value in
                       ("comedy", "comedy, drama", "comedy", "horror")]}
        for query in ("3a", "3b", "3c", "3d", "3e", "3f", "3g"):
            mmqa[query] = ["False", "True", "False", "True", "True", "False"]
        for query in ("6a", "6b", "6c"):
            mmqa[query] = ["True", "False", "True"]
        instructions = {}
        with redirect_stdout(io.StringIO()):
            for scenario, cases in (("movie", movie), ("mmqa", mmqa)):
                for key, answers in cases.items():
                    with self.subTest(scenario=scenario, query=key):
                        query = self._query(scenario, key)
                        result, _, _ = self._parity(query, answers)
                        if query.operation.stage.kind == "filter":
                            instructions[(scenario, key)] = query.operation.stage.method.instruction
                        if scenario == "movie":
                            labels={"scoreSentiment","score","originalScore"}
                            self.assertTrue(labels<=set(query.operation.frame.columns))
                            self.assertTrue(labels.isdisjoint(query.operation.describe()['model_columns']))
                        if (scenario, key) == ("movie", "4"):
                            self.assertAlmostEqual(result["positivity_ratio"].iloc[0], 2 / 3)
                        elif (scenario, key) == ("movie", "8"):
                            self.assertEqual(dict(zip(result["scoreSentiment"], result["count"])),
                                             {"NEGATIVE": 1, "POSITIVE": 2})
                        elif (scenario, key) == ("movie", "9"):
                            self.assertEqual(query.describe()['ranking'], 'map')
                            self.assertEqual(result['reviewScore'].tolist(), [5.0, 1.0, 3.0, 3.0, 3.0])
                            self.assertEqual(result['reviewId'].tolist(),
                                ['ant-duplicate', 'ant-negative', 'ant-duplicate', 'ant-range', 'ant-nan'])
                        elif (scenario, key) == ("movie", "10"):
                            self.assertEqual(dict(zip(result["movieId"], result["movieScore"])),
                                             {"other_movie": 2.33, "taken_3": 4.33,
                                              'ant_man_and_the_wasp_quantumania': 3.0})
                        elif (scenario, key) == ("mmqa", "1"):
                            self.assertIsInstance(result, pd.Series)
                            self.assertEqual(result.name, "director")
                            self.assertEqual(result.tolist(), ["Expected Director"])
                        elif scenario == "mmqa" and key.startswith("6"):
                            self.assertTrue(pd.isna(query.operation.frame.iloc[1]["Destinations"]))
                            self.assertEqual(query.operation.records[1].values["Destinations"], "nan")
        self.assertEqual(len({instructions[("mmqa", "3" + suffix)] for suffix in "abcdefg"}), 7)
        self.assertEqual(len({instructions[("mmqa", "6" + suffix)] for suffix in "abc"}), 3)

    @unittest.skipUnless(REAL_MMQA_DATA and Path(REAL_MMQA_DATA).is_dir(),
                         "requires SEMLOOM_NATIVE_TEST_MMQA_DATA with author-generated sf25 text CSVs")
    def test_real_sf25_airport_null_cell_matches_original_messages_without_changing_data(self):
        self.data = Path(REAL_MMQA_DATA).resolve()
        path = self.data / "tampa_international_airport.csv"
        original_bytes = path.read_bytes()
        for key in ("6a", "6b", "6c"):
            with self.subTest(query=key):
                query = self._query("mmqa", key)
                self.assertEqual(len(query.operation.records), 25)
                missing = query.operation.frame["Destinations"].isna()
                self.assertEqual(int(missing.sum()), 1)
                position = int(missing.to_numpy().nonzero()[0][0])
                self.assertEqual(query.operation.records[position].values["Destinations"], "nan")
                result, _, _ = self._parity(query, ["True"] * 25)
                self.assertEqual(result["Airlines"].tolist(), query.operation.frame["Airlines"].tolist())
                self.assertTrue(pd.isna(query.operation.frame.iloc[position]["Destinations"]))
                self.assertEqual(path.read_bytes(), original_bytes)

    def test_mmqa_preserves_full_extract_input_and_noncontinuous_original_index(self):
        q1, q4 = self._query("mmqa", "1"), self._query("mmqa", "4")
        self.assertEqual(len(q1.operation.records), 3, "Extract precedes the Bob Whitewood selection")
        self.assertEqual(q4.operation.frame.index.tolist(), [1, 3, 4, 5])
        self.assertEqual([r.source_index for r in q4.operation.records], [1, 3, 4, 5])
        self.assertTrue(all(set(r.values) == {"text"} for r in q4.operation.records))
        self.assertEqual(q4.operation.frame["row_id"].tolist(), [5, 11, 11, 12])
        self.assertEqual(len({r.row_id for r in q4.operation.records}), 4)

    def test_extract_retains_actual_extra_fields_and_title_collision_before_original_grouping(self):
        query = self._query("mmqa", "4")
        answers = [json.dumps(dict(genres=genre, title=title, undeclared="retained"))
            for genre, title in (("comedy", "Replacement Fixture"), ("drama", "Orange County"),
                                 ("comedy", "Replacement Fixture"), ("horror", "Cobweb"))]
        with redirect_stdout(io.StringIO()):
            result, chain, native = self._parity(query, answers)
        self.assertEqual(chain["frame"]["undeclared"].tolist(), ["retained"] * 4)
        self.assertEqual(native["frame"].loc[1, "title"], "Replacement Fixture")
        self.assertIn("Replacement Fixture", result.loc[result["genre"] == "comedy", "movies_in_genre"].iloc[0])

    def test_missing_extract_keys_preserve_original_keyerror_after_known_successful_calls(self):
        for key, count in (("1", 3), ("4", 4)):
            with self.subTest(query=key), redirect_stdout(io.StringIO()):
                _, chain, native = self._parity(self._query("mmqa", key), ["{}"] * count, error=KeyError)
                self.assertEqual(len(chain["calls"]), count)
                self.assertEqual(native["calls"]["counts"]["succeeded"], count)
                self.assertNotIn("director" if key == "1" else "genres", native["frame"].columns)

    def test_empty_map_and_filter_keep_original_output_shapes_without_model_start(self):
        reviews = pd.read_csv(self.data / "Reviews.csv")
        reviews = reviews[reviews["id"] != "taken_3"]
        reviews.to_csv(self.data / "Reviews.csv", index=False)
        query = self._query("movie", "4")
        self.assertIsNone(query.operation)
        execute = Mock(side_effect=AssertionError("empty Q4 must not emit an operator"))
        result, report = query.execute(execute)
        self.assertIsNone(report)
        self.assertEqual(result["positivity_ratio"].tolist(), [0.0])
        execute.assert_not_called()
        query = self._query("movie", "8")
        self.assertEqual(query.operation.describe()["model_columns"], ["reviewText"])
        result, _, native = self._parity(query, [])
        self.assertEqual(dict(zip(result["scoreSentiment"], result["count"])), {"NEGATIVE": 0, "POSITIVE": 0})
        self.assertIn("_map", native["frame"].columns)
        self.assertNotIn("owner_ready", native)
        query = self._query("mmqa", "3a")
        self._parity(query, ["False"] * len(query.operation.records))

    def test_empty_extract_does_not_invent_declared_output_columns(self):
        for filename in ("ben_piazza_text_data.csv", "lizzy_caplan_text_data.csv"):
            pd.read_csv(self.data / filename).iloc[:0].to_csv(self.data / filename, index=False)
        for key in ("1", "4"):
            with self.subTest(query=key), redirect_stdout(io.StringIO()):
                query = self._query("mmqa", key)
                self.assertEqual(query.operation.describe()["model_columns"], ["text"])
                _, _, native = self._parity(query, [], error=KeyError)
                self.assertNotIn("owner_ready", native)

    def test_medical_original_queries_match_native_accessor_and_record_chain(self):
        answers = {'1': ['True', 'False', 'False', 'False'],
                   '4': ['True', 'True', 'False', 'False'],
                   '10': [json.dumps(dict(text_diagnosis=value)) for value in
                          ('ALLERGY', 'ACNE', 'ACNE', 'MIGRAINE')]}
        for key, responses in answers.items():
            with self.subTest(query=key), redirect_stdout(io.StringIO()):
                query = self._query('medical', key)
                result, _, _ = self._parity(query, responses)
                self.assertEqual(query.describe()['scale_factor'], 11112)
                self.assertEqual(query.describe()['expected_model_calls'], 4)
                self.assertEqual(query.operation.describe()['model_columns'], ['symptoms'])
                self.assertNotIn('text_diagnosis', query.operation.frame.columns)
                self.assertTrue(all(set(row.values) == {'symptoms'} for row in query.operation.records))
                instruction = (query.operation.stage.method.output_columns['text_diagnosis'] if key == '10'
                               else query.operation.stage.method.instruction)
                self.assertIn('The results are not used for human health evaluation', instruction)
                quality = query.evaluate(result)['original_metric']
                if key == '1':
                    self.assertIsInstance(result, pd.Series)
                    self.assertEqual(result.name, 'patient_id')
                    self.assertEqual(result.tolist(), [10])
                    self.assertEqual(quality, dict(precision=1.0, recall=1.0, f1_score=1.0))
                elif key == '4':
                    self.assertEqual(result['average_acne_age'].tolist(), [52.0])
                    self.assertEqual(query.operation.frame.index.tolist(), [1, 1, 2, 4])
                    self.assertEqual(quality['absolute_error'], 0.0)
                else:
                    self.assertEqual(result.columns.tolist(), ['patient_id', 'text_diagnosis'])
                    self.assertEqual(quality, dict(precision=1.0, recall=1.0, f1_score=1.0))

    def test_medical_q4_keeps_duplicate_patient_rows_and_original_join_labels(self):
        path = self.data / 'data/patient_data.csv'
        patients = pd.read_csv(path)
        additional = patients.loc[patients.patient_id == 30].copy()
        additional['age'] = 54
        pd.concat((patients, additional), ignore_index=True).to_csv(path, index=False)
        query = self._query('medical', '4')
        self.assertEqual(query.operation.frame.index.tolist(), [1, 1, 5, 2, 4, 5])
        self.assertEqual([row.source_index for row in query.operation.records], [1, 1, 5, 2, 4, 5])
        result, _, _ = self._parity(query, ['True', 'True', 'True', 'False', 'False', 'True'])
        self.assertEqual(result['average_acne_age'].tolist(), [53.0])

    def test_medical_all_negative_and_empty_filters_keep_native_series_and_nan(self):
        for empty in (False, True):
            if empty:
                path = self.data / 'data/text_symptoms_data.csv'
                pd.read_csv(path).iloc[:0].to_csv(path, index=False)
            for key in ('1', '4'):
                with self.subTest(empty=empty, query=key):
                    query = self._query('medical', key)
                    result, _, native = self._parity(query, ['False'] * len(query.operation.records))
                    if key == '1':
                        self.assertIsInstance(result, pd.Series)
                        self.assertEqual(result.name, 'patient_id')
                        self.assertEqual(len(result), 0)
                    else:
                        self.assertTrue(pd.isna(result['average_acne_age'].iloc[0]))
                    if empty:
                        self.assertNotIn('owner_ready', native)

    def test_medical_q10_missing_keys_and_empty_extract_preserve_original_keyerror(self):
        query = self._query('medical', '10')
        _, chain, native = self._parity(query, ['{}'] * 4, error=KeyError)
        self.assertEqual(len(chain['calls']), 4)
        self.assertEqual(native['calls']['counts']['succeeded'], 4)
        self.assertNotIn('text_diagnosis', native['frame'].columns)
        path = self.data / 'data/text_symptoms_data.csv'
        pd.read_csv(path).iloc[:0].to_csv(path, index=False)
        _, _, native = self._parity(self._query('medical', '10'), [], error=KeyError)
        self.assertNotIn('owner_ready', native)

    def test_medical_q10_evaluator_preserves_missing_result_length_error(self):
        query = self._query('medical', '10')
        with self.assertRaisesRegex(ValueError, 'inconsistent numbers of samples'):
            query.evaluate(pd.DataFrame(dict(patient_id=[10], text_diagnosis=['ALLERGY'])))

    def test_medical_q10_keeps_partial_json_and_actual_extra_or_collision_keys(self):
        query = self._query('medical', '10')
        result, _, _ = self._parity(query,
            ['{"text_diagnosis":"ALLERGY"}', '{}', '{}', '{"text_diagnosis":"MIGRAINE"}'])
        self.assertEqual(result.text_diagnosis.tolist(), ['ALLERGY', None, None, 'MIGRAINE'])
        result, chain, native = self._parity(query,
            ['{"text_diagnosis":"ALLERGY"}',
             '{"text_diagnosis":"ACNE","patient_id":44,"extra":"retained"}',
             '{"text_diagnosis":"ACNE","patient_id":44,"extra":"retained"}',
             '{"text_diagnosis":"MIGRAINE"}'])
        self.assertEqual(result.patient_id.tolist(), [10, '44', '44', 70])
        self.assertEqual(result.columns.tolist(), ['patient_id', 'text_diagnosis'])
        self.assertEqual(chain['frame'].extra.tolist(), [None, 'retained', 'retained', None])
        pd.testing.assert_frame_equal(chain['frame'], native['frame'])

    def test_medical_nondefault_scale_uses_original_suffixed_csv_paths(self):
        for path in (self.data / 'data').glob('*.csv'):
            path.rename(path.with_name(path.stem + '_25.csv'))
        query = SemBenchTextQuery(CHECKOUT, self.data, 'medical', '1', execution_id='medical-sf25',
            model_name='openai/local-fixture', generation=self.generation, scale_factor=25)
        result, _, _ = self._parity(query, ['True', 'False', 'False', 'False'])
        self.assertEqual(result.tolist(), [10])
        self.assertEqual(query.describe()['scale_factor'], 25)
        self.assertEqual(query.evaluate(result)['original_metric']['f1_score'], 1.0)

    def test_medical_labels_stay_outside_execution_scan_and_input_mutation_is_rejected(self):
        query = self._query('medical', '1')
        (self.data / 'data/patient_data_with_labels.csv').unlink()
        execute, _, _ = self._chain(query, ['True', 'False', 'False', 'False'])
        result, _ = query.execute(execute)
        self.assertEqual(query.evaluate(result)['original_metric']['f1_score'], 1.0)
        path = self.data / 'data/text_symptoms_data.csv'
        path.write_text(path.read_text() + '\n')
        with self.assertRaisesRegex(ValueError, 'changed after preflight'):
            query.execute(Mock(side_effect=AssertionError('changed input must not reach model execution')))

    def test_medical_configured_generation_reaches_record_and_native_model_boundary(self):
        from src.experiments.sembench_text_query import configuration
        config = dict(schema='semloom.sembench_text_run.v1', scenario='medical', query='10',
            query_id='medical-generation', backend='lotus-native', model_name='openai/local-fixture',
            generation={}, engine=dict(model_path='/tmp/model', model_python='/tmp/python',
                driver_python='/tmp/python', max_model_calls=4), resources={},
            profile=dict(native_lm_max_batch_size=2))
        path = self.data / 'config.json'
        for options in ({}, dict(max_tokens=128, stop=['DONE'], temperature=0.25, seed=7)):
            with self.subTest(generation=options):
                config['generation'] = options
                path.write_text(json.dumps(config))
                _, _, generation, _ = configuration(path)
                query = SemBenchTextQuery(CHECKOUT, self.data, 'medical', '10',
                    execution_id='medical-generation', model_name='openai/local-fixture', generation=generation)
                self._parity(query, ['{"text_diagnosis":"ALLERGY"}'] * 4)

    @unittest.skipUnless(REAL_MEDICAL_DATA and Path(REAL_MEDICAL_DATA).is_dir(),
                         'requires SEMLOOM_NATIVE_TEST_MEDICAL_DATA with authored full Medical CSVs')
    def test_real_medical_inputs_match_original_messages_results_and_evaluator(self):
        self.data = Path(REAL_MEDICAL_DATA).resolve()
        symptoms = pd.read_csv(self.data / 'data/text_symptoms_data.csv')
        labels = pd.read_csv(self.data / 'data/patient_data_with_labels.csv').set_index('patient_id')
        original = {path: path.read_bytes() for path in (self.data / 'data').glob('*.csv')}
        self.assertEqual(len(symptoms), 1200)
        self.assertEqual(len(labels), 11112)
        self.assertEqual(int((labels.text_diagnosis == 'allergy').sum()), 50)
        self.assertEqual(int((labels.text_diagnosis == 'acne').sum()), 50)
        for key in ('1', '4', '10'):
            with self.subTest(query=key):
                query = self._query('medical', key)
                self.assertEqual(query.describe()['expected_model_calls'], 1200)
                # Oracle labels provide fixed software replies only in this
                # test. Execution records retain the original symptoms alone.
                diagnoses = [labels.loc[patient_id, 'text_diagnosis']
                             for patient_id in query.operation.frame.patient_id]
                answers = ([str(diagnosis == ('allergy' if key == '1' else 'acne'))
                            for diagnosis in diagnoses] if key != '10' else
                           [json.dumps(dict(text_diagnosis=diagnosis.upper())) for diagnosis in diagnoses])
                result, _, _ = self._parity(query, answers)
                metric = query.evaluate(result)['original_metric']
                self.assertEqual(metric['absolute_error'] if key == '4' else metric['f1_score'],
                                 0.0 if key == '4' else 1.0)
        self.assertEqual(original, {path: path.read_bytes() for path in original})


if __name__ == "__main__":
    unittest.main()
