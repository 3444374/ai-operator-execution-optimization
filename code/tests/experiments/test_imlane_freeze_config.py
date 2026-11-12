"""Original source/SDK/tokenizer input freezing, without any model owner."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pandas as pd
import pyarrow as pa

from src.experiments import native_imlane_filters as entry
from src.baselines.text.sembench_prepared import finish_relation
from integrations.imlane_duckdb import mmqa_filter_udf as kernel
from tests.experiments.test_native_imlane_filters import config

CHECKOUT = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')
TOKENIZER = os.environ.get('SEMLOOM_NATIVE_TEST_TOKENIZER')


@unittest.skipUnless(CHECKOUT and TOKENIZER, 'set pinned source and local tokenizer paths')
class FreezeConfigTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        pd.DataFrame({'title': ['duplicate', 'duplicate', 'other'],
                      'text': ['a comedy', 'a comedy', 'line one\nline two 好']}).to_csv(
            self.root / 'lizzy_caplan_text_data.csv', index=False)
        self.seed = config()
        for name in ('input_sha256', 'prompt_columns', 'max_model_calls'):
            self.seed.pop(name)
        self.seed.update(query_id='3a', data_dir=str(self.root), sembench_checkout=CHECKOUT,
                         tokenizer=TOKENIZER)

    def test_first_raw_order_source_count_and_original_full_messages_tokens(self):
        observed = []
        original = entry.prepare
        def capture(query, **kwargs):
            result = original(query, **kwargs)
            observed.append(list(result[0].columns))
            return result
        with patch.object(entry, 'prepare', side_effect=capture):
            frozen = entry.freeze_config(self.seed)
        self.assertEqual(observed, [frozen['prompt_columns']])
        self.assertEqual(set(frozen['prompt_columns']), {'title', 'text'})
        self.assertEqual(frozen['max_model_calls'], 3)
        self.assertEqual(frozen['input_sha256'], {'lizzy_caplan_text_data.csv': hashlib.sha256(
            (self.root / 'lizzy_caplan_text_data.csv').read_bytes()).hexdigest()})
        query, method, prepared, requests, tokens, _rows = entry.prepare_query(frozen)
        from lotus.templates.task_instructions import df2multimodal_info, filter_formatter
        from lotus.nl_expression import nle2str
        from transformers import AutoTokenizer
        messages = [filter_formatter(method.lm, doc, nle2str(method.method.instruction, method.columns))
                    for doc in df2multimodal_info(query.operation.frame, method.columns)]
        self.assertEqual([row['messages'] for row in requests], messages)
        self.assertEqual([json.loads(row['messages_json']) for row in prepared], messages)
        self.assertIn('[Title]:', str(messages))
        self.assertIn('[Text]:', str(messages))
        tokenizer = AutoTokenizer.from_pretrained(TOKENIZER, local_files_only=True, trust_remote_code=False)
        self.assertEqual(tokens, [tokenizer.apply_chat_template(m, tokenize=True,
            add_generation_prompt=True) for m in messages])
        with self.assertRaisesRegex(ValueError, 'original prompt columns'):
            entry.prepare_query(dict(frozen, prompt_columns=['Title', 'Text']))
        with self.assertRaisesRegex(ValueError, 'every original occurrence'):
            entry.prepare_query(dict(frozen, max_model_calls=25))
        (self.root / 'lizzy_caplan_text_data.csv').write_text('title,text\nchanged,data\n')
        with self.assertRaisesRegex(ValueError, 'source files changed'):
            entry.prepare_query(frozen)

    def test_cli_config_consumed_unchanged_across_python_hash_seeds(self):
        seed_path = self.root / 'seed.json'
        seed_path.write_text(json.dumps(self.seed))
        entry.main(['freeze-config', '--config', str(seed_path), '--output', str(self.root / 'frozen')])
        config_path = self.root / 'frozen/CONFIG.json'
        script = '''import json,sys
from src.experiments.native_imlane_filters import prepare_query
q,m,p,r,t,rows=prepare_query(json.load(open(sys.argv[1])))
print(json.dumps(dict(columns=m.columns,requests=r,tokens=t)))'''
        results = []
        for seed in ('1', '42'):
            completed = subprocess.run([sys.executable, '-B', '-c', script, str(config_path)],
                env=dict(os.environ, PYTHONHASHSEED=seed), text=True, capture_output=True, timeout=90,
                check=True)
            results.append(json.loads(completed.stdout.strip().splitlines()[-1]))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0]['columns'], json.loads(config_path.read_text())['prompt_columns'])
        with self.assertRaises(FileExistsError):
            entry.main(['freeze-config', '--config', str(seed_path), '--output', str(self.root / 'frozen')])

    def assert_native_filter(self, seed):
        frozen = entry.freeze_config(seed)
        query, method, prepared, requests, tokens, rows = entry.prepare_query(frozen)
        self.assertEqual(frozen['max_model_calls'], len(query.operation.records))
        self.assertEqual([r['title'] for r in rows], [str(r.source_index) for r in query.operation.records])
        self.assertEqual([json.loads(r['text']) for r in rows], [r.values for r in query.operation.records])
        from lotus.templates.task_instructions import df2multimodal_info, filter_formatter
        from lotus.nl_expression import nle2str
        from transformers import AutoTokenizer
        messages = [filter_formatter(method.lm, doc, nle2str(method.method.instruction, method.columns))
                    for doc in df2multimodal_info(query.operation.frame, method.columns)]
        self.assertEqual([r['messages'] for r in requests], messages)
        tokenizer = AutoTokenizer.from_pretrained(TOKENIZER, local_files_only=True, trust_remote_code=False)
        self.assertEqual(tokens, [tokenizer.apply_chat_template(m, tokenize=True,
            add_generation_prompt=True) for m in messages])
        relation = entry.FilterRelation(query, method, prepared, requests, tokens, rows)
        source = self.root / 'prepared-messages.json'
        source.write_text(json.dumps({'rows': rows}))
        kernel.prepared.cache_clear()
        self.addCleanup(kernel.prepared.cache_clear)
        # Repeated messages may have different replies. Reverse the native
        # batch and bind every response by occurrence, never message/arrival.
        reversed_rows = rows[::-1]
        table = pa.table({'c0': [r['occurrence_id'] for r in reversed_rows],
                          'c1': [r['title'] for r in reversed_rows], 'c2': [r['text'] for r in reversed_rows]})
        for answers in ([('False', 'True', 'invalid')[i % 3] for i in range(len(rows))], ['False'] * len(rows)):
            by_id = dict(zip((r['occurrence_id'] for r in rows), answers, strict=True))
            def infer(actual_messages, *, occurrence_ids):
                self.assertEqual(actual_messages, messages[::-1])
                self.assertEqual(occurrence_ids, [r['occurrence_id'] for r in reversed_rows])
                return [by_id[identity] for identity in occurrence_ids]
            with patch.dict(os.environ, {kernel.CONFIG_ENV: str(source)}), \
                    patch.object(kernel.model_provider, 'infer_batch', side_effect=infer):
                decisions = kernel.infer_mmqa(table).column(0).to_pylist()
            native = dict(variant='original-author', expected_DSO_sha256=entry.DSO_SHA256,
                return_type='TINYINT', full_filter_materialized_before_title_sink=True,
                source_rows=len(rows), eligible_rows=len(rows),
                accepted_occurrences=[r for r, yes in zip(reversed_rows, decisions, strict=True) if yes])
            actual = relation.sink(native)
            expected, _ = finish_relation(query, method, prepared,
                [dict(call_id=r['call_id'], generated_text=text) for r, text in zip(requests, answers, strict=True)])
            if isinstance(expected, pd.Series):
                pd.testing.assert_series_equal(actual, expected)
            else:
                pd.testing.assert_frame_equal(actual, expected)
        return query, rows

    def test_movie_tasks_and_legal_source_sizes_change_only_configuration(self):
        seed = dict(self.seed, schema=entry.MOVIE_SCHEMA)
        for count in (7, 120):
            pd.DataFrame({'reviewId': ['duplicate'] * count, 'id': ['other'] + ['taken_3'] * (count-1),
                          'reviewText': ['same prompt'] * count}).to_csv(self.root / 'Reviews.csv', index=False)
            for query_id in ('1', '2', '3', '4'):
                query, rows = self.assert_native_filter(dict(seed, query_id=query_id))
                self.assertEqual(len(rows), count if query_id == '1' else count-1)
                self.assertEqual(set(query.operation.records[0].values), {'reviewText'})

    def test_medical_scale_is_authored_filename_not_model_occurrence_count(self):
        (self.root / 'data').mkdir()
        symptoms = pd.DataFrame({'patient_id': [1,1,2], 'symptoms': ['same', 'same', 'other']})
        patients = pd.DataFrame({'patient_id': [1,1,2], 'age': [20,40,60], 'disease': ['Acne'] * 3})
        for scale in (7, 1200):
            for name, frame in (('text_symptoms_data', symptoms), ('patient_data', patients),
                                ('patient_data_with_labels', patients)):
                frame.to_csv(self.root / f'data/{name}_{scale}.csv', index=False)
            for query_id, count in (('1',3), ('4',5)):
                _, rows = self.assert_native_filter(dict(self.seed, schema=entry.MEDICAL_SCHEMA,
                    query_id=query_id, scale_factor=scale))
                self.assertEqual(len(rows), count)

    def test_new_filter_fields_need_no_backend_task_or_column_branch(self):
        from src.baselines.text.sembench_text import _operation
        from src.serving.backends.vllm_protocol import GenerationSpec
        generation = dict(self.seed['generation'], max_tokens=32)
        frame = pd.DataFrame({'first': ['same','same'], 'second': ['line\n一','line\n一'],
                              'third': ['one','two'], 'gold': ['never sent','never sent']}, index=[8,8])
        operation = _operation('filter', frame, ('{first} and {second} and {third} agree',), {},
            model_name='cpu-no-model', generation=GenerationSpec.from_dict(generation), query_id='new-task')
        def execute(invoke):
            value = invoke(operation, None)
            return value['frame'][['first']], value
        query = SimpleNamespace(operation=operation, files={}, execute=execute, query='new-task', query_id='new-task')
        seed = dict(self.seed, query_id='new-task', generation=generation)
        with patch.object(entry, 'SemBenchTextQuery', return_value=query):
            _, rows = self.assert_native_filter(seed)
        self.assertNotEqual(rows[0]['occurrence_id'], rows[1]['occurrence_id'])
        self.assertNotIn('gold', json.loads(rows[0]['text']))

    def test_original_nonfilter_and_empty_inputs_rejected_during_freeze(self):
        pd.DataFrame({'reviewId':['x'], 'id':['taken_3'], 'reviewText':['same']}).to_csv(
            self.root / 'Reviews.csv', index=False)
        for query_id in ('5','8','9'):
            with self.subTest(query_id=query_id), self.assertRaisesRegex(ValueError, 'one original Filter'):
                entry.freeze_config(dict(self.seed, schema=entry.MOVIE_SCHEMA, query_id=query_id))
        pd.DataFrame(columns=['title','text']).to_csv(self.root/'lizzy_caplan_text_data.csv', index=False)
        with self.assertRaisesRegex(ValueError, 'nonempty original input'):
            entry.freeze_config(self.seed)
