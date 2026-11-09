"""CPU fixtures for frame projection; pandas and LOTUS are explicit substitutes."""

from contextlib import contextmanager
from dataclasses import asdict
from functools import partial
import importlib.util
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus.record_chain import LotusRecordChain
from src.semantic_methods.lotus.record_method import LotusRecordMethod, record_frame_input
from src.semantic_methods.lotus.record_types import (
    ExtractMethod, MapMethod, RecordOccurrence, StageSpec,
)
from src.semantic_methods.lotus.records import FilterMethod
from src.serving.backends.vllm_protocol import GenerationSpec, IDENTITY_FIELDS


class FixtureFrame:
    def __init__(self, data, index):
        self.index = index
        self.columns = (list(data) if isinstance(data, dict) else
                        list(dict.fromkeys(k for row in data for k in row)))
        self.rows = ([dict(zip(data, values)) for values in zip(*data.values())]
                     if isinstance(data, dict) else data)


def fixture_messages(_lm, doc, *args, **kwargs):
    return [dict(role='system', content='fixture formatter'),
            dict(role='user', content=json.dumps(dict(doc=doc, args=args, kwargs=kwargs),
                                                ensure_ascii=False))]


@contextmanager
def formatter_fixture():
    lotus = SimpleNamespace(settings=SimpleNamespace(enable_cache=False, serialization_format='default'))
    types = SimpleNamespace(SerializationFormat=SimpleNamespace(DEFAULT='default'))
    with patch.dict('sys.modules', {'lotus': lotus, 'lotus.types': types,
                                   'pandas': SimpleNamespace(DataFrame=FixtureFrame)}):
        yield


def method_for(stage, project):
    method = LotusRecordMethod.__new__(LotusRecordMethod)
    method.stage, method.method = stage, stage.method
    method.columns = list(stage.method.input_columns if stage.kind == 'extract'
                          else stage.method.resolved_input_columns)
    method.instruction = getattr(stage.method, 'instruction', None)
    method.lm = SimpleNamespace()
    method.instructions = SimpleNamespace(
        df2multimodal_info=lambda frame, columns: [
            {k: row[k] for k in columns} for row in frame.rows],
        map_formatter=fixture_messages, extract_formatter=fixture_messages)
    method._filter_formatter = SimpleNamespace(messages=lambda rows, **kwargs: [
        fixture_messages(method.lm, {k: row[k] for k in method.columns}) for row in rows])
    method.prepare_rows = partial(method.prepare_rows, project_input_columns=project)

    def interpret(results):
        if any(row['status'] != 'succeeded' for row in results):
            raise RuntimeError('fixture model call did not succeed')
        outputs = [row['text'] != 'False' if stage.kind == 'filter' else
                   json.loads(row['text']) if stage.kind == 'extract' else row['text'] for row in results]
        return SimpleNamespace(outputs=outputs)

    method.interpret_results = interpret
    return method


def source_rows():
    return [RecordOccurrence('occ-' + str(i), i, 7 if i < 2 else 19,
        dict(text='重复 review\n🙂', patient_id='duplicate', metadata={'keep': [i]}),
        source_refs={'source': {'position': i}}) for i in range(3)]


class InputProjectionTests(unittest.TestCase):
    def setUp(self):
        self.scope = formatter_fixture()
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)

    def test_frame_input_only_materializes_declared_vectors_and_keeps_full_source(self):
        rows = source_rows()
        before = [r.describe() for r in rows]
        old = record_frame_input(rows)
        projected = record_frame_input(rows, columns=['patient_id', 'text'])
        self.assertEqual(list(projected), ['patient_id', 'text'])
        self.assertEqual(projected['text'], [r.values['text'] for r in rows])
        self.assertIs(old[0], rows[0].values)
        self.assertEqual([r.describe() for r in rows], before)
        self.assertEqual(len(projected['text']), 3)

    def test_map_and_extract_messages_and_occurrences_match_with_reordered_columns(self):
        for spec in (MapMethod('fixture', 'Use {text} and {patient_id}',
                               resolved_input_columns=('patient_id', 'text')),
                     ExtractMethod('fixture', ('patient_id', 'text'), {'diagnosis': 'fixture'})):
            generation = GenerationSpec(max_tokens=48, stop=(),
                response_format={'type': 'json_object'} if isinstance(spec, ExtractMethod) else None)
            stage = StageSpec('ordinary', spec, generation)
            rows = source_rows()
            before = [r.describe() for r in rows]
            left, right = [method_for(stage, flag).prepare_rows(rows, query_id='q') for flag in (False, True)]
            names = (*IDENTITY_FIELDS, 'messages_json')
            self.assertEqual([{k: r[k] for k in names} for r in left],
                             [{k: r[k] for k in names} for r in right])
            self.assertEqual(len({r['call_id'] for r in right}), 3)
            self.assertEqual([r.describe() for r in rows], before)

    def test_null_missing_and_nontext_inputs_keep_the_existing_validation(self):
        stage = StageSpec('map', MapMethod('fixture', 'Use {text}', resolved_input_columns=('text',)),
                          GenerationSpec())
        for project in (False, True):
            method = method_for(stage, project)
            self.assertEqual(method.prepare_rows([], query_id='q'), [])
            for values in ({}, {'text': 9}, {'text': None}):
                row = RecordOccurrence('a', 0, 'label', values)
                with self.assertRaises(ValueError):
                    method.prepare_rows([row], query_id='q')
            row = RecordOccurrence('a', 0, 'label', {'text': None, 'extra': 'kept'})
            result = method.prepare_rows([row], query_id='q', allow_nulls=True)
            doc = json.loads(json.loads(result[0]['messages_json'])[1]['content'])['doc']
            self.assertIsNone(doc['text'])
            self.assertEqual(row.values['extra'], 'kept')

    def run_relation(self, project):
        stages = (
            StageSpec('summary', MapMethod('fixture', 'Use {text}', 'summary',
                resolved_input_columns=('text',)), GenerationSpec(max_tokens=32, stop=())),
            StageSpec('extract', ExtractMethod('fixture', ('summary',), {'diagnosis': 'fixture'}),
                GenerationSpec(max_tokens=64, stop=(), response_format={'type': 'json_object'})),
            StageSpec('filter', FilterMethod('fixture', 'Accept {diagnosis}',
                resolved_input_columns=('diagnosis',)), GenerationSpec()),
            StageSpec('final', MapMethod('fixture', 'Use {diagnosis}', 'final',
                resolved_input_columns=('diagnosis',)), GenerationSpec(max_tokens=32, stop=())),
        )
        with patch('src.semantic_methods.lotus.record_chain.LotusRecordMethod',
                   side_effect=lambda stage: method_for(stage, project)), \
                patch('src.semantic_methods.lotus.record_chain.lm_report', return_value={}):
            chain = LotusRecordChain(stages, query_id='relation-fixture', relation_barriers=True,
                                     _require_bound=True)
            run = chain.start_relation(source_rows())
            requests = []
            while run.position < len(stages):
                prepared = run.prepare()
                generation = asdict(run.method.stage.generation)
                requests += [dict({k: row[k] for k in (*IDENTITY_FIELDS, 'messages_json')},
                                  generation=generation) for row in prepared]
                results = []
                for row in reversed(prepared):
                    i, stage = row['source_position'], row['algorithm_stage']
                    text = ('False' if i == 2 else 'True') if stage == 'filter' else (
                        json.dumps(({'diagnosis': 'old', 'dynamic': 'retained'} if i == 0 else
                                    {'diagnosis': None} if i == 1 else {})) if stage == 'extract' else
                        stage + '/' + str(i))
                    results.append(dict({k: row[k] for k in IDENTITY_FIELDS}, status='succeeded',
                        text=text, submission_confirmed=True, prompt_tokens=3, completion_tokens=2,
                        total_tokens=5))
                run.complete(results)
                if run.relation_pending:
                    self.assertEqual(run.prepare(), [])
                    run.complete_relation(source_exhausted=True)
            result = run.result()
            return dict(records=[r.describe() for r in result['records']],
                        calls=result['calls'], columns=result['columns']), requests

    def test_projection_preserves_full_relation_assignment_parents_and_all_calls(self):
        expected, expected_requests = self.run_relation(False)
        actual, actual_requests = self.run_relation(True)
        self.assertEqual(actual, expected)
        self.assertEqual(actual_requests, expected_requests)
        self.assertEqual(len(actual_requests), 11)
        self.assertEqual(len(actual['records']), 2)
        for row in actual['records']:
            self.assertIsNone(row['values']['diagnosis'])
            self.assertEqual(row['values']['dynamic'], 'retained')
            self.assertIn('metadata', row['values'])
        calls = {c['call_id']: c for c in actual['calls']}
        filters = [c for c in calls.values() if c['algorithm_stage'] == 'filter']
        self.assertEqual(len(filters[0]['parent_call_ids']), 2)


@unittest.skipUnless(all(importlib.util.find_spec(name) for name in ('lotus', 'pandas')),
                     'requires the existing LOTUS/pandas CPU environment')
class NativeInputProjectionTests(unittest.TestCase):
    def test_original_formatter_messages_match_for_map_extract_and_nulls(self):
        import lotus

        with lotus.settings.context(enable_cache=False):
            for columns in (('text',), ('patient_id', 'text')):
                for extract in (False, True):
                    spec = (ExtractMethod('openai/local-fixture', columns, {'diagnosis': 'fixture'})
                            if extract else MapMethod('openai/local-fixture',
                                'Use ' + ' and '.join('{' + c + '}' for c in columns),
                                resolved_input_columns=columns))
                    stage = StageSpec('ordinary', spec, GenerationSpec(max_tokens=48, stop=(),
                        response_format={'type': 'json_object'} if extract else None))
                    method = LotusRecordMethod(stage)
                    for nulls in (False, True):
                        rows = source_rows()
                        if nulls:
                            rows[0].values['text'] = None
                        results = [method.prepare_rows(rows, query_id='q', allow_nulls=nulls,
                            project_input_columns=project) for project in (False, True)]
                        names = (*IDENTITY_FIELDS, 'messages_json')
                        self.assertEqual([{k: r[k] for k in names} for r in results[0]],
                                         [{k: r[k] for k in names} for r in results[1]])


if __name__ == '__main__':
    unittest.main()
