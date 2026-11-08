"""Source validation releases read rows while preserving owned input snapshots."""
from copy import deepcopy
from dataclasses import replace
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
import weakref

from src.execution_provider.adapters.records_query_runtime import _RecordLocalRows
from src.scheduling.core.session_contract import Usage
from src.semantic_methods.lotus.record_chain import ChainRun
from src.semantic_methods.lotus.record_results import _source_binding
from src.semantic_methods.lotus.record_types import ExtractMethod, RecordOccurrence, StageSpec, stage_identity
from src.serving.backends.vllm_protocol import GenerationSpec


class _RetainedInput(dict):
    """Weak-referenceable input data for the phase-lifetime check."""


class SourceRetentionTests(unittest.TestCase):
    def row(self, position=0):
        return RecordOccurrence('row-'+str(position), position, position,
            dict(passage='source text', nested=[dict(value=position)]),
            dict(source=dict(key=position)))

    def runtime(self, expected, actual):
        owner = SimpleNamespace(deadline=float('inf'),
            core=SimpleNamespace(capacity=SimpleNamespace(usage=Usage)))
        return _RecordLocalRows(owner, actual, SimpleNamespace(current=expected), None)

    def completed_extract_phase(self):
        stage = StageSpec('extract', ExtractMethod('fixture', ('passage',), {'entity': None}),
            GenerationSpec(response_format={'type': 'json_object'}))
        method = SimpleNamespace(stage=stage,
            lm=SimpleNamespace(stats=SimpleNamespace(model_dump=lambda: {})),
            interpret_results=lambda rows: SimpleNamespace(outputs=[json.loads(row['text']) for row in rows]))
        run = ChainRun(SimpleNamespace(query_id='query', stages=(stage,), methods=(method,)),
            [self.row()], _relation_execution=True)
        stream = self.runtime(run.current, [replace(run.current[0])])
        stream._pull_source()
        with self.assertRaises(StopIteration):
            stream._pull_source()
        identity = stage_identity('query', run.current[0], stage.stage_id)
        prepared = _RetainedInput(identity, messages_json='completed input prompt')
        binding = _RetainedInput(_source_binding(run.current[0]))
        run.prepared = [prepared]
        stream.run = run
        stream._prepared = {prepared['row_id']: prepared}
        stream._phase_bindings = {binding['row_id']: binding}
        stream._extract_results = [dict(identity, status='succeeded', text='{"entity":"parsed"}')]
        stream._stage_ids, stream._phase_count = (stage.stage_id,), 1
        stream.owner.phase_history = []
        stream.owner.transport = SimpleNamespace(retained_refs=Mock(return_value=0))
        stream.owner._supply_feedback = None
        stream.pool = SimpleNamespace(used=(0, 0))
        stream.session = SimpleNamespace(session_id=1,
            close=Mock(return_value=SimpleNamespace(status='CLOSED', usage=Usage())))
        stream.driver = SimpleNamespace(finished=True, close=Mock())
        return stream, (weakref.ref(prepared), weakref.ref(binding))

    def test_phase_inputs_retire_only_after_successful_completion_and_driver_close(self):
        for failure in (None, 'transport', 'complete', 'adopt', 'driver_close'):
            with self.subTest(failure=failure):
                stream, references = self.completed_extract_phase()
                original_results = deepcopy(stream._extract_results)
                error = LookupError('phase fixture failure')
                if failure == 'transport':
                    stream.owner.transport.retained_refs.return_value = 1
                elif failure == 'complete':
                    stream.run.complete = Mock(side_effect=error)
                elif failure == 'adopt':
                    stream.run.prepared = []
                    stream.section = SimpleNamespace(_adoption=Mock(side_effect=error))
                    stream.run._local_section = stream.section
                elif failure == 'driver_close':
                    stream.driver.close.side_effect = error
                if failure is None:
                    stream._finish_phase()
                    self.assertEqual([reference() is None for reference in references], [True, True])
                    self.assertEqual(stream.run.prepared, [])
                    self.assertEqual(stream.run.calls[0]['status'], 'succeeded')
                    self.assertEqual(stream.run.current[0].values['entity'], 'parsed')
                else:
                    with self.assertRaises(RuntimeError if failure == 'transport' else LookupError) as raised:
                        stream._finish_phase()
                    if failure != 'transport':
                        self.assertIs(raised.exception, error)
                    self.assertEqual([reference() is not None for reference in references], [True, True])
                    if failure == 'driver_close':
                        self.assertEqual(stream.run.calls[0]['status'], 'succeeded')
                    if failure == 'transport':
                        stream.session.close.assert_not_called()
                self.assertEqual(stream._extract_results, original_results)

    def test_source_validation_releases_replaced_relation_records(self):
        stream = self.runtime([self.row()], [self.row()])
        previous = weakref.ref(stream.run.current[0])
        stream._pull_source()
        with self.assertRaises(StopIteration):
            stream._pull_source()
        stream.run.current = []
        self.assertIsNone(previous())
        self.assertEqual(stream.offered, {'row-0'})
        self.assertTrue(stream.source_eof and stream.source_closed)

    def test_source_validation_keeps_missing_duplicate_and_changed_detection(self):
        changed = replace(self.row(), values=dict(passage='changed', nested=[dict(value=0)]))
        for actual, message in (([self.row()], 'omitted expected'),
                                ([self.row(), self.row()], 'changed or duplicated'),
                                ([changed, self.row(1)], 'changed or duplicated')):
            with self.subTest(message=message):
                stream = self.runtime([self.row(), self.row(1)], actual)
                with self.assertRaisesRegex(ValueError, message):
                    while True:
                        stream._pull_source()

    def test_pulled_source_snapshot_survives_caller_mutation(self):
        actual = self.row()
        stream = self.runtime([self.row()], [actual])
        pulled = stream._pull_source()
        actual.values['nested'][0]['value'] = 'changed'
        actual.source_refs['source']['key'] = 'changed'
        self.assertEqual(pulled.values['nested'], [dict(value=0)])
        self.assertEqual(pulled.source_refs, dict(source=dict(key=0)))
        with self.assertRaises(StopIteration):
            stream._pull_source()

    def test_from_dict_keeps_nested_input_and_description_isolated(self):
        data = dict(row_id='row-0', source_position=0, source_index=0,
            values=dict(nested=[dict(value='original')]),
            source_refs=dict(source=dict(key='original')),
            index_updates=dict(nested=[dict(value='original')]))
        original = deepcopy(data)
        record = RecordOccurrence.from_dict(data)
        for field in ('values', 'source_refs', 'index_updates'):
            data[field].clear()
        self.assertEqual(record.describe(), original)
        described = record.describe()
        described['values']['nested'][0]['value'] = 'changed'
        described['source_refs']['source']['key'] = 'changed'
        described['index_updates']['nested'][0]['value'] = 'changed'
        self.assertEqual(record.describe(), original)


if __name__ == '__main__':
    unittest.main()
