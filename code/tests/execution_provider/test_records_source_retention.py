"""Source validation releases read rows while preserving owned input snapshots."""
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
import unittest
import weakref

from src.execution_provider.adapters.records_query_runtime import _RecordLocalRows
from src.scheduling.core.session_contract import Usage
from src.semantic_methods.lotus.record_types import RecordOccurrence


class SourceRetentionTests(unittest.TestCase):
    def row(self, position=0):
        return RecordOccurrence('row-'+str(position), position, position,
            dict(passage='source text', nested=[dict(value=position)]),
            dict(source=dict(key=position)))

    def runtime(self, expected, actual):
        owner = SimpleNamespace(deadline=float('inf'),
            core=SimpleNamespace(capacity=SimpleNamespace(usage=Usage)))
        return _RecordLocalRows(owner, actual, SimpleNamespace(current=expected), None)

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
