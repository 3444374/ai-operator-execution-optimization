"""Generic Filter verification preserves the existing Movie caller."""
import json
import unittest

from src.baselines.text.sembench_movie import MOVIE_FILTER_INSTRUCTION
from src.execution_provider.wire import v3
from src.experiments.postgresql.filter_bindings import verify_filter_decisions
from src.experiments.postgresql.movie_queries import verify_filter_decisions as verify_movie


def trace(instruction):
    plan = v3.SemanticFilterPlan(instruction, 'model')
    digest = v3.semantic_payload_digest(semantic_spec_sha256=v3.semantic_spec_digest(plan),
        input_value='input', canonical_messages_utf8=v3.canonical_messages(instruction, 'input'))
    base = dict(version=1, backend_pid=7, stream=1, offer=1, sequence=0)
    rows = [dict(base, phase='before_offer', row_id='r', payload_digest=digest),
            dict(base, phase='accepted'), dict(base, phase='decision', kept=True)]
    return ['LOG: SEMLOOM_FILTER_BINDING ' + json.dumps(row) for row in rows]


class FilterBindingsTests(unittest.TestCase):
    def test_custom_instruction_binds_request_before_decision(self):
        self.assertEqual(verify_filter_decisions(trace('Keep.'), {'r': 'input'}, 'model',
            complete=True, instruction='Keep.'), {'r': True})
        with self.assertRaisesRegex(ValueError, 'payload differs'):
            verify_filter_decisions(trace('Keep.'), {'r': 'input'}, 'model',
                complete=True, instruction='Changed.')

    def test_movie_compatibility_entry_point_keeps_its_instruction(self):
        self.assertEqual(verify_movie(trace(MOVIE_FILTER_INSTRUCTION), {'r': 'input'},
            'model', complete=True), {'r': True})

    def test_missing_decision_cannot_be_inferred_from_final_rows(self):
        with self.assertRaisesRegex(ValueError, 'no final row decision'):
            verify_filter_decisions(trace('Keep.')[:-1], {'r': 'input'}, 'model',
                complete=True, instruction='Keep.')
