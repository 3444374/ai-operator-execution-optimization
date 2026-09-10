"""Producer-derived expectations reject swapped sequences even for repeated payloads."""
import copy
import hashlib
import json
import unittest

from src.execution_provider.semantic_map import SemanticMapPlan, canonical_messages
from src.execution_provider.wire.map_codec import semantic_payload_digest
from src.experiments.postgresql.map_bindings import parse_pg_bindings, verify_bound_map_results


class ProducerBindingTests(unittest.TestCase):
    def setUp(self):
        self.plan = SemanticMapPlan('Instruction', 'model', 64)
        self.inputs = [('a', 'same payload'), ('b', 'same payload')]
        self.predictions = [('b', 'second'), ('a', 'first')]
        self.digest = semantic_payload_digest(semantic_spec_sha256=self.plan.digest,
            input_value='same payload', canonical_messages_utf8=canonical_messages('Instruction', 'same payload'))
        self.lines = self.offer(1, 0, 'a') + self.offer(2, 1, 'b')
        self.bindings = parse_pg_bindings(self.lines)
        self.tasks = [dict(event='core_map_task', session_id=3, sequence=s, payload_digest=self.digest) for s in (0, 1)]
        self.completions = [dict(event='core_map_completion', session_id=3, sequence=s,
            payload_digest=self.digest, response_model_id='model', finish_reason='stop', raw_output=text)
            for s, text in ((1, 'second'), (0, 'first'))]
        self.sessions = [dict(event='session_start', session_id=3, peer_pid=123)]

    def offer(self, offer, sequence, identity, accepted=True, stream=1):
        base = dict(version=1, backend_pid=123, stream=stream, offer=offer, sequence=sequence)
        events = [dict(base, phase='before_offer', row_id=identity, payload_digest=self.digest)]
        if accepted:
            events.append(dict(base, phase='accepted'))
        return ['timestamp [123] LOG:  SEMLOOM_MAP_BINDING ' + json.dumps(e) + '\n' for e in events]

    def verify(self, *, bindings=None, events=None, sessions=None):
        return verify_bound_map_results(self.inputs, self.predictions,
            self.bindings if bindings is None else bindings,
            self.tasks + self.completions if events is None else events,
            self.sessions if sessions is None else sessions, plan=self.plan)

    def test_reversed_completion_with_identical_payloads_passes(self):
        self.assertTrue(self.verify()['independent_sequence_verified'])
        events = copy.deepcopy(self.tasks + self.completions)
        for event in events[2:]:
            event['raw_output_sha256'] = hashlib.sha256(event.pop('raw_output').encode()).hexdigest()
        self.assertEqual(self.verify(events=events)['matched_rows'], 2)

    def test_swapped_sequence_is_not_used_to_rebuild_expectations(self):
        events = copy.deepcopy(self.tasks + self.completions)
        for event in events[2:]:
            event['sequence'] = 1 - event['sequence']
        with self.assertRaisesRegex(ValueError, 'output differs'):
            self.verify(events=events)

    def test_missing_sequence_and_completion_before_task_are_rejected(self):
        events = copy.deepcopy(self.tasks + self.completions)
        events[-1].pop('sequence')
        with self.assertRaises(ValueError):
            self.verify(events=events)
        with self.assertRaises(ValueError):
            self.verify(events=self.completions + self.tasks)

    def test_query_namespaces_restart_at_zero_without_mixing(self):
        other = parse_pg_bindings(self.offer(1, 0, 'a', stream=2) + self.offer(2, 1, 'b', stream=2))
        self.assertEqual(self.verify(bindings=other)['producer_namespace'], [123, 2])
        with self.assertRaisesRegex(ValueError, 'mixed producer'):
            self.verify(bindings=[self.bindings[0], other[1]])
        with self.assertRaisesRegex(ValueError, 'socket'):
            self.verify(sessions=[dict(event='session_start', session_id=3, peer_pid=999)])

    def test_backpressure_candidate_is_not_confused_with_accepted_row(self):
        lines = self.offer(1, 0, 'a', accepted=False) + self.offer(2, 0, 'b') + self.offer(3, 1, 'a')
        bindings = parse_pg_bindings(lines)
        events = copy.deepcopy(self.tasks + self.completions)
        for event in events[2:]:
            event['sequence'] = 1 - event['sequence']
        self.assertEqual(self.verify(bindings=bindings, events=events)['matched_rows'], 2)

    def test_invented_acceptance_duplicate_or_missing_producer_fails(self):
        with self.assertRaises(ValueError):
            parse_pg_bindings(self.lines[1:])
        with self.assertRaises(ValueError):
            parse_pg_bindings(self.lines + self.lines)
        with self.assertRaises(ValueError):
            self.verify(bindings=[])


if __name__ == '__main__':
    unittest.main()
