"""Exact sizing and fixed-output replay; no SDK, tokenizer assets or model."""
from copy import deepcopy
from dataclasses import asdict
import json
import random
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_chain_token_batches import PreparedTokenOrganizer
from src.research.data_organization.message_frame_sizes import (
    MessageSizeReuseOrganizer, direct_json_frame_sizes, reused_message_frame_sizes,
)
from src.research.data_organization.message_frame_replay import compare, fixture_frontier, replay
from src.semantic_methods.lotus import record_chain as chains
from src.semantic_methods.lotus.record_relation import complete_extract_relation
from src.serving.backends.async_engine_wire import _frame
from src.serving.backends.vllm_protocol import GenerationSpec
from tests.execution_provider.test_prepared_request_organization import candidates, organizer
from tests.semantic_methods.test_lotus_chain_composition import ResponseFixture, normalized, source_rows
from tests.semantic_methods.test_lotus_chain_relation_handoff import reference


class ScriptedMethod:
    def __init__(self, stage):
        self.stage = stage
        self.lm = SimpleNamespace(stats=SimpleNamespace(model_dump=lambda: {}))

    def prepare_rows(self, records, *, query_id, allow_nulls=False):
        return [dict(chains.stage_identity(query_id, row, self.stage.stage_id),
            messages_json=json.dumps([dict(role='system', content=self.stage.method.model_name),
                dict(role='user', content=json.dumps(row.values, ensure_ascii=False))], ensure_ascii=False))
            for row in records]

    def interpret_results(self, results):
        values = [r['text'] for r in results]
        if self.stage.kind == 'filter':
            values = [v == 'True' for v in values]
        elif self.stage.kind == 'extract':
            values = [json.loads(v) for v in values]
        return SimpleNamespace(outputs=values)


class MessageFrameSizeTests(unittest.TestCase):
    def frames(self, message):
        row = dict(call_id='occurrence', messages_json=message,
                   source_metadata=dict(values=[None, True, 1.5, '中文']))
        submit = dict(op='submit', request_id='engine:9', row=dict(row),
                      generation=asdict(GenerationSpec()), deadline=None)
        rpc = dict(row=dict(row), generation=asdict(GenerationSpec()), maximum=8192,
                   request_key=[0, 9], parent_call_ids=['parent'])
        return submit, rpc

    def test_exact_sizes_for_escaped_utf8_and_full_nested_messages(self):
        rng = random.Random(19)
        messages = ['', '\x00\b\t\n\r"\\中文é😀', json.dumps([
            dict(role='user', content=[dict(type='text', text='"中文\\')])], ensure_ascii=False)]
        messages += [''.join(rng.choices('abc"\\\n\t中文é😀\x00', k=n)) for n in range(100)]
        for message in messages:
            submit, rpc = self.frames(message)
            before = deepcopy((submit, rpc))
            limits = SimpleNamespace(item_input_bytes=2**20, input_bytes=2**20)
            for method in (reused_message_frame_sizes, direct_json_frame_sizes):
                self.assertEqual(method(submit, rpc, limits),
                                 PreparedTokenOrganizer._request_frame_sizes(submit, rpc, limits))
            self.assertEqual((submit, rpc), before)

    def test_exact_and_one_byte_short_caps_reject_in_both_arms(self):
        submit, rpc = self.frames('长\\"\n'*100)
        a, b = len(_frame(submit, 2**20))-4, len(_frame(rpc, 2**20))-4
        for caps in ((a, b), (a-1, b), (a, b-1)):
            limits = SimpleNamespace(item_input_bytes=caps[0], input_bytes=caps[1])
            for method in (PreparedTokenOrganizer._request_frame_sizes,
                           direct_json_frame_sizes, reused_message_frame_sizes):
                if caps == (a, b):
                    self.assertEqual(method(submit, rpc, limits), caps)
                else:
                    with self.assertRaisesRegex(ValueError, 'byte capacity'):
                        method(submit, rpc, limits)

    def test_existing_ready_selection_and_tokenization_stay_identical(self):
        for options in (dict(B=None), dict(B=80), dict(B=None, C=2),
                        dict(B=None, input_bytes=1700, item_input_bytes=1700)):
            rows = candidates([2]*5, stages=['map', 'map', 'map', 'filter', 'filter'])
            original, _ = organizer(rows, **options)
            reused, _ = organizer(rows, **options)
            reused = MessageSizeReuseOrganizer(reused.owner, reused.tokenizer, reused.concurrency)
            reused.request_window = original.request_window
            before = tuple(c.task.payload for c in rows)
            self.assertEqual(original(rows), reused(rows))
            self.assertEqual(original.history, reused.history)
            self.assertEqual(original._estimates, reused._estimates)
            self.assertEqual(original.tokenizer.calls, reused.tokenizer.calls)
            self.assertEqual(tuple(c.task.payload for c in rows), before)

    def test_replay_preserves_model_params_tokens_duplicates_and_result_order(self):
        frontier = fixture_frontier(7, 256)
        for entry in frontier:
            entry['generation'].update(seed=5, response_format=dict(type='json_object'))
            entry['parent_call_ids'] = ['original-parent']
            entry['media'] = []  # The entire input record is checked for mutation.
        report = compare(frontier, repeats=2, window=3, input_bytes=2**20)
        self.assertEqual(report['requests'], 7)
        self.assertEqual(len({e['row']['call_id'] for e in frontier}), 7)
        self.assertEqual(len({e['row']['messages_json'] for e in frontier}), 1)
        self.assertEqual(report['equivalence']['batches'], [(0, 1, 2), (3, 4, 5), (6,)])
        self.assertEqual(report['equivalence']['tokenizer_calls'], 7)

    def test_failed_prediction_still_rejects_model_context_overflow(self):
        rows = candidates([4090])
        original, _ = organizer(rows, B=None)
        reused = MessageSizeReuseOrganizer(original.owner, original.tokenizer, original.concurrency)
        for value in (original, reused):
            with self.assertRaisesRegex(ValueError, 'model context'):
                value(rows)

    def test_real_chain_fixed_results_keep_dependencies_and_extract_relation(self):
        for pattern in ('FM', 'MF', 'FME', 'EM'):
            stages, field = [], 'passage'
            for i, kind in enumerate(pattern):
                if kind == 'F':
                    method = chains.FilterMethod('cpu-fixture', 'Keep {'+field+'}')
                elif kind == 'M':
                    method = chains.MapMethod('cpu-fixture', 'Rewrite {'+field+'}', 'derived_'+str(i))
                    field = method.output_column
                else:
                    method = chains.ExtractMethod('cpu-fixture', (field,), {'field_'+str(i): 'text'})
                    field = next(iter(method.output_columns))
                generation = GenerationSpec(response_format=dict(type='json_object') if kind == 'E' else None)
                stages.append(chains.StageSpec('step_'+str(i), method, generation))
            outputs, requests = [], []
            with patch.object(chains, 'LotusRecordMethod', ScriptedMethod):
                chain = chains.LotusRecordChain(tuple(stages), query_id='fixed-replay',
                                               relation_barriers=True, _require_bound=True)
                for arm in (PreparedTokenOrganizer, MessageSizeReuseOrganizer):
                    infer, captured = ResponseFixture(chain), []
                    def checked(prepared, *, generation):
                        results = infer(prepared, generation=generation)
                        by_id = {r['call_id']: r for r in results}
                        frontier = [dict(row=row, generation=asdict(generation),
                            model=dict(name='cpu-fixture', revision='scripted'), token_ids=[11, 22],
                            parent_call_ids=[], result=by_id[row['call_id']]) for row in prepared]
                        replay(frontier, arm, window=2, input_bytes=2**20)
                        captured.extend(deepcopy(frontier))
                        return results
                    result = reference(chain, source_rows(size=4), checked)
                    outputs.append(normalized(result))
                    requests.append(captured)
                self.assertEqual(outputs[0], outputs[1], pattern)
                self.assertEqual(requests[0], requests[1], pattern)
                self.assertEqual([r['source_position'] for r in outputs[0]['records']],
                                 sorted(r['source_position'] for r in outputs[0]['records']))
                if 'E' in pattern:
                    extract = next(s for s in stages if s.kind == 'extract')
                    with self.assertRaisesRegex(ValueError, 'source EOF'):
                        complete_extract_relation([], columns=[], extract_stage=extract,
                            query_id=chain.query_id, calls=[], source_exhausted=False)


if __name__ == '__main__':
    unittest.main()
