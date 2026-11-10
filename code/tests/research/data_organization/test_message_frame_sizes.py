"""Exact sizing and fixed-output replay; no SDK, tokenizer assets or model."""
from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path
import random
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_chain_token_batches import PreparedTokenOrganizer
from src.research.data_organization.message_frame_sizes import (
    MessageSizeReuseOrganizer, direct_json_frame_sizes, reused_message_frame_sizes,
)
from src.research.data_organization.message_frame_replay import compare, fixture_frontier, replay
from src.research.data_organization import message_frame_replay
from src.research.data_organization.native_frame_profile import profile
from src.semantic_methods.lotus import record_chain as chains
from src.semantic_methods.lotus.record_relation import complete_extract_relation
from src.serving.backends.async_engine_wire import _frame
from src.serving.backends.vllm_protocol import AsyncEngineLimits, GenerationSpec
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
        report = compare(frontier, repeats=2, window=3, input_bytes=2**20,
                         item_input_bytes=2**20, item_result_bytes=8192)
        self.assertEqual(report['requests'], 7)
        self.assertEqual(len({e['row']['call_id'] for e in frontier}), 7)
        self.assertEqual(len({e['row']['messages_json'] for e in frontier}), 1)
        self.assertEqual(report['equivalence']['batches'], [(0, 1, 2), (3, 4, 5), (6,)])
        self.assertEqual(report['equivalence']['tokenizer_calls'], 7)

    def test_replay_cli_preserves_distinct_item_aggregate_and_rpc_allowances(self):
        caps = dict(input_bytes=2**20, item_input_bytes=16384, item_result_bytes=131072)
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(message_frame_replay, 'AsyncEngineLimits', wraps=AsyncEngineLimits) as declared, \
                patch.object(message_frame_replay, '_frame', wraps=_frame) as frames, \
                patch.object(PreparedTokenOrganizer, '_request_frame_sizes',
                             wraps=PreparedTokenOrganizer._request_frame_sizes) as sizing:
            output = Path(directory) / 'replay.json'
            message_frame_replay.main(['--rows', '7', '--body-chars', '256', '--window', '3',
                '--repeats', '1', '--input-bytes', str(caps['input_bytes']),
                '--item-input-bytes', str(caps['item_input_bytes']),
                '--item-result-bytes', str(caps['item_result_bytes']), '--output', str(output)])
            report = json.loads(output.read_text())
        self.assertTrue(declared.call_args_list)
        for call in declared.call_args_list:
            self.assertEqual(call.args, (3,))
            self.assertEqual(call.kwargs, caps)
        self.assertEqual({name: report[name] for name in caps}, caps)
        self.assertEqual(report['equivalence']['batches'], [[0, 1, 2], [3, 4, 5], [6]])
        self.assertTrue(sizing.call_args_list)
        for call in sizing.call_args_list:
            _, rpc, actual = call.args
            self.assertEqual(rpc['maximum'], caps['item_result_bytes'])
            self.assertEqual({name: getattr(actual, name) for name in caps}, caps)
        self.assertEqual({call.args[0].get('op', 'rpc') for call in frames.call_args_list}, {'submit', 'rpc'})
        for call in frames.call_args_list:
            value, maximum = call.args
            if value.get('op') == 'submit':
                self.assertEqual(maximum, caps['item_input_bytes'])
            else:
                self.assertEqual(value['maximum'], caps['item_result_bytes'])
                self.assertEqual(maximum, caps['input_bytes'])

    def test_failed_prediction_still_rejects_model_context_overflow(self):
        rows = candidates([4090])
        original, _ = organizer(rows, B=None)
        reused = MessageSizeReuseOrganizer(original.owner, original.tokenizer, original.concurrency)
        for value in (original, reused):
            with self.assertRaisesRegex(ValueError, 'model context'):
                value(rows)

    def test_native_capture_recipe_records_every_tokenization_without_model_execution(self):
        from src.research.data_organization.message_frame_replay import ReplayTokenizer
        frontier = fixture_frontier(3, 256)
        calls = []
        def prepare(records, *, query_id):
            calls.append((records, query_id))
            return [dict(e['row']) for e in frontier]
        method = SimpleNamespace(stage=SimpleNamespace(generation=GenerationSpec(max_tokens=8)),
                                 prepare_rows=prepare)
        tokenizer = ReplayTokenizer(frontier)
        limits = SimpleNamespace(item_input_bytes=2**21, input_bytes=2**24, item_result_bytes=2**21)
        with patch.object(PreparedTokenOrganizer, '_request_frame_sizes',
                          wraps=PreparedTokenOrganizer._request_frame_sizes) as sizing:
            captured, report = profile(method, [1, 2, 3], tokenizer, frontier[0]['model'], limits,
                                       query_id='cpu-frame-replay', repeats=2)
        for call in sizing.call_args_list:
            self.assertEqual(call.args[1]['maximum'], 2**21)
            self.assertIs(call.args[2], limits)
        self.assertEqual(len(calls), 2)
        self.assertEqual(tokenizer.calls, 6)
        self.assertEqual([e['token_ids'] for e in captured], [e['token_ids'] for e in frontier])
        self.assertEqual([e['row'] for e in captured], [e['row'] for e in frontier])
        self.assertEqual(report['duplicate_message_counts'], [3])
        self.assertEqual(report['model_calls'], 0)
        for sample in report['samples']:
            self.assertGreater(sample['original_sizing_fraction_in_slice'], 0)
            self.assertLess(sample['original_sizing_fraction_in_slice'], 1)

    def test_native_capture_recipe_rejects_changed_token_ids_between_repeats(self):
        frontier = fixture_frontier(1, 32)
        method = SimpleNamespace(stage=SimpleNamespace(generation=GenerationSpec()),
            prepare_rows=lambda records, **kw: [dict(frontier[0]['row'])])
        class ChangingTokenizer:
            calls = 0
            def apply_chat_template(self, messages, **kw):
                self.calls += 1
                return [self.calls]
        limits = SimpleNamespace(item_input_bytes=2**20, input_bytes=2**20, item_result_bytes=8192)
        with self.assertRaisesRegex(AssertionError, 'token IDs'):
            profile(method, [1], ChangingTokenizer(), frontier[0]['model'], limits,
                    query_id='cpu-frame-replay', repeats=2)

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
                        replay(frontier, arm, window=2, input_bytes=2**20,
                               item_input_bytes=2**20, item_result_bytes=8192)
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
