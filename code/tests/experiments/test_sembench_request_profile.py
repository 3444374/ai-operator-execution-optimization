"""No-model capture preserves native request content and excludes volatile timings."""

from contextlib import redirect_stdout
import io
import json
from types import SimpleNamespace
import unittest

from src.experiments.sembench_request_profile import capture, main
from src.semantic_methods.lotus.record_types import RecordOccurrence, stage_identity
from src.serving.backends.vllm_protocol import GenerationSpec


class Method:
    stage = SimpleNamespace(generation=GenerationSpec(max_tokens=32, stop=()))

    def __init__(self):
        self.preparations = 0

    def prepare_rows(self, records, *, query_id):
        self.preparations += 1
        return [dict(stage_identity(query_id, row, 'map'),
                     messages_json=json.dumps([{'role': 'user', 'content': row.values['text']}], ensure_ascii=False),
                     prepare_started_ns=self.preparations) for row in records]


class Tokenizer:
    def __init__(self):
        self.calls = []

    def apply_chat_template(self, messages, **options):
        self.calls.append((messages, options))
        return {'input_ids': [101, 10, 20, 102]}


class RequestCaptureTests(unittest.TestCase):
    def records(self):
        return [RecordOccurrence('row-' + str(i), i, 7, {'text': '同一条 "review"'}) for i in range(2)]

    def test_complete_requests_keep_duplicates_generation_and_one_template_application(self):
        token, method = Tokenizer(), Method()
        result = capture(method, self.records(), token, query_id='cpu', repeats=2, max_capture_bytes=10000)
        self.assertEqual(len(result['requests']), 2)
        self.assertEqual(len({r['call_id'] for r in result['requests']}), 2)
        self.assertEqual(result['requests'][0]['row']['messages_json'], result['requests'][1]['row']['messages_json'])
        self.assertEqual(result['requests'][0]['generation']['max_tokens'], 32)
        self.assertEqual(result['requests'][0]['row']['prepare_started_ns'], 2)
        self.assertEqual(len(token.calls), 4)
        for messages, options in token.calls:
            self.assertEqual(options, dict(tokenize=True, add_generation_prompt=True, return_dict=False))
            self.assertEqual(messages, [{'role': 'user', 'content': '同一条 "review"'}])
        self.assertEqual(result['inference_calls'], 0)
        self.assertFalse(result['model_weights_loaded'])

    def test_tokenizer_drift_is_rejected_without_emitting_a_valid_capture(self):
        class Changing(Tokenizer):
            def apply_chat_template(inner, messages, **options):
                inner.calls.append(None)
                return [1, len(inner.calls)]
        with self.assertRaisesRegex(ValueError, 'changed complete requests'):
            capture(Method(), self.records(), Changing(), query_id='cpu', repeats=2, max_capture_bytes=10000)

    def test_source_reordering_and_capture_overflow_are_rejected(self):
        class Reordered(Method):
            def prepare_rows(self, records, **kwargs):
                return list(reversed(super().prepare_rows(records, **kwargs)))
        with self.assertRaisesRegex(ValueError, 'source occurrences'):
            capture(Reordered(), self.records(), Tokenizer(), query_id='cpu', repeats=1, max_capture_bytes=10000)
        with self.assertRaises(ValueError):
            capture(Method(), self.records(), Tokenizer(), query_id='cpu', repeats=1, max_capture_bytes=100)

    def test_empty_input_is_valid_without_tokenization(self):
        token = Tokenizer()
        result = capture(Method(), [], token, query_id='cpu', repeats=2, max_capture_bytes=1000)
        self.assertEqual(result['requests'], [])
        self.assertEqual(token.calls, [])
        self.assertIsNone(result['distributions']['prompt_tokens']['median'])

    def test_help_needs_no_tokenizer_assets_or_model(self):
        with redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as stopped:
            main(['--help'])
        self.assertEqual(stopped.exception.code, 0)


if __name__ == '__main__':
    unittest.main()
