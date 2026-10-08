import unittest
import base64
import io
import json

from src.baselines.text.frameworks.semantic_map import validate_source, application_prompt
from src.experiments.postgresql.query_inputs import QueryInputs
from src.experiments.postgresql.semantic_system_query import evaluate_outputs, native_file_capacity, record_native_response
from src.baselines.text.products.sema import projection_instruction


class SemanticMapComparisonTests(unittest.TestCase):
    def test_repeated_text_and_original_ids_preserve_row_occurrences(self):
        rows = [(0, 'row-a', 'movie', 'same review', 'original'),
                (1, 'row-b', 'movie', 'same review', 'original')]
        values = validate_source(rows, QueryInputs('movie', 'input_table', 2))
        self.assertEqual([v['source_example_id'] for v in values], ['row-a', 'row-b'])
        self.assertEqual([v['input_text'] for v in values], ['same review', 'same review'])

    def test_missing_and_duplicate_occurrences_are_rejected(self):
        rows = [(0, 'row-a', 'movie', 'review', 'original')]
        with self.assertRaises(ValueError):
            validate_source(rows, QueryInputs('movie', 'input_table', 2))
        with self.assertRaises(ValueError):
            validate_source(rows*2, QueryInputs('movie', 'input_table', 2))

    def test_prompt_preserves_instruction_and_complete_raw_text(self):
        text = "电影 'quoted'\n中文与 \"quotes\""
        self.assertEqual(application_prompt('classify', text), 'classify\n\nInput:\n' + text)

    def test_sema_instruction_exposes_native_json_scalar_format(self):
        instruction = projection_instruction('Classify the complete review.')
        self.assertTrue(instruction.startswith('Classify the complete review.\n'))
        self.assertIn('one JSON string in double quotes', instruction)
        self.assertTrue(instruction.endswith('Input: {review_text}'))

    def test_file_capacity_rejects_evaluation_burst_before_model_work(self):
        with self.assertRaisesRegex(ValueError, '1664.*1024'):
            native_file_capacity(512, limits=(1024, 1048576))
        self.assertEqual(native_file_capacity(128, limits=(1024, 1048576))['required'], 512)
        self.assertEqual(native_file_capacity(512, limits=(8192, 1048576))['soft_limit'], 8192)

    def test_cancelling_count_errors_remain_quality_errors(self):
        report = evaluate_outputs([('a', 'NEGATIVE'), ('b', 'POSITIVE')],
                                  {'a': 'POSITIVE', 'b': 'NEGATIVE'})
        self.assertEqual(report['count_error_from_classifications'], 0)
        self.assertEqual(report['false_positive'], 1)
        self.assertEqual(report['false_negative'], 1)

    def test_invalid_output_is_scored_without_silent_label_repair(self):
        report = evaluate_outputs([('a', 'Answer: POSITIVE')], {'a': 'POSITIVE'})
        self.assertEqual(report['invalid'], 1)

    def test_unknown_missing_and_duplicate_outputs_are_rejected(self):
        for rows in ([], [('x', 'POSITIVE')], [('a', 'POSITIVE'), ('a', 'POSITIVE')]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                evaluate_outputs(rows, {'a': 'POSITIVE'})

    def test_malformed_response_bytes_are_saved_before_parse_error(self):
        for response in (b'upstream text', b'\xff'):
            with self.subTest(response=response):
                stream = io.StringIO()
                with self.assertRaises((json.JSONDecodeError, UnicodeDecodeError)):
                    record_native_response(stream, b'{"model":"fixture"}', response, 200, 'fixture')
                evidence = json.loads(stream.getvalue())
                self.assertEqual(evidence['http_status'], 200)
                self.assertIsNone(evidence['response'])
                self.assertEqual(base64.b64decode(evidence['response_bytes_base64']), response)

    def test_wrong_model_response_is_saved_before_audit_error(self):
        response = {'model':'other', 'choices':[{'finish_reason':'stop'}]}
        stream = io.StringIO()
        with self.assertRaises(ValueError):
            record_native_response(stream, b'{"model":"fixture"}', json.dumps(response).encode(), 200, 'fixture')
        self.assertEqual(json.loads(stream.getvalue())['response'], response)
