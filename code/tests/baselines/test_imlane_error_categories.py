"""Real author UDF wrapper with explicit Arrow/kernel stand-ins; no author process."""
import importlib.util
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.native_calls import (
    NativeCall, NativeCallOutcome, _call_wire, _outcome_wire, _outcome_from_wire,
    outcome_kind, native_error_text,
)
from src.semantic_methods.lotus import native


def table(columns):
    return {key:[SimpleNamespace(as_py=lambda value=value:value) for value in values]
            for key, values in columns.items()}


class IMLaneErrorCategoryTests(unittest.TestCase):
    def udf(self, *, error=None, outcome=None):
        arrow, original = ModuleType('pyarrow'), ModuleType('integrations.sema.imlane_http_udf')
        arrow.string = arrow.int64 = lambda: None
        arrow.array = lambda values, **kwargs: values
        arrow.table = table
        def kernel(input):
            if error is not None:
                raise error
            return table(dict(native_outcome=[_outcome_wire(outcome)]))
        original.complete_http = kernel
        path = Path(__file__).resolve().parents[2]/'integrations/sema/imlane_complete_call.py'
        spec = importlib.util.spec_from_file_location('imlane_category_fixture', path)
        module = importlib.util.module_from_spec(spec)
        call = NativeCall('fixture', 0, b'{}', 'http://localhost/fixture', (), .1)
        with patch.dict('sys.modules', {'pyarrow':arrow, original.__name__:original}), \
             patch.dict('os.environ', {'IMLANE_HTTP_TRACE_DIR':''}):
            spec.loader.exec_module(module)
            result = module.complete_http(table(dict(native_call=[_call_wire(call)])))
        return _outcome_from_wire(result['native_outcome'][0].as_py())

    def test_capacity_and_identity_errors_do_not_enter_http_error_parser(self):
        for message in ('response capacity exceeded', 'input occurrence identity differs'):
            with self.subTest(message=message):
                outcome = self.udf(error=ValueError(message))
                self.assertEqual(outcome_kind(outcome), 'execution_error')
                self.assertEqual(outcome.transport_error, '')
                with patch.object(native, 'parse_lotus_response', side_effect=AssertionError('SDK parser entered')):
                    with self.assertRaisesRegex(ValueError, message) as caught:
                        native.parse_native_response(None, [], {}, None, outcome)
                self.assertEqual(caught.exception.native_execution_error, 'ValueError: '+message)
                self.assertIsNone(caught.exception.full_response)

    def test_actual_transport_outcome_remains_transport(self):
        original = NativeCallOutcome('fixture',0,None,'TimeoutError: fixture')
        outcome = self.udf(outcome=original)
        self.assertEqual(outcome, original)
        self.assertEqual(outcome_kind(outcome), 'transport_error')

    def test_author_failure_keeps_original_diagnostic_category(self):
        outcome = self.udf(error=RuntimeError('author UDF fixture'))
        self.assertEqual(outcome_kind(outcome), 'execution_error')
        self.assertEqual(outcome.execution_error, 'RuntimeError: author UDF fixture')

    def test_execution_error_keeps_http_cleanup_diagnostic_after_arrow_conversion(self):
        error = ValueError('response capacity exceeded')
        error.add_note('Native HTTP pool close also failed: OSError')
        outcome = self.udf(error=error)
        self.assertEqual(outcome_kind(outcome), 'execution_error')
        self.assertTrue(outcome.execution_error.startswith('ValueError: response capacity exceeded'))
        self.assertIn('pool close also failed: OSError', outcome.execution_error)

    def test_diagnostics_are_bounded_in_utf8_bytes(self):
        text = native_error_text(ValueError('中文'*4096))
        self.assertLessEqual(len(text.encode()),4096)
        self.assertTrue(text.startswith('ValueError:'))

    def test_normal_wire_keeps_legacy_fields_and_conflicts_are_rejected(self):
        outcome = NativeCallOutcome('fixture',0,None,'TimeoutError: fixture')
        self.assertEqual(set(json.loads(_outcome_wire(outcome))), {'call_id','row_id','response','transport_error'})
        with self.assertRaises(ValueError):
            outcome_kind(NativeCallOutcome('fixture',0,None,'TimeoutError: fixture','ValueError: fixture'))


if __name__ == '__main__':
    unittest.main()
