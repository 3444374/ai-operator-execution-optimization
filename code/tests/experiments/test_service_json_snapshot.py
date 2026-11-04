"""A persisted or caller-supplied receipt must keep the original JSON types."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from src.experiments import movie_records_service as service_results
from tests.experiments import test_movie_service_receipts as original
from tests.execution_provider.test_lotus_records_async_actor import CPUProcess


def _zero_completion_command(_spec, descriptors):
    return [sys.executable, str(Path(__file__).resolve()), *map(str, descriptors)]


class ServiceTypedSnapshotTests(unittest.TestCase):
    setUp = original.ServiceReceiptTests.setUp

    def _closed_zero_completion(self, check):
        with patch.object(CPUProcess, '_command', side_effect=_zero_completion_command):
            original.ServiceReceiptTests._closed_result(self, 'hold', check)

    def test_existing_file_with_boolean_completion_cannot_accept_integer_receipt(self):
        def check(owner, report, expected):
            retained = deepcopy(owner.last_query_report['unconsumed_call_results'])
            self.assertIs(type(retained[0]['completion_tokens']), int)
            self.assertEqual(retained[0]['completion_tokens'], 0)
            with patch.object(owner, 'take_unconsumed_results', side_effect=ValueError('CPU acceptance interrupted')):
                with self.assertRaises(ValueError):
                    service_results.accept_service_results(owner, report, expected, self.path)
            artifact, = self.path.iterdir()
            saved = json.loads(artifact.read_text())
            self.assertIs(type(saved['retained'][0]['completion_tokens']), int)
            saved['retained'][0]['completion_tokens'] = False
            artifact.write_text(json.dumps(saved, ensure_ascii=False, indent=2) + '\n')
            changed = artifact.read_bytes()
            with patch.object(service_results, 'write_private_json') as write, \
                    patch.object(owner, 'take_unconsumed_results', wraps=owner.take_unconsumed_results) as take:
                with self.assertRaisesRegex(ValueError, 'differs'):
                    service_results.accept_service_results(owner, report, expected, self.path)
                write.assert_not_called()
                take.assert_not_called()
            self.assertEqual(artifact.read_bytes(), changed)
            self.assertEqual(self.case.pool.snapshot()['allocations'], 1)
            self.assertIs(type(owner.last_query_report['unconsumed_call_results'][0]['completion_tokens']), int)
            self.assertEqual(owner.take_unconsumed_results(expected=retained), retained)
            self.assertEqual(self.case.pool.snapshot()['allocations'], 0)
        self._closed_zero_completion(check)

    def test_boolean_expected_completion_keeps_the_original_receipt_and_grant(self):
        def check(owner, _report, _expected):
            retained = deepcopy(owner.last_query_report['unconsumed_call_results'])
            self.assertIs(type(retained[0]['completion_tokens']), int)
            self.assertEqual(retained[0]['completion_tokens'], 0)
            wrong = deepcopy(retained)
            wrong[0]['completion_tokens'] = False
            before = owner.last_close_report
            with self.assertRaisesRegex(ValueError, 'changed'):
                owner.take_unconsumed_results(expected=wrong)
            self.assertIs(owner.last_close_report, before)
            self.assertEqual(self.case.pool.snapshot()['allocations'], 1)
            self.assertIs(type(owner.last_query_report['unconsumed_call_results'][0]['completion_tokens']), int)
            reordered = [dict(reversed(tuple(item.items()))) for item in retained]
            self.assertEqual(owner.take_unconsumed_results(expected=reordered), retained)
            self.assertEqual(self.case.pool.snapshot()['allocations'], 0)
            self.assertEqual(owner.take_unconsumed_results(expected=[]), [])
        self._closed_zero_completion(check)


if __name__ == '__main__':
    import asyncio
    from src.serving.backends.async_engine_process import serve
    from tests.serving.async_engine_fixture import FixtureEngine

    class ZeroCompletionEngine(FixtureEngine):
        async def generate_prepared(self, *args, **kwargs):
            result = await super().generate_prepared(*args, **kwargs)
            result.update(completion_tokens=0, total_tokens=result['prompt_tokens'],
                          output_token_ids_json='[]')
            return result

    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=ZeroCompletionEngine))
