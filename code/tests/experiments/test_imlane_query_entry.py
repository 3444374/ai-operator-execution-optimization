"""Public method mode identity and unknown author-work propagation; no models."""
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig
from src.baselines.text.frameworks.native_calls import NativeCallExecutor
from src.experiments.postgresql.imlane_query import (
    IMLANE_METHOD_MODES, imlane_config_for_arm, load_imlane_call_configs,
)


def configs():
    return {mode: IMLaneCallConfig(mode, '/tmp/bridge.json', 1, 1, '/tmp/build.json')
            for mode in ('cpython', 'ray')}


class IMLaneEntryTests(unittest.TestCase):
    def test_modes_are_explicit_for_all_three_methods(self):
        values = configs()
        for arm, runtime in IMLANE_METHOD_MODES.items():
            self.assertIs(imlane_config_for_arm(arm, values), values[runtime])
            with self.assertRaisesRegex(ValueError, 'matching ready-runtime'):
                imlane_config_for_arm(arm, {})
        with self.assertRaisesRegex(ValueError, 'arm mode'):
            load_imlane_call_configs({'cpython': values['ray']})
        with self.assertRaisesRegex(ValueError, 'explicit cpython/ray'):
            load_imlane_call_configs({'ray-data': values['ray']})

    def test_private_file_configuration_is_loaded_and_extra_fields_rejected(self):
        from dataclasses import asdict
        import json
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'config.json'
            value = asdict(configs()['cpython'])
            path.write_text(json.dumps(value))
            self.assertEqual(load_imlane_call_configs({'cpython': path})['cpython'].runtime, 'cpython')
            value['fixture_endpoint'] = 'http://127.0.0.1/'
            path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, 'unsupported fields'):
                load_imlane_call_configs({'cpython': path})

    def test_factory_keeps_effective_limits_and_unknown_cleanup_on_early_close(self):
        class AuthorExecutor:
            def __init__(self, config, options, *, max_batch_rows, max_batch_bytes):
                self.options = options
                self.max_batch_rows = min(128, max_batch_rows)
                self.max_batch_bytes = min(32*1024**2, max_batch_bytes)
                self.last_close_report = None
            def iter_batch(self, calls, *, cancelled, timeout_s):
                try:
                    yield calls[0]
                    raise AssertionError('test should stop consumption')
                finally:
                    self.last_close_report = dict(clean=False, graph_consumer_stopped=True,
                        external_coordinator_requires_cleanup=True, author_batches_drained=False)
            def close(self):
                pass
        with mock.patch('src.baselines.text.frameworks.imlane_calls.IMLaneCallExecutor', AuthorExecutor), \
                mock.patch('src.baselines.text.frameworks.native_calls._GraphControl', side_effect=AssertionError('extra scheduler')):
            graph = NativeCallExecutor('imlane', imlane_config=configs()['cpython'])
            iterator = graph.iter_batch(('one', 'two'), timeout_s=3)
            self.assertEqual(next(iterator), 'one')
            iterator.close()
            self.assertEqual(graph.max_batch_rows, 128)
            self.assertFalse(graph.last_close_report['clean'])
            self.assertTrue(graph.last_close_report['external_coordinator_requires_cleanup'])

    def test_imlane_requires_explicit_config_and_cannot_change_other_backends(self):
        with self.assertRaises(TypeError):
            NativeCallExecutor('imlane')
        with self.assertRaisesRegex(ValueError, 'requires the IMLane'):
            NativeCallExecutor('daft', imlane_config=configs()['cpython'])


if __name__ == '__main__':
    unittest.main()
