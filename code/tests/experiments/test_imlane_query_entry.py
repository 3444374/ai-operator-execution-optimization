"""Public method mode identity and unknown author-work propagation; no models."""
from dataclasses import replace
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
    def test_all_six_private_capacities_reach_both_author_factories(self):
        from dataclasses import asdict
        import json
        from types import SimpleNamespace
        from src.baselines.text.frameworks.imlane_calls import IMLaneCallExecutor
        from src.experiments.postgresql.imlane_query import imlane_runtime_identity
        limits = dict(max_batch_rows=256, max_input_payload_bytes=4096, max_input_wire_bytes=8192,
                      max_item_payload_bytes=1024, max_pending_calls=8, max_batch_response_bytes=2048)
        with tempfile.TemporaryDirectory() as temporary:
            def delegate(*args, **kwargs):
                return SimpleNamespace(config={'namespace': 'fixture'}, work_root=Path(temporary),
                                       limits=kwargs['limits'], last_close_report=None)
            with mock.patch('src.baselines.text.frameworks.imlane_calls.SemaIMLaneExecutor', side_effect=delegate):
                for mode in ('cpython', 'ray'):
                    path = Path(temporary) / (mode + '.json')
                    path.write_text(json.dumps(dict(asdict(configs()[mode]), **limits)))
                    config = load_imlane_call_configs({mode: path})[mode]
                    direct = IMLaneCallExecutor(config)
                    graph = NativeCallExecutor('imlane', imlane_config=config)
                    self.assertEqual(asdict(direct.delegate.limits), limits)
                    self.assertEqual(asdict(graph._imlane.delegate.limits), limits)
                    self.assertEqual(imlane_runtime_identity(config)['configured_limits'], limits)
                    # The caller's smaller source allowance still applies without resetting other capacities.
                    lower = NativeCallExecutor('imlane', imlane_config=config,
                                               max_batch_rows=64, max_batch_bytes=512)
                    self.assertEqual(asdict(lower._imlane.delegate.limits),
                                     dict(limits, max_batch_rows=64, max_input_payload_bytes=512))

    def test_private_capacity_types_and_maxima_are_checked_before_factory(self):
        from dataclasses import asdict
        from src.baselines.text.frameworks.imlane_limits import IMLaneBatchLimits
        import json
        defaults = asdict(IMLaneBatchLimits())
        caps = dict(defaults, max_batch_rows=4096)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'config.json'
            for name, maximum in caps.items():
                for invalid in (True, 0, '1', maximum + 1):
                    path.write_text(json.dumps(dict(asdict(configs()['cpython']), **{name: invalid})))
                    with self.subTest(name=name, invalid=invalid), self.assertRaises(ValueError):
                        load_imlane_call_configs({'cpython': path})

    def test_rejected_input_cannot_reuse_a_previous_clean_report(self):
        from types import SimpleNamespace
        from src.baselines.text.frameworks.imlane_calls import IMLaneCallExecutor
        with tempfile.TemporaryDirectory() as temporary, mock.patch(
                'src.baselines.text.frameworks.imlane_calls.SemaIMLaneExecutor') as factory:
            factory.return_value = SimpleNamespace(config={'namespace': 'fixture'},
                work_root=Path(temporary), last_close_report={'clean': True})
            executor = IMLaneCallExecutor(configs()['cpython'])
            executor.last_close_report = {'clean': True}
            executor.last_preflight = {'input_rows': 1}
            with self.assertRaisesRegex(ValueError, 'non-NativeCall'):
                list(executor.iter_batch((object(),)))
            self.assertIsNone(executor.last_close_report)
            self.assertIsNone(executor.last_preflight)
            self.assertIsNone(executor.delegate.last_close_report)

    def test_selected_capacity_reaches_real_public_factory_and_legacy_remains_128(self):
        from types import SimpleNamespace
        from src.baselines.text.frameworks.imlane_calls import IMLaneCallExecutor
        with tempfile.TemporaryDirectory() as temporary:
            def delegate(*args, **kwargs):
                return SimpleNamespace(config={'namespace':'fixture'}, work_root=Path(temporary), limits=kwargs['limits'])
            with mock.patch('src.baselines.text.frameworks.imlane_calls.SemaIMLaneExecutor', side_effect=delegate):
                for maximum in (128, 2000):
                    selected = replace(configs()['cpython'], max_batch_rows=maximum)
                    direct = IMLaneCallExecutor(selected)
                    graph = NativeCallExecutor('imlane', imlane_config=selected)
                    self.assertEqual(direct.max_batch_rows, maximum)
                    self.assertEqual(graph.max_batch_rows, maximum)
                    self.assertEqual(graph._imlane.delegate.limits.max_batch_rows, maximum)
                    lower = NativeCallExecutor('imlane', imlane_config=selected, max_batch_rows=64)
                    self.assertEqual(lower.max_batch_rows, 64)

    def test_private_capacity_field_is_loaded_and_invalid_values_are_rejected(self):
        from dataclasses import asdict
        import json
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'config.json'
            value = asdict(configs()['cpython'])
            value.pop('max_batch_rows')
            path.write_text(json.dumps(value))
            self.assertEqual(load_imlane_call_configs({'cpython':path})['cpython'].max_batch_rows, 128)
            value['max_batch_rows'] = 2000
            path.write_text(json.dumps(value))
            self.assertEqual(load_imlane_call_configs({'cpython':path})['cpython'].max_batch_rows, 2000)
        for invalid in (0, True, '2000', 4097):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                replace(configs()['cpython'], max_batch_rows=invalid)
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
        from src.baselines.text.frameworks.native_calls import NativeCallOutcome
        first = NativeCallOutcome('one', 0, None, '')
        class AuthorExecutor:
            def __init__(self, config, options, *, max_batch_rows, max_batch_bytes):
                self.options = options
                self.max_batch_rows = min(128, max_batch_rows)
                self.max_batch_bytes = min(32*1024**2, max_batch_bytes)
                self.last_close_report = None
            def iter_batch(self, calls, *, cancelled, timeout_s):
                try:
                    yield first
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
            self.assertIs(next(iterator), first)
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
