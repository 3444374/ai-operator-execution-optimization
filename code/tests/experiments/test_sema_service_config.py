"""Sema response/source capacities through the public query and resident factory."""

from contextlib import ExitStack
from dataclasses import asdict
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.experiments.postgresql.sema_native_query import (
    SemaServiceConfig, open_native_service, validate_native_options,
)


class SemaServiceConfigTests(unittest.TestCase):
    def config(self):
        return SemaServiceConfig(request_bytes=8192, response_bytes=4096, intake_requests=3,
            intake_bytes=49152, snapshot_rows=2, snapshot_payload_bytes=16384,
            pending_response_bytes=12288, cumulative_response_bytes=32768, query_response_bytes=65536)

    def test_public_configuration_roundtrip_retains_all_capacities(self):
        config = self.config()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'service.json'
            path.write_text(json.dumps(asdict(config)))
            loaded = SemaServiceConfig.load(path)
        self.assertEqual(loaded, config)
        service = loaded.service_limits(11, 5)
        self.assertEqual((service.max_requests, service.request_bytes, service.response_bytes,
            service.intake_requests, service.intake_bytes, service.pending_response_bytes,
            service.cumulative_response_bytes), (11, 8192, 4096, 3, 49152, 12288, 65536))
        graph = loaded.native_limits(4)
        self.assertEqual(asdict(graph), dict(snapshot_rows=2, snapshot_payload_bytes=16384,
            pending_response_bytes=12288, cumulative_response_bytes=32768))

    def test_invalid_capacities_fail_before_query_reservation(self):
        for field in ('snapshot_rows', 'snapshot_payload_bytes', 'pending_response_bytes',
                      'cumulative_response_bytes', 'query_response_bytes', 'intake_bytes'):
            with self.subTest(field=field), self.assertRaises(ValueError):
                SemaServiceConfig(**{field: 0})
        with self.assertRaisesRegex(ValueError, 'held request'):
            validate_native_options('sema-native-daft-request-service', SimpleNamespace(concurrency=2), 2,
                                    service_config=SemaServiceConfig(snapshot_rows=3))

    def test_daft_and_raydata_factory_pass_public_capacities_to_real_service_type(self):
        from src.execution_provider.adapters.sema_native import SemaNativeService
        for arm, backend in (('sema-native-daft-request-service', 'daft'),
                             ('sema-native-ray-data-request-service', 'ray-data')):
            executor = SimpleNamespace(backend=backend, iter_batch=lambda *_a, **_k: iter(()))
            with self.subTest(arm=arm), mock.patch.object(SemaNativeService, '__enter__', lambda self: self), \
                    mock.patch.object(SemaNativeService, '__exit__', lambda *_args: None), \
                    mock.patch('src.execution_provider.adapters.sema_native.NativeCallExecutor',
                               return_value=executor) as create, ExitStack() as stack:
                config = self.config()
                service = open_native_service(stack, arm, query_id='factory',
                    model=FixedModelConfig('http://localhost/model', 'fixture', 5000),
                    limits=config.service_limits(8, 5), trace_path=Path('/tmp/unused-sema-config.jsonl'),
                    options=NativeGraphOptions(), held_tasks=4, service_config=config)
                self.assertEqual(service.native_limits, config.native_limits(4))
                self.assertEqual(create.call_args.kwargs['max_batch_rows'], 2)
                self.assertEqual(create.call_args.kwargs['max_batch_bytes'], 32768)

    def test_resident_factory_loads_configuration_before_runtime_start(self):
        from src.execution_provider.semantic_map import SemanticMapPlan
        from src.experiments.postgresql.persistent_native_adapter_query import PersistentAdapterGroup
        config = self.config()
        group = PersistentAdapterGroup(('sema-native-daft-request-service',),
            plan=SemanticMapPlan('Return one JSON label.', 'fixture', 8),
            model=FixedModelConfig('http://localhost/model', 'fixture', 5000),
            ledger=object(), root=Path('/tmp/unused-resident-sema'), max_held_tasks=4,
            sema_service_config=config)
        self.assertEqual(group.sema_service_config, config)
        self.assertEqual(group.owners, {})

    def test_cli_rejects_service_configuration_on_native_direct_and_other_systems(self):
        from src.experiments.postgresql.native_adapter_query import main
        for arm in ('sema-native-direct', 'fixed-map-native-daft'):
            args = ['--arm', arm, '--input', '/tmp/unused-input', '--plan', '/tmp/unused-plan',
                    '--model', '/tmp/unused-model', '--budget', '/tmp/unused-budget',
                    '--output', '/tmp/unused-output', '--unit-id', 'q', '--budget-id', 'b',
                    '--max-attempts', '1', '--sema-service-config', '/tmp/unused-service']
            with self.subTest(arm=arm), mock.patch('sys.stderr'), self.assertRaises(SystemExit) as raised:
                main(args)
            self.assertEqual(raised.exception.code, 2)

    def test_public_author_verifier_rejects_unpinned_binary_before_runtime_or_fixture(self):
        from integrations.sema import verify_public
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'unverified'
            binary.write_bytes(b'not an author executable')
            with mock.patch.object(verify_public.sys, 'platform', 'linux'), mock.patch('sys.stderr'), \
                    mock.patch.object(verify_public, 'library_identity') as runtime, \
                    mock.patch.object(verify_public.fixture_module, 'fixture_server') as fixture, \
                    self.assertRaises(SystemExit) as raised:
                verify_public.main(['--arm', 'sema-native-daft-request-service',
                    '--binary', str(binary), '--output', str(root / 'new-output')])
            self.assertEqual(raised.exception.code, 2)
            runtime.assert_not_called()
            fixture.assert_not_called()
            self.assertFalse((root / 'new-output').exists())
