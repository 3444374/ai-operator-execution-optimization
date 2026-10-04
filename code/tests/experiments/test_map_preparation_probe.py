"""Probe observer/cleanup use the real APIs before any loopback HTTP run."""

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.adapters.ray_map_transport import RayMapTransport
from src.experiments.map_observation_probe import SyntheticTable
from tests.execution_provider.test_ray_map_transport import FakeRay


class PreparationProbeTests(unittest.TestCase):
    def run_fixture(self, root, *, invalid=False, staged=False):
        path=Path(__file__).resolve().parents[3]/'experiments/results/diagnostics/map_input_preparation_20261004/raw/run_server.py'
        spec=importlib.util.spec_from_file_location('probe',path)
        probe=importlib.util.module_from_spec(spec);spec.loader.exec_module(probe)
        endpoint=SimpleNamespace(config=probe.FixedModelConfig('http://localhost/unused','model',1000),
                                 calls=[],active=0,peak=1,failures=[],capture=None)
        async def execute(table,index,template):
            body=table['payload'][index].as_py()
            sequence=template.key.sequence
            endpoint.calls.append(dict(sequence=sequence,request_sha256=hashlib.sha256(body).hexdigest()))
            reply=json.dumps(dict(model='model',choices=[dict(message=dict(
                content=probe.output_value('row',sequence,'platform')),finish_reason='stop')],
                usage=dict(prompt_tokens=2,completion_tokens=1))).encode()
            return template.key,b'invalid' if invalid else reply,1,2
        ray=FakeRay(execute)
        def factory(physical,guard):
            return lambda config,**kwargs:build_fixed_model_execution(config,**kwargs,
                preparation_factory=(lambda transport,limits,notify:transport.prepare_inputs(limits,notify)) if staged else None,
                transport_factory=lambda *args:RayMapTransport(*args,physical=physical,before_request=guard,ray_api=ray))
        def batches(data, limits, **kwargs):yield SyntheticTable(data)
        with patch.object(probe,'ray_map_factory',factory), \
             patch('src.execution_provider.adapters.ray_map_transport.iter_payload_batches',batches), \
             patch('src.execution_provider.adapters.map_preparation.iter_payload_batches',batches), \
             patch('socket.socket.connect',side_effect=AssertionError('network forbidden in probe unit')):
            return probe.run_case(root,endpoint,'fixture-cluster','arrow',staged,4)

    def test_observer_arguments_match_the_actual_durable_guard(self):
        with tempfile.TemporaryDirectory() as root:
            for staged in (False,True):
                with self.subTest(staged=staged):
                    result=self.run_fixture(Path(root)/str(staged),staged=staged)
                    self.assertEqual(result['status'],'passed')
                    self.assertEqual(result['accounted_requests'],4)
                    self.assertFalse(result['cleanup_errors'])

    def test_failed_decoder_releases_unpublished_delivery_leases_and_closes_shared_unit(self):
        with tempfile.TemporaryDirectory() as root:
            for staged in (False,True):
                with self.subTest(staged=staged):
                    output=Path(root)/str(staged)
                    with self.assertRaisesRegex(RuntimeError,'evidence retained'):
                        self.run_fixture(output,invalid=True,staged=staged)
                    result=json.loads((output/'result.json').read_text())
                    self.assertEqual(result['status'],'failed')
                    self.assertFalse(result['cleanup_errors'])
