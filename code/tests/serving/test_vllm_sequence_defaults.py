"""Unspecified native sequences are distinct from bounded client requests; CPU only."""
from dataclasses import asdict
from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.lotus_native_local_model import LocalService
from src.serving.backends import vllm_engine as common
from src.semantic_methods.lotus.records import RecordsResources


class SequenceDefaultTests(unittest.TestCase):
    def spec(self, **values):
        return common.EngineSpec('/tmp/sequence-fixture-model', '/tmp/model-python',
                                 '/tmp/driver-python', **values)

    def test_missing_and_null_sequence_values_survive_wire_roundtrip_as_unspecified(self):
        required = dict(model_path='/tmp/sequence-fixture-model', model_python='/tmp/model-python',
                        driver_python='/tmp/driver-python')
        for extra in ({}, {'max_num_seqs': None}):
            with self.subTest(extra=extra):
                spec = common.EngineSpec.from_dict(dict(required, **extra))
                self.assertIsNone(spec.max_num_seqs)
                self.assertIsNone(common.EngineSpec.from_dict(json.loads(json.dumps(asdict(spec)))).max_num_seqs)

    def test_explicit_sequence_values_preserve_the_user_value_and_validation(self):
        for value in (1, 8, 128):
            with self.subTest(value=value):
                spec = self.spec(max_num_seqs=value)
                self.assertEqual(common.EngineSpec.from_dict(asdict(spec)).max_num_seqs, value)
        for value in (False, True, 0, -1, 'auto', '8', 1.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.spec(max_num_seqs=value)

    def test_native_HTTP_reference_keeps_old_default_and_explicit_overrides(self):
        with tempfile.TemporaryDirectory() as folder:
            for value in (None, 1, 128):
                with self.subTest(value=value):
                    spec = self.spec(max_num_seqs=value)
                    service = LocalService(spec, 'fixture-model', RecordsResources(), Path(folder), 'fixture-device')
                    service.port = 39282
                    command = service.command()
                    self.assertEqual(command[command.index('--max-num-seqs')+1], str(8 if value is None else value))
                    for flag, expected in [('--max-model-len', spec.max_model_len),
                            ('--max-num-batched-tokens', spec.max_num_batched_tokens),
                            ('--gpu-memory-utilization', spec.gpu_memory_utilization)]:
                        self.assertEqual(command[command.index(flag)+1], str(expected))

    def test_finite_synchronous_constructor_retains_its_old_default_and_explicit_values(self):
        vllm, sampling = ModuleType('vllm'), ModuleType('vllm.sampling_params')
        vllm.__version__ = common.VLLM_VERSION
        vllm.SamplingParams = lambda **kwargs:kwargs
        sampling.StructuredOutputsParams = lambda **kwargs:kwargs
        calls = []
        vllm.LLM = lambda **kwargs:calls.append(kwargs) or SimpleNamespace()
        with patch.dict(sys.modules, {'vllm': vllm, 'vllm.sampling_params': sampling}), \
                patch.object(common.EngineSpec, 'validate_local', return_value={}), \
                patch.object(common, '_validate_current_runtime'), \
                patch.object(common.importlib.metadata, 'version', return_value=common.VLLM_VERSION):
            for value in (None, 1, 128):
                common.VllmEngine(self.spec(max_num_seqs=value), common.GenerationSpec())
        self.assertEqual([v['max_num_seqs'] for v in calls], [8, 1, 128])
        for kwargs in calls:
            self.assertEqual(kwargs['max_model_len'], 4096)
            self.assertEqual(kwargs['max_num_batched_tokens'], 4096)
            self.assertEqual(kwargs['gpu_memory_utilization'], .8)

    def test_unspecified_native_sequences_do_not_remove_owner_or_IPC_bounds(self):
        from src.execution_provider.adapters.lotus_records_core import create_records_core, resolve_core_profile
        from src.semantic_methods.lotus.records import FilterMethod
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=3)
        engine = self.spec()
        resolved = resolve_core_profile(engine, resources, dict(mode='async', max_inflight='auto'))
        self.assertEqual(resolved['max_inflight'], 3)
        self.assertIsNone(resolved['engine_max_num_seqs'])
        self.assertFalse(resolved['performance_tuned'])
        owner = create_records_core(FilterMethod('fixture', 'Keep {reviewText}'), engine,
            common.GenerationSpec(), resources, profile=dict(mode='async', max_inflight='auto'))
        try:
            self.assertIsNone(owner._actors.engine.max_num_seqs)
            self.assertEqual(owner._actors.limits.max_inflight, 3)
            self.assertEqual(owner._actors.limits.result_slots, 3)
            self.assertEqual(owner.capacity.limits(owner.resources).active_requests, 3)
        finally:
            owner.close(timeout=.1)

    def test_normal_Movie_check_preserves_omitted_null_and_explicit_engine_values(self):
        from src.experiments import lotus_records_movie as movie
        from tests.experiments import test_movie_async_token_entry as fixture
        helper = fixture.AsyncTokenEntryTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        for maximum in ('omitted', None, 128):
            with self.subTest(maximum=maximum):
                config = helper.config()
                if maximum == 'omitted':
                    config['engine'].pop('max_num_seqs')
                else:
                    config['engine']['max_num_seqs'] = maximum
                output = io.StringIO()
                with patch.object(movie, 'model_preflight', return_value=dict(inference_calls=0)) as preflight, \
                        patch('src.execution_provider.adapters.lotus_records_core.create_records_core') as allocate, redirect_stdout(output):
                    result = movie.main(['check', '--source-manifest', helper.helper.manifests['movie-q1'],
                        '--sembench-checkout', str(helper.root), '--config', str(helper.save(config))])
                self.assertEqual(result, 0)
                allocate.assert_not_called()
                expected = None if maximum == 'omitted' else maximum
                self.assertEqual(preflight.call_args.args[-1].max_num_seqs, expected)
                report = json.loads(output.getvalue())
                self.assertEqual(report['core_execution']['engine_max_num_seqs'], expected)
                self.assertEqual(report['record_batching']['R'], 2)
                self.assertEqual(report['record_batching']['B'], 100000)

    def test_both_session_lifecycles_forward_unspecified_and_explicit_values(self):
        from src.experiments import movie_records_session as session
        from src.execution_provider.adapters import lotus_records_core as core, lotus_records_async_actor as actor
        from tests.experiments import test_movie_records_session as fixture
        helper = fixture.PlanTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        for lifecycle in ('resident', 'isolated'):
            for maximum in ('omitted', None, 128):
                with self.subTest(lifecycle=lifecycle, maximum=maximum):
                    plan = deepcopy(helper.plan); plan['lifecycle'] = lifecycle
                    plan['resources']['max_active_batches'] = 3
                    plan['profile'] = dict(mode='async', max_inflight=2)
                    if maximum == 'omitted':
                        plan['engine'].pop('max_num_seqs', None)
                    else:
                        plan['engine']['max_num_seqs'] = maximum
                    ray = SimpleNamespace(is_initialized=lambda:False, init=lambda **kwargs:None)
                    with patch.dict(sys.modules, {'ray':ray}), patch.object(core, 'SemLoomRecordsCore') as owner, \
                            patch.object(actor, 'AsyncRecordsActorOwner') as actors, \
                            patch('psutil.Process', return_value=SimpleNamespace(pid=41, create_time=lambda:10.)):
                        owner.return_value.start.return_value = dict(pid=41, engine_uid='fixture')
                        driver = session._MovieDriver(plan, {}, Path(helper.manifests['movie-q1']).parent/(lifecycle+'-'+str(maximum)),
                            plan['queries'][0])
                        driver.start()
                    expected = None if maximum == 'omitted' else maximum
                    self.assertEqual(actors.call_args.args[1].max_num_seqs, expected)
                    self.assertEqual(actors.call_args.args[3].max_active_batches, 2)
