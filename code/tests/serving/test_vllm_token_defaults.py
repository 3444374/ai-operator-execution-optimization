"""Native token-budget omission at SemLoom's async boundary; CPU fixtures only."""

import asyncio
from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import asdict
import io
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.lotus_native_local_model import LocalService
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends import vllm_engine as common


class NativeTokenDefaultTests(unittest.TestCase):
    def spec(self, **values):
        # Isolate iteration-token defaults from delegated context confirmation.
        values.setdefault('max_model_len', 4096)
        return common.EngineSpec('/tmp/token-budget-fixture', '/tmp/model-python',
                                 '/tmp/driver-python', **values)

    def test_missing_null_and_explicit_values_survive_config_and_wire_copies(self):
        required = dict(model_path='/tmp/token-budget-fixture', model_python='/tmp/model-python',
                        driver_python='/tmp/driver-python', max_model_len=4096)
        for extra in ({}, {'max_num_batched_tokens': None}, {'max_num_batched_tokens': 1},
                      {'max_num_batched_tokens': 4096}, {'max_num_batched_tokens': 8192}):
            with self.subTest(extra=extra):
                spec = common.EngineSpec.from_dict(dict(required, **extra))
                copied = common.EngineSpec.from_dict(json.loads(json.dumps(asdict(spec))))
                self.assertEqual(copied.max_num_batched_tokens, extra.get('max_num_batched_tokens'))
                self.assertEqual(copied.max_model_len, 4096)
                self.assertEqual(copied.gpu_memory_utilization, .8)
        for invalid in (False, True, 0, -1, 'auto', '4096', 1.5):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.spec(max_num_batched_tokens=invalid)

    def test_sync_and_LOTUS_native_preserve_old_default_and_explicit_values(self):
        from src.baselines.text.frameworks.sync_reference_engine import ReferenceVllmEngine
        vllm, sampling = ModuleType('vllm'), ModuleType('vllm.sampling_params')
        vllm.__version__ = common.VLLM_VERSION
        vllm.SamplingParams = lambda **kwargs: kwargs
        sampling.StructuredOutputsParams = lambda **kwargs: kwargs
        calls = []
        vllm.LLM = lambda **kwargs: calls.append(kwargs) or SimpleNamespace()
        with tempfile.TemporaryDirectory() as folder, \
                patch.dict(sys.modules, {'vllm': vllm, 'vllm.sampling_params': sampling}), \
                patch.object(common.EngineSpec, 'validate_local', return_value={}), \
                patch.object(common, '_validate_current_runtime'), \
                patch.object(common.importlib.metadata, 'version', return_value=common.VLLM_VERSION):
            for maximum in (None, 1, 4096, 8192):
                with self.subTest(maximum=maximum):
                    spec = self.spec(max_num_batched_tokens=maximum)
                    ReferenceVllmEngine(spec, common.GenerationSpec())
                    service = LocalService(spec, 'fixture-model', RecordsResources(), Path(folder), 'fixture-device')
                    service.port = 39282
                    command = service.command()
                    expected = 4096 if maximum is None else maximum
                    self.assertEqual(command[command.index('--max-num-batched-tokens')+1], str(expected))
                    self.assertEqual(calls[-1]['max_num_batched_tokens'], expected)
                    self.assertEqual(calls[-1]['max_model_len'], 4096)
                    self.assertEqual(calls[-1]['gpu_memory_utilization'], .8)

    def test_request_context_and_W_checks_remain_independent_of_native_iteration_budget(self):
        from src.execution_provider.adapters import lotus_chain_token_budget as tokens
        from src.serving.backends.vllm_async_engine import VllmAsyncEngine
        from tests.serving.test_async_engine_process import row

        async def too_long(spec):
            VllmAsyncEngine(spec, common.GenerationSpec(max_tokens=4097))

        for maximum in (None, 1, 8192):
            with self.subTest(maximum=maximum):
                spec = self.spec(max_num_batched_tokens=maximum)
                budget = tokens.ChainTokenBudget(object(), spec, active_tokens=8192,
                    tokenizer_signature='0'*64, template_signature='1'*64,
                    expected_output={'map': tokens.OutputTokenEstimate(2, 'CPU fixture')})
                with patch.object(tokens, 'chat_token_count', return_value=20):
                    cost = budget.estimate(row(), common.GenerationSpec())
                self.assertEqual(cost['reserved_tokens'], 28)
                with patch.object(tokens, 'chat_token_count', return_value=4090), \
                        self.assertRaisesRegex(ValueError, 'model context'):
                    budget.estimate(row(), common.GenerationSpec())
                budget.active_tokens = 27
                with patch.object(tokens, 'chat_token_count', return_value=20), \
                        self.assertRaisesRegex(ValueError, 'exceeds W'):
                    budget.estimate(row(), common.GenerationSpec())
                with self.assertRaisesRegex(ValueError, 'max_tokens exceeds max_model_len'):
                    asyncio.run(too_long(spec))

    def test_calibration_identity_keeps_explicit_4096_separate_from_unspecified(self):
        from src.experiments.calibration import core
        from tests.experiments.test_core_calibration import CoreCalibrationTests
        helper = CoreCalibrationTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        explicit = core.core_identity_signature(helper.identity)
        for missing in (False, True):
            with self.subTest(missing=missing):
                plan = deepcopy(helper.plan)
                if missing:
                    plan['engine'].pop('max_num_batched_tokens')
                else:
                    plan['engine']['max_num_batched_tokens'] = None
                identity = core.build_core_identity(plan, helper.checked,
                    hardware=helper.hardware, model=helper.model, software=helper.software)
                self.assertNotEqual(core.core_identity_signature(identity), explicit)
                selected = core.core_candidate_plan(plan, helper.checked,
                    dict(max_inflight=8, cpu_workers=1), identity=identity)
                self.assertEqual(selected['engine'], plan['engine'])
                self.assertEqual(selected['profile']['max_inflight'], 8)

    def test_Movie_check_preserves_missing_null_and_explicit_budgets(self):
        from src.experiments import lotus_records_movie as movie
        from tests.experiments import test_movie_async_token_entry as fixture
        helper = fixture.AsyncTokenEntryTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        for maximum in ('omitted', None, 1, 4096, 8192):
            with self.subTest(maximum=maximum):
                config = helper.config()
                if maximum == 'omitted':
                    config['engine'].pop('max_num_batched_tokens', None)
                else:
                    config['engine']['max_num_batched_tokens'] = maximum
                output = io.StringIO()
                with patch.object(movie, 'model_preflight', return_value=dict(inference_calls=0)) as preflight, \
                        patch('src.execution_provider.adapters.lotus_records_core.create_records_core') as allocate, \
                        redirect_stdout(output):
                    status = movie.main(['check', '--source-manifest', helper.helper.manifests['movie-q1'],
                        '--sembench-checkout', str(helper.root), '--config', str(helper.save(config))])
                self.assertEqual(status, 0)
                allocate.assert_not_called()
                engine = preflight.call_args.args[-1]
                self.assertEqual(engine.max_num_batched_tokens, None if maximum == 'omitted' else maximum)
                self.assertEqual(engine.max_model_len, 4096)
                report = json.loads(output.getvalue())
                self.assertEqual(report['record_batching']['B'], 100000)
                self.assertEqual(report['record_batching']['R'], 2)

    def test_existing_admission_binding_isolates_native_budget_without_changing_cache_format(self):
        from src.experiments.movie_matrix_support import run_record_admission
        from src.execution_provider.adapters.lotus_records_core import resolve_core_profile
        from src.semantic_methods.lotus.records import FilterMethod
        from src.scheduling.runtime import calibration
        from src.scheduling.runtime.calibration import identity_signature
        identity = {group: {'fixture': group} for group in
            ('hardware', 'model', 'generation', 'execution', 'workload', 'measurement')}
        identity['measurement']['work_unit'] = 'model_calls'
        directory = tempfile.TemporaryDirectory(); self.addCleanup(directory.cleanup)
        manifest = Path(directory.name)/'fixture-manifest.json'; manifest.write_text('{}')
        cache = dict(identity=identity, cache_dir=str(Path(directory.name)/'cache'))
        original = deepcopy(cache)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=2)
        profile = dict(mode='async', max_inflight=1, admission=dict(kind='legacy_resource_feedback'))
        modern = resolve_core_profile(self.spec(), resources,
            dict(profile, admission=dict(kind='resource_feedback')))
        with self.assertRaisesRegex(ValueError, 'requires explicit legacy_resource_feedback'):
            run_record_admission(cache, modern, SimpleNamespace(manifest_path=manifest),
                FilterMethod('fixture', 'Keep {reviewText}'), common.GenerationSpec(), self.spec(),
                resources, lambda *args, **kwargs:self.fail('no query execution'))
        signatures = []
        for maximum in ('omitted', None, 4096, 8192):
            with self.subTest(maximum=maximum):
                engine = asdict(self.spec())
                if maximum == 'omitted':
                    engine.pop('max_num_batched_tokens')
                else:
                    engine['max_num_batched_tokens'] = maximum
                spec = common.EngineSpec.from_dict(engine)
                resolved = resolve_core_profile(spec, resources, profile)
                with patch.object(calibration, 'run_admission_query',
                        return_value=dict(result=dict(query_report=dict(status='CPU fixture')))) as select:
                    run_record_admission(cache, resolved, SimpleNamespace(manifest_path=manifest),
                        FilterMethod('fixture', 'Keep {reviewText}'), common.GenerationSpec(), spec,
                        resources, lambda *args, **kwargs:self.fail('no query execution'))
                bound = select.call_args.args[0]
                self.assertEqual(bound['model']['actual']['max_num_batched_tokens'],
                    None if maximum == 'omitted' else maximum)
                signatures.append(identity_signature(bound))
                self.assertEqual(cache, original)
        self.assertEqual(signatures[0], signatures[1])
        self.assertEqual(len(set(signatures)), 3)

    def test_resident_and_isolated_sessions_forward_unspecified_and_explicit_budgets(self):
        from src.experiments import movie_records_session as session
        from src.execution_provider.adapters import lotus_records_core as core, lotus_records_async_actor as actor
        from tests.experiments import test_movie_records_session as fixture
        helper = fixture.PlanTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        for lifecycle in ('resident', 'isolated'):
            for maximum in ('omitted', None, 1, 4096, 8192):
                with self.subTest(lifecycle=lifecycle, maximum=maximum):
                    plan = deepcopy(helper.plan); plan['lifecycle'] = lifecycle
                    plan['engine']['max_model_len'] = 4096
                    plan['resources']['max_active_batches'] = 3
                    plan['profile'] = dict(mode='async', max_inflight=2)
                    if maximum == 'omitted':
                        plan['engine'].pop('max_num_batched_tokens', None)
                    else:
                        plan['engine']['max_num_batched_tokens'] = maximum
                    ray = SimpleNamespace(is_initialized=lambda:False, init=lambda **kwargs:None)
                    with patch.dict(sys.modules, {'ray':ray}), patch.object(core, 'SemLoomRecordsCore') as owner, \
                            patch.object(actor, 'AsyncRecordsActorOwner') as actors, \
                            patch('psutil.Process', return_value=SimpleNamespace(pid=41, create_time=lambda:10.)):
                        owner.return_value.start.return_value = dict(pid=41, engine_uid='fixture')
                        driver = session._MovieDriver(plan, {}, Path(helper.manifests['movie-q1']).parent/
                            ('token-default-'+lifecycle+'-'+str(maximum)),
                            plan['queries'][0])
                        driver.start()
                    engine = actors.call_args.args[1]
                    self.assertEqual(engine.max_num_batched_tokens, None if maximum == 'omitted' else maximum)
                    self.assertEqual(engine.max_model_len, 4096)
                    self.assertEqual(actors.call_args.args[3].max_active_batches, 3)
