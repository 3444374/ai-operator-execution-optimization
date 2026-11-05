"""Native token-budget omission at SemLoom's async boundary; CPU fixtures only."""

import asyncio
from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import asdict
import io
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.sync_reference_engine import ReferenceVllmEngine
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
        for invalid in (False, True, 0, -1, 'auto', '4096', 1.5):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.spec(max_num_batched_tokens=invalid)

    def test_sync_reference_omits_unknown_token_budget_and_keeps_explicit_values(self):
        vllm, sampling = ModuleType('vllm'), ModuleType('vllm.sampling_params')
        vllm.__version__ = common.VLLM_VERSION
        vllm.SamplingParams = lambda **kwargs: kwargs
        sampling.StructuredOutputsParams = lambda **kwargs: kwargs
        calls = []
        vllm.LLM = lambda **kwargs: calls.append(kwargs) or SimpleNamespace()
        with patch.dict(sys.modules, {'vllm': vllm, 'vllm.sampling_params': sampling}), \
                patch.object(common.EngineSpec, 'validate_local', return_value={}), \
                patch.object(common, '_validate_current_runtime'), \
                patch.object(common.importlib.metadata, 'version', return_value=common.VLLM_VERSION):
            for maximum in (None, 1, 4096, 8192):
                with self.subTest(maximum=maximum):
                    spec = self.spec(max_num_batched_tokens=maximum)
                    ReferenceVllmEngine(spec, common.GenerationSpec())
                    if maximum is None:
                        self.assertNotIn('max_num_batched_tokens', calls[-1])
                    else:
                        self.assertEqual(calls[-1]['max_num_batched_tokens'], maximum)
                    self.assertEqual(calls[-1]['max_model_len'], 4096)

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
        signatures = []
        for maximum in ('omitted', None, 4096, 8192):
            with self.subTest(maximum=maximum):
                plan = deepcopy(helper.plan)
                if maximum == 'omitted':
                    plan['engine'].pop('max_num_batched_tokens')
                else:
                    plan['engine']['max_num_batched_tokens'] = maximum
                original = deepcopy(plan)
                identity = core.build_core_identity(plan, helper.checked,
                    hardware=helper.hardware, model=helper.model, software=helper.software)
                signatures.append(core.core_identity_signature(identity))
                selected = core.core_candidate_plan(plan, helper.checked,
                    dict(max_inflight=8, cpu_workers=1), identity=identity)
                self.assertEqual(selected['engine'], plan['engine'])
                self.assertEqual(selected['profile']['max_inflight'], 8)
                self.assertEqual(plan, original)
        self.assertEqual(len(set(signatures)), 4)

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
