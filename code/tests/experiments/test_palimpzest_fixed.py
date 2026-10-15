"""Fixed-plan controls; optional actual PZ/SDK tests use deterministic CPU fixtures."""

import base64
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from io import StringIO
import json
import os
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.baselines.common.private_artifacts import write_private_json
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.experiments.palimpzest.fixed import BackendRunProfile, FixedEnvironment, FixedMovieProfile, select_fixed_movie
from src.experiments.palimpzest.fixed_runner import _prepare_daft_runtime, execute_fixed_movie, executor_identity
from src.experiments.palimpzest.model import LocalModelBinding, fetch_model_metadata, verify_model_metadata
from src.experiments.palimpzest.native import NativeOptions
from src.experiments.palimpzest.source import load_palimpzest
from src.experiments.postgresql.query_workloads import file_identity, prepare
from src.experiments.postgresql.sembench_queries import MovieDataset
from src.semantic_methods.palimpzest.backend import PalimpzestAdapterError


class FixedProfileTests(unittest.TestCase):
    def test_limit_sampling_and_incomplete_external_identity_are_rejected(self):
        for values in ({'query_id': 2}, {'query_id': True}, {'sampling_validation': 'silently-skip'},
                       {'sampling_validation': 'sentinel'}, {'options': {'max_workers': 1}}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                FixedMovieProfile(**values)
        for digest in ('', 'unknown', 'A'*64):
            with self.subTest(digest=digest), self.assertRaises(ValueError):
                FixedEnvironment(digest, 'b'*64, 'c'*64)
        with self.assertRaises(ValueError):
            BackendRunProfile('d'*64, mode='automatic-tuning')
        with self.assertRaisesRegex(ValueError, 'native-auto is not yet qualified'):
            BackendRunProfile('d'*64, mode='native-auto')
        self.assertEqual(BackendRunProfile('d'*64).mode, 'fixed')

    def test_arbitrary_callable_cannot_be_named_vendor_or_semloom(self):
        fake = Mock(iter_batch=Mock())
        for arm in ('native-adapted', 'daft', 'ray-data', 'imlane-cpython', 'imlane-ray', 'semloom', 'unknown'):
            with self.subTest(arm=arm), self.assertRaises(ValueError):
                executor_identity(arm, fake)

    def test_other_arms_skip_daft_runtime_import_and_setup(self):
        import builtins
        original = builtins.__import__
        def forbid(name,*args,**kwargs):
            if name=='daft' or name.startswith('daft.') or name=='src.infrastructure.daft_runtime':
                raise AssertionError('non-Daft runtime setup imported Daft')
            return original(name,*args,**kwargs)
        for arm in ('original-native','native-adapted','ray-data','imlane-cpython','imlane-ray','semloom'):
            with self.subTest(arm=arm), patch('builtins.__import__',side_effect=forbid):
                evidence = {'unchanged':True}
                _prepare_daft_runtime(arm,evidence)
                self.assertEqual(evidence,{'unchanged':True})


def native_response(answer):
    return SimpleNamespace(model='fixed-fixture-model',
        choices=[SimpleNamespace(message=SimpleNamespace(content='ANSWER: '+answer+'\n---'))],
        usage=SimpleNamespace(model_dump=lambda: dict(prompt_tokens=11, completion_tokens=3)))


@contextmanager
def daft_module_fixture(module):
    # Restore only the replaced key; clearing newly imported extension modules
    # from sys.modules can make their subsequent initialization invalid.
    present,previous = 'daft' in sys.modules,sys.modules.get('daft')
    sys.modules['daft'] = module
    try:
        yield
    finally:
        if present:
            sys.modules['daft'] = previous
        else:
            sys.modules.pop('daft',None)


@unittest.skipUnless(os.environ.get('SEMLOOM_PZ_LIBRARY_TEST') == '1',
                     'requires explicitly pinned PZ/SDK environment; no actual model or HTTP')
class FixedMovieLibraryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pz_root = Path(os.environ['SEMLOOM_TEST_PALIMPZEST_ROOT'])
        cls.bench = Path(os.environ['SEMLOOM_TEST_SEMBENCH_ROOT'])
        cls.pz = load_palimpzest(cls.pz_root)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        manifest = prepare(self.root/'data', 'movie', [
            ('movie-row-'+str(i), 'taken_3', 'software fixture '+str(i), 'POSITIVE', 'repeated-public-id')
            for i in range(2)], {'source': 'synthetic SemBench software fixture'}, max_rows=2)
        self.dataset = MovieDataset(self.bench, self.root/'data/manifest.json',
            expected_manifest_digest=manifest['sha256'], role='software_fixture')
        self.binding = LocalModelBinding(FixedModelConfig(
            'http://127.0.0.1:8000/v1/chat/completions', 'fixed-fixture-model', 1000))
        self.verified = verify_model_metadata(self.binding, dict(object='list', data=[
            dict(id='fixed-fixture-model', object='model', owned_by='vllm')]))
        self.environment = FixedEnvironment('a'*64, 'b'*64, 'c'*64)
        self.backend_profile = BackendRunProfile('d'*64)

    def fixed(self, **kwargs):
        return select_fixed_movie(self.dataset, self.pz_root, self.binding, self.verified,
            environment=kwargs.pop('environment', self.environment),
            profile=kwargs.pop('profile', FixedMovieProfile(NativeOptions(2, 2))), **kwargs)

    def test_daft_registers_before_lazy_calls_and_reuses_shared_runtime_for_payloads(self):
        from src.baselines.text.frameworks import native_calls
        from src.data.materializers.payloads import PayloadBatchLimits,iter_payload_batches
        from src.execution_provider.adapters.full_response import FullModelResponse
        from src.infrastructure import daft_runtime
        from src.semantic_methods.palimpzest.query_execution import QueryExecutionLimits
        # Actual PZ/SDK/shared helper/Arrow; only the vendor graph and HTTP result
        # are software fixtures. This is not actual Daft or model qualification.
        fake,events,state = ModuleType('daft'),[],dict(runner=None)
        def get_runner():
            events.append(('get_runner',daft_runtime._configured))
            if state['runner'] is None:
                state['runner'] = SimpleNamespace(name='native')
            return state['runner']
        def set_native(num_threads=None):
            events.append(('set_native',num_threads))
            if state['runner'] is not None:
                raise RuntimeError('fixture: cannot set runner more than once')
            state['runner'] = SimpleNamespace(name='native')
        class Frame:
            def __init__(self,table):
                self.table = table
            def into_batches(self,rows):
                self.rows = rows
                return self
            def to_arrow_iter(self,**_kwargs):
                yield from self.table.to_batches(max_chunksize=self.rows)
        def from_arrow(table):
            events.append(('payload_graph',get_runner().name))
            return Frame(table)
        fake.set_runner_native,fake.from_arrow = set_native,from_arrow
        def complete(_executor,calls,**_kwargs):
            events.append(('native_graph',get_runner().name))
            for call in calls:
                body = dict(id='fixture',object='chat.completion',created=1,model=self.binding.model.model_id,
                    choices=[dict(index=0,finish_reason='stop',message=dict(role='assistant',content='ANSWER: TRUE\n---'))],
                    usage=dict(prompt_tokens=11,completion_tokens=3,total_tokens=14))
                yield native_calls.NativeCallOutcome(call.call_id,call.row_id,
                    FullModelResponse(200,(('content-type','application/json'),),json.dumps(body).encode()))
        fixed = self.fixed()
        executor = native_calls.NativeCallExecutor('daft')
        try:
            with daft_module_fixture(fake), patch.object(daft_runtime,'_configured',None), \
                    patch.object(native_calls.NativeCallExecutor,'iter_batch',complete), \
                    patch('src.experiments.palimpzest.fixed_runner.fetch_model_metadata',return_value=self.verified), \
                    patch('sys.stdout',StringIO()):
                first = execute_fixed_movie(fixed.fresh('daft-first'),arm='daft',executor=executor,
                    limits=QueryExecutionLimits(2,3,2),backend_profile=self.backend_profile)
                rows = ((0,0,b'first complete call'),(0,1,b'second complete call'))
                tables = list(iter_payload_batches(rows,PayloadBatchLimits(2,1024),batch_rows=1))
                self.assertEqual(tuple((row['session_id'],row['sequence'],row['payload'])
                    for table in tables for row in table.to_pylist()),rows)
                second = execute_fixed_movie(fixed.fresh('daft-repeat'),arm='daft',executor=executor,
                    limits=QueryExecutionLimits(2,3,2),backend_profile=self.backend_profile)
                self.assertEqual(daft_runtime._configured,('native',None))
        finally:
            executor.close()
        self.assertEqual(events[0],('set_native',None))
        self.assertEqual([row for row in events if row[0]=='set_native'],[('set_native',None)])
        self.assertTrue(all(row[1]==('native',None) for row in events if row[0]=='get_runner'))
        self.assertTrue(any(row[0]=='payload_graph' for row in events))
        for result in (first,second):
            self.assertEqual(result.records[0].count,2)
            self.assertEqual(result.evidence['ledger']['completion_completed'],2)
            setup = result.evidence['daft_runner_setup']
            self.assertEqual(setup['requested_runner'],'native')
            self.assertFalse(setup['thread_override_requested'])
            self.assertEqual(setup['status'],'configured_or_reused')
            self.assertGreaterEqual(setup['seconds'],0)
            self.assertGreaterEqual(result.evidence['query_seconds'],setup['seconds'])

    def test_incompatible_shared_daft_configuration_refuses_before_any_model_call(self):
        from palimpzest.query.generators import generators
        from src.baselines.text.frameworks.native_calls import NativeCallExecutor
        from src.infrastructure import daft_runtime
        from src.semantic_methods.palimpzest.query_execution import QueryExecutionLimits
        for configured,message in ((('ray',None),'already configured as ray'),
                                    (None,'foreign native runner already initialized')):
            with self.subTest(shared_configuration=configured):
                fixed,evidence = self.fixed(),{}
                fake,executor = ModuleType('daft'),NativeCallExecutor('daft')
                fake.set_runner_native = Mock(side_effect=RuntimeError('foreign native runner already initialized'))
                try:
                    with daft_module_fixture(fake), patch.object(daft_runtime,'_configured',configured), \
                            patch('src.experiments.palimpzest.fixed_runner.fetch_model_metadata',return_value=self.verified), \
                            patch.object(generators.litellm,'completion') as completion, \
                            patch.object(NativeCallExecutor,'iter_batch') as dispatch:
                        with self.assertRaisesRegex(RuntimeError,message):
                            execute_fixed_movie(fixed.fresh('incompatible-daft'),arm='daft',executor=executor,
                                limits=QueryExecutionLimits(2,3,2),backend_profile=self.backend_profile,evidence=evidence)
                    completion.assert_not_called()
                    dispatch.assert_not_called()
                    if configured is None:
                        fake.set_runner_native.assert_called_once_with()
                    else:
                        fake.set_runner_native.assert_not_called()
                    self.assertEqual(evidence['ledger']['completion_submitted'],0)
                    self.assertEqual(evidence['daft_runner_setup']['status'],'failed')
                    self.assertGreaterEqual(evidence['daft_runner_setup']['seconds'],0)
                    with self.assertRaisesRegex(PalimpzestAdapterError,'unhealthy'):
                        fixed.fresh('after-runtime-failure')
                finally:
                    executor.close()

    def test_daft_setup_waits_for_matching_fixed_model_metadata(self):
        from palimpzest.query.generators import generators
        from src.baselines.text.frameworks.native_calls import NativeCallExecutor
        from src.semantic_methods.palimpzest.query_execution import QueryExecutionLimits
        fixed,evidence = self.fixed(),{}
        executor = NativeCallExecutor('daft')
        changed = replace(self.verified,stable_metadata_sha256='f'*64)
        try:
            with patch('src.experiments.palimpzest.fixed_runner.fetch_model_metadata',return_value=changed), \
                    patch('src.experiments.palimpzest.fixed_runner._prepare_daft_runtime') as setup, \
                    patch.object(generators.litellm,'completion') as completion, \
                    patch.object(NativeCallExecutor,'iter_batch') as dispatch:
                with self.assertRaisesRegex(PalimpzestAdapterError,'metadata changed'):
                    execute_fixed_movie(fixed.fresh('daft-model-change'),arm='daft',executor=executor,
                        limits=QueryExecutionLimits(2,3,2),backend_profile=self.backend_profile,evidence=evidence)
            setup.assert_not_called()
            dispatch.assert_not_called()
            completion.assert_not_called()
            self.assertNotIn('daft_runner_setup',evidence)
            self.assertEqual(evidence['ledger']['completion_submitted'],0)
        finally:
            executor.close()

    def test_selects_once_and_fresh_copies_keep_complete_plan_without_reoptimizing(self):
        from palimpzest.query.optimizer.optimizer import Optimizer
        from palimpzest.query.generators import generators
        original = Optimizer.optimize
        with patch.object(Optimizer, 'optimize', autospec=True, side_effect=original) as optimize, \
                patch.object(generators.litellm, 'completion', side_effect=AssertionError('model call')) as completion:
            fixed = self.fixed()
            first, second = fixed.fresh('first'), fixed.fresh('second')
            first.begin()
            second.begin()
        self.assertEqual(optimize.call_count, 1)
        completion.assert_not_called()
        self.assertEqual(first.frozen.fingerprint, second.frozen.fingerprint)
        self.assertIsNot(first.native_strategy, second.native_strategy)
        for before, after in zip(first.frozen.plan, second.frozen.plan, strict=True):
            self.assertIsNot(before, after)
            for name in ('datasource', 'generator'):
                if hasattr(before, name):
                    self.assertIsNot(getattr(before, name), getattr(after, name))
        manifest = fixed.describe()['manifest']
        self.assertEqual(manifest['effective_optimizer_flags'], dict(allow_bonded_query=True,
            allow_rag_reduction=False, allow_mixtures=False, allow_critic=False, allow_split_merge=False))
        self.assertEqual(manifest['optimizer_models'], ['hosted_vllm/qwen/Qwen1.5-0.5B-Chat'])
        self.assertEqual(manifest['sampling_validation'], dict(mode='none', model_calls=0, seconds=0.0))
        with self.assertRaisesRegex(PalimpzestAdapterError, 'cannot execute more than once'):
            first.begin()
        with self.assertRaisesRegex(PalimpzestAdapterError, 'run id cannot be reused'):
            fixed.fresh('first')

    def test_environment_and_config_changes_change_complete_identity(self):
        first = self.fixed()
        cache = self.fixed(environment=replace(self.environment, service_cache_sha256='d'*64))
        workers = self.fixed(profile=FixedMovieProfile(NativeOptions(1, 1)))
        self.assertEqual(first._template.plan_id, workers._template.plan_id)
        self.assertNotEqual(first.identity, cache.identity)
        self.assertNotEqual(first.identity, workers.identity)

    def test_fresh_source_generator_or_strategy_mutation_is_fatal_before_metadata_or_completion(self):
        for changed in ('source', 'generator', 'strategy'):
            with self.subTest(changed=changed):
                fixed = self.fixed()
                fresh = fixed.fresh(changed)
                if changed == 'source':
                    next(op.datasource for op in fresh.frozen.plan if hasattr(op, 'datasource')).vals.iloc[0, 2] = 'changed'
                elif changed == 'generator':
                    next(op.generator for op in fresh.frozen.plan if hasattr(op, 'generator')).model_name = 'unknown'
                else:
                    fresh.native_strategy.max_workers += 1
                with patch('src.experiments.palimpzest.fixed_runner.fetch_model_metadata') as metadata:
                    with self.assertRaises(PalimpzestAdapterError):
                        execute_fixed_movie(fresh, arm='original-native', backend_profile=self.backend_profile)
                metadata.assert_not_called()
                with self.assertRaisesRegex(PalimpzestAdapterError, 'unhealthy'):
                    fixed.fresh('after-fatal-'+changed)

    def test_metadata_failure_is_fatal_and_retains_zero_submission_accounting(self):
        fixed, evidence = self.fixed(), {}
        with patch('src.experiments.palimpzest.fixed_runner.fetch_model_metadata',
                   side_effect=TimeoutError('fixture metadata unavailable')):
            with self.assertRaises(TimeoutError):
                execute_fixed_movie(fixed.fresh('metadata-failure'), arm='original-native',
                                    backend_profile=self.backend_profile, evidence=evidence)
        self.assertEqual(evidence['status'], 'failed')
        self.assertEqual(evidence['ledger']['completion_submitted'], 0)
        with self.assertRaisesRegex(PalimpzestAdapterError, 'unhealthy'):
            fixed.fresh('after-metadata-failure')

    def test_initial_full_metadata_anchor_still_cannot_be_rebound_to_the_same_stable_identity(self):
        for precreated in (False,True):
            with self.subTest(precreated=precreated):
                fixed = self.fixed()
                fresh = fixed.fresh('before-raw-change') if precreated else None
                altered = replace(fixed.verified,metadata_sha256='f'*64)
                self.assertEqual(altered.stable_identity(),fixed.verified.stable_identity())
                self.assertNotEqual(altered,fixed.verified)
                fixed.verified = altered
                with patch('src.experiments.palimpzest.fixed_runner.fetch_model_metadata') as fetch:
                    with self.assertRaisesRegex(PalimpzestAdapterError,'fixed local model binding'):
                        if fresh is None:
                            fixed.fresh('after-raw-change')
                        else:
                            execute_fixed_movie(fresh,arm='original-native',backend_profile=self.backend_profile)
                fetch.assert_not_called()

    def test_repeat_get_allows_only_response_generated_values_and_keeps_each_raw_body(self):
        import httpx
        metadata = dict(object='list',data=[dict(id=self.binding.model.model_id,object='model',owned_by='vllm',
            created=100,root='/fixture/checkpoint',parent=None,max_model_len=8192,
            permission=[dict(id='modelperm-first',created=100,object='model_permission',allow_sampling=True)])])
        changed = deepcopy(metadata)
        changed['data'][0]['created'] += 1
        changed['data'][0]['permission'][0].update(id='modelperm-next',created=101)
        payloads = [json.dumps(value).encode() for value in (metadata,changed)]
        remaining,requests,completion_requests = iter(payloads),[],[]
        def send(_client,request,**kwargs):
            requests.append((request.method,str(request.url)))
            if request.method=='GET' and str(request.url)==self.binding.api_base+'/models':
                body = next(remaining)
            elif request.method=='POST' and str(request.url)==self.binding.model.endpoint_url:
                completion_requests.append(json.loads(request.content))
                body = json.dumps(dict(id='fixture',object='chat.completion',created=1,model=self.binding.model.model_id,
                    choices=[dict(index=0,finish_reason='stop',message=dict(role='assistant',content='ANSWER: TRUE\n---'))],
                    usage=dict(prompt_tokens=11,completion_tokens=3,total_tokens=14))).encode()
            else:
                raise AssertionError('undeclared fixture request')
            response = httpx.Response(200,stream=httpx.ByteStream(body),request=request,headers={'content-type':'application/json'})
            if not kwargs.get('stream'):
                response.read()
            return response
        initial = {}
        with patch.object(httpx.Client,'send',send), patch('sys.stdout',StringIO()):
            self.verified = fetch_model_metadata(self.binding,evidence=initial)
            fixed = self.fixed()
            result = execute_fixed_movie(fixed.fresh('response-only-change'),arm='original-native',backend_profile=self.backend_profile)
        repeated = result.evidence['metadata_observation']
        self.assertEqual(requests.count(('GET',self.binding.api_base+'/models')),2)
        self.assertEqual(requests.count(('POST',self.binding.model.endpoint_url)),2)
        self.assertEqual(len(requests),4)
        self.assertEqual(len(completion_requests),2)
        self.assertEqual(result.records[0].count,2)
        self.assertEqual(result.evidence['ledger']['completion_completed'],2)
        self.assertEqual(result.evidence['ledger']['parser_default_retained'],0)
        self.assertEqual(result.evidence['ledger']['usage_available_calls'],2)
        self.assertTrue(result.evidence['native_bridge']['patches_restored'])
        self.assertTrue(repeated['matches_selected_identity'])
        self.assertNotEqual(initial['metadata_sha256'],repeated['metadata_sha256'])
        self.assertEqual(initial['stable_metadata_sha256'],repeated['stable_metadata_sha256'])
        self.assertEqual(base64.b64decode(initial['raw_body_base64']),payloads[0])
        self.assertEqual(base64.b64decode(repeated['raw_body_base64']),payloads[1])
        self.assertEqual(fixed._selected_verified.metadata_sha256,initial['metadata_sha256'])
        retained = os.environ.get('SEMLOOM_PZ_METADATA_FIXED_OUTPUT')
        if retained:
            root = Path(retained); root.mkdir(parents=True,exist_ok=False)
            write_private_json(root/'initial-metadata.json',initial)
            write_private_json(root/'run.json',dict(result.evidence,calls=result.ledger.calls()))
            write_private_json(root/'qualification.json',dict(status='passed',actual_http_posts=0,model_calls=0,
                fixture_metadata_GETs=2,fixture_completion_POSTs=2,original_sdk=True,original_pz=True,
                count=result.records[0].count,fixture='HTTPX sender fixture; no real model or server'))

    def test_changed_stable_metadata_is_fatal_before_generation_and_retains_original_body(self):
        import httpx
        from palimpzest.query.generators import generators
        fixed,evidence = self.fixed(),{}
        metadata = dict(object='list',data=[dict(id=self.binding.model.model_id,object='model',owned_by='vllm',root='/changed/checkpoint')])
        body = json.dumps(metadata).encode()
        original_init,requests = httpx.Client.__init__,[]
        def handler(request):
            requests.append((request.method,str(request.url)))
            if request.method!='GET' or str(request.url)!=self.binding.api_base+'/models':
                raise AssertionError('identity failure must not submit a model request')
            return httpx.Response(200,stream=httpx.ByteStream(body))
        def initialize(client,*args,**kwargs):
            kwargs['transport'] = httpx.MockTransport(handler)
            original_init(client,*args,**kwargs)
        with patch.object(httpx.Client,'__init__',initialize), patch.object(generators.litellm,'completion') as completion:
            with self.assertRaisesRegex(PalimpzestAdapterError,'metadata changed'):
                execute_fixed_movie(fixed.fresh('stable-change'),arm='original-native',backend_profile=self.backend_profile,evidence=evidence)
        completion.assert_not_called()
        self.assertEqual(evidence['ledger']['completion_submitted'],0)
        self.assertEqual(base64.b64decode(evidence['metadata_observation']['raw_body_base64']),body)
        self.assertEqual(evidence['metadata_observation']['status'],'verified')
        self.assertFalse(evidence['metadata_observation']['matches_selected_identity'])
        self.assertEqual(requests,[('GET',self.binding.api_base+'/models')])
        self.assertGreaterEqual(evidence['metadata_check_seconds'],0)
        with self.assertRaisesRegex(PalimpzestAdapterError,'unhealthy'):
            fixed.fresh('after-stable-change')

    def test_same_base_model_rebinding_cannot_inherit_the_selected_identity_or_send_more_requests(self):
        for precreated in (False, True):
            with self.subTest(precreated=precreated):
                fixed, evidence = self.fixed(), {}
                fresh = fixed.fresh('precreated') if precreated else None
                old_identity = fixed.identity
                replacement = LocalModelBinding(replace(self.binding.model, model_id='another-valid-served-model'))
                fixed.binding = replacement
                fixed.verified = verify_model_metadata(replacement, dict(object='list', data=[
                    dict(id=replacement.model.model_id, object='model', owned_by='vllm')]))
                with patch('src.experiments.palimpzest.fixed_runner.fetch_model_metadata') as metadata:
                    with self.assertRaisesRegex(PalimpzestAdapterError, 'fixed local model binding'):
                        if fresh is None:
                            fixed.fresh('after-rebinding')
                        else:
                            execute_fixed_movie(fresh, arm='original-native',
                                backend_profile=self.backend_profile, evidence=evidence)
                metadata.assert_not_called()
                self.assertEqual(fixed.identity, old_identity)
                if fresh is not None:
                    self.assertEqual(evidence['status'], 'failed')
                    self.assertEqual(evidence['ledger']['completion_submitted'], 0)
                with self.assertRaisesRegex(PalimpzestAdapterError, 'unhealthy'):
                    fixed.fresh('after-fatal-rebinding')

    def test_describe_does_not_expose_mutable_selected_manifest_references(self):
        fixed = self.fixed()
        public = fixed.describe()
        public['manifest']['local_model']['actual_served_model'] = 'another-model'
        public['manifest']['effective_config']['available_models'].append('unknown')
        public['manifest']['source_contents'][0]['values_sha256'] = '0'*64
        fixed.fresh('after-changing-description')
        self.assertEqual(fixed.describe()['manifest']['local_model']['actual_served_model'], self.binding.model.model_id)
        self.assertEqual(len(fixed.describe()['manifest']['effective_config']['available_models']), 1)

    def test_changed_local_adapter_source_invalidates_the_selection_before_any_fresh_execution(self):
        fixed = self.fixed()
        sources = fixed.describe()['manifest']['adapter_source_files']
        for expected in ('experiments/palimpzest/fixed.py', 'experiments/palimpzest/fixed_runner.py',
                         'semantic_methods/palimpzest/query_execution.py', 'semantic_methods/palimpzest/semloom.py',
                         'baselines/text/frameworks/native_calls.py', 'scheduling/core/session.py',
                         'infrastructure/daft_runtime.py'):
            self.assertIn(expected, sources)
        sources['experiments/palimpzest/fixed_runner.py']['sha256'] = '0'*64
        with patch('src.experiments.palimpzest.fixed.adapter_source_identity', return_value=sources):
            with self.assertRaises(PalimpzestAdapterError):
                fixed.fresh('changed-adapter-source')
        with self.assertRaisesRegex(PalimpzestAdapterError, 'unhealthy'):
            fixed.fresh('after-adapter-source-change')

    def test_daft_helper_source_drift_rejects_precreated_run_before_metadata(self):
        fixed = self.fixed()
        fresh = fixed.fresh('before-helper-drift')
        sources = fixed.describe()['manifest']['adapter_source_files']
        sources['infrastructure/daft_runtime.py']['sha256'] = '0'*64
        evidence = {}
        with patch('src.experiments.palimpzest.fixed.adapter_source_identity',return_value=sources), \
                patch('src.experiments.palimpzest.fixed_runner.fetch_model_metadata') as metadata:
            with self.assertRaisesRegex(PalimpzestAdapterError,'source content or dependency'):
                execute_fixed_movie(fresh,arm='original-native',backend_profile=self.backend_profile,evidence=evidence)
        metadata.assert_not_called()
        self.assertEqual(evidence['ledger']['completion_submitted'],0)

    def test_capacity_contract_failure_poisoning_precedes_metadata_or_model_submission(self):
        from src.semantic_methods.palimpzest.query_execution import NativeAdaptedExecutor, QueryExecutionLimits
        fixed, evidence = self.fixed(), {}
        executor = NativeAdaptedExecutor(max_workers=1)
        try:
            with patch('src.experiments.palimpzest.fixed_runner.fetch_model_metadata') as metadata:
                with self.assertRaisesRegex(ValueError, 'original PZ configuration identity'):
                    execute_fixed_movie(fixed.fresh('wrong-capacity'), arm='native-adapted', executor=executor,
                        limits=QueryExecutionLimits(3, 3, 2), backend_profile=self.backend_profile, evidence=evidence)
            metadata.assert_not_called()
            self.assertEqual(evidence['status'], 'failed')
            with self.assertRaisesRegex(PalimpzestAdapterError, 'unhealthy'):
                fixed.fresh('after-capacity-failure')
        finally:
            self.assertTrue(executor.close())

    def test_original_and_native_adapted_share_plan_prompt_and_result_while_empty_count_stays_invalid(self):
        import httpx
        from palimpzest.query.generators import generators
        from src.semantic_methods.palimpzest.query_execution import NativeAdaptedExecutor, QueryExecutionLimits
        fixed = self.fixed()
        sdk_sends, evidence = [], []
        def send(_client, request, **_kwargs):
            sdk_sends.append(json.loads(request.content))
            body = dict(id='fixture', object='chat.completion', created=1, model='fixed-fixture-model',
                choices=[dict(index=0, finish_reason='stop',
                              message=dict(role='assistant', content='ANSWER: TRUE\n---'))],
                usage=dict(prompt_tokens=11, completion_tokens=3, total_tokens=14))
            return httpx.Response(200, headers={'content-type': 'application/json'},
                                  stream=httpx.ByteStream(json.dumps(body).encode()), request=request)
        with patch('src.experiments.palimpzest.fixed_runner.fetch_model_metadata', return_value=self.verified), \
                patch('sys.stdout', StringIO()):
            with patch.object(generators.litellm, 'completion', side_effect=lambda **_kw: native_response('TRUE')):
                original = execute_fixed_movie(fixed.fresh('original-control'), arm='original-native',
                                               backend_profile=self.backend_profile)
            # PZ's recorded workers, adapter-held stacks and backend worker pool
            # are separate deployment choices, not a unified concurrency knob.
            executor = NativeAdaptedExecutor(max_workers=1)
            try:
                with patch.object(httpx.Client, 'send', send):
                    adapted = execute_fixed_movie(fixed.fresh('adapted-control'), arm='native-adapted',
                        executor=executor, limits=QueryExecutionLimits(2, 3, 2), backend_profile=self.backend_profile)
            finally:
                self.assertTrue(executor.close())
            with patch.object(generators.litellm, 'completion', side_effect=lambda **_kw: native_response('FALSE')):
                empty = execute_fixed_movie(fixed.fresh('all-negative'), arm='original-native',
                                           backend_profile=self.backend_profile)
        self.assertEqual(original.records[0].count, adapted.records[0].count)
        self.assertEqual(original.evidence['fixed_identity'], adapted.evidence['fixed_identity'])
        self.assertEqual({tuple(call['source_indices']): call['messages_sha256'] for call in original.ledger.calls()},
                         {tuple(call['source_indices']): call['messages_sha256'] for call in adapted.ledger.calls()})
        self.assertEqual(len(sdk_sends), 2)
        self.assertFalse(adapted.evidence['prepared_execution']['pz_threadpool_created'])
        self.assertFalse(adapted.evidence['backend_qualified'])
        self.assertEqual(empty.records, [])
        self.assertEqual(empty.evidence['status'], 'compatibility_invalid_empty_count')
        self.assertFalse(empty.evidence['relation_compatible'])
        self.assertEqual(empty.evidence['classification_audit']['false_negative'], 2)
        for result in (original, adapted, empty):
            evidence.append(dict(result.evidence, calls=result.ledger.calls()))
        retained = os.environ.get('SEMLOOM_PZ_FIXED_OUTPUT')
        if retained:
            root = Path(retained)
            root.mkdir(parents=True, exist_ok=False)
            write_private_json(root/'fixed-identity.json', fixed.describe())
            write_private_json(root/'runs.json', evidence)
            write_private_json(root/'qualification.json', dict(status='passed',
                fixture_source=file_identity(Path(__file__)),
                scope='actual pinned original PZ and SDK; deterministic completion/HTTPX sender fixtures',
                fixture_sdk_sends=len(sdk_sends), actual_http_posts=0, model_inference_requests=0,
                vendor_backend_qualified=False, performance_qualified=False))

    def test_semloom_uses_the_same_fixed_plan_and_original_sdk_with_query_owned_operator_sessions(self):
        from src.baselines.text.frameworks import native_calls
        from src.execution_provider.adapters.full_response import FullModelResponse
        from src.semantic_methods.palimpzest.query_execution import QueryExecutionLimits
        from src.semantic_methods.palimpzest.semloom import SemLoomProgress
        fixed = self.fixed()
        fresh = fixed.fresh('semloom-control')
        limits = QueryExecutionLimits(2, 3, 2, batch_timeout_s=1, close_timeout_s=1)
        calls, events = [], []
        async def kernel(call, control=None, *, client=None):
            calls.append(call)
            body = dict(id='fixture', object='chat.completion', created=1, model='fixed-fixture-model',
                choices=[dict(index=0, finish_reason='stop',
                              message=dict(role='assistant', content='ANSWER: TRUE\n---'))],
                usage=dict(prompt_tokens=11, completion_tokens=3, total_tokens=14))
            return native_calls.NativeCallOutcome(call.call_id, call.row_id,
                FullModelResponse(200, (('content-type', 'application/json'),), json.dumps(body).encode()))
        progress = SemLoomProgress(self.binding.model, query_id=fresh.run_id,
            operator_ids=fresh.semantic_operator_ids, limits=limits, max_active_requests=8)
        try:
            with patch.object(native_calls, '_request', kernel), \
                    patch('src.experiments.palimpzest.fixed_runner.fetch_model_metadata', return_value=self.verified), \
                    patch('sys.stdout', StringIO()):
                result = execute_fixed_movie(fresh, arm='semloom', backend_profile=self.backend_profile,
                    executor=progress, limits=limits, record=events.append)
            self.assertEqual(result.records[0].count, 2)
            self.assertEqual(len(calls), 2)
            self.assertEqual(result.evidence['ledger']['completion_completed'], 2)
            self.assertEqual(result.evidence['prepared_execution']['preparation_memory_slots'], 3)
            self.assertEqual(result.evidence['arm']['declared_request_capacity'], 8)
            self.assertFalse(result.evidence['arm']['ray_backend'])
            self.assertEqual(progress.snapshot()['registered_jobs'], 0)
            self.assertTrue(progress.last_close_report['method_complete'])
            self.assertTrue(progress.last_close_report['http_pool_closed'])
            self.assertFalse(progress.last_close_report['unconfirmed_work'])
            identities = [event for event in events if event['event'] == 'pz_generation_identity']
            self.assertEqual({item['identity']['physical_operator'] for item in identities},
                             set(fresh.semantic_operator_ids))
            retained = os.environ.get('SEMLOOM_PZ_FIXED_OUTPUT')
            if retained:
                root = Path(retained+'-semloom')
                root.mkdir(parents=True, exist_ok=False)
                write_private_json(root/'evidence.json', dict(result.evidence, events=events,
                    calls=result.ledger.calls(), semloom=progress.snapshot(), close=progress.last_close_report))
                write_private_json(root/'qualification.json', dict(status='passed',
                    fixture_source=file_identity(Path(__file__)),
                    scope='actual pinned PZ/SDK and real local Core/IO; original HTTP kernel response fixture',
                    fixture_kernel_calls=len(calls), actual_http_posts=0, model_inference_requests=0,
                    physical_backend='local complete NativeCall transport', ray_backend=False,
                    vendor_backend_qualified=False, performance_qualified=False))
        finally:
            progress.close(timeout_s=1)


if __name__ == '__main__':
    unittest.main()
