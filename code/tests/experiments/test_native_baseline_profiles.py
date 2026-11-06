"""Existing Daft native options reach Movie callers without starting frameworks."""

import builtins
from copy import deepcopy
from dataclasses import asdict, replace
import json
from pathlib import Path
import socket
from unittest.mock import Mock, patch
import unittest

from integrations.imlane_records import chain_runtime
from src.baselines.text.frameworks import daft_lotus_records, daft_lotus_cascade
from src.baselines.text.movie_records import MovieRecordSource
from src.baselines.text.movie_tasks import MovieTask
from src.experiments import lotus_records_movie as movie
from src.experiments import movie_records_session as session
from src.experiments.movie_records_config import daft_native_profile
from src.serving.backends.vllm_engine import VllmEngine
from src.serving.backends.vllm_async_engine import VllmAsyncEngine
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
from tests.experiments import test_movie_records_session as session_fixtures


class NativeBaselineProfileTests(unittest.TestCase):
    def setUp(self):
        original_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name.split('.')[0] in ('ray', 'daft', 'vllm'):
                self.fail('profile wiring attempted a real framework import')
            return original_import(name, *args, **kwargs)

        for scope in (patch.object(builtins, '__import__', guarded_import),
                patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')),
                patch.object(socket.socket, 'connect_ex', side_effect=AssertionError('network forbidden')),
                patch.object(VllmEngine, '__init__', side_effect=AssertionError('real engine forbidden')),
                patch.object(VllmAsyncEngine, '__init__', side_effect=AssertionError('real engine forbidden'))):
            scope.start()
            self.addCleanup(scope.stop)
        self.fixture = session_fixtures.PlanTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = Path(self.fixture.manifests['movie-q1']).parent.parent
        self.engine = EngineSpec.from_dict(self.fixture.plan['engine'])
        self.method = MovieTask(1).method(self.fixture.plan['model_name'])
        self.path = self.root / 'native-config.json'

    def read(self, config, *, cascade=False):
        self.path.write_text(json.dumps(config))
        return (movie.read_cascade_config if cascade else movie.read_config)(self.path)

    def config(self):
        return dict(schema='semloom.movie_records_run.v1', backend='daft-native-local-model',
            query_id='ordinary-native', task='movie-q1', method=asdict(self.method),
            generation=self.fixture.plan['generation'], engine=self.fixture.plan['engine'],
            resources=self.fixture.plan['resources'])

    def cascade_config(self):
        return dict(schema='semloom.movie_records_cascade_run.v1', backend='daft-native-local-model-cascade',
            query_id='native-cascade', method=asdict(MovieTask(3).method(self.fixture.plan['model_name'])),
            helper_model_name='openai/cpu-helper', cascade_args={}, max_calls=4,
            generations=dict(main=asdict(GenerationSpec()), helper=asdict(GenerationSpec(logprobs=5))),
            engines=dict(main=asdict(self.engine), helper=asdict(replace(self.engine, model_path='/tmp/helper'))),
            resources=self.fixture.plan['resources'])

    def test_profile_preserves_omission_none_and_explicit_threads_without_mutation(self):
        for profile in ({}, {'runner_threads': None}, {'runner_threads': 2}):
            with self.subTest(profile=profile):
                original = deepcopy(profile)
                self.assertEqual(daft_native_profile(profile), profile)
                self.assertEqual(profile, original)
                config = dict(self.config(), profile=profile)
                self.assertEqual(self.read(config)[0], json.loads(json.dumps(config)))
        self.assertNotIn('profile', self.read(self.config())[0])

    def test_invalid_or_unsupported_profile_fails_before_a_backend_is_created(self):
        for profile in (None, [], {'runner_threads': True}, {'runner_threads': 0},
                        {'runner_threads': -1}, {'runner_threads': 2.0}, {'model_workers': 2}):
            with self.subTest(profile=profile), self.assertRaises(ValueError):
                self.read(dict(self.config(), profile=profile))
        for backend in ('ray-data-local-model', 'semloom-local-model-core'):
            with self.subTest(backend=backend), self.assertRaisesRegex(ValueError, 'Daft native backend'):
                self.read(dict(self.config(), backend=backend, profile={}))

    def test_ordinary_movie_worker_forwards_only_existing_daft_options(self):
        source = MovieRecordSource.from_manifest(self.fixture.manifests['movie-q1'])

        def collect(source, checkout, method, generation, execute, *, query_id, observer):
            execute()
            return dict(status='completed')

        for index, profile in enumerate(({}, {'runner_threads': None}, {'runner_threads': 2})):
            with self.subTest(profile=profile):
                config, method, generation, resources, engine = self.read(dict(self.config(), profile=profile))
                output = self.root / ('ordinary-' + str(index))
                output.mkdir()
                with patch.object(movie, 'run_movie_query', side_effect=collect), \
                        patch.object(daft_lotus_records, 'evaluate') as evaluate:
                    movie._model_worker(config, source, 'CPU-checkout', method, generation, resources, engine, output)
                evaluate.assert_called_once_with(source, method, engine, generation, resources,
                    query_id=config['query_id'], **profile)

    def test_isolated_daft_session_forwards_profile_and_preserves_original_query_error(self):
        for index, profile in enumerate(({}, {'runner_threads': None}, {'runner_threads': 2})):
            with self.subTest(profile=profile):
                plan = dict(self.fixture.plan, backend='daft', lifecycle='isolated', profile=profile)
                checked = session.check_plan(plan)
                context = dict(owner_context={}, claims={q['query_id']: object() for q in checked['queries']},
                    checked_queries={q['query_id']: q for q in checked['queries']}, sembench_checkout='CPU-checkout')
                directory = self.root / ('isolated-' + str(index))
                driver = session._MovieDriver(plan, context, directory, plan['queries'][0])
                self.assertTrue(driver.start()['no_session_model'])
                failure = RuntimeError('original native query failure')

                def collect(source, checkout, method, generation, execute, *, query_id, observer):
                    return execute()

                with patch.object(chain_runtime, 'check_external_lease', return_value={}), \
                        patch.object(movie, 'run_movie_query', side_effect=collect), \
                        patch.object(daft_lotus_records, 'evaluate', side_effect=failure) as evaluate:
                    with self.assertRaises(RuntimeError) as caught:
                        driver.query(plan['queries'][0], Mock())
                self.assertIs(caught.exception, failure)
                self.assertEqual(evaluate.call_args.kwargs, dict(query_id='q1', **profile))
                self.assertEqual(driver.close()['status'], 'closed')

    def test_named_daft_session_and_ray_runtime_reuse_remain_separate(self):
        plan = dict(self.fixture.plan, backend='daft', lifecycle='native_single_thread_session',
                    profile=dict(session.DAFT_SESSION_PROFILE))
        self.assertTrue(session.check_plan(plan)['native_single_thread_session'])
        for profile in ({'runner_threads': 2}, {}):
            with self.subTest(profile=profile), self.assertRaisesRegex(ValueError, 'explicit different'):
                session.check_plan(dict(plan, profile=profile))
        ray = dict(self.fixture.plan, backend='ray-data', lifecycle='runtime_reuse_only', profile={})
        self.assertTrue(session.check_plan(ray)['supports_runtime_reuse_only'])
        for profile in ({'runner_threads': 2}, {'max_concurrency': 2}, [], None):
            with self.subTest(profile=profile), self.assertRaisesRegex(ValueError, 'empty profile'):
                session.check_plan(dict(ray, profile=profile))

    def test_cascade_parser_and_factory_use_the_same_existing_daft_thread_option(self):
        for profile in ({}, {'runner_threads': None}, {'runner_threads': 2}):
            with self.subTest(profile=profile):
                config, _, _, resources, engines, _ = self.read(
                    dict(self.cascade_config(), profile=profile), cascade=True)
                source = object()
                with patch.object(daft_lotus_cascade, 'DaftCascadeBackend') as backend:
                    self.assertIs(movie._cascade_backend(config, source, engines, resources), backend.return_value)
                backend.assert_called_once_with(source, engines=engines, resources=resources, **profile)
        with self.assertRaisesRegex(ValueError, 'Daft native cascade'):
            self.read(dict(self.cascade_config(), backend='ray-data-local-model-cascade', profile={}), cascade=True)
        with self.assertRaisesRegex(ValueError, 'positive integer'):
            self.read(dict(self.cascade_config(), profile={'runner_threads': True}), cascade=True)


if __name__ == '__main__':
    unittest.main()
