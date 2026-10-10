"""Controlled LOTUS preparation/cleanup with real Core; no SDK, HTTP or model."""

from contextlib import contextmanager
import json
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task
from src.scheduling.core.session_contract import Usage
from src.semantic_methods.budget import MethodBudget, MethodCapacity, row_reservation
from src.semantic_methods.continuation import Continue, MethodLimits, Request
from src.semantic_methods.driver import MethodDriver
from src.semantic_methods.lotus.batch import LotusBatchExecutor
from src.semantic_methods.lotus.driver import iter_two_map_rows
from src.semantic_methods.lotus.sdk import PreparedLotusCall, check_model_config
from tests.execution_provider.test_native_tasks import fixture


class LotusControlTests(unittest.TestCase):
    @contextmanager
    def batch(self, *, stop_phase=None, expired=False, initially_cancelled=False,
              prevalidate=False, retain=True):
        execution, backend, _ = fixture()
        state = dict(now=0., cancelled=initially_cancelled, events=[], actual=[])
        config = FixedModelConfig('http://localhost/v1/chat/completions', 'fixture', 1000, 'fixture')

        def preparing(phase):
            state['events'].append(phase)
            if phase == stop_phase:
                state['now'] = .02 if expired else 0.
                state['cancelled'] = not expired

        def prepare(lm, messages, kwargs):
            preparing('prepare')
            return PreparedLotusCall(json.dumps(dict(model='fixture', messages=messages)).encode(),
                                     'http://localhost/v1', 'fixture', 1.)

        def check(call, model):
            preparing('model')
            check_model_config(call, model)

        def native(*args, **kwargs):
            preparing('native')
            return prepare_native_task(*args, **kwargs)

        class Flow(NativeTaskSession):
            def offer(self, tasks):
                preparing('offer')
                return super().offer(tasks)

            def advance(self, maximum):
                state['events'].append('advance')
                return super().advance(maximum)

            def wait(self, progress, timeout_s=None):
                for key in tuple(backend.pending):
                    state['actual'].append(backend.pending[key][1].task.payload)
                    backend.complete(key, encode_full_response(FullModelResponse(200, (), b'{}')))

            def close(self, *, clean=False):
                state['events'].append('close')
                return super().close(clean=clean)

        sdk = ModuleType('openai')
        sdk.OpenAIError = type('OpenAIError', (Exception,), {})
        selected = LotusBatchExecutor(execution, config, query_id='query', operator_id='map',
            cancelled=lambda: state['cancelled'], timeout_s=.01,
            prevalidate_batch=prevalidate,retain_full_responses=retain)
        with patch.dict('sys.modules', {'openai': sdk}), \
             patch('src.semantic_methods.lotus.batch.NativeTaskSession', Flow), \
             patch('src.semantic_methods.lotus.batch.prepare_call', prepare), \
             patch('src.semantic_methods.lotus.sdk.prepare_call', prepare), \
             patch('src.semantic_methods.lotus.batch.check_model_config', check), \
             patch('src.semantic_methods.lotus.batch.prepare_native_task', native), \
             patch('src.semantic_methods.lotus.batch.lotus_response', return_value=SimpleNamespace()), \
             patch('src.semantic_methods.lotus.batch.time', SimpleNamespace(monotonic=lambda: state['now'])):
            yield selected, execution, state

    def run_batch(self, selected):
        return selected(None, [([dict(role='user', content=str(i))], None) for i in range(2)], {}, False, 'fixture')

    def test_preparation_stop_prevents_offer_and_dispatch(self):
        for phase in ('prepare', 'model', 'native'):
            for expired in (False, True):
                with self.subTest(phase=phase, expired=expired), self.batch(stop_phase=phase, expired=expired) as (selected, execution, state):
                    with self.assertRaises(TimeoutError if expired else RuntimeError):
                        self.run_batch(selected)
                    self.assertNotIn('offer', state['events'])
                    self.assertNotIn('advance', state['events'])
                    self.assertEqual(state['events'].count('prepare'), 1)
                    self.assertEqual(execution.engine.capacity.usage(), Usage())
                    self.assertEqual(execution.engine.jobs.jobs, {})

    def test_cancel_during_offer_prevents_dispatch(self):
        with self.batch(stop_phase='offer') as (selected, execution, state):
            with self.assertRaisesRegex(RuntimeError, 'cancelled'):
                self.run_batch(selected)
            self.assertNotIn('advance', state['events'])
            self.assertEqual(execution.engine.capacity.usage(), Usage())

    def test_normal_batch_and_initial_cancellation(self):
        with self.batch() as (selected, execution, state):
            self.assertEqual(len(self.run_batch(selected)), 2)
            self.assertIn('advance', state['events'])
            self.assertEqual(execution.engine.capacity.usage(), Usage())
        with self.batch(initially_cancelled=True) as (selected, execution, state):
            with self.assertRaisesRegex(RuntimeError, 'cancelled'):
                self.run_batch(selected)
            self.assertEqual(state['events'], [])
            self.assertEqual(execution.engine.jobs.jobs, {})

    def test_prevalidation_is_timed_and_cancelled_before_flow_creation(self):
        for expired in (False, True):
            with self.subTest(expired=expired), self.batch(stop_phase='prepare', expired=expired, prevalidate=True) as (selected, execution, state):
                with self.assertRaises(TimeoutError if expired else RuntimeError):
                    self.run_batch(selected)
                self.assertEqual(state['events'], ['prepare'])
                self.assertEqual(execution.engine.jobs.jobs, {})
                self.assertEqual(state['actual'], [])

    def test_prepared_values_are_shared_without_full_response_retention(self):
        prepared, received = [], []
        with self.batch(prevalidate=True, retain=False) as (selected, execution, state):
            selected.on_prepared=lambda index, call: prepared.append((index, call))
            selected.on_response=lambda index, full: received.append((index, full))
            self.assertEqual(len(self.run_batch(selected)), 2)
            self.assertEqual(state['events'].count('prepare'), 2)
            self.assertEqual(state['actual'], [call.payload for _,call in prepared])
            self.assertEqual([index for index,_ in received], [0,1])
            self.assertEqual(selected.last_responses, ())
            self.assertEqual(execution.engine.capacity.usage(), Usage())


class LotusChainCleanupTests(unittest.TestCase):
    def run_chain(self, execution, rows, method=None):
        limits = MethodLimits(128, 128, 256, 2)
        return list(iter_two_map_rows(execution, method, rows, query_id='query', operator_id='chain',
            limits=limits, capacity=MethodCapacity(2, 2 * row_reservation(limits))))

    def failed_source(self, error):
        raise error
        yield  # Exercise failure at next(source), after driver construction.

    def test_source_error_survives_both_cleanup_errors_and_job_close_is_attempted(self):
        execution, _, _ = fixture()
        primary, consumer, job = ValueError('source failed'), OSError('consumer failed'), RuntimeError('job failed')
        close_driver, close_job = MethodDriver.close, execution.engine.close_job
        def close(driver):
            close_driver(driver)
            raise consumer
        def close_owned_job(handle):
            close_job(handle)
            raise job
        with patch.object(MethodDriver, 'close', close), patch.object(execution.engine, 'close_job', side_effect=close_owned_job) as closing:
            with self.assertRaises(ValueError) as caught:
                self.run_chain(execution, self.failed_source(primary))
        self.assertIs(caught.exception, primary)
        self.assertEqual(closing.call_count, 1)
        self.assertEqual(primary.lotus_cleanup_errors, (('driver', consumer), ('job', job)))
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertEqual(execution.engine.jobs.jobs, {})

    def test_cleanup_only_failure_propagates_after_attempting_job_close(self):
        execution, _, _ = fixture()
        consumer = OSError('consumer failed')
        original = MethodDriver.close
        def close(driver):
            original(driver)
            raise consumer
        with patch.object(MethodDriver, 'close', close), patch.object(execution.engine, 'close_job', wraps=execution.engine.close_job) as closing:
            with self.assertRaises(OSError) as caught:
                self.run_chain(execution, ())
        self.assertIs(caught.exception, consumer)
        self.assertEqual(closing.call_count, 1)
        self.assertEqual(consumer.lotus_cleanup_errors, (('driver', consumer),))

    def test_construction_error_survives_grant_and_session_cleanup_errors(self):
        execution, _, _ = fixture()
        primary, grant_error, session_error = ValueError('construction failed'), OSError('grant failed'), RuntimeError('session failed')
        original_open, original_close = type(execution).open_job, MethodBudget.close
        def open_job(instance, *args, **kwargs):
            job, session, budget = original_open(instance, *args, **kwargs)
            close = session.close_consumer
            def failed_close():
                close()
                raise session_error
            session.close_consumer = failed_close
            return job, session, budget
        def close_grant(grant):
            original_close(grant)
            raise grant_error
        with patch('src.semantic_methods.lotus.driver.MethodDriver', side_effect=primary), \
             patch.object(MethodBudget, 'close', close_grant), \
             patch.object(type(execution), 'open_job', open_job), \
             patch.object(execution.engine, 'close_job', wraps=execution.engine.close_job) as closing:
            with self.assertRaises(ValueError) as caught:
                self.run_chain(execution, ())
        self.assertIs(caught.exception, primary)
        self.assertEqual(closing.call_count, 1)
        self.assertEqual(primary.lotus_cleanup_errors, (('grant', grant_error), ('session', session_error)))
        self.assertEqual(execution.engine.jobs.jobs, {})

    def test_failed_consumer_keeps_unconfirmed_remote_work_until_terminal(self):
        execution, backend, _ = fixture()
        primary, closing = ValueError('source failed'), OSError('consumer failed')
        class Method:
            def start(self, value):
                return Continue(Request('lotus-map', b'{}', 1, 256), b'state')
        def rows():
            yield b'first'
            yield b'second'
            raise primary
        original = MethodDriver.close
        def close(driver):
            original(driver)
            raise closing
        with patch.object(MethodDriver, 'close', close):
            with self.assertRaises(ValueError) as caught:
                self.run_chain(execution, rows(), Method())
        self.assertIs(caught.exception, primary)
        self.assertEqual(execution.engine.capacity.usage().active_requests, 1)
        self.assertEqual(len(backend.routes), 1)
        for key in tuple(backend.pending):
            backend.complete(key)
        execution.engine.advance()
        self.assertEqual(execution.engine.capacity.usage(), Usage())
        self.assertEqual(execution.engine.jobs.jobs, {})


if __name__ == '__main__':
    unittest.main()
