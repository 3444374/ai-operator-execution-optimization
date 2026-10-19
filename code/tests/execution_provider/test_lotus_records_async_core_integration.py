"""Deterministic model boundary for opt-in production Core/Async integration.

The actual Core, bounded async transport, Ray/Daft and LOTUS preparation compose
with the production threaded actor and three-channel bridge. Only the physical
model engine and interpreter qualification are CPU substitutes. File gates are
fixture controls; they implement no scheduling or production capacity policy.
"""
from __future__ import annotations

import asyncio
import csv
from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / 'src').is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.serving.backends.async_engine_process import AsyncEngineProcess, serve
from src.serving.backends.vllm_engine import _prepared, result_record


def _fixture_json(path, value):
    raw = (json.dumps(value, ensure_ascii=False, allow_nan=False) + '\n').encode()
    temporary = Path(path).with_name('.fixture-' + str(os.getpid()) + '-' + str(time.time_ns()) + '.tmp')
    try:
        with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)  # Atomic publication; existing event names are errors.
    finally:
        temporary.unlink(missing_ok=True)


def _fixture_key(call_id):
    return hashlib.sha256(call_id.encode()).hexdigest()


class FixtureControl:
    """Bounded test coordinator; only the owning test thread mutates Core."""

    def __init__(self, root):
        self.root = Path(root)

    def configure(self, *, unknown_rows=(), close_error=False):
        _fixture_json(self.root / 'async-fixture-plan.json', dict(
            unknown_rows=list(unknown_rows), close_error=close_error))

    def entered(self):
        result = []
        for path in self.root.glob('entered-*.json'):
            if not path.is_file() or path.is_symlink() or path.stat().st_size > 16384:
                raise ValueError('invalid CPU fixture event')
            result.append(json.loads(path.read_bytes()))
        return sorted(result, key=lambda row: row['sequence'])

    def wait_entered(self, count, *, timeout=5):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            result = self.entered()
            if len(result) >= count:
                return result
            time.sleep(0.002)
        raise TimeoutError('controlled CPU requests did not enter within the fixture deadline')

    def release(self, call_id):
        # Exclusive release catches a coordinator that accidentally replays work.
        with os.fdopen(os.open(self.root / ('release-' + _fixture_key(call_id)),
                              os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb'):
            pass

    def finished(self, call_id):
        return (self.root / ('finished-' + _fixture_key(call_id) + '.json')).is_file()


class ControlledProcess(AsyncEngineProcess):
    """Actual production IPC/child supervision, with a fixed CPU engine child."""

    def __init__(self, *args, **kwargs):
        from src.serving.backends.vllm_engine import EngineSpec

        with patch.object(EngineSpec, 'validate_driver_environment', return_value={'scope': 'CPU fixture'}):
            super().__init__(*args, **kwargs)

    @staticmethod
    def _command(spec, descriptors):
        return [sys.executable, '-B', str(Path(__file__).resolve()),
                '--controlled-cpu-serve', *map(str, descriptors)]


class ControlledEngine:
    """Event-gated fixed responses; counters refer to fixture evaluations only."""

    def __init__(self, spec, generation, *, limits):
        self.root = Path(spec.model_path)
        plan = self.root / 'async-fixture-plan.json'
        if not plan.is_file() or plan.is_symlink() or plan.stat().st_size > 16384:
            raise ValueError('explicit CPU fixture plan required')
        self.plan = json.loads(plan.read_bytes())
        self.uid = 'controlled-cpu-async-' + str(os.getpid())
        self.engine_uid = self.uid
        self.calls = set()
        self.maximum = spec.max_model_calls
        self.closed = False

    def ready(self):
        if 'vllm' in sys.modules:
            raise AssertionError('CPU fixture must not import vLLM')
        return dict(engine_uid=self.uid, physical_gpu=False, physical_gpu_work=False,
                    actual_model_calls=0, inference_kind='event-gated CPU fixture; synthetic token usage')

    async def generate_prepared(self, row, *, request_id, generation, deadline_monotonic):
        identity = _prepared([row])[0][0]
        if identity['call_id'] in self.calls or len(self.calls) >= self.maximum:
            raise AssertionError('CPU fixture observed a duplicate or excess evaluation')
        sequence = len(self.calls)
        self.calls.add(identity['call_id'])
        key = _fixture_key(identity['call_id'])
        start = time.monotonic()
        _fixture_json(self.root / ('entered-' + key + '.json'), dict(identity, sequence=sequence,
            request_id=request_id, engine_uid=self.uid, entered_monotonic=start))
        while not (self.root / ('release-' + key)).exists():
            if time.monotonic() >= deadline_monotonic:
                raise TimeoutError('controlled request exceeded its actual deadline')
            await asyncio.sleep(0.002)
        unknown = identity['source_position'] in self.plan['unknown_rows']
        result = result_record(identity, engine_uid=self.uid, start=start, end=time.monotonic(),
            status='unknown' if unknown else 'succeeded', submitted=True)
        result.update(text=None if unknown else ('Answer: True' if identity['source_position'] == 0 else 'Answer: False'),
            prompt_tokens=11, completion_tokens=None if unknown else 1, total_tokens=None if unknown else 12,
            prompt_token_ids_json=json.dumps(list(range(11))), output_token_ids_json='null' if unknown else '[19]',
            logprobs_json='null', finish_reason='abort' if unknown else 'stop', engine_request_id=request_id,
            engine_terminal_confirmed=not unknown, usage_source='synthetic CPU fixture; actual_model_calls=0')
        _fixture_json(self.root / ('finished-' + key + '.json'), dict(
            call_id=identity['call_id'], status=result['status'], finished_monotonic=time.monotonic()))
        return result

    async def abort(self, request_id):
        # An abort acknowledgement cannot prove completion or release a gate.
        return dict(requested=True, engine_terminal_confirmed=False)

    async def close(self, timeout=10):
        self.closed = True
        if self.plan['close_error']:
            raise RuntimeError('controlled CPU engine API close failed')
        return dict(status='closed', engine_api_shutdown_confirmed=True,
                    reserved_model_calls=len(self.calls), actual_model_calls=0,
                    gpu_exit_confirmed=None)


@contextmanager
def cpu_runtime(*, count=2, unknown_rows=(), close_error=False):
    """Fresh real local libraries/actors; only the physical model is synthetic."""
    import daft
    from daft.runners import _get_runner
    import ray
    from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
    from src.execution_provider.adapters.lotus_records_async_actor import AsyncRecordsActorOwner
    from src.execution_provider.adapters.lotus_records_core import SemLoomRecordsCore
    from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
    from src.serving.backends.vllm_async_engine import AsyncEngineLimits
    from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

    class CpuActorOwner(AsyncRecordsActorOwner):
        def _model_actor_type(self):
            production_actor = super()._model_actor_type()

            class CpuModelActor(production_actor):
                def _process_type(self):
                    return ControlledProcess

            return CpuModelActor

        def _model_actor_options(self):
            settings = super()._model_actor_options()
            if settings.get('num_gpus') != 1 or settings.get('max_concurrency') != self.limits.max_inflight + 2:
                raise AssertionError('original actor allocation/control capacity differs')
            return dict(settings, num_gpus=0)

    if (ray.__version__, daft.__version__) != ('2.56.1', '0.7.21'):
        raise AssertionError('fixed CPU library versions required')
    if ray.is_initialized() or _get_runner() is not None:
        raise AssertionError('run each integration case in a fresh supervised driver')
    with tempfile.TemporaryDirectory(prefix='ac-input-') as inputs, \
            tempfile.TemporaryDirectory(prefix='acray-', dir='/tmp') as ray_temp:
        inputs = Path(inputs)
        original = inputs / 'Reviews.csv'
        with original.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(['reviewId', 'id', 'reviewText', 'scoreSentiment'])
            writer.writerows([['same', 'taken_3', 'review ' + str(i), 'POSITIVE' if i == 0 else 'NEGATIVE']
                              for i in range(count)])
        source = MovieRecordSource.from_manifest(prepare_movie_source(original, inputs / 'records',
            expected_csv_sha256=hashlib.sha256(original.read_bytes()).hexdigest(), max_rows=count))
        control = FixtureControl(inputs)
        control.configure(unknown_rows=unknown_rows, close_error=close_error)
        method, generation = FilterMethod('openai/async-cpu-fixture'), GenerationSpec()
        spec = EngineSpec(str(inputs), sys.executable, sys.executable, max_model_calls=6)
        resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
            max_active_batches=2, query_timeout_s=90, startup_timeout_s=60)
        limits = AsyncEngineLimits(max_inflight=2, result_slots=2,
            input_bytes=4 * 1024**2, result_bytes=4 * 1024**2, command_slots=2, control_slots=4)
        runtime = SimpleNamespace(source=source, control=control, owner=None, actor_owner=None, closed=None,
                                  cancelled=threading.Event(), threads=[])
        try:
            ray.init(address='local', num_cpus=3, num_gpus=0, include_dashboard=False,
                object_store_memory=80 * 1024**2, _temp_dir=ray_temp, _node_ip_address='127.0.0.1')
            with patch.object(EngineSpec, 'validate_driver_environment', return_value={'scope': 'CPU fixture'}):
                actors = CpuActorOwner(method, spec, generation, resources, limits=limits)
                runtime.actor_owner = actors
                runtime.owner = SemLoomRecordsCore(method, spec, generation, resources, actor_owner=actors)
                yield runtime
        finally:
            primary = sys.exc_info()[1]
            errors = []
            for row in control.entered():
                if not (inputs / ('release-' + _fixture_key(row['call_id']))).exists():
                    control.release(row['call_id'])
            for thread in runtime.threads:
                thread.join(timeout=2)
                if thread.is_alive():
                    errors.append(RuntimeError('CPU fixture coordinator did not exit'))
            if runtime.owner is not None:
                try:
                    runtime.closed = runtime.owner.close(timeout=5)
                except BaseException as error:
                    errors.append(error)
            try:
                ray.shutdown()
            except BaseException as error:
                errors.append(error)
            if errors:
                if primary is None:
                    raise errors[0]
                for error in errors:
                    primary.add_note('CPU fixture cleanup: ' + type(error).__name__)


@unittest.skipUnless(os.environ.get('RUN_LOTUS_RECORDS_ASYNC_CPU_LIBRARIES') == '1',
                     'explicit new Core/Async CPU-library qualification required')
class AsyncCoreIntegrationTests(unittest.TestCase):
    def save(self, name, value):
        output = os.environ.get('ASYNC_CORE_CPU_ARTIFACTS')
        if output:
            from src.baselines.common.private_artifacts import write_private_json
            from src.baselines.common.redact import redact_json_values

            write_private_json(Path(output) / (name + '.json'), redact_json_values(value))

    def release_second(self, runtime, entered_count=2):
        errors = []

        def release():
            try:
                entered = runtime.control.wait_entered(entered_count, timeout=20)
                current = entered[-2:]
                second = next(row for row in current if row['source_position'] == 1)
                runtime.control.release(second['call_id'])
            except BaseException as error:
                errors.append(error)
                runtime.cancelled.set()

        thread = threading.Thread(target=release, name='cpu-fixture-release', daemon=True)
        runtime.threads.append(thread)
        thread.start()
        return errors

    def reap(self, owner, *, timeout=5):
        until = time.monotonic() + timeout
        while owner.core.capacity.usage().held_tasks:
            owner.core.reap(owner.capacity.step_actions)
            if time.monotonic() >= until:
                self.fail('late known Core responsibility did not settle')
            owner.core.wake.wait(owner.core.wake.generation, 0.005)

    def test_second_request_enters_while_first_is_held_and_completes_first(self):
        from src.scheduling.core.session import SessionEngine
        from src.scheduling.runtime.async_backend import BoundedAsyncBackend

        with cpu_runtime() as runtime:
            owner, control = runtime.owner, runtime.control
            stream = owner.evaluate(runtime.source, query_id='async-core-reverse',
                                    cancelled=runtime.cancelled.is_set)
            self.assertIsInstance(owner.core, SessionEngine)
            self.assertIsInstance(owner.transport.backend, BoundedAsyncBackend)
            errors = self.release_second(runtime)
            first = next(stream)
            self.assertEqual(errors, [])
            self.assertEqual(first['source_position'], 1)
            self.assertEqual(owner.core.capacity.usage().active_requests, 1)
            self.assertEqual(len(control.entered()), 2)
            first_call = next(row['call_id'] for row in control.entered() if row['source_position'] == 0)
            self.assertFalse(control.finished(first_call))
            control.release(first_call)
            second = next(stream)
            self.assertEqual(second['source_position'], 0)
            with self.assertRaises(StopIteration):
                next(stream)
            results = [first, second]
            expected = {row['call_id'] for row in runtime.source.expected('async-core-reverse')}
            self.assertEqual({row['call_id'] for row in results}, expected)
            self.assertEqual(len({row['engine_uid'] for row in results}), 1)
            self.assertEqual(sum(row['prompt_tokens'] for row in results), 22)
            self.assertEqual(sum(row['completion_tokens'] for row in results), 2)
            self.assertEqual(sum(row['total_tokens'] for row in results), 24)
            self.assertTrue(all(value == 0 for value in asdict(owner.core.capacity.usage()).values()))
            close = owner.close(timeout=5)
            self.assertEqual(close['status'], 'closed')
            self.assertTrue(close['transport_closed'])
            self.assertEqual(close['retained_remote_inputs'], 0)
            self.assertIsNone(close['gpu_exit_confirmed'])
            self.save('reverse', dict(results=results, close=close, entered=control.entered(),
                actual_model_calls=0, fixture_evaluations=2, physical_gpu_exit_checked=False))

    def test_slow_consumer_cancelled_late_usage_stays_with_old_query(self):
        with cpu_runtime(count=3) as runtime:
            owner, control = runtime.owner, runtime.control
            first = owner.evaluate(runtime.source, query_id='async-old', cancelled=runtime.cancelled.is_set)
            errors = self.release_second(runtime)
            delivered = next(first)
            self.assertEqual(errors, [])
            self.assertEqual(delivered['source_position'], 1)
            self.assertEqual(len(control.entered()), 2, 'paused next must not submit the third row')
            usage = asdict(owner.core.capacity.usage())
            self.assertEqual(usage['active_requests'], 1)
            self.assertLessEqual(usage['held_tasks'], 2)
            old_call = next(row for row in control.entered() if row['source_position'] == 0)
            before = first.close()
            self.assertEqual(before['status'], 'unknown')
            self.assertEqual(owner.core.capacity.usage().active_requests, 1)
            self.assertFalse(control.finished(old_call['call_id']), 'cancel acknowledgement is not a terminal')
            with self.assertRaises(RuntimeError):
                owner.evaluate(runtime.source, query_id='too-early')
            control.release(old_call['call_id'])
            self.reap(owner)
            second = owner.evaluate(runtime.source, query_id='async-new')
            old_report = owner.last_query_report
            late = old_report['unconsumed_call_results']
            self.assertEqual([(row['query_id'], row['source_position'], row['total_tokens']) for row in late],
                             [('async-old', 0, 12)])

            def release_new():
                try:
                    known = set()
                    until = time.monotonic() + 20
                    while len(known) < 3 and time.monotonic() < until:
                        for row in control.entered():
                            if row['query_id'] == 'async-new' and row['call_id'] not in known:
                                known.add(row['call_id'])
                                control.release(row['call_id'])
                        time.sleep(0.002)
                    if len(known) != 3:
                        raise TimeoutError('new-query CPU requests did not enter')
                except BaseException as error:
                    errors.append(error)
                    runtime.cancelled.set()

            thread = threading.Thread(target=release_new, name='cpu-fixture-next-query', daemon=True)
            runtime.threads.append(thread)
            thread.start()
            results = list(second)
            self.assertEqual(errors, [])
            self.assertEqual({row['query_id'] for row in results}, {'async-new'})
            self.assertEqual(len(results), 3)
            self.assertTrue({row['call_id'] for row in late + [delivered]}.isdisjoint(row['call_id'] for row in results))
            self.assertEqual(len({row['engine_uid'] for row in [delivered, *late, *results]}), 1)
            self.assertTrue(all(value == 0 for value in asdict(owner.core.capacity.usage()).values()))
            self.save('late-cross-query', dict(delivered=delivered, paused_usage=usage, old_close=before,
                old_late_report=old_report, results=results, core_usage=asdict(owner.core.capacity.usage()),
                entered=control.entered(), actual_model_calls=0, fixture_evaluations=5))

    def test_consumed_unknown_keeps_credit_and_partial_usage(self):
        from src.semantic_methods.lotus.records import CallLedger

        with cpu_runtime(unknown_rows=(0,)) as runtime:
            owner, control = runtime.owner, runtime.control
            stream = owner.evaluate(runtime.source, query_id='async-unknown', cancelled=runtime.cancelled.is_set)
            errors = self.release_second(runtime)
            known = next(stream)
            self.assertEqual(errors, [])
            first = next(row for row in control.entered() if row['source_position'] == 0)
            control.release(first['call_id'])
            unknown = next(stream)
            self.assertEqual(unknown['status'], 'unknown')
            self.assertEqual(unknown['prompt_tokens'], 11)
            self.assertIsNone(unknown['completion_tokens'])
            self.assertIsNone(unknown['total_tokens'])
            with self.assertRaises(RuntimeError):
                next(stream)
            self.assertEqual(owner.core.capacity.usage().active_requests, 1)
            self.assertEqual(owner.core.capacity.usage().held_tasks, 1)
            self.assertEqual(owner.transport.retained_refs(), 1)
            with self.assertRaises(RuntimeError):
                owner.evaluate(runtime.source, query_id='cannot-reset-unknown')
            ledger = CallLedger(runtime.source.expected('async-unknown'), 2)
            ledger.record(known)
            ledger.record(unknown)
            close = owner.close(timeout=5)
            self.assertEqual(close['status'], 'unknown')
            self.assertEqual(close['core_usage']['held_tasks'], 1)
            self.assertIsNone(close['gpu_exit_confirmed'])
            self.save('unknown', dict(known=known, unknown=unknown, calls=ledger.report(), close=close,
                actual_model_calls=0, fixture_evaluations=2, unknown_responsibility_retained=True))

    def test_api_close_failure_preserves_known_late_usage(self):
        with cpu_runtime(close_error=True) as runtime:
            owner, control = runtime.owner, runtime.control
            stream = owner.evaluate(runtime.source, query_id='async-close-error', cancelled=runtime.cancelled.is_set)
            errors = self.release_second(runtime)
            known = next(stream)
            self.assertEqual(errors, [])
            held = next(row for row in control.entered() if row['source_position'] == 0)
            stream.close()
            control.release(held['call_id'])
            self.reap(owner)
            close = owner.close(timeout=5)
            self.assertEqual(close['status'], 'unknown')
            late = owner.last_query_report['unconsumed_call_results']
            self.assertEqual([(row['source_position'], row['status'], row['total_tokens']) for row in late],
                             [(0, 'succeeded', 12)])
            self.assertEqual(known['total_tokens'], 12)
            self.assertIsNone(close['gpu_exit_confirmed'])
            self.save('close-error', dict(known=known, late=late, close=close,
                actual_model_calls=0, fixture_evaluations=2, query_success_restored=False))

    def test_owned_actor_death_keeps_unknown_credit_and_known_prefix(self):
        import ray
        from src.semantic_methods.lotus.records import CallLedger

        with cpu_runtime() as runtime:
            owner, control = runtime.owner, runtime.control
            query_id = 'async-owned-actor-death'
            stream = owner.evaluate(runtime.source, query_id=query_id, cancelled=runtime.cancelled.is_set)
            errors = self.release_second(runtime)
            known = next(stream)
            self.assertEqual(errors, [])
            self.assertEqual((known['source_position'], known['status'], known['total_tokens']),
                             (1, 'succeeded', 12))
            entered = control.entered()
            held = next(row for row in entered if row['source_position'] == 0)
            self.assertEqual(len(entered), 2)
            self.assertFalse(control.finished(held['call_id']))
            before = asdict(owner.core.capacity.usage())
            self.assertEqual(before['active_requests'], 1)
            self.assertEqual(before['held_tasks'], 1)
            self.assertEqual(owner.transport.retained_refs(), 1)

            # This exact handle belongs to this fixture. The failure travels
            # through the real Ray future; no terminal or ActorDiedError is injected.
            ray.kill(runtime.actor_owner.model_actor, no_restart=True)
            with self.assertRaises(RuntimeError) as failure:
                next(stream)
            self.assertIn('MODEL_UNAVAILABLE', str(failure.exception))
            after = asdict(owner.core.capacity.usage())
            self.assertEqual(after, before, 'actor death cannot confirm the remote terminal')
            self.assertEqual(owner.transport.retained_refs(), 1)
            self.assertFalse(control.finished(held['call_id']))
            with self.assertRaises(RuntimeError):
                owner.evaluate(runtime.source, query_id='cannot-replace-dead-actor-query')
            self.assertEqual(control.entered(), entered, 'rejected query must not send new work')

            ledger = CallLedger(runtime.source.expected(query_id), 2)
            ledger.record(known)
            # The missing receipt has no authoritative engine usage. This is
            # an independent experiment-ledger placeholder, not an engine result.
            ledger.mark_unconfirmed('OwnedRayActorDeathUnconfirmed')
            calls = ledger.report()
            self.assertEqual(calls['counts']['succeeded'], 1)
            self.assertEqual(calls['counts']['unknown'], 1)
            self.assertEqual(calls['usage']['known_prompt_tokens'], 11)
            self.assertEqual(calls['usage']['known_completion_tokens'], 1)
            self.assertIsNone(calls['usage']['total_tokens'])
            missing = next(row for row in calls['records'] if row['source_position'] == 0)
            self.assertFalse(missing['submission_confirmed'])
            self.assertIsNone(missing['prompt_tokens'])
            self.assertIsNone(missing['completion_tokens'])
            close = owner.close(timeout=5)
            self.assertEqual(close['status'], 'unknown')
            self.assertEqual(close['core_usage'], before)
            self.assertEqual(close['retained_remote_inputs'], 1)
            self.assertIsNone(close['gpu_exit_confirmed'])
            self.assertNotEqual((close['model_owner'].get('engine_close_report') or {}).get(
                'engine_api_shutdown_confirmed'), True)
            self.assertEqual(known['total_tokens'], 12)
            self.save('actor-death', dict(known=known, held_identity=held, calls=calls,
                primary_error_type=type(failure.exception).__name__, primary_error=str(failure.exception),
                usage_before=before, usage_after=after, close=close, entered=entered,
                actual_model_calls=0, fixture_evaluations=2, query_success_restored=False,
                actor_kill_scope='exact owned zero-GPU model actor; no restart',
                unknown_placeholder_source='experiment ledger; missing engine receipt',
                physical_gpu_exit_checked=False))


if __name__ == '__main__':
    if len(sys.argv) == 5 and sys.argv[1] == '--controlled-cpu-serve':
        asyncio.run(serve(*map(int, sys.argv[2:]), engine_factory=ControlledEngine))
    else:
        unittest.main()
