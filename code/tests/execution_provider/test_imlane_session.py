"""Local subprocess/HTTP protocol fixtures; no author runtime, model or GPU."""
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
import test_sema_imlane as bridge_fixture

from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig, IMLaneCallExecutor
from src.execution_provider.adapters.sema_imlane import SemaIMLaneExecutor


class IMLaneCloseReportTests(unittest.TestCase):
    def test_default_bridge_started_during_close_is_not_reported_stopped_after_stop_failure(self):
        # Deterministically place a default bridge start just before close acquires
        # the launch lock; no author process, HTTP fixture or model is started.
        with tempfile.TemporaryDirectory() as directory:
            delegate = object.__new__(SemaIMLaneExecutor)
            delegate._lock = threading.Lock()
            delegate._closed = False
            delegate._session = delegate._process = None
            executor = object.__new__(IMLaneCallExecutor)
            executor.delegate = delegate
            executor.last_close_report = None
            executor._uncertain = Path(directory) / 'uncertain.json'
            process = mock.Mock()
            process.poll.return_value = None
            begin_close = delegate.begin_close

            def launch_before_close_lock():
                with delegate._lock:
                    delegate._process = process
                return begin_close()

            with mock.patch.object(delegate, 'begin_close', side_effect=launch_before_close_lock), \
                 mock.patch.object(delegate, '_stop', side_effect=TimeoutError('bridge still alive')) as stop:
                with self.assertRaisesRegex(TimeoutError, 'bridge still alive'):
                    executor.close()
            stop.assert_called_once_with(process)
            self.assertFalse(executor.last_close_report['bridge_stopped'])
            self.assertFalse(executor.last_close_report['clean'])
            self.assertEqual(executor.last_close_report['remote_http_stopped'], 'unavailable')
            self.assertFalse(json.loads(executor._uncertain.read_text())['bridge_stopped'])
            another = object.__new__(IMLaneCallExecutor)
            another._uncertain = executor._uncertain
            with self.assertRaisesRegex(RuntimeError, 'unsettled'):
                another.start()


class IMLaneSessionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = bridge_fixture.SemaIMLaneBridgeTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def executor(self, action='', *, after_end='', stats_change=''):
        original = self.fixture.executor()
        script = self.fixture.root / ('session-fixture-' + str(time.monotonic_ns()))
        script.write_text('#!' + sys.executable + '\n' + '''
import asyncio,json,os,sys,time
from pathlib import Path
sys.path.insert(0, CODE_ROOT)
from src.baselines.text.frameworks.native_calls import _call_from_wire,_request,_outcome_wire
from src.execution_provider.adapters.full_response import encode_full_response
def emit(tag, value):
 print('SEMA_IMLANE_'+tag+' '+json.dumps(value),flush=True)
names=['max_batch_rows','max_input_payload_bytes','max_input_wire_bytes',
       'max_item_payload_bytes','max_pending_calls','max_batch_response_bytes']
limits=dict(zip(names,map(int,sys.argv[4:10])))
emit('META',dict(native_executors=2,native_task_threads=4))
emit('LIMITS',limits)
emit('SESSION',dict(protocol=1))
batch_count=0
for line in sys.stdin:
 command=json.loads(line)
 if command['kind']=='close':
  emit('SESSION_END',dict(protocol=1));break
 batch_count+=1
 batch=command['batch_id']
 raw=Path(command['input_path']).read_bytes()
 calls=[_call_from_wire(line) for line in raw.splitlines()]
 total=0
 for call in reversed(calls):
  outcome=asyncio.run(_request(call))
  size=(len(encode_full_response(outcome.response)) if outcome.response else
        len((outcome.transport_error or outcome.execution_error).encode()))
  total+=size
  value=dict(batch_id=batch,outcome=json.loads(_outcome_wire(outcome)))
  ACTION
  emit('RESULT',value)
 stats=dict(submitted=len(calls),delivered=len(calls),pending_peak=min(len(calls),limits['max_pending_calls']),
  input_payload_bytes=sum(len(c.payload) for c in calls),stdin_wire_bytes=len(raw),
  delivered_response_bytes=total,session_batches=batch_count,
  timing_ns={k:1 for k in ['input_read_parse_ns','input_arrow_ns','context_setup_ns','function_register_ns',
   'author_execute_inclusive_ns','output_copy_encode_ns','output_copy_decode_ns',
   'output_wire_ns','stdout_write_ns','batch_wall_ns']},
  lifecycle=dict(context_creations=int(batch_count==1),registrations=int(batch_count==1),
                 author_calls=len(calls),output_copies=len(calls)))
 STATS_CHANGE
 emit('BATCH_END',dict(batch_id=batch,stats=stats))
 AFTER_END
'''.replace('CODE_ROOT', repr(str(self.fixture.code_root))).replace('ACTION', action or 'pass')
   .replace('AFTER_END', after_end or 'pass').replace('STATS_CHANGE', stats_change or 'pass'))
        script.chmod(0o700)
        config = dict(original.config, bridge_binary=str(script),
                      bridge_sha256=hashlib.sha256(script.read_bytes()).hexdigest())
        path = self.fixture.root / (script.name + '.json')
        path.write_text(json.dumps(config))
        executor = SemaIMLaneExecutor(path, original.options, reuse_bridge=True)
        self.addCleanup(executor.close)
        return executor

    def calls(self):
        return [replace(self.fixture.call(i), row_id=5) for i in range(3)]

    def test_multiple_batches_reuse_one_process_and_keep_duplicate_sources_and_full_response(self):
        self.fixture.server.status = 429
        executor = self.executor()
        calls = self.calls()
        with mock.patch('src.execution_provider.adapters.imlane_session.subprocess.Popen',
                        wraps=__import__('subprocess').Popen) as launching:
            first = list(executor.iter_batch(calls))
            first_report = executor.last_close_report
            pid = executor._process.pid
            second = list(executor.iter_batch(calls))
            self.assertEqual(executor._process.pid, pid)
            self.assertEqual(launching.call_count, 1)
        self.assertEqual([v.call_id for v in first], [c.call_id for c in reversed(calls)])
        self.assertEqual([v.row_id for v in second], [5, 5, 5])
        self.assertEqual(self.fixture.server.calls, [self.fixture.raw] * 6)
        self.assertEqual(first[0].response.body, self.fixture.server.body)
        self.assertEqual(first[0].response.status_code, 429)
        self.assertEqual([v for k, v in first[0].response.headers if k == 'X-Repeat'], ['one', 'two'])
        self.assertEqual(first[0].response.body, second[0].response.body)
        self.assertNotEqual(first_report['batch_id'], executor.last_close_report['batch_id'])
        self.assertEqual(first_report['bridge_stats']['lifecycle']['context_creations'], 1)
        self.assertEqual(executor.last_close_report['bridge_stats']['lifecycle']['context_creations'], 0)
        self.assertTrue(executor.last_close_report['batch_callbacks_returned'])
        self.assertTrue(executor.last_close_report['clean'])
        self.assertFalse(executor.last_close_report['bridge_stopped'])
        self.assertIn('unavailable', executor.last_close_report['author_lane_restore_ack'])
        for name, value in executor.last_close_report['timing_ns'].items():
            self.assertIs(type(value), int, name)
            self.assertGreaterEqual(value, 0, name)
        executor.close()
        self.assertEqual(executor._session.process.returncode, 0)

    def test_foreign_batch_duplicate_occurrence_and_crash_poison_without_relaunch(self):
        for action, error in (("value['batch_id']='old-batch'", ValueError),
                              ("emit('RESULT',value)", ValueError),
                              ('os._exit(7)', RuntimeError)):
            with self.subTest(action=action):
                executor = self.executor(action)
                with self.assertRaises(error):
                    list(executor.iter_batch(self.calls()))
                self.assertTrue(executor.poisoned)
                self.assertTrue(executor.last_close_report['bridge_stopped'])
                self.assertFalse(executor.last_close_report['clean'])
                with mock.patch('src.execution_provider.adapters.imlane_session.subprocess.Popen') as launching:
                    with self.assertRaisesRegex(RuntimeError, 'uncertain previous batch'):
                        list(executor.iter_batch(self.calls()))
                    launching.assert_not_called()

    def test_consumer_parser_failure_and_cancel_keep_remote_completion_unknown(self):
        executor = self.executor()
        iterator = executor.iter_batch(self.calls())
        primary = ValueError('fixture parser first failure')
        with self.assertRaises(ValueError) as observed:
            try:
                next(iterator)
                raise primary
            finally:
                iterator.close()
        self.assertIs(observed.exception, primary)
        self.assertFalse(executor.last_close_report['clean'])
        self.assertEqual(executor.last_close_report['remote_http_stopped'], 'unavailable')
        self.assertTrue(executor.poisoned)

        executor = self.executor()
        cancelled = False
        iterator = executor.iter_batch(self.calls(), cancelled=lambda: cancelled)
        next(iterator)
        cancelled = True
        with self.assertRaises(InterruptedError):
            next(iterator)
        self.assertTrue(executor.last_close_report['bridge_stopped'])
        self.assertFalse(executor.last_close_report['batch_callbacks_returned'])

    def test_transport_error_is_a_complete_value_but_quarantines_context(self):
        executor = self.executor()
        call = self.fixture.call(endpoint='http://127.0.0.1:1/closed')
        outcome, = executor.iter_batch([call])
        self.assertTrue(outcome.transport_error)
        self.assertIsNone(outcome.response)
        self.assertTrue(executor.last_close_report['batch_callbacks_returned'])
        self.assertFalse(executor.last_close_report['clean'])
        self.assertTrue(executor.last_close_report['bridge_stopped'])
        self.assertTrue(executor.poisoned)

    def test_incomplete_end_unclaimed_results_and_invalid_counts_never_allow_reuse(self):
        for stats_change, after_end in (("stats['delivered']=0", ''),
                                       ("stats['submitted']=True", ''),
                                       ("stats['timing_ns']['batch_wall_ns']=-1", ''),
                                       ('', "emit('RESULT',value)")):
            with self.subTest(stats_change=stats_change, after_end=after_end):
                executor = self.executor(stats_change=stats_change, after_end=after_end)
                with self.assertRaises(ValueError):
                    list(executor.iter_batch([self.fixture.call()]))
                self.assertFalse(executor.last_close_report['clean'])
                self.assertTrue(executor.poisoned)

    def test_author_logs_after_end_are_bounded_without_rejecting_a_healthy_batch(self):
        executor = self.executor(after_end="print('fixture author log',flush=True)")
        for _ in range(2):
            list(executor.iter_batch([self.fixture.call()]))
            self.assertTrue(executor.last_close_report['clean'])

    def test_cancel_before_next_batch_preserves_healthy_session_and_empty_sends_nothing(self):
        executor = self.executor()
        list(executor.iter_batch([self.fixture.call()]))
        process = executor._process
        before = len(self.fixture.server.calls)
        with self.assertRaises(InterruptedError):
            list(executor.iter_batch([self.fixture.call()], cancelled=lambda: True))
        self.assertEqual(list(executor.iter_batch([])), [])
        self.assertIs(executor._process, process)
        self.assertEqual(len(self.fixture.server.calls), before)
        self.assertFalse(executor.poisoned)
        list(executor.iter_batch([self.fixture.call()]))

    def test_session_timeout_terminates_child_without_claiming_remote_cancel(self):
        executor = self.executor(action='time.sleep(.3)')
        with self.assertRaises(TimeoutError):
            list(executor.iter_batch([self.fixture.call()], timeout_s=.1))
        self.assertTrue(executor.poisoned)
        self.assertFalse(executor.last_close_report['clean'])
        self.assertEqual(executor.last_close_report['remote_http_stopped'], 'unavailable')

    def test_fixed_udf_identity_change_stops_live_session(self):
        executor = self.executor()
        list(executor.iter_batch(self.calls()))
        # Alter the recorded identity only; the real UDF file is never edited.
        executor.config['udf_sha256'] = '0' * 64
        before = len(self.fixture.server.calls)
        with self.assertRaisesRegex(ValueError, 'recorded identity'):
            list(executor.iter_batch(self.calls()))
        self.assertEqual(len(self.fixture.server.calls), before)
        self.assertTrue(executor.poisoned)
        self.assertTrue(executor.last_close_report['bridge_stopped'])

    def test_first_protocol_error_survives_stop_and_evidence_errors(self):
        executor = self.executor("value['outcome']['row_id']=999")
        stop = executor._stop
        def fail_stop(process):
            stop(process)
            raise OSError('fixture cleanup failure')
        with mock.patch.object(executor, '_stop', side_effect=fail_stop), \
             mock.patch('src.execution_provider.adapters.imlane_session.write_private_json',
                        side_effect=OSError('fixture record failure')):
            with self.assertRaisesRegex(ValueError, 'occurrence identity') as observed:
                list(executor.iter_batch(self.calls()))
        self.assertIn('IMLane session cleanup error: OSError', observed.exception.__notes__)
        self.assertIn('IMLane session record error: OSError', observed.exception.__notes__)

    def test_public_executor_marks_uncertain_coordinator_for_next_owner(self):
        delegate = self.executor()
        config = IMLaneCallConfig('cpython', str(delegate.config_path), 123, 456,
            str(self.fixture.root / 'unused-build.json'), reuse_bridge=True)
        executor = IMLaneCallExecutor(config, delegate.options)
        self.addCleanup(executor.close)
        with mock.patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator', return_value={}):
            iterator = executor.iter_batch(self.calls())
            next(iterator)
            iterator.close()
            self.assertTrue(executor._uncertain.is_file())
            self.assertTrue(executor.last_close_report['external_coordinator_requires_cleanup'])
            another = IMLaneCallExecutor(config, delegate.options)
            self.addCleanup(another.close)
            with self.assertRaisesRegex(RuntimeError, 'unsettled'):
                list(another.iter_batch(self.calls()))

    def test_public_native_factory_passes_opt_in_and_allows_healthy_reuse(self):
        from src.baselines.text.frameworks.native_calls import NativeCallExecutor
        delegate = self.executor()
        config = IMLaneCallConfig('ray', str(delegate.config_path), 123, 456,
            str(self.fixture.root / 'unused-build.json'), reuse_bridge=True)
        executor = NativeCallExecutor('imlane', delegate.options, imlane_config=config)
        self.addCleanup(executor.close)
        with mock.patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator', return_value={}):
            list(executor.iter_batch(self.calls()))
            pid = executor._imlane.delegate._process.pid
            list(executor.iter_batch(self.calls()))
            self.assertEqual(executor._imlane.delegate._process.pid, pid)
            self.assertTrue(executor.last_close_report['reuse_allowed'])
            self.assertEqual(executor.last_close_report['bridge_lifecycle'], 'session')
            self.assertFalse(executor.last_close_report['bridge_stopped'])

    def test_close_while_iterator_suspended_blocks_new_owner_before_iterator_finally(self):
        delegate = self.executor()
        config = IMLaneCallConfig('cpython', str(delegate.config_path), 123, 456,
            str(self.fixture.root / 'unused-build.json'), reuse_bridge=True)
        executor = IMLaneCallExecutor(config, delegate.options)
        self.addCleanup(executor.close)
        with mock.patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator', return_value={}):
            iterator = executor.iter_batch(self.calls())
            next(iterator)
            executor.close()
            self.assertTrue(executor._uncertain.is_file())
            first_record = executor._uncertain.read_bytes()
            another = IMLaneCallExecutor(config, delegate.options)
            self.addCleanup(another.close)
            with self.assertRaisesRegex(RuntimeError, 'unsettled'):
                another.start()
            iterator.close()
            self.assertEqual(executor._uncertain.read_bytes(), first_record)

    def test_context_exit_retains_parser_first_error_and_persists_uncertain_work(self):
        delegate = self.executor()
        config = IMLaneCallConfig('cpython', str(delegate.config_path), 123, 456,
            str(self.fixture.root / 'unused-build.json'), reuse_bridge=True)
        executor = IMLaneCallExecutor(config, delegate.options)
        self.addCleanup(executor.close)
        first_error = ValueError('parser first error in context')
        with mock.patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator', return_value={}):
            with self.assertRaises(ValueError) as observed:
                with executor:
                    iterator = executor.iter_batch(self.calls())
                    next(iterator)
                    raise first_error
            self.assertIs(observed.exception, first_error)
            self.assertTrue(executor._uncertain.is_file())
            iterator.close()

    def test_concurrent_close_after_transport_end_still_blocks_next_coordinator_owner(self):
        from src.execution_provider.adapters.imlane_session import IMLaneBridgeSession
        delegate = self.executor()
        config = IMLaneCallConfig('cpython', str(delegate.config_path), 123, 456,
            str(self.fixture.root / 'unused-build.json'), reuse_bridge=True)
        executor = IMLaneCallExecutor(config, delegate.options)
        self.addCleanup(executor.close)
        session = executor.delegate._session = IMLaneBridgeSession(executor.delegate)
        end_seen, resume = threading.Event(), threading.Event()
        idle = session._idle
        def pause_after_end(*args, **kwargs):
            if session.batches == 1:
                end_seen.set()
                if not resume.wait(3):
                    raise TimeoutError('fixture end pause expired')
            return idle(*args, **kwargs)
        call = self.fixture.call(endpoint='http://127.0.0.1:1/closed')
        with mock.patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator', return_value={}), \
             mock.patch.object(session, '_idle', side_effect=pause_after_end), \
             ThreadPoolExecutor(max_workers=1) as thread:
            result = thread.submit(lambda: list(executor.iter_batch([call])))
            try:
                self.assertTrue(end_seen.wait(3))
                self.assertTrue(executor.delegate.author_submission_pending)
                executor.close()
                self.assertTrue(executor._uncertain.is_file())
                another = IMLaneCallExecutor(config, delegate.options)
                self.addCleanup(another.close)
                with self.assertRaisesRegex(RuntimeError, 'unsettled'):
                    another.start()
            finally:
                resume.set()
            with self.assertRaises((InterruptedError, RuntimeError, ValueError)):
                result.result(timeout=3)

    def test_foreign_close_record_and_start_after_close_are_rejected(self):
        executor = self.executor()
        list(executor.iter_batch([self.fixture.call()]))
        session = executor._session
        with mock.patch.object(session, '_record', return_value=(b'SESSION_END', {'protocol': True})):
            with self.assertRaisesRegex(ValueError, 'close record'):
                executor.close()
        self.assertTrue(executor.poisoned)
        self.assertIsNotNone(session.process.poll())
        with self.assertRaises(RuntimeError):
            list(executor.iter_batch([self.fixture.call()]))

    def test_explicit_option_and_input_rejection_never_start_child(self):
        executor = self.executor()
        executor.limits = replace(executor.limits, max_batch_rows=1)
        with mock.patch('src.execution_provider.adapters.imlane_session.subprocess.Popen') as launching:
            with self.assertRaises(ValueError):
                list(executor.iter_batch(self.calls()))
            with self.assertRaises(InterruptedError):
                list(executor.iter_batch([self.fixture.call()], cancelled=lambda: True))
            self.assertEqual(list(executor.iter_batch([])), [])
            launching.assert_not_called()
        self.assertFalse(executor.poisoned)
        self.assertFalse(self.fixture.executor().reuse_bridge)
        with self.assertRaises(TypeError):
            SemaIMLaneExecutor(executor.config_path, executor.options, reuse_bridge=1)


if __name__ == '__main__':
    unittest.main()
