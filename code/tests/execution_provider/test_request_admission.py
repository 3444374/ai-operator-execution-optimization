"""Request capacity is independent of sessions and only released on known outcomes."""
from concurrent.futures import ThreadPoolExecutor
import socket
import threading
import unittest

from src.execution_provider.adapters.semantic_session import CompletionAdapterError, CompletionRequest
from src.execution_provider.adapters.semantic_session import run_v3_session
from src.execution_provider.wire import v3
from src.execution_provider.wire.framing import encode_frame, read_frame
from src.execution_provider.completion import Completion
from src.execution_provider.request_admission import RequestAdmission, RequestCapacity

REQUEST = CompletionRequest('a' * 64, 'model', (), {})
RESULT = Completion('TRUE', 'model', 1, 1, 'stop')


class ControlledAdapter:
    model_id = 'model'

    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.failure = None
        self.calls = 0

    def execution_id_for(self, version):
        return f'controlled-v{version}'

    def complete(self, request):
        self.calls += 1
        self.started.set()
        if not self.release.wait(2):
            raise RuntimeError('test did not release the model')
        if self.failure is not None:
            raise self.failure
        return RESULT


class RequestAdmissionTests(unittest.TestCase):
    def test_busy_is_rejected_without_queue_or_redispatch_and_capacity_returns(self):
        model = ControlledAdapter()
        gate = RequestAdmission(model, 1)
        self.assertEqual(gate.execution_id_for(3), 'controlled-v3')
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(gate.complete, REQUEST)
            try:
                self.assertTrue(model.started.wait(1))
                self.assertEqual(gate.capacity(), RequestCapacity(1, 0))
                with self.assertRaises(CompletionAdapterError) as caught:
                    gate.complete(REQUEST)
                self.assertEqual(caught.exception.code, 'MODEL_REQUEST_REJECTED')
                self.assertEqual(model.calls, 1)
                self.assertEqual(gate.capacity(), RequestCapacity(1, 0))
            finally:
                model.release.set()
            self.assertEqual(pending.result(timeout=1), RESULT)
        self.assertEqual(gate.capacity(), RequestCapacity(0, 0))
        self.assertEqual(gate.complete(REQUEST), RESULT)
        self.assertEqual(model.calls, 2)

    def test_configured_parallel_capacity_accepts_two_and_rejects_a_third(self):
        model = ControlledAdapter()
        both_started = threading.Barrier(3)
        original = model.complete
        def synchronized(request):
            both_started.wait(timeout=2)
            return original(request)
        model.complete = synchronized
        gate = RequestAdmission(model, 2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            pending = [pool.submit(gate.complete, REQUEST) for _ in range(2)]
            try:
                both_started.wait(timeout=2)
                self.assertEqual(gate.capacity(), RequestCapacity(2, 0))
                with self.assertRaises(CompletionAdapterError):
                    gate.complete(REQUEST)
            finally:
                model.release.set()
            self.assertEqual([work.result(timeout=1) for work in pending], [RESULT, RESULT])
        self.assertEqual(gate.capacity(), RequestCapacity(0, 0))
        self.assertEqual(model.calls, 2)

    def test_client_disconnect_does_not_release_a_running_model_request(self):
        model = ControlledAdapter()
        model.execution_id_for = lambda version: v3.GOLDEN_EXECUTION_ID
        gate = RequestAdmission(model, 1)
        plan = v3.SemanticFilterPlan('Keep.', 'model')
        first, first_peer = socket.socketpair()
        second, second_peer = socket.socketpair()
        for peer in (first_peer, second_peer):
            peer.settimeout(1)
        with first, first_peer, second, second_peer, ThreadPoolExecutor(max_workers=2) as pool:
            running = pool.submit(run_v3_session, first, gate)
            try:
                first_peer.sendall(encode_frame(v3.build_open_message(plan)))
                self.assertEqual(read_frame(first_peer)['type'], 'opened')
                first_peer.sendall(encode_frame(v3.build_task_message(plan, sequence=0, input_value='same')))
                self.assertTrue(model.started.wait(1))
                first_peer.close()
                self.assertEqual(gate.capacity(), RequestCapacity(1, 0))
                rejected = pool.submit(run_v3_session, second, gate)
                second_peer.sendall(encode_frame(v3.build_open_message(plan)))
                self.assertEqual(read_frame(second_peer)['type'], 'opened')
                second_peer.sendall(encode_frame(v3.build_task_message(plan, sequence=0, input_value='same')))
                self.assertEqual(read_frame(second_peer)['code'], 'MODEL_REQUEST_REJECTED')
                rejected.result(timeout=1)
                self.assertEqual(model.calls, 1)
            finally:
                model.release.set()
            running.result(timeout=1)
        self.assertEqual(gate.capacity(), RequestCapacity(0, 0))
        self.assertEqual(gate.complete(REQUEST), RESULT)

    def test_unknown_outcome_keeps_capacity_but_known_terminal_error_releases_it(self):
        for unknown in (False, True):
            with self.subTest(remote_outcome_unknown=unknown):
                model = ControlledAdapter()
                model.release.set()
                model.failure = CompletionAdapterError('MODEL_TIMEOUT', remote_outcome_unknown=unknown)
                gate = RequestAdmission(model, 1)
                if unknown:
                    with self.assertLogs('src.execution_provider.request_admission', level='WARNING'):
                        with self.assertRaises(CompletionAdapterError):
                            gate.complete(REQUEST)
                else:
                    with self.assertRaises(CompletionAdapterError):
                        gate.complete(REQUEST)
                self.assertEqual(gate.capacity(), RequestCapacity(0, int(unknown)))
                model.failure = None
                if unknown:
                    with self.assertRaises(CompletionAdapterError) as caught:
                        gate.complete(REQUEST)
                    self.assertEqual(caught.exception.code, 'MODEL_REQUEST_REJECTED')
                    self.assertEqual(model.calls, 1)
                else:
                    self.assertEqual(gate.complete(REQUEST), RESULT)

    def test_unexpected_adapter_failure_never_assumes_remote_completion(self):
        model = ControlledAdapter()
        model.release.set()
        model.failure = RuntimeError('private payload')
        gate = RequestAdmission(model, 1)
        with self.assertLogs('src.execution_provider.request_admission', level='WARNING') as logs:
            with self.assertRaises(RuntimeError):
                gate.complete(REQUEST)
        self.assertNotIn('private payload', ''.join(logs.output))
        self.assertEqual(gate.capacity(), RequestCapacity(0, 1))
