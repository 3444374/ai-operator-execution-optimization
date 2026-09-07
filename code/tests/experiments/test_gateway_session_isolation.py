"""Interleaved session observation must retain the originating session identity."""
from concurrent.futures import ThreadPoolExecutor
import socket
import threading
import unittest

from src.execution_provider.adapters.semantic_session import CompletionRequest
from src.experiments.gateway_observer import SessionObserver
from src.execution_provider.session_dispatch import run_session


class SessionIsolationTests(unittest.TestCase):
    def test_observation_failure_always_restores_session_context(self):
        for failing_event in ('session_start', 'session_end'):
            with self.subTest(event=failing_event):
                def record(event):
                    if event['event'] == failing_event:
                        raise RuntimeError('sink failed')
                observer = SessionObserver(record)
                first, second = socket.socketpair()
                with first, second:
                    with self.assertRaises(RuntimeError):
                        observer.run_session(first, lambda connection: None)
                self.assertIsNone(observer.current_session)

    def test_initial_frame_timeout_is_observed_after_socket_closure(self):
        events = []
        observer = SessionObserver(events.append)
        reader, writer = socket.socketpair()
        with reader, writer:
            reader.settimeout(.01)
            with self.assertRaises(TimeoutError):
                observer.run_session(reader, run_session, completion_adapter=None,
                                     response_delay_ms=0, tamper_evidence_digest=False,
                                     disconnect_on_task=False, completion_fixture=None)
        self.assertTrue(events[-1]['connection_closed'])
        self.assertEqual(events[-1]['termination'], 'raised')
        self.assertIsNone(observer.current_session)

    def test_interleaved_tasks_and_errors_keep_session_identity(self):
        events = []
        observer = SessionObserver(events.append)
        first_started = threading.Event()
        second_started = threading.Event()
        first_completed = threading.Event()
        identities = {}
        request = CompletionRequest('a' * 64, 'model', (), {})

        def first(connection):
            identities['first'] = observer.current_session
            first_started.set()
            self.assertTrue(second_started.wait(2))
            observer.complete(request, lambda _: 'ok')
            first_completed.set()
            connection.close()

        def second(connection):
            identities['second'] = observer.current_session
            second_started.set()
            self.assertTrue(first_completed.wait(2))
            def fail(_):
                raise ValueError('private input')
            with self.assertRaises(ValueError):
                observer.complete(request, fail)
            connection.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            a, peer_a = socket.socketpair()
            b, peer_b = socket.socketpair()
            with a, peer_a, b, peer_b:
                one = pool.submit(observer.run_session, a, first)
                self.assertTrue(first_started.wait(2))
                two = pool.submit(observer.run_session, b, second)
                one.result(timeout=3)
                two.result(timeout=3)
        tasks = [event for event in events if event['event'] == 'task']
        terminals = [event for event in events if event['event'] in ('task_complete', 'task_error')]
        self.assertEqual([event['session_id'] for event in tasks], [identities['first'], identities['second']])
        self.assertEqual([event['session_id'] for event in terminals], [identities['first'], identities['second']])
        self.assertEqual((observer.sessions, observer.tasks, observer.current_session), (2, 2, None))
        self.assertNotIn('private input', str(events))
