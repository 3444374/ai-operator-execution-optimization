"""Public Core/HTTP diagnostic substitution; actual Daft/Ray checks run separately."""

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

from src.execution_provider.adapters.full_response import FullResponseTransport
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.adapters.sema_semloom import SemaSemLoomService
from src.execution_provider.adapters.sema_service import SemaServiceLimits

import test_sema_service as service_tests


def _public_core_diagnostic(service):
    class GuardedDiagnosticTransport(FullResponseTransport):
        async def execute(self, task, endpoint):
            try:
                service._before_request(task)
            except Exception:
                # Match the existing Ray hook's confirmed pre-send response.
                return b'{"bridge_error":"MODEL_UNAVAILABLE"}'
            return await super().execute(task, endpoint)

    with mock.patch('src.execution_provider.adapters.native_tasks.FullResponseTransport',
                    GuardedDiagnosticTransport):
        return build_native_execution(service.model_config, physical=None,
            max_tasks=service.max_held_tasks, max_active_requests=service.max_active_requests,
            observer=service._observe)


class SemaSemLoomDiagnosticTests(service_tests.SemaServiceTests):
    def setUp(self):
        super().setUp()
        patch = mock.patch.object(SemaSemLoomService, '_build_execution', _public_core_diagnostic)
        patch.start()
        self.addCleanup(patch.stop)
        self.physical = RayMapConfig('127.0.0.1:16379', 1, 1, 2**21, 2**23)

    def service(self, max_active_requests=1, **kwargs):
        return SemaSemLoomService(query_id='query-a', model_config=FixedModelConfig(self.url, 'fixture-model', 2000),
            physical=self.physical, limits=self.limits, trace_path=self.root / 'requests.jsonl',
            max_held_tasks=2, max_active_requests=max_active_requests, **kwargs)

    def test_complete_json_payload_and_equal_requests_stay_separate(self):
        # Include the explicitly supported non-streaming single-model schema.
        super().test_complete_json_payload_and_equal_requests_stay_separate()
        rows = [json.loads(line) for line in (self.root / 'requests.jsonl').read_text().splitlines()]
        self.assertEqual([r['core_task_sequence'] for r in rows[:2]], [0, 1])

    def test_capacity_waiting_uses_public_session_and_returns_all_results(self):
        self.server.gate = threading.Event()
        with self.service() as service:
            with ThreadPoolExecutor(max_workers=3) as callers:
                futures = [callers.submit(self.post, service.endpoint_url) for _ in range(3)]
                self.assertTrue(self.server.received.wait(1))
                time.sleep(0.05)
                self.assertEqual(len(self.server.calls), 1)
                self.server.gate.set()
                self.assertEqual([f.result()[0] for f in futures], [200, 200, 200])
            service.end_input()
        self.assertEqual(service.summary['forwarded_posts'], 3)
        self.assertEqual(service.cleanup_errors, [])
        self.assertEqual(service._execution.engine.capacity.records, {})
        events = [e['event'] for e in service.summary['core_events']]
        self.assertIn('submitted', events)
        self.assertIn('terminal', events)

    def test_first_http_error_suppresses_other_accepted_and_unaccepted_requests(self):
        self.server.status = 503
        self.server.gate = threading.Event()
        with self.service() as service:
            with ThreadPoolExecutor(max_workers=3) as callers:
                futures = [callers.submit(self.post, service.endpoint_url) for _ in range(3)]
                self.assertTrue(self.server.received.wait(1))
                self.server.gate.set()
                results = [f.result() for f in futures]
            self.assertIn((503, self.server.body), [r[:2] for r in results])
            self.assertEqual(service.first_error.status, 503)
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
        self.assertEqual(len(self.server.calls), 1)
        self.assertEqual(service.summary['forwarded_posts'], 1)
        self.assertEqual(service._execution.engine.capacity.records, {})

    def test_cancellation_with_remote_work_keeps_result_ownership_until_settled(self):
        self.server.gate = threading.Event()
        with self.service() as service:
            with ThreadPoolExecutor(max_workers=1) as callers:
                future = callers.submit(self.post, service.endpoint_url)
                self.assertTrue(self.server.received.wait(1))
                service.cancel()
                self.assertEqual(self.post(service.endpoint_url)[0], 410)
                self.server.gate.set()
                self.assertIn(future.result()[0], (200, 502))
        self.assertEqual(len(self.server.calls), 1)
        self.assertEqual(service._execution.engine.capacity.records, {})
        self.assertEqual(service.cleanup_errors, [])

    def test_streaming_or_wrong_model_is_rejected_before_model_post(self):
        with self.service() as service:
            body = json.dumps({'model': 'fixture-model', 'stream': True, 'messages': []}).encode()
            self.assertEqual(self.post(service.endpoint_url, body)[0], 502)
            self.assertEqual(service.summary['forwarded_posts'], 0)
        self.assertEqual(self.server.calls, [])

    def test_out_of_order_complete_responses_keep_their_http_caller(self):
        def reply_for(body):
            tag = json.loads(body)['messages'][0]['content']
            return 200, json.dumps({'native_value': tag}).encode(), 0.06 if tag == 'first' else 0
        self.server.reply_for = reply_for
        with self.service(max_active_requests=2) as service:
            with ThreadPoolExecutor(max_workers=2) as callers:
                bodies = [self.payload.replace(b'same', tag.encode()) for tag in ('first', 'second')]
                futures = [callers.submit(self.post, service.endpoint_url, bodies[0])]
                self.assertTrue(self.server.received.wait(1))
                futures.append(callers.submit(self.post, service.endpoint_url, bodies[1]))
                completed = [json.loads(f.result()[1])['native_value'] for f in as_completed(futures)]
                self.assertEqual(completed, ['second', 'first'])
                self.assertEqual([json.loads(f.result()[1])['native_value'] for f in futures], ['first', 'second'])
            service.end_input()
        self.assertEqual(Counter(r[1] for r in self.server.calls), Counter(bodies))
        self.assertEqual(service._execution.engine.capacity.records, {})

    def test_main_path_requires_actual_ray_config_and_daft_payload_backend(self):
        config = FixedModelConfig(self.url, 'fixture-model', 2000)
        with self.assertRaisesRegex(ValueError, 'RayMapConfig'):
            SemaSemLoomService(query_id='q', model_config=config, physical=None,
                limits=self.limits, trace_path=self.root / 'invalid.jsonl', max_held_tasks=2, max_active_requests=1)
        with self.assertRaisesRegex(ValueError, 'Daft payload'):
            SemaSemLoomService(query_id='q', model_config=config,
                physical=RayMapConfig('127.0.0.1:16379', 1, 1, 2**21, 2**23, payload_backend='arrow'),
                limits=self.limits, trace_path=self.root / 'invalid.jsonl', max_held_tasks=2, max_active_requests=1)


if __name__ == '__main__':
    unittest.main()
