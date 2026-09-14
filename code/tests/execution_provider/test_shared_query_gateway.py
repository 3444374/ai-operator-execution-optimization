"""Shared query compute through real socket workers and a controlled backend."""

import asyncio
from functools import partial
import threading
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.server import main, parse_args
from src.scheduling.core.session_jobs import shared_compute_job_budget
from tests.execution_provider.test_multisession_gateway import service, Client, completion, wait_for
from tests.execution_provider.test_query_registration import control, binding


class SharedQueryGatewayTests(unittest.TestCase):
    def test_idle_queries_and_slow_consumer_do_not_strand_compute(self):
        for count in (2, 4):
            with self.subTest(queries=count):
                released = threading.Event()
                started = []

                async def execute(task, endpoint):
                    started.append(task.key)
                    while not released.is_set():
                        await asyncio.sleep(.005)
                    return completion('ok')

                factory = partial(build_fixed_model_execution, allocate_job=shared_compute_job_budget)
                with patch('src.execution_provider.multiplexed_gateway.trusted_peer', return_value=(1, 42)):
                    with service(execute, max_jobs=count, max_tasks=count * 4,
                                 max_active_requests=4, max_connections=count * 2 + 3,
                                 execution_factory=factory) as (path, gateway, events):
                        controls, clients = [], []
                        try:
                            for _ in range(count):
                                conn, opened = control(path, 1)
                                controls.append((conn, opened))
                                self.assertEqual(opened['type'], 'query_opened')
                            for index in (0, 1):
                                client = Client(path, window=4,
                                    binding=binding(controls[index][1]['token'], 0))
                                clients.append(client)
                                for row in range(4):
                                    self.assertEqual(client.offer(str(row)), 1)
                                client.poll()
                                wait_for(lambda: len(started) == (index + 1) * 4)
                                self.assertEqual(gateway.engine.capacity.usage().active_requests, 4)
                                if index == 1:
                                    # A still retains three completed results in the gateway.
                                    self.assertEqual(gateway.engine.capacity.usage().held_tasks, 7)
                                released.set()
                                self.assertEqual(client.result()['raw_output'], 'ok')
                                wait_for(lambda: gateway.engine.capacity.usage().active_requests == 0)
                                released.clear()
                            for client in clients:
                                for _ in range(3):
                                    client.poll()
                                    self.assertEqual(client.result()['raw_output'], 'ok')
                        finally:
                            released.set()
                            for client in clients:
                                client.close()
                            for conn, _ in controls:
                                conn.close()
                        wait_for(lambda: sum(e['event'] == 'job_drained' for e in events) == count)
                        self.assertEqual(gateway.engine.capacity.usage().held_tasks, 0)
                        self.assertEqual(len(gateway.engine.jobs.jobs), 0)

    def test_shared_cli_requires_incremental_execution(self):
        self.assertEqual(parse_args(['--socket', '/tmp/unused']).job_compute_policy, 'equal-share')
        with self.assertRaisesRegex(SystemExit, 'shared compute requires incremental'):
            main(['--socket', '/tmp/unused', '--job-compute-policy', 'shared'])
        with self.assertRaisesRegex(SystemExit, 'without a custom factory'):
            main(['--socket', '/tmp/unused', '--incremental-map', '--job-compute-policy', 'shared'],
                 incremental_execution_factory=lambda: None)
