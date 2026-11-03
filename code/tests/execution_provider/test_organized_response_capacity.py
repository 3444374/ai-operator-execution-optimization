"""Small PG response quotas must survive each public organization mode."""

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from src.execution_provider.adapters.map_organization import (
    MapOrganizationConfig, organization_factory, tokenizer_fingerprint,
)
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.completion import CompletionRequest
from src.scheduling.core.session_contract import SessionSpec, Usage
from tests.execution_provider.test_map_organization import CountingTokenizer
from tests.execution_provider.test_multisession_gateway import Client, completion, service, wait_for


class OrganizedResponseCapacityTests(unittest.TestCase):
    def factory(self, directory, mode):
        Path(directory, 'tokenizer_config.json').write_text('{}')
        config = MapOrganizationConfig(mode, 4, 2, 128, 1024, 'model', 'r1', 's1', directory,
                                       tokenizer_fingerprint(directory), 512)
        return organization_factory(config, tokenizer=CountingTokenizer())

    def test_all_modes_preserve_configured_result_reservation_before_dispatch(self):
        for mode in ('rows', 'work', 'length'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                sent = []

                async def execute(task, endpoint):
                    sent.append(task)
                    return completion('ok')

                execution = self.factory(directory, mode)(
                    FixedModelConfig('http://localhost/v1/chat/completions', 'model', 100,
                                     max_response_bytes=4096),
                    max_tasks=4, max_active_requests=2, result_bytes=8192, execute=execute,
                )
                request = CompletionRequest('a' * 64, 'model', ({'role': 'user', 'content': 'hello'},),
                                            {'max_tokens': 8}, protocol_version=6)
                job, session, _ = execution.open_job('small', SessionSpec(
                    'small', 'map', 'default', work_unit='tokens'))
                try:
                    task = execution.prepare_task(request, 0)
                    self.assertEqual(task.max_result_bytes, 4096)
                    self.assertEqual(task.info.work.for_stage('result').units, 4096)
                    self.assertEqual(session.offer((task,)).accepted_prefix_count, 1)
                    self.assertEqual(execution.engine.capacity.usage().result_bytes, 4096)
                    self.assertEqual(execution.engine.capacity.limits.result_bytes, 8192)
                finally:
                    session.close_consumer()
                    execution.engine.close_job(job)
                    self.assertTrue(execution.close())
                self.assertEqual(sent, [])
                self.assertEqual(execution.engine.capacity.usage(), Usage())

    def test_gateway_delivers_with_small_organized_quota_in_each_mode(self):
        for mode in ('rows', 'work', 'length'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                build = self.factory(directory, mode)
                sent = []

                def configured(model, **options):
                    return build(replace(model, max_response_bytes=4096), **options)

                async def execute(task, endpoint):
                    sent.append(task.task.max_result_bytes)
                    return completion('small')

                with service(execute, max_jobs=1, result_bytes=8192,
                             execution_factory=configured) as (path, gateway, events):
                    client = Client(path, window=4)
                    try:
                        self.assertEqual(client.offer('hello'), 1)
                        client.poll()
                        self.assertEqual(client.result()['raw_output'], 'small')
                    finally:
                        client.close()
                    wait_for(lambda: any(event['event'] == 'job_drained' for event in events))
                    self.assertEqual(gateway.engine.capacity.limits.item_result_bytes, 4096)
                    self.assertEqual(gateway.engine.capacity.limits.result_bytes, 8192)
                    self.assertEqual(gateway.engine.capacity.usage(), Usage())
                self.assertEqual(sent, [4096])


if __name__ == '__main__':
    unittest.main()
