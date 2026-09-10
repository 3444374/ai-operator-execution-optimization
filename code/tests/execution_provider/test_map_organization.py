"""Complete request token work, finite candidates, actual dispatch order and rejection."""
from dataclasses import replace
import asyncio
import json
from pathlib import Path
import tempfile
import threading
import unittest

from src.execution_provider.adapters.map_organization import (
    MapOrganizationConfig, organization_factory, tokenizer_fingerprint,
)
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.completion import CompletionRequest
from src.scheduling.core.session_contract import SessionSpec
from src.scheduling.organization.session_window import WorkWindowOrganizer
from tests.scheduling.test_organized_session import described, organized
from tests.execution_provider.test_multisession_gateway import service, Client, completion, wait_for
from src.execution_provider.wire import v6


class CountingTokenizer:
    def apply_chat_template(self, messages, **kwargs):
        self.messages, self.options = messages, kwargs
        return list(range(min(sum(len(m['content']) for m in messages) + 2, kwargs['max_length'])))


class MapOrganizationTests(unittest.TestCase):
    def test_context_rejection_and_inflight_disconnect_settle_before_next_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, 'tokenizer_config.json').write_text('{}')
            config=MapOrganizationConfig('length',4,2,30,64,'model','r1','s1',tmp,
                                         tokenizer_fingerprint(tmp),64)
            started=threading.Event();release=threading.Event();submitted=[]

            async def execute(task, endpoint):
                submitted.append(task.key.sequence);started.set()
                while not release.is_set():
                    await asyncio.sleep(.001)
                return completion('ok')

            factory=organization_factory(config,tokenizer=CountingTokenizer())
            with service(execute,max_jobs=1,execution_factory=factory) as (path,gateway,events):
                client=Client(path,window=4)
                client.send(v6.build_task_message(client.plan,sequence=0,input_value='x'*80))
                self.assertEqual(client.result()['type'],'error')
                client.close()
                wait_for(lambda: any(e['event']=='job_drained' for e in events))
                self.assertEqual(submitted,[])
                client=Client(path,window=4)
                self.assertEqual(client.offer('short'),1);client.poll()
                self.assertTrue(started.wait(2));client.close();release.set()
                wait_for(lambda: len([e for e in events if e['event']=='job_drained'])==2)
                self.assertEqual(gateway.engine.capacity.usage().held_tasks,0)
                client=Client(path,window=4)
                self.assertEqual(client.offer('new'),1);client.poll()
                self.assertEqual(client.result()['raw_output'],'ok');client.close()
                wait_for(lambda: len([e for e in events if e['event']=='job_drained'])==3)
                self.assertEqual(submitted,[0,0])

    def test_rows_and_work_group_differ_but_input_order_can_be_identical(self):
        histories = []
        for target in (None, 3):
            engine, session, backend, _ = organized(active_requests=4)
            engine.policies = replace(engine.policies, organize=WorkWindowOrganizer(3, target, candidate_window_rows=3))
            session.offer([described(i, w) for i, w in enumerate((2, 1, 3, 1))])
            session.advance(4)
            keys = list(backend.pending)
            histories.append(([k.sequence for k in keys], [backend.pending[k][1].member.size for k in keys]))
        self.assertEqual(histories[0][0], [0, 1, 2, 3])
        self.assertEqual(histories[0][0], histories[1][0])
        self.assertNotEqual(histories[0][1], histories[1][1])

    def test_length_sort_cannot_reveal_short_task_beyond_candidate_window(self):
        engine, session, backend, _ = organized()
        engine.policies = replace(engine.policies, organize=WorkWindowOrganizer(2, 20, True, 2))
        session.offer([described(i, w) for i, w in enumerate((8, 3, 1, 2))])
        session.advance(4)
        self.assertEqual([k.sequence for k in backend.pending], [1, 0, 2, 3])

    def test_preparation_preserves_request_and_fits_typed_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, 'tokenizer_config.json').write_text('{}')
            config = MapOrganizationConfig('work', 4, 2, 32, 128, 'model', 'r1', 's1', tmp,
                                           tokenizer_fingerprint(tmp), 64)
            tokenizer = CountingTokenizer()
            events = []
            execution = organization_factory(config, tokenizer=tokenizer)(
                FixedModelConfig('http://localhost/v1/chat/completions', 'model', 100),
                max_tasks=4, max_active_requests=2, observer=events.append,
            )
            request = CompletionRequest('a'*64, 'model', ({'role':'user','content':'hello'},),
                                        {'max_tokens':8}, protocol_version=6)
            try:
                task = execution.prepare_task(request, 0)
                self.assertEqual(task.estimated_work, 15)
                self.assertEqual(task.info.work.primary.unit, 'tokens')
                self.assertEqual(json.loads(task.payload)['messages'], list(request.canonical_messages))
                job, session, _ = execution.open_job('test', SessionSpec('test', 'map', 'default', work_unit='tokens'))
                self.assertEqual(session.offer((task,)).accepted_prefix_count, 1)
                session.cancel()
                execution.engine.advance()
                execution.engine.close_job(job)
                self.assertEqual(execution.engine.capacity.usage().held_tasks, 0)
                with self.assertRaisesRegex(ValueError, 'exceeds model context'):
                    execution.prepare_task(replace(request, canonical_messages=({'role':'user','content':'x'*80},)), 1)
                with self.assertRaisesRegex(ValueError, 'Map v6'):
                    execution.prepare_task(replace(request, protocol_version=3), 1)
                self.assertEqual(sum(e['event']=='map_work_described' for e in events), 1)
            finally:
                execution.close()
            Path(tmp, 'tokenizer_config.json').write_text('{"changed":true}')
            with self.assertRaisesRegex(ValueError, 'declared identity'):
                organization_factory(config, tokenizer=tokenizer)


if __name__ == '__main__':
    unittest.main()
