"""Native adapters release an iterator before executing source or model work."""
import sys
import json
from pathlib import Path
import tempfile
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from src.baselines.text.frameworks.lotus_pg import open_rows as lotus_rows
from src.baselines.text.frameworks.ray_data_pg_http import open_rows as ray_rows,RaySqlHttpConfig


class NativeEntryTests(unittest.TestCase):
    def test_pg_preparation_records_wait_even_when_gateway_does_not_become_ready(self):
        from src.experiments.postgresql import query_execution as execution
        for failed in (False,True):
            with self.subTest(failed=failed),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);log=root/'pg.log';log.write_text('before query\n');tick=[0]
                def command(*args):
                    tick[0]+=1_000_000_000
                    return [],root/'g.sock'
                def plan(*args):tick[0]+=2_000_000_000;return 'fixture SQL'
                @contextmanager
                def spawn(*args):
                    tick[0]+=3_000_000_000
                    yield SimpleNamespace(pid=1,returncode=0)
                def wait(*args):
                    tick[0]+=4_000_000_000
                    if failed:raise RuntimeError('fixture gateway startup failure')
                sampler=SimpleNamespace(summary=lambda:{})
                errors=SimpleNamespace(capture=lambda phase:nullcontext(),attempt=lambda phase,f:f())
                with patch.object(execution.time,'monotonic_ns',side_effect=lambda:tick[0]), \
                        patch.object(execution,'pg_gateway_command',side_effect=command), \
                        patch.object(execution,'prepare_pg_query',side_effect=plan), \
                        patch.object(execution,'owned_child_process',side_effect=spawn), \
                        patch.object(execution,'wait_for_path',side_effect=wait), \
                        patch.object(execution,'ProcessSampler',return_value=nullcontext(sampler)), \
                        patch.object(execution,'record_pg_query',return_value={'status':'completed'}) as query:
                    call=lambda:execution.run_pg(SimpleNamespace(query_timeout_s=1),SimpleNamespace(max_rows=1),
                        None,SimpleNamespace(info=SimpleNamespace(backend_pid=2)),log,None,None,root,errors)
                    if failed:
                        with self.assertRaisesRegex(RuntimeError,'gateway startup failure'):call()
                        query.assert_not_called()
                    else:
                        _,resources=call()
                        query.assert_called_once()
                        self.assertEqual(resources['pg_preparation']['status'],'completed')
                saved=json.loads((root/'gateway-preparation.json').read_text())
                self.assertEqual(saved['status'],'failed' if failed else 'completed')
                self.assertEqual([saved[name] for name in ('gateway_command_seconds','pg_plan_seconds',
                    'gateway_spawn_seconds','gateway_ready_wait_seconds','total_seconds')],[1,2,3,4,10])

    def test_lotus_query_failure_occurs_in_iteration(self):
        from contextlib import nullcontext
        original=Mock(side_effect=ValueError('native query failed'))
        with patch('src.baselines.text.frameworks.lotus_pg.observe_filter_rows',return_value=nullcontext()),\
             patch('src.baselines.text.frameworks.lotus_pg.run_original_lotus_query',original):
            with lotus_rows(None,None,None,3,Mock(),Mock()) as rows:
                original.assert_not_called()
                with self.assertRaisesRegex(ValueError,'native query failed'):next(rows)
                original.assert_called_once()

    def test_ray_sql_plan_failure_occurs_in_iteration(self):
        read=Mock(side_effect=ValueError('SQL support probe failed'))
        ray=SimpleNamespace(__version__='2.56.1',is_initialized=lambda:True,data=SimpleNamespace(read_sql=read))
        llm=SimpleNamespace(HttpRequestProcessorConfig=Mock(),build_processor=Mock())
        inputs=SimpleNamespace(select_sql=lambda **kwargs:('SELECT fixture',[]))
        with patch.dict(sys.modules,{'ray':ray,'ray.data':ray.data,'ray.data.llm':llm}):
            with ray_rows(inputs,None,RaySqlHttpConfig(1,1,1,1),None,None) as rows:
                read.assert_not_called()
                with self.assertRaisesRegex(ValueError,'SQL support probe'):next(rows)
                read.assert_called_once()
