"""Owner-clock and failure boundaries; native SQL/model operations are fixtures."""

import csv
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pandas as pd

from src.experiments import native_imlane_sembench as entry


class WorkerBoundaryTests(unittest.TestCase):
    def run_fixture(self, *, query_failure=False, close_failure=False, write_failure=False, native_extract_format=False):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        args = SimpleNamespace(output=root / 'worker', build_record=root / 'build.json',
            kind='thread', threads=None, executor_count=None)
        args.build_record.write_text('{}')
        config = dict(schema=entry.SCHEMA, query_id='fixture-q1', sembench_checkout='/unused',
            data_dir='/unused', input_sha256={}, model_name='openai/fixture', tokenizer='/unused',
            generation=dict(max_tokens=8192, temperature=0, top_p=1, seed=0, stop=[],
                presence_penalty=0, frequency_penalty=0, repetition_penalty=1,
                response_format={'type': 'json_object'}), model_service={}, max_model_calls=25,
            query_timeout_s=1200, cleanup_s=120, stop_grace_s=8, request_timeout_s=1200)
        if native_extract_format:
            config.update(schema=entry.TEXT_SCHEMA, task=dict(scenario='mmqa', query='1'))
            config['generation'].pop('response_format')
        build = dict(binary_path='/unused/driver', kernel_path='/unused/kernel',
                     duckdb_library_path='/unused/libduckdb.so')
        events, clock = [], [100_000_000_000]
        answer = pd.Series(['original director'], name='director', index=[8])
        class Service:
            def __init__(self, cfg, directory, **kwargs):
                self.socket_path = root / 'fixture.sock'
                self.served_model_name = 'fixture'
                self.report = {'status': 'starting'}
                self.ready_info = dict(max_model_len=30000, engine_instances=1)
                self.closed = False
                self.config = cfg
            def __enter__(self):
                self.report['started_monotonic'] = clock[0] / 1e9
                events.append('model-start')
                clock[0] += 90_000_000_000
                events.append('model-ready')
                return self
            def __exit__(self, *_args): self.close()
            def check(self): pass
            def begin_cleanup(self, _deadline): pass
            def close(self, **_kwargs):
                if self.closed: return
                self.closed = True
                events.append('model-close')
                clock[0] += 5_000_000_000
                if close_failure: raise RuntimeError('original close failure')
                self.report['status'] = 'closed'
        def prepare(config):
            self.assertEqual(events, [])
            return (SimpleNamespace(describe=lambda: {'fixture_rows': 1, 'expected_model_calls': 25},
                        operation=SimpleNamespace(generation=entry.GenerationSpec.from_dict(
                            dict(config['generation'], response_format={'type': 'json_object'}))),
                        bind_execution=lambda: None, verify_source=lambda: None), None,
                    [{'call_id': 'source-0', 'messages_json': '[{"role":"user","content":"original"}]'}],
                    [{'call_id': 'source-0', 'messages': [{'role': 'user', 'content': 'original'}]}], [[1, 2]])
        def fresh_prepare(query):
            self.assertEqual(events, ['model-start', 'model-ready'])
            events.append('fresh-source')
            clock[0] += 2_000_000_000
            return (None,
                    [{'call_id': 'source-0', 'messages_json': '[{"role":"user","content":"original"}]'}],
                    [{'call_id': 'source-0', 'messages': [{'role': 'user', 'content': 'original'}]}])
        def query(_args, report, **kwargs):
            self.assertEqual(kwargs['query_started_ns'], 190_000_000_000)
            self.assertEqual(clock[0], 192_000_000_000)
            provider = json.loads((_args.output / 'model-provider.json').read_text())
            self.assertEqual(provider['sampling_params']['structured_outputs'], {'json_object': True})
            with (_args.output / 'input.csv').open() as stream:
                self.assertEqual(list(csv.DictReader(stream))[0]['call_id'], 'source-0')
            events.append('native-query-sink')
            clock[0] += 3_000_000_000
            report.update(full_sink_observed_monotonic_ns=clock[0], result_ready_monotonic_ns=clock[0],
                native_alive_after_sink=True, source_to_sink_seconds=(clock[0] - kwargs['query_started_ns']) / 1e9)
            events.append('post-sink-audit')
            clock[0] += 18_000_000_000
            if query_failure:
                error = ValueError('original post-sink audit failure')
                error.query_result = answer
                raise error
            return answer, dict(quality={'fixture': True}, ipc={'fixture': True}, semantic={'fixture': True})
        real_write = entry.write_private_json
        def write(path, value):
            if write_failure and Path(path).name == 'worker.json':
                raise OSError('worker persistence failure')
            real_write(path, value)
        with patch.object(entry, 'verified_build', return_value=build), \
                patch.object(entry, 'prepare_query', side_effect=prepare), \
                patch.object(entry, 'prepare', side_effect=fresh_prepare), \
                patch.object(entry, 'run_relation_query', side_effect=query), \
                patch.object(entry, 'time', SimpleNamespace(monotonic_ns=lambda: clock[0],
                    monotonic=time.monotonic, time=time.time)), \
                patch.object(entry, 'write_private_json', side_effect=write):
            error = None
            try: report = entry.run_worker(args, config, service_factory=Service)
            except BaseException as caught:
                error = caught
                report = json.loads((args.output / 'author.json').read_text())
        return report, error, events, args.output

    def test_cold_total_and_fresh_source_clock_remain_distinct(self):
        report, error, events, output = self.run_fixture()
        self.assertIsNone(error)
        self.assertEqual(events, ['model-start', 'model-ready', 'fresh-source', 'native-query-sink',
                                  'post-sink-audit', 'model-close'])
        self.assertEqual(report['model_startup_seconds'], 90)
        self.assertEqual(report['ready_source_to_sink_seconds'], 5)
        self.assertEqual(report['cold_worker_entry_to_sink_seconds'], 95)
        saved = json.loads((output / 'full-sink-observed.json').read_text())
        self.assertEqual(saved['frames']['result']['columns'], ['director'])
        self.assertEqual(saved['frames']['result']['data'], [['original director']])

    def test_original_extract_owns_json_format_even_when_common_config_is_plain(self):
        report, error, _, _ = self.run_fixture(native_extract_format=True)
        self.assertIsNone(error)
        self.assertEqual(report['generation']['response_format'], {'type': 'json_object'})

    def test_post_sink_failure_preserves_result_and_primary_error(self):
        report, error, _events, output = self.run_fixture(query_failure=True, write_failure=True)
        self.assertIsInstance(error, ValueError)
        self.assertEqual(str(error), 'original post-sink audit failure')
        self.assertIn('worker report also failed: OSError', error.__notes__)
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['ready_source_to_sink_seconds'], 5)
        self.assertTrue((output / 'full-sink-observed.json').exists())

    def test_close_failure_does_not_promote_the_completed_sink(self):
        report, error, _events, output = self.run_fixture(close_failure=True)
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual(str(error), 'original close failure')
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['cold_worker_entry_to_sink_seconds'], 95)
        self.assertTrue((output / 'full-sink-observed.json').exists())


if __name__ == '__main__':
    unittest.main()
