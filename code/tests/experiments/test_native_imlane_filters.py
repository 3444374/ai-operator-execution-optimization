"""Single-query Filter configuration, identity, worker failure and clock seams."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pandas as pd
import pyarrow as pa

from integrations.imlane_duckdb import build, mmqa_filter_udf as kernel
from src.experiments import native_imlane_filters as entry
from tests.baselines.text.test_imlane_sembench_udf import PreparedDriverBuildTests


def config():
    return json.loads((Path(__file__).resolve().parents[2] /
        'configs/sembench_imlane_mmqa_filter.example.json').read_text())


class FilterBindingTests(unittest.TestCase):
    def test_only_explicit_single_filter_query_and25_budget_are_allowed(self):
        value = config()
        for query in entry.QUERIES:
            value['query_id'] = query
            self.assertEqual(entry.configuration(value)[0]['query_id'], query)
        for query in ('1', '4', '3b-3g', '6a-6c'):
            value['query_id'] = query
            with self.assertRaises(ValueError): entry.configuration(value)
        value['query_id'], value['max_model_calls'] = '6a', 75
        with self.assertRaises(ValueError): entry.configuration(value)

    def test_real_arrow_uses_original_parser_and_occurrence_order(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'messages.json'
            rows = [dict(occurrence_id=str(i), title='same airline', text=str(i),
                         messages=[{'role': 'user', 'content': str(i)}]) for i in range(3)]
            path.write_text(json.dumps({'rows': rows}))
            table = pa.table({'c0': ['2', '0', '1'], 'c1': ['same airline'] * 3, 'c2': ['2', '0', '1']})
            with patch.dict(os.environ, {kernel.CONFIG_ENV: str(path)}), \
                    patch.object(kernel.model_provider, 'infer_batch', return_value=['False then True', 'False', 'invalid']) as infer:
                result = kernel.infer_mmqa(table)
                self.assertEqual(result.column(0).to_pylist(), [1, 0, 1])
                self.assertEqual(infer.call_args.args[0], [rows[i]['messages'] for i in (2, 0, 1)])
                self.assertEqual(result.schema.field(0).type, pa.int8())
                self.assertEqual(kernel.infer_mmqa(table.slice(0, 0)).num_rows, 0)
                self.assertEqual(infer.call_count, 1)
            bad = table.set_column(1, 'c1', pa.array(['wrong'] * 3))
            with patch.dict(os.environ, {kernel.CONFIG_ENV: str(path)}), patch.object(kernel.model_provider, 'infer_batch') as infer:
                with self.assertRaises(ValueError): kernel.infer_mmqa(bad)
                infer.assert_not_called()

    def test_provider_failure_is_not_retried_or_replaced(self):
        with patch.object(kernel, 'prepared', return_value={'a':dict(title='a', text='b', messages=[])}), \
                patch.dict(os.environ, {kernel.CONFIG_ENV:'fixture'}), \
                patch.object(kernel.model_provider, 'infer_batch', side_effect=RuntimeError('original failure')) as infer:
            with self.assertRaisesRegex(RuntimeError, 'original failure'):
                kernel.infer_mmqa(pa.table({'c0':['a'],'c1':['a'],'c2':['b']}))
            self.assertEqual(infer.call_count, 1)


class FilterDriverTests(PreparedDriverBuildTests):
    # Reuse the existing link-command fixture; no real compiler or DSO is used.
    def test_original_filter_links_without_varchar_patch_or_rebuild(self):
        digest = hashlib.sha256((self.duckdb_source / build.CONVERTER_HEADER).read_bytes()).hexdigest()
        with patch.object(build, 'ORIGINAL_CONVERTER_SHA256', digest), \
                patch.object(build.subprocess, 'run', side_effect=self.compiler_fixture) as run:
            self.assertEqual(build.driver(self.args, native_filter=True), 0)
        record = json.loads(self.args.output.with_suffix('.build.json').read_text())
        self.assertEqual(record['variant'], 'original-author')
        self.assertEqual(record['schema'], 'semloom.imlane.filter_driver_build.v1')
        self.assertIn(str(build.OWNED / 'mmqa_filter.cpp'), run.call_args_list[0].args[0])
        self.assertEqual(len(run.call_args_list), 2)
        self.assertFalse(record['DSO_rebuilt'])


class FilterWorkerTests(unittest.TestCase):
    def exercise(self, *, audit_failure=False, close_failure=False, persistence_failure=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); clock, events = [100_000_000_000], []
            args = SimpleNamespace(output=root/'worker', build_record=root/'build.json', kind='thread', threads=None, executor_count=None)
            args.build_record.write_text('{}')
            native_build = dict(binary_path='/unused/driver', kernel_path='/unused/kernel', duckdb_library_path='/unused/lib.so')
            result = pd.DataFrame({'Airlines':['original airline']})
            class Service:
                def __init__(self, *_args, **_kwargs):
                    self.socket_path=root/'fixture.sock'; self.served_model_name='fixture'
                    self.ready_info=dict(max_model_len=32768); self.report={}; self.closed=False
                def __enter__(self):
                    self.report['started_monotonic']=clock[0]/1e9; events.append('start')
                    clock[0]+=90_000_000_000; events.append('ready'); return self
                def __exit__(self, *_): self.close()
                def check(self): pass
                def begin_cleanup(self, _): pass
                def close(self, **_):
                    if self.closed: return
                    self.closed=True; events.append('close'); clock[0]+=5_000_000_000
                    if close_failure: raise RuntimeError('close failed')
            def prepare(_):
                self.assertEqual(events,['start','ready']); events.append('source'); clock[0]+=2_000_000_000
                rows=[dict(occurrence_id='r',source_position=0,title='original airline',text='raw',messages=[])]
                return SimpleNamespace(describe=lambda:{}), SimpleNamespace(columns=['Airlines']), [], [], [[1]], rows
            def run(_args, report, **kwargs):
                self.assertEqual(kwargs['query_started_ns'],190_000_000_000)
                clock[0]+=3_000_000_000; events.append('sink')
                report.update(full_sink_observed_monotonic_ns=clock[0],result_ready_monotonic_ns=clock[0],
                              native_alive_after_sink=True,source_to_sink_seconds=5)
                clock[0]+=18_000_000_000; events.append('audit')
                if audit_failure:
                    error=ValueError('audit failed');error.query_result=result;raise error
                return result,dict(quality={},ipc={},semantic={})
            real_persist = entry.persist_relation_observation
            def persist(*args, **kwargs):
                if persistence_failure: raise OSError('relation persistence failed')
                return real_persist(*args, **kwargs)
            with patch.object(entry,'verified_build',return_value=native_build), \
                    patch.object(entry,'prepare_query',side_effect=prepare), \
                    patch.object(entry,'run_relation_query',side_effect=run), \
                    patch.object(entry,'persist_relation_observation',side_effect=persist), \
                    patch.object(entry,'time',SimpleNamespace(monotonic_ns=lambda:clock[0],monotonic=time.monotonic,time=time.time)):
                error=None
                try: report=entry.run_worker(args,config(),service_factory=Service)
                except (RuntimeError,ValueError,OSError) as caught:
                    error=caught;report=json.loads((args.output/'worker.json').read_text())
            if not persistence_failure:
                self.assertTrue((args.output/'full-sink-observed.json').exists())
            return report,error,events

    def test_ready_source_and_startup_inclusive_cold_remain_distinct(self):
        report,error,events=self.exercise()
        self.assertIsNone(error);self.assertEqual(report['model_startup_seconds'],90)
        self.assertEqual(report['ready_source_to_sink_seconds'],5)
        self.assertEqual(report['cold_worker_entry_to_sink_seconds'],95)
        self.assertEqual(events,['start','ready','source','sink','audit','close'])

    def test_post_sink_audit_failure_preserves_sink_but_not_success(self):
        report,error,_=self.exercise(audit_failure=True)
        self.assertIsInstance(error,ValueError);self.assertEqual(report['status'],'failed')
        self.assertEqual(report['source_to_sink_seconds'],5)

    def test_close_failure_preserves_sink_but_not_success(self):
        report,error,_=self.exercise(close_failure=True)
        self.assertIsInstance(error,RuntimeError);self.assertEqual(report['status'],'failed')
        self.assertEqual(report['cold_worker_entry_to_sink_seconds'],95)

    def test_relation_write_failure_still_writes_failed_worker_receipt(self):
        report,error,_=self.exercise(persistence_failure=True)
        self.assertIsInstance(error,OSError);self.assertEqual(report['status'],'failed')
        self.assertEqual(report['source_to_sink_seconds'],5)


if __name__ == '__main__': unittest.main()
