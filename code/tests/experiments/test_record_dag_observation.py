"""Finite offline fixtures and existing real CPU Core/IPC; never model/GPU calls."""
from copy import deepcopy
import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.experiments.record_dag_observation import summarize_capture


def call(name='a', stage='filter', sequence=0, **extra):
    return dict(query_id='q', algorithm_stage=stage, call_id=name, model_role='main', call_index=0,
        row_id='row' + str(sequence), source_position=sequence, engine_uid='engine',
        core_task_key=dict(session_id=1, sequence=sequence), parent_call_ids=[],
        status='succeeded', prompt_tokens=10, completion_tokens=2, **extra)


def native(kind, at, c):
    return dict(event='native_' + kind, monotonic_ns=at, pid=10, identities=[c])


class DAGObservationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.description = dict(schema='semloom.record_dag_observation.input.v1', kind='CPU_fixture',
            calls=[dict(path='calls.json')], events=['events.jsonl'])

    def run_capture(self, calls, events=(), *, samples=None, spans=()):
        (self.root / 'calls.json').write_text(json.dumps(calls))
        (self.root / 'events.jsonl').write_text(''.join(json.dumps(e) + '\n' for e in events))
        (self.root / 'events.status.json').write_text(json.dumps(dict(closed=True, flush_confirmed=True, dropped=0, errors=0)))
        if samples is not None:
            self.description['samples'] = ['samples.jsonl']
            (self.root / 'samples.jsonl').write_text(''.join(json.dumps(e) + '\n' for e in samples))
        return summarize_capture(self.root, self.description, cpu_spans=spans)

    def test_serial_lineage_retains_filter_occurrences_without_inventing_selection(self):
        a, b = call(), call('b', 'map', 1)
        b['parent_call_ids'] = ['a']
        r = self.run_capture([a, b], [native('open', 1, a), native('close', 3, a), native('open', 4, b), native('close', 6, b)])
        self.assertEqual([(e['source'], e['target']) for e in r['data_lineage']], [('filter', 'map')])
        f = next(n for n in r['stages'] if n['stage_id'] == 'filter')
        self.assertEqual(f['input_occurrences'], 1)
        self.assertIsNone(f['output_rows'])
        self.assertEqual(f['unavailable']['output_rows'], 'row_selection_or_relation_output_not_recorded')

    def test_q7_both_filters_read_source_and_intersect_instead_of_chaining_selectivity(self):
        summary = dict(query_id=7, stages=[dict(stage_id='filter-' + str(i), prompt_rows=3,
            decisions=[[0, True], [1, False], [2, True]], started_monotonic_ns=i*10, terminal_monotonic_ns=i*10+5) for i in range(2)])
        (self.root/'q7.json').write_text(json.dumps(summary))
        self.description['animals_summary'] = dict(path='q7.json')
        r = self.run_capture([])
        edges = {(e['source'], e['target']) for e in r['declared_edges']}
        self.assertIn(('source', 'filter-0'), edges); self.assertIn(('source', 'filter-1'), edges)
        self.assertNotIn(('filter-0', 'filter-1'), edges)
        self.assertEqual([n['input_rows'] for n in r['stages'] if n['stage_id'].startswith('filter')], [3, 3])
        self.assertTrue(all(e['evidence'].startswith('declared_') for e in r['declared_edges']))

    def test_repeated_query_and_ambiguous_engine_members_are_not_merged(self):
        a, b = call(), call('a', 'filter', 1)
        b['engine_uid'] = 'different-owner'
        r = self.run_capture([a, b], [native('open', 1, a)])
        self.assertEqual(r['observed_call_count'], 2)
        self.assertIn('event_member_unresolved', r['issues'])
        b['query_id'] = 'other'
        with self.assertRaisesRegex(ValueError, 'select one query_id'): self.run_capture([a, b])
        self.description['query_id'] = 'q'
        self.assertEqual(self.run_capture([a, b])['observed_call_count'], 1)
        a['engine_request_id'] = 'native-r'
        event = native('open', 1, dict(a, call_id='native-call-scheme'))
        event.update(engine_uid='engine', request_id='native-r')
        closed = dict(event, event='native_close', monotonic_ns=3)
        r = self.run_capture([a], [event, closed])
        self.assertEqual(len(r['native_call_aliases']), 1)
        self.assertEqual(r['stages'][0]['windows'][0]['union_ns'], 2)
        event['engine_uid'] = 'other'
        self.assertEqual(self.run_capture([a], [event])['stages'][0]['windows'], [])
        event['engine_uid'] = 'engine'
        event['identities'][0]['call_id'] = None
        r = self.run_capture([a], [event])
        self.assertEqual(r['native_call_aliases'], [])
        self.assertEqual(r['issues']['event_member_unresolved'], 1)

    def test_ready_wait_and_backpressure_do_not_invent_exact_barrier_cause(self):
        a = call()
        def event(action, at, **extra):
            return dict(event='method_ready_boundary', action=action, monotonic_ns=at,
                        session_id=1, method_row_ref='rowref', stage_index=0, **extra)
        r = self.run_capture([a], [event('method_ready', 1), event('Core_backpressure', 3),
                                  event('Core_offer_accepted', 6, key=a['core_task_key'])])
        w = r['stages'][0]['waits'][0]
        self.assertEqual((w['start_ns'], w['end_ns'], w['backpressure_events']), (1, 6, 1))
        self.assertEqual(w['reason'], 'Core_backpressure_observed_constraint_unknown')

    def test_logical_capacity_and_native_API_peak_are_not_physical_release(self):
        a, b = call(), call('b', 'filter', 1)
        events = [native('open', 1, a), native('open', 2, b), native('close', 4, a), native('close', 5, b),
                  dict(event='Core_released', monotonic_ns=6, usage=dict(result_bytes=100, active_requests=0))]
        r = self.run_capture([a, b], events)
        self.assertEqual(r['stages'][0]['windows'][0]['peak_observed_API_intervals'], 2)
        self.assertEqual(r['Core_usage_peaks']['values']['result_bytes'], 100)
        self.assertIn('not_exclusive_physical', r['Core_usage_peaks']['scope'])

    def test_missing_parent_footer_or_terminal_stays_unknown(self):
        a = call(); a['parent_call_ids'] = ['missing']
        self.run_capture([a], [native('open', 1, a)])
        (self.root/'events.status.json').unlink()
        r = summarize_capture(self.root, self.description)
        self.assertIsNone(r['capture_complete'])
        self.assertEqual(r['unresolved_native_count'], 1)
        self.assertIn('parent_lineage_unresolved', r['issues'])
        self.assertEqual(r['sink_footer_missing_or_unqualified'], ['events.jsonl'])

    def test_repeated_receipts_and_notifications_are_not_completed_members(self):
        a = call()
        r = self.run_capture([a, deepcopy(a)], [dict(event='driver_batch_future_done', monotonic_ns=i) for i in range(4)])
        self.assertEqual(r['observed_call_count'], 1)
        self.assertEqual(r['stages'][0]['statuses'], {'succeeded': 1})
        self.assertEqual(r['stages'][0]['windows'], [])
        conflicting = dict(a, status='unknown', completion_tokens=99)
        r = self.run_capture([a, conflicting])
        self.assertEqual(r['stages'][0]['statuses'], {'conflicting_receipt': 1})
        self.assertIsNone(r['stages'][0]['output_tokens'])
        self.assertEqual(r['issues']['conflicting_call_receipts'], 1)

    def test_distinct_clock_domains_do_not_create_overlap_or_transfer_latency(self):
        a, b = call(), call('b', 'map', 1)
        self.run_capture([a, b], [native('open', 10, a), native('close', 20, a)])
        (self.root/'other.jsonl').write_text('\n'.join(json.dumps(e) for e in [native('open', 11, b), native('close', 19, b)]))
        self.description['events'].append('other.jsonl')
        r = summarize_capture(self.root, self.description)
        self.assertEqual(r['overlaps'][0]['status'], 'unknown:clock_domains_unjoined')
        self.assertEqual(r['overlaps'][0]['by_domain'], [])

    def test_overlapping_windows_and_shared_CPU_span_are_not_added_per_member(self):
        a, b = call(), call('b', 'map', 1)
        events = [native('open', 1, a), native('close', 5, a), native('open', 2, b), native('close', 7, b)]
        spans = [dict(span_id='s', status='observed', members=[a, b], operation='shared_prepare', wall_ns=8, exclusive_thread_CPU_ns=5)]
        r = self.run_capture([a, b], events, spans=spans)
        self.assertEqual(r['overlaps'][0]['by_domain'][0]['overlap_ns'], 3)
        self.assertEqual(len(r['physical_CPU_spans']), 1)
        self.assertTrue(all(n['cpu'][0]['shared_stage_members'] == ['filter', 'map'] for n in r['stages']))
        self.assertTrue(all(n['cpu'][0]['attribution'] == 'shared_span_not_per_member_cost' for n in r['stages']))

    def test_unbound_GPU_and_missing_CPU_metrics_are_not_idle_zero(self):
        a = call(); self.description['clock_bindings'] = {p:dict(domain='host', basis='explicit fixture binding') for p in ['events.jsonl', 'samples.jsonl']}
        samples = [dict(sample_start_monotonic_ns=2, sample_end_monotonic_ns=3,
            values=dict(gpu_utilization_pct=0, host_cpu_busy_cores=0),
            missing=dict(host_cpu_busy_cores='cpu_percent_baseline_priming'), sources=dict(gpu=dict(physical_gpu_uuids=['GPU-real'],status='snapshot:nvml')))]
        r = self.run_capture([a], [native('open', 1, a), native('close', 4, a)], samples=samples)
        fields = r['stages'][0]['resources'][0]['values']
        self.assertIsNone(fields['gpu_utilization_pct']['minimum'])
        self.assertIsNone(fields['host_cpu_busy_cores']['minimum'])
        self.description['gpu_uuids'] = ['GPU-real']
        self.assertEqual(self.run_capture([a], [native('open', 1, a), native('close', 4, a)], samples=samples)['stages'][0]['resources'][0]['values']['gpu_utilization_pct']['minimum'], 0)

    def test_cancelled_unknown_prefix_and_path_escape_are_preserved(self):
        a = call(); a['status'] = 'unknown'
        r = self.run_capture([a], [dict(event='method_ready_boundary', monotonic_ns=1,
            session_id=1, method_row_ref='r', stage_index=0, action='method_ready')])
        self.assertEqual(r['stages'][0]['statuses'], {'unknown': 1})
        self.assertEqual(r['unresolved_ready_count'], 1)
        self.description['events'] = ['../escape']
        with self.assertRaises(ValueError): summarize_capture(self.root, self.description)

    def test_actual_Core_ready_hook_on_off_keeps_results_calls_and_cleanup(self):
        from src.semantic_methods.driver import MethodDriver
        from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
        from tests.execution_provider.test_lotus_record_local_progression import ProgressionTests, ThreadedRuntime, query_fixture, fixture
        helper = ProgressionTests(); helper.setUp(); self.addCleanup(helper.doCleanups)
        chain, rows = fixture.chain_for('FM'), fixture.source_rows(size=2)
        helper.fixture.config_for(chain, rows)
        original = MethodDriver._ready_event
        normalized, captured = [], []
        observer_path = Path(__file__).resolve().parents[2]/'scripts/profiling/record_observation/record_observer.py'
        for mode in ('off', 'on'):
            directory = self.root/mode; directory.mkdir()
            (directory/'config.json').write_text(json.dumps(dict(observation=dict(max_events_per_process=1000, max_event_bytes=65536))))
            with patch.dict(os.environ, dict(SEM_RECORD_OBSERVATION=mode, SEM_RECORD_OBSERVER_ROOT=str(directory))):
                spec = importlib.util.spec_from_file_location('dag_fixture_observer_' + mode, observer_path)
                observer = importlib.util.module_from_spec(spec); spec.loader.exec_module(observer)
                observer.install_ready_hook()
                try:
                    with ThreadedRuntime([]), patch.object(AsyncCoreModelActor, '_process_type', return_value=query_fixture.Process):
                        owner = helper.owner()
                        try:
                            stream = owner.evaluate(iter(rows), chain, expected=rows, query_id=chain.query_id)
                            records = list(stream)
                            normalized.append(fixture.normalized(dict(records=records, calls=owner.calls, columns=stream.columns)))
                            self.assertFalse(any(owner.last_close_report['core_usage'].values()))
                            self.assertEqual(owner.last_close_report['status'], 'closed')
                        finally: owner.close(timeout=3)
                finally:
                    MethodDriver._ready_event = original
                    observer.close_events()
            captured.extend(json.loads(line) for path in (directory/'events').glob('*.jsonl') for line in path.read_text().splitlines())
            if mode == 'on':
                (directory/'calls.json').write_text(json.dumps(owner.calls))
                observed = summarize_capture(directory, dict(schema='semloom.record_dag_observation.input.v1',
                    kind='actual_CPU_Core_fixture', calls=[dict(path='calls.json')],
                    events=[str(p.relative_to(directory)) for p in (directory/'events').glob('*.jsonl')]))
                self.assertEqual(sum(len(n['waits']) for n in observed['stages']), len(owner.calls))
                self.assertEqual(observed['unresolved_ready_count'], 0)
                self.assertNotIn('ready_offer_join_unresolved', observed['issues'])
        self.assertEqual(normalized[0], normalized[1])
        self.assertTrue(any(e['event'] == 'method_ready_boundary' and e['action'] == 'Core_offer_accepted' for e in captured))

    def test_CLI_verifies_originals_writes_outside_and_never_overwrites(self):
        self.run_capture([call()])
        path = Path(__file__).resolve().parents[2]/'scripts/profiling/record_observation/workflow.py'
        spec = importlib.util.spec_from_file_location('dag_workflow_test', path)
        workflow = importlib.util.module_from_spec(spec); spec.loader.exec_module(workflow)
        manifest = self.root/'ORIGINALS.json'; workflow.originals_manifest(self.root, manifest)
        with tempfile.TemporaryDirectory() as directory:
            desc = Path(directory)/'input.json'; desc.write_text(json.dumps(self.description))
            output = Path(directory)/'summary.json'
            args = ['summarize','--root',str(self.root),'--manifest',str(manifest),'--input',str(desc),'--output',str(output)]
            with contextlib.redirect_stdout(io.StringIO()): workflow.main(args)
            report = json.loads(output.read_text())
            self.assertEqual(report['captures'][0]['observed_call_count'],1)
            self.assertEqual(report['analysis_inputs'], [workflow.file_identity(desc)])
            with self.assertRaises(FileExistsError): workflow.main(args)
            (self.root/'calls.json').write_text('[]')
            with self.assertRaisesRegex(ValueError,'original bytes changed'): workflow.main(args)


if __name__ == '__main__': unittest.main()
