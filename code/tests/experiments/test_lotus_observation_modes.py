"""Deterministic observation and proxy checks; no LOTUS SDK or models."""
from contextlib import contextmanager
import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
from urllib.request import Request, urlopen
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.full_response import FullModelResponse
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.shared_request_budget import SharedClaimedUnit
from src.experiments.postgresql import supplier_adapter_query as supplier
from src.experiments.postgresql.native_adapter_metrics import compare_methods, summarize_calls
from src.experiments.postgresql.persistent_native_adapter_query import PersistentAdapterGroup
from src.experiments.postgresql.sembench_query_cli import load_config
from tests.experiments.test_native_adapter_query import fixture_server


class Writer:
    def __init__(self):
        self.events=[]

    def record(self, event):
        self.events.append(event)


def fixture_prepare(lm, messages, kwargs):
    """A declared JSON-only substitute, not LiteLLM's transformation."""
    return SimpleNamespace(payload=json.dumps(dict(model='fixture', messages=messages, **kwargs),
        ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode())


def observations():
    writer=Writer()
    raw=io.StringIO()
    return supplier.MethodObservations(writer, raw, 'fixture'), writer, raw


class LotusObservationModeTests(unittest.TestCase):
    def test_default_and_explicit_qualification_preserve_native_arguments_and_expected_calls(self):
        for options in ({}, dict(observation_mode='qualification')):
            observed, writer, raw=observations()
            data=[([dict(role='user', content='same')], None)]*2
            kwargs=dict(max_tokens=16)
            result=[SimpleNamespace(model_dump=lambda:dict(value='ok')) for _ in data]
            def original(actual, effective, show, description):
                self.assertIs(actual, data)
                self.assertIs(effective, kwargs)
                self.assertEqual((show, description), (False, 'fixture'))
                self.assertEqual(len(observed.calls), 2)
                return result
            lm=SimpleNamespace(_process_uncached_messages=original)
            observer=supplier._LotusBatchObservation(lm, [dict(row_id='a'), dict(row_id='b')], observed, **options)
            with patch('src.semantic_methods.lotus.sdk.prepare_call', side_effect=fixture_prepare) as prepare:
                self.assertIs(observer(lm, data, kwargs, False, 'fixture'), result)
                self.assertEqual(prepare.call_count, 2)
            self.assertEqual(len(raw.getvalue().splitlines()), 2)
            self.assertEqual(sum(e['event']=='lotus_http_call_prepared' for e in writer.events), 2)

    def test_performance_keeps_duplicate_input_occurrences_and_batch_receipts_without_preparation(self):
        observed, writer, raw=observations()
        result=[SimpleNamespace(model_dump=lambda:dict(value='ok'))]*2
        lm=SimpleNamespace(_process_uncached_messages=lambda *_:result)
        observer=supplier._LotusBatchObservation(lm, [dict(row_id='a'), dict(row_id='b')], observed,
            observation_mode='performance')
        with patch('src.semantic_methods.lotus.sdk.prepare_call', side_effect=AssertionError('unexpected construction')):
            self.assertIs(observer(lm, [([], None)]*2, {}, False, 'fixture'), result)
        calls=list(observed.calls.values())
        self.assertEqual([c['row_id'] for c in calls], ['a', 'b'])
        self.assertTrue(all(c['request_values_sha256'] is None for c in calls))
        self.assertFalse(any(e['event']=='lotus_http_call_prepared' for e in writer.events))
        receipts=[e for e in writer.events if e['event']=='caller_response']
        self.assertEqual(receipts[0]['monotonic_ns'], receipts[1]['monotonic_ns'])
        self.assertEqual(summarize_calls(writer.events, calls)['request_e2e']['count'], 2)
        self.assertEqual(raw.getvalue(), '')
        with self.assertRaisesRegex(ValueError, 'unavailable'):
            compare_methods(calls, calls)

    def test_both_modes_observe_replacement_execution_payloads_without_extra_preparation(self):
        for mode in supplier.LOTUS_OBSERVATION_MODES:
            observed, writer, raw=observations()
            class Executor:
                def __call__(self, lm, data, kwargs, show, description):
                    for index, item in enumerate(data):
                        self.on_prepared(index, fixture_prepare(lm, item[0], kwargs))
                        if self.on_response is not None:
                            self.on_response(index, FullModelResponse(200, (), b'{}'))
                    return [SimpleNamespace(model_dump=lambda:dict(value='ok'))]*len(data)
            lm=SimpleNamespace(_process_uncached_messages=lambda *_:self.fail('original pool used'))
            observer=supplier._LotusBatchObservation(lm, [dict(row_id='a'), dict(row_id='b')], observed,
                Executor(), observation_mode=mode)
            with patch('src.semantic_methods.lotus.sdk.prepare_call', side_effect=AssertionError('duplicate preparation')):
                observer(lm, [([], None)]*2, {}, False, 'fixture')
            self.assertTrue(all(c['request_values_sha256'] for c in observed.calls.values()))
            self.assertEqual(sum(e['scope']=='HTTP encoding used by execution' for e in writer.events
                if e['event']=='lotus_http_call_prepared'), 2)
            self.assertEqual(len(raw.getvalue().splitlines()), 4 if mode=='qualification' else 0)

    def test_native_errors_cancellation_and_partial_results_keep_the_original_behavior(self):
        for mode in supplier.LOTUS_OBSERVATION_MODES:
            for error in (ValueError('native failure'), InterruptedError('native cancelled'), TimeoutError('native timeout')):
                observed, _, _=observations()
                def original(*_):
                    raise error
                lm=SimpleNamespace(_process_uncached_messages=original)
                observer=supplier._LotusBatchObservation(lm, [dict(row_id='a')], observed, observation_mode=mode)
                with patch('src.semantic_methods.lotus.sdk.prepare_call', side_effect=fixture_prepare):
                    with self.assertRaises(type(error)) as caught:
                        observer(lm, [([], None)], {}, False, 'fixture')
                self.assertIs(caught.exception, error)
            for result, message in (([], 'incomplete response list'), ([object()], 'failed complete SDK response')):
                observed, _, _=observations()
                lm=SimpleNamespace(_process_uncached_messages=lambda *_:result)
                observer=supplier._LotusBatchObservation(lm, [dict(row_id='a')], observed, observation_mode=mode)
                with patch('src.semantic_methods.lotus.sdk.prepare_call', side_effect=fixture_prepare):
                    with self.assertRaisesRegex(ValueError, message):
                        observer(lm, [([], None)], {}, False, 'fixture')

    def test_native_exception_values_remain_values_and_source_count_fails_before_native(self):
        for mode in supplier.LOTUS_OBSERVATION_MODES:
            observed, writer, raw=observations()
            error=ValueError('native error value')
            lm=SimpleNamespace(_process_uncached_messages=lambda *_:[error])
            observer=supplier._LotusBatchObservation(lm, [dict(row_id='a')], observed, observation_mode=mode)
            with patch('src.semantic_methods.lotus.sdk.prepare_call', side_effect=fixture_prepare):
                self.assertIs(observer(lm, [([], None)], {}, False, 'fixture')[0], error)
            self.assertEqual([e['error_type'] for e in writer.events if e['event']=='caller_error'], ['ValueError'])
            self.assertEqual(raw.getvalue(), '')
            lm._process_uncached_messages=lambda *_:self.fail('invalid source reached native')
            observer=supplier._LotusBatchObservation(lm, [dict(row_id='a')], observed, observation_mode=mode)
            with self.assertRaisesRegex(ValueError, 'differs from the source'):
                observer(lm, [], {}, False, 'fixture')

    def test_unknown_modes_and_non_lotus_performance_fail_before_budget_or_runtime(self):
        lm=SimpleNamespace(_process_uncached_messages=lambda *_:None)
        for mode in ('unknown', None, True):
            with self.assertRaisesRegex(ValueError, 'observation mode'):
                supplier._LotusBatchObservation(lm, [], None, observation_mode=mode)
        with self.assertRaisesRegex(ValueError, 'requires a LOTUS arm'):
            PersistentAdapterGroup(('fixed-map-native-daft',),
                plan=None, model=None, ledger=None, root='unused', lotus_observation_mode='performance')

    def test_movie_config_keeps_default_and_requires_explicit_known_observation_mode(self):
        example=Path(__file__).resolve().parents[3]/'deploy/autodl/sembench_lotus_queries.example.json'
        config=load_config(example)
        self.assertNotIn('lotus_observation_mode', config)
        with tempfile.TemporaryDirectory() as root:
            target=Path(root)/'config.json'
            for mode in supplier.LOTUS_OBSERVATION_MODES:
                target.write_text(json.dumps(dict(config, lotus_observation_mode=mode)))
                self.assertEqual(load_config(target)['lotus_observation_mode'], mode)
            target.write_text(json.dumps(dict(config, lotus_observation_mode='unknown')))
            with self.assertRaisesRegex(ValueError, 'observation mode'):
                load_config(target)

    def test_persistent_group_records_mode_and_forwards_it_without_inferring_from_phase(self):
        from src.experiments.postgresql import persistent_native_adapter_query as persistent
        for mode in supplier.LOTUS_OBSERVATION_MODES:
            with tempfile.TemporaryDirectory() as root:
                root=Path(root)
                group=PersistentAdapterGroup(('lotus-adapted-native',),
                    plan=SemanticMapPlan('Return ok.', 'fixture', 16),
                    model=FixedModelConfig('http://127.0.0.1:1/v1/chat/completions', 'fixture', 1000),
                    ledger=None, root=root/'group', lotus_observation_mode=mode)
                group.deadline=time.monotonic()+1800
                owner=SimpleNamespace(physical=None, queries=0, lifecycle=lambda:dict(fixture=True))
                group.owners={'lotus-adapted-native':owner}
                query=root/'query'
                query.mkdir()
                def run(arm, **arguments):
                    self.assertEqual(arguments['lotus_observation_mode'], mode)
                    value=dict(status='passed', rows=2, actual_posts=2, full_query_seconds=1,
                        lotus_observation_mode=mode, query_preparation_started_ns=1,
                        execution=dict(t_submit_ns=2, t_query_terminal_ns=3, ready_query_timing_scope='fixture'))
                    (query/'summary.json').write_text(json.dumps(value))
                    return value
                with patch.object(persistent, 'run_supplier_query', run):
                    group.run('lotus-adapted-native', unit_id='query', root=query,
                        load_source=lambda:iter(()), phase='measurement')
                self.assertEqual(json.loads((query/'persistent-query.json').read_text())['lotus_observation_mode'], mode)


    def test_failed_persistent_sidecar_records_selected_mode_and_preserves_primary_exception(self):
        from src.experiments.postgresql import persistent_native_adapter_query as persistent
        arms=('lotus-adapted-native', 'duckdb-adapted-native', 'fixed-map-native-daft')
        for mode in supplier.LOTUS_OBSERVATION_MODES:
            for arm in arms:
                with self.subTest(mode=mode, arm=arm), tempfile.TemporaryDirectory() as root:
                    root=Path(root)
                    group=PersistentAdapterGroup(arms, plan=SemanticMapPlan('Return ok.', 'fixture', 16),
                        model=FixedModelConfig('http://127.0.0.1:1/v1/chat/completions', 'fixture', 1000),
                        ledger=None, root=root/'group', lotus_observation_mode=mode)
                    group.deadline=time.monotonic()+1800
                    owner=SimpleNamespace(physical=None, queries=0, lifecycle=lambda:dict(fixture=True))
                    group.owners={arm:owner}
                    query=root/'query'
                    query.mkdir()
                    primary=ValueError('query failed before any request')
                    def run(selected_arm, **arguments):
                        self.assertEqual(selected_arm, arm)
                        if arm.startswith(('lotus-', 'duckdb-')):
                            self.assertEqual(arguments['lotus_observation_mode'], mode if arm.startswith('lotus-') else 'qualification')
                        else:
                            self.assertNotIn('lotus_observation_mode', arguments)
                        (query/'summary.json').write_text(json.dumps(dict(status='failed', actual_posts=0)))
                        raise primary
                    with patch.object(persistent, 'run_supplier_query', run), \
                         patch.object(persistent, 'run_prepared_map_query', run):
                        with self.assertRaises(ValueError) as caught:
                            group.run(arm, unit_id='query', root=query, load_source=lambda:iter(()), phase='measurement')
                    self.assertIs(caught.exception, primary)
                    self.assertTrue(group.poisoned)
                    self.assertEqual(owner.queries, 0)
                    saved=json.loads((query/'persistent-query.json').read_text())
                    self.assertEqual(saved['status'], 'failed')
                    self.assertEqual(saved['lotus_observation_mode'], mode if arm.startswith('lotus-') else None)
                    self.assertEqual(saved['query_summary_sha256'], hashlib.sha256((query/'summary.json').read_bytes()).hexdigest())


class LotusObservationProxyTests(unittest.TestCase):
    """Use the real gateway, durable budget and query recorder with a supplier substitute."""
    def run_query(self, root, endpoint, mode, *, stages=1, extra_post=False, budget_limit=None):
        root=Path(root)
        rows=[dict(row_id='a', text='duplicate input'), dict(row_id='b', text='duplicate input')]
        maximum=len(rows)*stages
        ledger=CellBudgetLedger.create(root/'budget.sqlite', AttemptBudget('fixture', budget_limit or maximum),
            deadline_utc=time.time()+30)
        posts=[]

        @contextmanager
        def lotus_rows(stack, arm, values, plan, model, execution, observed, options, unit, stage_specs, stop,
                       tokenizer, **kwargs):
            self.assertEqual(kwargs['lotus_observation_mode'], mode)
            def original(data, generation, show, description):
                responses=[]
                for messages, _ in data:
                    payload=fixture_prepare(None, messages, generation).payload
                    posts.append(payload)
                    with urlopen(Request(model.endpoint_url, payload, {'Content-Type':'application/json'}), timeout=3) as response:
                        value=json.loads(response.read())
                    responses.append(SimpleNamespace(model_dump=lambda value=value:value))
                if extra_post:
                    with urlopen(Request(model.endpoint_url, posts[-1], {'Content-Type':'application/json'}), timeout=3):
                        self.fail('extra POST escaped the declared call budget')
                return responses
            lm=SimpleNamespace(_process_uncached_messages=original)
            observer=supplier._LotusBatchObservation(lm, values, observed, observation_mode=mode)
            def execute():
                for stage in range(stages):
                    data=[([dict(role='user', content=row['text'] if stage==0 else 'ok')], None) for row in values]
                    result=observer(lm, data, dict(max_tokens=16, temperature=0), False, 'fixture')
                for row, response in zip(values, result):
                    yield row['row_id'], response.model_dump()['choices'][0]['message']['content']
            yield execute, dict(supplier='deterministic substitute; no LOTUS SDK', observation_mode=mode)

        error=None
        result=None
        with patch.object(supplier, '_lotus_rows', lotus_rows), \
             patch('src.semantic_methods.lotus.sdk.prepare_call', side_effect=fixture_prepare) as prepare:
            try:
                result=supplier.run_supplier_query('lotus-adapted-native' if stages==1 else 'lotus-two-map-native-staged',
                    load_source=lambda:iter(rows), plan=SemanticMapPlan('Return ok.', 'fixture', 16),
                    model=FixedModelConfig(endpoint, 'fixture', 3000), ledger=ledger, unit_id='query',
                    root=root/'query', options=NativeGraphOptions(), query_timeout_s=5,
                    allowed_outputs=('ok',), reference_outputs={'a':'ok', 'b':'ok'},
                    lotus_observation_mode=mode)
            except BaseException as failure:
                error=failure
        saved=json.loads((root/'query'/'summary.json').read_text())
        budget=ledger.snapshot()
        if budget['units']:
            with self.assertRaisesRegex(BudgetExhausted, 'closed'):
                SharedClaimedUnit(ledger.path, ledger.budget, 'query').reserve('a'*64)
        return result, error, saved, budget, prepare.call_count, posts

    def test_single_and_two_stage_queries_keep_actual_posts_outputs_occurrences_and_budget(self):
        for stages in (1, 2):
            bodies=[]
            for mode in supplier.LOTUS_OBSERVATION_MODES:
                with tempfile.TemporaryDirectory() as root, fixture_server(delay=0) as (endpoint, requests):
                    result, error, saved, budget, prepared, posts=self.run_query(root, endpoint, mode, stages=stages)
                    self.assertIsNone(error)
                    self.assertEqual(result['status'], 'passed')
                    self.assertEqual((len(requests), result['actual_posts'], result['attempted_posts']), (2*stages,)*3)
                    self.assertEqual(prepared, 2*stages if mode=='qualification' else 0)
                    self.assertEqual(result['quality']['accuracy'], 1)
                    self.assertEqual(result['execution']['recorded_rows'], 2)
                    self.assertEqual(len(result['method_calls']), 2*stages)
                    self.assertEqual(result['call_timing']['request_e2e']['count'], 2*stages)
                    self.assertEqual(len(result['call_timing']['successor_waits']), 2 if stages==2 else 0)
                    self.assertEqual(result['lotus_observation_mode'], mode)
                    self.assertEqual(result['method_http_values_audit']['status'], 'passed' if mode=='qualification' else 'unavailable')
                    self.assertEqual(budget['allocated_requests'], 2*stages)
                    self.assertEqual(budget['units'][0]['claimed'], 1)
                    self.assertFalse(result['performance_qualified'])
                    self.assertAlmostEqual(result['full_query_seconds'],
                        (result['execution']['t_query_terminal_ns']-result['query_preparation_started_ns'])/1e9)
                    trace=[json.loads(line) for line in (Path(root)/'query'/'http-trace.jsonl').read_text().splitlines()]
                    self.assertTrue(all(t['request_body_sha256']==t['forwarded_body_sha256'] for t in trace))
                    bodies.append(requests)
            self.assertEqual(bodies[0], bodies[1])

    def test_overrun_is_refused_before_upstream_and_keeps_failed_evidence_and_closed_budget(self):
        for mode in supplier.LOTUS_OBSERVATION_MODES:
            with tempfile.TemporaryDirectory() as root, fixture_server(delay=0) as (endpoint, requests):
                _, error, saved, budget, _, _=self.run_query(root, endpoint, mode, extra_post=True)
                self.assertIsNotNone(error)
                self.assertEqual(len(requests), 2)
                self.assertEqual(saved['status'], 'failed')
                self.assertEqual(saved['attempted_posts'], 2)
                self.assertEqual(saved['lotus_observation_mode'], mode)
                self.assertEqual(budget['allocated_requests'], 2)
                self.assertEqual(budget['units'][0]['claimed'], 1)

    def test_insufficient_total_budget_sends_no_posts_in_either_mode(self):
        for mode in supplier.LOTUS_OBSERVATION_MODES:
            with tempfile.TemporaryDirectory() as root, fixture_server(delay=0) as (endpoint, requests):
                _, error, saved, budget, prepared, _=self.run_query(root, endpoint, mode, budget_limit=1)
                self.assertIsInstance(error, BudgetExhausted)
                self.assertEqual((requests, prepared, saved['attempted_posts']), ([], 0, 0))
                self.assertEqual(saved['status'], 'failed')
                self.assertEqual(budget['allocated_requests'], 0)


if __name__=='__main__':
    unittest.main()
