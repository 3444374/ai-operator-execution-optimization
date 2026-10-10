"""Query identity, resource release and repeated native-library fixture calls."""
import hashlib
from collections import Counter
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock

from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.shared_request_budget import CellBudgetLedger
from src.experiments.postgresql.persistent_native_adapter_query import PersistentAdapterGroup
from tests.experiments.test_native_adapter_query import fixture_server


def raw_rows(count):
    return tuple(dict(row_id=str(i),text='same fixture text') for i in range(count))


class PersistentAdapterTests(unittest.TestCase):
    def test_sema_group_executor_reuses_core_with_changed_inputs_and_query_traces(self):
        from src.baselines.text.products import sema
        from src.execution_provider.adapters import sema_semloom
        from src.execution_provider.adapters.full_response import FullResponseTransport
        from src.execution_provider.adapters.native_tasks import build_native_execution
        from tests.execution_provider.test_sema_service import _PROCESS

        def make_execution(owner):
            class Transport(FullResponseTransport):
                async def execute(self, task, endpoint):
                    owner._before_request(task)
                    return await super().execute(task, endpoint)
            with mock.patch('src.execution_provider.adapters.native_tasks.FullResponseTransport', Transport):
                return build_native_execution(owner.model_config, physical=None,
                    max_tasks=owner.max_held_tasks, max_active_requests=owner.max_active_requests,
                    observer=owner._observe)

        physical = RayMapConfig('unused', 1, 1, 2**21, 2**23)
        arm = 'sema-method-semloom-request-service'
        with tempfile.TemporaryDirectory() as directory, fixture_server(content='"ok"') as (url, requests):
            root = Path(directory)
            binary = root / 'fake-sema'
            binary.write_text('#!' + sys.executable + '\n' + _PROCESS)
            binary.chmod(0o700)
            ledger = CellBudgetLedger.create(root / 'budget.sqlite', AttemptBudget('sema-resident', 5),
                                             deadline_utc=time.time() + 30)
            with mock.patch.object(sema, 'BINARY_SHA256', hashlib.sha256(binary.read_bytes()).hexdigest()), \
                    mock.patch.object(sema_semloom.SemaSemLoomExecutor, '_build_execution', make_execution), \
                    mock.patch('src.experiments.postgresql.persistent_native_adapter_query._runtime',
                               return_value=(physical, dict(owner='CPU/fake diagnostic; no Ray'))), \
                    mock.patch.object(PersistentAdapterGroup, 'ray_resources', return_value=dict(status='unavailable')), \
                    PersistentAdapterGroup((arm,), plan=SemanticMapPlan('Return ok.', 'fixture', 16),
                        model=FixedModelConfig(url, 'fixture', 1000), ledger=ledger, root=root / 'group',
                        options=NativeGraphOptions(concurrency=4), max_held_tasks=4, physical=physical,
                        sema_binary=binary, query_timeout_s=3, sema_executor_scope='group-diagnostic') as group:
                identities = []
                for number, count in enumerate((2, 3)):
                    values = tuple(dict(row_id=str(i), text='query-' + str(number) + '-row-' + str(i))
                                   for i in range(count))
                    result = group.run(arm, unit_id='q' + str(number), root=root / ('q' + str(number)),
                        load_source=lambda: iter(values), phase='qualification', allowed_outputs=('ok',))
                    identities.append(result['persistent_lifecycle'])
                    self.assertEqual(result['actual_posts'], count)
                    self.assertEqual(result['request_service']['forwarded_posts'], count)
                    self.assertEqual(result['identity']['sema_executor_scope'], 'group-diagnostic')
                    self.assertEqual(result['request_service']['executor_scope'], 'persistent group diagnostic')
                    self.assertEqual(result['persistent_lifecycle']['core_jobs'], 0)
                    self.assertFalse(group.owners[arm].sema_executor.execution.engine.capacity.records)
                    traces = [json.loads(line) for line in (root / ('q' + str(number)) / 'http-trace.jsonl').read_text().splitlines()]
                    self.assertEqual({trace['query_id'] for trace in traces}, {'q' + str(number)})
                for key in ('owner_id', 'execution_id', 'sema_pid'):
                    self.assertEqual(len({value[key] for value in identities}), 1)
                self.assertEqual(len(requests), 5)
                self.assertEqual([body['messages'][0]['content'] for body in requests],
                    ['query-0-row-0', 'query-0-row-1', 'query-1-row-0', 'query-1-row-1', 'query-1-row-2'])
            self.assertFalse(group.owners[arm].sema_executor._thread.is_alive())
            self.assertEqual(json.loads((root / 'group' / 'group-summary.json').read_text())['status'], 'passed')

    def test_fixed_held_budget_survives_queries_with_independent_active_capacity(self):
        with tempfile.TemporaryDirectory() as directory,fixture_server() as (url,requests):
            root=Path(directory)
            ledger=CellBudgetLedger.create(root/'budget.sqlite',AttemptBudget('held-fixture',32),deadline_utc=time.time()+30)
            arm='fixed-map-semloom-local-diagnostic'
            with PersistentAdapterGroup((arm,),plan=SemanticMapPlan('Return ok.','fixture',16),
                    model=FixedModelConfig(url,'fixture',1000),ledger=ledger,root=root/'group',
                    options=NativeGraphOptions(concurrency=16),max_held_tasks=128,
                    sema_native_threads=4,query_timeout_s=3) as group:
                engine=group.owners[arm].execution.engine
                self.assertEqual(engine.capacity.limits.held_tasks,128)
                self.assertEqual(engine.capacity.limits.active_requests,16)
                self.assertEqual(engine.capacity.limits.input_bytes,128*1048576)
                self.assertEqual(engine.capacity.limits.result_bytes,128*1048576)
                for number in range(2):
                    result=group.run(arm,unit_id='held-'+str(number),root=root/('query-'+str(number)),
                        load_source=lambda:iter(raw_rows(16)),phase='qualification',allowed_outputs=('ok',))
                    self.assertEqual(result['semloom_capacity'],dict(held_tasks=128,active_requests=16))
                    self.assertFalse(engine.capacity.records)
                    self.assertFalse(engine.jobs.jobs)
            self.assertEqual(len(requests),32)

    def test_sema_native_owner_receives_threads_independently_from_http_capacity(self):
        from src.baselines.text.products import sema
        native=mock.Mock(pid=12345)
        prepared=mock.MagicMock()
        prepared.__enter__.return_value=native
        with tempfile.TemporaryDirectory() as directory,fixture_server() as (url,requests),\
                mock.patch.object(sema,'prepare_projection',return_value=prepared) as prepare:
            root=Path(directory)
            ledger=CellBudgetLedger.create(root/'budget.sqlite',AttemptBudget('sema-threads',1),deadline_utc=time.time()+30)
            arm='sema-native-direct'
            with PersistentAdapterGroup((arm,),plan=SemanticMapPlan('Return ok.','fixture',16),
                    model=FixedModelConfig(url,'fixture',1000),ledger=ledger,root=root/'group',
                    options=NativeGraphOptions(concurrency=16),max_held_tasks=128,
                    sema_native_threads=4,query_timeout_s=3) as group:
                self.assertIs(group.owners[arm].sema_native(raw_rows(1)),native)
                self.assertEqual(prepare.call_args.kwargs['num_threads'],4)
                self.assertEqual(group.max_held_tasks,128)
                self.assertEqual(group.options.concurrency,16)
            self.assertFalse(requests)

    def test_one_core_survives_three_queries_with_changed_row_counts_and_new_budgets(self):
        with tempfile.TemporaryDirectory() as directory, fixture_server() as (url, requests):
            root = Path(directory)
            budget = AttemptBudget('persistent-fixture',7)
            ledger = CellBudgetLedger.create(root/'budget.sqlite',budget,deadline_utc=time.time()+30)
            arm = 'fixed-map-semloom-local-diagnostic'
            with PersistentAdapterGroup((arm,),plan=SemanticMapPlan('Return ok.','fixture',16),
                    model=FixedModelConfig(url,'fixture',1000),ledger=ledger,root=root/'group',
                    query_timeout_s=3) as group:
                engine = group.owners[arm].execution
                ids=[]
                for number,(count,phase) in enumerate(zip((2,3,2),('qualification','warmup','measurement'))):
                    query = root/('query-'+str(number))
                    result = group.run(arm,unit_id='query-'+str(number),root=query,
                        load_source=lambda:iter(raw_rows(count)),phase=phase,allowed_outputs=('ok',))
                    self.assertIs(group.owners[arm].execution,engine)
                    self.assertFalse(engine.engine.capacity.records)
                    self.assertFalse(engine.engine.jobs.jobs)
                    self.assertEqual(result['actual_posts'],count)
                    sidecar=json.loads((query/'persistent-query.json').read_text())
                    self.assertEqual(sidecar['measurement_phase'],phase)
                    self.assertEqual(sidecar['query_summary_sha256'],hashlib.sha256((query/'summary.json').read_bytes()).hexdigest())
                    self.assertLessEqual(sidecar['t_release_ns'],sidecar['t_submit_ns'])
                    self.assertLess(sidecar['t_submit_ns'],sidecar['t_eof_ns'])
                    ids.append(sidecar['persistent_lifecycle']['owner_id'])
                    traces=[json.loads(line) for line in (query/'http-trace.jsonl').read_text().splitlines()]
                    self.assertEqual({t['query_id'] for t in traces},{'query-'+str(number)})
                self.assertEqual(len(set(ids)),1)
                self.assertEqual(len(requests),7)
            self.assertEqual(json.loads((root/'group/group-summary.json').read_text())['status'],'passed')

    def test_cancelled_query_drains_its_core_and_stops_later_query_forwarding(self):
        with tempfile.TemporaryDirectory() as directory, fixture_server(delay=.15) as (url, requests):
            root=Path(directory)
            ledger=CellBudgetLedger.create(root/'budget.sqlite',AttemptBudget('cancel-fixture',8),deadline_utc=time.time()+30)
            arm='fixed-map-semloom-local-diagnostic'
            with PersistentAdapterGroup((arm,),plan=SemanticMapPlan('Return ok.','fixture',16),
                    model=FixedModelConfig(url,'fixture',1000),ledger=ledger,root=root/'group',
                    query_timeout_s=.05) as group:
                with self.assertRaises((TimeoutError,ValueError)):
                    group.run(arm,unit_id='cancel-query',root=root/'cancel-query',
                        load_source=lambda:iter(raw_rows(8)),phase='warmup')
                self.assertFalse(group.owners[arm].execution.engine.capacity.records)
                self.assertFalse(group.owners[arm].execution.engine.jobs.jobs)
                count=len(requests)
                with self.assertRaisesRegex(RuntimeError,'stopped'):
                    group.run(arm,unit_id='later-query',root=root/'later-query',
                        load_source=lambda:iter(raw_rows(1)),phase='measurement')
                self.assertEqual(len(requests),count)
            summary=json.loads((root/'cancel-query/summary.json').read_text())
            self.assertEqual(summary['status'],'failed')
            sidecar=json.loads((root/'cancel-query/persistent-query.json').read_text())
            self.assertEqual(sidecar['measurement_phase'],'warmup')


@unittest.skipUnless(os.environ.get('SEMLOOM_PERSISTENT_GROUP'),'requires pinned native libraries in an isolated process')
class PersistentActualLibraries(unittest.TestCase):
    def test_interleaved_group_reuses_owners_without_reusing_model_results(self):
        selected=os.environ['SEMLOOM_PERSISTENT_GROUP']
        groups={
            'fixed':('fixed-map-native-daft','fixed-map-native-ray','fixed-map-semloom'),
            'lotus':('lotus-adapted-native','lotus-method-semloom-local-diagnostic','lotus-method-semloom'),
            'duckdb':('duckdb-adapted-native','duckdb-method-semloom'),
            'sema':('sema-native-direct','sema-native-transparent','sema-method-semloom-request-service'),
            'two-map':('lotus-two-map-native-staged','lotus-two-map-semloom-staged','lotus-two-map-semloom-incremental'),
        }
        arms=groups[selected]
        if os.environ.get('SEMLOOM_PERSISTENT_ARM'):
            self.assertIn(os.environ['SEMLOOM_PERSISTENT_ARM'],arms)
            arms=(os.environ['SEMLOOM_PERSISTENT_ARM'],)
        root=Path(os.environ['SEMLOOM_PERSISTENT_OUTPUT'])
        root.mkdir(mode=0o700,parents=True,exist_ok=False)
        posts=16*len(arms)*(2 if selected=='two-map' else 1)
        with fixture_server(content='"ok"' if selected=='sema' else 'ok') as (url, requests):
            ledger=CellBudgetLedger.create(root/'budget.sqlite',AttemptBudget('actual-persistent',posts),deadline_utc=time.time()+900)
            with PersistentAdapterGroup(arms,plan=SemanticMapPlan('Return ok.','fixture',16),
                    model=FixedModelConfig(url,'fixture',5000),ledger=ledger,root=root/'group',
                    options=NativeGraphOptions(concurrency=4,num_threads=8),
                    physical=RayMapConfig('unused',2,2,2**21+24,2**23),
                    ray_temp_root=Path(os.environ['SEMLOOM_PERSISTENT_RAY_ROOT']),
                    tokenizer_path=Path(os.environ['SEMLOOM_TOKENIZER']) if os.environ.get('SEMLOOM_TOKENIZER') else None,
                    duckdb_library=Path(os.environ['SEMLOOM_DUCKDB_LIBRARY']) if os.environ.get('SEMLOOM_DUCKDB_LIBRARY') else None,
                    sema_binary=Path(os.environ['SEMLOOM_SEMA_BINARY']) if os.environ.get('SEMLOOM_SEMA_BINARY') else None,
                    stages=[dict(instruction='Summarize {text}.',output_column='summary'),
                            dict(instruction='Return ok for {summary}.',output_column='answer')] if selected=='two-map' else None) as group:
                identities={arm:[] for arm in arms}
                for number,(count,phase) in enumerate(((4,'qualification'),(8,'warmup'),(4,'measurement'))):
                    order=arms[number%len(arms):]+arms[:number%len(arms)]
                    for arm in order:
                        query=root/(arm+'-'+str(number))
                        result=group.run(arm,unit_id=arm+'-'+str(number),root=query,
                            load_source=lambda:iter(raw_rows(count)),phase=phase,allowed_outputs=('ok',))
                        self.assertEqual(result['status'],'passed')
                        self.assertEqual(result['rows'],count)
                        self.assertEqual(result['actual_posts'],count*(2 if selected=='two-map' else 1))
                        identities[arm].append(result['persistent_lifecycle'])
                for arm,values in identities.items():
                    for key in ('owner_id','execution_id','lm_id','duckdb_connection_id','sema_pid','ray_session_id'):
                        self.assertEqual(len({v[key] for v in values}),1,(arm,key))
                self.assertEqual(len(requests),posts)
            self.assertEqual(json.loads((root/'group/group-summary.json').read_text())['status'],'passed')


@unittest.skipUnless(os.environ.get('SEMLOOM_PERSISTENT_SUPPLIER'),
                     'requires a pinned supplier binary in an isolated process')
class PersistentSupplierLibraries(unittest.TestCase):
    def test_changed_inputs_and_scale_keep_the_native_owner_and_reset_query_state(self):
        arm=os.environ['SEMLOOM_PERSISTENT_SUPPLIER']
        self.assertIn(arm,('duckdb-adapted-native','duckdb-method-semloom',
            'sema-native-direct','sema-native-transparent','sema-method-semloom-request-service',
            'lotus-adapted-native','lotus-method-semloom-local-diagnostic','lotus-method-semloom'))
        root=Path(os.environ['SEMLOOM_PERSISTENT_OUTPUT'])
        root.mkdir(mode=0o700,parents=True,exist_ok=False)
        inputs=[tuple(dict(row_id='query-'+str(number)+'-row-'+str(i),
            text='fixture-query-'+str(number)+'-item-'+str(i).zfill(4)+'-end') for i in range(count))
            for number,count in enumerate((8,8,128))]
        (root/'fixture-inputs.json').write_text(json.dumps(inputs,indent=2)+'\n')
        identities=[];vectors=[];replacements=[];bridges=[]
        from src.semantic_methods import duckdb_ai
        from src.baselines.text.products import sema
        original_bridge=duckdb_ai.DuckDBSemLoomBridge
        original_replace=sema._PreparedProjection.replace_source
        current_query=None
        class ObservedBridge(original_bridge):
            def __init__(self,extension,execute):
                def observed(calls,cancelled):
                    vectors.append(dict(query=current_query,rows=[call.row for call in calls],
                        native_query_ids=[call.query_id for call in calls],
                        native_call_ids=[call.call_id for call in calls]))
                    yield from execute(calls,cancelled)
                super().__init__(extension,observed)
                bridges.append(self)
        def observed_replace(native,values,source_root,endpoint):
            values=tuple(values)
            original_replace(native,values,source_root,endpoint)
            replacements.append(dict(query=current_query,pid=native.pid,rows=len(values),
                endpoint=endpoint,source_sha256=hashlib.sha256(
                    (Path(source_root)/'sema-source.csv').read_bytes()).hexdigest()))
        with fixture_server(content='"ok"' if arm.startswith('sema-') else 'ok') as (url,requests):
            ledger=CellBudgetLedger.create(root/'budget.sqlite',AttemptBudget('supplier-persistent',144),
                deadline_utc=time.time()+900)
            with mock.patch.object(duckdb_ai,'DuckDBSemLoomBridge',ObservedBridge), \
                    mock.patch.object(sema._PreparedProjection,'replace_source',observed_replace), \
                    PersistentAdapterGroup((arm,),plan=SemanticMapPlan('Return ok.','fixture',16),
                    model=FixedModelConfig(url,'fixture',60000),ledger=ledger,root=root/'group',
                    options=NativeGraphOptions(concurrency=4,num_threads=8),
                    physical=RayMapConfig('unused',2,2,2**21+24,2**23),
                    ray_temp_root=Path(os.environ['SEMLOOM_PERSISTENT_RAY_ROOT']),
                    tokenizer_path=Path(os.environ['SEMLOOM_TOKENIZER']) if os.environ.get('SEMLOOM_TOKENIZER') else None,
                    duckdb_library=Path(os.environ['SEMLOOM_DUCKDB_LIBRARY']),
                    sema_binary=Path(os.environ['SEMLOOM_SEMA_BINARY']),
                    sema_executor_scope=os.environ.get('SEMLOOM_SEMA_EXECUTOR_SCOPE', 'query')) as group:
                for number,values in enumerate(inputs):
                    current_query='supplier-query-'+str(number)
                    before=len(requests)
                    result=group.run(arm,unit_id=current_query,root=root/current_query,
                        load_source=lambda:iter(values),phase='qualification',allowed_outputs=('ok',))
                    self.assertEqual(result['status'],'passed')
                    self.assertEqual(result['rows'],len(values))
                    self.assertEqual(result['actual_posts'],len(values))
                    self.assertEqual(len(requests)-before,len(values))
                    expected=Counter(value['text'] for value in values)
                    observed=Counter()
                    for request in requests[before:]:
                        payload=json.dumps(request)
                        matches=[text for text in expected if text in payload]
                        self.assertEqual(len(matches),1,(current_query,matches))
                        observed.update(matches)
                    self.assertEqual(observed,expected)
                    identities.append(result['persistent_lifecycle'])
                    if arm.startswith('duckdb-'):
                        connection=group.owners[arm].connection
                        stored=connection.execute('SELECT source_position,row_id FROM adapter_inputs ORDER BY source_position').fetchall()
                        self.assertEqual(stored,[(i,value['row_id']) for i,value in enumerate(values)])
                    if group.owners[arm].execution is not None:
                        self.assertFalse(group.owners[arm].execution.engine.capacity.records)
                        self.assertFalse(group.owners[arm].execution.engine.jobs.jobs)
                    if group.owners[arm].sema_executor is not None:
                        executor = group.owners[arm].sema_executor
                        self.assertFalse(executor.execution.engine.capacity.records)
                        self.assertEqual(executor.snapshot()['core_jobs'], 0)
                        self.assertEqual(executor.snapshot()['object_bytes'], 0)
                for key in ('owner_id','execution_id','lm_id','duckdb_connection_id','sema_pid','ray_session_id'):
                    self.assertEqual(len({value[key] for value in identities}),1,(arm,key))
                self.assertEqual(len(requests),144)
            if arm=='duckdb-method-semloom':
                self.assertEqual([value['rows'] for value in vectors],[list(range(n)) for n in (8,8,128)])
                calls=[call for value in vectors for call in value['native_call_ids']]
                self.assertEqual(len(set(calls)),144)
                self.assertEqual(len(bridges),3)
                self.assertTrue(all(bridge._closed and bridge.retained_batches==0 for bridge in bridges))
            if arm.startswith('sema-'):
                if group.owners[arm].sema_executor is not None:
                    self.assertFalse(group.owners[arm].sema_executor._thread.is_alive())
                self.assertEqual([value['rows'] for value in replacements],[8,8,128])
                self.assertEqual(len({value['pid'] for value in replacements}),1)
                self.assertEqual(len({value['source_sha256'] for value in replacements}),3)
                if arm!='sema-native-direct':
                    self.assertEqual(len({value['endpoint'] for value in replacements}),3)
                    for value in replacements:
                        self.assertIn('/sema/'+value['query']+'/',value['endpoint'])
                with self.assertRaises(ProcessLookupError):
                    os.kill(identities[0]['sema_pid'],0)
            (root/'native-vectors.json').write_text(json.dumps(vectors,indent=2)+'\n')
            (root/'source-replacements.json').write_text(json.dumps(replacements,indent=2)+'\n')
            (root/'fixture-requests.jsonl').write_text(''.join(json.dumps(value)+'\n' for value in requests))
            (root/'fixture-verification.json').write_text(json.dumps(dict(
                arm=arm,query_rows=[8,8,128],fixture_posts=144,real_model_posts=0,
                lifecycles=identities,native_owner_reused=True,input_text_and_row_ids_replaced=True,
                query_state_released=True),indent=2)+'\n')
            self.assertEqual(json.loads((root/'group/group-summary.json').read_text())['status'],'passed')

    @unittest.skipUnless(os.environ.get('SEMLOOM_PERSISTENT_SUPPLIER')=='sema-native-direct',
                         'one actual author-process endpoint replacement probe')
    def test_actual_author_session_changes_the_endpoint_before_its_next_select(self):
        from src.baselines.text.products.sema import prepare_projection
        root=Path(os.environ['SEMLOOM_PERSISTENT_OUTPUT']+'-endpoint')
        root.mkdir(mode=0o700,parents=True,exist_ok=False)
        values=[dict(source_example_id='endpoint-first',input_text='first-endpoint-input')]
        changed=[dict(source_example_id='endpoint-second',input_text='second-endpoint-input')]
        with fixture_server(content='"ok"') as (first_url,first), \
                fixture_server(content='"ok"') as (second_url,second):
            with prepare_projection(values,SemanticMapPlan('Return ok.','fixture',16),
                    FixedModelConfig(first_url,'fixture',60000),
                    binary=Path(os.environ['SEMLOOM_SEMA_BINARY']),root=root,num_threads=4) as native:
                pid=native.pid
                self.assertEqual(list(native.execute()),[('endpoint-first','ok')])
                replacement=root/'next'
                replacement.mkdir()
                native.replace_source(changed,replacement,second_url)
                self.assertEqual(list(native.execute()),[('endpoint-second','ok')])
                self.assertEqual(native.pid,pid)
                self.assertEqual(len(first),1)
                self.assertEqual(len(second),1)
                self.assertIn(values[0]['input_text'],json.dumps(first[0]))
                self.assertIn(changed[0]['input_text'],json.dumps(second[0]))
            with self.assertRaises(ProcessLookupError):os.kill(pid,0)
        (root/'endpoint-verification.json').write_text(json.dumps(dict(
            same_author_pid=True,endpoint_changed=True,input_changed=True,
            fixture_posts=2,real_model_posts=0,process_closed=True),indent=2)+'\n')


if __name__=='__main__':
    unittest.main()
