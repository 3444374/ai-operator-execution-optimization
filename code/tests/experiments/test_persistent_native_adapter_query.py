"""Query identity, resource release and repeated native-library fixture calls."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import unittest

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


if __name__=='__main__':
    unittest.main()
