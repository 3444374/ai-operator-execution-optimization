"""CPU-only fixtures for the existing public Movie session API.

The real factory, lifecycle, Core and socket process run unchanged. Ray is the
existing threaded replacement; checkout/evaluation/lease and model preflight are
explicit external-boundary fixtures. No model, GPU or real Ray is started.
"""
from contextlib import ExitStack
from copy import deepcopy
import csv
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

from src.baselines.text.movie_records import MovieRecordSource, prepare_movie_source
from src.baselines.text.movie_tasks import MovieTask
from src.experiments import lotus_records_movie as movie, movie_records_session as session
from src.experiments.cell_budget import ClaimedUnit, Reservation
from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.experiments import test_movie_async_token_entry as entry
from tests.execution_provider.test_lotus_record_local_query import Process


def with_actual_cpu_pid(process_type):
    """Supply the real owned CPU child's PID, absent from legacy stub ready()."""
    class CPUProcessWithPID(process_type):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.ready_report = dict(self.ready_report, pid=self.process.pid)
    return CPUProcessWithPID


class MovieTokenResidentFixture:
    def __init__(self, case):
        helper = entry.AsyncTokenEntryTests()
        helper.setUp()
        case.addCleanup(helper.doCleanups)
        self.helper, self.root, self.model = helper, helper.root, helper.model
        self.manifests = dict(helper.helper.manifests)
        self.owners, self.outputs = [], []
        self.runtime = helper.runtime()
        ready = patch.object(AsyncCoreModelActor, '_process_type', return_value=with_actual_cpu_pid(Process))
        ready.start()
        helper.addCleanup(ready.stop)
        rows = MovieRecordSource.from_manifest(self.manifests['movie-q1']).rows()
        self.replies = {'filter|'+r['row_id']: dict(text=str(r['source_position'] == 0)) for r in rows}
        self.write_replies()

    def write_replies(self):
        self.model.write_text(json.dumps(dict(responses=self.replies)))

    def source_manifest(self, name, rows, *, task):
        path = self.root/(name+'.csv')
        with path.open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(['reviewId', 'id', 'reviewText', 'scoreSentiment'])
            writer.writerows(rows)
        return str(prepare_movie_source(path, self.root/name,
            expected_csv_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            max_rows=max(1, len(rows)), task=task))

    def plan(self, mode, *, lifecycle='resident', tasks=('movie-q1', 'movie-q1')):
        if mode not in {'RF', 'RD'}:
            raise ValueError('declare RF fixed rolling or RD request feedback')
        config = self.helper.rolling_config()
        if mode == 'RD':
            config['core_profile'] = dict(mode='async', admission=dict(kind='resource_feedback'))
        plan = dict(schema='semloom.movie_session_plan.v1', backend='semloom-core',
            lifecycle=lifecycle, model_name='openai/cpu-fixture',
            generation=deepcopy(config['generation']), engine=deepcopy(config['engine']),
            resources=deepcopy(config['resources']), profile=deepcopy(config['core_profile']),
            prepared_batching=deepcopy(config['prepared_batching']), rolling_refill=True,
            queries=[dict(query_id='resident-query-'+str(i), task=task,
                manifest=self.manifests[task], phase='warmup' if i == 0 else 'measured')
                for i, task in enumerate(tasks)])
        plan['engine']['max_model_calls'] = max(1, sum(
            len(MovieRecordSource.from_manifest(q['manifest']).expected(q['query_id']))
            for q in plan['queries']))
        return plan

    def context(self, plan):
        claims, start = {}, 1
        for query in plan['queries']:
            count = len(MovieRecordSource.from_manifest(query['manifest']).expected(query['query_id']))
            claims[query['query_id']] = ClaimedUnit(Reservation(query['query_id'], start, count), None)
            start += count
        engine = plan['engine']
        return dict(owner_context={}, claims=claims, sembench_checkout=self.root,
            runtime_binding={**{k: engine[k] for k in ('model_path', 'model_python', 'driver_python')},
                'tokenizer_path': engine['tokenizer_path'], 'cuda_visible_devices': '0'})

    def events(self):
        path = Path(str(self.model)+'.events')
        return [] if not path.exists() else [json.loads(line) for line in path.read_text().splitlines()]

    def owner(self, mode, *, resident):
        plan = self.plan(mode)
        if not self.runtime.ray.is_initialized():
            self.runtime.ray.init()
        return core.create_records_core(MovieTask(1).method(plan['model_name']),
            EngineSpec.from_dict(plan['engine']), GenerationSpec.from_dict(plan['generation']),
            RecordsResources(**plan['resources']), profile=plan['profile'], rolling_refill=True,
            prepared_batching=PreparedBatching(**plan['prepared_batching']), resident=resident)

    def run(self, plan, *, checkpoint=None, observer=None, context=None):
        output = self.root/('resident-output-'+str(len(self.outputs)))
        self.outputs.append(output)
        original_factory = core.create_records_core

        def allocate(*args, **kwargs):
            owner = original_factory(*args, **kwargs)
            self.owners.append(owner)
            return owner

        with ExitStack() as stack:
            stack.enter_context(patch('integrations.imlane_records.chain_runtime.check_external_lease', return_value={}))
            stack.enter_context(patch.object(EngineSpec, 'validate_driver_environment', return_value={}))
            stack.enter_context(patch.object(movie, 'model_preflight', return_value=dict(inference_calls=0)))
            stack.enter_context(patch.object(core, 'create_records_core', side_effect=allocate))
            return session.run_movie_session(plan, self.context(plan) if context is None else context,
                output, checkpoint=checkpoint, observer=observer)


def query_close(query):
    return query['backend_report']['backend_close']


def query_calls(query):
    return query_close(query)['calls']
