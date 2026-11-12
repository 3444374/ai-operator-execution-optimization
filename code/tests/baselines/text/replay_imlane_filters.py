"""Full saved-answer Filter replay; author SQL and model are software fixtures.

Use the real Arrow kernel, Unix IPC, physical request budget and original
SemBench formatter/parser/sink/evaluator. Source/message identities, never
concurrent arrival order, select each saved answer.
"""

import argparse
import asyncio
from copy import deepcopy
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
from transformers import AutoTokenizer

from integrations.imlane_duckdb import model_ipc_worker as worker, model_provider, mmqa_filter_udf
from integrations.imlane_duckdb.model_wire import SHUTDOWN_SCHEMA
from src.baselines.text import sembench_prepared
from src.baselines.text.sembench_movie_native import q3_decision
from src.experiments import native_imlane_filters as binding, native_imlane_relation as relation
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.infrastructure.private_artifacts import content_digest, file_identity, json_metric, new_private_directory, publish_private_json, read_json, write_private_json
from tests.baselines.text.replay_imlane_sembench import IdentityRenderer
from tests.baselines.text.test_imlane_duckdb_model_ipc_worker import FakeEngine, FakeSampling


def reference(originals, query_id):
    originals = Path(originals)
    messages = read_json(originals / 'packet/PROPOSED-messages.json')['rows']
    mapping = read_json(originals / 'packet/CALL-MAP.json')
    saved = read_json(originals / 'output/run/cold/result-ready.json')['result']
    label = 'Q' + query_id
    rows = sorted(((value['original_source_position'], key) for key, value in mapping.items()
                   if value['query'] == label))
    texts = {item['occurrence_id']: item['generated_text'] for item in saved['queries'][label]['predictions']}
    return dict(messages=[messages[key]['messages'] for _, key in rows],
        texts=[texts[key] for _, key in rows], result=saved['frames'][label],
        source_positions=[position for position, _ in rows])


def timed_relation(bound, accepted, directory, expected, sampling, receipt_dir, *, fail_after_audit=False):
    directory.mkdir(mode=0o700)
    clock, events = [100_000_000_000], []
    scope = SimpleNamespace(env={}, children=[], track_root=Mock(), check=Mock())
    process = SimpleNamespace(returncode=None)
    release = directory / 'sink-release.ack'
    native = dict(status='query_returned', return_type='TINYINT', source_rows=len(bound.rows), eligible_rows=len(bound.rows),
        variant='original-author', expected_DSO_sha256=binding.DSO_SHA256,
        full_filter_materialized_before_title_sink=True, accepted_occurrences=accepted,
        result_clock='CLOCK_MONOTONIC', result_ready_monotonic_ns=101_000_000_000,
        sink_handoff=dict(protocol=relation.ACK_PROTOCOL, release_path=str(release)))
    class Scope:
        def __enter__(self):
            events.append('author-open'); return scope
        def __exit__(self, *_):
            events.append('author-close'); clock[0] += 19_000_000_000
    def poll():
        if process.returncode is None and release.exists():
            assert release.read_text().splitlines()[1].startswith('complete ')
            events.append('ACK-then-driver-exit'); process.returncode = 0; clock[0] += 5_000_000_000
        return process.returncode
    process.poll, process.wait = poll, lambda **_: poll()
    def spawn(*_):
        publish_private_json(directory / 'query.json', native); clock[0] = 102_000_000_000
        return process, Mock()
    def sink(value):
        assert process.returncode is None and not release.exists()
        frame = bound.sink(value)
        assert original_value(frame) == expected
        events.append('full-original-memory-sink'); clock[0] = 103_000_000_000
        return frame
    original_digest, original_value = sembench_prepared.content_digest, sembench_prepared.result_value
    observed = dict(digests=0, serialization=0)
    def digest(value):
        assert release.exists(); observed['digests'] += 1; clock[0] += 7_000_000_000
        return original_digest(value)
    def serialize(value):
        assert release.exists(); observed['serialization'] += 1; clock[0] += 11_000_000_000
        return original_value(value)
    def audit(value, frame):
        events.append('post-sink-audit')
        with patch.object(sembench_prepared, 'content_digest', side_effect=digest), \
                patch.object(sembench_prepared, 'result_value', side_effect=serialize):
            audited = bound.audit(value, frame, receipt_dir, sampling)
        if fail_after_audit:
            raise ValueError('injected post-sink audit failure')
        return audited
    report = {}
    window = SimpleNamespace(check=Mock(), accept_ready=Mock(), begin_cleanup=Mock(return_value=10000.))
    with patch.object(relation, 'DuckDBScope', return_value=Scope()), \
            patch.object(relation.owner, 'spawn', side_effect=spawn), \
            patch.object(relation.owner, 'resource_state', return_value={}), \
            patch.object(relation.time, 'monotonic_ns', side_effect=lambda: clock[0]):
        frame, audited = relation.run_relation_query(SimpleNamespace(output=directory), report,
            kernel=Path(mmqa_filter_udf.__file__), command=['explicit-author-software-fixture'],
            window=window, query_started_ns=100_000_000_000, sink=sink, audit=audit, check=lambda: None)
    assert report['source_to_sink_seconds'] == 3
    assert observed['digests'] > 0 and observed['serialization'] == 1
    assert events == ['author-open', 'full-original-memory-sink', 'ACK-then-driver-exit', 'post-sink-audit', 'author-close']
    write_private_json(directory / 'audit.json', audited)
    return dict(status='passed', source_to_sink_seconds=3, clock='controlled software clock, not performance',
        post_sink_cost=observed, events=events, original_quality=audited['quality'],
        complete_relation=audited['semantic']['operator_relation'], report=report)


async def replay(config, originals, output):
    query, method, prepared, requests, tokens, rows = binding.prepare_query(config)
    frozen = reference(originals, config['query_id'])
    assert frozen['source_positions'] == list(range(25))
    assert [row['messages'] for row in requests] == frozen['messages']
    tokenizer = AutoTokenizer.from_pretrained(config['tokenizer'], local_files_only=True)
    assert tokens == [tokenizer.apply_chat_template(value, tokenize=True, add_generation_prompt=True)
                      for value in frozen['messages']]
    sampling = query.operation.generation.kwargs()
    budget = AttemptBudget(config['execution_id'], 25)
    ledger = CellBudgetLedger.create(output / 'budget.sqlite', budget, deadline_utc=None)
    ledger.reserve_unit('query', 25); unit = ledger.claim_shared_unit('query')
    for name in ('model-calls', 'overflow-calls'): (output / name).mkdir(mode=0o700)
    write_private_json(output / 'messages.json', dict(rows=rows))
    engine, stop = FakeEngine(), asyncio.Event()
    engine.renderer = IdentityRenderer(requests, tokens)
    engine.vllm_config.model_config.max_model_len = 32768
    engine.texts = dict(enumerate(frozen['texts'])); engine.release.set()
    factories = []
    def factory(value):
        factories.append(deepcopy(value))
        return worker.NativeModel(engine, FakeSampling, 'final-only', object(), 'software-fixture-no-vllm', FakeSampling)
    primary = None
    with tempfile.TemporaryDirectory(prefix='imlane-filter-', dir='/tmp') as temporary:
        socket_path = Path(temporary) / 'model.sock'
        worker_config = dict(model='/software-fixture/no-weights', served_model_name='filter-software-fixture',
            dtype='bfloat16', engine_options={}, socket_path=str(socket_path),
            ready_path=str(output / 'ready.json'), shutdown_path=str(output / 'shutdown.json'),
            request_budget=dict(path=str(ledger.path), budget_id=budget.budget_id, limit=25, unit_id='query'))
        task = asyncio.create_task(worker.serve(worker_config, model_factory=factory, stop=stop, install_signals=False))
        try:
            async with asyncio.timeout(5):
                while not Path(worker_config['ready_path']).exists():
                    if task.done(): task.result()
                    await asyncio.sleep(.001)
            provider = dict(schema=model_provider.CONFIG_SCHEMA, provider_mode='software_ipc',
                socket_path=str(socket_path), model=worker_config['served_model_name'], sampling_params=sampling,
                request_timeout_s=30, receipt_dir=str(output / 'model-calls'))
            write_private_json(output / 'provider.json', provider)
            order = list(range(24, -1, -2)) + list(range(1, 25, 2))
            batches = [order[:7], order[7:16], order[16:]]
            async def batch(positions):
                chosen = [rows[index] for index in positions]
                table = pa.table({f'c{i}': pa.array([row[key] for row in chosen], type=pa.string())
                                  for i, key in enumerate(('occurrence_id', 'title', 'text'))})
                values = (await asyncio.to_thread(mmqa_filter_udf.infer_mmqa, table)).column(0).to_pylist()
                assert values == [int(q3_decision(frozen['texts'][index])) for index in positions]
                return [dict(occurrence_id=row['occurrence_id'], source_position=row['source_position'], title=row['title'])
                        for row, positive in zip(chosen, values, strict=True) if positive]
            with patch.dict(os.environ, {model_provider.CONFIG_ENV: str(output / 'provider.json'),
                                         mmqa_filter_udf.CONFIG_ENV: str(output / 'messages.json')}):
                accepted = await asyncio.gather(*(batch(part) for part in batches))
            assert len(engine.calls) == unit.attempts == 25
            for prompt, effective, _, _ in engine.calls:
                assert vars(effective) == dict(sampling, output_kind='final-only')
                assert prompt['prompt_token_ids'] == tokens[prompt['fixture_index']]
            bound = binding.FilterRelation(query, method, prepared, requests, tokens, rows, query.bind_execution())
            timing = timed_relation(bound, [value for batch_rows in reversed(accepted) for value in batch_rows],
                output / 'relation', frozen['result'], sampling, output / 'model-calls')
            write_private_json(output / 'TIMING.json', timing)
            write_private_json(output / 'overflow-provider.json', dict(provider, receipt_dir=str(output / 'overflow-calls')))
            with patch.dict(os.environ, {model_provider.CONFIG_ENV: str(output / 'overflow-provider.json')}):
                try: await asyncio.to_thread(model_provider.infer_batch, [requests[0]['messages']])
                except model_provider.ModelBatchError as error: overflow = error.imlane_model_provider_report
                else: raise AssertionError('26th request reached generate')
            assert len(engine.calls) == unit.attempts == 25
            assert not overflow['calls'][0]['attempted']
            assert overflow['calls'][0]['response']['error_type'] == 'BudgetExhausted'
            assert overflow['engine_generate_invocations'] == 0
            write_private_json(output / 'BUDGET.json', dict(consumed=25, refunded=0, rejected26=True))
        except BaseException as error:
            primary = error; raise
        finally:
            ledger.close_shared_unit('query')
            write_private_json(output / 'shutdown.json', dict(schema=SHUTDOWN_SCHEMA, native_shutdown_deadline=time.monotonic() + 5))
            stop.set()
            try: await asyncio.wait_for(asyncio.shield(task), 5)
            except BaseException as error:
                if primary is None: raise
                primary.add_note('software owner close also failed: ' + repr(error))
        assert not socket_path.exists() and len(engine.shutdowns) == len(factories) == 1 and not engine.active
    return dict(status='passed', query_id=config['query_id'], requests=25, saved_answers=25,
        original_messages_equal=True, complete_CPU_token_arrays_equal=True,
        historic_GPU_full_token_arrays_qualified=False, original_relation_sink_evaluator_equal=True,
        real_Arrow_and_AF_UNIX=True, reordered_batches=batches, budget26_rejected_before_generate=True,
        software_owner_closed=True, socket_removed=True, timing=timing,
        model_calls=0, author_DSO_SQL=False, GPU=False,
        quality_scope='historical saved answers only; no new model quality or performance',
        source_identity=query.describe(), effective_prompt_columns=method.columns,
        original_reference=file_identity(Path(originals) / 'output/run/cold/result-ready.json'))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config-dir', 'q3-originals', 'q6-originals', 'output-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args(argv)
    output = args.output_dir.resolve(); new_private_directory(output)
    reports = []
    for query_id in ('3b', '3c', '3d', '3e', '3f', '3g', '6a', '6b', '6c'):
        directory = output / query_id; directory.mkdir(mode=0o700)
        report = dict(status='failed', query_id=query_id, model_calls=0)
        try:
            config = read_json(args.config_dir / ('CONFIG-' + query_id + '.json'))
            report.update(asyncio.run(replay(config,
                args.q6_originals if query_id.startswith('6') else args.q3_originals, directory)))
        except BaseException as error:
            report.update(error_type=type(error).__name__, error=str(error)); raise
        finally: write_private_json(directory / 'CPU.json', report)
        reports.append(report); print(query_id + ': passed 25 saved answers; models 0', flush=True)
    write_private_json(output / 'CPU.json', dict(status='passed', queries=9, saved_answers=225,
        software_generate=225, extra_request_rejections=9, real_models=0, GPU=False, cases=reports))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
