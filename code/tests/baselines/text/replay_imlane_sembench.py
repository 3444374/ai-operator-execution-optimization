"""Replay frozen MMQA Q1 answers through real Arrow/Unix IPC, without a model.

The renderer/engine and author SQL process are explicit software fixtures. The
original formatter, tokenizer, parser, full query and evaluator are real code.
Saved answers are keyed by original message identity, never arrival order.
"""

import argparse
import asyncio
from copy import deepcopy
import json
import os
from pathlib import Path
import tempfile
import time
import traceback
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa

from integrations.imlane_duckdb import model_ipc_worker as worker, model_provider, sembench_udf
from integrations.imlane_duckdb.model_wire import SHUTDOWN_SCHEMA
from src.baselines.text import sembench_prepared
from src.experiments import native_imlane_relation as relation
from src.experiments import native_imlane_sembench as binding
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.infrastructure.private_artifacts import (
    content_digest, file_identity, new_private_directory, publish_private_json,
    read_json, write_private_json,
)
from tests.baselines.text.test_imlane_duckdb_model_ipc_worker import FakeEngine, FakeSampling


class IdentityRenderer:
    def __init__(self, requests, tokens):
        self.rows = {content_digest(row['messages']): (index, row['messages'], token_ids)
                     for index, (row, token_ids) in enumerate(zip(requests, tokens, strict=True))}
        self.calls = []

    async def render_chat_async(self, conversations, chat_params):
        assert len(conversations) == 1
        index, messages, token_ids = self.rows[content_digest(conversations[0])]
        assert conversations[0] == messages
        self.calls.append(dict(source_position=index, messages=deepcopy(messages), token_ids=token_ids))
        await asyncio.sleep(0)
        return conversations, [dict(type='token', prompt_token_ids=token_ids, fixture_index=index)]


def timed_relation(query, method, prepared, predictions, directory, reference, reference_relation):
    """Use the maintained ACK function with an explicitly fake author process."""
    directory.mkdir(mode=0o700)
    clock, events = [100_000_000_000], []
    scope = SimpleNamespace(env={}, children=[], track_root=Mock(), check=Mock())
    process = SimpleNamespace(returncode=None)
    release = directory / 'sink-release.ack'
    native = dict(status='query_returned', variant=binding.VARIANT, return_type='VARCHAR',
        full_projection_materialized=True, source_rows=len(prepared), mapped_occurrences=predictions,
        result_clock='CLOCK_MONOTONIC', result_ready_monotonic_ns=101_000_000_000,
        sink_handoff=dict(protocol=relation.ACK_PROTOCOL, release_path=str(release)))
    bound = binding.PreparedRelation(query, method, prepared, execute_query=query.bind_execution())
    audit_clock_calls = dict(message_digest=0, relation_serialization=0)
    original_digest, original_value = sembench_prepared.content_digest, sembench_prepared.result_value

    class Scope:
        def __enter__(self):
            events.append('author-open')
            return scope

        def __exit__(self, *_args):
            events.append('author-close')
            clock[0] += 20_000_000_000

    def poll():
        if process.returncode is None and release.exists():
            assert release.read_text().splitlines()[1].startswith('complete ')
            events.append('ACK-observed-and-author-exit')
            process.returncode = 0
            clock[0] += 1_000_000_000
        return process.returncode

    process.poll = poll
    process.wait = lambda **_kwargs: poll()

    def spawn(*_args):
        publish_private_json(directory / 'query.json', native)
        clock[0] = 102_000_000_000
        return process, Mock()

    def sink(value):
        assert process.returncode is None and not release.exists()
        result = bound.sink(value)
        assert original_value(result) == reference['result']
        events.append('full-original-memory-sink')
        clock[0] = 103_000_000_000
        return result

    def slow_digest(value):
        audit_clock_calls['message_digest'] += 1
        clock[0] += 7_000_000_000
        return original_digest(value)

    def slow_serialization(value):
        audit_clock_calls['relation_serialization'] += 1
        clock[0] += 11_000_000_000
        return original_value(value)

    def audit(_native, result):
        assert process.returncode == 0 and release.exists()
        events.append('post-sink-audit')
        with patch.object(sembench_prepared, 'content_digest', side_effect=slow_digest), \
                patch.object(sembench_prepared, 'result_value', side_effect=slow_serialization):
            semantic = bound.audit()
        assert semantic['operator_relation'] == reference_relation['semantic']['operator_relation']
        assert semantic['parsed_outputs'] == reference_relation['semantic']['parsed_outputs']
        assert [row['text'] for row in semantic['calls']] == reference['texts']
        events.append('original-evaluation')
        quality = query.evaluate(result)
        assert quality == reference['quality']
        clock[0] += 13_000_000_000
        return dict(semantic=semantic, quality=quality)

    report = {}
    window = SimpleNamespace(check=Mock(), accept_ready=Mock(), begin_cleanup=Mock(return_value=1e12))
    with patch.object(relation, 'DuckDBScope', return_value=Scope()), \
            patch.object(relation.owner, 'spawn', side_effect=spawn), \
            patch.object(relation.owner, 'resource_state', return_value={'software_author_closed': True}), \
            patch.object(relation.time, 'monotonic_ns', side_effect=lambda: clock[0]):
        result, audited = relation.run_relation_query(SimpleNamespace(output=directory), report,
            kernel=Path(sembench_udf.__file__), command=['software-author-fixture'], window=window,
            query_started_ns=100_000_000_000, sink=sink, audit=audit, check=lambda: None)
        assert report['source_to_sink_seconds'] == 3.0
        assert report['result_ready_monotonic_ns'] == 103_000_000_000
        assert report['sink_release']['published_monotonic_ns'] <= report['audit_started_monotonic_ns']
        assert report['audit_started_monotonic_ns'] == 104_000_000_000
        assert audit_clock_calls == dict(message_digest=25, relation_serialization=1)
        assert events == ['author-open', 'full-original-memory-sink', 'ACK-observed-and-author-exit',
                          'post-sink-audit', 'original-evaluation', 'author-close']
        relation.persist_relation_observation(directory, report, result)
    write_private_json(directory / 'semantic-audit.json', audited)
    return dict(report=report, events=events, controlled_seconds=3.0,
        injected_audit_seconds=25 * 7 + 11, injected_evaluation_seconds=13,
        audit_clock_calls=audit_clock_calls, full_relation_equal=True,
        original_sink_equal=True, evaluator_equal=True, quality_is_saved_reply_replay=True)


async def replay(config, reference, reference_relation, output):
    query, method, prepared, requests, tokens = binding.prepare_query(config)
    assert [row['source_position'] for row in prepared] == list(range(25))
    assert [row['messages'] for row in requests] == [json.loads(row['messages_json']) for row in reference['calls']]
    assert tokens == reference['token_ids']
    assert len({content_digest(row['messages']) for row in requests}) == 25
    sampling = query.operation.generation.kwargs()
    assert sampling['structured_outputs'] == {'json_object': True}
    budget = AttemptBudget(config['query_id'], 25)
    ledger = CellBudgetLedger.create(output / 'physical-budget.sqlite', budget, deadline_utc=None)
    ledger.reserve_unit('query', 25)
    unit = ledger.claim_shared_unit('query')
    for name in ('model-calls', 'overflow-calls'):
        (output / name).mkdir(mode=0o700)
    engine, stop = FakeEngine(), asyncio.Event()
    engine.renderer = IdentityRenderer(requests, tokens)
    engine.vllm_config.model_config.max_model_len = 16384
    engine.texts = dict(enumerate(reference['texts']))
    engine.release.set()
    model_factories = []

    def factory(config):
        model_factories.append(deepcopy(config))
        return worker.NativeModel(engine, FakeSampling, 'final-only', object(),
                                  'software-fixture-no-vllm', FakeSampling)

    primary = None
    with tempfile.TemporaryDirectory(prefix='imlane-q1-', dir='/tmp') as temporary:
        socket_path = Path(temporary) / 'model.sock'
        worker_config = dict(model='/software-fixture/no-weights', served_model_name='q1-software-fixture',
            dtype='bfloat16', engine_options={}, socket_path=str(socket_path),
            ready_path=str(output / 'ready.json'), shutdown_path=str(output / 'shutdown.json'),
            request_budget=dict(path=str(ledger.path), budget_id=budget.budget_id, limit=25, unit_id='query'))
        task = asyncio.create_task(worker.serve(worker_config, model_factory=factory, stop=stop,
                                                install_signals=False))
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
            assert sorted(order) == list(range(25))
            batches = [order[:7], order[7:16], order[16:]]

            async def batch(positions):
                rows = [prepared[index] for index in positions]
                table = pa.table({'c0': pa.array([row['call_id'] for row in rows], type=pa.string()),
                                  'c1': pa.array([row['messages_json'] for row in rows], type=pa.string())})
                result = await asyncio.to_thread(sembench_udf.infer_prepared, table)
                assert result.schema == sembench_udf.OUTPUT
                texts = result.column(0).to_pylist()
                assert texts == [reference['texts'][index] for index in positions]
                return [dict(call_id=row['call_id'], generated_text=text)
                        for row, text in zip(rows, texts, strict=True)]

            with patch.dict(os.environ, {model_provider.CONFIG_ENV: str(output / 'provider.json')}):
                replies = await asyncio.gather(*(batch(positions) for positions in batches))
            predictions = [row for batch_reply in reversed(replies) for row in batch_reply]
            assert len(engine.calls) == 25 and unit.attempts == 25 and unit.remaining == 0
            for prompt, effective, _identity, _started in engine.calls:
                values = vars(effective).copy()
                structured = values.pop('structured_outputs')
                assert vars(structured) == {'json_object': True}
                assert values == {**{key: value for key, value in sampling.items() if key != 'structured_outputs'},
                                  'output_kind': 'final-only'}
                assert prompt['prompt_token_ids'] == tokens[prompt['fixture_index']]
            ipc = binding.audit_ipc(output / 'model-calls', requests, tokens, predictions, sampling)
            write_private_json(output / 'IPC-AUDIT.json', ipc)
            timing = timed_relation(query, method, prepared, predictions, output / 'relation', reference,
                                    reference_relation)
            write_private_json(output / 'TIMING.json', timing)
            provider['receipt_dir'] = str(output / 'overflow-calls')
            write_private_json(output / 'overflow-provider.json', provider)
            with patch.dict(os.environ, {model_provider.CONFIG_ENV: str(output / 'overflow-provider.json')}):
                try:
                    await asyncio.to_thread(model_provider.infer_batch, [requests[0]['messages']])
                except model_provider.ModelBatchError as error:
                    overflow = error.imlane_model_provider_report
                else:
                    raise AssertionError('the 26th software request reached native generate')
            assert len(engine.calls) == 25 and unit.attempts == 25
            assert overflow['status'] == 'failed'
            assert overflow['calls'][0]['attempted'] is False
            assert overflow['calls'][0]['response']['error_type'] == 'BudgetExhausted'
            assert overflow['engine_generate_invocations'] == 0
            ledger.close_shared_unit('query')
            write_private_json(output / 'BUDGET.json', dict(allocated=25, spent=unit.attempts,
                refunded=0, next_request_rejected=True, software_generate_invocations=len(engine.calls),
                overflow_response=overflow['calls'][0]['response']))
        except BaseException as error:
            primary = error
            raise
        finally:
            write_private_json(output / 'shutdown.json',
                dict(schema=SHUTDOWN_SCHEMA, native_shutdown_deadline=time.monotonic() + 5))
            stop.set()
            try:
                await asyncio.wait_for(asyncio.shield(task), 5)
            except BaseException as error:
                if primary is None: raise
                primary.add_note('software owner close also failed: ' + repr(error))
        assert not socket_path.exists() and len(engine.shutdowns) == 1 and not engine.active
        assert len(engine.drained) == 25 and len(model_factories) == 1
    return dict(status='passed', requests=25, original_message_and_token_arrays_equal=True,
        each_source_saved_text_equal=True, complete_relation_sink_evaluator_equal=True,
        reordered_batches=batches, software_generate_invocations=25, next_request_rejected=True,
        real_model_calls=0, GPU=False, native_IMLane_DSO_SQL_executed=False, vllm_SDK_loaded=False,
        sampler='software types; original generation values and JSON-object translation verified',
        software_owner=dict(factories=1, shutdowns=1, active_generators=0, socket_removed=True),
        timing=timing, quality_reference=reference['quality'],
        quality_scope='saved historical responses only; not new IMLane model quality or performance')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'reference', 'reference-relation', 'output-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args(argv)
    output = args.output_dir.resolve(); new_private_directory(output)
    report = dict(status='failed', model_calls=0, GPU=False,
        config=file_identity(args.config), reference=file_identity(args.reference),
        reference_relation=file_identity(args.reference_relation))
    try:
        report.update(asyncio.run(replay(read_json(args.config), read_json(args.reference),
                                        read_json(args.reference_relation), output)))
    except BaseException as error:
        report.update(error_type=type(error).__name__, error=str(error), traceback=traceback.format_exc())
        raise
    finally:
        write_private_json(output / 'CPU.json', report)
    print(json.dumps({key: report[key] for key in ('status', 'requests', 'software_generate_invocations',
        'next_request_rejected', 'real_model_calls', 'native_IMLane_DSO_SQL_executed')}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
