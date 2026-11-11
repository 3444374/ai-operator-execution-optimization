"""Medical Q4 saved-answer replay with explicit occurrence fixture correlation.

The real Arrow/Unix IPC/provider/budget/parser/sink run on CPU. Only author SQL
and generation are fixtures. The fixture binds batch ID and position out of
band, never by message hash or asynchronous arrival, even for repeated prompts.
"""

import argparse
import asyncio
from collections import Counter
from contextvars import ContextVar
from copy import deepcopy
import json
import os
from pathlib import Path
import tempfile
import time
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
from transformers import AutoTokenizer

from integrations.imlane_duckdb import model_ipc_worker as worker, model_provider, mmqa_filter_udf
from integrations.imlane_duckdb.model_wire import SHUTDOWN_SCHEMA
from src.baselines.text.sembench_movie_native import q3_decision
from src.experiments import native_imlane_filters as binding
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.infrastructure.private_artifacts import (
    content_digest, file_identity, json_metric, new_private_directory, read_json, write_private_json,
)
from tests.baselines.text.replay_imlane_filters import timed_relation
from tests.baselines.text.test_imlane_duckdb_model_ipc_worker import FakeEngine, FakeSampling


def reference(originals, phase):
    root = Path(originals) / 'output/run' / phase
    prepared = read_json(root / 'input/prepared.json')
    saved = read_json(root / 'result-ready.json')['result']
    predictions = {row['occurrence_id']: row for row in saved['predictions']}
    assert len(predictions) == len(saved['predictions']) == len(prepared['calls']) == 1200
    messages, texts = [], []
    for row, call in zip(prepared['rows'], prepared['calls'], strict=True):
        prediction = predictions[row['row_id']]
        assert prediction['source_position'] == call['source_position'] == row['source_position']
        message = json.loads(call['messages_json'])
        assert prediction['messages_sha256'] == content_digest(message)
        assert prediction['decision'] == q3_decision(prediction['generated_text'])
        messages.append(message); texts.append(prediction['generated_text'])
    return dict(messages=messages, texts=texts, rows=prepared['rows'], result=saved['frame'],
        quality=read_json(root / 'AUDIT.json')['quality'],
        evidence={name: file_identity(root / name) for name in
                  ('input/prepared.json', 'result-ready.json', 'AUDIT.json')})


async def replay(config, originals, phase, output):
    query, method, prepared, requests, tokens, rows = binding.prepare_query(config)
    frozen = reference(originals, phase)
    assert [row['messages'] for row in requests] == frozen['messages']
    assert [row['source_position'] for row in rows] == list(range(1200))
    for row, old in zip(rows, frozen['rows'], strict=True):
        assert (row['title'], row['text']) == (str(old['patient_id']), old['symptoms'])
    tokenizer = AutoTokenizer.from_pretrained(config['tokenizer'], local_files_only=True, trust_remote_code=False)
    assert tokens == [tokenizer.apply_chat_template(value, tokenize=True, add_generation_prompt=True)
                      for value in frozen['messages']]
    groups = Counter(content_digest(row['messages']) for row in requests)
    assert len(groups) == 1153 and sum(n > 1 for n in groups.values()) == 43
    sampling = query.operation.generation.kwargs()
    budget = AttemptBudget(config['execution_id'] + '-' + phase, 1200)
    ledger = CellBudgetLedger.create(output / 'budget.sqlite', budget, deadline_utc=None)
    ledger.reserve_unit('query', 1200); unit = ledger.claim_shared_unit('query')
    for name in ('model-calls', 'overflow-calls'): (output / name).mkdir(mode=0o700)
    write_private_json(output / 'messages.json', dict(rows=rows))
    write_private_json(output / 'tokens.json', dict(token_ids=tokens))
    source_position = ContextVar('saved_occurrence_position')
    fixture_batches = {}
    positions = {row['occurrence_id']: i for i, row in enumerate(rows)}
    original_batch, original_row = model_provider._run_batch, worker.ModelWorker._row

    def correlate(config, request, receipt):
        # Test-only side channel at the existing positional seam. Nothing is
        # inserted into messages, model request, generation options or prompts.
        fixture_batches[request['batch_id']] = [positions[call['occurrence_id']]
            for call in receipt['calls']] if 'occurrence_id' in receipt['calls'][0] else [0]
        return original_batch(config, request, receipt)

    async def row_fixture(self, request, messages, call):
        position = fixture_batches[request['batch_id']][call['position']]
        token = source_position.set(position)
        try:
            return await original_row(self, request, messages, call)
        finally:
            source_position.reset(token)

    class Renderer:
        async def render_chat_async(self, conversations, _params):
            index = source_position.get()
            assert conversations == [requests[index]['messages']]
            # Deliberately reorder concurrent completions independently of the
            # receipt association. All duplicate raw responses stay distinct.
            await asyncio.sleep((index % 5) / 10000)
            return conversations, [dict(type='token', prompt_token_ids=tokens[index], fixture_index=index)]

    engine, stop = FakeEngine(), asyncio.Event()
    engine.renderer = Renderer(); engine.vllm_config.model_config.max_model_len = 32768
    engine.texts = dict(enumerate(frozen['texts'])); engine.release.set()
    factories = []
    def factory(value):
        factories.append(deepcopy(value))
        return worker.NativeModel(engine, FakeSampling, 'final-only', object(), 'software-no-model', FakeSampling)
    primary = None
    with tempfile.TemporaryDirectory(prefix='imlane-medical-', dir='/tmp') as temporary:
        socket_path = Path(temporary) / 'model.sock'
        worker_config = dict(model='/software-fixture/no-weights', served_model_name='medical-software-fixture',
            dtype='bfloat16', engine_options={}, socket_path=str(socket_path),
            ready_path=str(output / 'ready.json'), shutdown_path=str(output / 'shutdown.json'),
            request_budget=dict(path=str(ledger.path), budget_id=budget.budget_id, limit=1200, unit_id='query'))
        with patch.object(model_provider, '_run_batch', side_effect=correlate), \
                patch.object(worker.ModelWorker, '_row', new=row_fixture):
            task = asyncio.create_task(worker.serve(worker_config, model_factory=factory, stop=stop, install_signals=False))
            try:
                async with asyncio.timeout(5):
                    while not Path(worker_config['ready_path']).exists():
                        if task.done(): task.result()
                        await asyncio.sleep(.001)
                provider = dict(schema=model_provider.CONFIG_SCHEMA, provider_mode='software_ipc',
                    socket_path=str(socket_path), model=worker_config['served_model_name'], sampling_params=sampling,
                    request_timeout_s=120, receipt_dir=str(output / 'model-calls'))
                write_private_json(output / 'provider.json', provider)
                order = list(range(1198, -1, -2)) + list(range(1199, 0, -2))
                batches = [order[i:i + 137] for i in range(0, len(order), 137)]
                async def batch(indices):
                    chosen = [rows[i] for i in indices]
                    table = pa.table({f'c{i}': pa.array([row[key] for row in chosen], type=pa.string())
                                      for i, key in enumerate(('occurrence_id', 'title', 'text'))})
                    values = (await asyncio.to_thread(mmqa_filter_udf.infer_mmqa, table)).column(0).to_pylist()
                    assert values == [int(q3_decision(frozen['texts'][i])) for i in indices]
                    return [dict(occurrence_id=row['occurrence_id'], source_position=row['source_position'], title=row['title'])
                            for row, positive in zip(chosen, values, strict=True) if positive]
                with patch.dict(os.environ, {model_provider.CONFIG_ENV: str(output / 'provider.json'),
                                             mmqa_filter_udf.CONFIG_ENV: str(output / 'messages.json')}):
                    accepted = await asyncio.gather(*(batch(part) for part in batches))
                assert len(engine.calls) == unit.attempts == 1200
                returned = [call for path in (output / 'model-calls').glob('*.terminal.json')
                            for call in read_json(path)['calls']]
                assert len(returned) == len({call['occurrence_id'] for call in returned}) == 1200
                for call in returned:
                    assert call['response']['generated_text'] == frozen['texts'][positions[call['occurrence_id']]]
                for prompt, effective, _, _ in engine.calls:
                    assert vars(effective) == dict(sampling, output_kind='final-only')
                    assert prompt['prompt_token_ids'] == tokens[prompt['fixture_index']]
                bound = binding.FilterRelation(query, method, prepared, requests, tokens, rows)
                timing = timed_relation(bound, [value for part in reversed(accepted) for value in part],
                    output / 'relation', frozen['result'], sampling, output / 'model-calls')
                assert timing['original_quality'] == frozen['quality']
                try:
                    timed_relation(bound, [value for part in accepted for value in part],
                        output / 'failure-relation', frozen['result'], sampling,
                        output / 'model-calls', fail_after_audit=True)
                except ValueError as error:
                    assert str(error) == 'injected post-sink audit failure'
                    assert error.query_report['status'] == 'failed'
                    assert error.query_report['source_to_sink_seconds'] == 3
                    assert error.query_report['sink_release']['outcome'] == 'complete'
                    assert json_metric(error.query_result.to_dict(orient='split')) == frozen['result']
                    write_private_json(output / 'FAILURE.json', error.query_report)
                else:
                    raise AssertionError('post-sink failure was hidden')
                # Compare the entire selected joined frame, not just its mean.
                original_frame = query.operation.frame.iloc[[i for i, text in enumerate(frozen['texts']) if q3_decision(text)]]
                actual = pd.DataFrame(**timing['complete_relation'])
                pd.testing.assert_frame_equal(actual, original_frame, check_dtype=False)
                write_private_json(output / 'overflow-provider.json', dict(provider, receipt_dir=str(output / 'overflow-calls')))
                with patch.dict(os.environ, {model_provider.CONFIG_ENV: str(output / 'overflow-provider.json')}):
                    try: await asyncio.to_thread(model_provider.infer_batch, [requests[0]['messages']])
                    except model_provider.ModelBatchError as error: overflow = error.imlane_model_provider_report
                    else: raise AssertionError('1201st request reached generate')
                assert len(engine.calls) == unit.attempts == 1200
                assert not overflow['calls'][0]['attempted']
                assert overflow['calls'][0]['response']['error_type'] == 'BudgetExhausted'
                assert overflow['engine_generate_invocations'] == 0
                write_private_json(output / 'BUDGET.json', dict(consumed=1200, refunded=0, rejected1201=True))
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
    return dict(status='passed', phase=phase, saved_answers=1200, unique_messages=1153,
        repeated_groups=43, extra_occurrences=47, duplicate_raw_answers_retained=True,
        original_messages_equal=True, complete_CPU_token_arrays_equal=True,
        historical_GPU_full_token_arrays_qualified=False, full_relation_sink_evaluator_equal=True,
        real_Arrow_and_AF_UNIX=True, reversed_source_and_concurrent_batches=True,
        budget1201_rejected_before_generate=True, software_owner_closed=True, socket_removed=True,
        timing=timing, full1200_post_sink_failure_preserves_clock=True, real_model_calls=0, author_DSO_SQL=False, GPU=False,
        quality_scope='saved answers only; no new model quality or performance', reference=frozen['evidence'])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'originals', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args(argv)
    output = args.output.resolve(); new_private_directory(output)
    reports = []
    for phase in ('cold', 'warm'):
        directory = output / phase; directory.mkdir(mode=0o700)
        report = dict(status='failed', real_model_calls=0)
        try:
            report.update(asyncio.run(replay(read_json(args.config), args.originals, phase, directory)))
        except BaseException as error:
            report.update(error_type=type(error).__name__, error=str(error)); raise
        finally:
            write_private_json(directory / 'CPU.json', report)
        reports.append(report); print(phase + ': passed 1200 saved answers; real models 0', flush=True)
    write_private_json(output / 'CPU.json', dict(status='passed', saved_answers=2400, real_model_calls=0,
        source_phases='historical cold/warm, not new live timings', cases=reports))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
