"""Capture a real SemBench initial frontier and attribute CPU preparation costs.

Uses the installed LOTUS formatter and an existing local tokenizer. It never
constructs a serving owner, executes an operator, downloads assets or loads weights.
"""
import argparse
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from statistics import median
import time
from types import SimpleNamespace

from src.execution_provider.adapters.lotus_chain_token_batches import PreparedTokenOrganizer
from src.modalities.text.tokenization import chat_token_count
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS, _prepared
from .message_frame_sizes import direct_json_frame_sizes, reused_message_frame_sizes


class RecordingTokenizer:
    def __init__(self, tokenizer):
        self.tokenizer, self.ids = tokenizer, None

    def apply_chat_template(self, *args, **kwargs):
        result = self.tokenizer.apply_chat_template(*args, **kwargs)
        self.ids = result['input_ids'] if isinstance(result, Mapping) else result
        return result


def distribution(values):
    values = sorted(values)
    return dict(min=values[0], median=median(values),
                p95=values[(95*len(values)+99)//100-1], max=values[-1])


def profile(method, records, tokenizer, model, limits, *, query_id, repeats):
    """Time native preparation and the same pair of size projections separately."""
    generation = asdict(method.stage.generation)
    token = RecordingTokenizer(tokenizer)
    arms = dict(original=PreparedTokenOrganizer._request_frame_sizes,
                direct_json=direct_json_frame_sizes, reuse=reused_message_frame_sizes)
    samples, frontier, stable = [], [], None
    for repeat in range(repeats):
        started = time.perf_counter_ns()
        rows = method.prepare_rows(records, query_id=query_id)
        costs = dict(formatter=time.perf_counter_ns()-started, validation=0, tokenizer=0, envelope=0)
        sizing = {name: 0 for name in arms}
        current, shape = [], dict(messages_utf8_bytes=[], prompt_tokens=[], submit_bytes=[], rpc_bytes=[])
        for i, row in enumerate(rows):
            started = time.perf_counter_ns()
            _, conversations = _prepared([row])
            costs['validation'] += time.perf_counter_ns()-started
            started = time.perf_counter_ns()
            count = chat_token_count(token, conversations[0])
            costs['tokenizer'] += time.perf_counter_ns()-started
            started = time.perf_counter_ns()
            submit = dict(op='submit', request_id='f'*32+':'+str((1 << 63)-1),
                row={k: row[k] for k in (*IDENTITY_FIELDS, 'messages_json')}, generation=generation, deadline=None)
            rpc = dict(row=row, generation=generation, maximum=limits.item_result_bytes, request_key=[0, i])
            costs['envelope'] += time.perf_counter_ns()-started
            reference = None
            names = tuple(arms)
            for name in names[repeat % 3:]+names[:repeat % 3]:
                started = time.perf_counter_ns()
                measured = arms[name](submit, rpc, limits)
                sizing[name] += time.perf_counter_ns()-started
                if reference is not None and measured != reference:
                    raise AssertionError('native request frame sizes changed')
                reference = measured
            current.append(dict(row=row, generation=generation, model=model,
                token_ids=list(token.ids), parent_call_ids=[],
                result=dict(call_id=row['call_id'], status='not_executed', text='cpu-only-placeholder')))
            for name, value in zip(shape, (len(row['messages_json'].encode()), count, *reference), strict=True):
                shape[name].append(value)
        identity = [(tuple(e['row'][k] for k in IDENTITY_FIELDS), e['row']['messages_json'], e['token_ids'])
                    for e in current]
        if stable is not None and identity != stable:
            raise AssertionError('native preparation changed full requests or token IDs between repeats')
        stable = identity
        frontier = current
        total = sum(costs.values())+sizing['original']
        samples.append(dict(**{k+'_seconds': v/1e9 for k, v in costs.items()},
            sizing_seconds={k: v/1e9 for k, v in sizing.items()}, profiled_slice_original_seconds=total/1e9,
            original_sizing_fraction_in_slice=sizing['original']/total))
    return frontier, dict(requests=len(frontier), samples=samples, warmups=0,
        first_repeat='retained; may include lazy template/formatter initialization',
        distributions={k: distribution(v) for k, v in shape.items()},
        duplicate_message_counts=sorted(Counter(e['row']['messages_json'] for e in frontier).values(), reverse=True),
        timing_scope='formatter + prepared validation + native tokenizer + envelope construction + original sizing; '
                     'excludes Core payload encode/decode, source loading, LM/tokenizer construction, emission and file writes',
        quantile='p95 nearest rank', model_calls=0, model_weights_loaded=False, gpu_used=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'sembench-checkout', 'data-dir', 'tokenizer', 'output-dir'):
        parser.add_argument('--'+name, required=True)
    parser.add_argument('--rows', type=int, default=128)
    parser.add_argument('--repeats', type=int, default=5)
    for name in ('item-input-bytes', 'input-bytes', 'item-result-bytes'):
        parser.add_argument('--'+name, type=int, required=True)
    args = parser.parse_args(argv)
    if min(args.rows, args.repeats, args.item_input_bytes, args.input_bytes, args.item_result_bytes) < 1:
        parser.error('row count, repeats and existing byte allowances must be positive')
    import lotus
    from transformers import AutoTokenizer
    from src.experiments.sembench_text_query import configuration, query_from_config
    from src.infrastructure.private_artifacts import new_private_directory, write_private_json
    from src.semantic_methods.lotus.record_method import LotusRecordMethod
    from src.semantic_methods.lotus.source import CHAIN_FILES, source_report

    started = time.perf_counter_ns()
    config, engine, generation, _ = configuration(args.config)
    if engine.chat_template is not None:
        raise ValueError('request profiling requires the tokenizer-owned chat template')
    query = query_from_config(config, args.sembench_checkout, args.data_dir, generation)
    operation = query.operation
    if operation is None or not operation.records:
        parser.error('the original query has no initial ready operator input')
    tokenizer = AutoTokenizer.from_pretrained(args.tokenizer, local_files_only=True)
    with lotus.settings.context(enable_cache=False):
        method = LotusRecordMethod(operation.stage)
        setup = (time.perf_counter_ns()-started)/1e9
        frontier, report = profile(method, operation.records[:args.rows], tokenizer, asdict(engine),
            SimpleNamespace(item_input_bytes=args.item_input_bytes, input_bytes=args.input_bytes,
                            item_result_bytes=args.item_result_bytes),
            query_id=query.query_id, repeats=args.repeats)
    report.update(setup_seconds=setup, query=query.describe(), source=source_report('native-frontier', CHAIN_FILES),
                  tokenizer_template_sha256=hashlib.sha256(json.dumps(tokenizer.chat_template).encode()).hexdigest(),
                  tokenizer_class=type(tokenizer).__name__,
                  tokenizer_revision=tokenizer.init_kwargs.get('_commit_hash'),
                  input_scope='first declared records of one initial ready operator; no model-dependent successor inferred')
    output = Path(args.output_dir)
    new_private_directory(output)
    write_private_json(output/'frontier.json', frontier)
    write_private_json(output/'profile.json', report)
    print(json.dumps(dict(requests=report['requests'], model_calls=0, gpu_used=False)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
