"""Capture an original initial text operator with a local tokenizer, without inference."""

import argparse
from collections.abc import Mapping
from dataclasses import asdict
import json
from pathlib import Path
import time

from src.infrastructure.private_artifacts import (
    content_digest, new_private_directory, write_private_json,
)
from src.infrastructure.redact import redact_text
from src.modalities.text.tokenization import chat_token_count
from src.research.data_organization.prefix_opportunity import analyze, distribution
from src.serving.backends.async_engine_wire import _frame
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS, _prepared


class RecordingTokenizer:
    def __init__(self, tokenizer):
        self.tokenizer, self.ids = tokenizer, None

    def apply_chat_template(self, *args, **kwargs):
        result = self.tokenizer.apply_chat_template(*args, **kwargs)
        self.ids = result.get('input_ids') if isinstance(result, Mapping) else result
        return result


def capture(method, records, tokenizer, *, query_id, repeats, max_capture_bytes):
    """Use the real formatter and one full chat-template application per occurrence."""
    for name, value in (('repeats', repeats), ('max_capture_bytes', max_capture_bytes)):
        if type(value) is not int or value < 1:
            raise ValueError(name + ' must be a positive integer')
    token = RecordingTokenizer(tokenizer)
    generation = asdict(method.stage.generation)
    samples, reference, requests = [], None, []
    for _ in range(repeats):
        started = time.perf_counter_ns()
        prepared = method.prepare_rows(records, query_id=query_id)
        costs = dict(formatter=time.perf_counter_ns() - started, validation=0, tokenizer=0, sizes=0)
        if len(prepared) != len(records):
            raise ValueError('preparation must retain every declared source occurrence')
        if [(row['row_id'], row['source_position']) for row in prepared] != [
                (row.row_id, row.source_position) for row in records]:
            raise ValueError('preparation changed the source occurrences or their order')
        current, total_bytes = [], 0
        for index, row in enumerate(prepared):
            started = time.perf_counter_ns()
            _, conversations = _prepared([row])
            costs['validation'] += time.perf_counter_ns() - started
            started = time.perf_counter_ns()
            count = chat_token_count(token, conversations[0])
            costs['tokenizer'] += time.perf_counter_ns() - started
            started = time.perf_counter_ns()
            snapshot = {name: row[name] for name in (*IDENTITY_FIELDS, 'messages_json')}
            submit = dict(op='submit', request_id='f' * 32 + ':' + str((1 << 63) - 1),
                          row=snapshot, generation=generation, deadline=None)
            item = dict(call_id=row['call_id'], row=row, generation=generation,
                token_ids=list(token.ids), prompt_tokens=count,
                messages_utf8_bytes=len(row['messages_json'].encode('utf-8')),
                representative_submit_frame_bytes=len(_frame(submit, max_capture_bytes)),
                capture_position=index)
            total_bytes += len(json.dumps(item, ensure_ascii=False).encode('utf-8'))
            if total_bytes > max_capture_bytes:
                raise ValueError('captured requests exceed max_capture_bytes')
            costs['sizes'] += time.perf_counter_ns() - started
            current.append(item)
        digest = content_digest([dict(row={name: item['row'][name] for name in
            (*IDENTITY_FIELDS, 'messages_json')}, generation=item['generation'], token_ids=item['token_ids'])
            for item in current])
        if reference is not None and digest != reference:
            raise ValueError('formatter or tokenizer changed complete requests between repeats')
        reference, requests = digest, current
        samples.append({name + '_seconds': value / 1e9 for name, value in costs.items()})
    return dict(schema='semloom.semantic_request_capture.v1', requests=requests,
        request_digest=reference, samples=samples, inference_calls=0, model_weights_loaded=False,
        gpu_used=False, warmup_repeats=0,
        digest_scope='occurrence identity, complete messages, generation and token IDs; excludes prepare timing',
        first_repeat='retained, including lazy formatter/template initialization',
        size_scope='representative submit envelope with fixed request ID and deadline=None; '
                   'includes four-byte frame header, not observed runtime traffic',
        timing_scope='formatter, prepared validation, tokenizer, representative sizing; '
                     'excludes setup, source reading, digest verification and output writing',
        distributions={key: distribution([item[key] for item in requests]) for key in
            ('prompt_tokens', 'messages_utf8_bytes', 'representative_submit_frame_bytes')})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'sembench-checkout', 'data-dir', 'tokenizer', 'output-dir'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--max-rows', type=int, required=True)
    parser.add_argument('--block-tokens', type=int, action='append', required=True,
                        help='explicit block-size hypothesis; not detected GPU configuration')
    parser.add_argument('--window-rows', type=int, default=64)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--max-capture-bytes', type=int, default=64 * 1024**2)
    args = parser.parse_args(argv)
    if (min(args.max_rows, args.window_rows, args.repeats, args.max_capture_bytes, *args.block_tokens) < 1
            or len(set(args.block_tokens)) != len(args.block_tokens)):
        parser.error('counts must be positive and block sizes distinct')
    output = Path(args.output_dir).resolve()
    new_private_directory(output)
    started = time.perf_counter_ns()
    try:
        import lotus
        from transformers import AutoTokenizer
        from src.experiments.sembench_text_query import configuration, query_from_config
        from src.semantic_methods.lotus.record_method import LotusRecordMethod
        from src.semantic_methods.lotus.source import CHAIN_FILES, source_report

        config, engine, generation, _ = configuration(args.config)
        if engine.chat_template is not None:
            raise ValueError('request profiling requires the tokenizer-owned chat template')
        query = query_from_config(config, args.sembench_checkout, args.data_dir, generation)
        operation = query.operation
        if operation is None:
            raise ValueError('the original query has no declared initial operator')
        tokenizer_path = Path(args.tokenizer).resolve()
        if not tokenizer_path.is_dir():
            raise ValueError('tokenizer must name an existing local directory')
        tokenizer = AutoTokenizer.from_pretrained(str(tokenizer_path), local_files_only=True,
                                                  trust_remote_code=False)
        with lotus.settings.context(enable_cache=False):
            method = LotusRecordMethod(operation.stage)
            setup_seconds = (time.perf_counter_ns() - started) / 1e9
            captured = capture(method, operation.records[:args.max_rows], tokenizer,
                query_id=query.query_id, repeats=args.repeats, max_capture_bytes=args.max_capture_bytes)
        captured.update(query=query.describe(), engine=asdict(engine), setup_seconds=setup_seconds,
            source=source_report('initial-request-profile', CHAIN_FILES),
            tokenizer=dict(path=str(tokenizer_path), tokenizer_class=type(tokenizer).__name__,
                template_digest=content_digest(tokenizer.chat_template),
                vocabulary_digest=content_digest(tokenizer.get_vocab()),
                backend_digest=(content_digest(tokenizer.backend_tokenizer.to_str())
                                if tokenizer.is_fast else None),
                special_tokens_digest=content_digest(tokenizer.special_tokens_map),
                revision=tokenizer.init_kwargs.get('_commit_hash')),
            input_scope='first declared occurrences of one initial operator; '
                        'original source snapshot is fully read; no model-dependent successors',
            complete_initial_operator=len(operation.records) <= args.max_rows)
        captured['profile_elapsed_seconds_before_write'] = (time.perf_counter_ns() - started) / 1e9
        write_private_json(output / 'requests.json', captured)
        analysis = analyze(captured['requests'], block_sizes=args.block_tokens, window_rows=args.window_rows)
        analysis['request_digest'] = captured['request_digest']
        analysis['complete_initial_operator'] = captured['complete_initial_operator']
        analysis['profile_elapsed_seconds_before_analysis_write'] = (time.perf_counter_ns() - started) / 1e9
        write_private_json(output / 'opportunity.json', analysis)
        print(json.dumps(dict(status='completed', requests=analysis['requests'], inference_calls=0,
                              complete_initial_operator=captured['complete_initial_operator'])))
        return 0
    except Exception as error:
        report = dict(status='failed', error_type=type(error).__name__, error=redact_text(str(error)),
                      inference_calls=0, elapsed_seconds=(time.perf_counter_ns() - started) / 1e9)
        write_private_json(output / 'failure.json', report)
        print(json.dumps(report, ensure_ascii=False))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
