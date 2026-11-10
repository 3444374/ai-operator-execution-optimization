"""Compare CPU size prediction over one finite, already-ready request frontier.

No model is started. Captured tokens and fixed results are replay inputs, not
evidence that native tokenization or GPU output is deterministic.
"""
import argparse
from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import platform
import sys
import time
from types import SimpleNamespace

from src.execution_provider.adapters.lotus_chain_token_batches import PreparedTokenOrganizer
from src.infrastructure.redact import redact_json_values
from src.scheduling.core.session_contract import OfferedTask, SessionSpec, TaskKey
from src.scheduling.core.session_policy import TaskCandidate
from src.semantic_methods.lotus.record_types import RecordOccurrence, stage_identity
from src.serving.backends.async_engine_wire import _frame
from src.serving.backends.vllm_protocol import AsyncEngineLimits, EngineSpec, GenerationSpec
from .message_frame_sizes import DirectJsonSizeOrganizer, MessageSizeReuseOrganizer


def encoded(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode()


def fixture_frontier(rows, body_chars):
    """Repeated text with distinct occurrences; codepoint IDs are a CPU fixture."""
    body = ('review "quoted" \\ newline\n中文 ' * (body_chars//20+1))[:body_chars]
    messages = [dict(role='system', content='Keep this request unchanged.'),
                dict(role='user', content=body)]
    token_ids = [ord(c) for c in 'user: '+body+'\nassistant:']
    result = []
    for i in range(rows):
        row = dict(stage_identity('cpu-frame-replay', RecordOccurrence('occ-'+str(i), i, 7, {}),
                                  'map'), messages_json=json.dumps(messages, ensure_ascii=False))
        result.append(dict(row=row, generation=asdict(GenerationSpec(max_tokens=8)),
            model=dict(name='scripted-cpu', revision='no-model'), token_ids=token_ids,
            parent_call_ids=[], result=dict(call_id=row['call_id'], text='fixed-'+str(i))))
    return result


class ReplayTokenizer:
    """Return the complete recorded IDs for the exact recorded conversation."""
    def __init__(self, frontier):
        self.tokens, self.calls = {}, 0
        for entry in frontier:
            key = encoded(json.loads(entry['row']['messages_json']))
            if key in self.tokens and self.tokens[key] != entry['token_ids']:
                raise ValueError('one recorded conversation has conflicting token IDs')
            self.tokens[key] = entry['token_ids']

    def apply_chat_template(self, messages, **options):
        self.calls += 1
        return self.tokens[encoded(messages)]


def replay(frontier, organizer_type, *, window, input_bytes, item_input_bytes, item_result_bytes):
    """Use production selection and encoding, then restore fixed occurrence results."""
    tokenizer = ReplayTokenizer(frontier)
    limits = AsyncEngineLimits(window, input_bytes=input_bytes,
        item_input_bytes=item_input_bytes, item_result_bytes=item_result_bytes)
    spec = SessionSpec('replay', 'ready', 'cpu-fixture')
    candidates = tuple(TaskCandidate(TaskKey(0, i), OfferedTask(i,
        encoded(dict(row=entry['row'], generation=entry['generation'],
                     parent_call_ids=entry['parent_call_ids'])), 1, limits.item_result_bytes), spec)
        for i, entry in enumerate(frontier))
    records = {c.key: SimpleNamespace(compute=False, member=None) for c in candidates}
    owner = SimpleNamespace(resources=SimpleNamespace(max_active_batches=limits.max_inflight),
        engine=EngineSpec('/tmp/cpu-frame-replay', sys.executable, sys.executable,
            max_model_len=max(len(e['token_ids'])+e['generation']['max_tokens'] for e in frontier)),
        core=SimpleNamespace(capacity=SimpleNamespace(records=records)),
        _actors=SimpleNamespace(limits=limits, model_ready=None),
        prepared_batching=None, rolling_refill=True, request_feedback=None, deadline=1e9)
    organizer = organizer_type(owner, tokenizer, None)
    pending, batches, results, wire = list(candidates), [], {}, hashlib.sha256()
    while pending:
        selected = organizer(pending)
        if not selected:
            raise RuntimeError('the finite ready frontier made no progress')
        batches.append(tuple(k.sequence for k in selected))
        for key in selected:
            entry = frontier[key.sequence]
            row, generation = entry['row'], entry['generation']
            # The real send encoder still materializes both complete frames.
            wire.update(_frame(dict(op='submit', request_id='replay:'+str(key.sequence),
                row=row, generation=generation, deadline=owner.deadline), limits.item_input_bytes))
            wire.update(_frame(dict(row=row, generation=generation, maximum=limits.item_result_bytes,
                request_key=[key.session_id, key.sequence]), limits.input_bytes))
            call_id = row['call_id']
            if call_id in results or entry['result']['call_id'] != call_id:
                raise ValueError('fixed results must retain unique occurrence identities')
            results[call_id] = entry['result']
            del records[key]
        chosen = set(selected)
        pending = [candidate for candidate in pending if candidate.key not in chosen]
    ordered = [results[e['row']['call_id']] for e in sorted(frontier,
               key=lambda e:e['row']['source_position'])]
    return dict(batches=batches, history=organizer.history, tokenizer_calls=tokenizer.calls,
                wire_sha256=wire.hexdigest(), result_sha256=hashlib.sha256(encoded(ordered)).hexdigest())


def compare(frontier, *, repeats, window, input_bytes, item_input_bytes, item_result_bytes):
    original = deepcopy(frontier)
    arms = dict(original=PreparedTokenOrganizer, direct_json=DirectJsonSizeOrganizer,
                reuse=MessageSizeReuseOrganizer)
    # One unreported warm-up per arm; all measured repeats are retained.
    for constructor in arms.values():
        replay(frontier, constructor, window=window, input_bytes=input_bytes,
               item_input_bytes=item_input_bytes, item_result_bytes=item_result_bytes)
    durations, result = {name: [] for name in arms}, None
    for repeat in range(repeats):
        for name in (tuple(arms) if repeat % 2 == 0 else tuple(reversed(arms))):
            started = time.perf_counter_ns()
            observed = replay(frontier, arms[name], window=window, input_bytes=input_bytes,
                item_input_bytes=item_input_bytes, item_result_bytes=item_result_bytes)
            durations[name].append((time.perf_counter_ns()-started)/1e9)
            if result is not None and observed != result:
                raise AssertionError('size, membership, complete wire or fixed assembly changed')
            result = observed
    if frontier != original:
        raise AssertionError('replay modified a request, tokens, model, parents or fixed result')
    return dict(schema='semloom.message_frame_replay.v1', status='completed', requests=len(frontier),
        input_sha256=hashlib.sha256(encoded(frontier)).hexdigest(),
        python=platform.python_version(), platform=platform.system(), architecture=platform.machine(),
        window=window, input_bytes=input_bytes,
        item_input_bytes=item_input_bytes, item_result_bytes=item_result_bytes,
        seconds=durations, warmups_per_arm=1,
        timing_scope='complete CPU frontier replay: task encoding, recorded-token lookup, production '
            'organization, size prediction, full wire encoding and fixed result assembly',
        source_read_in_timing=False, no_model_calls=True, native_tokenizer_executed=False,
        gpu_metrics=None, host_memory=None,
        unavailable_reason='no GPU execution or isolated process-memory sampling', equivalence=result)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, help='JSON array of an already-ready captured frontier')
    parser.add_argument('--rows', type=int, default=64)
    parser.add_argument('--body-chars', type=int, default=4096)
    parser.add_argument('--window', type=int, default=8)
    for name in ('input-bytes', 'item-input-bytes', 'item-result-bytes'):
        parser.add_argument('--'+name, type=int, required=True)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if min(args.rows, args.body_chars, args.window, args.input_bytes,
           args.item_input_bytes, args.item_result_bytes, args.repeats) < 1:
        parser.error('sizes, window and repeats must be positive')
    started = time.perf_counter_ns()
    frontier = (json.loads(args.input.read_text()) if args.input else
                fixture_frontier(args.rows, args.body_chars))
    source_seconds = (time.perf_counter_ns()-started)/1e9
    if not frontier:
        parser.error('provide one nonempty already-ready frontier')
    report = compare(frontier, repeats=args.repeats, window=args.window, input_bytes=args.input_bytes,
        item_input_bytes=args.item_input_bytes, item_result_bytes=args.item_result_bytes)
    report.update(source_seconds=source_seconds,
                  run_id=args.output.stem,
                  input_kind='captured_ready_frontier' if args.input else 'scripted_codepoint_fixture',
                  fixture_body_chars=None if args.input else args.body_chars)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(redact_json_values(report), indent=2, ensure_ascii=False)+'\n')
    print(json.dumps(dict(status=report['status'], requests=report['requests'], seconds=report['seconds'])))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
