"""Observe author quick TopK comparisons over the existing native model owner.

LOTUS retains pivot selection, partitions, batch completion, parsing and ranking.
Only request identity/budget and post-sink replay belong to this adapter.
"""
from contextlib import contextmanager
from copy import deepcopy
import importlib
import time
from types import SimpleNamespace

from src.infrastructure.private_artifacts import content_digest
from src.semantic_methods.lotus.records import identity
from .lotus_native_stage import _observe_messages


def make_lm(operation, concurrency):
    from lotus.models import LM

    generation = operation.generation
    options = dict(max_tokens=generation.max_tokens, temperature=generation.temperature,
        top_p=generation.top_p, stop=list(generation.stop),
        presence_penalty=generation.presence_penalty, frequency_penalty=generation.frequency_penalty)
    if generation.seed is not None:
        options['seed'] = generation.seed
    return LM(operation.model_name, max_batch_size=concurrency, **options)


@contextmanager
def comparisons(operation, lm, batch):
    """Bind document object identity, including equal-valued source occurrences."""
    module = importlib.import_module('lotus.sem_ops.sem_topk')
    quick, compare = module.llm_quicksort, module.compare_batch_binary
    positions, seen = {}, set()
    entered = False

    def observed_quick(docs, model, instruction, K, **options):
        nonlocal entered
        if entered or model is not lm or K != len(operation.records):
            raise ValueError('TopK changed its full ranking or model owner')
        entered = True
        # The original accessor builds this list in source order. Do not run a
        # duplicate formatter prepass inside the measured query; post-sink
        # replay checks every complete message against the frozen source.
        if len(docs) != len(operation.records) or len({id(doc) for doc in docs}) != len(docs):
            raise ValueError('TopK documents lost their source occurrence identity')
        positions.update((id(doc), i) for i, doc in enumerate(docs))
        return quick(docs, model, instruction, K, **options)

    def observed_compare(pairs, model, instruction, strategy=None):
        if model is not lm or strategy is not None:
            raise ValueError('TopK comparison changed its ordinary native model strategy')
        source_pairs = [(positions[id(left)], positions[id(right)]) for left, right in pairs]
        for left, right in source_pairs:
            key = tuple(sorted((left, right)))
            if left == right or key in seen:
                raise ValueError('native quick full ranking repeated a source pair')
            seen.add(key)
        return batch(source_pairs, lambda: compare(pairs, model, instruction, strategy=strategy))

    module.llm_quicksort, module.compare_batch_binary = observed_quick, observed_compare
    try:
        yield
        if not entered:
            raise RuntimeError('original TopK did not enter its quick algorithm')
    finally:
        module.llm_quicksort, module.compare_batch_binary = quick, compare


def execute_native_topk(operation, *, invoke, shared_owner, query_id, budget):
    """Borrow one owner; each original partition gets an ordered native binding."""
    import lotus
    import numpy as np

    owner = shared_owner
    if owner.generation != operation.generation:
        raise ValueError('TopK generation differs from the bound native owner')
    maximum = operation.describe()['expected_model_calls']
    if budget.remaining < maximum:
        raise ValueError('TopK requires its worst-case complete comparison allowance')
    lm = make_lm(operation, owner.concurrency)
    rng = np.random.get_state()
    report = dict(status='failed', original_operators_used=True, production_core_used=False,
        scheduler_owner='original LOTUS quick / LM / LiteLLM batch_completion',
        request_upper_bound=maximum, calls=[], pairs=[], partitions=[],
        rng_state=[rng[0], rng[1].tolist(), int(rng[2]), int(rng[3]), float(rng[4])])

    def batch(source_pairs, compare):
        batch_id = query_id + '-partition-' + str(len(report['partitions']))
        expected = [identity(batch_id, 'pair-' + str(left) + '-' + str(right),
            len(report['pairs']) + i, algorithm_stage='topk-compare')
            for i, (left, right) in enumerate(source_pairs)]
        entries = [dict(call_id=call['call_id'], left=left, right=right)
            for call, (left, right) in zip(expected, source_pairs, strict=True)]
        report['pairs'].extend(entries)
        if len(report['pairs']) > maximum:
            raise RuntimeError('native TopK exceeded its proved comparison upper bound')
        binding = primary = None

        def capture(prompts):
            for entry, prompt in zip(entries, prompts, strict=True):
                entry['messages'] = deepcopy(prompt['messages'])
            binding.capture_prompts(prompts)

        try:
            with owner.query(lm, expected, budget, query_id=batch_id) as binding:
                if 'model_ready_observed_ns' not in report:
                    report['model_ready_observed_ns'] = time.monotonic_ns()
                records = [SimpleNamespace(row_id=call['row_id']) for call in expected]
                with _observe_messages(lm, records, capture):
                    answer = compare()
                counts = binding.report()['calls']['counts']
                if counts['succeeded'] != len(expected) or counts['missing']:
                    raise RuntimeError('native TopK comparison lacks a confirmed terminal')
                return answer
        except BaseException as error:
            primary = error
            raise
        finally:
            if binding is not None:
                try:
                    native = binding.report()
                    report['calls'].extend(native['calls']['records'])
                    report['partitions'].append(native)
                except BaseException as accounting:
                    report['accounting_error_type'] = type(accounting).__name__
                    if primary is None:
                        raise
                    primary.add_note('TopK accounting failed: ' + type(accounting).__name__)

    try:
        if maximum:
            owner.start()
            report['model_ready_observed_ns'] = time.monotonic_ns()
        with lotus.settings.context(lm=lm, enable_cache=False), comparisons(operation, lm, batch):
            frame, stats = invoke()
        if stats['total_llm_calls'] != len(report['calls']):
            raise RuntimeError('native TopK stats differ from its confirmed comparison calls')
        report.update(status='completed', native_stats=stats)
        return dict(report, frame=frame)
    except BaseException as error:
        error.operator_report = report
        raise


def complete_topk_calls(operation, operator):
    calls, pairs = operator['calls'], operator['pairs']
    by_id = {call['call_id']: call for call in calls}
    if (len(by_id) != len(calls) or len(calls) != len(pairs)
            or len(calls) > operation.describe()['expected_model_calls']
            or set(by_id) != {pair['call_id'] for pair in pairs}
            or any(call['status'] != 'succeeded' for call in calls)):
        raise RuntimeError('TopK lacks unique successful comparison occurrences')
    for pair in pairs:
        if by_id[pair['call_id']]['messages_sha256'] != content_digest(pair['messages']):
            raise ValueError('native TopK message identity differs from its source pair')
    return calls


def audit(query, operator, result, relation):
    """After sink, replay the same author algorithm/RNG using its actual replies."""
    import lotus
    from lotus.types import LMOutput
    import numpy as np
    from src.baselines.text.sembench_prepared import result_value

    complete_topk_calls(query.operation, operator)
    calls = {call['call_id']: call for call in operator['calls']}
    cursor, restored_relations = 0, []
    lm = make_lm(query.operation, 1)
    original_call = type(lm).__call__
    state = operator['rng_state']
    previous_rng = np.random.get_state()

    def batch(source_pairs, compare):
        nonlocal cursor
        saved = operator['pairs'][cursor:cursor + len(source_pairs)]
        if [(p['left'], p['right']) for p in saved] != source_pairs:
            raise ValueError('TopK replay changed its source comparison sequence')
        cursor += len(source_pairs)

        def answer(current, messages, *args, **kwargs):
            if current is not lm or messages != [p['messages'] for p in saved]:
                raise ValueError('TopK original formatter changed its complete pair messages')
            return LMOutput(outputs=[calls[p['call_id']]['text'] for p in saved])

        type(lm).__call__ = answer
        try:
            return compare()
        finally:
            type(lm).__call__ = original_call

    def execute(operation, invoke):
        with lotus.settings.context(lm=lm, enable_cache=False), comparisons(operation, lm, batch):
            frame, stats = invoke()
        restored_relations.append(frame.copy(deep=True))
        return dict(frame=frame, native_stats=stats)

    try:
        np.random.set_state((state[0], np.array(state[1], dtype=np.uint32), *state[2:]))
        expected, _ = query.execute(execute)
    finally:
        np.random.set_state(previous_rng)
        type(lm).__call__ = original_call
    if (cursor != len(operator['pairs']) or len(restored_relations) != 1
            or result_value(restored_relations[0]) != result_value(relation)
            or result_value(expected) != result_value(result)):
        raise ValueError('TopK native relation or full original sink differs from saved-comparison replay')
