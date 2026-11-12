"""Observe original LOTUS exact Join using the existing resident model owner.

The author owns pair construction, formatting, LM batching, parsing and the
joined relation. This adapter binds ordered source occurrences and post-sink
replay; it neither builds an alternative execution graph nor prunes pairs.
"""
from copy import deepcopy
import time

from src.infrastructure.private_artifacts import content_digest
from src.semantic_methods.lotus.records import identity
from .lotus_native_stage import _observe_messages


def make_lm(operation, concurrency):
    from lotus.models import LM

    # The Join accessor owns its :left/:right instruction syntax; a unary
    # Filter method must not parse or rewrite it just to construct the LM.
    generation = operation.generation
    options = dict(max_tokens=generation.max_tokens, temperature=generation.temperature,
        top_p=generation.top_p, stop=list(generation.stop), presence_penalty=generation.presence_penalty,
        frequency_penalty=generation.frequency_penalty)
    if generation.seed is not None:
        options['seed'] = generation.seed
    return LM(operation.model_name, max_batch_size=concurrency, **options)


def finish_accounting(report, *, require_complete=True):
    """Transfer receipt accounting after full sink, or preserve it on failure."""
    binding = report.pop('_native_binding')
    try:
        report.update(binding.report())
        counts = report['calls']['counts']
        if require_complete and (counts['succeeded'] != report['expected_model_calls'] or counts['missing']):
            raise RuntimeError('native Join lacks a confirmed result for every ordered pair')
        if require_complete:
            report['status'] = 'completed'
    except BaseException as error:
        report['accounting_error_type'] = type(error).__name__
        error.operator_report = report
        raise


def execute_native_join(operation, *, invoke, shared_owner, query_id, budget):
    import lotus

    owner = shared_owner
    if owner.generation != operation.generation:
        raise ValueError('Join generation differs from its bound native owner')
    expected = [identity(query_id, record.row_id, record.source_position, algorithm_stage='join-pair')
                for record in operation.records]
    if budget.remaining < len(expected):
        raise ValueError('Join requires its complete ordered-pair allowance')
    lm = make_lm(operation, owner.concurrency)
    report = dict(status='failed', original_operators_used=True, production_core_used=False,
        scheduler_owner='original LOTUS sem_join / LM / LiteLLM batch_completion',
        expected_model_calls=len(expected), prompts=[])
    binding = None

    def capture(prompts):
        report['prompts'] = deepcopy(prompts)
        binding.capture_prompts(prompts)

    try:
        with owner.query(lm, expected, budget, query_id=query_id) as binding:
            report['model_ready_observed_ns'] = time.monotonic_ns()
            with lotus.settings.context(lm=lm, enable_cache=False), \
                    _observe_messages(lm, operation.records, capture):
                frame = invoke()
        # Keep this inactive binding alive until the original full query returns.
        # Its report constructs per-pair accounting, which belongs after sink.
        report.update(status='native_returned', _native_binding=binding)
    except BaseException as error:
        if binding is not None:
            report['_native_binding'] = binding
            try:
                finish_accounting(report, require_complete=False)
            except BaseException as accounting:
                error.add_note('Join accounting failed: ' + type(accounting).__name__)
        error.operator_report = report
        raise
    return dict(report, frame=frame)


def audit(query, operator, result, relation):
    """After the full sink, replay original Join and query with their raw replies."""
    import lotus
    from lotus.types import LMOutput
    from src.baselines.text.sembench_prepared import result_value

    calls = {call['row_id']: call for call in operator['calls']['records']}
    messages, texts = [], []
    for record, prompt in zip(query.operation.records, operator['prompts'], strict=True):
        call = calls[record.row_id]
        if (prompt['row_id'] != record.row_id or call['status'] != 'succeeded'
                or call['messages_sha256'] != content_digest(prompt['messages'])):
            raise ValueError('Join messages lost their ordered source-pair identity')
        messages.append(prompt['messages'])
        texts.append(call['text'])
    lm = make_lm(query.operation, 1)
    invoked = False

    def answer(current, actual, **kwargs):
        nonlocal invoked
        if current is not lm or invoked or actual != messages:
            raise ValueError('original Join replay changed its complete pair messages')
        invoked = True
        return LMOutput(outputs=texts)

    def execute(operation, invoke):
        original = type(lm).__call__
        try:
            type(lm).__call__ = answer
            with lotus.settings.context(lm=lm, enable_cache=False):
                frame = invoke()
        finally:
            type(lm).__call__ = original
        if result_value(frame) != result_value(relation):
            raise ValueError('original Join replay changed its complete native relation')
        return dict(frame=frame)

    replayed, _ = query.execute(execute)
    if not invoked or result_value(replayed) != result_value(result):
        raise ValueError('original Join replay changed its complete query sink')
