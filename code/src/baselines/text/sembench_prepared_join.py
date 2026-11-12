"""Original exact LOTUS Join around identity-keyed native backend replies.

The SDK owns pair prompts, Boolean parsing and relation construction. Ray/Daft
consume the captured messages; this module never submits or schedules a model.
"""

from contextlib import contextmanager
from copy import deepcopy
import json
import os
import time

from src.semantic_methods.lotus.records import identity
from .frameworks.lotus_native_join import make_lm, observe_native_join_inputs


@contextmanager
def _answers(lm, answer):
    import lotus

    original = type(lm).__call__
    type(lm).__call__ = answer
    try:
        with lotus.settings.context(lm=lm, enable_cache=False):
            yield
    finally:
        type(lm).__call__ = original


class _Prepared(BaseException):
    pass


def prepare(query):
    operation = query.operation
    lm = make_lm(operation, 1)
    batches = []
    started = time.monotonic_ns()

    def capture(current, messages, *args, **kwargs):
        if current is not lm:
            raise ValueError('native Join preparation invoked another model')
        batches.append(deepcopy(messages))
        raise _Prepared()

    with _answers(lm, capture):
        try:
            operation.frame.sem_join(operation.right_frame, operation.instruction)
        except _Prepared:
            pass
    messages, = batches
    prepared = [dict(identity(query.query_id, record.row_id, record.source_position,
                              algorithm_stage='join-pair'),
        messages_json=json.dumps(message, ensure_ascii=False, allow_nan=False),
        prepare_worker_pid=os.getpid(), prepare_started_ns=started,
        prepare_finished_ns=time.monotonic_ns())
        for record, message in zip(operation.records, messages, strict=True)]
    return lm, prepared


def finish_relation(query, lm, texts, execute_query):
    from lotus.types import LMOutput

    batches, join_inputs = [], []

    def answer(current, messages, *args, **kwargs):
        if current is not lm or batches:
            raise ValueError('the original exact Join must use one bound native LM call')
        batches.append(messages)
        return LMOutput(outputs=texts)

    def execute(operation, invoke):
        with _answers(lm, answer), observe_native_join_inputs(join_inputs):
            frame = invoke()
        return dict(frame=frame)

    result, operator = (query.execute if execute_query is None else execute_query)(execute)
    return result, dict(texts=texts, parsed_outputs=None, operator=operator,
        native_join_inputs=join_inputs, native_join_messages=batches)


def audit(prepared, relation):
    from .frameworks.lotus_native_join import describe_native_join_inputs

    messages, = relation['native_join_messages']
    if messages != [json.loads(row['messages_json']) for row in prepared]:
        raise ValueError('original Join sink reconstructed different model messages')
    return describe_native_join_inputs(relation['native_join_inputs'])
