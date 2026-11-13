"""Animals Filter binding: the original LOTUS query owns its ordinary sink.

The caller supplies the qualified native Ray executor and finite query budget.
"""
from copy import deepcopy
import hashlib
import json
import time

from src.baselines.image.sembench_animals import filter_passes


def message_digest(message):
    return hashlib.sha256(json.dumps(message, sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':')).encode()).hexdigest()


def rows_per_pass(physical_attempt_limit, query_id=7):
    passes = filter_passes(query_id)
    if type(physical_attempt_limit) is not int or physical_attempt_limit <= 0 or physical_attempt_limit % passes:
        raise ValueError('Animals Filter requires a finite positive full-query physical attempt limit')
    return physical_attempt_limit // passes


def reconcile(rows, predictions):
    expected = [row['call_id'] for row in rows]
    received = [row['call_id'] for row in predictions]
    if not expected or len(set(expected)) != len(expected):
        raise ValueError('each original Q7 Filter requires unique source occurrences')
    if len(received) != len(expected) or len(set(received)) != len(expected) or set(received) != set(expected):
        raise ValueError('missing, duplicate or unknown native outcome')
    by_id = {row['call_id']: row['generated_text'] for row in predictions}
    return [by_id[key] for key in expected]


class FilterBinding:
    def __init__(self, execute, occurrence_count, *, query_id=7):
        if type(occurrence_count) is not int or occurrence_count <= 0:
            raise ValueError('Animals Filter needs the positive frozen source occurrence count')
        self.execute = execute
        self.occurrence_count = occurrence_count
        self.query_id, self.expected_passes = query_id, filter_passes(query_id)
        self.passes = []
        self.messages = []

    def __call__(self, messages):
        stage = len(self.passes)
        if stage >= self.expected_passes or len(messages) != self.occurrence_count or any(p['status'] != 'complete' for p in self.passes):
            raise ValueError('Animals Filter requires complete frozen-source passes, without retries or warmup')
        rows = [dict(call_id=f'q{self.query_id}-pass-{stage}-row-{i}', messages=deepcopy(message))
                for i, message in enumerate(messages)]
        # Spend before dispatch: a failed stage must not be retried on this object.
        self.passes.append(dict(stage=stage, status='dispatched'))
        self.messages.append([row['messages'] for row in rows])
        result = reconcile(rows, self.execute(rows))
        self.passes[-1]['status'] = 'complete'
        return result

    def require_complete(self):
        if len(self.passes) != self.expected_passes or any(p['status'] != 'complete' for p in self.passes):
            raise ValueError('Animals Filter incomplete; do not publish a partial sink')
        for outcome, messages in zip(self.passes, self.messages, strict=True):
            outcome['message_sha256'] = [message_digest(message) for message in messages]


def run(dataset, execute, *, query_id=7, on_source_started=None, on_result_ready=None,
        physical_attempt_limit=None):
    """Native execution must already enforce the process-wide public-generate cap.

    Original runner calls the original parser and full City intersection itself.
    Reference labels only enter evaluate_original after the query is complete.
    """
    import lotus
    from lotus.models import LM
    from lotus.types import LMOutput
    from src.baselines.image.sembench_animals import bind_original_query, evaluate_original

    source = dataset.preflight(query_id)
    if physical_attempt_limit is not None and rows_per_pass(physical_attempt_limit, query_id) != source['rows']:
        raise ValueError('Animals Filter physical attempt limit differs from the complete frozen source')
    binding = FilterBinding(execute, source['rows'], query_id=query_id)
    execute_query = bind_original_query(dataset.checkout, dataset.bind_frame(), query_id)
    class NativeLM(LM):
        def __call__(self, messages, *args, **kwargs):
            return LMOutput(outputs=binding(messages))
    lm = NativeLM(model='openai/native-ray-binding', temperature=0, max_tokens=8192,
                  max_batch_size=4, cache={}, top_p=1, seed=0, stop=[],
                  frequency_penalty=0, presence_penalty=0, max_ctx_len=32768)
    with lotus.settings.context(lm=lm, enable_cache=False):
        source_started = time.monotonic_ns()
        if on_source_started is not None:
            on_source_started(source_started)
        result = execute_query()
        sink_ready = time.monotonic_ns()
        if on_result_ready is not None:
            on_result_ready(result, sink_ready)
    audit_started = time.monotonic_ns()
    binding.require_complete()
    audit_finished = time.monotonic_ns()
    evaluation_started = time.monotonic_ns()
    quality = evaluate_original(dataset.checkout, result, dataset.frame(references=True), query_id)
    return result, quality, binding.passes, dict(source_started_monotonic_ns=source_started,
        sink_ready_monotonic_ns=sink_ready,
        binding_audit_started_monotonic_ns=audit_started, binding_audit_finished_monotonic_ns=audit_finished,
        evaluation_started_monotonic_ns=evaluation_started, evaluation_finished_monotonic_ns=time.monotonic_ns())
