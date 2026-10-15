"""Observe original Generator outcomes separately from actual HTTP reservations."""
from copy import deepcopy
import hashlib
import threading
import time

from src.baselines.common.private_artifacts import content_digest


def _value(value, name, default=None):
    return value.get(name, default) if isinstance(value, dict) else getattr(value, name, default)


def _usage(response):
    usage = _value(response, 'usage')
    if hasattr(usage, 'model_dump'):
        usage = usage.model_dump()
    if (type(usage) is not dict or any(type(usage.get(name)) is not int or usage[name] < 0
            for name in ('prompt_tokens', 'completion_tokens'))):
        return dict(usage_status='unavailable', prompt_tokens=None, output_tokens=None)
    return dict(usage_status='available', prompt_tokens=usage['prompt_tokens'],
                output_tokens=usage['completion_tokens'])


class PalimpzestLedger:
    """Thread-safe bounded observer; no request submission or parsing decisions."""
    def __init__(self, *, max_calls, record=None):
        if type(max_calls) is not int or max_calls < 1:
            raise ValueError('PZ observation requires a positive call bound')
        self.max_calls, self.record = max_calls, record
        self._calls, self._lock = {}, threading.Lock()

    def _emit(self, event, call):
        if self.record is not None:
            self.record(dict(event=event, **deepcopy(call)))

    def started(self, call_id, kwargs, row_id=None):
        with self._lock:
            if call_id in self._calls or len(self._calls) >= self.max_calls:
                raise ValueError('duplicate or excessive PZ completion observation')
            call = dict(call_id=call_id, source_indices=list(row_id or ()),
                        submitted_ns=time.monotonic_ns(), completed_ns=None,
                        requested_model=kwargs.get('model'),
                        messages_sha256=content_digest(kwargs.get('messages')),
                        completion_failed=False, completion_error_type=None,
                        parse_status='not_attempted', parse_error_type=None,
                        generator_finished=False, generator_error_type=None,
                        native_passed_operator=None, usage_status='unavailable',
                        prompt_tokens=None, output_tokens=None)
            self._calls[call_id] = call
            self._emit('pz_completion_submitted', call)

    def finished(self, call_id, response=None, error=None):
        with self._lock:
            call = self._calls[call_id]
            if call['completed_ns'] is not None:
                raise ValueError('PZ completion was already observed as finished')
            call.update(completed_ns=time.monotonic_ns(), completion_failed=error is not None,
                        completion_error_type=type(error).__name__ if error is not None else None,
                        response_model=_value(response, 'model'), **_usage(response))
            choices = _value(response, 'choices', []) or []
            text = _value(_value(choices[0], 'message'), 'content') if choices else None
            if isinstance(text, str):
                call.update(raw_output_sha256=hashlib.sha256(text.encode()).hexdigest(),
                            raw_output_bytes=len(text.encode()))
            self._emit('pz_completion_finished', call)

    def parsed(self, call_id, field_answers=None, error=None, parser_default=False):
        with self._lock:
            call = self._calls[call_id]
            if call['parse_status'] != 'not_attempted':
                raise ValueError('PZ answer parser was observed more than once')
            call.update(parse_status='default_retained' if error is not None or parser_default else 'parsed',
                        parse_error_type=type(error).__name__ if error is not None else None)
            self._emit('pz_answer_parsed', call)

    def generator_finished(self, call_id, result=None, error=None, row_id=None):
        with self._lock:
            call = self._calls[call_id]
            if call['generator_finished']:
                raise ValueError('PZ generator was observed as finished more than once')
            if row_id is not None and list(row_id) != call['source_indices']:
                raise ValueError('PZ generator source occurrence association changed')
            answers = result[0] if isinstance(result, tuple) and len(result) == 4 else None
            passed = answers.get('passed_operator') if isinstance(answers, dict) else None
            call.update(generator_finished=True,
                        generator_error_type=type(error).__name__ if error is not None else None,
                        native_passed_operator=passed if type(passed) is bool else None)
            self._emit('pz_generator_finished', call)

    def calls(self):
        with self._lock:
            return deepcopy(list(self._calls.values()))

    def snapshot(self):
        calls = self.calls()
        available = [call for call in calls if call['usage_status'] == 'available']
        complete_usage = len(available) == len(calls) and bool(calls)
        return dict(schema='semloom.palimpzest_completion_ledger.v1',
                    completion_submitted=len(calls),
                    completion_completed=sum(call['completed_ns'] is not None for call in calls),
                    completion_outstanding=sum(call['completed_ns'] is None for call in calls),
                    completion_failed=sum(call['completion_failed'] for call in calls),
                    parser_default_retained=sum(call['parse_status'] == 'default_retained' for call in calls),
                    parser_not_attempted=sum(call['parse_status'] == 'not_attempted' for call in calls),
                    generator_failed=sum(call['generator_error_type'] is not None for call in calls),
                    usage_available_calls=len(available), usage_unavailable_calls=len(calls)-len(available),
                    usage_status='available' if complete_usage else 'unavailable',
                    total_prompt_tokens=sum(call['prompt_tokens'] for call in available) if complete_usage else None,
                    total_output_tokens=sum(call['output_tokens'] for call in available) if complete_usage else None,
                    known_prompt_tokens=sum(call['prompt_tokens'] for call in available),
                    known_output_tokens=sum(call['output_tokens'] for call in available),
                    monetary_cost_usd=None, monetary_cost_status='unavailable',
                    accounting_scope='Generator calls; actual HTTP attempts require the separate POST budget')

    def predictions(self, source_occurrences):
        """Failed/default answers remain invalid even when native Filter emits False."""
        output, seen = [], set()
        for call in self.calls():
            indices = call['source_indices']
            if len(indices) != 1 or indices[0] not in source_occurrences:
                raise ValueError('Movie Filter completion lacks one known source occurrence')
            identity = source_occurrences[indices[0]]
            if identity in seen:
                raise ValueError('Movie occurrence received more than one Filter completion')
            seen.add(identity)
            valid = (call['completed_ns'] is not None and not call['completion_failed']
                     and call['parse_status'] == 'parsed' and call['generator_finished']
                     and call['generator_error_type'] is None
                     and type(call['native_passed_operator']) is bool)
            label = ('POSITIVE' if call['native_passed_operator'] else 'NEGATIVE') if valid else None
            output.append((identity, label))
        return output
