"""Finite bridge storage declarations; the author owns execution concurrency."""
from dataclasses import asdict, dataclass

MAX_IMLANE_BATCH_ROWS = 4096


class IMLaneCapacityError(ValueError):
    def __init__(self, reason, actual, maximum, *, rows):
        self.details = dict(reason=reason, actual=actual, maximum=maximum,
                            input_rows=rows, author_submission_started=False)
        super().__init__(f'IMLane {reason}: input={actual}, effective_limit={maximum}; '
                         f'input_rows={rows}; author submission has not started')


@dataclass(frozen=True)
class IMLaneBatchLimits:
    max_batch_rows: int = 128
    max_input_payload_bytes: int = 32 * 1024**2
    max_input_wire_bytes: int = 48 * 1024**2
    max_item_payload_bytes: int = 8 * 1024**2
    max_pending_calls: int = 128
    max_batch_response_bytes: int = 32 * 1024**2

    def __post_init__(self):
        caps = (MAX_IMLANE_BATCH_ROWS, 32 * 1024**2, 48 * 1024**2,
                8 * 1024**2, 128, 32 * 1024**2)
        for (name, value), maximum in zip(asdict(self).items(), caps):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError('IMLane finite capacity is invalid: ' + name)

    @property
    def legacy(self):
        return self == IMLaneBatchLimits()

    def cpp_arguments(self):
        return [str(v) for v in asdict(self).values()]

    def inspect_calls(self, calls, *, include_wire=True):
        from .native_calls import NativeCall, _call_wire
        rows = len(calls)
        if rows > self.max_batch_rows:
            raise IMLaneCapacityError('total row capacity', rows, self.max_batch_rows, rows=rows)
        if any(type(call) is not NativeCall for call in calls):
            raise ValueError('IMLane input contains a non-NativeCall item; author submission has not started')
        if len({call.call_id for call in calls}) != rows:
            raise ValueError('IMLane duplicate call identity; author submission has not started')
        total = sum(len(call.payload) for call in calls)
        largest = max((len(call.payload) for call in calls), default=0)
        for reason, actual, maximum in (
                ('single payload byte capacity', largest, self.max_item_payload_bytes),
                ('total payload byte capacity', total, self.max_input_payload_bytes)):
            if actual > maximum:
                raise IMLaneCapacityError(reason, actual, maximum, rows=rows)
        wire = sum(len(_call_wire(call).encode()) + 1 for call in calls) if include_wire else None
        if wire is not None and wire > self.max_input_wire_bytes:
            raise IMLaneCapacityError('stdin wire byte capacity', wire, self.max_input_wire_bytes, rows=rows)
        return dict(input_rows=rows, payload_bytes=total, stdin_wire_bytes=wire,
                    max_item_payload_bytes_observed=largest, configured_limits=asdict(self),
                    max_pending_calls=self.max_pending_calls, pending_limit_is_execution_concurrency=False,
                    wire_byte_scope='utf8_json_lines_including_newlines',
                    resident_memory_limit='not established by encoded-byte capacities')
