"""Workload acceptance and deadline declarations, independent of native scheduling."""
from dataclasses import asdict, dataclass
import math


@dataclass(frozen=True)
class LotusCallLimits:
    request_timeout_s: float
    max_request_bytes: int = 1024 * 1024
    max_response_bytes: int = 1024 * 1024
    max_batch_rows: int = 4096
    max_batch_input_bytes: int = 32 * 1024 * 1024
    max_batch_response_bytes: int = 16 * 1024 * 1024
    batch_timeout_s: float | None = 120

    def __post_init__(self):
        for name in ('max_request_bytes', 'max_response_bytes', 'max_batch_rows',
                     'max_batch_input_bytes', 'max_batch_response_bytes'):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError('LOTUS call capacity must be a positive integer: ' + name)
        if self.max_request_bytes > 8 * 1024 * 1024 or self.max_response_bytes > 8 * 1024 * 1024:
            raise ValueError('LOTUS per-call byte capacity exceeds the complete-call representation')
        timeouts = (self.request_timeout_s,) + (() if self.batch_timeout_s is None else (self.batch_timeout_s,))
        for value in timeouts:
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 120:
                raise ValueError('LOTUS request and batch deadlines must be finite and at most 120 seconds')

    def check_model(self, model_config):
        if self.request_timeout_s * 1000 != model_config.timeout_ms:
            raise ValueError('LOTUS request deadline differs from the selected model configuration')

    def check_prepared(self, call):
        if len(call.payload) > self.max_request_bytes:
            raise ValueError('LOTUS prepared request exceeds the declared byte capacity')
        if call.timeout_s != self.request_timeout_s:
            raise ValueError('LOTUS prepared request deadline differs from the declared profile')

    def preflight_rows(self, rows, *, native_rows=None):
        if rows > self.max_batch_rows or native_rows is not None and rows > native_rows:
            raise ValueError('LOTUS target batch rows exceed the selected executor capacity')

    def describe(self):
        return dict(asdict(self), response_byte_scope='complete_http_with_metadata',
                    batch_input_byte_scope='sum_of_prepared_payload_bytes',
                    request_deadline_scope='transport_total',
                    batch_deadline_scope=('externally_supervised_completion' if self.batch_timeout_s is None
                                          else 'preparation_through_sdk_and_eof'),
                    cleanup_deadline_scope='separate')
