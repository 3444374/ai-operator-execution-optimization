"""Sema post-HTTP native executor selection and service-clock observations."""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from .native_adapter_metrics import sample_distribution
from .imlane_query import imlane_config_for_arm


SEMA_NATIVE_ARMS = {
    'sema-native-daft-request-service': 'daft',
    'sema-native-ray-data-request-service': 'ray-data',
    'sema-imlane-request-service': 'imlane',
    'sema-native-imlane-request-service': 'imlane',
    'sema-native-imlane-ray-request-service': 'imlane',
}


@dataclass(frozen=True)
class SemaServiceConfig:
    """Public limits for the post-HTTP service; Sema owns its native SQL pool."""

    request_bytes: int = 1048576
    response_bytes: int = 1048576 - 8204
    intake_requests: int = 128
    intake_bytes: int = 32 * 1024 * 1024
    snapshot_rows: int | None = None
    snapshot_payload_bytes: int = 32 * 1024 * 1024
    pending_response_bytes: int = 32 * 1024 * 1024
    cumulative_response_bytes: int = 32 * 1024 * 1024
    query_response_bytes: int | None = None

    def __post_init__(self):
        self.service_limits(1, 1)
        self.native_limits(128)

    @classmethod
    def load(cls, value):
        if value is None or type(value) is cls:
            return value
        path = Path(value)
        if path.stat().st_size > 65536:
            raise ValueError('Sema service configuration exceeds its byte allowance')
        fields = json.loads(path.read_text())
        if type(fields) is not dict:
            raise ValueError('Sema service configuration requires a JSON object')
        return cls(**fields)

    def service_limits(self, max_requests, timeout_s):
        from src.execution_provider.adapters.sema_service import SemaServiceLimits
        return SemaServiceLimits(max_requests, self.request_bytes, self.response_bytes, timeout_s,
                                 self.intake_requests, self.intake_bytes, self.pending_response_bytes,
                                 self.query_response_bytes)

    def native_limits(self, held_tasks):
        from src.execution_provider.adapters.sema_native import SemaNativeLimits
        rows = held_tasks if self.snapshot_rows is None else self.snapshot_rows
        if type(rows) is not int or not 1 <= rows <= held_tasks:
            raise ValueError('Sema snapshot rows must fit the declared held request capacity')
        return SemaNativeLimits(rows, self.snapshot_payload_bytes, self.pending_response_bytes,
                                self.cumulative_response_bytes)


def validate_native_options(arm, options, held_tasks, imlane_config=None, imlane_call_configs=None,
                            service_config=None):
    if arm not in SEMA_NATIVE_ARMS:
        return
    if not 1 <= held_tasks <= 128 or options.concurrency > held_tasks:
        raise ValueError('Sema native service holds at most 128 complete requests or responses')
    if service_config is not None:
        service_config.native_limits(held_tasks)
    if arm == 'sema-imlane-request-service' and imlane_config is None:
        raise ValueError('Sema IMLane requires an explicit ready-runtime configuration')
    if arm in ('sema-native-imlane-request-service','sema-native-imlane-ray-request-service'):
        imlane_config_for_arm(arm, imlane_call_configs or {})


def open_native_service(stack, arm, *, query_id, model, limits, trace_path,
                        options, held_tasks, imlane_config=None, imlane_call_configs=None,
                        service_config=None):
    from src.execution_provider.adapters.sema_native import SemaNativeService
    validate_native_options(arm, options, held_tasks, imlane_config, imlane_call_configs, service_config)
    service_options = ({'snapshot_rows': held_tasks} if service_config is None else
                       {'native_limits': service_config.native_limits(held_tasks)})
    backend = SEMA_NATIVE_ARMS[arm]
    executor = None
    if backend == 'imlane':
        from src.execution_provider.adapters.sema_imlane import SemaIMLaneExecutor, SemaIMLaneService
        if arm == 'sema-imlane-request-service':
            executor = stack.enter_context(SemaIMLaneExecutor(imlane_config, options))
        else:
            from src.baselines.text.frameworks.native_calls import NativeCallExecutor
            executor = NativeCallExecutor('imlane', options, max_batch_rows=(held_tasks if service_config is None
                else service_config.native_limits(held_tasks).snapshot_rows),
                imlane_config=imlane_config_for_arm(arm, imlane_call_configs or {}))
            stack.callback(executor.close)
        return stack.enter_context(SemaIMLaneService(executor=executor,
            query_id=query_id, upstream_url=model.endpoint_url, limits=limits,
            trace_path=trace_path, options=options,
            held_request_capacity=held_tasks, **service_options))
    return stack.enter_context(SemaNativeService(
        query_id=query_id, upstream_url=model.endpoint_url, limits=limits,
        trace_path=trace_path, backend=backend, options=options,
        held_request_capacity=held_tasks, executor=executor, **service_options))


def service_phase_summary(rows):
    """Each duration uses the service's own monotonic clock, not worker clocks."""
    rows = tuple(rows)
    seen = set()
    phases = (
        ('body_read', 'proxy_arrived_ns', 'body_read_ns'),
        ('admission_wait', 'body_read_ns', 'service_accepted_ns'),
        ('enqueue', 'service_accepted_ns', 'native_enqueued_ns'),
        ('snapshot_wait', 'native_enqueued_ns', 'native_snapshot_ns'),
        ('graph_start', 'native_snapshot_ns', 'native_graph_started_ns'),
        ('native_execution_to_receipt', 'native_graph_started_ns', 'native_outcome_received_ns'),
        ('receipt_to_http_write', 'native_outcome_received_ns', 'response_written_ns'),
        ('service_request_e2e', 'proxy_arrived_ns', 'response_written_ns'),
    )
    samples = {name: [] for name, _, _ in phases}
    for row in rows:
        identity = row['request_sequence']
        if type(identity) is not int or identity < 0 or identity in seen:
            raise ValueError('Sema native trace repeats or changes HTTP occurrence identity')
        seen.add(identity)
        for name, start, end in phases:
            a, b = row.get(start), row.get(end)
            if a is None or b is None:
                continue
            if type(a) is not int or type(b) is not int or a < 1 or b < a:
                raise ValueError('Sema native service clocks are invalid or reversed')
            samples[name].append((b-a)/1e9)
    result = {name: dict(sample_distribution(values, unit='seconds'),
                        missing_count=len(rows)-len(values)) for name, values in samples.items()}
    return dict(request_count=len(rows), phases=result,
        clock='one Sema service process monotonic clock',
        occurrence_identity='HTTP request sequence; native source row unavailable',
        native_execution_scope='vendor graph or IMLane execution, model HTTP and result collection overlap',
        phase_addition='per-request sequential service phases only; concurrent request sums are not query time',
        native_prepool_ready=dict(status='unavailable', reason='Sema original request pool precedes service'),
        model_inference=dict(status='unavailable', reason='no model-server per-call execution clock'))


def gateway_phase_summary(rows):
    """Observe actual POST intervals in one gateway process, independently of graphs."""
    rows = tuple(rows)
    spans = (
        ('body_read', 'received_monotonic_ns', 'request_body_read_completed_monotonic_ns'),
        ('post_accounting', 'before_forward_started_monotonic_ns', 'before_forward_completed_monotonic_ns'),
        ('connection_and_submit', 'upstream_dispatch_started_monotonic_ns', 'upstream_headers_send_started_monotonic_ns'),
        ('model_http', 'upstream_headers_send_started_monotonic_ns', 'upstream_response_body_read_completed_monotonic_ns'),
        ('response_audit', 'after_forward_started_monotonic_ns', 'after_forward_completed_monotonic_ns'),
        ('http_write', 'response_write_started_monotonic_ns', 'response_write_completed_monotonic_ns'),
    )
    result = {}
    for name, start, end in spans:
        values = []
        for row in rows:
            a, b = row.get(start), row.get(end)
            if a is None or b is None:
                continue
            if type(a) is not int or type(b) is not int or a < 1 or b < a:
                raise ValueError('Sema gateway stage clocks are invalid or reversed')
            values.append((b-a)/1e9)
        result[name] = dict(sample_distribution(values, unit='seconds'),
                            missing_count=len(rows)-len(values))
    return dict(post_count=len(rows), phases=result,
        clock='one observation gateway process monotonic clock',
        model_http_scope='actual HTTP headers send through complete response read; includes serving queue and network',
        addition='nested inside native execution; neither pure inference nor additive to service/query time')
