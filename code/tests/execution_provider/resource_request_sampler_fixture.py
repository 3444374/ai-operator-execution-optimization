"""Explicit CPU ready/source data for the actual request sampler."""
from src.execution_provider.adapters.records_native_source import (
    PRIVATE_CLOCK, RESOURCE_OWNER_SCHEMA, native_sample_age,
)
from src.execution_provider.adapters.records_request_feedback import ResourceRequestSupplyFeedback
from src.scheduling.submission_control.feedback_options import ResourceRequestConfig
from tests.scheduling.test_incremental_session import setup


def ready(engine_uid):
    return dict(engine_uid=engine_uid, batch_mode='independent_final_results', closed=False,
        resource_owner=dict(schema=RESOURCE_OWNER_SCHEMA, engine_uid=engine_uid,
            request_memory_owner='vllm', native_feedback_clock=PRIVATE_CLOCK))


def frame_age(packet, *, age_s=0.):
    native = (packet.get('ipc') or {}).get('native_feedback')
    packet['native_sample_age'] = (None if native is None else native_sample_age(
        native, ready(native['engine_uid']), captured_at_s=native['observed_at_s']+age_s))
    return packet


def request_sampler(owner, session_id, *, options=None, started_at_s=0.):
    limits, maximum = owner._actors.limits, owner.resources.max_active_batches
    if not hasattr(owner, 'core'):
        owner.core, _, _, _ = setup(held_tasks=maximum, active_requests=maximum,
            active_work=maximum, input_bytes=limits.input_bytes, result_bytes=limits.result_bytes,
            item_input_bytes=limits.item_input_bytes, item_result_bytes=limits.item_result_bytes)
    options = (ResourceRequestConfig(poll_interval_s=.1, window_s=.25, max_sample_age_s=.25)
               if options is None else options)
    feedback = ResourceRequestSupplyFeedback(owner, options)
    feedback.session_id, feedback.started = session_id, started_at_s
    feedback._bind_request_policy(session_id, 1)
    return feedback
