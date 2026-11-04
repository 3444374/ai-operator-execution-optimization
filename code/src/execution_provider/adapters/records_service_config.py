"""Pure service declarations shared by records preflight and owner construction."""
from src.semantic_methods.lotus.records import RecordsResources
from .records_capacity import CoreRecordsCapacity


def service_record_options(resources, options):
    resources = (RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=None)
                 if resources is None else resources)
    if (type(resources) is not RecordsResources or resources.max_active_batches is not None
            or resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1):
        raise ValueError('service allocation requires ordinary resources with max_active_batches=None')
    absent = ('async_limits', 'prepared_batching', 'batch_concurrency', 'batch_feedback')
    disabled = ('rolling_refill', 'experimental_skip_token_packing',
                'interleaved_preparation', 'event_driven_preparation')
    if (any(options.get(name) is not None for name in absent)
            or any(options.get(name, False) is not False for name in disabled)):
        raise ValueError('service payload grants currently support ordinary async requests without IPC overrides')
    profile = options.get('profile')
    if profile is not None and type(profile) is not dict:
        raise ValueError('service payload grants require an explicit ordinary async profile')
    profile = {} if profile is None else dict(profile)
    profile.setdefault('mode', 'async')
    if profile['mode'] != 'async':
        raise ValueError('service payload grants require async execution')
    if profile.get('admission') is None and 'max_inflight' not in profile:
        profile['max_inflight'] = 'auto'
    if (type(profile.get('admission')) is dict
            and profile['admission'].get('kind') == 'legacy_resource_feedback'):
        raise ValueError('historical feedback profiles retain their own explicit resource declarations')
    shape = CoreRecordsCapacity() if options.get('capacity') is None else options['capacity']
    if type(shape) is not CoreRecordsCapacity or shape.budget is not None:
        raise ValueError('service allocation requires a per-item shape without an existing budget')
    return resources, profile, shape


def preview_service_records_runtime(engine, resources, total_bytes, **options):
    """Preview one complete declaration without claiming bytes or creating an owner."""
    from .records_profile import resolve_records_runtime
    from .records_service_budget import RecordsPayloadPool

    resources, profile, shape = service_record_options(resources, options)
    plan = RecordsPayloadPool(total_bytes).preview(shape)
    configured = resolve_records_runtime(engine, resources, profile, capacity=plan.capacity,
                                          async_limits=plan.ipc_limits)
    configured.profile.update(service_payload_plan=plan.describe(), service_payload_bytes=total_bytes,
        service_allocation_acquired=False,
        resource_budget_source='service declaration preview; no allocation acquired; not measured RAM')
    return configured
