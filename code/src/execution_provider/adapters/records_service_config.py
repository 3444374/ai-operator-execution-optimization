"""Pure service declarations shared by records preflight and owner construction."""
from src.semantic_methods.lotus.records import RecordsResources
from .records_capacity import CoreRecordsCapacity


def service_record_options(resources, options):
    resources = (RecordsResources(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=None)
                 if resources is None else resources)
    if (type(resources) is not RecordsResources or resources.max_active_batches is not None
            or resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1):
        raise ValueError('service allocation requires ordinary resources with max_active_batches=None')
    rolling = options.get('rolling_refill',False)
    if type(rolling) is not bool:
        raise ValueError('rolling_refill must be boolean')
    absent = ('async_limits', 'batch_concurrency', 'batch_feedback')
    if any(options.get(name) is not None for name in absent):
        raise ValueError('service payload grants set IPC capacity and do not accept batch controls')
    delivery = options.get('service_delivery')
    if rolling:
        from src.semantic_methods.lotus.prepared_batching import PreparedBatching
        from .records_service_delivery import OrdinaryDeliveryDeclaration, OrdinaryDeliverySession
        batching = options.get('prepared_batching')
        if (batching is not None and type(batching) is not PreparedBatching
                or type(delivery) not in (OrdinaryDeliveryDeclaration,OrdinaryDeliverySession)):
            raise ValueError('rolling service requests require actual complete delivery declarations')
    elif delivery is not None or options.get('prepared_batching') is not None:
        raise ValueError('prepared service requests require rolling_refill and a complete delivery declaration')
    profile = options.get('profile')
    if profile is not None and type(profile) is not dict:
        raise ValueError('service payload grants require an explicit ordinary async profile')
    profile = {} if profile is None else dict(profile)
    profile.setdefault('mode', 'async')
    if profile['mode'] != 'async':
        raise ValueError('service payload grants require async execution')
    if profile.get('admission') is None and 'max_inflight' not in profile:
        profile['max_inflight'] = 'auto'
    shape = CoreRecordsCapacity() if options.get('capacity') is None else options['capacity']
    if type(shape) is not CoreRecordsCapacity or shape.budget is not None:
        raise ValueError('service allocation requires a per-item shape without an existing budget')
    return resources, profile, shape


def preview_service_records_runtime(engine, resources, total_bytes, **options):
    """Preview one complete declaration without claiming bytes or creating an owner."""
    from .records_profile import resolve_record_batching, resolve_records_runtime
    from .records_service_budget import RecordsPayloadPool

    resources, profile, shape = service_record_options(resources, options)
    plan = RecordsPayloadPool(total_bytes).preview(shape,delivery=options.get('service_delivery'))
    configured = resolve_records_runtime(engine, resources, profile, capacity=plan.capacity,
        async_limits=plan.ipc_limits,prepared_batching=options.get('prepared_batching'),
        rolling_refill=options.get('rolling_refill',False))
    resolve_record_batching(configured.profile, prepared_batching=options.get('prepared_batching'),
        rolling_refill=options.get('rolling_refill', False),
        interleaved_preparation=options.get('interleaved_preparation', False),
        event_driven_preparation=options.get('event_driven_preparation', False))
    configured.profile.update(service_payload_plan=plan.describe(), service_payload_bytes=total_bytes,
        service_allocation_acquired=False,
        resource_budget_source='service declaration preview; no allocation acquired; not measured RAM')
    return configured
