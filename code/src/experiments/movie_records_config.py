"""Shared Movie records configuration; validation delegates to existing Core types.

Both run and session entries use these helpers without importing their execution
runner. This module starts no runtime, model, data scan or request controller.
"""

from src.semantic_methods.lotus.records import RecordsResources

ASYNC_CORE_BACKEND = "semloom-local-model-core-async"


def daft_native_profile(profile):
    """The existing Daft graphs accept only an optional native thread count."""
    if type(profile) is not dict or profile.keys() - {'runner_threads'}:
        raise ValueError('Daft native profile accepts runner_threads only')
    threads = profile.get('runner_threads')
    if threads is not None and (type(threads) is not int or threads < 1):
        raise ValueError('runner_threads must be a positive integer or None')
    return dict(profile)


def records_resources(config):
    """Keep caller declarations; service allocation supplies its single-row shape."""
    values = config.get('resources', {})
    if type(values) is not dict:
        raise ValueError('resources must be an object')
    if 'service_payload_bytes' in config:
        values = dict(prepare_batch_rows=1, model_batch_rows=1) | values
    return RecordsResources(**values)


def records_service_pool(config, context=None):
    """One actual process-local pool shared by this entry's successive owners."""
    if 'service_payload_bytes' not in config:
        return None
    from src.execution_provider.adapters.records_service_budget import RecordsPayloadPool
    total = config['service_payload_bytes']
    if type(total) is not int or total <= 0:
        raise ValueError('service_payload_bytes must be positive integer payload bytes')
    if context is None:
        return RecordsPayloadPool(config['service_payload_bytes'])
    pool = context.get('_records_payload_pool')
    if pool is None:
        pool = context['_records_payload_pool'] = RecordsPayloadPool(config['service_payload_bytes'])
    if type(pool) is not RecordsPayloadPool or pool.total_bytes != config['service_payload_bytes']:
        raise ValueError('session must retain its original service payload pool')
    pool.snapshot()  # Enforce the original process before an owner is allocated.
    return pool


def core_capacity(config):
    from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity

    if "core_capacity" not in config:
        return None
    value = config["core_capacity"]
    if type(value) is not dict:
        raise ValueError("core_capacity must be an object")
    return CoreRecordsCapacity(**value)


def records_runtime(config, *, resources=None, engine=None):
    """Shared pure resolution for ordinary Movie, Session, check and startup."""
    from src.execution_provider.adapters.records_profile import resolve_records_runtime
    from src.serving.backends.vllm_protocol import EngineSpec

    resources = records_resources(config) if resources is None else resources
    engine = EngineSpec.from_dict(config['engine']) if engine is None else engine
    profile = config.get('profile') if 'profile' in config else core_profile(config)
    if 'async_limits' in config and type(config['async_limits']) is not dict:
        raise ValueError('async_limits must be an object')
    if 'service_payload_bytes' in config:
        if config.get('backend') not in ('semloom-core', 'semloom-local-model-core', ASYNC_CORE_BACKEND):
            raise ValueError('service_payload_bytes requires an async Core backend')
        from src.execution_provider.adapters.records_service_config import preview_service_records_runtime
        return preview_service_records_runtime(engine, resources, config['service_payload_bytes'],
            profile=profile, capacity=core_capacity(config), async_limits=config.get('async_limits'),
            **record_batching_options(config))
    configured = resolve_records_runtime(engine, resources, profile, capacity=core_capacity(config),
        async_limits=config.get('async_limits'), prepared_batching=config.get('prepared_batching'),
        rolling_refill=config.get('rolling_refill', False))
    return configured


def core_profile(config):
    profile = config.get("core_profile")
    if config["backend"] == ASYNC_CORE_BACKEND:
        if profile is not None and (type(profile) is not dict or profile.get("mode", "async") != "async"):
            raise ValueError("the explicit async backend requires async Core mode")
        profile = dict(profile or {}, mode="async")
    return profile


def record_batching_options(config):
    """Select prepared transport independently from optional application token packing."""
    if not (set(config) & {'prepared_batching', 'batch_concurrency', 'batch_feedback', 'rolling_refill',
                           'preparation_mode'}):
        return {}
    from src.semantic_methods.lotus.prepared_batching import PreparedBatching
    value = config.get('prepared_batching')
    if value is not None:
        if type(value) is not dict:
            raise ValueError('prepared_batching must be an object or null')
        value = PreparedBatching(**value)
    options = dict(prepared_batching=value, batch_concurrency=config.get('batch_concurrency'))
    mode = config.get('preparation_mode', 'serial')
    if type(mode) is not str or mode not in ('serial', 'interleaved', 'event_driven'):
        raise ValueError('preparation_mode must be serial, interleaved or event_driven')
    if mode != 'serial':
        options['interleaved_preparation'] = True
    if mode == 'event_driven':
        options['event_driven_preparation'] = True
    if 'rolling_refill' in config:
        options['rolling_refill'] = config['rolling_refill']
    if 'batch_feedback' in config:
        from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig
        feedback = config['batch_feedback']
        options['batch_feedback'] = None if feedback is None else SupplyFeedbackConfig.from_dict(feedback)
    return options
