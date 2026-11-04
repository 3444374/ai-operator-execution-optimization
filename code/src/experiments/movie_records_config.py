"""Shared Movie records configuration; validation delegates to existing Core types.

Both run and session entries use these helpers without importing their execution
runner. This module starts no runtime, model, data scan or request controller.
"""

from dataclasses import asdict, replace

from src.semantic_methods.lotus.records import RecordsResources

ASYNC_CORE_BACKEND = "semloom-local-model-core-async"


def core_capacity(config):
    from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity

    if "core_capacity" not in config:
        return None
    value = config["core_capacity"]
    if type(value) is not dict:
        raise ValueError("core_capacity must be an object")
    return CoreRecordsCapacity(**value)



def async_limits(config, *, resources=None, resolved=None):
    declared = "async_limits" in config
    value = config.get("async_limits", {})
    if type(value) is not dict:
        raise ValueError("async_limits must be an object")
    configured = records_runtime(config, resources=resources)
    return configured.async_limits if declared else None


def records_runtime(config, *, resources=None, engine=None):
    """Shared pure resolution for ordinary Movie, Session, check and startup."""
    from src.execution_provider.adapters.records_profile import resolve_records_runtime
    from src.serving.backends.vllm_protocol import EngineSpec

    resources = RecordsResources(**config.get('resources', {})) if resources is None else resources
    engine = EngineSpec.from_dict(config['engine']) if engine is None else engine
    profile = config.get('profile') if 'profile' in config else core_profile(config)
    if 'async_limits' in config and type(config['async_limits']) is not dict:
        raise ValueError('async_limits must be an object')
    configured = resolve_records_runtime(engine, resources, profile, capacity=core_capacity(config),
        async_limits=config.get('async_limits'), prepared_batching=config.get('prepared_batching'),
        rolling_refill=config.get('rolling_refill', False))
    if configured.capacity.budget is not None and config.get('prepared_batching') is not None:
        effective = replace(resources, max_active_batches=configured.profile['owner_max_inflight'])
        configured.capacity.method_capacity(effective, configured.capacity.method_limits(1))
    return configured



def record_admission_cache(config, *, resources, resolved):
    """Bind effective storage/IPC choices; equivalent defaults keep old identity."""
    from copy import deepcopy
    from src.execution_provider.adapters.records_capacity import CoreRecordsCapacity, resolve_records_async_limits

    cache = config.get("admission_cache")
    if cache is None or not set(config) & {"core_capacity", "async_limits"}:
        return cache
    capacity = core_capacity(config) or CoreRecordsCapacity()
    effective = replace(resources, max_active_batches=resolved.get("owner_max_inflight", resolved["max_inflight"]))
    limits = resolve_records_async_limits(effective, capacity,
        async_limits(config, resources=resources, resolved=resolved))
    default = CoreRecordsCapacity()
    if capacity == default and limits == resolve_records_async_limits(effective, default):
        return cache
    cache = deepcopy(cache)
    cache["identity"]["execution"]["records_capacity_v1"] = dict(
        core=dict(asdict(capacity), poll_interval_s=float(capacity.poll_interval_s)),
        IPC=dict(asdict(limits), feedback_interval_s=float(limits.feedback_interval_s)))
    return cache



def core_profile(config):
    from src.execution_provider.adapters.records_profile import expand_core_profile
    profile = expand_core_profile(config.get("core_profile"))
    if config["backend"] == ASYNC_CORE_BACKEND:
        if profile is not None and (type(profile) is not dict or profile.get("mode", "async") != "async"):
            raise ValueError("the explicit async backend requires async Core mode")
        profile = dict(profile or {}, mode="async")
    return profile



def record_batching_options(config):
    """Only explicit application token batches select the existing chain core."""
    if not (set(config) & {'prepared_batching', 'batch_concurrency', 'batch_feedback', 'rolling_refill',
                           'experimental_skip_token_packing', 'preparation_mode'}):
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
    if 'experimental_skip_token_packing' in config:
        options['experimental_skip_token_packing'] = config['experimental_skip_token_packing']
    if 'batch_feedback' in config:
        from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig
        feedback = config['batch_feedback']
        options['batch_feedback'] = None if feedback is None else SupplyFeedbackConfig.from_dict(feedback)
    return options
