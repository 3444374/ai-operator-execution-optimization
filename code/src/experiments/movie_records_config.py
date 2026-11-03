"""Shared Movie records configuration; validation delegates to existing Core types.

Both run and session entries use these helpers without importing their execution
runner. This module starts no runtime, model, data scan or request controller.
"""

from dataclasses import asdict, replace

from src.semantic_methods.lotus.records import RecordsResources

ASYNC_CORE_BACKEND = "semloom-local-model-core-async"


def core_capacity(config):
    from src.execution_provider.adapters.lotus_records_core import CoreRecordsCapacity

    if "core_capacity" not in config:
        return None
    value = config["core_capacity"]
    if type(value) is not dict:
        raise ValueError("core_capacity must be an object")
    return CoreRecordsCapacity(**value)



def async_limits(config, *, resources=None, resolved=None):
    from src.execution_provider.adapters.lotus_records_core import (
        CoreRecordsCapacity, resolve_core_profile, resolve_records_async_limits,
    )

    declared = "async_limits" in config
    if not declared and "core_capacity" not in config:
        return None  # The records factory binds defaults to this owner and Core capacity.
    value = config.get("async_limits", {})
    if type(value) is not dict:
        raise ValueError("async_limits must be an object")
    if resources is None:
        resources = RecordsResources(**config.get("resources", {}))
    if resolved is None:
        from src.serving.backends.vllm_engine import EngineSpec
        resolved = resolve_core_profile(EngineSpec.from_dict(config["engine"]), resources,
            core_profile(config), rolling_refill=config.get('rolling_refill', False), capacity=core_capacity(config))
    if resolved["mode"] != "async":
        if declared:
            raise ValueError("async_limits requires the explicit async backend or async Core profile")
        return None
    effective = replace(resources, max_active_batches=resolved.get("owner_max_inflight", resolved["max_inflight"]))
    capacity = core_capacity(config) or CoreRecordsCapacity()
    limits = resolve_records_async_limits(effective, capacity, value)
    if capacity.budget is not None and config.get('prepared_batching') is not None:
        capacity.method_capacity(effective, capacity.method_limits(1))
    return limits if declared else None



def record_admission_cache(config, *, resources, resolved):
    """Bind effective storage/IPC choices; equivalent defaults keep old identity."""
    from copy import deepcopy
    from src.execution_provider.adapters.lotus_records_core import CoreRecordsCapacity, resolve_records_async_limits

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
    from src.execution_provider.adapters.lotus_records_core import expand_core_profile
    profile = expand_core_profile(config.get("core_profile"))
    if config["backend"] == ASYNC_CORE_BACKEND:
        if profile is not None and (type(profile) is not dict or profile.get("mode", "async") != "async"):
            raise ValueError("the explicit async backend requires async Core mode")
        profile = dict(profile or {}, mode="async")
    return profile



def record_batching_options(config):
    """Only explicit application token batches select the existing chain core."""
    if not (set(config) & {'prepared_batching', 'batch_concurrency', 'batch_feedback', 'rolling_refill',
                           'experimental_skip_token_packing'}):
        return {}
    from src.semantic_methods.lotus.prepared_batching import PreparedBatching
    value = config.get('prepared_batching')
    if value is not None:
        if type(value) is not dict:
            raise ValueError('prepared_batching must be an object or null')
        value = PreparedBatching(**value)
    options = dict(prepared_batching=value, batch_concurrency=config.get('batch_concurrency'))
    if 'rolling_refill' in config:
        options['rolling_refill'] = config['rolling_refill']
    if 'experimental_skip_token_packing' in config:
        options['experimental_skip_token_packing'] = config['experimental_skip_token_packing']
    if 'batch_feedback' in config:
        from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig
        feedback = config['batch_feedback']
        options['batch_feedback'] = None if feedback is None else SupplyFeedbackConfig.from_dict(feedback)
    return options
