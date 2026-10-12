"""Explicit caller-owned author IMLane modes for original supplier methods."""
from dataclasses import fields
import json
from pathlib import Path

from src.baselines.common.private_artifacts import require_outside_git
from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig


IMLANE_METHOD_MODES = {
    'lotus-method-native-imlane': 'cpython',
    'lotus-method-native-imlane-ray': 'ray',
    'duckdb-method-native-imlane': 'cpython',
    'duckdb-method-native-imlane-ray': 'ray',
    'sema-native-imlane-request-service': 'cpython',
    'sema-native-imlane-ray-request-service': 'ray',
}


def load_imlane_call_configs(configs=None):
    if configs is None:
        return {}
    if type(configs) is not dict or set(configs) - {'cpython', 'ray'}:
        raise ValueError('IMLane configurations require explicit cpython/ray keys')
    result = {}
    for runtime, value in configs.items():
        if type(value) is not IMLaneCallConfig:
            path = Path(value).resolve()
            require_outside_git(path)
            data = json.loads(path.read_text())
            if type(data) is not dict or set(data) - {f.name for f in fields(IMLaneCallConfig)}:
                raise ValueError('IMLane call configuration contains unsupported fields')
            value = IMLaneCallConfig(**data)
        if value.runtime != runtime:
            raise ValueError('IMLane arm mode differs from its caller-owned Coordinator')
        result[runtime] = value
    return result


def imlane_config_for_arm(arm, configs):
    runtime = IMLANE_METHOD_MODES.get(arm)
    if runtime is None:
        return None
    value = configs.get(runtime)
    if type(value) is not IMLaneCallConfig or value.runtime != runtime:
        raise ValueError('IMLane method requires an explicit matching ready-runtime configuration')
    return value


def imlane_runtime_identity(config):
    return dict(owner='IMLane author Coordinator', execution_backend='imlane',
        imlane_runtime=config.runtime, declared_author_executors=config.n_executors,
        runtime_lifecycle='caller owns Coordinator and any author Ray runtime',
        call_graph='original DBEnd bridge; explicit complete-call row capacity',
        configured_max_batch_rows=config.max_batch_rows,
        remote_http_stopped='unavailable')
