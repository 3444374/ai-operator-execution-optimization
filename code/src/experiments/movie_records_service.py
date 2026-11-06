"""Persist closed-query accounting before explicitly accepting retained receipts."""
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import uuid

from src.infrastructure.private_artifacts import content_digest, file_identity, write_private_json
from src.infrastructure.json_content import json_content_equal
from src.execution_provider.adapters.records_runtime_identity import observe_ray_runtime


def service_runtime_identity(ray):
    identity = observe_ray_runtime(ray)
    if identity is None:
        raise RuntimeError('service entry requires its original connected Ray runtime identity')
    return identity


def check_service_runtime(ray, original):
    current = service_runtime_identity(ray)
    if original is None or current[0] is not original[0] or current[1] != original[1]:
        raise RuntimeError('service Ray runtime changed; replacement runtime left untouched')


def close_service_owner(owner, ray, original, *, timeout):
    """Check before any original-owner RPC; local unstarted cleanup needs no Ray."""
    if owner.cleanup_confirmed():
        return owner.last_close_report
    actors = owner._actors
    if (actors.ray is not None or actors.model_actor is not None
            or actors.prepare_actors or actors.model_ready is not None):
        check_service_runtime(ray, original)
    return owner.close(timeout=timeout)


def finish_service_runtime(ray, original, owner):
    """Keep the original connection available until its owner's RPC proof completes."""
    if original is None or original[0] is not ray or sys.modules.get('ray') is not ray:
        raise RuntimeError('original service Ray connection is unavailable or replaced')
    if ray.is_initialized() is False:
        if not owner.cleanup_confirmed():
            raise RuntimeError('original Ray stopped before owner cleanup was confirmed')
    else:
        check_service_runtime(ray, original)
        if not owner.cleanup_confirmed():
            return dict(status='retained', runtime_shutdown_confirmed=False,
                scope='original connection retained for pending owner cleanup; no restart or reconnect')
        ray.shutdown()
        if ray.is_initialized() is not False:
            raise RuntimeError('original service Ray connection did not confirm shutdown')
    return dict(status='closed', runtime_shutdown_confirmed=True,
        scope='driver connection only; physical processes need independent observation',
        independent_gpu_exit_confirmed=None)


def _persist_snapshot(directory, prefix, snapshot):
    snapshot = json.loads(json.dumps(snapshot, ensure_ascii=False, allow_nan=False))
    path = Path(directory)/(prefix+content_digest(snapshot)+'.json')
    if path.is_symlink():
        raise ValueError('service accounting cannot reuse a symlink')
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except json.JSONDecodeError:
            # Preserve an incomplete earlier write; the complete retry gets
            # its own private artifact and never replaces that evidence.
            path = path.with_name(path.stem+'-'+uuid.uuid4().hex+path.suffix)
            write_private_json(path, snapshot)
            existing = snapshot
        if not json_content_equal(existing, snapshot):
            raise ValueError('existing service accounting differs from its receipt snapshot')
    else:
        write_private_json(path, snapshot)
    descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return path


def finish_service_pool(pool, directory, *, owned):
    snapshot = pool.close() if owned else pool.snapshot()
    path = _persist_snapshot(directory, 'service-payload-pool-', snapshot)
    return snapshot, dict(path=path.name, **file_identity(path))


def verify_service_owner(owner, configured):
    if owner.execution_profile.get('service_payload_plan') != configured.profile.get('service_payload_plan'):
        report = owner.close(timeout=owner.resources.startup_timeout_s)
        error = ValueError('service pool cannot acquire the complete checked payload plan')
        error.owner_close = report
        raise error


def accept_service_results(owner, report, expected, directory, *, primary=None):
    if not owner.cleanup_confirmed():
        return None
    query = owner.last_query_report
    retained = [] if query is None else deepcopy(query['unconsumed_call_results'])
    if not retained:
        return None
    from .movie_records_query import merge_unconsumed_call_results
    merge_unconsumed_call_results(report, expected, query, primary=primary)
    snapshot = dict(query=deepcopy(report), expected=deepcopy(expected), retained=retained)
    path = _persist_snapshot(directory, 'service-query-accounting-', snapshot)
    transferred = owner.take_unconsumed_results(expected=retained)
    return dict(path=path.name, **file_identity(path), records=len(transferred),
                source='closed original owner; persisted accounting before consumer acceptance')
