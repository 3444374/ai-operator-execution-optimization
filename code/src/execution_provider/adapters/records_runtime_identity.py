"""Read an existing Ray connection identity without importing or starting Ray."""
import sys
from dataclasses import asdict


def observe_ray_runtime(ray):
    try:
        if (ray is None or sys.modules.get('ray') is not ray
                or not callable(getattr(ray, 'is_initialized', None))
                or ray.is_initialized() is not True
                or not callable(getattr(ray, 'get_runtime_context', None))):
            return None
        context = ray.get_runtime_context()
        identity = dict(job_id=context.get_job_id(), node_id=context.get_node_id())
        if any(type(value) is not str or not value.strip() for value in identity.values()):
            return None
        return ray, identity
    except Exception:
        return None


class ModelRuntimeBinding:
    """One observation for an existing producer; never initialize or reconnect."""

    def __init__(self):
        self.attempted, self.original = False, None

    def bind(self, observed=None):
        if observed is not None and (type(observed) is not tuple or len(observed) != 2
                or type(observed[1]) is not dict or set(observed[1]) != {'job_id', 'node_id'}
                or any(type(v) is not str or not v.strip() for v in observed[1].values())):
            raise ValueError('model runtime requires the observed module, job and node identity')
        if self.attempted:
            if observed is not None and (self.original is None or observed[0] is not self.original[0]
                                        or observed[1] != self.original[1]):
                raise RuntimeError('model runtime cannot be rebound after its first observation')
            self.check()
            return
        current = observe_ray_runtime(sys.modules.get('ray') if observed is None else observed[0])
        if observed is not None and (current is None or current[0] is not observed[0] or current[1] != observed[1]):
            raise RuntimeError('supplied model runtime identity is unavailable or changed')
        self.attempted = True
        self.original = None if current is None else (current[0], dict(current[1]))

    def check(self, *, require_identity=False):
        if self.original is None:
            if require_identity:
                raise RuntimeError('original model Ray runtime identity was unavailable')
            return
        current = observe_ray_runtime(self.original[0])
        if current is None or current[0] is not self.original[0] or current[1] != self.original[1]:
            raise RuntimeError('original model Ray runtime is unavailable or changed')


def prepared_cleanup_confirmed(actors, transport, core, report, *, closing, feedback=None,
                               expected_identity=None):
    """Read actual producer/transport terminal proof without changing ownership."""
    try:
        if not closing or type(report) is not dict:
            return False
        actual = actors.last_close_report
        model = report.get('model_owner')
        if (type(model) is not dict or type(actual) is not dict or report.get('status') != 'closed'
                or report.get('cleanup_errors') or report.get('transport_closed') is not True
                or report.get('retained_remote_inputs') != 0):
            return False
        ready = actors.model_ready
        for value in (model, actual):
            if (value.get('status') != 'closed' or value.get('cleanup_errors')
                    or value.get('actor_kill_requests_confirmed') is not True
                    or value.get('model_ready') != ready):
                return False
        if expected_identity is not None and any((ready or {}).get(n) != v
                                                 for n, v in expected_identity.items()):
            return False
        if actors.model_actor is not None:
            if (actual.get('engine_close_report') or {}).get('engine_api_shutdown_confirmed') is not True:
                return False
        elif actors._start_failed:
            return False
        if core is not None and any(asdict(core.capacity.usage()).values()):
            return False
        if transport is not None:
            handoff = transport.audit_handoff_report()
            if (transport.retained_refs() or transport.backend._closed is not True
                    or transport.backend._thread.is_alive()
                    or handoff['pending_receipts'] or handoff['deferred_core_acks']):
                return False
        rpc = actors._data_rpc_progress()
        if rpc.pending or rpc.inflight or rpc.batch_submits_pending or rpc.unconfirmed:
            return False
        if feedback is not None and feedback.snapshot_rpc_report().get(
                'snapshot_rpc_terminal_evidence_complete') is not True:
            return False
        return True
    except Exception:
        return False


def retired_close_metadata(value):
    """Keep close metadata without retaining result arrays or result payloads."""
    if type(value) is not dict:
        raise TypeError('retired close metadata requires a report object')
    paths = ('implementation', 'execution', 'status', 'query_completed', 'source_closed',
        'source_exhausted', 'transport_closed', 'retained_remote_inputs', 'expected_occurrences',
        'received_occurrences', 'production_core_connected', 'model_retained', 'work_unit',
        'model_owner.status', 'model_owner.actor_kill_requests_confirmed',
        'model_owner.data_rpc_pending', 'model_owner.data_rpc_unconfirmed',
        'model_owner.model_ready.engine_uid', 'model_owner.model_ready.producer_owner_uid',
        'model_owner.model_ready.pid', 'model_owner.engine_close_report.status',
        'model_owner.engine_close_report.engine_api_shutdown_confirmed',
        'model_owner.engine_close_report.ipc_close.status',
        'model_owner.engine_close_report.ipc_close.process_exit_confirmed',
        'model_owner.engine_close_report.ipc_close.io_thread_closed')
    paths += tuple('core_usage.' + name for name in
                   ('held_tasks', 'input_bytes', 'result_bytes', 'active_requests', 'active_work'))
    paths += tuple('model_owner.engine_close_report.ipc_close.producer_quiescence.' + name for name in
                   ('schema', 'confirmed', 'owner_uid', 'engine_uid', 'process_pid', 'process_returncode',
                    'issued_submissions', 'received_submissions'))
    result = {}
    for path in paths:
        names, current = path.split('.'), value
        for name in names:
            if type(current) is not dict or name not in current:
                break
            current = current[name]
        else:
            if current is None or type(current) in (str, bool, int, float):
                target = result
                for name in names[:-1]:
                    target = target.setdefault(name, {})
                target[names[-1]] = current
    result['cleanup_errors'] = []
    return result
