"""Native ready batches enter the existing Engine without a vendor request pool."""

from dataclasses import replace

from ...planning.work import StageWork, WorkDescriptor
from ...scheduling.core.session_contract import OfferedTask, SessionSpec, TaskInfo
from .full_response import FullResponseTransport
from .incremental_execution import build_fixed_model_execution
from .model_config import MAX_MODEL_RESPONSE_BYTES
from .ray_map_transport import RayMapConfig, ray_map_factory


def prepare_native_task(payload: bytes, sequence: int, *, row_sequence: int, call_id: str,
                        stage_id: str = "model", work: WorkDescriptor | None = None,
                        max_result_bytes: int = MAX_MODEL_RESPONSE_BYTES) -> OfferedTask:
    # Payload is already the supplier's non-streaming, single-model HTTP body.
    # In particular, LiteLLM's parameter conversion belongs to its own adapter.
    if type(payload) is not bytes:
        raise ValueError("native request payload must be bytes")
    if work is None:
        work = WorkDescriptor((StageWork("model", 1, "work_units"),), "model", "request-count")
    return OfferedTask(sequence, payload, work.primary.units, max_result_bytes,
                       info=TaskInfo(call_id, row_sequence, stage_id, work))


def build_native_execution(config, *, physical: RayMapConfig | None, execute=None, **options):
    """Use Daft/Ray for the main path; physical=None explicitly selects a local diagnostic."""
    if physical is None:
        return build_fixed_model_execution(config, execute=execute,
                                          transport_factory=None if execute else FullResponseTransport,
                                          **options)
    if execute is not None or type(physical) is not RayMapConfig:
        raise ValueError("native Ray execution requires RayMapConfig and its own transport")
    return ray_map_factory(replace(physical, response_mode="full"))(config, **options)


class NativeTaskSession:
    """One query/operator flow; no extra task, result, or capacity queue.

    The caller supplies finite OfferedTask tuples, retains the unaccepted suffix,
    consumes Delivery.result with decode_full_response, then releases its LeaseId.
    close is called only after the consumer has discarded all delivery bytes.
    The service owner retains the execution and reaps remote unknown work.
    """

    def __init__(self, execution, query_id: str, operator_id: str):
        self.execution = execution
        self.job, self.session, self.budget = execution.open_job(query_id,
            SessionSpec(query_id, operator_id, "text-completion", work_unit=execution.work_unit))
        self._closed = False

    @property
    def limits(self):
        return self.session.limits

    def offer(self, tasks):
        return self.session.offer(tasks)

    def advance(self, max_deliveries=1):
        cleanup = self.execution.engine.advance()
        result = self.session.advance(max_deliveries)
        deadlines = [d for d in (cleanup.next_deadline, result.next_deadline) if d is not None]
        return replace(result,
            has_immediate_work=cleanup.has_immediate_work or result.has_immediate_work,
            next_deadline=min(deadlines) if deadlines else None)

    def wait(self, progress, timeout_s=None):
        if progress.has_immediate_work:
            return
        timeout = self.limits.poll_interval_s if timeout_s is None else timeout_s
        if progress.next_deadline is not None:
            timeout = min(timeout, max(0, progress.next_deadline - self.execution.engine.clock()))
        self.execution.engine.wake.wait(progress.generation, timeout)

    def release(self, leases):
        self.session.release(leases)

    def end_input(self):
        # Methods with continuations must wait until no later stage can be produced.
        self.session.seal()

    def request_cancel(self):
        self.session.request_cancel()

    def close(self, *, clean=False):
        report = self.session.close_consumer(clean=clean)
        if not self._closed:
            self.execution.engine.close_job(self.job)
            self._closed = True
        return report
