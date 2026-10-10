"""Native ready batches enter the existing Engine without a vendor request pool."""

from dataclasses import replace
import threading

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
    """Use the declared Ray payload backend; physical=None selects a local diagnostic."""
    if physical is None:
        execution = build_fixed_model_execution(config, execute=execute,
            transport_factory=None if execute else FullResponseTransport, **options)
    else:
        if execute is not None or type(physical) is not RayMapConfig:
            raise ValueError("native Ray execution requires RayMapConfig and its own transport")
        execution = ray_map_factory(replace(physical, response_mode="full"))(config, **options)

    def close(timeout=5.0):
        # Backend teardown cannot claim service cleanup while a flow or unknown
        # remote request still owns the Engine's storage/capacity.
        if execution.engine.capacity.records or execution.engine.jobs.jobs:
            return False
        return execution.close(timeout)

    return replace(execution, close=close)


class NativeQueryJob:
    """One caller-owned query grant, reused by sequential or declared operator flows."""

    def __init__(self, execution, query_id, *, flow_count=1):
        self.execution, self.query_id = execution, query_id
        self.job, self.limits, self.budget = execution.open_query_job(query_id, flow_count)
        self._cancelled = threading.Event()
        self._closed = False
        self.cleanup_errors = []

    def open_session(self, operator_id):
        if self._closed:
            raise RuntimeError('native query Job is closed')
        if self._cancelled.is_set():
            raise InterruptedError('native query Job was cancelled')
        return NativeTaskSession(self.execution, self.query_id, operator_id,
                                 job=self.job, limits=self.limits, cancelled=self._cancelled.is_set)

    def request_cancel(self):
        self._cancelled.set()
        self.execution.engine.wake.notify()

    def close(self):
        self.request_cancel()
        if not self._closed:
            self.execution.engine.close_job(self.job)
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, error_type, error, traceback):
        try:
            self.close()
        except BaseException as cleanup:
            self.cleanup_errors.append(dict(phase='close_job', type=type(cleanup).__name__))
            if error is None:
                raise
            error.add_note('Native query Job cleanup also failed: ' + type(cleanup).__name__)


class NativeTaskSession:
    """One query/operator flow; no extra task, result, or capacity queue.

    The caller supplies finite OfferedTask tuples, retains the unaccepted suffix,
    consumes Delivery.result with decode_full_response, then releases its LeaseId.
    close is called only after the consumer has discarded all delivery bytes.
    The service owner retains the execution and reaps remote unknown work.
    """

    def __init__(self, execution, query_id: str, operator_id: str, *, job=None, limits=None, cancelled=None):
        self.execution = execution
        self._owns_job = job is None
        self._cancelled = cancelled
        spec = SessionSpec(query_id, operator_id, "text-completion", work_unit=execution.work_unit)
        if self._owns_job:
            if limits is not None:
                raise ValueError('native borrowed limits require an explicit query Job')
            self.job, self.session, self.budget = execution.open_job(query_id, spec)
        else:
            self.budget = execution.engine.jobs.require(job, joining=True).budget
            if limits is None or job.label != query_id:
                raise ValueError('native borrowed session requires its query identity and limits')
            self.job = job
            self.session = execution.engine.open(spec, limits, job=job)
        self._closed = False

    @property
    def limits(self):
        return self.session.limits

    def offer(self, tasks):
        if self._cancelled is not None and self._cancelled():
            self.session.request_cancel()
        return self.session.offer(tasks)

    def advance(self, max_deliveries=1):
        if self._cancelled is not None and self._cancelled():
            self.session.request_cancel()
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
        error = None
        try:
            report = self.session.close_consumer(clean=clean)
        except BaseException as failure:
            error = failure
        try:
            if not self._closed:
                if self._owns_job:
                    self.execution.engine.close_job(self.job)
                self._closed = True
        except BaseException as cleanup:
            if error is None:
                raise
            error.add_note('Native owned Job cleanup also failed: ' + type(cleanup).__name__)
        if error is not None:
            raise error
        return report
