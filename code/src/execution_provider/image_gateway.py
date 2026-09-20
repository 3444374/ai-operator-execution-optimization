"""Image protocol specialization of the shared bounded connection owner."""

from __future__ import annotations

from dataclasses import dataclass, replace

from .multiplexed_gateway import MultiSessionMapGateway, MapConnection, PendingOffer
from .adapters.image_execution import build_image_execution
from .adapters.image_method import ImageEmbeddingMethod
from .completion import CompletionAdapterError
from .wire import image
from .wire.framing import ProtocolError, encode_frame, has_duplicate_fields, read_frame
from ..scheduling.core.session_contract import SessionSpec, State, TaskProfile, OfferResult, AdvanceResult
from ..semantic_methods.budget import MethodBudgetPool, MethodCapacity, row_reservation
from ..semantic_methods.continuation import MethodLimits
from ..semantic_methods.driver import MethodDriver, MethodResult, RowIdentity
from ..modalities.image.contracts import build_image_work_descriptor


@dataclass(frozen=True)
class ImageAdvance:
    progress: AdvanceResult
    results: tuple[MethodResult, ...]


class ImageConnection(MapConnection):
    def run_image(self, opened):
        plan = self.gateway.image_config.plan
        mode, window = image.validate_open(opened, plan)
        sequence = 0
        pending = {}
        if mode != self.gateway.image_config.mode or window > self.max_tasks:
            raise ProtocolError("INVALID_OPEN")
        self.connection.sendall(encode_frame(dict(type="opened", protocol_version=7,
            **image.identities(plan, mode), max_inflight_tasks=window)))

        def deliver():
            if not pending:
                raise ProtocolError("INVALID_TASK")
            while True:
                if self._peer_stopped():
                    raise ConnectionResetError("image peer stopped")
                update = self._session.advance(1)
                result = update.progress
                if result.state in (State.FAILED, State.CANCELLED):
                    raise ProtocolError("IMAGE_EXECUTION_FAILED")
                if update.results:
                    item = update.results[0]
                    try:
                        if item.row.sequence not in pending:
                            raise ProtocolError("IMAGE_EXECUTION_FAILED")
                        payload_digest = pending[item.row.sequence]
                        message = image.completion_message(plan, mode, item.row.sequence,
                                                           payload_digest, item.value)
                        self.connection.sendall(encode_frame(message))
                        del pending[item.row.sequence]
                    finally:
                        # A blocked send retains this result lease. Failed send
                        # relinquishes the local consumer before core cleanup.
                        self._session.release((item.row,))
                    return
                if not result.has_immediate_work:
                    self.wait_for_progress(result)

        while True:
            message = read_frame(self.connection)
            if message is None:
                return not pending
            if has_duplicate_fields(message):
                raise ProtocolError("INVALID_TASK")
            if type(message.get("protocol_version")) is not int or message["protocol_version"] != 7:
                raise ProtocolError("INVALID_TASK")
            if message == {"type": "close", "protocol_version": 7}:
                return not pending
            if message == {"type": "receive", "protocol_version": 7} and mode == "staged":
                deliver()
                continue
            request = image.validate_task(message, plan, mode, sequence)
            if len(pending) >= window:
                raise ProtocolError("INVALID_TASK")
            offered = self._session.offer((PendingOffer(request, sequence),))
            accepted = offered.accepted_prefix_count == 1
            if not accepted and offered.status != "BACKPRESSURE":
                raise ProtocolError("IMAGE_EXECUTION_FAILED")
            if accepted:
                pending[sequence] = request.payload_digest
                sequence += 1
            if mode == "reference":
                if not accepted:
                    raise ProtocolError("IMAGE_EXECUTION_FAILED")
                deliver()
            else:
                self.connection.sendall(encode_frame(dict(type="offered", protocol_version=7,
                    sequence=str(sequence - 1 if accepted else sequence), accepted=accepted)))


class ImageGateway(MultiSessionMapGateway):
    def __init__(self, config, *, execution_factory=None, **kwargs):
        self.image_config = config
        super().__init__(config, execution_factory=execution_factory or build_image_execution, **kwargs)
        self.connection_buffer_bytes += kwargs["max_tasks"] * 128
        self.method_limits = MethodLimits(262144, 1, config.plan.result_bytes, 1)
        self.row_reservation = row_reservation(self.method_limits)
        self.method_pool = MethodBudgetPool(MethodCapacity(kwargs["max_tasks"],
            kwargs["max_tasks"] * self.row_reservation))
        self.method_drivers = {}

    def validate_opening_message(self, opened):
        mode, _ = image.validate_open(opened, self.image_config.plan)
        if mode != self.image_config.mode:
            raise ProtocolError("INVALID_OPEN")

    def standalone_spec(self):
        digest = self.image_config.plan.digest
        return SessionSpec("registered", "image", digest, "ai_embed", "pixels",
                           (TaskProfile("image", digest, "ai_embed"),))

    def _register(self, state, operation, args):
        result = super()._register(state, operation, args)
        session = state.mailbox.session
        grant = self.method_pool.allocate(MethodCapacity(
            session.limits.held_tasks, session.limits.held_tasks * self.row_reservation))
        plan = self.image_config.plan

        def describe_work(row, ordinal, request):
            return build_image_work_descriptor(row_count=1, encoded_bytes=len(request.payload),
                model_revision=plan.model_revision, processor_revision=plan.processor_revision,
                dtype=plan.dtype, input_size=plan.input_size, embedding_dimension=plan.dimension)

        try:
            self.method_drivers[session.session_id] = MethodDriver(session,
                ImageEmbeddingMethod(plan), self.method_limits, grant, describe_work=describe_work)
        except BaseException:
            grant.close()
            raise
        self._observe_method("image_method_opened", session.session_id)
        return result

    def _observe_method(self, event, session_id):
        self.observe({"event": event, "engine_session_id": session_id,
            "method_capacity": {"runs": self.method_pool.capacity.runs, "bytes": self.method_pool.capacity.bytes},
            "method_allocated": self.method_pool.allocated, "method_used": self.method_pool.used})

    def session_command(self, state, operation, args):
        session = state.mailbox.session
        driver = self.method_drivers[session.session_id]
        if operation == "offer":
            pending = args[0]
            # Validate the wire identity before transferring this row's ownership.
            self.execution.prepare_task(pending.request, pending.sequence)
            accepted = driver.offer_row(RowIdentity(pending.sequence, "pg-image"), pending.request.encoded)
            self._observe_method("image_row_accepted" if accepted else "image_row_backpressured", session.session_id)
            return OfferResult(int(accepted), "ACCEPTED" if accepted else "BACKPRESSURE",
                               "" if accepted else "method capacity", self.progress.generation)
        if operation == "advance":
            session.set_dispatch_enabled(True)
            progress = driver.advance(1)
            return ImageAdvance(replace(progress, deliveries=(), generation=self.progress.generation), driver.results(1))
        if operation == "release":
            for row in args[0]:
                driver.release_result(row)
            self._observe_method("image_result_released", session.session_id)
            return None
        raise ValueError("unknown image owner command")

    def before_connection_release(self, state):
        session = state.mailbox.session
        if session is None:
            return
        driver = self.method_drivers.pop(session.session_id, None)
        if driver is None:
            return
        try:
            if (state.mailbox.clean and not state.mailbox.cancelled.is_set()
                    and driver.error is None and session.state in (State.OPEN, State.DRAINING)):
                driver.end_input()
                driver.advance(1)  # Seal after all row results have been released.
                driver.advance(1)  # Observe the now-empty sealed session.
        finally:
            driver.close()
            self._observe_method("image_method_closed", session.session_id)

    def close(self):
        closed = super().close()
        return (closed and not self.method_drivers and self.method_pool.used == (0, 0)
                and self.method_pool.allocated == (0, 0)
                and self.engine.capacity.usage().held_tasks == 0)

    def make_connection(self, connection, mailbox, job, limits):
        return ImageConnection(self, connection, mailbox, job, limits)

    def run_connection_protocol(self, state, adapter, connection, opened, handler):
        try:
            state.mailbox.clean = bool(adapter.run_image(opened))
        except (ProtocolError, ValueError, CompletionAdapterError):
            connection.sendall(encode_frame({"type": "error", "protocol_version": 7,
                                             "code": "IMAGE_EXECUTION_FAILED"}))
            raise
