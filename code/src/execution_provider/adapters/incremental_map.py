"""Window-one compatibility adapter over the shared incremental Map runtime."""

from ...scheduling.core.session_contract import State
from ..completion import Completion, CompletionAdapterError, CompletionRequest
from ..wire import v5
from .completion_response import decode_backend_completion
from .incremental_runtime import IncrementalMapRuntime


class IncrementalMapAdapter(IncrementalMapRuntime):
    """Keep the v5 synchronous completion interface on the common service ledger."""

    def execution_id_for(self, version: int) -> str | None:
        return v5.INCREMENTAL_EXECUTION_ID if version == 5 else None

    def complete(self, request: CompletionRequest) -> Completion:
        if (
            self._session is None
            or request.protocol_version != 5
            or request.model_id != self.model_id
        ):
            raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
        if request.generation_profile is not None:
            raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
        task = self.prepare_task(request, self._sequence)
        if self._peer_stopped():
            raise ConnectionResetError("provider peer stopped")
        if self._session.offer((task,)).accepted_prefix_count != 1:
            raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
        self._sequence += 1
        while True:
            if self._peer_stopped():
                self._session.cancel()
                raise ConnectionResetError("provider peer stopped")
            result = self._session.advance(1)
            if result.state == State.FAILED:
                raise CompletionAdapterError(
                    self._transport_error or "MODEL_UNAVAILABLE",
                    remote_outcome_unknown=bool(self.engine.capacity.usage().active_requests),
                )
            if result.deliveries:
                (delivery,) = result.deliveries
                try:
                    return decode_backend_completion(delivery.result)
                finally:
                    self._session.release((delivery.lease_id,))
            if not result.has_immediate_work:
                self.engine.wake.wait(
                    result.generation, self.engine.capacity.limits.poll_interval_s
                )
