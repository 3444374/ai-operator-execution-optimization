"""Bounded task intake and completion polling over one version-six connection."""

from collections import deque
from dataclasses import asdict

from .completion_response import decode_backend_completion
from ..completion import CompletionAdapterError, CompletionRequest
from ..wire import v6
from ..wire.framing import ProtocolError, encode_frame, read_frame, has_duplicate_fields
from ...scheduling.core.session_contract import State


class IncrementalMapProtocol:
    """One service ledger; delivery leases remain charged until their frame is sent."""

    def execution_id_for(self, version):
        return v6.EXECUTION_ID if version == 6 else None

    def run_incremental(self, connection, opened):
        context = None
        error_sequence = None
        pending = {}
        ready = deque()
        active_delivery = None
        sequence = 0

        def progress():
            result = self._session.advance(self.max_tasks)
            ready.extend(result.deliveries)
            if result.state == State.FAILED:
                raise CompletionAdapterError(
                    result.error
                    if result.error in ("MODEL_TIMEOUT", "MODEL_UNAVAILABLE")
                    else "MODEL_UNAVAILABLE"
                )
            return result

        def send(message):
            connection.sendall(encode_frame(message))

        try:
            context = v6.validate_open(opened)
            if context.model_id != self.model_id:
                raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
            send(
                {
                    "type": "opened",
                    "protocol_version": 6,
                    "semantic_spec_digest": context.semantic_spec_digest,
                    "physical_algorithm_digest": context.physical_algorithm_digest,
                    "provider_execution_digest": context.provider_execution_digest,
                    "max_inflight_tasks": self.max_tasks,
                    "max_frame_bytes": v6.MAX_FRAME_BYTES,
                    "max_input_bytes": v6.MAX_INPUT_BYTES,
                    "max_output_bytes": v6.MAX_OUTPUT_BYTES,
                }
            )
            while True:
                message = read_frame(connection)
                if message is None:
                    return not pending
                if message.get("type") == "poll":
                    if (
                        has_duplicate_fields(message)
                        or message != {"type": "poll", "protocol_version": 6}
                        or type(message["protocol_version"]) is not int
                        or not pending
                    ):
                        raise ProtocolError("INVALID_TASK")
                    # The peer may cancel while a model is running; EOF never settles compute.
                    while not ready:
                        if self._peer_stopped():
                            raise ConnectionResetError("provider peer stopped")
                        result = progress()
                        if not ready and not result.has_immediate_work:
                            self.wait_for_progress(result)
                    delivery = ready.popleft()
                    active_delivery = delivery
                    error_sequence = delivery.key.sequence
                    completion = decode_backend_completion(delivery.result)
                    send(
                        v6.build_completion_message(
                            context,
                            sequence=delivery.key.sequence,
                            payload_digest=pending[delivery.key.sequence],
                            completion=completion,
                        )
                    )
                    if self._observer:
                        self._observer(
                            {
                                "event": "map_completion",
                                "sequence": delivery.key.sequence,
                                "payload_digest": pending[delivery.key.sequence],
                                **asdict(completion),
                            }
                        )
                    self._session.release((delivery.lease_id,))
                    active_delivery = None
                    del pending[delivery.key.sequence]
                else:
                    error_sequence = sequence
                    _, digest = v6.validate_task(
                        message, expected_sequence=sequence, open_context=context
                    )
                    request = CompletionRequest(
                        digest,
                        context.model_id,
                        tuple(dict(item) for item in message["canonical_messages"]),
                        dict(opened["generation_constraints"]),
                        protocol_version=6,
                    )
                    offered = self._session.offer(
                        (self.prepare_task(request, sequence),)
                    )
                    if offered.status == "REJECTED":
                        raise CompletionAdapterError("INVALID_TASK")
                    accepted = offered.accepted_prefix_count
                    if accepted:
                        if self._observer:
                            self._observer({"event": "map_task", "sequence": sequence,
                                            "payload_digest": digest})
                        pending[sequence] = digest
                        sequence += 1
                    # Intake does not dispatch: poll exposes the accepted window to
                    # the existing organizer, rather than forcing singleton groups.
                    send(
                        {
                            "type": "accepted",
                            "protocol_version": 6,
                            "sequence": str(error_sequence),
                            "accepted_prefix_count": accepted,
                        }
                    )
                error_sequence = None
        except (ProtocolError, CompletionAdapterError) as failure:
            code = failure.code if failure.code in v6.ERROR_CODES else "INVALID_TASK"
            try:
                send(v6.build_error_message(code, sequence=error_sequence))
            except OSError:
                pass
        except (OSError, ValueError, RecursionError):
            # Framing/transport failure is terminal, never a task replay.
            return
        finally:
            # These deliveries have settled remotely; no peer can consume them now.
            abandoned = ([active_delivery] if active_delivery is not None else []) + list(ready)
            if abandoned:
                self._session.release(tuple(delivery.lease_id for delivery in abandoned))
