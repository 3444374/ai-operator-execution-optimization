"""Bounded task intake and completion polling over one version-six connection."""

from collections import deque
from dataclasses import asdict
import json

from .incremental_map import IncrementalMapAdapter
from .completion_response import parse_completion
from .semantic_session import CompletionAdapterError, CompletionRequest
from ..wire import v6
from ..wire.framing import ProtocolError, encode_frame, read_frame, has_duplicate_fields
from ...scheduling.core.session_contract import State


class IncrementalMapSessionAdapter(IncrementalMapAdapter):
    """One service ledger; delivery leases remain charged until their frame is sent."""

    def execution_id_for(self, version):
        return v6.EXECUTION_ID if version == 6 else None

    def run_incremental(self, connection, opened):
        context = None
        error_sequence = None
        pending = {}
        ready = deque()
        sequence = 0

        def progress():
            result = self._session.advance(self.max_tasks)
            ready.extend(result.deliveries)
            if result.state == State.FAILED:
                raise CompletionAdapterError(self._transport_error or "MODEL_UNAVAILABLE")
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
                    return
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
                            self.engine.wake.wait(result.generation, 0.01)
                    delivery = ready.popleft()
                    error_sequence = delivery.key.sequence
                    value = json.loads(delivery.result)
                    if isinstance(value, dict) and "bridge_error" in value:
                        raise CompletionAdapterError(value["bridge_error"])
                    try:
                        completion = parse_completion(value)
                    except (ValueError, TypeError):
                        raise CompletionAdapterError("MODEL_RESPONSE_INVALID") from None
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
                    accepted = self._session.offer(
                        (self.prepare_task(request, sequence),)
                    ).accepted_prefix_count
                    if accepted:
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
