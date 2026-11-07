"""Read image frames and retain each result until its send finishes."""

import select

from .gateway_runtime import PendingOffer, SessionProxy
from .wire import image
from .wire.framing import ProtocolError, encode_frame, has_duplicate_fields, read_frame
from ..scheduling.core.session_contract import State


class ImageConnection:
    def __init__(self, gateway, connection, mailbox, job, limits):
        self.gateway, self.connection, self.mailbox = gateway, connection, mailbox
        self._session = SessionProxy(mailbox)
        self.model_id = gateway.model_id
        self.max_tasks = limits.held_tasks
        self.poll_interval_s = limits.poll_interval_s
        self.job, self.session_id = job, mailbox.session.session_id

    def _peer_stopped(self):
        return self.mailbox.cancelled.is_set() or bool(
            select.select([self.connection], [], [], 0)[0]
        )

    def wait_for_progress(self, result):
        self.gateway.progress.wait(result.generation, self.poll_interval_s)

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
