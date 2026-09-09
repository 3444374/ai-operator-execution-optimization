"""Dispatch a connection to its independently owned, versioned session state."""

import socket

from .adapters.recording import run_recording_session
from .adapters.semantic_session import (
    run_v3_session,
    run_v4_session,
    run_v5_session,
)
from .completion import CompletionAdapter
from .wire.framing import ProtocolError, read_frame


def run_session(
    connection: socket.socket,
    *,
    completion_adapter: CompletionAdapter,
    response_delay_ms: int,
    tamper_evidence_digest: bool,
    disconnect_on_task: bool,
    completion_fixture: str | None,
    incremental_handler=None,
    open_message=None,
) -> bool | None:
    try:
        try:
            opened = read_frame(connection) if open_message is None else open_message
        except (ProtocolError, ValueError, RecursionError):
            connection.close()
            return
        if opened is None:
            connection.close()
            return
        protocol_version = opened.get("protocol_version")
        if (
            type(protocol_version) is int
            and protocol_version == 6
            and incremental_handler is not None
        ):
            incremental_handler(connection, opened)
            return
        if type(protocol_version) is int and protocol_version in (3, 4, 5):
            run_session = {3: run_v3_session, 4: run_v4_session, 5: run_v5_session}[
                protocol_version
            ]
            return run_session(
                connection,
                completion_adapter,
                open_message=opened,
                response_delay_ms=response_delay_ms,
                tamper_evidence_digest=tamper_evidence_digest,
                disconnect_on_task=disconnect_on_task,
                completion_fixture=completion_fixture,
            )
            return
        run_recording_session(
            connection,
            open_message=opened,
            response_delay_ms=response_delay_ms,
            tamper_evidence_digest=tamper_evidence_digest,
            disconnect_on_task=disconnect_on_task,
            completion_fixture=completion_fixture,
        )
    finally:
        connection.close()
