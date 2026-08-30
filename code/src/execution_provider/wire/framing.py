"""Bounded length-prefixed UTF-8 JSON framing shared by versioned wire schemas."""

from __future__ import annotations

import json
import socket
import struct
from typing import Any


MAX_FRAME_BYTES = 1024 * 1024


class ProtocolError(Exception):
    """A fail-closed protocol violation safe to report without payload data."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def encode_frame(message: dict[str, Any]) -> bytes:
    """Encode one canonical UTF-8 JSON frame with a four-byte big-endian length."""
    payload = json.dumps(
        message,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if not payload or len(payload) > MAX_FRAME_BYTES:
        raise ProtocolError("invalid_frame_length")
    return struct.pack("!I", len(payload)) + payload


def read_frame(connection: socket.socket) -> dict[str, Any] | None:
    """Read one bounded frame; return None only for clean EOF between frames."""
    header = _read_exact(connection, 4, allow_initial_eof=True)
    if header is None:
        return None
    length = struct.unpack("!I", header)[0]
    if length == 0 or length > MAX_FRAME_BYTES:
        raise ProtocolError("invalid_frame_length")
    raw = _read_exact(connection, length, allow_initial_eof=False)
    assert raw is not None
    try:
        message = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError("invalid_json") from error
    if not isinstance(message, dict):
        raise ProtocolError("invalid_message")
    return message


def _read_exact(
    connection: socket.socket,
    length: int,
    *,
    allow_initial_eof: bool,
) -> bytes | None:
    chunks: list[bytes] = []
    received = 0
    while received < length:
        chunk = connection.recv(length - received)
        if not chunk:
            if allow_initial_eof and received == 0:
                return None
            raise ProtocolError("unexpected_eof")
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)
