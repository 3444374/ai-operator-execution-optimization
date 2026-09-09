"""Live query capabilities; only the gateway owner may register or join flows."""

from dataclasses import dataclass, field
import secrets
import socket
import struct

from ..scheduling.core.session_contract import SessionSpec
from .wire.framing import ProtocolError, has_duplicate_fields

BINDING_VERSION = 1
TOKEN_BYTES = 32


class QueryRegistrationError(Exception):
    """Safe registration failure; never carry credentials or policy exception text."""

    def __init__(self, code):
        self.code = code
        super().__init__(code)


def trusted_peer(connection):
    """Use kernel credentials, never a PID supplied in a frame."""
    if not hasattr(socket, "SO_PEERCRED"):
        raise ValueError("query registration requires kernel peer process credentials")
    pid, uid, _gid = struct.unpack(
        "3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    )
    if pid <= 0 or uid < 0:
        raise ValueError("invalid peer credentials")
    return uid, pid


def validate_registration(message):
    kind = message.get("type")
    fields = {
        "query_open": {"type", "binding_version", "flow_count"},
        "stream_join": {"type", "binding_version", "token", "flow"},
    }.get(kind)
    if (
        fields is None
        or has_duplicate_fields(message)
        or set(message) != fields
        or type(message["binding_version"]) is not int
        or message["binding_version"] != BINDING_VERSION
    ):
        raise ProtocolError("INVALID_OPEN")
    if kind == "query_open":
        value = message["flow_count"]
        if type(value) is not int or not 1 <= value < 2**31:
            raise ProtocolError("INVALID_OPEN")
    else:
        token, flow = message["token"], message["flow"]
        if (
            type(token) is not str
            or len(token) != TOKEN_BYTES * 2
            or any(char not in "0123456789abcdef" for char in token)
            or type(flow) is not int
            or not 0 <= flow < 2**31
        ):
            raise ProtocolError("INVALID_OPEN")
    return kind


@dataclass
class QueryRegistration:
    job: object
    peer: tuple[int, int]
    flow_count: int
    limits: object
    token: str = field(repr=False)
    joined: set[int] = field(default_factory=set)


class QueryRegistry:
    """A live control connection owns membership; an empty Job is not query completion."""

    def __init__(self, execution):
        self.execution = execution
        self.registrations = {}

    def register(self, peer, flow_count):
        job, limits, budget = self.execution.open_query_job("pg-query", flow_count)
        token = secrets.token_hex(TOKEN_BYTES)
        registration = QueryRegistration(job, peer, flow_count, limits, token)
        self.registrations[token] = registration
        return registration, budget

    def join(self, peer, token, flow):
        registration = self.registrations.get(token)
        if (
            registration is None
            or registration.peer != peer
            or type(flow) is not int
            or not 0 <= flow < registration.flow_count
            or flow in registration.joined
        ):
            raise ValueError("query membership rejected")
        session = self.execution.engine.open(
            SessionSpec(
                "registered", f"operator-{flow}", "fixed-chat", work_unit=self.execution.work_unit
            ),
            registration.limits,
            job=registration.job,
        )
        # Ordinals are one-shot in this version; rescan is not supported by the PG carrier.
        registration.joined.add(flow)
        return registration, session

    def close(self, registration):
        if self.registrations.pop(registration.token, None) is None:
            return
        engine = self.execution.engine
        if registration.job in engine.jobs.jobs:
            engine.close_job(registration.job)
