"""Live query capabilities; only the gateway owner may register or join flows."""

from dataclasses import dataclass, field
import secrets
import socket
import struct
from enum import Enum

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


class ConnectionEnd(Enum):
    CONTROL_EOF = "control_eof"
    STREAM_CLEAN_END = "stream_clean_end"
    PEER_FAILURE = "peer_failure"
    SERVICE_STOP = "service_stop"


class QueryRegistry:
    """A live control connection owns membership; an empty Job is not query completion."""

    def __init__(self, execution):
        self.execution = execution
        self.registrations = {}

    def register(self, peer, flow_count):
        token = secrets.token_hex(TOKEN_BYTES)
        if token in self.registrations:
            raise ValueError("query capability collision")
        job, limits, budget = self.execution.open_query_job("pg-query", flow_count)
        try:
            registration = QueryRegistration(job, peer, flow_count, limits, token)
            self.registrations[token] = registration
            return registration, budget
        except BaseException:
            self.registrations.pop(token, None)
            self.execution.engine.close_job(job)
            raise

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
        spec = SessionSpec(
            "registered", f"operator-{flow}", "fixed-chat", work_unit=self.execution.work_unit
        )
        session = self.execution.engine.open(spec, registration.limits, job=registration.job)
        try:
            # Publication transfers responsibility to the connection owner only on success.
            registration.joined.add(flow)
            return registration, session
        except BaseException:
            registration.joined.discard(flow)
            session.close_consumer()
            raise

    def has_job_room(self):
        engine = self.execution.engine
        return len(engine.jobs.jobs) < engine.jobs.maximum

    def is_closing(self, job):
        registered = self.execution.engine.jobs.jobs.get(job)
        return registered is not None and registered.closing

    def connection_ended(self, job, registration, session, event, *, consumer_stopped=False):
        """Interpret transport outcomes here; socket closure never settles remote compute."""
        clean = event in (ConnectionEnd.CONTROL_EOF, ConnectionEnd.STREAM_CLEAN_END)
        if consumer_stopped and session is not None:
            session.close_consumer(clean=clean)
        if registration is not None:
            if event != ConnectionEnd.STREAM_CLEAN_END:
                self.close(registration)
        elif job in self.execution.engine.jobs.jobs:
            self.execution.engine.close_job(job)

    def close(self, registration):
        if self.registrations.pop(registration.token, None) is None:
            return
        engine = self.execution.engine
        if registration.job in engine.jobs.jobs:
            engine.close_job(registration.job)
