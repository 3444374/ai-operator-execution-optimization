"""Fixed OpenAI-compatible Chat Completions adapter for validated semantic tasks."""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from urllib import parse

from .model_config import (
    FixedModelConfig as FixedModelConfig,
    load_fixed_model_config as load_fixed_model_config,
    MAX_MODEL_RESPONSE_BYTES as MAX_MODEL_RESPONSE_BYTES,
    VLLM_CHOICE_FORMAT as VLLM_CHOICE_FORMAT,
)
from ..generation_profile import GenerationProfile
from .completion_response import parse_completion as _parse_completion
from ..wire.v5 import FIXED_EXECUTION_ID as MAP_EXECUTION_ID
from ..completion import CompletionAdapterError, Completion, CompletionRequest


FIXED_EXECUTION_ID = "semloom.provider.openai-compatible-fixed.uds.v3"
CHOICE_EXECUTION_ID = "semloom.provider.openai-compatible-fixed.uds.v4"
_RESPONSE_READ_BYTES = 65_536


class _RequestDeadline:
    """Abort one fixed-endpoint connection when its total deadline expires."""

    def __init__(self, timeout_ms: int) -> None:
        timeout_seconds = timeout_ms / 1000
        self._expires_at = time.monotonic() + timeout_seconds
        self._connection: http.client.HTTPConnection | None = None
        self._connection_socket: socket.socket | None = None
        self._expired = False
        self._lock = threading.Lock()
        self._timer = threading.Timer(timeout_seconds, self._expire)
        self._timer.daemon = True
        self._timer.start()

    @property
    def expired(self) -> bool:
        with self._lock:
            return self._expired

    def remaining_seconds(self) -> float:
        remaining = self._expires_at - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        return remaining

    def bind(self, connection: http.client.HTTPConnection) -> None:
        with self._lock:
            if self._expired:
                expired = True
            else:
                self._connection = connection
                expired = False
        if expired:
            connection.close()
            raise TimeoutError

    def bind_socket(self, connection_socket: socket.socket | None) -> None:
        if connection_socket is None:
            raise OSError("fixed endpoint connection has no socket")
        with self._lock:
            if self._expired:
                expired = True
            else:
                self._connection_socket = connection_socket
                expired = False
        if expired:
            try:
                connection_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            raise TimeoutError

    def set_socket_timeout(self) -> None:
        timeout_seconds = self.remaining_seconds()
        with self._lock:
            connection_socket = self._connection_socket
        if connection_socket is None:
            raise OSError("fixed endpoint connection has no socket")
        connection_socket.settimeout(timeout_seconds)

    def bind_response(self, response: http.client.HTTPResponse) -> None:
        with self._lock:
            expired = self._expired
        if expired:
            response.close()
            raise TimeoutError

    def close(self) -> None:
        self._timer.cancel()
        self._timer.join()

    def _expire(self) -> None:
        with self._lock:
            self._expired = True
            connection = self._connection
            connection_socket = self._connection_socket
            if connection_socket is None and connection is not None:
                connection_socket = connection.sock
        if connection_socket is not None:
            try:
                connection_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if connection is not None:
            connection.close()


class _ResolutionAttempt:
    """One shared, bounded DNS lookup for the fixed endpoint."""

    def __init__(self) -> None:
        self.completed = threading.Event()
        self.addresses: list[tuple[int, int, int, str, tuple[object, ...]]] | None = None
        self.error: Exception | None = None


class _FixedEndpointResolver:
    """Resolve once per successful endpoint identity with at most one worker."""

    def __init__(self, hostname: str, port: int) -> None:
        self._hostname = hostname
        self._port = port
        self._attempt: _ResolutionAttempt | None = None
        self._lock = threading.Lock()

    def resolve(
        self,
        deadline: _RequestDeadline,
    ) -> list[tuple[int, int, int, str, tuple[object, ...]]]:
        with self._lock:
            attempt = self._attempt
            if attempt is None or (attempt.completed.is_set() and attempt.error is not None):
                attempt = _ResolutionAttempt()
                self._attempt = attempt
                worker = threading.Thread(
                    target=self._resolve,
                    args=(attempt,),
                    daemon=True,
                )
                worker.start()

        if not attempt.completed.wait(timeout=deadline.remaining_seconds()):
            raise TimeoutError
        deadline.remaining_seconds()
        if attempt.error is not None:
            raise attempt.error
        if not attempt.addresses:
            raise OSError("fixed endpoint resolver returned no addresses")
        return attempt.addresses

    def _resolve(self, attempt: _ResolutionAttempt) -> None:
        try:
            addresses = socket.getaddrinfo(
                self._hostname,
                self._port,
                type=socket.SOCK_STREAM,
            )
            if not addresses:
                raise OSError("fixed endpoint resolver returned no addresses")
            attempt.addresses = addresses
        except Exception as error:
            attempt.error = error
        finally:
            attempt.completed.set()


class OpenAICompatibleFixedAdapter:
    """Send each validated task to one fixed non-streaming model endpoint."""

    execution_id = FIXED_EXECUTION_ID
    choice_execution_id = CHOICE_EXECUTION_ID

    def execution_id_for(self, protocol_version: int) -> str | None:
        return {3: self.execution_id, 4: self.choice_execution_id, 5: MAP_EXECUTION_ID}.get(
            protocol_version
        )

    def __init__(self, config: FixedModelConfig) -> None:
        self._config = config
        self.model_id = config.model_id
        parsed = parse.urlsplit(config.endpoint_url)
        hostname = parsed.hostname
        assert hostname is not None
        endpoint_port = parsed.port
        if endpoint_port is None:
            endpoint_port = 443 if parsed.scheme == "https" else 80
        self._resolver = _FixedEndpointResolver(hostname, endpoint_port)

    def complete(self, completion_request: CompletionRequest) -> Completion:
        if completion_request.model_id != self._config.model_id:
            raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
        body = {
            "model": completion_request.model_id,
            "messages": list(completion_request.canonical_messages),
            **completion_request.generation_constraints,
        }
        profile = completion_request.generation_profile
        if profile is not None:
            if (
                type(profile) is not GenerationProfile
                or self._config.choice_format != VLLM_CHOICE_FORMAT
            ):
                raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
            body["structured_outputs"] = {"choice": list(profile.choices)}
        payload = json.dumps(
            body,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self._config.bearer_token is not None:
            headers["Authorization"] = f"Bearer {self._config.bearer_token}"
        parsed = parse.urlsplit(self._config.endpoint_url)
        endpoint_path = parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
        connection_type = (
            http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        )
        deadline = _RequestDeadline(self._config.timeout_ms)
        connection: http.client.HTTPConnection | None = None
        response: http.client.HTTPResponse | None = None
        dispatched = False
        terminal_response = False
        try:
            resolved_addresses = self._resolver.resolve(deadline)
            connection = connection_type(
                parsed.hostname,
                parsed.port,
                timeout=deadline.remaining_seconds(),
            )

            def connect_resolved(
                _address: tuple[str, int],
                _timeout: object,
                source_address: tuple[str, int] | None,
            ) -> socket.socket:
                return _connect_resolved(
                    resolved_addresses,
                    deadline,
                    source_address=source_address,
                )

            connection._create_connection = connect_resolved  # type: ignore[attr-defined]
            deadline.bind(connection)
            connection.connect()
            deadline.bind_socket(connection.sock)
            deadline.set_socket_timeout()
            dispatched = True
            connection.request("POST", endpoint_path, body=payload, headers=headers)
            deadline.bind_socket(connection.sock)
            deadline.set_socket_timeout()
            response = connection.getresponse()
            deadline.bind_response(response)
            if 300 <= response.status < 400 or not (200 <= response.status < 300):
                terminal_response = True
                if 400 <= response.status < 500:
                    code = "MODEL_REQUEST_REJECTED"
                elif 500 <= response.status < 600:
                    code = "MODEL_UNAVAILABLE"
                else:
                    code = "MODEL_RESPONSE_INVALID"
                raise CompletionAdapterError(code)
            response_bytes = _read_response(response, deadline)
            terminal_response = True
        except (TimeoutError, socket.timeout):
            raise CompletionAdapterError(
                "MODEL_TIMEOUT", remote_outcome_unknown=dispatched and not terminal_response
            ) from None
        except http.client.HTTPException:
            code = "MODEL_TIMEOUT" if deadline.expired else "MODEL_RESPONSE_INVALID"
            raise CompletionAdapterError(
                code, remote_outcome_unknown=dispatched and not terminal_response
            ) from None
        except OSError:
            code = "MODEL_TIMEOUT" if deadline.expired else "MODEL_UNAVAILABLE"
            raise CompletionAdapterError(
                code, remote_outcome_unknown=dispatched and not terminal_response
            ) from None
        finally:
            if response is not None:
                response.close()
            if connection is not None:
                connection.close()
            deadline.close()
        if deadline.expired:
            raise CompletionAdapterError("MODEL_TIMEOUT")
        try:
            response_value = json.loads(response_bytes.decode("utf-8"))
            return _parse_completion(response_value)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            raise CompletionAdapterError("MODEL_RESPONSE_INVALID") from None


def _connect_resolved(
    addresses: list[tuple[int, int, int, str, tuple[object, ...]]],
    deadline: _RequestDeadline,
    *,
    source_address: tuple[str, int] | None,
) -> socket.socket:
    last_error: OSError | None = None
    for family, socket_type, protocol, _canonical_name, socket_address in addresses:
        connection_socket: socket.socket | None = None
        try:
            connection_socket = socket.socket(family, socket_type, protocol)
            if source_address is not None:
                connection_socket.bind(source_address)
            connection_socket.settimeout(deadline.remaining_seconds())
            connection_socket.connect(socket_address)
            return connection_socket
        except TimeoutError:
            if connection_socket is not None:
                connection_socket.close()
            raise
        except OSError as error:
            last_error = error
            if connection_socket is not None:
                connection_socket.close()
    if last_error is not None:
        raise last_error
    raise OSError("fixed endpoint resolver returned no usable addresses")


def _read_response(
    response: http.client.HTTPResponse,
    deadline: _RequestDeadline,
) -> bytes:
    buffer = bytearray()
    while len(buffer) <= MAX_MODEL_RESPONSE_BYTES:
        deadline.remaining_seconds()
        chunk = response.read1(
            min(
                _RESPONSE_READ_BYTES,
                MAX_MODEL_RESPONSE_BYTES + 1 - len(buffer),
            )
        )
        deadline.remaining_seconds()
        if not chunk:
            if response.length not in (None, 0):
                raise CompletionAdapterError("MODEL_RESPONSE_INVALID", remote_outcome_unknown=True)
            break
        buffer.extend(chunk)
        deadline.remaining_seconds()
    if deadline.expired:
        raise TimeoutError
    if len(buffer) > MAX_MODEL_RESPONSE_BYTES:
        raise CompletionAdapterError("MODEL_RESPONSE_INVALID", remote_outcome_unknown=True)
    return bytes(buffer)


__all__ = [
    "FIXED_EXECUTION_ID",
    "FixedModelConfig",
    "OpenAICompatibleFixedAdapter",
    "load_fixed_model_config",
]
