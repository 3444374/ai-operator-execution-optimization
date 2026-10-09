"""Finite HTTP responses for native parsers, carried in existing result leases."""

from dataclasses import dataclass
import json
import struct

from ..completion import CompletionAdapterError
from .async_fixed_model import AsyncFixedModelTransport


_MAGIC = b"SLHTTP\n\x01"
MAX_RESPONSE_HEADER_BYTES = 8192


@dataclass(frozen=True)
class FullModelResponse:
    status_code: int
    headers: tuple[tuple[str, str], ...]
    body: bytes
    http_version: str = "HTTP/1.1"


def response_prefix(status_code, headers=(), http_version="HTTP/1.1"):
    if type(status_code) is not int or not 100 <= status_code <= 599:
        raise ValueError("invalid HTTP status")
    if type(http_version) is not str or len(http_version.encode()) > 32:
        raise ValueError("invalid HTTP version")
    if type(headers) not in (tuple, list) or len(headers) > 128:
        raise ValueError("response headers exceed bound")
    if any(type(item) not in (tuple, list) or len(item) != 2
           or any(type(value) is not str for value in item) for item in headers):
        raise ValueError("invalid response headers")
    # Check before serializing even a single unexpectedly large header.
    if sum(len(value.encode()) for item in headers for value in item) > MAX_RESPONSE_HEADER_BYTES:
        raise ValueError("response headers exceed bound")
    header = json.dumps(dict(status_code=status_code, headers=headers, http_version=http_version),
                        ensure_ascii=True, separators=(",", ":")).encode()
    if len(header) > MAX_RESPONSE_HEADER_BYTES:
        raise ValueError("response headers exceed bound")
    return _MAGIC + struct.pack("!I", len(header)) + header


def encode_full_response(response: FullModelResponse) -> bytes:
    if type(response.body) is not bytes:
        raise ValueError("response body must be bytes")
    return response_prefix(response.status_code, response.headers, response.http_version) + response.body


def decode_full_response(payload: bytes) -> FullModelResponse:
    if type(payload) is not bytes:
        raise ValueError("response must be bytes")
    if not payload.startswith(_MAGIC):
        # Existing Ray transport reports confirmed local pre-send failures this way.
        # These are never presented to a native parser as an upstream HTTP response.
        try:
            value = json.loads(payload)
        except (ValueError, UnicodeError):
            value = None
        if isinstance(value, dict) and value.get("bridge_error") in (
            "MODEL_REQUEST_REJECTED", "MODEL_UNAVAILABLE", "MODEL_RESPONSE_INVALID"
        ):
            raise CompletionAdapterError(value["bridge_error"])
        raise ValueError("invalid complete response")
    start = len(_MAGIC) + 4
    if len(payload) < start:
        raise ValueError("invalid complete response")
    size = struct.unpack("!I", payload[len(_MAGIC):start])[0]
    if size > MAX_RESPONSE_HEADER_BYTES or len(payload) < start + size:
        raise ValueError("invalid complete response")
    try:
        header = json.loads(payload[start:start + size])
        if not isinstance(header, dict) or set(header) != {"status_code", "headers", "http_version"}:
            raise ValueError("invalid complete response")
        response_prefix(header["status_code"], header["headers"], header["http_version"])
        return FullModelResponse(header["status_code"], tuple(tuple(h) for h in header["headers"]),
                                 payload[start + size:], header["http_version"])
    except (UnicodeError, TypeError, KeyError) as error:
        raise ValueError("invalid complete response") from error


class FullResponseTransport(AsyncFixedModelTransport):
    def response_prefix(self, response):
        return response_prefix(response.status_code,
            tuple((name.decode("ascii"), value.decode("latin-1")) for name, value in response.headers.raw),
            response.http_version)

    def response_result(self, response, buffer):
        return buffer
