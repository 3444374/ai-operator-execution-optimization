"""Complete prepared calls over the existing, version-checked native HTTP kernel."""

import hashlib
import ipaddress
import json
from pathlib import Path
from urllib.parse import urlsplit

from src.baselines.text.frameworks import native_calls
from src.scheduling.runtime.async_backend import ConfirmedTaskFailure
from . import full_response
from .model_config import FixedModelConfig


_SOURCE_HASHES = {
    native_calls: "ec02723d9d997d120ea66429b1edaa87709c7028e37aa1339e6f0a6bc3ffbb6f",
    full_response: "ef0bdfeb99bd5aab1c1b4002ee1f61469ee270966437d71eac66b2ddbd99ebde",
}
NATIVE_CALL_PROFILE = 'fixed-local-nonredirect.v1'


def validate_native_call_sources():
    for module, expected in _SOURCE_HASHES.items():
        if hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() != expected:
            raise RuntimeError("NativeCall complete-call dependency changed: " + module.__name__)


def native_call_transport_identity():
    """Workers and callers compare the complete transport, including private helpers."""
    validate_native_call_sources()
    return dict(schema="semloom.native_call_full.v1", profile=NATIVE_CALL_PROFILE, sources={
        "native_call_transport": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        **{module.__name__: value for module, value in _SOURCE_HASHES.items()},
    })


class NativeCallTransport:
    """Keep the complete call and response; the caller owns scheduling and parsing."""

    def __init__(self, config, max_active_requests, observer=None):
        if type(config) is not FixedModelConfig or config.choice_format is not None:
            raise TypeError("complete calls require a fixed ordinary model configuration")
        address = urlsplit(config.endpoint_url)
        try:
            loopback = ipaddress.ip_address(address.hostname).is_loopback
        except ValueError:
            loopback = address.hostname == "localhost"
        if not loopback or address.scheme != "http" or address.query or config.timeout_ms > 120000:
            raise ValueError("complete calls require one registered finite loopback route")
        if type(max_active_requests) is not int or max_active_requests < 1:
            raise ValueError("complete calls require a positive request capacity")
        validate_native_call_sources()
        self.config, self.max_active_requests = config, max_active_requests
        self._client = None
        self.closed = False

    def validate_call(self, call):
        if type(call) is not native_calls.NativeCall or call.endpoint != self.config.endpoint_url:
            raise ValueError("complete call changed its registered route")
        if call.follow_redirects:
            raise ValueError("complete-call fixed local profile forbids following redirects")
        authorization = [value for name, value in call.headers if name.lower() == "authorization"]
        if (authorization != ["Bearer " + (self.config.bearer_token or "EMPTY")]
                or call.timeout_s != self.config.timeout_ms / 1000):
            raise ValueError("complete call changed its fixed authorization or deadline")
        value = json.loads(call.payload)
        if type(value) is not dict or value.get("model") != self.config.model_id or value.get("stream", False):
            raise ValueError("complete call changed its single-model nonstreaming body")

    async def execute(self, request, endpoint):
        try:
            if self.closed or endpoint != "model":
                raise ValueError("complete-call transport is unavailable")
            call = native_calls._call_from_wire(request.task.payload.decode())
            self.validate_call(call)
            info = request.task.info
            if (info is None or (info.call_id, info.row_sequence) != (call.call_id, call.row_id)
                    or call.max_response_bytes > request.task.max_result_bytes):
                raise ValueError("complete call changed its task identity or result reservation")
        except Exception as error:
            raise ConfirmedTaskFailure(request.key, {"exception_type": type(error).__name__}) from error
        if self._client is None:
            self._client = native_calls._new_http_client()
        outcome = await native_calls._request(call, client=self._client)
        if ((outcome.call_id, outcome.row_id) != (call.call_id, call.row_id)
                or native_calls.outcome_kind(outcome) != "response"):
            # A completed local await does not establish that remote work stopped.
            raise RuntimeError("complete-call remote outcome is unknown")
        return full_response.encode_full_response(outcome.response)

    async def close(self):
        if self._client is not None:
            await native_calls._close_http_client(self._client)
        self.closed = True
