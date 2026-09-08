"""Bounded fixed-model HTTP transport; it owns no queueing or scheduling policy."""

import asyncio
import json
from dataclasses import asdict


class AsyncFixedModelTransport:
    def __init__(self, config, max_active_requests, observer=None):
        self.config = config
        self.max_active_requests = max_active_requests
        self._observer = observer
        self._client = None

    async def execute(self, request, endpoint):
        import httpx

        if endpoint != "model":
            raise ValueError("unknown endpoint")
        if self._client is None:
            headers = {"Content-Type": "application/json", "Accept-Encoding": "identity"}
            if self.config.bearer_token:
                headers["Authorization"] = f"Bearer {self.config.bearer_token}"
            self._client = httpx.AsyncClient(
                headers=headers,
                trust_env=False,
                timeout=None,
                follow_redirects=False,
                limits=httpx.Limits(
                    max_connections=self.max_active_requests,
                    max_keepalive_connections=self.max_active_requests,
                ),
            )
        if self._observer:
            self._observer({"event": "http_started", "key": asdict(request.key)})
        try:
            # Total deadline includes DNS, connection setup and bounded response reads.
            async with asyncio.timeout(self.config.timeout_ms / 1000):
                async with self._client.stream(
                    "POST", self.config.endpoint_url, content=request.task.payload
                ) as response:
                    buffer = bytearray()
                    async for chunk in response.aiter_raw(chunk_size=4096):
                        if len(buffer) + len(chunk) > request.task.max_result_bytes:
                            raise ValueError("response exceeds bound")
                        buffer.extend(chunk)
                    if not 200 <= response.status_code < 300:
                        code = (
                            "MODEL_REQUEST_REJECTED"
                            if 400 <= response.status_code < 500
                            else "MODEL_UNAVAILABLE"
                            if response.status_code >= 500
                            else "MODEL_RESPONSE_INVALID"
                        )
                        return json.dumps({"bridge_error": code}).encode()
                    return bytes(buffer)
        finally:
            if self._observer:
                self._observer({"event": "http_finished", "key": asdict(request.key)})

    async def close(self):
        if self._client is not None:
            await self._client.aclose()
