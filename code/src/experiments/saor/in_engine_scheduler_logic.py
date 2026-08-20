"""Pure DRR/VTC scheduling semantics for the vLLM reproduction capability.

These classes deliberately contain no vLLM, Daft, Ray, credit, ready-window,
or network code.  They are the executable semantic oracle for a future
``--scheduler-cls`` adapter; they are not themselves a serving scheduler.
"""

from __future__ import annotations

import heapq
import math
import re
from collections import deque
from dataclasses import dataclass


_CLIENT_ID = re.compile(r"job(?:0|[1-9][0-9]*)\Z")
_REQUEST_TOKEN = re.compile(r"[0-9a-f]{32}\Z")
_IDENTITY_PREFIX = "saor-xlayer.v1"


class RequestIdentityError(ValueError):
    """Raised instead of silently assigning an invalid request to one client."""


def encode_request_identity(client_id: str, request_token: str) -> str:
    """Build the exact value intended for the HTTP ``X-Request-Id`` header."""

    if not _CLIENT_ID.fullmatch(client_id):
        raise RequestIdentityError("client_id must be a canonical jobN identifier")
    if not _REQUEST_TOKEN.fullmatch(request_token):
        raise RequestIdentityError("request token must be 32 lowercase hex characters")
    return f"{_IDENTITY_PREFIX}/{client_id}/{request_token}"


def decode_request_identity(request_id: object) -> tuple[str, str]:
    """Recover ``(client_id, request_token)`` and fail closed on any drift."""

    if not isinstance(request_id, str):
        raise RequestIdentityError("request identity is missing")
    parts = request_id.split("/")
    if len(parts) != 3 or parts[0] != _IDENTITY_PREFIX:
        raise RequestIdentityError("request identity has an invalid envelope")
    client_id, request_token = parts[1:]
    if not _CLIENT_ID.fullmatch(client_id):
        raise RequestIdentityError("request identity contains an invalid client_id")
    if not _REQUEST_TOKEN.fullmatch(request_token):
        raise RequestIdentityError("request identity contains an invalid unique token")
    return client_id, request_token


def recover_unique_client_ids(
    request_ids: list[object],
    *,
    expected_clients: set[str] | None = None,
) -> tuple[str, ...]:
    """Decode a batch while rejecting duplicate identities and client collapse."""

    decoded = [decode_request_identity(value) for value in request_ids]
    tokens = [token for _client, token in decoded]
    if len(tokens) != len(set(tokens)):
        raise RequestIdentityError("request identities are not unique")
    clients = tuple(client for client, _token in decoded)
    if expected_clients is not None and set(clients) != expected_clients:
        raise RequestIdentityError(
            "request client partition collapsed or contains an unexpected client"
        )
    return clients


@dataclass(frozen=True)
class FairRequest:
    """One scheduler-visible request with a fixed-output work estimate."""

    request_id: str
    client_id: str
    prompt_tokens: int
    output_cap: int
    arrival_seq: int

    def __post_init__(self) -> None:
        if not self.request_id:
            raise ValueError("request_id must be non-empty")
        if not _CLIENT_ID.fullmatch(self.client_id):
            raise ValueError("client_id must be a canonical jobN identifier")
        if self.prompt_tokens < 0 or self.output_cap <= 0:
            raise ValueError("prompt_tokens must be non-negative and output_cap positive")
        if self.arrival_seq < 0:
            raise ValueError("arrival_seq must be non-negative")

    @property
    def estimated_work(self) -> int:
        """The preregistered DRR charge: prompt tokens plus frozen output cap."""

        return self.prompt_tokens + self.output_cap


class FcfsPolicy:
    """Deterministic global FCFS oracle used by custom-FCFS order tests."""

    def __init__(self) -> None:
        self._queue: list[tuple[int, int, str, FairRequest]] = []
        self._enqueue_seq = 0
        self._request_ids: set[str] = set()

    def enqueue(self, request: FairRequest) -> None:
        if request.request_id in self._request_ids:
            raise ValueError("duplicate request_id")
        self._request_ids.add(request.request_id)
        heapq.heappush(
            self._queue,
            (request.arrival_seq, self._enqueue_seq, request.request_id, request),
        )
        self._enqueue_seq += 1

    def pop_next(self) -> FairRequest | None:
        if not self._queue:
            return None
        return heapq.heappop(self._queue)[-1]


class DrrPolicy:
    """Pure per-client FIFO deficit round robin for fixed-output requests."""

    def __init__(self, quantum: int) -> None:
        if quantum <= 0:
            raise ValueError("DRR quantum must be positive")
        self.quantum = quantum
        self._queues: dict[str, deque[FairRequest]] = {}
        self._active: deque[str] = deque()
        self._deficits: dict[str, int] = {}
        self._request_ids: set[str] = set()

    def enqueue(self, request: FairRequest) -> None:
        if request.request_id in self._request_ids:
            raise ValueError("duplicate request_id")
        self._request_ids.add(request.request_id)
        queue = self._queues.setdefault(request.client_id, deque())
        was_empty = not queue
        queue.append(request)
        if was_empty:
            # Standard DRR clears credit after an idle period.  Deficit is
            # retained across rounds only while the client stays backlogged.
            self._deficits[request.client_id] = 0
            self._active.append(request.client_id)

    def pop_next(self) -> FairRequest | None:
        if not self._active:
            return None
        while self._active:
            client_id = self._active[0]
            queue = self._queues[client_id]
            self._deficits[client_id] += self.quantum
            head = queue[0]
            if head.estimated_work > self._deficits[client_id]:
                self._active.rotate(-1)
                continue
            request = queue.popleft()
            self._deficits[client_id] -= request.estimated_work
            if queue:
                self._active.rotate(-1)
            else:
                self._active.popleft()
                self._deficits[client_id] = 0
            return request
        return None

    def deficit(self, client_id: str) -> int:
        return self._deficits.get(client_id, 0)


@dataclass
class _InFlightService:
    client_id: str
    output_tokens_accounted: int = 0


class VtcPolicy:
    """Virtual-token-counter oracle with online actual-service accounting.

    Prompt service is known and charged at dispatch.  Output service is charged
    incrementally as it becomes known, or at completion.  No output oracle is
    consulted when selecting a request.
    """

    def __init__(self, weights: dict[str, float] | None = None) -> None:
        self._weights: dict[str, float] = {}
        for client_id, weight in (weights or {}).items():
            self._validate_weight(client_id, weight)
            self._weights[client_id] = float(weight)
        self._queues: dict[str, deque[FairRequest]] = {}
        self._virtual_counters: dict[str, float] = {}
        self._actual_service: dict[str, int] = {}
        self._inflight: dict[str, _InFlightService] = {}
        self._request_ids: set[str] = set()

    @staticmethod
    def _validate_weight(client_id: str, weight: float) -> None:
        if not _CLIENT_ID.fullmatch(client_id):
            raise ValueError("client_id must be a canonical jobN identifier")
        if (
            isinstance(weight, bool)
            or not isinstance(weight, (int, float))
            or not math.isfinite(weight)
            or weight <= 0
        ):
            raise ValueError("VTC client weight must be finite and positive")

    def _is_active(self, client_id: str) -> bool:
        return bool(self._queues.get(client_id)) or any(
            item.client_id == client_id for item in self._inflight.values()
        )

    def _active_clients(self) -> tuple[str, ...]:
        known = set(self._queues) | {
            item.client_id for item in self._inflight.values()
        }
        return tuple(sorted(client for client in known if self._is_active(client)))

    def enqueue(self, request: FairRequest, *, weight: float | None = None) -> None:
        if request.request_id in self._request_ids:
            raise ValueError("duplicate request_id")
        was_active = self._is_active(request.client_id)
        if weight is not None:
            self._validate_weight(request.client_id, weight)
            existing = self._weights.get(request.client_id)
            if existing is not None and existing != float(weight):
                raise ValueError("VTC client weight changed")
            self._weights[request.client_id] = float(weight)
        self._weights.setdefault(request.client_id, 1.0)
        self._virtual_counters.setdefault(request.client_id, 0.0)
        self._actual_service.setdefault(request.client_id, 0)
        if not was_active:
            active = self._active_clients()
            if active:
                active_floor = min(self._virtual_counters[item] for item in active)
                self._virtual_counters[request.client_id] = max(
                    self._virtual_counters[request.client_id], active_floor
                )
        self._queues.setdefault(request.client_id, deque()).append(request)
        self._request_ids.add(request.request_id)

    def pop_next(self) -> FairRequest | None:
        backlogged = [
            client_id for client_id, queue in self._queues.items() if queue
        ]
        if not backlogged:
            return None
        client_id = min(
            backlogged,
            key=lambda item: (
                self._virtual_counters[item],
                self._queues[item][0].arrival_seq,
                item,
                self._queues[item][0].request_id,
            ),
        )
        request = self._queues[client_id].popleft()
        self._inflight[request.request_id] = _InFlightService(client_id)
        self._charge(client_id, request.prompt_tokens)
        return request

    def account_output(self, request_id: str, output_token_delta: int) -> None:
        if output_token_delta < 0:
            raise ValueError("output token delta must be non-negative")
        inflight = self._inflight.get(request_id)
        if inflight is None:
            raise ValueError("output service references an unknown in-flight request")
        inflight.output_tokens_accounted += output_token_delta
        self._charge(inflight.client_id, output_token_delta)

    def complete(self, request_id: str, *, actual_output_tokens: int) -> None:
        inflight = self._inflight.get(request_id)
        if inflight is None:
            raise ValueError("completion references an unknown in-flight request")
        if actual_output_tokens < inflight.output_tokens_accounted:
            raise ValueError("actual output service is below already-accounted service")
        self.account_output(
            request_id, actual_output_tokens - inflight.output_tokens_accounted
        )
        del self._inflight[request_id]

    def _charge(self, client_id: str, token_service: int) -> None:
        self._actual_service[client_id] += token_service
        self._virtual_counters[client_id] += (
            token_service / self._weights[client_id]
        )

    def virtual_counter(self, client_id: str) -> float:
        return self._virtual_counters.get(client_id, 0.0)

    def accumulated_service(self, client_id: str) -> int:
        return self._actual_service.get(client_id, 0)

    @property
    def has_backlog(self) -> bool:
        return any(self._queues.values())
