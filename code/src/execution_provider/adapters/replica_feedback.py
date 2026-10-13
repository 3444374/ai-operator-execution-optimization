"""Bounded completion-rate estimates; service metrics providers remain caller-owned."""

from dataclasses import dataclass
import threading
import time

from ...scheduling.endpoint_routing.replica_adaptation import ReplicaFeedback
from .full_response import decode_full_response
from ..completion import CompletionAdapterError


@dataclass
class _Rate:
    active: int = 0
    last_at: float = 0.0
    busy_s: float = 0.0
    work: int = 0
    rate: float | None = None
    rate_at: float | None = None


class ReplicaCompletionFeedback:
    """Estimated completed work / busy transport time, including client waits.

    This is an owner-local completion signal, not GPU utilization, actual token
    throughput or service capacity. Failed/unknown calls do not add completed
    work. Only Core can retire their outstanding request/work reservations.
    Storage is one accumulator per declared endpoint; no per-request history.
    """

    def __init__(self, endpoint_ids, config, *, observations=None, clock=time.monotonic):
        self._rates = {k: _Rate() for k in endpoint_ids}
        self.config, self.clock = config, clock
        self.observations = observations or {}
        if set(self.observations) - set(self._rates):
            raise ValueError("metrics provider refers to an unknown replica")
        self._lock = threading.Lock()

    def _advance(self, state, now):
        if state.active:
            state.busy_s += max(0.0, now - state.last_at)
        state.last_at = now

    def started(self, endpoint_id):
        with self._lock:
            state = self._rates[endpoint_id]
            self._advance(state, self.clock())
            state.active += 1

    def finished(self, endpoint_id, work, *, successful):
        with self._lock:
            state = self._rates[endpoint_id]
            now = self.clock()
            self._advance(state, now)
            state.active -= 1
            if successful:
                state.work += work
            if state.busy_s > 0 and state.work > 0 and (
                    state.busy_s >= self.config.sample_interval_s or not state.active):
                rate = state.work / state.busy_s
                state.rate = rate if state.rate is None else (
                    self.config.rate_alpha * rate + (1 - self.config.rate_alpha) * state.rate)
                state.rate_at = now
                state.work = 0
                state.busy_s = 0.0
            elif not state.active:
                state.work = 0
                state.busy_s = 0.0

    def latest(self, endpoint_id):
        with self._lock:
            state = self._rates[endpoint_id]
            rate, rate_at, active = state.rate, state.rate_at, state.active
        pressure_at = waiting = kv = None
        provider = self.observations.get(endpoint_id)
        if provider is not None:
            observation = provider.latest(active)
            if observation.sample_age_s is not None and observation.has_service_metrics:
                pressure_at = observation.observed_at_s - observation.sample_age_s
                waiting, kv = observation.waiting, observation.kv_usage
        return ReplicaFeedback(rate_at, rate, pressure_at, waiting, kv)


def successful_response(payload, response_mode):
    if type(payload) is not bytes:
        return False
    if response_mode == "full":
        try:
            return 200 <= decode_full_response(payload).status_code < 300
        except (ValueError, CompletionAdapterError):
            return False
    return not payload.startswith(b'{"bridge_error":')
