"""Process-owned spending of prepaid request allowances, without ledger I/O.

Experiment runners own durable reservation and configuration. Model adapters
consume the same finite claim; this does not implement scheduler admission.
"""
from dataclasses import dataclass
import os
import re
import threading
import time

_DIGEST = re.compile(r'[0-9a-f]{64}\Z')


class BudgetError(RuntimeError):
    """The authoritative attempt history could not be verified or persisted."""


class BudgetExhausted(BudgetError):
    """All permitted attempts have been reserved."""


@dataclass(frozen=True)
class Reservation:
    unit_id: str
    first_attempt: int
    requests: int


class ClaimedUnit:
    """One process owns these already-charged attempts; reserve performs no I/O."""
    def __init__(self, reservation: Reservation, deadline_utc: float | None):
        self.reservation = reservation
        self._pid = os.getpid()
        self._deadline = None if deadline_utc is None else time.monotonic() + max(0, deadline_utc - time.time())
        self._used = 0
        self._execution_bound = False
        self._lock = threading.Lock()

    def __getstate__(self):
        raise TypeError('a claimed unit cannot be transferred or copied')

    @property
    def attempts(self):
        with self._lock:
            return self._used

    @property
    def remaining(self):
        return self.reservation.requests - self.attempts

    def bind_execution(self):
        """Bind a precharged claim once before allocating a session model owner."""
        with self._lock:
            if os.getpid() != self._pid:
                raise BudgetError('claimed budget belongs to another process')
            if self._deadline is not None and time.monotonic() >= self._deadline:
                raise BudgetExhausted('experiment deadline reached')
            if self._execution_bound or self._used:
                raise BudgetError('claimed budget execution is already bound or spent')
            self._execution_bound = True

    def reserve(self, request_sha256: str):
        if not isinstance(request_sha256, str) or not _DIGEST.fullmatch(request_sha256):
            raise BudgetError('invalid request digest')
        with self._lock:
            if os.getpid() != self._pid:
                raise BudgetError('claimed budget belongs to another process')
            if self._deadline is not None and time.monotonic() >= self._deadline:
                raise BudgetExhausted('experiment deadline reached')
            if self._used == self.reservation.requests:
                raise BudgetExhausted('reserved unit budget exhausted')
            attempt = self.reservation.first_attempt + self._used
            self._used += 1
            return attempt
