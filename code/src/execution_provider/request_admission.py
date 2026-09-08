"""Bound model dispatch independently of idle provider connections; never queue work."""

from dataclasses import dataclass
import logging
import threading

from .completion import CompletionAdapter, CompletionAdapterError, CompletionRequest
from .completion import Completion


@dataclass(frozen=True)
class RequestCapacity:
    """Locally active calls and reservations whose remote outcome is unconfirmed."""

    active: int
    uncertain: int


class RequestAdmission:
    """A completion Adapter with immediate rejection and conservative capacity release."""

    def __init__(self, adapter: CompletionAdapter, limit: int):
        if type(limit) is not int or limit < 1:
            raise ValueError("request limit must be a positive integer")
        self._adapter = adapter
        self._limit = limit
        self._lock = threading.Lock()
        self._active = 0
        self._uncertain = 0
        self.model_id = adapter.model_id

    def execution_id_for(self, version: int) -> str | None:
        return self._adapter.execution_id_for(version)

    def capacity(self) -> RequestCapacity:
        with self._lock:
            return RequestCapacity(self._active, self._uncertain)

    def complete(self, request: CompletionRequest) -> Completion:
        with self._lock:
            if self._active + self._uncertain >= self._limit:
                raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
            self._active += 1
        uncertain = False
        try:
            return self._adapter.complete(request)
        except CompletionAdapterError as error:
            uncertain = error.remote_outcome_unknown
            raise
        except BaseException:
            uncertain = True
            raise
        finally:
            with self._lock:
                self._active -= 1
                self._uncertain += int(uncertain)
            if uncertain:
                logging.getLogger(__name__).warning(
                    "model outcome unconfirmed; retaining one request reservation"
                )
