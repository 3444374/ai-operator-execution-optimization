"""A bounded cancellation watchdog for a dedicated synchronous query connection."""

import threading
import math


class QueryDeadline:
    def __init__(self, seconds, cancel, on_trigger):
        if seconds is not None and (type(seconds) not in (int, float)
                                    or not math.isfinite(seconds) or seconds <= 0 or not callable(cancel)):
            raise ValueError("a positive timeout requires a bounded cancellation callback")
        self.expired = threading.Event()
        self.cancel_error = None
        self._cancel, self._on_trigger = cancel, on_trigger
        self._timer = threading.Timer(seconds, self._expire) if seconds is not None else None
        if self._timer is not None:
            self._timer.daemon = True

    def _expire(self):
        self.expired.set()
        self._on_trigger()
        try:
            self._cancel()
        except BaseException as failure:
            self.cancel_error = failure

    def start(self):
        if self._timer is not None:
            self._timer.start()

    def check(self):
        if self.expired.is_set():
            raise TimeoutError("query deadline exceeded")

    def stop(self):
        if self._timer is not None:
            self._timer.cancel()
            self._timer.join(timeout=2)
            if self._timer.is_alive():
                raise RuntimeError("query cancellation has not settled; connection must not be reused")
        if self.cancel_error is not None:
            raise self.cancel_error
