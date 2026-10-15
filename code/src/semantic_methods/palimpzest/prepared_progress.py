"""Finite vendor iterator progress; query control and continuations stay on the caller thread."""

from queue import Empty, Full, Queue
import threading

from .backend import PalimpzestAdapterError


class FiniteBatchProgress:
    """start/poll/finish seam; a Core implementation can drive sessions directly.

    poll returns one ('outcome', NativeCallOutcome), ('error', BaseException), or
    ('eof', close_report) event, or None after its timeout. EOF means this finite
    batch ended, not that a PZ method or a query/operator session may be sealed.
    finish is invoked only when the method exits, with its actual success status.
    """

    def __init__(self, executor):
        self.executor = executor
        self._events = Queue(maxsize=1)
        self._abort = threading.Event()
        self._reader = None
        self.last_close_report = None
        self.finite_batches = True

    @property
    def active(self):
        return self._reader is not None

    @property
    def accepts_ready(self):
        return not self.active and not self._abort.is_set()

    def _put(self, value):
        while not self._abort.is_set():
            try:
                self._events.put(value, timeout=0.01)
                return
            except Full:
                pass

    def start(self, calls, *, identities, cancelled, timeout_s):
        if self.active or self._abort.is_set():
            raise PalimpzestAdapterError("PZ finite progress still owns its prior iterator")
        calls = tuple(calls)
        if set(identities) != {call.call_id for call in calls}:
            raise PalimpzestAdapterError("PZ ready batch lacks complete occurrence metadata")

        def consume():
            iterator = None
            try:
                iterator = self.executor.iter_batch(calls, cancelled=cancelled, timeout_s=timeout_s)
                for outcome in iterator:
                    self._put(("outcome", outcome))
            except BaseException as error:
                self._put(("error", error))
            finally:
                try:
                    if iterator is not None:
                        iterator.close()
                except BaseException as error:
                    self._put(("error", error))
                self._put(("eof", getattr(self.executor, "last_close_report", None)))

        self._reader = threading.Thread(target=consume, name="pz-native-iterator", daemon=True)
        self._reader.start()
        return len(calls)

    def poll(self, timeout_s):
        try:
            event = self._events.get(timeout=max(0, timeout_s))
        except Empty:
            if self._abort.is_set() and self._reader is not None and not self._reader.is_alive():
                self._reader = None
                self.last_close_report = getattr(self.executor, "last_close_report", None)
                return "eof", self.last_close_report
            return None
        if event[0] == "eof":
            self._reader.join(0.05)
            if self._reader.is_alive():
                raise PalimpzestAdapterError("PZ finite iterator has not relinquished ownership")
            self._reader = None
            self.last_close_report = event[1]
        return event

    def finish(self, *, method_complete, timeout_s):
        # The strategy drains poll before calling finish. No hidden method/session seal.
        if self.active:
            self._abort.set()
            self._reader.join(max(0, timeout_s))
            if not self._reader.is_alive():
                self._reader = None
                self.last_close_report = getattr(self.executor, "last_close_report", None)
        clean = not self.active
        return dict(reader_stopped=clean, method_complete=bool(method_complete),
                    unconfirmed_work=not clean or self._abort.is_set(), executor_close_report=self.last_close_report)
