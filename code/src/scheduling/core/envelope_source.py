"""Blocking envelope production for the synchronous scheduler."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from queue import Empty, Full, Queue
from typing import Iterable, Iterator

from .models import PayloadEnvelope


SOURCE_QUEUE_PUT_TIMEOUT_S = 0.01
SOURCE_THREAD_CLOSE_TIMEOUT_S = 0.1


@dataclass(frozen=True)
class _SourceFailure:
    error: BaseException


class _ConcurrentEnvelopeSource:
    """Expose a blocking input iterator without blocking completion polling."""

    _END = object()
    _POLL = object()

    def __init__(
        self,
        envelopes: Iterable[PayloadEnvelope],
        *,
        poll_interval_s: float,
    ) -> None:
        self._queue: Queue[object] = Queue(maxsize=1)
        self._cancelled = threading.Event()
        self._poll_interval_s = poll_interval_s
        self._thread = threading.Thread(
            target=self._produce,
            args=(envelopes,),
            daemon=True,
            name="scheduler-envelope-source",
        )
        self._thread.start()

    def __iter__(self) -> Iterator[object]:
        try:
            while True:
                try:
                    item = self._queue.get(timeout=self._poll_interval_s)
                except Empty:
                    yield self._POLL
                    continue
                if item is self._END:
                    return
                if isinstance(item, _SourceFailure):
                    raise item.error
                yield item
        finally:
            self.close()

    def close(self) -> None:
        self._cancelled.set()
        self._thread.join(timeout=SOURCE_THREAD_CLOSE_TIMEOUT_S)

    def _produce(self, envelopes: Iterable[PayloadEnvelope]) -> None:
        try:
            for envelope in envelopes:
                if not self._put(envelope):
                    return
        except BaseException as exc:
            self._put(_SourceFailure(exc))
        finally:
            self._put(self._END)

    def _put(self, item: object) -> bool:
        while not self._cancelled.is_set():
            try:
                self._queue.put(item, timeout=SOURCE_QUEUE_PUT_TIMEOUT_S)
                return True
            except Full:
                continue
        return False
