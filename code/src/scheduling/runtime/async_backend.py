"""Bounded coroutine transport for incremental sessions, with no synchronous drive wrapper.

The worker owns asynchronous I/O only. Admission, ordering and resource policy
remain in SessionEngine. A transport exception is not proof that remote work ended.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future, TimeoutError
from itertools import count
from dataclasses import dataclass
from typing import Awaitable, Callable

from ..core.session_contract import (
    Acceptance,
    BackendTask,
    Submission,
    TaskKey,
    Terminal,
    Uncertain,
)


@dataclass
class _Pending:
    handle: str
    task: BackendTask
    future: Future
    reported_unknown: bool = False
    completed_order: int | None = None


class BoundedAsyncBackend:
    """Bound coroutine and result slots; only completed responses produce terminals.

    execute must perform asynchronous I/O and enforce the task's response size
    limit while reading, before buffering an unbounded response. The adapter never
    cancels an HTTP await and misrepresents that as cancellation of model work.
    close is an explicit blocking teardown operation, never called by advance.
    """

    def __init__(
        self,
        execute: Callable[[BackendTask, str], Awaitable[bytes]],
        *,
        max_tasks: int,
        notify: Callable[[], None],
        finalize: Callable[[], Awaitable[None]] | None = None,
        isolate_failures: bool = False,
    ):
        if type(max_tasks) is not int or max_tasks <= 0:
            raise ValueError("max_tasks must be positive")
        self._execute, self._finalize = execute, finalize
        self._isolate_failures = isolate_failures
        self._maximum, self._notify = max_tasks, notify
        self._lock = threading.Lock()
        self._slots: dict[TaskKey, _Pending] = {}
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._closed = False
        self._completion_counter = count()
        self._closing_future: Future | None = None
        self._thread = threading.Thread(target=self._run, name="semloom-async-io", daemon=True)
        self._thread.start()
        if not self._ready.wait(5):
            raise RuntimeError("asynchronous transport did not start")

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        self._loop.run_forever()
        self._loop.close()

    async def _invoke(self, task: BackendTask, endpoint: str) -> bytes:
        return await self._execute(task, endpoint)

    def _completed(self, key: TaskKey) -> None:
        self._slots[key].completed_order = next(self._completion_counter)
        self._notify()

    def try_submit(self, task: BackendTask, endpoint: str) -> Submission:
        if not self._lock.acquire(blocking=False):
            return Submission(Acceptance.NOT_ACCEPTED)
        try:
            if self._closed or len(self._slots) >= self._maximum or task.key in self._slots:
                return Submission(Acceptance.NOT_ACCEPTED)
            handle = f"{task.key.session_id}:{task.key.sequence}"
            coroutine = self._invoke(task, endpoint)
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
            except BaseException:
                coroutine.close()
                raise
            # At most max_tasks callbacks can be queued: completed slots remain
            # occupied until poll transfers their terminal event to the engine.
            self._slots[task.key] = _Pending(handle, task, future)
            future.add_done_callback(lambda done: self._completed(task.key))
            return Submission(Acceptance.ACCEPTED, handle)
        finally:
            self._lock.release()

    def poll(
        self, handles: tuple[tuple[TaskKey, str | None], ...], max_events: int
    ) -> tuple[Terminal | Uncertain, ...]:
        if type(max_events) is not int or max_events <= 0:
            raise ValueError("max_events must be positive")
        if not self._lock.acquire(blocking=False):
            return ()
        try:
            events = []
            completed = sorted(
                (
                    (key, slot)
                    for key, slot in self._slots.items()
                    if slot.completed_order is not None
                ),
                key=lambda item: item[1].completed_order,
            )
            for key, slot in completed:
                if not slot.future.done() or slot.reported_unknown:
                    continue
                # Report an uncertain transport failure once, retaining its slot.
                # No successful event is consumed if this poll raises instead.
                try:
                    result = slot.future.result()
                except BaseException as error:
                    slot.reported_unknown = True
                    if not self._isolate_failures:
                        raise RuntimeError("asynchronous remote outcome unknown") from None
                    events.append(
                        Uncertain(
                            key,
                            slot.handle,
                            "MODEL_TIMEOUT"
                            if isinstance(error, TimeoutError)
                            else "MODEL_UNAVAILABLE",
                        )
                    )
                    if len(events) == max_events:
                        break
                    continue
                if type(result) is not bytes or len(result) > slot.task.task.max_result_bytes:
                    if not self._isolate_failures:
                        slot.reported_unknown = True
                        raise RuntimeError("asynchronous result exceeds its reservation")
                    events.append(Terminal(key, slot.handle, "failed"))
                    if len(events) == max_events:
                        break
                    continue
                if (key, slot.handle) not in handles:
                    raise RuntimeError("asynchronous terminal has no registered owner")
                events.append(Terminal(key, slot.handle, "completed", result))
                if len(events) == max_events:
                    break
            result = tuple(events)
            for event in result:
                if type(event) is Terminal:
                    del self._slots[event.key]
            return result
        finally:
            self._lock.release()

    def request_cancel(self, key: TaskKey, handle: str | None) -> None:
        # There is no authoritative remote-cancel operation in this transport.
        # Continue reading the bounded response so the engine can settle it later.
        return None

    def close(self, timeout: float = 5.0) -> bool:
        if timeout <= 0:
            raise ValueError("close timeout must be positive")
        with self._lock:
            if not self._thread.is_alive():
                return True
            if any(not slot.future.done() for slot in self._slots.values()):
                return False
            if not self._closed:
                self._closed = True
                if self._finalize:
                    self._closing_future = asyncio.run_coroutine_threadsafe(
                        self._finalize(), self._loop
                    )
        error = None
        if self._closing_future:
            try:
                self._closing_future.result(timeout=timeout)
            except TimeoutError:
                return False
            except Exception:
                error = RuntimeError("asynchronous transport finalization failed")
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout)
        if error:
            raise error
        return not self._thread.is_alive()
