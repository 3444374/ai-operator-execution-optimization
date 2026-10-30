"""Bounded coroutine transport for incremental sessions, with no synchronous drive wrapper.

The worker owns asynchronous I/O only. Admission, ordering and resource policy
remain in SessionEngine. A transport exception is not proof that remote work ended.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future, TimeoutError, wait
import time
from itertools import count
from dataclasses import dataclass
from typing import Awaitable, Callable

from ..core.session_contract import (
    Acceptance,
    BackendTask,
    BatchSubmission,
    Submission,
    TaskKey,
    Terminal,
    Uncertain,
)


class ConfirmedTaskFailure(RuntimeError):
    """An adapter has affirmative evidence that this task's remote borrowing ended."""

    def __init__(self, key: TaskKey, reason: dict):
        self.key, self.reason = key, reason
        super().__init__("confirmed task failure: " + reason["exception_type"])


@dataclass
class _Pending:
    handle: str
    task: BackendTask
    future: Future
    reported_unknown: bool = False
    completed_order: int | None = None
    producer_stopped: bool = False


class BoundedAsyncBackend:
    """Bound coroutine and result slots; responses and confirmed failures produce terminals.

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
        cancel_pending: Callable[[TaskKey], None] | None = None,
    ):
        if type(max_tasks) is not int or max_tasks <= 0:
            raise ValueError("max_tasks must be positive")
        self._execute, self._finalize = execute, finalize
        self._isolate_failures = isolate_failures
        self._cancel_pending = cancel_pending
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

    def try_submit_batch(self, tasks: tuple[BackendTask, ...], endpoint: str) -> BatchSubmission:
        """Reserve all local slots or none; publish each completion independently."""
        if not tasks or len({t.key for t in tasks}) != len(tasks):
            raise ValueError("a batch requires distinct member tasks")
        if not self._lock.acquire(blocking=False):
            return BatchSubmission(Acceptance.NOT_ACCEPTED)
        try:
            if (self._closed or len(self._slots) + len(tasks) > self._maximum
                    or any(t.key in self._slots for t in tasks)):
                return BatchSubmission(Acceptance.NOT_ACCEPTED)
            handles = tuple(f"{t.key.session_id}:{t.key.sequence}" for t in tasks)
            # Reserve every member before scheduling any coroutine. After this
            # point a loop failure is uncertain ownership, never zero acceptance.
            for task, handle in zip(tasks, handles):
                self._slots[task.key] = _Pending(handle, task, Future())
            failure = None
            for task in tasks:
                coroutine = None
                try:
                    if failure is not None:
                        raise failure
                    coroutine = self._invoke(task, endpoint)
                    future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
                except BaseException as error:
                    if coroutine is not None:
                        coroutine.close()
                    failure = error
                    future = Future()
                    future.set_exception(error)
                self._slots[task.key].future = future
                future.add_done_callback(lambda done, key=task.key: self._completed(key))
            return BatchSubmission(Acceptance.ACCEPTED, handles)
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
                if not slot.future.done() or slot.reported_unknown and not slot.producer_stopped:
                    continue
                # Report an uncertain transport failure once, retaining its slot.
                # No terminal event is consumed if this poll raises instead.
                try:
                    result = slot.future.result()
                except BaseException as error:
                    if type(error) is ConfirmedTaskFailure and error.key == key:
                        if (key, slot.handle) not in handles:
                            raise RuntimeError("asynchronous failure has no registered owner") from None
                        events.append(Terminal(key, slot.handle, "failed"))
                        if len(events) == max_events:
                            break
                        continue
                    if slot.producer_stopped:
                        if (key, slot.handle) not in handles:
                            raise RuntimeError("retired borrowing has no registered owner") from None
                        # cancelled ends borrowing, not the model's unknown
                        # outcome. The adapter retains its separate call ledger.
                        events.append(Terminal(key, slot.handle, "cancelled"))
                        if len(events) == max_events:
                            break
                        continue
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
        if self._cancel_pending is not None:
            self._loop.call_soon_threadsafe(self._cancel_pending, key)
        # There is no authoritative remote-cancel operation in this transport.
        # Continue reading the bounded response so the engine can settle it later.
        return None

    def retire_producer(self, keys: tuple[TaskKey, ...]) -> int:
        """End selected borrowing after the adapter proves its producer stopped.

        Caller must have an exact-owner remote quiescence acknowledgement.
        This method additionally requires the local I/O loop and all callbacks
        to have stopped. It emits no model result and never reopens admission.
        Known completions still win a close race; uncertainty is not rewritten.
        """
        if (type(keys) is not tuple or len(keys) > self._maximum
                or any(type(key) is not TaskKey or any(type(value) is not int or not 0 <= value < 1 << 64
                    for value in (key.session_id, key.sequence)) for key in keys)
                or len(set(keys)) != len(keys)):
            raise ValueError("retirement requires unique full task keys")
        with self._lock:
            if not self._closed or self._thread.is_alive():
                raise RuntimeError("local producer is not stopped")
            slots = [self._slots[key] for key in keys if key in self._slots]
            if any(not slot.future.done() for slot in slots):
                raise RuntimeError("local delivery callback remains active")
            count = sum(not slot.producer_stopped for slot in slots)
            for slot in slots:
                slot.producer_stopped = True
        self._notify()
        return count

    def close(self, timeout: float = 5.0, *, wait_pending=False) -> bool:
        if timeout <= 0:
            raise ValueError("close timeout must be positive")
        until = time.monotonic()+timeout if wait_pending else None
        if wait_pending:
            with self._lock:
                pending = tuple(slot.future for slot in self._slots.values() if not slot.future.done())
            if pending and wait(pending, timeout=timeout).not_done:
                return False
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
                self._closing_future.result(timeout=timeout if until is None else max(0, until-time.monotonic()))
            except TimeoutError:
                return False
            except Exception:
                error = RuntimeError("asynchronous transport finalization failed")
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout if until is None else max(0, until-time.monotonic()))
        if error:
            raise error
        return not self._thread.is_alive()
