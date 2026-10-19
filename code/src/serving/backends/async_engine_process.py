"""Bounded private async-model IPC, separate from baseline/Core scheduling.

Data, control and terminal results have separate sockets. Parent I/O keeps
reading control even when the consumer holds every reserved result slot.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import threading
import time
import uuid

from . import vllm_engine as common
from .vllm_async_engine import AsyncEngineLimits, VllmAsyncEngine, result_record

_HEADER = struct.Struct("!I")
_OPEN_FRAME_BYTES = 64 * 1024
_CODE_ROOT = Path(__file__).resolve().parents[3]


def _close_sockets(sockets, errors):
    for sock in sockets:
        try:
            sock.close()
        except Exception as error:
            errors.append("socket_close: " + type(error).__name__)


def _frame(value, maximum):
    data = bytearray()
    for piece in json.JSONEncoder(ensure_ascii=False, allow_nan=False,
                                  separators=(",", ":")).iterencode(value):
        piece = piece.encode()
        if len(data) + len(piece) > maximum:
            raise ValueError("async engine IPC item exceeds its byte capacity")
        data.extend(piece)
    return _HEADER.pack(len(data)) + data


def _compact_result(value):
    # Preserve the shared converter's identity, usage and confirmation values.
    # Only potentially large output representations are omitted on overflow.
    return dict(value, text=None, prompt_token_ids_json="null", output_token_ids_json="null",
                logprobs_json="null", status="unknown" if value["status"] == "unknown" else "failed",
                error_type="ResultCapacityExceeded", result_fields_omitted=True)


async def _read_frame(sock, maximum):
    loop = asyncio.get_running_loop()
    data = bytearray()
    while len(data) < 4:
        piece = await loop.sock_recv(sock, 4 - len(data))
        if not piece:
            raise EOFError("async engine command socket closed")
        data.extend(piece)
    size = _HEADER.unpack(data)[0]
    if not 0 < size <= maximum:
        raise ValueError("async engine IPC frame length exceeds capacity")
    data.clear()
    while len(data) < size:
        piece = await loop.sock_recv(sock, min(65536, size - len(data)))
        if not piece:
            raise EOFError("async engine IPC frame was truncated")
        data.extend(piece)
    value = json.loads(data)
    if type(value) is not dict:
        raise ValueError("async engine IPC requires an object")
    return value, size


@dataclass(frozen=True)
class SubmitResult:
    status: str
    request_id: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class TerminalEvent:
    request_id: str
    identity: dict
    result: dict | None
    engine_terminal_confirmed: bool
    error_type: str | None = None


@dataclass
class _Slot:
    identity: dict
    input_bytes: int
    accepted: bool = False
    terminal: dict | None = None
    abort_requested: bool = False
    result_bytes: int = 0


class AsyncEngineProcess:
    """One private child, bounded handoff and caller-owned result consumption.

    submit transfers local ownership only. Child acceptance is observable in
    snapshot; poll transfers terminal evidence. Unknown keeps its reservations.
    This component never retries, batches, routes or selects scheduling policy.
    """

    def __init__(self, engine, generation, *, limits=None, startup_timeout_s=120):
        self.spec = common.EngineSpec.from_dict(asdict(engine))
        self.generation = common.GenerationSpec.from_dict(asdict(generation))
        self.generation.kwargs()
        self.limits = AsyncEngineLimits() if limits is None else limits
        if type(self.limits) is not AsyncEngineLimits:
            raise TypeError("limits must be AsyncEngineLimits")
        common._number(startup_timeout_s, "startup timeout", low=0, inclusive_low=False)
        self.spec.validate_driver_environment()
        self._owner_uid = uuid.uuid4().hex
        self._sequence = 0
        self._slots = {}
        self._data_commands, self._controls, self._ready = deque(), deque(), deque()
        self._condition = threading.Condition()
        self._stop, self._io_done = threading.Event(), threading.Event()
        self._closing = self._close_sent = self._drain_requested = False
        self._broken = None
        self.ready_report = self.engine_close_report = self.last_close_report = None
        self._drain_report = None
        self._query_id = None
        self.process = None
        self._thread = None
        self._data = self._control = self._results = None
        self._sockets = []
        children, setup_errors = [], []
        try:
            try:
                for name in ("_data", "_control", "_results"):
                    parent, child = socket.socketpair()
                    self._sockets.extend((parent, child))
                    children.append(child)
                    setattr(self, name, parent)
                    parent.setblocking(False)
                opening = _frame(dict(op="open", owner_uid=self._owner_uid, engine=asdict(self.spec),
                    generation=asdict(self.generation), limits=asdict(self.limits)),
                    min(self.limits.control_frame_bytes, _OPEN_FRAME_BYTES))
                self._controls.append(opening)
                self.process = subprocess.Popen(self._command(self.spec, [s.fileno() for s in children]),
                    pass_fds=tuple(s.fileno() for s in children), start_new_session=True,
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    env=dict(os.environ, PYTHONPATH=str(_CODE_ROOT), HF_HUB_OFFLINE="1",
                             TRANSFORMERS_OFFLINE="1"), bufsize=0)
                self._thread = threading.Thread(target=self._io, name="semloom-async-engine-ipc", daemon=True)
                self._thread.start()
            finally:
                _close_sockets(children, setup_errors)
            if setup_errors:
                raise RuntimeError("async engine child socket cleanup failed")
            deadline = time.monotonic() + startup_timeout_s
            with self._condition:
                while self.ready_report is None and self._broken is None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("async engine startup deadline expired")
                    self._condition.wait(min(0.05, remaining))
                if self._broken is not None:
                    raise RuntimeError("async engine startup failed: " + self._broken)
        except BaseException as startup_error:
            cleanup_deadline = time.monotonic() + 1
            try:
                self.close(timeout=1)
            except BaseException as cleanup_error:
                errors = ["startup_cleanup: " + type(cleanup_error).__name__]
                self._finish_io(cleanup_deadline, errors)
                self.last_close_report = dict(status="unknown", process_exit_confirmed=False,
                    gpu_exit_confirmed=None, descendants_exit_confirmed=None, io_thread_closed=False,
                    cleanup_scope="async_engine_api_and_direct_child", cleanup_errors=errors)
            self.last_close_report["cleanup_errors"].extend(setup_errors)
            # The caller can inspect the scoped receipt and retry this same owned
            # child if cleanup was unconfirmed, without losing the startup cause.
            startup_error.async_close_report = self.last_close_report
            startup_error.async_engine_process = self
            raise

    @staticmethod
    def _command(spec, descriptors):
        return [spec.model_python, "-m", __name__, "--serve", *map(str, descriptors)]

    def _usage(self):
        return dict(held_requests=len(self._slots),
            active_requests=sum(s.terminal is None or not s.terminal["confirmed"] for s in self._slots.values()),
            accepted_requests=sum(s.accepted for s in self._slots.values()),
            input_bytes=sum(s.input_bytes for s in self._slots.values()),
            reserved_result_bytes=len(self._slots) * self.limits.item_result_bytes,
            buffered_result_bytes=sum(s.result_bytes for s in self._slots.values()),
            ready_results=len(self._ready), queued_commands=len(self._data_commands),
            queued_controls=len(self._controls),
            unknown_requests=sum(s.terminal is not None and not s.terminal["confirmed"]
                                 for s in self._slots.values()))

    def snapshot(self):
        with self._condition:
            return dict(self._usage(), broken=self._broken, closing=self._closing,
                        child_alive=self.process is not None and self.process.poll() is None,
                        child_drain_report=self._drain_report)

    def submit(self, row, *, generation=None, deadline_monotonic):
        common._number(deadline_monotonic, "request deadline", low=0, inclusive_low=False)
        effective = self.generation if generation is None else generation
        if not isinstance(effective, common.GenerationSpec):
            raise TypeError("generation must be GenerationSpec")
        effective = common.GenerationSpec.from_dict(asdict(effective))
        effective.kwargs()
        if effective.max_tokens > self.spec.max_model_len:
            raise ValueError("max_tokens exceeds max_model_len")
        identities, _ = common._prepared([row])
        identity = identities[0]
        snapshot = {name: row[name] for name in (*common.IDENTITY_FIELDS, "messages_json")}
        # Every accepted item must be able to return known usage even if its
        # full output overflows. The shared schema supplies the small receipt.
        example = result_record(identity, engine_uid="x" * 32, start=1e20, end=1e20,
                                status="unknown", submitted=False, error_type="ResultCapacityExceeded")
        example.update(prompt_tokens=(1 << 64) - 1, completion_tokens=(1 << 64) - 1,
            total_tokens=(1 << 65) - 2, engine_request_id="x" * 256,
            engine_terminal_confirmed=False, finish_reason="length",
            model_start_monotonic=1.7976931348623157e308,
            model_end_monotonic=1.7976931348623157e308,
            model_elapsed_s=1.7976931348623157e308)
        _frame(dict(op="terminal", request_id="x" * 64, confirmed=False,
                    result=_compact_result(example)), self.limits.item_result_bytes)
        with self._condition:
            if self._io_done.is_set() or self.process is None or self.process.poll() is not None:
                # The reader still drains any already-written terminal prefix.
                # Reject new work without overwriting those pending receipts.
                self._broken = self._broken or "ChildUnavailable"
            if self._broken is not None or self._closing:
                return SubmitResult("rejected", reason="owner unavailable")
            if time.monotonic() >= deadline_monotonic:
                return SubmitResult("rejected", reason="deadline expired before acceptance")
            if self._slots and self._query_id != identity["query_id"]:
                return SubmitResult("rejected", reason="previous query still owns inputs or results")
            if self._sequence >= (1 << 63):
                return SubmitResult("rejected", reason="request identity sequence exhausted")
            used = self._usage()
            if (used["active_requests"] >= self.limits.max_inflight
                    or len(self._slots) >= self.limits.result_slots
                    or used["reserved_result_bytes"] + self.limits.item_result_bytes > self.limits.result_bytes
                    or len(self._data_commands) >= self.limits.command_slots):
                return SubmitResult("backpressure", reason="request, result or IPC capacity exhausted")
            request_id = self._owner_uid + ":" + str(self._sequence)
            data = _frame(dict(op="submit", request_id=request_id, row=snapshot,
                generation=asdict(effective), deadline=deadline_monotonic), self.limits.item_input_bytes)
            size = len(data) - 4
            if used["input_bytes"] + size > self.limits.input_bytes:
                return SubmitResult("backpressure", reason="input byte capacity exhausted")
            self._slots[request_id] = _Slot(identity, size)
            self._data_commands.append(data)
            self._sequence += 1
            self._query_id = identity["query_id"]
            self._condition.notify_all()
            return SubmitResult("queued", request_id)

    def abort(self, request_id):
        with self._condition:
            if self._closing:
                return False
            slot = self._slots.get(request_id)
            if slot is None or slot.terminal is not None:
                return False
            if slot.abort_requested:
                return True
            if len(self._controls) >= self.limits.control_slots:
                return False
            self._controls.append(_frame(dict(op="abort", request_id=request_id),
                                        self.limits.control_frame_bytes))
            slot.abort_requested = True
            self._condition.notify_all()
            return True

    def poll(self, max_events=1):
        if type(max_events) is not int or not 0 < max_events <= self.limits.result_slots:
            raise ValueError("poll bound must be positive and fit result_slots")
        events = []
        with self._condition:
            for _ in range(min(max_events, len(self._ready))):
                key = self._ready.popleft()
                slot = self._slots[key]
                event = slot.terminal
                events.append(TerminalEvent(key, dict(slot.identity), event.get("result"),
                                           event["confirmed"], event.get("error_type")))
                if event["confirmed"]:
                    del self._slots[key]
                else:
                    slot.terminal = dict(event, result=None)
                    slot.result_bytes = 0
            self._condition.notify_all()
        return tuple(events)

    def drain(self, timeout=10):
        common._number(timeout, "drain timeout", low=0, inclusive_low=False)
        deadline = time.monotonic() + timeout
        with self._condition:
            self._drain_requested = True
            while self._usage()["active_requests"] and self._broken is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(min(0.05, remaining))
            usage = self._usage()
            return dict(status="drained" if not usage["active_requests"] else "unknown", **usage)

    def wait(self, timeout=0.05):
        common._number(timeout, "wait timeout", low=0, inclusive_low=False)
        with self._condition:
            if not self._ready and self._broken is None:
                self._condition.wait(timeout)

    def _uncertain(self, reason):
        self._broken = self._broken or reason
        for key, slot in self._slots.items():
            if slot.terminal is None:
                slot.terminal = dict(confirmed=False, result=None, error_type=reason)
                self._ready.append(key)
        self._condition.notify_all()

    def _receive(self, value, *, results, encoded_bytes=0):
        with self._condition:
            op = value.get("op")
            if results:
                key = value.get("request_id")
                slot = self._slots.get(key)
                if op != "terminal" or slot is None or slot.terminal is not None:
                    raise ValueError("async engine terminal identity conflict")
                result, confirmed = value.get("result"), value.get("confirmed")
                if type(result) is not dict or type(confirmed) is not bool:
                    raise ValueError("async engine terminal shape invalid")
                if any(result.get(k) != v for k, v in slot.identity.items()):
                    raise ValueError("async engine result changed call identity")
                if result.get("status") not in ("succeeded", "failed", "unknown"):
                    raise ValueError("async engine result status invalid")
                if confirmed == (result["status"] == "unknown"):
                    raise ValueError("unknown model work cannot release capacity")
                if result.get("engine_terminal_confirmed", confirmed) is not confirmed:
                    raise ValueError("model and transport confirmation disagree")
                slot.terminal = value
                slot.result_bytes = encoded_bytes
                if confirmed:
                    slot.input_bytes = 0
                else:
                    self._broken = self._broken or "ModelOutcomeUnknown"
                self._ready.append(key)
            elif op == "ready":
                if self.ready_report is not None:
                    raise ValueError("duplicate model ready")
                self.ready_report = value["report"]
            elif op == "accepted":
                key = value.get("request_id", "")
                prefix, _, sequence = key.rpartition(":")
                if prefix != self._owner_uid or not sequence.isdecimal() or int(sequence) >= self._sequence:
                    raise ValueError("accepted response has no issued request identity")
                slot = self._slots.get(key)
                if slot is not None:
                    slot.accepted = True
            elif op == "drained":
                self._drain_report = value
            elif op == "closed":
                self.engine_close_report = value["report"]
            elif op == "error":
                self._uncertain(value.get("error_type", "ChildError"))
            elif op != "abort_requested":
                raise ValueError("unexpected async engine control response")
            self._condition.notify_all()

    def _io(self):
        incoming = {self._control: bytearray(), self._results: bytearray()}
        outgoing = {self._control: None, self._data: None}
        try:
            while not self._stop.is_set():
                with self._condition:
                    if outgoing[self._control] is None:
                        # An accepted cancel must cross the control channel
                        # before close makes the child stop reading it.
                        if self._controls:
                            outgoing[self._control] = memoryview(self._controls.popleft())
                        elif self._closing and not self._close_sent:
                            self._close_sent = True
                            outgoing[self._control] = memoryview(_frame(dict(op="close"),
                                self.limits.control_frame_bytes))
                        elif self._drain_requested:
                            self._drain_requested = False
                            outgoing[self._control] = memoryview(_frame(dict(op="drain"),
                                self.limits.control_frame_bytes))
                    if outgoing[self._data] is None and self._data_commands:
                        outgoing[self._data] = memoryview(self._data_commands.popleft())
                reads, writes, _ = select.select(list(incoming),
                    [sock for sock, data in outgoing.items() if data is not None], [], 0.01)
                # Control always gets the first bounded read/write opportunity.
                for sock in (self._control, self._data):
                    if sock in writes:
                        try:
                            data = outgoing[sock]
                            sent = sock.send(data[:65536])
                            outgoing[sock] = data[sent:] if sent < len(data) else None
                        except BlockingIOError:
                            pass
                for sock in (self._control, self._results):
                    if sock not in reads:
                        continue
                    maximum = self.limits.control_frame_bytes if sock is self._control else self.limits.item_result_bytes
                    buffer = incoming[sock]
                    data = sock.recv(min(65536, maximum + 4 - len(buffer)))
                    if not data:
                        if buffer:
                            raise ValueError("truncated async engine response")
                        del incoming[sock]
                        continue
                    buffer.extend(data)
                    while len(buffer) >= 4:
                        size = _HEADER.unpack(buffer[:4])[0]
                        if not 0 < size <= maximum:
                            raise ValueError("async engine response exceeded frame capacity")
                        if len(buffer) < size + 4:
                            break
                        value = json.loads(buffer[4:4 + size])
                        del buffer[:4 + size]
                        self._receive(value, results=sock is self._results, encoded_bytes=size)
                if not incoming:
                    break
        except Exception as error:
            if not self._stop.is_set():
                with self._condition:
                    self._uncertain(type(error).__name__)
        finally:
            with self._condition:
                if not self._closing:
                    self._broken = self._broken or "ChildTransportClosed"
                if any(slot.terminal is None for slot in self._slots.values()):
                    self._uncertain("ChildOutcomeUnknown")
                if self.ready_report is None:
                    self._uncertain("ChildStartupUnknown")
                self._condition.notify_all()
            self._io_done.set()

    def close(self, timeout=15):
        common._number(timeout, "close timeout", low=0, inclusive_low=False)
        deadline = time.monotonic() + timeout
        errors = []
        try:
            with self._condition:
                self._closing = True
                while self.engine_close_report is None and self.process is not None and self.process.poll() is None:
                    remaining = deadline - time.monotonic()
                    if remaining <= min(1, timeout / 3):
                        break
                    self._condition.wait(min(0.05, remaining))
            if self.process is not None:
                try:
                    self.process.wait(timeout=max(0, deadline - time.monotonic()) / 2)
                except subprocess.TimeoutExpired:
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=max(0, deadline - time.monotonic()) / 2)
                    except subprocess.TimeoutExpired:
                        self.process.kill()
                        self.process.wait(timeout=max(0, deadline - time.monotonic()))
        except Exception as error:
            errors.append("direct_child_cleanup: " + type(error).__name__)
        finally:
            try:
                if self._thread is not None:
                    self._io_done.wait(max(0, deadline - time.monotonic()))
            except Exception as error:
                errors.append("io_drain: " + type(error).__name__)
            self._finish_io(deadline, errors)
        with self._condition:
            self._uncertain("ClosedWithoutConfirmedTerminal")
            usage = self._usage()
            exited = self.process is None
            try:
                if self.process is not None:
                    exited = self.process.poll() is not None
            except Exception as error:
                errors.append("direct_child_poll: " + type(error).__name__)
            api = (self.engine_close_report or {}).get("engine_api_shutdown_confirmed") is True
            thread_closed = self._thread is None
            try:
                if self._thread is not None:
                    thread_closed = not self._thread.is_alive()
            except Exception as error:
                errors.append("io_thread_status: " + type(error).__name__)
            self.last_close_report = dict(status="closed" if exited and api and thread_closed
                and not usage["active_requests"] and not errors else "unknown", cleanup_scope="async_engine_api_and_direct_child",
                process_exit_confirmed=exited, gpu_exit_confirmed=None,
                descendants_exit_confirmed=None, engine_api=self.engine_close_report,
                io_thread_closed=thread_closed, cleanup_errors=errors, **usage)
            return self.last_close_report

    def _finish_io(self, deadline, errors):
        self._stop.set()
        _close_sockets(self._sockets, errors)
        if self._thread is not None:
            try:
                self._thread.join(max(0, deadline - time.monotonic()))
            except Exception as error:
                errors.append("io_thread_join: " + type(error).__name__)


async def serve(data_fd, control_fd, result_fd, *, engine_factory=VllmAsyncEngine):
    """Private child entry; a fixture factory is allowed only through Python tests."""
    sockets = [socket.socket(fileno=fd) for fd in (data_fd, control_fd, result_fd)]
    data, control, results = sockets
    for sock in sockets:
        sock.setblocking(False)
        sock.set_inheritable(False)
    loop = asyncio.get_running_loop()
    engine = None
    tasks = []
    owned = {}
    waiter = None
    failure = None
    try:
        opening, _ = await _read_frame(control, _OPEN_FRAME_BYTES)
        if opening.get("op") != "open":
            raise ValueError("async engine requires open before commands")
        limits = AsyncEngineLimits(**opening["limits"])
        spec = common.EngineSpec.from_dict(opening["engine"])
        generation = common.GenerationSpec.from_dict(opening["generation"])
        engine = engine_factory(spec, generation, limits=limits)
        await loop.sock_sendall(control, _frame(dict(op="ready", report=engine.ready()), limits.control_frame_bytes))
        control_queue = asyncio.Queue(maxsize=limits.control_slots)
        result_queue = asyncio.Queue(maxsize=limits.result_slots)
        abort_queue = asyncio.Queue(maxsize=limits.max_inflight)
        early_aborts = set()
        closing = asyncio.Event()
        failure = loop.create_future()
        next_sequence = 0

        async def writer(sock, queue):
            while True:
                packet, key = await queue.get()
                try:
                    await loop.sock_sendall(sock, packet)
                    if key is not None:
                        owned.pop(key, None)
                finally:
                    queue.task_done()

        async def reply(value):
            await control_queue.put((_frame(value, limits.control_frame_bytes), None))

        async def abort_requests():
            # Native ABORT may wait on the engine's own IPC. Its single bounded
            # worker must not stop the reader from seeing drain or close.
            while True:
                key = await abort_queue.get()
                try:
                    await engine.abort(key)
                finally:
                    abort_queue.task_done()

        async def run(key, slot, value):
            row = value["row"]
            try:
                if slot["aborted"] or closing.is_set():
                    identities, _ = common._prepared([row])
                    now = time.monotonic()
                    result = result_record(identities[0], engine_uid=engine.engine_uid,
                        start=now, end=now, status="failed", submitted=False,
                        error_type="CancelledBeforeSubmission")
                    result["engine_terminal_confirmed"] = True
                else:
                    result = await engine.generate_prepared(row, request_id=key,
                        generation=common.GenerationSpec.from_dict(value["generation"]),
                        deadline_monotonic=value["deadline"])
            except Exception as error:
                identities, _ = common._prepared([row])
                now = time.monotonic()
                result = result_record(identities[0], engine_uid=engine.engine_uid,
                    start=now, end=now, status="unknown", submitted=False,
                    error_type=type(error).__name__)
            confirmed = result["status"] != "unknown" and result.get("engine_terminal_confirmed", True) is True
            event = dict(op="terminal", request_id=key, confirmed=confirmed, result=result)
            try:
                packet = _frame(event, limits.item_result_bytes)
            except ValueError:
                event["result"] = _compact_result(result)
                packet = _frame(event, limits.item_result_bytes)
            await result_queue.put((packet, key))

        async def input_commands():
            nonlocal next_sequence
            while not closing.is_set():
                value, size = await _read_frame(data, limits.item_input_bytes)
                key = value.get("request_id")
                if value.get("op") != "submit" or key != opening["owner_uid"] + ":" + str(next_sequence):
                    raise ValueError("async engine submit sequence invalid")
                next_sequence += 1
                if (len(owned) >= min(limits.max_inflight, limits.result_slots)
                        or sum(s["bytes"] for s in owned.values()) + size > limits.input_bytes
                        or (len(owned) + 1) * limits.item_result_bytes > limits.result_bytes):
                    raise ValueError("async engine child reservation exceeded")
                common._prepared([value["row"]])
                slot = dict(bytes=size, aborted=key in early_aborts, task=None)
                early_aborts.discard(key)
                owned[key] = slot
                await reply(dict(op="accepted", request_id=key))
                slot["task"] = asyncio.create_task(run(key, slot, value))
                slot["task"].add_done_callback(observe_request)

        def observe_request(task):
            if not task.cancelled():
                error = task.exception()
                if error is not None and not failure.done():
                    failure.set_result(error)

        async def control_commands():
            while not closing.is_set():
                value, _ = await _read_frame(control, limits.control_frame_bytes)
                op = value.get("op")
                if op == "abort":
                    key = value["request_id"]
                    requested = True
                    if key in owned:
                        slot = owned[key]
                        if not slot["aborted"]:
                            slot["aborted"] = True
                            try:
                                abort_queue.put_nowait(key)
                            except asyncio.QueueFull:
                                requested = False
                    else:
                        prefix, _, sequence = key.rpartition(":")
                        if (prefix != opening["owner_uid"] or not sequence.isdecimal()
                                or int(sequence) >= next_sequence + limits.max_inflight):
                            raise ValueError("abort identity outside the bounded input window")
                        if int(sequence) >= next_sequence:
                            early_aborts.add(key)
                    await reply(dict(op="abort_requested", request_id=key, requested=requested,
                                     engine_terminal_confirmed=False))
                elif op == "drain":
                    await reply(dict(op="drained", pending=len(owned)))
                elif op == "close":
                    closing.set()
                else:
                    raise ValueError("unexpected async engine control command")

        tasks = [asyncio.create_task(writer(control, control_queue)),
                 asyncio.create_task(writer(results, result_queue)),
                 asyncio.create_task(input_commands()), asyncio.create_task(control_commands()),
                 asyncio.create_task(abort_requests())]
        waiter = asyncio.create_task(closing.wait())
        done, _ = await asyncio.wait([*tasks, waiter, failure], return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task is failure:
                raise task.result()
            if task is not waiter:
                task.result()
        closing.set()
        # Native shutdown and result writes are independent of command reading.
        report = await engine.close(timeout=10)
        pending = [slot["task"] for slot in owned.values() if slot["task"] is not None]
        if pending:
            done, waiting = await asyncio.wait(pending, timeout=1)
            for task in waiting:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        await asyncio.wait_for(result_queue.join(), 1)
        await reply(dict(op="closed", report=report))
        await asyncio.wait_for(control_queue.join(), 1)
        waiter.cancel()
    except Exception as error:
        try:
            await asyncio.wait_for(loop.sock_sendall(control,
                _frame(dict(op="error", error_type=type(error).__name__), 4096)), 0.2)
        except Exception:
            pass
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if engine is not None:
            await engine.close(timeout=1)
        pending = [slot["task"] for slot in owned.values() if slot["task"] is not None]
        if waiter is not None:
            pending.append(waiter)
        if failure is not None:
            failure.cancel()
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for sock in sockets:
            sock.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 5 or sys.argv[1] != "--serve":
        raise SystemExit("async model worker requires three inherited socket descriptors")
    asyncio.run(serve(*map(int, sys.argv[2:])))
