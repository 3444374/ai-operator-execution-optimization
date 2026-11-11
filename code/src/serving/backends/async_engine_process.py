"""Parent ownership for private async-model IPC, separate from scheduling.

Data, control and terminal results have separate sockets. Parent I/O keeps
reading control even when the consumer holds every reserved result slot.
"""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import threading
import time
import uuid

from src.infrastructure.deadlines import deadline_expired
from . import vllm_protocol as common
from .vllm_protocol import AsyncEngineLimits, result_record
from .async_engine_wire import _HEADER, _OPEN_FRAME_BYTES, _frame, _compact_result

_CODE_ROOT = Path(__file__).resolve().parents[3]


def _close_sockets(sockets, errors):
    for sock in sockets:
        try:
            sock.close()
        except Exception as error:
            errors.append("socket_close: " + type(error).__name__)


@dataclass(frozen=True)
class SubmitResult:
    status: str
    request_id: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class BatchSubmitResult:
    status: str
    request_ids: tuple[str, ...] = ()
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
    producer_stopped: bool = False


class AsyncEngineProcess:
    """One private child, bounded handoff and caller-owned result consumption.

    submit transfers local ownership only. Child acceptance is observable in
    snapshot; poll transfers terminal evidence. Unknown keeps its reservations.
    This component never retries, chooses batch membership or selects routing/scheduling policy.
    """

    def __init__(self, engine, generation, *, limits, startup_timeout_s=120,
                 worker_env=None, worker_log=None, result_signal=None):
        self.spec = common.EngineSpec.from_dict(asdict(engine))
        self.generation = common.GenerationSpec.from_dict(asdict(generation))
        self.limits = limits
        if type(self.limits) is not AsyncEngineLimits:
            raise TypeError("limits must be AsyncEngineLimits")
        common._number(startup_timeout_s, "startup timeout", low=0, inclusive_low=False)
        self.spec.validate_driver_environment()
        self._owner_uid = uuid.uuid4().hex
        self._sequence = 0
        self._submitted_batches = self._peak_batch_members = self._peak_batch_frame_bytes = 0
        self._slots = {}
        self._data_commands, self._controls, self._ready = deque(), deque(), deque()
        self._condition = threading.Condition()
        self._result_signal = result_signal
        self._stop, self._io_done = threading.Event(), threading.Event()
        self._closing = self._close_sent = self._drain_requested = False
        self._close_deadline = None
        self._broken = None
        self.ready_report = self.engine_close_report = self.last_close_report = None
        self._drain_report = None
        self._native_feedback = None
        self._native_mfu = None
        self._producer_report = self._producer_quiescence = None
        self._retired_unknown = set()
        self._query_id = None
        self.process = None
        self.process_created = None
        self._owned_process = None
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
                environment = dict(os.environ, PYTHONPATH=str(_CODE_ROOT), HF_HUB_OFFLINE="1",
                                   TRANSFORMERS_OFFLINE="1")
                environment.update(worker_env or {})
                log = None if worker_log is None else open(worker_log, 'xb')
                try:
                    self.process = subprocess.Popen(self._command(self.spec, [s.fileno() for s in children]),
                        pass_fds=tuple(s.fileno() for s in children), start_new_session=True,
                        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL if log is None else log,
                        stderr=subprocess.DEVNULL if log is None else log, env=environment, bufsize=0)
                    import psutil
                    self.process_created = psutil.Process(self.process.pid).create_time()
                finally:
                    if log is not None:
                        log.close()
                self._owned_process = self.process
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
        return [spec.model_python, "-m", "src.serving.backends.async_engine_worker", "--serve", *map(str, descriptors)]

    def _usage(self):
        return dict(held_requests=len(self._slots),
            active_requests=sum(not s.producer_stopped and (s.terminal is None or not s.terminal["confirmed"])
                                for s in self._slots.values()),
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
                        owner_uid=self._owner_uid,
                        issued_submissions=self._sequence,
                        child_alive=self.process is not None and self.process.poll() is None,
                        child_drain_report=self._drain_report,
                        **({"native_mfu": self._native_mfu} if self.spec.enable_mfu_metrics else {}),
                        native_feedback=None if self._native_feedback is None else dict(self._native_feedback),
                        **(dict(submitted_batch_frames=self._submitted_batches,
                                peak_batch_members=self._peak_batch_members,
                                peak_batch_frame_bytes=self._peak_batch_frame_bytes)
                           if self._submitted_batches else {}))

    def _check_receipt_capacity(self, identity):
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

    def submit(self, row, *, generation=None, deadline_monotonic, kv_transfer_params=None):
        if deadline_monotonic is not None:
            common._number(deadline_monotonic, "request deadline", low=0, inclusive_low=False)
        effective = self.generation if generation is None else generation
        if not isinstance(effective, common.GenerationSpec):
            raise TypeError("generation must be GenerationSpec")
        effective = common.GenerationSpec.from_dict(asdict(effective))
        from .model_context import model_context_limit
        if effective.max_tokens > model_context_limit(self.spec, self.ready_report):
            raise ValueError("max_tokens exceeds max_model_len")
        identities, _ = common._prepared([row])
        identity = identities[0]
        if kv_transfer_params is not None and (
                self.spec.kv_transfer_config is None or not isinstance(kv_transfer_params, dict)):
            raise ValueError('KV transfer requires a connector and an object descriptor')
        snapshot = {name: row[name] for name in (*common.IDENTITY_FIELDS, "messages_json")}
        self._check_receipt_capacity(identity)
        with self._condition:
            if self._io_done.is_set() or self.process is None or self.process.poll() is not None:
                # The reader still drains any already-written terminal prefix.
                # Reject new work without overwriting those pending receipts.
                self._broken = self._broken or "ChildUnavailable"
            if self._broken is not None or self._closing:
                return SubmitResult("rejected", reason="owner unavailable")
            if deadline_expired(deadline_monotonic):
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
                generation=asdict(effective), deadline=deadline_monotonic,
                **({'kv_transfer_params': kv_transfer_params} if kv_transfer_params is not None else {})),
                self.limits.item_input_bytes)
            size = len(data) - 4
            if used["input_bytes"] + size > self.limits.input_bytes:
                return SubmitResult("backpressure", reason="input byte capacity exhausted")
            self._slots[request_id] = _Slot(identity, size)
            self._data_commands.append(data)
            self._sequence += 1
            self._query_id = identity["query_id"]
            self._condition.notify_all()
            return SubmitResult("queued", request_id)

    def submit_batch(self, rows, *, generation, deadline_monotonic, kv_transfer_params=None):
        """One bounded data frame, atomic local acceptance, independent terminals."""
        if deadline_monotonic is not None:
            common._number(deadline_monotonic, "request deadline", low=0, inclusive_low=False)
        if not rows or len(rows) > self.limits.max_inflight:
            return BatchSubmitResult("backpressure", reason="member request capacity exhausted")
        effective = common.GenerationSpec.from_dict(asdict(generation))
        from .model_context import model_context_limit
        if effective.max_tokens > model_context_limit(self.spec, self.ready_report):
            raise ValueError("max_tokens exceeds max_model_len")
        identities, _ = common._prepared(list(rows))
        if kv_transfer_params is not None and (
                self.spec.kv_transfer_config is None or not isinstance(kv_transfer_params, list)
                or len(kv_transfer_params) != len(rows)
                or any(not isinstance(item, dict) for item in kv_transfer_params)):
            raise ValueError('KV transfer requires a connector and one object descriptor per row')
        for identity in identities:
            self._check_receipt_capacity(identity)
        if len({r['query_id'] for r in identities}) != 1:
            raise ValueError("batch members must belong to one query")
        snapshots = [{name: row[name] for name in (*common.IDENTITY_FIELDS, "messages_json")} for row in rows]
        with self._condition:
            if self._io_done.is_set() or self.process is None or self.process.poll() is not None:
                self._broken = self._broken or "ChildUnavailable"
            if self._broken is not None or self._closing or deadline_expired(deadline_monotonic):
                return BatchSubmitResult("rejected", reason="owner unavailable or deadline expired")
            if self._slots and self._query_id != identities[0]['query_id']:
                return BatchSubmitResult("rejected", reason="previous query still owns work")
            count, used = len(rows), self._usage()
            if self._sequence + count > 1 << 63:
                return BatchSubmitResult("rejected", reason="request identity sequence exhausted")
            if (used['active_requests'] + count > self.limits.max_inflight
                    or len(self._slots) + count > self.limits.result_slots
                    or used['reserved_result_bytes'] + count*self.limits.item_result_bytes > self.limits.result_bytes
                    or len(self._data_commands) >= self.limits.command_slots):
                return BatchSubmitResult("backpressure", reason="request, result or command capacity exhausted")
            keys = tuple(self._owner_uid + ':' + str(self._sequence+i) for i in range(count))
            items = [dict(request_id=key, row=row) for key, row in zip(keys, snapshots)]
            if kv_transfer_params is not None:
                for item, params in zip(items, kv_transfer_params, strict=True):
                    item['kv_transfer_params'] = params
            generation_values = asdict(effective)
            for item in items:
                _frame(dict(op='submit', **item, generation=generation_values, deadline=deadline_monotonic),
                       self.limits.item_input_bytes)
            data = _frame(dict(op='submit_batch', requests=items, generation=generation_values,
                               deadline=deadline_monotonic), min(self.limits.input_bytes, (1 << 32)-1))
            size = len(data)-4
            if used['input_bytes'] + size > self.limits.input_bytes:
                return BatchSubmitResult("backpressure", reason="input byte capacity exhausted")
            charges = [len(_frame(item, self.limits.input_bytes))-4 for item in items]
            charges[0] += size-sum(charges)
            for key, identity, charge in zip(keys, identities, charges):
                self._slots[key] = _Slot(identity, charge)
            self._data_commands.append(data)
            self._sequence += count
            self._query_id = identities[0]['query_id']
            self._submitted_batches += 1
            self._peak_batch_members = max(self._peak_batch_members, count)
            self._peak_batch_frame_bytes = max(self._peak_batch_frame_bytes, size)
            self._condition.notify_all()
            return BatchSubmitResult("queued", keys)

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
                if event["confirmed"] or slot.producer_stopped:
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
            if not self._ready and self._broken is None and not self._io_done.is_set():
                self._condition.wait(timeout)

    def _uncertain(self, reason):
        self._broken = self._broken or reason
        for key, slot in self._slots.items():
            if slot.terminal is None:
                slot.terminal = dict(confirmed=False, result=None, error_type=reason)
                self._ready.append(key)
        self._condition.notify_all()
        if self._result_signal is not None:
            self._result_signal.set()

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
                if self.spec.enable_mfu_metrics and value.get("native_mfu") is not None:
                    observed = value["native_mfu"]
                    if observed.get("engine_uid") == self.ready_report.get("engine_uid"):
                        if self._native_mfu is None or observed.get("sequence", -1) >= self._native_mfu.get("sequence", -1):
                            self._native_mfu = observed
                slot.result_bytes = encoded_bytes
                if confirmed:
                    slot.input_bytes = 0
                else:
                    self._broken = self._broken or "ModelOutcomeUnknown"
                self._ready.append(key)
                if self._result_signal is not None:
                    self._result_signal.set()
            elif op == "ready":
                if self.ready_report is not None:
                    raise ValueError("duplicate model ready")
                from .model_context import checked_model_context
                if self.spec.max_model_len is None or 'model_context' in value['report']:
                    context = checked_model_context(self.spec, value['report'])
                    if self.generation.max_tokens > context['max_model_len']:
                        raise ValueError('generation exceeds effective model context')
                self.ready_report = value["report"]
                if self.spec.enable_mfu_metrics:
                    self._native_mfu = self.ready_report.get("native_mfu_initial")
            elif op == "accepted":
                key = value.get("request_id", "")
                prefix, _, sequence = key.rpartition(":")
                if prefix != self._owner_uid or not sequence.isdecimal() or int(sequence) >= self._sequence:
                    raise ValueError("accepted response has no issued request identity")
                slot = self._slots.get(key)
                if slot is not None:
                    slot.accepted = True
            elif op == "feedback":
                snapshot = value.get("snapshot")
                if (type(snapshot) is dict and self.ready_report is not None
                        and snapshot.get("engine_uid") == self.ready_report.get("engine_uid")):
                    self._native_feedback = snapshot
            elif op == "drained":
                self._drain_report = value
            elif op == "closed":
                proof = value.get("producer_stopped")
                if proof is not None:
                    if (type(proof) is not dict or not self._closing
                            or proof.get("owner_uid") != self._owner_uid
                            or type(proof.get("engine_uid")) is not str or not proof["engine_uid"]
                            or proof.get("engine_uid") != (self.ready_report or {}).get("engine_uid")
                            or type(proof.get("received_submissions")) is not int
                            or not 0 <= proof["received_submissions"] <= self._sequence):
                        raise ValueError("async producer close identity conflict")
                    if any(slot.accepted and int(key.rpartition(":")[2]) >= proof["received_submissions"]
                           for key, slot in self._slots.items()):
                        raise ValueError("async producer close omitted accepted input")
                    self._producer_report = proof
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
                            outgoing[self._control] = memoryview(_frame(dict(op="close", deadline=self._close_deadline),
                                self.limits.control_frame_bytes))
                        elif self._drain_requested and not self._closing:
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
        api_deadline = deadline - min(1, timeout / 3)
        errors = []
        try:
            with self._condition:
                if not self._close_sent:
                    self._close_deadline = api_deadline
                self._closing = True
                while self.engine_close_report is None and self.process is not None and self.process.poll() is None:
                    remaining = api_deadline - time.monotonic()
                    if remaining <= 0:
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
            if any(slot.terminal is None for slot in self._slots.values()):
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
            # This is a resource-lifetime acknowledgement, never a model
            # terminal. wait/poll use the original Popen child, not PID-only
            # liveness; the nonce/engine and issued prefix bind this close.
            peer = self._producer_report or {}
            if (self._producer_quiescence is None and not errors and api and exited and thread_closed
                    and self._io_done.is_set()
                    and self.process is not None and self.process is self._owned_process
                    and self.process.returncode == 0
                    and (self.engine_close_report or {}).get("status") == "closed"
                    and all(peer.get(name) is True for name in (
                        "input_reader_stopped", "request_tasks_stopped", "abort_worker_stopped",
                        "result_writer_stopped"))):
                self._producer_quiescence = dict(schema="semloom.async_producer_quiescence.v1",
                    confirmed=True, owner_uid=self._owner_uid, engine_uid=peer["engine_uid"],
                    issued_submissions=self._sequence, received_submissions=peer["received_submissions"],
                    process_pid=self.process.pid, process_returncode=self.process.returncode,
                    scope="engine API, owned child, command/result producers and parent I/O stopped",
                    outcome_confirmation=False, gpu_exit_confirmed=None)
            if self._producer_quiescence is not None:
                self._data_commands.clear()
                self._controls.clear()
                ready = set(self._ready)
                for key, slot in tuple(self._slots.items()):
                    slot.producer_stopped = True
                    slot.input_bytes = 0
                    if slot.terminal is not None and not slot.terminal["confirmed"]:
                        self._retired_unknown.add(key)
                        # Unpolled evidence remains bounded and deliverable.
                        # A previously transferred unknown owns no input now.
                        if key not in ready:
                            del self._slots[key]
            usage = self._usage()
            self.last_close_report = dict(status="closed" if exited and api and thread_closed
                and not usage["active_requests"] and not errors else "unknown", cleanup_scope="async_engine_api_and_direct_child",
                process_exit_confirmed=exited, gpu_exit_confirmed=None,
                descendants_exit_confirmed=None, engine_api=self.engine_close_report,
                io_thread_closed=thread_closed, cleanup_errors=errors,
                producer_quiescence=self._producer_quiescence,
                retired_unknown_requests=len(self._retired_unknown), transport_error=self._broken, **usage)
            if self._submitted_batches:
                self.last_close_report.update(submitted_batch_frames=self._submitted_batches,
                    peak_batch_members=self._peak_batch_members, peak_batch_frame_bytes=self._peak_batch_frame_bytes)
            return self.last_close_report

    def _finish_io(self, deadline, errors):
        self._stop.set()
        _close_sockets(self._sockets, errors)
        if self._thread is not None:
            try:
                self._thread.join(max(0, deadline - time.monotonic()))
            except Exception as error:
                errors.append("io_thread_join: " + type(error).__name__)
