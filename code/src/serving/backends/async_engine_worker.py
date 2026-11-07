"""Private async-model worker event loop and inherited-descriptor entry."""
from __future__ import annotations

import asyncio
import json
import socket
import time

from . import vllm_protocol as common
from .async_engine_wire import _HEADER, _OPEN_FRAME_BYTES, _frame, _compact_result
from .vllm_async_engine import VllmAsyncEngine
from .vllm_protocol import AsyncEngineLimits, result_record


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



async def serve(data_fd, control_fd, result_fd, *, engine_factory=VllmAsyncEngine):
    """Private child entry; a fixture factory is allowed only through Python tests."""
    sockets = [socket.socket(fileno=fd) for fd in (data_fd, control_fd, result_fd)]
    data, control, results = sockets
    for sock in sockets:
        sock.setblocking(False)
        sock.set_inheritable(False)
    loop = asyncio.get_running_loop()
    engine = limits = None
    close_deadline = report = None
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
        if callable(getattr(engine, "observe_gpu_identity", None)):
            await engine.observe_gpu_identity()
        await loop.sock_sendall(control, _frame(dict(op="ready", report=engine.ready()), limits.control_frame_bytes))
        control_queue = asyncio.Queue(maxsize=limits.control_slots)
        result_queue = asyncio.Queue(maxsize=limits.result_slots)
        abort_queue = asyncio.Queue(maxsize=limits.max_inflight)
        early_aborts = set()
        closing = asyncio.Event()
        failure = loop.create_future()
        next_sequence = 0

        feedback_packet = None

        async def writer(sock, queue):
            nonlocal feedback_packet
            while True:
                packet, key = await queue.get()
                try:
                    await loop.sock_sendall(sock, packet)
                    if packet is feedback_packet:
                        feedback_packet = None
                    if key is not None:
                        owned.pop(key, None)
                finally:
                    queue.task_done()

        async def reply(value):
            await control_queue.put((_frame(value, limits.control_frame_bytes), None))

        async def feedback_updates():
            nonlocal feedback_packet
            latest_sequence = None
            wait_first = getattr(engine, 'wait_for_first_feedback', None)
            if callable(wait_first):
                try:
                    await wait_first()
                except Exception:
                    pass  # Optional notification failure retains periodic sampling.
            while not closing.is_set():
                # One optional packet at most, leaving a slot for critical control.
                if feedback_packet is None and control_queue.qsize() < control_queue.maxsize - 1:
                    try:
                        snapshot = engine.feedback_snapshot()
                        if snapshot is not None and snapshot["sequence"] != latest_sequence:
                            packet = _frame(dict(op="feedback", snapshot=snapshot), limits.control_frame_bytes)
                            control_queue.put_nowait((packet, None))
                            feedback_packet, latest_sequence = packet, snapshot["sequence"]
                    except Exception:
                        pass  # Optional feedback never replaces a terminal or close cause.
                await asyncio.sleep(limits.feedback_interval_s)

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
            if opening['engine'].get('enable_mfu_metrics', False):
                from src.observability.metrics.mfu import optional_packet
                packet = optional_packet(event, lambda: engine.mfu_snapshot(), _frame, limits.item_result_bytes)
            await result_queue.put((packet, key))

        async def input_commands():
            nonlocal next_sequence
            while not closing.is_set():
                value, size = await _read_frame(data, min(limits.input_bytes, (1 << 32)-1))
                if value.get('op') == 'submit':
                    if size > limits.item_input_bytes:
                        raise ValueError('single request exceeds IPC item allowance')
                    items = [dict(request_id=value['request_id'], row=value['row'])]
                    charges = [size]
                elif value.get('op') == 'submit_batch':
                    items = value.get('requests')
                    if type(items) is not list or not 0 < len(items) <= limits.max_inflight:
                        raise ValueError('invalid IPC batch members')
                    charges = [len(_frame(item, limits.input_bytes))-4 for item in items]
                    charges[0] += size-sum(charges)
                else:
                    raise ValueError('invalid IPC data command')
                count = len(items)
                if (len(owned)+count > min(limits.max_inflight, limits.result_slots)
                        or sum(s["bytes"] for s in owned.values()) + size > limits.input_bytes
                        or (len(owned) + count) * limits.item_result_bytes > limits.result_bytes):
                    raise ValueError("async engine child reservation exceeded")
                common._prepared([item['row'] for item in items])
                for i, item in enumerate(items):
                    if item.get('request_id') != opening['owner_uid'] + ':' + str(next_sequence+i):
                        raise ValueError('async engine submit sequence invalid')
                    _frame(dict(op='submit', **item, generation=value['generation'], deadline=value['deadline']),
                           limits.item_input_bytes)
                next_sequence += count
                for item, charge in zip(items, charges):
                    key = item['request_id']
                    owned[key] = dict(bytes=charge, aborted=key in early_aborts, task=None)
                    early_aborts.discard(key)
                for item in items:
                    key, slot = item['request_id'], owned[item['request_id']]
                    await reply(dict(op='accepted', request_id=key))
                    slot['task'] = asyncio.create_task(run(key, slot, dict(row=item['row'],
                        generation=value['generation'], deadline=value['deadline'])))
                    slot['task'].add_done_callback(observe_request)
                value = items = None  # Do not retain other members after their terminal.

        def observe_request(task):
            if not task.cancelled():
                error = task.exception()
                if error is not None and not failure.done():
                    failure.set_result(error)

        async def control_commands():
            nonlocal close_deadline
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
                    deadline = value["deadline"]
                    common._number(deadline, "close deadline", low=0, inclusive_low=False)
                    close_deadline = deadline
                    closing.set()
                else:
                    raise ValueError("unexpected async engine control command")

        tasks = [asyncio.create_task(writer(control, control_queue)),
                 asyncio.create_task(writer(results, result_queue)),
                 asyncio.create_task(input_commands()), asyncio.create_task(control_commands()),
                 asyncio.create_task(abort_requests())]
        if limits.feedback_interval_s and callable(getattr(engine, "feedback_snapshot", None)):
            tasks.append(asyncio.create_task(feedback_updates()))
        waiter = asyncio.create_task(closing.wait())
        done, _ = await asyncio.wait([*tasks, waiter, failure], return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task is failure:
                raise task.result()
            if task is not waiter:
                task.result()
        closing.set()
        if len(tasks) > 5:
            tasks[5].cancel()
            await asyncio.gather(tasks[5], return_exceptions=True)
        # Freeze input before shutdown; no late data-channel frame may create
        # another generation after the engine's close acknowledgement.
        for task in tasks[2:4]:
            task.cancel()
        await asyncio.gather(*tasks[2:4], return_exceptions=True)
        # Native shutdown and result writes are independent of command reading.
        remaining = close_deadline - time.monotonic()
        report = (await engine.close(timeout=remaining) if remaining > 0 else
                  dict(status="unknown", engine_api_shutdown_confirmed=False,
                       gpu_exit_confirmed=None, error_type="CloseDeadlineExceeded"))
        pending = [slot["task"] for slot in owned.values() if slot["task"] is not None]
        if pending:
            done, waiting = await asyncio.wait(pending, timeout=1)
            for task in waiting:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        tasks[4].cancel()
        await asyncio.gather(tasks[4], return_exceptions=True)
        await asyncio.wait_for(result_queue.join(), 1)
        tasks[1].cancel()
        await asyncio.gather(tasks[1], return_exceptions=True)
        await reply(dict(op="closed", report=report, producer_stopped=dict(
            owner_uid=opening["owner_uid"], engine_uid=engine.engine_uid,
            received_submissions=next_sequence, input_reader_stopped=tasks[2].done(),
            request_tasks_stopped=all(task.done() for task in pending),
            abort_worker_stopped=tasks[4].done(), result_writer_stopped=tasks[1].done())))
        await asyncio.wait_for(control_queue.join(), 1)
        waiter.cancel()
    except Exception as error:
        try:
            await asyncio.wait_for(loop.sock_sendall(control,
                _frame(dict(op="error", error_type=type(error).__name__),
                       limits.control_frame_bytes if limits is not None else 4096)), 0.2)
        except Exception:
            pass
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if engine is not None and report is None:
            # An unexpected peer failure still needs bounded best-effort cleanup.
            # It has no normal close receipt and cannot establish API success.
            remaining = 1 if close_deadline is None else close_deadline - time.monotonic()
            if remaining > 0:
                await engine.close(timeout=remaining)
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
