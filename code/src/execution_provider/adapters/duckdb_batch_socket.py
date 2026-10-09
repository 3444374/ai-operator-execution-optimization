"""Dedicated finite-batch DuckDB transport over the existing bounded JSON framing."""

import base64
import select

from ..completion import CompletionAdapterError
from ..wire.framing import (
    MAX_FRAME_BYTES, ProtocolError, RegisteredStream, encode_frame, has_duplicate_fields, read_frame,
)
from ...scheduling.core.session_contract import State
from .full_response import decode_full_response
from .native_tasks import NativeTaskSession, prepare_native_task


PROTOCOL = "semloom.duckdb.batch.v1"
# Space for base64, identities and the already bounded response headers.
MAX_SOCKET_RESULT_BYTES = (MAX_FRAME_BYTES - 32768) * 3 // 4


def _message(message, kind, fields):
    if (type(message) is not dict or has_duplicate_fields(message)
            or set(message) != {"type", "protocol", *fields}
            or message.get("type") != kind or message.get("protocol") != PROTOCOL):
        raise ProtocolError("INVALID_MESSAGE")


def _uint(value):
    if (type(value) is not str or not value.isascii() or not value.isdigit()
            or (len(value) > 1 and value[0] == "0") or len(value) > 20
            or int(value) >= 2**64):
        raise ProtocolError("INVALID_TASK")
    return int(value)


class DuckDBBatchConnection:
    """Runs on the Engine owner thread; the listener owns socket/service teardown.

    One borrowed connection carries one operator flow. This is neither a SQL
    service nor a concurrent query graph. The integration owner serializes socket
    dispatch or supplies its existing owner-thread service loop.
    """

    def __init__(self, execution, *, frame_timeout_s=5.0):
        if frame_timeout_s <= 0:
            raise ValueError("frame timeout must be positive")
        self.execution, self.frame_timeout_s = execution, frame_timeout_s

    def serve(self, connection):
        flow = None
        old_timeout = connection.gettimeout()
        connection.settimeout(self.frame_timeout_s)
        stream = RegisteredStream(connection)

        def send(kind, **fields):
            connection.sendall(encode_frame(dict(type=kind, protocol=PROTOCOL, **fields)))

        def error(code):
            usage = (self.execution.engine.capacity.usage(flow.session.session_id)
                     if flow is not None else None)
            send("error", code=code, uncertain_requests=usage.active_requests if usage else 0)

        def result(delivery):
            raw = decode_full_response(delivery.result)
            if delivery.info is None:
                raise ProtocolError("INVALID_RESULT")
            send("result", sequence=str(delivery.key.sequence), row_sequence=str(delivery.info.row_sequence),
                 call_id=delivery.info.call_id, stage_id=delivery.info.stage_id,
                 status_code=raw.status_code, headers=raw.headers, http_version=raw.http_version,
                 body_base64=base64.b64encode(raw.body).decode("ascii"))

        try:
            opened = read_frame(connection)
            _message(opened, "open", {"query_id", "operator_id"})
            flow = NativeTaskSession(self.execution, opened["query_id"], opened["operator_id"])
            result_bytes = min(flow.limits.item_result_bytes, flow.limits.result_bytes,
                               MAX_SOCKET_RESULT_BYTES)
            send("opened", max_offer_tasks=flow.limits.offer_tasks,
                 max_input_bytes=min(flow.limits.input_bytes, flow.limits.item_input_bytes),
                 max_result_bytes=result_bytes, max_frame_bytes=MAX_FRAME_BYTES,
                 work_unit=self.execution.work_unit)
            if self.execution.work_unit != "work_units":
                raise ProtocolError("UNSUPPORTED_WORK_UNIT")
            while True:
                message = read_frame(stream)
                if message is None:
                    return
                kind = message.get("type")
                if kind == "offer":
                    _message(message, "offer", {"tasks"})
                    rows = message["tasks"]
                    if type(rows) is not list or len(rows) > flow.limits.offer_tasks:
                        raise ProtocolError("INVALID_TASK")
                    tasks = []
                    for row in rows:
                        if (type(row) is not dict or set(row) != {
                            "sequence", "row_sequence", "call_id", "stage_id", "payload"
                        } or type(row["payload"]) is not str):
                            raise ProtocolError("INVALID_TASK")
                        tasks.append(prepare_native_task(row["payload"].encode("utf-8"),
                            _uint(row["sequence"]), row_sequence=_uint(row["row_sequence"]),
                            call_id=row["call_id"], stage_id=row["stage_id"], max_result_bytes=result_bytes))
                    offered = flow.offer(tuple(tasks))
                    send("accepted", accepted_prefix_count=offered.accepted_prefix_count,
                         status=offered.status, reason=offered.reason)
                elif kind == "end":
                    _message(message, "end", set())
                    flow.end_input()
                    send("ended")
                elif kind == "cancel":
                    _message(message, "cancel", set())
                    flow.request_cancel()
                    flow.advance(1)
                    send("cancelled", uncertain_requests=flow.close().uncertain_requests)
                    return
                elif kind == "poll":
                    _message(message, "poll", set())
                    while True:
                        progress = flow.advance(1)
                        if progress.deliveries:
                            delivery = progress.deliveries[0]
                            try:
                                result(delivery)
                            finally:
                                flow.release((delivery.lease_id,))
                            break
                        if progress.state == State.FAILED:
                            error(progress.error if progress.error in ("MODEL_TIMEOUT", "MODEL_UNAVAILABLE")
                                  else "MODEL_UNAVAILABLE")
                            return
                        if progress.state == State.FINISHED:
                            send("finished")
                            return
                        if progress.state == State.CANCELLED:
                            send("cancelled", uncertain_requests=flow.close().uncertain_requests)
                            return
                        if progress.blocked_reason == "NEED_INPUT":
                            send("idle")
                            break
                        # Read cancellation/EOF while waiting; a local disconnect does
                        # not confirm cancellation at the model service.
                        if select.select([connection], [], [], 0)[0]:
                            control = read_frame(connection)
                            if control is None:
                                return
                            _message(control, "cancel", set())
                            flow.request_cancel()
                            flow.advance(1)
                            send("cancelled", uncertain_requests=flow.close().uncertain_requests)
                            return
                        flow.wait(progress)
                else:
                    raise ProtocolError("INVALID_MESSAGE")
        except CompletionAdapterError as failure:
            try: error(failure.code)
            except OSError: pass
        except (ProtocolError, ValueError, TypeError, RecursionError):
            try: error("INVALID_MESSAGE")
            except OSError: pass
        except OSError:
            return
        finally:
            if flow is not None:
                flow.close(clean=flow.session.state == State.FINISHED)
            connection.settimeout(old_timeout)
