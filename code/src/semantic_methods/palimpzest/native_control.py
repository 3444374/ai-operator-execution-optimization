"""Native-adapted control: original SDK HTTPX sends on a bounded native I/O pool."""

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from src.baselines.text.frameworks.native_calls import NativeCallOutcome, native_error_text
from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from .backend import PalimpzestAdapterError


class NativeAdaptedExecutor:
    """Control for adaptation cost; this is neither a vendor graph nor SemLoom Core."""

    def __init__(self, *, max_workers, max_batch_rows=4096, max_batch_bytes=32 * 1024**2,
                 before_send=None, after_send=None):
        if any(type(value) is not int or value < 1 for value in (max_workers, max_batch_rows, max_batch_bytes)):
            raise ValueError("native-adapted control needs positive worker/row/byte allowances")
        self.max_workers, self.max_batch_rows, self.max_batch_bytes = max_workers, max_batch_rows, max_batch_bytes
        if (before_send is None) != (after_send is None) or any(
                value is not None and not callable(value) for value in (before_send, after_send)):
            raise TypeError("native-adapted actual-send observation requires both callbacks")
        self.before_send, self.after_send = before_send, after_send
        self._senders, self._lock = {}, threading.Lock()
        self._active = self._closed = self._unsettled = False
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="pz-native-http")
        self.last_close_report = None

    def bind_sdk_request(self, call_id, request, sender):
        with self._lock:
            if self._closed or self._unsettled or call_id in self._senders:
                raise PalimpzestAdapterError("native-adapted SDK request owner is not available")
            self._senders[call_id] = (request, sender)

    def _send(self, call, request, sender):
        import httpx
        response = full = error = reservation = None
        observed = False
        try:
            if self.before_send is not None:
                reservation = self.before_send(call, request)
                observed = True
            # Streaming here bounds raw response bytes; the model request is nonstreaming.
            response = sender(request, stream=True)
            body = bytearray()
            for chunk in response.iter_raw():
                body.extend(chunk)
                if len(body) > call.max_response_bytes:
                    raise PalimpzestAdapterError("native-adapted response exceeds its byte allowance")
            full = FullModelResponse(response.status_code,
                tuple((name.decode("ascii"), value.decode("latin-1")) for name, value in response.headers.raw),
                bytes(body), response.http_version)
            if len(encode_full_response(full)) > call.max_response_bytes:
                raise PalimpzestAdapterError("native-adapted complete response exceeds its byte allowance")
            return NativeCallOutcome(call.call_id, call.row_id, full)
        except httpx.TransportError as failure:
            error = failure
            return NativeCallOutcome(call.call_id, call.row_id, None, transport_error=native_error_text(failure))
        except BaseException as failure:
            error = failure
            raise
        finally:
            if response is not None:
                response.close()
            if observed and self.after_send is not None:
                try:
                    self.after_send(reservation, response=full, error=error)
                except BaseException as failure:
                    observation_error = PalimpzestAdapterError("PZ actual-send observation failed")
                    if error is not None:
                        observation_error.add_note("Provider send also failed: " + type(error).__name__)
                    raise observation_error from failure

    def iter_batch(self, calls, *, cancelled=None, timeout_s=120):
        calls = tuple(calls)
        with self._lock:
            if self._closed or self._active or self._unsettled:
                raise PalimpzestAdapterError("native-adapted control already owns active or unconfirmed work")
            if (not calls or len(calls) > self.max_batch_rows
                    or sum(len(call.payload) for call in calls) > self.max_batch_bytes
                    or len({call.call_id for call in calls}) != len(calls)
                    or any(call.call_id not in self._senders for call in calls)):
                raise PalimpzestAdapterError("native-adapted batch changed its prepared Request contract")
            self._active = True
            senders = [self._senders.pop(call.call_id) for call in calls]
        futures, received, clean = {}, 0, False
        try:
            if cancelled and cancelled():
                raise InterruptedError("native-adapted batch cancelled before send")
            futures = {self._pool.submit(self._send, call, *sender): call
                       for call, sender in zip(calls, senders, strict=True)}
            for future in as_completed(futures, timeout=timeout_s):
                if cancelled and cancelled():
                    raise InterruptedError("native-adapted batch cancelled")
                outcome = future.result()
                received += 1
                if outcome.transport_error:
                    self._unsettled = True
                yield outcome
            clean = not self._unsettled
        finally:
            for future in futures:
                future.cancel()
            unfinished = any(not future.done() for future in futures)
            with self._lock:
                self._unsettled |= unfinished or received != len(calls)
                self._active = False
                self.last_close_report = dict(clean=clean and not self._unsettled,
                    prepared=len(calls), received=received, unconfirmed_work=self._unsettled,
                    reuse_allowed=not self._unsettled, remote_http_stopped="unavailable",
                    scheduler_owner="native-adapted original SDK HTTPX I/O pool",
                    max_workers=self.max_workers)

    def close(self):
        with self._lock:
            self._closed = True
            safe = not self._active and not self._unsettled
            self._senders.clear()
        self._pool.shutdown(wait=safe, cancel_futures=True)
        return safe
