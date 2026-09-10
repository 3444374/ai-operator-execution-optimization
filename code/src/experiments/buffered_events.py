"""Bounded event buffering with one writer; overflow or I/O failure invalidates a run."""
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import threading
import time

from src.baselines.common.private_artifacts import require_outside_git


class EventRecordingError(RuntimeError):
    pass


def compact_event(event):
    """Retain correlation and counters, hashing output instead of retaining its text."""
    allowed = {'event', 'session_id', 'task', 'monotonic_ns', 'sequence', 'payload_digest',
               'response_model_id', 'finish_reason', 'prompt_tokens', 'output_tokens', 'usage',
               'attempt', 'request_values_sha256', 'request_bytes_sha256', 'key', 'code',
               'elapsed_seconds', 'remaining', 'connection_id', 'job_id'}
    result = {key: value for key, value in event.items() if key in allowed}
    if isinstance(event.get('raw_output'), str):
        encoded = event['raw_output'].encode('utf-8')
        result.update(raw_output_sha256=hashlib.sha256(encoded).hexdigest(), raw_output_bytes=len(encoded))
    return result


class BufferedEvents:
    """record() only encodes and queues bytes; close() drains and fsyncs outside query time."""
    def __init__(self, path: Path, *, max_pending_bytes=8 * 1024 * 1024, max_events=8192,
                 batch_bytes=65536, flush_interval=.05, close_timeout=10):
        if any(type(x) is not int or x < 1 for x in (max_pending_bytes, max_events, batch_bytes)):
            raise ValueError('positive event buffer limits required')
        if any(type(x) not in (int, float) or not math.isfinite(x) or x <= 0
               for x in (flush_interval, close_timeout)):
            raise ValueError('positive event writer deadlines required')
        path = Path(path)
        require_outside_git(path)
        self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        self._queue = queue.Queue(max_events)
        self._limit, self._batch_bytes = max_pending_bytes, batch_bytes
        self._interval, self._timeout = flush_interval, close_timeout
        self._lock = threading.Lock()
        self._closing = self._closed = False
        self._error = None
        self._pending = self._peak = self._accepted = self._written = self._enqueue_ns = self._write_ns = 0
        self._thread = threading.Thread(target=self._write, name='experiment-events', daemon=True)
        self._thread.start()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def record(self, event):
        started = time.monotonic_ns()
        value = (json.dumps(event, ensure_ascii=False, allow_nan=False, separators=(',', ':')) + '\n').encode()
        with self._lock:
            if self._error is not None or self._closing:
                raise EventRecordingError('event writer failed or closed') from self._error
            if self._pending + len(value) > self._limit:
                self._error = EventRecordingError('event byte buffer exhausted')
                raise self._error
            try:
                self._queue.put_nowait(value)
            except queue.Full as error:
                self._error = EventRecordingError('event count buffer exhausted')
                raise self._error from error
            self._pending += len(value)
            self._peak = max(self._peak, self._pending)
            self._accepted += 1
            self._enqueue_ns += time.monotonic_ns() - started

    def _write_batch(self, batch):
        payload = b''.join(batch)
        before = time.monotonic_ns()
        offset = 0
        while offset < len(payload):
            written = os.write(self._fd, payload[offset:])
            if written <= 0:
                raise OSError('event write made no progress')
            offset += written
        with self._lock:
            self._pending -= len(payload)
            self._written += len(batch)
            self._write_ns += time.monotonic_ns() - before

    def _write(self):
        batch, size = [], 0
        try:
            while True:
                try:
                    value = self._queue.get(timeout=self._interval)
                except queue.Empty:
                    if batch:
                        self._write_batch(batch)
                        batch, size = [], 0
                    continue
                if value is None:
                    if batch:
                        self._write_batch(batch)
                    os.fsync(self._fd)
                    return
                batch.append(value)
                size += len(value)
                if size >= self._batch_bytes:
                    self._write_batch(batch)
                    batch, size = [], 0
        except BaseException as error:
            with self._lock:
                self._error = error
        finally:
            os.close(self._fd)

    def close(self):
        with self._lock:
            if self._closed:
                if self._error:
                    raise EventRecordingError('event writer failed') from self._error
                return
            self._closing = True
        until = time.monotonic() + self._timeout
        while self._thread.is_alive():
            try:
                self._queue.put(None, timeout=min(.05, max(.001, until - time.monotonic())))
                break
            except queue.Full:
                if time.monotonic() >= until:
                    raise EventRecordingError('event writer close deadline reached')
        self._thread.join(max(0, until - time.monotonic()))
        with self._lock:
            if self._thread.is_alive():
                raise EventRecordingError('event writer close deadline reached')
            self._closed = True
            if self._error is not None:
                raise EventRecordingError('event writer failed') from self._error
            if self._written != self._accepted or self._pending:
                raise EventRecordingError('event history is incomplete')

    def snapshot(self):
        with self._lock:
            return {'accepted_events': self._accepted, 'written_events': self._written,
                    'pending_bytes': self._pending, 'peak_pending_bytes': self._peak,
                    'enqueue_seconds': self._enqueue_ns / 1e9, 'write_seconds': self._write_ns / 1e9,
                    'closed': self._closed, 'error_type': type(self._error).__name__ if self._error else None}
