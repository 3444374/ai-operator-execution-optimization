"""Small serial control client; the supplied supervisor owns every process."""
from __future__ import annotations

import json
import os
import selectors
import time
from typing import Callable


class RecordsRejected(RuntimeError):
    """A command failed before submission; the owner remains available."""


class RecordsBridge:
    def __init__(self, process, *, abort_owner: Callable[[], None], timeout_s=10.0, log=None):
        if not callable(abort_owner) or not 0 < timeout_s <= 600:
            raise ValueError("records bridge requires a supervisor and finite timeout")
        self.process, self.abort_owner, self.timeout_s = process, abort_owner, timeout_s
        self.log = log or (lambda _line: None)
        self._buffer = bytearray()
        self._terminal = self._closed = False
        try:
            os.set_blocking(process.stdin.fileno(), False)
            os.set_blocking(process.stdout.fileno(), False)
            self.meta = self._read(time.monotonic() + timeout_s)
            if self.meta.get("event") != "ready" or self.meta.get("protocol") != "imlane.records.v1":
                raise RuntimeError("records bridge startup differs")
        except BaseException as error:
            self._terminal = True
            try:
                self.abort()
            except BaseException as cleanup:
                error.add_note("Owner cleanup remains unconfirmed: " + type(cleanup).__name__)
            raise

    def _wait(self, stream, event, deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("records bridge response deadline exceeded; native work may remain")
        with selectors.DefaultSelector() as selector:
            selector.register(stream, event)
            if not selector.select(remaining):
                raise TimeoutError("records bridge response deadline exceeded; native work may remain")

    def _read(self, deadline):
        while True:
            if b"\n" in self._buffer:
                line, _, remaining = self._buffer.partition(b"\n")
                self._buffer = bytearray(remaining)
                text = line.decode("utf-8")
                if text.startswith("IMLANE_RECORDS "):
                    return json.loads(text.partition(" ")[2])
                self.log(text)
                continue
            self._wait(self.process.stdout, selectors.EVENT_READ, deadline)
            part = os.read(self.process.stdout.fileno(), 65536)
            if not part:
                raise RuntimeError("records bridge ended without a complete response")
            self._buffer.extend(part)
            if len(self._buffer) > 8 * 1048576:
                raise RuntimeError("records bridge frame exceeds its allowance")

    def request(self, op, *, deadline=None, **values):
        if self._terminal or self._closed:
            raise RuntimeError("records bridge owner is terminal or closed")
        wire = (json.dumps(dict(op=op, **values), ensure_ascii=False, allow_nan=False) + "\n").encode()
        if len(wire) > 131072:
            raise ValueError("records command exceeds wire allowance before submission")
        deadline = min(time.monotonic() + self.timeout_s, deadline) if deadline is not None else time.monotonic() + self.timeout_s
        try:
            offset = 0
            while offset < len(wire):
                self._wait(self.process.stdin, selectors.EVENT_WRITE, deadline)
                offset += os.write(self.process.stdin.fileno(), wire[offset:])
            reply = self._read(deadline)
            if time.monotonic() > deadline:
                raise TimeoutError("records bridge response arrived after its deadline")
            if reply.get("ok") is False:
                if reply.get("owner_terminal") is False:
                    raise RecordsRejected(reply.get("error", "records command was rejected"))
                raise RuntimeError(reply.get("error", "records owner became terminal"))
            if reply.get("ok") is not True or reply.get("event") != op:
                raise RuntimeError("records reply does not match command")
            return reply
        except RecordsRejected:
            raise
        except BaseException as error:
            self._terminal = True
            try:
                self.abort()
            except BaseException as cleanup:
                error.add_note("Owner cleanup remains unconfirmed: " + type(cleanup).__name__)
            raise

    def begin(self, query_id):
        return self.request("begin", query_id=query_id)

    def submit(self, block_id, rows):
        return self.request("submit", block_id=block_id, rows=rows)

    def ready(self, *, deadline=None):
        return self.request("poll", deadline=deadline)["ready"]

    def collect(self, block_id):
        return self.request("collect", block_id=block_id)["rows"]

    def wait_ready(self, *, timeout_s):
        if not 0 < timeout_s <= 600:
            raise ValueError("records task deadline must be positive and finite")
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                blocks = self.ready(deadline=deadline)
            except TimeoutError as cause:
                if time.monotonic() < deadline:
                    raise
                error = TimeoutError("records native task deadline exceeded; owner must stop")
                for note in getattr(cause, "__notes__", ()):
                    error.add_note(note)
                raise error from cause
            if blocks:
                return blocks
            time.sleep(min(.01, max(0, deadline - time.monotonic())))
        self._terminal = True
        error = TimeoutError("records native task deadline exceeded; owner must stop")
        try:
            self.abort()
        except BaseException as cleanup:
            error.add_note("Owner cleanup remains unconfirmed: " + type(cleanup).__name__)
        raise error

    def end(self):
        return self.request("end")

    def abort(self):
        self._terminal = True
        # A failed callback must remain retryable; completion is supervisor evidence.
        self.abort_owner()
        if self.process.poll() is None:
            raise RuntimeError("records supervisor did not confirm bridge termination")
        self._closed = True

    def close(self):
        if self._closed:
            return
        if self._terminal:
            self.abort()
            return
        try:
            self.request("close")
            if self.process.wait(timeout=self.timeout_s) != 0:
                raise RuntimeError("records bridge exit was unsuccessful")
        except BaseException as error:
            self._terminal = True
            try:
                self.abort()
            except BaseException as cleanup:
                error.add_note("Owner cleanup remains unconfirmed: " + type(cleanup).__name__)
            raise
        self._closed = True
