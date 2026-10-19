"""Bounded submission/collection of records; IMLane owns all task scheduling."""
from __future__ import annotations

import itertools
import json
import math
from contextlib import nullcontext
from pathlib import Path
import time

from integrations.imlane_records.filter_workload import PROTOCOL
from integrations.imlane_records.model_lifecycle import MODEL_CLOSE_GRACE_S, model_closed


class RecordsResults:
    """Iterator accepted by the existing ordinary Movie query consumer.

The caller supplies a v2 bridge and its author scope's finalizer. Nothing here
allocates a model, a worker pool or a vendor scheduler.
"""

    def __init__(self, rows, client, *, query_id, mapping, stage_id, block_prefix, control_dir, deadline,
                 close_owner, block_rows=4, max_pending=2, cleanup_scope=nullcontext,
                 close_timeout_s=MODEL_CLOSE_GRACE_S):
        if (type(block_rows) is not int or not 0 < block_rows <= 16
                or type(max_pending) is not int or not 0 < max_pending <= 2
                or not callable(close_owner) or not callable(cleanup_scope)
                or type(close_timeout_s) not in (int, float) or not math.isfinite(close_timeout_s)
                or not MODEL_CLOSE_GRACE_S <= close_timeout_s <= 600
                or type(deadline) not in (int, float) or not math.isfinite(deadline)
                or client.meta.get("protocol") != PROTOCOL):
            raise ValueError("records carrier requires the explicit v2 bridge, bounded capacity and author finalizer")
        self.source, self.client = iter(rows), client
        self.mapping = mapping
        self.stage_id, self.block_prefix = stage_id, block_prefix
        self.query_id, self.control = query_id, Path(control_dir)
        self.deadline, self.close_owner = deadline, close_owner
        self.cleanup_scope, self.close_timeout_s = cleanup_scope, close_timeout_s
        self.block_rows, self.max_pending = block_rows, max_pending
        self.pending, self.index = {}, 0
        self.exhausted = self.failed = self.started = self.closed = False
        self.primary = None
        self.last_close_report = None
        self.model_close = None
        self.workload_closed = False
        self._values = self._run()

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._values)

    def _remaining(self):
        value = self.deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError("IMLane records query deadline expired")
        return min(600, value)

    def _fill(self):
        while not (self.failed or self.exhausted) and len(self.pending) < self.max_pending:
            try:
                rows = list(itertools.islice(self.source, self.block_rows))
                if not rows:
                    self.exhausted = True
                    return
                block = self.block_prefix + "-" + str(self.index)
                self.index += 1
                encoded = self.mapping.encode(rows)
                self._remaining()
                self.client.submit(block, encoded, deadline=self.deadline)
                self.pending[block] = encoded
            except BaseException as error:
                self.primary, self.failed = error, True
                return

    def _run(self):
        self._remaining()
        self.client.begin(self.query_id, deadline=self.deadline)
        self.started = True
        self._fill()
        while self.pending:
            try:
                ready = self.client.wait_ready(timeout_s=self._remaining())
                for block in ready:
                    if block not in self.pending:
                        raise ValueError("IMLane returned an undeclared ready block")
                    values = self.mapping.decode(self.client.collect(block, deadline=self.deadline), self.pending[block])
                    del self.pending[block]
                    if any(row["status"] != "succeeded" for row in values):
                        self.failed = True
                    # The original consumer records every terminal, including
                    # suffix successes and failures with authoritative usage.
                    yield from values
                self._fill()
            except BaseException as error:
                self.primary, self.failed = error, True
                raise
        if self.primary is not None:
            raise self.primary

    def _close_workload(self):
        # Work deadlines stop inference; cleanup receives its own finite window.
        deadline = time.monotonic() + self.close_timeout_s
        call = "imlane-" + self.block_prefix + "-close:" + self.query_id
        source = dict(call_id=call, row_id=0, query_id=self.query_id, stage_id=self.stage_id,
                      record_json=json.dumps({"operation": "close"}))
        block = self.block_prefix + "-close"
        self.client.submit(block, [source], deadline=deadline)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("IMLane model cleanup deadline expired")
        ready = self.client.wait_ready(timeout_s=remaining)
        if ready != [block]:
            raise ValueError("records close was not the only remaining author task")
        rows = self.client.collect(block, deadline=deadline)
        if len(rows) != 1 or rows[0]["call_id"] != call or rows[0]["row_id"] != 0:
            raise ValueError("records close result occurrence differs")
        value = json.loads(rows[0]["outcome_json"])
        payload = value["result"]
        if (value.get("protocol") != PROTOCOL or value.get("query_id") != self.query_id
                or value.get("stage_id") != self.stage_id or payload.get("kind") != "close"
                or payload.get("_occurrence_id") != call or payload.get("_position") != 0):
            raise ValueError("records close result protocol differs")
        self.model_close = payload["close_report"]
        if value["status"] != "ok" or not model_closed(self.model_close):
            raise RuntimeError("IMLane model process close remains unconfirmed")
        self.client.end(deadline=deadline)
        self.client.close(deadline=deadline)
        self.workload_closed = True

    def close(self):
        # StopBudget defers a signal exception until owned teardown has returned.
        with self.cleanup_scope():
            return self._close()

    def _close(self):
        if self.closed:
            return self.last_close_report
        error = None
        try:
            if self.workload_closed:
                pass
            elif self.started and not self.pending and not self.client._terminal:
                self._close_workload()
            else:
                (self.control / "stop").touch()
                self.client.abort()
        except BaseException as failure:
            error = failure
            (self.control / "stop").touch()
            try:
                self.client.abort()
            except BaseException as cleanup:
                failure.add_note("records author abort remains unconfirmed: " + type(cleanup).__name__)
        try:
            report = self.close_owner()
            self.closed = (report.get("resources_confirmed") is True
                           and (not self.started or self.workload_closed))
            self.last_close_report = dict(model=self.model_close, author=report,
                                          status="closed" if self.closed else "unknown",
                                          gpu_exit_confirmed=None,
                                          gpu_exit_reason="requires independent GPU qualification")
            if not self.closed and error is None:
                error = RuntimeError("IMLane author scope resources remain unconfirmed" if
                                     report.get("resources_confirmed") is not True else
                                     "IMLane model API close remains unconfirmed")
        except BaseException as cleanup:
            if error is None:
                error = cleanup
            else:
                error.add_note("records author finalization also failed: " + type(cleanup).__name__)
        if error is not None:
            raise error
        return self.last_close_report
