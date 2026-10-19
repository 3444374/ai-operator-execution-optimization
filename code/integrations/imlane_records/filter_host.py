"""Bounded submission/collection of records; IMLane owns all task scheduling."""
from __future__ import annotations

import itertools
import json
from pathlib import Path
import time

from integrations.imlane_records.filter_workload import Occurrences, PROTOCOL


class FilterResults:
    """Iterator accepted by the existing ordinary Movie query consumer.

The caller supplies a v2 bridge and its author scope's finalizer. Nothing here
allocates a model, a worker pool or a vendor scheduler.
"""

    def __init__(self, rows, client, *, query_id, max_calls, control_dir, deadline,
                 close_owner, block_rows=4, max_pending=2):
        if (type(block_rows) is not int or not 0 < block_rows <= 16
                or type(max_pending) is not int or not 0 < max_pending <= 2
                or not callable(close_owner) or client.meta.get("protocol") != PROTOCOL):
            raise ValueError("Filter requires the explicit v2 bridge, bounded capacity and author finalizer")
        self.source, self.client = iter(rows), client
        self.mapping = Occurrences(query_id, max_calls=max_calls)
        self.query_id, self.control = query_id, Path(control_dir)
        self.deadline, self.close_owner = deadline, close_owner
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
            raise TimeoutError("IMLane Filter query deadline expired")
        return min(600, value)

    def _fill(self):
        while not (self.failed or self.exhausted) and len(self.pending) < self.max_pending:
            try:
                rows = list(itertools.islice(self.source, self.block_rows))
                if not rows:
                    self.exhausted = True
                    return
                block = "filter-" + str(self.index)
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
        call = "imlane-filter-close:" + self.query_id
        source = dict(call_id=call, row_id=0, query_id=self.query_id, stage_id="filter",
                      record_json=json.dumps({"operation": "close"}))
        block = "filter-close"
        self.client.submit(block, [source], deadline=self.deadline)
        ready = self.client.wait_ready(timeout_s=self._remaining())
        if ready != [block]:
            raise ValueError("Filter close was not the only remaining author task")
        rows = self.client.collect(block, deadline=self.deadline)
        if len(rows) != 1 or rows[0]["call_id"] != call or rows[0]["row_id"] != 0:
            raise ValueError("Filter close result occurrence differs")
        value = json.loads(rows[0]["outcome_json"])
        payload = value["result"]
        if (value.get("protocol") != PROTOCOL or value.get("query_id") != self.query_id
                or value.get("stage_id") != "filter" or payload.get("kind") != "close"
                or payload.get("_occurrence_id") != call or payload.get("_position") != 0):
            raise ValueError("Filter close result protocol differs")
        self.model_close = payload["close_report"]
        if (value["status"] != "ok" or self.model_close.get("status") != "closed"
                or self.model_close.get("process_exit_confirmed") is not True
                or self.model_close.get("cleanup_errors")):
            raise RuntimeError("IMLane model process close remains unconfirmed")
        self.client.end(deadline=self.deadline)
        self.client.close()
        self.workload_closed = True

    def close(self):
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
                failure.add_note("Filter author abort remains unconfirmed: " + type(cleanup).__name__)
        try:
            report = self.close_owner()
            self.last_close_report = dict(model=self.model_close, author=report,
                                          gpu_exit_confirmed=None,
                                          gpu_exit_reason="requires independent GPU qualification")
            self.closed = report.get("resources_confirmed") is True
            if not self.closed and error is None:
                error = RuntimeError("IMLane author scope resources remain unconfirmed")
        except BaseException as cleanup:
            if error is None:
                error = cleanup
            else:
                error.add_note("Filter author finalization also failed: " + type(cleanup).__name__)
        if error is not None:
            raise error
        return self.last_close_report
