"""Coalesce ready experiment records without waiting for future requests."""
import asyncio
import hashlib
import time


class BatchedRequestGuard:
    """Commit up to 16 queued rows on the next loop turn, then permit sending.

    Queues belong to their event loop. Cancelled callers remain charged once
    queued; transport cancellation checks still decide whether a row is sent.
    No timer, worker thread, extra capacity or speculative request is added.
    """
    def __init__(self, ledger, observe_request, record):
        self.ledger, self.observe_request, self.record = ledger, observe_request, record
        self.pending = {}

    async def __call__(self, task):
        started = time.monotonic_ns()
        body = task.task.payload
        digest = hashlib.sha256(body).hexdigest()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        queue = self.pending.setdefault(loop, [])
        queue.append((future, task, body, digest))
        if len(queue) == 1:
            loop.call_soon(self._flush, loop)
        cancelled = False
        while True:
            try:
                attempt = await asyncio.shield(future)
                break
            except asyncio.CancelledError:
                cancelled = True
                if future.cancelled():
                    raise
        if cancelled:
            raise asyncio.CancelledError()
        self.record(dict(event='remote_request_guard',
                         key=dict(session_id=task.key.session_id, sequence=task.key.sequence),
                         attempt=attempt, status='completed', elapsed_ns=time.monotonic_ns()-started,
                         accounting_mode='batched'))

    def _flush(self, loop):
        queue = self.pending[loop]
        batch = queue[:16]
        del queue[:16]
        if queue:
            loop.call_soon(self._flush, loop)
        else:
            del self.pending[loop]
        started = time.monotonic_ns()
        try:
            attempts = self.ledger.reserve_many([row[3] for row in batch])
            committed = time.monotonic_ns()
            for attempt, (_, _, body, _) in zip(attempts, batch):
                self.observe_request(attempt, body)
            self.record(dict(event='remote_request_batch', rows=len(batch),
                             first_attempt=attempts[0], last_attempt=attempts[-1],
                             reserve_ns=committed-started, elapsed_ns=time.monotonic_ns()-started))
        except Exception as error:
            for future, *_ in batch:
                future.set_exception(error)
        else:
            for attempt, (future, *_) in zip(attempts, batch):
                future.set_result(attempt)
