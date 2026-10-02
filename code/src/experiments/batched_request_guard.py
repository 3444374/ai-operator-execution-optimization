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
        event = dict(event='remote_request_guard',
                     key=dict(session_id=task.key.session_id, sequence=task.key.sequence),
                     attempt=None, status='failed', accounting_mode='batched')
        queue = self.pending.setdefault(loop, [])
        queue.append((future, body, digest, event))
        if len(queue) == 1:
            loop.call_soon(self._flush, loop)
        cancelled = False
        try:
            while True:
                try:
                    await asyncio.shield(future)
                    break
                except asyncio.CancelledError:
                    cancelled = True
                    if future.cancelled():
                        raise
                except Exception:
                    if cancelled:
                        raise asyncio.CancelledError() from None
                    raise
            if cancelled:
                raise asyncio.CancelledError()
            event['status'] = 'completed'
        finally:
            event['elapsed_ns'] = time.monotonic_ns()-started
            if cancelled:
                event['status'] = 'cancelled'
            try:
                self.record(event)
            except Exception:
                if event['status'] == 'completed':
                    raise

    def _flush(self, loop):
        queue = self.pending[loop]
        batch = queue[:16]
        del queue[:16]
        if queue:
            loop.call_soon(self._flush, loop)
        else:
            del self.pending[loop]
        started = time.monotonic_ns()
        attempts = committed = error = None
        stage = 'reserve'
        try:
            attempts = self.ledger.reserve_many([row[2] for row in batch])
            committed = time.monotonic_ns()
            for attempt, (_, _, _, event) in zip(attempts, batch):
                event['attempt'] = attempt
            stage = 'request_observe'
            for attempt, (_, body, _, _) in zip(attempts, batch):
                self.observe_request(attempt, body)
        except Exception as failure:
            error = failure
        try:
            self.record(dict(event='remote_request_batch', rows=len(batch),
                             first_attempt=attempts[0] if attempts is not None else None,
                             last_attempt=attempts[-1] if attempts is not None else None,
                             reserve_ns=committed-started if committed is not None else None,
                             elapsed_ns=time.monotonic_ns()-started,
                             status='failed' if error is not None else 'completed', stage=stage))
        except Exception as failure:
            if error is None:
                error = failure
        if error is not None:
            for future, *_ in batch:
                future.set_exception(error)
        else:
            for attempt, (future, *_) in zip(attempts, batch):
                future.set_result(attempt)
