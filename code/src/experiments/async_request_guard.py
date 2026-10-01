"""One owned I/O thread for durable pre-dispatch accounting; observation stays on the loop."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib
import time


class ThreadedRequestGuard:
    """The calling transport bounds pending guards by its active task capacity.

    Cancellation waits for the queued transaction to settle and never refunds it.
    Close drains the thread before the experiment's event writers are closed.
    """
    def __init__(self, ledger, observe_request, record):
        self.ledger, self.observe_request, self.record = ledger, observe_request, record
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='semloom-budget')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.pool.shutdown(wait=True)

    async def __call__(self, task):
        started = time.monotonic_ns()
        body = task.task.payload
        digest = hashlib.sha256(body).hexdigest()
        hashed = time.monotonic_ns()
        times = []
        resumed = observed = attempt = None
        cancelled, status = False, 'failed'
        def reserve():
            times.append(time.monotonic_ns())
            try:
                return self.ledger.reserve(digest)
            finally:
                times.append(time.monotonic_ns())
        submitted = time.monotonic_ns()
        future = asyncio.get_running_loop().run_in_executor(self.pool, reserve)
        try:
            while True:
                try:
                    attempt = await asyncio.shield(future)
                    break
                except asyncio.CancelledError:
                    cancelled = True
                    if future.cancelled():
                        raise
            resumed = time.monotonic_ns()
            if cancelled:
                raise asyncio.CancelledError()
            self.observe_request(attempt, body)
            observed = time.monotonic_ns()
            status = 'completed'
        finally:
            ended = time.monotonic_ns()
            if resumed is None:
                resumed = ended
            self.record(dict(event='remote_request_guard',
                key=dict(session_id=task.key.session_id, sequence=task.key.sequence),
                attempt=attempt, status='cancelled' if cancelled else status,
                hash_ns=hashed-started,
                reserve_queue_ns=times[0]-submitted if times else None,
                reserve_ns=times[1]-times[0] if len(times)==2 else None,
                reserve_resume_ns=resumed-times[1] if len(times)==2 else None,
                request_observe_ns=observed-resumed if observed is not None else None,
                elapsed_ns=ended-started))
