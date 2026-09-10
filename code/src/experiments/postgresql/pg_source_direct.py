"""Stream raw PG rows into a bounded diagnostic HTTP client after query release.

The supplied connection belongs to this query. Its read-only repeatable-read
transaction and cursor are closed after HTTP tasks settle. Input-order mode
counts completed, unconsumed results against the same fixed task window.
"""
import asyncio
from contextlib import asynccontextmanager
import time

from .map_direct import DirectMap
from .map_query_recording import query_failure_scope


class PgSourceDirectMap(DirectMap):
    @asynccontextmanager
    async def query(self, connection, inputs, session, *, result_order='input'):
        if result_order not in ('input', 'completion'):
            raise ValueError('unsupported result order')
        if not connection.autocommit or connection.closed or int(connection.info.transaction_status) != 0:
            raise ValueError('source requires a dedicated idle autocommit connection')
        pending = {}
        next_sequence = read_rows = 0
        statement, parameters = inputs.select_sql()
        self.source_metrics = dict(rows=0, fetch_ns=0, construction_ns=0, peak_pending=0,
            result_order=result_order, snapshot='repeatable read, read only',
            source_row_limit=inputs.max_source_bytes, input_limit=inputs.max_input_bytes,
            task_window=self.concurrency)

        async def results():
            nonlocal next_sequence, read_rows
            # Setup is part of iteration, so the recorder sees query failures.
            async with connection.transaction():
                with query_failure_scope():
                    await connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                async with connection.cursor() as cursor:
                    source = cursor.stream(statement, parameters, size=1)
                    reading = None
                    exhausted = False
                    read_started = None
                    try:
                        with query_failure_scope():
                            while pending or not exhausted:
                                if not exhausted and reading is None and len(pending) < self.concurrency:
                                    read_started = time.monotonic_ns()
                                    reading = asyncio.create_task(anext(source, None))
                                    self.source_metrics['peak_pending'] = max(
                                        self.source_metrics['peak_pending'], len(pending) + 1)
                                deliverable = ([pending[next_sequence]] if pending and result_order == 'input'
                                               else list(pending.values()))
                                waiting = deliverable + ([reading] if reading is not None else [])
                                done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
                                if reading in done:
                                    raw = reading.result()
                                    reading = None
                                    self.source_metrics['fetch_ns'] += time.monotonic_ns() - read_started
                                    if raw is None:
                                        exhausted = True
                                    else:
                                        read_rows += 1
                                        if read_rows > inputs.max_rows:
                                            raise ValueError('source has more rows than declared')
                                        before = time.monotonic_ns()
                                        row = inputs.convert(raw)
                                        self.source_metrics['construction_ns'] += time.monotonic_ns() - before
                                        sequence = read_rows - 1
                                        pending[sequence] = asyncio.create_task(self._one(sequence, row, session))
                                        self.source_metrics['rows'] = read_rows
                                for sequence, task in tuple(pending.items()):
                                    if task in done and (result_order == 'completion' or sequence == next_sequence):
                                        # Retain completed responsibility while consumer is suspended at yield.
                                        yield task.result()
                                        del pending[sequence]
                                        if result_order == 'input':
                                            next_sequence += 1
                    finally:
                        tasks = list(pending.values()) + ([reading] if reading is not None else [])
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                        pending.clear()
                        await source.aclose()
        stream = results()
        try:
            yield stream
        finally:
            await stream.aclose()
