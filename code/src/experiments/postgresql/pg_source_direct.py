"""Stream raw PG rows into a bounded diagnostic HTTP client after query release.

The supplied connection belongs to this query. Its read-only repeatable-read
transaction and cursor are closed after HTTP tasks settle. Input-order mode
counts completed, unconsumed results against the same fixed task window.
"""
import asyncio
from contextlib import asynccontextmanager
import time

from .map_direct import DirectMap


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

        async with connection.transaction():
            await connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            async with connection.cursor() as cursor:
                source = cursor.stream(statement, parameters, size=1)

                async def results():
                    nonlocal next_sequence, read_rows
                    exhausted = False
                    while pending or not exhausted:
                        while not exhausted and len(pending) < self.concurrency:
                            before = time.monotonic_ns()
                            raw = await anext(source, None)
                            self.source_metrics['fetch_ns'] += time.monotonic_ns() - before
                            if raw is None:
                                exhausted = True
                                break
                            read_rows += 1
                            if read_rows > inputs.max_rows:
                                raise ValueError('source has more rows than declared')
                            before = time.monotonic_ns()
                            row = inputs.convert(raw)
                            self.source_metrics['construction_ns'] += time.monotonic_ns() - before
                            sequence = read_rows - 1
                            pending[sequence] = asyncio.create_task(self._one(sequence, row, session))
                            self.source_metrics['rows'] = read_rows
                            self.source_metrics['peak_pending'] = max(self.source_metrics['peak_pending'], len(pending))
                        if not pending:
                            break
                        if result_order == 'input':
                            result = await pending[next_sequence]
                            del pending[next_sequence]
                            next_sequence += 1
                            yield result
                        else:
                            done, _ = await asyncio.wait(pending.values(), return_when=asyncio.FIRST_COMPLETED)
                            for sequence, task in tuple(pending.items()):
                                if task in done:
                                    result = task.result()
                                    del pending[sequence]
                                    yield result
                stream = results()
                try:
                    yield stream
                finally:
                    await stream.aclose()
                    for task in pending.values():
                        task.cancel()
                    await asyncio.gather(*pending.values(), return_exceptions=True)
                    await source.aclose()
