"""Bounded raw reads, construction, ordering and failure cleanup before real PG."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
import unittest

from src.experiments.postgresql.query_inputs import QueryInputs
from src.experiments.postgresql.pg_source_direct import PgSourceDirectMap


class Source:
    autocommit = True
    closed = False
    info = SimpleNamespace(transaction_status=0)

    def __init__(self, rows):
        self.rows = rows
        self.read = 0
        self.cursor_closed = self.transaction_closed = self.stream_closed = False
        self.statements = []

    @asynccontextmanager
    async def transaction(self):
        try:
            yield
        finally:
            self.transaction_closed = True

    async def execute(self, statement):
        self.statements.append(statement)

    @asynccontextmanager
    async def cursor(self):
        try:
            yield self
        finally:
            self.cursor_closed = True

    async def stream(self, statement, parameters, *, size):
        assert size == 1
        try:
            for row in self.rows:
                self.read += 1
                yield row
        finally:
            self.stream_closed = True


class Direct(PgSourceDirectMap):
    def __init__(self):
        self.concurrency = 2
        self.calls, self.finished = [], []
        self.first = asyncio.Event()

    async def _one(self, sequence, row, session):
        self.calls.append(row['input_text'])
        try:
            if sequence == 0:
                await self.first.wait()
            return row['source_example_id'], row['input_text']
        finally:
            self.finished.append(sequence)


class PgSourceDirectTests(unittest.IsolatedAsyncioTestCase):
    def inputs(self, maximum=10):
        return QueryInputs('movie', 'reviews', maximum)

    def source(self):
        return Source([(i, str(i), 'film', 'review'+str(i)) for i in range(6)])

    async def test_completed_results_keep_window_while_input_head_is_slow(self):
        source, direct = self.source(), Direct()
        async with direct.query(source, self.inputs(), 1) as rows:
            head = asyncio.create_task(anext(rows))
            for _ in range(10):
                await asyncio.sleep(0)
            self.assertEqual(source.read, 2)
            self.assertEqual(direct.finished, [1])
            direct.first.set()
            self.assertEqual(await head, ('0', 'review0'))
            result = [row async for row in rows]
            self.assertEqual([row[0] for row in result], ['1', '2', '3', '4', '5'])
        self.assertTrue(source.cursor_closed and source.transaction_closed and source.stream_closed)
        self.assertEqual(direct.source_metrics['peak_pending'], 2)
        self.assertEqual(source.statements, ['SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'])

    async def test_completion_order_and_early_close_stop_source_and_tasks(self):
        source, direct = self.source(), Direct()
        async with direct.query(source, self.inputs(), 2, result_order='completion') as rows:
            self.assertEqual(await anext(rows), ('1', 'review1'))
        self.assertEqual(source.read, 2)
        self.assertCountEqual(direct.finished, [0, 1])
        self.assertTrue(source.stream_closed and source.transaction_closed)

    async def test_bad_raw_value_cannot_start_http_and_cleans_prior_tasks(self):
        source, direct = self.source(), Direct()
        source.rows[1] = (1, '1', 'film', None)
        with self.assertRaisesRegex(ValueError, 'representation'):
            async with direct.query(source, self.inputs(), 3) as rows:
                await anext(rows)
        self.assertNotIn(None, direct.calls)
        self.assertEqual(source.read, 2)
        self.assertTrue(source.cursor_closed and source.transaction_closed)

    async def test_row_limit_is_error_without_silent_truncation(self):
        source, direct = self.source(), Direct()
        direct.first.set()
        with self.assertRaisesRegex(ValueError, 'more rows'):
            async with direct.query(source, self.inputs(2), 4) as rows:
                _ = [row async for row in rows]
        self.assertEqual(source.read, 3)
        self.assertEqual(len(direct.calls), 2)

    async def test_empty_source_has_no_tasks(self):
        source, direct = Source([]), Direct()
        async with direct.query(source, self.inputs(), 5) as rows:
            self.assertEqual([row async for row in rows], [])
        self.assertEqual(direct.calls, [])

    def test_squad_uses_raw_columns_and_rejects_byte_overflow(self):
        inputs = QueryInputs('squad', 'questions', 4, max_source_bytes=30, max_input_bytes=80)
        value = inputs.convert((0, 'id', '中文', 'Q?'))
        self.assertEqual(value['input_text'], 'Context:\n中文\n\nQuestion:\nQ?\n\nAnswer:\n')
        with self.assertRaisesRegex(ValueError, 'byte limit'):
            inputs.convert((1, 'id', '中'*10, 'Q?'))
        with self.assertRaises(ValueError):
            QueryInputs('movie', 'reviews; DROP TABLE reviews', 1)
        self.assertNotIn('scoreSentiment', self.inputs().select_sql()[0])
