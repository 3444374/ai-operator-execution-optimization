"""Deterministic Arrow workload for the original IMLane backend executors."""
import asyncio
import os
import pyarrow as pa
import pyarrow.compute as pc


def transform(table):
    if table.column_names != ['id', 'position', 'value', 'text']:
        raise ValueError('fixture input schema differs')
    table.validate(full=True)
    return table.append_column('doubled', pc.multiply(table['value'], 2)).append_column(
        'worker_pid', pa.array([os.getpid()] * table.num_rows, type=pa.int64()))


async def async_transform(table):
    await asyncio.sleep(0.005)
    return transform(table)
