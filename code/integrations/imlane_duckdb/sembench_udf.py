"""Prepared text requests through the author's Arrow/CPython prediction path.

The SQL projection carries call_id alongside the returned VARCHAR. This kernel
only decodes the already prepared message envelope; original output parsing and
the complete SemBench relation remain at the sink.
"""

import json

import pyarrow as pa

from integrations.imlane_duckdb import model_provider


INPUT = pa.schema([('c0', pa.string()), ('c1', pa.string())])
OUTPUT = pa.schema([pa.field('generated_text', pa.string(), nullable=False)])


def infer_prepared(table):
    """Return one unchanged model string per native input occurrence."""
    if not isinstance(table, pa.Table):
        raise TypeError('prepared IMLane prediction requires a pyarrow.Table')
    table.validate(full=True)
    if (table.column_names != INPUT.names
            or any(field.type != INPUT.field(i).type for i, field in enumerate(table.schema))
            or any(column.null_count for column in table.columns)):
        raise ValueError('prepared IMLane prediction requires two non-NULL UTF8 columns')
    call_ids = table.column(0).to_pylist()
    if any(not value for value in call_ids) or len(set(call_ids)) != len(call_ids):
        raise ValueError('prepared IMLane prediction requires unique nonempty occurrence IDs')
    if not call_ids:
        return pa.Table.from_arrays([pa.array([], type=pa.string())], schema=OUTPUT)
    messages = [json.loads(value) for value in table.column(1).to_pylist()]
    texts = model_provider.infer_batch(messages, occurrence_ids=call_ids)
    if (type(texts) is not list or len(texts) != len(call_ids)
            or any(type(text) is not str for text in texts)):
        raise ValueError('native model provider changed occurrence cardinality or text type')
    return pa.Table.from_arrays([pa.array(texts, type=pa.string())], schema=OUTPUT)
