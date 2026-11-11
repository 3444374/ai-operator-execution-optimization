"""Original MMQA Filter parser inside the author's native Arrow prediction.

SQL retains the original label/text and an occurrence ID. Prepared messages are
bound to that occurrence; native batching may reorder batches without changing
which prompt or answer belongs to a source. No model owner or scheduler here.
"""

from functools import cache
import json
import os
from pathlib import Path

import pyarrow as pa

from integrations.imlane_duckdb import model_provider
from integrations.imlane_duckdb.q3_udf import _output
from src.baselines.text.sembench_movie_native import q3_decision
from src.infrastructure.private_artifacts import read_json

CONFIG_ENV = 'SEMLOOM_IMLANE_MMQA_FILTER_MESSAGES'


@cache
def prepared(path):
    values = read_json(Path(path))['rows']
    rows = {row['occurrence_id']: row for row in values}
    if len(rows) != len(values):
        raise ValueError('native Filter occurrence identities must be unique')
    return rows


def infer_mmqa(table):
    table.validate(full=True)
    if (table.column_names != ['c0', 'c1', 'c2']
            or any(field.type != pa.string() for field in table.schema)
            or any(column.null_count for column in table.columns)):
        raise ValueError('native Filter requires occurrence_id, label and text as non-NULL UTF8')
    if not table.num_rows:
        return _output([])
    rows = prepared(os.environ[CONFIG_ENV])
    inputs = table.to_pydict()
    if len(set(inputs['c0'])) != table.num_rows:
        raise ValueError('repeated native Filter occurrence')
    messages = []
    for occurrence, label, text in zip(inputs['c0'], inputs['c1'], inputs['c2'], strict=True):
        row = rows[occurrence]
        if (label, text) != (row['title'], row['text']):
            raise ValueError('native Filter source differs from its prepared occurrence')
        messages.append(row['messages'])
    replies = model_provider.infer_batch(messages)
    if (type(replies) is not list or len(replies) != table.num_rows
            or any(type(text) is not str for text in replies)):
        raise ValueError('native model changed Filter cardinality or output type')
    # Author's numeric converter expects int8, not Arrow's bit-packed bool.
    # This is the same original true-before-false/default-true parser used by
    # the already qualified TINYINT MMQA path; it is not an alternate encoding.
    return _output([int(q3_decision(text)) for text in replies])
