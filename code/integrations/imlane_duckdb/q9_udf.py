"""Q9 Arrow workload kernel; author IMLane owns projection scheduling."""
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

import pyarrow as pa

from integrations.imlane_duckdb.q3_udf import _configuration
from src.infrastructure.private_artifacts import content_digest, publish_private_json


def input_rows(table):
    """Original Q9 four-string Arrow boundary, independent of Q3's movie filter."""
    if not isinstance(table, pa.Table):
        raise TypeError('Q9 requires a pyarrow.Table')
    table.validate(full=True)
    if (table.column_names != ['c0', 'c1', 'c2', 'c3']
            or any(field.type != pa.string() for field in table.schema)
            or any(column.null_count for column in table.columns)):
        raise ValueError('Q9 requires four non-NULL UTF8 columns')
    rows = [dict(zip(('occurrence_id', 'id', 'reviewId', 'reviewText'), values, strict=True))
            for values in zip(*(column.to_pylist() for column in table.columns), strict=True)]
    ids = [row['occurrence_id'] for row in rows]
    if (any(not value for value in ids) or len(set(ids)) != len(ids)
            or any(row['id'] != 'ant_man_and_the_wasp_quantumania' for row in rows)):
        raise ValueError('Q9 requires unique occurrences of the selected movie')
    return rows


def prepared_messages(config_path):
    """Read the original occurrence-keyed envelope with its frozen byte digest."""
    config = json.loads(Path(config_path).read_text())
    raw = Path(config['messages_path']).read_bytes()
    if hashlib.sha256(raw).hexdigest() != config['messages_sha256']:
        raise ValueError('Q9 prepared messages changed')
    return json.loads(raw)['rows']


def numeric_score(text):
    # Exact original SemBench Movie Q9 numeric sink; CPU compared against its execution.
    try:
        score = float(text)
        if score < 1 or score > 5:
            score = 3
    except (ValueError, TypeError):
        score = 3
    return float(score)


def infer_q9(table):
    rows = input_rows(table)
    config_path = str(Path(os.environ['SEMLOOM_IMLANE_Q9_CONFIG']).resolve())
    provider, directory, mode, provider_digest = _configuration(config_path)
    prepared = prepared_messages(config_path)
    messages = []
    for row in rows:
        original = prepared[row['occurrence_id']]
        assert all(row[key] == original[key] for key in ('occurrence_id', 'id', 'reviewId', 'reviewText'))
        messages.append(original['messages'])
    call = dict(logical_rows=len(rows), status='unknown', started_monotonic_ns=time.monotonic_ns())
    sources = [dict(occurrence_id=row['occurrence_id'], id=row['id'], reviewId=row['reviewId'],
                    messages_sha256=content_digest(message))
               for row, message in zip(rows, messages, strict=True)]
    receipt = dict(schema='semloom.imlane.duckdb.q9.batch.v1', status='unknown',
        provider_mode=mode, worker_pid=os.getpid(), provider_source_sha256=provider_digest,
        logical_rows=len(rows), provider_calls=[call], sources=sources, predictions=[])
    destination = directory / ('q9-batch-' + uuid.uuid4().hex + '.json')
    try:
        replies = provider(messages)
        call.update(returned=True, finished_monotonic_ns=time.monotonic_ns())
        assert isinstance(replies, list) and len(replies) == len(rows) and all(isinstance(x, str) for x in replies)
        scores = [numeric_score(reply) for reply in replies]
        receipt['predictions'] = [dict(source, generated_text=reply, score=score)
            for source, reply, score in zip(sources, replies, scores, strict=True)]
        output = pa.table({'score': pa.array(scores, type=pa.float64())})
        output.validate(full=True)
        call['status'] = receipt['status'] = 'succeeded'
    except BaseException as error:
        receipt.update(status='failed', error_type=type(error).__name__)
        call.setdefault('finished_monotonic_ns', time.monotonic_ns())
        publish_private_json(destination, receipt)
        raise
    publish_private_json(destination, receipt)
    return output
