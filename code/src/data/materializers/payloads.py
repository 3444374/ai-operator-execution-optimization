"""Finite, binary payload batches from an already selected database input window."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PayloadBatchLimits:
    rows: int
    bytes: int

    def __post_init__(self):
        if any(type(value) is not int or value < 1 for value in (self.rows, self.bytes)):
            raise ValueError("payload batch limits must be positive integers")


def iter_payload_batches(rows, limits: PayloadBatchLimits, *, batch_rows: int):
    """Stream Daft partitions of one finite window without collecting its output.

    The caller supplies a sealed tuple, not an unbounded producer. The extra
    24 bytes per row cover two int64 keys and large-binary offsets in the Arrow table.
    This counts Arrow data, not the RSS of Python, Daft or Ray processes.
    """
    if type(rows) is not tuple or not 1 <= len(rows) <= limits.rows:
        raise ValueError("payload window exceeds the declared row count")
    if type(batch_rows) is not int or not 1 <= batch_rows <= limits.rows:
        raise ValueError("invalid output batch row count")
    seen, size = set(), 0
    for session_id, sequence, payload in rows:
        if (type(session_id) is not int or type(sequence) is not int
                or not 0 <= session_id < 2**63 or not 0 <= sequence < 2**63
                or type(payload) is not bytes or (session_id, sequence) in seen):
            raise ValueError("invalid or duplicate payload identity")
        seen.add((session_id, sequence))
        size += len(payload) + 24
    if size > limits.bytes:
        raise ValueError("payload window exceeds the declared byte count")

    import daft
    import pyarrow as pa
    from .text import configure_daft_runner

    configure_daft_runner("native")
    schema = pa.schema([("session_id", pa.int64()), ("sequence", pa.int64()), ("payload", pa.large_binary())])
    table = pa.Table.from_arrays([pa.array(column, type=field.type)
                                 for column, field in zip(zip(*rows), schema)], schema=schema)
    stream = daft.from_arrow(table).into_batches(batch_rows).to_arrow_iter(results_buffer_size=1)
    expected = iter(rows)
    try:
        for batch in stream:
            result = pa.Table.from_batches([batch]) if isinstance(batch, pa.RecordBatch) else batch
            if (result.schema != schema or not 1 <= result.num_rows <= batch_rows
                    or result.nbytes > limits.bytes):
                raise ValueError("Daft changed the payload representation")
            for row in zip(*(result.column(name).to_pylist() for name in schema.names)):
                if next(expected, None) != row:
                    raise ValueError("Daft changed a sealed payload or its identity")
            yield result
        if next(expected, None) is not None:
            raise ValueError("Daft omitted a sealed payload")
    finally:
        stream.close()
