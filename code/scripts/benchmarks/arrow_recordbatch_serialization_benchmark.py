import argparse

from common import now_seconds, print_table, require_module, write_csv


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark Arrow RecordBatch IPC serialization.")
    parser.add_argument("--rows", type=int, nargs="+", default=[1000, 10000, 100000])
    parser.add_argument("--cols", type=int, nargs="+", default=[4, 16])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", default=None)
    return parser.parse_args()


def make_batch(row_count: int, col_count: int):
    import numpy as np
    import pyarrow as pa

    arrays = [
        pa.array(np.arange(row_count, dtype=np.float64) + col_id)
        for col_id in range(col_count)
    ]
    names = [f"c{col_id}" for col_id in range(col_count)]
    return pa.record_batch(arrays, names=names)


def serialize_ipc(batch):
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue()


def deserialize_ipc(buffer):
    import pyarrow as pa

    reader = pa.ipc.open_stream(buffer)
    return reader.read_next_batch()


def main():
    args = parse_args()
    require_module("numpy")
    require_module("pyarrow")

    rows = []
    for row_count in args.rows:
        for col_count in args.cols:
            batch = make_batch(row_count, col_count)
            for repeat in range(args.repeats):
                start = now_seconds()
                buffer = serialize_ipc(batch)
                serialize_s = now_seconds() - start

                start = now_seconds()
                restored = deserialize_ipc(buffer)
                deserialize_s = now_seconds() - start

                size_mb = len(buffer) / 1024 / 1024
                rows.append(
                    {
                        "benchmark": "arrow_recordbatch_serialization",
                        "rows": row_count,
                        "cols": col_count,
                        "repeat": repeat,
                        "ipc_size_mb": size_mb,
                        "serialize_ms": serialize_s * 1000,
                        "deserialize_ms": deserialize_s * 1000,
                        "serialize_mb_s": size_mb / serialize_s if serialize_s > 0 else 0.0,
                        "deserialize_mb_s": size_mb / deserialize_s if deserialize_s > 0 else 0.0,
                        "restored_rows": restored.num_rows,
                    }
                )

    print_table(rows)
    write_csv(args.output, rows)


if __name__ == "__main__":
    main()

