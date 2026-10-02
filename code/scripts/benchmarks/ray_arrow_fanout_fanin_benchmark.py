import argparse

from common import now_seconds, print_table, require_module, write_csv


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark Ray fan-in with Arrow RecordBatch objects.")
    parser.add_argument("--upstream", type=int, nargs="+", default=[8, 32])
    parser.add_argument("--downstream", type=int, nargs="+", default=[8, 32])
    parser.add_argument("--total-rows", type=int, default=65536)
    parser.add_argument("--embedding-dim", type=int, default=128)
    parser.add_argument("--strategies", nargs="+", choices=["fine", "coalesced"], default=["fine", "coalesced"])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--num-cpus", type=int, default=None)
    parser.add_argument("--output", default=None)
    return parser.parse_args()


def make_record_batch(row_count: int, start_doc_id: int, bucket_id: int, embedding_dim: int):
    import numpy as np
    import pyarrow as pa

    doc_ids = np.arange(start_doc_id, start_doc_id + row_count, dtype=np.int64)
    tenant_ids = (doc_ids % 16).astype(np.int32)
    bucket_ids = np.full(row_count, bucket_id, dtype=np.int32)
    values = np.resize(np.arange(embedding_dim, dtype=np.float32), row_count * embedding_dim)
    embeddings = pa.FixedSizeListArray.from_arrays(pa.array(values), embedding_dim)
    return pa.record_batch(
        [
            pa.array(doc_ids),
            pa.array(tenant_ids),
            pa.array(bucket_ids),
            embeddings,
        ],
        names=["doc_id", "tenant_id", "bucket_id", "embedding"],
    )


def split_counts(total: int, parts: int) -> list[int]:
    base = total // parts
    remainder = total % parts
    return [base + (1 if index < remainder else 0) for index in range(parts)]


def build_objects(strategy: str, upstream_count: int, downstream_count: int, total_rows: int, embedding_dim: int):
    batches_by_downstream = [[] for _ in range(downstream_count)]
    if strategy == "fine":
        object_count = upstream_count * downstream_count
        row_counts = split_counts(total_rows, object_count)
        doc_id = 0
        object_index = 0
        for _ in range(upstream_count):
            for downstream_id in range(downstream_count):
                row_count = row_counts[object_index]
                batch = make_record_batch(row_count, doc_id, downstream_id, embedding_dim)
                batches_by_downstream[downstream_id].append(batch)
                doc_id += row_count
                object_index += 1
        return batches_by_downstream

    row_counts = split_counts(total_rows, downstream_count)
    doc_id = 0
    for downstream_id in range(downstream_count):
        row_count = row_counts[downstream_id]
        batch = make_record_batch(row_count, doc_id, downstream_id, embedding_dim)
        batches_by_downstream[downstream_id].append(batch)
        doc_id += row_count
    return batches_by_downstream


def main():
    args = parse_args()
    require_module("ray")
    require_module("numpy")
    require_module("pyarrow")

    import ray

    @ray.remote
    def consume_batches(*batches):
        return sum(batch.num_rows for batch in batches)

    ray.init(num_cpus=args.num_cpus, ignore_reinit_error=True)
    rows = []
    try:
        for upstream_count in args.upstream:
            for downstream_count in args.downstream:
                for strategy in args.strategies:
                    for repeat in range(args.repeats):
                        batches_by_downstream = build_objects(
                            strategy,
                            upstream_count,
                            downstream_count,
                            args.total_rows,
                            args.embedding_dim,
                        )

                        start = now_seconds()
                        refs_by_downstream = [
                            [ray.put(batch) for batch in batches]
                            for batches in batches_by_downstream
                        ]
                        put_s = now_seconds() - start

                        start = now_seconds()
                        reducer_refs = [
                            consume_batches.remote(*refs)
                            for refs in refs_by_downstream
                        ]
                        reduced_rows = sum(ray.get(reducer_refs))
                        fanin_s = now_seconds() - start

                        object_count = sum(len(refs) for refs in refs_by_downstream)
                        rows.append(
                            {
                                "benchmark": "ray_arrow_fanout_fanin",
                                "strategy": strategy,
                                "upstream": upstream_count,
                                "downstream": downstream_count,
                                "total_rows": args.total_rows,
                                "embedding_dim": args.embedding_dim,
                                "repeat": repeat,
                                "objects": object_count,
                                "avg_rows_per_object": reduced_rows / object_count if object_count else 0.0,
                                "put_all_ms": put_s * 1000,
                                "fanin_ms": fanin_s * 1000,
                                "reduced_rows": reduced_rows,
                                "rows_per_s": reduced_rows / fanin_s if fanin_s > 0 else 0.0,
                            }
                        )
    finally:
        ray.shutdown()

    print_table(rows)
    write_csv(args.output, rows)


if __name__ == "__main__":
    main()
