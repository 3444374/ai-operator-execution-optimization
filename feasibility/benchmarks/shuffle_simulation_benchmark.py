import argparse

from common import now_seconds, print_table, require_module, write_csv


def parse_args():
    parser = argparse.ArgumentParser(description="Simulate hash shuffle with fine/coalesced object strategies.")
    parser.add_argument("--upstream", type=int, nargs="+", default=[8, 32, 128])
    parser.add_argument("--downstream", type=int, nargs="+", default=[8, 32])
    parser.add_argument("--rows-per-partition", type=int, default=10000)
    parser.add_argument("--strategies", nargs="+", choices=["fine", "coalesced"], default=["fine", "coalesced"])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", default=None)
    return parser.parse_args()


def make_partition(rows_per_partition: int, partition_id: int):
    import numpy as np

    keys = np.arange(rows_per_partition, dtype=np.int64) + partition_id * rows_per_partition
    values = keys * 2
    return keys, values


def fine_shuffle(upstream_count: int, downstream_count: int, rows_per_partition: int):
    objects = []
    for upstream_id in range(upstream_count):
        keys, values = make_partition(rows_per_partition, upstream_id)
        bucket_ids = keys % downstream_count
        for downstream_id in range(downstream_count):
            mask = bucket_ids == downstream_id
            objects.append((downstream_id, keys[mask], values[mask]))
    merged_rows = 0
    for downstream_id in range(downstream_count):
        for obj_downstream_id, keys, _values in objects:
            if obj_downstream_id == downstream_id:
                merged_rows += len(keys)
    return len(objects), merged_rows


def coalesced_shuffle(upstream_count: int, downstream_count: int, rows_per_partition: int):
    import numpy as np

    buckets = [[] for _ in range(downstream_count)]
    for upstream_id in range(upstream_count):
        keys, values = make_partition(rows_per_partition, upstream_id)
        bucket_ids = keys % downstream_count
        for downstream_id in range(downstream_count):
            mask = bucket_ids == downstream_id
            buckets[downstream_id].append((keys[mask], values[mask]))

    objects = []
    merged_rows = 0
    for downstream_id, chunks in enumerate(buckets):
        key_chunks = [chunk[0] for chunk in chunks if len(chunk[0]) > 0]
        value_chunks = [chunk[1] for chunk in chunks if len(chunk[1]) > 0]
        if key_chunks:
            keys = np.concatenate(key_chunks)
            values = np.concatenate(value_chunks)
        else:
            keys = np.array([], dtype=np.int64)
            values = np.array([], dtype=np.int64)
        objects.append((downstream_id, keys, values))
        merged_rows += len(keys)
    return len(objects), merged_rows


def main():
    args = parse_args()
    require_module("numpy")

    rows = []
    for upstream_count in args.upstream:
        for downstream_count in args.downstream:
            for strategy in args.strategies:
                for repeat in range(args.repeats):
                    start = now_seconds()
                    if strategy == "fine":
                        object_count, merged_rows = fine_shuffle(
                            upstream_count,
                            downstream_count,
                            args.rows_per_partition,
                        )
                    else:
                        object_count, merged_rows = coalesced_shuffle(
                            upstream_count,
                            downstream_count,
                            args.rows_per_partition,
                        )
                    total_s = now_seconds() - start
                    total_rows = upstream_count * args.rows_per_partition
                    rows.append(
                        {
                            "benchmark": "shuffle_simulation",
                            "strategy": strategy,
                            "upstream": upstream_count,
                            "downstream": downstream_count,
                            "rows_per_partition": args.rows_per_partition,
                            "repeat": repeat,
                            "objects": object_count,
                            "merged_rows": merged_rows,
                            "total_s": total_s,
                            "rows_per_s": total_rows / total_s if total_s > 0 else 0.0,
                        }
                    )

    print_table(rows)
    write_csv(args.output, rows)


if __name__ == "__main__":
    main()

