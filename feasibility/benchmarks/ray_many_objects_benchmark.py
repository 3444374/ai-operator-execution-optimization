import argparse
import os

from common import now_seconds, print_table, require_module, write_csv


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark Ray fan-in with many small objects.")
    parser.add_argument("--total-mb", type=int, nargs="+", default=[16])
    parser.add_argument("--objects", type=int, nargs="+", default=[1, 16, 64, 256])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--num-cpus", type=int, default=None)
    parser.add_argument("--output", default=None)
    return parser.parse_args()


def make_payload(size_bytes: int) -> bytes:
    return os.urandom(size_bytes)


def main():
    args = parse_args()
    require_module("ray")

    import ray

    @ray.remote
    def consume(*chunks):
        return sum(len(chunk) for chunk in chunks)

    ray.init(num_cpus=args.num_cpus, ignore_reinit_error=True)
    rows = []
    try:
        for total_mb in args.total_mb:
            total_bytes = total_mb * 1024 * 1024
            for object_count in args.objects:
                object_size = max(1, total_bytes // object_count)
                for repeat in range(args.repeats):
                    start = now_seconds()
                    refs = [ray.put(make_payload(object_size)) for _ in range(object_count)]
                    put_s = now_seconds() - start

                    start = now_seconds()
                    consumed_bytes = ray.get(consume.remote(*refs))
                    fanin_s = now_seconds() - start

                    rows.append(
                        {
                            "benchmark": "ray_many_objects",
                            "total_mb": total_mb,
                            "objects": object_count,
                            "object_kb": object_size / 1024,
                            "repeat": repeat,
                            "put_all_ms": put_s * 1000,
                            "fanin_ms": fanin_s * 1000,
                            "consumed_mb": consumed_bytes / 1024 / 1024,
                            "fanin_mb_s": total_mb / fanin_s if fanin_s > 0 else 0.0,
                        }
                    )
    finally:
        ray.shutdown()

    print_table(rows)
    write_csv(args.output, rows)


if __name__ == "__main__":
    main()
