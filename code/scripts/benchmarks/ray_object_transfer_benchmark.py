import argparse
import os

from common import now_seconds, print_table, require_module, write_csv


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark Ray object transfer overhead.")
    parser.add_argument("--sizes-kb", type=int, nargs="+", default=[1, 10, 100, 1024, 10240])
    parser.add_argument("--types", nargs="+", choices=["bytes", "numpy", "arrow"], default=["bytes", "numpy", "arrow"])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--num-cpus", type=int, default=None)
    parser.add_argument("--output", default=None)
    return parser.parse_args()


def make_object(object_type: str, size_bytes: int):
    if object_type == "bytes":
        return os.urandom(size_bytes)
    if object_type == "numpy":
        import numpy as np

        count = max(1, size_bytes // 8)
        return np.arange(count, dtype=np.float64)
    if object_type == "arrow":
        import numpy as np
        import pyarrow as pa

        count = max(1, size_bytes // 8)
        return pa.record_batch([pa.array(np.arange(count, dtype=np.float64))], names=["value"])
    raise ValueError(f"Unsupported object type: {object_type}")


def main():
    args = parse_args()
    require_module("ray")
    for object_type in args.types:
        if object_type == "numpy":
            require_module("numpy")
        if object_type == "arrow":
            require_module("numpy")
            require_module("pyarrow")

    import ray

    @ray.remote
    def identity(value):
        return value

    ray.init(num_cpus=args.num_cpus, ignore_reinit_error=True)
    rows = []
    try:
        for object_type in args.types:
            for size_kb in args.sizes_kb:
                size_bytes = size_kb * 1024
                value = make_object(object_type, size_bytes)
                for repeat in range(args.repeats):
                    start = now_seconds()
                    ref = ray.put(value)
                    put_s = now_seconds() - start

                    start = now_seconds()
                    ray.get(ref)
                    get_s = now_seconds() - start

                    start = now_seconds()
                    ray.get(identity.remote(ref))
                    roundtrip_s = now_seconds() - start

                    rows.append(
                        {
                            "benchmark": "ray_object_transfer",
                            "object_type": object_type,
                            "size_kb": size_kb,
                            "repeat": repeat,
                            "put_ms": put_s * 1000,
                            "get_ms": get_s * 1000,
                            "roundtrip_ms": roundtrip_s * 1000,
                            "roundtrip_mb_s": size_bytes / 1024 / 1024 / roundtrip_s if roundtrip_s > 0 else 0.0,
                        }
                    )
    finally:
        ray.shutdown()

    print_table(rows)
    write_csv(args.output, rows)


if __name__ == "__main__":
    main()
