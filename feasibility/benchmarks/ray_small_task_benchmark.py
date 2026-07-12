import argparse

from common import now_seconds, print_table, require_module, summarize, write_csv


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark Ray small task overhead.")
    parser.add_argument("--tasks", type=int, nargs="+", default=[100, 1000, 10000])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--num-cpus", type=int, default=None)
    parser.add_argument("--compute", choices=["empty", "numpy"], default="empty")
    parser.add_argument("--output", default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    require_module("ray")
    if args.compute == "numpy":
        require_module("numpy")

    import ray

    ray.init(num_cpus=args.num_cpus, ignore_reinit_error=True)

    if args.compute == "numpy":
        import numpy as np

        @ray.remote
        def task():
            return float(np.arange(128, dtype=np.float64).sum())
    else:

        @ray.remote
        def task():
            return 1

    rows = []
    try:
        for task_count in args.tasks:
            durations = []
            for repeat in range(args.repeats):
                start = now_seconds()
                refs = [task.remote() for _ in range(task_count)]
                ray.get(refs)
                duration = now_seconds() - start
                durations.append(duration)
                rows.append(
                    {
                        "benchmark": "ray_small_task",
                        "compute": args.compute,
                        "tasks": task_count,
                        "repeat": repeat,
                        "total_s": duration,
                        "avg_task_ms": duration / task_count * 1000,
                        "tasks_per_s": task_count / duration if duration > 0 else 0.0,
                    }
                )

            summary = summarize(durations)
            rows.append(
                {
                    "benchmark": "ray_small_task_summary",
                    "compute": args.compute,
                    "tasks": task_count,
                    "repeat": "summary",
                    "total_s": summary["mean"],
                    "avg_task_ms": summary["mean"] / task_count * 1000,
                    "tasks_per_s": task_count / summary["mean"] if summary["mean"] > 0 else 0.0,
                }
            )
    finally:
        ray.shutdown()

    print_table(rows)
    write_csv(args.output, rows)


if __name__ == "__main__":
    main()
