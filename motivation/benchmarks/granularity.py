import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
BENCHMARK_DIR = ROOT_DIR / "validation" / "benchmarks"
if str(BENCHMARK_DIR) not in sys.path:
    sys.path.insert(0, str(BENCHMARK_DIR))

from common import now_seconds, print_table, require_module, write_csv


STRATEGIES = ("fine", "two_stage_coalesced", "downstream_coalesced", "upstream_bundled")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Attribute AI operator motivation gains to task count, object count, and fan-in dependencies."
    )
    parser.add_argument("--upstream", type=int, nargs="+", default=[8, 32])
    parser.add_argument("--downstream", type=int, nargs="+", default=[8, 32])
    parser.add_argument("--total-rows", type=int, default=65536)
    parser.add_argument("--payload-bytes-per-row", type=int, default=512)
    parser.add_argument("--compute-us-per-row", type=float, default=0.25)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--num-cpus", type=int, default=None)
    parser.add_argument("--output", default="motivation/results/fake_cpu/granularity.csv")
    return parser.parse_args()


def split_counts(total: int, parts: int) -> list[int]:
    base = total // parts
    remainder = total % parts
    return [base + (1 if index < remainder else 0) for index in range(parts)]


def summarize_payloads(payloads: list[dict]) -> dict:
    return {
        "rows": sum(item["rows"] for item in payloads),
        "bytes": sum(item["bytes"] for item in payloads),
        "logical_objects": sum(item.get("logical_objects", 1) for item in payloads),
    }


def main():
    args = parse_args()
    require_module("ray")
    import ray

    @ray.remote
    def ai_operator(rows: int, downstream_id: int, payload_bytes_per_row: int, compute_us_per_row: float):
        import time

        start = time.perf_counter()
        if compute_us_per_row > 0:
            time.sleep(rows * compute_us_per_row / 1_000_000.0)
        return {
            "downstream_id": downstream_id,
            "rows": rows,
            "bytes": rows * payload_bytes_per_row,
            "logical_objects": 1,
            "operator_ms": (time.perf_counter() - start) * 1000.0,
        }

    @ray.remote
    def ai_operator_upstream_bundle(
        row_counts: list[int],
        payload_bytes_per_row: int,
        compute_us_per_row: float,
    ):
        import time

        start = time.perf_counter()
        rows = sum(row_counts)
        if compute_us_per_row > 0:
            time.sleep(rows * compute_us_per_row / 1_000_000.0)
        payloads = [
            {
                "downstream_id": downstream_id,
                "rows": row_count,
                "bytes": row_count * payload_bytes_per_row,
                "logical_objects": 1,
            }
            for downstream_id, row_count in enumerate(row_counts)
        ]
        return {
            "payloads": payloads,
            "rows": rows,
            "bytes": rows * payload_bytes_per_row,
            "logical_objects": len(payloads),
            "operator_ms": (time.perf_counter() - start) * 1000.0,
        }

    @ray.remote
    def coalesce_payloads(downstream_id: int, *payloads):
        merged = summarize_payloads(list(payloads))
        merged["downstream_id"] = downstream_id
        return merged

    @ray.remote
    def fanin_payloads(downstream_id: int, *payloads):
        import time

        start = time.perf_counter()
        merged = summarize_payloads(list(payloads))
        return {
            "downstream_id": downstream_id,
            "rows": merged["rows"],
            "bytes": merged["bytes"],
            "logical_objects": merged["logical_objects"],
            "input_refs": len(payloads),
            "fanin_ms": (time.perf_counter() - start) * 1000.0,
        }

    @ray.remote
    def fanin_upstream_bundles(downstream_id: int, *bundles):
        import time

        start = time.perf_counter()
        selected = [bundle["payloads"][downstream_id] for bundle in bundles]
        merged = summarize_payloads(selected)
        return {
            "downstream_id": downstream_id,
            "rows": merged["rows"],
            "bytes": merged["bytes"],
            "logical_objects": merged["logical_objects"],
            "input_refs": len(bundles),
            "fanin_ms": (time.perf_counter() - start) * 1000.0,
        }

    ray.init(num_cpus=args.num_cpus, ignore_reinit_error=True)
    rows = []

    try:
        for upstream_count in args.upstream:
            for downstream_count in args.downstream:
                slots = upstream_count * downstream_count
                if args.total_rows < slots:
                    raise SystemExit("total_rows must be at least upstream * downstream")
                slot_rows = split_counts(args.total_rows, slots)

                for strategy in STRATEGIES:
                    for repeat in range(args.repeats):
                        start = now_seconds()
                        operator_start = now_seconds()
                        coalesce_ms = 0.0

                        if strategy == "fine":
                            op_refs_by_downstream = [[] for _ in range(downstream_count)]
                            for index, row_count in enumerate(slot_rows):
                                downstream_id = index % downstream_count
                                ref = ai_operator.remote(
                                    row_count,
                                    downstream_id,
                                    args.payload_bytes_per_row,
                                    args.compute_us_per_row,
                                )
                                op_refs_by_downstream[downstream_id].append(ref)
                            op_refs = [ref for refs in op_refs_by_downstream for ref in refs]
                            ray.get(op_refs)
                            operator_ms = (now_seconds() - operator_start) * 1000.0
                            fanin_refs = [
                                fanin_payloads.remote(downstream_id, *op_refs_by_downstream[downstream_id])
                                for downstream_id in range(downstream_count)
                            ]
                            ai_operator_tasks = slots
                            coalesce_tasks = 0
                            fanin_tasks = downstream_count
                            ray_refs_produced_before_fanin = slots
                            ray_refs_used_by_fanin = slots
                            logical_payloads_used_by_fanin = slots

                        elif strategy == "two_stage_coalesced":
                            op_refs_by_downstream = [[] for _ in range(downstream_count)]
                            for index, row_count in enumerate(slot_rows):
                                downstream_id = index % downstream_count
                                ref = ai_operator.remote(
                                    row_count,
                                    downstream_id,
                                    args.payload_bytes_per_row,
                                    args.compute_us_per_row,
                                )
                                op_refs_by_downstream[downstream_id].append(ref)
                            op_refs = [ref for refs in op_refs_by_downstream for ref in refs]
                            ray.get(op_refs)
                            operator_ms = (now_seconds() - operator_start) * 1000.0
                            coalesce_start = now_seconds()
                            coalesced_refs = [
                                coalesce_payloads.remote(downstream_id, *op_refs_by_downstream[downstream_id])
                                for downstream_id in range(downstream_count)
                            ]
                            ray.get(coalesced_refs)
                            coalesce_ms = (now_seconds() - coalesce_start) * 1000.0
                            fanin_refs = [
                                fanin_payloads.remote(downstream_id, coalesced_refs[downstream_id])
                                for downstream_id in range(downstream_count)
                            ]
                            ai_operator_tasks = slots
                            coalesce_tasks = downstream_count
                            fanin_tasks = downstream_count
                            ray_refs_produced_before_fanin = slots + downstream_count
                            ray_refs_used_by_fanin = downstream_count
                            logical_payloads_used_by_fanin = slots

                        elif strategy == "downstream_coalesced":
                            rows_by_downstream = [
                                sum(slot_rows[index] for index in range(downstream_id, slots, downstream_count))
                                for downstream_id in range(downstream_count)
                            ]
                            op_refs_by_downstream = [
                                [
                                    ai_operator.remote(
                                        row_count,
                                        downstream_id,
                                        args.payload_bytes_per_row,
                                        args.compute_us_per_row,
                                    )
                                ]
                                for downstream_id, row_count in enumerate(rows_by_downstream)
                            ]
                            op_refs = [refs[0] for refs in op_refs_by_downstream]
                            ray.get(op_refs)
                            operator_ms = (now_seconds() - operator_start) * 1000.0
                            fanin_refs = [
                                fanin_payloads.remote(downstream_id, op_refs_by_downstream[downstream_id][0])
                                for downstream_id in range(downstream_count)
                            ]
                            ai_operator_tasks = downstream_count
                            coalesce_tasks = 0
                            fanin_tasks = downstream_count
                            ray_refs_produced_before_fanin = downstream_count
                            ray_refs_used_by_fanin = downstream_count
                            logical_payloads_used_by_fanin = downstream_count

                        else:
                            row_counts_by_upstream = [
                                slot_rows[start_index : start_index + downstream_count]
                                for start_index in range(0, slots, downstream_count)
                            ]
                            bundle_refs = [
                                ai_operator_upstream_bundle.remote(
                                    row_counts,
                                    args.payload_bytes_per_row,
                                    args.compute_us_per_row,
                                )
                                for row_counts in row_counts_by_upstream
                            ]
                            ray.get(bundle_refs)
                            operator_ms = (now_seconds() - operator_start) * 1000.0
                            fanin_refs = [
                                fanin_upstream_bundles.remote(downstream_id, *bundle_refs)
                                for downstream_id in range(downstream_count)
                            ]
                            ai_operator_tasks = upstream_count
                            coalesce_tasks = 0
                            fanin_tasks = downstream_count
                            ray_refs_produced_before_fanin = upstream_count
                            ray_refs_used_by_fanin = upstream_count * downstream_count
                            logical_payloads_used_by_fanin = slots

                        fanin_results = ray.get(fanin_refs)
                        e2e_ms = (now_seconds() - start) * 1000.0
                        fanin_ms = sum(item["fanin_ms"] for item in fanin_results)
                        output_bytes = sum(item["bytes"] for item in fanin_results)

                        rows.append(
                            {
                                "strategy": strategy,
                                "repeat": repeat,
                                "upstream": upstream_count,
                                "downstream": downstream_count,
                                "total_rows": args.total_rows,
                                "payload_bytes_per_row": args.payload_bytes_per_row,
                                "compute_us_per_row": args.compute_us_per_row,
                                "ai_operator_tasks": ai_operator_tasks,
                                "coalesce_tasks": coalesce_tasks,
                                "fanin_tasks": fanin_tasks,
                                "total_ray_tasks": ai_operator_tasks + coalesce_tasks + fanin_tasks,
                                "ray_refs_produced_before_fanin": ray_refs_produced_before_fanin,
                                "ray_refs_used_by_fanin": ray_refs_used_by_fanin,
                                "logical_payloads_used_by_fanin": logical_payloads_used_by_fanin,
                                "operator_ms": operator_ms,
                                "coalesce_ms": coalesce_ms,
                                "fanin_ms": fanin_ms,
                                "e2e_ms": e2e_ms,
                                "rows_per_second": args.total_rows / (e2e_ms / 1000.0),
                                "output_mb": output_bytes / (1024 * 1024),
                            }
                        )
    finally:
        ray.shutdown()

    write_csv(args.output, rows)
    print_table(rows)


if __name__ == "__main__":
    main()
