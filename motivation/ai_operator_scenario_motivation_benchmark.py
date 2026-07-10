import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
BENCHMARK_DIR = ROOT_DIR / "validation" / "benchmarks"
if str(BENCHMARK_DIR) not in sys.path:
    sys.path.insert(0, str(BENCHMARK_DIR))

from common import now_seconds, print_table, require_module, write_csv


SCENARIOS = ("embed_rag", "classify_filter", "offline_llm")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare candidate AI operator scenarios on the same Ray/Arrow pipeline."
    )
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=list(SCENARIOS))
    parser.add_argument("--upstream", type=int, nargs="+", default=[8, 32])
    parser.add_argument("--downstream", type=int, nargs="+", default=[8, 32])
    parser.add_argument("--total-rows", type=int, default=65536)
    parser.add_argument("--embedding-dim", type=int, default=128)
    parser.add_argument("--text-tokens", type=int, default=32)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--num-cpus", type=int, default=None)
    parser.add_argument("--output", default="validation/results/ai_operator_scenario_motivation.csv")
    return parser.parse_args()


def split_counts(total: int, parts: int) -> list[int]:
    base = total // parts
    remainder = total % parts
    return [base + (1 if index < remainder else 0) for index in range(parts)]


def make_text(doc_id: int, tenant_id: int, category: str, text_tokens: int) -> str:
    prefix_id = tenant_id % 4
    shared_prefix = f"system_prompt_{prefix_id} classify enterprise record"
    token = f" topic_{doc_id % 97}"
    return f"{shared_prefix} doc {doc_id} tenant {tenant_id} category {category}" + token * text_tokens


def make_document_batch(row_count: int, start_doc_id: int, bucket_id: int, text_tokens: int):
    import numpy as np
    import pyarrow as pa

    doc_ids = np.arange(start_doc_id, start_doc_id + row_count, dtype=np.int64)
    tenant_ids = (doc_ids % 16).astype(np.int32)
    categories = [f"cat_{doc_id % 8}" for doc_id in doc_ids]
    texts = [
        make_text(int(doc_id), int(tenant_id), category, text_tokens)
        for doc_id, tenant_id, category in zip(doc_ids, tenant_ids, categories)
    ]
    bucket_ids = np.full(row_count, bucket_id, dtype=np.int32)

    return pa.record_batch(
        [
            pa.array(doc_ids),
            pa.array(tenant_ids),
            pa.array(categories),
            pa.array(texts),
            pa.array(bucket_ids),
        ],
        names=["doc_id", "tenant_id", "category", "text", "bucket_id"],
    )


def concat_batches(batches: list):
    import pyarrow as pa

    if len(batches) == 1:
        return batches[0]
    table = pa.Table.from_batches(batches)
    return table.combine_chunks().to_batches(max_chunksize=table.num_rows)[0]


def build_batches(strategy: str, upstream_count: int, downstream_count: int, total_rows: int, text_tokens: int):
    batches_by_downstream = [[] for _ in range(downstream_count)]
    object_count = upstream_count * downstream_count
    row_counts = split_counts(total_rows, object_count)
    doc_id = 0
    object_index = 0

    for _ in range(upstream_count):
        for downstream_id in range(downstream_count):
            row_count = row_counts[object_index]
            batch = make_document_batch(row_count, doc_id, downstream_id, text_tokens)
            batches_by_downstream[downstream_id].append(batch)
            doc_id += row_count
            object_index += 1

    if strategy == "fine":
        return batches_by_downstream
    return [[concat_batches(batches)] for batches in batches_by_downstream]


def main():
    args = parse_args()
    require_module("ray")
    require_module("numpy")
    require_module("pyarrow")

    import ray

    @ray.remote
    def run_ai_operator(batch, scenario: str, embedding_dim: int):
        import numpy as np
        import pyarrow as pa

        doc_ids = batch.column("doc_id").to_numpy(zero_copy_only=False)
        tenant_ids = batch.column("tenant_id").to_numpy(zero_copy_only=False)
        bucket_ids = batch.column("bucket_id")
        categories = batch.column("category")
        texts = batch.column("text").to_pylist()
        text_lengths = np.fromiter((len(text) for text in texts), dtype=np.float32, count=batch.num_rows)

        if scenario == "embed_rag":
            offsets = np.arange(embedding_dim, dtype=np.float32)
            base = ((doc_ids % 1009).astype(np.float32) + text_lengths)[:, None]
            embeddings_np = ((base + offsets[None, :]) % 997) / 997.0
            embeddings = pa.FixedSizeListArray.from_arrays(
                pa.array(embeddings_np.astype(np.float32, copy=False).reshape(-1)),
                embedding_dim,
            )
            output = pa.record_batch(
                [pa.array(doc_ids), pa.array(tenant_ids), categories, bucket_ids, embeddings],
                names=["doc_id", "tenant_id", "category", "bucket_id", "embedding"],
            )
        elif scenario == "classify_filter":
            label_ids = (doc_ids % 8).astype(np.int32)
            scores = (((doc_ids % 1009).astype(np.float32) + text_lengths) % 100) / 100.0
            keep = scores >= 0.25
            output = pa.record_batch(
                [
                    pa.array(doc_ids),
                    pa.array(tenant_ids),
                    categories,
                    bucket_ids,
                    pa.array(label_ids),
                    pa.array(scores),
                    pa.array(keep),
                ],
                names=["doc_id", "tenant_id", "category", "bucket_id", "label_id", "score", "keep"],
            )
        elif scenario == "offline_llm":
            prompt_tokens = np.maximum((text_lengths // 5).astype(np.int32), 1)
            completion_tokens = (32 + (doc_ids % 64)).astype(np.int32)
            total_tokens = prompt_tokens + completion_tokens
            prefix_ids = (tenant_ids % 4).astype(np.int32)
            output = pa.record_batch(
                [
                    pa.array(doc_ids),
                    pa.array(tenant_ids),
                    categories,
                    bucket_ids,
                    pa.array(prefix_ids),
                    pa.array(prompt_tokens),
                    pa.array(completion_tokens),
                    pa.array(total_tokens),
                ],
                names=[
                    "doc_id",
                    "tenant_id",
                    "category",
                    "bucket_id",
                    "prefix_id",
                    "prompt_tokens",
                    "completion_tokens",
                    "total_tokens",
                ],
            )
        else:
            raise ValueError(f"Unknown scenario: {scenario}")

        return {
            "batch": output,
            "rows": output.num_rows,
            "bytes": output.nbytes,
        }

    @ray.remote
    def fanin_outputs(downstream_id: int, *payloads):
        import pyarrow as pa

        batches = [payload["batch"] for payload in payloads]
        table = pa.Table.from_batches(batches).combine_chunks()
        return {
            "downstream_id": downstream_id,
            "rows": sum(payload["rows"] for payload in payloads),
            "input_objects": len(payloads),
            "input_bytes": sum(payload["bytes"] for payload in payloads),
            "output_bytes": table.nbytes,
        }

    ray.init(num_cpus=args.num_cpus, ignore_reinit_error=True)
    rows = []

    try:
        for scenario in args.scenarios:
            for upstream_count in args.upstream:
                for downstream_count in args.downstream:
                    if args.total_rows < upstream_count * downstream_count:
                        raise SystemExit(
                            "total_rows must be at least upstream * downstream "
                            f"for this benchmark; got total_rows={args.total_rows}, "
                            f"upstream={upstream_count}, downstream={downstream_count}."
                        )
                    for strategy in ("fine", "coalesced"):
                        for repeat in range(args.repeats):
                            start = now_seconds()
                            batches_by_downstream = build_batches(
                                strategy,
                                upstream_count,
                                downstream_count,
                                args.total_rows,
                                args.text_tokens,
                            )
                            build_s = now_seconds() - start

                            input_object_count = sum(len(batches) for batches in batches_by_downstream)
                            input_bytes = sum(
                                batch.nbytes
                                for batches in batches_by_downstream
                                for batch in batches
                            )

                            start = now_seconds()
                            refs_by_downstream = [
                                [ray.put(batch) for batch in batches]
                                for batches in batches_by_downstream
                            ]
                            put_s = now_seconds() - start

                            start = now_seconds()
                            output_refs_by_downstream = [
                                [
                                    run_ai_operator.remote(ref, scenario, args.embedding_dim)
                                    for ref in refs
                                ]
                                for refs in refs_by_downstream
                            ]
                            all_output_refs = [
                                ref
                                for refs in output_refs_by_downstream
                                for ref in refs
                            ]
                            if all_output_refs:
                                ray.wait(all_output_refs, num_returns=len(all_output_refs))
                            operator_s = now_seconds() - start

                            start = now_seconds()
                            reducer_refs = [
                                fanin_outputs.remote(downstream_id, *refs)
                                for downstream_id, refs in enumerate(output_refs_by_downstream)
                            ]
                            reduced = ray.get(reducer_refs)
                            fanin_s = now_seconds() - start

                            reduced_rows = sum(item["rows"] for item in reduced)
                            output_bytes = sum(item["output_bytes"] for item in reduced)
                            end_to_end_s = build_s + put_s + operator_s + fanin_s
                            rows.append(
                                {
                                    "benchmark": "ai_operator_scenario_motivation",
                                    "scenario": scenario,
                                    "strategy": strategy,
                                    "upstream": upstream_count,
                                    "downstream": downstream_count,
                                    "total_rows": args.total_rows,
                                    "embedding_dim": args.embedding_dim if scenario == "embed_rag" else 0,
                                    "text_tokens": args.text_tokens,
                                    "repeat": repeat,
                                    "input_objects": input_object_count,
                                    "avg_input_object_kb": input_bytes / input_object_count / 1024
                                    if input_object_count
                                    else 0.0,
                                    "avg_output_object_kb": output_bytes / input_object_count / 1024
                                    if input_object_count
                                    else 0.0,
                                    "build_ms": build_s * 1000,
                                    "put_ms": put_s * 1000,
                                    "operator_ms": operator_s * 1000,
                                    "fanin_ms": fanin_s * 1000,
                                    "end_to_end_ms": end_to_end_s * 1000,
                                    "rows_per_s": reduced_rows / end_to_end_s if end_to_end_s > 0 else 0.0,
                                    "reduced_rows": reduced_rows,
                                    "output_bytes": output_bytes,
                                }
                            )
    finally:
        ray.shutdown()

    print_table(rows)
    write_csv(args.output, rows)


if __name__ == "__main__":
    main()
