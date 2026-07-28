"""Command-line surface for the PostgreSQL AI-operator profiler."""

from __future__ import annotations

import argparse
import os

from .workloads import WORKLOAD_NAMES


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Profile PostgreSQL-triggered AI operator execution through "
            "Daft, Ray, and an external model service."
        )
    )
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--seed-rows", type=int, default=0)
    parser.add_argument(
        "--seed-workload",
        choices=WORKLOAD_NAMES,
        default="synthetic",
    )
    parser.add_argument("--reset-documents", action="store_true")
    parser.add_argument("--total-rows", type=int, default=10000)
    parser.add_argument("--db-fetch-rows", type=int, default=1024)
    parser.add_argument(
        "--data-source",
        choices=["arrow_postgres", "daft_postgres"],
        default="arrow_postgres",
    )
    parser.add_argument("--source-workload-name")
    parser.add_argument("--source-max-prompt-tokens", type=int)
    parser.add_argument(
        "--source-order",
        choices=["doc_id", "arrival_time"],
        default="doc_id",
    )
    parser.add_argument(
        "--operator",
        choices=["ai_embed", "ai_complete"],
        default="ai_embed",
    )
    parser.add_argument("--ray-batch-rows", type=int, default=1024)
    parser.add_argument(
        "--batching-policy",
        choices=[
            "fixed_rows",
            "token_budget",
            "best_fit_token_budget",
            "row_cap_aware_token_budget",
            "length_align_fixed_rows",
            "length_align_token_budget",
            "prefix_aware_fixed_rows",
            "prefix_aware_token_budget",
        ],
        default="fixed_rows",
    )
    parser.add_argument("--token-budget", type=int, default=0)
    parser.add_argument("--embedding-dim", type=int, default=128)
    parser.add_argument(
        "--model-backend",
        choices=["fake", "compatible_http", "http_openai", "ollama"],
        default="fake",
    )
    parser.add_argument("--embedding-endpoint-url")
    parser.add_argument("--embedding-endpoint-urls")
    parser.add_argument(
        "--embedding-model",
        default=os.environ.get("EMBEDDING_MODEL", "local-embedding"),
    )
    parser.add_argument(
        "--embedding-api-key",
        default=os.environ.get("EMBEDDING_API_KEY"),
    )
    parser.add_argument(
        "--embedding-request-timeout-s",
        type=float,
        default=120.0,
    )
    parser.add_argument("--completion-endpoint-url")
    parser.add_argument("--completion-endpoint-urls")
    parser.add_argument(
        "--completion-model",
        default=os.environ.get("COMPLETION_MODEL", "local-completion"),
    )
    parser.add_argument(
        "--completion-api-key",
        default=os.environ.get("COMPLETION_API_KEY"),
    )
    parser.add_argument(
        "--completion-request-timeout-s",
        type=float,
        default=120.0,
    )
    parser.add_argument("--completion-max-tokens", type=int, default=128)
    parser.add_argument(
        "--completion-return-token-ids",
        action="store_true",
    )
    parser.add_argument(
        "--completion-prompt-format",
        choices=["raw", "chatml"],
        default="raw",
    )
    parser.add_argument("--completion-temperature", type=float)
    parser.add_argument(
        "--output-cost-mode",
        choices=[
            "prompt_only",
            "fixed_output_cap",
            "trace_target_output",
        ],
        default="fixed_output_cap",
    )
    parser.add_argument("--cost-model-id", default="")
    parser.add_argument("--cost-tokenizer-id", default="")
    parser.add_argument("--model-metrics-url")
    parser.add_argument("--model-metrics-urls")
    parser.add_argument("--model-workers", type=int, default=2)
    parser.add_argument(
        "--actor-workers-per-endpoint",
        type=int,
        default=0,
        help="Number of Ray HTTP client actors per service endpoint.",
    )
    parser.add_argument("--ray-actor-max-concurrency", type=int, default=1)
    parser.add_argument("--ray-worker-num-cpus", type=float, default=0.25)
    parser.add_argument("--max-inflight", type=int, default=8)
    parser.add_argument(
        "--endpoint-routing",
        choices=["round_robin", "least_queued", "prefix_affinity"],
        default="round_robin",
    )
    parser.add_argument(
        "--pool-routing",
        choices=["none", "request_cost"],
        default="none",
    )
    parser.add_argument("--endpoint-pool-ids")
    parser.add_argument("--endpoint-gpu-ids")
    parser.add_argument(
        "--long-request-token-threshold",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--scheduling-policy",
        choices=[
            "static",
            "queue_adaptive",
            "aimd",
            "ewma_aimd",
            "pid",
            "aimd_hol",
        ],
        default="static",
    )
    parser.add_argument("--adaptive-min-inflight", type=int, default=2)
    parser.add_argument("--adaptive-max-inflight", type=int, default=16)
    parser.add_argument("--adaptive-queue-threshold", type=int, default=0)
    parser.add_argument(
        "--adaptive-running-threshold",
        type=int,
        default=128,
    )
    parser.add_argument("--adaptive-kv-threshold", type=float, default=0.85)
    parser.add_argument(
        "--adaptive-poll-interval-s",
        type=float,
        default=0.05,
    )
    parser.add_argument("--controller-min-window", type=int)
    parser.add_argument("--controller-max-window", type=int, default=16)
    parser.add_argument("--controller-initial-window", type=int)
    parser.add_argument(
        "--adaptive-sample-interval-s",
        type=float,
        default=0.25,
    )
    parser.add_argument("--ewma-alpha", type=float, default=0.3)
    parser.add_argument(
        "--pid-proportional-gain",
        type=float,
        default=0.5,
    )
    parser.add_argument("--pid-integral-gain", type=float, default=0.1)
    parser.add_argument(
        "--pid-derivative-gain",
        type=float,
        default=0.05,
    )
    parser.add_argument("--hol-age-congestion-s", type=float, default=2.0)
    parser.add_argument("--hol-age-low-load-s", type=float, default=0.5)
    parser.add_argument("--control-trace-output")
    parser.add_argument("--arrival-replay", action="store_true")
    parser.add_argument("--arrival-time-scale", type=float, default=1.0)
    parser.add_argument(
        "--submission-granularity",
        choices=["batch", "request"],
        default="batch",
    )
    parser.add_argument(
        "--flush-policy",
        choices=["immediate", "fixed_timeout", "queue_adaptive"],
        default="immediate",
    )
    parser.add_argument("--flush-timeout-ms", type=float, default=25.0)
    parser.add_argument("--flush-max-wait-ms", type=float, default=50.0)
    parser.add_argument("--flush-trace-output")
    parser.add_argument("--submission-trace-output")
    parser.add_argument("--resource-trace-output")
    parser.add_argument(
        "--resource-sample-interval-s",
        type=float,
        default=0.25,
    )
    parser.add_argument("--model-flops-per-token", type=float, default=0.0)
    parser.add_argument("--gpu-peak-tflops", type=float, default=0.0)
    parser.add_argument("--mfu-precision", default="")
    parser.add_argument("--request-trace-output")
    parser.add_argument("--request-slo-ms", type=float, default=0.0)
    parser.add_argument("--scenario-id", default="manual")
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument(
        "--strategy",
        choices=["fine", "coalesced"],
        default="coalesced",
    )
    parser.add_argument(
        "--organizer",
        choices=["arrow", "daft"],
        default="arrow",
    )
    parser.add_argument(
        "--organizer-partition-mode",
        choices=["none", "into_partitions", "repartition"],
        default="none",
    )
    parser.add_argument("--organizer-partitions", type=int, default=0)
    parser.add_argument(
        "--daft-runner",
        choices=["native", "ray"],
        default="native",
    )
    parser.add_argument(
        "--executor",
        choices=["ray_actor", "ray_task", "python"],
        default="ray_actor",
    )
    parser.add_argument(
        "--writeback-mode",
        choices=["none", "json_text", "pgvector"],
        default="json_text",
    )
    parser.add_argument("--write-batch-rows", type=int, default=0)
    parser.add_argument("--warmup-runs", type=int, default=0)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument(
        "--run-phase",
        choices=["warmup", "formal"],
    )
    parser.add_argument("--run-repeat-index", type=int)
    parser.add_argument("--experiment-id", default="manual")
    parser.add_argument(
        "--output",
        default="feasibility/results/postgres_ai_operator_profile.csv",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)
