"""Profiler configuration resolution after argparse parsing."""

from __future__ import annotations

import argparse
import math
import os

from src.scheduling.runtime.ray_runtime import RayWorkerOptions


def embedding_endpoint_urls(args: argparse.Namespace) -> list[str]:
    return configured_urls(
        plural_cli=args.embedding_endpoint_urls,
        single_cli=args.embedding_endpoint_url,
        plural_env=os.environ.get("EMBEDDING_ENDPOINT_URLS"),
        single_env=os.environ.get("EMBEDDING_ENDPOINT_URL"),
    )


def completion_endpoint_urls(args: argparse.Namespace) -> list[str]:
    return configured_urls(
        plural_cli=args.completion_endpoint_urls,
        single_cli=args.completion_endpoint_url,
        plural_env=os.environ.get("COMPLETION_ENDPOINT_URLS"),
        single_env=os.environ.get("COMPLETION_ENDPOINT_URL"),
    )


def model_metrics_urls(args: argparse.Namespace) -> list[str]:
    return configured_urls(
        plural_cli=getattr(args, "model_metrics_urls", None),
        single_cli=getattr(args, "model_metrics_url", None),
        plural_env=os.environ.get("MODEL_METRICS_URLS"),
        single_env=os.environ.get("MODEL_METRICS_URL"),
    )


def configured_urls(
    *,
    plural_cli: str | None,
    single_cli: str | None,
    plural_env: str | None,
    single_env: str | None,
) -> list[str]:
    """Resolve explicit CLI values before plural/single environment defaults."""

    if plural_cli is not None:
        text = plural_cli
    elif single_cli is not None:
        text = single_cli
    elif plural_env is not None:
        text = plural_env
    elif single_env is not None:
        text = single_env
    else:
        return []
    return [value.strip() for value in text.split(",") if value.strip()]


def resolve_actor_workers_per_endpoint(
    args: argparse.Namespace,
    endpoint_count: int,
) -> int:
    if endpoint_count <= 0:
        raise SystemExit("endpoint_count must be positive")
    if args.actor_workers_per_endpoint < 0:
        raise SystemExit("--actor-workers-per-endpoint must be non-negative")
    if args.actor_workers_per_endpoint:
        return args.actor_workers_per_endpoint
    if endpoint_count > 1:
        raise SystemExit(
            "multi-endpoint ray_actor requires --actor-workers-per-endpoint"
        )
    if args.model_workers <= 0:
        raise SystemExit("--model-workers must be positive")
    return args.model_workers


def validate_ray_worker_resources(args: argparse.Namespace) -> None:
    if args.executor not in {"ray_actor", "ray_task"}:
        return
    if (
        not math.isfinite(args.ray_worker_num_cpus)
        or args.ray_worker_num_cpus <= 0
    ):
        raise SystemExit("--ray-worker-num-cpus must be finite and positive")
    if args.executor == "ray_actor" and args.ray_actor_max_concurrency <= 0:
        raise SystemExit("--ray-actor-max-concurrency must be positive")


def ray_worker_options(
    args: argparse.Namespace,
) -> RayWorkerOptions | None:
    if args.executor not in {"ray_actor", "ray_task"}:
        return None
    validate_ray_worker_resources(args)
    return RayWorkerOptions(
        num_cpus=args.ray_worker_num_cpus,
        actor_max_concurrency=(
            args.ray_actor_max_concurrency
            if args.executor == "ray_actor"
            else 1
        ),
    )


def validate_shared_credit_policy_args(args: argparse.Namespace) -> None:
    """Fail closed on bounded-priority policy inputs before runtime setup."""

    if args.shared_credit_policy != "saor_bounded_priority":
        return
    if not args.arrival_replay:
        raise SystemExit("bounded priority requires arrival replay")
    if args.submission_granularity != "request":
        raise SystemExit("bounded priority requires request granularity")
    slo_ms = float(args.shared_credit_job_slo_ms)
    window_ms = float(args.shared_credit_priority_window_ms)
    cap = float(args.shared_credit_job_debt_cap_work)
    acquire_timeout_s = (
        args.completion_request_timeout_s
        if args.operator == "ai_complete"
        else args.embedding_request_timeout_s
    )
    if not math.isfinite(acquire_timeout_s) or acquire_timeout_s <= 0:
        raise SystemExit("shared-credit acquire timeout must be positive")
    if args.shared_credit_job_priority > 0 and (
        not math.isfinite(slo_ms)
        or not math.isfinite(window_ms)
        or slo_ms <= 0
        or window_ms <= 0
    ):
        raise SystemExit(
            "bounded priority Job requires a positive SLO target and priority window"
        )
    if cap < 0 or not math.isfinite(cap):
        raise SystemExit("bounded priority debt cap must be finite and non-negative")
    if args.shared_credit_job_priority == 0 and window_ms != 0:
        raise SystemExit("priority window requires positive Job priority")
    if args.shared_credit_job_priority == 0 and slo_ms != 0:
        raise SystemExit("Job SLO target requires positive Job priority")
    if args.shared_credit_job_priority > 0 and cap != 0:
        raise SystemExit("priority Job and debt-cap Job must be explicit distinct roles")
