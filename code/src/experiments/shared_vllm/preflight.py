"""Reusable live-service gates for shared-vLLM experiment runners."""

from __future__ import annotations

import time
from urllib import error, request

from src.observability.metrics import parse_prometheus_metrics


def wait_for_idle(
    health_url: str,
    metrics_urls: tuple[str, ...],
    timeout_s: float,
) -> None:
    """Wait until health succeeds and every vLLM queue is idle."""

    deadline_s = time.monotonic() + timeout_s
    last_reason = "not checked"
    while time.monotonic() < deadline_s:
        try:
            with request.urlopen(health_url, timeout=2.0) as response:
                healthy = response.status == 200
        except (OSError, error.URLError) as exc:
            healthy = False
            last_reason = f"health:{type(exc).__name__}"
        if healthy:
            all_idle = True
            for metrics_url in metrics_urls:
                try:
                    with request.urlopen(metrics_url, timeout=2.0) as response:
                        metrics = parse_prometheus_metrics(
                            response.read().decode("utf-8", errors="replace")
                        )
                    running = metrics.get("vllm:num_requests_running")
                    waiting = metrics.get("vllm:num_requests_waiting")
                    if running is None or waiting is None:
                        all_idle = False
                        last_reason = f"missing_idle_metrics_at_{metrics_url}"
                        break
                    if running != 0 or waiting != 0:
                        all_idle = False
                        last_reason = (
                            f"busy_at_{metrics_url}:"
                            f"running={running},waiting={waiting}"
                        )
                        break
                except (OSError, error.URLError) as exc:
                    all_idle = False
                    last_reason = (
                        f"metrics_at_{metrics_url}:{type(exc).__name__}"
                    )
                    break
            if all_idle:
                return
        time.sleep(0.25)
    raise TimeoutError(f"model service did not become idle: {last_reason}")
