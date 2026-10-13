"""Reuse nonblocking vLLM pressure sampling for explicitly declared replicas."""

from contextlib import contextmanager
from urllib.parse import urlsplit, urlunsplit

from ...observability.metrics.vllm import scrape_prometheus_metrics
from ...scheduling.runtime.observations import NonBlockingMetricsObservationProvider, ServiceMetricsSnapshot


def service_pressure(metrics):
    names = ("vllm:num_requests_running", "vllm:num_requests_waiting", "vllm:kv_cache_usage_perc")
    if any(name not in metrics for name in names):
        return None
    running, waiting, kv = (metrics[name] for name in names)
    if running < 0 or waiting < 0 or int(running) != running or int(waiting) != waiting:
        return None
    # Token counters are not interchangeable with descriptor work units.
    return ServiceMetricsSnapshot(int(running), int(waiting), kv)


@contextmanager
def observe_replica_metrics(directory):
    """One /metrics sampler per declared listener, outside the Core owner thread.

    The caller owns this context until execution cleanup. Metrics do not prove
    deployment/device identity or a server-wide hard capacity reservation.
    """
    if directory.routing != "adaptive":
        raise ValueError("live replica metrics require adaptive routing")
    providers = {}
    try:
        for replica in directory.replicas:
            url = urlsplit(replica.endpoint_url)
            metrics_url = urlunsplit((url.scheme, url.netloc, "/metrics", "", ""))
            def sample(url=metrics_url):
                return service_pressure(scrape_prometheus_metrics(url, timeout_s=1.0))
            providers[replica.endpoint_id] = NonBlockingMetricsObservationProvider(sample,
                poll_interval_s=directory.adaptation.sample_interval_s,
                stale_after_s=directory.adaptation.stale_after_s, close_timeout_s=1.5)
        yield providers
    finally:
        for provider in providers.values():
            provider.close()
