"""Read-only Ray worker limits required by native framework experiments."""

from __future__ import annotations

from collections.abc import Callable, Iterable


MINIMUM_RAY_WORKER_NOFILE = 65_536
RayNofileProbe = Callable[[str], tuple[int, int]]


def probe_ray_worker_nofile(address: str) -> tuple[int, int]:
    """Return ``RLIMIT_NOFILE`` from a real worker on the selected Ray cluster."""

    import ray

    was_initialized = ray.is_initialized()
    if not was_initialized:
        ray.init(address=address)

    @ray.remote(num_cpus=0)
    def worker_limit() -> tuple[int, int]:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        return int(soft), int(hard)

    try:
        soft, hard = ray.get(worker_limit.remote())
        return int(soft), int(hard)
    finally:
        if not was_initialized:
            ray.shutdown()


def validate_ray_worker_nofile(
    addresses: Iterable[str],
    *,
    minimum_soft: int = MINIMUM_RAY_WORKER_NOFILE,
    probe: RayNofileProbe = probe_ray_worker_nofile,
) -> dict[str, dict[str, int]]:
    """Probe each distinct Ray cluster and reject a worker soft limit below the gate."""

    if minimum_soft <= 0:
        raise ValueError("minimum Ray worker nofile must be positive")
    evidence: dict[str, dict[str, int]] = {}
    for address in sorted(set(addresses)):
        soft, hard = probe(address)
        evidence[address] = {
            "soft": int(soft),
            "hard": int(hard),
            "minimum_soft": minimum_soft,
        }
        if soft < minimum_soft:
            raise RuntimeError(
                "Ray worker RLIMIT_NOFILE is below the formal-run gate: "
                f"address={address}; soft={soft}; hard={hard}; "
                f"minimum_soft={minimum_soft}. Restart Ray from a shell with a "
                "sufficient nofile limit before running the matrix."
            )
    return evidence
