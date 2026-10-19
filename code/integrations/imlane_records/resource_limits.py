"""Per-run owner allowances; historical echo defaults remain unchanged."""


ECHO_LIMITS = dict(cpu_cores=8, rss_bytes=8 * 1024**3, shm_bytes=2 * 1024**3)


def owner_limits(value=None):
    if value is None:
        return dict(ECHO_LIMITS)
    if (type(value) is not dict or set(value) != set(ECHO_LIMITS)
            or any(type(number) is not int or number <= 0 for number in value.values())):
        raise ValueError("owner limits require positive integer CPU cores, RSS bytes and SHM bytes")
    return dict(value)


def check_resources(rss, shm, limits):
    if rss > limits["rss_bytes"] or shm > limits["shm_bytes"]:
        raise RuntimeError("records resource allowance exceeded")
