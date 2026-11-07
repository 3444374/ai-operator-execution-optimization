"""Service observations for explicitly configured scenario experiments."""

from __future__ import annotations

import shlex
import time
from pathlib import Path
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


def _tokens(cmdline: str) -> list[str]:
    try:
        return shlex.split(cmdline)
    except ValueError as exc:
        raise RuntimeError(f"vLLM cmdline is not shell-tokenizable: {exc}") from exc


def _option_values(cmdline: str, flag: str) -> list[str | None]:
    tokens = _tokens(cmdline)
    values: list[str | None] = []
    for index, token in enumerate(tokens):
        if token.startswith(f"{flag}="):
            values.append(token.split("=", 1)[1])
        elif token == flag:
            if index + 1 < len(tokens) and not tokens[index + 1].startswith("--"):
                values.append(tokens[index + 1])
            else:
                values.append(None)
    return values


def cmdline_for_port(cmdlines, port):
    """Return the cmdline carrying ``--port <port>`` / ``--port=<port>``, else None.

    Pure (no /proc I/O) so it is unit-testable with synthetic cmdline strings.
    """

    for command in cmdlines:
        if str(port) in _option_values(command, "--port"):
            return command
    return None


def flag_value_present(cmdline, flag, value):
    """True iff cmdline carries ``<flag> <value>`` or ``<flag>=<value>``. Pure for unit testing."""

    return str(value) in _option_values(cmdline, flag)


def prefix_cache_flag_enabled(cmdline):
    """Prefix-cache effective state from the cmdline.

    Returns True (ON) / False (OFF) / None (absent -> vLLM default, unverified from cmdline).
    TOKEN-based so ``--enable-prefix-caching=false`` is NOT misread as ON by a naive substring
    test. Pure for unit testing.
    """

    tokens = cmdline.split()
    flagged = [t for t in tokens if t == "--enable-prefix-caching" or t.startswith("--enable-prefix-caching=")]
    if not flagged:
        return None
    val = flagged[-1]
    if "=" not in val:
        return True  # bare flag == ON
    return val.split("=", 1)[1].strip().lower() not in ("false", "0", "off", "no")


def verify_endpoint_cmdlines(cmdline_pool, endpoint_urls, declared_flags, strict, tag="preflight"):
    """Pure verifier: raise (strict) or WARN (non-strict) on per-endpoint cmdline mismatches.

    ``declared_flags``: {flag: value_str}. Raises RuntimeError on any strict failure; non-strict
    only prints WARNs. ``tag`` labels log lines (e.g. "ramp" / "cost-profile") so the source
    runner is identifiable. Unit-tested directly with synthetic cmdline strings -- no /proc.
    """

    for url in endpoint_urls:
        port = url.rsplit(":", 1)[-1].split("/")[0]
        c = cmdline_for_port(cmdline_pool, port)
        if c is None:
            msg = f"port {port} ({url}): no matching vLLM process cmdline"
            if strict:
                raise RuntimeError(f"[{tag}][preflight] {msg} (strict)")
            print(f"[{tag}][preflight] WARN: {msg}", flush=True)
            continue
        for flag, val in declared_flags.items():
            if flag_value_present(c, flag, val):
                print(f"[{tag}][preflight] port {port} cmdline carries {flag} {val} (declared == effective)", flush=True)
            elif strict:
                raise RuntimeError(
                    f"port {port} cmdline missing {flag} {val} (strict; declared != effective). "
                    f"Start vLLM with the flag or disable strict for screening."
                )
            else:
                print(f"[{tag}][preflight] WARN: port {port} missing {flag} {val}; vLLM DEFAULT (effective != declared)", flush=True)
        pc = prefix_cache_flag_enabled(c)
        if pc is True:
            print(f"[{tag}][preflight] port {port} cmdline has --enable-prefix-caching (effective ON)", flush=True)
        elif pc is False:
            print(f"[{tag}][preflight] port {port} cmdline has --enable-prefix-caching=false (effective OFF)", flush=True)
        elif strict:
            raise RuntimeError(
                f"port {port}: --enable-prefix-caching NOT on cmdline; effective prefix-cache UNVERIFIED "
                f"(strict requires a verifiable prefix-cache; add the flag or relax strict)."
            )
        else:
            print(f"[{tag}][preflight] WARN: port {port}: --enable-prefix-caching NOT on cmdline -> vLLM DEFAULT", flush=True)


def _read_live_cmdlines():
    """Return {pid: cmdline_str} for live vllm.entrypoints processes (Linux pgrep + /proc)."""

    return {
        pid: str(state["cmdline"])
        for pid, state in _read_live_processes().items()
    }


def _linux_process_start_time_ticks(pid: str) -> str:
    """Read Linux field 22 without confusing spaces inside ``comm``."""

    raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    close = raw.rfind(")")
    if close < 0:
        raise RuntimeError(f"/proc/{pid}/stat is malformed")
    fields_after_comm = raw[close + 2:].split()
    if len(fields_after_comm) <= 19:
        raise RuntimeError(f"/proc/{pid}/stat has no start time")
    return fields_after_comm[19]


def _read_live_processes() -> dict[str, dict[str, object]]:
    """Return command and interpreter identity for live vLLM API processes."""

    import subprocess

    pg = subprocess.run(["pgrep", "-f", "vllm.entrypoints"], capture_output=True, text=True)
    pids = [p for p in pg.stdout.split() if p]
    processes: dict[str, dict[str, str]] = {}
    for pid in pids:
        try:
            argv = [
                token.decode("utf-8", "replace")
                for token in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
                if token
            ]
            cmdline = shlex.join(argv)
            start_time_ticks = _linux_process_start_time_ticks(pid)
        except OSError:
            continue
        processes[pid] = {
            "cmdline": cmdline,
            "argv0": argv[0] if argv else "unavailable",
            "start_time_ticks": start_time_ticks,
        }
    return processes
