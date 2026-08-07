"""vLLM declared-vs-effective config preflight (shared by the ramp + scenario runners).

The ramp driver (``code/scripts/baselines/multicard_scale_ramp.py``) historically owned
these verifiers; the cost-profile scenario runner
(``code/scripts/experiments/run_ai_operator_scenarios.py``) needs the same declared==effective
guarantee (audit F8). Per code/AGENTS.md §4 (低耦合: reusable logic lives in ``src/``, no
cross-script imports), the PURE cmdline-matching helpers + the live-process I/O wrapper live here
so both runners share one implementation.

The pure helpers (``cmdline_for_port`` / ``flag_value_present`` / ``prefix_cache_flag_enabled`` /
``verify_endpoint_cmdlines``) take synthetic cmdline strings and are unit-tested with no /proc and
no subprocess. The ``verify_live_vllm_config`` wrapper does the Linux pgrep + /proc/<pid>/cmdline
reads and is run on the server (not unit-tested locally).
"""

from __future__ import annotations

from pathlib import Path


def cmdline_for_port(cmdlines, port):
    """Return the cmdline carrying ``--port <port>`` / ``--port=<port>``, else None.

    Pure (no /proc I/O) so it is unit-testable with synthetic cmdline strings.
    """

    needles = (f"--port {port}", f"--port={port}")
    for c in cmdlines:
        if any(n in c for n in needles):
            return c
    return None


def flag_value_present(cmdline, flag, value):
    """True iff cmdline carries ``<flag> <value>`` or ``<flag>=<value>``. Pure for unit testing."""

    return f"{flag} {value}" in cmdline or f"{flag}={value}" in cmdline


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

    import subprocess

    pg = subprocess.run(["pgrep", "-f", "vllm.entrypoints"], capture_output=True, text=True)
    pids = [p for p in pg.stdout.split() if p]
    cmdlines: dict[str, str] = {}
    for pid in pids:
        try:
            cmdlines[pid] = (
                Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
            )
        except OSError:
            pass
    return cmdlines


def verify_live_vllm_config(endpoint_urls, declared_flags, strict, tag="preflight"):
    """Declared-vs-effective preflight against the LIVE vLLM processes.

    pgrep ``vllm.entrypoints`` + read each ``/proc/<pid>/cmdline`` + verify each endpoint port
    carries the declared flags + a verifiable prefix-cache. ``strict=True`` fail-closes (raise);
    ``strict=False`` only WARNs. Returns the {pid: cmdline} pool (for provenance capture).

    Server-only (Linux pgrep + /proc); the pure matching logic is ``verify_endpoint_cmdlines``,
    which is unit-tested independently.
    """

    cmdlines = _read_live_cmdlines()
    if not cmdlines:
        msg = f"[{tag}][preflight] no vllm.entrypoints process (pgrep)"
        if strict:
            raise RuntimeError(f"{msg} -- strict: cannot verify effective config")
        print(f"{msg} WARN; skipping config verify", flush=True)
        return cmdlines
    verify_endpoint_cmdlines(list(cmdlines.values()), endpoint_urls, declared_flags, strict, tag=tag)
    return cmdlines
