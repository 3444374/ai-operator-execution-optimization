"""Best-effort probe for the live vLLM prefix-caching flag.

``service_metadata.prefix_caching`` in the scenario config is a *declared*
contract. These helpers read the actual vLLM server process cmdline (the runner
is co-located with vLLM on the experiment host) so the runner can fail-closed
when the declared value contradicts the live service.

vLLM does not expose the prefix-cache-enabled state over ``/metrics`` (only the
runtime hit rate, and only under traffic), so the process cmdline is the most
reliable signal available. Detection is best-effort: returning ``None`` means
"could not determine" (non-Linux host, ``ps`` unavailable, no co-located vLLM,
or disagreeing processes); callers should warn rather than fail in that case.
"""

from __future__ import annotations

import re
import subprocess

# Full module path of the vLLM OpenAI server entry point. Using the full path
# (not just the partial "vllm.entrypoints") avoids matching unrelated processes
# that merely reference the string — e.g. `grep vllm.entrypoints`, an editor
# with the source open, or the runner's own subprocess running this probe.
_VLLM_PROC_MARKER = "vllm.entrypoints.openai.api_server"


def parse_prefix_caching_flag(cmdline: str) -> bool | None:
    """Return whether a vLLM cmdline enables prefix caching.

    vLLM exposes prefix caching via ``--enable-prefix-caching`` (store_true)
    and ``--no-enable-prefix-caching`` (store_false); absence returns ``None``
    (the version default, which varies across vLLM releases). argparse is
    last-wins, so the last matching flag on the cmdline wins. The
    ``--enable-prefix-caching=<bool>`` equals form is also accepted.
    """
    if not cmdline:
        return None
    result: bool | None = None
    for token in re.split(r"\s+", cmdline.strip()):
        if token == "--no-enable-prefix-caching":
            result = False
        elif token == "--enable-prefix-caching":
            result = True
        elif token.startswith("--enable-prefix-caching="):
            value = token.split("=", 1)[1].strip().lower()
            result = value not in ("false", "0", "no", "off")
    return result


def _list_process_cmdlines() -> list[str]:
    """Return all process cmdlines via ``ps -eo args``, or [] if unavailable."""
    try:
        completed = subprocess.run(
            ["ps", "-eo", "args"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return []
    return completed.stdout.splitlines()


def probe_live_prefix_caching() -> bool | None:
    """Best-effort live prefix-caching state from co-located vLLM processes.

    Returns the common flag when all detected vLLM ``api_server`` processes
    agree; ``None`` when none are found, ``ps`` is unavailable, the flag is
    absent from all of them, or they disagree.
    """
    flags: list[bool] = []
    for line in _list_process_cmdlines():
        if _VLLM_PROC_MARKER not in line:
            continue
        flag = parse_prefix_caching_flag(line)
        if flag is not None:
            flags.append(flag)
    if not flags:
        return None
    if len(set(flags)) == 1:
        return flags[0]
    return None
