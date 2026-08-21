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

import hashlib
import math
import re
import shlex
import sys
from pathlib import Path


SERVICE_IDENTITY_HASH_FIELDS = {
    "model_config_sha256": "config.json",
    "tokenizer_config_sha256": "tokenizer_config.json",
    "tokenizer_json_sha256": "tokenizer.json",
    "model_safetensors_index_sha256": "model.safetensors.index.json",
    "generation_config_sha256": "generation_config.json",
    "model_weight_00001_sha256": "model-00001-of-00004.safetensors",
    "model_weight_00002_sha256": "model-00002-of-00004.safetensors",
    "model_weight_00003_sha256": "model-00003-of-00004.safetensors",
    "model_weight_00004_sha256": "model-00004-of-00004.safetensors",
}
VLLM_DISTRIBUTION_HASH_FIELDS = {
    "vllm_metadata_sha256": "METADATA",
    "vllm_wheel_sha256": "WHEEL",
    "vllm_record_sha256": "RECORD",
}
VLLM_SOURCE_HASH_FIELDS = {
    "vllm_source_config_scheduler_sha256": "config/scheduler.py",
    "vllm_source_scheduler_sha256": "v1/core/sched/scheduler.py",
    "vllm_source_async_scheduler_sha256": "v1/core/sched/async_scheduler.py",
    "vllm_source_request_queue_sha256": "v1/core/sched/request_queue.py",
    "vllm_source_request_sha256": "v1/request.py",
}
_REQUIRED_SERVICE_IDENTITY_FIELDS = {
    "model",
    "model_path",
    "model_revision",
    "dtype",
    "service",
    "scheduler",
    "max_model_len",
    "max_num_seqs",
    "max_num_batched_tokens",
    "chunked_prefill",
    "prefix_caching",
    "mfu_metrics",
    "enforce_eager",
    "compilation_mode",
    "gpu_memory_utilization",
    *SERVICE_IDENTITY_HASH_FIELDS,
    *VLLM_DISTRIBUTION_HASH_FIELDS,
    *VLLM_SOURCE_HASH_FIELDS,
}
_SHA256 = re.compile(r"[0-9a-f]{64}")
_REVISION = re.compile(r"[0-9a-f]{40}")


def validate_service_identity(identity: dict[str, object]) -> None:
    """Validate the complete frozen model/build/runtime identity."""

    missing = sorted(_REQUIRED_SERVICE_IDENTITY_FIELDS - set(identity))
    unknown = sorted(set(identity) - _REQUIRED_SERVICE_IDENTITY_FIELDS)
    if missing or unknown:
        raise ValueError(
            f"service identity fields invalid: missing={missing} unknown={unknown}"
        )
    for field in ("model", "model_path", "dtype", "service"):
        if not isinstance(identity[field], str) or not identity[field]:
            raise ValueError(f"service identity {field} must be non-empty")
    if identity["scheduler"] != "vllm_native_fcfs":
        raise ValueError("service identity scheduler must be vllm_native_fcfs")
    if identity["dtype"] != "bfloat16":
        raise ValueError("matched service identity dtype must be bfloat16")
    if identity["service"] != "0.25.1":
        raise ValueError("matched service identity vLLM version must be 0.25.1")
    if identity["compilation_mode"] != "vllm_compile":
        raise ValueError("matched service identity must use vllm_compile")
    revision = identity["model_revision"]
    if not isinstance(revision, str) or _REVISION.fullmatch(revision) is None:
        raise ValueError("service identity model_revision must be a 40-hex commit")
    for field in (
        *SERVICE_IDENTITY_HASH_FIELDS,
        *VLLM_DISTRIBUTION_HASH_FIELDS,
        *VLLM_SOURCE_HASH_FIELDS,
    ):
        value = identity[field]
        if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
            raise ValueError(f"service identity {field} must be a SHA-256")
    for field in ("max_model_len", "max_num_seqs", "max_num_batched_tokens"):
        value = identity[field]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"service identity {field} must be a positive integer")
    for field in (
        "chunked_prefill",
        "prefix_caching",
        "mfu_metrics",
        "enforce_eager",
    ):
        if not isinstance(identity[field], bool):
            raise ValueError(f"service identity {field} must be boolean")
    if not identity["chunked_prefill"] or not identity["prefix_caching"]:
        raise ValueError("matched service requires chunked prefill and prefix caching")
    if not identity["mfu_metrics"] or identity["enforce_eager"]:
        raise ValueError("matched service requires MFU metrics and compile mode")
    utilization = identity["gpu_memory_utilization"]
    if (
        isinstance(utilization, bool)
        or not isinstance(utilization, (int, float))
        or not math.isfinite(float(utilization))
        or not 0 < float(utilization) <= 1
    ):
        raise ValueError("service identity gpu_memory_utilization must be in (0, 1]")


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


def _required_option(cmdline: str, flag: str, expected: object) -> str:
    values = _option_values(cmdline, flag)
    if len(values) != 1 or values[0] is None:
        raise RuntimeError(f"vLLM cmdline must define {flag} exactly once")
    actual = str(values[0])
    if actual != str(expected):
        raise RuntimeError(
            f"vLLM cmdline {flag} drift; expected {expected}, observed {actual}"
        )
    return actual


def _required_boolean_flag(cmdline: str, flag: str, expected: bool) -> bool:
    values = _option_values(cmdline, flag)
    if len(values) != 1:
        raise RuntimeError(f"vLLM cmdline must define {flag} exactly once")
    value = values[0]
    actual = True if value is None else value.lower() not in {
        "false", "0", "off", "no"
    }
    if actual != expected:
        raise RuntimeError(
            f"vLLM cmdline {flag} drift; expected {expected}, observed {actual}"
        )
    return actual


def verify_endpoint_service_identity(
    cmdline_pool: list[str],
    endpoint_urls: list[str] | tuple[str, ...],
    expected_identity: dict[str, object],
    *,
    tag: str = "saor-five-arm-service",
) -> dict[str, dict[str, object]]:
    """Pure exact gate for every service process used by the five-arm matrix."""

    validate_service_identity(expected_identity)
    option_fields = {
        "--model": "model_path",
        "--served-model-name": "model",
        "--dtype": "dtype",
        "--max-model-len": "max_model_len",
        "--gpu-memory-utilization": "gpu_memory_utilization",
        "--max-num-seqs": "max_num_seqs",
        "--max-num-batched-tokens": "max_num_batched_tokens",
    }
    boolean_fields = {
        "--enable-chunked-prefill": "chunked_prefill",
        "--enable-prefix-caching": "prefix_caching",
        "--enable-mfu-metrics": "mfu_metrics",
    }
    observed: dict[str, dict[str, object]] = {}
    for url in endpoint_urls:
        port = url.rsplit(":", 1)[-1].split("/")[0]
        matching_cmdlines = [
            command
            for command in cmdline_pool
            if cmdline_for_port([command], port) is not None
        ]
        if len(matching_cmdlines) != 1:
            raise RuntimeError(
                f"[{tag}][preflight] port {port}: expected one vLLM cmdline, "
                f"observed {len(matching_cmdlines)}"
            )
        cmdline = matching_cmdlines[0]
        endpoint_observed: dict[str, object] = {}
        for flag, field in option_fields.items():
            endpoint_observed[field] = _required_option(
                cmdline, flag, expected_identity[field]
            )
        for flag, field in boolean_fields.items():
            endpoint_observed[field] = _required_boolean_flag(
                cmdline, flag, bool(expected_identity[field])
            )
        if _option_values(cmdline, "--enforce-eager"):
            raise RuntimeError(
                "vLLM cmdline execution-mode drift; expected compile, observed eager"
            )
        if _option_values(cmdline, "--compilation-config"):
            raise RuntimeError(
                "vLLM cmdline compilation config is not the frozen 0.25.1 default"
            )
        actual_scheduler = scheduler_cls_value(cmdline)
        if actual_scheduler is not None:
            raise RuntimeError(
                "vLLM cmdline scheduler class drift; expected native FCFS"
            )
        endpoint_observed.update({
            "scheduler": "vllm_native_fcfs",
            "enforce_eager": False,
            "compilation_mode": "vllm_compile",
        })
        observed[url] = endpoint_observed
    return observed


def verify_model_artifact_identity(
    expected_identity: dict[str, object],
) -> dict[str, str]:
    """Bind the declared model revision to the frozen local artifact hashes."""

    validate_service_identity(expected_identity)
    model_root = Path(str(expected_identity["model_path"]))
    observed: dict[str, str] = {}
    errors = []
    for field, name in SERVICE_IDENTITY_HASH_FIELDS.items():
        path = model_root / name
        if not path.is_file():
            errors.append(f"model artifact is missing: {path}")
            continue
        digest_builder = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                digest_builder.update(chunk)
        digest = digest_builder.hexdigest()
        observed[field] = digest
        if digest != expected_identity[field]:
            errors.append(f"model artifact {name} hash drift")
    if errors:
        raise RuntimeError("; ".join(errors))
    return observed


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


def scheduler_cls_value(cmdline):
    """Return the explicit ``--scheduler-cls`` value, or ``None`` for native.

    The five-arm FCFS contract treats absence as the only accepted native-FCFS
    identity.  A malformed bare flag fails instead of being read as native.
    """

    tokens = cmdline.split()
    values = []
    for index, token in enumerate(tokens):
        if token.startswith("--scheduler-cls="):
            values.append(token.split("=", 1)[1])
        elif token == "--scheduler-cls":
            if index + 1 >= len(tokens) or tokens[index + 1].startswith("--"):
                raise RuntimeError("vLLM cmdline has a bare --scheduler-cls flag")
            values.append(tokens[index + 1])
    if len(values) > 1:
        raise RuntimeError("vLLM cmdline defines --scheduler-cls more than once")
    return values[0] if values else None


def verify_endpoint_scheduler_cls(
    cmdline_pool,
    endpoint_urls,
    expected_scheduler_cls,
    *,
    strict=True,
    tag="preflight",
):
    """Verify the actual service-layer scheduler class for every endpoint."""

    for url in endpoint_urls:
        port = url.rsplit(":", 1)[-1].split("/")[0]
        cmdline = cmdline_for_port(cmdline_pool, port)
        if cmdline is None:
            if strict:
                raise RuntimeError(
                    f"[{tag}][preflight] port {port}: no matching vLLM cmdline"
                )
            continue
        actual = scheduler_cls_value(cmdline)
        if actual != expected_scheduler_cls:
            expected = expected_scheduler_cls or "vLLM native FCFS (no --scheduler-cls)"
            observed = actual or "vLLM native FCFS (no --scheduler-cls)"
            raise RuntimeError(
                f"[{tag}][preflight] port {port}: scheduler class drift; "
                f"expected {expected}, observed {observed}"
            )


def verify_live_vllm_scheduler(
    endpoint_urls,
    expected_scheduler_cls,
    *,
    strict=True,
    tag="preflight",
):
    """Fail closed against live cmdlines and return provenance cmdlines."""

    cmdlines = _read_live_cmdlines()
    if not cmdlines:
        if strict:
            raise RuntimeError(
                f"[{tag}][preflight] no live vLLM process; scheduler is unverified"
            )
        return cmdlines
    verify_endpoint_scheduler_cls(
        list(cmdlines.values()),
        endpoint_urls,
        expected_scheduler_cls,
        strict=strict,
        tag=tag,
    )
    return cmdlines


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


def _read_live_processes() -> dict[str, dict[str, str]]:
    """Return command and interpreter identity for live vLLM API processes."""

    import subprocess

    pg = subprocess.run(["pgrep", "-f", "vllm.entrypoints"], capture_output=True, text=True)
    pids = [p for p in pg.stdout.split() if p]
    processes: dict[str, dict[str, str]] = {}
    for pid in pids:
        try:
            cmdline = (
                Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
            )
        except OSError:
            continue
        try:
            executable = str(Path(f"/proc/{pid}/exe").resolve(strict=True))
        except OSError:
            executable = "unavailable"
        processes[pid] = {"cmdline": cmdline, "executable": executable}
    return processes


def verify_live_vllm_service_identity(
    endpoint_urls: tuple[str, ...],
    expected_identity: dict[str, object],
    *,
    tag: str = "saor-five-arm-service",
) -> dict[str, object]:
    """Read live Linux process state and return only non-secret identity evidence."""

    processes = _read_live_processes()
    if not processes:
        raise RuntimeError(
            f"[{tag}][preflight] no live vLLM process; service identity is unverified"
        )
    cmdlines = {pid: state["cmdline"] for pid, state in processes.items()}
    endpoints = verify_endpoint_service_identity(
        list(cmdlines.values()), endpoint_urls, expected_identity, tag=tag
    )
    audit_executable = str(Path(sys.executable).resolve())
    for url in endpoint_urls:
        port = url.rsplit(":", 1)[-1].split("/")[0]
        matching = [
            state for state in processes.values()
            if cmdline_for_port([state["cmdline"]], port) is not None
        ]
        if len(matching) != 1 or matching[0]["executable"] != audit_executable:
            observed = matching[0]["executable"] if len(matching) == 1 else "ambiguous"
            raise RuntimeError(
                f"[{tag}][preflight] port {port}: service Python runtime drift; "
                f"expected {audit_executable}, observed {observed}"
            )
        endpoints[url]["python_executable"] = audit_executable
    model_artifacts = verify_model_artifact_identity(expected_identity)
    return {
        "status": "passed",
        "process_count": len(cmdlines),
        "endpoints": endpoints,
        "model_revision": expected_identity["model_revision"],
        "model_artifacts": model_artifacts,
    }


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
