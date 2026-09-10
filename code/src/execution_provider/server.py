"""Run the standalone SemLoom versioned Unix-domain-socket provider gateway."""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import stat
import time
import threading
from functools import partial
from pathlib import Path

from .adapters.golden import GoldenCompletionAdapter
from .adapters.openai_compatible_fixed import (
    OpenAICompatibleFixedAdapter,
)
from .adapters.model_config import load_fixed_model_config
from .completion import CompletionAdapter
from .completion import Completion
from .gateway_runtime import GatewayLimits, GatewayRuntime
from .limits import MAX_INCREMENTAL_TASKS
from .request_admission import RequestAdmission
from .session_dispatch import run_session as _run_session


def parse_args(argv=None) -> argparse.Namespace:
    """Parse the versioned provider gateway command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--max-connections", type=int, default=GatewayLimits.max_connections)
    parser.add_argument(
        "--max-active-jobs",
        type=int,
        default=1,
        help="registered incremental Jobs sharing one engine; storage is partitioned",
    )
    parser.add_argument(
        "--max-active-requests", type=int, default=GatewayLimits.max_active_requests
    )
    parser.add_argument(
        "--max-held-tasks",
        type=int,
        help="incremental accepted task budget; defaults to request capacity",
    )
    parser.add_argument("--input-buffer-bytes", type=int, help="incremental input byte budget")
    parser.add_argument("--organization-config", type=Path, help="local Map token-work organization configuration")
    parser.add_argument(
        "--result-buffer-bytes", type=int, help="incremental reserved result byte budget"
    )
    parser.add_argument("--frame-timeout-ms", type=int, default=GatewayLimits.frame_timeout_ms)
    parser.add_argument(
        "--incremental-map", action="store_true", help="serve Map with version-six bounded intake"
    )
    parser.add_argument("--once", action="store_true", help="serve one session and exit")
    adapter_group = parser.add_mutually_exclusive_group()
    adapter_group.add_argument(
        "--golden-fixture",
        type=Path,
        help="payload-digest to raw Filter text or explicit Map completion JSON object",
    )
    adapter_group.add_argument(
        "--fixed-model-config",
        type=Path,
        help="repository-external fixed OpenAI-compatible endpoint configuration",
    )
    parser.add_argument("--test-response-delay-ms", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument(
        "--test-tamper-evidence-digest", action="store_true", help=argparse.SUPPRESS
    )
    parser.add_argument("--test-disconnect-on-task", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--test-fill-connect-queue-ms", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--test-max-sessions", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument(
        "--test-completion-fixture",
        choices=(
            "error-message",
            "escaped-nul",
            "extra-field",
            "fractional-integer",
            "identity-mismatch",
            "integer-overflow",
            "invalid-utf8",
            "malformed-json",
            "missing-field",
            "non-object",
            "raw-nul",
            "wrong-integer-type",
            "v3-extra-field",
            "v3-error-code",
            "v3-error-extra-field",
            "v3-error-missing-field",
            "v3-error-sequence",
            "v3-finish-reason",
            "v3-invalid-usage",
            "v3-model-mismatch",
            "v3-open-error",
            "v3-open-error-sequence",
        ),
        help=argparse.SUPPRESS,
    )
    return parser.parse_args(argv)


def main(
    argv=None,
    *,
    adapter_wrapper=None,
    session_wrapper=None,
    incremental_observer=None,
    incremental_execution_factory=None,
) -> int:
    """Serve sessions; optional decorators observe this invocation only."""
    args = parse_args(argv)
    try:
        limits = GatewayLimits(
            args.max_connections, args.max_active_requests, args.frame_timeout_ms
        )
    except ValueError as error:
        raise SystemExit(str(error)) from None
    if args.test_response_delay_ms < 0:
        raise SystemExit("--test-response-delay-ms must be non-negative")
    if args.test_fill_connect_queue_ms < 0:
        raise SystemExit("--test-fill-connect-queue-ms must be non-negative")
    if args.test_max_sessions < 0:
        raise SystemExit("--test-max-sessions must be non-negative")
    if not 1 <= args.max_active_jobs <= limits.max_connections:
        raise SystemExit("active Jobs must fit connection capacity")
    if args.max_active_jobs != 1 and not args.incremental_map:
        raise SystemExit("multiple active Jobs require --incremental-map")
    if args.organization_config is not None:
        if not args.incremental_map or args.max_active_jobs != 1 or incremental_execution_factory is not None:
            raise SystemExit("organization configuration requires single-Job incremental Map and its own factory")
        from .adapters.map_organization import MapOrganizationConfig, organization_factory
        incremental_execution_factory = organization_factory(MapOrganizationConfig.load(args.organization_config))
    socket_path = args.socket.resolve()
    golden_fixtures = _load_golden_fixtures(args.golden_fixture)
    completion_adapter: CompletionAdapter
    incremental_adapter = None
    held_tasks = args.max_active_requests if args.max_held_tasks is None else args.max_held_tasks
    if (
        any(
            value is not None
            for value in (args.max_held_tasks, args.input_buffer_bytes, args.result_buffer_bytes)
        )
        and not args.incremental_map
    ):
        raise SystemExit("incremental buffer budgets require --incremental-map")
    if any(
        value is not None and value < 1
        for value in (args.input_buffer_bytes, args.result_buffer_bytes)
    ):
        raise SystemExit("incremental byte budgets must be positive")
    if args.incremental_map and (
        args.fixed_model_config is None or not 1 <= held_tasks <= MAX_INCREMENTAL_TASKS
    ):
        raise SystemExit(
            f"incremental Map v6 requires a fixed model, capacity 1..{MAX_INCREMENTAL_TASKS}"
        )
    if args.fixed_model_config is None:
        completion_adapter = GoldenCompletionAdapter(golden_fixtures)
    else:
        try:
            fixed_config = load_fixed_model_config(args.fixed_model_config)
        except ValueError:
            raise SystemExit("invalid fixed model configuration") from None
        if args.incremental_map:
            from .multiplexed_gateway import MultiSessionMapGateway

            incremental_adapter = MultiSessionMapGateway(
                fixed_config,
                max_jobs=args.max_active_jobs,
                max_connections=limits.max_connections,
                max_tasks=held_tasks,
                max_active_requests=limits.max_active_requests,
                frame_timeout_ms=limits.frame_timeout_ms,
                input_bytes=args.input_buffer_bytes,
                result_bytes=args.result_buffer_bytes,
                observer=incremental_observer,
                execution_factory=incremental_execution_factory,
            )
            completion_adapter = incremental_adapter
        else:
            completion_adapter = OpenAICompatibleFixedAdapter(fixed_config)
    try:
        if adapter_wrapper is not None:
            completion_adapter = adapter_wrapper(completion_adapter)
        if incremental_adapter is None:
            completion_adapter = RequestAdmission(completion_adapter, limits.max_active_requests)
        run_session = _run_session if session_wrapper is None else session_wrapper(_run_session)
        if socket_path.exists():
            mode = socket_path.stat().st_mode
            kind = "socket" if stat.S_ISSOCK(mode) else "non-socket file"
            raise SystemExit(f"refusing to replace existing {kind}: {socket_path}")
        socket_path.parent.mkdir(parents=True, exist_ok=True)

        stopping = threading.Event()

        def request_stop(_signum: int, _frame: object) -> None:
            stopping.set()
            if incremental_adapter is not None:
                incremental_adapter.request_stop()

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)

        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    except BaseException:
        if incremental_adapter is not None:
            incremental_adapter.close()
        raise
    socket_identity: tuple[int, int] | None = None
    try:
        listener.bind(str(socket_path))
        socket_metadata = socket_path.lstat()
        socket_identity = (socket_metadata.st_dev, socket_metadata.st_ino)
        os.chmod(socket_path, 0o600)
        listener.listen(0 if args.test_fill_connect_queue_ms else limits.max_connections)
        if args.test_fill_connect_queue_ms:
            blocker = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                blocker.connect(str(socket_path))
                time.sleep(args.test_fill_connect_queue_ms / 1000)
            finally:
                blocker.close()
            return 0
        handler = partial(
            run_session,
            completion_adapter=completion_adapter,
            response_delay_ms=args.test_response_delay_ms,
            tamper_evidence_digest=args.test_tamper_evidence_digest,
            disconnect_on_task=args.test_disconnect_on_task,
            completion_fixture=args.test_completion_fixture,
        )
        session_limit = 1 if args.once else args.test_max_sessions
        if incremental_adapter is None:
            GatewayRuntime(handler, limits).serve(listener, stopping, session_limit=session_limit)
        else:
            incremental_adapter.serve(
                listener, stopping, handler=handler, session_limit=session_limit
            )
    finally:
        listener.close()
        transport_closed = incremental_adapter is None or incremental_adapter.close()
        if socket_identity is not None:
            try:
                current_metadata = socket_path.lstat()
            except FileNotFoundError:
                pass
            else:
                current_identity = (current_metadata.st_dev, current_metadata.st_ino)
                if stat.S_ISSOCK(current_metadata.st_mode) and current_identity == socket_identity:
                    socket_path.unlink()
        if not transport_closed:
            raise RuntimeError("incremental transport did not close")
    return 0


def _load_golden_fixtures(path: Path | None) -> dict[str, str | Completion]:
    if path is None:
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SystemExit("invalid golden fixture file") from error
    if not isinstance(value, dict) or any(
        not isinstance(key, str)
        or len(key) != 64
        or any(character not in "0123456789abcdef" for character in key)
        or not isinstance(output, (str, dict))
        for key, output in value.items()
    ):
        raise SystemExit("golden fixture must map SHA-256 strings to raw text outputs")
    fixtures: dict[str, str | Completion] = {}
    completion_fields = {
        "raw_output",
        "response_model_id",
        "prompt_tokens",
        "output_tokens",
        "finish_reason",
    }
    for digest, output in value.items():
        if isinstance(output, str):
            fixtures[digest] = output
        else:
            if set(output) != completion_fields:
                raise SystemExit("Map golden fixture requires explicit completion metadata")
            fixtures[digest] = Completion(**output)
    return fixtures
