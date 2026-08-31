"""Run the standalone SemLoom versioned Unix-domain-socket provider gateway."""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import stat
import time
from pathlib import Path

from .adapters.golden import run_golden_session
from .adapters.recording import run_recording_session
from .wire.framing import ProtocolError, read_frame


def parse_args() -> argparse.Namespace:
    """Parse the versioned provider gateway command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--once", action="store_true", help="serve one session and exit")
    parser.add_argument(
        "--golden-fixture",
        type=Path,
        help="payload-digest to raw-output JSON object for wire-v3 tests",
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
    return parser.parse_args()


def main() -> int:
    """Bind one UDS listener and serve versioned provider sessions until stopped."""
    args = parse_args()
    if args.test_response_delay_ms < 0:
        raise SystemExit("--test-response-delay-ms must be non-negative")
    if args.test_fill_connect_queue_ms < 0:
        raise SystemExit("--test-fill-connect-queue-ms must be non-negative")
    if args.test_max_sessions < 0:
        raise SystemExit("--test-max-sessions must be non-negative")
    socket_path = args.socket.resolve()
    golden_fixtures = _load_golden_fixtures(args.golden_fixture)
    if socket_path.exists():
        mode = socket_path.stat().st_mode
        kind = "socket" if stat.S_ISSOCK(mode) else "non-socket file"
        raise SystemExit(f"refusing to replace existing {kind}: {socket_path}")
    socket_path.parent.mkdir(parents=True, exist_ok=True)

    stopping = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    socket_identity: tuple[int, int] | None = None
    try:
        listener.bind(str(socket_path))
        socket_metadata = socket_path.lstat()
        socket_identity = (socket_metadata.st_dev, socket_metadata.st_ino)
        os.chmod(socket_path, 0o600)
        listener.listen(0 if args.test_fill_connect_queue_ms else socket.SOMAXCONN)
        if args.test_fill_connect_queue_ms:
            blocker = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                blocker.connect(str(socket_path))
                time.sleep(args.test_fill_connect_queue_ms / 1000)
            finally:
                blocker.close()
            return 0
        listener.settimeout(0.25)
        served_sessions = 0
        while not stopping:
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            _run_session(
                connection,
                golden_fixtures=golden_fixtures,
                response_delay_ms=args.test_response_delay_ms,
                tamper_evidence_digest=args.test_tamper_evidence_digest,
                disconnect_on_task=args.test_disconnect_on_task,
                completion_fixture=args.test_completion_fixture,
            )
            served_sessions += 1
            if args.once or (
                args.test_max_sessions > 0
                and served_sessions >= args.test_max_sessions
            ):
                break
    finally:
        listener.close()
        if socket_identity is not None:
            try:
                current_metadata = socket_path.lstat()
            except FileNotFoundError:
                pass
            else:
                current_identity = (current_metadata.st_dev, current_metadata.st_ino)
                if (
                    stat.S_ISSOCK(current_metadata.st_mode)
                    and current_identity == socket_identity
                ):
                    socket_path.unlink()
    return 0


def _load_golden_fixtures(path: Path | None) -> dict[str, str]:
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
        or not isinstance(output, str)
        for key, output in value.items()
    ):
        raise SystemExit("golden fixture must map SHA-256 strings to raw text outputs")
    return value


def _run_session(
    connection: socket.socket,
    *,
    golden_fixtures: dict[str, str],
    response_delay_ms: int,
    tamper_evidence_digest: bool,
    disconnect_on_task: bool,
    completion_fixture: str | None,
) -> None:
    try:
        opened = read_frame(connection)
    except ProtocolError:
        connection.close()
        return
    if opened is None:
        connection.close()
        return
    protocol_version = opened.get("protocol_version")
    if type(protocol_version) is int and protocol_version == 3:
        run_golden_session(
            connection,
            golden_fixtures,
            open_message=opened,
            response_delay_ms=response_delay_ms,
            tamper_evidence_digest=tamper_evidence_digest,
            disconnect_on_task=disconnect_on_task,
            completion_fixture=completion_fixture,
        )
        return
    run_recording_session(
        connection,
        open_message=opened,
        response_delay_ms=response_delay_ms,
        tamper_evidence_digest=tamper_evidence_digest,
        disconnect_on_task=disconnect_on_task,
        completion_fixture=completion_fixture,
    )
