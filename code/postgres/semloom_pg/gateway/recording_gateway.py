"""Run the standalone SemLoom Unix-domain-socket recording gateway."""

from __future__ import annotations

import argparse
import os
import signal
import socket
import stat
from pathlib import Path

from protocol import run_recording_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", type=Path, required=True)
    parser.add_argument("--once", action="store_true", help="serve one session and exit")
    parser.add_argument("--test-response-delay-ms", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--test-tamper-evidence-digest", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--test-disconnect-on-task", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.test_response_delay_ms < 0:
        raise SystemExit("--test-response-delay-ms must be non-negative")
    socket_path = args.socket.resolve()
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
        listener.listen()
        listener.settimeout(0.25)
        while not stopping:
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            run_recording_session(
                connection,
                response_delay_ms=args.test_response_delay_ms,
                tamper_evidence_digest=args.test_tamper_evidence_digest,
                disconnect_on_task=args.test_disconnect_on_task,
            )
            if args.once:
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


if __name__ == "__main__":
    raise SystemExit(main())
