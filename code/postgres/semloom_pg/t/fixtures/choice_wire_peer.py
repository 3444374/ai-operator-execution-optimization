"""One local fault-injecting wire peer for PostgreSQL choice contract tests."""
import argparse
import json
from pathlib import Path
import socket
import struct
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from src.execution_provider.adapters.golden import GoldenCompletionAdapter
from src.execution_provider.adapters.semantic_session import run_v3_session, run_v4_session
from src.execution_provider.wire.framing import encode_frame


class FaultConnection:
    def __init__(self, connection, fault):
        self.connection = connection
        self.fault = fault

    def recv(self, length):
        return self.connection.recv(length)

    def close(self):
        self.connection.close()

    def sendall(self, frame):
        message = json.loads(frame[4:])
        target, _, mutation = self.fault.partition('-')
        expected_type = 'opened' if target == 'open' else 'completion'
        if message['type'] != expected_type:
            self.connection.sendall(frame)
            return
        if target == 'error':
            message = dict(type='error', protocol_version=4, sequence=message['sequence'],
                           code='MODEL_REQUEST_REJECTED')
        if mutation == 'profile':
            message['generation_profile_digest'] = '0' * 64
        elif mutation == 'version':
            message['protocol_version'] = 3
        elif mutation == 'extra':
            message['unexpected'] = 'not-public'
        elif mutation == 'missing':
            del message['code' if target == 'error' else 'generation_profile_digest']
        elif mutation == 'sequence':
            message['sequence'] = None if target == 'error' else '1'
        elif mutation == 'code':
            message['code'] = 'UNKNOWN_CODE'
        elif mutation == 'evidence':
            message['completion_evidence_digest'] = '0' * 64
        elif mutation == 'fractional':
            message['protocol_version'] = 4.4
        elif mutation == 'escaped-nul':
            message['raw_output'] = '\0'
        raw = encode_frame(message)[4:]
        if mutation == 'duplicate':
            raw = b'{"protocol_version":4,' + raw[1:]
        elif mutation == 'raw-nul':
            raw += b'\0not-public'
        self.connection.sendall(struct.pack('!I', len(raw)) + raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', type=Path, required=True)
    parser.add_argument('--golden-fixture', type=Path, required=True)
    parser.add_argument('--fault', required=True)
    args = parser.parse_args()
    if args.socket.exists():
        raise SystemExit('refusing to replace a filesystem entry')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(str(args.socket))
        listener.listen(1)
        try:
            connection, _ = listener.accept()
            adapter = GoldenCompletionAdapter(json.loads(args.golden_fixture.read_text()))
            runner = run_v3_session if args.fault == 'legacy' else run_v4_session
            runner(FaultConnection(connection, args.fault), adapter)
        finally:
            args.socket.unlink()


if __name__ == '__main__':
    main()
