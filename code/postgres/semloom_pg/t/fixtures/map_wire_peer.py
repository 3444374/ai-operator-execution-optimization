"""Bounded local completion/fault peer for generated Map PostgreSQL tests."""
import argparse
import hashlib
import json
from pathlib import Path
import socket
import struct
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from src.execution_provider.completion import Completion
from src.execution_provider.adapters.semantic_session import run_v5_session
from src.execution_provider.wire.framing import encode_frame
from src.execution_provider.wire.v5 import GOLDEN_EXECUTION_ID


def evidence(message, raw_output=None):
    def text(value):
        value = value.encode('utf-8') if isinstance(value, str) else value
        return struct.pack('!I', len(value)) + value
    identity = b''.join(message[key].encode('ascii') for key in (
        'semantic_spec_digest', 'physical_algorithm_digest', 'provider_execution_digest', 'semantic_payload_digest'))
    return hashlib.sha256(b'semloom-completion-v5\0' + identity + struct.pack('!Q', int(message['sequence']))
        + text(message['raw_output'] if raw_output is None else raw_output) + text(message['finish_reason']) + text(message['response_model_id'])
        + struct.pack('!QQ', int(message['prompt_tokens']), int(message['output_tokens']))).hexdigest()


class FixtureAdapter:
    model_id = None

    def __init__(self, output):
        self.output = output
        self.calls = 0

    def execution_id_for(self, version):
        return GOLDEN_EXECUTION_ID if version == 5 else None

    def complete(self, request):
        self.calls += 1
        return Completion(self.output, request.model_id, 17, 1, 'stop')


class FaultConnection:
    def __init__(self, connection, fault):
        self.connection, self.fault = connection, fault

    def recv(self, length):
        return self.connection.recv(length)

    def close(self):
        self.connection.close()

    def sendall(self, frame):
        message = json.loads(frame[4:])
        target, _, mutation = self.fault.partition('-')
        expected = 'opened' if target == 'open' else 'completion'
        if message['type'] != expected:
            self.connection.sendall(frame)
            return
        if target == 'open' and mutation == 'output-too-large':
            message = dict(type='error', protocol_version=5, sequence=None, code='OUTPUT_TOO_LARGE')
        elif target == 'error':
            message = dict(type='error', protocol_version=5, sequence=message['sequence'], code='OUTPUT_TOO_LARGE')
        if mutation == 'version':
            message['protocol_version'] = 4
        elif mutation == 'extra':
            message['unexpected'] = 'private-provider-text'
        elif mutation == 'missing':
            del message['code' if target == 'error' else 'provider_execution_digest']
        elif mutation == 'sequence':
            message['sequence'] = '1'
        elif mutation == 'sequence-number':
            message['sequence'] = 0
        elif mutation == 'code':
            message['code'] = 'NOT_ALLOWED'
        elif mutation in ('max_input_bytes', 'max_output_bytes', 'max_frame_bytes', 'max_inflight_tasks'):
            message[mutation] -= 1
        elif mutation in ('semantic_spec_digest', 'physical_algorithm_digest', 'provider_execution_digest',
                          'semantic_payload_digest', 'completion_evidence_digest'):
            message[mutation] = '0' * 64
        elif mutation == 'model':
            message['response_model_id'] = 'wrong-model'
        elif mutation == 'usage-number':
            message['output_tokens'] = 1
        elif mutation == 'usage-overflow':
            message['prompt_tokens'] = '18446744073709551616'
        elif mutation == 'usage-total-overflow':
            message['prompt_tokens'] = '18446744073709551615'
        elif mutation == 'usage-leading-zero':
            message['prompt_tokens'] = '017'
        elif mutation == 'usage-budget':
            message['output_tokens'] = '129'
        elif mutation == 'null':
            message['raw_output'] = None
        elif mutation == 'finish-empty':
            message['finish_reason'] = ''
        elif mutation == 'finish-long':
            message['finish_reason'] = 'x' * 33
        elif mutation in ('finish-length', 'finish-tool', 'finish-space', 'over-output', 'over-output-finish', 'over-output-evidence'):
            if mutation.startswith('over-output'):
                message['raw_output'] = 'x' * 65537
            message['finish_reason'] = {'finish-tool': 'tool_calls', 'finish-space': 'stop ',
                'over-output': 'stop', 'over-output-evidence': 'stop'}.get(mutation, 'length')
            message['completion_evidence_digest'] = evidence(message)
            if mutation == 'over-output-evidence':
                message['completion_evidence_digest'] = '0' * 64
        elif mutation == 'fractional':
            message['protocol_version'] = 5.4
        elif mutation == 'escaped-nul':
            message['raw_output'] = '\0'
        invalid_output = b'\xff' + b'x' * 65536
        if mutation == 'utf8-over-output':
            message['completion_evidence_digest'] = evidence(message, invalid_output)
        raw = encode_frame(message)[4:]
        if mutation == 'duplicate':
            raw = b'{"protocol_version":5,' + raw[1:]
        elif mutation == 'raw-nul':
            raw += b'\0private-provider-text'
        elif mutation == 'utf8':
            raw = raw.replace(b'hello', b'\xff')
        elif mutation == 'utf8-over-output':
            raw = raw.replace(b'hello', invalid_output)
        elif mutation == 'array':
            raw = b'[]'
        self.connection.sendall(struct.pack('!I', len(raw)) + raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--socket', type=Path, required=True)
    parser.add_argument('--fault', default='none')
    parser.add_argument('--output', default='hello')
    parser.add_argument('--output-length', type=int)
    args = parser.parse_args()
    if args.socket.exists():
        raise SystemExit('refusing to replace a filesystem entry')
    output = args.output if args.output_length is None else 'x' * args.output_length
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(str(args.socket))
        listener.listen(1)
        try:
            connection, _ = listener.accept()
            adapter = FixtureAdapter(output)
            run_v5_session(FaultConnection(connection, args.fault), adapter)
            if args.fault.startswith('open-') and adapter.calls != 0:
                raise AssertionError('invalid open must not execute a task')
        finally:
            args.socket.unlink()


if __name__ == '__main__':
    main()
