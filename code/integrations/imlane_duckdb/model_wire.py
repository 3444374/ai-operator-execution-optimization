"""One strict JSON document per uint64-length Unix-stream frame."""

import json
import math
import struct
import time


SCHEMA = 'semloom.imlane.duckdb.q3.model_ipc.v1'
READY_SCHEMA = 'semloom.imlane.duckdb.q3.model_ipc_ready.v1'
SHUTDOWN_SCHEMA = 'semloom.imlane.duckdb.q3.model_ipc_shutdown.v1'
_LENGTH = struct.Struct('!Q')


def _reject_constant(value):
    raise ValueError('non-finite JSON value: ' + value)


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate JSON key: ' + key)
        value[key] = item
    return value


def decode_json(payload):
    """Decode finite, unambiguous JSON without replacing malformed UTF-8."""
    return json.loads(payload.decode('utf-8'), parse_constant=_reject_constant, object_pairs_hook=_object)


def encode_frame(value):
    payload = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
    return _LENGTH.pack(len(payload)) + payload


async def read_frame(reader):
    size, = _LENGTH.unpack(await reader.readexactly(_LENGTH.size))
    return decode_json(await reader.readexactly(size))


async def write_frame(writer, value):
    writer.write(encode_frame(value))
    await writer.drain()


def receive_frame(sock, deadline_monotonic):
    """Receive one complete frame within the same absolute wall deadline."""
    if (type(deadline_monotonic) not in (int, float)
            or not math.isfinite(deadline_monotonic) or deadline_monotonic <= 0):
        raise ValueError('a finite positive absolute monotonic deadline is required')

    def exact(size):
        parts = bytearray()
        while len(parts) < size:
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('model IPC receive deadline exceeded')
            sock.settimeout(remaining)
            block = sock.recv(size - len(parts))
            if not block:
                raise EOFError('model IPC connection ended within a frame')
            parts.extend(block)
        return bytes(parts)

    size, = _LENGTH.unpack(exact(_LENGTH.size))
    return decode_json(exact(size))
