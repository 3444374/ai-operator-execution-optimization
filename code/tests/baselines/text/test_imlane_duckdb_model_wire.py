"""CPU-only strict framing; no model or framework is imported."""

import asyncio
import struct
import time
import unittest
from unittest.mock import patch

from integrations.imlane_duckdb import model_wire as wire


class ModelWireTests(unittest.IsolatedAsyncioTestCase):
    async def read(self, payload):
        reader = asyncio.StreamReader()
        reader.feed_data(payload)
        reader.feed_eof()
        return await wire.read_frame(reader)

    async def test_uint64_frame_roundtrip_exceeds_old_line_and_8mib_bounds(self):
        value = dict(text='雪' * (3 * 1024**2))
        packet = wire.encode_frame(value)
        self.assertGreater(len(packet), 8 * 1024**2)
        self.assertEqual(struct.unpack('!Q', packet[:8])[0], len(packet) - 8)
        self.assertEqual(await self.read(packet), value)

    async def test_strict_utf8_finite_and_unique_json_keys(self):
        for payload in (b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1,"x":2}', b'"\xff"'):
            with self.subTest(payload=payload), self.assertRaises((ValueError, UnicodeError)):
                await self.read(struct.pack('!Q', len(payload)) + payload)
        with self.assertRaises(ValueError):
            wire.encode_frame({'x': float('inf')})

    async def test_incomplete_header_and_payload_are_errors(self):
        for packet in (b'\x00', struct.pack('!Q', 4) + b'{}'):
            with self.subTest(packet=packet), self.assertRaises(asyncio.IncompleteReadError):
                await self.read(packet)

    async def test_write_uses_one_complete_frame_then_drain(self):
        class Writer:
            def write(self, value):
                self.value = value

            async def drain(self):
                self.drained = True

        writer = Writer()
        await wire.write_frame(writer, {'answer': ''})
        self.assertEqual(writer.value, wire.encode_frame({'answer': ''}))
        self.assertTrue(writer.drained)

    def test_sync_receive_recomputes_one_absolute_deadline_for_every_recv(self):
        class Socket:
            def __init__(self):
                self.packet = bytearray(wire.encode_frame({'text': '雪'}))
                self.timeouts = []

            def settimeout(self, value):
                self.timeouts.append(value)

            def recv(self, size):
                value = self.packet[:1]
                del self.packet[:1]
                return bytes(value)

        sock = Socket()
        clock = iter(100 + index / 1000 for index in range(200))
        with patch.object(wire.time, 'monotonic', side_effect=lambda: next(clock)):
            self.assertEqual(wire.receive_frame(sock, 101), {'text': '雪'})
        self.assertGreater(len(sock.timeouts), 8)
        self.assertTrue(all(a > b for a, b in zip(sock.timeouts, sock.timeouts[1:])))

    def test_expired_sync_deadline_sends_no_receive_and_eof_is_not_success(self):
        class Socket:
            def settimeout(self, value):
                pass

            def recv(self, size):
                raise AssertionError('expired receive must not start')

        with self.assertRaises(TimeoutError):
            wire.receive_frame(Socket(), time.monotonic() - 1)
        sock = Socket()
        sock.recv = lambda size: b''
        with self.assertRaises(EOFError):
            wire.receive_frame(sock, time.monotonic() + 1)


if __name__ == '__main__':
    unittest.main()
