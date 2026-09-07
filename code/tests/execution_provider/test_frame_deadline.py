"""A fragmented frame must obey one deadline, including its length header."""
import socket
import threading
import time
import unittest

from src.execution_provider.wire.framing import encode_frame, read_frame


class FrameDeadlineTests(unittest.TestCase):
    def test_fragmented_header_and_body_share_one_deadline(self):
        reader, writer = socket.socketpair()
        stop = threading.Event()
        frame = encode_frame({'input': 'long enough to outlive the deadline'})

        def drip():
            try:
                for byte in frame:
                    writer.sendall(bytes([byte]))
                    if stop.wait(.02):
                        return
            except OSError:
                pass

        with reader, writer:
            reader.settimeout(.15)
            worker = threading.Thread(target=drip)
            worker.start()
            start = time.monotonic()
            try:
                with self.assertRaises(TimeoutError):
                    read_frame(reader)
                self.assertLess(time.monotonic() - start, .6)
                self.assertEqual(reader.gettimeout(), .15)
            finally:
                stop.set()
                worker.join(timeout=1)
            self.assertFalse(worker.is_alive())

    def test_success_restores_timeout_and_leaves_the_next_frame_intact(self):
        reader, writer = socket.socketpair()
        with reader, writer:
            reader.settimeout(.5)
            first, second = {'input': 'λ'}, {'input': 'next'}
            writer.sendall(encode_frame(first) + encode_frame(second))
            self.assertEqual(read_frame(reader), first)
            self.assertEqual(reader.gettimeout(), .5)
            self.assertEqual(read_frame(reader), second)
