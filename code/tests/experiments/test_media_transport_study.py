import hashlib
import multiprocessing as mp
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.experiments.media_transport_study import _run_arm


def stalled_receiver(connection):
    connection.send(dict(ready=True))
    # Finite fallback also bounds the regression on the original implementation.
    time.sleep(2)
    connection.close()


def closing_receiver(connection):
    connection.send(dict(ready=True))
    connection.recv_bytes()
    connection.close()


class MediaTransportStudyTests(unittest.TestCase):
    def assert_closed(self, children_before):
        self.assertEqual({process.pid for process in mp.active_children()}, children_before)
        self.assertFalse([thread.name for thread in threading.enumerate()
                          if thread.name.startswith('image-transport-send')])

    def test_backpressured_send_obeys_deadline_and_reaps_worker_and_sender(self):
        children_before = {process.pid for process in mp.active_children()}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'encoded.bin'
            path.write_bytes(b'x' * (2 * 1024**2))
            started = time.monotonic()
            with patch('src.experiments.media_transport_study._worker', stalled_receiver):
                with self.assertRaisesRegex(TimeoutError, 'operation time'):
                    _run_arm([path], [((1, 1), 'unused')], kind='encoded', materialization='stream',
                             workers=1, timeout_s=0.8)
            self.assertLess(time.monotonic() - started, 2)
        self.assert_closed(children_before)

    def test_worker_eof_remains_the_primary_error_after_cleanup(self):
        children_before = {process.pid for process in mp.active_children()}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'encoded.bin'
            path.write_bytes(b'fixture')
            with patch('src.experiments.media_transport_study._worker', closing_receiver):
                with self.assertRaises(EOFError) as raised:
                    _run_arm([path, path], [((1, 1), 'unused')] * 2, kind='encoded',
                             materialization='stream', workers=2, timeout_s=5)
            self.assertFalse(getattr(raised.exception, '__notes__', ()))
        self.assert_closed(children_before)

    def test_successful_modes_preserve_pixels_occurrences_and_frame_accounting(self):
        from PIL import Image
        children_before = {process.pid for process in mp.active_children()}
        with tempfile.TemporaryDirectory() as directory:
            paths, references = [], []
            for index, color in enumerate(((10, 20, 30), (90, 80, 70), (100, 110, 120))):
                path = Path(directory) / f'{index}.png'
                with Image.new('RGB', (17 + index, 13), color) as image:
                    image.save(path)
                    references.append((image.size, hashlib.sha256(image.tobytes()).hexdigest()))
                paths.append(path)
            for kind in ('encoded', 'rgb'):
                for materialization in ('stream', 'stage'):
                    with self.subTest(kind=kind, materialization=materialization):
                        result = _run_arm(paths, references, kind=kind, materialization=materialization,
                                          workers=2, timeout_s=5)
                        self.assertTrue(result['matched_pixels'])
                        self.assertEqual([event['index'] for event in result['events']], [0, 1, 2])
                        self.assertEqual(result['actual_frame_bytes'],
                                         sum(event['received_bytes'] for event in result['events']))
                        self.assertTrue(all(event['received_bytes'] == event['sent_bytes']
                                            for event in result['events']))
                        self.assert_closed(children_before)


if __name__ == '__main__':
    unittest.main()
