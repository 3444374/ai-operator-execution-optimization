"""CPU-only ownership/holder/identity checks for migrated Q1 cleanup."""
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
from src.observability.process_resources.inactive_semaphores import cleanup_inactive_semaphores


class SemaphoreTests(unittest.TestCase):
    def test_cleanup_and_refusals(self):
        for case in ('clean', 'old', 'wrong_size', 'wrong_uid', 'holder', 'scan_denied', 'wrong_type'):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);shm=root/'shm';proc=root/'proc';shm.mkdir();proc.mkdir()
                name='sem.mp-fixture' if case!='wrong_type' else 'foreign'
                target=shm/name;target.write_bytes(b'0'*(31 if case=='wrong_size' else 32))
                before=dict(captured_epoch_s=time.time()-10);after=dict(captured_epoch_s=time.time()+10)
                if case=='old':os.utime(target,(1,1))
                if case=='holder':
                    pid=proc/'123';pid.mkdir();(pid/'fd').mkdir();(pid/'maps').write_text(name)
                unlink=Mock(side_effect=lambda _: (target.unlink(),0)[1]);lib=Mock(sem_unlink=unlink)
                with patch('ctypes.CDLL',return_value=lib),patch('os.getuid',return_value=os.getuid()+1 if case=='wrong_uid' else os.getuid()):
                    if case=='scan_denied':
                        pid=proc/'123';pid.mkdir()
                        with patch.object(Path,'read_text',side_effect=PermissionError):
                            with self.assertRaises(RuntimeError):cleanup_inactive_semaphores([name],before,after,shm_root=shm,proc_root=proc)
                    elif case=='clean':
                        receipt=cleanup_inactive_semaphores([name],before,after,shm_root=shm,proc_root=proc)
                        self.assertTrue(receipt[0]['removed']);unlink.assert_called_once_with(b'/mp-fixture')
                    else:
                        with self.assertRaises(RuntimeError):cleanup_inactive_semaphores([name],before,after,shm_root=shm,proc_root=proc)
                    if case!='clean':unlink.assert_not_called();self.assertTrue(target.exists())
