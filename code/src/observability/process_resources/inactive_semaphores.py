"""Evidence-bound inactive semaphore cleanup migrated from qualified Q1 lifecycle.

Caller must first prove host idle and no live owned processes, and supply only
ResourceMonitor.remaining_new_shm_names from this exclusive stage. Never scan
and unlink arbitrary old names. Original Linux ownership/time/holder checks stay.
"""
import hashlib
import os
from pathlib import Path

def cleanup_inactive_semaphores(names, before, after, *, shm_root=Path("/dev/shm"), proc_root=Path("/proc")):
    """Remove only observed new POSIX semaphores after all owned work exited."""
    import ctypes
    cleanup = []
    for name in names:
        if not name.startswith('sem.mp-'):
            raise RuntimeError('unexpected remaining SHM type; stop without unlinking')
        path = Path(shm_root) / name
        stat = path.stat()
        if not (stat.st_uid == os.getuid() and stat.st_size == 32
                and before['captured_epoch_s'] <= stat.st_mtime <= after['captured_epoch_s']):
            raise RuntimeError('remaining semaphore is not bound to this round')
        holders = []
        errors = []
        for proc in Path(proc_root).iterdir():
            if not proc.name.isdigit():
                continue
            try:
                if name in (proc/'maps').read_text():
                    holders.append(dict(pid=int(proc.name),kind='mapping'))
                for fd in (proc/'fd').iterdir():
                    try:
                        if name in os.readlink(fd):
                            holders.append(dict(pid=int(proc.name),kind='fd'))
                    except FileNotFoundError:
                        pass
            except (FileNotFoundError,ProcessLookupError):
                pass
            except PermissionError:
                errors.append(int(proc.name))
        evidence = dict(name=name,inode=stat.st_ino,uid=stat.st_uid,size=stat.st_size,
            mtime=stat.st_mtime,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),holders=holders,scan_errors=errors,
            scope='new name within this exclusive round; exact allocating library unproven; native cleanup did not remove it')
        if holders or errors or path.stat().st_ino != stat.st_ino:
            raise RuntimeError('semaphore holder or identity remains unresolved')
        libc = ctypes.CDLL(None,use_errno=True)
        unlink = libc.sem_unlink
        unlink.argtypes = [ctypes.c_char_p]
        unlink.restype = ctypes.c_int
        rc = unlink(('/'+name.removeprefix('sem.')).encode())
        if rc != 0:
            raise OSError(ctypes.get_errno(),'inactive semaphore cleanup failed')
        evidence.update(action='sem_unlink exact inactive owned name',removed=not path.exists())
        cleanup.append(evidence)
    return cleanup
