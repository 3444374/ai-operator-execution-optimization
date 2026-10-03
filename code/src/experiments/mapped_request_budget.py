"""Local experiment-only counter backed by an already charged, once-claimed unit.

All callers must share one host and a local filesystem. A descriptor reconnects
to live state; it cannot reconstruct an owner or restore charged attempts.
This component selects no tasks and grants no model concurrency.
"""
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
import errno
import fcntl
import hashlib
import mmap
import os
from pathlib import Path
import re
import stat
import struct
import threading
import time
import uuid
import weakref

from src.baselines.common.private_artifacts import new_private_directory
from .attempt_ledger import BudgetError, BudgetExhausted


_HEADER = struct.Struct('!8s16sQQQdB7x')
_SLOT = struct.Struct('!32sQQ')
_MAGIC = b'SMPAY1\0\0'
_OPEN, _DIRTY, _CLOSED = 0, 1, 2
_MAX_REQUESTS = 4096
_HANDLES = weakref.WeakSet()


def _discard_after_fork():
    # Closing a duplicate is safe; LOCK_UN here would unlock the parent's lock.
    for handle in tuple(_HANDLES):
        handle._discard_inherited()
    _HANDLES.clear()


os.register_at_fork(after_in_child=_discard_after_fork)


@dataclass(frozen=True)
class MappedUnitDescriptor:
    directory: str
    state_identity: tuple[int, int]
    lease_identity: tuple[int, int]
    nonce: bytes
    first_attempt: int
    requests: int
    deadline_monotonic: float
    state_bytes: int
    plan_sha256: str


def _open_private(path, identity):
    try:
        fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as error:
        raise BudgetError('mapped unit file unavailable') from error
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or (info.st_dev, info.st_ino) != identity:
        os.close(fd)
        raise BudgetError('mapped unit file identity or permissions differ')
    return fd


class MappedUnitClient:
    """Each process opens its own handles; descriptors alone are transferable."""

    def __init__(self, descriptor):
        self.descriptor, self._pid = descriptor, os.getpid()
        self._thread_lock = threading.Lock()
        self._state_fd = self._lease_fd = self._mapping = None
        try:
            if (type(descriptor) is not MappedUnitDescriptor or not 0 < descriptor.requests <= _MAX_REQUESTS
                    or not _HEADER.size <= descriptor.state_bytes <= _HEADER.size + _MAX_REQUESTS * _SLOT.size):
                raise BudgetError('invalid finite mapped descriptor')
            directory = Path(descriptor.directory)
            self._state_fd = _open_private(directory / 'state', descriptor.state_identity)
            self._lease_fd = _open_private(directory / 'owner', descriptor.lease_identity)
            if os.fstat(self._state_fd).st_size != descriptor.state_bytes:
                raise BudgetError('mapped unit size differs')
            self._mapping = mmap.mmap(self._state_fd, descriptor.state_bytes, access=mmap.ACCESS_WRITE)
            _HANDLES.add(self)
            with self._locked():
                header = self._header()
                items = [_SLOT.unpack_from(self._mapping, offset)
                         for offset in range(_HEADER.size, descriptor.state_bytes, _SLOT.size)]
                if (any(not 0 <= remaining <= original or original < 1 for _, original, remaining in items)
                        or sum(original for _, original, _ in items) != descriptor.requests
                        or sum(remaining for _, _, remaining in items) + header[4] != descriptor.requests):
                    raise BudgetError('mapped request counts differ')
                plan = b''.join(digest + struct.pack('!Q', original) for digest, original, _ in items)
                if hashlib.sha256(plan).hexdigest() != descriptor.plan_sha256 or len({x[0] for x in items}) != len(items):
                    raise BudgetError('mapped request plan differs')
                self._slots = {digest: (_HEADER.size + i * _SLOT.size, original)
                               for i, (digest, original, _) in enumerate(items)}
        except BaseException:
            self._discard_inherited()
            raise

    def __getstate__(self):
        raise TypeError('transfer the mapped descriptor, never a live client')

    @contextmanager
    def _locked(self):
        if os.getpid() != self._pid or self._mapping is None:
            raise BudgetError('mapped client is closed or belongs to another process')
        if not self._thread_lock.acquire(timeout=1):
            raise BudgetError('mapped client lock wait timed out')
        locked = False
        try:
            until = time.monotonic() + 1
            while not locked:
                try:
                    fcntl.flock(self._state_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    locked = True
                except OSError as error:
                    if error.errno not in (errno.EAGAIN, errno.EACCES):
                        raise BudgetError('mapped file lock unavailable') from error
                    if time.monotonic() >= until:
                        raise BudgetError('mapped file lock wait timed out')
                    time.sleep(.0005)
            yield
        finally:
            if locked:
                fcntl.flock(self._state_fd, fcntl.LOCK_UN)
            self._thread_lock.release()

    def _header(self):
        header = _HEADER.unpack_from(self._mapping)
        d = self.descriptor
        if (header[:4] != (_MAGIC, d.nonce, d.first_attempt, d.requests)
                or header[5] != d.deadline_monotonic or not 0 <= header[4] <= d.requests
                or header[6] not in (_OPEN, _DIRTY, _CLOSED)):
            raise BudgetError('mapped unit header differs')
        if header[6] == _DIRTY:
            raise BudgetError('mapped registration outcome is unknown')
        return header

    def _write_header(self, used, status):
        d = self.descriptor
        _HEADER.pack_into(self._mapping, 0, _MAGIC, d.nonce, d.first_attempt,
                          d.requests, used, d.deadline_monotonic, status)

    def _owner_alive(self):
        try:
            fcntl.flock(self._lease_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in (errno.EAGAIN, errno.EACCES):
                return True
            raise BudgetError('mapped owner check unavailable') from error
        fcntl.flock(self._lease_fd, fcntl.LOCK_UN)
        return False

    def reserve(self, request_sha256):
        if type(request_sha256) is not str or not re.fullmatch('[0-9a-f]{64}', request_sha256):
            raise BudgetError('invalid request digest')
        digest = bytes.fromhex(request_sha256)
        with self._locked():
            header = self._header()
            used = header[4]
            if header[6] == _CLOSED or time.monotonic() >= header[5]:
                raise BudgetExhausted('mapped unit closed or expired')
            if not self._owner_alive():
                self._write_header(used, _CLOSED)
                raise BudgetExhausted('mapped unit owner has ended')
            if used == header[3]:
                raise BudgetExhausted('mapped unit budget exhausted')
            if digest not in self._slots:
                raise BudgetError('request digest differs from prepared unit')
            offset, original = self._slots[digest]
            stored, actual, remaining = _SLOT.unpack_from(self._mapping, offset)
            if stored != digest or actual != original or not 0 <= remaining <= original:
                raise BudgetError('mapped request slot differs')
            if remaining == 0:
                raise BudgetExhausted('prepared request multiplicity exhausted')
            self._write_header(used, _DIRTY)
            _SLOT.pack_into(self._mapping, offset, digest, original, remaining - 1)
            self._write_header(used + 1, _OPEN)
            return self.descriptor.first_attempt + used

    @property
    def attempts(self):
        with self._locked():
            return self._header()[4]

    @property
    def remaining(self):
        return self.descriptor.requests - self.attempts

    def snapshot(self):
        with self._locked():
            header = self._header()
            remaining = sum(_SLOT.unpack_from(self._mapping, offset)[2] for offset, _ in self._slots.values())
            if remaining + header[4] != self.descriptor.requests:
                raise BudgetError('mapped final request counts differ')
            return dict(attempts=header[4], remaining=remaining, closed=header[6] == _CLOSED,
                        owner_alive=self._owner_alive(), state_bytes=self.descriptor.state_bytes)

    def close_budget(self):
        with self._locked():
            self._write_header(self._header()[4], _CLOSED)

    def _discard_inherited(self):
        if self._mapping is not None:
            self._mapping.close()
            self._mapping = None
        for name in ('_state_fd', '_lease_fd'):
            fd = getattr(self, name)
            if fd is not None:
                os.close(fd)
                setattr(self, name, None)

    def close(self):
        with self._thread_lock:
            self._discard_inherited()
        _HANDLES.discard(self)


class MappedUnitOwner:
    """Hold the owner lease until close; a crash makes further grants impossible."""

    @classmethod
    def prepare(cls, ledger, unit_id, request_sha256s, directory):
        if (type(request_sha256s) not in (tuple, list) or not 0 < len(request_sha256s) <= _MAX_REQUESTS
                or any(type(d) is not str or not re.fullmatch('[0-9a-f]{64}', d) for d in request_sha256s)):
            raise BudgetError('finite prepared request digests required')
        counts = sorted(Counter(bytes.fromhex(d) for d in request_sha256s).items())
        directory = Path(directory).absolute()
        new_private_directory(directory)
        claimed = ledger.claim_unit(unit_id)
        if claimed.reservation.requests != len(request_sha256s):
            raise BudgetError('prepared request total differs from charged unit')
        owner = cls()
        owner._pid, owner._lease_fd, owner.client = os.getpid(), None, None
        _HANDLES.add(owner)
        state_fd = None
        try:
            owner._lease_fd = os.open(directory / 'owner', os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_CLOEXEC, 0o600)
            fcntl.flock(owner._lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            state_fd = os.open(directory / 'state', os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_CLOEXEC, 0o600)
            nonce = uuid.uuid4().bytes
            body = _HEADER.pack(_MAGIC, nonce, claimed.reservation.first_attempt,
                                claimed.reservation.requests, 0, claimed._deadline, _OPEN)
            body += b''.join(_SLOT.pack(digest, count, count) for digest, count in counts)
            offset = 0
            while offset < len(body):
                written = os.write(state_fd, body[offset:])
                if written <= 0:
                    raise BudgetError('mapped initialization made no progress')
                offset += written
            os.fsync(state_fd)
            state, lease = os.fstat(state_fd), os.fstat(owner._lease_fd)
            descriptor = MappedUnitDescriptor(str(directory), (state.st_dev, state.st_ino),
                (lease.st_dev, lease.st_ino), nonce, claimed.reservation.first_attempt,
                claimed.reservation.requests, claimed._deadline, len(body),
                hashlib.sha256(b''.join(digest + struct.pack('!Q', count) for digest, count in counts)).hexdigest())
            owner.descriptor = descriptor
            owner.client = MappedUnitClient(descriptor)
            return owner
        except BaseException:
            owner._discard_inherited()
            raise
        finally:
            if state_fd is not None:
                os.close(state_fd)

    def __getstate__(self):
        raise TypeError('mapped owner cannot be transferred')

    def _discard_inherited(self):
        if self.client is not None:
            self.client._discard_inherited()
        if self._lease_fd is not None:
            os.close(self._lease_fd)
            self._lease_fd = None

    def close(self):
        if os.getpid() != self._pid:
            raise BudgetError('mapped owner belongs to another process')
        try:
            if self.client is not None and self.client._mapping is not None:
                self.client.close_budget()
        finally:
            self._discard_inherited()
            _HANDLES.discard(self)

    def __enter__(self):
        return self

    def __exit__(self, kind, error, traceback):
        try:
            self.close()
        except BudgetError:
            if error is None:
                raise
