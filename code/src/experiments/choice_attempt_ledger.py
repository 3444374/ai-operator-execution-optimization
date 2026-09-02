"""Durably reserve a shared 100-attempt choice smoke budget before HTTP I/O.

Only experiment runners use this ledger. A failed or uncertain attempt is never
refunded; a missing or corrupt existing ledger cannot silently create a new one.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import stat


MAX_ATTEMPTS = 100
MAX_LEDGER_BYTES = 65536
HEADER = {'schema_version': 1, 'budget_id': 'semloom.choice.4c.v1', 'limit': MAX_ATTEMPTS}
_SHA256 = re.compile(r'[0-9a-f]{64}\Z')


class BudgetError(RuntimeError):
    """The authoritative attempt history could not be verified or persisted."""


class BudgetExhausted(BudgetError):
    """All permitted attempts have been reserved."""


@contextmanager
def observe_http_posts(ledger: 'AttemptLedger', record):
    """Reserve and observe POST bytes in one isolated qualification process.

    Headers are never recorded. The original HTTP implementation sends the
    unchanged body only after reservation and the observer have succeeded.
    """
    original = http.client.HTTPConnection.request
    if getattr(original, '_choice_observer', False):
        raise BudgetError('nested HTTP budget observers are not supported')

    def request(connection, method, url, body=None, headers=None, *, encode_chunked=False):
        if method.upper() == 'POST':
            payload = body.encode('utf-8') if isinstance(body, str) else body
            if not isinstance(payload, bytes):
                raise BudgetError('smoke POST must have a complete byte body')
            attempt = ledger.reserve(hashlib.sha256(payload).hexdigest())
            record(attempt, payload)
        return original(connection, method, url, body, headers or {}, encode_chunked=encode_chunked)

    request._choice_observer = True
    http.client.HTTPConnection.request = request
    try:
        yield
    finally:
        http.client.HTTPConnection.request = original


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate ledger field')
        result[key] = value
    return result


class AttemptLedger:
    """Append-only, flock-serialized reservations shared by restarted runners."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.attempts

    @classmethod
    def create(cls, path: Path):
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, 'w', encoding='ascii') as handle:
            handle.write(json.dumps(HEADER, separators=(',', ':')) + '\n')
            handle.flush()
            os.fsync(handle.fileno())
        return cls(path)

    @contextmanager
    def _locked(self, exclusive):
        try:
            descriptor = os.open(self.path, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(descriptor, 'r+', encoding='ascii', newline='') as handle:
                if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                    raise BudgetError('attempt ledger must be a regular file')
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
                yield handle
        except (OSError, UnicodeError, ValueError) as error:
            raise BudgetError('attempt ledger is unavailable or invalid') from error

    def _count(self, handle):
        content = handle.read(MAX_LEDGER_BYTES + 1)
        if not content.endswith('\n') or len(content) > MAX_LEDGER_BYTES:
            raise BudgetError('incomplete or oversized attempt history')
        records = [json.loads(line, object_pairs_hook=_unique_object) for line in content.splitlines()]
        if not records or records[0] != HEADER:
            raise BudgetError('attempt budget identity mismatch')
        if type(records[0].get('schema_version')) is not int or type(records[0].get('limit')) is not int:
            raise BudgetError('invalid attempt budget header')
        if len(records) - 1 > MAX_ATTEMPTS:
            raise BudgetError('attempt history exceeds its limit')
        for sequence, record in enumerate(records[1:], 1):
            if (type(record) is not dict or set(record) != {'attempt', 'request_sha256'}
                    or type(record['attempt']) is not int or record['attempt'] != sequence
                    or type(record['request_sha256']) is not str
                    or _SHA256.fullmatch(record['request_sha256']) is None):
                raise BudgetError('invalid attempt history')
        return len(records) - 1

    @property
    def attempts(self):
        with self._locked(False) as handle:
            return self._count(handle)

    def reserve(self, request_sha256: str):
        if type(request_sha256) is not str or _SHA256.fullmatch(request_sha256) is None:
            raise BudgetError('invalid request digest')
        with self._locked(True) as handle:
            count = self._count(handle)
            if count == MAX_ATTEMPTS:
                raise BudgetExhausted('choice smoke attempt budget exhausted')
            handle.seek(0, os.SEEK_END)
            handle.write(json.dumps({'attempt': count + 1, 'request_sha256': request_sha256},
                                    separators=(',', ':')) + '\n')
            handle.flush()
            os.fsync(handle.fileno())
            return count + 1
