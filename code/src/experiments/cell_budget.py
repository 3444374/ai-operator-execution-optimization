"""Reserve whole experiment units durably; claimed processes spend in memory.

Reservations are never refunded. A claimed unit cannot be claimed again, even
after a crash before its first request. Only allocation and claim touch SQLite.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time

from src.baselines.common.private_artifacts import require_outside_git
from src.experiments.attempt_ledger import AttemptBudget, BudgetError, BudgetExhausted

SCHEMA = 'semloom.cell_budget.v1'
_ID = re.compile(r'[A-Za-z0-9_.-]{1,128}\Z')
_DIGEST = re.compile(r'[0-9a-f]{64}\Z')
_SQLITE_MAX_INT = 2**63 - 1


@dataclass(frozen=True)
class Reservation:
    unit_id: str
    first_attempt: int
    requests: int


class ClaimedUnit:
    """One process owns these already-charged attempts; reserve performs no I/O."""
    def __init__(self, reservation: Reservation, deadline_utc: float):
        self.reservation = reservation
        self._pid = os.getpid()
        self._deadline = time.monotonic() + max(0, deadline_utc - time.time())
        self._used = 0
        self._lock = threading.Lock()

    def __getstate__(self):
        raise TypeError('a claimed unit cannot be transferred or copied')

    @property
    def attempts(self):
        with self._lock:
            return self._used

    @property
    def remaining(self):
        return self.reservation.requests - self.attempts

    def reserve(self, request_sha256: str):
        if not isinstance(request_sha256, str) or not _DIGEST.fullmatch(request_sha256):
            raise BudgetError('invalid request digest')
        with self._lock:
            if os.getpid() != self._pid:
                raise BudgetError('claimed budget belongs to another process')
            if time.monotonic() >= self._deadline:
                raise BudgetExhausted('experiment deadline reached')
            if self._used == self.reservation.requests:
                raise BudgetExhausted('reserved unit budget exhausted')
            attempt = self.reservation.first_attempt + self._used
            self._used += 1
            return attempt


class CellBudgetLedger:
    """A local SQLite ledger for fixed-size units; no implicit initialization."""
    def __init__(self, path: Path, budget: AttemptBudget):
        self.path = Path(path).absolute()
        self.budget = budget
        if budget.limit > _SQLITE_MAX_INT:
            raise ValueError('budget exceeds supported integer range')
        require_outside_git(self.path)
        with self._transaction() as connection:
            self._header(connection)

    @classmethod
    def create(cls, path: Path, budget: AttemptBudget, *, deadline_utc: float):
        path = Path(path).absolute()
        require_outside_git(path)
        if budget.limit > _SQLITE_MAX_INT or not math.isfinite(deadline_utc) or deadline_utc <= time.time():
            raise ValueError('finite future deadline and supported budget are required')
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        with sqlite3.connect(path) as connection:
            connection.execute('PRAGMA synchronous=EXTRA')
            connection.executescript('''
                CREATE TABLE budget (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), schema TEXT NOT NULL,
                    budget_id TEXT NOT NULL, limit_count INTEGER NOT NULL CHECK(limit_count>0),
                    allocated INTEGER NOT NULL CHECK(allocated>=0 AND allocated<=limit_count),
                    deadline REAL NOT NULL);
                CREATE TABLE units (
                    unit_id TEXT PRIMARY KEY, first_attempt INTEGER NOT NULL,
                    requests INTEGER NOT NULL CHECK(requests>0), claimed INTEGER NOT NULL DEFAULT 0
                    CHECK(claimed IN (0,1)));
            ''')
            connection.execute('INSERT INTO budget VALUES(1,?,?,?,?,?)',
                               (SCHEMA, budget.budget_id, budget.limit, 0, deadline_utc))
        return cls(path, budget)

    @contextmanager
    def _transaction(self):
        try:
            if not stat.S_ISREG(self.path.lstat().st_mode):
                raise BudgetError('cell budget must be a regular non-symlink file')
            connection = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True, timeout=5)
            try:
                connection.execute('PRAGMA synchronous=EXTRA')
                connection.execute('BEGIN IMMEDIATE')
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            finally:
                connection.close()
        except (OSError, sqlite3.Error) as error:
            raise BudgetError('cell budget unavailable or corrupt') from error

    def _header(self, connection):
        rows = connection.execute('SELECT schema,budget_id,limit_count,allocated,deadline FROM budget').fetchall()
        if (len(rows) != 1 or rows[0][:3] != (SCHEMA, self.budget.budget_id, self.budget.limit)
                or type(rows[0][3]) is not int or not 0 <= rows[0][3] <= self.budget.limit
                or not isinstance(rows[0][4], (int, float)) or not math.isfinite(rows[0][4])):
            raise BudgetError('cell budget identity or state mismatch')
        return rows[0][3:]

    def _active(self, connection):
        allocated, deadline = self._header(connection)
        if time.time() >= deadline:
            raise BudgetExhausted('experiment deadline reached')
        return allocated, deadline

    def reserve_unit(self, unit_id: str, requests: int):
        if not isinstance(unit_id, str) or not _ID.fullmatch(unit_id) or type(requests) is not int or requests < 1:
            raise ValueError('invalid unit identity or request count')
        with self._transaction() as connection:
            allocated, _ = self._active(connection)
            if connection.execute('SELECT 1 FROM units WHERE unit_id=?', (unit_id,)).fetchone():
                raise BudgetError('unit has already been reserved')
            if requests > self.budget.limit - allocated:
                raise BudgetExhausted('experiment allocation budget exhausted')
            reservation = Reservation(unit_id, allocated + 1, requests)
            connection.execute('INSERT INTO units(unit_id,first_attempt,requests) VALUES(?,?,?)',
                               (unit_id, reservation.first_attempt, requests))
            connection.execute('UPDATE budget SET allocated=? WHERE singleton=1', (allocated + requests,))
        return reservation

    def claim_unit(self, unit_id: str):
        with self._transaction() as connection:
            allocated, deadline = self._active(connection)
            row = connection.execute('SELECT first_attempt,requests,claimed FROM units WHERE unit_id=?',
                                     (unit_id,)).fetchone()
            if (row is None or row[2] != 0 or row[0] < 1 or row[1] < 1
                    or row[0] + row[1] - 1 > allocated):
                raise BudgetError('unit missing, already claimed, or invalid')
            connection.execute('UPDATE units SET claimed=1 WHERE unit_id=?', (unit_id,))
        return ClaimedUnit(Reservation(unit_id, row[0], row[1]), deadline)

    def snapshot(self):
        with self._transaction() as connection:
            allocated, deadline = self._header(connection)
            units = connection.execute('SELECT unit_id,first_attempt,requests,claimed FROM units ORDER BY first_attempt').fetchall()
        return {'schema': SCHEMA, 'budget_id': self.budget.budget_id, 'limit': self.budget.limit,
                'allocated_requests': allocated, 'deadline_utc': deadline,
                'units': [dict(zip(('unit_id', 'first_attempt', 'requests', 'claimed'), row)) for row in units]}
