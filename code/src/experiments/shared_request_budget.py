"""Process-shared POST accounting for a once-claimed native benchmark unit."""
from dataclasses import dataclass
from pathlib import Path
import re

from .attempt_ledger import AttemptBudget, BudgetError, BudgetExhausted
from .cell_budget import CellBudgetLedger


@dataclass(frozen=True)
class SharedClaimedUnit:
    path: Path
    budget: AttemptBudget
    unit_id: str

    def _state(self, connection, allocated):
        row = connection.execute('''SELECT first_attempt,requests,claimed FROM units
            JOIN shared_units USING(unit_id) WHERE unit_id=?''', (self.unit_id,)).fetchone()
        if row is None or row[2] != 1 or row[0] < 1 or row[1] < 1 or row[0]+row[1]-1 > allocated:
            raise BudgetError('shared unit is not a valid exclusive claim')
        count, last = connection.execute('SELECT count(*),max(sequence) FROM shared_requests WHERE unit_id=?',
                                        (self.unit_id,)).fetchone()
        if count > row[1] or (count and last != count):
            raise BudgetError('shared request history invalid')
        return row[0], row[1], count

    def reserve(self, request_sha256):
        if not isinstance(request_sha256, str) or not re.fullmatch('[0-9a-f]{64}', request_sha256):
            raise BudgetError('invalid request digest')
        ledger = CellBudgetLedger(self.path, self.budget)
        with ledger._transaction() as connection:
            allocated, _ = ledger._active(connection)
            if connection.execute('SELECT 1 FROM closed_shared_units WHERE unit_id=?',(self.unit_id,)).fetchone():
                raise BudgetExhausted('shared query unit has been closed')
            first, maximum, used = self._state(connection, allocated)
            if used == maximum:
                raise BudgetExhausted('shared unit budget exhausted')
            connection.execute('INSERT INTO shared_requests VALUES(?,?,?)',
                               (self.unit_id, used+1, request_sha256))
        return first+used

    @property
    def attempts(self):
        ledger = CellBudgetLedger(self.path, self.budget)
        with ledger._transaction() as connection:
            allocated, _ = ledger._header(connection)
            return self._state(connection, allocated)[2]

    @property
    def remaining(self):
        ledger = CellBudgetLedger(self.path, self.budget)
        with ledger._transaction() as connection:
            allocated, _ = ledger._header(connection)
            _, maximum, used = self._state(connection, allocated)
            return maximum-used
