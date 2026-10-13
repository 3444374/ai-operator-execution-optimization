"""Durable per-phase counters for the experiment's append-only send records.

The caller holds BEGIN IMMEDIATE. INSERT triggers update counters in that same
transaction; neither a returned response nor an error refunds a reservation.
Full history verification belongs to initialization or explicit offline migration.
"""
import json
import os
import tempfile
from pathlib import Path
PHASES = ('software', 'quality', 'single', 'dual')
MODEL = '__model__'
SCOPES = PHASES + (MODEL,)
KINDS = ('gateway', 'router', 'model', 'fixture')
TABLE = 'send_totals'
TRIGGERS = {
    'send_totals_insert': """CREATE TRIGGER send_totals_insert AFTER INSERT ON sends BEGIN
        SELECT CASE WHEN NEW.phase NOT IN ('software','quality','single','dual')
            OR NEW.kind NOT IN ('gateway','router','model','fixture')
            OR NEW.phase IS NULL OR NEW.kind IS NULL THEN RAISE(ABORT,'unknown send phase or kind') END;
        UPDATE send_totals SET total=total+1, logical=logical+(NEW.kind='gateway') WHERE scope=NEW.phase;
        UPDATE send_totals SET total=total+1, logical=logical+(NEW.kind='gateway')
            WHERE scope='__model__' AND NEW.phase!='software';
    END""",
    'send_records_no_delete': """CREATE TRIGGER send_records_no_delete BEFORE DELETE ON sends BEGIN
        SELECT RAISE(ABORT,'send records cannot be refunded');
    END""",
    'send_records_identity': """CREATE TRIGGER send_records_identity
        BEFORE UPDATE OF phase,kind,sequence ON sends BEGIN
        SELECT RAISE(ABORT,'reserved send identity cannot change');
    END""",
}


def check_identity(root, database, *, initializing=False):
    """A missing or replaced initialized database never creates a fresh budget."""
    marker = Path(root)/'send-ledger-identity.json'
    if not marker.exists():
        if initializing:
            return False
        raise RuntimeError('send ledger initialization identity is missing')
    expected = json.loads(marker.read_text())
    if not Path(database).exists():
        raise RuntimeError('initialized send ledger database is missing')
    stat = Path(database).stat()
    if expected != {'schema': 1, 'device': stat.st_dev, 'inode': stat.st_ino}:
        raise RuntimeError('initialized send ledger database was replaced')
    return True


def mark_initialized(root, database):
    """Publish a durable file identity after the SQLite initialization commit."""
    root = Path(root)
    marker = root/'send-ledger-identity.json'
    stat = Path(database).stat()
    payload = json.dumps({'schema': 1, 'device': stat.st_dev, 'inode': stat.st_ino})+'\n'
    descriptor, temporary = tempfile.mkstemp(prefix='.send-ledger-identity-', dir=root)
    try:
        with os.fdopen(descriptor, 'w') as stream:
            stream.write(payload);stream.flush();os.fsync(stream.fileno())
        try:
            os.link(temporary, marker)
        except FileExistsError:
            check_identity(root, database)
        directory = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        os.unlink(temporary)


def _require_transaction(connection):
    if not connection.in_transaction:
        raise RuntimeError('send counters require the caller write transaction')


def _history(connection):
    values = {scope: [0, 0] for scope in SCOPES}
    for phase, kind, count in connection.execute('SELECT phase,kind,count(*) FROM sends GROUP BY phase,kind'):
        if phase not in PHASES or kind not in KINDS:
            raise RuntimeError('unknown phase or kind in retained send history')
        values[phase][0] += count
        values[phase][1] += count if kind == 'gateway' else 0
        if phase != 'software':
            values[MODEL][0] += count
            values[MODEL][1] += count if kind == 'gateway' else 0
    return {key: tuple(value) for key, value in values.items()}


def snapshot(connection):
    """Read five fixed rows and the indexed sequence maximum; no history scan."""
    _require_transaction(connection)
    rows = list(connection.execute('SELECT scope,total,logical FROM send_totals'))
    values = {scope: (total, logical) for scope, total, logical in rows}
    if len(rows) != len(SCOPES) or set(values) != set(SCOPES):
        raise RuntimeError('missing or extra send counter scope; refuse reset')
    if any(type(number) is not int or number < 0 for pair in values.values() for number in pair):
        raise RuntimeError('invalid send counter type or value')
    if any(logical > total for total, logical in values.values()):
        raise RuntimeError('logical send count exceeds transport count')
    if values[MODEL] != tuple(sum(values[phase][index] for phase in PHASES if phase != 'software') for index in (0, 1)):
        raise RuntimeError('model and phase send counts disagree')
    high = connection.execute('SELECT coalesce(max(sequence),0) FROM sends').fetchone()[0]
    if sum(values[phase][0] for phase in PHASES) != high:
        raise RuntimeError('send count and append sequence disagree')
    return values


def initialize_counts(connection, *, migrate=False, expected_rows=None):
    """Initialize an empty ledger or explicitly migrate a checked offline copy.

    Existing counters are verified, never rebuilt automatically. The caller must
    supply expected_rows for legacy nonempty migration while holding its write
    transaction. This does not alter budgets, clocks or any retained send row.
    """
    _require_transaction(connection)
    exists = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='send_totals'").fetchone()
    history = _history(connection)
    total = sum(history[phase][0] for phase in PHASES)
    if migrate and (type(expected_rows) is not int or expected_rows != total):
        raise RuntimeError('offline migration snapshot changed')
    high = connection.execute('SELECT coalesce(max(sequence),0) FROM sends').fetchone()[0]
    if high != total:
        raise RuntimeError('retained send sequence has gaps; refuse migration')
    if not exists:
        if total and (not migrate or type(expected_rows) is not int or expected_rows != total):
            raise RuntimeError('legacy send history requires explicit checked offline migration')
        connection.execute("""CREATE TABLE send_totals (scope TEXT PRIMARY KEY,
            total INTEGER NOT NULL CHECK(typeof(total)='integer' AND total>=0),
            logical INTEGER NOT NULL CHECK(typeof(logical)='integer' AND logical>=0 AND logical<=total))""")
        connection.executemany('INSERT INTO send_totals VALUES(?,?,?)', [(scope, *history[scope]) for scope in SCOPES])
        for sql in TRIGGERS.values():
            connection.execute(sql)
    actual = snapshot(connection)
    if actual != history:
        raise RuntimeError('retained history and durable send counters disagree; refuse reset')
    for name, expected in TRIGGERS.items():
        row = connection.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (name,)).fetchone()
        if row is None or row[0].strip() != expected.strip():
            raise RuntimeError('send counter trigger identity changed')
    return actual


def verify_increment(connection, before, phase, kind, sequence):
    """Confirm INSERT charged exactly once before the transaction may commit."""
    expected = dict(before)
    for scope in (phase, MODEL) if phase != 'software' else (phase,):
        total, logical = expected[scope]
        expected[scope] = (total + 1, logical + int(kind == 'gateway'))
    if sequence != sum(before[phase][0] for phase in PHASES) + 1 or snapshot(connection) != expected:
        raise RuntimeError('send insert and durable counter update were not atomic')
