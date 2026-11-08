"""Finite serialized delivery declarations for one ordinary prepared Filter.

Counts describe a preflighted source, never a request-window policy. One complete
compact-JSON array is charged here; independent handoff copies need separate
grants. Python/Ray RSS, arbitrary audit copies and model memory are not covered.
No task ownership, frame cache, model allocation or result mutation lives here.
"""
from __future__ import annotations

from bisect import bisect_left
from dataclasses import asdict, dataclass, field
import hashlib
import sys

from src.scheduling.core.session_contract import BatchKey, BatchMember, SessionSpec, TaskKey, UINT64_MAX
from src.semantic_methods.driver import RowIdentity
from src.semantic_methods.lotus.record_types import RecordOccurrence, stage_identity
from src.semantic_methods.lotus.records import CallLedger, identity
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS

from .records_model_adapter import _check_result_capacity, _encode


def _positive(value, name):
    if type(value) is not int or value <= 0:
        raise ValueError(name + ' must be a positive integer')
    return value


def _encoded(value):
    # The Python byte-container limit is not a service grant. Identity sizes
    # are validated by the existing SessionSpec and Method RowIdentity types.
    return _encode(value, sys.maxsize)


def _array_bytes(item_bytes, count):
    punctuation = len(_encoded([]))
    separator = len(_encoded([None, None])) - len(_encoded([None])) - len(_encoded(None))
    return item_bytes + punctuation + max(count - 1, 0) * separator


def _expected(query_id, occurrences):
    if type(query_id) is not str or not query_id.strip():
        raise ValueError('ordinary delivery requires its actual query identity')
    SessionSpec(query_id, 'chain-section-0', 'lotus-record-prepared-v1')
    values = []
    rows = set()
    for occurrence in occurrences:
        canonical = identity(query_id, occurrence['row_id'], occurrence['source_position'])
        if any(type(occurrence[n]) is not type(canonical[n]) or occurrence[n] != canonical[n]
               for n in IDENTITY_FIELDS):
            raise ValueError('delivery supports one ordinary Filter with its canonical query identity')
        RowIdentity(occurrence['source_position'], occurrence['row_id'])
        if occurrence['row_id'] in rows:
            raise ValueError('ordinary delivery row identities must be distinct')
        rows.add(occurrence['row_id'])
        values.append(canonical)
    CallLedger(values, max(1, len(values)))
    return tuple(sorted(tuple(v[n] for n in IDENTITY_FIELDS) for v in values))


def _chain_identity(expected):
    record = RecordOccurrence(expected['row_id'], expected['source_position'],
                              expected['source_position'], {})
    return stage_identity(expected['query_id'], record, 'filter')


def _projection_metadata(expected, count):
    return dict(expected, chain_call_id=_chain_identity(expected)['call_id'],
        parent_call_ids=[], core_task_key=asdict(TaskKey(UINT64_MAX, UINT64_MAX)),
        transport_batch=asdict(BatchMember(BatchKey(UINT64_MAX, UINT64_MAX), count - 1, count)),
        decision=False)


@dataclass(frozen=True)
class OrdinaryDeliveryDeclaration:
    """Immutable source identity and byte declarations, in canonical identity order."""

    query_id: str
    occurrences: tuple[tuple, ...]
    item_result_bytes: int
    item_upper_bounds: tuple[int, ...] = field(init=False)
    identity_sha256: str = field(init=False)
    array_bytes: int = field(init=False)

    def __post_init__(self):
        _positive(self.item_result_bytes, 'model result bytes')
        if type(self.occurrences) is not tuple or any(type(v) is not tuple
                or len(v) != len(IDENTITY_FIELDS) for v in self.occurrences):
            raise ValueError('delivery occurrences must be immutable complete identity tuples')
        expected = [dict(zip(IDENTITY_FIELDS, v)) for v in self.occurrences]
        canonical = _expected(self.query_id, expected)
        object.__setattr__(self, 'occurrences', canonical)
        expected = [dict(zip(IDENTITY_FIELDS, v)) for v in canonical]
        upper = []
        for occurrence in expected:
            _check_result_capacity(_chain_identity(occurrence), self.item_result_bytes)
            # A bounded canonical model result receives only these appended or
            # replaced fields. The encoding of an object merge is no longer
            # than the sum of both object encodings. Recounting its identity
            # is deliberately conservative and needs no guessed overhead.
            upper.append(self.item_result_bytes + len(_encoded(_projection_metadata(occurrence, self.count))))
        object.__setattr__(self, 'item_upper_bounds', tuple(upper))
        object.__setattr__(self, 'array_bytes', _array_bytes(sum(upper), self.count))
        identity_bytes = _encoded(dict(profile='ordinary_single_filter', query_id=self.query_id,
                                       occurrences=expected))
        object.__setattr__(self, 'identity_sha256', hashlib.sha256(identity_bytes).hexdigest())

    @property
    def count(self):
        return len(self.occurrences)

    def bytes_for_copies(self, copies):
        """Charge each independent complete encoded array; never infer RSS."""
        return self.array_bytes * _positive(copies, 'independent delivery copies')

    def describe(self):
        return dict(schema='semloom.ordinary_delivery.v1', profile='ordinary_single_filter',
            query_id=self.query_id, occurrences=self.count, identity_sha256=self.identity_sha256,
            item_result_bytes=self.item_result_bytes, item_upper_bounds=list(self.item_upper_bounds),
            array_bytes=self.array_bytes,
            scope='one compact-JSON receipt array; independent copies need separate bytes; not RSS')


@dataclass(frozen=True)
class OrdinaryDeliverySession:
    """Consecutive query declarations; storage covers the largest complete array."""

    deliveries: tuple[OrdinaryDeliveryDeclaration, ...]
    array_bytes: int = field(init=False)
    identity_sha256: str = field(init=False)

    def __post_init__(self):
        if (type(self.deliveries) is not tuple or not self.deliveries
                or any(type(v) is not OrdinaryDeliveryDeclaration for v in self.deliveries)):
            raise ValueError('a delivery session requires finite immutable query declarations')
        if len({v.query_id for v in self.deliveries}) != len(self.deliveries):
            raise ValueError('delivery session query identities must be distinct')
        if len({v.item_result_bytes for v in self.deliveries}) != 1:
            raise ValueError('one delivery session retains one model result item declaration')
        object.__setattr__(self, 'array_bytes', max(v.array_bytes for v in self.deliveries))
        declared = [dict(query_id=v.query_id, identity_sha256=v.identity_sha256) for v in self.deliveries]
        object.__setattr__(self, 'identity_sha256', hashlib.sha256(_encoded(declared)).hexdigest())

    def for_query(self, query_id):
        if type(query_id) is not str:
            raise ValueError('delivery query identity must be a string')
        for delivery in self.deliveries:
            if query_id == delivery.query_id:
                return delivery
        raise ValueError('query has no delivery declaration in this session')

    def bytes_for_copies(self, copies):
        return self.array_bytes * _positive(copies, 'independent delivery copies')

    def describe(self):
        return dict(schema='semloom.ordinary_delivery_session.v1',
            identity_sha256=self.identity_sha256, array_bytes=self.array_bytes,
            queries=[v.describe() for v in self.deliveries],
            scope='consecutive complete query arrays; overlapping queries require separate grants')


def declare_ordinary_delivery(query_id, expected, *, item_result_bytes):
    if type(expected) not in (list, tuple):
        raise ValueError('ordinary delivery occurrences must be a finite list or tuple')
    if any(type(v) is not dict or set(v) != set(IDENTITY_FIELDS) for v in expected):
        raise ValueError('ordinary delivery requires complete occurrence identities only')
    occurrences = tuple(tuple(v[n] for n in IDENTITY_FIELDS) for v in expected)
    return OrdinaryDeliveryDeclaration(query_id, occurrences, item_result_bytes)


def declare_ordinary_session(queries, *, item_result_bytes):
    """Use each query's actual identities, not the largest count with another ID."""
    if type(queries) is dict:
        queries = list(queries.items())
    if type(queries) not in (list, tuple) or any(type(v) not in (list, tuple) or len(v) != 2 for v in queries):
        raise ValueError('delivery session queries must be finite (query_id, expected) pairs')
    return OrdinaryDeliverySession(tuple(declare_ordinary_delivery(q, expected,
        item_result_bytes=item_result_bytes) for q, expected in queries))


def _key(value):
    if (type(value) is not dict or set(value) != {'session_id', 'sequence'}
            or any(type(v) is not int or not 0 <= v <= UINT64_MAX for v in value.values())):
        raise ValueError('delivery task and batch keys must be two uint64 integers')
    return value


def _checked_receipt(declaration, receipt):
    if type(declaration) is not OrdinaryDeliveryDeclaration or type(receipt) is not dict:
        raise ValueError('ordinary delivery checks require a declaration and result object')
    types = (str, str, str, str, int, int, str)
    if any(type(receipt.get(n)) is not t for n, t in zip(IDENTITY_FIELDS, types)):
        raise ValueError('ordinary delivery result lacks its complete typed identity')
    occurrence = tuple(receipt[n] for n in IDENTITY_FIELDS)
    index = bisect_left(declaration.occurrences, occurrence)
    if index == declaration.count or declaration.occurrences[index] != occurrence:
        raise ValueError('ordinary delivery result belongs to an undeclared occurrence')
    expected = dict(zip(IDENTITY_FIELDS, occurrence))
    if receipt.get('chain_call_id') != _chain_identity(expected)['call_id']:
        raise ValueError('ordinary delivery changed its original chain call identity')
    if type(receipt.get('parent_call_ids')) is not list or receipt['parent_call_ids']:
        raise ValueError('a single ordinary Filter has no parent calls')
    if 'decision' in receipt and type(receipt['decision']) is not bool:
        raise ValueError('ordinary Filter delivery decisions must be boolean when present')
    task = _key(receipt['core_task_key']) if 'core_task_key' in receipt else None
    if 'transport_batch' in receipt:
        member = receipt['transport_batch']
        if type(member) is not dict or set(member) != {'batch_key', 'index', 'size'}:
            raise ValueError('delivery batch annotation must retain its complete member fields')
        batch = _key(member['batch_key'])
        if (task is None or task['session_id'] != batch['session_id']
                or type(member['size']) is not int or type(member['index']) is not int
                or not 0 <= member['index'] < member['size'] <= declaration.count):
            raise ValueError('delivery batch annotation exceeds its declared query membership')
    try:
        size = len(_encode(receipt, declaration.item_upper_bounds[index]))
    except (ValueError, TypeError, OverflowError, UnicodeError) as error:
        raise ValueError('ordinary delivery result is not finite JSON within its declared bytes') from error
    CallLedger([expected], 1).record(receipt)
    return size


def check_ordinary_receipt(declaration, receipt):
    """Read-only check of one projected result; return its actual encoded byte count."""
    return _checked_receipt(declaration, receipt)


def check_ordinary_receipts(declaration, receipts, *, complete=False):
    """Read-only partial or complete-array check; no unknown result is upgraded."""
    if (type(declaration) is not OrdinaryDeliveryDeclaration or type(receipts) not in (list, tuple)
            or type(complete) is not bool or len(receipts) > declaration.count
            or complete and len(receipts) != declaration.count):
        raise ValueError('ordinary delivery array quantity differs from its declaration')
    seen, item_bytes = set(), 0
    for receipt in receipts:
        item_bytes += _checked_receipt(declaration, receipt)
        if receipt['call_id'] in seen:
            raise ValueError('ordinary delivery array repeats a call identity')
        seen.add(receipt['call_id'])
    size = _array_bytes(item_bytes, len(receipts))
    if size > declaration.array_bytes:
        raise ValueError('records bytes exceed the reserved item capacity')
    return size
