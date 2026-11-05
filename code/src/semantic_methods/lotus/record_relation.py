"""Complete a validated Extract relation before any downstream record method.

The execution owner must first consume source EOF and verify the full prefix
with the existing chain result accounting. This module owns only label-based
relation assembly and dependency values, never a stage loop or model lifetime.
The nonterminal Extract guard remains until that owner supplies this boundary.
"""

from copy import deepcopy
from dataclasses import dataclass
from numbers import Integral

from .record_types import RecordOccurrence, StageSpec, _name, _ordered_columns, stage_identity
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS


@dataclass(frozen=True)
class CompletedExtractRelation:
    records: tuple[RecordOccurrence, ...]
    columns: tuple[str, ...]
    parent_call_ids: dict[tuple[str, int], tuple[str, ...]]
    call_snapshot: tuple[dict, ...]


def complete_extract_relation(
    records: list[RecordOccurrence] | tuple[RecordOccurrence, ...], *,
    columns: list[str] | tuple[str, ...], extract_stage: StageSpec, query_id: str,
    calls: list[dict] | tuple[dict, ...], source_exhausted: bool,
) -> CompletedExtractRelation:
    """Assemble an already verified complete prefix ending in ordinary Extract.

    ``source_exhausted`` must reflect successful source EOF. The caller remains
    responsible for the existing result accountant's coverage and success checks;
    a partial list of records cannot prove source coverage here. Failed prefixes
    must never reach this function. Known calls stay a snapshot, not new work.

    Original Extract applies each actual key to all rows with the same index
    label, in source order. Preserve finite JSON source values directly instead
    of round-tripping a DataFrame through NaN/pd.NA or converting cells to text.
    Parents conservatively include every Extract call in the same label group:
    a later writer can change a field consumed by an earlier occurrence.
    """
    if type(calls) not in (list, tuple):
        raise TypeError("Extract relation calls must be a finite sequence")
    snapshot = deepcopy(list(calls))
    try:
        _name(query_id, "query_id")
        if source_exhausted is not True:
            raise ValueError("Extract relation requires successful source EOF")
        if type(extract_stage) is not StageSpec or extract_stage.kind != "extract":
            raise TypeError("Extract relation requires an ordinary Extract StageSpec")
        extract_stage.__post_init__()
        if type(records) not in (list, tuple) or any(type(r) is not RecordOccurrence for r in records):
            raise TypeError("Extract relation records must be a finite sequence of RecordOccurrence")
        if (type(columns) not in (list, tuple) or any(type(k) is not str for k in columns)
                or len(set(columns)) != len(columns)):
            raise ValueError("Extract relation requires a unique ordered column schema")
        ordered = sorted(records, key=lambda r: r.source_position)
        for name in ("row_id", "source_position"):
            if len({getattr(record, name) for record in ordered}) != len(ordered):
                raise ValueError("Extract relation requires globally unique " + name)
        for record in ordered:
            if not set(record.values).issubset(columns):
                raise ValueError("Extract relation schema omits actual data columns")
            if any(key not in record.values or record.values[key] != value
                   for key, value in record.index_updates.items()):
                raise ValueError("Extract relation update differs from its parsed record value")
        if any(type(call) is not dict or call.get("query_id") != query_id
               or call.get("status") != "succeeded" for call in snapshot):
            raise ValueError("Extract relation cannot consume failed, unknown, or foreign calls")
        terminal = [call for call in snapshot if call.get("algorithm_stage") == extract_stage.stage_id]
        by_occurrence = {}
        for call in terminal:
            key = (call.get("row_id"), call.get("source_position"))
            if key in by_occurrence:
                raise ValueError("Extract relation duplicated a terminal model evaluation")
            by_occurrence[key] = call
        if set(by_occurrence) != {(r.row_id, r.source_position) for r in ordered}:
            raise ValueError("Extract relation records differ from terminal model occurrences")
        updates, group_calls = {}, {}
        for record in ordered:
            call = by_occurrence[(record.row_id, record.source_position)]
            identity = stage_identity(query_id, record, extract_stage.stage_id,
                                      model_role=extract_stage.model_role)
            if any(call.get(key) != identity[key] for key in IDENTITY_FIELDS):
                raise ValueError("Extract relation changed a terminal evaluation identity")
            updates.setdefault(record.source_index, {}).update(deepcopy(record.index_updates))
            group_calls.setdefault(record.source_index, []).append(call["call_id"])
        result, parents = [], {}
        for record in ordered:
            values = {key: deepcopy(record.values.get(key)) for key in columns}
            values.update(updates[record.source_index])
            result.append(RecordOccurrence(record.row_id, record.source_position, record.source_index,
                                           values, record.source_refs))
            parents[(record.row_id, record.source_position)] = tuple(group_calls[record.source_index])
        return CompletedExtractRelation(tuple(result), tuple(columns), parents, tuple(snapshot))
    except Exception as error:
        error.chain_calls = deepcopy(snapshot)
        raise


def records_to_frame(records, *, columns=None, source_index=None, empty_prototype=None):
    """One final relation assembly, with original Extract label assignment.

The union of Extract's actual keys defines added columns, as in its accessor;
no declared-schema filtering or missing-key completion is applied to parsing.
Pass the original pandas Index to retain its dtype/name when filtering removes
all occurrences. Surviving records cannot recover that metadata by themselves.
An optional zero-row DataFrame supplies the final empty relation's column dtypes
and metadata. Its columns must exactly match explicit columns; source_index, if
given, still owns the index. It never casts or filters nonempty record values.
"""
    import pandas as pd

    records = sorted(records, key=lambda record: record.source_position)
    if empty_prototype is not None:
        if not isinstance(empty_prototype, pd.DataFrame):
            raise TypeError("empty_prototype must be a pandas DataFrame")
        if len(empty_prototype):
            raise ValueError("empty_prototype must have zero rows")
        if isinstance(empty_prototype.index, pd.MultiIndex):
            raise TypeError("empty_prototype must have a single pandas Index")
        _ordered_columns([], list(empty_prototype.columns))
    if columns is None:
        columns = (list(empty_prototype.columns) if not records and empty_prototype is not None
                   else list(dict.fromkeys(k for record in records for k in record.values)))
    else:
        columns = list(columns)
    if not records and empty_prototype is not None and list(empty_prototype.columns) != columns:
        raise ValueError("columns must match empty_prototype columns in order")
    labels = [record.source_index for record in records]
    if source_index is None:
        index = (empty_prototype.index.copy(deep=True)
                 if not records and empty_prototype is not None else labels)
    else:
        if not isinstance(source_index, pd.Index) or isinstance(source_index, pd.MultiIndex):
            raise TypeError("source_index must be a single pandas Index")
        index = (pd.Index(labels, dtype=source_index.dtype, name=source_index.name)
                 if records else source_index[:0])
        for label, value in zip(labels, index.tolist(), strict=True):
            if ((type(label) is int and (isinstance(value, bool) or not isinstance(value, Integral)))
                    or (type(label) is str and type(value) is not str) or value != label):
                raise ValueError("source_index dtype changed occurrence labels")
    if not records and empty_prototype is not None:
        frame = empty_prototype.copy(deep=True)
        frame.index = index
    else:
        frame = pd.DataFrame([{k: r.values.get(k) for k in columns} for r in records],
                             index=index, columns=columns)
    for record in records:
        for key, value in record.index_updates.items():
            if key not in frame.columns:
                frame[key] = None
            frame.loc[record.source_index, key] = value
    return frame
