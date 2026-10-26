"""Ordinary text Filter configuration, scan values and independent call accounting."""

from copy import deepcopy
from dataclasses import asdict, dataclass
import json
import math


_IDENTITY_FIELDS = (
    "query_id", "model_role", "algorithm_stage", "row_id", "source_position",
    "call_index", "call_id",
)


def _resolved_columns(value):
    if value is None:
        return None
    if (type(value) not in (list, tuple) or not value
            or any(type(name) is not str or not name.strip() for name in value)
            or len(set(value)) != len(value)):
        raise ValueError("resolved_input_columns must be a nonempty ordered set of column names")
    return tuple(value)


@dataclass(frozen=True)
class FilterMethod:
    model_name: str
    instruction: str
    default: bool = True
    output_tokens: tuple[str, str] = ("True", "False")
    system_prompt: str | None = None
    resolved_input_columns: tuple[str, ...] | None = None

    def __post_init__(self):
        if type(self.model_name) is not str or not self.model_name.strip():
            raise ValueError("Filter model_name must be a nonempty string")
        if type(self.instruction) is not str or not self.instruction.strip():
            raise ValueError("Filter instruction must be a nonempty string")
        if type(self.default) is not bool:
            raise ValueError("Filter default must be a boolean")
        if type(self.output_tokens) is not tuple or self.output_tokens != ("True", "False"):
            raise ValueError("ordinary text Filter requires the original True/False output tokens")
        if self.system_prompt is not None and type(self.system_prompt) is not str:
            raise ValueError("Filter system_prompt must be a string or None")
        object.__setattr__(self, "resolved_input_columns", _resolved_columns(self.resolved_input_columns))

    def describe(self):
        self.__post_init__()
        return asdict(self)

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict:
            raise ValueError("Filter method configuration must be a dictionary")
        options = dict(value)
        if "instruction" not in options:
            raise ValueError("Filter instruction must be explicitly supplied")
        if "output_tokens" in options:
            if type(options["output_tokens"]) not in (list, tuple):
                raise ValueError("Filter output_tokens must be an ordered pair")
            options["output_tokens"] = tuple(options["output_tokens"])
        try:
            return cls(**options)
        except TypeError as error:
            raise ValueError("unsupported Filter method configuration") from error


@dataclass(frozen=True)
class RecordScan:
    """CSV column values, text projection and an optional single equality.

    Sources own task binding and input preflight. Backends translate these
    immutable values into their native scan/filter/select APIs.
    """

    csv_schema: tuple[tuple[str, str], ...]
    payload_columns: tuple[str, ...]
    predicate: tuple[str, str | int] | None = None

    def __post_init__(self):
        if type(self.csv_schema) not in (list, tuple) or not self.csv_schema:
            raise ValueError("record scan requires an ordered CSV schema")
        columns = []
        for item in self.csv_schema:
            if (type(item) not in (list, tuple) or len(item) != 2
                    or type(item[0]) is not str or not item[0].strip()
                    or type(item[1]) is not str or item[1] not in ("string", "int64")):
                raise ValueError("record scan columns require a name and string/int64 type")
            columns.append(tuple(item))
        schema = dict(columns)
        if len(schema) != len(columns):
            raise ValueError("record scan CSV columns must be unique")
        if schema.get("row_id") != "string" or schema.get("source_position") != "int64":
            raise ValueError("record scan requires row_id:string and source_position:int64")
        if (type(self.payload_columns) not in (list, tuple) or not self.payload_columns
                or any(type(name) is not str or name not in schema
                       or schema[name] != "string" or name in _IDENTITY_FIELDS
                       for name in self.payload_columns)
                or len(set(self.payload_columns)) != len(self.payload_columns)):
            raise ValueError("record scan payload must name unique non-identity text columns")
        predicate = self.predicate
        if predicate is not None:
            if (type(predicate) not in (list, tuple) or len(predicate) != 2
                    or type(predicate[0]) is not str or predicate[0] not in schema):
                raise ValueError("record scan predicate must name one CSV equality column and value")
            expected = str if schema[predicate[0]] == "string" else int
            if (type(predicate[1]) is not expected
                    or expected is int and not -(2 ** 63) <= predicate[1] < 2 ** 63):
                raise ValueError("record scan equality value must match its CSV column type")
            predicate = tuple(predicate)
        object.__setattr__(self, "csv_schema", tuple(columns))
        object.__setattr__(self, "payload_columns", tuple(self.payload_columns))
        object.__setattr__(self, "predicate", predicate)

    def describe(self):
        self.__post_init__()
        return asdict(self)

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict:
            raise ValueError("record scan configuration must be a dictionary")
        try:
            return cls(**value)
        except TypeError as error:
            raise ValueError("unsupported record scan configuration") from error


@dataclass(frozen=True)
class RecordsResources:
    cpu_workers: int = 1
    gpu_workers: int = 1
    prepare_batch_rows: int = 4
    model_batch_rows: int = 4
    max_active_batches: int = 2
    query_timeout_s: float = 60
    startup_timeout_s: float = 120

    def __post_init__(self):
        for name in (
            "cpu_workers", "gpu_workers", "prepare_batch_rows", "model_batch_rows",
            "max_active_batches",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError("records capacity must be a positive integer: " + name)
        if self.gpu_workers != 1:
            raise ValueError("records currently require one GPU worker and TP1")
        for name in ("query_timeout_s", "startup_timeout_s"):
            value = getattr(self, name)
            try:
                valid = type(value) in (int, float) and math.isfinite(value) and value > 0
            except OverflowError:
                valid = False
            if not valid:
                raise ValueError("records deadline must be finite and positive: " + name)

    def describe(self):
        self.__post_init__()
        return asdict(self)


def identity(query_id, row_id, source_position, call_index=0, *,
             model_role="main", algorithm_stage="filter"):
    """Bind a method's role/stage and occurrence to one independent call.

    The default ordinary Filter keeps its original four-part ID. Nondefault
    role/stage calls append both values, so they cannot alias that namespace.
    Methods declare stages and call ordinals; this function does not schedule them.
    """
    if any(type(value) is not str or not value.strip() for value in (query_id, row_id)):
        raise ValueError("query_id and row_id must be nonempty strings")
    if any(type(value) is not int or value < 0 for value in (source_position, call_index)):
        raise ValueError("source_position and call_index must be nonnegative integers")
    if any(type(value) is not str or not value.strip() for value in (model_role, algorithm_stage)):
        raise ValueError("model_role and algorithm_stage must be nonempty strings")
    parts = [query_id, row_id, source_position, call_index]
    if model_role != "main" or algorithm_stage != "filter":
        parts.extend((model_role, algorithm_stage))
    call_id = "lotus-filter-v1:" + json.dumps(
        parts,
        ensure_ascii=False, separators=(",", ":"),
    )
    return dict(
        query_id=query_id, model_role=model_role, algorithm_stage=algorithm_stage, row_id=row_id,
        source_position=source_position, call_index=call_index, call_id=call_id,
    )


def _checked_identity(value):
    if type(value) is not dict or not all(key in value for key in _IDENTITY_FIELDS):
        raise ValueError("call result lacks complete identity")
    expected = identity(
        value["query_id"], value["row_id"], value["source_position"], value["call_index"],
        model_role=value["model_role"], algorithm_stage=value["algorithm_stage"],
    )
    if any(value[key] != expected[key] for key in _IDENTITY_FIELDS):
        raise ValueError("call result identity differs from its LOTUS call contract")
    return expected


class CallLedger:
    """Own every declared call independently of final rows or resource reports."""

    def __init__(self, expected: list[dict], max_calls: int):
        if type(max_calls) is not int or max_calls < 1:
            raise ValueError("max_calls must be a positive integer")
        if type(expected) is not list or len(expected) > max_calls:
            raise ValueError("declared calls exceed max_calls or are not a list")
        self.max_calls = max_calls
        self._expected = {}
        for value in expected:
            checked = _checked_identity(value)
            if set(value) != set(_IDENTITY_FIELDS) or checked["call_id"] in self._expected:
                raise ValueError("declared call identity is duplicate or contains extra fields")
            self._expected[checked["call_id"]] = checked
        self._records = {}

    def record(self, result):
        checked = _checked_identity(result)
        call_id = checked["call_id"]
        if call_id not in self._expected or checked != self._expected[call_id]:
            raise ValueError("result belongs to an undeclared call identity")
        if call_id in self._records:
            raise ValueError("duplicate call result")
        if result.get("status") not in ("succeeded", "failed", "unknown"):
            raise ValueError("call result requires a terminal status")
        confirmed = result.get("submission_confirmed", result["status"] == "succeeded")
        if type(confirmed) is not bool:
            raise ValueError("submission_confirmed must be a boolean")
        if result["status"] == "succeeded" and not confirmed:
            raise ValueError("a succeeded call must have confirmed submission")
        record = deepcopy(result)
        for name in ("prompt_tokens", "completion_tokens"):
            tokens = result.get(name)
            if tokens is not None and (type(tokens) is not int or tokens < 0):
                raise ValueError("call usage must be nonnegative integers or None")
            record[name] = tokens
        record["submission_confirmed"] = confirmed
        self._records[call_id] = record

    def mark_unconfirmed(self, error_type):
        if type(error_type) is not str or not error_type.strip():
            raise ValueError("an unconfirmed call requires its error_type")
        for call_id, expected in self._expected.items():
            if call_id not in self._records:
                self.record(dict(
                    expected, status="unknown", error_type=error_type,
                    submission_confirmed=False, prompt_tokens=None, completion_tokens=None,
                ))

    def report(self):
        records = [self._records[key] for key in self._expected if key in self._records]
        missing = [key for key in self._expected if key not in self._records]
        counts = dict(
            expected=len(self._expected), recorded=len(records), missing=len(missing),
            **{status: sum(row["status"] == status for row in records)
               for status in ("succeeded", "failed", "unknown")},
            submission_confirmed=sum(row["submission_confirmed"] for row in records),
            submission_unconfirmed=sum(not row["submission_confirmed"] for row in records),
        )
        usage = {}
        for name in ("prompt_tokens", "completion_tokens"):
            known = [row[name] for row in records if row[name] is not None]
            unknown = len(self._expected) - len(known)
            usage[name] = None if unknown else sum(known)
            usage["known_" + name] = sum(known)
            usage["unknown_" + name + "_calls"] = unknown
        usage["total_tokens"] = (
            None if usage["prompt_tokens"] is None or usage["completion_tokens"] is None
            else usage["prompt_tokens"] + usage["completion_tokens"]
        )
        return deepcopy(dict(
            schema="semloom.lotus_call_ledger.v1", max_calls=self.max_calls,
            records=records, counts=counts, usage=usage, missing_call_ids=missing,
        ))
