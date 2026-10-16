"""Plain Movie Filter configuration, identities and independent call accounting."""

from copy import deepcopy
from dataclasses import asdict, dataclass
import json
import math


MOVIE_Q3_INSTRUCTION = (
    "Determine if the following review is clearly positive. Review: {reviewText}"
)
_IDENTITY_FIELDS = (
    "query_id", "model_role", "algorithm_stage", "row_id", "source_position",
    "call_index", "call_id",
)


@dataclass(frozen=True)
class FilterMethod:
    model_name: str
    instruction: str = MOVIE_Q3_INSTRUCTION
    default: bool = True
    output_tokens: tuple[str, str] = ("True", "False")
    system_prompt: str | None = None

    def __post_init__(self):
        if type(self.model_name) is not str or not self.model_name.strip():
            raise ValueError("Filter model_name must be a nonempty string")
        if self.instruction != MOVIE_Q3_INSTRUCTION:
            raise ValueError("only the plain Movie Q3 reviewText instruction is supported")
        if type(self.default) is not bool:
            raise ValueError("Filter default must be a boolean")
        if type(self.output_tokens) is not tuple or self.output_tokens != ("True", "False"):
            raise ValueError("plain Movie Q3 requires the original True/False output tokens")
        if self.system_prompt is not None and type(self.system_prompt) is not str:
            raise ValueError("Filter system_prompt must be a string or None")

    def describe(self):
        self.__post_init__()
        return asdict(self)

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict:
            raise ValueError("Filter method configuration must be a dictionary")
        options = dict(value)
        if "output_tokens" in options:
            if type(options["output_tokens"]) not in (list, tuple):
                raise ValueError("Filter output_tokens must be an ordered pair")
            options["output_tokens"] = tuple(options["output_tokens"])
        try:
            return cls(**options)
        except TypeError as error:
            raise ValueError("unsupported Filter method configuration") from error


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
            raise ValueError("plain Movie Q3 currently requires one GPU worker and TP1")
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


def identity(query_id, row_id, source_position, call_index=0):
    """The occurrence and the call ordinal both participate in the stable ID."""
    if any(type(value) is not str or not value.strip() for value in (query_id, row_id)):
        raise ValueError("query_id and row_id must be nonempty strings")
    if any(type(value) is not int or value < 0 for value in (source_position, call_index)):
        raise ValueError("source_position and call_index must be nonnegative integers")
    call_id = "lotus-filter-v1:" + json.dumps(
        [query_id, row_id, source_position, call_index],
        ensure_ascii=False, separators=(",", ":"),
    )
    return dict(
        query_id=query_id, model_role="main", algorithm_stage="filter", row_id=row_id,
        source_position=source_position, call_index=call_index, call_id=call_id,
    )


def _checked_identity(value):
    if type(value) is not dict or not all(key in value for key in _IDENTITY_FIELDS):
        raise ValueError("call result lacks complete identity")
    expected = identity(
        value["query_id"], value["row_id"], value["source_position"], value["call_index"],
    )
    if any(value[key] != expected[key] for key in _IDENTITY_FIELDS):
        raise ValueError("call result identity differs from the plain Filter contract")
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
