"""Ordinary Filter values and occurrence mapping for the IMLane records bridge."""
from __future__ import annotations

import json

from src.semantic_methods.lotus.records import identity
from src.serving.backends.vllm_engine import IDENTITY_FIELDS

PROTOCOL = "imlane.records.v2"


def bind_source(source, method):
    """The driver freezes the public source projection and original method order."""
    from src.semantic_methods.lotus.engine import bind_filter_method
    from src.semantic_methods.lotus.records import RecordScan

    source.preflight(method)
    scan = source.scan_spec(method)
    if type(scan) is not RecordScan:
        raise TypeError("IMLane source scan must use RecordScan")
    bound = bind_filter_method(method)
    if not set(bound.resolved_input_columns).issubset(scan.payload_columns):
        raise ValueError("source scan omits a referenced Filter text column")
    return bound, scan


def payload_columns(value):
    if (type(value) not in (list, tuple) or not value
            or any(type(name) is not str or not name.strip() or name in IDENTITY_FIELDS for name in value)
            or len(set(value)) != len(value)):
        raise ValueError("Filter payload requires distinct nonidentity text column names")
    return tuple(value)


def validate_terminal(row, expected, *, engine_uid, messages_sha256=None):
    if type(row) is not dict or any(row.get(key) != expected[key] for key in IDENTITY_FIELDS):
        raise ValueError("Filter engine result association differs")
    status = row.get("status")
    if (status not in ("succeeded", "failed", "unknown") or type(row.get("submission_confirmed")) is not bool
            or (status == "succeeded" and not row["submission_confirmed"])):
        raise ValueError("Filter engine returned an invalid terminal")
    if (engine_uid is not None and (type(engine_uid) is not str or not engine_uid.strip())):
        raise ValueError("Filter ready engine UID is malformed")
    if row.get("engine_uid") != engine_uid or (status == "succeeded" and not engine_uid):
        raise ValueError("Filter engine UID differs from its owned ready instance")
    digest = row.get("messages_sha256")
    if (digest is not None and (type(digest) is not str or len(digest) != 64
                               or any(char not in "0123456789abcdef" for char in digest))):
        raise ValueError("Filter messages digest is malformed")
    if ((messages_sha256 is not None and digest != messages_sha256) or (status == "succeeded" and digest is None)):
        raise ValueError("Filter engine changed the original messages digest")
    for count_key, ids_key in (("prompt_tokens", "prompt_token_ids_json"),
                              ("completion_tokens", "output_token_ids_json")):
        count, encoded = row.get(count_key), row.get(ids_key)
        if count is not None and (type(count) is not int or count < 0):
            raise ValueError("Filter engine returned invalid usage evidence")
        if encoded is not None:
            if type(encoded) is not str:
                raise ValueError("Filter token IDs must use JSON values across interpreters")
            ids = json.loads(encoded)
            if ids is None and count is None and status != "succeeded":
                continue
            if type(ids) is not list or any(type(value) is not int or value < 0 for value in ids):
                raise ValueError("Filter token IDs are malformed")
            if count is not None and count != len(ids):
                raise ValueError("Filter usage differs from observed token IDs")
        if (count is not None or status == "succeeded") and (count is None or encoded is None):
            raise ValueError("Filter terminal lacks its authoritative token evidence")
    prompt, completion, total = (row.get(key) for key in ("prompt_tokens", "completion_tokens", "total_tokens"))
    if total is not None and (type(total) is not int or total < 0):
        raise ValueError("Filter total token usage is malformed")
    if prompt is not None and completion is not None and (type(total) is not int or total != prompt + completion):
        raise ValueError("Filter total usage differs from its token components")
    if status == "succeeded" and (type(row.get("engine_request_id")) is not str or not row["engine_request_id"]):
        raise ValueError("Filter success lacks an engine request identity")
    if status != "succeeded" and (type(row.get("error_type")) is not str or not row["error_type"]):
        raise ValueError("Filter failure lacks an error type")


def checked_identity(row):
    expected = identity(row["query_id"], row["row_id"], row["source_position"], row["call_index"])
    if any(row.get(key) != expected[key] for key in IDENTITY_FIELDS):
        raise ValueError("IMLane Filter occurrence differs from the ordinary method")
    return expected


def input_record(row, *, columns=None):
    value = json.loads(row["record_json"])
    if (type(value) is not dict or set(value) != {"identity_json", "payload_json"}
            or any(type(v) is not str for v in value.values())):
        raise ValueError("Filter records require separate identity and payload JSON strings")
    item = checked_identity(json.loads(value["identity_json"]))
    if (row["call_id"] != item["call_id"] or row["query_id"] != item["query_id"]
            or row["stage_id"] != item["algorithm_stage"]):
        raise ValueError("Filter transport and method occurrence differ")
    payload = json.loads(value["payload_json"])
    if type(payload) is not dict:
        raise ValueError("Filter payload must be a text column object")
    names = payload_columns(tuple(payload) if columns is None else columns)
    if set(payload) != set(names) or any(type(v) is not str for v in payload.values()):
        raise ValueError("Filter payload differs from its declared text columns")
    return dict(item, **payload)


class Occurrences:
    """The wire int64 is an occurrence ordinal, never a coerced source ID."""

    def __init__(self, query_id, *, max_calls, columns):
        if type(max_calls) is not int or not 0 < max_calls <= 4096:
            raise ValueError("IMLane Filter needs a finite query call allowance")
        self.query_id, self.max_calls = query_id, max_calls
        self.columns = payload_columns(columns)
        self.inputs, self.collected = {}, set()

    def encode(self, rows):
        staged = []
        seen = set(self.inputs)
        for source in rows:
            item = checked_identity(source)
            if item["query_id"] != self.query_id or item["call_id"] in seen:
                raise ValueError("Filter occurrence repeats or belongs to another query")
            if len(seen) >= self.max_calls or any(type(source.get(name)) is not str for name in self.columns):
                raise ValueError("Filter input exceeds its call allowance or lacks declared text")
            seen.add(item["call_id"])
            staged.append(dict(call_id=item["call_id"], row_id=len(seen) - 1,
                               query_id=self.query_id, stage_id=item["algorithm_stage"],
                               record_json=json.dumps(dict(identity_json=json.dumps(item, ensure_ascii=False),
                                   payload_json=json.dumps({name: source[name] for name in self.columns},
                                                           ensure_ascii=False)), ensure_ascii=False)))
        if not 0 < len(staged) <= 16:
            raise ValueError("IMLane Filter block must contain 1..16 occurrences")
        # A rejected suffix must not register any prefix that was never submitted.
        for row in staged:
            self.inputs[row["call_id"]] = row
        return staged

    def decode(self, rows, expected):
        if len(rows) != len(expected):
            raise ValueError("IMLane Filter output cardinality differs")
        results = []
        staged = set()
        for row, source in zip(rows, expected, strict=True):
            call = source["call_id"]
            if (row.get("call_id") != call or row.get("row_id") != source["row_id"]
                    or self.inputs.get(call) != source or call in self.collected or call in staged):
                raise ValueError("IMLane Filter output occurrence or order differs")
            value = json.loads(row["outcome_json"])
            if (value.get("protocol") != PROTOCOL or value.get("query_id") != self.query_id
                    or value.get("stage_id") != source["stage_id"]):
                raise ValueError("IMLane Filter output protocol or query differs")
            payload = value["result"]
            if (payload.get("kind") != "filter" or payload.get("_occurrence_id") != call
                    or payload.get("_position") != source["row_id"]):
                raise ValueError("IMLane Filter result does not name its wire occurrence")
            result = payload["engine_result"]
            item = input_record(source, columns=self.columns)
            validate_terminal(result, item, engine_uid=value["worker"].get("engine_uid"))
            status = result.get("status")
            if (status not in ("succeeded", "failed", "unknown")
                    or type(result.get("submission_confirmed")) is not bool
                    or value.get("status") != ("ok" if status == "succeeded" else "error")
                    or value.get("completion_known") != (status != "unknown")):
                raise ValueError("IMLane Filter terminal evidence is inconsistent")
            results.append(dict(result, imlane_worker=value["worker"]))
            staged.add(call)
        self.collected.update(staged)
        return results
