"""Canonical global-cascade occurrences over the unchanged v2 Arrow carrier."""

from copy import deepcopy
from dataclasses import asdict
import json

from integrations.imlane_records.filter_workload import PROTOCOL, validate_terminal
from src.semantic_methods.lotus.cascade import (
    CascadeStage, IDENTITY_FIELDS, cascade_identity, local_generation,
)


def checked_identity(value):
    identity = cascade_identity(value["query_id"], value["row_id"], value["source_position"],
        value["call_index"], model_role=value["model_role"], stage=value["algorithm_stage"])
    if any(value.get(key) != identity[key] for key in IDENTITY_FIELDS):
        raise ValueError("IMLane cascade call identity is not canonical")
    return identity


def stage_values(value):
    """Freeze the public stage without guessing columns, roles or call ordinals."""
    data = deepcopy(asdict(value) if type(value) is CascadeStage else value)
    if type(data) is not dict or set(data) != set(CascadeStage.__dataclass_fields__):
        raise ValueError("an explicit complete CascadeStage is required")
    for name in ("records", "input_columns", "output_tokens"):
        if type(data[name]) not in (list, tuple):
            raise ValueError("cascade stage sequences must use values")
        data[name] = tuple(data[name])
    stage = CascadeStage(**data)
    cascade_identity("binding", "binding", 0, 0, model_role=stage.model_role, stage=stage.name)
    columns = stage.input_columns
    if (not columns or len(set(columns)) != len(columns)
            or any(type(c) is not str or not c.strip() or c in IDENTITY_FIELDS for c in columns)
            or stage.output_tokens != ("True", "False") or type(stage.default) is not bool
            or type(stage.model_name) is not str or not stage.model_name.strip()
            or type(stage.instruction) is not str or not stage.instruction.strip()
            or stage.system_prompt is not None and type(stage.system_prompt) is not str
            or not 0 <= len(stage.records) <= 4096):
        raise ValueError("invalid bounded text cascade stage")
    seen, query = set(), None
    for row in stage.records:
        identity = checked_identity(row)
        if (set(row) != set(IDENTITY_FIELDS) | set(columns)
                or any(type(row[c]) is not str for c in columns)
                or (identity["model_role"], identity["algorithm_stage"]) != (stage.model_role, stage.name)
                or identity["call_id"] in seen
                or query is not None and identity["query_id"] != query):
            raise ValueError("cascade stage occurrence repeats or changes its bound role/payload")
        seen.add(identity["call_id"])
        query = identity["query_id"]
    generation = local_generation(stage)
    if (generation.logprobs is not None) != (stage.model_role == "helper"):
        raise ValueError("cascade helper alone owns token logprobs")
    return stage


def input_record(source, stage):
    value = json.loads(source["record_json"])
    if (type(value) is not dict or set(value) != {"identity_json", "payload_json"}
            or any(type(v) is not str for v in value.values())):
        raise ValueError("cascade records require identity and text payload JSON strings")
    identity = checked_identity(json.loads(value["identity_json"]))
    payload = json.loads(value["payload_json"])
    index = source["row_id"]
    if (type(index) is not int or not 0 <= index < len(stage.records)
            or source["call_id"] != identity["call_id"] or source["query_id"] != identity["query_id"]
            or source["stage_id"] != stage.name or identity["model_role"] != stage.model_role
            or type(payload) is not dict or set(payload) != set(stage.input_columns)
            or any(type(payload[c]) is not str for c in stage.input_columns)):
        raise ValueError("cascade wire and stage occurrence differ")
    row = dict(identity, **{c: payload[c] for c in stage.input_columns})
    if row != stage.records[index]:
        raise ValueError("cascade wire changed a declared occurrence or its original text")
    return row


class CascadeOccurrences:
    def __init__(self, stage):
        self.stage = stage_values(stage)
        self.ordinals = {row["call_id"]: i for i, row in enumerate(self.stage.records)}
        self.inputs, self.collected, self.receipts = {}, set(), {}
        self.raw_result_batches, self.unassociated_results = [], []

    def encode(self, rows):
        staged, seen = [], set(self.inputs)
        for row in rows:
            identity = checked_identity(row)
            call = identity["call_id"]
            if call not in self.ordinals or call in seen or row != self.stage.records[self.ordinals[call]]:
                raise ValueError("undeclared, changed or repeated cascade occurrence")
            seen.add(call)
            staged.append(dict(call_id=call, row_id=self.ordinals[call], query_id=row["query_id"],
                stage_id=self.stage.name, record_json=json.dumps(dict(
                    identity_json=json.dumps(identity, ensure_ascii=False),
                    payload_json=json.dumps({c: row[c] for c in self.stage.input_columns}, ensure_ascii=False)),
                    ensure_ascii=False, allow_nan=False)))
        if not 0 < len(staged) <= 16:
            raise ValueError("cascade transport block must contain 1..16 occurrences")
        self.inputs.update((row["call_id"], row) for row in staged)
        return staged

    def decode(self, rows, expected):
        raw = deepcopy(rows)
        results, errors = {}, []
        values = rows if type(rows) is list else []
        faulty = (type(rows) is not list or len(rows) != len(expected)
                  or [r.get("call_id") if type(r) is dict else None for r in values] != [r["call_id"] for r in expected])
        for source in expected:
            call = source["call_id"]
            try:
                matches = [r for r in values if type(r) is dict and r.get("call_id") == call]
                if len(matches) != 1 or call in self.collected:
                    raise ValueError("cascade terminal needs one unconsumed occurrence")
                wire = matches[0]
                value = json.loads(wire["outcome_json"])
                payload, worker = value["result"], value["worker"]
                if (wire["row_id"] != source["row_id"] or value.get("protocol") != PROTOCOL
                        or value.get("query_id") != source["query_id"] or value.get("stage_id") != self.stage.name
                        or payload.get("kind") != "cascade" or payload.get("_occurrence_id") != call
                        or payload.get("_position") != source["row_id"]
                        or worker.get("model_role") != self.stage.model_role):
                    raise ValueError("cascade output changed its stage, role or wire occurrence")
                result = payload["engine_result"]
                validate_terminal(result, input_record(source, self.stage), engine_uid=worker.get("engine_uid"))
                if (value.get("status") != ("ok" if result["status"] == "succeeded" else "error")
                        or value.get("completion_known") != (result["status"] != "unknown")):
                    raise ValueError("cascade terminal summary differs from its evidence")
                results[call] = dict(result, imlane_worker=worker)
                faulty = faulty or bool(result.get("imlane_protocol_error"))
            except (ValueError, TypeError, KeyError, AttributeError) as error:
                errors.append(error)
        self.receipts.update(results)
        ordered = [results[r["call_id"]] for r in expected if r["call_id"] in results]
        if faulty or errors:
            self.raw_result_batches.append(raw)
            self.unassociated_results.extend(r for r in values if type(r) is not dict or r.get("call_id") not in results)
            error = errors[0] if errors else ValueError("cascade wire order, cardinality or engine batch differs")
            error.partial_results = ordered
            raise error
        self.collected.update(results)
        return ordered
