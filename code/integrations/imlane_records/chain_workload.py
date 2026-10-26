"""Finite chain batches in one neutral records-v2 transport occurrence each."""

import json

from integrations.imlane_records.filter_workload import PROTOCOL
from src.baselines.text.frameworks.lotus_chain_results import _source_binding, record_row
from src.semantic_methods.lotus.record_chain import RecordOccurrence


STAGE = "finite-chain"


def batch_rows(source):
    value = json.loads(source["record_json"])
    if (type(value) is not dict or set(value) != {"batch_json"} or type(value["batch_json"]) is not str):
        raise ValueError("chain transport requires one batch_json string")
    rows = json.loads(value["batch_json"])
    if (type(rows) is not list or not 0 < len(rows) <= 16
            or any(type(row) is not dict or set(row) != {"record_json"}
                   or type(row["record_json"]) is not str for row in rows)):
        raise ValueError("chain transport requires a finite batch of 1..16 record values")
    return rows


class ChainBatches:
    def __init__(self, query_id, expected):
        self.query_id = query_id
        self.expected = {_source_binding(row)["row_id"]: _source_binding(row) for row in expected}
        self.sent, self.collected, self.inputs, self.index = set(), set(), {}, 0

    def encode(self, records):
        if (type(records) is not list or not 0 < len(records) <= 16
                or any(type(row) is not RecordOccurrence for row in records)):
            raise ValueError("chain batches require 1..16 RecordOccurrence values")
        ids = [row.row_id for row in records]
        if (len(set(ids)) != len(ids) or self.sent.intersection(ids)
                or any(self.expected.get(row.row_id) != _source_binding(row) for row in records)):
            raise ValueError("chain source changed or duplicated a declared occurrence")
        call = "imlane-chain-batch-v1:" + json.dumps([self.query_id, self.index], ensure_ascii=False)
        row = dict(call_id=call, row_id=self.index, query_id=self.query_id, stage_id=STAGE,
                   record_json=json.dumps(dict(batch_json=json.dumps([record_row(r) for r in records],
                                                                      ensure_ascii=False, allow_nan=False))))
        self.inputs[call] = row
        self.sent.update(ids)
        self.index += 1
        return [row]

    def decode(self, rows, expected):
        if len(rows) != 1 or len(expected) != 1:
            raise ValueError("one native chain task must return one batch envelope")
        row, source = rows[0], expected[0]
        call = source["call_id"]
        if (row.get("call_id") != call or row.get("row_id") != source["row_id"]
                or self.inputs.get(call) != source or call in self.collected):
            raise ValueError("chain transport occurrence changed or repeated")
        value = json.loads(row["outcome_json"])
        payload = value["result"]
        if (value.get("protocol") != PROTOCOL or value.get("query_id") != self.query_id
                or value.get("stage_id") != STAGE or payload.get("kind") != "chain"
                or payload.get("_occurrence_id") != call or payload.get("_position") != source["row_id"]):
            raise ValueError("chain batch protocol or occurrence differs")
        batch = payload["batch"]
        bindings = [_source_binding(RecordOccurrence.from_dict(json.loads(r["record_json"])))
                    for r in batch_rows(source)]
        if batch["inputs"] != bindings:
            raise ValueError("chain native batch changed its original source values")
        known = not any(call.get("status") == "unknown" for call in batch["calls"])
        if (value.get("status") != ("error" if batch.get("error_type") else "ok")
                or value.get("completion_known") is not known
                or type(value.get("model_entry_attempted")) is not bool):
            raise ValueError("chain terminal summary differs from its batch evidence")
        self.collected.add(source["call_id"])
        return [dict(status="failed" if batch.get("error_type") else "succeeded", batch=batch)]
