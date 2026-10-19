"""Pure occurrence and model-call accounting for a finite LOTUS record chain.

No vendor runtime, iterator, model allocation or closure scheduling belongs here.
The final relation and all declared model evaluations retain separate identities.
"""

from collections import deque
from copy import deepcopy
import hashlib
import json

from src.semantic_methods.lotus.record_chain import RecordOccurrence, stage_identity
from src.serving.backends.vllm_engine import IDENTITY_FIELDS


class ChainExecutionError(RuntimeError):
    """A worker method failure with authoritative calls retained by its consumer."""

    def __init__(self, remote_error_type):
        self.remote_error_type = remote_error_type
        super().__init__("LOTUS chain failed: " + remote_error_type)


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False)

def record_row(record):
    """Encode source values, without constructing prompts or model requests."""
    return {"record_json": _json(record.describe())}

def _metadata(record):
    return {key: deepcopy(getattr(record, key))
            for key in ("row_id", "source_position", "source_index", "source_refs")}

def _source_binding(record):
    canonical = json.dumps(json.loads(_json(record.describe())), ensure_ascii=False,
                           sort_keys=True, separators=(",", ":"), allow_nan=False)
    return dict(_metadata(record), record_sha256=hashlib.sha256(canonical.encode("utf-8")).hexdigest())


class ChainBatchResults:
    """Validate compact batch results without owning their physical execution."""

    def __init__(self, expected, chain):
        self.chain = chain
        self.expected = {record.row_id: _metadata(record) for record in expected}
        self.bindings = {record.row_id: _source_binding(record) for record in expected}
        self.received = set()
        self.calls, self.batch_stage_stats, self.unassociated_results = [], [], []
        self.raw_result_batches, self._raw_priorities, self._received_batches = {}, {}, set()
        self.columns = list(dict.fromkeys(
            [key for record in sorted(expected, key=lambda row: row.source_position) for key in record.values]
            + [stage.method.output_column for stage in chain.stages if stage.kind == "map"]))
        self._base_columns, self._derived_columns = list(self.columns), {}
        self._call_records, self._pending = {}, deque()
        self._cleanup_errors = []


    _backend_name = "LOTUS"

    def _execution_error(self, remote_error_type):
        return ChainExecutionError(remote_error_type)

    def accept(self, envelope):
        """Validate one full native-batch envelope, retaining filtered-call usage."""
        self._accept(envelope)

    def recover(self, envelope):
        """Merge an owner-read audit/failure snapshot without repeating calls."""
        self._record_calls(envelope["calls"], recovering=True)
        if "stage_stats" in envelope and envelope["stage_stats"] not in self.batch_stage_stats:
            self.batch_stage_stats.append(envelope["stage_stats"])
        self._recover_raw(envelope, priority=2 if "error_type" in envelope else 1)

    @property
    def pending(self):
        return self._pending

    def pop(self):
        return self._pending.popleft()

    def verify_coverage(self):
        if self.received != set(self.expected):
            raise ValueError(f"{self._backend_name} graph omitted expected input occurrences")

    def finalize_columns(self):
        self.columns = list(dict.fromkeys(self._base_columns + [
            name for position in sorted(self._derived_columns)
            for name in self._derived_columns[position]]))


    def _record_calls(self, calls, *, recovering=False):
        stages = {stage.stage_id: stage for stage in self.chain.stages}
        for call in calls:
            expected = self.expected.get(call.get("row_id"))
            if (expected is None or any(key not in call for key in IDENTITY_FIELDS)
                    or call["source_position"] != expected["source_position"]
                    or call["query_id"] != self.chain.query_id
                    or call["algorithm_stage"] not in stages
                    or call.get("status") not in {"succeeded", "failed", "unknown"}):
                raise ValueError(f"{self._backend_name} chain returned an undeclared or invalid model call")
            canonical = stage_identity(
                self.chain.query_id, RecordOccurrence.from_dict({**expected, "values": {}}),
                call["algorithm_stage"], model_role=stages[call["algorithm_stage"]].model_role)
            if any(call[name] != canonical[name] for name in IDENTITY_FIELDS):
                raise ValueError(f"{self._backend_name} chain changed a model evaluation identity")
            if call["call_id"] in self._call_records:
                if recovering:
                    from src.baselines.common.redact import redact_json_values

                    previous = self._call_records[call["call_id"]]
                    if call.get("error_type") == "NativeTaskUnconfirmed":
                        continue
                    if previous.get("error_type") == "NativeTaskUnconfirmed":
                        previous.update(call)
                        continue
                    comparable = redact_json_values(previous)
                    for key in ("status", "text", "submission_confirmed", "prompt_tokens",
                                "completion_tokens", "total_tokens", "engine_uid", "engine_request_id",
                                "finish_reason", "parent_call_ids"):
                        if comparable.get(key) is not None and call.get(key) is not None and comparable[key] != call[key]:
                            raise ValueError("conflicting recovered model response for " + call["call_id"])
                    continue
                raise ValueError(f"{self._backend_name} chain duplicated a model evaluation")
            self._call_records[call["call_id"]] = call
            self.calls.append(call)

    def _accept(self, envelope):
        batch_id = envelope["batch_id"]
        if type(batch_id) is not str or not batch_id or batch_id in self._received_batches:
            raise ValueError(f"{self._backend_name} graph returned a missing or duplicate native batch identity")
        inputs = envelope["inputs"]
        batch_ids = set()
        for item in inputs:
            row_id = item["row_id"]
            if (self.bindings.get(row_id) != item or row_id in self.received or row_id in batch_ids):
                raise ValueError(f"{self._backend_name} graph changed or duplicated an input occurrence")
            batch_ids.add(row_id)
        self.received.update(batch_ids)
        self._received_batches.add(batch_id)
        if any(call.get("row_id") not in batch_ids for call in envelope["calls"]):
            raise ValueError(f"{self._backend_name} chain call belongs to a different native batch")
        self._record_calls(envelope["calls"])
        self._cleanup_errors.extend(envelope.get("cleanup_errors", []))
        self.batch_stage_stats.append(envelope["stage_stats"])
        self._recover_raw(envelope, priority=2)
        for column in envelope["columns"]:
            if column not in self.columns:
                self.columns.append(column)
        if envelope.get("error_type"):
            error = self._execution_error(envelope["error_type"])
            error.chain_calls = list(self.calls)
            error.chain_stage_stats = list(self.batch_stage_stats)
            raise error
        rows = [RecordOccurrence.from_dict(row) for row in envelope["records"]]
        final_ids = [record.row_id for record in rows]
        previous = {}
        expected_stage = batch_ids
        for stage in self.chain.stages:
            stage_calls = [call for call in envelope["calls"]
                           if call["algorithm_stage"] == stage.stage_id]
            if (len(stage_calls) != len(expected_stage)
                    or {call["row_id"] for call in stage_calls} != expected_stage
                    or any(call["status"] != "succeeded" for call in stage_calls)):
                raise ValueError(f"{self._backend_name} chain omitted or repeated a dependent stage evaluation")
            for call in stage_calls:
                if call.get("parent_call_ids") != previous.get(call["row_id"], []):
                    raise ValueError(f"{self._backend_name} chain changed a parent evaluation association")
            previous = {call["row_id"]: [call["call_id"]] for call in stage_calls}
            if stage.kind == "filter":
                if any(type(call.get("decision")) is not bool for call in stage_calls):
                    raise ValueError(f"{self._backend_name} chain lacks Filter parser decisions")
                expected_stage = {call["row_id"] for call in stage_calls if call["decision"]}
        if len(set(final_ids)) != len(final_ids) or set(final_ids) != expected_stage:
            raise ValueError(f"{self._backend_name} chain final rows differ from terminal stage occurrences")
        for record in rows:
            if record.row_id not in batch_ids or _metadata(record) != self.expected[record.row_id]:
                raise ValueError(f"{self._backend_name} chain changed a final occurrence identity")
            self._derived_columns[record.source_position] = [key for key in record.values if key not in self._base_columns]
        self._pending.extend(rows)

    def _recover_raw(self, record, *, priority):
        if not record.get("raw_results"):
            return
        key = (record["batch_id"], record["raw_stage_id"])
        previous_priority = self._raw_priorities.get(key, 0)
        if priority == previous_priority:
            from src.baselines.common.redact import redact_json_values

            if redact_json_values(self.raw_result_batches[key]) != redact_json_values(record["raw_results"]):
                raise ValueError("conflicting recovered raw response snapshot")
        if priority > previous_priority:
            self.raw_result_batches[key] = record["raw_results"]
            self._raw_priorities[key] = priority
        self.unassociated_results = []
        for batch in self.raw_result_batches.values():
            for row in batch:
                matches = [] if type(row) is not dict else [call for call in self.calls if all(
                    row.get(name) == call[name] for name in IDENTITY_FIELDS)]
                duplicates = 0 if type(row) is not dict else sum(
                    type(other) is dict and all(other.get(name) == row.get(name) for name in IDENTITY_FIELDS)
                    for other in batch)
                if len(matches) != 1 or duplicates != 1:
                    self.unassociated_results.append(row)
