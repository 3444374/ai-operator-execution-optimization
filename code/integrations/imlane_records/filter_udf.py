"""Author-scheduled Arrow UDF: original LOTUS preparation and a private model child.

The process component is reused unchanged from the existing Daft adapter. It
owns no vendor scheduling policy. Only values cross the model interpreter ABI.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time
import uuid
from copy import deepcopy

import pyarrow as pa

from integrations.imlane_records.echo_udf import INPUT, OUTPUT
from integrations.imlane_records.filter_workload import PROTOCOL, input_record, validate_terminal
from integrations.imlane_records.model_lifecycle import close_owned_model, model_closed
from integrations.imlane_records.method_owner import owned_worker, slot as _slot
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources, RecordScan
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS, _prepared


class FilterWorker:
    def __init__(self, config, *, prepare_type=None, engine_type=None):
        from src.semantic_methods.lotus.engine import LotusFilterMethod, validate_generation

        self.method = FilterMethod.from_dict(config["method"])
        self.engine = EngineSpec.from_dict(config["engine"])
        self.generation = GenerationSpec.from_dict(config["generation"])
        self.resources = RecordsResources(**config["resources"])
        self.scan = RecordScan.from_dict(config["scan"])
        validate_generation(self.method, self.generation)
        if (self.method.resolved_input_columns is None
                or not set(self.method.resolved_input_columns).issubset(self.scan.payload_columns)):
            raise ValueError("IMLane worker requires caller-bound columns covered by the source scan")
        self.query_id = config["query_id"]
        self.deadline = config["deadline_monotonic"]
        self.control = Path(config["control_dir"])
        self.model_environment = config.get("model_environment", {})
        if (not isinstance(self.query_id, str) or not self.query_id or not self.control.is_dir()
                or type(self.deadline) not in (int, float) or not time.monotonic() < self.deadline < float("inf")
                or self.engine.max_model_calls is None or self.engine.max_model_calls > 4096
                or self.resources.model_batch_rows > 16 or self.resources.cpu_workers != 1):
            raise ValueError("IMLane Filter requires a bounded query, control directory and call budget")
        self.prepare = (prepare_type or LotusFilterMethod)(self.method)
        self.engine_type = engine_type
        self.kernel = None
        self._ownership_lock = threading.RLock()
        self.kernel_initialization_attempted = False
        self.calls = set()
        self.closed = self.broken = False
        self.close_report = None
        self.receipt_audit_errors = []
        self.uid = uuid.uuid4().hex

    def _check(self):
        if self.closed or self.broken:
            raise RuntimeError("Filter worker cannot infer after failure or close")
        if (self.control / "stop").exists():
            raise InterruptedError("IMLane Filter query cancelled")
        if time.monotonic() >= self.deadline:
            raise TimeoutError("IMLane Filter query deadline expired")

    def _kernel(self):
        with self._ownership_lock:
            self._check()
            if self.kernel is None:
                if self.kernel_initialization_attempted:
                    raise RuntimeError("IMLane model initialization cannot be retried")
                self.kernel_initialization_attempted = True
                if self.engine_type is None:
                    from integrations.imlane_records.model_process import IMLaneEngineProcess
                    self.kernel = IMLaneEngineProcess(
                        self.engine, self.generation, self.control, self.deadline, self.resources.startup_timeout_s,
                        model_environment=self.model_environment)
                else:
                    self.kernel = self.engine_type(
                        self.engine, self.generation, self.control, self.deadline, self.resources.startup_timeout_s)
            return self.kernel

    def run(self, sources):
        items_all = [input_record(row, columns=self.scan.payload_columns) for row in sources]
        results = []
        for offset in range(0, len(sources), self.resources.model_batch_rows):
            group = items_all[offset:offset + self.resources.model_batch_rows]
            prepared = None
            digests = [None] * len(group)
            attempted = False
            try:
                self._check()
                items = group
                ids = [row["call_id"] for row in items]
                if (any(row["query_id"] != self.query_id for row in items)
                        or len(set(ids)) != len(ids) or self.calls.intersection(ids)
                        or len(self.calls) + len(ids) > self.engine.max_model_calls):
                    raise ValueError("Filter query occurrence repeats or exceeds its request budget")
                self.calls.update(ids)
                prepared = self.prepare.prepare_rows(items)
                if type(prepared) is not list or len(prepared) != len(items):
                    raise ValueError("Filter formatter changed preparation cardinality")
                identities, _ = _prepared(prepared)
                if any(any(row[key] != item[key] for key in IDENTITY_FIELDS)
                       for row, item in zip(identities, items, strict=True)):
                    raise ValueError("Filter formatter changed the original occurrence")
                for row in prepared:
                    for key in ("prepare_worker_pid", "prepare_started_ns", "prepare_finished_ns"):
                        if type(row.get(key)) is not int or row[key] < 0:
                            raise ValueError("Filter formatter lacks preparation evidence")
                digests = [row["messages_sha256"] for row in identities]
                self._check()
                kernel = self._kernel()
                ready_uid = (getattr(kernel, "ready_report", None) or {}).get("engine_uid")
                if type(ready_uid) is not str or not ready_uid:
                    raise ValueError("Filter engine did not identify its ready instance")
                if (getattr(kernel, "process", None) is not None
                        and kernel.ready_report.get("pid") != kernel.process.pid):
                    raise ValueError("Filter ready PID differs from its private model child")
                attempted = True
                observed = kernel.run_prepared(prepared)
                raw = observed if type(observed) is list else [observed]
                faulty = (type(observed) is not list or len(raw) != len(prepared)
                          or [row.get("call_id") if type(row) is dict else None for row in raw]
                          != [item["call_id"] for item in identities])
                unassociated, associated, verified = [], [], []
                for item, original in zip(identities, prepared, strict=True):
                    matches = [row for row in raw if type(row) is dict and row.get("call_id") == item["call_id"]]
                    submitted = False
                    try:
                        if len(matches) != 1:
                            raise ValueError("Filter terminal needs one uniquely associated result")
                        row = dict(matches[0])
                        submitted = (row.get("submission_confirmed") is True and row.get("engine_uid") == ready_uid
                                     and row.get("messages_sha256") == item["messages_sha256"]
                                     and all(row.get(key) == item[key] for key in IDENTITY_FIELDS))
                        # Transport-level unknowns still know the prepared input,
                        # although they cannot attest engine acceptance or usage.
                        if row.get("status") == "unknown" and row.get("messages_sha256") is None:
                            row["messages_sha256"] = item["messages_sha256"]
                        validate_terminal(row, item, engine_uid=ready_uid, messages_sha256=item["messages_sha256"])
                        row.setdefault("messages_sha256", item["messages_sha256"])
                        row.update({key: original[key] for key in (
                            "prepare_worker_pid", "prepare_started_ns", "prepare_finished_ns")})
                        verified.append(row)
                    except Exception as error:
                        # Reject only the unverified item; other verified engine
                        # terminals in this same return retain their known usage.
                        row = self._failure(item, item["messages_sha256"], type(error).__name__, attempted=True,
                                            submitted=submitted)
                        faulty = True
                        unassociated.extend(matches)
                    if row["status"] != "succeeded":
                        self.broken = True
                    associated.append(row)
                declared = {item["call_id"] for item in identities}
                unassociated.extend(row for row in raw if type(row) is not dict
                                    or type(row.get("call_id")) is not str or row["call_id"] not in declared)
                if faulty:
                    self.broken = True
                    associated[0]["imlane_protocol_error"] = "ValueError"
                    from src.baselines.text.frameworks.daft_engine_process import _publish
                    try:
                        _publish(self.control / ("filter-audit-" + uuid.uuid4().hex + ".json"), dict(
                            query_id=self.query_id, engine_uid=ready_uid, expected=identities,
                            known_receipts=verified,
                            raw_results=deepcopy(raw), unassociated_results=deepcopy(unassociated)))
                    except Exception as error:
                        self.receipt_audit_errors.append("filter_receipt_audit: " + type(error).__name__)
                results.extend((row, attempted) for row in associated)
            except Exception as error:
                self.broken = True
                # A later subbatch cannot erase a previously observed terminal.
                for item, digest in zip(group, digests, strict=True):
                    results.append((self._failure(item, digest, type(error).__name__, attempted=attempted), attempted))
        return results

    def _failure(self, item, digest, error_type, *, attempted, submitted=False):
        return dict({key: item[key] for key in IDENTITY_FIELDS},
                    status="unknown" if attempted else "failed", text=None, submission_confirmed=submitted,
                    messages_sha256=digest, prompt_tokens=None, completion_tokens=None, total_tokens=None,
                    prompt_token_ids_json=None, output_token_ids_json=None, logprobs_json=None,
                    finish_reason=None, engine_request_id=None, error_type=error_type,
                    engine_uid=(getattr(self.kernel, "ready_report", None) or {}).get("engine_uid"))

    def close(self):
        with self._ownership_lock:
            return self._close()

    def _close(self):
        if self.closed:
            return self.close_report
        report = close_owned_model(self.kernel, attempted=self.kernel_initialization_attempted)
        if self.receipt_audit_errors:
            report = dict(report, receipt_audit_errors=list(self.receipt_audit_errors))
        self.close_report = report
        # Failed close remains retryable and keeps its engine handle.
        self.closed = model_closed(report)
        if not self.closed:
            self.broken = True
        return report

    def envelope(self, source, *, result=None, attempted=False, close=None):
        if close is None:
            ok = result["status"] == "succeeded"
            known = result["status"] != "unknown"
            payload = dict(kind="filter", engine_result=result)
            error_type = None if ok else result["error_type"]
        else:
            ok = model_closed(close)
            known, payload = True, dict(kind="close", close_report=close)
            error_type = None if ok else "ModelCloseUnconfirmed"
        payload.update(_occurrence_id=source["call_id"], _position=source["row_id"])
        return dict(protocol=PROTOCOL, query_id=source["query_id"], stage_id=source["stage_id"],
                    status="ok" if ok else "error", result=payload, error_type=error_type,
                    error=None if ok else error_type, model_entry_attempted=attempted, completion_known=known,
                    worker=dict(worker_pid=os.getpid(), worker_uid=self.uid,
                                model_role="main", python=sys.executable, arrow=pa.__version__,
                                engine_uid=(getattr(self.kernel, "ready_report", None) or {}).get("engine_uid")))


def infer_records(table):
    table.validate(full=True)
    if (table.column_names != INPUT.names or table.schema.metadata or not 0 < len(table) <= 16
            or any(field.type != INPUT.field(i).type or field.metadata for i, field in enumerate(table.schema))
            or any(table.column(i).null_count for i in range(table.num_columns))):
        raise ValueError("Filter requires the fixed bounded records schema")
    rows = table.to_pylist()
    if any((row["query_id"], row["stage_id"]) != (rows[0]["query_id"], "filter") for row in rows):
        raise ValueError("Filter table crosses query or stage")
    config = json.loads(Path(os.environ["IMLANE_FILTER_CONFIG"]).read_text())
    signature = hashlib.sha256(json.dumps(dict(kind="filter", **{key: config[key] for key in (
        "method", "engine", "generation", "resources", "model_environment", "scan")}), sort_keys=True).encode()).hexdigest()
    work = owned_worker(config, signature, FilterWorker)
    if config["query_id"] != work.query_id or any(row["query_id"] != work.query_id for row in rows):
        raise ValueError("Filter input does not belong to this interpreter query")
    operations = [json.loads(row["record_json"]) for row in rows]
    if len(rows) == 1 and operations == [{"operation": "close"}]:
        report = work.close()
        if work.closed:
            _slot.finished.add(work.query_id)
        outcomes = [work.envelope(rows[0], close=report)]
    else:
        outcomes = [work.envelope(source, result=result, attempted=attempted)
                    for source, (result, attempted) in zip(rows, work.run(rows), strict=True)]
    return pa.Table.from_pylist([dict(call_id=row["call_id"], row_id=row["row_id"],
                                    outcome_json=json.dumps(outcome, ensure_ascii=False, allow_nan=False))
                                for row, outcome in zip(rows, outcomes, strict=True)], schema=OUTPUT)
