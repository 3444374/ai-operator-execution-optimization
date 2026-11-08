"""A fixed cascade role executed by the author's CPython executor."""

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import threading
import time
import uuid

import pyarrow as pa

from integrations.imlane_records.cascade_workload import input_record, stage_values
from integrations.imlane_records.echo_udf import INPUT, OUTPUT
from integrations.imlane_records.filter_udf import FilterWorker
from integrations.imlane_records.filter_workload import PROTOCOL, validate_terminal
from integrations.imlane_records.method_owner import owned_worker, slot
from src.semantic_methods.lotus.cascade import CascadePreparer, IDENTITY_FIELDS, local_generation
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, _prepared


class CascadeWorker(FilterWorker):
    """Reuse private-child ownership and close; the original method owns stages."""

    def __init__(self, config, *, prepare_type=None, engine_type=None):
        self.stage = stage_values(config["stage"])
        self.engine = EngineSpec.from_dict(config["engine"])
        self.generation = local_generation(self.stage)
        self.resources = RecordsResources(**config["resources"])
        self.query_id, self.deadline = config["query_id"], config["deadline_monotonic"]
        self.control, self.model_environment = Path(config["control_dir"]), dict(config["model_environment"])
        if (config.get("schema") != "imlane.cascade.stage.v1" or not self.stage.records
                or any(r["query_id"] != self.query_id for r in self.stage.records)
                or not self.control.is_dir() or not time.monotonic() < self.deadline < float("inf")
                or self.resources.cpu_workers != 1 or self.resources.model_batch_rows > 16
                or self.resources.prepare_batch_rows > 16 or self.resources.max_active_batches > 2
                or self.engine.max_model_calls != len(self.stage.records)
                or self.generation.max_tokens > self.engine.max_model_len):
            raise ValueError("cascade worker requires one finite bound role, stage and query")
        self.prepare = (prepare_type or CascadePreparer)(self.stage)
        self.engine_type, self.kernel = engine_type, None
        self._ownership_lock = threading.RLock()
        self.kernel_initialization_attempted = False
        self.calls = set()
        self.closed = self.broken = False
        self.close_report = None
        self.receipt_audit_errors = []
        self.uid = uuid.uuid4().hex

    def run(self, sources):
        items = [input_record(row, self.stage) for row in sources]
        results = []
        for offset in range(0, len(items), self.resources.model_batch_rows):
            group = items[offset:offset + self.resources.model_batch_rows]
            digests, attempted = [None] * len(group), False
            try:
                self._check()
                ids = [r["call_id"] for r in group]
                if len(set(ids)) != len(ids) or self.calls.intersection(ids) or len(self.calls) + len(ids) > self.engine.max_model_calls:
                    raise ValueError("cascade occurrence repeats or exceeds its permanent stage allowance")
                self.calls.update(ids)
                start = time.monotonic_ns()
                prepared = self.prepare.prepare_rows(group)
                finish = time.monotonic_ns()
                identities, _ = _prepared(prepared)
                if len(prepared) != len(group) or any(
                        any(row[k] != item[k] for k in IDENTITY_FIELDS)
                        for row, item in zip(identities, group, strict=True)):
                    raise ValueError("cascade formatter changed occurrence identity or cardinality")
                digests = [r["messages_sha256"] for r in identities]
                kernel = self._kernel()
                ready_uid = (getattr(kernel, "ready_report", None) or {}).get("engine_uid")
                if (type(ready_uid) is not str or not ready_uid
                        or getattr(kernel, "process", None) is not None and kernel.ready_report.get("pid") != kernel.process.pid):
                    raise ValueError("cascade ready instance differs from its owned private child")
                attempted = True
                observed = kernel.run_prepared(prepared)
                raw = observed if type(observed) is list else [observed]
                faulty = (type(observed) is not list or len(raw) != len(group)
                    or [r.get("call_id") if type(r) is dict else None for r in raw] != ids
                    or any(type(r) is dict and "engine_protocol_error" in r for r in raw))
                associated, verified = [], []
                for item in identities:
                    matches = [r for r in raw if type(r) is dict and r.get("call_id") == item["call_id"]]
                    submitted = False
                    try:
                        if len(matches) != 1:
                            raise ValueError("cascade terminal needs one uniquely associated result")
                        row = dict(matches[0])
                        submitted = (row.get("submission_confirmed") is True and row.get("engine_uid") == ready_uid
                            and row.get("messages_sha256") == item["messages_sha256"]
                            and all(row.get(k) == item[k] for k in IDENTITY_FIELDS))
                        if row.get("status") == "unknown" and row.get("messages_sha256") is None:
                            row["messages_sha256"] = item["messages_sha256"]
                        validate_terminal(row, item, engine_uid=ready_uid, messages_sha256=item["messages_sha256"])
                        row.update(prepare_worker_pid=os.getpid(), prepare_started_ns=start, prepare_finished_ns=finish)
                        verified.append(row)
                    except Exception as error:
                        row = self._failure(item, item["messages_sha256"], type(error).__name__,
                                            attempted=True, submitted=submitted)
                        faulty = True
                    self.broken = self.broken or row["status"] != "succeeded"
                    associated.append(row)
                if faulty:
                    self.broken = True
                    associated[0]["imlane_protocol_error"] = "CascadeResultProtocolError"
                    from src.baselines.text.frameworks.daft_engine_process import _publish
                    try:
                        _publish(self.control / ("cascade-audit-" + uuid.uuid4().hex + ".json"), dict(
                            query_id=self.query_id, stage=self.stage.name, model_role=self.stage.model_role,
                            engine_uid=ready_uid, known_receipts=verified, raw_results=deepcopy(raw)))
                    except Exception as error:
                        self.receipt_audit_errors.append("cascade_receipt_audit: " + type(error).__name__)
                results.extend((r, attempted) for r in associated)
            except Exception as error:
                self.broken = True
                results.extend((self._failure(item, digest, type(error).__name__, attempted=attempted), attempted)
                               for item, digest in zip(group, digests, strict=True))
        return results

    def envelope(self, source, **kwargs):
        value = super().envelope(source, **kwargs)
        value["worker"]["model_role"] = self.stage.model_role
        if value["result"]["kind"] == "filter":
            value["result"]["kind"] = "cascade"
        return value


def infer_records(table):
    table.validate(full=True)
    if (table.column_names != INPUT.names or table.schema.metadata or not 0 < len(table) <= 16
            or any(f.type != INPUT.field(i).type or f.metadata for i, f in enumerate(table.schema))
            or any(table.column(i).null_count for i in range(table.num_columns))):
        raise ValueError("cascade requires the existing bounded records Arrow schema")
    path = Path(os.environ["IMLANE_CASCADE_CONFIG"])
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024**2:
        raise ValueError("cascade stage configuration is not a bounded private file")
    config = json.loads(path.read_text())
    rows = table.to_pylist()
    stage = stage_values(config["stage"])
    if any((r["query_id"], r["stage_id"]) != (config["query_id"], stage.name) for r in rows):
        raise ValueError("cascade input crosses query or stage")
    fingerprint = hashlib.sha256(json.dumps(dict(kind="cascade", **{k: config[k] for k in (
        "stage", "engine", "resources", "model_environment")}), sort_keys=True).encode()).hexdigest()
    worker = owned_worker(config, fingerprint, CascadeWorker)
    if worker.query_id != config["query_id"]:
        raise ValueError("cascade interpreter belongs to another query")
    if len(rows) == 1 and json.loads(rows[0]["record_json"]) == {"operation": "close"}:
        report = worker.close()
        if worker.closed:
            slot.finished.add(worker.query_id)
        outcomes = [worker.envelope(rows[0], close=report)]
    else:
        outcomes = [worker.envelope(source, result=result, attempted=attempted)
                    for source, (result, attempted) in zip(rows, worker.run(rows), strict=True)]
    return pa.Table.from_pylist([dict(call_id=r["call_id"], row_id=r["row_id"],
        outcome_json=json.dumps(outcome, ensure_ascii=False, allow_nan=False))
        for r, outcome in zip(rows, outcomes, strict=True)], schema=OUTPUT)
