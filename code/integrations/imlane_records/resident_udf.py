"""Opt-in resident ordinary Filter kernel on the unchanged author Arrow lane."""

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

import pyarrow as pa

from integrations.imlane_records.echo_udf import INPUT, OUTPUT
from integrations.imlane_records.filter_udf import FilterWorker
from integrations.imlane_records.filter_workload import PROTOCOL
from integrations.imlane_records.method_owner import owned_worker, slot
from integrations.imlane_records.model_lifecycle import close_owned_model, model_closed
from integrations.imlane_records.resident_model import QueryModel, ResidentEngineProcess
from src.semantic_methods.lotus.records import RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


class ResidentWorker:
    def __init__(self, config, *, engine_type=ResidentEngineProcess, prepare_type=None):
        self.engine = EngineSpec.from_dict(config["engine"])
        self.generation = GenerationSpec.from_dict(config["generation"])
        self.resources = RecordsResources(**config["resources"])
        self.query_id = config["query_id"]  # The interpreter role belongs to the session.
        identity(self.query_id, "session", 0)
        self.control, self.deadline = Path(config["control_dir"]), config["deadline_monotonic"]
        if (not self.control.is_dir() or self.control.is_symlink()
                or type(self.deadline) not in (float, int) or not time.monotonic() < self.deadline < float("inf")
                or self.engine.max_model_calls is None or self.engine.max_model_calls > 4096
                or self.resources.cpu_workers != 1 or self.resources.prepare_batch_rows > 16
                or self.resources.model_batch_rows > 16 or self.resources.max_active_batches > 2):
            raise ValueError("resident Filter requires the existing finite one-executor capacities")
        self.environment = dict(config["model_environment"])
        self.engine_type, self.prepare_type = engine_type, prepare_type
        self.uid = uuid.uuid4().hex
        self.model = self.work = None
        self.initializations = self.ready_count = self.reserved_calls = 0
        self.finished, self.controls = set(), set()
        self.method_model_name = None
        self.closed = self.broken = False
        self.close_report = None

    def _check(self):
        if self.closed or self.broken:
            raise RuntimeError("resident owner is terminal")
        if (self.control / "stop").exists():
            raise InterruptedError("IMLane resident session cancelled")
        if time.monotonic() >= self.deadline:
            raise TimeoutError("IMLane resident session expired")

    def snapshot(self):
        ready = dict(getattr(self.model, "ready_report", None) or {})
        return dict(engine_ready=ready, engine_uid=ready.get("engine_uid"), pid=ready.get("pid"),
                    model_process_created=getattr(self.model, "created", None),
                    worker_uid=self.uid, worker_pid=os.getpid(),
                    model_initialization_attempts=self.initializations, model_ready_count=self.ready_count,
                    model_load_count_observed=None, reserved_query_calls=self.reserved_calls,
                    **({"native_mfu": self.model.mfu_snapshot() if self.model is not None else None}
                       if self.engine.enable_mfu_metrics else {}))

    def start(self):
        self._check()
        if self.model is None:
            if self.initializations:
                raise RuntimeError("resident model initialization cannot be retried")
            self.initializations += 1
            try:
                self.model = self.engine_type(self.engine, self.generation, self.control, self.deadline,
                    self.resources.startup_timeout_s, model_environment=self.environment)
                ready = self.model.ready_report
                if (type(ready.get("engine_uid")) is not str or not ready["engine_uid"]
                        or getattr(self.model, "process", None) is not None
                        and ready.get("pid") != self.model.process.pid):
                    raise ValueError("resident model ready identity differs")
                self.ready_count += 1
            except BaseException:
                self.broken = True
                raise
        return self.snapshot()

    def begin(self, config):
        self._check()
        query = config["query_id"]
        control = Path(config["control_dir"])
        spec = EngineSpec.from_dict(config["engine"])
        if (self.model is None or self.work is not None or query in self.finished or query == self.query_id
                or len(self.finished) >= 62 or control.is_symlink() or not control.is_dir()
                or not control.resolve().is_relative_to(self.control.resolve())
                or control.resolve() == self.control.resolve() or str(control.resolve()) in self.controls
                or type(config["deadline_monotonic"]) not in (float, int)
                or not time.monotonic() < config["deadline_monotonic"] <= self.deadline
                or {k: v for k, v in asdict(spec).items() if k != "max_model_calls"}
                != {k: v for k, v in asdict(self.engine).items() if k != "max_model_calls"}
                or asdict(GenerationSpec.from_dict(config["generation"])) != asdict(self.generation)
                or config["resources"] != asdict(self.resources)
                or config["model_environment"] != self.environment
                or self.method_model_name is not None and config["method"]["model_name"] != self.method_model_name
                or spec.max_model_calls is None or spec.max_model_calls > 4094
                or self.reserved_calls + spec.max_model_calls > self.engine.max_model_calls):
            raise ValueError("query changes the resident deployment, repeats state or exceeds the session allowance")
        view = QueryModel(self.model, control, config["deadline_monotonic"])
        work = FilterWorker(config, prepare_type=self.prepare_type, engine_type=lambda *_: view)
        # Reserve a complete query before accepting data; failed/unknown calls
        # never refund its allowance or create a restart path.
        self.reserved_calls += spec.max_model_calls
        self.controls.add(str(control.resolve()))
        self.method_model_name = work.method.model_name
        self.work = work
        return self.snapshot()

    def end(self, query):
        if (self.closed or self.work is None or query != self.work.query_id
                or self.broken or self.work.broken or len(self.work.calls) != self.work.engine.max_model_calls):
            raise RuntimeError("only a complete healthy query can return its resident owner")
        self.finished.add(query)
        self.work = None
        return dict(status="query_finished", model_resident=True, **self.snapshot())

    def run(self, rows):
        if self.closed or self.work is None or any(row["query_id"] != self.work.query_id for row in rows):
            raise ValueError("resident data belongs to no active query")
        # Already accepted native blocks must still receive one terminal per
        # occurrence. FilterWorker refuses model entry after its first failure.
        self.work.broken = self.work.broken or self.broken
        if (self.control / "stop").exists():
            (self.work.control / "stop").touch()
        results = self.work.run(rows)
        self.broken = self.broken or self.work.broken
        return [self.work.envelope(row, result=result, attempted=attempted)
                for row, (result, attempted) in zip(rows, results, strict=True)]

    def close(self):
        if self.closed:
            return self.close_report
        self.close_report = dict(close_owned_model(self.model, attempted=bool(self.initializations)),
                                 resident=self.snapshot())
        self.closed = model_closed(self.close_report)
        self.broken = self.broken or not self.closed
        return self.close_report


def infer_records(table):
    table.validate(full=True)
    if (table.column_names != INPUT.names or table.schema.metadata or not 0 < len(table) <= 16
            or any(field.type != INPUT.field(i).type or field.metadata for i, field in enumerate(table.schema))
            or any(table.column(i).null_count for i in range(table.num_columns))):
        raise ValueError("resident Filter requires the fixed bounded Arrow records schema")
    rows = table.to_pylist()
    if any((row["query_id"], row["stage_id"]) != (rows[0]["query_id"], "filter") for row in rows):
        raise ValueError("resident records cross query or stage")
    config = json.loads(Path(os.environ["IMLANE_FILTER_CONFIG"]).read_text())
    fingerprint = hashlib.sha256(json.dumps(dict(kind="resident-filter", **config), sort_keys=True).encode()).hexdigest()
    worker = owned_worker(config, fingerprint, ResidentWorker)
    payload = json.loads(rows[0]["record_json"])
    operation = payload.get("operation") if len(rows) == 1 else None
    try:
        if operation == "session_start" and rows[0]["query_id"] == worker.query_id + ":startup":
            result = worker.start()
        elif operation == "query_begin":
            if set(payload) != {"operation", "config_json"} or type(payload["config_json"]) is not str:
                raise ValueError("query_begin requires the native string-only control carrier")
            config = json.loads(payload["config_json"])
            if type(config) is not dict or rows[0]["query_id"] != config.get("query_id"):
                raise ValueError("query_begin configuration belongs to another query")
            # Reject non-finite values even in fields not consumed by begin;
            # parse_constant alone also misses overflowing JSON numbers.
            json.dumps(config, allow_nan=False)
            result = worker.begin(config)
        elif operation == "query_end":
            result = worker.end(rows[0]["query_id"])
        elif operation == "session_close" and rows[0]["query_id"] == worker.query_id + ":close":
            result = worker.close()
            if not model_closed(result):
                raise RuntimeError("resident model API or process close is unconfirmed")
            slot.finished.add(worker.query_id)
        elif operation is not None:
            raise ValueError("unexpected resident lifecycle operation")
        else:
            outcomes = worker.run(rows)
            return pa.Table.from_pylist([dict(call_id=row["call_id"], row_id=row["row_id"],
                outcome_json=json.dumps(value, ensure_ascii=False, allow_nan=False))
                for row, value in zip(rows, outcomes, strict=True)], schema=OUTPUT)
        error = None
    except Exception as failure:
        worker.broken = True
        error, result = type(failure).__name__, dict(close_report=worker.close_report, **worker.snapshot())
    row = rows[0]
    result.update(kind="resident_control", operation=operation, _occurrence_id=row["call_id"], _position=row["row_id"])
    outcome = dict(protocol=PROTOCOL, query_id=row["query_id"], stage_id="filter",
        status="error" if error else "ok", error_type=error, error=error, result=result,
        model_entry_attempted=bool(worker.initializations), completion_known=True,
        worker=dict(worker_pid=os.getpid(), worker_uid=worker.uid))
    return pa.Table.from_pylist([dict(call_id=row["call_id"], row_id=row["row_id"],
        outcome_json=json.dumps(outcome, ensure_ascii=False, allow_nan=False))], schema=OUTPUT)
