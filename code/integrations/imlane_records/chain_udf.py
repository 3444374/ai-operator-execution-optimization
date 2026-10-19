"""IMLane Arrow carrier for the shared finite LOTUS chain batch kernel."""

import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import uuid

import pyarrow as pa

from integrations.imlane_records.chain_workload import STAGE, batch_rows
from integrations.imlane_records.echo_udf import INPUT, OUTPUT
from integrations.imlane_records.filter_workload import PROTOCOL
from integrations.imlane_records.method_owner import owned_worker, slot as _slot
from integrations.imlane_records.model_lifecycle import close_owned_model, model_closed
from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec


class ChainWorker:
    def __init__(self, config, *, engine_type=None):
        from integrations.imlane_records.model_process import IMLaneChainEngineProcess

        resources = RecordsResources(**config["resources"])
        engine = EngineSpec.from_dict(config["engine"])
        if (resources.cpu_workers != 1 or resources.model_batch_rows > 16 or resources.max_active_batches > 2
                or engine.max_model_calls is None or engine.max_model_calls > 4096):
            raise ValueError("IMLane chain requires one bounded worker and finite model allowance")
        environment = dict(config["model_environment"])

        class AssignedEngine(IMLaneChainEngineProcess):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, model_environment=environment, **kwargs)

        class OwnedBatch(FusedChainBatch):
            _engine_type = engine_type or AssignedEngine

        self.query_id, self.uid = config["query_id"], uuid.uuid4().hex
        if config["chain"]["query_id"] != self.query_id:
            raise ValueError("chain and transport must identify the same query")
        self.batch = OwnedBatch(config["chain"], config["engine"], config["expected"], config["control_dir"],
            config["deadline_monotonic"], resources.startup_timeout_s, config["driver_host"], config["control_token"])
        self.lock = threading.RLock()
        self.closed = self.broken = False
        self.close_report = None

    def run(self, source):
        with self.lock:
            if self.closed:
                raise RuntimeError("closed chain owner cannot infer again")
            batch = self.batch.run_rows(batch_rows(source))
            self.broken = self.broken or bool(batch.get("error_type"))
            return batch

    def close(self):
        with self.lock:
            if self.closed:
                return self.close_report
            self.close_report = close_owned_model(self.batch.engine, attempted=self.batch._engine_start_attempted)
            self.closed = model_closed(self.close_report)
            self.broken = self.broken or not self.closed
            return self.close_report

    def envelope(self, source, *, batch=None, close=None):
        if close is None:
            ok = not batch.get("error_type")
            known = not any(call.get("status") == "unknown" for call in batch["calls"])
            payload = dict(kind="chain", batch=batch)
            error = None if ok else batch["error_type"]
        else:
            ok, known = model_closed(close), True
            payload = dict(kind="close", close_report=close)
            error = None if ok else "ModelCloseUnconfirmed"
        payload.update(_occurrence_id=source["call_id"], _position=source["row_id"])
        ready = getattr(self.batch.engine, "ready_report", None) or {}
        return dict(protocol=PROTOCOL, query_id=self.query_id, stage_id=STAGE,
            status="ok" if ok else "error", result=payload, error_type=error, error=error,
            model_entry_attempted=self.batch._engine_start_attempted, completion_known=known,
            worker=dict(worker_pid=os.getpid(), worker_uid=self.uid, python=sys.executable,
                        arrow=pa.__version__, engine_uid=ready.get("engine_uid")))


def infer_records(table):
    table.validate(full=True)
    if (table.column_names != INPUT.names or table.schema.metadata or len(table) != 1
            or any(field.type != INPUT.field(i).type or field.metadata for i, field in enumerate(table.schema))
            or any(table.column(i).null_count for i in range(table.num_columns))):
        raise ValueError("chain tasks require one fixed-schema transport occurrence")
    source = table.to_pylist()[0]
    config = json.loads(Path(os.environ["IMLANE_CHAIN_CONFIG"]).read_text())
    if source["query_id"] != config["query_id"] or source["stage_id"] != STAGE:
        raise ValueError("chain task crosses query or workload stage")
    signature = hashlib.sha256(json.dumps(dict(kind="chain", stages=config["chain"]["stages"],
        **{key: config[key] for key in ("engine", "resources", "model_environment")}), sort_keys=True).encode()).hexdigest()
    work = owned_worker(config, signature, ChainWorker)
    if config["query_id"] != work.query_id:
        raise ValueError("chain input does not belong to the current interpreter query")
    if json.loads(source["record_json"]) == {"operation": "close"}:
        outcome = work.envelope(source, close=work.close())
        if work.closed:
            _slot.finished.add(work.query_id)
    else:
        outcome = work.envelope(source, batch=work.run(source))
    return pa.Table.from_pylist([dict(call_id=source["call_id"], row_id=source["row_id"],
        outcome_json=json.dumps(outcome, ensure_ascii=False, allow_nan=False))], schema=OUTPUT)
