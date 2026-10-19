"""Worker-local finite LOTUS chain computation, shared by native adapters.

The only stage loop belongs to LotusRecordChain.run_batch. This module owns one
private model process and audit snapshots, but no vendor scheduling or iterator.
"""

from copy import deepcopy
import json
import math
import os
from pathlib import Path
import socket
import time
import uuid

from src.semantic_methods.lotus.record_chain import LotusRecordChain, RecordOccurrence
from src.serving.backends.vllm_engine import EngineSpec, IDENTITY_FIELDS
from .lotus_chain_results import _json, _source_binding


def _check(deadline, control):
    if (Path(control) / "stop").exists():
        raise InterruptedError("LOTUS chain cancelled")
    if time.monotonic() >= deadline:
        raise TimeoutError("LOTUS chain query deadline expired")



class FusedChainBatch:
    """One native worker, with local methods and one private model process."""

    _engine_type = None

    def __init__(self, chain, engine, expected, control, deadline, startup_timeout_s,
                 driver_host, control_token):
        from .daft_engine_process import _publish

        if socket.gethostname() != driver_host:
            raise RuntimeError("LOTUS chain supports a single local host only")
        self.control = Path(control)
        if (self.control / "owner").read_text() != control_token:
            raise RuntimeError("LOTUS chain requires a shared local control directory")
        _check(deadline, self.control)
        self.chain = LotusRecordChain.from_dict(chain)
        if len({stage.method.model_name for stage in self.chain.stages}) != 1:
            raise ValueError("this fused chain requires one model deployment across all stages")
        self.engine_spec = EngineSpec.from_dict(engine)
        if any(stage.generation.max_tokens > self.engine_spec.max_model_len for stage in self.chain.stages):
            raise ValueError("chain max_tokens exceeds max_model_len")
        self.expected = {row["row_id"]: row for row in expected}
        self.received = set()
        self.deadline, self.startup_timeout_s = deadline, startup_timeout_s
        self.engine = None
        self._engine_start_attempted = False
        self.failed = False
        self.worker_report = self.control / ("chain-worker-" + uuid.uuid4().hex + ".json")
        _publish(self.worker_report, dict(engine_allocation_started=False, worker_pid=os.getpid()))

    def _infer(self, prepared, *, generation):
        from .daft_engine_process import _publish
        from .lotus_chain_engine_process import ChainEngineProcess

        _check(self.deadline, self.control)
        prior = {call["row_id"]: call["call_id"] for call in self._audit_calls}
        declared = [dict({key: row[key] for key in IDENTITY_FIELDS}, status="unknown",
                         parent_call_ids=[] if row["row_id"] not in prior else [prior[row["row_id"]]],
                         submission_confirmed=False, error_type="NativeTaskUnconfirmed",
                         prompt_tokens=None, completion_tokens=None, total_tokens=None)
                    for row in prepared]
        self._audit_calls.extend(declared)
        self._raw_stage_id = prepared[0]["algorithm_stage"]
        audit = dict(batch_id=self._batch_id, raw_stage_id=self._raw_stage_id, calls=self._audit_calls)
        _publish(self._audit_path, audit)
        if self.engine is None:
            _publish(self.worker_report, dict(engine_allocation_started=True, worker_pid=os.getpid()))
            self._engine_start_attempted = True
            self.engine = (self._engine_type or ChainEngineProcess)(
                self.engine_spec, generation, self.control, self.deadline,
                self.startup_timeout_s)
        results = self.engine.run_prepared(prepared, generation=generation)
        for declaration in declared:
            matches = [row for row in results if type(row) is dict and all(
                row.get(key) == declaration[key] for key in IDENTITY_FIELDS)]
            if len(matches) == 1:
                parents = declaration["parent_call_ids"]
                declaration.clear()
                declaration.update(deepcopy(matches[0]), parent_call_ids=parents)
        raw = [row for row in results if type(row) is not dict or sum(
            type(other) is dict and all(other.get(key) == row.get(key) for key in IDENTITY_FIELDS)
            for other in results) != 1 or not any(
                all(row.get(key) == item[key] for key in IDENTITY_FIELDS) for item in declared)]
        try:
            _publish(self._audit_path, dict(audit, raw_results=raw))
        except BaseException as exc:
            self._audit_errors.append("call_ledger_write: " + type(exc).__name__)
        return results

    def _run_batch(self, batch):
        from .daft_engine_process import _publish

        if self.failed:
            # Ray may already have queued another batch before the consumer
            # sees the first error. Preserve that error without another call.
            return self._failure_json
        self._audit_calls, self._audit_errors = [], []
        self._batch_id, self._raw_stage_id = uuid.uuid4().hex, None
        self._audit_path = self.control / ("chain-audit-" + self._batch_id + ".json")
        records = []
        envelope = dict(inputs=[],
                        batch_id=self._batch_id, raw_stage_id=None,
                        records=[], calls=[], stage_stats={}, columns=[], raw_results=[], error_type=None,
                        native_input_rows=0, worker_pid=os.getpid(), cleanup_errors=[])
        try:
            _check(self.deadline, self.control)
            batch_ids = set()
            for row in batch.to_pylist():
                if set(row) != {"record_json"} or type(row["record_json"]) is not str:
                    raise ValueError("chain input requires one record_json string column")
                record = RecordOccurrence.from_dict(json.loads(row["record_json"]))
                if (self.expected.get(record.row_id) != _source_binding(record)
                        or record.row_id in self.received or record.row_id in batch_ids):
                    raise ValueError("chain input changed or duplicated an occurrence")
                batch_ids.add(record.row_id)
                records.append(record)
            self.received.update(batch_ids)
            envelope.update(inputs=[_source_binding(record) for record in records], native_input_rows=len(records))
            result = self.chain.run_batch(records, self._infer)
            envelope.update(records=[record.describe() for record in result["records"]],
                            calls=result["calls"], stage_stats=result["stage_stats"],
                            columns=result["columns"])
            _check(self.deadline, self.control)
        except BaseException as exc:
            self.failed = True
            # The method attaches calls before parsing, including known usage
            # when a later parse or dependent generation fails.
            envelope.update(calls=getattr(exc, "chain_calls", envelope["calls"]),
                            stage_stats=getattr(exc, "chain_stage_stats", envelope["stage_stats"]),
                            raw_results=getattr(exc, "chain_raw_results", []),
                            records=[], error_type=type(exc).__name__)
            envelope["raw_stage_id"] = self._raw_stage_id
            try:
                _publish(self.control / ("chain-failure-" + self._batch_id + ".json"), envelope)
            except BaseException as recording:
                envelope["cleanup_errors"].append("failure_ledger_write: " + type(recording).__name__)
        finally:
            envelope["cleanup_errors"].extend(self._audit_errors)
            if self.engine is not None and (self.failed or len(self.received) == len(self.expected)):
                try:
                    envelope["engine_close"] = self.engine.close()
                except BaseException as exc:
                    envelope["cleanup_errors"].append("engine_close: " + type(exc).__name__)
                    try:
                        _publish(self.control / "engine-close-failure.json", dict(
                            status="unknown", process_exit_confirmed=False,
                            cleanup_errors=envelope["cleanup_errors"]))
                    except BaseException:
                        pass
        # Even a wholly discarded batch returns its preceding model-call ledger.
        encoded = _json(envelope)
        if self.failed:
            self._failure_json = encoded
        return encoded


    def run_rows(self, rows):
        """Run one bounded list of record_json rows and return its batch envelope."""
        class RowBatch:
            def to_pylist(self):
                if type(rows) is not list:
                    raise TypeError("chain input requires a finite list of record_json rows")
                return rows

        return json.loads(self._run_batch(RowBatch()))

    def __call__(self, batch):
        """Keep Ray's one-envelope Arrow output for its native map_batches API."""
        import pyarrow as pa

        return pa.Table.from_pylist([{"batch_json": self._run_batch(batch)}])

    def close(self, timeout=None):
        """Retry private engine cleanup; this never confirms physical GPU exit."""
        if timeout is not None and (type(timeout) not in (int, float)
                                    or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("chain close timeout must be finite and positive")
        if self.engine is None:
            if self._engine_start_attempted:
                return dict(status="unknown", engine_api_shutdown_confirmed=False,
                            process_exit_confirmed=None, gpu_exit_confirmed=None)
            return dict(status="closed", engine_api_shutdown_confirmed=True,
                        process_exit_confirmed=True, gpu_exit_confirmed=True)
        return self.engine.close() if timeout is None else self.engine.close(timeout=timeout)
