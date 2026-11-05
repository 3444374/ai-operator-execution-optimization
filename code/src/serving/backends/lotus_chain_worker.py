"""Native finite LOTUS method computation, shared by independent execution owners.

The only stage loop belongs to LotusRecordChain.run_batch. This module owns one
private model process and audit snapshots, but no scheduling or request-packing policy.
"""

from copy import deepcopy
import json
import math
import os
from pathlib import Path
import socket
import time
import uuid

from src.semantic_methods.lotus.record_chain import LotusRecordChain
from src.semantic_methods.lotus.record_types import (
    RecordOccurrence, _ordered_columns, _parent_seeds,
)
from src.serving.backends.vllm_protocol import EngineSpec, IDENTITY_FIELDS
from src.semantic_methods.lotus.record_results import _json, _source_binding


def _check(deadline, control):
    if (Path(control) / "stop").exists():
        raise InterruptedError("LOTUS chain cancelled")
    if time.monotonic() >= deadline:
        raise TimeoutError("LOTUS chain query deadline expired")


def _phase_values(chain, expected, initial_parents, columns, allow_nulls):
    chain = LotusRecordChain.from_dict(chain)
    if any(stage.kind == "extract" for stage in chain.stages[:-1]):
        raise ValueError("a fused phase must end at its Extract relation barrier")
    if len({stage.method.model_name for stage in chain.stages}) != 1:
        raise ValueError("this fused chain requires one model deployment across all stages")
    if type(allow_nulls) is not bool:
        raise TypeError("allow_nulls must be a boolean")
    if type(expected) not in (list, tuple) or any(type(row) is not dict for row in expected):
        raise ValueError("expected must be a finite sequence of source bindings")
    bindings = {row["row_id"]: deepcopy(row) for row in expected}
    if len(bindings) != len(expected):
        raise ValueError("expected contains duplicate occurrence IDs")
    parents = _parent_seeds([(row["row_id"], row["source_position"]) for row in expected], initial_parents)
    return chain, bindings, parents, _ordered_columns([], columns)


def _associate_declared_results(declared, results):
    # Index ordinary protocol identities once. Damaged/non-scalar identities
    # retain the original equality path, including Python's bool/int aliases.
    groups = None
    if type(results) is list and all(
        type(row) is dict and all(type(row.get(key)) in (str, int) for key in IDENTITY_FIELDS)
        for rows in (declared, results) for row in rows
    ):
        groups, keys = {}, []
        for row in results:
            key = tuple(row[name] for name in IDENTITY_FIELDS)
            keys.append(key)
            groups.setdefault(key, []).append(row)
        expected = {tuple(row[name] for name in IDENTITY_FIELDS) for row in declared}
    for declaration in declared:
        matches = (groups.get(tuple(declaration[key] for key in IDENTITY_FIELDS), [])
                   if groups is not None else
                   [row for row in results if type(row) is dict and all(
                       row.get(key) == declaration[key] for key in IDENTITY_FIELDS)])
        if len(matches) == 1:
            parents = declaration["parent_call_ids"]
            declaration.clear()
            declaration.update(deepcopy(matches[0]), parent_call_ids=parents)
    if groups is not None:
        return [row for row, key in zip(results, keys)
                if len(groups[key]) != 1 or key not in expected]
    return [row for row in results if type(row) is not dict or sum(
        type(other) is dict and all(other.get(key) == row.get(key) for key in IDENTITY_FIELDS)
        for other in results) != 1 or not any(
            all(row.get(key) == item[key] for key in IDENTITY_FIELDS) for item in declared)]


class FusedChainBatch:
    """One native worker, with local methods and one private model process."""

    _engine_type = None

    def __init__(self, chain, engine, expected, control, deadline, startup_timeout_s,
                 driver_host, control_token, *, initial_parents=None, columns=None,
                 allow_nulls=False, close_on_source_complete=True, release_completed_audit=False,
                 prepared_batching=None):
        from src.serving.backends.vllm_sync_process import _publish

        if type(release_completed_audit) is not bool:
            raise TypeError("release_completed_audit must be a boolean")
        self.release_completed_audit = release_completed_audit
        if socket.gethostname() != driver_host:
            raise RuntimeError("LOTUS chain supports a single local host only")
        self.control = Path(control)
        if (self.control / "owner").read_text() != control_token:
            raise RuntimeError("LOTUS chain requires a shared local control directory")
        _check(deadline, self.control)
        if type(close_on_source_complete) is not bool:
            raise TypeError("close_on_source_complete must be a boolean")
        self.chain, self.expected, self.initial_parents, self.columns = _phase_values(
            chain, expected, initial_parents, columns, allow_nulls)
        self.engine_spec = EngineSpec.from_dict(engine)
        self._configure_batching(prepared_batching)
        if self.engine_spec.max_model_len is not None and any(
                stage.generation.max_tokens > self.engine_spec.max_model_len for stage in self.chain.stages):
            raise ValueError("chain max_tokens exceeds max_model_len")
        self.allow_nulls, self.close_on_source_complete = allow_nulls, close_on_source_complete
        self._phase_options = initial_parents is not None or columns is not None or allow_nulls
        self._phase_stage_ids = [stage.stage_id for stage in self.chain.stages]
        self._used_stage_ids = set(self._phase_stage_ids)
        self._model_name = self.chain.stages[0].method.model_name
        self._phase_cleanup_errors, self._owner_close_started = [], False
        self.received = set()
        self.deadline, self.startup_timeout_s = deadline, startup_timeout_s
        self.engine = None
        self._model_context = None
        self._engine_start_attempted = False
        self.failed = False
        self.worker_report = self.control / ("chain-worker-" + uuid.uuid4().hex + ".json")
        _publish(self.worker_report, dict(engine_allocation_started=False, worker_pid=os.getpid()))

    def begin_relation_phase(self, chain, expected, *, initial_parents, columns,
                             allow_nulls, source_exhausted):
        """Accept an owner-completed relation, keeping this worker's existing model lifetime."""
        if (source_exhausted is not True or self.received != set(self.expected) or self.failed
                or self._phase_cleanup_errors or self._owner_close_started or self.close_on_source_complete):
            raise ValueError("previous relation phase is not a successful exhausted open phase")
        if type(initial_parents) is not dict or columns is None:
            raise ValueError("a relation phase requires explicit parent seeds and ordered columns")
        _check(self.deadline, self.control)
        next_chain, bindings, parents, schema = _phase_values(
            chain, expected, initial_parents, columns, allow_nulls)
        ids = {stage.stage_id for stage in next_chain.stages}
        context_limit = (self._bind_model_context()['max_model_len'] if self.engine is not None
                         else self.engine_spec.max_model_len)
        if (next_chain.query_id != self.chain.query_id or ids & self._used_stage_ids
                or next_chain.stages[0].method.model_name != self._model_name
                or context_limit is not None and any(
                    stage.generation.max_tokens > context_limit for stage in next_chain.stages)):
            raise ValueError("relation phase changed query, stage IDs, deployment or model context")
        self.chain, self.expected, self.initial_parents, self.columns = next_chain, bindings, parents, schema
        self.allow_nulls, self.received = allow_nulls, set()
        self._phase_options = True
        self._phase_stage_ids = [stage.stage_id for stage in next_chain.stages]
        self._used_stage_ids.update(ids)
        self._phase_cleanup_errors = []

    def _bind_model_context(self):
        from src.serving.backends.model_context import checked_model_context
        ready = getattr(self.engine, 'ready_report', None)
        if type(ready) is dict and (ready.get('kind', 'ready') != 'ready'
                                   or ready.get('closed', False) is not False):
            raise RuntimeError('fused worker requires a successful open model ready receipt')
        observed = checked_model_context(self.engine_spec, ready)
        if self._model_context is not None and observed != self._model_context:
            raise RuntimeError('fused worker model context changed during its engine lifetime')
        if any(stage.generation.max_tokens > observed['max_model_len'] for stage in self.chain.stages):
            raise ValueError('chain max_tokens exceeds resolved model context')
        self._model_context = observed
        return dict(observed)

    def _configure_batching(self, value):
        if value is not None:
            raise ValueError("native chain workers do not accept Core prepared batching")

    def _prepare_requests(self, prepared, generation):
        return None

    def _call_audit(self):
        return dict(batch_id=self._batch_id, raw_stage_id=self._raw_stage_id, calls=self._audit_calls,
                    phase_stage_ids=list(self._phase_stage_ids))

    def _start_engine(self, generation, declared, audit):
        from src.serving.backends.vllm_sync_process import _publish

        if self.engine is None:
            _publish(self.worker_report, dict(engine_allocation_started=True, worker_pid=os.getpid()))
            self._engine_start_attempted = True
            self.engine = self._make_engine(generation, declared, audit)

    def _make_engine(self, generation, declared, audit):
        from src.serving.backends.staged_generation_process import ChainEngineProcess

        return (self._engine_type or ChainEngineProcess)(
            self.engine_spec, generation, self.control, self.deadline, self.startup_timeout_s)

    def _run_prepared(self, prepared, generation, declared, audit):
        return self.engine.run_prepared(prepared, generation=generation)

    def _begin_batch(self, source_flush_reason, prepared_info):
        if source_flush_reason is not None or prepared_info is not None:
            raise ValueError("native chain workers receive whole record batches")

    def _method_options(self, prepared, prepared_info):
        return {}

    def _batch_receipts(self, envelope, *, failed):
        return envelope

    def _infer(self, prepared, *, generation):
        from src.serving.backends.vllm_sync_process import _publish

        _check(self.deadline, self.control)
        rejected = self._prepare_requests(prepared, generation)
        if rejected is not None:
            return rejected
        prior = {call["row_id"]: call["call_id"] for call in self._audit_calls}
        declared = [dict({key: row[key] for key in IDENTITY_FIELDS}, status="unknown",
                         parent_call_ids=(list(self.initial_parents[(row["row_id"], row["source_position"])])
                                          if row["row_id"] not in prior else [prior[row["row_id"]]]),
                         submission_confirmed=False, error_type="NativeTaskUnconfirmed",
                         prompt_tokens=None, completion_tokens=None, total_tokens=None)
                    for row in prepared]
        self._audit_calls.extend(declared)
        self._raw_stage_id = prepared[0]["algorithm_stage"]
        audit = self._call_audit()
        _publish(self._audit_path, audit)
        self._start_engine(generation, declared, audit)
        try:
            audit['model_context'] = self._bind_model_context()
        except Exception as exc:
            # Readiness failed before any stage request entered the model API.
            # Engine cleanup still owns any allocation uncertainty separately.
            results = [dict({name: row[name] for name in IDENTITY_FIELDS}, status='failed',
                submission_confirmed=False, text=None, error_type='ModelContextUnavailable',
                cause_type=type(exc).__name__, prompt_tokens=None,
                completion_tokens=None, total_tokens=None) for row in prepared]
        else:
            _publish(self._audit_path, audit)
            results = self._run_prepared(prepared, generation, declared, audit)
        raw = _associate_declared_results(declared, results)
        try:
            _publish(self._audit_path, dict(audit, raw_results=raw))
        except BaseException as exc:
            self._audit_errors.append("call_ledger_write: " + type(exc).__name__)
        return results

    def _run_batch(self, batch, *, source_flush_reason=None, prepared_info=None):
        from src.serving.backends.vllm_sync_process import _publish

        if self.failed:
            # Ray may already have queued another batch before the consumer
            # sees the first error. Preserve that error without another call.
            return self._failure_json
        self._audit_calls, self._audit_errors = [], []
        self._begin_batch(source_flush_reason, prepared_info)
        self._batch_id, self._raw_stage_id = uuid.uuid4().hex, None
        self._audit_path = self.control / ("chain-audit-" + self._batch_id + ".json")
        records, prepared = [], []
        envelope = dict(inputs=[],
                        batch_id=self._batch_id, raw_stage_id=None,
                        records=[], calls=[], stage_stats={}, columns=list(self.columns), raw_results=[], error_type=None,
                        phase_stage_ids=list(self._phase_stage_ids),
                        native_input_rows=0, worker_pid=os.getpid(), cleanup_errors=[])
        try:
            _check(self.deadline, self.control)
            if self._owner_close_started:
                raise RuntimeError("a closed chain owner cannot begin more work")
            batch_ids = set()
            for row in batch.to_pylist():
                keys = {'record_json'} if prepared_info is None else {'record_json', 'prepared'}
                if set(row) != keys or type(row["record_json"]) is not str:
                    raise ValueError("chain input requires one record_json string column")
                record = RecordOccurrence.from_dict(json.loads(row["record_json"]))
                if (self.expected.get(record.row_id) != _source_binding(record)
                        or record.row_id in self.received or record.row_id in batch_ids):
                    raise ValueError("chain input changed or duplicated an occurrence")
                batch_ids.add(record.row_id)
                records.append(record)
                if prepared_info is not None:
                    prepared.append(row['prepared'])
            self.received.update(batch_ids)
            envelope.update(inputs=[_source_binding(record) for record in records], native_input_rows=len(records))
            seeds = {(record.row_id, record.source_position): self.initial_parents[(record.row_id, record.source_position)]
                     for record in records}
            options = self._method_options(prepared, prepared_info)
            result = (self.chain.run_batch(records, self._infer, initial_parents=seeds,
                                          columns=self.columns, allow_nulls=self.allow_nulls, **options)
                      if self._phase_options else self.chain.run_batch(records, self._infer, **options))
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
                            columns=getattr(exc, "chain_columns", envelope["columns"]),
                            records=[], error_type=type(exc).__name__)
            envelope["raw_stage_id"] = self._raw_stage_id
            self._batch_receipts(envelope, failed=True)
            try:
                _publish(self.control / ("chain-failure-" + self._batch_id + ".json"), envelope)
            except BaseException as recording:
                envelope["cleanup_errors"].append("failure_ledger_write: " + type(recording).__name__)
        finally:
            self._batch_receipts(envelope, failed=False)
            envelope["cleanup_errors"].extend(self._audit_errors)
            if self.engine is not None and (self.failed or self.close_on_source_complete
                                           and len(self.received) == len(self.expected)):
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
            self._phase_cleanup_errors.extend(envelope["cleanup_errors"])
        # Even a wholly discarded batch returns its preceding model-call ledger.
        encoded = _json(envelope)
        if self.failed:
            self._failure_json = encoded
        if (self.release_completed_audit and not self.failed and not envelope["cleanup_errors"]
                and ("engine_close" not in envelope or
                     envelope["engine_close"]["status"] == "closed"
                     and not envelope["engine_close"]["cleanup_errors"])
                and not (self.control / "stop").exists() and time.monotonic() < self.deadline):
            # Encoding and audit recovery no longer need this worker's copy.
            # Rebind so any other owner of the list keeps its complete data.
            self._audit_calls = []
        return encoded


    def run_rows(self, rows, *, source_flush_reason=None, prepared_info=None):
        """Run one bounded list of record_json rows and return its batch envelope."""
        class RowBatch:
            def to_pylist(self):
                if type(rows) is not list:
                    raise TypeError("chain input requires a finite list of record_json rows")
                return rows

        return json.loads(self._run_batch(RowBatch(), source_flush_reason=source_flush_reason,
                                          prepared_info=prepared_info))

    def __call__(self, batch):
        """Keep Ray's one-envelope Arrow output for its native map_batches API."""
        import pyarrow as pa

        return pa.Table.from_pylist([{"batch_json": self._run_batch(batch)}])

    def close(self, timeout=None):
        """Retry private engine cleanup; this never confirms physical GPU exit."""
        if timeout is not None and (type(timeout) not in (int, float)
                                    or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("chain close timeout must be finite and positive")
        self._owner_close_started = True
        if self.engine is None:
            if self._engine_start_attempted:
                return dict(status="unknown", engine_api_shutdown_confirmed=False,
                            process_exit_confirmed=None, gpu_exit_confirmed=None)
            return dict(status="closed", engine_api_shutdown_confirmed=True,
                        process_exit_confirmed=True, gpu_exit_confirmed=True)
        return self.engine.close() if timeout is None else self.engine.close(timeout=timeout)
