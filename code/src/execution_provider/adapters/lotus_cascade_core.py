"""Global LOTUS cascade stages on the existing production records Core.

The records Core currently has an ordinary-Filter identity and generation seam.
This adapter materializes each already-declared stage into a private CSV with
one provider occurrence per logical draw. It then uses that Core unchanged:
Daft scans the stage CSV, its existing actors prepare messages, SessionEngine
owns admission/credits, and the existing asynchronous transport submits work.
The trusted model actor restores canonical cascade identities before inference.

The extra CSV, aliases, rescans and per-stage model startup are query costs, not
a new optimization. A private EngineProcess provides IPC/process supervision
only; it has no shared model pool, scheduling policy or batching queue. Stage
closure must settle Core work and registered model processes before switching
roles. GPU release still requires independent observation.
"""

from __future__ import annotations

from copy import deepcopy
import csv
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import tempfile
import time

from src.baselines.common.private_artifacts import file_identity, open_private_text
from src.baselines.text.frameworks.lotus_cascade_process import StageEngineCleanup
from src.semantic_methods.lotus.cascade import (
    CascadeStage, IDENTITY_FIELDS, _StageCalls, cascade_identity, local_generation,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec

from .lotus_records_core import (
    CoreRecordsCapacity, SemLoomRecordsCore, _ActorOwner, _CoreModelActor, _check,
)


class _StageSource:
    """Bounded declared-stage relation; repeated draws receive distinct aliases.

    Its total materialization belongs to the method's global stage relation and
    is not covered by Core's per-task logical-byte reservations or an RSS cap.
    The original source is checked by the backend before writing this relation.
    """

    def __init__(self, stage, method, control, maximum):
        self.csv_path = control / "stage.csv"
        self.method = method.describe()
        self.scan = RecordScan(
            (("row_id", "string"), ("source_position", "int64"),
             *((column, "string") for column in stage.input_columns)),
            stage.input_columns,
        )
        seed = stage.records[0]["query_id"] if stage.records else "empty"
        self.query_id = "cascade-" + hashlib.sha256(
            (seed + ":" + stage.name).encode("utf-8"),
        ).hexdigest()
        self.bindings, self.expected_rows = {}, []
        with open_private_text(self.csv_path, newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=[name for name, _ in self.scan.csv_schema])
            writer.writeheader()
            for index, row in enumerate(stage.records):
                provider = identity(self.query_id, "draw-" + str(index), index)
                canonical = {name: row[name] for name in IDENTITY_FIELDS}
                # Preflight the same raw row bytes Core will later reserve.
                from .lotus_records_core import _encode
                payload = {name: row[name] for name in stage.input_columns}
                _encode(dict(payload, **provider), maximum)
                self.expected_rows.append(provider)
                self.bindings[provider["call_id"]] = canonical
                writer.writerow(dict(payload, row_id=provider["row_id"],
                                     source_position=provider["source_position"]))
        self.file = file_identity(self.csv_path)

    def preflight(self, method):
        if method.describe() != self.method or file_identity(self.csv_path) != self.file:
            raise ValueError("cascade stage CSV or method binding changed")
        return dict(status="passed", eligible_rows=len(self.expected_rows),
                    provider_aliases=len(self.bindings), **self.file)

    def scan_spec(self, method):
        self.preflight(method)
        return self.scan

    def expected(self, query_id):
        if query_id != self.query_id:
            raise ValueError("cascade provider query identity changed")
        return deepcopy(self.expected_rows)

    def restore(self, value):
        canonical = self.bindings.get(value.get("call_id"))
        if canonical is None:
            raise ValueError("Core returned an undeclared cascade provider alias")
        expected = identity(value["query_id"], value["row_id"], value["source_position"],
                            value["call_index"])
        if any(value.get(key) != expected[key] for key in IDENTITY_FIELDS):
            raise ValueError("Core changed a cascade provider identity")
        return dict(value, **canonical, provider_call_id=expected["call_id"])


class _StageEngine:
    """Translate identities only around one finite, supervised local engine."""

    def __init__(self, kernel, stage, bindings):
        self.kernel, self.stage, self.bindings = kernel, stage, bindings

    def ready(self):
        return dict(self.kernel.ready_report, cascade_stage=self.stage.name,
                    model_role=self.stage.model_role,
                    generation=asdict(local_generation(self.stage)))

    def run_prepared(self, rows, *, deadline_monotonic):
        _check(deadline_monotonic)
        restored = []
        for row in rows:
            canonical = self.bindings.get(row.get("call_id"))
            if canonical is None:
                raise ValueError("prepared cascade provider alias is undeclared")
            restored.append(dict(row, **canonical))
        values = self.kernel.run_prepared(restored)
        if type(values) is not list or len(values) != len(rows):
            raise RuntimeError("cascade private engine returned an unconfirmed batch")
        result = []
        for provider, canonical, value in zip(rows, restored, values, strict=True):
            audit = _StageCalls(replace(self.stage, records=(canonical,)))
            audit.record(value)
            result.append(dict(value, **{name: provider[name] for name in IDENTITY_FIELDS},
                               provider_call_id=provider["call_id"]))
        return result

    def close(self, timeout):
        report = dict(self.kernel.close(timeout=timeout))
        report["engine_api_shutdown_confirmed"] = (
            (report.get("engine_api") or {}).get("engine_api_shutdown_confirmed") is True
        )
        return report


class _CascadeCoreModelActor(_CoreModelActor):
    """Use the old bounded-byte result/audit method with stage-aware inference."""

    def _process_type(self):
        from src.baselines.text.frameworks.daft_engine_process import EngineProcess
        return EngineProcess

    def ready(self, deadline):
        _check(deadline)
        if self.engine is None:
            if self._engine_start_attempted:
                raise RuntimeError("cascade model startup is unconfirmed; owner must stop")
            self._engine_start_attempted = True
            try:
                kernel = self._process_type()(
                    self.engine_spec, self.generation, Path(self.control), deadline,
                    self.startup_timeout_s,
                )
                self.engine = _StageEngine(kernel, self.stage, self.bindings)
            except BaseException:
                self._engine_start_failed = True
                raise
        _check(deadline)
        return self.engine.ready()


class _StageActorOwner(_ActorOwner):
    """The existing actor owner still owns all allocation/readiness/closure."""

    def __init__(self, *args, stage, bindings, control, **kwargs):
        super().__init__(*args, **kwargs)
        self.stage, self.bindings, self.control = stage, bindings, control

    def _model_actor_type(self):
        stage = replace(self.stage, records=())
        bindings, control = deepcopy(self.bindings), str(self.control)
        startup_timeout_s = self.resources.startup_timeout_s

        class StageCoreModelActor(_CascadeCoreModelActor):
            def __init__(self, engine, generation, model_batch_rows):
                # Ordinary Core validates its ordinary preparation configuration.
                # Actual model generation is explicitly the declared stage value.
                super().__init__(engine, asdict(local_generation(stage)), model_batch_rows)
                self.stage, self.bindings, self.control = stage, bindings, control
                self.startup_timeout_s = startup_timeout_s

        return StageCoreModelActor

    def release_wrapper_after_process_exit(self, registered):
        """The wrapper owns IPC only; no model lives in this Ray actor process.

        Ordinary RecordsActorOwner correctly retains an actor when its in-actor
        model API is unknown. Here a failed EngineProcess constructor can leave
        that API permanently unknown even after all registered model processes
        exited. Release only this trusted IPC wrapper, retaining all unknown API
        and GPU evidence and its handle for the caller's audit/retry scope.
        """
        confirmed = registered.get("process_exit_confirmed") is True
        requested = self.model_actor is None or id(self.model_actor) in self._killed
        errors = []
        if confirmed and not requested:
            try:
                self.ray.kill(self.model_actor, no_restart=True)
                self._killed.add(id(self.model_actor))
                requested = True
            except BaseException as error:
                errors.append("wrapper_actor_kill: " + type(error).__name__)
        return dict(registered_model_process_exit_confirmed=confirmed,
                    wrapper_actor_kill_request_confirmed=requested,
                    actor_process_exit_confirmed=None, gpu_exit_confirmed=None,
                    cleanup_errors=errors)


class _CoreStageRows:
    def __init__(self, backend, stage, source, owner, rows, cleanup, started):
        self.backend, self.stage, self.source = backend, stage, source
        self.owner, self.rows, self.cleanup, self.started = owner, rows, cleanup, started
        self.audit = _StageCalls(stage)
        self.last_close_report = None

    def __iter__(self):
        return self

    def __next__(self):
        value = self.source.restore(next(self.rows))
        self.audit.record(value)
        return value

    def _recover_result_receipts(self, reports, retained):
        """Recover verified private-engine receipts that never reached Core."""
        aliases = {row["call_id"]: provider for provider, row in self.source.bindings.items()}
        receipts = {row["call_id"]: row for row in retained}
        errors = []
        for report in reports:
            failure = report.get("result_failure")
            if failure is None:
                continue
            if type(failure) is not dict or type(failure.get("verified_call_results")) is not list:
                errors.append("result_receipt_recovery: ValueError")
                continue
            ready_uid = (report.get("engine_ready") or {}).get("engine_uid")
            for row in failure["verified_call_results"]:
                try:
                    call_id = row.get("call_id") if type(row) is dict else None
                    expected = self.audit.expected.get(call_id) if type(call_id) is str else None
                    if expected is None or any(row.get(key) != value for key, value in expected.items()):
                        raise ValueError("private receipt belongs to another cascade occurrence")
                    if call_id in self.audit.results:
                        # An already delivered unknown never becomes a success.
                        continue
                    if (sum(type(other) is dict and other.get("call_id") == call_id
                            for other in failure["verified_call_results"]) != 1
                            or type(row.get("engine_protocol_error")) is not str
                            or not row["engine_protocol_error"].strip()
                            or type(ready_uid) is not str or not ready_uid or row.get("engine_uid") != ready_uid):
                        raise ValueError("private receipt lacks its verified engine identity")
                    audit = _StageCalls(replace(self.stage, records=(expected,)))
                    audit.record(row)
                    receipt = dict(audit.ordered()[0], provider_call_id=aliases[call_id])
                    if call_id in receipts and receipts[call_id] != receipt:
                        raise ValueError("private receipt conflicts with an existing terminal")
                    receipts[call_id] = receipt
                except (ValueError, TypeError, KeyError, AttributeError) as error:
                    errors.append("result_receipt_recovery: " + type(error).__name__)
        return [receipts[key] for key in self.audit.expected if key in receipts], errors

    def close(self):
        if self.last_close_report is not None and self.last_close_report["status"] == "closed":
            return self.last_close_report
        # Existing Core closes its flow, then the actor/engine API and transport.
        # Registered-process supervision is independent and remains retryable.
        report, registered = _close_stage_owner(self.owner, self.cleanup, bool(self.stage.records))
        model = report.get("model_owner", {})
        engine = model.get("engine_close_report") or {}
        empty = not self.stage.records
        settled = (report["status"] == "closed" and report.get("transport_closed") is True
                   and report.get("retained_remote_inputs") == 0
                   and (empty or (engine.get("process_exit_confirmed") is True
                                  and engine.get("engine_api_shutdown_confirmed") is True))
                   and registered["process_exit_confirmed"] and not registered["errors"])
        query = deepcopy(report.get("last_query_report"))
        if query is not None:
            query["unconsumed_call_results"] = [self.source.restore(value)
                for value in query.get("unconsumed_call_results", [])]
        receipts, errors = self._recover_result_receipts(
            registered["reports"], [] if query is None else query["unconsumed_call_results"],
        )
        if errors:
            settled = False
        if query is not None:
            query["unconsumed_call_results"] = receipts
        self.last_close_report = dict(
            report, status="closed" if settled else "unknown", last_query_report=query,
            cleanup_errors=[*report.get("cleanup_errors", []), *errors],
            algorithm_stage=self.stage.name, model_role=self.stage.model_role,
            stage_process_exit_confirmed=bool(settled), registered_engine_cleanup=registered,
            control_dir=str(self.cleanup.control), stage_seconds=time.monotonic() - self.started,
            stage_csv=dict(self.source.file, rows=len(self.source.bindings),
                           cost_scope="extra raw stage CSV and provider aliases; not a Core RSS cap"),
            materialization_seconds=self.source.materialization_seconds,
            unconsumed_call_results=receipts,
        )
        self.backend._reports[self.stage.name] = deepcopy(self.last_close_report)
        if not settled:
            raise RuntimeError("cascade Core stage work or model process cleanup is unconfirmed")
        return self.last_close_report


def _close_stage_owner(owner, cleanup, started):
    """Always try independent registered-process cleanup; preserve the original error."""
    try:
        report = owner.close(timeout=15.0)
    except BaseException as error:
        errors = ["owner_close: " + type(error).__name__]
        active = getattr(owner, "_active", None)
        # Core's own collector only exposes receipts not previously delivered.
        # A later cleanup exception cannot erase their authoritative billing.
        collect = getattr(active, "collect_receipts", None)
        if collect is not None:
            try:
                collect()
            except BaseException as recording:
                errors.append("receipt_collection: " + type(recording).__name__)
        query = deepcopy(getattr(owner, "last_query_report", None))
        receipts = getattr(active, "unconsumed_call_results", [])
        if type(receipts) is list and receipts:
            query = {} if type(query) is not dict else query
            retained = query.setdefault("unconsumed_call_results", [])
            for value in receipts:
                if value not in retained:
                    retained.append(deepcopy(value))
        report = dict(status="unknown", cleanup_errors=errors,
                      owner_close_error=type(error).__name__, last_query_report=query,
                      production_core_connected=getattr(owner, "production_core_connected", False))
    finally:
        try:
            registered = cleanup.stop(started=started)
        except BaseException as error:
            registered = dict(reports=[], errors=["registered_cleanup: " + type(error).__name__],
                              process_exit_confirmed=False)
    actors = getattr(owner, "_actors", None)
    if isinstance(actors, _StageActorOwner):
        report["wrapper_actor_after_registered_exit"] = actors.release_wrapper_after_process_exit(registered)
    return report, registered


class CoreCascadeBackend:
    """One global cascade query; each stage uses a production records Core."""

    def __init__(self, source, *, engines, resources, capacity=None, cancelled=None):
        if type(engines) is not dict or set(engines) != {"main", "helper"}:
            raise ValueError("both main and helper engine deployments are required")
        self.engines = {role: EngineSpec.from_dict(asdict(spec)) for role, spec in engines.items()}
        if Path(self.engines["main"].model_path).resolve() == Path(self.engines["helper"].model_path).resolve():
            raise ValueError("cascade deployments require distinct explicit model paths")
        self.resources = RecordsResources(**asdict(resources))
        if resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1:
            raise ValueError("cascade Core requires prepare_batch_rows=model_batch_rows=1")
        self.capacity = CoreRecordsCapacity() if capacity is None else capacity
        if type(self.capacity) is not CoreRecordsCapacity:
            raise TypeError("cascade Core capacity must be CoreRecordsCapacity")
        self.source, self.cancelled = source, cancelled
        self.deadline = time.monotonic() + resources.query_timeout_s
        self._checked = self._production_connected = False
        self._active, self._startup_failure, self._reports = None, None, {}
        self._reserved = dict(main=0, helper=0)

    @property
    def close_reports(self):
        return deepcopy(list(self._reports.values()))

    @property
    def production_core_connected(self):
        return self._production_connected

    def check_lifecycle(self):
        _check(self.deadline, self.cancelled)

    def preflight(self, main_lm, helper_lm, effective, *, max_possible_calls,
                  method, helper_method):
        from tokenizers import Tokenizer
        import ray

        self.check_lifecycle()
        if self._checked:
            raise RuntimeError("cascade Core backend owns one query")
        if ray.__version__ != "2.56.1" or not ray.is_initialized():
            raise RuntimeError("cascade Core requires caller-owned Ray 2.56.1")
        alive = [node for node in ray.nodes() if node["Alive"]]
        if len(alive) != 1 or alive[0]["NodeID"] != ray.get_runtime_context().get_node_id():
            raise ValueError("cascade Core process supervision requires one local Ray node")
        self.source.preflight(method)
        self.scan = self.source.scan_spec(method)
        if not isinstance(self.scan, RecordScan):
            raise TypeError("cascade source.scan_spec(method) must return RecordScan")
        self.scan.describe()
        self.methods = {"main": FilterMethod.from_dict(asdict(method)),
                        "helper": FilterMethod.from_dict(asdict(helper_method))}
        for role, lm in (("main", main_lm), ("helper", helper_lm)):
            bound, spec = self.methods[role], self.engines[role]
            if (bound.model_name != lm.model or not bound.resolved_input_columns
                    or not set(bound.resolved_input_columns).issubset(self.scan.payload_columns)):
                raise ValueError("cascade source scan omits a bound method's input columns")
            spec.validate_driver_environment()
            tokenizer = Tokenizer.from_file(str(Path(spec.tokenizer_path or spec.model_path) / "tokenizer.json"))
            for label in ("True", "False"):
                tokens = tokenizer.encode(label, add_special_tokens=False).ids
                if len(tokens) != 1 or lm.encode_text(label) != tokens:
                    raise ValueError("original LM and deployed tokenizer label IDs differ")
            stage = CascadeStage(
                "helper_score" if role == "helper" else "fallback", role, lm.model,
                bound.instruction, bound.default, bound.system_prompt, bound.output_tokens,
                deepcopy(effective[role]), (), bound.resolved_input_columns,
            )
            local_generation(stage)
        self.max_possible_calls = max_possible_calls
        self.effective = deepcopy(effective)
        self._checked = True

    def _check_source(self, stage):
        """Check selected raw values against the native source before aliasing."""
        self.source.preflight(self.methods["main"])
        selected = {}
        for row in stage.records:
            key = row["row_id"], row["source_position"]
            values = {column: row[column] for column in stage.input_columns}
            if key in selected and values != selected[key]:
                raise ValueError("repeated cascade draws changed source values")
            selected[key] = values
        found = set()
        with Path(self.source.csv_path).open(newline="", encoding="utf-8") as stream:
            for raw in csv.DictReader(stream):
                self.check_lifecycle()
                for column, dtype in self.scan.csv_schema:
                    if dtype == "int64":
                        raw[column] = int(raw[column])
                if self.scan.predicate is not None:
                    column, value = self.scan.predicate
                    if raw[column] != value:
                        continue
                key = raw["row_id"], raw["source_position"]
                if key not in selected:
                    continue
                if key in found or any(raw[column] != value for column, value in selected[key].items()):
                    raise ValueError("cascade source changed or duplicated a selected occurrence")
                found.add(key)
        if found != set(selected):
            raise ValueError("cascade source omitted a declared stage occurrence")

    def __call__(self, stage):
        import lotus

        self.check_lifecycle()
        if not self._checked:
            raise RuntimeError("cascade Core requires method preflight")
        if self._active is not None and not (
                self._active.last_close_report or {}).get("stage_process_exit_confirmed"):
            raise RuntimeError("previous cascade Core stage cleanup is unconfirmed")
        if any(not report.get("stage_process_exit_confirmed") for report in self._reports.values()):
            raise RuntimeError("previous cascade Core stage cleanup is unconfirmed")
        if stage.name in self._reports:
            raise ValueError("cascade Core cannot execute a logical stage twice")
        audit = _StageCalls(stage)
        if len(audit.expected) != len(stage.records):
            raise ValueError("cascade Core stage contains duplicate call identities")
        for row in stage.records:
            canonical = cascade_identity(
                row["query_id"], row["row_id"], row["source_position"], row["call_index"],
                model_role=stage.model_role, stage=stage.name,
            )
            if any(row.get(key) != canonical[key] for key in IDENTITY_FIELDS):
                raise ValueError("cascade stage occurrence identity changed")
            if any(type(row[column]) is not str for column in stage.input_columns):
                raise ValueError("cascade stage payload columns must be text")
        bound = self.methods.get(stage.model_role)
        if (bound is None or stage.model_name != bound.model_name
                or stage.input_columns != bound.resolved_input_columns
                or stage.instruction != lotus.nl_expression.nle2str(bound.instruction, list(stage.input_columns))
                or stage.system_prompt != bound.system_prompt or stage.default != bound.default
                or tuple(stage.output_tokens) != bound.output_tokens
                or stage.effective_kwargs != self.effective[stage.model_role]):
            raise ValueError("cascade stage differs from its bound method")
        if sum(self._reserved.values()) + len(stage.records) > self.max_possible_calls:
            raise ValueError("cascade stage exceeds the global declared call bound")
        spec = self.engines[stage.model_role]
        reserved = self._reserved[stage.model_role] + len(stage.records)
        if spec.max_model_calls is not None and len(stage.records) > spec.max_model_calls:
            raise ValueError("cascade stage exceeds its engine lifetime call budget")
        self._reserved[stage.model_role] = reserved
        started = time.monotonic()
        self._check_source(stage)
        control = Path(tempfile.mkdtemp(prefix="semloom-cascade-core-"))
        source = _StageSource(stage, bound, control, self.capacity.item_input_bytes)
        source.materialization_seconds = time.monotonic() - started
        resources = replace(self.resources, query_timeout_s=max(0.000001, self.deadline - time.monotonic()))
        # The unchanged preparation/Core seam is ordinary. Only the trusted
        # model specialization receives actual per-stage helper generation.
        ordinary_generation = replace(local_generation(stage), logprobs=None)
        actors = _StageActorOwner(
            bound, spec, ordinary_generation, resources, stage=stage,
            bindings=source.bindings, control=control,
        )
        owner = SemLoomRecordsCore(bound, spec, ordinary_generation, resources,
                                  capacity=self.capacity, actor_owner=actors)
        cleanup = StageEngineCleanup(control)
        try:
            rows = owner.evaluate(source, query_id=source.query_id, cancelled=self.cancelled)
        except BaseException as error:
            self._startup_failure = owner, cleanup, stage
            report, registered = _close_stage_owner(owner, cleanup, bool(stage.records))
            self._reports[stage.name] = dict(report, registered_engine_cleanup=registered,
                stage_process_exit_confirmed=False, algorithm_stage=stage.name,
                model_role=stage.model_role, control_dir=str(control))
            error.add_note("cascade Core startup cleanup is retained in backend.close_reports")
            raise
        self._production_connected = self._production_connected or owner.production_core_connected
        self._active = _CoreStageRows(self, stage, source, owner, rows, cleanup, started)
        return self._active

    def close(self):
        if self._startup_failure is not None:
            owner, cleanup, stage = self._startup_failure
            report, registered = _close_stage_owner(owner, cleanup, bool(stage.records))
            self._reports[stage.name] = dict(report, registered_engine_cleanup=registered,
                stage_process_exit_confirmed=False, algorithm_stage=stage.name,
                model_role=stage.model_role, control_dir=str(cleanup.control))
            return deepcopy(self._reports[stage.name])
        return None if self._active is None else self._active.close()
