"""Opt-in session on one existing author CPython scope, bridge and model child.

The caller keeps its host lease, permanent query claims, signal supervision and
independent GPU audit. No executor, queue, batching or backpressure is replaced.
"""

from dataclasses import asdict, replace
import json
from pathlib import Path
from subprocess import PIPE, Popen
import time
import uuid

from integrations.imlane_records import owner, verify, verify_filter
from integrations.imlane_records.client import RecordsBridge
from integrations.imlane_records.filter_host import FilterResults
from integrations.imlane_records.filter_workload import PROTOCOL
from integrations.imlane_records.method_environment import preflight_method, validate_author_build
from integrations.imlane_records.model_lifecycle import MODEL_CLOSE_GRACE_S, model_closed
from integrations.imlane_records.model_process import assigned_environment
from src.baselines.common.private_artifacts import write_private_json
from src.semantic_methods.lotus.records import identity


class _QueryBridge:
    def __init__(self, session, config):
        self.session, self.config = session, config

    def __getattr__(self, name):
        return getattr(self.session.client, name)

    def begin(self, query_id, **kwargs):
        client = self.session.client
        client.begin(query_id, **kwargs)
        self.session.native_query = query_id
        self.session._control("query_begin", query_id, config=self.config, deadline=kwargs["deadline"])


class _QueryResults(FilterResults):
    def __init__(self, *args, session, **kwargs):
        self.session = session
        super().__init__(*args, **kwargs)

    def _close(self):
        if self.closed:
            return self.last_close_report
        session = self.session
        healthy = self.started and self.exhausted and not (self.pending or self.failed or self.primary)
        try:
            if healthy:
                self.model_close = session._control("query_end", self.query_id)
            else:
                session.broken = True
                (self.control / "stop").touch()
            if session.native_query == self.query_id and not self.pending and not session.client._terminal:
                session.client.end(deadline=time.monotonic() + MODEL_CLOSE_GRACE_S)
                session.native_query = None
            self.last_close_report = dict(status="query_finished" if healthy else "unknown",
                model_resident=True, model_close_performed=False,
                resident=self.model_close, gpu_exit_confirmed=None)
            self.closed = healthy
            if healthy:
                session.active = None
            else:
                raise RuntimeError("an incomplete or failed query makes its resident session terminal")
            return self.last_close_report
        except BaseException:
            session.broken = True
            raise


class IMLaneResidentSession:
    """start/evaluate/close; only healthy, drained queries may share the model."""

    def __init__(self, engine, generation, resources, *, author_args, model_environment,
                 deadline_monotonic, observer=None):
        if (resources.cpu_workers != 1 or resources.prepare_batch_rows > 16 or resources.model_batch_rows > 16
                or resources.max_active_batches > 2 or engine.max_model_calls is None
                or engine.max_model_calls > 4096 or type(deadline_monotonic) not in (int, float)
                or not time.monotonic() < deadline_monotonic < float("inf")):
            raise ValueError("resident Filter requires an explicit finite session within the existing native capacities")
        self.engine, self.generation, self.resources = engine, generation, resources
        self.args, self.environment = author_args, assigned_environment(model_environment)
        self.deadline = deadline_monotonic
        self.observer, self.observer_errors = observer, []
        self.session_id = "imlane-" + uuid.uuid4().hex
        self.scope = self.client = self.process = self.active = None
        self.native_query = None
        self.started = self.closed = self.broken = False
        self.model_start_requested = False
        self.model_close_receipt = None
        self.author_report = dict(cleanup=[])
        self.ready = self.last_close_report = None
        self.used_queries, self.query_bindings = set(), []

    @property
    def last_query_report(self):
        # The shared Movie owner already merges this receipt after final close.
        # A failed active query may recover further known usage during cleanup.
        return None if self.active is None else self.active.last_close_report

    def _emit(self, event):
        if self.observer is not None:
            try:
                self.observer(event)
            except Exception as error:
                self.observer_errors.append(dict(event=event, error_type=type(error).__name__))

    def _control(self, operation, query, *, deadline=None, **payload):
        deadline = time.monotonic() + MODEL_CLOSE_GRACE_S if deadline is None else deadline
        call = "resident-" + operation + ":" + query
        block = "resident-" + operation
        wire = dict(operation=operation, **payload)
        if operation == "query_begin":
            if set(payload) != {"config"} or type(payload["config"]) is not dict:
                raise ValueError("query_begin requires one structured local configuration")
            wire = dict(operation=operation,
                        config_json=json.dumps(payload["config"], allow_nan=False))
        row = dict(call_id=call, row_id=0, query_id=query, stage_id="filter",
                   record_json=json.dumps(wire, allow_nan=False))
        self.client.submit(block, [row], deadline=deadline)
        ready = self.client.wait_ready(timeout_s=min(600, max(.001, deadline - time.monotonic())))
        if ready != [block]:
            raise ValueError("resident control requires the fully drained author lane")
        rows = self.client.collect(block, deadline=deadline)
        if len(rows) != 1 or rows[0]["call_id"] != call or rows[0]["row_id"] != 0:
            raise ValueError("resident control occurrence differs")
        envelope = json.loads(rows[0]["outcome_json"])
        result = envelope["result"]
        if (envelope.get("protocol") != PROTOCOL or envelope.get("query_id") != query
                or envelope.get("stage_id") != "filter" or result.get("kind") != "resident_control"
                or result.get("operation") != operation or result.get("_occurrence_id") != call
                or result.get("_position") != 0 or envelope.get("status") != "ok"):
            error = RuntimeError("resident control failed: " + str(envelope.get("error_type")))
            error.resident_control_report = result
            raise error
        if self.ready is not None and operation != "session_close":
            for key in ("worker_uid", "worker_pid", "model_initialization_attempts", "model_ready_count"):
                if result.get(key) != self.ready[key]:
                    raise RuntimeError("resident worker or initialization count changed between queries")
            if result.get("engine_ready") != self.ready["engine_ready"]:
                raise RuntimeError("resident model identity changed between queries")
        return result

    def start(self):
        if self.closed or self.broken:
            raise RuntimeError("resident session cannot restart")
        if self.started:
            if self.ready is None:
                raise RuntimeError("resident startup failed; close before discarding this owner")
            return self.ready
        self.started = True
        self._emit("owner_start")
        args = self.args
        control = Path(args.output) / "resident-control"
        control.mkdir(mode=0o700)
        self.control = control
        args.filter_config = control / "session.json"
        args.cuda_visible_devices = self.environment["CUDA_VISIBLE_DEVICES"]
        config = dict(engine=asdict(self.engine), generation=asdict(self.generation),
            resources=asdict(self.resources), query_id=self.session_id, control_dir=str(control),
            deadline_monotonic=self.deadline, model_environment=self.environment)
        write_private_json(args.filter_config, config)
        # These harmless read-only checks precede any author or model launch.
        preflight_method(args.method_python, args.author_root, verify_filter.CODE)
        validate_author_build(args.author_root, args.bridge_binary)
        udf = Path(__file__).with_name("resident_udf.py")
        self.scope = verify_filter.FilterScope(args, "cpython-filter", owner, self.author_report, kernel=udf)
        try:
            self.scope.__enter__()
            self.process = Popen([str(args.bridge_binary), str(self.scope.port), str(udf), "infer_records", PROTOCOL],
                env=self.scope.env, cwd=self.scope.root, stdin=PIPE, stdout=PIPE, stderr=PIPE, start_new_session=True)
            self.scope.track_root(self.process)
            writer = verify._capture_stderr(self.process.stderr, self.scope.root / "bridge-stderr.log", self.author_report)
            self.scope.children.append((self.process, writer))

            def abort():
                self.scope.close()
                self.process.wait(timeout=3)
                if owner.resource_state(self.scope)["resources_confirmed"] is not True:
                    raise RuntimeError("resident author resources remain unconfirmed")

            self.client = RecordsBridge(self.process, abort_owner=abort, timeout_s=min(600, self.resources.startup_timeout_s),
                                        protocol=PROTOCOL)
            query = self.session_id + ":startup"
            self.client.begin(query)
            self.native_query = query
            self.model_start_requested = True
            self.ready = self._control("session_start", query,
                deadline=min(self.deadline, time.monotonic() + self.resources.startup_timeout_s))
            self.client.end()
            self.native_query = None
            self._emit("owner_ready")
            return self.ready
        except BaseException:
            self.broken = True
            raise

    def evaluate(self, source, method, *, query_id):
        if not self.started or self.closed or self.broken or self.active is not None or self.ready is None:
            raise RuntimeError("resident owner requires ready, healthy, drained query state")
        identity(query_id, "preflight", 0)
        expected = source.expected(query_id)
        if query_id in self.used_queries or not 0 < len(expected) <= 4094 or len(self.used_queries) >= 62:
            raise ValueError("resident query repeats or exceeds native control/occurrence capacity")
        from integrations.imlane_records.filter_workload import bind_source
        method, scan = bind_source(source, method)
        control = self.control / ("query-" + str(len(self.used_queries)))
        control.mkdir(mode=0o700)
        self.used_queries.add(query_id)
        deadline = min(self.deadline, time.monotonic() + self.resources.query_timeout_s)
        config = dict(method=asdict(method), engine=asdict(replace(self.engine, max_model_calls=len(expected))),
            generation=asdict(self.generation), resources=asdict(self.resources), query_id=query_id,
            control_dir=str(control), deadline_monotonic=deadline, scan=scan.describe(), model_environment=self.environment)
        rows = (dict(identity(query_id, row["row_id"], row["source_position"]),
                     **{name: row[name] for name in scan.payload_columns})
                for row in source.rows() if scan.predicate is None or row[scan.predicate[0]] == scan.predicate[1])
        self.active = _QueryResults(rows, _QueryBridge(self, config), query_id=query_id, max_calls=len(expected),
            columns=scan.payload_columns, control_dir=control, deadline=deadline,
            close_owner=lambda: {}, cleanup_scope=self.args.stop_budget.cleanup,
            block_rows=self.resources.prepare_batch_rows, max_pending=self.resources.max_active_batches, session=self)
        self.query_bindings.append(config)
        return self.active

    def close(self):
        if self.closed:
            return self.last_close_report
        self._emit("owner_close_start")
        errors, model = [], self.model_close_receipt
        with self.args.stop_budget.cleanup():
            if self.active is not None:
                try:
                    self.active.close()
                except BaseException:
                    self.broken = True
            if self.client is not None and model is None:
                try:
                    if self.native_query is not None or self.client._terminal:
                        raise RuntimeError("native tasks remain or transport is terminal")
                    query = self.session_id + ":close"
                    self.client.begin(query)
                    self.native_query = query
                    model = self._control("session_close", query)
                    self.model_close_receipt = model
                    self.client.end()
                    self.native_query = None
                    self.client.close()
                except BaseException as error:
                    known = getattr(error, "resident_control_report", {}).get("close_report")
                    if isinstance(known, dict):
                        model = self.model_close_receipt = known
                    errors.append(type(error).__name__)
                    (self.control / "stop").touch()
            if self.scope is not None:
                try:
                    self.scope.close()
                except BaseException as error:
                    errors.append(type(error).__name__)
            physical = owner.resource_state(self.scope) if self.scope is not None else dict(resources_confirmed=True)
            if self.model_start_requested and (model is None or not model_closed(model)):
                errors.append("model_api_close_unconfirmed")
            if physical["resources_confirmed"] is not True:
                errors.append("author_resources_unconfirmed")
        self.closed = not errors
        self.broken = self.broken or bool(errors)
        self.last_close_report = dict(status="closed" if self.closed else "unknown", cleanup_errors=errors,
            model=model, author=physical, observer_errors=list(self.observer_errors),
            gpu_exit_confirmed=None, independent_gpu_exit_required=True,
            no_model_process_started=not self.model_start_requested)
        self._emit("owner_closed" if self.closed else "owner_close_unconfirmed")
        self.last_close_report["observer_errors"] = list(self.observer_errors)
        return self.last_close_report
