"""Original global-cascade stages submitted through author DBEnd and Lane APIs."""

from copy import deepcopy
import csv
from dataclasses import asdict, replace
import json
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

from integrations.imlane_records import owner, verify
from integrations.imlane_records.cascade_workload import CascadeOccurrences, input_record, stage_values
from integrations.imlane_records.client import RecordsBridge
from integrations.imlane_records.filter_workload import PROTOCOL, validate_terminal
from integrations.imlane_records.method_environment import preflight_method
from integrations.imlane_records.model_lifecycle import model_closed
from integrations.imlane_records.model_process import assigned_environment
from integrations.imlane_records.records_host import RecordsResults
from integrations.imlane_records.verify_filter import FilterScope
from src.baselines.common.private_artifacts import require_outside_git, write_private_json
from src.semantic_methods.lotus.cascade import CascadeStage, local_generation
from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec


class CascadeScope(FilterScope):
    def __enter__(self):
        preflight_method(self.args.method_python, self.args.author_root, verify.CODE)
        subprocess.run([str(self.args.method_python), "-I", "-B", "-c",
            "import sys;sys.path.insert(0,sys.argv[1]);from src.semantic_methods.lotus.cascade import validate_cascade_source;validate_cascade_source()",
            str(verify.CODE)], check=True, capture_output=True, timeout=15)
        return super().__enter__()

    def workload_environment(self):
        return dict(IMLANE_CASCADE_CONFIG=str(self.args.cascade_config),
                    CUDA_VISIBLE_DEVICES=self.args.cuda_visible_devices,
                    HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1", LITELLM_LOCAL_MODEL_COST_MAP="True")


class CascadeResults(RecordsResults):
    def __init__(self, rows, client, *, stage, backend, control, close_owner):
        self.stage, self.backend, self.delivered = stage_values(stage), backend, set()
        self._close_failure = None
        super().__init__(rows, client, query_id=stage.records[0]["query_id"],
            mapping=CascadeOccurrences(stage), stage_id=stage.name, block_prefix="cascade-" + stage.name,
            control_dir=control, deadline=backend.deadline, close_owner=close_owner,
            cleanup_scope=backend.author_args.stop_budget.cleanup,
            block_rows=backend.resources.prepare_batch_rows, max_pending=backend.resources.max_active_batches)

    def __next__(self):
        self.backend.check_lifecycle()
        value = super().__next__()
        self.delivered.add(value["call_id"])
        return value

    def close(self):
        primary = None
        try:
            super().close()
        except BaseException as error:
            primary = error
        try:
            self.backend._check_author_evidence()
        except BaseException as error:
            primary = primary or error
        receipts = dict(self.mapping.receipts)
        try:
            for path in self.control.glob("cascade-audit-*.json"):
                if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024**2:
                    raise ValueError("cascade receipt audit exceeds its private file boundary")
                audit = json.loads(path.read_text())
                uid = ((self.model_close or {}).get("engine_ready") or {}).get("engine_uid")
                if (audit["query_id"] != self.query_id or audit["stage"] != self.stage.name
                        or audit["model_role"] != self.stage.model_role or audit["engine_uid"] != uid):
                    raise ValueError("cascade audit belongs to another owned role or stage")
                for row in audit["known_receipts"]:
                    source = self.mapping.inputs.get(row.get("call_id"))
                    if source is None:
                        raise ValueError("cascade audit names an undeclared occurrence")
                    validate_terminal(row, input_record(source, self.stage), engine_uid=uid)
                    prior = receipts.get(row["call_id"])
                    if prior is not None and {k:v for k,v in prior.items() if k != "imlane_worker"} != row:
                        raise ValueError("cascade audit conflicts with a delivered terminal")
                    receipts.setdefault(row["call_id"], row)
            if (self.model_close or {}).get("receipt_audit_errors"):
                raise RuntimeError("cascade worker receipt audit remains unconfirmed")
        except BaseException as error:
            if primary is None:
                primary = error
            else:
                primary.add_note("cascade receipt recovery: " + type(error).__name__)
        self._close_failure = self._close_failure or primary
        primary = self._close_failure
        author_state = self.backend._author_states.get(self.stage.name,
            (self.last_close_report or {}).get("author", {}))
        physical = author_state.get("resources_confirmed") is True
        settled = (primary is None and self.closed and model_closed(self.model_close) and physical)
        self.last_close_report = dict(self.last_close_report or {},
            author=deepcopy(author_state),
            algorithm_stage=self.stage.name, model_role=self.stage.model_role,
            author_physical_resources_confirmed=physical,
            stage_process_exit_confirmed=bool(settled),
            unconsumed_call_results=[r for k,r in receipts.items() if k not in self.delivered],
            raw_result_batches=deepcopy(self.mapping.raw_result_batches),
            unassociated_results=deepcopy(self.mapping.unassociated_results))
        if settled:
            self.backend._active = None
        if primary is not None:
            self.closed = False
            self.last_close_report["status"] = "unknown"
        self.backend._reports[self.stage.name] = deepcopy(self.last_close_report)
        if primary is not None:
            raise primary
        if not settled:
            raise RuntimeError("cascade stage retains unconfirmed author or model ownership")
        return self.last_close_report


class IMLaneCascadeBackend:
    """One caller-owned query; the author owns all physical submission/execution."""

    def __init__(self, source, *, engines, resources, author_args, model_environment, cancelled=None):
        if type(engines) is not dict or set(engines) != {"main", "helper"}:
            raise ValueError("cascade needs explicit helper and main deployments")
        self.engines = {role: EngineSpec.from_dict(asdict(spec)) for role,spec in engines.items()}
        if Path(self.engines["main"].model_path).resolve() == Path(self.engines["helper"].model_path).resolve():
            raise ValueError("cascade requires distinct explicit helper and main model paths")
        self.resources = RecordsResources(**asdict(resources))
        if (resources.cpu_workers != 1 or resources.prepare_batch_rows > 16
                or resources.model_batch_rows > 16 or resources.max_active_batches > 2):
            raise ValueError("IMLane uses one author executor, 1..16 rows and at most two pending blocks")
        self.author_args, self.model_environment = author_args, assigned_environment(model_environment)
        require_outside_git(Path(author_args.output))
        if not Path(author_args.output).is_dir():
            raise ValueError("caller must supply a fresh private query output and its existing owner supervision")
        self.source, self.cancelled = source, cancelled
        self.deadline = time.monotonic() + resources.query_timeout_s
        self._checked, self._active, self._startup_failed = False, None, False
        self._reports, self._author_states, self.author_report = {}, {}, dict(cleanup=[])
        self._author_failed, self._query_id = False, None
        self._methods, self._instructions, self._effective = {}, {}, {}
        self._remaining_calls = 0

    @property
    def close_reports(self):
        return deepcopy(list(self._reports.values()))

    def check_lifecycle(self):
        self.author_args.stop_budget.check()
        self._check_author_evidence()
        if time.monotonic() >= self.deadline:
            raise TimeoutError("IMLane cascade query deadline expired")
        if self.cancelled is not None and self.cancelled():
            raise InterruptedError("IMLane cascade query cancelled")

    def _check_author_evidence(self, namespace=None):
        report = self.author_report
        cleanups = report.get("cleanup", [])
        if (any(report.get(key) for key in ("sampler_errors", "stderr_capture_errors", "cleanup_errors",
                                           "final_cleanup_notices", "resource_allowance_exceeded"))
                or any(item.get("passed") is not True for item in cleanups)
                or namespace is not None and not any(item.get("namespace") == namespace for item in cleanups)):
            self._author_failed = True
        if self._author_failed:
            raise RuntimeError("cascade author cleanup or monitoring failed")

    def preflight(self, main_lm, helper_lm, effective, *, max_possible_calls, method, helper_method):
        from lotus.nl_expression import nle2str
        from tokenizers import Tokenizer

        self.check_lifecycle()
        if self._checked or type(max_possible_calls) is not int or max_possible_calls < 0:
            raise ValueError("cascade backend owns one query and its finite logical call bound")
        self.source.preflight(method)
        self.scan = self.source.scan_spec(method)
        if not isinstance(self.scan, RecordScan):
            raise TypeError("IMLane cascade requires a neutral RecordScan")
        for role,lm,binding in (("main",main_lm,method), ("helper",helper_lm,helper_method)):
            if not isinstance(binding, FilterMethod):
                raise TypeError("cascade requires separate bound FilterMethod values")
            binding = FilterMethod.from_dict(binding.describe())
            if (binding.model_name != lm.model or not binding.resolved_input_columns
                    or not set(binding.resolved_input_columns) <= set(self.scan.payload_columns)):
                raise ValueError("cascade scan omits a bound role's text columns")
            spec = self.engines[role]
            spec.validate_driver_environment()
            tokenizer = Tokenizer.from_file(str(Path(spec.tokenizer_path or spec.model_path) / "tokenizer.json"))
            for label in ("True", "False"):
                tokens = tokenizer.encode(label, add_special_tokens=False).ids
                if len(tokens) != 1 or lm.encode_text(label) != tokens:
                    raise ValueError("original LM and deployed tokenizer label IDs differ")
            dummy = CascadeStage("helper_score" if role == "helper" else "fallback", role,
                lm.model, binding.instruction, binding.default, binding.system_prompt,
                binding.output_tokens, effective[role], (), binding.resolved_input_columns)
            if local_generation(dummy).max_tokens > spec.max_model_len:
                raise ValueError("cascade max_tokens exceeds its model deployment")
            self._methods[role] = binding
            self._instructions[role] = nle2str(binding.instruction, list(binding.resolved_input_columns))
        self._remaining_calls, self._effective, self._checked = max_possible_calls, deepcopy(effective), True

    def _source_rows(self, stage):
        selected = {}
        for row in stage.records:
            selected.setdefault((row["row_id"], row["source_position"]), []).append(row)
        allowed = {(r["row_id"],r["source_position"]) for r in self.source.expected(stage.records[0]["query_id"])}
        if not set(selected) <= allowed or Path(self.source.csv_path).stat().st_size > 16 * 1024**2:
            raise ValueError("cascade source selection or finite CSV allowance differs")
        seen = set()
        with Path(self.source.csv_path).open(newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != [name for name,_ in self.scan.csv_schema]:
                raise ValueError("cascade source CSV schema differs")
            for raw in reader:
                self.check_lifecycle()
                row = {name:int(raw[name]) if kind == "int64" else raw[name] for name,kind in self.scan.csv_schema}
                if self.scan.predicate is not None and row[self.scan.predicate[0]] != self.scan.predicate[1]:
                    continue
                key = (row["row_id"],row["source_position"])
                if key not in selected:
                    continue
                if key in seen:
                    raise ValueError("source repeats a selected occurrence")
                seen.add(key)
                for call in selected[key]:
                    if any(type(row[c]) is not str or row[c] != call[c] for c in stage.input_columns):
                        raise ValueError("cascade source text differs from its original method input")
                    yield dict(call)
        if seen != set(selected):
            raise ValueError("cascade source omitted a selected occurrence")

    def __call__(self, stage):
        self.check_lifecycle()
        if not self._checked or self._active is not None or self._startup_failed:
            raise RuntimeError("cascade stage needs preflight and a confirmed previous author close")
        if self._reports and not list(self._reports.values())[-1]["stage_process_exit_confirmed"]:
            raise RuntimeError("previous cascade stage ownership remains unconfirmed")
        stage = stage_values(stage)
        binding = self._methods[stage.model_role]
        if (stage.name in self._reports or stage.model_name != binding.model_name
                or stage.instruction != self._instructions[stage.model_role]
                or stage.input_columns != binding.resolved_input_columns or stage.default != binding.default
                or stage.system_prompt != binding.system_prompt or stage.output_tokens != binding.output_tokens
                or stage.effective_kwargs != self._effective[stage.model_role]):
            raise ValueError("cascade stage differs from its caller-bound method")
        if not stage.records:
            return iter(())
        query_id = stage.records[0]["query_id"]
        if self._query_id is not None and self._query_id != query_id:
            raise ValueError("cascade backend belongs to another query")
        self._query_id = query_id
        spec = self.engines[stage.model_role]
        if len(stage.records) > self._remaining_calls or spec.max_model_calls is not None and len(stage.records) > spec.max_model_calls:
            raise ValueError("cascade stage exceeds its finite query or engine allowance")
        self._remaining_calls -= len(stage.records)
        control = Path(self.author_args.output) / ("cascade-" + stage.name)
        control.mkdir(mode=0o700)
        config = dict(schema="imlane.cascade.stage.v1", stage=asdict(stage),
            query_id=stage.records[0]["query_id"], engine=asdict(replace(spec,max_model_calls=len(stage.records))),
            resources=asdict(self.resources), control_dir=str(control), deadline_monotonic=self.deadline,
            model_environment=self.model_environment)
        try:
            client, finalizer = self._open_stage(stage, config, control)
            self._active = CascadeResults(self._source_rows(stage), client, stage=stage, backend=self,
                                          control=control, close_owner=finalizer)
            return self._active
        except BaseException as error:
            self._startup_failed = True
            self._reports[stage.name] = dict(status="unknown", algorithm_stage=stage.name,
                model_role=stage.model_role, stage_process_exit_confirmed=False,
                startup_error_type=type(error).__name__)
            raise

    def _open_stage(self, stage, config, control):
        """Caller already owns the host lease; reuse the existing native scope."""
        path = control / "worker.json"
        write_private_json(path, config)
        args = SimpleNamespace(**dict(vars(self.author_args), cascade_config=path,
                                      cuda_visible_devices=self.model_environment["CUDA_VISIBLE_DEVICES"]))
        kernel = self._workload_path()
        scope = CascadeScope(args, "cpython-cascade", owner, self.author_report, kernel=kernel)
        try:
            scope.__enter__()
            process = subprocess.Popen([str(args.bridge_binary),str(scope.port),str(kernel),"infer_records",PROTOCOL],
                env=scope.env, cwd=scope.root, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, start_new_session=True)
            scope.track_root(process)
            writer = verify._capture_stderr(process.stderr, scope.root / "bridge-stderr.log", self.author_report)
            scope.children.append((process,writer))
            def finalizer():
                try:
                    scope.close()
                finally:
                    self._author_states[stage.name] = owner.resource_state(scope)
                self._check_author_evidence(scope.namespace)
                return self._author_states[stage.name]
            def abort():
                state = finalizer()
                process.wait(timeout=3)
                if not state["resources_confirmed"]:
                    raise RuntimeError("cascade author scope resources remain unconfirmed")
            return RecordsBridge(process, abort_owner=abort, timeout_s=min(60,self.resources.query_timeout_s),
                                 protocol=PROTOCOL), finalizer
        except BaseException as error:
            try:
                scope.close()
            except BaseException as cleanup:
                error.add_note("cascade author startup cleanup: " + type(cleanup).__name__)
            raise

    def _workload_path(self):
        return verify.OWNED / "cascade_udf.py"

    def close(self):
        if self._active is not None:
            self._active.close()
        return self.close_reports
