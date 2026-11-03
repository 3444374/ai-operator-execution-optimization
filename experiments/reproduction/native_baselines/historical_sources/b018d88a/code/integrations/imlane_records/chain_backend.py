"""Native author scopes around the existing ordinary-record method segments."""

from dataclasses import asdict, replace
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

from integrations.imlane_records import owner, verify
from integrations.imlane_records.chain_host import ChainResults, ChainScope, configuration
from integrations.imlane_records.client import RecordsBridge
from integrations.imlane_records.filter_workload import PROTOCOL
from integrations.imlane_records.model_process import assigned_environment
from src.baselines.common.private_artifacts import require_outside_git, write_private_json
from src.baselines.text.frameworks.lotus_chain_native_relation import NativeExtractRelationRows
from src.semantic_methods.lotus.record_chain import LotusRecordChain, RecordOccurrence, _ordered_columns
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec


class _AuthorPhaseRows(ChainResults):
    def __init__(self, *args, author_state, **kwargs):
        self.author_state = author_state
        super().__init__(*args, **kwargs)

    def close(self):
        try:
            super().close()
        finally:
            report = self.last_close_report = dict(self.last_close_report or {})
            report["author_physical_state"] = dict(self.author_state)
            report.setdefault("model",self.carrier.model_close)
            model = report.get("model")
            report["engine_reports"] = ([model] if model and not model.get("no_model_process_started") else [])
        return self.last_close_report


class _AuthorRelationRows(NativeExtractRelationRows):
    def close(self):
        report = super().close()
        report.update(relation_barriers=self.chain.relation_barriers,
            layout="materialized_extract_relation_segments" if self.chain.relation_barriers else "fused_record_methods",
            native_batch_owner="author DBEnd/Coordinator/Lane and CPython executor")
        return report


class IMLaneChainBackend:
    """One caller-owned query; Map/Extract are ordinary single-stage chains."""

    def __init__(self, *, engine, resources, author_args, model_environment, cancelled=None):
        self.engine = EngineSpec.from_dict(asdict(engine))
        self.resources = RecordsResources(**asdict(resources))
        if (resources.cpu_workers != 1 or resources.prepare_batch_rows > 16
                or resources.model_batch_rows > 16 or resources.max_active_batches > 2):
            raise ValueError("IMLane chains require one author executor, 1..16 rows and at most two pending blocks")
        self.args, self.environment, self.cancelled = author_args, assigned_environment(model_environment), cancelled
        require_outside_git(Path(author_args.output))
        if not Path(author_args.output).is_dir():
            raise ValueError("the caller must provide a private output and its existing host supervision")
        self.author_report, self._author_failed, self._used = dict(cleanup=[]), False, False

    def _check_author(self, namespace=None):
        evidence = self.author_report
        if (any(evidence.get(key) for key in ("sampler_errors", "stderr_capture_errors", "cleanup_errors",
                                              "final_cleanup_notices", "resource_allowance_exceeded"))
                or any(item.get("passed") is not True for item in evidence["cleanup"])
                or namespace is not None and not any(item.get("namespace") == namespace for item in evidence["cleanup"])):
            self._author_failed = True
        if self._author_failed:
            raise RuntimeError("IMLane chain author cleanup or monitoring failed")

    def _cancelled(self):
        self._check_author()
        return self.args.stop_budget.stopped.is_set() or self.cancelled is not None and self.cancelled()

    def evaluate(self, records, chain, *, expected, columns=None):
        if self._used:
            raise RuntimeError("IMLane chain backend owns one query")
        self._used = True
        self.deadline = time.monotonic() + self.resources.query_timeout_s
        if self._cancelled():
            raise InterruptedError("IMLane chain cancelled before author allocation")
        chain = LotusRecordChain.from_dict(chain.describe())
        if type(expected) not in (list, tuple) or any(type(row) is not RecordOccurrence for row in expected):
            raise TypeError("IMLane requires a finite declaration of RecordOccurrence values")
        expected = tuple(RecordOccurrence.from_dict(row.describe()) for row in expected)
        if any(len({getattr(row,key) for row in expected}) != len(expected) for key in ("row_id","source_position")):
            raise ValueError("IMLane requires globally unique occurrence IDs and positions")
        if len({stage.method.model_name for stage in chain.stages}) != 1:
            raise ValueError("IMLane finite chains require one explicit model deployment")
        if (self.engine.max_model_calls is None or self.engine.max_model_calls > 4096
                or len(expected)*len(chain.stages) > self.engine.max_model_calls
                or any(stage.generation.max_tokens > self.engine.max_model_len for stage in chain.stages)):
            raise ValueError("IMLane chain exceeds its finite model context or permanent call allowance")
        self.engine.validate_driver_environment()
        schema = _ordered_columns(sorted(expected,key=lambda row:row.source_position),columns)
        self.source = iter(records)
        scratch = Path(self.args.output) / "relation-control"
        scratch.mkdir(mode=0o700)

        def open_phase(index, segment, inputs, *, initial_parents, columns, allow_nulls):
            self._check_author()
            control = Path(self.args.output) / ("phase-" + str(index))
            control.mkdir(mode=0o700)
            # The whole query was reserved above; each independently owned child
            # also receives only this segment's finite worst-case allowance.
            spec = replace(self.engine,max_model_calls=max(1,len(inputs)*len(segment.stages)))
            config = configuration(segment,spec,self.resources,inputs,control_dir=control,
                deadline=self.deadline,model_environment=self.environment,initial_parents=initial_parents,
                columns=columns,allow_nulls=allow_nulls)
            client, finalizer, physical = self._open_phase(segment,config,control)
            try:
                return _AuthorPhaseRows(self.source if index==0 else iter(inputs),client,
                    chain=segment,expected=inputs,resources=self.resources,control_dir=control,
                    deadline=self.deadline,close_owner=finalizer,author_state=physical,
                    cleanup_scope=self.args.stop_budget.cleanup,cancelled=self._cancelled,
                    initial_parents=initial_parents,columns=columns,close_on_eof=True)
            except BaseException as error:
                try:
                    finalizer()
                except BaseException as cleanup:
                    error.add_note("IMLane phase construction cleanup: " + type(cleanup).__name__)
                raise

        return _AuthorRelationRows(chain,expected,open_phase,scratch,self.deadline,self._cancelled,
            scheduler_owner="author IMLane DBEnd/Coordinator/Lane",columns=schema)

    def _workload_path(self):
        return verify.OWNED / "chain_udf.py"

    def _open_phase(self, segment, config, control):
        path = control / "worker.json"
        write_private_json(path,config)
        args = SimpleNamespace(**dict(vars(self.args),output=control,chain_config=path,
                                      cuda_visible_devices=self.environment["CUDA_VISIBLE_DEVICES"]))
        kernel = self._workload_path()
        scope = ChainScope(args,"cpython-chain",owner,self.author_report,kernel=kernel)
        physical = {}

        def finalizer():
            try:
                scope.close()
            finally:
                physical.update(owner.resource_state(scope))
            self._check_author(scope.namespace)
            return physical

        try:
            scope.__enter__()
            process = subprocess.Popen([str(args.bridge_binary),str(scope.port),str(kernel),"infer_records",PROTOCOL],
                env=scope.env,cwd=scope.root,stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,start_new_session=True)
            scope.track_root(process)
            writer = verify._capture_stderr(process.stderr,scope.root/"bridge-stderr.log",self.author_report)
            scope.children.append((process,writer))

            def abort():
                state = finalizer()
                process.wait(timeout=3)
                if not state["resources_confirmed"]:
                    raise RuntimeError("IMLane chain author scope remains unconfirmed")

            return RecordsBridge(process,abort_owner=abort,timeout_s=min(60,self.resources.query_timeout_s),
                                 protocol=PROTOCOL),finalizer,physical
        except BaseException as error:
            try:
                finalizer()
            except BaseException as cleanup:
                error.add_note("IMLane chain startup cleanup: " + type(cleanup).__name__)
            raise
