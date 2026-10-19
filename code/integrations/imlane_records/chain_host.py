"""Author-owned submission/collection around the shared finite chain kernel."""

from contextlib import nullcontext
from dataclasses import asdict
import json
import math
from pathlib import Path
import socket
import time
import uuid

from integrations.imlane_records.chain_workload import ChainBatches, STAGE
from integrations.imlane_records import verify
from integrations.imlane_records.method_environment import preflight_method
from integrations.imlane_records.model_lifecycle import MODEL_CLOSE_GRACE_S
from integrations.imlane_records.model_process import assigned_environment
from integrations.imlane_records.records_host import RecordsResults
from src.baselines.common.private_artifacts import require_outside_git
from src.baselines.text.frameworks.lotus_chain_results import ChainBatchResults, _source_binding
from src.semantic_methods.lotus.record_chain import LotusRecordChain, RecordOccurrence
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec


def configuration(chain, engine, resources, expected, *, control_dir, deadline, model_environment):
    """Freeze caller values and preflight before constructing an author owner."""
    chain = LotusRecordChain.from_dict(chain.describe())
    engine, resources = EngineSpec.from_dict(asdict(engine)), RecordsResources(**asdict(resources))
    if resources.cpu_workers != 1 or resources.model_batch_rows > 16 or resources.max_active_batches > 2:
        raise ValueError("IMLane finite chain requires one worker, 1..16 batch rows and at most two pending blocks")
    if len({stage.method.model_name for stage in chain.stages}) != 1:
        raise ValueError("IMLane finite chain requires one model deployment")
    if (type(expected) not in (tuple, list) or any(type(row) is not RecordOccurrence for row in expected)
            or any(len({getattr(row, key) for row in expected}) != len(expected)
                   for key in ("row_id", "source_position"))):
        raise ValueError("finite chain requires globally unique source occurrence identities")
    if (engine.max_model_calls is None or engine.max_model_calls > 4096
            or len(expected) * len(chain.stages) > engine.max_model_calls):
        raise ValueError("reserve the finite chain's worst case request allowance before author allocation")
    if any(stage.generation.max_tokens > engine.max_model_len for stage in chain.stages):
        raise ValueError("chain max_tokens exceeds max_model_len")
    if type(deadline) not in (int, float) or not math.isfinite(deadline) or deadline <= time.monotonic():
        raise ValueError("finite chain needs an absolute finite work deadline")
    model_environment = assigned_environment(model_environment)
    engine.validate_driver_environment()
    control = Path(control_dir)
    require_outside_git(control)
    if not control.is_dir() or (control / "owner").exists():
        raise ValueError("chain owner needs a new private control directory")
    token = uuid.uuid4().hex
    (control / "owner").write_text(token)
    (control / "owner").chmod(0o600)
    return dict(schema="imlane.chain.worker.v1", chain=chain.describe(), query_id=chain.query_id,
                engine=asdict(engine), resources=asdict(resources), expected=[_source_binding(r) for r in expected],
                deadline_monotonic=deadline, control_dir=str(control), driver_host=socket.gethostname(),
                control_token=token, model_environment=dict(model_environment))


class ChainScope(verify.RecordScope):
    def __enter__(self):
        if not getattr(self.args, "method_python", None):
            raise ValueError("finite chains require an explicit isolated method interpreter")
        preflight_method(self.args.method_python, self.args.author_root, verify.CODE, chain=True)
        return super().__enter__()

    def stop_root(self, process, writer, *, owner=False):
        return super().stop_root(process, writer, owner=owner, timeout_s=MODEL_CLOSE_GRACE_S)

    def workload_environment(self):
        return dict(IMLANE_CHAIN_CONFIG=str(self.args.chain_config),
                    CUDA_VISIBLE_DEVICES=self.args.cuda_visible_devices,
                    HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1", LITELLM_LOCAL_MODEL_COST_MAP="True")


class ChainResults:
    """Single-pass relation results; all stage calls remain in the shared ledger."""

    def __init__(self, records, client, *, chain, expected, resources, control_dir, deadline,
                 close_owner, cleanup_scope=nullcontext, cancelled=None):
        self.chain = LotusRecordChain.from_dict(chain.describe())
        self.state = ChainBatchResults(expected, self.chain)
        self.control = Path(control_dir)
        self.cancelled = cancelled
        self.carrier = RecordsResults(records, client, query_id=self.chain.query_id,
            mapping=ChainBatches(self.chain.query_id, expected), stage_id=STAGE, block_prefix="chain",
            control_dir=control_dir, deadline=deadline, close_owner=close_owner, cleanup_scope=cleanup_scope,
            block_rows=resources.model_batch_rows, max_pending=resources.max_active_batches)
        self.completed = self._done = False
        self.last_close_report = None

    @property
    def calls(self):
        return self.state.calls

    @property
    def columns(self):
        return self.state.columns

    @property
    def batch_stage_stats(self):
        return self.state.batch_stage_stats

    @property
    def raw_result_batches(self):
        return self.state.raw_result_batches

    @property
    def unassociated_results(self):
        return self.state.unassociated_results

    def __iter__(self):
        return self

    def _failure(self, error):
        # Consume already accepted blocks before raising; no fresh fill follows
        # an error terminal. The collector retains usage before any parser error.
        self.carrier.failed = True
        try:
            for value in self.carrier if self.carrier.started else ():
                try:
                    self.state.accept(value["batch"])
                except BaseException as later:
                    error.add_note("additional chain batch: " + type(later).__name__)
        except BaseException as later:
            error.add_note("chain carrier drain: " + type(later).__name__)
        self._done = True
        error.chain_calls = list(self.calls)
        error.chain_stage_stats = list(self.batch_stage_stats)
        return error

    def __next__(self):
        if self._done:
            raise StopIteration
        try:
            if self.cancelled is not None and self.cancelled():
                (self.control / "stop").touch(mode=0o600)
                raise InterruptedError("IMLane chain cancelled")
            while not self.state.pending:
                try:
                    value = next(self.carrier)
                except StopIteration:
                    self.state.verify_coverage()
                    self.state.finalize_columns()
                    self.completed = self._done = True
                    raise
                self.state.accept(value["batch"])
            return self.state.pop()
        except StopIteration:
            raise
        except BaseException as error:
            raise self._failure(error)

    def close(self):
        error = None
        try:
            self.last_close_report = self.carrier.close()
        except BaseException as failure:
            error = failure
            self.completed = False
            self.last_close_report = self.carrier.last_close_report
        finally:
            # Audit files belong to this private owner. Recovery adds ledger
            # evidence only, never a final relation or another model request.
            for pattern in ("chain-failure-*.json", "chain-audit-*.json"):
                for path in sorted(self.control.glob(pattern)):
                    try:
                        if path.stat().st_size > 64 * 1024**2:
                            raise ValueError("chain audit exceeds the existing IPC byte allowance")
                        self.state.recover(json.loads(path.read_text()))
                    except BaseException as recovery:
                        self.completed = False
                        if error is None:
                            error = recovery
                        else:
                            error.add_note("chain audit recovery: " + type(recovery).__name__)
        if self.state.cleanup_errors:
            self.completed = False
            if error is None:
                error = RuntimeError("IMLane chain worker audit/cleanup remains unconfirmed")
            self.last_close_report = dict(self.last_close_report or {}, status="unknown",
                                          cleanup_errors=list(self.state.cleanup_errors))
        if error is not None:
            error.chain_calls = list(self.calls)
            error.chain_stage_stats = list(self.batch_stage_stats)
            raise error
        return self.last_close_report
