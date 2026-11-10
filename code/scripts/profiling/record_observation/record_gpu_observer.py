"""Worker-local execute/state/sample observation; never reads GPU tensors."""
import hashlib
import inspect
import importlib.metadata
import importlib.util
from contextlib import nullcontext
import os
from pathlib import Path
import torch
from vllm.v1.worker.gpu_worker import Worker
from record_observer import emit, emit_buffered, settings

# Observing an older source checkout must still use the qualified tool helper.
_helper_path=Path(__file__).resolve().parents[3]/'src/observability/native_gpu_decisions.py'
_spec=importlib.util.spec_from_file_location('_record_native_gpu_decisions',_helper_path)
_decisions=importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_decisions)
cpu_counts=_decisions.cpu_counts


_MODULES = {'vllm.v1.worker.gpu_model_runner':'v1', 'vllm.v1.worker.gpu.model_runner':'v2'}


def runner_contract(runner):
    module=type(runner).__module__
    expected=settings().get('native_runner_contracts',{}).get(module)
    try:
        if importlib.metadata.version('vllm')!=settings().get('native_vllm_version'):return None
        worker_path=inspect.getsourcefile(Worker)
        worker_digest=hashlib.sha256(Path(worker_path).read_bytes()).hexdigest() if worker_path else None
        if worker_digest!=settings().get('native_worker_sha256'):return None
        path=inspect.getsourcefile(type(runner))
        digest=hashlib.sha256(Path(path).read_bytes()).hexdigest() if path else None
    except (OSError,TypeError,importlib.metadata.PackageNotFoundError):digest=None
    return _MODULES.get(module) if expected is not None and digest==expected else None


def state_members(state, kind):
    if state is None:return None,'no_execute_state'
    if kind=='v2':
        batch=getattr(state,'input_batch',None)
        requests=getattr(batch,'req_ids',None)
        counts=cpu_counts(getattr(batch,'num_scheduled_tokens',None))
        if not isinstance(requests,(list,tuple)) or counts is None or len(requests)!=len(counts):
            return None,'sample_CPU_members_or_counts_unavailable'
        if any(type(r) is not str for r in requests) or len(set(requests))!=len(requests):
            return None,'invalid_sample_member_identity'
        return dict(zip(requests,counts)),'execute_model_state.input_batch'
    if kind=='v1':
        scheduler=getattr(state,'scheduler_output',None)
        if scheduler is None and isinstance(state,tuple) and len(state)==10:scheduler=state[0]
        values=getattr(scheduler,'num_scheduled_tokens',None)
        if isinstance(values,dict) and all(type(k) is str and type(v) is int and v>=0 for k,v in values.items()):
            return dict(values),'execute_model_state.scheduler_output'
        return None,'sample_scheduler_state_unavailable'
    return None,'native_runner_contract_unqualified'


def range_push(marker):
    try:
        torch.cuda.nvtx.range_push(marker)
        return True
    except Exception as error:
        emit('worker_clock_unknown',marker=marker,boundary='range_push',status='unknown',
             error_type=type(error).__name__)
        return False


def range_pop(marker,opened):
    if not opened:return
    try:torch.cuda.nvtx.range_pop()
    except Exception as error:
        emit('worker_clock_unknown',marker=marker,boundary='range_pop',status='unknown',
             error_type=type(error).__name__)


class ObservedWorker(Worker):
    def execute_model(self,scheduler_output):
        sequence=getattr(self,'_record_execute_sequence',0)+1
        self._record_execute_sequence=sequence
        marker='record-iteration:'+str(os.getpid())+':'+str(sequence)
        kind=runner_contract(self.model_runner)
        members=dict(scheduler_output.num_scheduled_tokens) if kind else None
        emit('iteration_launch_start',iteration=sequence,marker=marker,observer_execute_id=sequence,
             scheduled_tokens=members,actual_runner_module=type(self.model_runner).__module__,
             membership_reason=None if kind else 'native_runner_contract_unqualified')
        opened=range_push(marker)
        succeeded=False
        try:
            decisions=(_decisions.observe_decisions(self.model_runner,scheduler_output,sequence,emit_buffered)
                       if kind=='v1' and settings().get('native_vllm_version')=='0.25.1' else nullcontext())
            with decisions:
                value=super().execute_model(scheduler_output);succeeded=True
            return value
        finally:
            range_pop(marker,opened)
            state=getattr(self.model_runner,'execute_model_state',None) if succeeded else None
            # Keep scalar metadata only, never a strong tensor/state reference.
            self._record_handoff=None if state is None else (id(state),sequence)
            emit('iteration_launch_end',iteration=sequence,marker=marker,observer_execute_id=sequence,
                 status='returned' if succeeded else 'error')
            emit('execute_state_handoff',observer_execute_id=sequence,
                 state_identity=None if state is None else str(os.getpid())+':'+str(id(state)),
                 actual_runner_module=type(self.model_runner).__module__)

    def sample_tokens(self,grammar_output):
        state=getattr(self.model_runner,'execute_model_state',None)
        members,source=state_members(state,runner_contract(self.model_runner))
        handoff=getattr(self,'_record_handoff',None)
        sample=getattr(self,'_record_sample_sequence',0)+1;self._record_sample_sequence=sample
        step=handoff[1] if state is not None and handoff is not None and handoff[0]==id(state) else None
        # Negative IDs label unbound sample observations; they are not native steps.
        sequence=step if step is not None else -sample
        marker='record-sample:'+str(os.getpid())+':'+str(sequence)
        fields=dict(iteration=sequence,marker=marker,observer_sample_id=sample,handoff_step=step,
                    state_identity=None if state is None else str(os.getpid())+':'+str(id(state)),
                    step_identity_source='actual_consumed_state' if step is not None else 'unknown',
                    actual_runner_module=type(self.model_runner).__module__,membership_reason=source)
        # 426 requires actual sample-entry scheduled_tokens. Omit on unknown.
        if members is not None:fields['scheduled_tokens']=members
        emit('iteration_sample_start',**fields)
        opened=range_push(marker)
        try:return super().sample_tokens(grammar_output)
        finally:
            range_pop(marker,opened)
            emit('iteration_sample_end',iteration=sequence,marker=marker,observer_sample_id=sample,handoff_step=step)
            self._record_handoff=None
