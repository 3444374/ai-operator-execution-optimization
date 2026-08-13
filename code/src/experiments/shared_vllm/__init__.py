"""Shared-vLLM multi-job configuration, metrics, evidence, and runner."""

from .config import (
    GroupRunIdentity,
    RunnerOptions,
    SharedVllmConfig,
    SharedVllmScenario,
    StateAwareControlConfig,
    build_job_command,
    load_config,
)
from .evidence import (
    _coordinator_name,
    _load_resume_manifest,
    _redact_command,
    _request_trace_succeeded,
    _rewrite_group_runs,
    _run_instance_id,
    _validate_final_credit,
    _validate_job_evidence,
    _validate_replay_starts,
    _validate_runner_topology,
)
from .metrics import (
    active_set_phase_summary,
    cumulative_service_disparity,
    group_metric_delta,
    group_resource_summary,
    jain_fairness,
    normalized_job_service_rates,
    shared_credit_trace_summary,
)
from .runner import _run_group, _validate_rehearsal_record, run_experiment
from .runtime import _RayCreditObserver

__all__ = [name for name in globals() if not name.startswith("__")]
