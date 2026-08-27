# Shared vLLM experiment modules

This package keeps experiment orchestration separate from reusable metric derivation:

- `config.py` and `cli.py` load the experiment contract and construct Job commands.
- `runner.py`, `runtime.py`, and `direct_control.py` execute groups and collect raw evidence.
- `evidence.py` validates durable records and replay identity.
- `metrics.py` preserves the stable metric import surface and contains only group-level
  service deltas and shared-credit utilization.
- `fairness_metrics.py` derives service-rate and completion-accounted fairness.
- `resource_metrics.py` summarizes GPU, vLLM, CPU, memory, and energy samples.
- `active_set.py` audits observed Job overlap, borrowing, reclaim, and post-completion drain.
- `ready_event_metrics.py` joins bounded ready-window registration and grant events.
- `saor_event_metrics.py` audits the lossless bounded-SAOR release ledger.
- `work_evidence.py` and `saor_projection_evidence.py` validate work accounting and
  offline debt projections.

Callers should continue importing the public metric functions from `metrics.py` or the package
`__init__.py`; the focused modules are implementation seams and test targets.
