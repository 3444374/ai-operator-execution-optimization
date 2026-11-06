# Shared-service research values and offline metrics

This directory contains typed experiment values, evidence readers and metric derivation.
Shared-service coordination and SAOR policies live in
[research/multitask](../../research/multitask/README.md); ordinary records do not import them.

- `config.py` keeps the typed scenarios, work-cost values, control settings, capacity arithmetic
  and argument readers used by research methods and offline evidence audits.
- `runtime.py` retains host/resource sampling helpers; `preflight.py` supplies the idle reader
  used by the maintained scenario script.
- `evidence.py` validates stored Job records and replay identity.
- `metrics.py` exports group-level service deltas and shared-credit utilization.
- `fairness_metrics.py` derives service rates and completion-accounted fairness.
- `resource_metrics.py` summarizes recorded GPU, vLLM, CPU, memory and energy samples.
- `active_set.py` reads Job overlap, borrowing, reclaim and post-completion drain.
- `ready_event_metrics.py` joins ready-window registration and grant events.
- `saor_event_metrics.py` reads the retained SAOR release ledger.
- `work_evidence.py` and `saor_projection_evidence.py` check request work and offline debt projections.

Import metric functions from `metrics.py`. The old JSON loader, profiler Job command builder
and runner CLI have left the maintained code. Their exact sources and the earlier complete
launcher tree are recorded in the [recovery notes](../../../../experiments/reproduction/native_baselines/README.md#retired-shared-vllm-loader).
Original measurements and the 14 research assets remain unchanged.
