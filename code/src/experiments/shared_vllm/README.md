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

Missing `vllm_waiting` stays unknown through observe-only rows and capacity
actuation: its estimated model queue work is `None`, not zero. Endpoint scope,
the original count/unknown marker and sampling time remain unchanged. Without
queue-clear evidence, bounded capacity control restores its calibrated static
arm and resets upshift hysteresis; fresh KV pressure can still support the
existing downshift guard even before a service rate is available. Known zero
waiting retains backlog/rate bootstrap, and positive waiting retains congestion
protection. [Helper tests](../../../tests/experiments/test_shared_vllm_runtime.py)
exercise the actual four-sample path; this is a correctness fix, not a new
controller or performance result.

<a id="unknown-waiting-integration"></a>

Integration applies only `482778bcd9f15772d55a338cc98f98f137109973` on
`81228bddcf525e44baf6ae99cd3f90008aa663e9`; replica research and routing are not imported.
The four queue-consuming controllers were checked for nullable work, while the
image and broker producers retain their observed integer queues. Saved JSON
keeps an unknown estimate as `null`; the actual CSV writer/readback keeps it
empty, with known zero still `0` before capacity actuation. The final 13-module
combination passes 196 tests with no skips, including this persistence check,
image snapshots, broker, existing queue/KV controls and timing contracts.
The source revision's 155 tests are a separate, overlapping combination.
Independent Standards and Spec reviews have no mandatory findings; this
integration starts no server, model or GPU workload and adds no performance claim.
