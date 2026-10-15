# Palimpzest fixed-version adapters

PZ owns semantic operators, optimization and legal DAG dependencies. This module
provides controlled private seams for PZ `0.8.2.sem_agg`, commit
`6efc19841c49d727ce42f5c2dd55e7433601d2d9`; it is parallel to LOTUS.
The [comparison contract](../../../../experiments/plans/reference/对照规范.md#execution-comparison-levels)
separates complete-system quality/cost/time from execution of one selected plan.

Deployment declares CPU/GPU, memory and model-service budgets, and chooses a fixed
configuration or a verified native automatic mode. PZ determines semantic readiness;
the selected backend owns dispatch, workers and backpressure. The thin adapter adds
no common concurrency controller. Its finite prepared-request/stack and snapshot
limits bound retained memory, rather than claiming to tune backend concurrency.
System comparisons freeze configuration rules and permit reasonable backend-specific
concurrency; capacity-matched mechanism experiments state the common work unit separately.
HTTP request budgets and vLLM sequence/token budgets remain distinct. Native Ray pool
autoscaling does not prove HTTP/GPU-window tuning; IMLane's declared `n` and SemLoom's
declared cap remain explicit. Dispatch within a cap is not verified online cap tuning.
Existing fixed-configuration evidence retains its original identity; these rules do
not require historical reruns or a new tuning matrix.

`completion_bridge(completion=..., observer=...)` keeps the original Generator's
prompt factory, parser, statistics and exception defaults. It substitutes the
module-local LiteLLM reference and guards source hashes. A configuration or
observer error fails the adapter; a genuine model or parser error retains the
upstream False/None default and is independently recorded as an invalid prediction.
The bridge rejects overlap and retains its patch while any started Generator is
still active. Its return does not independently prove that remote work stopped.

`freeze_best_plan(processor)` selects once with the original optimizer.
`execute_frozen_plan(processor, frozen)` calls the original execution strategy
with that same plan object, rejecting changed operator/generator parameters.
This helper requires no validator; sampled/validated system execution must use
the original processor rather than silently skipping sentinel work.
`plan_id` alone is insufficient: source contents, configuration, dependencies,
request hashes and service/cache state remain separate runner evidence.

The [native Movie runner](../../../scripts/experiments/run_sembench_palimpzest.py)
uses unchanged Q3/Q2 method bodies from SemBench
`c814e3807e72d4cf876b852b17e77f3cc94575c2`. It captures their terminal explicit
`Dataset.run(config)` call to avoid the generic runner's Gemini fallback, then
uses PZ's original run with explicit `QueryProcessorConfig` and parallel workers.
Q3 audits complete selected filtering and COUNT; Q2 retains LIMIT5 and records
actual submitted/completed work rather than assuming five requests.

Use the [configuration template](../../../../deploy/autodl/sembench_palimpzest.example.json)
and [separate driver dependencies](../../../requirements/palimpzest.txt).
Runtime paths and `FixedModelConfig` stay outside Git. The old vLLM enum is only
an explicitly recorded transport binding to one exact loopback `/v1/models`
identity. Unknown names never select Gemini or another provider. Upstream model
card costs are not actual service prices; missing usage and monetary cost remain
unavailable. Data preparation, service preparation and query execution are distinct.

```bash
export LITELLM_LOCAL_MODEL_COST_MAP=True HF_HUB_OFFLINE=1
PYTHONPATH=code "$PZ_DRIVER_PYTHON" code/scripts/experiments/run_sembench_palimpzest.py \
  --config deploy/autodl/sembench_palimpzest.example.json --data-preflight
PYTHONPATH=code "$PZ_DRIVER_PYTHON" code/scripts/experiments/run_sembench_palimpzest.py \
  --config deploy/autodl/sembench_palimpzest.example.json --preflight
```

`--execute` acquires the existing host-scope lease with stale recovery disabled,
verifies local model metadata and writes a new private run directory. CPU fixtures,
real-model runs and each of PZ+SemLoom/IMLane/Daft/Ray Data need separate evidence;
the completion seam is not a transfer of the entire query DAG to those backends.

The newer attachment review concerns PZ `807ed301`/1.5.3, not this SemBench tag.
Full backend qualification additionally needs a query-local ExecutionStrategy seam
before the original per-row thread pool, plus final SDK-request preparation and
original completion/parse/statistics continuation. The current completion bridge
is a native/diagnostic boundary only. A native-adapted control must use the same
prepare/finish path as each qualified backend. LIMIT/join/distinct/semantic aggregate
and unverified multi-call shapes are unsupported in that first adapted slice.
