# Shared execution modules: ownership cleanup

Status: controlled checks and one 12-request real-model regression passed.
This is a behavior-preserving refactor from `bf327649`, not a new scheduling method or SQL capability.
The [implementation plan](../../../plans/semloom_incremental_session_design.md#shared-module-cleanup)
records scope and source decisions. No historical result or active execution path was deleted.

## Changes

| Shared owner | Consumers |
|---|---|
| `adapters/incremental_runtime.py` | Window-one and multi-in-flight Map adapters are siblings; v6 no longer inherits the v5 completion loop |
| `wire/map_codec.py` | v5 compatibility exports and v6 bindings share one semantic validator/digest implementation |
| `completion.py` | Request/result types and adapter/error contracts no longer require the synchronous session runner |
| `adapters/model_config.py` | Both HTTP implementations use the same fixed model configuration |
| `scheduling/core/policy_contracts.py` | Both schedulers consume admission/routing/shared-credit interfaces directly |

Legacy imports remain aliases to the same objects. A fresh-process import test confirms the new
adapter/core modules do not load the synchronous scheduler, synchronous session runner, v5 adapter,
v5 wire facade, or synchronous HTTP implementation. The all-version gateway still intentionally
loads its supported protocol dispatchers.

The migration audit compares 43 moved function/class definitions and 24 remaining definitions with
their previous ASTs; all match. The window-one adapter went from 303 to 54 lines and v5 from 510 to
43 lines because their common implementation moved. Across affected production Python files, net
change is **+31 lines**, including compatibility imports. This is clearer ownership, not a claim
that hundreds of lines of required execution logic were unnecessary.

`scheduler.py` still has consumers in `runtime/execution.py` and the profiling execution path.
Recording, Filter and synchronous Map still consume older wire protocols. Their loops/versions
cannot be removed merely because newer entry points exist.

## Verification

- Linux: scheduling 335, provider 28, PG contracts 115, observer 17; **495 passed**.
- Local targeted adapter/ownership checks: **11 passed**.
- PostgreSQL 18.3: regression 1 and **1958 TAP checks** passed using the unchanged verified C binary.
- Source identity: **638 files** matched the actual validation copy.
- Real-model regression: one fresh ledger, **12 POSTs**, no retry or generation warm-up. The same
  bounded driver checks synchronous SELECT, v6 SELECT/INSERT, constraint rollback, two-request
  cancellation, recovery and LIMIT. Actual HTTP peak stayed **2** within one PG query.
- Ten incremental tasks and six sessions drained with all credits zero. Controller cleanup confirmed
  the model port closed and both GPUs at 1 MiB; postflight found no owned processes or Raylets.

The model remains Qwen2.5-7B-Instruct revision `a09a35458c702b33eeacc393d103063234e8bc28`, with
vLLM 0.25.1, the prior fixed service settings, temperature 0 and normal/cancel output limits 128/256.
Nine model files were rehashed and core/text preflight passed. No downloads or installs were needed.
These checks establish compatibility; they do not establish throughput or quality improvements.

## Evidence

The readable [summary](raw/summary.json) contains counts, plans, real results, cleanup and archive hash.
The [artifact index](raw/artifact-index.json) lists the SHA256 of all 27 files in the
[compressed evidence archive](raw/validation-artifacts.tar.gz), including test logs, source manifest,
AST audit, model identity, request ledger, raw events and the reusable driver/controller source.
All archive members were redacted and scanned before compression. Prior evidence remains in place.

The initial lint check found one import left unused after extraction; it was removed. The first export
check counted its own process as a survivor; excluding only that PID confirmed no residual work.
Both observations are retained in the archive; neither caused a failed or repeated model run.
