# Independent phase deadlines and execution assembly

Status: implemented and checked against baseline `2462c76d`. The
[plan](../../../plans/semloom_incremental_session_design.md#19-phase-deadlines-and-execution-assembly-2026-09-08)
records the source decisions. This is correctness and ownership cleanup, not a scheduling
performance or model-quality experiment.

## Changes

`SessionTimeouts` independently specifies queue, backend and consumer durations. None disables
only that phase's deadline. Existing callers omitting this policy retain their original
`wait_timeout_s` behavior. The production gateway supplies a policy with no queue/consumer
phase deadline; PG cancellation and socket RPC deadlines remain active. The HTTP model timeout
still applies, and unresolved remote work retains capacity during bounded cleanup.

`incremental_runtime.py` now owns connection/session lifecycle and draining. Fixed-model HTTP
I/O moved to `async_fixed_model.py`; `incremental_execution.py` composes the existing backend,
core, policies and task description. `server.main(incremental_execution_factory=...)` and both
Map adapters accept this assembly seam. The default builder supports existing SessionPolicies,
work descriptors, work units and phase deadlines. Describing demand/locality does not rewrite
the canonical model input. No second scheduler was introduced.

The HTTP execution and finalizer ASTs match the prior implementations after normalizing method
and error-field renames. Default request-count work and organization remain unchanged. The
outdated 1–64 range in the PG binding design was corrected, and the CLI guide now leads with v6.

## Validation

- Local targeted suite: **50 passed**. Linux: **337 scheduling**, **32 provider**, **115 PG
  contracts**, **17 observer** tests; **501 total**.
- PostgreSQL 18.3: regression **1**, TAP **1961** across 12 files. C code was unchanged; the
  previously verified extension binary SHA was checked, not rebuilt for this Python-only slice.
- Fake clock: three accepted tasks, one request slot, 11-second backend deadline and 8-second
  tasks complete at 8/16/24 seconds. At second 11 the queue no longer fails the session.
  Explicit queue/backend/consumer deadlines still fail their corresponding phase and preserve
  resource ownership. This long-queue timing evidence is controlled, not a timed model experiment.
- Actual v6 socket adapter with injected assembly: synthetic length work (1/3 units), shared
  locality and last-task-first selection produce results in order 1/0, preserve canonical input,
  and release all task resources. These are synthetic work estimates, not calibrated token costs.
- One fresh real-model ledger: **12 POSTs**, no retry or generation warm-up. Synchronous SELECT,
  v6 SELECT/INSERT, rollback, cancellation, recovery and LIMIT pass. The gateway uses 65 held
  tasks, two active requests and separate 8 MiB input/result budgets; PG uses a two-row window.
  HTTP peak is **2**, ten incremental tasks and six sessions drain with all resources zero.
- **641 source/test files** match the final validation copy. Controller and postflight confirm
  model port closed, both GPUs at 1 MiB, no owned processes or Raylets.

Model remains Qwen2.5-7B-Instruct revision `a09a35458c702b33eeacc393d103063234e8bc28`,
vLLM 0.25.1 and the existing fixed service settings. Nine model files were rehashed; core/text
preflight passed. No dependency installation or model download was performed.

## Retained code and next migration

The v5 `incremental-map-window-one` bridge remains. Current consumers include its adapter,
gateway CLI, observation-tool mode selection, PG provider registration, tests and documentation.
Before retirement, qualify v6 window=1 for SELECT/INSERT, NULL/LIMIT, cancellation/recovery,
input/result association and errors; migrate those callers and then remove the bridge-specific
adapter/flags/registration together. Synchronous semantic references and historical evidence
remain independent of that removal.

Compatibility exports, the synchronous scheduler and current wire consumers were not removed.
The core and incremental service still allow one active session. Multi-Job resource ownership
and extraction of the PG row-window helper are subsequent work, not completed capabilities.

## Evidence

The [summary](raw/summary.json), [artifact index](raw/artifact-index.json) and
[archive](raw/validation-artifacts.tar.gz) contain 25 redacted/scanned artifacts: test logs,
HTTP move audit, source manifest, model/service identity, request ledger, results and reusable
controller/driver. All checks passed without a failed model run; historical evidence is unchanged.
