# Consumer deadline fix and v5 incremental bridge retirement

Status: deadline regression fixed; v5 incremental window-one bridge retired after qualification.
Baseline: `e803e659`; the [plan](../../../plans/semloom_incremental_session_design.md#20-consumer-deadline-publication-and-window-one-bridge-retirement-2026-09-08)
records the ordered checks. Synchronous semantic references and wire v5 remain supported.

## Deadline correction

The core computed `next_deadline` before publishing READY results as consumer-owned LEASED
results. The regression reproduced both missing deadlines: delivery at second 2 with a 3-second
consumer duration returned None instead of 5; the legacy 10-second duration returned None
instead of 12. Both original failures are preserved in `deadline-before.log`.

Deadline selection now includes the leases about to transfer. Ownership is still published only
after constructing the return value, without introducing backend/policy/clock/sink calls after
transfer. Tests check the deadline, timeout at that deadline, explicit disabling and release.
The default gateway disables consumer phase deadlines, so the direct evidence for this timer
fix is controlled-clock testing. Real-model results below verify execution and lifecycle; they
do not claim to exercise an enabled consumer timer.

## Actual cleanup and migration

Removed the old `incremental_map.py` adapter, bridge CLI argument, PG profile/ops/identity
branches and its exported v5 execution identity. The observer now selects one incremental mode.
Removed the unused runtime sequence field. Lifecycle tests migrated from the deleted bridge
suite to `test_incremental_window_one.py`; they still check disconnect/pipelining with retained
remote credit, late recovery, quarantine, invalid response cleanup and setup failure.

Implementation paths (`code/src` and PG `src`) net **101 fewer lines**. This excludes tests,
documentation and evidence. No replacement compatibility adapter or second scheduler was added.
Old CLI/profile/identity names are explicitly rejected, not silently aliased.

For one-row operation, launch the gateway with:

```text
--incremental-map --max-held-tasks 1 --max-active-requests 1
```

Configure the database connection as well:

```sql
SET semloom_pg.provider_execution_profile = 'incremental-map';
SET semloom_pg.provider_window_tasks = 1;
```

The previous bridge's source is recoverable at the baseline commit. Historical evidence remains
unchanged. Other compatibility exports, the legacy timeout fallback, synchronous scheduler and
existing wire consumers were not removed. Multi-session/multi-Job resource management and PG
window-helper extraction remain subsequent tasks.

## Ordered verification

1. Before deletion: old bridge and new v6-window-one controlled suites passed (50 targeted tests).
   PG18.3 regression and 1961 TAP checks passed after migrating the window-one PG test to v6.
   SELECT/INSERT, NULL, volatile input, LIMIT and physical request counts remained correct.
2. Before deletion: a fresh **10-request real-model run** compared the actual old v5 incremental
   profile with v6 window one. EXPLAIN confirms the old bridge provider was used for reference.
   Outputs matched; INSERT, constraint rollback, cancellation, recovery and LIMIT passed.
3. After deletion: Linux **338 scheduling + 33 provider + 115 PG contracts + 17 observer = 503**
   tests and local **52** targeted tests passed. Rebuilt PostgreSQL 18.3 extension passed
   regression **1** and **1963 TAP** checks, including explicit rejection of the removed PG name.
4. Final code: separate real-model runs used the synchronous reference, then v6 window **1**
   (**10 POSTs**) and window **2** (**12 POSTs**). Both passed the same result, write, rollback,
   cancellation/recovery and LIMIT checks. Observed HTTP peaks were **1** and **2** respectively;
   incremental task counts were 8/10, and all six incremental sessions drained in each run.

Total physical requests: **32**, across three fresh ledgers, with no model retry or generation
warm-up. This is small synthetic correctness evidence, not quality, optimal-concurrency or
performance qualification. The two fixed input strings plus NULL are the same controls used
in earlier bounded runs. Request/response digests, model identity and token usage were checked.

Qualification source: **642 files**; final source: **640 files**, all matching their run copies.
Model remains Qwen2.5-7B-Instruct revision `a09a35458c702b33eeacc393d103063234e8bc28`,
vLLM 0.25.1 and unchanged serving settings. Nine model files were rehashed per preparation;
core/text preflight passed. No dependency installation or model download was required.

All three controllers confirmed port closure and both GPUs at 1 MiB. Final audit found no owned
processes or Raylets, and the deleted adapter is no longer importable. The rebuilt extension SHA
is in the summary. The qualification source and old binary backup remain separately available
on the validation host; they were not overwritten by final tests.

## Evidence

The [summary](raw/summary.json), [artifact index](raw/artifact-index.json) and
[archive](raw/validation-artifacts.tar.gz) preserve **62** redacted/scanned artifacts: original
failure and passing tests, qualification/final manifests, build and PG logs, all three model
ledgers and result/event files, driver/controller sources, identity audits and cleanup checks.
The model driver varies only window/profile and its corresponding exact request budget; its
full source is archived for each run. There were no failed model runs or hidden retries.
