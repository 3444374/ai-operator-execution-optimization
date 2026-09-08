# Incremental capacity separation and terminal completion cleanup

Status: controlled checks, PG18.3 tests and two independent 12-request real-model runs passed.
This is engineering correctness evidence from `21909415` plus the archived source manifests.
The [plan](../../../plans/semloom_incremental_session_design.md#18-capacity-separation-and-completion-cleanup-2026-09-08)
records scope and source decisions; no performance or quality improvement is claimed.

## Changes and behavior

- v5/v6 share settled backend completion decoding. Malformed JSON/UTF-8 now produces
  `MODEL_RESPONSE_INVALID` on v6, instead of silently closing the connection.
- v6 releases settled deliveries abandoned by terminal errors or disconnects. Previously the
  error path could retain a delivered-result lease and quarantine the engine. Successful
  delivery still releases after sending; unresolved remote work still drains or quarantines.
- Accepted tasks, input bytes, reserved result bytes and active HTTP requests are separately
  configurable. Omitted options preserve prior defaults. Polling uses the session's configured
  interval; SessionLimits uses named fields and validates before starting the I/O thread.
- PG allocates query-owned pending arrays by window size and checks metadata/row reservations
  against byte budgets before allocating. The former 64-task ceiling is removed. The remaining
  signed int32 field range comes from the PG JSON decoder, not GPU capacity calibration.
- The v5 window-one bridge, synchronous reference, older active protocols and historical
  experiment evidence remain. This does not implement multi-Job resource allocation or GPU
  memory accounting. Each component's byte budget is a local reservation, not total process RSS.

## Verification

- Local adapter/ownership suite: **14 passed**. Linux provider **31**, PG contracts **115**.
- Rebuilt extension on PostgreSQL 18.3: regression **1**, TAP **1961** across 12 files passed.
  The extended fixture runs **65 retained rows through a two-request backend**, and rejects
  the same row window with an insufficient byte budget. Existing Filter and synchronous Map
  tests remain in the regression/TAP and contract suites.
- Controlled socket tests cover malformed JSON, invalid UTF-8, missing completion fields,
  bridge errors and settled-resource cleanup. A separate test admits only two of three tasks
  under the result byte budget, despite a 65-task ceiling and a one-request backend.
- Two fresh model ledgers each issued exactly **12 POSTs**, no retries or generation warm-up.
  Each checks synchronous SELECT, v6 SELECT/INSERT, constraint rollback, cancellation,
  recovery and LIMIT. Final gateway configuration: 65 held tasks, two active requests,
  8 MiB input and 8 MiB result budgets; PG uses its two-row window for these real-model cases.
  The 65-row workload above is controlled-backend evidence, not a 65-row model run.
- Final source identity: **639 files** match; rebuilt extension SHA is in the summary.
  Actual HTTP peak is **2**; ten incremental tasks and six sessions drained with resources zero.
  Both controllers confirmed the model port closed and both GPUs at 1 MiB; final process
  audit found no owned processes or Raylets.

Model: Qwen2.5-7B-Instruct revision `a09a35458c702b33eeacc393d103063234e8bc28`,
vLLM 0.25.1, unchanged service configuration and temperature 0. Nine model files were rehashed
and core/text preflight passed. No model downloads or dependency installations were needed.
Source/configuration changes occurred only between runs. The second run followed moving budget
validation before thread startup; its manifest is the final implementation.

## Evidence and development failures

The [summary](raw/summary.json), [artifact index](raw/artifact-index.json) and
[archive](raw/validation-artifacts.tar.gz) preserve 35 redacted, scanned files: test/build logs,
source manifests, both model ledgers/results, final controller/driver and model identities.
Historical evidence was not deleted. Commands in the archived controllers use redacted paths;
real runtime settings remain outside Git.

The first new socket test violated the core's single-driving-thread rule; its harness was fixed.
The corrected test then exposed the unreleased settled-result lease described above. These
observations are recorded in development-notes.json. Neither was hidden by a model retry;
both model runs passed independently. Scheduling policy/core code was unchanged, so its full
335-test suite was not rerun for this slice.
