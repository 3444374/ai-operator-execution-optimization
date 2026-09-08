# Accepted-window organization and independent request execution

Status: final source passes controlled checks and real-model confirmation; services stopped.

This implements the next single-flow connection in the [incremental design](../../../plans/semloom_incremental_session_design.md#organized-window).
An independent finite producer supplies already authorized tasks; the organizer operates only on
accepted tasks, and the existing session owns their buffers, submissions, completions and release.
No PG code or wire protocol changed. This is an engineering correctness check, not a performance study.

## Interface and reuse

`TaskInfo` separates call/row/stage identity from `TaskKey`. It reuses `WorkDescriptor` for calibrated
work-unit identity and explicit locality. Immutable typed metadata and opaque metadata share a per-task
size bound; work must match the amount charged by admission. Optional metadata preserves legacy calls.

`WorkWindowOrganizer` reuses `slice_service_quanta` over compatible capability/stage/unit/locality
members, with optional local work sorting. It takes no source iterator and owns no payload cache.
The core validates its returned members and retains only pending task keys across backpressure.
`BatchKey` and `BatchMember` persist in backend requests and deliveries, while logical identity is
unchanged. The consumer can associate a completion with its original row irrespective of delivery order.

The existing backend accepts one complete task per physical request. A two-member organization batch
therefore expands into **two independently charged requests**. Group selection is not atomic remote
acceptance, prompt concatenation, or a single request returning multiple row results. Partial failures
retain the original fail-session policy; cancellation of siblings does not return their credits before
authoritative terminal events. Old calls without an organizer retain their prior selection behavior.

## Controlled checks and repaired progress signal

| Candidate | Local core/transport/method/organization checks | Linux full scheduling suite |
|---|---|---|
| Initial organized path | [53 pass](raw/local-tests-first.txt) | [334 pass](raw/first/linux-tests.txt) |
| Final progress fix | [54 pass](raw/local-tests-final.txt) | [335 pass](raw/final/linux-tests.txt) |

Coverage includes partial offer, work/locality grouping, independent request/work counters, reverse
completion and row association, release backpressure, retained membership after backend rejection,
invalid/duplicate/unowned member plans, bounded metadata and unit mismatch, oversized complete tasks,
member failure, cancellation and cross-session late cleanup, and two-stage method producers.
The recording router also verifies that locality and work reach the existing routing interface.

After the first real run passed, review found that the old immediate-progress signal examined only
the input-order head. With work costs `[3,1,1]`, capacity 3 and a one-action step, a small member is
submitted first; another small member still fits, but the old predicate reported no immediate work.
The [failing regression](raw/progress-before-fix.txt) is preserved. The [fix](raw/progress-fix.patch)
checks for fitting work in the accepted queued window; unsuccessful dispatch still respects `retry_at`.
All affected checks were rerun after this source change. This is not an extra model retry hidden in the
first result: the final source receives its own four-request confirmation below.

Commands from the repository root:

```sh
PYTHONPATH=code python3 -m unittest tests.scheduling.test_organized_session tests.scheduling.test_incremental_session tests.scheduling.test_async_backend tests.scheduling.test_method_continuation
env -u PYTHONPATH -u RAY_ADDRESS python -m unittest discover -s code/tests/scheduling -t code -p 'test_*.py'
```

## Real-model setup and scope

Both real runs use two synthetic rows, each performing draft then revise. The second stage uses its
own first-stage model response. Each wave is a two-member organization batch with two independently
in-flight HTTP requests. The producer, method continuations, WorkWindowOrganizer, SessionEngine,
BoundedAsyncBackend and real model all participate; this is not fixture output replay.

The same verified Qwen2.5-7B-Instruct revision `a09a35458c702b33eeacc393d103063234e8bc28` runs on one GPU
with BF16, eager execution, FCFS, model context 4096, and separate driver/model environments. Nine model
files are rehashed before each preparation. Core/text environment preflight is read-only; no dependency,
model, database or service configuration change occurred between runs. Private settings remain outside Git.
Each controller owns and cleans up its localhost service; invocation is `python launch.py --settings <private-settings.json>`.

Session limits are two held tasks, two active requests, two diagnostic work units, 16 KiB total input,
8 KiB per input, 64 KiB per raw response, 1 KiB metadata per task, and 16 actions per step. Each method
has at most two stages and 64 bytes of state. Each model call allows 64 output tokens at temperature 0.
The two capability names resolve to the same generation model in this diagnostic adapter. Unit cost
one and shared-locality labels are synthetic declarations, not cost calibration or measured cache reuse.

Each run has a separate durable four-POST ledger. Total authorized usage across both runs is eight;
there is no extra warm-up generation or HTTP retry. The first source, result and ledger are retained
under `raw/first`; the final confirmation has its own source manifest and records. The isolated remote
checkout reports base `2d2dee35`, so that Git value alone does not identify the tested working tree.
The candidate extends `9f555a00`; 624 non-Markdown source/test hashes identify each tested source.

## Observed real results and source audit

Both [first](raw/first/run-summary.json) and [final](raw/final/run-summary.json) runs pass four POSTs,
two organization batches, two final rows and peak HTTP concurrency two. Each batch contains two
single-member physical requests. The final [responses](raw/final/run-responses.json) retain member and
logical identity; an independent readback confirmed each revise prompt contains its own draft response.
The [trace](raw/final/run-trace.json) and summary confirm backpressure before releasing first-stage
results, zero final resource counters and a joined transport thread. The
[final controller](raw/final/prepare-controller-summary.json) reports stopped service, closed port and
both GPUs at 1 MiB; [first cleanup](raw/first/prepare-controller-summary.json) passed as well.

The independent ledgers record [four initial](raw/first/prepare-budget.jsonl) and
[four final](raw/final/prepare-budget.jsonl) requests. Neither controller/model run failed; the retained
failure is the targeted controlled regression that motivated the progress fix. All
[624 final source/test hashes](raw/final/source-hashes.json) match the current local candidate and the
Linux test source. The first manifest remains alongside its original result. Ruff format/check pass for
the seven changed Python files. Synthetic artifacts are redacted and hashed in the
[artifact index](raw/artifact-index.json); machine configuration and original private logs stay outside Git.

## Remaining work

This is the explicit single-member-request path through organization. A physical request containing
multiple tasks still needs a backend protocol for complete/partial member results and physical-request
retirement. Preparation tensors and aggregate method/output memory, multiple live sessions, cross-row
calibration, optimized policies, and PG asynchronous consumption remain unimplemented here.

The future PG producer must determine which inputs may be evaluated and submitted ahead of consumption.
A finite window limits extra work, but does not by itself establish correct LIMIT, volatile expression,
error timing, snapshot or cancellation semantics. Output order is a consumer responsibility; batch and
completion order are not SQL row order. Old runners remain until migrated to this same core and checked.
