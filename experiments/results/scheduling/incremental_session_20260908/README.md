# Single-flow incremental execution verification

This records the initial controlled-core snapshot. Later bounded async transport and real-model checks
are recorded [separately](../incremental_real_20260908/README.md).

## Purpose and source

This is an engineering correctness check for the common execution context used by data organization
and scheduling research. Baseline: `2d2dee35`; implementation branch: `codex/incremental-session`.
The [detailed design](../../../plans/semloom_incremental_session_design.md) owns the interface and the
future data-organization integration. No model-quality, throughput, fairness or GPU-memory claim is made.

SessionEngine/SchedulingSession now accept an explicit prefix of immutable tasks, advance without
waiting for producer or backend work, issue result leases, and retain uncertain remote reservations
across close/open. One bounded live-record table accounts for input, result and compute ownership.
READY results follow completion order; consumers release leases rather than silently returning capacity
at model completion. Every advance makes at most one bounded poll; terminal, dispatch and delivery
state actions share a configured budget. Table scans are bounded by the held-task limit.

Existing admission and endpoint routing policies are called through a typed adapter. FIFO task choice
is a replaceable function over immutable candidates; it cannot mutate the ledger or dispatch an
unaccepted identity. Local FIFO credit without history events is qualified; its new explicit retirement
operation removes completed Job bookkeeping after all outstanding responsibility drains. Legacy
finish_job keeps its historical behavior. The synchronous scheduler and its whole-result collector
still have runtime consumers and were not deleted or silently replaced.

## Setup and methods

All new execution tests use a deterministic nonblocking fixture backend and an injected clock. Source
was synchronized to a fresh data-disk Git worktree on the authorized server. Existing runtime env and
core preflight were checked; driver dependencies were reused, without installation or model download.
The unchanged baseline and candidate scheduling suites include existing CPU Ray-adapter tests. No
PostgreSQL cluster, model server or real-model request was started by this verification.

```bash
# Clear inherited PYTHONPATH and RAY_ADDRESS before running these commands.
python -m unittest discover -s code/tests/scheduling -t code -p 'test_*.py'
python -m unittest discover -s code/tests/scheduling -t code -p 'test_incremental_session.py'
python -m ruff check code/src/scheduling/core/session*.py \
  code/src/scheduling/submission_control/shared_credit.py \
  code/tests/scheduling/test_incremental_session.py
```

The baseline is the existing synchronous execution path. A direct comparison uses its existing fake
adapter and the new controlled backend to check normal ordered identities, invocation count, maximum
inflight and round-robin endpoint decisions. This is not equivalence for every dynamic policy or timing
metric. New UNKNOWN-submit handling deliberately retains capacity; old blocking run remains unchanged.

## Results and evidence

| Check | Result |
|---|---|
| Local baseline attempt | 277 discovered tests, one import error because pyarrow is absent; retained in raw/local-baseline.log |
| Linux unchanged baseline | 281/281 pass |
| Linux candidate 1 | 310/310 pass |
| Linux candidate 2 | 312/312 pass, adds binary identity and delivery-commit rollback |
| Linux final candidate | 314/314 pass; [summary](raw/summary.json), [log](raw/scheduling-r3.log) |
| Local final new-core tests | 33/33 pass; [log](raw/local-final.log) |
| Code checks | Pass; [log](raw/ruff-r3.log) |
| Source identity and cleanup | 616 files match local bytes; zero Raylet PIDs; [hashes](raw/source-hashes.json), [summary](raw/summary.json) |

Earlier attempts are preserved; the missing local dependency was resolved by using the existing Linux
driver environment, not by skipping or weakening its tests. Remote original logs and source archives
remain in private task artifacts. Public copies are redacted, ANSI control sequences removed and trailing
whitespace normalized, with published SHA-256 values in raw/artifact-index.json.

Tests cover prefix transfer, whole-batch rejection, reverse completion, duplicate identities/results,
input/result/compute budgets, NOT_ACCEPTED deadlines, UNKNOWN submit, cancellation and release,
late completion across sessions, generation-based wakeup, finite poll deadlines, one-action progression,
sink failure, returned and unreturned leases, partial delivery-commit rollback, reentrancy, immutable
binary payload/work identity, selector replacement and invalid selections. A 1000-task stream checks
that no completed task record remains after release; 100 consecutive Jobs check retirement of local
FIFO history. These are bounded-state assertions, not a process RSS or real-backend buffer measurement.

## Interpretation and remaining work

This implements a single-flow execution context, not a second scheduler or an independent external
submitter. Data organization should operate inside this context and share its ownership/capacity model.
The first backend submission has one member; multi-member submissions must add explicit membership,
one compute lease per physical submission, per-task result association and partial-failure handling.
StageBlockDescriptor/BoundedStageBroker are existing assets for that work; they are not yet connected.

Real nonblocking backends, staged preparation buffers, multi-task submission, dynamic/remote credit,
full legacy-driver migration, multi-Job fairness and PG async/wire integration remain pending. Passing
these tests does not expand the prior PostgreSQL or real-model evidence. No active legacy implementation
was removed merely because a new interface exists.
