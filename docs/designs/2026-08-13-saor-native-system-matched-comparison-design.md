# SAOR Native-System Matched Comparison Design

Date: 2026-08-13
Status: approved for implementation; GPU execution deferred while the server is offline

## 1. Goal and decision

Build the smallest fail-closed evidence path that can answer two deliberately
separated questions:

> Under the same two-Job database AI workload and physical service envelope,
> how do the complete Daft Native, Daft Ray, Ray Data, Project frozen-static,
> and Project SAOR systems behave empirically?

> Under the same Job-level arrival regime used by that system comparison, does
> Project bounded-ready SAOR still occupy a useful trade-off relative to
> Project bounded-ready FIFO, DRR, and VTC-style controls?

This is a **baseline and complete-system remeasurement**, not another SAOR
selector search. The bounded-ready implementation, event-ledger false-negative
fix, matched-observation selector rehearsal, and observation bridge are already
complete. The next code change must not add a new selector, scan a debt cap,
enable dynamic K, add reservation, or expand to four Jobs.

The selected architecture is:

1. keep the native and Project execution paths separate so each scheduler owner
   remains genuine;
2. add one thin matrix orchestrator that owns only the balanced cell order,
   service-idle barriers, immutable contract, and evidence locations;
3. add one independent readiness/audit and summary layer that rejects mismatched
   inputs or timing semantics before producing two separately labelled tables:
   system comparison and Project-internal selector sanity.

## 2. Why the historical numbers cannot be joined

The existing Project selector rehearsal and historical native multi-Job matrix
answer different questions:

| Difference | Project selector rehearsal | Historical native matrix | Consequence |
|---|---|---|---|
| source | PostgreSQL -> Daft with request-manifest guard | JSONL is read before the official graph | source timing is not matched |
| arrival | per-request arrival replay inside each Job | Job-level stagger, eager rows inside each Job | arrival semantics are not matched |
| timing | profiler operator/request/service stages | shard/Job barrier around the native graph | request P99 is not jointly observable |
| scheduler | Project credit + bounded-ready + selector | Daft/Ray Data owns graph scheduling | correct ownership differs by design |
| metrics | request P99/SLO and completion ledger | primarily Job/group JCT and service/resource deltas | old rows cannot be put in one complete table |

Matching only manifest SHA values is therefore insufficient. The old native
results remain valid native-interference observations, but they are not the
system-level SAOR matched comparison.

## 3. Frozen comparison contract

### 3.1 Arms and scheduler ownership

| Arm | Scheduler owner | Allowed controls | Prohibited controls |
|---|---|---|---|
| Daft `prompt()` Native | Daft native runner | preregistered Daft-native calibration only | Project K/W, credit, coordinator, bounded-ready |
| Daft `prompt()` Ray | Daft Ray runner | preregistered Daft-Ray calibration only | Project K/W, credit, coordinator, bounded-ready |
| Ray Data native graph | Ray Data streaming executor | preregistered batch size and actor concurrency | Project K/W, credit, coordinator, bounded-ready |
| Project frozen-static | Project | frozen static K/W and existing Project pipeline | bounded-ready or dynamic capacity |
| Project SAOR | Project | the same maximum K/W as frozen-static and `0.125W_e` guarded debt | larger envelope, online tuning, dynamic K |

The orchestrator may launch, stop, and observe cells. It may not translate
Project scheduler flags into native-arm options.

The executable matrix has two reporting blocks:

| Block | Arms | Role |
|---|---|---|
| system comparison | Daft Native, Daft Ray, Ray Data, Project frozen-static, Project bounded-ready SAOR | complete-system empirical comparison across scheduler owners |
| Project selector sanity | Project bounded-ready FIFO, DRR, VTC-style, and SAOR | same-regime internal control; never called native baseline |

SAOR is one physical arm reused by both reports, not two differently configured
SAOR variants. Strict priority remains an upper-bound diagnostic from the prior
rehearsal and is not required in this minimal rerun. The internal block is a
short one- or two-repeat development sanity check, not selector formal and not a
new parameter search. It is required because the common native comparison uses
Job-level eager arrivals, whereas the earlier matched-ready selector rehearsal
used per-request arrival replay; selector ordering is allowed to be
arrival-regime dependent.

The phase schedule makes that reuse literal: warm-up runs all eight unique arm
identities once; formal runs only the five complete-system arms for three
repeats; selector-sanity development runs only FIFO/DRR/VTC-style for one or two
repeats. Its SAOR rows are the first matching formal SAOR repeats, identified by
the same physical run IDs. Development repeats therefore cannot exceed formal
repeats, and no executed cell exists solely to be discarded by the reports.

`bounded-ready FIFO` does **not** mean that bounded-ready is an intrinsic part
of the FIFO algorithm. A selector decision can be written as

```text
selected request = arg min over visible ready set R(t) of selector score S(r)
```

Single-head FIFO and bounded-ready SAOR change both `R(t)` and `S(r)`, so their
difference cannot identify a selector effect. The matched-control therefore
gives global FIFO the same Project-owned `R(t)` as DRR, VTC-style, and SAOR and
changes only `S(r)`. Its full report name is
`Project bounded-ready + global FIFO matched-control`, not native FIFO and not
a claim that FIFO requires bounded-ready.

The three distinct causal comparisons are retained explicitly:

| Comparison | Effect isolated | Current evidence |
|---|---|---|
| frozen-static -> single-head + shared FIFO | shared capacity/borrowing | prior observation bridge |
| single-head + shared FIFO -> bounded-ready + global FIFO | ready-set exposure | prior observation bridge |
| bounded-ready FIFO/DRR/VTC-style -> bounded-ready SAOR | selector score/order | prior matched-ready rehearsal plus the new same-regime sanity block |

The new short sanity block does not replace the first two bridge comparisons;
it only checks whether the third comparison changes under Job-level eager
arrivals.

### 3.2 Workload and arrival semantics

All arms use the same two immutable manifests, document IDs, endpoint pinning,
prompt/output limits, model, protocol, and quality contract. The arrival shape
is frozen as:

```text
bulk Job starts at t = 0 s
foreground Job starts at t = 5 s
all rows inside a Job are eager at that Job's release time
```

This Job-level stagger is the strongest common native contract. Daft and Ray
Data do not expose a faithful per-request timed-replay interface without an
external feeder taking over part of their scheduling. The Project arms must
therefore use the same eager-within-Job shape in this system comparison.
The prior per-request-arrival selector rehearsal remains the mechanism-level
causal result and is not silently reinterpreted as this system comparison.

The manifests must still contain the complete source-row hashes and endpoint
assignment. Arrival timestamps inside those manifests are ignored for this
matrix except as audited metadata; the matrix config owns the two Job release
times explicitly.

### 3.3 PostgreSQL source and timing boundary

Every arm must prove that its rows originate from the same PostgreSQL workload:

1. before a cell starts, the runner verifies the manifest against PostgreSQL by
   document ID and source-row hash;
2. the timed interval begins immediately before the arm starts its PostgreSQL
   scan/materialization for the released Job;
3. it ends when all expected outputs for both Jobs are gathered and validated;
4. the performance matrix uses `database_operator_e2e_s` derived from this
   common boundary.

For native graphs, PostgreSQL rows may be materialized into an in-memory table
or framework-native DataFrame before the official AI call only if that
materialization is inside the timed interval. Pre-reading JSONL before the cell
is a compatibility/diagnostic path and is not rankable in this matrix.

The initial performance matrix uses `writeback_mode=none` for every arm. A
separate exactly-once PostgreSQL sink gate may be run at small scale, but sink
time must not be present for only a subset of performance arms.

### 3.4 Shared physical and service contract

All five arms freeze:

- the same two GPUs, CPU allocation, Ray cluster, endpoint set, and host;
- unmodified vLLM with explicit FCFS scheduling, the same model/revision,
  dtype, max sequences, max batched tokens, chunked-prefill, prefix-cache,
  eager/compile mode, and GPU-memory-utilization values;
- Chat Completions, raw user prompt, temperature 0, fixed output cap, no retry,
  and the same request timeout;
- one warm-up plus three balanced/interleaved formal repeats when a future
  formal run is authorized;
- an empty-service barrier before and after every cell, one host runner lease,
  immutable config and repository commit, and failure evidence for every cell.

Native arms retain their own independently preregistered calibration. Matching
physical resources does not mean forcing the same Project K/W values into a
framework whose public scheduler has different control variables.

## 4. Architecture and interfaces

### 4.1 Static contract and readiness audit

A dedicated contract module loads one schema-versioned matrix configuration and
produces immutable arm specifications. It must fail closed on:

- a missing or duplicated required arm in either reporting block;
- a Project option appearing in a native arm;
- different manifests, Job offsets, endpoint mapping, protocol, output cap, or
  service signature across arms;
- Project frozen-static, bounded-ready FIFO/DRR/VTC-style, and SAOR using
  different K/W, actor topology, organizer, source, or non-selector arguments;
- SAOR not using bounded concrete pre-registration or not using exactly
  `debt_cap_fractions=[0.125,null]` for bulk/foreground;
- native paths using pre-timed JSONL materialization for a rankable cell;
- missing PostgreSQL verification, source timing, resource instrumentation, or
  scheduler provenance fields;
- an output root that already exists.

Readiness is read-only: it resolves environment variables, checks files and
calibration signatures, and writes an audit report, but it does not contact the
model or start Ray jobs.

### 4.2 Thin global orchestrator

The orchestrator creates a deterministic balanced order within each phase's
eligible arm set while the static contract still contains eight unique arm
identities. The matrix index records phase, block membership, and physical arm
identity so each formal SAOR execution can feed both summaries without being
rerun under a different configuration. A required fresh `matrix_output_root`
owns the index, host lease, matrix state, and every cell directory; paths beside
the checked-in config are never used as mutable run state.
For each cell it:

1. waits for service idle and obtains the host runner lease;
2. verifies the PostgreSQL-backed manifest contract;
3. dispatches the arm to its existing native or Project executor;
4. captures the common cell boundary, vLLM counters/gauges, GPU resources,
   command provenance, and output paths;
5. waits for final idle, validates completeness, and atomically updates the
   matrix index;
6. stops after the first invalid cell while preserving the failure artifact.

The orchestrator does not implement batching, admission, routing, request
credit, ready queues, or selector logic.

### 4.3 Execution adapters

Native adapters extend the existing official framework paths so PostgreSQL
materialization occurs within the measured Job lifecycle. Daft continues to
execute `daft.functions.prompt`; Ray Data continues to execute its official
HTTP processor graph. Daft Native and Daft Ray remain distinct arms.

Project frozen-static and SAOR reuse the shared-vLLM profiler/runner. Their
contract differs only in static partition versus bounded-ready guarded debt;
all maximum envelope and pipeline arguments remain identical.

Each adapter returns a neutral cell record containing identity, scheduler
provenance, common timestamps, Job completion evidence, service-token deltas,
resource paths, and metric availability. It does not fabricate unsupported
request-level timing.

### 4.4 Independent summarizer

The summarizer consumes completed matrix evidence, not live services. It
revalidates the contract and emits:

- `all_runs.csv`: every warm-up/formal/failed cell and its order;
- `formal_summary.csv`: means, all repeat values, sample CV, and availability;
- `system_summary.csv`: only the three native systems, Project frozen-static,
  and Project bounded-ready SAOR;
- `project_selector_sanity.csv`: only bounded-ready FIFO, DRR, VTC-style, and
  SAOR, explicitly labelled development/internal;
- `job_summary.csv`: bulk/foreground JCT, overlap, completion order, and
  single-to-multi slowdown when matched single controls exist;
- `resource_summary.csv`: GPU utilization, power/energy, MFU, vLLM
  running/waiting/KV and service-token deltas;
- `validation.json`: exact contract checks, excluded metrics, failures, and
  claim boundary.

The summarizer reports observations. It never returns `winner`, never authorizes
SAOR selector formal, and never attributes a complete-system difference solely
to guarded debt. A system-level improvement accompanied by no SAOR advantage
over the internal bounded-ready controls is reported as a Project pipeline or
ready-exposure result, not a selector result.

## 5. Metrics and missing-data rules

| Metric | Cross-system status | Rule |
|---|---|---|
| correct service tokens/s | required | vLLM prompt+generation counter delta / common cell wall |
| database operator E2E / group JCT | required | common PostgreSQL-source-to-gather boundary |
| bulk and foreground Job JCT | required | Job release to validated completion |
| actual overlap | required | must be positive for the concurrent cell |
| GPU utilization, power/energy, MFU | required | during-cell time series only |
| vLLM running/waiting/KV | required | during-cell aggregation only |
| exactly-once/output validity | required | document-set equality, no duplicate/missing output |
| request P50/P95/P99 and SLO | conditional | only if the arm exposes genuine per-request timestamps on the common clock |
| Project completion lag/no-service | Project-only diagnostic | never fill native rows with zero |
| native internal queue/actor metrics | native-only diagnostic | report by arm; do not require Project analogues |

Unsupported metrics are the literal state `unavailable`, with a reason. Empty,
zero, shard-completion replication, or inferred timestamps are prohibited
substitutes.

## 6. Failure and claim boundaries

The matrix fails closed when any formal cell has a contract mismatch, incomplete
output, non-empty final service queue, missing resource trace, invalid
provenance, non-positive overlap, or a service counter that cannot be attributed
to the cell. Failed cells remain in the index and are never dropped from the
report.

Even a clean matrix can claim only the empirical performance of five complete
systems plus a same-regime Project-internal development sanity result under this
frozen two-Job contract. It cannot claim:

- that SAOR's selector beat FIFO/DRR/VTC-style;
- that bounded-ready belongs to Daft or Ray Data;
- a vLLM-internal fairness theorem or token-level service bound;
- generalization to four Jobs, another machine, images, reservation, or dynamic
  K;
- request-tail superiority where a native arm exposes only Job barriers.

## 7. Test and execution sequence

Implementation follows TDD and stops locally after infrastructure verification:

1. contract-loader tests for both reporting blocks, scheduler ownership,
   source/timing, service signature, two-Job offsets, Project K/W matching,
   physical SAOR-arm reuse, and existing-root rejection;
2. command tests proving native commands contain no Project scheduling flags and
   Project arms differ only at the intended policy boundary;
3. fake-executor orchestration tests for balanced order, idle barriers, atomic
   failure retention, resource evidence, and runner-lease cleanup;
4. summarizer tests for unavailable request tails, counter-derived throughput,
   positive overlap, exactly-once, repeats/CV, and fail-closed missing data;
5. compile and affected-unit-suite verification;
6. static readiness against the example config.

No long GPU experiment is part of this implementation batch. Once the server is
available, execution must first follow the runtime/AutoDL preflight, then run a
small correctness/rehearsal gate. Formal 1+3 repeats require a separate explicit
decision after the gate artifacts are reviewed.

## 8. Documentation impact

The implementation must update the code/script README, learning walkthrough,
SAOR plan/status entry, infrastructure status, project index, and project log.
It must not sync Wiki or cloud documents, per the user's explicit instruction.
