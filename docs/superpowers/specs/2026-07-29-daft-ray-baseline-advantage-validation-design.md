# Daft+Ray Baseline Advantage Validation Design

Date: 2026-07-29

Status: approved in discussion. The user approved the pre-registered success
thresholds before implementation.

## 1. Goal

This experiment must determine where the project Daft+Ray execution path has a
measurable advantage over independently calibrated direct and official-runtime
baselines. It must not search for a favorable metric after seeing the results.

The comparison answers four ordered questions:

1. What is the serving ceiling of the unchanged dual-vLLM deployment?
2. What overhead does Daft+Ray add in a saturated single-job steady state?
3. Can Daft+Ray reach the same ceiling with less upstream pressure or a faster
   transient ramp?
4. Under shared 1/2/4-job contention, can endpoint-shared work credit improve
   tail latency, SLO compliance, or fairness without materially reducing
   aggregate throughput?

The design does not claim that Ray or Daft makes vLLM kernels execute faster.
Its candidate advantages are pressure efficiency, transient saturation,
multi-endpoint coordination, and multi-job service control.

## 2. Evidence that changes the immediate priority

The 512-row project calibration completed 9/9 cells with correct request
identity and empty final queues, but it is not a valid formal comparison.
The theoretically equivalent nonbinding active-work arm and static K256 arm
reported about 4.15K and 11.74K total tokens/s respectively.

Read-only trace diagnosis ruled out active-work backpressure, actor count,
payload drift, output-work drift, and summary arithmetic as the main cause.
The slow arm spent about 28.6 additional seconds inside the HTTP/vLLM request
wall. It was also the first full-concurrency cell. Its two endpoints admitted
requests in waves and reached a much lower aggregate running-request peak than
the later K256 cell.

An actor-ready barrier is still required for measurement hygiene, but actor
creation explains only about three seconds and does not explain the main gap.
The leading hypothesis is a first-touch high-concurrency cold path in the HTTP
client, operating system connection path, vLLM HTTP ingress, or service state.
Therefore no full formal matrix may start until an equivalence gate separates
cold-path effects from policy effects.

## 3. Alternatives considered

### 3.1 One exhaustive matrix

Running all systems, pressure levels, scales, and job counts immediately would
produce many numbers but would preserve the current measurement confound. It
would also make a failed cell difficult to attribute. This option is rejected.

### 3.2 Jump directly to multi-job

Multi-job contention is the most likely place to demonstrate the value of
shared Ray actors and endpoint-shared credit. However, skipping the single-job
overhead and cold-path audit would leave open whether any fairness benefit is
paid for by an avoidable client/runtime defect. This option is retained only
as a later stage.

### 3.3 Staged causal ladder

This is the selected design. Each stage has a gate and freezes its chosen
configuration before the next stage starts:

1. measurement equivalence;
2. single-job steady state and pressure curve;
3. small-job transient and 1-to-2-GPU scaling;
4. shared 1/2/4-job contention;
5. secondary database-product and semantic-system references.

## 4. Baseline roles

All primary numeric arms use the same immutable Chat Completions manifest, two
independent one-GPU vLLM endpoints, model flags, endpoint assignment, output
cap, and service-token counters.

| Arm | Role |
|---|---|
| vLLM Bench | serving ceiling, not a database operator |
| bounded AsyncIO HTTP | strongest no-Daft/no-Ray causal baseline |
| Daft Native `prompt()` | official Daft execution baseline |
| Daft Ray `prompt()` | official Daft distributed runtime baseline |
| Ray Data HTTP Processor | official Ray external-inference baseline |
| project static request admission | framework-cost control |
| project token-work/refill | proposed single-job execution policy |
| project shared work credit/fair queue | proposed multi-job policy |

An OceanBase-style lightweight arm may be implemented as an explicitly labeled
set-oriented database-operator emulation. It must not be called the official
OceanBase implementation. The official OceanBase adapter remains a capability
gate and becomes numeric only when the product function can target the same
local vLLM service with auditable semantics. pgai remains an embedding/vector
baseline unless it exposes an equivalent generative operator.

## 5. Measurement contract

### 5.1 Actor readiness

Every Ray actor exposes a side-effect-free ready method. The driver waits for
all actor-ready references before starting the measured end-to-end timer.
Ready-barrier duration is recorded separately and is never silently included
in steady-state JCT.

### 5.2 Same-pressure warm-up

Each formal configuration receives one warm-up at the same concurrency or
active-work limit. A low-pressure warm-up does not qualify for a full-pressure
formal cell. Warm-ups use the same manifest semantics but are excluded from
formal aggregates.

The first equivalence gate contains only two theoretically equivalent arms:
static K256 and nonbinding work98K with K256. It runs one warm-up plus three
interleaved formal repeats per arm. Their only intended difference is the
presence of a nonbinding active-work check.

### 5.3 HTTP timing

The non-streaming Chat request records monotonic local intervals and epoch
markers for:

- actor method entry;
- HTTP request start;
- response headers available;
- response body fully read;
- actor result publication.

The headers-wait interval contains connection establishment, server ingress,
vLLM queueing, and inference; it does not identify server accept time. The
body-read interval covers bytes after headers. Streaming is not enabled because
it would change request semantics. Claims must respect this observability
boundary.

### 5.4 Validity

Every repeat requires:

- immutable manifest SHA and exact source-row validation;
- exactly-once completion and zero worker failure;
- both endpoints used with predeclared work skew;
- positive and uncontaminated service-token deltas;
- actual output-work drift no greater than 1%;
- final vLLM running and waiting queues equal to zero;
- trace coverage for every request and submission.

## 6. Experiment stages

### Stage A: measurement-equivalence gate

Compare static K256 with K256 plus a nonbinding 98,304-work limit. The gate
passes only when repeat-mean throughput and JCT differ by no more than 5%, at
least two of three repeat differences lie within that band, and no validity
guard fails. A run-order trend is reported even if the aggregate passes.

If the gate fails, stop. Use the new HTTP timing and endpoint traces to isolate
client/OS ingress versus vLLM service behavior. Do not launch the broad matrix.

### Stage B: single-job capacity and framework cost

Calibrate every primary arm independently. Select the smallest safe pressure
that reaches at least 97% of that arm's maximum observed throughput. Then run
one warm-up and three seeded, interleaved formal repeats on a disjoint held-out
manifest.

Primary metrics are service total/generation tokens/s, JCT, request P95/P99,
MFU, vLLM running/waiting, active work, client pressure, and HTTP timing.

### Stage C: transient saturation and scale

Run 32/64/128/256-row jobs separately. Report time-to-95%-ceiling, ramp regret,
JCT, and the minimum request/work pressure that reaches the ceiling. This
directly tests whether an underfilled approximately 15-second job can be
reorganized and submitted quickly enough to approach a 5-second completion.

Repeat matched work on one endpoint and two endpoints. Report speedup and
scaling efficiency, not only aggregate dual-GPU throughput.

### Stage D: shared 1/2/4-job contention

Use the same frozen per-job manifests and endpoints. Compare:

1. independently calibrated bounded HTTP jobs;
2. independent project schedulers without shared credit;
3. endpoint-shared static request/work credit;
4. work-conserving weighted fair credit.

Include simultaneous starts, staggered arrival with idle borrowing, and at
least one asymmetric foreground/background workload. Report aggregate
throughput, per-job JCT/P99/SLO, Jain fairness over completed token work,
weighted service share, starvation, and idle-capacity borrowing.

### Stage E: secondary system references

Only after the core causal matrix is valid:

- run the official OceanBase product arm if its capability gate passes;
- otherwise run the labeled OceanBase-style lightweight emulation;
- compare pgai on its equivalent embedding path;
- retain LOTUS/Palimpzest as system-level semantic planning references unless
  call count, quality, and work are made equivalent.

## 7. Pre-registered promotion rules

The project is promoted for a single-job throughput claim only when its
steady-state service throughput is at least 95% of independently calibrated
bounded HTTP and it improves another predeclared outcome.

A pressure-efficiency claim requires the same at-least-97%-of-ceiling
throughput with at least 20% less active work or inflight pressure.

A transient claim requires at least 20% lower time-to-ceiling or ramp regret,
with P99 no more than 5% worse.

A multi-job claim requires aggregate throughput at least 95% of the calibrated
naive baseline and at least 10% improvement in P99, SLO violation ratio, or
Jain/weighted fairness, with no starvation and no correctness regression.

At least two of three formal repeats must agree in direction. Results that do
not pass these thresholds are recorded as negative or equivalent findings.
The experiment must not rename lower pressure, lower work, or a selected
subgroup as an acceleration after seeing the data.

## 8. Handoff and stop conditions

The repository contains all templates and commands needed by a new remote
agent. The remote runbook remains the authority for startup, lease, endpoint,
Ray-address, output-directory, evidence-preservation, and cleanup rules.

The next remote agent must:

1. perform read-only runner/lease/endpoint/Ray/GPU/git preflight;
2. use the pushed commit in an idle checkout or dedicated worktree;
3. create a fresh output directory;
4. run only the Stage A gate;
5. stop and preserve evidence if Stage A fails;
6. start broader calibration only after the gate passes.

No formal matrix is started merely because all adapters can execute.
