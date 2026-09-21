# Accelerated Arrival Replay Design

## Goal

Run short, reproducible single-GPU flush experiments from the existing
BurstGPT-derived workload without changing stored timestamps or claiming that
the accelerated request rate is the original production rate.

The imported 1024-row trace spans 52,184 seconds. The first 512 rows span
39,757 seconds, so literal replay is unsuitable for an iterative local
experiment.

## Selected Approach

Add one explicit CLI parameter:

```text
--arrival-time-scale FLOAT
```

The default is `1.0`. Values must be finite and greater than zero. Replay
normalizes the first raw arrival to zero and schedules every later row at:

```text
scaled_offset = (raw_arrival_s - first_raw_arrival_s) * arrival_time_scale
```

The database keeps the original BurstGPT timestamps. Raw arrival metadata in
the request remains unchanged; only the replay clock offset is scaled.
`arrival_time_scale` is recorded in dry-run output, the main run CSV, flush
trace metadata, and the experiment manifest.

## Alternatives Rejected

1. Rewrite timestamps during import. This destroys the reusable raw workload
   and obscures which experiments used accelerated time.
2. Cap long gaps. This changes the inter-arrival distribution nonlinearly and
   can manufacture queue bursts.
3. Select only the densest trace window. This is useful for a later controlled
   burst experiment but introduces selection bias into the first comparison.

## Runtime and Error Semantics

- Scaling is active only with `--arrival-replay`.
- Offline paths and replay with scale `1.0` remain unchanged.
- Missing, decreasing, negative, or non-finite raw arrivals remain errors.
- Zero, negative, or non-finite scale values fail before database or model
  execution.
- Fixed timeout and hard maximum wait remain wall-clock quantities; they are
  not scaled.
- All strategies in one comparison use the same rows and scale.

## Verification

Tests must prove:

- scale `1.0` preserves existing deadlines;
- scale `0.001` maps a raw 100-second gap to 0.1 seconds;
- invalid scale values are rejected;
- dry-run and result rows record the resolved scale;
- real Daft-to-Ray contract still executes every row exactly once.

The full Python test suite, CLI help, compile check, and real local Ray
contract must pass before GPU execution.

## Experiment Sequence

### Fast Gate

- Workload: first 64 `sharegpt_burstgpt` rows in arrival order.
- Time scale: `0.0001`.
- Policies: immediate, fixed timeout, queue-adaptive.
- Common configuration: token budget 6144, static K_max 8, real PostgreSQL,
  Daft, Ray, and vLLM.
- Repeats: one measured run per policy. This gate only checks that run,
  request/submission, flush/control, resource, and manifest artifacts are
  non-empty and internally consistent.

### Formal Comparison

- Workload: first 512 `sharegpt_burstgpt` rows in arrival order.
- Time scale: `0.0005`; the replay span is about 19.9 seconds.
- Policies: immediate, fixed timeout, queue-adaptive.
- Common configuration: token budget 6144, static K_max 8, identical model
  endpoint and completion settings.
- Repeats: one warm-up plus five measured repeats per policy.
- No fake model backend and no writeback in the scheduling comparison.

The report includes throughput in rows/s and tokens/s, end-to-end and service
latency percentiles, flush reasons and batch sizes, queue/running/KV and GPU
time series, controller decisions, request completion counts, and
mean/standard deviation/95% confidence intervals across measured repeats.

## Claim Boundary

The result compares scheduling strategies under one controlled accelerated
BurstGPT-derived workload on one local GPU. It does not estimate the absolute
production traffic rate, prove multi-GPU scaling, or represent the internal
PostgreSQL 18.3 platform.
