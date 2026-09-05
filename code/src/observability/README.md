# Observability cores

This package owns passive measurement and stable profiling schemas. Measurement code must not take
over the execution policy it observes.

- `request_gateway.py` is the five-arm Job-labelled HTTP pass-through. It forwards each body exactly
  once with no admission limit, retry, cache, route choice, or payload rewrite, then records lifecycle
  clocks and endpoint-reported token usage for cross-framework tail/fairness metrics.
- `metrics/` contains reusable metric collectors and summaries.
- `profiling/` contains the PostgreSQL AI operator profiler's stable result schema and helpers.

The gateway is an observation boundary, not a baseline executor or scheduler. Any future queue,
backpressure, batching, or retry belongs in the measured system and must not be added here.

`process_resources/` provides Linux FD identity observations, explicit missing values, sampling windows,
stable baselines, operation/error capture and gzip JSONL persistence. A tick is a sequential observation
batch, not an atomic snapshot. PostgreSQL session attribution and threshold policies live under
`src/experiments/postgresql/`; the collector makes no qualification decision.
