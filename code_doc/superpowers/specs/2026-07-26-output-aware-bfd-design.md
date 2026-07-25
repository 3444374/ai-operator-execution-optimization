# Output-Aware Cost and Deterministic BFD Design

## 1. Goal

Add output-aware request costing and deterministic Best-Fit Decreasing (BFD)
packing to the existing
`PostgreSQL -> Daft -> Arrow -> Ray task/actor -> vLLM` pipeline.

The implementation must support two separate research questions:

1. How do different output-cost signals change token-budget data organization?
2. Does global BFD improve packing over sequential token-budget batching?

It must not modify Daft, Ray, or vLLM internals, split a prompt, change
admission control, or describe trace-derived output lengths as online
predictions.

## 2. Approved Semantic Boundaries

### 2.1 Offline and online behavior

True global BFD is an offline data-organization strategy because it must see
and reorder the complete organizer input.

- Offline organization may use global BFD.
- Arrival replay preserves arrival order and never uses global BFD.
- Arrival replay may use the same output-aware cost calculation with the
  existing sequential pending-batch builder.
- Window-local online packing is a possible later strategy, not part of this
  design.

### 2.2 Output cost is not the generation limit

The organizer cost estimate and the model request limit are distinct:

- `completion_max_tokens` controls the real vLLM generation limit.
- `output_cost_mode` controls the estimated output contribution used for
  upstream batching.

Changing the cost mode must not silently change the backend request.

### 2.3 Trace target is controlled trace metadata

`trace_target_output` uses the workload row's `target_output_tokens`. It is an
offline trace-assisted cost signal. It is not an online deployable estimator
and must be labelled as such in CSVs and reports.

The deployable baseline remains `fixed_output_cap`. A learned estimator is a
later sub-project and will be added only after a real trained implementation
exists.

The current `sharegpt_burstgpt` importer pairs independent sources with
`zip(prompts, traces)`: the prompt comes from ShareGPT, while
`target_output_tokens` comes from the corresponding BurstGPT trace position.
Therefore this field is not the actual or oracle Qwen output length for that
prompt. The current trace mode may support:

- deterministic cost-path and packing validation;
- sensitivity analysis under a known heterogeneous cost signal;
- replay studies that intentionally preserve BurstGPT arrival/output metadata.

It must not support a claim that output length was accurately predicted, that
the BFD cost matches realized GPU work, or that it is an upper bound for the
current Qwen workload.

### 2.4 True offline-oracle evidence is a separate gate

A future oracle comparison requires output lengths measured for the same
prompt, model, tokenizer, generation parameters, and stopping behavior used in
the evaluated run. Those labels must be collected in a calibration run and
replayed only on a disjoint evaluation run. Model-service aggregate usage must
not be divided across prompts and presented as per-request truth.

This design deliberately does not add an oracle mode before those labels
exist. GPU performance results from `trace_target_output` remain
trace-cost sensitivity evidence, not output-estimation evidence.

## 3. Considered Approaches

### 3.1 Shared scalar cost resolver and independent packing core

Use one small engine-independent module for output-cost resolution and one
small engine-independent module for BFD. Arrow and Daft adapt their rows to the
packing core; arrival replay reuses only the cost resolver.

This approach is selected because two real consumers need identical cost
semantics, while only offline organization needs BFD.

### 3.2 Put all logic in `organizers.py`

This minimizes file count but requires arrival replay to duplicate output-cost
selection. The two execution modes could then drift silently. This approach is
rejected.

### 3.3 General estimator/plugin framework

A general cost protocol could support multimodal and learned estimators, but
there is no second real modality or trained estimator yet. This approach is
rejected as premature abstraction.

## 4. Components

### 4.1 Request cost resolution

Create `code/src/request_costs.py` with the following interface:

```text
OutputCostMode = Literal[
    "prompt_only",
    "fixed_output_cap",
    "trace_target_output",
]


resolve_output_tokens(
    mode: OutputCostMode,
    *,
    completion_max_tokens: int,
    target_output_tokens: object,
) -> int
```

Behavior:

| Mode | Output contribution | Recorded source |
|---|---|---|
| `prompt_only` | `0` | `configured_zero` |
| `fixed_output_cap` | non-negative `completion_max_tokens` | `backend_completion_cap` |
| `trace_target_output` | required non-negative integer `target_output_tokens` | `burstgpt_unpaired_trace_metadata` for the current workload |

Unknown modes, negative caps, missing trace targets, booleans, non-integers,
and negative trace targets fail with a clear `ValueError`. The resolver does
not import Arrow, Daft, Ray, or vLLM.

The complete row cost remains:

```text
prompt_tokens + resolved_output_tokens
```

Prompt tokens must also be a non-negative integer at the adapter boundary.

### 4.2 Deterministic BFD core

Create `code/src/packing.py` with the following interface:

```text
PackItem(row_index: int, stable_id: str, cost_units: int)

best_fit_decreasing(
    items: Sequence[PackItem],
    *,
    capacity: int,
    max_rows: int,
) -> tuple[tuple[int, ...], ...]
```

The result contains input `row_index` values grouped by output batch.

Algorithm:

1. Sort items by descending `cost_units`, then `stable_id`, then `row_index`.
2. For each item, consider existing batches that remain within both
   `capacity` and `max_rows`.
3. Choose the eligible batch with the least remaining capacity after
   placement.
4. Break equal remaining-capacity ties by batch creation order.
5. Create a new batch when no existing batch is eligible.
6. If a single item exceeds `capacity`, put it alone in a new oversized
   batch.

The function validates positive limits and unique non-negative row indexes. It
does not mutate input or depend on engine objects.

Required invariants:

- every input row appears exactly once;
- no prompt or row payload is split;
- every non-oversized batch respects both limits;
- every oversized batch contains exactly one row;
- identical inputs produce identical membership and order.

### 4.3 Organizer adapter

Extend `OrganizerConfig` with:

```python
output_cost_mode: OutputCostMode = "fixed_output_cap"
```

Add one batching policy:

```text
best_fit_token_budget
```

Do not create policy-name combinations for every cost mode. Batching policy
and cost mode are orthogonal.

For sequential token-budget policies, `_row_token_cost()` uses the shared cost
resolver. For `best_fit_token_budget`, the organizer:

1. validates `prompt_tokens`;
2. resolves the output contribution;
3. creates `PackItem` values using `doc_id`, or source row index when `doc_id`
   is unavailable;
4. passes the configured `token_budget` to the BFD core as `capacity`;
5. calls the shared BFD core once for that organizer input;
6. materializes each group with Arrow `take`.

ArrowOrganizer and DaftOrganizer call the same `organize_arrow_table()` path.
No second BFD implementation is permitted.

### 4.4 Arrival replay adapter

Extend `_row_arrivals()` to select output contribution through the shared cost
resolver.

Arrival replay continues to:

- iterate rows in non-decreasing arrival order;
- use `PendingBatchBuilder`;
- preserve flush-window and backpressure behavior;
- avoid all BFD sorting.

If `output_cost_mode=trace_target_output`, every replayed row must contain a
valid target. Failure occurs before emitting a partial lifecycle seed for the
invalid row.

### 4.5 Prompt, model, and modality replacement

The reusable boundary is the packer's scalar `cost_units`; it is intentionally
not named `tokens`. The BFD core must not import or inspect prompt text,
tokenizers, model IDs, image columns, frame counts, or backend configuration.

- Replacing prompts changes input rows and recomputed cost features, not the
  packer, Ray scheduler, lifecycle schema, or scenario runner.
- Replacing a model changes run configuration and the concrete cost adapter.
  Qwen model IDs, context lengths, tokenizers, and generation defaults must not
  be constants in the cost or packing core.
- Adding a real multimodal path adds a modality adapter that converts its
  observed feature, such as frames or image patches, into `cost_units`.
  Packing, routing, submission, lifecycle joins, and experiment orchestration
  continue to use the same core.
- If a future model needs multiple independent capacity constraints, the code
  must add a real multi-resource packing algorithm rather than hide
  incomparable units inside an arbitrary scalar.

No empty estimator/plugin hierarchy is introduced now. The stable
`PackItem(row_index, stable_id, cost_units)` boundary is sufficient for the
current text caller and the explicitly planned multimodal caller.

## 5. Global and Local Packing Scope

BFD is global only relative to the rows visible in one organizer call.

The run schema records:

```text
output_cost_mode
output_cost_source
packing_cost_unit
cost_model_id
cost_tokenizer_id
packing_algorithm
packing_scope
packing_budget_utilization_mean
packing_budget_utilization_p95
packing_oversized_rows
packing_input_rows
packing_batch_count
batch_estimated_cost_units_p50
batch_estimated_cost_units_p95
batch_estimated_cost_units_p99
batch_estimated_cost_units_max
```

Allowed scope labels:

| Value | Meaning |
|---|---|
| `organizer_input` | one complete organizer input was packed |
| `fetch_chunk_local` | separate database fetch chunks were packed |
| `partition_local` | separate Daft partitions were packed |
| `arrival_order` | no BFD; online order was preserved |

For a formal global-BFD claim:

- `db_fetch_rows >= total_rows`;
- the selected Daft partition configuration must not create independent
  packing domains;
- the recorded scope must be `organizer_input`.

If these conditions do not hold, execution may continue for engineering
experiments, but the output must use the appropriate local scope. Reports must
not call it global BFD.

For the current workload, `output_cost_source` is
`burstgpt_unpaired_trace_metadata`, not `oracle` or `measured_output`.
The text path records `packing_cost_unit=tokens`. Model and tokenizer
provenance fields must reflect the configuration that produced the input cost,
not merely the backend currently serving the request.

## 6. Failure and Compatibility Behavior

- Missing or invalid trace target: fail explicitly.
- Invalid prompt token count: fail explicitly.
- Unknown cost mode or packing policy: fail explicitly.
- Empty input: return no batches and zero-valued packing metrics.
- Oversized row: preserve it as a one-row batch and increment the oversized
  metric.
- Existing default behavior remains `fixed_output_cap` with the current
  sequential policy.
- Existing fixed-row policies retain membership behavior.
- Backend generation, scheduler, routing, flush, writeback, and request
  execution semantics remain unchanged.
- Existing arrival-replay lifecycle semantics remain unchanged. Offline
  organizer runs gain an explicit lifecycle origin solely to make their
  per-request metrics observable.

## 7. Observability

Existing run, request, submission, flush, control, and resource outputs remain
the source of truth.

Request rows continue to record `estimated_output_tokens`; this field is the
pre-execution value selected by `output_cost_mode`. It must not be confused
with:

- `client_estimated_output_tokens`, derived from returned text;
- `actual_output_tokens`, present only when a backend provides genuine
  per-request usage;
- `target_output_tokens`, which is unpaired BurstGPT metadata in the current
  workload.

Every request trace also records `request_time_origin`:

| Value | `arrival_epoch_s` meaning | `flush_epoch_s` meaning |
|---|---|---|
| `replayed_arrival` | scaled source arrival in the replay timeline | pending batch close time |
| `offline_job_start` | one shared job-start epoch before source fetch | organized batch ready time |

This makes offline sequential and BFD request E2E comparable from the same
job boundary. Reports must not compare either value with a trace that uses a
different time origin without stating the mismatch.

Packing evaluation additionally reports:

- batch estimated-cost-unit P50/P95/P99/max, interpreted using
  `packing_cost_unit`;
- budget utilization mean/P95;
- submission count;
- oversized row count;
- observed vLLM tokens/s;
- run E2E;
- request E2E P50/P95/P99;
- batch service P99.

## 8. Testing Strategy

Every behavior change follows RED -> GREEN.

### 8.1 Cost tests

Test all three modes and reject missing, boolean, non-integer, and negative
trace targets. Verify that changing cost mode never changes
`completion_max_tokens` passed to the backend.

### 8.2 Packing tests

Test:

- the canonical budget-10 example produces `[6,4]` and `[5,3,2]`;
- deterministic tie-breaking;
- simultaneous row and token limits;
- use of a modality-neutral `capacity`/`cost_units` core;
- oversized rows are isolated;
- empty input;
- duplicate row indexes and invalid limits;
- exactly-once membership.

### 8.3 Organizer tests

Run the same table through ArrowOrganizer and DaftOrganizer and assert equal
BFD document membership. Verify sequential policies retain existing behavior.

### 8.4 Daft-Ray contract

Pass output-aware BFD batches through the real local
Daft -> Arrow -> Ray task/actor contract. Assert:

- every source document is executed exactly once;
- no document content changes;
- request and submission IDs remain joinable;
- offline request traces use `request_time_origin=offline_job_start`;
- batch limits and oversized behavior match the pure packer.

Fake backends remain limited to unit/contract behavior; GPU results use the
real compatible vLLM endpoint.

## 9. Experiment Gates

### 9.1 Fatal-flaw audit

The current 1024-row ShareGPT/BurstGPT workload has:

```text
target_output_tokens distinct = 518
prompt + trace-target P50/P95/P99/max = 335/1211/1693/2618
P99/P50 = 5.054
rows over token budget 6144 = 0
correlation(prompt_tokens, target_output_tokens) = -0.0015
```

This is sufficient variation for a trace-cost packing sensitivity experiment
and does not trigger the oversized-outlier fatal flaw at budget 6144. The near
zero correlation and independent-source importer prohibit treating the target
as an oracle for realized Qwen output.

### 9.2 Progressive execution

1. **64-row real-component gate**
   Verify schemas, scope, exactly-once membership, real vLLM success delta,
   request/submission joins, timing invariants, and non-empty traces.
2. **512-row tuning experiment**
   Compare output cost modes and sequential/BFD organization with seeded
   repetitions.
3. **1024-row confirmation**
   Run only configurations selected on 512 rows.
4. **2048-row held-out**
   Run without retuning and only after the 1024 gate passes.

The 64-row gate is infrastructure validation, not performance evidence.
Within each performance scale, every policy cell uses the identical document
set and all non-treatment controls. Results from different row counts are not
combined into one policy delta.

### 9.3 Comparison matrix

The minimum 512-row matrix is:

```text
sequential token budget × prompt_only
sequential token budget × fixed_output_cap
sequential token budget × trace_target_output
BFD token budget        × prompt_only
BFD token budget        × fixed_output_cap
BFD token budget        × trace_target_output
```

Length-align and prefix-aware policies remain independent baselines and are
not changed in this implementation.

All six cells support packing-structure comparisons. GPU throughput and
latency from `trace_target_output` are exploratory sensitivity results because
the trace cost is not paired with realized Qwen output. Promotion of an
output-aware estimator requires the separate calibrated-label gate in
Section 2.4.

## 10. Success Criteria

The implementation phase is complete only when:

- cost semantics are shared by offline organization and arrival replay;
- BFD is deterministic and engine-independent;
- every complete row is preserved exactly once;
- global/local scope is explicit in every relevant run;
- Arrow and Daft produce identical membership for the same organizer input;
- existing batching, replay, scheduler, and lifecycle tests pass;
- full unit suite and compile checks pass;
- the real 64-row Daft -> Ray -> vLLM gate passes.

Policy promotion requires later repeated evidence. BFD is not declared better
merely because it has been implemented. Trace-derived cost is not declared an
output estimator merely because it changes packing or performance.
