# Output-Aware Cost and Deterministic BFD Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add shared output-cost semantics, modality-neutral deterministic BFD packing, complete offline request tracing, and real single-GPU evidence to the existing PostgreSQL → Daft → Arrow → Ray → vLLM pipeline.

**Architecture:** A small `request_costs.py` module resolves the text-generation output contribution, while an engine-independent `packing.py` module accepts only stable row identities and scalar `cost_units`. Arrow and Daft share one organizer adapter; arrival replay reuses only the cost resolver and preserves arrival order. Offline organizer runs emit lifecycle seeds from a shared job-start origin so sequential and BFD request latency remain comparable without changing execution behavior.

**Tech Stack:** Python 3.10, standard-library dataclasses and `unittest`, PyArrow 24, Daft 0.7.20, Ray 2.56, PostgreSQL 18.4, pgvector 0.8.2, vLLM 0.25.1, Qwen2.5-1.5B.

## Global Constraints

- Formal execution remains `PostgreSQL -> Daft -> Arrow -> Ray task/actor -> real compatible-http vLLM`.
- Do not modify Daft, Ray, or vLLM internals.
- Every source row remains one complete model request; never split prompt contents.
- Global BFD is offline only. Arrival replay remains ordered and uses `PendingBatchBuilder`.
- The BFD core uses `cost_units` and `capacity`; it must not import Arrow, Daft, Ray, tokenizers, model IDs, prompt text, or image types.
- `completion_max_tokens` controls backend generation. `output_cost_mode` controls only the batching estimate.
- `trace_target_output` is recorded as `burstgpt_unpaired_trace_metadata`; never label it oracle or measured output.
- Existing defaults remain sequential batching with `output_cost_mode=fixed_output_cap`.
- Every production behavior change follows RED → GREEN.
- Unit tests may use synthetic tables. GPU evidence must use the real local PostgreSQL, Daft, Ray, and vLLM components.
- Every generated CSV contains actual `server_version` and `pgvector_version`.
- Run the 64-row gate before the 512-row matrix. After the 512 matrix passes
  its integrity audit, rerun only the selected baseline and selected adaptive
  configuration at 1024 rows for three formal repeats. Do not run 2048 in
  this implementation plan.
- The 64-row gate is infrastructure-only. Every performance comparison uses
  the same 512 source documents, source order, fetch size, model, generation
  cap, token budget, row cap, K_max, and writeback setting across all six
  cells; only packing algorithm and output-cost mode vary.
- Keep `.superpowers/` untracked, keep the feature branch isolated, and do not merge `main`.

---

### Task 1: Shared Output-Cost Semantics

**Files:**
- Create: `code/src/request_costs.py`
- Create: `code/tests/modalities/text/test_request_costs.py`

**Interfaces:**
- Produces:

```python
OutputCostMode = Literal[
    "prompt_only",
    "fixed_output_cap",
    "trace_target_output",
]

def resolve_output_tokens(
    mode: OutputCostMode,
    *,
    completion_max_tokens: int,
    target_output_tokens: object,
) -> int

def output_cost_source(mode: OutputCostMode) -> str
```

- `output_cost_source("trace_target_output")` returns
  `burstgpt_unpaired_trace_metadata`.
- The module has no engine imports.

- [ ] **Step 1: Write the failing cost tests**

Create `code/tests/modalities/text/test_request_costs.py`:

```python
from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.request_costs import output_cost_source, resolve_output_tokens


class RequestCostTests(unittest.TestCase):
    def test_modes_resolve_only_the_output_contribution(self) -> None:
        self.assertEqual(
            resolve_output_tokens(
                "prompt_only",
                completion_max_tokens=16,
                target_output_tokens=9,
            ),
            0,
        )
        self.assertEqual(
            resolve_output_tokens(
                "fixed_output_cap",
                completion_max_tokens=16,
                target_output_tokens=9,
            ),
            16,
        )
        self.assertEqual(
            resolve_output_tokens(
                "trace_target_output",
                completion_max_tokens=16,
                target_output_tokens=9,
            ),
            9,
        )

    def test_modes_have_explicit_non_oracle_sources(self) -> None:
        self.assertEqual(output_cost_source("prompt_only"), "configured_zero")
        self.assertEqual(
            output_cost_source("fixed_output_cap"),
            "backend_completion_cap",
        )
        self.assertEqual(
            output_cost_source("trace_target_output"),
            "burstgpt_unpaired_trace_metadata",
        )

    def test_invalid_values_fail_explicitly(self) -> None:
        invalid_targets = [None, True, 1.5, "4", -1]
        for value in invalid_targets:
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    ValueError,
                    "target_output_tokens",
                ):
                    resolve_output_tokens(
                        "trace_target_output",
                        completion_max_tokens=16,
                        target_output_tokens=value,
                    )
        for cap in (-1, True, 1.5):
            with self.subTest(cap=cap):
                with self.assertRaisesRegex(
                    ValueError,
                    "completion_max_tokens",
                ):
                    resolve_output_tokens(
                        "fixed_output_cap",
                        completion_max_tokens=cap,
                        target_output_tokens=0,
                    )
        with self.assertRaisesRegex(ValueError, "output cost mode"):
            output_cost_source("unknown")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\modalities\text\test_request_costs.py -v
```

Expected: import failure because `src.request_costs` does not exist.

- [ ] **Step 3: Implement the minimal resolver**

Create `code/src/request_costs.py`:

```python
"""Engine-independent output-cost semantics for complete requests."""

from __future__ import annotations

from typing import Literal


OutputCostMode = Literal[
    "prompt_only",
    "fixed_output_cap",
    "trace_target_output",
]

_OUTPUT_COST_SOURCES = {
    "prompt_only": "configured_zero",
    "fixed_output_cap": "backend_completion_cap",
    "trace_target_output": "burstgpt_unpaired_trace_metadata",
}


def _non_negative_int(value: object, field_name: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def output_cost_source(mode: OutputCostMode) -> str:
    try:
        return _OUTPUT_COST_SOURCES[mode]
    except KeyError as exc:
        raise ValueError(f"unknown output cost mode: {mode}") from exc


def resolve_output_tokens(
    mode: OutputCostMode,
    *,
    completion_max_tokens: int,
    target_output_tokens: object,
) -> int:
    cap = _non_negative_int(
        completion_max_tokens,
        "completion_max_tokens",
    )
    output_cost_source(mode)
    if mode == "prompt_only":
        return 0
    if mode == "fixed_output_cap":
        return cap
    return _non_negative_int(
        target_output_tokens,
        "target_output_tokens",
    )
```

- [ ] **Step 4: Verify GREEN and engine independence**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\modalities\text\test_request_costs.py -v
Select-String -Path code\src\request_costs.py -Pattern 'pyarrow|daft|ray|vllm'
```

Expected: all tests pass and the dependency scan returns no matches.

- [ ] **Step 5: Commit Task 1**

```powershell
git add code/src/request_costs.py code/tests/modalities/text/test_request_costs.py
git commit -m "feat: resolve output cost modes"
```

---

### Task 2: Deterministic Modality-Neutral BFD

**Files:**
- Create: `code/src/packing.py`
- Create: `code/tests/planning/test_packing.py`

**Interfaces:**
- Produces:

```python
@dataclass(frozen=True)
class PackItem:
    row_index: int
    stable_id: str
    cost_units: int

def best_fit_decreasing(
    items: Sequence[PackItem],
    *,
    capacity: int,
    max_rows: int,
) -> tuple[tuple[int, ...], ...]

@dataclass(frozen=True)
class PackingSummary:
    utilization_mean: float
    utilization_p95: float
    oversized_rows: int
    input_rows: int
    batch_count: int
    cost_units_p50: float
    cost_units_p95: float
    cost_units_p99: float
    cost_units_max: int

def summarize_packing(
    batch_cost_units: Sequence[int],
    batch_row_counts: Sequence[int],
    *,
    capacity: int,
) -> PackingSummary
```

- Consumes no engine types.

- [ ] **Step 1: Write failing BFD and summary tests**

Create `code/tests/planning/test_packing.py` with:

```python
from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.packing import PackItem, best_fit_decreasing, summarize_packing


def items(costs: list[int]) -> list[PackItem]:
    return [
        PackItem(index, f"row-{index}", cost)
        for index, cost in enumerate(costs)
    ]


class PackingTests(unittest.TestCase):
    def test_canonical_best_fit_decreasing_membership(self) -> None:
        packed = best_fit_decreasing(
            items([6, 5, 4, 3, 2]),
            capacity=10,
            max_rows=3,
        )
        self.assertEqual(packed, ((0, 2), (1, 3, 4)))

    def test_ties_are_deterministic(self) -> None:
        source = [
            PackItem(2, "b", 5),
            PackItem(0, "a", 5),
            PackItem(1, "a", 5),
        ]
        expected = ((0, 1), (2,))
        for _ in range(5):
            self.assertEqual(
                best_fit_decreasing(
                    source,
                    capacity=10,
                    max_rows=2,
                ),
                expected,
            )

    def test_row_limit_and_oversized_rows_are_enforced(self) -> None:
        packed = best_fit_decreasing(
            items([12, 4, 3, 2]),
            capacity=10,
            max_rows=2,
        )
        self.assertEqual(packed, ((0,), (1, 2), (3,)))
        flattened = [index for group in packed for index in group]
        self.assertEqual(sorted(flattened), [0, 1, 2, 3])

    def test_empty_and_invalid_inputs(self) -> None:
        self.assertEqual(
            best_fit_decreasing([], capacity=10, max_rows=2),
            (),
        )
        with self.assertRaisesRegex(ValueError, "duplicate row_index"):
            best_fit_decreasing(
                [PackItem(0, "a", 1), PackItem(0, "b", 1)],
                capacity=10,
                max_rows=2,
            )
        for capacity, max_rows in ((0, 1), (1, 0), (-1, 1)):
            with self.subTest(capacity=capacity, max_rows=max_rows):
                with self.assertRaises(ValueError):
                    best_fit_decreasing(
                        [],
                        capacity=capacity,
                        max_rows=max_rows,
                    )

    def test_summary_excludes_oversized_batches_from_utilization(self) -> None:
        summary = summarize_packing(
            [10, 8, 12],
            [2, 2, 1],
            capacity=10,
        )
        self.assertEqual(summary.utilization_mean, 0.9)
        self.assertEqual(summary.utilization_p95, 1.0)
        self.assertEqual(summary.oversized_rows, 1)
        self.assertEqual(summary.input_rows, 5)
        self.assertEqual(summary.batch_count, 3)
        self.assertEqual(summary.cost_units_max, 12)

        fixed_rows = summarize_packing(
            [9, 7],
            [2, 2],
            capacity=0,
        )
        self.assertEqual(fixed_rows.utilization_mean, 0.0)
        self.assertEqual(fixed_rows.oversized_rows, 0)
        self.assertEqual(fixed_rows.input_rows, 4)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\planning\test_packing.py -v
```

Expected: import failure because `src.packing` does not exist.

- [ ] **Step 3: Implement immutable items and BFD**

Create `code/src/packing.py`. Use one private mutable `_OpenBatch` only inside
the pure function:

```python
"""Deterministic scalar-capacity packing for complete request rows."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence


def _positive_int(value: object, field_name: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
    ):
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _non_negative_int(value: object, field_name: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


@dataclass(frozen=True)
class PackItem:
    row_index: int
    stable_id: str
    cost_units: int

    def __post_init__(self) -> None:
        _non_negative_int(self.row_index, "row_index")
        if not isinstance(self.stable_id, str) or not self.stable_id:
            raise ValueError("stable_id must be a non-empty string")
        _non_negative_int(self.cost_units, "cost_units")


@dataclass
class _OpenBatch:
    creation_index: int
    row_indexes: list[int]
    total_cost_units: int


def best_fit_decreasing(
    items: Sequence[PackItem],
    *,
    capacity: int,
    max_rows: int,
) -> tuple[tuple[int, ...], ...]:
    resolved_capacity = _non_negative_int(capacity, "capacity")
    resolved_max_rows = _positive_int(max_rows, "max_rows")
    row_indexes = [item.row_index for item in items]
    if len(row_indexes) != len(set(row_indexes)):
        raise ValueError("duplicate row_index values are not allowed")

    ordered = sorted(
        items,
        key=lambda item: (
            -item.cost_units,
            item.stable_id,
            item.row_index,
        ),
    )
    batches: list[_OpenBatch] = []
    for item in ordered:
        eligible = [
            batch
            for batch in batches
            if len(batch.row_indexes) < resolved_max_rows
            and batch.total_cost_units + item.cost_units
            <= resolved_capacity
        ]
        if eligible:
            selected = min(
                eligible,
                key=lambda batch: (
                    resolved_capacity
                    - batch.total_cost_units
                    - item.cost_units,
                    batch.creation_index,
                ),
            )
            selected.row_indexes.append(item.row_index)
            selected.total_cost_units += item.cost_units
            continue
        batches.append(
            _OpenBatch(
                creation_index=len(batches),
                row_indexes=[item.row_index],
                total_cost_units=item.cost_units,
            )
        )
    return tuple(tuple(batch.row_indexes) for batch in batches)
```

- [ ] **Step 4: Implement the packing summary**

In the same module, add nearest-rank percentile and `PackingSummary`.
Oversized batches are counted separately and excluded from utilization:

```python
def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil(percentile / 100.0 * len(ordered)) - 1
    return ordered[min(max(index, 0), len(ordered) - 1)]


@dataclass(frozen=True)
class PackingSummary:
    utilization_mean: float
    utilization_p95: float
    oversized_rows: int
    input_rows: int
    batch_count: int
    cost_units_p50: float
    cost_units_p95: float
    cost_units_p99: float
    cost_units_max: int


def summarize_packing(
    batch_cost_units: Sequence[int],
    batch_row_counts: Sequence[int],
    *,
    capacity: int,
) -> PackingSummary:
    resolved_capacity = _positive_int(capacity, "capacity")
    if len(batch_cost_units) != len(batch_row_counts):
        raise ValueError("batch costs and row counts must have equal length")
    costs = [
        _non_negative_int(value, "batch cost")
        for value in batch_cost_units
    ]
    rows = [
        _non_negative_int(value, "batch row count")
        for value in batch_row_counts
    ]
    utilization = (
        [
            cost / resolved_capacity
            for cost in costs
            if cost <= resolved_capacity
        ]
        if resolved_capacity > 0
        else []
    )
    oversized_rows = (
        sum(
            row_count
            for cost, row_count in zip(costs, rows)
            if cost > resolved_capacity
        )
        if resolved_capacity > 0
        else 0
    )
    return PackingSummary(
        utilization_mean=(
            sum(utilization) / len(utilization)
            if utilization
            else 0.0
        ),
        utilization_p95=_percentile(utilization, 95),
        oversized_rows=oversized_rows,
        input_rows=sum(rows),
        batch_count=len(costs),
        cost_units_p50=_percentile([float(value) for value in costs], 50),
        cost_units_p95=_percentile([float(value) for value in costs], 95),
        cost_units_p99=_percentile([float(value) for value in costs], 99),
        cost_units_max=max(costs, default=0),
    )
```

- [ ] **Step 5: Verify GREEN and engine independence**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\planning\test_packing.py -v
Select-String -Path code\src\packing.py -Pattern 'pyarrow|daft|ray|vllm|prompt|tokenizer|image'
```

Expected: all tests pass and the dependency/domain scan returns no matches.

- [ ] **Step 6: Commit Task 2**

```powershell
git add code/src/packing.py code/tests/planning/test_packing.py
git commit -m "feat: add deterministic BFD packing"
```

---

### Task 3: Share BFD Across Arrow and Daft Organizers

**Files:**
- Modify: `code/src/organizers.py:17-253`
- Modify: `code/tests/planning/test_organizers.py`

**Interfaces:**
- Consumes `OutputCostMode`, `resolve_output_tokens`, `PackItem`,
  `best_fit_decreasing`, and `summarize_packing`.
- Extends:

```python
BatchingPolicy = Literal[
    "fixed_rows",
    "token_budget",
    "best_fit_token_budget",
    "length_align_fixed_rows",
    "length_align_token_budget",
    "prefix_aware_fixed_rows",
    "prefix_aware_token_budget",
]

@dataclass(frozen=True)
class OrganizerConfig:
    ...
    output_cost_mode: OutputCostMode = "fixed_output_cap"

@dataclass(frozen=True)
class OrganizedBatches:
    batches: list[pa.Table]
    metrics: dict[str, object]
    batch_cost_units: tuple[int, ...] = ()
    batch_row_counts: tuple[int, ...] = ()
```

- `best_fit_token_budget` uses `batch_size` as `max_rows`.
- Arrow and Daft call the same `organize_arrow_table`.

- [ ] **Step 1: Add failing organizer tests**

Extend `code/tests/planning/test_organizers.py`:

```python
def output_aware_table() -> pa.Table:
    return pa.table(
        {
            "doc_id": [10, 11, 12, 13, 14],
            "prompt": ["a", "b", "c", "d", "e"],
            "prompt_tokens": [6, 5, 4, 3, 2],
            "target_output_tokens": [0, 0, 0, 0, 0],
        }
    )


def memberships(result) -> list[list[int]]:
    return [
        batch.column("doc_id").to_pylist()
        for batch in result.batches
    ]


def test_arrow_best_fit_uses_shared_deterministic_membership(self) -> None:
    result = make_organizer(
        "arrow",
        OrganizerConfig(
            batch_size=3,
            batching_policy="best_fit_token_budget",
            token_budget=10,
            output_cost_mode="prompt_only",
        ),
    ).organize(output_aware_table())

    self.assertEqual(memberships(result), [[10, 12], [11, 13, 14]])
    self.assertEqual(result.metrics["packing_algorithm"], "best_fit_decreasing")
    self.assertEqual(result.metrics["packing_scope"], "organizer_input")
    self.assertEqual(result.metrics["packing_cost_unit"], "tokens")
    self.assertEqual(result.metrics["packing_input_rows"], 5)
    self.assertEqual(result.metrics["packing_batch_count"], 2)


def test_arrow_and_daft_best_fit_membership_is_identical(self) -> None:
    config = OrganizerConfig(
        batch_size=3,
        runner="native",
        batching_policy="best_fit_token_budget",
        token_budget=10,
        output_cost_mode="prompt_only",
    )
    arrow = make_organizer("arrow", config).organize(output_aware_table())
    daft = make_organizer("daft", config).organize(output_aware_table())

    self.assertEqual(memberships(arrow), memberships(daft))
    self.assertEqual(arrow.batch_cost_units, daft.batch_cost_units)


def test_trace_cost_changes_membership_without_changing_generation_cap(self) -> None:
    table = pa.table(
        {
            "doc_id": [1, 2, 3],
            "prompt": ["a", "b", "c"],
            "prompt_tokens": [2, 2, 2],
            "target_output_tokens": [8, 0, 0],
        }
    )
    result = make_organizer(
        "arrow",
        OrganizerConfig(
            batch_size=3,
            batching_policy="best_fit_token_budget",
            token_budget=10,
            completion_max_tokens=16,
            output_cost_mode="trace_target_output",
        ),
    ).organize(table)

    self.assertEqual(memberships(result), [[1], [2, 3]])
    self.assertEqual(result.metrics["output_cost_source"],
                     "burstgpt_unpaired_trace_metadata")
```

Add cases for missing/boolean/negative prompt tokens, missing trace targets,
one oversized row, empty input, `max_rows`, and the existing sequential and
fixed-row membership tests.

- [ ] **Step 2: Run organizer tests and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\planning\test_organizers.py -v
```

Expected: failures because the policy, config field, and BFD metrics do not
exist.

- [ ] **Step 3: Add the cost adapter and BFD branch**

In `code/src/organizers.py`:

```python
def _row_token_cost(
    table: pa.Table,
    row_index: int,
    config: OrganizerConfig,
) -> int:
    if "prompt_tokens" not in table.column_names:
        raise ValueError("prompt_tokens column is required for token batching")
    prompt_value = table.column("prompt_tokens")[row_index].as_py()
    if (
        not isinstance(prompt_value, int)
        or isinstance(prompt_value, bool)
        or prompt_value < 0
    ):
        raise ValueError(
            "prompt_tokens must contain non-negative integers"
        )
    target_value = (
        table.column("target_output_tokens")[row_index].as_py()
        if "target_output_tokens" in table.column_names
        else None
    )
    return prompt_value + resolve_output_tokens(
        config.output_cost_mode,
        completion_max_tokens=config.completion_max_tokens,
        target_output_tokens=target_value,
    )


def _best_fit_batches(
    table: pa.Table,
    config: OrganizerConfig,
) -> list[pa.Table]:
    items = [
        PackItem(
            row_index=index,
            stable_id=(
                str(table.column("doc_id")[index].as_py())
                if "doc_id" in table.column_names
                else str(index)
            ),
            cost_units=_row_token_cost(table, index, config),
        )
        for index in range(table.num_rows)
    ]
    groups = best_fit_decreasing(
        items,
        capacity=config.token_budget,
        max_rows=config.batch_size,
    )
    return [
        table.take(pa.array(group, type=pa.int64()))
        for group in groups
    ]
```

Make `_token_budget_batches` call `_row_token_cost(table, row_index, config)`.
Do not change its sequential membership rule.

- [ ] **Step 4: Make Daft use the same BFD implementation**

For `best_fit_token_budget`, materialize all Arrow chunks emitted for one
Daft organizer call, concatenate them once, and call
`organize_arrow_table(combined, config)` once:

```python
arrow_tables = list(df.to_arrow_iter())
combined = (
    pa.concat_tables(arrow_tables)
    if arrow_tables
    else table.slice(0, 0)
)
batches = organize_arrow_table(combined, self.config)
```

Retain the existing streaming loop for other non-fixed policies. Do not add a
second Daft-specific BFD function. Record `packing_scope=partition_local` when
a non-BFD token policy packs more than one emitted Daft partition
independently. BFD records `organizer_input` because it concatenates the
complete organizer input before the one shared call.

- [ ] **Step 5: Record exact packing inputs and summary**

After constructing batches:

```python
batch_cost_units = tuple(
    sum(
        _row_token_cost(batch, index, config)
        for index in range(batch.num_rows)
    )
    for batch in batches
)
batch_row_counts = tuple(batch.num_rows for batch in batches)
summary = summarize_packing(
    batch_cost_units,
    batch_row_counts,
    capacity=config.token_budget,
)
```

Pass `capacity=0` for fixed-row policies so they record zero utilization and
oversized rows while retaining batch/input counts and cost-unit percentiles.
Add exact metric keys from the design:

```text
output_cost_mode
output_cost_source
packing_cost_unit
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

Return the raw tuples through `OrganizedBatches`.

- [ ] **Step 6: Verify organizer GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\modalities\text\test_request_costs.py -v
.conda\pg-ai-profile\python.exe code\tests\planning\test_packing.py -v
.conda\pg-ai-profile\python.exe code\tests\planning\test_organizers.py -v
```

Expected: all tests pass, including identical Arrow/Daft membership.

- [ ] **Step 7: Commit Task 3**

```powershell
git add code/src/organizers.py code/tests/planning/test_organizers.py
git commit -m "feat: organize requests with shared BFD"
```

---

### Task 4: Wire Cost Modes Into Profiler and Arrival Replay

**Files:**
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py:174-218`
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py:488-580`
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py:1913-2711`
- Modify: `code/scripts/README.md`
- Modify: `code/tests/observability/test_postgres_profile_scheduling.py`
- Modify: `code/tests/scheduling/test_runtime_batching.py`

**Interfaces:**
- New CLI:

```text
--output-cost-mode {prompt_only,fixed_output_cap,trace_target_output}
--cost-model-id STRING
--cost-tokenizer-id STRING
```

- `_batch_envelopes(..., output_cost_mode)` and
  `_row_arrivals(..., output_cost_mode)` use the shared resolver.
- Arrival replay continues to use `PendingBatchBuilder`; BFD sorting is never
  called.

- [ ] **Step 1: Write failing CLI and adapter tests**

Add profiler tests that parse each cost mode and assert dry-run fields:

```python
self.assertEqual(row["output_cost_mode"], "trace_target_output")
self.assertEqual(
    row["output_cost_source"],
    "burstgpt_unpaired_trace_metadata",
)
self.assertEqual(row["packing_cost_unit"], "tokens")
self.assertEqual(row["cost_model_id"], "qwen2.5-1.5b")
self.assertEqual(
    row["cost_tokenizer_id"],
    "Qwen2.5-1.5B-Instruct",
)
```

Extend `_batch_envelopes` tests with a table containing
`target_output_tokens=[7, 3]`. With `trace_target_output`, assert the
`BatchRequest.estimated_output_tokens` is 10 while the supplied
`completion_max_tokens` remains 16.

Extend `_row_arrivals` tests to assert per-row output estimates `[7, 3]`,
source order unchanged, and missing target fails before any invalid row is
returned.

- [ ] **Step 2: Verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
```

Expected: parser rejects the new flags and helper signatures do not accept the
cost mode.

- [ ] **Step 3: Add CLI and shared row resolution**

Add parser choices using the exact `OutputCostMode` values. Default to
`fixed_output_cap`.

Create one profiler-boundary helper:

```python
def _row_output_tokens(
    table: pa.Table | pa.RecordBatch,
    row_index: int,
    *,
    output_cost_mode: OutputCostMode,
    completion_max_tokens: int,
) -> int:
    target_value = (
        table.column("target_output_tokens")[row_index].as_py()
        if "target_output_tokens" in table.column_names
        else None
    )
    return resolve_output_tokens(
        output_cost_mode,
        completion_max_tokens=completion_max_tokens,
        target_output_tokens=target_value,
    )
```

Use it in `_batch_envelopes` and `_row_arrivals`. Pass
`args.completion_max_tokens` separately to the backend exactly as before.

- [ ] **Step 4: Pass cost configuration to both execution paths**

Set:

```python
OrganizerConfig(
    ...
    output_cost_mode=args.output_cost_mode,
)
```

Pass the same mode through `_arrival_replay_envelopes` into `_row_arrivals`.
Keep:

```python
token_budget = (
    args.token_budget
    if args.batching_policy == "token_budget"
    else 0
)
```

for replay. Explicitly reject
`--arrival-replay --batching-policy best_fit_token_budget` with:

```text
arrival replay does not support best_fit_token_budget
```

- [ ] **Step 5: Aggregate exact packing metrics**

Maintain run-local lists:

```python
packing_batch_cost_units: list[int] = []
packing_batch_row_counts: list[int] = []
organizer_calls = 0
organizer_packing_scopes: list[str] = []
```

For offline organization, extend them from `OrganizedBatches`. For replay,
append `PendingBatch.estimated_total_tokens` and `PendingBatch.row_count` in
`close_batch`.

Resolve scope:

```python
if args.arrival_replay:
    packing_scope = "arrival_order"
elif organizer_calls > 1:
    packing_scope = "fetch_chunk_local"
elif "partition_local" in organizer_packing_scopes:
    packing_scope = "partition_local"
else:
    packing_scope = "organizer_input"
```

Resolve algorithm labels without inferring them from batch sizes:

```python
if args.arrival_replay:
    packing_algorithm = "sequential_pending"
elif args.batching_policy == "best_fit_token_budget":
    packing_algorithm = "best_fit_decreasing"
elif args.batching_policy == "fixed_rows":
    packing_algorithm = "fixed_rows"
else:
    packing_algorithm = "sequential"
```

Compute one final `PackingSummary` across all batches. Record every design
metric in dry-run and real run rows. For fixed-row capacity zero, utilization
and oversized fields are zero while counts and cost-unit percentiles remain
populated.

- [ ] **Step 6: Verify profiler and replay GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_runtime_batching.py -v
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
```

Expected: all tests pass; replay order and flush tests remain unchanged.

- [ ] **Step 7: Update CLI documentation and commit**

Document the three cost modes, source labels, model/tokenizer provenance, and
the rule that BFD is rejected in arrival replay.

```powershell
git add code/scripts/profiling/postgres_ai_operator_profile.py code/scripts/README.md code/tests/observability/test_postgres_profile_scheduling.py code/tests/scheduling/test_runtime_batching.py
git commit -m "feat: wire output cost metrics"
```

---

### Task 5: Add Explicit Offline Request Lifecycle Origins

**Files:**
- Modify: `code/src/scheduling/lifecycle.py`
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py:1072-1205`
- Modify: `code/scripts/profiling/postgres_ai_operator_profile.py:1885-2443`
- Modify: `code/tests/scheduling/test_request_lifecycle.py`
- Modify: `code/tests/observability/test_postgres_profile_scheduling.py`

**Interfaces:**
- Produces:

```python
RequestTimeOrigin = Literal[
    "replayed_arrival",
    "offline_job_start",
]
```

- Adds `request_time_origin` to `RequestLifecycleSeed` and `RequestTraceRow`.
- `_offline_batch_envelopes(...)` returns envelopes and exactly one seed per
  complete source row.
- Request trace schema becomes version 2.

- [ ] **Step 1: Write failing lifecycle-origin tests**

Update existing seed fixtures to use `request_time_origin="replayed_arrival"`.
Add:

```python
def test_request_trace_preserves_time_origin(self) -> None:
    seed = RequestLifecycleSeed(
        request_id="job:row:1",
        submission_id="job:batch:0",
        doc_id="1",
        prompt_tokens=4,
        estimated_output_tokens=2,
        prefix_key="",
        arrival_epoch_s=100.0,
        flush_epoch_s=101.0,
        request_time_origin="offline_job_start",
    )
    rows = build_request_trace_rows(
        [seed],
        [
            SubmissionLifecycleEvent(
                submission_id="job:batch:0",
                pool_id="default",
                endpoint_id="task-0",
                gpu_id="0",
                submit_epoch_s=101.1,
                completion_epoch_s=102.0,
                status="completed",
            )
        ],
        {
            "job:batch:0": SubmissionServiceTiming(
                submission_id="job:batch:0",
                service_start_epoch_s=101.2,
                service_end_epoch_s=101.9,
            )
        },
        {"1": 1},
        {},
        slo_target_s=None,
    )
    self.assertEqual(rows[0].request_time_origin, "offline_job_start")
    self.assertEqual(rows[0].e2e_s, 2.0)
```

Add validation rejecting any origin outside the two literals.

- [ ] **Step 2: Write the failing offline seed test**

Add to `test_postgres_profile_scheduling.py`:

```python
def test_offline_batch_envelopes_seed_every_row_from_job_start(self) -> None:
    table = pa.table(
        {
            "doc_id": [1, 2],
            "prompt_tokens": [6, 4],
            "target_output_tokens": [3, 2],
            "prefix_key": ["p", "p"],
        }
    )
    envelopes, seeds = profile._offline_batch_envelopes(
        [table],
        job_id="job",
        operator="ai_complete",
        completion_max_tokens=16,
        output_cost_mode="trace_target_output",
        batch_index_start=7,
        job_start_epoch_s=100.0,
        ready_epoch_s=101.0,
    )

    self.assertEqual(envelopes[0].request.request_id, "job:batch:7")
    self.assertEqual([seed.doc_id for seed in seeds], ["1", "2"])
    self.assertEqual(
        [seed.estimated_output_tokens for seed in seeds],
        [3, 2],
    )
    self.assertTrue(
        all(seed.arrival_epoch_s == 100.0 for seed in seeds)
    )
    self.assertTrue(all(seed.flush_epoch_s == 101.0 for seed in seeds))
    self.assertTrue(
        all(
            seed.request_time_origin == "offline_job_start"
            for seed in seeds
        )
    )
```

- [ ] **Step 3: Verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_request_lifecycle.py -v
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
```

Expected: dataclasses lack `request_time_origin` and the offline helper is
missing.

- [ ] **Step 4: Implement typed origins and schema 2**

Add the literal and validate it in `RequestLifecycleSeed.__post_init__`.
Propagate it unchanged into `RequestTraceRow`.

In `_write_request_trace`, set:

```python
"schema_version": 2,
"request_time_origin": row.request_time_origin,
```

Arrival replay seeds always set `replayed_arrival`.

- [ ] **Step 5: Build static envelopes and seeds once**

Implement:

```python
def _offline_batch_envelopes(
    batches: Iterable[pa.Table | pa.RecordBatch],
    *,
    job_id: str,
    operator: str,
    completion_max_tokens: int,
    output_cost_mode: OutputCostMode,
    batch_index_start: int,
    job_start_epoch_s: float,
    ready_epoch_s: float,
) -> tuple[list[PayloadEnvelope], list[RequestLifecycleSeed]]:
```

For each batch:

1. create its `PayloadEnvelope` with a globally increasing batch index;
2. require a non-null `doc_id` for every row;
3. resolve each row's prompt and output estimate with the same adapter used by
   the envelope;
4. create one seed with `arrival_epoch_s=job_start_epoch_s`,
   `flush_epoch_s=ready_epoch_s`, and
   `request_time_origin=offline_job_start`;
5. return no partial result if validation fails.

Use a run-local `offline_batch_index` counter so multiple database fetch
chunks never reuse a `submission_id`.

- [ ] **Step 6: Permit tracing on the typed offline Ray path**

Change validation so request tracing requires:

```text
executor in {ray_task, ray_actor}
scheduling_policy in {static, aimd, ewma_aimd, pid}
```

It no longer requires `--arrival-replay`.

Capture `offline_job_start_epoch_s` from the shared `MonotonicEpochClock`
before the source-fetch loop. Immediately after each organizer call, capture
one `ready_epoch_s`, build envelopes/seeds, and submit those envelopes through
the existing typed Ray path. Do not alter scheduler order, routing, admission,
or backend arguments.

- [ ] **Step 7: Verify lifecycle GREEN**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_request_lifecycle.py -v
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py -v
```

Expected: all tests pass; replay rows retain `replayed_arrival`, offline rows
use `offline_job_start`, and submission IDs remain unique across fetch chunks.

- [ ] **Step 8: Commit Task 5**

```powershell
git add code/src/scheduling/lifecycle.py code/scripts/profiling/postgres_ai_operator_profile.py code/tests/scheduling/test_request_lifecycle.py code/tests/observability/test_postgres_profile_scheduling.py
git commit -m "feat: trace offline request lifecycles"
```

---

### Task 6: Real Daft-Ray Contracts, Regression, and Result Summaries

**Files:**
- Modify: `code/tests/scheduling/test_scheduling_daft_ray_contract.py`
- Create: `code/scripts/analysis/summarize_output_aware_bfd.py`
- Create: `code/tests/experiments/test_output_aware_summary.py`
- Modify: `code/scripts/README.md`

**Interfaces:**
- Real local contract executes Daft-organized BFD batches through both Ray
  task and actor adapters.
- Summary CLI:

```text
--runs PATH
--output PATH
```

- Summary output is long-form CSV with:

```text
scenario_id,metric,n,mean,sample_std,p50,min,max
```

- Metrics summarized:
  `tokens_per_s`, `e2e_s`, `request_e2e_s_p50`,
  `request_e2e_s_p95`, `request_e2e_s_p99`,
  `batch_service_s_p99`, `operator_invocations`,
  `packing_budget_utilization_mean`,
  `packing_budget_utilization_p95`,
  `batch_estimated_cost_units_p95`.

The formal run row and summary also include resource-efficiency evidence:

```text
gpu_utilization_pct_mean
gpu_utilization_pct_p50
gpu_utilization_pct_p95
gpu_utilization_pct_max
gpu_utilization_below_10pct_ratio
gpu_memory_used_mib_mean
gpu_memory_used_mib_max
gpu_memory_utilization_pct_mean
gpu_memory_utilization_pct_max
gpu_power_w_mean
gpu_power_w_max
gpu_energy_j
energy_j_per_1k_observed_tokens
vllm_running_mean
vllm_running_p95
vllm_running_max
vllm_waiting_mean
vllm_waiting_p95
vllm_waiting_max
vllm_kv_cache_usage_mean
vllm_kv_cache_usage_p95
vllm_kv_cache_usage_max
mfu_estimate
mfu_status
mfu_estimation_method
mfu_time_basis
model_flops_per_token
gpu_peak_tflops
mfu_precision
```

MFU is emitted only when the run explicitly supplies a reviewed
`model_flops_per_token` estimate and the GPU peak throughput for the recorded
precision. It is computed from observed vLLM prompt+generation tokens over
`operator_wall_s`. Missing inputs produce an empty estimate and an explicit
status; GPU utilization is never relabelled as MFU. Power/energy fields follow
the same rule when the device does not expose `power.draw`.

- [ ] **Step 1: Write the failing real framework contract**

Add a contract using `DaftOrganizer` with:

```python
OrganizerConfig(
    batch_size=3,
    runner="native",
    batching_policy="best_fit_token_budget",
    token_budget=10,
    output_cost_mode="prompt_only",
)
```

Use costs `[6, 5, 4, 3, 2]`. Pass resulting Arrow tables through the existing
real local Ray task and actor adapters. Assert:

```python
self.assertEqual(task_groups, [[1, 3], [2, 4, 5]])
self.assertEqual(actor_groups, task_groups)
self.assertEqual(
    sorted(doc_id for group in task_groups for doc_id in group),
    [1, 2, 3, 4, 5],
)
self.assertEqual(len(task_request_rows), 5)
self.assertTrue(
    all(
        row.request_time_origin == "offline_job_start"
        for row in task_request_rows
    )
)
```

- [ ] **Step 2: Run contract and verify RED**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_daft_ray_contract.py -v
```

Expected: failure until Tasks 3-5 provide BFD membership and offline seeds.

- [ ] **Step 3: Write the failing summary test**

Create a temporary `runs.csv` containing warm-up and three formal rows for two
scenarios. Assert warm-up is excluded, `n=3`, mean/median/min/max are exact,
and sample standard deviation uses `statistics.stdev`.

Create `code/tests/experiments/test_output_aware_summary.py`:

```python
from __future__ import annotations

import csv
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.summarize_output_aware_bfd import summarize_runs


class OutputAwareSummaryTests(unittest.TestCase):
    def test_summary_uses_only_formal_rows(self) -> None:
        rows = [
            {
                "phase": "warmup",
                "scenario_id": "bfd",
                "tokens_per_s": "999",
            },
            *[
                {
                    "phase": "formal",
                    "scenario_id": scenario,
                    "tokens_per_s": str(value),
                }
                for scenario, values in (
                    ("bfd", [10, 20, 30]),
                    ("sequential", [5, 10, 15]),
                )
                for value in values
            ],
        ]

        summary = summarize_runs(rows, metrics=("tokens_per_s",))

        bfd = next(
            row
            for row in summary
            if row["scenario_id"] == "bfd"
        )
        self.assertEqual(bfd["n"], 3)
        self.assertEqual(bfd["mean"], 20.0)
        self.assertEqual(bfd["sample_std"], 10.0)
        self.assertEqual(bfd["p50"], 20.0)
        self.assertEqual(bfd["min"], 10.0)
        self.assertEqual(bfd["max"], 30.0)

    def test_summary_rejects_missing_or_invalid_formal_metrics(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing metric"):
            summarize_runs(
                [{"phase": "formal", "scenario_id": "bfd"}],
                metrics=("tokens_per_s",),
            )
        with self.assertRaisesRegex(ValueError, "numeric"):
            summarize_runs(
                [
                    {
                        "phase": "formal",
                        "scenario_id": "bfd",
                        "tokens_per_s": "bad",
                    }
                ],
                metrics=("tokens_per_s",),
            )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 4: Implement the small summary CLI**

Create `code/scripts/analysis/summarize_output_aware_bfd.py` using only `argparse`,
`csv`, `statistics`, and `pathlib`. Reject missing required columns and
non-numeric formal values. Sort rows by `scenario_id`, then by the fixed metric
order above. If there are no formal rows, exit nonzero with
`no formal rows found` and do not create an output file.

Use:

```python
#!/usr/bin/env python3
"""Summarize repeated output-aware BFD scenario runs."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Sequence


METRICS = (
    "tokens_per_s",
    "e2e_s",
    "request_e2e_s_p50",
    "request_e2e_s_p95",
    "request_e2e_s_p99",
    "batch_service_s_p99",
    "operator_invocations",
    "packing_budget_utilization_mean",
    "packing_budget_utilization_p95",
    "batch_estimated_cost_units_p95",
)


def summarize_runs(
    rows: Iterable[dict[str, str]],
    *,
    metrics: Sequence[str] = METRICS,
) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    formal_rows = 0
    for row in rows:
        if row.get("phase") != "formal":
            continue
        formal_rows += 1
        scenario_id = row.get("scenario_id", "")
        if not scenario_id:
            raise ValueError("formal row has no scenario_id")
        for metric in metrics:
            if metric not in row or row[metric] == "":
                raise ValueError(f"missing metric: {metric}")
            try:
                value = float(row[metric])
            except ValueError as exc:
                raise ValueError(
                    f"metric {metric} must be numeric"
                ) from exc
            grouped[(scenario_id, metric)].append(value)
    if formal_rows == 0:
        raise ValueError("no formal rows found")

    metric_order = {
        metric: index for index, metric in enumerate(metrics)
    }
    summary = []
    for (scenario_id, metric), values in sorted(
        grouped.items(),
        key=lambda item: (
            item[0][0],
            metric_order[item[0][1]],
        ),
    ):
        summary.append(
            {
                "scenario_id": scenario_id,
                "metric": metric,
                "n": len(values),
                "mean": statistics.mean(values),
                "sample_std": (
                    statistics.stdev(values)
                    if len(values) > 1
                    else 0.0
                ),
                "p50": statistics.median(values),
                "min": min(values),
                "max": max(values),
            }
        )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    with args.runs.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    try:
        summary = summarize_runs(rows)
    except ValueError as exc:
        parser.error(str(exc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "scenario_id",
            "metric",
            "n",
            "mean",
            "sample_std",
            "p50",
            "min",
            "max",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary)


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run focused and full verification**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\experiments\test_output_aware_summary.py -v
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_daft_ray_contract.py -v
.conda\pg-ai-profile\python.exe -m unittest discover -s code/tests -t code -p "test_*.py" -v
.conda\pg-ai-profile\python.exe -m compileall -q code/src code/scripts code/tests
git diff --check
```

Expected: the full suite passes, including real local Daft→Arrow→Ray task and
actor contracts; compile and diff checks are clean.

- [ ] **Step 6: Document and commit Task 6**

Document the summary command and metric schema.

```powershell
git add code/tests/scheduling/test_scheduling_daft_ray_contract.py code/scripts/analysis/summarize_output_aware_bfd.py code/tests/experiments/test_output_aware_summary.py code/scripts/README.md
git commit -m "test: validate output-aware BFD contracts"
```

---

### Task 7: Real 64-Row Gate, Seeded 512-Row Matrix, and 1024 Confirmation

**Files:**
- Create: `experiments/results/output_aware_bfd_20260726/gate_config.json`
- Create: `experiments/results/output_aware_bfd_20260726/formal_512_config.json`
- Create: `experiments/results/output_aware_bfd_20260726/confirm_1024_config.json`
- Create: `experiments/results/output_aware_bfd_20260726/manifest*.json`
- Create: `experiments/results/output_aware_bfd_20260726/*.csv`
- Create: `experiments/results/output_aware_bfd_20260726/README.md`
- Modify: `experiments/results/README.md`
- Modify: `experiments/plans/data_organization_batching.md`
- Modify: `code/README.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_OUTLINE.md` only if the repeated evidence changes a project conclusion
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Consumes the profiler, scenario runner, and summary CLI.
- Produces one real-component infrastructure gate and one randomized 512-row
  method matrix.

- [ ] **Step 1: Run a fresh preflight**

Run:

```powershell
.conda\pg-ai-profile\python.exe -m unittest discover -s code/tests -t code -p "test_*.py" -q
.conda\pg-ai-profile\python.exe -m compileall -q code/src code/scripts code/tests
git diff --check
```

Check the real service:

```powershell
Invoke-WebRequest http://localhost:8000/health -UseBasicParsing
Invoke-WebRequest http://localhost:8000/metrics -UseBasicParsing
```

Expected: tests and compilation pass; health returns HTTP 200; vLLM metrics
show zero running and waiting requests before the gate.

- [ ] **Step 2: Create the six-cell 64-row gate config**

Use one common offline configuration:

```json
{
  "schema_version": 1,
  "experiment_id": "output_aware_bfd_gate_20260726",
  "seed": 20260726,
  "warmup_runs_per_scenario": 0,
  "formal_repeats": 1,
  "common_args": [
    "--database-url", "postgresql://postgres:postgres@localhost:5432/ai_operator",
    "--total-rows", "64",
    "--db-fetch-rows", "64",
    "--ray-batch-rows", "16",
    "--operator", "ai_complete",
    "--executor", "ray_task",
    "--model-backend", "compatible_http",
    "--completion-endpoint-url", "http://localhost:8000/v1/completions",
    "--completion-model", "qwen2.5-1.5b",
    "--completion-max-tokens", "16",
    "--cost-model-id", "qwen2.5-1.5b",
    "--cost-tokenizer-id", "Qwen2.5-1.5B-Instruct",
    "--model-metrics-url", "http://localhost:8000/metrics",
    "--completion-request-timeout-s", "180",
    "--source-workload-name", "sharegpt_burstgpt",
    "--source-order", "doc_id",
    "--data-source", "daft_postgres",
    "--organizer", "daft",
    "--daft-runner", "native",
    "--organizer-partition-mode", "none",
    "--batching-policy", "token_budget",
    "--token-budget", "6144",
    "--scheduling-policy", "static",
    "--max-inflight", "8",
    "--writeback-mode", "none",
    "--request-slo-ms", "10000"
  ],
  "scenarios": [
    {"scenario_id": "seq_prompt", "args": ["--batching-policy", "token_budget", "--output-cost-mode", "prompt_only"]},
    {"scenario_id": "seq_fixed", "args": ["--batching-policy", "token_budget", "--output-cost-mode", "fixed_output_cap"]},
    {"scenario_id": "seq_trace", "args": ["--batching-policy", "token_budget", "--output-cost-mode", "trace_target_output"]},
    {"scenario_id": "bfd_prompt", "args": ["--batching-policy", "best_fit_token_budget", "--output-cost-mode", "prompt_only"]},
    {"scenario_id": "bfd_fixed", "args": ["--batching-policy", "best_fit_token_budget", "--output-cost-mode", "fixed_output_cap"]},
    {"scenario_id": "bfd_trace", "args": ["--batching-policy", "best_fit_token_budget", "--output-cost-mode", "trace_target_output"]}
  ]
}
```

The scenario-level `--batching-policy` value intentionally overrides the
common value because arguments are appended in scenario order and argparse
uses the last occurrence.

- [ ] **Step 3: Execute and audit the 64-row gate**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\experiments\run_ai_operator_scenarios.py --config experiments\results\output_aware_bfd_20260726\gate_config.json --profiler code\scripts\profiling\postgres_ai_operator_profile.py --python-executable .conda\pg-ai-profile\python.exe --output-dir experiments\results\output_aware_bfd_20260726\gate --health-url http://localhost:8000/health --metrics-url http://localhost:8000/metrics --idle-timeout-s 120
```

Fail and stop unless every scenario satisfies:

- `status=ok`;
- `server_version` and `pgvector_version` are non-empty;
- `vllm_request_success_delta=64`;
- request CSV contains 64 rows and 64 unique request/doc IDs;
- every request submission ID exists in the submission CSV;
- every request uses `request_time_origin=offline_job_start`;
- every lifecycle timestamp is finite and ordered;
- BFD rows record `packing_algorithm=best_fit_decreasing`;
- all six rows record `packing_scope=organizer_input`;
- `packing_input_rows=64`;
- vLLM ends with running=waiting=0.

Across all six scenarios, require the same sorted `(doc_id, prompt_tokens)`
rows. A row-count match without document-set equality is not sufficient.

The gate proves infrastructure only. Do not rank policies from these six
single runs.

- [ ] **Step 4: Create and run the seeded 512-row matrix**

Copy the gate config to `formal_512_config.json` and change exactly:

```json
{
  "experiment_id": "output_aware_bfd_512_20260726",
  "total_rows": 512,
  "db_fetch_rows": 512,
  "warmup_runs_per_scenario": 1,
  "formal_repeats": 3
}
```

Represent `total_rows` and `db_fetch_rows` by editing their values in
`common_args`; do not add duplicate flags. Execute the randomized scenario
runner only after the 64-row audit passes.

Run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\experiments\run_ai_operator_scenarios.py --config experiments\results\output_aware_bfd_20260726\formal_512_config.json --profiler code\scripts\profiling\postgres_ai_operator_profile.py --python-executable .conda\pg-ai-profile\python.exe --output-dir experiments\results\output_aware_bfd_20260726\formal_512 --health-url http://localhost:8000/health --metrics-url http://localhost:8000/metrics --idle-timeout-s 120
```

- [ ] **Step 5: Summarize and audit the 512-row evidence**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\analysis\summarize_output_aware_bfd.py --runs experiments\results\output_aware_bfd_20260726\formal_512\runs.csv --output experiments\results\output_aware_bfd_20260726\formal_512_summary.csv
```

Audit all three formal repeats per scenario for:

- exactly 512 successful requests;
- exactly-once request/doc coverage;
- request→submission foreign keys;
- `organizer_input` scope;
- matching model/tokenizer/cost-source fields;
- no failed request;
- final vLLM idle state;
- non-empty resource and submission traces;
- request E2E P50/P95/P99 matching recomputation within `1e-6`.

For every repeat index, require all six scenarios to have an identical sorted
`(doc_id, prompt_tokens)` request set and exactly 512 rows. Do not compare a
failed, truncated, or different-input run with a complete run. The randomized
schedule may change execution order but never the source slice.

Compare BFD with sequential only within the same output-cost mode. Report
packing utilization, submission count, tokens/s, run E2E, request E2E
P50/P95/P99, and batch service P99. Treat trace-cost GPU differences as
exploratory sensitivity evidence because the BurstGPT target is not paired
with the Qwen output.

- [ ] **Step 6: Write the seven-part result report**

The README must contain:

1. experiment setting;
2. design and randomized schedule;
3. rigor/invariant audit;
4. CSV-backed measurements;
5. facts, inferences, unresolved questions, and prohibited claims;
6. meaning for data organization and future multimodal/model replacement;
7. the exact gate for a later 1024 confirmation.

- [ ] **Step 7: Run the selected 1024-row confirmation**

After the 512 audit selects one baseline and one adaptive configuration,
create `confirm_1024_config.json` with the same model, tokenizer, prompts,
generation cap, token budget, sampling interval, GPU, and all non-policy
settings. Change only:

```json
{
  "experiment_id": "output_aware_bfd_1024_20260726",
  "total_rows": 1024,
  "db_fetch_rows": 1024,
  "warmup_runs": 1,
  "repeats": 3
}
```

Run only those two selected configurations. Verify exactly 1024 identical
document IDs per scenario and three successful formal repeats. Report this as
scale confirmation; do not mix 1024 rows with the 512-row six-cell effect-size
calculation.

Do not say BFD or output-aware estimation is better unless all three repeated
runs and the reported metrics support that statement. Do not call
`trace_target_output` an oracle.

- [ ] **Step 7: Update project records and run final verification**

Update the listed project files and register every new artifact. Because
`experiments/plans/data_organization_batching.md` is a knowledge file, remind
the user about Wiki synchronization at session close; do not sync
automatically.

Run:

```powershell
.conda\pg-ai-profile\python.exe -m unittest discover -s code/tests -t code -p "test_*.py" -q
.conda\pg-ai-profile\python.exe -m compileall -q code/src code/scripts code/tests
git diff --check
git status --short
```

Expected: all tests and compilation pass; only intended code, tests,
documentation, and validated result artifacts are changed; `.superpowers/`
remains untracked.

- [ ] **Step 8: Commit validated code and evidence**

Commit the gate separately from the repeated matrix:

```powershell
git commit -m "experiment: validate output-aware BFD infra"
git commit -m "experiment: compare output-aware BFD packing"
```

Stage only the files belonging to each successful result. Keep the branch
unmerged.

## Plan Self-Review

- **Spec coverage:** Tasks 1-2 implement shared cost semantics and a
  deterministic engine-independent BFD core. Task 3 provides one Arrow/Daft
  implementation. Task 4 covers replay reuse, scope, provenance, and packing
  metrics. Task 5 closes the offline lifecycle gap. Task 6 covers real
  framework contracts and reusable summaries. Task 7 implements the 64 and
  512 gates.
- **Prompt/model/modality replacement:** The only reusable packing boundary is
  `PackItem.cost_units`. Prompt and model changes remain configuration/data
  changes; a future real multimodal caller supplies its scalar adapter without
  modifying the packer or Ray scheduler.
- **Type consistency:** `OutputCostMode`, `PackItem.cost_units`,
  `capacity`, `request_time_origin`, and the packing metric names are identical
  across all tasks.
- **Compatibility:** Default fixed-output sequential behavior remains intact.
  Arrival replay never invokes BFD. Backend `completion_max_tokens` is passed
  unchanged.
- **Claim discipline:** Unpaired BurstGPT targets are trace metadata, the
  64-row gate is infrastructure evidence, and the 512 matrix is the first
  repeated performance evidence.
- **Scale comparability:** All policy conclusions come from the shared
  512-document matrix. The separate 64-document gate is never mixed into a
  performance delta.
- **No omissions:** Every task names exact files, interfaces, RED/GREEN
  commands, expected outcomes, and commit boundaries.
