# Daft PostgreSQL Data Entry with Existing Writeback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Daft-backed PostgreSQL data entry path to the existing profiling script while keeping current `none/json_text/pgvector` writeback unchanged.

**Execution status:** Completed on 2026-07-17 in the main workspace at the user's request. Verified with unit tests, dry-runs, py_compile, and local PG18.4 smoke rows in `tmp/postgres_daft_source_e2e.csv`.

**Architecture:** Introduce a `DataSource` boundary before the existing organizer boundary. `PostgresArrowSource` preserves the current psycopg-to-Arrow path, while `DaftPostgresSource` reads PostgreSQL rows through Daft and returns Arrow tables that the existing `ArrowOrganizer` / `DaftOrganizer` can batch. Lance remains a future sink option and is not implemented in this plan.

**Tech Stack:** Python 3.10, psycopg, PyArrow `>=16,<25`, Daft 0.7.20, unittest, existing `postgres_ai_operator_profile.py`.

**Runtime note:** Daft `read_sql` against PostgreSQL required `sqlglot` and `connectorx`. `sqlglot` was installed from PyPI; `connectorx-0.4.5-cp310-none-win_amd64.whl` was installed from `tmp/` after manual download.

## Global Constraints

- Follow `karpathy-guidelines`: minimal code, surgical changes, explicit assumptions, verifiable success criteria.
- Default behavior must remain current PostgreSQL psycopg fetch plus Arrow organizer.
- Existing writeback modes stay unchanged: `none`, `json_text`, `pgvector`.
- Do not add LanceDB dependency or `--writeback-mode lance` in this implementation.
- Daft runner is process-global; configure it once and fail clearly if a process tries to switch runner.
- Results from smoke runs go under `tmp/`, not formal `motivation/results/`.
- Do not claim performance conclusions from smoke runs.

---

## File Structure

- Create `code/src/sources.py`: Data source abstraction and two implementations.
- Modify `code/scripts/postgres_ai_operator_profile.py`: add `--data-source arrow_postgres|daft_postgres`, use the source in the fetch loop, keep existing executor and writeback code.
- Create `code/tests/test_sources.py`: unit tests for source query generation and Daft runner reuse behavior that can run without a live PostgreSQL server where possible.
- Modify `code/README.md`: document the new data source switch and keep Lance as future extension.
- Modify `code/scripts/README.md`: update flow diagram from `fetch_record_batch` to `DataSource`.
- Modify `PROJECT_INDEX.md` and `PROJECT_LOG.md`: register the new source module and scope.

---

### Task 1: DataSource Boundary

**Files:**
- Create: `code/src/sources.py`
- Test: `code/tests/test_sources.py`

**Interfaces:**
- Consumes: `pyarrow as pa`
- Produces:
  - `SourceConfig(limit: int, offset: int) -> dataclass`
  - `SourceBatch(table: pa.Table | None, metrics: dict[str, float]) -> dataclass`
  - `PostgresArrowSource.fetch(conn, config: SourceConfig) -> SourceBatch`
  - `DaftPostgresSource.fetch(database_url: str, config: SourceConfig) -> SourceBatch`
  - `make_source(name: Literal["arrow_postgres", "daft_postgres"])`

- [ ] **Step 1: Write the failing unit test for Arrow source query behavior**

Add to `code/tests/test_sources.py`:

```python
from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.sources import SourceConfig, postgres_documents_query


class SourceTests(unittest.TestCase):
    def test_postgres_documents_query_uses_limit_and_offset(self) -> None:
        sql, params = postgres_documents_query(SourceConfig(limit=128, offset=256))

        self.assertIn("SELECT doc_id, tenant_id, category, text", sql)
        self.assertIn("ORDER BY doc_id", sql)
        self.assertEqual(params, (128, 256))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the failing test**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_sources.py
```

Expected: FAIL with `ModuleNotFoundError: No module named 'src.sources'`.

- [ ] **Step 3: Implement the source dataclasses and query helper**

Create `code/src/sources.py`:

```python
"""Data source backends for PostgreSQL-driven AI operator profiles."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa


SourceName = Literal["arrow_postgres", "daft_postgres"]


@dataclass(frozen=True)
class SourceConfig:
    limit: int
    offset: int


@dataclass(frozen=True)
class SourceBatch:
    table: pa.Table | None
    metrics: dict[str, float]


def postgres_documents_query(config: SourceConfig) -> tuple[str, tuple[int, int]]:
    if config.limit <= 0:
        raise ValueError("limit must be positive")
    if config.offset < 0:
        raise ValueError("offset must be non-negative")
    return (
        """
        SELECT doc_id, tenant_id, category, text
        FROM documents
        ORDER BY doc_id
        LIMIT %s OFFSET %s
        """,
        (config.limit, config.offset),
    )
```

- [ ] **Step 4: Run the test**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_sources.py
```

Expected: PASS.

---

### Task 2: Implement PostgresArrowSource

**Files:**
- Modify: `code/src/sources.py`
- Test: `code/tests/test_sources.py`

**Interfaces:**
- Consumes: `SourceConfig`, `postgres_documents_query`
- Produces: `PostgresArrowSource.fetch(conn, config) -> SourceBatch`

- [ ] **Step 1: Add fake cursor test for PostgresArrowSource**

Append to `code/tests/test_sources.py`:

```python
from src.sources import PostgresArrowSource


class FakeCursor:
    def __init__(self):
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.executed = (sql, params)

    def fetchall(self):
        return [
            (1, 1, "cat_a", "hello world"),
            (2, 1, "cat_a", "goodbye world"),
        ]


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()

    def cursor(self):
        return self.cursor_obj


class PostgresArrowSourceTests(unittest.TestCase):
    def test_fetch_returns_arrow_table(self) -> None:
        conn = FakeConnection()
        source = PostgresArrowSource()

        batch = source.fetch(conn, SourceConfig(limit=2, offset=0))

        self.assertEqual(batch.table.num_rows, 2)
        self.assertEqual(batch.table.column_names, ["doc_id", "tenant_id", "category", "text"])
        self.assertGreaterEqual(batch.metrics["db_fetch_s"], 0.0)
        self.assertGreaterEqual(batch.metrics["arrow_build_s"], 0.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_sources.py
```

Expected: FAIL with `ImportError` or `AttributeError` for `PostgresArrowSource`.

- [ ] **Step 3: Implement PostgresArrowSource**

Add to `code/src/sources.py`:

```python
class PostgresArrowSource:
    name = "arrow_postgres"

    def fetch(self, conn, config: SourceConfig) -> SourceBatch:
        fetch_start = time.perf_counter()
        sql, params = postgres_documents_query(config)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        db_fetch_s = time.perf_counter() - fetch_start
        if not rows:
            return SourceBatch(table=None, metrics={"db_fetch_s": db_fetch_s, "arrow_build_s": 0.0})

        arrow_start = time.perf_counter()
        columns = list(zip(*rows, strict=True))
        table = pa.table(
            {
                "doc_id": pa.array(columns[0], type=pa.int64()),
                "tenant_id": pa.array(columns[1], type=pa.int32()),
                "category": pa.array(columns[2], type=pa.string()),
                "text": pa.array(columns[3], type=pa.string()),
            }
        )
        arrow_build_s = time.perf_counter() - arrow_start
        return SourceBatch(table=table, metrics={"db_fetch_s": db_fetch_s, "arrow_build_s": arrow_build_s})
```

- [ ] **Step 4: Run tests**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_sources.py
```

Expected: PASS.

---

### Task 3: Implement DaftPostgresSource

**Files:**
- Modify: `code/src/sources.py`
- Test: `code/tests/test_sources.py`

**Interfaces:**
- Consumes: `database_url: str`, `SourceConfig`
- Produces: `DaftPostgresSource.fetch(database_url, config) -> SourceBatch`

- [ ] **Step 1: Add test for Daft source SQL construction**

Append to `code/tests/test_sources.py`:

```python
from src.sources import daft_sql_query


class DaftSqlTests(unittest.TestCase):
    def test_daft_sql_query_uses_literal_limit_offset(self) -> None:
        sql = daft_sql_query(SourceConfig(limit=32, offset=64))

        self.assertIn("SELECT doc_id, tenant_id, category, text", sql)
        self.assertIn("LIMIT 32 OFFSET 64", sql)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_sources.py
```

Expected: FAIL with `ImportError` for `daft_sql_query`.

- [ ] **Step 3: Implement Daft SQL helper and source skeleton**

Add to `code/src/sources.py`:

```python
def daft_sql_query(config: SourceConfig) -> str:
    if config.limit <= 0:
        raise ValueError("limit must be positive")
    if config.offset < 0:
        raise ValueError("offset must be non-negative")
    return (
        "SELECT doc_id, tenant_id, category, text "
        "FROM documents "
        "ORDER BY doc_id "
        f"LIMIT {config.limit} OFFSET {config.offset}"
    )


class DaftPostgresSource:
    name = "daft_postgres"

    def fetch(self, database_url: str, config: SourceConfig) -> SourceBatch:
        import daft

        sql = daft_sql_query(config)
        read_start = time.perf_counter()
        df = daft.read_sql(sql, database_url)
        table = df.to_arrow()
        read_s = time.perf_counter() - read_start
        if table.num_rows == 0:
            return SourceBatch(table=None, metrics={"db_fetch_s": read_s, "arrow_build_s": 0.0})
        return SourceBatch(table=table, metrics={"db_fetch_s": read_s, "arrow_build_s": 0.0})
```

- [ ] **Step 4: Run unit tests**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_sources.py
```

Expected: PASS.

- [ ] **Step 5: Add source factory**

Add to `code/src/sources.py`:

```python
def make_source(name: SourceName):
    if name == "arrow_postgres":
        return PostgresArrowSource()
    if name == "daft_postgres":
        return DaftPostgresSource()
    raise ValueError(f"Unknown source: {name}")
```

Add to `code/tests/test_sources.py`:

```python
from src.sources import make_source


class SourceFactoryTests(unittest.TestCase):
    def test_make_source_returns_named_source(self) -> None:
        self.assertEqual(make_source("arrow_postgres").name, "arrow_postgres")
        self.assertEqual(make_source("daft_postgres").name, "daft_postgres")
```

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_sources.py
```

Expected: PASS.

---

### Task 4: Wire DataSource into Main Script

**Files:**
- Modify: `code/scripts/postgres_ai_operator_profile.py`
- Test: existing dry-run and smoke commands

**Interfaces:**
- Consumes:
  - `make_source(name)`
  - `SourceConfig(limit, offset)`
  - existing `make_organizer(...)`
- Produces:
  - CLI flag `--data-source arrow_postgres|daft_postgres`
  - CSV fields `data_source`, `source_fetch_s`

- [ ] **Step 1: Add CLI argument and dry-run output**

Modify imports:

```python
from src.sources import SourceConfig, make_source
```

Add parser argument near `--db-fetch-rows`:

```python
parser.add_argument("--data-source", choices=["arrow_postgres", "daft_postgres"], default="arrow_postgres")
```

Add dry-run output field:

```python
"data_source": args.data_source,
```

- [ ] **Step 2: Run dry-run**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run --data-source daft_postgres --organizer daft `
  --output tmp\postgres_profile_dry_run.csv
```

Expected: JSON includes `"data_source": "daft_postgres"`.

- [ ] **Step 3: Replace fetch loop with source fetch**

In `run_once`, before the loop:

```python
source = make_source(args.data_source)
```

Replace:

```python
batch, fetch_metrics = fetch_record_batch(conn, args.db_fetch_rows, offset)
```

with:

```python
source_config = SourceConfig(limit=args.db_fetch_rows, offset=offset)
if args.data_source == "arrow_postgres":
    source_batch = source.fetch(conn, source_config)
else:
    source_batch = source.fetch(args.database_url, source_config)
table = source_batch.table
fetch_metrics = source_batch.metrics
```

Replace uses of `batch` in that block:

```python
if table is None:
    break
db_fetch_s += fetch_metrics["db_fetch_s"]
arrow_build_s += fetch_metrics["arrow_build_s"]
offset += table.num_rows
remaining = args.total_rows - processed_rows
if table.num_rows > remaining:
    table = table.slice(0, remaining)
organized = organizer.organize(table)
processed_rows += table.num_rows
```

- [ ] **Step 4: Add CSV fields**

In the final row, add:

```python
"data_source": args.data_source,
```

Keep existing `db_fetch_s` and `arrow_build_s` fields. For `daft_postgres`, `db_fetch_s` represents Daft SQL read plus materialization to Arrow, and `arrow_build_s` remains `0.0`.

- [ ] **Step 5: Run dry-runs**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run --data-source arrow_postgres --organizer arrow `
  --output tmp\postgres_profile_dry_run.csv
```

Expected: JSON includes `"data_source": "arrow_postgres"`.

Run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run --data-source daft_postgres --organizer daft `
  --output tmp\postgres_profile_dry_run.csv
```

Expected: JSON includes `"data_source": "daft_postgres"`.

---

### Task 5: Run Minimal PostgreSQL E2E Smokes

**Files:**
- No code changes
- Output: `tmp/postgres_daft_source_e2e.csv`

**Interfaces:**
- Consumes: `postgres_ai_operator_profile.py` CLI
- Produces: verified smoke rows for both source paths

- [ ] **Step 1: Run baseline source**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 64 --total-rows 64 `
  --db-fetch-rows 32 --ray-batch-rows 16 `
  --embedding-dim 16 --executor python `
  --data-source arrow_postgres --organizer arrow `
  --strategy coalesced --writeback-mode none `
  --experiment-id source_arrow_smoke `
  --output tmp\postgres_daft_source_e2e.csv
```

Expected: status `ok`, `data_source=arrow_postgres`, `total_rows=64`, `object_count=4`.

- [ ] **Step 2: Run Daft source**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 64 --total-rows 64 `
  --db-fetch-rows 32 --ray-batch-rows 16 `
  --embedding-dim 16 --executor python `
  --data-source daft_postgres --organizer daft --daft-runner native `
  --strategy coalesced --writeback-mode none `
  --experiment-id source_daft_smoke `
  --output tmp\postgres_daft_source_e2e.csv
```

Expected: status `ok`, `data_source=daft_postgres`, `organizer=daft`, `total_rows=64`, `object_count=4`.

- [ ] **Step 3: Inspect output**

Run:

```powershell
Get-Content tmp\postgres_daft_source_e2e.csv | Select-Object -Last 3
```

Expected: header plus the two smoke rows. Do not copy this file into formal result directories.

---

### Task 6: Documentation and Lance Boundary

**Files:**
- Modify: `code/README.md`
- Modify: `code/scripts/README.md`
- Modify: `PROJECT_INDEX.md`
- Modify: `PROJECT_LOG.md`

**Interfaces:**
- Consumes: implemented CLI flags and source module
- Produces: documented current scope and future Lance boundary

- [ ] **Step 1: Update code README**

Add this exact note under the PostgreSQL profile section:

```markdown
Data entry can use either the baseline psycopg path or Daft:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run --data-source daft_postgres --organizer daft `
  --output tmp\postgres_profile_dry_run.csv
```

Current writeback remains `none`, `json_text`, or `pgvector`. Lance is a future
optional sink backend and is not part of this implementation.
```

- [ ] **Step 2: Update script README**

Replace the flow block with:

```text
PostgreSQL documents/job table
  -> DataSource (arrow_postgres or daft_postgres)
  -> ArrowOrganizer / DaftOrganizer
  -> FakeEmbeddingActor.embed
  -> submit_with_backpressure
  -> write_embeddings / finish_job
  -> append_metrics
```

Add CLI bullets:

```markdown
- `--data-source arrow_postgres|daft_postgres`
- `--organizer arrow|daft`
- `--organizer-partition-mode none|into_partitions|repartition`
- `--organizer-partitions`
- `--daft-runner native|ray`
```

- [ ] **Step 3: Update project index and log**

In `PROJECT_INDEX.md`, add:

```markdown
| `code/src/sources.py` | PostgreSQL data source backends: psycopg/Arrow baseline and Daft SQL entry | Switching data entry paths |
| `code/tests/test_sources.py` | Unit tests for source query construction and source factory | Modifying data source behavior |
```

In `PROJECT_LOG.md`, add a 2026-07-17 entry:

```markdown
## 2026-07-17 Daft PostgreSQL data entry plan/implementation

- Added the plan and implementation path for `--data-source daft_postgres`.
- Current writeback remains `none/json_text/pgvector`.
- Lance is documented as a future optional sink using LanceDB Python API, not implemented in this step.
```

- [ ] **Step 4: Run final checks**

Run:

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_sources.py
.conda\pg-ai-profile\python.exe code\tests\test_organizers.py
$env:PYTHONPYCACHEPREFIX='tmp\pycache'; .conda\pg-ai-profile\python.exe -m py_compile code\src\sources.py code\src\organizers.py code\scripts\postgres_ai_operator_profile.py
```

Expected: all tests pass; `py_compile` exits with code `0`.

---

## Self-Review

- Spec coverage: Daft as PostgreSQL data entry is covered by Tasks 1-5. Existing writeback is preserved by Task 4 and documented in Task 6. Lance is explicitly future-only in Global Constraints and Task 6.
- Placeholder scan: The plan contains no unfinished markers or unspecified implementation steps.
- Type consistency: `SourceConfig`, `SourceBatch`, `PostgresArrowSource`, `DaftPostgresSource`, and `make_source` are defined before use. The main script consumes Arrow tables from sources and passes them to existing organizers.
