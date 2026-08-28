# `semloom_pg` capability spike

`semloom_pg` is the first `REL_18_3` capability slice from
`experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`. It proves that a
fail-closed SQL marker can be lowered to a planner-visible `CustomPath`/`CustomScan` with an ordinary
PostgreSQL child plan. It is not the final semantic plan/task/result protocol and does not call an
HTTP, Ray, vLLM, or SemLoom scheduling backend.

The current supported query shape is deliberately narrow:

- one top-level `ai_semantic.map(text)` in a single-table `SELECT` target list;
- ordinary child filters and projections;
- forward execution with child order preserved;
- `LIMIT`, including `LIMIT 0` and early stop after one row;
- an in-process recording transform that returns `recorded:<input>` and preserves SQL `NULL`.

The planner rejects joins, inheritance, subqueries, CTEs, aggregates, grouping, windows, `DISTINCT`,
sorting, set operations, row locks, set-returning targets, nested marker use, and marker use outside
the target list. The executor rejects backward scan, mark/restore, rescan, and EPQ. Parallel execution
is disabled. `INSERT ... SELECT`, query-cancel TAP coverage, provider sessions, wire framing, retries,
and external model calls remain pending; this slice must not be described as a complete database AI
operator.

The planner hook must be loaded before a statement containing the marker is planned. The regression
script loads the library in its session. A persistent deployment must put `semloom_pg` in
`session_preload_libraries` or `shared_preload_libraries`; merely installing the SQL function is not
sufficient.

Build and test against an exact `REL_18_3` installation. The Makefile rejects a `pg_config` whose
reported version is not 18.3:

```bash
make PG_CONFIG=/path/to/postgresql-18.3/bin/pg_config
make PG_CONFIG=/path/to/postgresql-18.3/bin/pg_config install
make PG_CONFIG=/path/to/postgresql-18.3/bin/pg_config installcheck
```

`installcheck` uses the standard PGXS regression harness and expects an already running PostgreSQL
18.3 server. Set `PGHOST`, `PGPORT`, and `PGUSER` for that server as needed. Existing PostgreSQL 18.4
deployments are compatibility/rehearsal environments and do not substitute for this qualification.
