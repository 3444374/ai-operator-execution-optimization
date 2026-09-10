# PostgreSQL resource measurement tools

## Database-source queries

`query_workloads.py` prepares separate raw/reference files; `query_tables.py` installs and verifies
an immutable PG relation. `query_inputs.py` constructs messages from raw columns, preserving original
Movie review IDs separately from unique row occurrences. `query_config.py` declares one arm/task;
`query_runner.py` coordinates execution and post-query evaluation. `query_execution.py` owns the PG,
direct, LOTUS and Ray lifecycles; `query_evaluation.py` verifies actual requests, outputs and decisions.
`query_supervisor.py` bounds owned worker lifetime and closes its shared POST allocation on exit.

For PG Map, `pg_total_budget=true` selects total retained bytes instead of equal per-row reservations;
`pg_staging_bytes` controls the separate single-row preparation area. `window_memory.py` checks the
producer's per-operator memory trace and final release. Its summed peaks are not concurrent query RSS.
[Controlled and real-model checks](../../../../experiments/results/postgresql/pg_window_budget_20260910/README.md).

Use `query_cli.py` through [database_queries.py](../../../scripts/experiments/database_queries.py).
PG-source direct is a bounded execution reference; Ray SQL/HTTP and original SemBench LOTUS programs
retain native execution ownership. These entries have controlled HTTP evidence, with real model quality
and performance pending. [Scope, tests and failure history](../../../../experiments/results/postgresql/database_queries_20260910/README.md).

## Resource collectors

These tools observe the existing synchronous SemMap fixture path. Production SQL, planner, provider and
wire semantics remain in their existing modules. The [Map engineering contract](../../../../experiments/plans/postgresql_semmap_generation_contract.md)
owns the implementation and verification plan.

| Module | Responsibility |
|---|---|
| `resource_lifecycle.py` | Immutable settings, required phases and pure final assessment |
| `resource_phase.py` | Baseline, operation checkpoint, cleanup sampling and phase evidence |
| `semmap_resource_runner.py` | Exclusive run directory, build, isolated PG cases and CLI |
| `resource_qualification.py` | Sampled peak and cleanup policies with versioned thresholds |
| `provider_session_attribution.py` | Strict session/task replay, scoped socket attribution and residual identity checks |
| `semmap_resource_gateway_observer.py` | Fixture CLI composing the shared observer with an optional bounded handshake barrier |
| `resource_client_v3.c` | Parameterized single-row libpq fixture consumer with an explicit exit barrier |
| `runtime_helpers.py` | Shared owned-process and isolated PostgreSQL helpers, also used by choice checks |

Filter and Map share [session observation](../gateway_observer.py) and the
[configured request ledger](../attempt_ledger.py). The fixed-model observation CLI remains
`src.experiments.choice_gateway_observer` for compatibility; it accepts a budget ID/limit and optional
`--session-events` for either operator. Historical result scripts are evidence snapshots, not runtime dependencies.
The fixture runner accepts `--pg-user` and `--pg-port`; its streaming client uses the actual connection's
host, port, role and database. Model endpoint/identity/timeout remain in repository-external fixed-model JSON.

Collector and recorder primitives live in `src/observability/process_resources/`. Test categories are
lifecycle, collection, attribution, policy, observer and CLI; the old `audit_round2` source-string checks
have been replaced by observable behavior checks. Diagnostic mode is 1×100 and never grants formal
qualification. See [CLI usage](../../../scripts/README.md) and [current evidence](../../../../experiments/results/postgresql/semmap_resource_lifecycle_20260906/README.md).
