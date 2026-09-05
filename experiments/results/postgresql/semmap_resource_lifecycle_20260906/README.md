# SemMap resource measurement lifecycle repair — 2026-09-06

## Purpose and scope

This is engineering evidence for the resource-measurement tools supporting PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化.
The source baseline is `semmap-resource-v2@e5f4dd12`; changes are on `codex/semmap-resource-repair`.
Production PostgreSQL, Map semantics, provider and wire v5 are unchanged. The current plan is
[Map contract §8.4.3](../../../plans/postgresql_semmap_generation_contract.md).

This directory distinguishes controlled local execution from Linux/PG execution. A controlled test's
`valid/passed` exercises the assessment function with synthetic process observations; it is not a PostgreSQL
resource qualification, workload run or performance result. No real model is requested.

## Local code verification

The initial baseline checks ran 33 process tests (2 Linux skips), 46 resource tests (3 import errors because
local psycopg was absent), and 12 attribution tests. Five added behavioral counterexamples then produced
3 failures and 2 errors: changing threads/active sockets accepted as baseline, initial/final sampling
exceptions escaping, and exception bodies persisted. No dependency was installed to mask those failures.

The repaired local suites currently run 115 checks: 113 pass, 2 skip because macOS lacks Linux procfs/SO_PEERCRED.
[Machine-readable counts](raw/local-tests.json) and per-group logs preserve the actual result.
The runner now loads psycopg only in the actual PG adapter, so controlled orchestration requires no database package.
Python compileall and diff whitespace checks pass. Old v1 replay still reports observed 3 / limit 2 and a failed verdict.

Controlled tests execute real file creation and subprocess/control-file effects, the actual phase recorder,
pure policy and final aggregate. They cover normal completion, operation failure, incomplete observations,
old-directory hash preservation, cleanup before client exit, and intermediate/absent failure retention.

## Implementation and measurement identity

New runs use `semloom.pg.resource.v2.1`, revision `phase-lifecycle-1`; old v1/v2 artifacts and verdicts are unchanged.
A sample tick is a sequential observation batch with time ranges, not an atomic snapshot. Failed FD read attempts
are retained separately from a later consistent observation; unresolved partial/invalid observations prevent qualification.
Socket-specific peaks remain client ≤1, accepted ≤1, combined ≤2; cleanup requires original live process identities,
exact FD/thread counts, matching FD identities, no attributed socket residual, and stable event drain.
RSS peak/end limits remain PG 16/8 MiB and gateway 32/16 MiB.

The implementation separates lifecycle progress, measurement validity, known failures and final qualification.
All required phases feed the same aggregate; diagnostic output uses a separate diagnostic status and cannot qualify.
A socket's peer PID alone does not identify the backend FD: attribution additionally requires a unique new resource
and its accepted peer in the same usable observation batch. Scope is the synchronous single-session experiment.

## Target environment and limits

Linux checks at 836448ab pass 126/126, including the real two-process socket/cleanup integration and 11 choice checks.
The private PG18.3 extension build passes -O2 -Werror. An earlier added -Wextra attempt failed on PostgreSQL
header unused-parameter warnings; its log is retained.

The first actual 1×100 diagnostic at 836448ab has valid passing pressure and cancel/recovery assessments.
The instantaneous disconnect window has no sampled tick, so it is not evaluated; subsequent phases/cases
are skipped. Formal qualification remains blocked. Revision phase-lifecycle-2 registers a separate test-only
handshake barrier for single-query fault/recovery connections before a new run. Pressure timing is unchanged.
The old diagnostic README is historical evidence at `a4119e73`, with a corrected actual workload of 3×2000; its
raw was read independently during this repair. The summary confirms expected100/observed6000 and
expected101/observed6001 mismatches. Its stress cleanup trace contains two invalid process records,
and the reported checksum/source/environment manifests were not found inside that artifact root.
The old gateway-exit correctness record also reports observed XX000 versus expected08006. It does not qualify the new implementation.

Formal 3×2000 is blocked and has not been authorized. This repair cannot establish completion of the full Map
engineering work, model quality, asynchronous scheduling or performance benefits.
