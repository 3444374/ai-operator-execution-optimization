# Filter→Map binding verification

## Purpose and implementation

This engineering check supports the shared PostgreSQL foundation for data organization and execution
scheduling research. On baseline `19326609`, the development branch `codex/semantic-call-binding`
now accepts one top-level relational Filter followed by one generated Map, as specified in the
[call/binding design](../../../plans/postgresql_call_binding_design.md). Existing single operators and
two-Filter AND remain controls. No new scheduling method or model-quality claim is evaluated.

The new internal carrier separates semantic fields, function identity and tuple routing. The Map node
uses a native PostgreSQL ExprState after pulling its child, including LIMIT/OFFSET. Ordinary outputs
keep their PostgreSQL evaluation placement; the generated result occupies an independent internal column.
The implementation reuses existing operator machines, query runtimes, provider sessions and gateway.
Legacy carriers retain their formats. SQL signatures, semantic digests and wire versions are unchanged.

## Setup and execution

A Git sparse checkout supplied the baseline on the user-authorized server; only self-owned source and
synthetic fixtures were transferred. The first full worktree checkout stalled on lazy GitHub fetching;
it was stopped before compilation and replaced with an explicit sparse checkout using platform network
configuration. No test result came from that preparation attempt.

Core runtime preflight had passed in the same session. Source, clusters and artifacts were placed on the
data disk, with an independent PostgreSQL 18.3 software prefix. Existing services were unchanged. Driver
Python and PostgreSQL compiler dependencies were reused. Every PG attempt used a fresh UTF8 cluster,
C.utf8, a private Unix socket, a selected local port, and strict `-O2 -Werror` compilation. Model budget
and actual real-model requests were zero. Fixture gateway capacity was four connections and one active
request. This is a correctness check; no performance tuning, quality experiment or resource qualification
was performed. A failing check would prevent completion; none of the four PG attempts failed.

```bash
python -m unittest discover -s code/tests/postgres -t code -p 'test_*.py'
python -m unittest discover -s code/tests/execution_provider -t code -p 'test_*.py'
python -m unittest discover -s code/tests/experiments -t code -p 'test_gateway*py'
# PG_CONFIG points to the independent PostgreSQL 18.3 installation:
make clean
make -j4 COPT='-O2 -Werror'
make install COPT='-O2 -Werror'
PG_TEST_INITDB_EXTRA_OPTS='--encoding=UTF8' make installcheck COPT='-O2 -Werror'
```

## Results and provenance

| Check | Observed result |
|---|---|
| Local Python/C | 115/115, [log](raw/local-r3.log) |
| Final Linux Python/C and gateway | 115 + 17 + 6 = 138/138, [summary](raw/final-python-summary.json) |
| PG attempt 1 | Regression 1/1, 10 TAP files / 1875 checks; [summary](raw/r1-pg-verification.json) |
| PG attempt 2 | Adds RLS, snapshot, cancellation and revocation: 1889 checks; [summary](raw/r2-pg-verification.json) |
| PG attempt 3 | Adds carrier copy/text roundtrip and malformed-node rejection: 1908 checks; [summary](raw/r3-pg-verification.json) |
| PG attempt 4 | Adds native hook counts and input EXECUTE checks: regression 1/1, 1910/1910 checks; [summary](raw/r4-pg-verification.json), [TAP](raw/r4-installcheck.log) |
| Source identity | All 611 non-Markdown source/test files match local bytes; [hashes](raw/server-source-hashes.json) |
| Build/install identity | Matching SHA-256 in every PG summary |
| Cleanup | All test clusters stopped; final Linux audit reports zero owned service PIDs |

Each attempt has its own [source delta](raw/delta-r1.json) ([2](raw/delta-r2.json),
[3](raw/delta-r3.json), [4](raw/delta-r4.json)). The final delta contains 19 source/test files, verified
before Linux tests. These identify tested working-tree snapshots above the stated baseline, rather than
pretending the candidate was already committed. Public logs are redacted and trailing whitespace is
normalized; the [artifact index](raw/artifact-index.json) records both hashes. Private originals remain
on the server. Full PG logs from earlier attempts were preserved before the next run.

The final 45 composition checks cover recording/exact/choice Filter, independent input/result values,
NULL and empty output, discarded input expressions, LIMIT zero/early stop, OFFSET, repeated custom and
generic plans, INSERT success/failure atomicity, RLS, repeatable-read snapshots, cancellation and recovery,
marker revocation, input function permissions and native execution-hook counts. Identical VOLATILE
ordinary and Map input expressions with OFFSET 3 LIMIT 1 make four ordinary calls plus one late Map
input call, as required by the corrected PostgreSQL baseline. Three-call, OR and nested Map remain
planning errors. Direct carrier tests cover copyObject/text serialization, invalid versions, fields,
keys, function OIDs, semantic nodes, binding shapes and non-text projected results.

## Interpretation and next step

One Filter→one generated Map is verified on the development branch with synthetic model responses.
This evidence does not qualify a real-model composed query, arbitrary nested operators, joins, multiple
Map outputs, asynchronous execution, PG accepted-prefix/multi-in-flight, model quality or performance.
The existing separate-operator real-model evidence retains its old source identity. Incremental Core
sessions and their PG bridge remain subsequent work under the project architecture.
