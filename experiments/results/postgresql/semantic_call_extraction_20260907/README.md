# Map call analysis extraction

## Purpose and scope

This engineering check supports the PostgreSQL execution foundation for the project's data organization
and scheduling research. It checks the first preparation step in the
[call binding design](../../../plans/postgresql_call_binding_design.md#8-a1首个实现步骤分离map调用分析).
It does not compare scheduling methods or model quality.

The behavior baseline is `main@b4b93b2e`; the working branch is `codex/semantic-call-binding`.
Map occurrence, original-source and constant-argument checks move from `sem_map_path.c` into
`sem_map_call.{c,h}`. Extension registration and Map path construction use that interface. The existing
Filter collector, marker identity/counting, path placement, private plan format, executor, wire and SQL
remain unchanged. The internal collector is renamed from `semloom_supported_marker` to `semloom_map_call`.
There is no shared call record or independent result binding yet.

## Setup and design

This is a before/after structural check, with no performance baseline or ablation claims. All 15 affected
function bodies match the baseline after the collector rename, including functions remaining in the path
module; the [comparison](raw/equivalence.json) records each function. Existing tests cover the externally
observable behavior, so the refactor adds no assertions that merely repeat its new file layout.

Local tests use Python 3.12. The server reuses its driver environment and a separate copy of the existing
PostgreSQL 18.3 toolchain. Core runtime preflight passed before setup; its full machine report remains in
the private runtime artifacts. The source, test clusters and logs use a fresh data-disk directory, with
a separate PG installation prefix and no changes to old clusters or services. Tests use synthetic
recording/golden/HTTP fixtures. Real model request budget and actual count are both zero.

The [source manifest](raw/source-manifest.json) fixes all 618 transferred code/runtime files. Every hash
matched on the server before the Linux tests. These are working-tree snapshot identities, not an invented
Git commit; subsequent status-only Markdown edits do not change the tested production sources.
No company source, private runtime environment or credentials were transferred.

## Commands and measured results

Commands below use the recorded source snapshot, an existing driver Python and a fresh PG18.3 prefix:

```bash
python -m unittest discover -s code/tests/postgres -t code -p 'test_*.py'
python -m unittest discover -s code/tests/execution_provider -t code -p 'test_*.py'
python -m unittest discover -s code/tests/experiments -t code -p 'test_gateway*py'
# In code/postgres/semloom_pg; PG_CONFIG points at the isolated installation.
make clean
make -j4 COPT='-O2 -Werror'
make install COPT='-O2 -Werror'
# Run as the test database owner against the isolated UTF8 cluster.
PG_TEST_INITDB_EXTRA_OPTS='--encoding=UTF8' make installcheck COPT='-O2 -Werror'
```

The driver clears inherited PYTHONPATH and RAY_ADDRESS for Python tests. PG uses C.utf8, a local Unix
socket and a dynamically selected test port. PGXS creates its own additional TAP clusters. Every failure
stops a completion claim; expectations and test cases remain unchanged from the baseline.

| Check | Actual result and evidence |
|---|---|
| Function-body comparison | 15/15 equivalent; [per-function result](raw/equivalence.json) |
| Local first attempt | 115 run, 16 setup errors from sandbox-denied localhost bind; [failure](raw/local-postgres.log) |
| Local same-suite rerun with socket permission | 115/115 pass; [log](raw/local-postgres-network.log) |
| Linux PostgreSQL Python/C contracts | 115/115 pass; [log](raw/final-python-postgres.log) |
| Linux gateway contracts | 17/17 pass; [log](raw/final-python-execution_provider.log) |
| Linux gateway observation | 6/6 pass; [log](raw/final-python-experiments.log) |
| PG18.3 strict compilation | Pass; [build](raw/r1-extension-build.log) |
| PG regression and TAP | 1/1 regression, eight TAP files and 1808/1808 assertions; [log](raw/r1-installcheck.log) |
| Build/install identity | Equal SHA-256 in [PG summary](raw/r1-pg-verification.json) |
| Source identity and cleanup | 618 hashes match, no owned service PIDs; [Linux summary](raw/final-python-summary.json) |

There was one PG attempt, which passed. The two local attempts are reported separately: the rerun changes
only permission to create localhost fixture sockets; it does not modify production code, tests or thresholds.
Public artifacts are redacted and trailing whitespace is normalized before saving; their
[hash index](raw/artifact-index.json) and [formatting provenance](raw/formatting-provenance.json) retain the
export/public hash distinction. The final source removes one empty EOF line from sem_map_call.c after
tests; its tested/final hashes are recorded separately, with no function-body change or test rerun.

## Facts, interpretation and remaining work

These results support unchanged behavior of the existing synchronous Map/Filter paths and two-Filter
composition after the extraction. The isolated database and fixture processes stopped after testing;
no inference service was started, and the server is not needed by a continuing test from this step.

They do not establish A1's shared SemanticCall/V1 binding, Filter→Map, asynchronous execution, model quality,
resource qualification or performance. The next database step is the common call/binding implementation;
A2a still needs the explicit permission and FINAL/OFFSET projection prototype before enabling composition.
The separate incremental-session design still needs B1 characterization and B2 implementation.
