# Shared call and tuple binding verification

## Purpose and source

This is an engineering correctness check for the PostgreSQL execution foundation, supporting both
data organization and scheduling research. It implements the common-call and tuple-routing parts of
[the database design](../../../plans/postgresql_call_binding_design.md). It does not enable Filter→Map
or establish model quality, performance, or an asynchronous provider.

The baseline is `11e89b08` on `codex/semantic-call-binding`. Both collectors now create independently
owned `SemloomSemanticCall` occurrences. The tuple pump delegates mapping to `semantic_binding`, reads
input from the child, and writes completions to the binding's result column. Legacy paths retain their
old layouts; explicit mappings support reordered passthrough and an independent result slot. Type,
typmod, collation, dropped-column and coverage checks are centralized. The public SQL and wire remain
unchanged. The new outer plan-carrier format remains pending.

The server copied the previous isolated source and received only the explicitly authorized 15-file
delta. The three [delta manifests](raw/delta-r1.json) ([r2](raw/delta-r2.json), [r3](raw/delta-r3.json))
identify each attempt. After testing, hashes of the server's existing baseline plus the delta were read
back and compared locally: all [605 non-Markdown source/test files](raw/server-source-hashes.json) match.
The separate [Linux summary](raw/final-python-summary.json) checks the 15 changed files. These are
working-tree snapshot identities, not claims that an uncommitted candidate was already a Git commit.

## Setup and methods

Core runtime preflight passed before setup. The machine report stays in private runtime artifacts.
Source, clusters and logs use a fresh data-disk directory; an independent PostgreSQL 18.3 installation
reuses the existing compiler/driver environment. Old clusters, installations and services remain unchanged.
No company source or runtime credentials were transferred. Model budget and actual model requests are zero.

The controls are the existing synchronous Map/Filter and two-Filter tests. New test-only callers compile
the production call/binding implementation inside PostgreSQL, exercise memory ownership, and inspect
actual `set_plan_references` output. No production test GUC or SQL entry point is added.

```bash
python -m unittest discover -s code/tests/postgres -t code -p 'test_*.py'
python -m unittest discover -s code/tests/execution_provider -t code -p 'test_*.py'
python -m unittest discover -s code/tests/experiments -t code -p 'test_gateway*py'
# In the isolated extension checkout with PG_CONFIG set to PostgreSQL 18.3:
make clean
make -j4 COPT='-O2 -Werror'
make install COPT='-O2 -Werror'
PG_TEST_INITDB_EXTRA_OPTS='--encoding=UTF8' make installcheck COPT='-O2 -Werror'
```

PG tests run as the database owner with C.utf8, independent UTF8 clusters, a Unix socket and a selected
local test port. Python tests clear inherited PYTHONPATH and RAY_ADDRESS. Capacity configuration is
unchanged; no tuning, formal experiment or scheduling comparison occurs.

## Results and failed attempts

| Check | Actual result |
|---|---|
| Local Python/C contracts | 115/115; [log](raw/local-python-r1.log) |
| Linux Python/C, gateway, observation | 115 + 17 + 6 = 138/138; [summary](raw/final-python-summary.json) |
| First PG attempt | Production build and SQL regression pass; the test-only caller misses `utils/memutils.h`, so three TAP files report build failures; [aggregate](raw/r1-installcheck.log), [compiler error](raw/r1-binding-build.log) |
| Second PG attempt | Header fixed, existing assertions preserved: strict build, regression 1/1, nine TAP files and 1847/1847; [summary](raw/r2-pg-verification.json) |
| Third PG attempt | Adds one setrefs prototype assertion: strict build, regression 1/1, nine TAP files and 1848/1848; [summary](raw/r3-pg-verification.json), [TAP](raw/r3-installcheck.log) |
| New checks | 40 assertions: occurrence copies/keys, mapping and memory ownership, binary/NULL/reused slots, malformed mappings/types, recovery, and setrefs result identity |
| Build/install identity | Equal SHA-256 in the final PG summary |
| Cleanup | Every test cluster stopped; the Linux summary finds no owned service PIDs |

All attempts remain separate. The second run changes only the missing test header; the third adds a
specific mechanism check after the binding tests pass. No threshold or error expectation was relaxed.
Public logs are redacted and trailing whitespace normalized, with [normalization hashes](raw/whitespace-normalization.json)
and a [public artifact index](raw/artifact-index.json); private original logs remain on the server.

## Projection characterization and design correction

A separate eight-row fixture defines VOLATILE functions that count calls and return either their input
or SQL NULL. The first diagnostic omits the recording gateway and its Filter query fails; its EXPLAIN
also precedes explicit hook loading in later sessions. Those [first results](raw/projection-probe-summary.json)
are retained and are not evidence of a valid Filter plan.

The corrected diagnostic explicitly loads `semloom_pg` before planning and starts a bounded recording
gateway. All three queries succeed, and both gateway and cluster stop. Its actual plans and counts are:

| Query, all with OFFSET 3 LIMIT 1 | Count |
|---|---|
| Ordinary `SELECT tick(body), tick(body)` | 8 ordinary calls; [plan/output](raw/projection-probe-r2-ordinary.log) |
| Same outputs through one all-TRUE recording Filter | 8 ordinary calls; [plan/output](raw/projection-probe-r2-filter.log) |
| Old single Map: ordinary tick plus Map input that counts and returns NULL | 4 ordinary calls, 4 Map-input evaluations, no model request; [plan/output](raw/projection-probe-r2-legacy_map.log) |

Thus the previous draft's two-total-calls expectation for OFFSET was wrong for PostgreSQL 18.3. The
planned new Map placement intentionally evaluates its input after OFFSET/LIMIT, while ordinary outputs
retain their native reference behavior: four ordinary calls plus one Map-input call in the corresponding
all-surviving-row example. That five-call outcome is a revised requirement, not an implemented result.

The new setrefs prototype confirms that a marker matching scan result column 2 becomes INDEX_VAR 2 in
both the output and retained custom expression; the function dependency remains. This supports explicit
native ACL/object-hook handling for the future Map carrier. It is not a full new-carrier revocation test.

## Interpretation and next step

The common call/tuple-binding foundation is verified for existing paths and direct binding callers.
Next implement the versioned outer carrier and the actual Filter→Map consumer, verifying independent
input evaluation, ordinary-expression behavior, function permissions, NULL/LIMIT and cancellation.
Shared-call keys are planner-local; binding tests do not qualify image models or arbitrary SQL shapes.
Incremental session implementation and its PG bridge remain separate work.
