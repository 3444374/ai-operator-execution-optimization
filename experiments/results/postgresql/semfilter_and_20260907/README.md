# Two SemFilter predicates and bounded gateway sessions

## Purpose and source

This is an engineering correctness check for PostgreSQL-native semantic operators and their external
execution service, supporting the execution foundation for the project's two scheduling research topics.
It is not a scheduling or model-performance experiment. The specification is the
[AND implementation slice](../../../plans/postgresql_ai_semantic_operator_architecture_20260827.md#semfilter-and-slice).

The branch is `codex/semfilter-and`, based on `1d83c975`. Validation used working snapshots rather than
claiming an uncommitted tree was a Git commit. The final [source manifest](raw/source-manifest.json)
contains 584 Python, C, header, SQL, TAP, fixture and Makefile hashes; all match the Linux source.
The server reused its previously verified `20b22a55` source for unchanged code and received only necessary
self-owned code/test deltas. No company source, credentials, deployment configuration or model was transferred.
The exact tested runtime is the final branch code matching that manifest. Documentation updates do not
change this runtime identity. Main has not been changed by this task.

## Setting and design

An isolated PostgreSQL 18.3 installation and UTF8 test clusters used an existing driver environment.
Runtime preflight `core,text` passed before setup. Source, clusters and logs were on the data disk;
existing services and the original server checkout were not modified. The extension build used
`COPT='-O2 -Werror'`. PGXS ran regression and every TAP file with
`PG_TEST_INITDB_EXTRA_OPTS='--encoding=UTF8'` and `LANG=C.utf8`.

The public gateway admits at most eight session threads by default, with a separate default limit of
one active or uncertain model request. It has no task admission queue; excess connections close and
excess requests receive `MODEL_REQUEST_REJECTED`. Each session still uses the existing synchronous
one-task wire. An idle connection occupies no model request capacity. The configurable 120000 ms
frame deadline covers header plus body, preventing a slow peer from extending it with each fragment.
The listener backlog is also configured from the connection limit; kernel pending connections are
separate from accepted session workers.

The SQL fixture uses ten rows with TRUE/FALSE/UNKNOWN, SQL NULL, empty text, Unicode and repeated values.
Two existing Filter scans retain independent plans, input evaluation, counters and sessions through one
gateway. Additional checks cover projections, duplicate expressions, volatile input functions,
prepared plans, INSERT/rollback, LIMIT, RLS, permissions, snapshots, cancellation and recovery.
OR, NOT, three Filter calls and Map/Filter mixtures remain rejected.

Gateway tests exercise idle-session progress, independent sequences, connection and request capacity,
late completion after disconnect, shutdown, frame deadlines and observational identity. A controlled
Adapter verifies request capacity two without using a model. Linux additionally completes 40 sessions
through one still-running gateway and checks FD/thread counts return exactly to the initial counts.
This is a small lifecycle diagnostic; no RSS threshold or formal resource qualification is inferred.

## Results and failures retained

| Check | Result | Evidence |
|---|---|---|
| Local operator/Adapter contracts | 115/115 | [log](raw/local-postgres-final-2.log) |
| Local gateway contracts | 16 passed, one Linux-only skip | [log](raw/local-gateway-final-2.log) |
| Local observation contracts | 6/6 | [log](raw/local-observers-final-2.log) |
| Linux operator/Adapter contracts | 115/115 | [log](raw/final-python-postgres.log) |
| Linux gateway contracts | 17/17, including live-process FD/thread recovery | [log](raw/final-python-execution_provider.log) |
| Linux observation contracts | 6/6 | [log](raw/final-python-experiments.log) |
| PostgreSQL 18.3 | strict build; regression 1/1; eight TAP files, 1808/1808 | [summary](raw/r7-pg-verification.json), [TAP](raw/r7-installcheck.log) |
| Source and cleanup | 584 hashes match; zero owned service processes | [summary](raw/final-python-summary.json) |
| Real-model requests | 0; synthetic HTTP and golden fixtures only | test source and execution arguments |

The initial serial gateway failed the idle-peer progress test, preserved in the
[baseline failure](raw/local-gateway-red-3.log). Local preparation failures included an incorrect unittest
module path and sandbox-denied sockets; test-development failures included a nonexistent test helper,
a missing mock socket, an overbroad mock identity and outdated source/diagnostic assertions. They were
corrected without weakening the intended behavior checks; original logs remain in the task artifacts.

The seven PG attempts are separate records, with no overwritten conclusions:

1. `r1`: strict build passed; regression retained an obsolete assertion rejecting two Filter calls.
2. `r2`: corrected regression passed. Fault-injection socket wrappers lacked timeout forwarding; new
   downstream cost metadata was rejected by the cost validator. TAP failed.
3. `r3`: old seven TAP files passed. New tests used the wrong CustomScan name and exposed missing native
   Filter EXECUTE checks after marker lowering.
4. `r4`: native permission checking and node lookup passed; one v3 test incorrectly expected a digest
   field only emitted by the choice/Map schemas. It was replaced with a choice-plan identity check.
5. `r5`: regression and 1806 TAP checks passed, including RLS, snapshot and cancellation cases.
6. `r6`: a new volatile-input counterexample failed: two Filter calls evaluated the input function once.
   PG had absorbed downstream projection into the first scan. The original 1806 checks stayed green.
7. `r7`: disallowing projection absorption at intermediate Filter paths makes each input evaluate at
   its own stage. Regression and all 1808 TAP checks pass; both build and installed library hashes match.

All attempt summaries and TAP outputs are in `raw/`; the
[artifact index](raw/artifact-index.json) also records hashes of retained private supporting logs. Published logs remove trailing whitespace after
redaction; `sha256` identifies the pre-format redacted artifact and `published_sha256` the Git copy.
The original private logs and failure outputs remain unchanged.
Independent review additionally found response-size truncation and premature HTTP Content-Length EOF
being treated as known completion, and connection refusal being treated as possible dispatch. The final
Adapter retains capacity for uncertain remote outcomes and releases it for known pre-dispatch failure.
Tests cover all three cases and preserve the original total-timeout error. Session closure precedes
its end event; task context links HTTP attempts to completions even for identical concurrent payloads.

## Interpretation and remaining work

Source and tests establish bounded synchronous multi-session service and the specified two-Filter AND
shape, including independent volatile input evaluation and PostgreSQL-native function permission checks.
The PG pump, semantic machines, provider port and wire schemas are reused. No scheduling speedup,
model quality, deployment identity, whole-process RSS bound, or arbitrary SQL composition is established.
The encoded frame cap is not total RSS: decoded objects, plans/tasks and request/response buffers can
coexist. Golden fixture storage and kernel socket buffers are separate allocations.

Disconnect does not cancel remote model computation. The gateway waits for local Adapter completion or
its deadline; an unknown outcome keeps its request reservation. Recovery requires externally confirming
service completion and restarting the gateway. The bounded daemon DNS resolver may outlive a request
inside a live process; this check does not claim every backend thread can be forcibly reclaimed.
Historical exclusive-session resource attribution remains exclusive-session; generic task/session events
now support concurrency, but that does not qualify the old resource experiment for concurrent workloads.

All test clusters and owned gateways have stopped. Filter→Map, per-session batch/accepted-prefix,
out-of-order completion and the incremental SemLoom scheduling interface remain separate work.
The next composition can reuse this gateway and the existing node lifecycle; it still needs its own
input/output binding and cancellation checks. No merge, push or real-model run occurred in this task.
