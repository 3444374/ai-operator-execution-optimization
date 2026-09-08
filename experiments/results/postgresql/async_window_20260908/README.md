# PostgreSQL Map with bounded multi-in-flight execution

Status: implementation, controlled PG checks and one 12-request real-model run passed.
Scope: one generated Map using the version-six provider; this is not Filter async execution,
multiple active gateway sessions, or a performance/quality comparison.
Design and source decisions: [PG binding design §13](../../../plans/postgresql_call_binding_design.md#pg-async-readiness).

## What runs

PG still calls its ordinary child plan and returns one tuple through CustomScan. The optional
provider `offer` acknowledges zero or one accepted task; `receive` requests a completion for any
accepted sequence. Transport RPCs wait for their own replies, but task acknowledgements do not
wait for the model. There is no PG background thread, executor replacement, or core patch.

The `incremental-map` profile uses `semloom.provider.incremental-map.uds.v6`. Map semantics and
completion evidence share the v5 codec; the transport version and execution digest are distinct.
The v5 synchronous profile remains available. PG retains owned input/message bytes and materialized
row slots, matches completion identities, and returns rows in the current Map contract's input order.
It does not add a SQL Sort node or promise order for queries without ORDER BY beyond this carrier rule.

The gateway accepts a bounded window before polling drives the existing organizer and SessionEngine.
Two members still become two independent HTTP requests. Completion leases remain charged until their
frames are sent. Closing PG stops delivery; submitted remote work retains its credit until a terminal
response, and unknown outcomes quarantine the engine. One engine serves connections sequentially.

## Configuration and tested scope

- Gateway: `--incremental-map --max-active-requests 2` with the existing fixed-model config.
- PG: `semloom_pg.provider_execution_profile='incremental-map'` and `provider_window_tasks=2`.
- Task limits accept 1–64; PG's configured limit must fit the gateway handshake. These are configurable
  engineering limits, not GPU capacity measurements or experimentally tuned settings.
- `provider_window_bytes` defaults to 8 MiB and divides retained-row reservations across the window.
  A row context plus its output reservation exceeding that share fails with `54000`.
- The initial multi-row path accepts plain column/constant projections on an ordinary child without
  quals. Other input/child expressions run through v6 with an effective window of one, visible in
  `EXPLAIN`. Filter and composed incremental paths remain unsupported.

The byte setting is not a bound on total PostgreSQL RSS, the serving process, or transient child
materialization before a large row can be checked. Cancellation/INSERT errors can leave already
accepted work to drain; model calls are not rolled back with database writes.

## Controlled checks

| Check | Final result and evidence |
|---|---|
| Linux provider / PG contracts / shared core and observer tests | 26 / 115 / 58 passed; [provider](raw/provider-final.log.txt), [PG](raw/postgres-final.log.txt), [shared](raw/shared-final.log.txt) |
| Local provider / PG contracts | 25 passed + 1 platform skip / 115 passed; [provider](raw/local-provider.log.txt), [PG](raw/local-postgres.log.txt) |
| PostgreSQL 18.3 | Strict `-O2 -Werror` build, SQL regression 1 passed, 12 TAP files / 1958 assertions passed; [final log](raw/full-final.log.txt) |
| New PG cases | Reverse completion, row association, SELECT/INSERT, NULL, LIMIT 0/1, volatile-input window-one fallback, cancellation/recovery, 40 rows across child pages, bad acknowledgement version and wrong completion sequence/payload |
| PG-owned lifecycle | Concurrent UPDATE/INSERT did not change the running query's snapshot; RLS exposed one row with one controlled request; revoked function execution returned `42501` with zero requests; [check](raw/lifecycle-summary.json), [script](raw/lifecycle-check.py.txt) |

The additional lifecycle check used the same binary and source, a controlled backend, four synthetic
requests and zero model requests. It did not replace the real-model test below.

Failure evidence is retained: [first build](raw/build-first.log.txt) failed on new indentation warnings;
[initial TAP](raw/tap-first.log.txt) assumed an UPDATE preserved the first physical row, corrected by
an explicit id predicate; [first full regression](raw/full-first.log.txt) found three changed legacy
error messages. Restoring legacy validation order passed the [intermediate suite](raw/full-second.log.txt).
The final suite additionally covers owned input bytes and malformed v6 responses. No assertion was relaxed.

## Real-model run

One run, fixed budget 12 POSTs, no HTTP retries or generation warm-up. Model: Qwen2.5-7B-Instruct,
revision `a09a35458c702b33eeacc393d103063234e8bc28`; vLLM 0.25.1, PyTorch 2.11.0,
Transformers 5.14.1. The nine model files were rehashed before starting. Generation used temperature 0,
128 output tokens normally and 256 for cancellation; request/statement deadlines were 120 seconds.
The owned single-GPU server used BF16, model length 4096, native FCFS, at most four serving sequences,
eager execution and disabled prefix caching. No package, model, or dataset was downloaded.

| Phase | Actual POSTs |
|---|---:|
| Synchronous reference SELECT | 2 |
| v6 SELECT | 2 |
| v6 INSERT | 2 |
| v6 INSERT rejected by target constraint | 2 |
| v6 two-row generation cancelled | 2 |
| Recovery on the same gateway | 1 |
| LIMIT 1 | 1 |

[Driver summary](raw/run-summary.json) and [events](raw/run-incremental-events.jsonl) show actual overlapping
HTTP transfers peaked at two within one PG query. The incremental core submitted and settled ten tasks;
six sessions drained with all resource counters zero. SELECT/INSERT matched the synchronous reference,
returned text matched observed model completions, prompt usage matched the tokenizer, and output usage
stayed within the request limit. The rejected INSERT wrote zero rows; cancellation returned `57014` and
subsequent queries succeeded. NULL, LIMIT 0 and EXPLAIN made no model request.

The durable [ledger](raw/prepare-ledger.jsonl) contains twelve attempts plus its header. The
[controller](raw/prepare-controller-summary.json) confirms successful cleanup, a closed model port,
and both GPUs at 1 MiB. PG and gateway sockets were removed and transport cleanup reported success.
The [driver](raw/prepare-real_check.py.txt) and [controller source](raw/prepare-launch.py.txt) use private
settings, which are not exported. This is execution correctness evidence, not semantic accuracy,
throughput improvement, GPU-memory accounting, or formal resource qualification.

## Source identity and remaining work

The working copy started from `27729f34`; the [633-file manifest](raw/pg-async-source-manifest.json) identifies
actual source/test contents, independently of the copied server checkout's older Git HEAD. The installed
extension SHA256 is `f39503189fc22ced13be5e64733c1aaf075bedf9d9d1495218a63573bad43396`.
[Postflight](raw/postflight.json) rechecked the manifest and found no owned processes. Raw artifact hashes
are in the [index](raw/artifact-index.json).

Filter/Map composition through v6, richer expression eligibility, multiple active sessions sharing the
engine, physical multi-member requests and preparation/GPU-memory accounting remain separate work.
Current multi-in-flight support requires explicit profile selection; it does not silently change old queries.
