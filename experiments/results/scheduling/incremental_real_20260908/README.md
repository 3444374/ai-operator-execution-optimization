# Incremental session with a real model

## Purpose and implementation

This verifies the execution context that will host data organization and scheduling policies. It extends
[the controlled-core checks](../incremental_session_20260908/README.md) according to
[the current design](../../../plans/semloom_incremental_session_design.md#12-有界异步传输与真实核心验证).
Source is `e84d308d` plus the two file hashes in [source provenance](raw/source-identity.json).
The Linux test source, real-run source and final local candidate match across all
[618 non-Markdown source/test files](raw/source-hashes.json).

BoundedAsyncBackend runs actual asynchronous I/O in one transport thread. It does not wrap synchronous
drive or own submission policy. Its slots include responses not yet consumed by poll; completion order
is recorded independently of input order. An unclassified transport exception retains its slot and
reports uncertainty once. request_cancel does not cancel the HTTP await or claim that vLLM stopped;
a completed response remains the authority for returning compute capacity. The explicit close operation
joins the transport thread and runs its asynchronous client finalizer.

## Setup and method

A separate fresh localhost service reuses the same verified 7B model, versions and single-RTX-4090
configuration as the [PG smoke](../../postgresql/filter_map_real_20260908/README.md). No asset or dependency
changes occurred between runs. Source and artifacts are on the data disk, with driver and vLLM isolated.
The new durable AttemptLedger permits exactly five POST attempts; the preceding PG ledger was checked
byte-for-byte unchanged. No retry, extra warm-up generation, PG cluster or gateway is used here.

The diagnostic HTTP coroutine uses the existing build_completion_request_body helper and one shared
httpx.AsyncClient with at most two connections. It reads raw chunks under a 64 KiB response limit before
returning bytes to the core. Session limits are two held tasks, 4096 input bytes, 131072 reserved result
bytes, two active requests and two estimated work units. Task choice remains FIFO; one task is one
physical submission in this reference. Normal requests allow 64 output tokens and the cancelled-session
request 128, at temperature 0. The payload is a sealed public synthetic request, not SQL or a PG plan.

## Observed results

| Check | Result |
|---|---|
| Local core and transport tests | 37/37; [log](raw/local-tests.log) |
| Linux full scheduling suite | 318/318; [log](raw/async-full.log) |
| Code checks | Ruff passes; [log](raw/async-ruff.log) |
| Real model requests | 5/5, peak two overlapping HTTP operations, five authoritative responses |
| Prefix/backpressure | Offer three accepts two; completing without release keeps the result reservation; release permits the third |
| Normal first session | Three deliveries match decoded HTTP responses; seal/release finishes and closes |
| Cancelled session | Close reports one unpolled remote responsibility; no result is delivered to its consumer |
| Next session | It opens while the old reservation remains, receives only its own TaskKey, and completes |
| Final cleanup | All five resource counters are zero, transport thread exits, service exits/port closes, GPUs return to 1 MiB |

The real [summary](raw/run-summary.json), [state/usage trace](raw/run-trace.json),
[requests and responses](raw/run-responses.json), [budget](raw/prepare-budget.jsonl) and
[controller cleanup](raw/prepare-controller-summary.json) retain the observations. Four responses are
delivered: three from session 0 and one from session 2; session 1's response is used only for cleanup.
The real run observes completion order as returned, while deterministic tests establish reverse-order
handling. It does not manufacture delay or claim that the cancelled request was still computing when
cancel was issued: it had been admitted to HTTP and its terminal had not yet been consumed by the core.

## Scope and next work

This is an actual SessionEngine/backend/model path, not a replay of the PG smoke or fixture output.
It is a bounded five-request correctness observation, not model quality, throughput, GPU-memory
qualification, remote-abort support or a production PG async integration. The HTTP coroutine is a
diagnostic model adapter; broader failure/connection tests and the PG request protocol remain work.
Batch members, preparation-stage budgets and multi-Job policies must integrate into this execution
context, with one owner per resource; they are not implemented by this single-member test.

The old synchronous runtime still has consumers and is preserved. No failing run was hidden; all new
controlled and real attempts passed. Raw private logs remain on the server. Published artifacts are
redacted and whitespace-normalized, with [hashes](raw/artifact-index.json).
