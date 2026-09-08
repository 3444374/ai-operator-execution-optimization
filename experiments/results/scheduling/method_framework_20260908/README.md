# Sequential method framework verification

This engineering slice prepares per-row multistage methods for the existing execution context.
It implements no cascade algorithm. The [design](../../../plans/semloom_incremental_session_design.md#13-多阶段方法接入准备工程决策2026-09-08)
and [module guide](../../../../code/src/semantic_methods/README.md) define ownership and remaining work.

## Source and controlled checks

The local candidate extends `03dbb681` on `codex/incremental-session`. The isolated Linux checkout still
reports its original base `2d2dee35`; that Git value alone does not identify its working tree.
All [621 non-Markdown source/test files](raw/source-hashes.json) match the local candidate byte-for-byte,
including the previous async transport and this slice. The same source served Linux and real-model tests.
No source changes were made between those checks.

| Check | Observed result |
|---|---|
| Local session, transport and method tests | [44 pass](raw/local-tests.txt) |
| Linux full scheduling regression | [325 pass](raw/linux-tests.txt) |
| Changed Python files | Ruff format and lint pass |
| Default capability | Existing callers retain the original behavior |
| Declared profiles | Two stages receive their respective capability/operator; unknown profile rejects the entire offer |
| Method lifecycle | Zero-task final, one/two stages, wrong/duplicate completion, state/result size and stage limits checked |
| One held-task slot | Next stage is backpressured until the previous delivery lease is released |
| Method callback failure | Run fails; driver releases its result lease in finally |

The synthetic two-stage unit method uses embedding/generation capability labels with a controlled
backend. It does not execute an embedding model. Existing scheduler, credit, cancellation, and async
transport checks remain in the regression suite. PG code and wire were unchanged and not rerun here.

Commands from the repository root:

```sh
PYTHONPATH=code python3 -m unittest tests.scheduling.test_incremental_session tests.scheduling.test_async_backend tests.scheduling.test_method_continuation
env -u PYTHONPATH -u RAY_ADDRESS python -m unittest discover -s code/tests/scheduling -t code -p 'test_*.py'
```

## Actual model path and retained failure

After the user's additional authorization, a fresh localhost service used the previously verified
Qwen2.5-7B-Instruct revision `a09a35458c702b33eeacc393d103063234e8bc28`.
All nine model files were rehashed; [model identity](raw/prepare-model-verification.json) and
[environment check](raw/preflight-summary.json) are retained. The controller uses the same single-GPU
BF16, eager, FCFS, model-length 4096 setup as the previous core smoke; driver and model environments
remain separate. No installation, download, PG cluster, or gateway was needed.

Two synthetic rows each execute draft then revise. Both named profiles resolve to the same generation
model in this diagnostic adapter. The revise prompt includes the actual draft response. A single task
and HTTP request can be active; input is capped at 8 KiB, raw response at 64 KiB, continuation state at
64 bytes, and each row at two stages. Each request permits 64 output tokens at temperature zero.
There is one durable budget of four POST attempts, no extra warm-up generation and no HTTP retry.

The first controller run failed **before any POST**: the diagnostic script requested `advance(2)` with
`held_tasks=1`, and the existing core rejected the invalid delivery bound. The [failure](raw/prepare-driver.log.txt)
and [cleanup](raw/prepare-controller-summary.json) are preserved. Only the diagnostic call was corrected
to `advance(1)`. A second preparation directory reused the same still-empty four-attempt ledger; production
code, limits, and assertions were unchanged. The archived driver versions retain this difference.

The corrected run passed all four requests with peak HTTP concurrency one:

- The [trace](raw/run-fixed-trace.json) and [responses](raw/run-fixed-responses.json) preserve two task keys
  per original row and draft/revise capability order. Raw HTTP result identity and token usage were checked.
- Both transitions encountered backpressure before lease release, then accepted the next request.
- Source input was already materialized; the session remained open until both methods returned final values.
- The [summary](raw/run-fixed-summary.json) records two final rows, all five resource counters zero, and
  the async transport thread closed. The [budget](raw/prepare-budget.jsonl) records exactly four attempts.
- The [controller](raw/prepare-fixed-controller-summary.json) confirms process cleanup, closed model port,
  and both GPUs back at 1 MiB. The earlier failed service was also stopped before the corrected attempt.

Controller invocation is `python launch.py --settings <private-settings.json>`; machine paths and runtime
configuration stay outside Git. Exported raw files are redacted; the [artifact index](raw/artifact-index.json)
records their hashes. The reusable production code contains no temporary server address or model path.

## Scope

This establishes a sequential continuation interface connected to an actual backend. It does not
establish embedding support, approximate Filter quality, LOTUS parity, capability-aware endpoint
routing, parallel fan-out, aggregate method memory accounting, physical multi-member batching, PG async
integration, or performance results. Those require their own implementations and checks.

The next optimization can implement the `start/resume` adapter and reuse the existing session;
algorithm-specific calibration, authorization and parsing remain method responsibilities. The driver
must bound live rows and total continuation/final-result bytes, associate successful deliveries with
rows, release leases on error, and seal only after no method can produce more requests.
