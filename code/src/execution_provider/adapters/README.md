# Native ready-task execution

`native_tasks.py` accepts already prepared, non-streaming single-model text calls before a
supplier's request pool. It uses the existing `SessionEngine`, Job grants, organization and
delivery leases. It neither interprets messages nor changes generation parameters.
`semantic_methods.MethodDriver` continues to own sequential row methods; callbacks can decode
the same full response. No new method scheduler is introduced.

## Python consumer

- `build_native_execution(config, physical=RayMapConfig(...), **capacity_options)` uses existing
  Daft payload batches and Ray HTTP workers. The capacities and policies are those of
  `build_fixed_model_execution`. `physical=None` explicitly selects a local diagnostic.
- `prepare_native_task(payload, sequence, row_sequence=..., call_id=..., stage_id="model",
  work=..., max_result_bytes=...)` produces the existing `OfferedTask`. Its payload is the
  supplier-converted HTTP request body. The default descriptor measures request count; it is
  not a token or GPU cost estimate. The supplier owns SDK conversion and semantic eligibility.
- `NativeTaskSession(execution, query_id, operator_id)` registers one Job and one operator
  flow. A service can share the same execution with multiple registered flows/Jobs using the
  existing grants and scheduling APIs; each owner must keep all core mutations on its owner
  thread. `request_cancel()` is the cross-thread cancellation signal.
- `offer(tuple_of_tasks)` transfers only `accepted_prefix_count` items. Sequences start at zero
  and increase only for accepted items. The caller keeps the suffix. A tuple exceeding
  `limits.offer_tasks`, or an invalid member anywhere, is rejected as a whole. Storage pressure
  within a valid tuple returns a possibly empty prefix. Offering never sends a request.
- `advance(max_deliveries)` advances the shared Engine, then returns existing `Delivery`
  objects in completion order. `Delivery.info` keeps call/row/stage identity; the Job label is
  the query identity and `SessionSpec.flow_id` is the operator identity. Physical grouping does
  not rewrite those values. `wait(progress)` uses the existing wake signal and deadlines.
- `decode_full_response(delivery.result)` returns `FullModelResponse(status_code, headers,
  body, http_version)`. The native consumer interprets its body, including usage and logprobs.
  After consumption call `release((delivery.lease_id,))`; a result stays charged until release.
  The public wrapper retains no separate task or result history.
- `end_input()` seals this task producer. Call it only when no continuation can produce more
  tasks; source EOF alone does not meet that condition for a multi-stage method.
- `close(clean=False)` requires the consumer to discard its result bytes first. It requests
  cleanup of its flow and Job, but does not close the shared backend. The service owner keeps
  advancing/reaping the Engine, then closes the execution when all resources have settled.
  Unknown remote work stays charged and is never retried or declared remotely cancelled.
  `execution.close()` returns false while any registered Job or task is still retained;
  completing a local failed HTTP await does not establish full service cleanup.

## Complete HTTP responses

`full_response.py` carries a bounded header plus raw response bytes inside the existing result
reservation. The header includes HTTP status, duplicate-preserving header pairs and protocol
version; it is at most 8192 encoded bytes and 128 pairs. The task's `max_result_bytes` covers
both header and body, so callers must allow room for both. Response compression remains visible
in the headers and raw bytes; decompression, if required by a native SDK, belongs to its adapter.
The encoder does not parse JSON or extract fields. Malformed JSON and HTTP error responses
remain actual status/body pairs for the native parser.

`AsyncFixedModelTransport` keeps its default response and HTTP error mapping.
`FullResponseTransport` changes only its response packaging. `RayMapConfig.response_mode`
defaults to `completion`; the native builder selects `full`. An explicitly owned worker service
for native calls must also be created with `response_mode="full"`. Worker identity includes this
mode, so a query cannot borrow a service with incompatible response behavior. Confirmed local
pre-send failures have no invented HTTP status; decoding them raises `CompletionAdapterError`.
Transport failures still use the Core's unknown-remote handling.

## DuckDB local batch transport

`duckdb_batch_socket.DuckDBBatchConnection(execution).serve(connection)` handles a borrowed
local socket on the Engine owner thread. It reuses the existing four-byte big-endian length
and UTF-8 JSON framing, with a 1048576-byte frame limit. The listener owns acceptance, socket
close and service teardown. This initial handler runs one connection at a time; concurrent
socket dispatch needs an owner-thread service loop and remains pending. It does not receive SQL.

Every message has `protocol="semloom.duckdb.batch.v1"` and the exact fields listed here.
Decimal sequence strings avoid loss when clients use different JSON number representations.

| Direction | Message and fields |
|---|---|
| Client → service | `open`: `query_id`, `operator_id` |
| Service → client | `opened`: `max_offer_tasks`, `max_input_bytes`, `max_result_bytes`, `max_frame_bytes`, `work_unit` |
| Client → service | `offer`: `tasks`, each containing `sequence`, `row_sequence`, `call_id`, `stage_id`, `payload` |
| Service → client | `accepted`: `accepted_prefix_count`, `status`, `reason` |
| Client → service | `poll`, `end`, or `cancel`, with no additional fields |
| Service → client | `result`: `sequence`, `row_sequence`, `call_id`, `stage_id`, `status_code`, `headers`, `http_version`, `body_base64` |
| Service → client | `idle`, `ended`, or `finished`, with no additional fields |
| Service → client | `cancelled`: `uncertain_requests`; `error`: `code`, `uncertain_requests` |

The `payload` string is the supplier's prepared UTF-8 HTTP body, encoded without reserialization.
The socket adapter currently measures request count. It advertises a conservative result limit
that includes response headers and leaves room for base64 and identities in the output frame.
Each `poll` transfers at most one result; its core lease lasts until the bounded send succeeds
or the connection consumer stops. Input batches stay finite and the caller retains an unaccepted
suffix. `cancel` or EOF during polling stops the local consumer, while unconfirmed model work
remains charged in the shared Engine. The listener owner must continue advancing/reaping it.

## Adopted source behavior and scope

This is an engineering choice based on the fixed consumers in the
[overall design](../../../../experiments/plans/语义系统对照.md#native-semloom-pairs).
[LOTUS LM](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/models/lm.py)
separates uncached messages before native rate control and LiteLLM batch execution. Its native
cache, physical/virtual usage statistics, top-choice and logprob interpretation remain supplier
work. SemLoom receives ready uncached batches before that execution; outer accessor
materialization and algorithm-required waiting are not removed by a task adapter.
[DuckDB provider](https://github.com/leonardovida/duckdb-ai/blob/9b7b16a5d5bfa97180b8be48d69bd9a4a4106419/src/duckdb_ai_provider.cpp)
prepares the request, executes provider control, then checks status and parses completion fields.
The [extension](https://github.com/leonardovida/duckdb-ai/blob/9b7b16a5d5bfa97180b8be48d69bd9a4a4106419/src/duckdb_ai_extension.cpp)
uses `RunProviderJobs` for prepared row jobs. Its supplier adapter must select SemLoom before
that worker execution and retain native prompt, parsing, NULL/error and usage policy.

The focused controlled tests are `test_native_tasks.py` and `test_full_response.py` under
`code/tests/execution_provider/`. They test prefix ownership, out-of-order completion, result
backpressure, cancellation/late completion, unknown outcomes, complete HTTP errors, and the old
PG decoder/error mapping. Global status/registry updates and real-model qualification belong to
the integration task. Real cascades, multiple models, cross-row joins and parallel stage expansion
remain pending.

[Sema request service](sema_request_service.md) adds query-owned transparent and Daft/Ray service
paths after the author's native request pool. It retains native supply and SQL parsing; it does
not establish a SQL executor replacement. The [integrated query entry](../../experiments/postgresql/README.md#native-adapter-query)
retains all three paths; [model and fixture evidence](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md)
records native supply, complete bodies, query exit and unavailable native-call timing.
