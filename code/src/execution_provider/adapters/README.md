# Native task execution

The independent [LOTUS text Filter records reference](lotus_records_static.py) uses Daft plus
Ray actors and is a `diagnostic_reference`, with no production Core connection.
Its shared method API, CPU checks and historical fixed-source model qualification are documented in the
[slice record](../../../../experiments/results/diagnostics/lotus_records_local_model_20261016/README.md).

The static reference and [records Core adapter](lotus_records_core.py) consume the
source's neutral `RecordScan`: typed CSV schema, text payload projection and one
optional equality. The source owns `preflight(method)` and task binding; the
shared original LOTUS formatter owns instruction column references. Neither
backend embeds Movie column names or predicates. Core still accepts one raw row
per task through its existing session and asynchronous transport; the reference
retains its existing bounded static window. Their occurrence, usage, unknown
work and closure responsibilities are unchanged.
The driver binds the original LOTUS column order into the method value before
allocating preparation actors. Workers preserve that order and validate the
instruction's referenced-column set. Sources still receive the public method
for task preflight; continuous queries do not mutate that source binding.

The [records actor owner](lotus_records_actor_owner.py) provides shared preparation/model
allocation, readiness and retryable closure. It owns no query scheduler or Core session.
`start`, `wait_ready`, `prepare_actors` and `model_actor` let callers pass prepared ObjectRefs
directly to the model; trusted subclasses can specialize `_model_actor_type`.
An attempted model construction that raises remains unknown and cannot be retried as a new
construction. Unknown API closure retains the model handle. A literal API acknowledgement
and successful actor kill requests close only `ray_actor_kill_requests_and_engine_api`;
process/GPU exit remain unknown until separately observed. API shutdown has ten seconds,
with eleven seconds for its Ray response, independently of startup limits. A static query's
unsettled work remains unknown even when this owner scope closes.
Trusted private-process owners may override `_model_close_timeout_s` to include
bounded IPC teardown; the response wait adds one second and Core's explicit
cleanup deadline still caps it. The default ten/eleven-second windows are unchanged.
The static reference uses this owner without changing its query/window scheduler. Its historical
model qualification and the new owner's CPU checks retain separate fixed sources in the slice record.

The separate [Core records adapter](lotus_records_core.py) connects raw bounded records to the
existing `SessionEngine` and `BoundedAsyncBackend`. Preparation references reach the same model
owner directly; the consumer releases Core leases after decoding bounded terminal bytes. Unknown
work retains its Core credit and references. This first profile requires both batch sizes to be one.
The explicit Movie selector is `semloom-local-model-core`; its production marker comes from the
actual Core owner/iterator. Real Ray/Daft/LOTUS/SemBench CPU composition has been checked with
a synthetic model kernel. The separate [frozen ordinary Core qualification](../../../../experiments/results/diagnostics/lotus_records_local_model_20261016/README.md#core-model-qualification-20261019)
binds `ebe3035f`: one owner/engine, two independent query sessions and four actual model calls.
Core credits and retained references return to zero; API closure, observed OS exits and the
independent GPU observation remain separate evidence. Later sources and finite chains do not
inherit that qualification.

Core also accepts an explicit, configuration-matched `RecordsActorOwner` through
`actor_owner=`. Trusted owners can specialize model options/arguments and the
`submit_core`, `cancel_core`, `acknowledge_core` hooks. Core passes the complete
session/sequence key; receipt acknowledgement follows validation and storage.
Cancellation does not free active work or unknown input references. Owner mode
metadata cannot override actual Core, cleanup or GPU observations. Both the
default and injected owners honor Core's cleanup deadline and retain the same
pending close request on timeout. These are [CPU-checked integration hooks](../../../../experiments/results/diagnostics/local_async_engine_20261019/README.md#core-actor-hooks);
the default remains synchronous. The optional Async actor and CLI now consume these hooks;
their [separate CPU integration evidence](../../../../experiments/results/diagnostics/async_records_actor_20261019/README.md)
keeps component, query and cleanup qualifications bound to their tested sources.

The separate [finite-chain Core adapter](lotus_record_chain_core.py) admits raw bounded
fused batches through the same production Core and one asynchronous slot. A local host
worker invokes the shared LOTUS batch kernel and owns its isolated model-process pipe;
this profile uses no Ray or Daft. One owner supports one query, one active batch and one
CPU worker. Core work is a conservative stage-call upper bound. Unknown work retains
credit/input; bounded close keeps one cleanup future for that same owner. Logical wire
bytes exclude Python copies, audit files and retained consumer relations. Its 23 CPU
checks include a real private process/OS exit, with model/environment fixtures; real
model/GPU, a chain CLI and cross-query reuse remain pending in the slice record.

This module accepts prepared single-model request bytes through the existing Core.
It does not interpret prompts, change generation parameters, or replace a supplier's parser.
The implementation is adopted from integration source `671a22dd4285319209396ece3572bb89ff5ab74e`;
[main qualification](../../../../experiments/results/diagnostics/main_foundation_20261015/README.md)
records the composed main source and the checks it actually passed.

## Python consumer

`prepare_native_task` produces an `OfferedTask` with call, row and stage identity.
Its default work estimate counts requests; it is not a token or GPU cost estimate.
`build_native_execution(config, physical=RayMapConfig(...))` uses existing Daft payload batches
and Ray HTTP workers. `physical=None` selects the local diagnostic transport.
The builder retains the existing capacity and scheduling policies. Native input defaults to 1 MiB.
`max_request_bytes` may explicitly select at most 8 MiB; a nondefault value requires an explicit
`input_bytes` total at least as large. Ray window, object and optional preparation capacities must
fit that choice before transport creation. PG public factories and wire framing remain at 1 MiB.
The three private assembly factories form one capacity closure;
[boundary and lifecycle qualification](../../../../experiments/results/diagnostics/main_lotus_animals_20261015/README.md)
keeps logical input, prepared suffix, SDK/proxy copies and process RSS distinct.

`NativeTaskSession` registers a query Job and one operator flow. `offer` transfers only its
accepted prefix, leaving the caller responsible for the suffix; offering sends no request.
`advance` returns completion-order deliveries, and `wait` uses Core progress and deadlines.
The consumer decodes a delivery, discards the returned bytes, then releases its lease.
Results remain charged until that release. Empty releases preserve the progress generation;
invalid batches and closed handles remain errors.

`NativeQueryJob` borrows one existing query grant across declared operator flows.
Closing a borrowed session keeps the query grant. The query owner closes the Job after its
consumers stop. `request_cancel` is the cross-thread cancellation signal; the owner thread
performs other Core mutations. Source EOF is not sufficient to seal a method that can still
produce continuation tasks.

Closing a flow does not close the shared backend. Unknown remote work remains charged;
the service owner continues to advance/reap the Engine until it can settle the work.
`execution.close()` returns false while any Job or task remains. Identified Ray payload
failures prove that HTTP was not entered and can settle that task; HTTP transport failures,
worker loss and cancellation alone do not prove remote completion. There is no automatic retry.

## Complete HTTP responses

`FullResponseTransport` carries status, duplicate-preserving header pairs, HTTP version and raw
body bytes in the existing result reservation. The envelope permits at most 8192 encoded header
bytes and 128 pairs; `max_result_bytes` covers both header and body. SDK-specific decompression,
JSON interpretation, usage accounting and error policy belong to the consumer.

`AsyncFixedModelTransport` retains its completion response and HTTP error mapping.
`RayMapConfig.response_mode` defaults to `completion`; the native builder selects `full`.
A separately owned native worker pool must also select `full`. Worker identity includes this
mode, so an incompatible pool cannot be borrowed. Confirmed pre-send failures have no invented
HTTP status and raise `CompletionAdapterError` when decoded.

Both transports reuse an HTTPX client and connection pool. A standard `CookieJar` belongs to
one execute task, including redirects within that request. Concurrent calls and later queries
cannot inherit its cookies. Redirect following remains disabled by default; cancellation
releases that request's cookie context, and the transport owner closes the client.

These are engineering decisions within the existing Core and transport interfaces.
[Native lifecycle tests](../../../tests/execution_provider/test_native_tasks.py),
[HTTPX isolation tests](../../../tests/execution_provider/test_httpx_request_cookies.py),
[complete response tests](../../../tests/execution_provider/test_full_response.py) and
[real CPU Ray tests](../../../tests/execution_provider/test_native_ray_integration.py)
exercise the implementation. Supplier adapters, a complete LOTUS query, GPU benefits and PG
integration on this composed source require their own qualification.

## Optional local asynchronous records engine

[`AsyncRecordsActorOwner`](lotus_records_async_actor.py) plugs into the same Core through
`actor_owner=`. The explicit Movie selector is `semloom-local-model-core-async`; the synchronous
default stays unchanged. Driver Python owns the threaded actor, model Python the bounded private
engine process. Full Core task keys distinguish sessions; terminal receipts remain held until
transport acknowledgement. Unknown work retains both responsibility and input references.
Effective limits are reported, and `max_active_batches + 2` concurrency leaves control slots.

Close exports bounded keyed receipts that the registered transport can add to query accounting.
It does not manufacture a Core terminal or free unknown credit. The [fixed-source CPU evidence](../../../../experiments/results/diagnostics/async_records_actor_20261019/README.md#root-集成公开入口与关闭回执)
separates actor contracts, actual Core/Ray/Daft/LOTUS preparation with a CPU child, the original
return-gate RED and its narrow GREEN. API/direct-child/IPC closure does not confirm GPU or all
descendants. The public runner grants a separate twenty-second cleanup window, including the
owner's fifteen-second private teardown; the original synchronous ten/eleven-second windows stay.

The [cancel/close repair](../../../../experiments/results/diagnostics/async_cancel_close_20261019/README.md)
lets close adopt pending full-key abort intentions within the same original deadline, then close the
same engine. `abort_handoff.scope=parent_control_queue_only` records this adoption separately from
the original cancel RPC. A late RPC returning false stays false; queued control does not confirm a
native abort or terminal. Known usage, unknown Core credits and retained inputs remain owned.
The historical r2 cancel failure is preserved; CPU reproductions do not identify its winning race.

[`CoreCascadeBackend`](lotus_cascade_core.py) reuses production sessions for each original LOTUS
helper/sample/fallback stage. Its provider aliases, bounded stage CSV, model startup and closure
are explicit costs. The [cascade method contract](../../semantic_methods/lotus/cascade.README.md)
owns global calibration and thresholds; this adapter owns no replacement controller.
