# Prepared task execution

Ordinary records use local model IPC and native engines. Typed image requests use
[ImageGateway](../image_gateway.py), [image frames](../image_connection.py) and
[image_execution](image_execution.py). The old fixed-endpoint HTTP factories,
transports, Map configuration and v6 transport protocol are retired. Their exact
source, tests and historical measurements remain available through the
[complete recovery notes](../../../../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md#retired-fixed-http-adapters).
LOTUS VLM/MMQA, Kalypso and the official Ray HTTP baseline retain their own native connections.

Map sessions and their golden metadata import [`map_codec.py`](../wire/map_codec.py)
directly. The redundant `wire/v5.py` export module is removed; protocol version 5,
its actual encoder and the distinct v3/v4 implementations remain.

## Python consumer

[IncrementalExecution](incremental_execution.py) holds an existing Core, the caller's
prepared-task function and service cleanup callback. Its original Job allocation
and flow partition methods remain. It imports no fixed HTTP transport and does not
supply a backend or invent an extra request queue.

[prepare_native_task](native_tasks.py) produces an OfferedTask carrying caller-owned
bytes, call/row/stage identity and a declared work description. The default estimate
counts requests; it is not a token or GPU cost estimate. Its default complete-result
reservation remains 1 MiB, now named by the existing frame-size constant rather than
an HTTP model configuration. Core retains its independent item and total capacities.

NativeTaskSession transfers only the accepted prefix, keeps the suffix with the
caller, and returns opaque completion-order result bytes. The consumer discards the
bytes and releases the result lease. Empty releases retain progress; source EOF alone
does not release a result or seal a method that can produce continuation tasks.
NativeQueryJob reuses one existing grant across its declared flows and closes that
Job after its consumers stop. Cancellation does not prove remote completion.

Service cleanup belongs to the injected owner. Closing a flow does not close the
shared backend; unknown work remains accounted for until Core receives an actual
terminal. The generic task API does not add retries or a replacement HTTP/IPC gateway.
The existing Image backend owns its typed prepare/model lifecycle and worker cleanup.

The [native SDK HTTPX observer](../../experiments/native_http_observer.py) and
shared request budgets remain independent of these removed factories. Its unused
aiohttp session/response wrappers and their three dedicated tests are retired;
the actual HTTPX observation function and its caller-owned accounting are unchanged. The offline
SemFilter calibration reader still recognizes its historical profile name; that
artifact identity does not restore the retired service selection.

## Optional single-model finite record plans

`SemLoomRecordChainCore(..., execution="record_local_sections")` runs a legal finite
Filter/Map/Extract plan using the shared `ChainRun` and one prepared Async model owner.
`execution="fused"` remains the default. This is a Python API option, not an automatic
selector or a change to a baseline's native execution.

These are separate choices: ordinary records `sync`/`async` selects model handoff;
chain `fused`/`record_local_sections` selects batch-stage or record-local progression;
fixed/dynamic admission selects whether the request window changes during a query.
Selecting async alone does not select record-local chains or dynamic admission.
Current defaults remain ordinary records sync, chain fused, and fixed admission.
The research preference for record-local progression does not change those API defaults.

Both `CoreRecordsCapacity` and `CoreChainCapacity` are defined in
[`records_capacity.py`](records_capacity.py). They share validation of per-item bytes,
metadata, progress actions and polling, while retaining their separate request/storage
and fused stage-work calculations. The original owner imports expose the same types.
Capacity values do not import the query owner or construct a model runtime.

Record-local F/M sections use `LotusChainFIFO` over the existing Core/MethodDriver.
Extract waits for actual source EOF, complete predecessor finals and drained result
responsibility; the original parser and relation assembler then run once before continuation.
By default one model is started and finally closed per query, including all its sections.
The explicit [rolling resident factory](#resident-rolling-token-owner) below is a separate
lifetime option. Multiple deployments and arbitrary DAGs are not provided here.

The caller owns Ray 2.56.1. Preparation/model batch sizes must both be 1; explicit
`max_active_batches` declares the outer request slots and their Core/actor/IPC reservations.
The shared records resolver also checks this path; the engine's per-iteration sequence
limit is a separate configuration and is not rewritten. Formatting/parsing remains in the
method caller and no Filter prepare actor is allocated. Core/MethodBudget logical limits
do not bound whole-query relations, audit memory, process RSS or GPU KV bytes.
The full-query mode defaults to request-count admission (`token_budget=None`).
An explicit existing `ChainTokenBudget` can be passed to the same owner:

```python
owner = SemLoomRecordChainCore(engine, resources, execution="record_local_sections",
                             token_budget=budget)
```

This option supports complete Filter/Map plans with one model deployment and one model role.
Extract and model-role changes are rejected before source consumption or model startup;
their existing count-mode behavior remains available. The budget's engine and exact set of
stage output estimates must match the query. The owner copies its declared configuration.
Every legal continuation is estimated from its actual prepared messages and stage generation,
including columns produced by earlier calls. The existing Core alone holds both the request
limit (`max_active_batches`) and token work limit (`budget.active_tokens`); the latter reserves
estimated complete input plus the stage output cap, rather than actual KV bytes or GPU work.
Expected output is diagnostic and does not reduce that reservation. Confirmed terminals and
the existing producer-retirement protocol settle work; cancellation alone cannot release it.

<a id="chain-token-batching-boundary"></a>
`token_budget` here is an in-flight reservation limit W, not a per-batch packing budget B.
`LotusChainProgram._step` estimates each actual prepared continuation; `MethodDriver._submit`
offers one task and `LotusChainActorBackend._submit_task` sends `[row]` to the actor.
Core checks the sum of active reservations; an authoritative terminal or validated producer-retirement
proof can settle a reservation. Cancellation or an unknown result alone cannot do so;
retirement does not upgrade the unknown result to success.
No batch membership or close boundary is formed from that sum. Preparation/model batch rows
remain 1; C separately limits active requests, and vLLM owns its internal compute batches.
Existing `PendingBatchBuilder` and text organizer token packing are not wired into this chain.
The desired per-batch token organization and its minimal reuse plan are in the
[data organization plan](../../../../experiments/plans/数据组织.md#chain-token-batching).

Source positions are increasing and protocol occurrence IDs unique; user index labels
and business IDs may repeat. A query failure stops continuation and retains earlier facts.
Late receipts may supplement the stopped query's report but cannot rerun a parser or
produce successors. Actual unknown results and known usage remain; producer retirement
and physical GPU exit are distinct checks.

See [whole-query CPU cases](../../../tests/execution_provider/test_lotus_record_local_query.py),
[whole-query token cases](../../../tests/execution_provider/test_lotus_record_local_tokens.py),
[prepared owner cases](../../../tests/execution_provider/test_lotus_prepared_actor_owner.py),
the [shared method API](../../semantic_methods/lotus/README.md#record-local-sections)
and [actual qualification scope](../../../INFRA_STATUS.md#record-local-sections-c827).

## Optional completion progression on the same async backend

The record-local owner defaults to `progression="record"`: each completed record
can submit its next legal stage. An explicit `progression="batch_stage"` waits for
the other members of that record's fixed group to complete the current stage:

```python
owner = SemLoomRecordChainCore(engine, resources, execution="record_local_sections",
    progression="batch_stage", progression_batch_rows=2)
```

This option requires FIFO, count admission, one model deployment and role, and a
complete Filter/Map plan. Unsupported plans reject before source consumption or
model startup; existing default Extract relation barriers remain available.
Groups follow accepted source order. A full group closes at its declared size;
source EOF closes the last partial group. Filtered/final members have no further
stage to wait for. Each subsequent stage has its own completion barrier.

`progression_batch_rows` counts records in a dependency group. It is independent
of token work W and does not build a native inference batch: both preparation and
model request batch sizes remain 1. The group must fit the retained row and byte
grant, currently derived from `max_active_batches`; reject oversized groups
instead of waiting for members that cannot be admitted. A singleton at C1
degenerates to per-record progression.

Both policies use the same `LotusChainProgram`/`MethodRun`, Core, actor and async
IPC. Original formatting/parsing and bounded continuation preparation can occur
before the group is complete; only successor submission is gated. Confirmed Core
deliveries are released immediately, while retained row state remains charged to
MethodBudget. Failure/cancel stops successors and keeps the existing unknown and
producer-retirement rules. `close()['progression']` records the actual policy.

The caller continues to own and retain Ray across queries. This batch-stage option owns
and closes one fresh model per query; it does not implement cross-query model
borrowing. The old fused path remains the default complete implementation and
receives no Ray wrapper. This group barrier is an explicit internal mechanism
comparison, not a replacement for native system baselines or GPU qualification.
See [controlled async IPC tests](../../../tests/execution_provider/test_lotus_record_local_progression.py),
[method/request differentials](../../../tests/semantic_methods/test_lotus_chain_composition.py)
and [current scope](../../../INFRA_STATUS.md#record-local-completion-progression).

<a id="prepared-request-batching"></a>
## Optional prepared-request submission batches

The synchronous `execution="fused"` entry accepts an independent application batch
configuration. Its default `prepared_batching=None` keeps the previous submission
boundaries and performs no additional tokenization. For a fixed-row/token comparison:

```python
from src.semantic_methods.lotus.prepared_batching import PreparedBatching
from src.semantic_methods.lotus.records import RecordsResources

# Use the same engine, resources, chain and source for each separate owner/run.
resources = RecordsResources(cpu_workers=1, model_batch_rows=32, max_active_batches=1)
fixed = PreparedBatching("fixed_rows", max_rows=32)
tokens = PreparedBatching("token_budget", max_rows=None, token_budget=17506)
owner = SemLoomRecordChainCore(engine, resources, prepared_batching=tokens)
rows = owner.evaluate(source, chain, expected=expected, query_id=chain.query_id)
try:
    result = list(rows)
finally:
    report = owner.close()
groups = owner.request_batches
```

These numbers are example capacities, not calibrated recommendations. In token mode,
`max_rows=None` means **no application batch row limit**; omitting `max_rows` also
selects None. B, the independent logical-byte allowance and successful source EOF
determine the groups. `resources.model_batch_rows` is only a read chunk size;
neither it nor `EngineSpec.max_num_seqs` closes an application token batch.
No larger fixed row number is substituted. Core's stage-call accounting uses the
whole query's already-declared call upper bound, which cannot limit any individual
batch and is never passed to the batcher.

Fixed-row mode requires a positive `max_rows` and retains the existing bounded
fused behavior (`max_rows <= model_batch_rows`) for the row-based control. An explicit
positive row cap in token mode is accepted only for compatibility with historical
limited configurations. In particular, the old B=17506/max_rows=128 measurement
remains a row-capped result; it does not qualify the no-row-limit token target.
Future SemBench runs for that target must record `"max_rows": null`, and keep
byte/context/engine capacities separately visible. Do not change a running unit's
configuration or reinterpret its old measurements.

The cross-read token path currently accepts **one Filter or one Map stage**, one
deployment and one role. Multi-stage token chains fail before source consumption
and model preflight, rather than silently keeping the old per-block splits. The
original fixed-row chains remain available. Successor buffering/re-estimation
across a multi-stage chain is pending; no new operator barriers or pipeline modes
are introduced in this synchronous path. Extract rejects this fused configuration;
the optional async record path is described separately below.
The existing serial requirement `cpu_workers=max_active_batches=1` remains.
The tokenizer must own the chat template (`EngineSpec.chat_template=None`).

The original method creates complete chat messages. A local tokenizer counts the
full rendered prompt; each request adds the stage's `generation.max_tokens` output
cap. **B** targets the sum of these token estimates in one application submission.
It is configurable independently of the number of concurrent batches **C**, which
remains 1 in this candidate. Increasing B can put more short requests in each list;
it does not increase C. A single request's context limit and vLLM's internal
scheduled-token budget are separate constraints, not limits on application B.
A budget oversize request is submitted intact as an explicitly flagged singleton;
no prompt is truncated or split.

`PendingBatchBuilder` retains a partial batch across read chunks. It closes at B,
before the next complete request would exceed B, at the independent byte guard,
or on successful source EOF. Only an explicitly configured legacy row cap can
produce a `max_rows` flush. An arbitrary read tail does not flush. The
source is finite and synchronously available; timed/blocked-source flush is not
implemented. The existing `CoreChainCapacity.item_input_bytes` separately guards
the complete wire rows: raw records, prepared messages and prepared identities.
The charge includes JSON framing and escaping. A single request beyond that
byte allowance fails before submission. There can be one bounded lookahead request;
these are logical buffer limits, not a whole-query RSS or GPU-memory bound. Expected
source declarations, audit/consumer ledgers and worker copies remain outside them.

Each selected group enters the existing worker/private process and native synchronous
`LLM.chat(list)` API. The token producer prepares and estimates each request once;
the worker reuses those messages and costs, validates source/call identities and order,
and checks their method/deployment/generation/policy signature and B bounds. Only
scalar cost/flush information and that signature use bounded control metadata;
prepared messages remain in charged input. The same method interpreter consumes
the responses. Fixed-row and disabled paths keep their original preparation behavior.
Charging the complete wire row may close a byte-limited group earlier than the old
raw-record-plus-message estimate. vLLM owns internal scheduling and may process a list larger than its
running-sequence capacity; the adapter does not cut it at `max_num_seqs`.

`owner.request_batches` retains group IDs, membership, token/output-cap costs,
flush reasons (`token_budget`, `max_rows`, `max_bytes`, `source_exhausted`) and
oversize flags. Per-call results remain authoritative for acceptance and usage.
The returned list owns independent nested batch records. The result accountant
provides a scalar count for close reporting without constructing these snapshots.
Failed/malformed replies, cancellation or deadline stop further batches. Known
prefix facts survive; unsent source rows do not become unknown model work. An
uncertain attempted batch keeps the original Core credit and cleanup responsibility.
There is no retry or model lifetime change. The disabled default remains unchanged
and does no additional tokenizer work.

The separate record-local `token_budget` argument controls in-flight work **W** and
remains off by default; this serial B path does not combine it with C or W. The
existing low-level `PendingBatchBuilder.set_token_budget` only allows changes while
a batch is empty. Runtime feedback has not been connected to that boundary here:
this candidate uses a constant configured B. Future joint B/C feedback is research,
not part of this fix. Historical concurrency sweeps are not evidence of a B sweep;
the new CPU cases vary B at fixed C solely to verify membership behavior.

[CPU query/IPC cases](../../../tests/execution_provider/test_lotus_prepared_batching.py)
exercise the real Core entry, original LOTUS methods and private process protocol with
deterministic inference. [Reuse checks](../../../tests/execution_provider/test_lotus_prepared_reuse.py)
compare native input/parameters/results, count preparation/estimation passes and reject
changed identities or configurations. Real-model coverage and quality/performance
limits are source/task specific; [qualification status](../../../INFRA_STATUS.md#prepared-request-batching)
records the completed Q1 reuse comparison and the remaining unmeasured combinations.
The observed comparison does not establish strict per-row equivalence, a stable
speedup or recovery of the whole historical API gap.

## Optional async record token batches

The optional record path accepts `PreparedBatching("token_budget", None, B)` on
`execution="record_local_sections"`, with fixed `batch_concurrency=C_batch`.
It uses the existing `MethodRun`, Core, async actor and engine process. It does
not add a second model owner. The default remains `prepared_batching=None`.
This interface requires `progression="record"`, FIFO, no custom method-ready
selection and `token_budget=None` (the independent in-flight token guard W is off).
`batch_concurrency` defaults to 1 when batching is enabled, and cannot exceed the
declared request capacity. Fixed-row and fused references retain their interfaces.

```python
# B and C_batch are example settings, not a measured performance recommendation.
resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                             max_active_batches=128)
owner = SemLoomRecordChainCore(engine, resources,
    execution="record_local_sections",
    prepared_batching=PreparedBatching("token_budget", None, 17506),
    batch_concurrency=4)
```

The units remain separate. **B** is the sum of complete rendered prompt tokens
plus each stage's maximum output in one application transport batch. **C_batch**
counts transport batches with members still owning compute, including unknown
members. **R** is the Core member-request capacity, inherited here from
`resources.max_active_batches`; it also bounds resident method rows in this
minimal path. That inherited R bound can limit the currently available frontier;
there is no independent per-batch row cap. Engine `max_num_seqs` and
`max_num_batched_tokens` govern native scheduling rather than application batches.
The application bulk frame is not a claim that vLLM executes a physical batch of
the same size at once.

An optional `rolling_refill=True` uses the existing **request** window R instead
of a whole-batch window. Omit `batch_concurrency` and `batch_feedback` in this
mode; mixing either with rolling refill is rejected before model startup.
The default is `False`, preserving the existing fixed/feedback C_batch path.

```python
owner = SemLoomRecordChainCore(engine, resources,
    execution="record_local_sections",
    prepared_batching=PreparedBatching("token_budget", None, B),
    rolling_refill=True)
```

A confirmed member terminal frees its request reservation; result consumption
and row release return their separate storage grants. A new compatible group
can then use that capacity while a slow member of an earlier group remains in
flight. With legacy budgets, R bounds Core compute, resident rows, the actor and IPC owner;
byte/result reservations and unknown responsibility are unchanged. A submitted
group remains immutable: refill creates another transport batch, not new members
inside the old batch or a change to vLLM's native continuous batching. B only
organizes each handoff, and the ready-frontier/EOF tail does not wait to fill B.
Prepared async transport now carries successfully stored member ACKs on its next
bounded result poll, including a final poll after the last member transfer.
The actor applies the validated ACK set before selecting the next page. A lost
response can repeat the ACKs; an unacknowledged page remains readable. The result
frame and its byte limit are unchanged. ACK metadata is bounded by the original
accepted batch and the existing input-byte limit; it does not cache another result
copy. Post-transfer ACK responsibility can outlive the corresponding Core terminal.
An unknown receipt may advance delivery while retaining its model borrowing.
Sink rejection never queues an ACK. Existing single-request/custom actor handoffs
keep their own ACK interface. A settled query drains final ACKs before actor kill;
unfinished work retains producer-first cancellation and receipt recovery. All drain
waits use the existing cleanup deadline. The
[ACK/page cases](../../../tests/execution_provider/test_lotus_batch_poll_ack.py)
cover these CPU mechanisms; real Ray/GPU latency still requires a matched run.
Extract retains its complete-relation barrier. The ordinary records factory,
Movie JSON `check/run` and isolated-session plan accept the same boolean option.
Explicit `CoreRecordsCapacity.budget` separates the fixed request window C, hard request slots R,
method residency and IPC grants; see the [budget fields](../../experiments/movie_matrix.README.md#显式独立资源预算默认关闭本地cpu候选).
Declarations and IPC resolution live in [records_capacity.py](records_capacity.py);
execution selection and historical options live in [records_profile.py](records_profile.py).
The existing `lotus_records_core` imports still refer to the same capacity types.
Low-level direct constructors and nonrolling whole batches retain the old R translation.
The ordinary async Filter/rolling factory now resolves default storage from existing
IPC byte declarations and per-item envelopes via `resolve_records_runtime`. Soft C
does not resize those allocations. Explicit complete budgets and typed IPC fields
retain their values. With `R=None`, typed limits or an explicit JSON `max_inflight` supplies
the request count; other partial fields do not supply one. Complete JSON is parsed once
into the same type. Partial null slots and input/result totals inherit their owner defaults.
The SDK and Movie/Session now share the default R=None. Supported ordinary async and rolling
paths resolve existing finite grants; missing sources fail before startup. An explicit R
retains its original owner priority. Direct static, nonrolling prepared and low-level paths
require their supported finite declaration. These declarations do not detect physical capacity.
The [capacity ownership inventory and CPU evidence](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md)
separate that remaining work from the independent budgets already supported.
Pure resource parsing and copied snapshot report fields live in
[records_feedback_observation.py](records_feedback_observation.py). The existing sampler
retains locks, clocks, consumption and decisions; the organizer and its frame-size cache are unchanged.
Shared result encoding and bounded model-owner adaptation live in
[records_model_adapter.py](records_model_adapter.py). Async actor imports no longer
load the records query host. Existing actor imports remain available;
the transport, result reservations, ACK and shutdown behavior are unchanged.
The model-side async call ledger, result retention and private IPC live in
[records_async_model.py](records_async_model.py). Driver-side Ray allocation,
data RPC permits and terminal observation stay in
[lotus_records_async_actor.py](lotus_records_async_actor.py). Its allocation hook
imports the same actor class directly; actor-private observations and tests use
the implementation module, including their clock and encoding substitutions.
Shared records I/O and producer cleanup live in [records_transport.py](records_transport.py).
Both records query hosts use that module; the chain backend and host can load
without importing the single-filter query host. Existing transport imports keep the
same objects. Capacity, unknown results and receipt ACKs retain
the same owners; this extraction introduces no scheduling loop or counter.
Record-local source validation, MethodDriver progress, complete Extracts and result
consumption live in [records_query_runtime.py](records_query_runtime.py). The chain
owner assembles its model, transport and capacity; the iterator loads independently
of those constructors; its existing query-host import refers to the same iterator.
Its preparation order, deadlines, cancellation and result release are unchanged.
Fused batch transfer and result iteration live in
[lotus_chain_fused_runtime.py](lotus_chain_fused_runtime.py). The chain owner retains
configuration, model assembly, relation-phase transitions and final resource cleanup.
It imports the original transport and iterator classes directly; there is no new
owner, facade or scheduling loop, and record-local transport is unchanged.
The retained single native snapshot RPC and its cancellation/audit state live in
[records_native_sampler.py](records_native_sampler.py). Feedback policies reuse
that sampler; it imports no query constructors or window controller. Existing
source qualification, close and unknown-RPC ownership remain unchanged.
Legacy-budget receipts report `rolling_refill=true`, `C_request=R`, `C_batch=null`; observed
batch/member counts remain separate. W, dynamic request-window feedback and
joint B/C control are not enabled by this option. Full expected relations and
audit retention remain outside these active-row logical grants. This is an
engineering composition of existing admission and transport contracts; only
[CPU fixture checks](../../../tests/execution_provider/test_lotus_token_rolling_refill.py)
and [normal-entry checks](../../../tests/experiments/test_movie_async_token_entry.py)
cover it so far, with no real-Ray/model/GPU performance qualification.

The method prepares actual messages once. The organizer counts each Core task's
full message/template plus stage output cap and groups compatible ready requests
across source read chunks. A batch flushes at B, before an incompatible request,
at the separate byte/request capacity, on successful EOF, or when the current
legal ready frontier is exhausted. A short frontier is submitted without waiting
for a future dependency to finish. Oversize-B requests stay intact singletons.
There is no flush timer or asynchronous blocked-source contract.

One selected group uses one actor data RPC and one private engine data frame.
Each member retains its own request identity, result, ACK and cancellation
responsibility. Its admitted result-byte reservation also bounds poll replies and
close-audit receipts, even when the actor item allowance is larger. Oversized
confirmed completions become `ResultCapacityExceeded` failures with known usage
retained; unknown outcomes remain unknown. A fast member can release its result
and start its legal successor
while a peer remains unfinished, subject to the explicit C_batch/R/byte grants.
Closing a batch does not add a semantic completion barrier. Existing relation
barriers, including Extract, remain. The finite same-deployment/role chain scope
comes from the existing record-local owner; arbitrary DAG fan-in and cross-query
model reuse are not introduced here.

`report["async_token_batches"]` records selected groups, recent member/token/flush
information, peak active batch/member counts and encoded RPC/frame byte bounds.
IPC accounting covers the JSON body, excluding its four-byte framing header;
Ray serialization overhead, Python object overhead, expected/full relations and
audit retention are outside those logical limits. This is not a whole-query RSS
bound. Unknown work retains its reservation until an authoritative terminal or
the existing exact-owner producer-quiescence proof; retiring borrowing does not
change an unknown model outcome into success.

Closing the producer does not mean that bounded result pages have been drained.
The private `more_results` reply flag identifies additional currently available
receipts beyond the returned page. Closed delivery retains only a grant-bounded
ACK frontier, advances after each member ACK, and leaves the close audit snapshot
intact. Unacknowledged pages remain readable. An unknown receipt's delivery ACK
does not refund its separate borrowing responsibility. Close retries retain the
previously published audit prefix. A repeated receipt can retry a lost ACK only
after that member has completed local validation and sink transfer.

Poll sizing counts each encoded member and the complete JSON envelope once,
then validates the final bounded frame before exposing any receipt. Sizing is
local to that poll; no retained page cache is added. The original page boundaries,
per-member result allowance and ACK/unknown ownership remain.

The default tokenizer is loaded from the declared engine tokenizer/model. A caller
may supply `batching_tokenizer` for a controlled environment; matching its rendered
template to the native model remains the caller's responsibility. CPU fixtures use
a small tokenizer and inference substitute. [Query and independent-member cases](../../../tests/execution_provider/test_lotus_async_token_batches.py)
and [lifecycle boundary cases](../../../tests/execution_provider/test_lotus_async_batch_lifecycle.py)
check this optional API. The early standalone `c79fcc6d` candidate excluded PR83;
the merged integration starts from `2212e331` and adds the async core, closed-page
repair and ordinary entry. Exact `41ce4c24` now has independently reviewed real
Ray/model/GPU functional coverage and the Movie Q1/Q2/Q3 comparison matrix.

The feature remains disabled by default. No overall cold-query speedup is
established; Q1 strict native-output parity fails and all Q3 configurations return
COUNT=7 against gold=14. MMF complete returned-payload parity, full-Core GPU
capacity-failure mapping, independent streaming read-chunk GPU cases, P99 and
saturation remain unverified. That qualification does not cover subsequent feedback or
[rolling resident reuse](#resident-rolling-token-owner) candidates; joint B/C control remains unsupported.
The [current scope](../../../INFRA_STATUS.md#async-record-token-batches) and
[public PR84 summary](https://github.com/3444374/ai-operator-execution-optimization/pull/84)
separate completed checks, measured negative results and remaining limits.

The ordinary records factory `create_records_core` accepts the same optional
`prepared_batching` and `batch_concurrency`. It requires async mode without stage
timing. Non-rolling batch mode excludes resource-feedback admission; rolling mode
can opt into request feedback as described below. A finite source must provide its
validated `rows()` relation as well as the existing `preflight`, `scan_spec` and
`expected` contracts. The adapter binds one ordinary Filter stage to the unchanged
method and generation, using the existing record-chain owner. It preserves the
original chain call ID as `chain_call_id` while mapping each receipt into the
ordinary Filter ledger's declared identity; no request is replayed. Completed
reports check the actual B/C_batch/R and full member indices against the selection.
`transport_batches[].receipt_members` records each member's two identities,
submission confirmation, Core key, native request ID, status and native end time.
Failed/unknown query receipts keep their original usage and confirmation.

This finite chain already retains a full expected relation. The ordinary ledger
adapter delivers raw receipts after the existing complete chain traversal; it is
not a live result-stream API, and does not add a per-batch execution barrier. There
is no new scheduler or engine. The normal Movie config/CLI and Session driver use this
factory; resident is accepted only for the explicit rolling mode below. Admission cache
and unsupported row-capped combinations fail before model construction. Configuration and commands
are maintained in the [Movie entry documentation](../../experiments/movie_matrix.README.md#record-token-batch-entry).

### Query packet counters

`last_close_report.async_token_batches.selected_batches` counts nonempty organizer
selections. `selected_seal_reason_counts` covers the entire query with five fixed
keys: `token_budget`, `request_capacity`, `frame_bytes`, `compatibility` and
`ready_frontier_exhausted`. Their sum equals `selected_batches`; `recent_batches`
remains only the latest 64 examples. A reason records the existing branch order,
not proof that bypassing it would admit another legal member. Selection is not
backend acceptance. A pending Core selection reused after `NOT_ACCEPTED` does not
call the organizer again.

`data_batch_rpcs` retains its existing count of submit calls that returned an
awaitable/reference. `batch_handoff.submit_operations_started` also includes a
synchronous submit exception. Neither counts the owner's internal backpressure
retries. `batch_handoff.actor_ack_counts` separately counts
the first recognized actor `accepted`/`rejected` reply, or `unknown` if no recognized
reply was observed. `pending_actor_acks` counts operations still awaiting that
classification. Result polling, lost result ACK retries and later member failures
do not reclassify the initial handoff. These are transport batches, not native GPU
batches or per-request success counts. An actor acceptance can also retain a
cancelled member without submitting it to the engine. An unknown handoff counter
never refunds any responsibility.

Both counter sets are fixed-size snapshots and reset with the existing per-query
organizer/transport. Model-process `submitted_batch_frames` remains an owner-lifetime
IPC queue counter; it is not silently treated as a per-query count after reuse.
The [CPU counter tests](../../../tests/execution_provider/test_lotus_batch_counters.py)
cover history eviction, handoff failure/cancellation, polling and resident reuse.
Counter qualification is CPU-only; no new model performance evidence is implied.

<a id="fixed-rolling-token-packing-ablation-cpu-experiment-only"></a>
The former positive-B token-packing bypass and its CPU microbenchmark are historical only.
Their exact source and experiment identities are [recoverable](../../../../experiments/reproduction/native_baselines/README.md#retired-positive-B-token-packing-bypass).
Current rolling requests omit `prepared_batching` to skip token packing; explicit B still enables packing.

### Optional request resource feedback (local CPU candidate)

Ordinary Core and rolling token factories select `ResourceRequestConfig` and the
reviewed `ResourceRequestController` for `admission={"kind":"resource_feedback"}`.
Omitted admission remains fixed. An enabled query starts at one request and uses
only actual ready work, source-qualified native frames and applied owner/Core/IPC
and result reservations. These declarations are not measured GPU capacity.
The same retained snapshot RPC supplies native and optional physical observations.
Source age is conservatively mapped to driver time. Observation and source are
read together; neither a new RPC nor a newer cached frame refreshes an old source.

The adapter retains original completion receipts, legal successors, independent
member refill, Extract relations, unknown work and ACK ownership. A smaller
window stops future selection. Already accepted requests drain under their
original ownership. Native rejection counts remain unknown where no source is
connected. The existing `require_memory_feedback=true` option now requires a
source-qualified cgroup v2 kernel interval for growth. It remains false by default.
No KV/busy percentage cutoff or cross-query learned hint
enters this policy. Missing backend ready capability fails explicitly.

The implementation is [records_request_feedback.py](records_request_feedback.py).
[Public wiring evidence](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/public_request_wiring_checks.json)
records 309 selected CPU methods, 308 successes and one upstream-checkout skip.
Model, real Ray and GPU qualification remain pending.

The same resource RPC reads kernel memory stalls and reclaim events in the model
actor's visible cgroup hierarchy, including ancestors. Source/PID/query identity,
sequence and source time are checked independently of native inference progress.
A repeated sample retains its original driver time. Missing data, unsupported
systems, first reads and reader errors remain unknown; changing the source in a
query cannot clear previously bound pressure. Known pressure uses the existing
controller's reduction, preserving all accepted task/result ownership.

This is interval feedback, not a RAM allocation or RSS ceiling. A clean interval
does not establish free bytes. Unknown feedback prevents growth; an initial one
request or the existing verified window may continue. It cannot prevent every
allocation failure, stop all work, or cover driver/Ray/model-worker allocations
outside the observed hierarchy. Independent physical backing storage, complete
relation accounting and cross-process recovery remain pending. The kernel reader
runs only through already enabled `MetricSources` reads, adds kernel-file I/O to
that measured path and starts no extra thread or RPC. See the
[source and CPU checks](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#host-memory-pressure).

<a id="retired-request-feedback"></a>
The previous request strategy, its named profiles, cross-query hints and request
admission cache are retired from current entry points. Recover their exact
implementation and instructions from commit `9da299e90f79fd0b71f2a4983a90884f8ba960b5`:
[adapter notes](https://github.com/3444374/ai-operator-execution-optimization/blob/9da299e90f79fd0b71f2a4983a90884f8ba960b5/code/src/execution_provider/adapters/README.md), [runtime notes](https://github.com/3444374/ai-operator-execution-optimization/blob/9da299e90f79fd0b71f2a4983a90884f8ba960b5/code/src/scheduling/runtime/calibration.README.md) and
[configuration guide](https://github.com/3444374/ai-operator-execution-optimization/blob/9da299e90f79fd0b71f2a4983a90884f8ba960b5/docs/%E6%89%A7%E8%A1%8C%E9%85%8D%E7%BD%AE%E4%B8%8E%E4%BC%98%E5%8C%96%E5%BC%80%E5%85%B3.md). No compatibility execution route
is installed for these retired interfaces. The earlier five named source
projections retain their separate [recovery records](../../../../experiments/reproduction/native_baselines/README.md).
Fixed-B `batch_feedback` remains supported with its existing
`SupplyFeedbackConfig` and batch trial implementation. Its `ResourceFeedbackConfig`
and `ResourceFeedbackController` do not enable the retired request entry.
Previously saved full `batch_feedback.options` objects must drop the obsolete
`search_policy="reference"` and `physical_probe_policy="idle_majority"` fields;
the current parser rejects those selectors. Keep `max_batches` and every other
declared batch option unchanged. Removing just these two inert defaults produces
the same current default configuration; it does not replace custom options with
defaults or translate retired request strategies into batch feedback.
The shared work-comparison function remains used by current request and batch
controllers; the finite point-search helper remains used by batch feedback.
Joint B adjustment is unimplemented.

<a id="resident-rolling-token-owner"></a>
### Resident rolling token model owner (local CPU candidate)

`create_records_core(..., prepared_batching=batching, rolling_refill=True, resident=True)`
retains a compatible prepared model/engine/worker/IPC across sequential ordinary Filter
queries. The existing Movie Session selects it with `lifecycle="resident"`; no new plan
schema or model pool is introduced. Fixed rolling RF retains its declared C;
request feedback uses the admission configuration above and starts from one for
every query. It retains no cross-query search state. C_batch is null.
Non-rolling token batches and batch feedback remain excluded from this resident
path. Default `resident=False` preserves the single-query lifetime.

```python
owner = create_records_core(method, engine, generation, resources, profile=profile,
    prepared_batching=batching, rolling_refill=True, resident=True)
try:
    model_ready = owner.start()  # Original native readiness; no application inference.
    first = list(owner.evaluate(first_source, query_id="first"))
    first_result = interpret_complete_result(first)  # Caller owns business parsing/projection.
    reused = list(owner.evaluate(next_source, query_id="reused", method=compatible_method))
    reused_result = interpret_complete_result(reused)
finally:
    owner.close()  # Final model/IPC shutdown, separate from healthy query EOF.
```

Queries retain fresh sources, ledgers, result consumers, token organizers and
measurement state. Request measurements reset for each query; the caller owns
business parsing and closes the owner if that parsing fails. The existing
SessionEngine keeps monotonically increasing numeric session IDs.
Normal EOF proves query transport/Core/actor/IPC responsibility and feedback RPCs are
drained before reporting `model_retained=true`, `model_owner.status="retained"`.
Completed iterator reports remain independent snapshots. Failure, cancellation, early
consumer close or unconfirmed draining stops reuse and closes the owner; unknown
outcomes are not cleared. Model-name changes reject before work; a different fixed
deployment, generation or capacity needs a new owner. This is not arbitrary chain reuse.

The [CPU session cases](../../../tests/experiments/test_movie_token_resident_session.py)
use real CPU socket children and a threaded Ray substitute. Real-Ray/model reuse,
physical cleanup and query performance remain pending for this exact candidate.
Native KV/cache effects and cross-query scheduling experience are distinct;
see [timing and scope](../../../INFRA_STATUS.md#model-query-reuse-scope).

## Optional fixed-B supply feedback (local CPU candidate)

`SemLoomRecordChainCore` and the Python `create_records_core` factory accept
`batch_feedback=SupplyFeedbackConfig(max_batches=operational_batch_ceiling)` from
`src.scheduling.submission_control.stage_work`. This requires the existing
record-local token path, `batch_concurrency=1`, FIFO, record progression and W off;
the ceiling must fit R. Omission retains the fixed path and installs no sampler.
The normal Movie `check/run` JSON and isolated session plans accept the same
`batch_feedback` object (`max_batches` and optional `options`); omitted/null keeps
feedback off. Partial `options` inherit `SupplyFeedbackConfig` defaults, including
computed work, local pairs and geometric exploration. The shared factory and
typed config own validation; no alternate runner is installed. See the
[normal entry](../../experiments/movie_matrix.README.md#record-token-batch-entry).
Resident sessions remain unsupported by this finite-query token path.

Only the application **batch** window changes. B remains the caller's fixed work
budget, and R, Core bytes, IPC slots/bytes and native startup declarations remain
separate. The start is one batch, with no task token reference, gold, cached best
setting or hard exploration timeout. The explicit ceiling is an operational
limit, never a measured performance platform. Runtime B adjustment is deferred.

The existing method-ready callback and Core sink count preparation admissions and
submissions; original confirmed receipts provide completion and input/output/cache
work. Core task `WorkDescriptor`/`StageWork` supplies the actual ready-work units.
Window reports pair these rates/demand and logical capacity with same-owner,
same-session native running/waiting/KV/preemption frames over the existing IPC
feedback channel. Prepared rate is method-to-Core admission, not all CPU work.
Repeated/stale/missing frames cannot establish spare hardware. Queued backend work,
no ready supply, tail/dependency wait, pressure and configuration boundaries have
separate reasons. A feedback gap invalidates a trial and returns to its last safe
setting without aborting members or releasing unknown credits. It clears the
interrupted comparison, retains elapsed/attempt/change budgets and trial cost,
and permits new measurements after fresh feedback resumes; it is not a permanent
stop. Only the interrupted candidate's per-query visit marker is released when
eligible; its attempt count is retained. Failed snapshot cancellation retains the outstanding RPC until its future
settles, so repeated polls cannot stack unconfirmed sampling requests.
Pressure protection keeps priority; a pending gap is retained until a qualified
non-pressure frame invalidates the comparison, so protection cannot hide it.

A ready backlog survives normal gaps between compute batches. An instantaneous
active-batch count below the selected window does not erase that window's
measurements: its dispatch gaps remain in measured wall time. No progress cannot
form a new measurement. Empty ready supply, stale feedback, queue/capacity and
tail boundaries retain their existing guards. A window change still clears the
partial measurements and waits for transition/settling before measuring anew.

<a id="snapshot-rpc-ownership"></a>
Control RPC evidence uses the existing normal query close receipt at
`owner_close.batch_supply_feedback`. `snapshot_rpc_receipts` records each collected
return or result exception, its local request index/session, driver timestamps,
cancel outcome and returned native identity/sequence/time headers. It stores no
prompt or result text. `eligible_for_provider` means schema/identity eligibility,
not proof that the provider or controller consumed a fresh frame.
After stopping the sampler, close/report nonblockingly collects a ready original
Future; the final report also rechecks after the existing actor close. A later
explicit close can publish new terminal evidence without changing the earlier
report object or model-result ledger. There is no new snapshot or blocking wait
to obtain this evidence.
If the sampler outlives close's join timeout, each report copies the request slot,
receipts and reference flags under one short ownership lock. Dispatch, Future
access and provider joins run outside that lock; a live sampler remains unknown
until its result is collected. Driver clock failures and non-finite or overflowing
source timestamps are stored as `None` with the corresponding `*_error_type`. These
metadata failures do not erase a known RPC return/exception or stop owner cleanup;
complete RPC terminal evidence does not imply complete timestamp metadata.
Returned frames also use the optional native receive/reject audit and preserve
source UID/session/sequence/time and a separate driver receive timestamp. Audit
callbacks run after terminal collection, outside the ownership lock; provider
freshness remains a separate decision.

With the existing optional provider observer enabled, each control RPC also records
local Future callback registration, last poll observation/count, ready notification,
collection start and result-read timestamps in that same receipt slot. The timing
hook adds no RPC or result read; observer off installs no timing callback.
`issued_at_s` precedes the local `.remote()` call, not a measured wire send. The
ready callback timestamps notification in the driver, an upper bound on local
readiness; late registration, delayed callbacks or missing clocks leave earlier
readiness unknown. A positive collection-minus-notification interval bounds some
post-readiness local waiting; it is not a pure polling or transport measurement.
Poll observations are timestamped after `.done()` and are not atomic readiness
bounds. Callback completion alone never settles a remote RPC. The optional
`control_request_index` joins RPC/sample/consumer metadata; it is a snapshot RPC
index, not a model-request count. Duplicate source frames retain the adopted RPC's
consumer identity. Native, driver and provider clock domains are labelled separately;
cross-process differences require valid clock anchors and are not exact wire latency.
These fields do not change RPC issue cadence, freshness, three-window confirmation,
batch/request admission units or the default-disabled feedback policy.

The existing batch-feedback sampler separately signals a coalescing Event when its
original Future completes, with observation on or off. The same provider thread
can then collect and publish a valid result during its regular wait. The callback
only signals: it never reads a result, releases a reference or issues another RPC.
An early-collected result no longer consumes the next regular dispatch turn.
That turn can issue the next RPC if the previous one reached a known terminal;
with a 0.25s poll and a promptly returned RPC, issues therefore occur every 0.25s
instead of every 0.5s. Completion alone never dispatches another RPC. Duplicate
notifications cannot create RPCs or refresh a source sequence/age. Current
session/engine/age validation still applies before publication. Missing wake registration falls
back to the original periodic poll, with an explicit receipt error. Close wakes
the wait; cancellation and late terminal evidence keep the existing ownership
rules. An accepted provider generation also signals the existing Core wake;
ordinary and rolling feedback can consume it before their next control-poll
deadline. Only the owner control thread decides or submits work. The unchanged
request policy may make a provisional trial before the first model completion
when its existing source, ready-work, pressure and capacity requirements hold;
this is not measured performance gain or hardware-capacity discovery. Ongoing
RPCs, local cancellation and unknown outcomes still retain the original reference.
CPU preparation, consumer pauses and existing Core retry deadlines can delay
control or submission. First snapshots from an actor's previous/unbound session
remain rejected. No sampling/configuration threshold or policy default changed.
The optional request-supply path above reuses these same collection and session
ownership rules; fixed rolling without admission still installs no sampler.
The [CPU collection cases](../../../tests/execution_provider/test_lotus_feedback_ready_collection.py)
and [cadence/control cases](../../../tests/execution_provider/test_records_feedback_cadence.py)
cover readiness, configured dispatch ticks, close, unknown cancellation and phase reuse,
including an unfinished first model request and no repeated control notification;
real-backend latency and overhead on this exact new source still require validation.

The optional child IPC publisher now waits for the first actual `NativeStats.record`
scheduler sample and sends that first frame without the mandatory interval sleep.
Later publications retain `feedback_interval_s`, source sequence/time and the same
single-packet/control-byte bounds. No synthetic idle frame is created. Shutdown
cancels a wait that has not received any source; feedback disabled starts no publisher.
This follows the fixed [vLLM 0.25.1 output-handler stat-logger seam](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/engine/async_llm.py#L590-L653),
which receives actual [scheduler output statistics](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/core/sched/scheduler.py#L1692-L1703).
Those sources may only become available after a model step: availability before
the first model request finishes is not guaranteed. Actor session switches still
require actual admitted model work; unbound/previous-session replies are rejected.
The [CPU IPC first-source cases](../../../tests/serving/test_native_first_feedback.py)
hold the first fixture generation pending while its true fixture statistics cross
the real local sockets, and cover missing sources, disabled feedback, notification
failure, abort and close. They do not establish first-frame timing on real vLLM/Ray.

The separate `last_native` fallback commits and reuses frames only for the current
Core session, under the same lock as `bind`'s session/cache reset. If audit spans a
phase change, the old RPC remains a known terminal receipt, but its frame cannot
repopulate that fallback or supply a pending RPC in the later session. This does
not change native source freshness or the RPC's reference/cancellation lifecycle.

`snapshot_rpc_pending` remains explicitly unknown while no remote result is
available, including failed cancellation or a locally cancelled read Future.
Cancellation request, sampler stop and physical producer exit do not establish
a normal RPC return. `snapshot_rpc_cleanup_status="closed"` and
`snapshot_rpc_terminal_evidence_complete=True` mean all issued attempts have
collected returns/result exceptions and the sampler stopped; a terminal exception
is still labelled `raised`, not a normal frame. The existing owner data-plane
close status and unknown model outcomes keep their meanings; control evidence
must be checked separately. Historical e42 retained-ref flags cannot be upgraded
using these new receipts or process-cleanup evidence.

The supply policy defaults to **no elapsed-time, change-count or candidate-attempt
cutoff** (`exploration_s=None`, `max_changes=None`, `max_candidate_attempts=None`,
`min_remaining=0`). An unresolved default candidate can be remeasured in the same
query. A resolved adjacent point permits the next geometric point; an unresolved
adjacent point cannot be skipped. The operational batch ceiling and original
request/byte capacities remain finite. A finite attempt limit is an explicit
legacy budget choice and exhaustion is reported as censored, not as a platform.
The batch trial `ResourceFeedbackConfig()` defaults to two candidate attempts;
passing that whole options object selects its full settings.
Partial JSON options inherit the supply defaults, including the null attempt
limit. Python callers can likewise replace fields on `SupplyFeedbackConfig(M).options`
or use `SupplyFeedbackConfig.from_dict` without importing legacy budget defaults.

Default rules are engineering choices, not measured hardware saturation criteria.
Their code owners are [supply policy](../../scheduling/submission_control/stage_work.py),
[trial/search policy](../../scheduling/submission_control/adaptive.py),
[batch organizer/sampler](lotus_chain_token_batches.py) and
[ordinary factory](lotus_records_core.py):

| Rule | Supply default and effect | Code basis |
|---|---|---|
| Enable/start | Omitted/null is off; enabled starts at one batch, without a historical hint | `_record_batching_options`, `SupplyFeedbackController.reset`, `BatchSupplyFeedback.bind` |
| Work/capacity | B fixed; explicit `max_batches <= R`; original member/byte/IPC bounds remain | `resolve_record_batching`, `PreparedTokenOrganizer.__call__` |
| Time/change budgets | Both null: no algorithm cutoff; finite values explicitly retain the old budget rules | `ResourceFeedbackConfig`, `_exploration_time_left`, `_within_change_budget` |
| Candidate attempts | Null: unresolved candidates may retry; interruption preserves spent attempts and releases only its own visit marker | `SupplyFeedbackConfig.options`, `_GeometricSearch.available/next/interrupt` |
| Remaining work | Zero: only no pending work stops the scan; a positive value explicitly opts into the old tail/amortization heuristic | `_next_trial`, `_update` |
| Sampling | Poll 0.25s, window 1s, at least 16 completions per measurement; missing/stale/incoherent data cannot establish idle capacity | `ResourceFeedbackConfig`, `update_supply`, `_update` |
| Window changes | Settle old members, one cooldown window, discard the first progress window; none is a total exploration deadline | `_change`, `_update` |
| Candidate selection | Geometric points up to the explicit ceiling; local work-direction comparisons, 3% gain margin; unresolved comparisons can extend to six windows and one fresh safe-reference recheck per trial | `_GeometricSearch.__init__`, `_point`, `_compare`, `_local_pair_evidence` |
| Pressure | Existing KV 0.85 congestion / 0.50 low-load guards, factor 0.5 reduction; confirm KV twice, preemption/explicit memory pressure can act immediately | `AimdConfig`, `_update` |
| Supply/queue | No ready supply, unfilled batch window, waiting backend work, tail/dependency wait or a local capacity boundary holds growth | `update_supply` |
| Completion/evidence | EOF ends useful work; nonmonotonic work stops adaptation; resolved geometric points end that declared search range, not a global optimum claim | `update_supply`, `_update`, `_GeometricSearch.stop_reason`, `report` |
| Point-history scope | Actual work-context change / pressure recovery can reset point-local progress; controller elapsed/change budgets and accumulated trial wall do not reset. A feedback gap does not rebase the whole search | `rebase_work`, `record_pressure`, `resume_pressure`, `_invalidate_feedback_gap` |
| Retention/deadlines | Recent trace arrays retain 64 entries; this does not cap batches, requests, attempts or changes. Existing owner query/startup deadlines remain lifecycle limits | trace/report code; `RecordsResources`, record owner lifecycle |

The old standalone AIMD `low_load_running=64` is not a threshold used by this
trial path (`ResourceFeedbackController` binds the field to its maximum and uses
its own `_update`). Test loop counts/deadlines are fixture bounds, not policy
stopping rules. The policy can still hold on incomplete evidence or end its
declared geometric range; without physical CPU/GPU/free-memory interfaces it
cannot guarantee growth whenever hardware has spare capacity. The 3% comparison
margin can classify a smaller positive gain as `no_gain`. None of these outcomes proves a
hardware limit.

The policy reuses `ResourceFeedbackController` work-aware trials/comparison and
pressure protection, with demand gating. Default comparison requires actual
computed/cached counters; unavailable work keeps the comparison unresolved.
Existing KV protection thresholds are engineering guardrails, not saturation
measurements. CPU/GPU utilization, physical free memory and native startup capacity
readback are currently unknown; IPC startup limits and declared native limits are
reported separately. KV is a usage ratio; free KV bytes are not returned by this
interface. No GPU percentage triggers growth. Mixed-stage cost attribution is
not qualified by the token-work comparison; stage descriptors remain explicit.

[Policy replays](../../../tests/scheduling/test_supply_feedback.py) and
[Core/actor/CPU-IPC differentials](../../../tests/execution_provider/test_lotus_batch_supply_feedback.py)
check missing feedback, pressure, bounds, no-gain rollback, original prompts/results,
successors, Extract barriers and unknown ownership. Positive CPU integration uses
an explicit delivered-work test configuration, a simulated engine-stat source and
a logical busy-Core sampling frontier with an injected gap. A separate real-clock case
checks original transport/results/cleanup without requiring a finite fixture to
last long enough for exploration. Default computed policy is replay-tested.
These are engineering checks, not GPU
resource benefit or full real-engine feedback qualification. Cold startup,
observation and trial work stay in the caller's original query clock.

## Optional method-ready successor selection

The generic `MethodDriver` still defaults to `round_robin`; `LotusChainFIFO` and
the full-query record-local owner still default to `fifo`. Explicit selection uses
the unchanged historical `SuccessorFirst` settings:

```python
from src.semantic_methods.driver import SuccessorFirst

owner = SemLoomRecordChainCore(engine, resources, execution="record_local_sections",
    ready_order="successor_first", successor_first=SuccessorFirst(max_burst=2, max_wait_s=1.0))
```

The two values above are explicit example settings, not defaults or selected optima.
The policy chooses among already legal method-ready calls before `Core.offer`.
An aged oldest call wins first; otherwise successors precede fresh sources until the
declared accepted-successor burst gives a waiting source its turn. Backpressure does
not consume a turn or reset age. This does not reorder accepted Core work, preempt
native inference, speculate on future methods, or change source preparation order.

The full-query options are limited to complete Filter/Map plans with one model deployment
and one model role. Extract and role changes reject custom ready options before source
consumption or model startup; the default query behavior keeps its existing support.
These options can be used together with `token_budget=budget` on that same supported plan.
Ready selection controls calls before `Core.offer`; it does not reorder calls already queued
inside Core. Token W limits active native work, so an accepted offer alone does not prove
native startup. Both limits and their existing settlement rules remain in force.
The old wrapper's `clock` maps to the owner's `ready_clock`; both pass to the same driver,
and `on_ready_event` passes through unchanged. These clocks describe local method ready
and Core offer times, never native model acceptance or GPU execution.

Policy clock failures use the original method failure/cleanup path. Optional event clock
or callback failures disable that observation without changing the policy's ready age.
Default execution without priority or observation performs no new ready clock reads.
Fair service requires continuing admission opportunities, finite live rows/stages, a
working monotonic clock and progressing engine/consumer; there is no wall-clock waiting guarantee.

[Ready-order cases](../../../tests/scheduling/test_method_ready_order.py),
[clock cases](../../../tests/scheduling/test_method_ready_clock.py) and
[whole-query cases](../../../tests/execution_provider/test_lotus_record_local_priority.py)
cover the restored implementation; [joint token/priority cases](../../../tests/execution_provider/test_lotus_record_local_token_priority.py)
cover default, separate and combined settings, lifecycle and bounded ready service with token estimates.
The [two historical negative observations](../../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#priority-reverse-small-pair)
remain unchanged. The priority path above has no cross-query resident reuse, so those
historical first/reuse runs are not reproduced merely by exposing this option.


<a id="records-model-context"></a>
## 异步模型上下文

SemLoom异步路径的`EngineSpec.max_model_len`省略或null时，`AsyncEngineArgs`省略该关键字，
由固定版本vLLM解析。原模型ready回执新增`semloom.model_context.v1`，包含请求值、实际正整数长度、
实际引擎身份、配置摘要与观测内容摘要；私有通信在接纳请求前核对，不从缺测推导无限长度。
原生渲染后的完整提示加输出上限仍须适合实际长度，超限不截断或拆分请求。
应用token组织、可选在途工作量与已启动模型的接纳缓存使用该回执，外层额度与原未知结果记录不变。

显式正整数精确保留；当前同步 SemLoom 的省略行为见[同步参数所有权](#sync-native-context-ownership)。
原生 Ray／Daft／IMLane 同步对照也保留省略／null，见[同步参数所有权](#sync-native-context-ownership)；旧 Movie HTTP 包装和入口已退役，
历史原配置从[恢复记录](../../../../experiments/reproduction/native_baselines/README.md#movie-http-retirement)读取。
显存比例、数据精度、方法／生成参数及单 GPU 支持保持。
普通冷查询的上下文就绪成本计入原查询报告，空查询不为缓存启动模型；常驻查询沿用原ready回执。
公共离线calibration在省略上下文时复用首个已声明完整样本的实际owner回执，随后构造身份；
每个执行owner仍须在生成前核对自身ready及解析长度，逐调用记录须对应其引擎身份。
首个样本在缓存未命中时只使用一次；缓存命中时仍需执行这次确认，调用与耗时分别进入
`scan_units`和`model_context_confirmation`，不伪装零成本缓存命中。显式上下文保留原零扫描命中。
模型实际配置的观测摘要仍绑定完整请求；缓存的上下文部分排除实例标识及查询调用额度，其他模型参数保持。
模型前的输入专用token验证仍需显式上下文；离线接线的[CPU证据与剩余项](../../../../experiments/results/diagnostics/records_config_refactor_20261103/README.md#offline-model-context)
不包含真实模型资格。
[CPU范围与剩余项](../../../../experiments/results/diagnostics/records_config_refactor_20261103/README.md#异步模型上下文的-cpu-候选)
不包含真实模型或性能资格。

普通Core与`PreparedRecordsCore`均提供只读`model_ready`，读取不会分配模型。
token wrapper在独立查询的`start()`仍只返回延迟分配声明；实际回执在原`evaluate()`返回、
迭代器开始提交前核对，作为启动声明中的`query_model_ready`保存，不把启动阶段改报为模型加载。
空查询须有完整零调用证据才可没有模型回执，不能借此接受未知或缺失回执。
有限 IMLane 的链 host/backend 及 cascade host/worker 不再将省略／null 上下文改成 4096，
显式值保持，实际长度由模型 owner 确认。原转换的独审与历史修订见
[兼容记录](../../../../experiments/results/diagnostics/records_config_refactor_20261103/README.md#owner-context-compatibility)。



## records Core 可选动态接纳

`create_records_core` 的固定默认与 sync/async 入口保持。显式
`profile={"mode":"async","admission":{"kind":"resource_feedback"}}`
启用[当前请求反馈](#optional-request-resource-feedback-local-cpu-candidate)，每个查询从一个请求开始。
owner 的请求总额、准备参数及 actor／IPC 容量在启动前解析；反馈只调整该范围内的后续接纳。
已退役请求策略的原参数、提示与缓存说明从[准确历史版本](#retired-request-feedback)恢复。

应用层未完成请求与引擎每轮处理的序列分别计限：`EngineSpec.max_num_seqs` 或
`max_num_batched_tokens` 未指定／null时，异步 Core 不传对应引擎参数，交由安装的
vLLM 0.25.1 解析；显式正整数原样传入。当前同步 `VllmEngine` 和原生图的
`ReferenceVllmEngine` 也省略这两项。旧 Movie HTTP 入口已退出维护版。
启动回执的 `native_capacity` 从构造后的配置读回，即使未开启反馈；缺失字段保持未知。
未指定不表示无限容量或自动性能调优，也不改变外层 C/B/W、上下文长度或显存比例。
该引擎序列配置不再截断 records Core 的请求额度。外层额度可以更大，仍受
原 Core、actor、IPC 的请求／结果槽位与字节预留约束；这不表示这些请求同时在 GPU 上运行。
未采用独立预算的兼容路径中，`async_limits.max_inflight` 必须等于 R，`result_slots` 至少为 R，不能只按初始 C 配置。
[显式独立预算](../../experiments/movie_matrix.README.md#显式独立资源预算默认关闭本地cpu候选)可为省略的 IPC 字段提供有限缺省值：
请求／命令槽来自 `active_requests`，结果槽来自 `held_tasks`，总字节来自对应预算，控制预留沿原协议公式。
显式 IPC 值保持，可小于 R；两层仍分别记账，实际接纳继续受槽位及字节背压。

异步 Core 的 RPC 线程预算与上述驻留槽位分开。`RecordsResources.model_rpc_workers`
是可选正整数；省略时沿用调用方声明的 `cpu_workers` 作为数据 RPC 并发预算，不读取
输入长度、R 或引擎序列容量，也不声称探测了宿主可用 CPU。Ray 模型 actor 的线程并发为
该数据预算加两个控制进展位置：一个供关闭／drain 等待，另一个供 snapshot、abort、ACK。
Ray CPU/GPU 预留及 vLLM 参数不因此改变；部署方可按其 CPU 预算显式覆盖 RPC worker 数。

工厂路径的数据 RPC 只交接一次已确认接受的请求；容量或 IPC 暂拒会立即返回
`backpressure`，等待留在现有 transport 事件循环。原 `_held`、ticket、逐成员轮询和
ACK 继续拥有请求与结果，无第二套队列或责任计数。取消不释放尚未终结的数据 RPC 许可，
控制 RPC 不占数据许可；结果未确认仍保持 unknown 责任及原关闭回执。
许可在已发行 RPC 的非取消终态 Future 回调中归还，调用方 Task 反复取消或事件循环关闭
不能提前退款。本地 Future 被取消或观察注册失败时，保留最多 RPC worker 数的临时引用、
停止新数据发行并唤醒未发行等待者；关闭／drain 在原截止时间内凭 Ray 对象就绪核实。
未获终态证据则 RPC 清理报告仍为 unknown，不把本地取消当远端完成或模型结果。
`AsyncRecordsActorOwner.submit_core/submit_prepared_core/submit_prepared_batch`
现在返回须在 transport loop 等待的 awaitable，不能再交给 `ray.get`；内部 transport
已同步迁移。低层异步 actor 的旧 `run_core/run_prepared_core/submit_prepared_batch` 阻塞接口
及专用等待循环已删除，现用 `try_submit_*` 接纳、poll交接、ACK确认沿原职责执行。
owner的同名批提交方法仍返回awaitable，同步actor的`run_core`继续提供实际同步执行，两者不属于删除对象。
直接actor用例已迁到现用接口，背压不占新请求；原未知结果与未结束RPC不提前归还。
[本批27项纯内存与实际IPC结果](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#retired-blocking-async-rpcs)
分别记录源码和覆盖范围，不提供额外模型或性能资格。
`model_submit` 继续计实际 `.remote()` 交接墙钟，背压重试计入该阶段；模型与依赖等待仍单列。
执行元数据新增解析后的 RPC worker 数、来源、控制预留及交接协议，不能将其当硬件最优值。
[线程与责任回归](../../../tests/execution_provider/test_lotus_rpc_capacity.py)仅为 CPU 资格；
真实 Ray 的线程池、控制进展及 2,000 条模型提交仍待独立实机验收。

`allow_exploration=True` 允许当前控制器进行可逆性能试探；省略 admission 时保持固定路径，
不启动该反馈采样。可选时间、变更次数与尾部工作预算由调用者显式声明，不作为硬件性能结论。
`max_inflight="auto"` 只在 async 模式下选用声明的 Core 请求额度，不测量或扩大实际资源。
同步 Core 继续按完整请求执行；有限同步批参照由 `lotus_records_static` 单独提供。
请求反馈调整单请求窗口 C；每批 token 工作预算 B 由组批选项声明。非 rolling 的
`batch_feedback` 调整应用批窗口，联合调整 B 尚未实现。当前配置及其采样时钟见
[运行时说明](../../scheduling/runtime/calibration.README.md#resource-request-public-current)。

### 静态参照的显式 session

维护 Movie session 现在可选择 `backend="semloom-static-reference"`、
`profile={"mode":"sync"}`，继续使用原 `RecordsResources` 的 prepare/model batch。
这是 Core 外有限批参照，不是 Core B4；Core 的原单行合同不变。
resident 在查询前调用原模型 owner 的 start，两次完整查询复用模型，query EOF 不关闭模型，
session 结束一次原 close。不同 FilterMethod 指令明确拒绝，不能复用旧 formatter 冒称支持。
准备批、原有限模型批、原 LOTUS parser、身份和未确认关闭责任不变。
本接线已做维护 CLI/CPU first-reuse 与失败边界检查，真实模型/物理退出仍需本候选单独资格。


The optional [native source clock](records_native_source.py) validates the actual
vLLM ready owner and model-actor source age before converting a native sample
to driver time. Repeated source identities retain their first mapped timestamp.
Missing or conflicting source data cannot qualify request growth. The existing
public feedback selection is unchanged in this increment; no physical request
capacity is inferred. CPU scope is recorded in the
[source check summary](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/native_source_checks.json).


The public default-capacity resolver and Movie preflight/session/calibration share
one `RecordsRuntimeConfig`. Core and Method count guards sit above the byte-based
whole-item allowance; explicit result slots remain an independent IPC declaration. Omitted
slots and totals fit the final declared IPC request count and item sizes. A smaller
explicit IPC frame/total is preserved and does not shrink a different Core item's
required reservation. Ordinary calibration records `records_capacity_policy_v2`.
The retired request profiles and admission cache retain their earlier allocation
and cache rules only in the [fixed-source recovery records](../../../../experiments/reproduction/native_baselines/README.md#retired-request-feedback).
The ordinary measured-configuration calibration cache remains supported.
[Default-capacity evidence](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/default_capacity_checks.json)
records 388 selected CPU methods, 387 successes and one upstream-checkout skip.
Direct low-level constructors, nonrolling batch defaults and sync/async model-spec
separation remain separate work. No automatic hardware grant or model performance
qualification is claimed.


### Unspecified legacy request count with complete grants

`RecordsResources(max_active_batches=None)` preserves an unspecified legacy R.
The ordinary async and rolling factories can resolve it from an existing complete
`RecordsCapacityBudget`; Core, Method residency and IPC retain their own finite
allocations. The caller's declaration stays None in `declared_legacy_request_budget`,
while the applied owner count comes from `budget.active_requests`. Missing grants
raise before actor allocation, including profiles requesting `max_inflight="auto"`.

This is explicit-budget support, with CPU checks for config, CLI, session, query
reuse and offline identity. Ordinary R still defaults to 2. Omitted IPC result slots
now follow the applied IPC request declaration; omitted totals fit their complete
items. Automatic physical resource allocation remains unfinished; None does not
mean unlimited memory or measured GPU capacity.
See [the check summary](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/unspecified_request_checks.json).


### Native safety while model work is unknown

The accepted native frame keeps its own freshness when a retained failed or
unknown receipt has no usable token work. Performance comparison can pause
without suppressing source-qualified pressure. Sequence/time checks still reject
old or repeated pressure increments. Core unknown ownership, receipt retention,
consumer delivery, ACK and release are unchanged. The reviewed two-file repair
is composed with the complete-budget None support; [combined CPU scope](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/capacity_pressure_composition_checks.json)
does not qualify a real model or automatic hardware capacity.

<a id="byte-derived-records-capacity"></a>
### Counts derived from assigned records storage

`CoreRecordsCapacity.with_storage_bytes(input_bytes=..., result_bytes=..., method_bytes=...)`
returns a new complete budget while preserving the per-item declarations and the assigned bytes.
It requires all three positive integer amounts and rejects an existing explicit budget or storage
that cannot hold one declared item and the ordinary continuation envelope. It never acquires RAM
or establishes an allocation's provenance, exclusivity or ownership across service instances.

IPC reserves one full `item_result_bytes` before each model request. The derived request count
uses the complete reservations that fit `result_bytes`; Core and IPC result count guards sit one
above that number. Input backpressure still uses the actual encoded bytes, independently.
Method count uses the minimum `row_reservation` allowed by positive `MethodLimits`, with its guard
one above that byte allowance, so actual continuation sizes hit their byte grant first.
The existing Core, Method pool, actor and IPC keep their own release and unknown handling.

The returned budget works with `RecordsResources(max_active_batches=None)` and the existing
async factory, including `max_inflight="auto"` or modern admission starting at one. Explicit
six-field budgets and IPC objects keep their meaning. No new controller, CLI option or native
vLLM setting is introduced. SDK and ordinary omitted configuration now use R=None and must
resolve a supported finite source; this conversion does not acquire physical memory.
Caller-owned relations, retained audit data, Python/Ray objects and model memory are outside
these logical byte amounts. Actual hardware capacity and full query memory qualification remain pending.

The retired request adapter's source-age rules are available in the
[exact historical notes](#retired-request-feedback). Current request feedback
uses its bound native and independent safety sources as described above.

### 同步模型的上下文回执

同步 `VllmEngine.ready()` 现从同一实例的 `LLM.model_config.max_model_len` 构造
`model_context`，沿用已有引擎／配置身份与摘要格式。缺失、非正整数、与显式值不符、
输出上限不容纳或同一实例内变化时拒绝 ready；不会从旧缺省补出观察值。
无效 ready 后已分配的实例仍由原 owner 关闭，私有管道继续传递原错误结果。
关闭未确认时不再发布 ready；重试确认后仍可查询已关闭实例的上下文。
接口依据为[vLLM 0.25.1 原生实现](https://raw.githubusercontent.com/vllm-project/vllm/v0.25.1/vllm/entrypoints/llm.py)。

本节记录早期只补同步回执的增量：当时原 `LLM` 构造参数、`chat`、结果转换和关闭实现保持，
4096／8／4096 及链传播尚待迁移；后续当前实现见[同步参数所有权](#sync-native-context-ownership)。CPU native-API stub
刻意允许原生报告与配置缺省不同，以检查来源；这不表示真实 vLLM 会忽略显式参数。
[CPU 检查](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/sync_context_checks.json)
包含原生 API stub、实际本机私有管道、失败 ready 与子进程回收，不证明真实模型资格。


### 当前模块中的准备与派发交错候选

`create_records_core` 可显式传 `interleaved_preparation=True` 与
`event_driven_preparation=True`，支持预检完整有限 source 的 rolling 请求，
可使用固定许可或当前 `resource_feedback`；B 可以省略或显式设置。
默认均为 False；Movie JSON／CLI 通过 `preparation_mode` 接入。实现沿用 `b258c575` 的已审机制，
查询循环位于 `records_query_runtime`，继续使用原 MethodDriver、Core、RPC 接纳及结果消费。
不改变 LOTUS 请求、Filter→Map 原数据、Extract 的完整关系条件、单记录补位或原生引擎参数。

当前组合将准备行数判断改为 MethodBudgetPool 的实际行数，字节不足时保留原退让；
Core 的 held_tasks 不再被用作是否订阅 RPC 通知的请求容量判断。RPC 快照只描述已有许可状态，
ACK 不归还 compute／结果／unknown。原帧头及逐候选计量缓存继续使用，附带原生上下文校验。
关闭、拒绝、重绑和不确定回执仍由各自原管理对象处理。

当前资源反馈在结果及后继推进之后更新；窗口变化会重新检查已准备前缀，
等待也使用反馈已有的下一次轮询时间。准备、Core 存储、请求与 RPC 许可分别保留原持有者。
扩缩窗及取消的 [CPU 组合记录](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#prepared-feedback-interleave)
不表示真实模型吞吐或完整查询已经加速。

[组合记录](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/prepared_supply_composition_checks.json)
包含显式独立预算、Method 行／字节先于请求额度用完、held_tasks=1、None 加完整预算的公共工厂
检查。继承测试不全部算作新方法；默认关闭的回归另核。旧候选一次单许可 CPU 查询明显变慢，
原结果保留，局部较早派发不证明完整查询变快。没有真实模型、Ray 或 GPU 资格。


### 已退役请求反馈的原始说明

旧适配器的等待中快照与采样修订按[固定历史文档](#retired-request-feedback)追溯。
原失败、原件与具名来源不改写；现用采样继续由 `RecordsNativeSampler` 持有同一个 RPC，
未知结果保留原责任，不因教程退役而减少收尾检查。

### Movie 配置到已有准备选项的接线

Movie 入口新增默认关闭的 preparation_mode，serial 与省略相同；interleaved／event_driven
映射到上面的 SDK 选项。纯组批解析与 PreparedRecordsCore 使用同一准备选项检查；配置、预检、
工厂、实际回执及校准身份相互对应。rolling 路径可组合固定许可或当前 `resource_feedback`，无需设置 B。
详细取值与后期提示错误可能保留合法前缀用量的说明见
[Movie 配置](../../experiments/movie_matrix.README.md#record-preparation-mode)；
[CPU 接线检查](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/preparation_cli_checks.json)
不扩大到真实模型、任意链、自动容量或性能结论。


### 同步逐调用的实际长度检查

共享同步内核现使 ready 和 run_prepared 使用同一模型上下文核对。即使调用者没有先读 ready，
也须在 chat 与调用数预留之前确认实际长度；同一实例的长度变化、未知值或显式配置不匹配均拒绝。
实际长度替换内核运行中的旧长度缓存，关闭未确认／已关闭／忙碌的拒绝行为继续保留。
[逐调用 CPU 检查](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/sync_generation_checks.json)
包含省略／显式配置、大小上下文、不同阶段输出及调用额度。本节对应只补逐调用检查的早期增量；
同步构造、fused 与完整提示的后续迁移分别由[同步参数所有权](#sync-native-context-ownership)说明。

<a id="sync-native-context-ownership"></a>
### 同步参数所有权与完整提示检查

同步 `VllmEngine`、原生 baseline 的 `ReferenceVllmEngine` 及异步引擎只传显式指定的五项可选参数：
max_model_len、max_num_seqs、max_num_batched_tokens、dtype 和 gpu_memory_utilization；省略／null 时交给实际 vLLM。
两条构造路径使用共享 EngineSpec 的可选参数提取，没有同步层的 BF16／0.8 补值。
ready 记录请求参数、实际上下文及 native_capacity；精度与显存比例从实际 model/cache config 读取，缺测保留 None，
不当作空闲内存或可用请求数。显式参数、生成语义及现有单卡支持范围保持。

同步调用使用同一 LLM 的原生聊天渲染，核对每条完整提示 tokens 加 max_tokens 后再加入原生队列，
不重新套模板、不截断原文。渲染或总长预检失败是明确未提交，调用数不增加；进入原生队列后异常仍为
unknown，预留不退。回执 token IDs 与预检输入不同则失败并保留实际用量。原生 API 来自固定版本的
[离线方法](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/entrypoints/offline_utils.py)和
[LLM](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/entrypoints/llm.py)；未修改 vLLM 源码。

fused worker 不再把省略上下文改成 4096；首次模型调用前核实同一 spec／engine_uid 的成功 ready，
各阶段与关系复用继续检查实际值。组批回执记录实际上下文摘要和模型身份，缓存不跨模型复用；
未知或错误 ready 拒绝提交，模型分配和清理的不确定性仍单独保留。普通与 static session 的当前 SemLoom
预检也仅对显式上下文提前检查，实际值仍需 ready 后核实，CPU 计划检查不是模型就绪证明。

原生 Ray／Daft／IMLane 对照由 baselines 的 ReferenceVllmEngine／ReferenceFusedChainBatch
选择 `LLM.chat`；共用 IPC、方法、结果与清理。省略容量不再由独立入口补 4096／8／4096，
链、cascade、session 与 IMLane 仅对显式上下文做模型启动前检查。LOTUS 客户端批大小及原生图批大小独立保留。
旧导入和历史具名原件保持。
[CPU 检查与原始缺项](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/sync_default_checks.json)
记录早期 SemLoom 迁移、当时仍补值的原生对照和来源核对；当前 baseline 省略行为由
[构造与 ready 回归](../../../tests/serving/test_sync_default_ownership.py)、
[进程入口回归](../../../tests/serving/test_sync_reference_routes.py)及
[IMLane 配置回归](../../../tests/experiments/test_context_owner_compatibility.py)覆盖。
真实模型／GPU、自动容量、geometric 及计算中关闭仍待验证。


<a id="service-payload-grants"></a>
### Exclusive payload grants inside one service process

`RecordsPayloadPool(total_bytes)` accepts a finite amount assigned by service configuration.
The ordinary async `create_records_core(..., service_budget=pool, capacity=shape)` now allocates
one disjoint grant before constructing its model owner. Resources may be omitted on this path;
explicit resources must use single-row preparation/model calls and `max_active_batches=None`.
No R or result-slot value is supplied by the caller. The existing byte conversion and IPC
reservations derive finite counts; modern request feedback still begins at one.

The immutable plan separately charges Core input/results/metadata, a pending source item,
one ordinary continuation envelope, both IPC endpoints' input/result/control allowances,
actor and transport prepared/result allowances, and retained closing results. Its largest
complete plan must fit the available pool bytes. Existing IPC frame sizes and control reserves
are protocol declarations, not new request-window policy. `preview(shape)` acquires nothing.
`service_allocation_bytes` can limit one instance's share when the service creates several owners;
allocating one grant never grants the same bytes to another owner. Pools and grant capabilities
cannot be copied, serialized or used from a forked process. This is a static engineering choice,
not a second request controller or a replacement for Core/Method/IPC task accounting.

```python
from src.execution_provider.adapters.records_service_budget import RecordsPayloadPool
from src.execution_provider.adapters.lotus_records_core import create_records_core

pool = RecordsPayloadPool(service_config.payload_bytes)
owner = create_records_core(method, engine, generation, service_budget=pool, capacity=shape)
```

The service must keep the actual owner and close it. Its ready receipt binds the grant to both
the actual engine and producer IDs; reuse keeps those IDs and the grant. Unconfirmed RPC,
producer cleanup or Core storage retains all grant bytes. Model API shutdown does not prove
GPU exit. An empty query supplies no invented model identity. `pool.close()` stops allocation
and reports pending until its existing owners return their grants; it does not force reclaim.

Closing a query may retain actual call receipts. `owner.take_unconsumed_results()` transfers
those receipts once after the query closes, without parsing, retrying or changing unknown
outcomes. A service owner cannot replace that query before transfer. The grant returns only
after owner close is confirmed, Core/transport/RPC are clear and retained receipts have been
transferred. Previously returned report snapshots remain intact. A cancelled query can also
transfer its receipts before reusing the same live owner; the grant then remains assigned.

The pool is exclusive only among owners sharing that pool in that process. Its bytes are
declared logical payload allowances, not acquired RAM, observed host/cgroup free memory,
Python/Ray RSS, preparation/serialization scratch, full relations/audits or GPU/KV storage.
The service must supply the total; no automatic hardware discovery is implemented. Ordinary
Movie CLI/session entries can now declare `service_payload_bytes`; see
[entry settings](../../experiments/movie_matrix.README.md#record-service-payload).
Rolling Movie CLI/session persistence-copy grants, cross-process allocation and restart
recovery remain pending. Finite ordinary Filter rolling SDK grants are described below.
This opt-in path rejects historical profiles and IPC overrides. Without a pool, ordinary R
still defaults to 2; omitted IPC slots and totals now fit the declared IPC request capacity.
See [CPU scope and evidence](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#service-payload-grants).


### Service grants also retain the original observation RPC

The ordinary modern request-feedback owner now uses `RecordsNativeSampler.snapshot_rpc_report()`
at close. The method collects only after the existing sampler has stopped and reads the original
request, retained ref/Future and terminal receipts under its lock. It reuses the sampler's existing
terminal proof; it dispatches no replacement snapshot and does not treat client cancellation as
a remote return. A stopped observation thread alone cannot prove complete cleanup.

Owner close reports `feedback_close` separately from the earlier per-query admission report.
Until `snapshot_rpc_terminal_evidence_complete` is true, close remains unknown and the service
grant remains allocated. Grant settlement checks the current sampler again. A known return or
RPC exception can complete this proof without changing model outcomes. Core/Method/IPC and
unconsumed-result responsibilities still apply independently. The legacy feedback implementation,
source-duration policy and native model settings are unchanged.
See [CPU evidence](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#service-snapshot-retention).

### Original-owner cleanup and complete delivery declarations

`pool.retry_cleanup(allocation_id, timeout=...)` only retries close on its registered original
owner in the same process. When remote cleanup is still needed, the connected module, job and node must
match the identity verified before startup or at the original ready receipt. Missing identity, shutdown,
replacement or a reinitialized runtime is rejected without importing or initializing Ray.
An unbound grant still uses its original `cancel()`. A query with only local results to accept
does not need another remote cleanup attempt: `pool.take_unconsumed_results(allocation_id)`
transfers the original owner's closed-query receipts, even after Ray shutdown. It does not
persist them for the caller. Retired history, foreign grants and copied handles are not recovery.

The normal Movie driver saves complete retained receipts and query accounting into a private
content-named file, synchronizes the file and its directory, then explicitly accepts that exact
receipt snapshot. Failed storage, failed acceptance, unknown producer/RPC cleanup or a changed
snapshot retains the grant. A complete earlier artifact can be reused; partial failed writes
remain as evidence. The returned report links the pool-status artifact. It does not retroactively
rewrite the earlier session timeline. Driver shutdown also checks its original Ray identity.

Core records its first observable connection before actor startup, verifies it again after
startup and before remote cleanup, and never replaces an unavailable first observation with
a later connection. Entries bind the verified connection immediately after their own Ray
initialization; a replacement before the first model start is rejected too. Each ordinary or prepared
records owner stores its observation in `ModelRuntimeBinding` from `records_runtime_identity.py`;
service readiness reads that same object. Constructing it does not inspect Ray or allocate model resources.
Startup cleanup errors are notes on the original exception. Old fixtures without an observable
identity retain normal operation but provide no reconnect proof. A never-started owner still
can close locally. These checks do not establish actual Ray recovery or physical GPU exit.

`cleanup_confirmed()` reads the actual actor identity and API/kill-request proof, empty data RPC,
stopped transport, zero Core usage and complete modern snapshot proof. A cached `closed` status
alone is insufficient; a wrong producer report must remain repairable on the original runtime.
Direct repeated `close()` and a registered pool cleanup can reuse that complete original proof
after Ray stops, while local unconsumed receipts keep their allocation. This path performs no
remote action, result acceptance or grant return. Wrong-thread calls still fail, and cleanup
timeouts are checked before actions. A legacy sampler without terminal proof cannot use this path.

An entry keeps its original Ray connection while owner cleanup is unconfirmed, including a
pending observation RPC after model API close. It reports `retained` and an unknown teardown,
then can retry the same owner after the original RPC returns. Merely local result persistence
does not require keeping Ray alive. The session driver registry is retired only after teardown
succeeds, even when the byte grant returned earlier. A shutdown error therefore preserves the
same driver for another close attempt. The ordinary worker exposes its original runtime tuple
and `error.records_finish_runtime()` on the live exception; call it after original-owner cleanup
has completed. Its observer reports `runtime_retained` or `runtime_close_unconfirmed` instead
of `runtime_closed` when appropriate. These Python capabilities work only in that still-live
process; a private CLI worker's exit does not provide a resumable service or socket reconnect.

`records_service_delivery.py` provides a separate pure declaration for one ordinary prepared
Filter's complete receipt array, using every actual query/row identity and the existing codec.
It checks projected fields and actual encoded bytes without mutating results. A consecutive
session uses the largest actual array declaration, rather than combining one query's row count
with another query's identity. Each independent complete-array copy needs its own allowance.
The rolling SDK path described below consumes this declaration; the Movie CLI still accepts
service grants only for its ordinary nonrolling path. Neither path claims to fund Python RSS,
caller-owned relation/audit copies or an undeclared arbitrary prepared query.
Socketpair and original Future/ref handles are local capabilities; no cross-process reconnect
or restart-safe allocator is implemented. See
[checks and remaining work](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#service-entry-recovery).

The prepared ordinary wrapper also avoids copying an entire `calls` field which it immediately
replaces with its ordinary projection. Permanent close clears its extra iteration references.
After healthy EOF it may iterate the original close-call snapshot only if the actual stream,
successful report, zero Core use, transport refs, data RPC and audit handoff all agree; otherwise
it reads live calls as before. Previous external snapshots and late unknown receipts remain.
The pure complete-array check sums already-validated item encodings plus the same codec's array
punctuation, avoiding another whole-array encode. These changes perform no new dispatch, ACK
or grant return. They do not eliminate all full-result copies; rolling allocation is a
separate service-only operation below.


<a id="rolling-service-grants"></a>
## Rolling ordinary Filter service payload grants

`create_records_core` accepts `service_budget=RecordsPayloadPool(total_bytes)` and
`service_delivery=declare_ordinary_delivery(query_id, source.expected(query_id),
item_result_bytes=shape.item_result_bytes)` with the existing async profile,
`PreparedBatching`, `rolling_refill=True` and a per-item `CoreRecordsCapacity` shape.
Omit `resources` to use single-row preparation/model requests and no legacy R declaration.
A resident owner can instead receive `declare_ordinary_session` for every actual query.
The declaration covers one ordinary Filter, finite distinct occurrences and their exact
query identities; an undeclared source/query fails before actor allocation. A pure
`preview_service_records_runtime` validates the same shape without allocating an owner.
Dictionary inputs and direct immutable-tuple construction use the declaration's same identity
validation and ordering once. Public signatures, identity fingerprints and byte totals remain;
[CPU checks](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#ownership-simplify)
also cover inherited cleanup deadlines and ordinary service result ownership.

The service supplies a finite logical byte total. The pool derives the instance request
slots, Method storage and both IPC endpoints together, then charges the complete query
result arrays separately. Eleven named array roles cover raw and parsed receipts,
Method adoption, failure raw results and error calls, chain/stream close snapshots,
ordinary projection, merged-getter work, consumer snapshot and transfer work. Each uses
the largest actual query array in a consecutive session. Codec text/bytes have their own
single-item allowance. These payload envelopes exclude the existing complete source/expected
relations, Python/Ray overhead, caller audit copies and model/KV memory. The finite prepared
source still materializes its complete relation; this is not whole-query RAM or a GPU grant.
There is no guessed request-count ceiling and no hardware discovery. Without a service
pool the existing R/result defaults still apply. Model-generation and native baseline
settings are unchanged.

The wrapper binds its actual unstarted chain to the grant, observes the actual model and
producer ready identities, and checks the original Ray module/job/node before model actions.
Consecutive queries retain one resident grant; query completion alone does not return it.
Unknown data/observation RPCs and unconsumed results keep all granted bytes. An explicit
`take_unconsumed_results(expected=...)` preserves JSON scalar types before accepting a
snapshot, including boolean versus integer values. When a rolling service caller omits
`expected` and receives nonempty results, `take_unconsumed_results()` only transfers those
local rows; it performs no fallible retirement and retains the grant. The caller next invokes
`owner.close()` or registered `pool.retry_cleanup(allocation_id)` after it has the returned
rows. A cleanup error then cannot hide an unreturned result. A local transfer interrupted
before returning restores its prior delivered/report state. An explicit expected snapshot
already belongs to the caller, so its existing acceptance/retirement path remains available.
An empty retry can finish outstanding cleanup because it transfers no unique payload.

Final service close first creates an independent caller report. Complete actual cleanup
and result transfer then allow retirement of the private chain/iterator payload references;
the wrapper retains only scalar cleanup metadata, `calls_count` and `payload_transferred`.
The transport, prepared organizer and actor owner each release their own retained fields;
the chain owner keeps the transfer order and proof checks. The actor owner also reads its
actual retained close reports, so cleanup does not rely on an extra completion marker.
Later reports therefore do not retain full `calls` arrays. Previously returned reports and
rows keep their values. Private cyclic references must release their arrays explicitly,
without waiting for Python garbage collection, before the pool can return bytes. A same-process
interruption after the original retirement proof can continue only with that original chain
and unmodified certificate; a replacement owner cannot inherit its allocation. Wrapper
iteration and delivery references are removed before its producer reference, so interruption
at the final producer store leaves a locally finishable state.

This SDK path does not wire rolling result persistence through the Movie CLI/session.
Their current whole-report persistence copies need separate complete declarations first.
A cgroup limit/headroom observation is not exclusive physical RAM, and the current anonymous
socketpairs have no reconnect listener or durable owner/result journal. Original-owner restart,
client reconnect and automatic physical capacity remain pending. Actor acknowledgement can
precede durable service storage, so the existing fsync-before-result-acceptance operation is
not an owner-death exactly-once guarantee. See the
[CPU evidence and remaining work](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#rolling-service-delivery).


Internal rolling producer retirement now carries only the original object marker.
The grant already owns allocation and model identity; duplicated mutable metadata and mirror checks were removed.
Actual producer cleanup, unconsumed results and pending RPC still retain the grant; see the
[focused CPU checks](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#retirement-marker-simplify).


<a id="prepared-requests-without-token-packing"></a>
### Rolling prepared requests without token packing

Set `rolling_refill=True` and omit `prepared_batching` or pass `None` to
`create_records_core`. The existing prepared-request pipeline supports fixed request
permits or `resource_feedback`, with either explicit preparation mode. Its receipts
show `B=None` and `policy=None`; no positive B or bypass flag is manufactured.
The older `async_token_batches` report key remains, with a null policy.

The same Core, MethodDriver, transport and model owner remain responsible for work.
Complete requests are grouped by available request permits, compatibility and RPC/IPC
bytes. Token counts still verify prompt plus output against the model's confirmed
context; they do not seal packets or become native iteration parameters. Explicit
engine parameters retain their own meaning and are not derived from B, C or R.

The finite source is still read into a complete relation before this pipeline starts,
and ordinary ledger delivery follows traversal. Preparation interleaving concerns
method/prompt preparation and dispatch. It does not claim CSV-read overlap or lower
complete-query latency. The service SDK additionally requires its complete delivery
declaration; rolling service Movie CLI persistence remains pending.
[Implementation and retained CPU failures](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#prepared-requests-no-token-packing)
separate this local candidate from actual model and performance results.


<a id="ordinary-ipc-omissions"></a>
### Ordinary IPC omissions follow the actual declaration

Both factories and direct IPC objects derive omitted totals from declared request/result
slots and complete item sizes. `AsyncEngineLimits(max_inflight=...)` requires that count;
both engine constructors require its explicit limits object. There is no fallback of eight
requests or fixed 16-MiB totals. Null slots and totals mean omitted values; partial JSON
inherits the existing Core budget before defaults are resolved. Explicit counts and smaller
byte totals remain independent and are never enlarged to match C.

The SDK-wide R default is None. Ordinary async and supported rolling paths can use a full
Core budget, typed IPC, an explicit request count in partial JSON, or service payload grants.
Other direct paths require their supported finite declaration before startup. Per-item limits,
control quotas, confirmed delivery, ACK and unknown responsibility remain. These are logical
allocations; physical RAM, complete query relations and RSS are still outside this account.
[Current CPU scope and original failures](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#sdk-ipc-capacity-followup)
cover omission, explicit smaller reservations, startup rejection, native construction and reuse.


### Query runtime data ownership

Ordinary Filter input, receipt validation, delivery and query-side cleanup live in
[records_filter_runtime.py](records_filter_runtime.py). The model owner keeps configuration, model reuse and resource
management, and imports the actual iterator class it constructs. [Matching before/after CPU cases and source totals](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#ordinary-filter-runtime)
record the class move and retained historical source paths.

`records_scan.prepare_native_records_scan` now owns the common Daft Native CSV schema, predicate and projection setup
for Core and the static reference. Both owners retain their original method checks, model startup, iterator and batching.
The helper returns the existing frame and caller-owned Ray module; it introduces no runtime owner or configuration knob.
[Matching CPU scope and missing CLI checkout](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#shared-record-scan)
keep the unexecuted cases explicit.

`records_query_runtime` releases and observes its own query payloads after the owner validates service transfer and cleanup.
`lotus_chain_fused_runtime` ends the consumed input/session and opens the next relation input; the query owner selects the
next phase and assembles its result accountant. No extra owner, copy of the data ledger or facade is introduced.
[CPU scope, interruption recovery and pending OS checks](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#runtime-owned-query-state)
retain the original failures. Private old pickle locations are no longer forced through fake module metadata.

The record-local source table now releases each expected record reference after the corresponding input passes the original
lazy comparison. Keys and offered IDs still verify duplicates and complete input; unread rows are not serialized at startup.
`RecordOccurrence.from_dict` delegates copying mutable fields to its existing constructor instead of copying the whole input first.
Other query relations and their owners remain unchanged. This is a reference-lifetime correction, not measured RSS reduction.

<a id="async-ipc-layout"></a>
### IPC process and wire responsibilities

[async_engine_process.py](../../serving/backends/async_engine_process.py) keeps the actual parent owner, input and result
accounting, poll/ACK and teardown. [async_engine_worker.py](../../serving/backends/async_engine_worker.py) owns the actual child
loop and inherited-descriptor CLI; the parent launches that module. Shared packet encoding and compact result projection live in
[async_engine_wire.py](../../serving/backends/async_engine_wire.py), used directly by parent, worker and candidate sizing.
Observers and fixtures import the implementation they exercise; old serve imports and duplicate forwarding functions are absent.
[Combined source and CPU evidence](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#runtime-structure-and-bridge-capacity)
record unchanged result/unknown responsibility and the remaining real worker/socket checks. The three files total 939 lines,
compared with 920 before the split; the smaller parent file is a responsibility change, not a code-size reduction.

### Configuration ownership

The ordinary owner resolves timing and admission together once, retaining the requested timing in direct-constructor metadata.
The chain owner validates its own combinations without calling and discarding the ordinary records profile.
It reuses the exact immutable PreparedBatching value and leaves interval validation to ResourceRequestConfig.
Native observed intervals, resource declarations, supplied-owner matching and mutable token-budget state keep their existing checks.
[Counterexample, source and CPU coverage](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#single-pass-records-configuration)
describe the report correction; factory metadata and actual timing execution are unchanged.

### Actual serialization modules and close errors

Types and functions retain the module where Python defines them. New pickle payloads name
records_capacity, records_model_adapter or records_transport directly; no __module__ assignment
redirects them to the single-filter host. Standard pickle round trips succeed with that host unavailable,
while supported public capacity and execution types still expose the same objects. Actual Ray actor serialization remains to be checked on the server.

The query owner no longer forwards the unused private `_CoreModelActor`, `_capacity_failure`, `_encode`, `_measure`
or `_await_result` names. Internal callers import their model, transport and capacity objects from the defining modules;
the cascade adapter imports only its actual query owner from `lotus_records_core`. Current object serialization still names
those defining modules. The earlier private Core imports and pickle names are recoverable in a separate
[`0c28e246e8ce54fd5edf3375f99e2e24495d891a` tree](https://github.com/3444374/ai-operator-execution-optimization/tree/0c28e246e8ce54fd5edf3375f99e2e24495d891a),
without a forwarding shell in the maintained path. Public factories, capacity types and resolvers keep their existing imports.

Transport close preserves a finalizer's original exception as its cause and keeps the existing outer RuntimeError message.
Local stop/join and retry behavior, remote terminal evidence, result ACKs and unknown responsibility keep their existing rules.
The exact-RecordOccurrence source path copies through the original constructor using dataclasses.replace, without an intermediate describe dictionary.
[Source and original close errors](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#actual-adapter-modules-and-close-causes)
retain the earlier diagnostic scope. The subsequent [close-audit repair](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#close-audit-ack-handoff)
registers only receipts actually returned by the actor's close audit as exposed. An in-flight batch closes its model API,
recovers those results and drains delivery/ACKs while the result actor remains callable, then requests actor release.
Known local results still drain before the model-close call. A failed or interrupted handoff keeps its original exception,
unknown cleanup and actual RPC terminal report; a local Future cancellation does not confirm remote completion.
The first original server failure is explained by the closed-receipt ACK rejection. The second remains intermittent and unresolved;
79 passing local CPU methods do not establish actual Ray/model shutdown or qualify the new server version.

The two original CPU IPC methods subsequently passed once each on exact `f4e4868d`, with owned-process cleanup and lease release confirmed.
This does not explain the second historical intermittent failure. A final [route and source audit](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#core-closeout-verification)
found no missing close callback in ordinary, prepared/rolling, resident or fused entry points. The real-Ray symbol round trip and existing Future tests
passed on exact `5539a19f` with Ray 2.56.1 and zero model calls. They remain separate from active model-owner shutdown;
their standard invocation is in [the CPU test guide](../../../tests/records_cpu.md#real-ray-adapter-closeout).
The separate model run on exact `5539a19f` completed eight ordinary-async and eight rolling requests, with confirmed call terminals and final process cleanup.
Input and output token IDs matched across both paths, but each had three false negatives. Fixed test capacity and finite logical byte budgets do not establish dynamic-policy performance or physical memory capacity.
The strengthened 14-query CPU IPC test passed once on exact `b422951d`; its 168 synthetic engine submissions are separate from the 16 actual model calls.

### Feedback data and policy imports

Request and batch option types live in [feedback_options.py](../../scheduling/submission_control/feedback_options.py).
The common SupplyState and paired-work measurement live in [feedback_work.py](../../scheduling/submission_control/feedback_work.py).
The modern request adapter and its shared sampling method import those definitions directly, including the local update path;
they no longer load adaptive or stage_work merely to obtain data. Explicit batch feedback continues to use its existing policy.
The unused EWMA/HOL variants and their old package exports are retired; their accurate historical sources remain recoverable.
[Behavior, source identities and unresolved IPC checks](../../../../experiments/results/diagnostics/records_runtime_refactor_20261104/README.md#feedback-duties-and-record-copies)
separate unchanged policy behavior from code organization and memory-copy work.
