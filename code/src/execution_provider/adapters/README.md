# Native task execution

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

Record-local F/M sections use `LotusChainFIFO` over the existing Core/MethodDriver.
Extract waits for actual source EOF, complete predecessor finals and drained result
responsibility; the original parser and relation assembler then run once before continuation.
One model is started and finally closed per query, including all its sections. Cross-query
reuse, multiple deployments and arbitrary DAGs are not provided by this option.

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

The caller continues to own and retain Ray across queries. Each query still owns
and closes one fresh model; this option does not implement cross-query model
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

The local candidate also accepts `PreparedBatching("token_budget", None, B)` on
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

The default tokenizer is loaded from the declared engine tokenizer/model. A caller
may supply `batching_tokenizer` for a controlled environment; matching its rendered
template to the native model remains the caller's responsibility. CPU fixtures use
a small tokenizer and inference substitute. [Query and independent-member cases](../../../tests/execution_provider/test_lotus_async_token_batches.py)
and [lifecycle boundary cases](../../../tests/execution_provider/test_lotus_async_batch_lifecycle.py)
check this optional API. Real Ray/model/GPU correctness and performance and
dynamic C_batch or joint B/C control remain pending. The standalone `c79fcc6d`
candidate excluded PR83. This local integration starts from exact `2212e331` and
adds the async core, closed-page repair and ordinary entry; its combined source
requires new qualification and does not inherit the base's model/GPU evidence.

The ordinary records factory `create_records_core` accepts the same optional
`prepared_batching` and `batch_concurrency`. It requires fixed async mode without
resource-feedback admission or stage timing. A finite source must provide its
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
is no new scheduler or engine. The normal Movie config/CLI and isolated session
driver use this factory; resident reuse, dynamic admission/cache and unsupported
row-capped combinations fail before model construction. Configuration and commands
are maintained in the [Movie entry documentation](../../experiments/movie_matrix.README.md#record-token-batch-entry).

## Optional fixed-B supply feedback (local CPU candidate)

`SemLoomRecordChainCore` and the Python `create_records_core` factory accept
`batch_feedback=SupplyFeedbackConfig(max_batches=operational_batch_ceiling)` from
`src.scheduling.submission_control.stage_work`. This requires the existing
record-local token path, `batch_concurrency=1`, FIFO, record progression and W off;
the ceiling must fit R. Omission retains the fixed path and installs no sampler.
The Movie CLI/JSON and resident-session schemas do not yet expose this new field.

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
setting without aborting members or releasing unknown credits.

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
an explicit delivered-work test configuration and a simulated engine-stat source;
default computed policy is replay-tested. These are engineering checks, not GPU
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
remain unchanged. The public full-query owner has no cross-query resident reuse, so those
historical first/reuse runs are not reproduced merely by exposing this option.


## records Core 可选动态接纳

`lotus_records_core.create_records_core` 的固定默认与 sync/async 既有路径保持。
可显式选择 `profile="delivered-work-scan"` 或 `profile="computed-work-local-pairs"`；
这两个名称是 legacy 实验参数对照，不推荐其
10秒／剩余64条等限制作为新的资源平台研究配置。冻结历史源码仍从具名复现还原。
名称与原实验参数、首次／复用条件的唯一映射见[配置说明](../../scheduling/runtime/calibration.README.md#原实验配置选项)。
`profile={"mode":"async","max_inflight": C0,"admission": {"kind":"resource_feedback", ...}}`
显式接回原单遍 native-feedback admission；选项来自现有 `ResourceFeedbackConfig`，
不是另一个执行器。`C0` 是起始软窗口，owner 上界为声明的
`RecordsResources.max_active_batches`；prepare 数、模型配置和 actor/IPC 容量在启动前确定。
应用层未完成请求与引擎每轮处理的序列分别计限：`EngineSpec.max_num_seqs` 保持传给
vLLM 的每轮序列上限，不再截断 records Core 的请求额度。外层额度可以更大，仍受
原 Core、actor、IPC 的请求／结果槽位与字节预留约束；这不表示这些请求同时在 GPU 上运行。
显式 `async_limits.max_inflight/result_slots` 必须覆盖该 owner 上界，不是只覆盖 C0。

`allow_exploration=True` 显式允许单遍性能试探；不设 admission 时保持固定且不启动 native
反馈采样。当前显式字典的默认 `exploration_s=None, min_remaining=0, max_changes=None`
不设内部总探索期限、剩余条数门槛或变更次数上限；正值仅供显式 legacy 预算对照。
`max_inflight="auto"` 仅在显式 async 下选择声明的 Core request budget，
不是实测调优，也不自动扩大调用方额度。同步 Core 仍只
接收 model batch 1；有限同步批参照 `lotus_records_static` 不冒称 Core 批处理。
当前反馈只调未完成请求窗口 C；每批行数或累计 token 工作预算 B 是组批边界，
不是这个窗口。当前 records Core 仍为 B1，没有扫描 B 或按 token 组批与动态反馈联动。

普通查询可用同一公共 `scheduling.runtime.calibration.run_admission_query` 接精确配置复用。
Movie 的薄配置槽及计时/资格边界见 [calibration.README](../../scheduling/runtime/calibration.README.md)。
恢复的是两版旧策略本身，历史波动和负结果保留；旧短查询功能实测的边界见[证据台账](../../../../experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md#records-dynamic-sad24-621)。
两版原参数的同条件首次／复用模型验收须另列，不能以开关能运行或短查询返回替代。


### 静态参照的显式 session

维护 Movie session 现在可选择 `backend="semloom-static-reference"`、
`profile={"mode":"sync"}`，继续使用原 `RecordsResources` 的 prepare/model batch。
这是 Core 外有限批参照，不是 Core B4；Core 的原单行合同不变。
resident 在查询前调用原模型 owner 的 start，两次完整查询复用模型，query EOF 不关闭模型，
session 结束一次原 close。不同 FilterMethod 指令明确拒绝，不能复用旧 formatter 冒称支持。
准备批、原有限模型批、原 LOTUS parser、身份和未确认关闭责任不变。
本接线已做维护 CLI/CPU first-reuse 与失败边界检查，真实模型/物理退出仍需本候选单独资格。
