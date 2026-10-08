# PostgreSQL resource measurement tools

## Database-source queries

`semantic_system_query.py`执行有期限、逐条持久计数的原生Movie Map查询。
PG只提供原始只读关系；LOTUS、Daft、DuckDB社区AI扩展及Sema分别拥有提示、解析和执行。
透明HTTP观察器核对原始正文未改、模型身份、生成长度、结束原因、实际调用和token用量；
不增加原生并发控制或重试。输出关联与真假审计在完整消费后进行。
PG＋SemLoom继续使用原有`query_cli.py`，同任务比较说明见[方案](../../../../experiments/plans/语义系统对照.md)。
`native_file_capacity`在单元预留和实际POST之前只读核对文件描述符额度，估计包含同进程的SDK、代理入站与上游连接。
额度由调用方准备，不以检查替代原生执行或施加HTTP并发限制。

<a id="ready-query-timing"></a>

### 数据就绪后的SQL／原生API计时

`ready_query_recording.py`分开组件就绪、实际提交和统一消费结束；新`ready-timing.json`用SHA-256关联原`execution.json`。
`ready_semantic_query.py`让PG经同一HTTP观察代理执行，PG仍只在既有guard中计数。
`semantic_system_query.py --timing-mode ready`先读原始输入并进入`prepare_rows`，再执行原生SQL／API；默认`application`保持。
HTTP新增单调的接收、转发、上游响应与本地写出时刻，分别保留真实上游状态和回调导致的客户端错误。
Sema支持取消自有进程；其他原生API通过SDK请求超时和调用方单元进程期限处理在途工作。
就绪后的超时或HTTP首错停止继续转发，完整计时口径与验证见[方案](../../../../experiments/plans/SQL就绪计时.md)和[报告](../../../../experiments/results/postgresql/query_ready_timing_20261008/README.md)。

`query_workloads.py` prepares separate raw/reference files; `query_tables.py` installs and verifies
an immutable PG relation. `query_inputs.py` constructs messages from raw columns, preserving original
Movie review IDs separately from unique row occurrences. `query_config.py` declares one arm/task;
`query_runner.py` coordinates execution and post-query evaluation. `query_execution.py` owns the PG,
direct, LOTUS and Ray lifecycles; `query_evaluation.py` verifies actual requests, outputs and decisions.
`query_supervisor.py` bounds owned worker lifetime and closes its shared POST allocation on exit.
Optional `RayMapConfig.worker_pool` binds an existing caller-owned worker service; driver/gateway
remain per-query. Model identity and capacity must match, and a query exclusively claims workers
until all outcomes settle. [Synthetic diagnosis and timing scope](../../../../experiments/results/postgresql/text_map_preparation_tuning_20260930/README.md)
report service startup separately and retain the default query-owned worker control.
`filter_bindings.py` and `map_bindings.py` verify producer row identity before comparing decisions
or outputs. The Filter verifier accepts the query's instruction; `movie_queries.py` retains the
original Movie-specific entry point. Shared-query integration reuses both verifiers and the existing
gateway/session observers; its controlled endpoint lives in `tests/experiments/test_shared_query_integration.py`.

All query evaluators now enter through `map_query_recording.evaluate_recording`: completed state,
full row/byte counts and the recorded SHA are required. `native_map_bindings.py` verifies direct/Ray
input occurrences against actual requests, completions and final outputs, including duplicate text.
`query_failure_scope` reports errors before adapter cleanup; arbitrary uninstrumented context entry
can only be timed at return. Direct reading and result delivery share a bounded event wait.
The shared `../query_resources.py` checks all recorded logical usage transitions, including missing
observations. PG plan summaries distinguish configured capacity from an exposed plan window.

Ray declares `ray_num_cpus=4`, `ray_actors=2`, `ray_object_store_bytes=268435456` independently of
HTTP `concurrency`; each actor requires capacity and SQL reading requires a remaining CPU slot.
Process sampling includes descendants and registered PG readers, with an initial reader RSS snapshot.
Short-lived reader peaks and actual object-store usage may remain unavailable; summed RSS can count
shared pages more than once. [Validation and remaining work](../../../../experiments/results/postgresql/ab_validation_20260911/README.md).

Timed asynchronous recording checks the existing event-loop deadline after entry, around each row
write, and before EOF success. `async_deadline` reports the declared loop-clock deadline and the actual
observation/abort time. Written rows remain provisional on timeout; cleanup is timed separately.

`flow_timing.py` correlates an explicitly instrumented PG test build with Core submissions and client
receives through independent producer row bindings. It is a finite diagnostic, not a new production
capacity ledger or a substitute for semantic/resource validation.

`query_runner.py` records invocation time before manifest/model preparation as
`query_preparation_started_ns`, preserving the existing SQL/stream timing fields.
`waiting_positions.py` combines this boundary, Core terminal receipts, existing PG stage traces and
client rows. It applies only to a declared sealed immutable full scan: invocation-to-EOF includes
new gateway/tokenizer preparation, while the persistent driver and shared model service are outside
the per-query boundary. Its waiting areas include not-yet-materialized work and are not memory bytes.

`persistent_gateway.py` reuses one gateway/tokenizer for a finite sequence of nonempty full-scan
Map queries. The group owns startup, shutdown and one durable POST allocation. Each query retains
its invocation boundary, actual Job drain and socket-session closure; event projections refer to
unchanged group JSONL byte intervals. Shared startup is reported separately, and peer gateway RSS
can be sampled together. Query EOF never implies HTTP transport shutdown.

`QueryConfig.event_content="compact"` selects the existing compact gateway observer for PG Map.
Actual request-value hashes, producer row associations, output hashes and logical resource usage
remain checked. Organization candidate/group reconstruction is explicitly unavailable in this mode;
a matched full-observation query supplies that diagnosis. PG stage tracing is a separate query option.
`movie_partition.py` excludes earlier Movie inputs and selects new movie-disjoint tuning/evaluation
partitions by deterministic hashes. `m1_campaign.py` now requires an explicit v2 stage, shared
resources, paired run orders and exact stage POST allocation. Legacy fixed matrices are rejected.
Its offline preflight checks Core response reservations, input bytes and conservative PG retained/staging
estimates before database access. `m1_selection.py` uses complete throughput, sustained HTTP supply and
all paired repeats to report a provisional platform or an explicit inconclusive result. These observed
ranges are not confidence intervals. `m1_measurement.py` reports per-domain residence integrals,
model-reported token usage and output-value differences without counting reserved bytes as physical RSS.
Screening reuses `PersistentMapGateway` and the existing PG-source direct runner, then stops;
work tuning and independent evaluation require separate explicit schedules. Direct includes SQL reading
and client setup, so it is not a pure service ceiling. Evaluation settings must match a SHA-identified
tuning decision. [Current M1 design](../../../../experiments/plans/数据组织.md#m1-throughput-platform).

For PG Map, `pg_total_budget=true` selects total retained bytes instead of equal per-row reservations;
`pg_staging_bytes` controls the separate single-row preparation area. `window_memory.py` checks the
producer's per-operator memory trace and final release. Its summed peaks are not concurrent query RSS.
[Controlled and real-model checks](../../../../experiments/results/postgresql/pg_window_budget_20260910/README.md).

Optional `organization_config` and `organization_sha256` select a fixed local Map organization file.
The runner copies the verified configuration into the private unit directory and requires PG Map with
total retention enabled. `organization_evaluation.py` checks complete-request token estimates against
actual POST bytes and server prompt usage, accepted candidate prefixes, group membership, dispatch
order and active token work. Native Ray/LOTUS execution does not use these controls.

Organization audit v2 reconstructs active request/work reservations from Core submission and
authoritative terminal events and compares every recorded compute snapshot. It verifies raw member
submission order without sorting and reports HTTP start order separately. The declared generation
budget comes from the semantic plan. Optional capacity-refusal observations distinguish failed flow
eligibility checks from selected-member dispatch checks; their counts include repeated probes.
Legacy traces without this observation report unknown refusal counts. Rows/work groups still expand
into independent requests; they do not define a model batch or group completion barrier.
[Audit and matched real-model diagnostics](../../../../experiments/results/postgresql/cd_validation_20260911/README.md).

Organization audits accept `expected_task_count` from independently selected raw inputs. Zero tasks
with no opened flow may finish without a drain event; absent evidence alone is insufficient.
The [server follow-up](../../../../experiments/results/scheduling/capacity_wait_server_20260914/README.md)
validates empty/nonempty PG queries and ordinary SQL result buffering. The probe uses distinct
write-once case checkpoints and a final summary. Client receive time is not SemMap node delivery time.

Use `query_cli.py` through [database_queries.py](../../../scripts/experiments/database_queries.py).
PG-source direct is a bounded execution reference; Ray SQL/HTTP and original SemBench LOTUS programs
retain native execution ownership. These entries have controlled HTTP evidence, with real model quality
and performance pending. [Scope, tests and failure history](../../../../experiments/results/postgresql/database_queries_20260910/README.md).

## Matched text Map comparison

`QueryConfig.map_payload_backend` declares the selected finite payload batching backend and must
match `RayMapConfig.payload_backend` before gateway startup. Both default to `daft` for existing
configurations. The optional `arrow` path constructs independent Arrow batches from already selected
rows without a Daft graph. It is identified as an Arrow/Ray diagnostic and is rejected by the
existing `pg-daft-ray` campaign and comparison reader. Native baseline graphs remain unchanged.

`QueryConfig.ray_address` optionally connects native Ray Data to an existing local single-node Ray 2.56.1
cluster. Shared mode checks the declared CPU/GPU and object-store totals, includes driver connection and
query-specific graph/worker creation in query time, and disconnects without stopping the caller-owned
cluster. Omit `ray_temp_root` in this mode. The default query-owned runtime remains available. Shared
service process RSS is unavailable to the descendant sampler and must not be presented as total memory.

`query_runner` records model-configuration SHA and per-query executor lifetime. `text_map_comparison`
reads SHA-identified summaries plus their recorded output/evaluation files, requires the execution roles of the selected profile
and complete repeats, and selects the lowest observed median for each role on tuning inputs. It rejects
failed, incomplete, modified or differently timed records; ordinary incorrect labels remain in the report.
Evaluation references the tuning report SHA, matches selected configurations and requires a different
input manifest. Different manifests do not establish sample independence. Hardware/service provenance,
data overlap/history, complete resource accounting and predeclared run budgets remain separate checks.
Old records missing these identities are not silently upgraded. The offline comparator calls no model and
does not change `m1_selection`. [Plan](../../../../experiments/plans/completed/文本Map对比_20260930.md),
[local checks](../../../../experiments/results/postgresql/text_map_comparison_preparation_20260928/README.md).

`text_map_campaign` executes one explicitly declared qualification, tuning or evaluation stage through
the existing supervised query workers. Preflight checks SHA-identified inputs, immutable installation,
model/environment records, complete candidate orders, shared runtime and exact request totals. Execution
requires an existing fresh finite ledger and a database environment variable; the stage wall deadline also
limits each worker. The first error preserves completed records and stops the stage. It never retries,
replenishes quota or starts a following stage. Map evaluation records model-reported token usage for all
four paths; fixture responses without usage remain unavailable.

After every worker, the campaign compares the recorded configuration identity and query unit ID with
the dispatched candidate before accepting results. The first mismatch stops the stage and retains spent
quota. A stable changed configuration cannot become a tuning choice.

PG startup writes `gateway-preparation.json` and returns `resources.pg_preparation`: command
construction, PG plan preparation, gateway process creation and socket readiness wait, including failed
waits. `observer.json.startup` reports gateway module imports and observer setup; interpreter startup
is outside that import span. Ray Map emits `core_ray_startup` for library import, driver connection,
actor creation/readiness and first payload materialization, including lazy Daft/Arrow loading.
`core_ray_first_submit` records the first attempted RPC after its POST guard, not remote HTTP start.
Durations use one local process clock; nested spans must not be added. The existing preparation-to-EOF
measurement still includes startup. [Review](../../../../experiments/results/postgresql/text_map_four_path_comparison_20260930/README.md#branch-review).

Native Ray 2.56.1 HTTP requests are awaited sequentially within one batch. Batch row count is not
HTTP concurrency. `ray_async_batches_per_actor` separately controls the native asynchronous batch
limit and is propagated through each job's Ray runtime environment; its default is one. Actor count
times this limit must fit the declared HTTP allowance. Source block count can further reduce usable
parallelism, so the observed HTTP peak remains necessary. The initial model comparison used one
async batch and only two SQL blocks; its slow native result is not a strong Ray baseline.
[Source audit and follow-up](../../../../experiments/results/postgresql/text_map_matched_20260930/README.md).


## Source information controls

`m2_source.py` implements five experiment-owned input paths: streaming FIFO, window FIFO,
window length ordering, paid global length metadata, and explicit reuse of that metadata.
The window paths share `WorkWindowOrganizer`; global paths keep only row identity, position,
work and request digest in a size-limited SQLite file. Payload is fetched again from the same
read-only repeatable-read PG snapshot and checked against the digest before submission.
`m2_runner.py` uses the existing bounded `DirectMap` client, durable POST allocation and common
disk result sink, restoring source order for every arm. Scan, tokenization, sorting and rereads
are included in invocation-to-EOF; reused metadata names its separately measured producer.
These are `direct_client_control` experiments, not new SemMap planner or carrier capabilities.
The callable entry is `run_information_query`; the finite experiment schedule is retained with
the [real trial evidence](../../../../experiments/results/postgresql/capacity_organization_image_validation_20260920/README.md).

## Resource collectors

These tools observe the existing synchronous SemMap fixture path. Production SQL, planner, provider and
wire semantics remain in their existing modules. The [Map engineering contract](../../../../experiments/plans/生成算子.md)
owns the implementation and verification plan.

| Module | Responsibility |
|---|---|
| `resource_lifecycle.py` | Immutable settings, required phases and pure final assessment |
| `resource_phase.py` | Baseline, operation checkpoint, cleanup sampling and phase evidence |
| `semmap_resource_runner.py` | Exclusive run directory, build, isolated PG cases and CLI |
| `resource_qualification.py` | Sampled peak and cleanup policies with versioned thresholds |
| `provider_session_attribution.py` | Strict session/task replay, scoped socket attribution and residual identity checks |
| `semmap_resource_gateway_observer.py` | Fixture CLI composing the shared observer with an optional bounded handshake barrier |
| `resource_client_v3.c` | Parameterized single-row libpq fixture consumer with an explicit exit barrier |
| `runtime_helpers.py` | Shared owned-process and isolated PostgreSQL helpers, also used by choice checks |

Filter and Map share [session observation](../gateway_observer.py) and the
[configured request ledger](../attempt_ledger.py). The fixed-model observation CLI remains
`src.experiments.choice_gateway_observer` for compatibility; it accepts a budget ID/limit and optional
`--session-events` for either operator. Historical result scripts are evidence snapshots, not runtime dependencies.
The fixture runner accepts `--pg-user` and `--pg-port`; its streaming client uses the actual connection's
host, port, role and database. Model endpoint/identity/timeout remain in repository-external fixed-model JSON.

Collector and recorder primitives live in `src/observability/process_resources/`. Test categories are
lifecycle, collection, attribution, policy, observer and CLI; the old `audit_round2` source-string checks
have been replaced by observable behavior checks. Diagnostic mode is 1×100 and never grants formal
qualification. See [CLI usage](../../../scripts/README.md) and [current evidence](../../../../experiments/results/postgresql/semmap_resource_lifecycle_20260906/README.md).

`profile="main"` compares PG/SemLoom Daft/Ray, PG-source direct, native Ray Data, and native Daft.
`profile="local-ablation"` compares the two SemLoom execution backends. The default
`legacy-four-paths` preserves the original records and their selection identities. Evaluation cannot
switch profiles after tuning. Main preflight rejects the old under-supplied Ray shape, mismatched
native CPU declarations and unequal candidate capacity sets.

`text_map_candidates` generates three native supply settings (16/64/128) per main role without
starting any services or model requests. Ray uses 1/2/4 actors, 16/32/32 async batches per actor,
one row per batch and four SQL blocks. The new `query_daft` entry uses Daft Native 0.7.21 SQL
reading and its async batch UDF scheduling. The matched HTTP kernel is not built-in `prompt()`;
one-row batches with native in-flight settings 15/63/127 yield observed peaks 16/64/128 in the
held-response fixture. Native startup uses `set_runner_native(num_threads=8)`; this is not physical
CPU isolation. Timeout closes outgoing POST allocation and the existing supervisor bounds process
lifetime; no native instantaneous model cancellation is claimed.
[Candidate coverage and evidence](../../../../experiments/results/postgresql/text_map_native_candidates_20260930/README.md).

### Ray远程错误记录

Ray Map遇到payload准备、HTTP、RPC提交/等待或返回值检查异常时，记录`core_ray_execution_error`。
事件保留任务key、阶段、安全异常类型/原因链和代码位置；不包含异常消息、请求正文或服务凭据。
公开事件也保留阶段与`remote_outcome=unconfirmed`；诊断信息不是完成回执，不改变未确认额度或不重试行为。
实际断连与后续查询恢复记录见[诊断修订](../../../../experiments/results/postgresql/text_map_four_path_comparison_20260930/README.md)。
