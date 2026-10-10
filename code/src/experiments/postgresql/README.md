# PostgreSQL resource measurement tools

## Database-source queries

`semantic_system_query.py`执行有期限、逐条持久计数的原生Movie Map查询。
PG只提供原始只读关系；LOTUS、Daft、DuckDB社区AI扩展及Sema分别拥有提示、解析和执行。
透明HTTP观察器核对原始正文未改、模型身份、生成长度、结束原因、实际调用和token用量；
不增加原生并发控制或重试。输出关联与真假审计在完整消费后进行。
PG＋SemLoom继续使用原有`query_cli.py`，同任务比较说明见[方案](../../../../experiments/plans/语义系统对照.md)。
`native_file_capacity`在单元预留和实际POST之前只读核对文件描述符额度，估计包含同进程的SDK、代理入站与上游连接。
额度由调用方准备，不以检查替代原生执行或施加HTTP并发限制。

<a id="native-adapter-query"></a>

### 原生方法与可替换执行器

`native_adapter_query.py`提供外部有限输入的共同固定Map参照，`supplier_adapter_query.py`装配已交付的供应商方法。
输入加载与组件准备计入完整应用；`record_prepared_execution`直接记录实际API入口至消费结束，方法／图构建留在查询内。
原生SQL reader、PG和原生语义系统旧入口保持。新入口使用已有公共批次与方法驱动，不新增调度核心。

| `--arm` | 方法与执行所有者 |
|---|---|
| `fixed-map-native-daft`、`fixed-map-native-ray`、`fixed-map-semloom` | 图外生成相同`SemanticMapPlan`完整调用，分别由Daft Native、Ray Data及SemLoom Daft／Ray执行 |
| `lotus-adapted-native`、`lotus-method-semloom` | 原`sem_map`、SDK转换及解析，分别进入原LiteLLM请求池或公共执行入口 |
| `lotus-two-map-native-staged`、`lotus-two-map-semloom-staged`、`lotus-two-map-semloom-incremental` | 原两Map方法；阶段等待参照及既有MethodDriver逐行继续执行 |
| `duckdb-adapted-native`、`duckdb-method-semloom` | 匹配的patched扩展，原生或C ABI完整批次执行，再进入原C++解析 |
| `sema-native-direct`、`sema-native-transparent`、`sema-method-semloom-request-service` | 作者二进制直达、透明观察或后置请求服务；原请求池与供给保持 |
| `fixed-map-semloom-local-diagnostic` | 本地无Ray诊断；单独标识，不进入主实现性能比较 |

`native_adapter_metrics.py`按行、阶段与逻辑调用编号关联任务就绪和原调用者完整响应，统计单位为一次完整模型调用。
LOTUS批次的调用者取得完整SDK响应列表后才记返回；逐行继续执行则在supplier resume收到完整HTTP响应时记返回。
DuckDB保留C++的steady-clock值，Python回调另记本进程时刻；未证明两种时钟相同，不做跨钟减法。
Sema作者产物缺少池前任务就绪及逐行完整响应对应，调用端到端标为不可观测；请求服务自己的阶段另列。
原始字节、HTTP元数据与SDK值对象的哈希各自说明表示形式，完整HTTP时段不改名为纯模型计算。
查询与调用分别保存全部分位样本、秩与最大值标记；组织等缺少实际起止的阶段写不可观测。

源码与实际库核对、旧PG18.3回归、失败及资源释放见[工程报告](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md)。
主机配置与模型文件由调用方预检；请求／状态／结果各自有限，原生等待与观察成本仍计入实际耗时。
更大工作负载、极端尾部、多模型、级联、Join、并行展开及PG多Map仍需分别验证。

`persistent_native_adapter_query.py`的`PersistentAdapterGroup`复用上述入口，持有常驻Ray连接、各arm的Core／worker和LOTUS LM。
每查询创建独立消费流、观察与累计账本单元，关流后核对资源归还；源输入和当前formatter／图／首批物化不从查询计时中移出。
实际API提交仍归原`ready-timing.json`；新的`persistent-query.json`另记当前输入读取前的release、EOF及原摘要SHA-256。
固定Map三条与LOTUS原生／本地诊断／Daft＋Ray三条通过实际库重复fixture；启动另列，当前原生Ray仍每图创建自身actor。
单arm独立集群与共享部署的实际Ray额度分别保存；Sema作者进程可保留，默认服务Core仍按查询创建。
Sema 可显式选择 `sema_executor_scope=group-diagnostic`：专属固定线程持有同一 Core／worker，
当前查询借用执行器并拥有独立 `NativeTaskSession`、HTTP 入口、响应 lease 和观察；组退出才关闭执行器。
连续查询的输入、前发送回调、session 身份与结果分别核对；取消、错误或资源未归还后停止后续借用。
CPU／替身查询和退出检查已通过，实际 Daft／Ray 对象释放与真实模型耗时仍需单独验证，默认保持 `query`。
DuckDB与Sema五条已独立通过`8 → 8 → 128`行实际库fixture和同Sema进程更换URL检查；
同连接／作者进程复用、输入替换和当前结果关联分别核对，后置Core按查询创建的事实保留。
两Map常驻实际库、修复后的执行节奏及真实模型复核仍按自身证据判断。
调用方式见[脚本入口](../../../scripts/README.md#native-adapter-query)，原始事件与适用范围见[同题报告](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#persistent-fixture)。
LOTUS额外记录完整SDK响应列表返回时刻，先于观察序列化与原方法统计；原提交、HTTP和消费时刻保持。
常驻owner从组件取得开始统一清理，已建执行器立即登记原drain动作；退出及摘要读取／写入通过现有CellErrors保存，
原查询异常保持顶层。修复前后诊断与三个独立供给问题见[同题修复记录](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#adapter-performance-repair)。
供应商release、producer退出和Sema服务关闭的第一错误分别处理；最终同一源码的真实库回归与八路径输入替换见[共同检查](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#adapter-final-source)。
DuckDB现有identity／summary在退出时同时保存bridge与内层executor的清理字段和关闭报告；观察与iterator退出连续失败仍保持原观察错误。模型与最新CPU来源分别见[同题模型记录](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#repair-model)。
单查询和常驻CLI新增可选持有任务数与Sema作者线程参数，活动请求数仍取原生options的concurrency。
两项省略时保持原装配；H128下输入与结果额度各128MiB，Sema Core／worker 的默认作用范围仍为单查询。
DuckDB批回调在原生请求池之前分流，原池设置保持其64上限，SemLoom请求数单独装配。
[准备记录](../../../../experiments/results/postgresql/native_adapter_integration_20261009/README.md#capacity-preparation)保存实际配置与无模型检查。

`duckdb-method-semloom-local-diagnostic`复用相同SQL批回调、响应解释和公共Core，只将执行传输换成本地HTTP。
常驻入口的可选`--adapter-timings`默认关闭，开启后保存最后一个SQL向量的推进与原生消费累计时间，
字段及包含关系见[DuckDB局部说明](../../../integrations/duckdb_ai/README.md#可选推进与消费诊断)。
实际库检查可选`SEMLOOM_ADAPTER_TIMINGS=1`；相同桥的本地与Daft／Ray比较仍须分别记录真实模型观察。

<a id="ready-query-timing"></a>

### 数据就绪后的SQL／原生API计时

`ready_query_recording.py`分开组件就绪、实际提交和统一消费结束；新`ready-timing.json`用SHA-256关联原`execution.json`。
`ready_semantic_query.py`支持PG、PG-source direct、原生Ray Data和Daft Native Map，经同一HTTP观察代理执行；各入口仍只在既有guard中计数。
`run_ready_pg_query`保留原PG接口与schema；新增`run_ready_database_query`及异步记录器共用提交／EOF口径。
direct先准备连接与客户端，原生Ray先连接调用方集群，Daft先设置一次Native线程；SQL扫描、原生reader连接、提示与查询图仍计入查询。
本次新增入口通过72项查询相关本地检查，服务器接入与模型比较待执行；十二路径范围见[统一方案](../../../../experiments/plans/语义系统对照.md#unified-map-comparison)。
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
