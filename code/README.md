# AI 任务研究工程

本页说明目录、接口与执行流程；接纳及验证范围见[实现状态](INFRA_STATUS.md)，
运行命令只在[脚本入口](scripts/README.md)维护，方法与后端细节从所属模块读取。

| 对象 | 接口说明 |
|---|---|
| LOTUS 完整请求、records、链与原评价 | [方法模块](src/semantic_methods/lotus/README.md) |
| Core 与完整响应 | [provider adapters](src/execution_provider/adapters/README.md) |
| 模型引擎 | [同步引擎](src/serving/backends/vllm_engine.py)、[异步引擎](src/serving/backends/vllm_async_engine.py) |
| PG 载体与 SQL 支持形状 | [extension](postgres/semloom_pg/README.md) |
| 观测定义与离线结果 | [observability](src/observability/README.md)、[离线工具](scripts/analysis/OFFLINE_RESULTS.md) |

## 文本Map诊断选项

下列可选接口保留各自用途，实际默认以源码为准；完整数字与失败只从对应报告读取。

| 选项 | 使用要求与现状 | 结果来源 |
|---|---|---|
| `RayMapConfig.worker_pool` | 默认 `None` 由查询创建；显式池由调用方在已连接的 Ray 中创建，查询独占借用并核对模型／容量；创建者保持连接，未确认错误后停止借用 | [worker 复用](../experiments/results/postgresql/text_map_worker_reuse_real_20260930/README.md) |
| `RayMapConfig.payload_backend` | 默认 `daft`，`arrow` 为显式直接分批对照；完整行请求、身份与取消不变 | [Arrow 实际模型](../experiments/results/postgresql/text_map_arrow_real_20261003/README.md) |
| `RayMapConfig.preparation` | 默认 `None`；准备借用、复制空间和未知调用责任分开，原型不推荐启用 | [准备分段与真实负结果](../experiments/results/postgresql/text_map_preparation_validation_20261004/README.md) |
| `coalesce_queued_preparation` | 默认 `False`；尾块追加仅保留显式诊断 | [追加与返回等待](../experiments/results/diagnostics/map_queued_preparation_20261008/README.md) |
| `remote_budget_mode=batched` | 默认逐行同步；显式批量最多合并 16 条已就绪记录，下次推进持久提交后才能 RPC，额度不足整批拒绝，不等待未来请求 | [批量记账负结果](../experiments/results/diagnostics/request_budget_batch_20261003/README.md) |
| 共享预付观察、线程记账与就绪窗口等待 | 保留原诊断身份；共享观察未进入正式查询入口，就绪等待仅保留补丁；不从局部计数变快推导完整查询收益 | [共享观察](../experiments/results/diagnostics/mapped_request_budget_20261003/README.md)、[线程记账](../experiments/results/postgresql/text_map_threaded_accounting_20261002/README.md)、[窗口反例](../experiments/results/diagnostics/ready_window_coalescing_20261003/README.md) |

新 RPC 组合和共享准备的采用条件归[数据组织方案](../experiments/plans/数据组织.md#execution-engineering-hypotheses)。

## 当前 PostgreSQL 文本 Map 的执行流程

下面描述已接入的增量 Map 路径；外部 runner 的历史流程按末尾固定来源读取。

```mermaid
flowchart LR
    SQL[SQL语义调用] --> Plan[PG计划中的SemMap]
    Plan --> Child[普通子计划读取行]
    Child --> Task[PG生成带序号的语义任务]
    Task --> Gateway[外部gateway与共享执行核心]
    Gateway --> Batch[Arrow批次：Daft或直接分批]
    Batch --> Worker[Ray HTTP worker]
    Worker --> Model[外部模型服务]
    Model --> Result[结果核对与容量归还]
    Result --> PG[PG解析结果并恢复行序]
    PG --> Client[SQL结果]
```

1. PostgreSQL将支持的`ai_semantic.map(text, instruction, options)`调用放入可见的`SemMap`执行节点。
   版本化语义计划保存instruction、prompt/parser、模型、生成参数及NULL/错误/顺序政策。
   普通子计划负责数据读取、常规谓词、snapshot和权限；外部执行器消费PG生成的任务。
2. PG执行节点按有限窗口保留输入与序号，通过本地Unix socket发送任务。无法安全预取的子计划会
   将有效窗口降为1。提交被拒时保留对应行，接纳前不前移已接受序号。
3. gateway核对协议、语义与任务身份，注册查询Job和session；共享执行核心限制请求数、输入与结果容量。
   当前Ray选择默认经Daft分批，可显式选择直接Arrow批次；Ray对象容量另行记账。
4. Ray worker取出每行完整payload，通过HTTP调用外部模型服务。一个传输批次可包含多行；每行仍为
   一个独立请求，Ray worker不负责GPU推理，也不修改数据库拥有的生成语义。
5. 返回结果核对任务身份、长度和解析规则，再经provider回到PG；PG关联原始行并按输入序号交付。
   查询结束或取消停止新增任务；已发送调用确认后归还资源，未知远端结果继续保留计费。

| 步骤 | 当前源码入口 |
|---|---|
| SQL识别与计划构建 | [sem_map_call.c](postgres/semloom_pg/src/planner/sem_map_call.c)、[sem_map_path.c](postgres/semloom_pg/src/planner/sem_map_path.c) |
| PG输入绑定、窗口、结果关联 | [sem_pump.c](postgres/semloom_pg/src/executor/sem_pump.c)、[uds_provider.c](postgres/semloom_pg/src/provider/uds_provider.c) |
| gateway注册及Job/session接入 | [multiplexed_gateway.py](src/execution_provider/multiplexed_gateway.py)、[incremental_session.py](src/execution_provider/adapters/incremental_session.py) |
| 公共容量、接纳、提交与交付 | [session.py](src/scheduling/core/session.py)、[async_backend.py](src/scheduling/runtime/async_backend.py) |
| 数据批次、Ray对象和HTTP worker | [payloads.py](src/data/materializers/payloads.py)、[ray_map_transport.py](src/execution_provider/adapters/ray_map_transport.py)、[async_fixed_model.py](src/execution_provider/adapters/async_fixed_model.py) |

本轮使用固定请求计数与单模型endpoint；工作量描述和策略装配入口是
[incremental_execution.py](src/execution_provider/adapters/incremental_execution.py)。
Core没有自动改写prompt或拆分单行推理，也没有在本轮执行自适应模型路由或GPU/kernel优化。

HTTP执行路径仍可选；默认Ray worker由查询创建，可选服务由调用方创建并供查询独占借用。
实验中的共享gateway已验证顺序查询复用；query-job模式的并发查询另见[查询共享检查](../experiments/results/postgresql/query_sharing_lifecycle_check_20260914/README.md)，两者不自动替换默认部署方式。
当前SQL支持形态见[extension说明](postgres/semloom_pg/README.md#supported-query-shapes)：
单个Filter→单个生成型Map的受限组合已有[绑定验证](../experiments/results/postgresql/filter_map_binding_20260907/README.md)，
可选`query-job`将受支持的算子流归属同一个查询Job；本次耗时诊断只运行单Map。
多Map、任意组合、join及更宽SQL接入尚未实现，不以当前文本Map验证代替这些能力。

<a id="query-accounting-scopes"></a>
### 查询归属、请求发送与计数范围

多个PG backend进程可以分别登记查询，共享同一个gateway/Engine；同一查询的多个受支持算子流归属一份Job预算。
查询、算子流和任务分别由Job（查询调度组）、session/flow（算子任务流）与任务key（流内关联键）关联，worker进程数量不增加查询份额。
当前[PG登记协议](../experiments/plans/数据库接入.md#query-job-版本化登记层)要求各流与控制连接来自同一可信`uid/pid`（用户号/进程号）；
同一查询的PG并行执行进程共用登记、PG算子流的网络登记与查询身份跨机传递仍pending。

当前gateway由一个控制线程推进一个Engine，[异步backend](src/scheduling/runtime/async_backend.py)维护独立输入输出（I/O）循环，
[Ray HTTP actor](src/execution_provider/adapters/ray_map_transport.py)在worker进程取payload并发送请求。
统一维护资源责任与分布式发送可以同时成立。当前SemLoom文本Map的Ray实验路径在远程调用（RPC）前记账，原生Ray/Daft从各自HTTP调用进程记账；
计数设施需要支持这些进程共享同一额度。这一差异不表示并行能力不同，也不决定生产服务进程数或payload传输拓扑。

| 类型 | 当前实现与作用范围 | 完成或取消后的处理 |
|---|---|---|
| 累计实验请求预算 | [SharedClaimedUnit](src/experiments/shared_request_budget.py)用SQLite事务统一领取编号；[共享映射候选](src/experiments/mapped_request_budget.py)先持久预付有限单元，再供同机进程共享领取 | 同摘要的独立调用分别计费；已领取但未发送仍消费预算，不因完成、取消或失败退还 |
| 在途请求与工作量容量 | [SessionCapacity](src/scheduling/core/session_capacity.py)依据任务记录检查同一Engine内的总量、Job和session上限；工作量单位由执行组装声明 | 权威终态结清后归还计算名额；未知远端结果继续保留占用，记录的活动数不等于实际GPU活动 |
| 统计与观测 | [HTTP观察器](src/experiments/native_http_observer.py)记录获准编号、调用开始、响应和错误，Core及PG另记录提交、终态与结果交付 | 事件用于核对调用和结果，不发放额外执行容量；客户端调用开始不能证明服务端已接收或模型只执行一次 |

[Ray actor配置](src/execution_provider/adapters/ray_map_transport.py)明确禁用自动重启及任务重试；远端错误按未确认结果保留记录。
这不构成故障条件下模型执行恰好一次的保证。获准调用、客户端发送、服务端接收、确认完成与结果交付须按实际证据分别核对，
不能要求取消或未知结果时所有计数相等。记账与记录会改变提交节奏和准备窗口，不能直接从完整时间扣除其累计耗时。

[服务级协调分析](../docs/research/优化方法依据.md#workflow-621-多-job-共享服务必须使用服务级协调)已说明局部容量之和的问题；
[通用Ray共享协调器](src/scheduling/runtime/shared_credit_ray.py)保留在既有外部执行路径，但[当前PG增量组装](src/execution_provider/adapters/incremental_execution.py)未接入它。
多个gateway使用同一模型服务时，单实例上限不代表整个部署的总上限；该接入及故障验证仍pending。
原生Ray/Daft从各自调用进程接入共同实验预算和观测，执行与调度仍由原生系统拥有，不经过SemLoom任务选择和容量策略。

现有PG查询共享的受控与有限真实模型检查由[查询共享报告](../experiments/results/postgresql/query_sharing_lifecycle_check_20260914/README.md)维护，
其中执行、关联和回收检查与复述质量负结果分别解释。共享映射候选的Linux/真实Ray SDK及本机HTTP检查由
[计数接入报告](../experiments/results/diagnostics/mapped_request_budget_20261003/README.md#server-observer)维护，其PG/真实模型检查尚未完成，不能混用两项资格。

## 目录结构

```
code/
├── src/
│   ├── data/                     ← PostgreSQL/Daft source、Arrow/Daft materializer、sink、workload
│   ├── planning/                 ← 纯 cost estimator 与 work-unit packing；不依赖执行引擎
│   ├── semantic_methods/         ← 原方法、SDK、records与关系处理；共享结果已归位
│   ├── scheduling/
│   │   ├── core/                 ← typed state、capacity contract、execution ledger、scheduler loop
│   │   ├── organization/         ← pending batching、token/work budget、service quantum
│   │   ├── submission_control/   ← request/work准入与容量策略；不是外部原生baseline执行器
│   │   ├── endpoint_routing/     ← pinned/queue/work/prefix routing
│   │   └── runtime/              ← Ray adapter、服务观测缓存、SAOR 容量/多阶段薄适配
│   ├── serving/                  ← completion/embedding backend 与 vLLM probe
│   ├── modalities/
│   │   ├── text/                 ← prompt/output-token work 语义
│   │   └── image/                ← encoded bytes/tensor/CLIP/source/audit 合同
│   ├── observability/            ← metrics 与 profiler 配置/replay/trace/Ray 接线
│   ├── baselines/
│   │   ├── common/               ← manifest/result/provenance/validity gate
│   │   ├── text/                 ← ceilings/controls/frameworks/products/orchestration
│   │   └── image/                ← provenance 与 Daft/Ray Data native graph
│   ├── experiments/              ← calibration、scenario、shared-vLLM 编排
│   ├── execution_provider/       ← gateway/wire与前端→Core适配；不接管原生baseline调度
│   └── infrastructure/           ← config env、机器/资产合同、runtime env 与 runner lease
├── scripts/
│   ├── data|services|baselines/  ← 数据导入、服务入口、原生 baseline runner
│   └── profiling|experiments|analysis|environment/ ← 画像、编排、分析、跨机器检查
├── tests/                        ← 按生产域镜像；测试数量随代码变化，以 unittest discovery 为准
├── configs/                      ← vendor baseline pin 与可复现实验配置
└── requirements.txt
```

目录职责与采用决定见[架构§8.1.2](../experiments/plans/系统架构.md#code-directory-boundaries)，各接点的接纳状态见[baseline交接边界表](src/baselines/README.md#text-execution-boundaries)。上图列现有根目录；records／链模型worker、Kalypso接线及`experiments/reproduction/native_baselines/`已随PR52进入main，[原45d184df实机来源](https://github.com/3444374/ai-operator-execution-optimization/tree/45d184dfaf0ef3a85ccccc805be7fc38e646b527)保持独立，源码接纳不扩大其验证范围。

`semantic_methods/lotus/`拥有原方法与SDK解释，`execution_provider/adapters/lotus_*core*.py`属于SemLoom适配，`baselines/text/frameworks/`保留原生系统执行owner。普通records在worker准备／执行，查询调用方收齐并重排完整回执后调用原LOTUS parser。Kalypso是独立作者系统；五份具名历史树含两种Core版本，是还原材料，不是native backend注册目录。

PR56已将纯结果模块归位到`semantic_methods/lotus/record_results.py`；同步模型进程与阶段清理分别归`serving/backends/vllm_sync_process.py`和`stage_process_cleanup.py`，旧import与同步进程CLI保留。逐阶段生成进程和链worker仍待分批迁移。三个旧请求桥保留入口；实验名、历史容器与还原CLI无需为改名再造副本。CSV／本地records目标无需PG，对应能力预检修复已随PR52接纳；旧SQL路径保留为显式可选接入，实际实现和验证范围仍从源码及报告读取。

跨机器运行不要直接对着 `requirements.txt` 反复试装。先按
`deploy/runtime/README.md` 选择 machine profile 与能力组，保存只读 preflight 报告；
可选能力分别维护在 `requirements/`，例如 learned estimator 与通用下载工具不会被
强制装入每个 driver 或 vLLM 环境。

图像 baseline 的来源合同由 `src/baselines/image/provenance.py` 统一维护。正式 native
baseline 当前为 Daft 内置 `embed_image` 和由 Ray Data 自己调度的官方
`read_sql → map_batches(CPU) → map_batches(GPU)` graph；项目自写的 Daft fused/staged
UDF 只保留为机制诊断 reference。所有 runner CSV/manifest 必须记录 scheduler owner、
upstream source、是否包含项目调度代码和 formal eligibility。

Daft 官方 803,580-row ResNet18 vendor-code baseline 固定在
`configs/image_vendor_baselines.json`。该轨道直接运行固定 commit 的官方 Daft/Ray Data
入口；允许的适配只有凭证/路径、物理 GPU 数、外部指标采集和结果审计，禁止加入项目
actor pool、credit、active window、routing 或 backpressure。

目录化后的完整测试发现必须指定 top-level，避免 `tests/experiments` 与
`src/experiments` 被 Python 当成同名顶级包：

```bash
python -m unittest discover -s code/tests -t code -p 'test_*.py'
```

## PostgreSQL data source backends

`code/src/data/sources/postgres_text.py` defines the data entry boundary used by
`code/scripts/profiling/postgres_ai_operator_profile.py`:

- `arrow_postgres`: baseline psycopg read plus Arrow table construction.
- `daft_postgres`: Daft `read_sql` PostgreSQL entry; it requires `sqlglot` and
  `connectorx`.

Data entry can use either the baseline psycopg path or Daft:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
  --dry-run --data-source daft_postgres --organizer daft `
  --output tmp\postgres_profile_dry_run.csv
```

Current writeback remains `none`, `json_text`, or `pgvector`. Lance is a future
optional sink backend and is not part of this implementation.

## Runtime code boundaries

The main profiling script should stay as orchestration code. Reusable behavior
now lives under `code/src/`:

- `data/sources/postgres_text.py`: PostgreSQL/Daft data entry.
- `data/materializers/text.py`: Arrow/Daft batch organization.
- `modalities/text/costs.py`: strict, shared output-cost modes/provenance and the
  completion actual-token-work extractor injected into the generic scheduler.
- `planning/work.py`: modality-neutral staged `WorkDescriptor` and atomic
  `RuntimeStateSnapshot`; preserves scalar credit compatibility while exposing
  prepare/model/result demand, locality, deadline/SLO, uncertainty and
  calibration identity.
- `planning/blocks.py` + `scheduling/runtime/stage_broker.py`: engine-neutral physical
  block identity and a real encoded/prepare/ready/model lifecycle. Ready bytes/work are
  reserved before CPU work starts, so concurrent completions cannot exceed declared caps.
- `planning/packing/scalar.py`: deterministic, modality-neutral classic BFD and a
  row-cap-first placement candidate sharing the same validation and ordering.
- `serving/backends/`: 公共 backend 合同、embedding backend 与
  completion/async/Ollama backend；包入口保持原公开 import 不变。
- `modalities/image/`: engine-independent image batch/result semantics, a lazy Daft
  PostgreSQL image source, CPU CLIP preprocessing, and a tensor-only GPU actor.
  The actor owns no hidden batching; organizer/scheduler remains the batching
  owner. Native framework baselines are deliberately separate under
  `baselines/image/frameworks/`.
  `modalities/image/metrics.py` only derives scale-aware unit-resource and
  streaming-onset fields from observed totals; it does not infer hidden engine
  queues or relabel text TTFT/ITL as image metrics.
  `staged.py` validates representation/signatures; `staged_execution.py` adds the optional
  static HSE path. CPU actors return descriptor and prepared tensor as separate Ray objects,
  so the driver observes only metadata while the tensor stays in the object store until a
  real ready lease is issued. Enable it explicitly with
  `--arm project_ray --project-execution-mode hse_static`; the default direct-dependency
  path remains the matched static control.
- `infrastructure/gpu_allocation.py` and `serving/vllm_resources.py`: explicit deployment GPU scope, UUID mask intersection and optional native vLLM DP configuration; [launcher and qualification](../experiments/results/diagnostics/main_service_auto_20261015/README.md) keep requested topology separate from live single-card proof.
- `infrastructure/runtime_env.py`: one shared contract for `PYTHONPATH` plus single-threaded
  OpenBLAS/MKL/OMP/NumExpr settings inherited by Ray workers and multi-job
  subprocesses. This prevents a 4-job run from multiplying 32 BLAS threads per
  worker before any model request is sent.
- `infrastructure/owned_processes.py`: birth-matched process observation, signalling and bounded
  exit waits. The records supervisor owns CLI orchestration and retains its existing helper imports;
  the shared serving process and cleanup modules import the neutral helpers directly.
- `infrastructure/request_budget.py`: process-local spending of prepaid request allowances;
  persistent ledgers and comparison setup remain under `experiments/`. Shared timing, buffered events,
  model-reuse and HTTPX observations belong to `observability/`, with legacy experiment imports retained.
- `serving/backends/vllm_sync_process.py` and `stage_process_cleanup.py`: synchronous private model
  IPC and registered stage-process cleanup. Existing baseline paths reexport the same objects;
  the old synchronous child CLI remains available alongside the new serving entry point.
- `infrastructure/config_env.py` and `infrastructure/environment.py`: strict
  `${ENV_VAR}` config expansion plus read-only machine/Python/model/dataset
  preflight. Missing values fail closed; installs/downloads remain explicit CLI
  actions.
- Image workers apply the same rule inside long-lived Ray/Daft processes:
  `experiments/run_image_clip_e2e.py` defaults Torch intra-op/inter-op pools to `1/1`,
  records them in schema v8, and the SemLoom Ray pool verifies the observed
  values before admitting work. Ray `num_cpus` remains an admission token, not
  an OS thread quota; actor count and per-actor thread count are separate
  experiment variables.
  The image runner passes the shared `ray_runtime_env()` contract at every
  `ray.init`, so workers receive repository imports and numeric thread limits
  without relying on an interactive shell's `PYTHONPATH`.
  Schema v8 also separates Ray cluster slots from external Daft source threads
  and includes both in the host physical-resource budget.
  `--source-cpu-threads` is independent from preprocess `--cpu-workers`, so
  capacity sweeps change one stage at a time.
  SemLoom execution additionally times source iterator waits, driver batch
  materialization, and Ray submission so the residual framework gap is not
  mislabeled as GPU or PCIe time.
- `baselines/`: vLLM Bench service ceiling、项目自写 bounded controls、Daft/Ray Data
  framework-native adapters、OceanBase product-native adapter、immutable manifest 和
  fail-closed 双 endpoint gate。`text/frameworks/` 只封装 vendor API graph，不注入 SemLoom
  credit/router；`provenance.py` 防止 control/ceiling 被误报为原生 baseline。
- `data/sinks/postgres.py`: PostgreSQL embedding/completion result normalization and
  batched SQL execution are separate from transaction ownership. SemLoom runners build a
  `PostgresWritePlan`, execute it, and commit explicitly; historical `write_*` functions
  remain compatibility wrappers with their original implicit-commit behavior.
- `observability/metrics/`: `timing.py`、`csv.py`、`statistics.py`、`resources.py`、
  `vllm.py`、`retrieval.py`、`squad.py` 分别负责计时、schema-safe CSV、重复统计、
  GPU/能耗/MFU、vLLM TTFT/ITL/cache 指标、显式真值检索质量和 SQuAD v1.1
  Exact Match/token-F1；
  包入口保持原公开 import 不变。Formal runs preflight the main
  output against dry-run keys before database or GPU work. Empty files receive
  a header; non-empty files reject appended rows whose ordered keys do not
  exactly match the existing header.
  `model_request_tokens_per_s` isolates the model-submission window,
  `operator_tokens_per_s` uses the operator wall, and legacy `tokens_per_s`
  remains the complete E2E rate; comparisons must not mix these time bases.
- `observability/profiling/traces.py`: versioned control/flush/submission/request/resource CSV
  serializers. Control schema 2 records the actual `hol_age_s` input;
  submission schema 3 uses scheduler lifecycle IDs and records pool,
  endpoint, GPU, status, and error instead of synthesizing batch IDs for
  request-granularity runs.
- `observability/profiling/cli.py`: the profiler command-line surface only; it does not start
  Ray, connect to PostgreSQL, or inspect the environment beyond argument
  defaults.
- `observability/profiling/config.py`: post-parse endpoint/metrics precedence and Ray worker
  resource resolution. Explicit CLI values win over plural/single environment
  defaults.
- `observability/profiling/schema.py`: the ordered formal summary-row contract and schema-drift
  guard, separate from runtime orchestration.
- `observability/profiling/replay.py`: offline/replayed Arrow envelope construction,
  token-budget row grouping, batch/request submission expansion, and request
  lifecycle seed assembly. It does not submit Ray work or call model services.
- `observability/profiling/ray.py`: Ray task/actor submitters, endpoint topology, typed
  profiling wiring, credit release/fan-in, and the explicitly retained legacy
  adaptive baselines. It does not parse CLI arguments or write trace CSVs.
- `data/workloads/text.py`: small built-in seed workloads for smoke/dev only.
- `scheduling/`: engine-independent typed core split by decision boundary:
  `core/`, `organization/`, `submission_control/`, `endpoint_routing/`, and `runtime/`.
  `core/control.py` owns the neutral capacity-arm contract;
  `core/scheduler.py` groups endpoint capacity, shared credit, ready-window limits, and
  Job policy values into self-validating configuration objects while keeping the legacy
  constructor compatible. `runtime/execution.py` is the reusable physical-execution
  facade consumed by profiling and future operator adapters;
  `core/execution.py` owns exactly-once pending/completion/lifecycle state and accepts a
  modality-specific actual-work extractor. `submission_control/saor.py` contains only the
  finite-action DPP policy and typed inputs; `ordered_release.py` is the single owner of
  per-Job ready queues, active work and monotonic `release_seq`. Neither module contains
  K128/K160, KV thresholds, endpoint counts or text/image branches: calibrated arms,
  predicted service and admissible actions are caller inputs.
  `submission_control/stage_work.py` is a bounded candidate that moves only one
  offline-calibrated work-credit step and falls back to the workload-specific
  static point on stale or mismatched state; it is not yet a performance claim.
  Duplicate root compatibility modules have been removed. The formal
  payload/execution path remains Daft -> Arrow -> Ray.

`fake` is retained only as a local control backend for offline smoke tests and
pipeline debugging. It is not a model-service result source. For vLLM-compatible
experiments, use `--model-backend compatible_http`; the older `http_openai`
name is accepted only as a compatibility alias. `AI_COMPLETE` is selected with
`--operator ai_complete`. Use `/v1/completions` for the original multi-prompt
mechanism path, or `/v1/chat/completions` with
`--completion-protocol chat_completions` for product/official-runtime
compatibility. `--completion-http-transport httpx_async` creates one persistent,
bounded async connection pool per Ray actor. In Completions mode one actor call
keeps the original multi-prompt HTTP body; in Chat mode a multi-row actor call
dispatches one independent Chat request per row with `asyncio.gather`. For local
Ollama smoke runs, use
`--model-backend ollama --completion-endpoint-url http://localhost:11434`.

`src/observability/profiling/` owns profiler implementation. The obsolete root
`src/profile_*.py` compatibility modules have been removed; production and test callers now
use the owning subpackage directly, and an AST architecture test prevents the old paths from returning.

## Shared execution implementation

The versioned adapters now depend on shared modules directly:

| Owner | Responsibility |
|---|---|
| [execution_provider/completion.py](src/execution_provider/completion.py) | Request/result values, adapter interface and redacted errors; no session loop |
| [execution_provider/adapters/model_config.py](src/execution_provider/adapters/model_config.py) | Fixed endpoint/model configuration and validation for both HTTP implementations |
| [execution_provider/multiplexed_gateway.py](src/execution_provider/multiplexed_gateway.py) | Shared owner loop and bounded connection proxies; public session operations own failure and consumer closure |
| [execution_provider/adapters/incremental_execution.py](src/execution_provider/adapters/incremental_execution.py) | Fixed-model assembly; existing policies, work descriptions and backend can be supplied independently of wire handling |
| [execution_provider/adapters/async_fixed_model.py](src/execution_provider/adapters/async_fixed_model.py) | Bounded HTTP I/O, model timeout and client cleanup |
| [execution_provider/adapters/ray_map_transport.py](src/execution_provider/adapters/ray_map_transport.py) | Daft payload batches, query-owned workers and optional caller-owned named workers with exclusive query claims |
| [execution_provider/wire/map_codec.py](src/execution_provider/wire/map_codec.py) | Common Map semantic validation and digest construction |
| [scheduling/core/policy_contracts.py](src/scheduling/core/policy_contracts.py) | Admission/routing/shared-credit interfaces used by both scheduling loops |

`incremental_session.py` contains the shared v6 protocol handler; obsolete standalone runtime and adapter classes have been removed.
`v5.py` preserves its public imports; v6 binds the common codec directly. The synchronous scheduler
also preserves its old type exports, while the incremental core imports their owner directly.
Existing synchronous runners and recording/Filter/reference protocols still have consumers.
Historical experiment records are retained. This changes code ownership, not supported SQL or scheduling policy.
[Cleanup verification](../experiments/results/scheduling/shared_modules_20260908/README.md) covers
import isolation, compatibility aliases, full scheduling/PG checks and a fresh 12-request model run.

Session owners use `fail(reason)`, `set_dispatch_enabled(enabled)` and `close_consumer(clean=...)`.
Call `close_consumer` only after the sender has stopped accessing all deliveries; ordinary `close()`
continues to retain unreleased leases. A remote request still needs an authoritative terminal to release capacity.
`SessionEngine(choose_flow=...)` and the fixed-model execution factory accept a Job/flow selector over
immutable ready identities and history. The default remains Job-first round robin; the Engine validates
selection and owns all capacity checks. Old standalone adapter imports are intentionally retired;
embedded users should use the same `MultiSessionMapGateway` with `max_jobs=1`.


## PostgreSQL Map through the incremental core

The `incremental-map` profile now adds the v6 task acknowledgement/completion port and a bounded
PG row window. It reuses the existing organizer, core and HTTP adapter. Plain input expressions
can have multiple model requests in flight; other expressions retain window one. See the
[configuration and checks](../experiments/results/postgresql/async_window_20260908/README.md)
for exact supported shapes, 1958 PG TAP checks, lifecycle checks and 12 real model requests.
Filter/composed v6 paths remain pending. Multiple registered Jobs and sessions share the external core.

The transitional `incremental-map-window-one` bridge has been retired. Use the same
`incremental-map` v6 profile for both one-row and larger windows; set gateway held tasks,
backend requests and PG `provider_window_tasks` to 1 for single-row operation. See the
[migration and checks](../experiments/results/scheduling/bridge_retirement_20260908/README.md).
Synchronous v5 Map and its semantic reference remain supported. Historical bridge evidence
continues to describe its original source version.

## Incremental execution context

`SessionEngine` and `SchedulingSession` in `scheduling/core/session.py` provide a controlled single-flow
implementation of `offer`, `advance`, `seal`, `cancel`, `release` and `close`. They reuse existing local
admission, routing and FIFO credit policies. Task choice is configurable over an immutable bounded
candidate window; resource ownership is managed by the engine. Result leases remain charged until
released, and uncertain remote work remains charged after session close.

This is where later data organization and submission strategies cooperate, rather than a parallel
submitter outside the execution layer. The current reference submits one task at a time. Multi-member
submissions and prepare-stage buffers need explicit member/result mapping and shared budgets before
integration. Existing staged block descriptors and brokers remain reusable inputs to that work.
The [design](../experiments/plans/增量执行.md#session) and
[verification](../experiments/results/scheduling/incremental_session_20260908/README.md) distinguish this tested core from pending real backend/PG integration.
`scheduling/runtime/async_backend.py` now supplies a bounded coroutine transport. It uses one async I/O
thread, retains uncertain outcomes and only releases a slot after poll transfers the terminal event.
[Real-model checks](../experiments/results/scheduling/incremental_real_20260908/README.md) pass five requests through SessionEngine, including two concurrent
HTTP operations, release backpressure and late-result cleanup across sessions. The diagnostic client
reuses the existing request builder and bounds response reads. Production PG bridging and multi-member
submission remain pending. The old synchronous runtime still serves existing callers with its original outputs.

The [semantic method module](src/semantic_methods/README.md) adds bounded sequential continuations
and predeclared per-task capabilities. It supports a method returning another request after a result,
while the existing session retains scheduling and lease ownership. The opt-in `MethodDriver` now
bounds multiple rows and retained payloads using fixed grants from a shared `MethodBudgetPool`.
Cascade algorithms, dynamic method budget allocation, production model capability routing, and
PG method integration remain pending. See the [V1 plan](../experiments/plans/增量执行.md#method-driver).

The opt-in `WorkWindowOrganizer` now connects the existing complete-task work slicing to accepted
session tasks. `TaskInfo` carries row/call/stage identity and `WorkDescriptor`; `BatchMember` preserves
organization membership through backend submission and delivery. The current backend expands a group
into independent one-member requests, each charged separately. A pending group retains only task keys
across backpressure. Unconfigured callers keep their existing single-task behavior. See
[the organized-window design](../experiments/plans/增量执行.md#session-organized-window).

## Scheduling foundation

`code/src/scheduling/` contains immutable request metadata, endpoint topology,
data-organization policies, request/work admission, shared multi-job credit,
endpoint routing, a deterministic policy-composition scheduler, and Ray runtime
adapters. Policy modules do not import Daft, Arrow, Ray, or HTTP; only runtime
adapters receive the active Ray module explicitly.

`PayloadEnvelope` is generic in its engine-owned payload type; the scheduling core never
uses `object` as a payload escape hatch. `SynchronousExecutionEngine` composes admission,
routing, submission and grouped capacity contracts. Profiling remains an observation and
experiment caller of that execution capability; it no longer constructs the scheduler
directly.

The first SAOR core revision selects among caller-enumerated safe actions using queue and
weighted-fairness debt. The executable `saor_release` path now runs inside the existing named
shared-credit coordinator: it keeps the calibrated request/work envelope fixed, recomputes
weighted dominant-share entitlement over the active Job set, grants only fitting Job heads,
and updates fairness debt from actual completion work. It is completion-driven and
non-preemptive, so vLLM still owns FCFS, continuous batching, chunked prefill, and KV
management. The runner records `actuated_saor_release`, per-Job debt, and an observed
bulk-borrow/foreground-reclaim/bulk-reborrow phase audit. SLO-weighted release is deliberately
rejected until per-Job SLO debt is connected; the current executable default uses
`slo_weight=0`.

The separate development policy `saor_bounded_priority` now implements the frozen
v0.5.1 lexicographic contract without changing `saor_release`: debt-critical fitting
heads receive one completion-corrected recovery lease; a debt-critical ready head that
does not fit can open a head-specific reclaim barrier; an SLO priority window is evaluated
next; all remaining opportunities use the original SAOR selector. Per-Job priority, SLO,
window, and debt cap are explicit configuration, never inferred from Job names or arrival
order. Scheduler timeout cancellation removes the waiter and closes any targeted hold.
The coordinator emits a monotonic, lossless release-event ledger; 250 ms snapshots remain
phase/resource diagnostics and are not mechanism truth. This path is locally verified and
development-only: no new GPU rehearsal or formal performance claim exists yet.

`saor_bounded_ready` is the observation-contract revision and deliberately has a
different policy name. The old bounded-priority path remains a single-head regression
baseline. For the new path, each Job pre-registers only concrete, already-arrived
requests inside a finite window derived from its existing effective K and the sum of
per-endpoint shared W; there is no extra tuned queue-size knob and no unbounded payload
prefetch. The coordinator therefore sees a bounded ready set while vLLM still receives
ordinary complete requests and retains FCFS/continuous-batching ownership. Submission
trace schema 6 separates `ready`, credit `registered`, credit `granted`, `submit`, service,
and completion time. Release-event schema 2 records actor-side registration and grant
events with request IDs and epoch timestamps. The group runner uses submission traces to
prove the concrete-ready lifecycle, then pairs foreground register-to-grant events inside
the coordinator's single clock domain and fails closed on foreign fallback;
`max_ready_requests_seen`/`max_ready_work_seen` audit the actual
window. Local tests cover multi-candidate visibility, exactly-once ordering, finite-work
validation, timeout cancellation, config routing, and fail-closed gate profiles. Two independent
GPU development rehearsals now make the 0.125K guard a formal-registration candidate; the
0.25K guard failed its bulk-SLO bound in both rounds. This is not a completed formal comparison
or a fairness theorem.

The pure ordered-release fast path publishes validated Job-head requests with a monotonic
sequence and releases capacity on completion. Release work and predicted epoch service are separate fields, so a
long request is not silently treated as service completed within the current control slot.
The convenience action builder requires every service/goodput/tail/energy/switch prediction;
release-action values are explicit marginal deltas against the zero-delta hold reference.
GPU utilization, `waiting`, KV thresholds and phase labels remain observation/experiment
inputs outside the policy; no single metric is hard-coded into the action selector.
`scheduling/runtime/saor_capacity.py` is a capacity-only development adapter wired to the
existing shared-vLLM named-credit actor. It learns only the current safe arm, evaluates
adjacent configured arms, and fails back to a configured frozen point on stale/signature/
invalid observations. This does not yet wire ordered release or fairness debt into the
formal runtime. This capacity-only adapter remains `not-promoted` and separate from
fixed-envelope `saor_release`; it must not be described as full SAOR.

`scheduling/runtime/saor_pipeline.py` is an engine-neutral two-stage differential-
backpressure core for image/heterogeneous pipelines. It changes only bounded flow limits
for prestarted worker pools; it does not create Ray actors. The current image runner still
submits unresolved CPU futures directly to GPU actors, so the controller remains unwired
until an explicit pending-prepare → ready-tensor → pending-model broker exposes true stage
queues. Its tests prove selection/fallback invariants only, not image performance.

The package facade now resolves compatibility exports lazily, so importing SAOR does not
load AIMD/PID/UCB or Ray adapters. This removes an import-time dependency, but it does not
yet make the repository SAOR-only: the profiler, shared-vLLM runner, image multi-Job runner,
and observability wiring still import legacy baselines directly. Those callers must migrate
to a tested SAOR runtime profile before legacy files can be archived or removed.

Endpoint state separates service health (`healthy`) from request-specific
admission capacity (`available`). Routers use `schedulable_endpoints()` for
selection, while `healthy_endpoints()` remains a pure health query. Temporary
request/work-credit exhaustion raises typed backpressure so the scheduler
collects a completion and retries; a manifest-pinned request keeps both its
endpoint and that endpoint's pool.
Arrival replay is produced through a one-element bounded source queue so waiting for
the next source arrival cannot block Ray completion collection. The scheduler
keeps submit/routing/credit/lifecycle state on its main thread: it prioritizes
an already-ready arrival, polls `ray.wait(timeout=0)` during arrival gaps, and
uses blocking collection only when admission or active-work capacity is full.
This preserves offered-load saturation while releasing request-level credit
and recording completion timestamps promptly under sparse or bursty replay.
Only `saor_bounded_ready` adds a second, independently bounded scheduler window in
front of shared credit; its count/work limits are inherited from the frozen K/W contract.

The shared-vLLM experiment runner can pin a distinct immutable request manifest
and source offset for every job. This is required for staggered short/long or
otherwise heterogeneous-job evidence; reusing the same rows for all jobs only
validates concurrency semantics and cannot support a work-aware fairness claim.
`shared_fifo` in that runner is a project-owned global ready-enqueue FIFO under the same
shared envelope; it is not the no-project control. The shared-vLLM runner now treats
`direct_no_job` as an in-process arm: it merges timed immutable Job arrivals and applies only
one endpoint-local HTTP concurrency bound before unmodified vLLM FCFS, while reusing the same
interleaving, service counters, resource samples and idle gates. It is a formal control (not a
vendor-native baseline), explicitly records that no work-credit envelope was applied, and
uses its own matched-solo normalization. The standalone `run-jobs-control` CLI remains useful
for diagnostics but is no longer the formal matrix boundary. Workload lifecycle and credit
borrow/reclaim/reborrow are separate gates; static/direct have no applicable credit mechanism.
For a causal single-short control, a static scenario may declare
`static_partition_count` larger than its active `job_count`. The unused fixed
partition stays reserved, so `job_count=1, static_partition_count=2` reproduces
the K/work quota available to one job in a two-job static partition without
launching a synthetic competing job. Other policies reject this field.
The lifecycle module joins complete-row replay seeds, immutable submission
events, backend service timestamps, and explicitly sourced token counts into
exactly-once request trace rows.

The historical PostgreSQL-source profiler configuration used:

```text
PostgreSQL -> Daft -> Arrow payload boundary -> Ray task/actor -> endpoint
```

The synchronous fake adapter used in unit tests is not a formal execution path.
The profiler's static `ray_task` and `ray_actor` paths now delegate to the
typed scheduler through `RaySubmissionAdapter`. The Daft-to-Ray contract tests
use a real `DaftOrganizer`, Arrow batches, and local single-node Ray task and
actor execution:

A model-service endpoint is not a Ray actor worker. One endpoint is an
independently addressed HTTP service; `--actor-workers-per-endpoint` creates
that many Ray HTTP client actors for each endpoint. The configured upper bound
on simultaneous actor method execution is:

```text
effective actor concurrency =
  endpoint_count * actor_workers_per_endpoint * ray_actor_max_concurrency
```

These HTTP client workers reserve CPU but explicitly reserve `0` Ray GPUs,
because the external model-service endpoint owns the GPU. Ray task retries,
actor restarts, and actor task retries remain disabled for formal completion
accounting. The profiler CSV records the Ray version, resolved worker/resource
settings, service endpoint count, actor worker count, and per-worker submission
counts. Python executor rows leave `ray_version` empty and use the explicit
non-applicable sentinels `ray_actor_max_concurrency=0` and
`ray_worker_num_cpus=0.0`. Ray task rows record their effective
CPU reservation but use `ray_actor_max_concurrency=0`, because an actor-only
constraint is not applicable to a task. Internally the task worker options
still use their safe task definition. Fake Ray task/actor definitions receive
the same CPU, zero-GPU, and retry/restart options as other Ray workers; this
does not make the fake backend an HTTP or performance path.
Endpoint-local worker rotation and the legacy endpoint round-robin submitter
are created once per run, so fetch chunks do not reset either position.
Per-worker metrics remain chunk-local deltas for correct run-level merging.
Jobs created before Ray initialization or submission failures are marked
`failed` without masking the original exception.

The bounded actor submitter owns explicit per-worker slots and supports stable
round-robin or least-active-work assignment. A canonical Ray handle releases
the selected worker exactly once after either success or failure. Summary rows
record configured/effective slots, per-worker peaks/failures and slot-held
utilization; submission trace schema 4 records planning-batch, service-quantum,
worker identity, credit-held and Ray-to-service timing. Slot-held utilization
includes queue, HTTP and service time and must not be interpreted as GPU
compute utilization.

`submission_granularity=service_quantum` uses the same complete-row slicer in
offline and arrival-replay paths. It preserves planning-batch identity while
allowing smaller HTTP/Ray completions to release active-work credit
independently; an oversized row remains intact and is explicitly marked.

```powershell
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_models.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_policies.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduler.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_ray_adapter.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_adaptive_admission.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_dynamic_admission.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_flush_policies.py
.conda\pg-ai-profile\python.exe code\tests\observability\test_postgres_profile_scheduling.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_scheduling_daft_ray_contract.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_token_budget_controller.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_shared_credit.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_shared_credit_ray.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_execution_ledger.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_saor.py
.conda\pg-ai-profile\python.exe code\tests\scheduling\test_ordered_release.py
```

Typed AIMD, optional EWMA-AIMD, and PID controllers can now drive the same Ray
task/actor scheduler. Service metrics are sampled by a background provider, so
network scrapes never run on the admission-decision path. Stale samples do not
update controller state; every decision can be written to a control-trace CSV
with the exact sample age. The legacy `queue_adaptive` branch remains an
explicit baseline. UCB has a tested finite-action policy and SLO-aware reward,
but is not exposed in the profiler until epoch-level request metrics are
available. None of these code tests is evidence of a throughput or latency
improvement.

Static token budget remains the default. The optional `service_quantum`
controller chooses only from an offline-calibrated action set and changes by at
most one step per closed batch using arrival/service-rate feedback. Endpoint
admission can additionally bound estimated prompt+output active work, so
different batch sizes and request lengths do not consume identical credits.
Least-work routing uses predicted outstanding work rather than request count.
For concurrent database jobs, an optional named Ray actor owns shared
per-endpoint request/work credits and applies deficit-round-robin fairness with
work-conserving borrowing. These mechanisms are implemented and unit-tested but
remain GPU-unvalidated until the staged dual-GPU matrices complete.

Data organization exposes `token_budget` (sequential),
`best_fit_token_budget` (classic BFD), and
`row_cap_aware_token_budget` (decreasing order with row-cap-first placement).
All three enforce the same token and row constraints, and Arrow/Daft call the
same pure packing functions. Sequential remains the default until repeated
GPU experiments show that another mechanism generalizes.

Pool and endpoint routing support request-cost pools, least-queued selection,
deterministic prefix affinity, health fallback, and explicit per-endpoint pool
and GPU identifiers. The independent flush policy core supports immediate,
fixed-timeout, queue-adaptive, and SLO-aware EWMA decisions with a hard maximum
wait. `slo_ewma` smooths arrival and per-endpoint service rates, floors the
service denominator with a capacity value calibrated by the saturated-work
curve, interpolates the wait window around aggregate load ratio 1.0, and
enforces oldest-request SLO slack plus hysteresis. Missing/stale feedback falls
back to the configured maximum fixed window.
Least-queued routing includes scheduler-local in-flight submissions instead of
reusing the profiler's static startup snapshot. Multi-endpoint vLLM runs should
pass every Prometheus endpoint through `--model-metrics-urls`; counters and
request gauges are aggregated across endpoints, while KV-cache pressure uses
the maximum endpoint value. GPU snapshots aggregate all visible devices.
`--source-order arrival_time` only defines source order; it does not reproduce
arrival gaps. Online flush experiments must additionally enable
`--arrival-replay`. The replay path normalizes the first arrival to zero,
preserves subsequent gaps with a monotonic clock, builds complete-row pending
batches, and writes a separate flush trace. Batching decides which rows belong
together, flush decides when a partial batch closes, and admission limits how
many submissions may be in flight. The default
`--submission-granularity batch` preserves the historical multi-row HTTP call.
`--submission-granularity request` keeps the same organization and flush
boundaries but expands each closed group into complete one-row HTTP requests,
so every completed request immediately releases one admission slot and the
scheduler can continuously replenish vLLM. In request mode, `max_inflight`
counts requests rather than multi-row batches; results from the two modes must
not be compared as if the same numeric K represented the same offered load.

Optional request tracing records one client-observed row per prompt with
explicit request- or submission-granularity completion timing and SLO metrics.
Epoch timestamps are
derived from a single wall-clock anchor plus monotonic elapsed time. Aggregate
endpoint token usage is never divided across prompts. For vLLM, the explicit
`--completion-return-token-ids` opt-in records genuine per-choice output-token
counts and finish reasons; generic compatible endpoints keep the extension
disabled and those fields remain unavailable.
Backend service epochs are marked as a separate clock domain; cross-domain
submit-to-service time remains empty when the clocks cannot be ordered.
Request trace schema 3 retains the explicit `submission_id` join key and adds
`finish_reason`; consumers do not reconstruct identity from row position.

Typed adaptive controllers consume each asynchronous vLLM metrics sample at
most once. A still-recent cached sample remains visible for diagnostics but is
marked non-fresh after its first decision, preventing AIMD/EWMA/PID from
reapplying one scrape at scheduler-loop speed. The HOL-age controller uses the
configured sample interval as its control clock; its current signal is still
the age of the oldest in-flight submission and should not be described as pure
pre-submit queueing delay.

`code/src/experiments/scenarios/core.py` and
`code/scripts/experiments/run_ai_operator_scenarios.py` provide deterministic formal-run
interleaving, per-run service-idle gates, failure incidents, redacted commands,
and an atomically updated manifest. Scenario argument strings may reference
explicit `${ENV_NAME}` values; an unset variable fails before health checks or
profiler subprocesses, so model, endpoint, database, and dataset-facing config
can change without editing Python. Each profiler subprocess uses explicit
phase/repeat identity and writes isolated request/submission/time-series files.

The real contract test covers Daft `RecordBatch`/Arrow conversion and local Ray
task/actor exactly-once execution. It is integration evidence only. GPU
performance claims require the real PostgreSQL source and vLLM endpoint.

## 命令与历史实现

工作负载导入、组织 smoke、PG 画像和 baseline 运行从[脚本入口](scripts/README.md)选择，不在本页重复命令。
已停止的 SAOR 实现、旧运行配方及其来源见[精简前代码说明](https://github.com/3444374/ai-operator-execution-optimization/blob/0b8aca5ff599e1b6525b77608e3865b20a4fd996/code/README.md#stopped-saor-native-system-implementation-record)。

<a id="代码组织的读法"></a>

## 代码组织的读法

代码按执行职责和输入模态组织。数据库载体拥有语义与查询生命周期，provider负责传输，
数据源与物化负责准备输入，组织与调度使用工作量描述，外部服务执行模型，观测模块记录过程。
文本token与图像frame、像素、字节的换算放在模态适配中；公共调度消费明确的工作量。

阅读当前路径先核对源码与实现状态，再沿任务生成、提交、外部执行、返回和写回定位耗时。
SQL、Ray任务、模型服务与结果消费具有各自的生命周期，不能把一个阶段的完成当成完整查询完成。
原生候选由被测执行引擎拥有执行与调度；项目内部策略对照从原始身份和来源记录解释。
