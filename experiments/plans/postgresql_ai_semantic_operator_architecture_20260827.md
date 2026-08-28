# SemLoom PostgreSQL 内置 AI 语义算子整体架构与实施计划

更新日期：2026-08-28
状态：`current / architecture-defined / implementation-in-progress`
当前实现事实：`code/postgres/semloom_pg/` 已有 PostgreSQL planner-visible `SemMap`
`CustomPath/CustomScan` capability spike；`REL_18_3` PGXS regression 与 preload/prepared-plan/
snapshot/cancel TAP 已通过。统一 execution-provider 协议、`INSERT ... SELECT` 和 Sema/LOTUS
兼容适配器仍待完成。既有 PostgreSQL source/sink、
Daft/Arrow、Ray、vLLM/CLIP、调度与观测继续作为外部物理执行基座。
当前排期边界：锁定 PostgreSQL `REL_18_3`，完成 exact `SemMap`/`SemFilter`、一个普通关系 child
plan、query-scoped provider session，以及一条最小、显式可识别的 `SemFilter` 第二 physical path。
既有 PostgreSQL 18.4 部署与结果只作 compatibility/rehearsal 证据，不替代 `REL_18_3` 资格验证。
数据库资格完成后优先比较 IMLane-like batch placement。`SemJoin`、aggregate/top-k/group-by、
fusion/AQE、Kalypso-like lineage 与跨算子 prefix lease 仅作后续参考，不构成当前实现承诺。
替代关系：本文取代 2026-08-21 的 PostgreSQL+LOTUS 主计划与 LOTUS 语义前端子计划；旧文档进入
`archive/`，只保留历史决策和源码审计价值。

## 1. 架构决策

项目后续采用以下主路径：

> **先用 PostgreSQL extension `CustomPath/CustomScan` 证明语义算子的 child-plan、query lifecycle 与受限
> semantic alternatives；只有 LOTUS/Cortex 类目标优化或稳定 plan identity 被 extension 的可复现限制
> 阻挡时，才升级为受控的 PostgreSQL 18.3 core patch。无论载体如何，数据库都把已绑定、已选择
> semantic algorithm 且不可由外部改写的任务交给 SemLoom execution provider。**

这里的“内置”表示 PostgreSQL 拥有 SQL 表达、普通关系 child plan、snapshot、权限、语义算子计划、
结果解释以及 cancel/error/result lifecycle。模型、Ray、vLLM 和 Python runtime 仍在数据库进程外；
输入 payload 会通过数据库管理的有界通道传出，因此不能写成“数据物理上不离开数据库”。

Sema 是数据库原生化的主要参照，不是需要逐模块复制的实现。首个 capability spike 不修改 PostgreSQL
core；之后用 marker identity、prepared-plan invalidation、hook coexistence、filter–join placement、semantic
alternative costing 审查当前载体。future join/aggregate 只记录公开 hook 形状，不参与当前决定。若
extension 能安全表达当前目标，就继续使用；否则才维护最小 core patch。即使改内核也继续使用
function-like SQL marker，不同时修改 `gram.y`，不把 storage、Ray 或 vLLM 纳入 fork。

## 2. 参照系统的角色

| 对象 | 本项目中的角色 | 不承担什么 |
|---|---|---|
| Sema | 主要架构参照：semantic operator 是显式 plan node，数据库拥有优化和执行状态 | 不作为 PostgreSQL 代码依赖；论文中的 DuckDB 实现不能证明 PG extension 已可行 |
| Cortex AISQL | function-like SQL、数据库内 AI-aware plan/cost/rewrite 与外部 Cortex Platform 分工的工程参照 | 不公开 Snowflake core 源码，不能直接迁移为 PostgreSQL patch |
| PostgreSQL Custom Scan / planner hooks | 首个 capability spike；验证 ordinary child、tuple pump、projection、取消和错误 | 不是最终原生 node identity，也不证明任意 semantic rewrite 可由 extension 完成 |
| PostgreSQL 18.3 core semantic module | 条件性载体；当 extension 无法可靠支持原生 identity 或目标 optimizer rewrite 时使用 | 不修改 raw grammar、storage、Ray 或 vLLM；没有复现阻断前不扩 patch surface |
| LOTUS | 语义算子相关工作、可选 `lotus_compat` 行为 profile、未修改完整系统 baseline | 不定义核心 IR，不要求 `lotus-ai==1.2.4` 才能运行数据库算子 |
| IMLane | DBEnd/数据转换、数据库物理 batch pump、异步提交和 Lane/resource scheduler 的直接 baseline | 不拥有 semantic SQL/rewrite；其已有组批与提交机制不能重新包装成项目新颖性 |
| Kalypso | stage/dependency/prefix 生命周期、KV-aware admission 和 virtual pinning 的直接参照 | 不是 DB bridge；不能把 CP child 生成、物化和整条 query output 所有权照搬到外部层 |
| SemLoom 现有物理执行代码 | 外部 execution provider 的物理执行和调度 implementation | 不解析 SQL，不决定 `SemFilter` 真值，不修改 prompt 或 output parser |
| Daft / Ray / vLLM / typed CLIP actor | provider 后方的数据执行、分布式运行和模型服务设施 | 不拥有 PostgreSQL snapshot、ACL 或 query lifecycle |

这个调整保留课题原来的研究目标：研究语义算子产生的外部 AI work 如何组织、提交、路由和在多 Job
间调度；改变的是数据库语义层的中心依赖和实施顺序。

## 3. 目标架构

```text
用户 SQL
  |
  v
PostgreSQL 18.3 semantic module
  |-- carrier E: marker + CustomPath/CustomScan
  |   or conditional carrier K: SemanticExpr/path-generation
  |       `-- native Unary/State only if executor lifecycle is also blocked
  |-- function-like SQL marker + companion extension binder/catalog
  |-- explicit reference/LOTUS-like/Cortex-like alternatives
  |-- unary semantic executor + ordinary relational child
  |-- tuple pump + task compiler + result parser
  |-- snapshot / ACL / cancel / error / result lifecycle
  |
  |  AiProviderPort.open / drive / close
  v
SemLoom execution-provider gateway
  |-- recording adapter             # deterministic protocol/lifecycle test
  |-- direct HTTP adapter           # fixed endpoint capability
  `-- SemLoom scheduling adapter    # proposed external physical execution
        |-- WorkDescriptor / cost estimator
        |-- sealed-task work-unit organization
        |-- admission / continuous replenishment
        |-- multi-Job scheduling / endpoint routing
        |-- future conditional candidate: stage dependency / prefix lease / KV-aware admission
        |-- Ray
        `-- vLLM or typed CLIP actor
  |
  |  CompletionBatch
  v
PostgreSQL semantic physical operator
  |-- validate identity and plan digest
  |-- parse typed result
  |-- SemMap: append output column
  |-- SemFilter: keep/drop tuple
  |-- restore required order
  `-- emit tuple to downstream SQL operator or INSERT sink
```

一句话：**PostgreSQL 拥有 Sema/Cortex-like semantic plan 与 SQL-visible result，IMLane-like seam 把
sealed work 交给 SemLoom，并保留多 Job 与多 endpoint 这一研究主体；Kalypso-like dependency/KV
admission 只在后续证据证明有增量时另行评估。**

## 4. 数据库、provider 与模型服务的所有权

| 责任 | 所有者 |
|---|---|
| SQL surface、参数绑定、类型、输出 schema、NULL/error/order policy | PostgreSQL selected semantic carrier + companion extension |
| ordinary child plan、filter/projection、snapshot、ACL、transaction | PostgreSQL |
| semantic operator kind、prompt program、result parser、quality/reference policy | PostgreSQL semantic optimizer/executor |
| task identity、query deadline、cancel、可见结果与资源清理 | PostgreSQL semantic executor |
| row/task mapping、reorder/drain、是否产生 filter/join child | PostgreSQL semantic executor |
| sealed task 的物理分组、提交时机、inflight work、Job share、endpoint route | SemLoom execution provider；具体 batch placement 由 matched prototype 决定 |
| stage/dependency metadata、prefix lease、cache-domain stickiness | 后续参考：若另行立项，PostgreSQL 产生 lineage event，SemLoom 维护物理资源状态；当前排期不实现 |
| Ray task/actor 的执行与回收 | 对应 provider implementation |
| continuous batching、KV cache、token generation | vLLM native scheduler |
| LOTUS prompt/output compatibility | 可选 `lotus_compat` profile；不覆盖核心默认语义 |
| LOTUS/Sema/IMLane/Daft/Ray 的可运行原生完整路径 | 各系统自己；条件匹配时作 full-system baseline；Kalypso 当前仅作论文参照 |

### 4.1 provider 明确不能做什么

provider 不接收原始 SQL 或 PostgreSQL Plan，也不能：

- 重新连接 PostgreSQL 拉取表或额外列；
- 改写 instruction、canonical messages、generation constraints 或 parser；
- 把多条独立任务融合成一个改变输出对应关系的 semantic prompt；
- 决定 `SemFilter` 的三值逻辑或 `SemExtract` 的输出列；
- 将失败任务静默改为 NULL、重试或成功；
- 在 query cancel 后继续产生数据库可见结果。

provider 可以改变任务的物理执行顺序、work-unit membership、并发、提交时机和 endpoint，只要结果
身份、语义 payload digest 和完成状态保持可验证。Kalypso-like 元数据也不授予 provider 生成关系 tuple
或决定 cardinality 的权限：PostgreSQL 验证 parent completion 后才注册 child，或显式报告不再有 child。

## 5. 语义算子模型

### 5.1 逻辑算子注册表

| Operator kind | 输入与输出 | 首版状态 |
|---|---|---|
| `SEM_MAP` | 每个输入 tuple 生成一个 typed value，并附加为新列 | 当前唯一必做算子 |
| `SEM_FILTER` | 每个输入 tuple 生成 boolean/unknown，数据库决定是否保留 | 第二个算子，用来证明算子会改变关系 cardinality |
| `SEM_EXTRACT` | 从 tuple 生成声明式结构化字段 | 参考方向；不纳入当前排期 |
| `SEM_JOIN` | 对候选 tuple pair 执行语义谓词 | 参考方向；需要独立 binary module、候选生成和 cardinality 设计 |
| `SEM_ORDER_BY` | 根据语义比较关系确定顺序 | 参考方向；不纳入当前排期 |
| `SEM_AGG` | 对一组 tuple 产生聚合结果 | 参考方向；需要独立 blocking module |

Sema 将逐 tuple 生成新列的算子称为 `SemProj`；本项目首版使用 `SEM_MAP`，是为了与现有文本 workload
和 LOTUS `sem_map` 术语保持可辨认的映射。两者只在本文规定的 row-preserving projection 子集上对应，
不能据此把 Sema 或 LOTUS 的全部 operator semantics 视为相同。

首版 SQL 使用 extension 可注册的 marker function，不修改 PostgreSQL grammar：

```sql
SELECT
    doc_id,
    ai_semantic.map(
        prompt,
        instruction => 'Complete the task described by the input.',
        options => '{"temperature":0,"max_tokens":256}'::jsonb
    ) AS completion
FROM documents
WHERE workload_name = 'matched_two_job';
```

marker function 不是 HTTP UDF。planner 必须把受支持表达式降低成项目 semantic plan node；若表达式
残留为普通 scalar function，执行时立即报错，不允许静默逐行调用远端服务。`ai.complete` 等历史名称
以后只能作为显式兼容 alias，不能成为另一套语义实现。

### 5.2 数据库内 `SemanticPlanSpec`

数据库内 canonical plan 至少包含：

```text
SemanticPlanSpec
  schema_version
  operator_kind / operator_instance_id
  bound_input_exprs / output_type
  normalized_instruction
  prompt_program + prompt_program_sha256
  result_parser + parser_sha256
  null_policy / error_policy / order_policy
  physical_algorithm_kind / physical_role
  model_requirements / generation_constraints
  semantic_spec_digest
  physical_algorithm_digest
```

plan 不包含 Pandas、LOTUS AST、Daft、Ray 或 vLLM 对象。首版 `SemMap` 不预留 quality、cascade、
fusion 或任意 optimization flags；出现第二条真实 physical path 时再增加被 planner 消费的字段。每个
字段要么被实现消费，要么因不支持而在计划阶段报错，不能接受后静默丢弃。

第二条 physical path 出现时，plan schema 才增加显式 `quality_policy = EXACT | APPROX(...)`、
`reference_algorithm_id`、quality metric/target/confidence、evidence key/epoch 与 reference fallback policy。
`EXACT` 只允许 reference algorithm；`APPROX` 必须由 query/profile 显式选择，不能由 cost optimizer
自动把精确语义降级为近似语义。

### 5.3 `PreparedSemanticTask`

数据库从 child tuple 编译出 provider-neutral task：

```text
PreparedSemanticTask
  protocol_version
  query_id / operator_instance_id / task_id
  physical_role
  canonical_messages or typed model input
  semantic_payload_sha256
  expected_raw_output_kind
  generation_constraints
  work_hint + calibration_signature
  locality_key
  deadline
```

`result_parser` 不交给 provider 执行。provider 返回 raw completion 和模型 usage；数据库按 plan 中的
parser 产生 boolean、text 或结构化值。row/pair/group 到 task 的映射只保存在 executor state，provider
只看 query-scoped opaque `task_id`；这样 future fused task、cascade 和 one-to-many task 不会被“一行等于
一次调用”锁死。`work_hint` 是调度提示，不定义 SQL 语义。

单算子未来若出现 cascade/join，parent-stage 映射仍应先保存在 PostgreSQL executor state，provider v1
只接收当前已经可执行的 sealed tasks。仅当 SemLoom 实测需要感知算子内部阶段或跨算子 dependency，
且另行立项后，才评估是否增加 `pipeline_id/stage_id`、`parent_task_id/prefix_lease_id`、reusable-prefix
digest 与 lineage terminal event。当前 v1 和当前排期不包含这些 query-graph 字段。

### 5.4 `CompletionRecord`

```text
CompletionRecord
  query_id / operator_instance_id / task_id
  semantic_payload_sha256 / semantic_spec_digest / physical_algorithm_digest
  terminal_state = completed | failed | cancelled
  raw_output or typed transport error
  prompt_tokens / output_tokens / finish_reason
  provider_evidence_digest
```

每个 task 只能出现一个数据库接受的终态。executor 用本地 task binding 恢复 tuple identity 和顺序。
exactly-once 只描述数据库结果行和 task terminal state；
外部模型调用不可回滚，不能写成 exactly-once inference。

## 6. 唯一外部 seam：`AiProviderPort`

PostgreSQL 内部的 `SemanticExecPump` 对 executor caller 只暴露 `begin/next/stop/explain`；它隐藏 child
slot lifetime、task binding、bounded prefetch、reorder、latch wait 和 error cleanup。一个
`SemQueryRuntime` 在同一 query 的 semantic nodes 间共享 query-scoped provider session；首版只有一个
operator，因而 pump 与 session 是一对一。其 implementation 通过异步、有界的外部 port 连接 owned gateway：

```c
AiProviderSession *open(AiProvider *, const AiSessionSpec *, AiProviderError *);
AiDriveStatus drive(AiProviderSession *,
                    const AiDriveRequest *,
                    AiDriveResult *,
                    AiProviderError *);
void close(AiProviderSession **,
           AiCloseDisposition,
           AiCloseReport *);  /* NULL-safe, bounded, non-throwing */
```

`drive` 在一个 entry point 中同时推进 submit、completion drain、end-of-input 和 backpressure：

```text
AiDriveRequest
  tasks[]                    # caller-owned immutable slice
  end_of_input               # 只能与空 tasks 同时出现
  max_completions
  wait_until

AiDriveResult
  accepted_prefix            # 未接受后缀仍由 PostgreSQL 持有
  completions[]
  accepts_more
  drained
  available_tasks / bytes / work
```

interface 的完整合同包括：

- session 只对应一个 query 和 immutable operator/spec digest set；当前协议 v1 的 set 恰有一个 operator；
- provider 只能接受 task slice 的连续前缀；`accepted_prefix=0` 是正常 backpressure，不是错误；
- tasks、serialized bytes、estimated work 与 PostgreSQL reorder buffer 都有界；任一达到上限便停止拉 child；
- provider 可乱序完成，但每个 accepted task 最多交付一个 terminal completion；数据库按本地 binding
  恢复 tuple 与顺序；
- v1 第一次空 `end_of_input=true` 封闭唯一 operator 的输入，之后只允许用空 drive 排空 completions；
  future lineage protocol 若另行立项，再评估 per-operator seal events；
- close disposition 区分 `DRAINED`、`EARLY_STOP`、`QUERY_CANCEL` 与 `QUERY_ERROR`；cancel 的可靠语义是
  停止 admission、禁止迟到结果进入 SQL，并 best-effort 请求外部中止，不承诺 vLLM 已停止计算；
- disconnect、protocol drift、digest mismatch、unknown task 和缺失终态均 fail closed；首版不自动重连或 retry；
- `close` 只返回脱敏计数与 digest，不返回或持久化原始 prompt/output。

该 module 具有真实 seam：recording、direct HTTP 和 SemLoom 是三个 adapters。production 先用 Unix-domain
socket、4-byte big-endian length 加 UTF-8 JSON frame；C/Python golden vectors 固定整数时间、SHA-256、
Unicode、拆包/粘包和未知字段行为。在 profile 证明 serialization/data copy 是主要成本前，不增加共享
内存、Arrow Flight 或自定义零拷贝；IMLane 的 ArrowLane 路径保留为后续可测替代 adapter。

## 7. 四种 grouping/batching 必须分开

### 7.1 semantic batching / fusion

将多条 tuple 写入一个联合 prompt、融合两个 semantic operator、改变调用数或 parser 的优化，会改变
semantic execution path，甚至改变结果质量。它属于 PostgreSQL semantic optimizer/executor，必须：

- 生成新的显式 plan/task identity；
- 记录 tuple membership、prompt program 和 parser；
- 与 reference path 或人工 ground truth 比较质量；
- 在 local vLLM 与 remote API 下分别校准 latency/cost/quality；
- 首版默认关闭。

### 7.2 database execution-batch formation

PostgreSQL operator 必须拥有 child pull、row/task mapping、pending/reorder/drain 与有界 overfetch；但它
是否还应决定 sealed tasks 的 execution-batch membership，目前不先锁死。需要实现两个 matched profiles：

1. `db_batch_preserved`：数据库按 row/token 上限关闭 execution batch，gateway 保持该 batch；这是
   IMLane-like baseline；
2. `provider_rebatch`：数据库按 task stream 调用 `drive`，SemLoom 可在接受后的 sealed tasks 中按
   work/locality 重新组批。

两者使用相同 `SemanticPlanSpec`、task digest set、模型、endpoint capacity 和 output parser。只有 matched
实验能决定何种 placement 更轻量；论文中机制位于哪一层不能替代该证据。

### 7.3 SemLoom physical work organization

项目现有 token/work/prefix organization 只把多个**已经独立编译完成**的 tasks 组成 `WorkUnit`，
不修改 canonical messages、调用数、parser 或一行一结果关系。它属于 SemLoom provider。

因此：

```text
数据库 semantic batching/fusion
  = 允许改变语义调用结构，必须显式进入计划和质量验证

SemLoom work-aware organization
  = 只改变独立任务的物理提交结构，不改变语义
```

### 7.4 vLLM continuous batching 与 Kalypso-like dependency admission 参考

vLLM continuous batching 继续拥有 iteration-level GPU request scheduling、KV block allocation 和 decode
推进；SemLoom 不修改它。Kalypso 的 pipeline/stage/task 不是 row batch，其 task dependency、prefix
residency 和 stage-aware admission 只作条件性参考。若后续真实 workload 证明 SemLoom 必须感知这些
状态，再另行设计 lineage/lease 合同和实验；当前排期不实现。即使未来采用，数据库仍决定 parent 是否
产生 child，外部层也不拥有关系物化或 Cartesian Product pair 生成。

### 7.5 语义计划优化与数据执行优化是两个正交轴

semantic plan 与 external physical execution 是当前两条优化轴，不能以“先做调度”为由长期缺失数据库
优化，也不能用更少模型调用掩盖低效执行。dependency/KV execution 只作为条件性参考：

| 优化轴 | 必须进入的机制 | 主要参照 | 选择与状态所有者 |
|---|---|---|---|
| semantic algorithm / plan | reference path、ordinary cheap predicate ordering、SemFilter proxy/oracle cascade、filter–join placement、candidate generation、model role、后续 fusion/prompt batching/AQE | LOTUS、Cortex AISQL、Sema | PostgreSQL semantic optimizer/executor；`EXACT` 只保留 reference，显式 `APPROX` 下先证明 candidate 满足同一 quality policy，再比较 cost |
| external physical execution | execution-batch placement、sealed-task work/locality grouping、bounded async drive、continuous refill、multi-Job share、endpoint routing | IMLane、现有 SemLoom | PostgreSQL pump 与 SemLoom provider 按 §7.2 的 matched profiles 划分 |
| dependency/KV execution（参考） | stage lineage、prefix lease、cache-domain stickiness、KV-aware admission | Kalypso | 不构成当前排期；若另行立项，PostgreSQL 产生 lineage，SemLoom 管物理 lease/admission，vLLM 管真实 KV blocks |
| model serving | continuous batching、iteration scheduling、KV allocator、decode | vLLM | vLLM；首版不修改 |

首个 `SemMap` 只能验证 reference execution 与外部 seam；真正的数据库优化资格从 `SemFilter` 第二条
physical path 开始。LOTUS 没有与 filter/join/top-k 同类的专用 `SemMap` approximation，因此不能虚构
一个“LOTUS SemMap optimizer”作为首个完成项。

PostgreSQL 普通 path comparison 默认候选产生同一关系结果。LOTUS proxy/cascade、similarity-pruned join、
Cortex join-to-classification 与 Sema fusion 可能只满足统计质量目标，不能未经声明就作为普通等价 paths：

- 规划前已有与 semantic spec、model、parser、数据 profile、quality policy 和 evidence epoch 全部匹配的
  资格证据时，显式 `APPROX` query 才能把 candidate 作为同一近似语义合同下的 path；
- 只有运行期 sampling 后才能判断资格时，planner 只添加一个 `ADAPTIVE_FILTER`/`ADAPTIVE_JOIN` path；
  该 node 在数据库内完成 sample、资格判断和 reference fallback，不能先把未合格 proxy path 交给
  PostgreSQL 按 cost 选择；
- evidence 失配、过期或 prepared-plan identity 变化时，只允许 reference path 并触发重新规划。

## 8. 代码构建蓝图

### 8.1 目录与长期维护方式

```text
code/postgres/
├── pg18_core_patch/                  # conditional；carrier audit 选择 core 前不创建
│   ├── upstream.lock                 # URL + REL_18_3 exact commit
│   ├── series
│   ├── patches/                      # git format-patch；唯一 core patch 源
│   ├── scripts/                      # apply/build/test，不保存第二份手写 core overlay
│   └── test_vectors/
└── semloom_pg/                       # companion PGXS extension + capability spike
    ├── Makefile / semloom_pg.control / sql/
    ├── src/
    │   ├── extension.c               # hook chaining / method registration
    │   ├── marker.c                  # marker residual 时 fail closed
    │   ├── planner.c / sem_path.c / sem_plan.c
    │   ├── sem_scan.c                # CustomScan spike adapter
    │   ├── sem_query_runtime.c       # query-scoped session / ResourceOwner
    │   ├── sem_pump.c                # per-node begin/next/stop/explain 深 module
    │   ├── task_compiler.c / result_parser.c
    │   └── uds_provider.c / wire_v1.c
    ├── expected/ / sql/
    └── t/                            # TAP cancel/crash/transaction tests

code/src/execution_provider/
├── contracts.py / wire_v1.py
├── server.py / session.py / profiles.py / evidence.py
├── adapters/
│   ├── recording.py
│   └── direct_http.py
└── semloom/
    ├── task_mapping.py               # protocol task -> scheduler internal type
    ├── organization.py
    ├── scheduler_session.py          # incremental offer/advance/seal/cancel
    └── ray_workers.py

code/tests/execution_provider/
code/tests/compatibility/lotus_v124/   # recording fixtures；不进入 provider runtime
code/scripts/services/run_execution_provider_gateway.py
```

不把完整 PostgreSQL source vendor 进主仓库。仅当 carrier audit 选择 core 时，core 修改才在固定 upstream fork/worktree 中开发，经
`git format-patch` 登记；`upstream.lock` 锁定 commit，patch apply + PostgreSQL regression/isolation/TAP
共同验收。现有 stock `deploy/postgres18.4/` 保留无 patch 对照，patched deployment 另建入口时再按
deploy 规则登记。

### 8.2 两阶段复用同一个 executor-shaped module

extension spike 的 `SemMapState` 与条件性 core `SemanticUnaryState` 都只调用：

```c
sem_exec_begin(...);
sem_exec_next(...);       /* PostgreSQL pull executor 的唯一热路径 */
sem_exec_stop(...);       /* early stop / query cancel / error，幂等 */
sem_exec_explain(...);
```

`SemQueryRuntime` 隐藏 query-scoped provider open/close、operator registration 和 error cleanup；
`SemanticExecPump` 隐藏 slot/Datum copy、child prefetch、task binding、task/byte/work budgets、completion
reorder、socket/latch wait 和 result parse。spike 通过后先审查载体：若继续 extension，
`CustomScan` 直接调用同一 module；若只需 core planner seam，`SemanticExpr`/path generation 仍 lower
为 `CustomScan`；只有 executor lifecycle 也受阻时才换成 native `SemanticUnary/State`。外部 gateway
interface 与 SQL observable tests 保持不变。这使 capability 代码既可成为正式实现，也可成为迁移依据，
而不是一次性 runner。

### 8.3 条件性 core patch 的最小 node family

carrier decision 逐项依据反例，不用“原生感”投票：

下列 node family 是条件性上界，不构成当前 patch checklist；实际只实现已复现阻断对应的层。

| 目标 | 继续 extension 的充分证据 | 采用 core patch 的触发条件 |
|---|---|---|
| unary `SemMap/SemFilter` | marker 按 OID fail-closed lowering；CustomPath placement、projection、cost、prepared plan 与 lifecycle 全部通过 | residual marker、plan invalidation、setrefs 或 hook chaining 存在不能封闭的错误 |
| LOTUS-like alternatives | reference/proxy-oracle 等均成为可 EXPLAIN、带不同 cost/quality evidence 的显式 CustomPaths | 只能在 provider 内暗换算法，或 extension 无法稳定保留 plan identity |
| Cortex-like placement/rewrite | cheap predicate ordering 与受限 filter–join placement 能在公开 hooks 中生成合法 paths，并与其他 extension 共存 | 必须修改 analyzed expression identity/core rewrite 才能证明合法性，或 hook 模拟无法覆盖目标 topology |
future join/aggregate 只保留公开 hook 能力记录；相应算子另行立项前，不参与当前 carrier decision。

若只有 semantic identity/path generation 受阻，最小形态是：

```text
SemanticExpr
  -> semantic path-generation seam
  -> CustomPath/CustomScan
```

只有 executor lifecycle 也有独立复现阻断，才进一步增加 `SemanticUnary Plan/State -> SemanticExecPump`。
`SemMap` 和 `SemFilter` 可共享 unary node；future `SemanticJoin` 与 blocking operators 仅说明若另行立项时
不能复用错误的 unary lifecycle，不构成当前实现范围。core patch 只负责解除当前阻断所需的 node/path/
executor support；function-like SQL 仍由 companion extension 按 marker OID lowering，不改 raw grammar。

若采用 native executor node，其上界至少涉及 `primnodes.h/pathnodes.h/plannodes.h/execnodes.h`、node walker/support、planner/
createplan/setrefs、executor dispatch、EXPLAIN 与 build files，因此它是 spike 之后的独立 reviewed patch
series，不能与首个 gateway vertical slice 混成一个变更。

### 8.4 gateway 是第二个深 module

gateway 的 caller 只学习 `open/drive/close`。其 implementation 隐藏 wire framing、session state、task
dedup、backpressure、evidence、`PreparedSemanticTask → WorkDescriptor → WorkUnit`、admission、routing、
Ray/vLLM adapters 与 late-result suppression。gateway 内部而非 wire 暴露变化轴：

```python
class SchedulingSession(Protocol):
    def offer(self, units: Sequence[WorkUnit]) -> int: ...
    def advance(self, max_results: int, deadline_s: float): ...
    def seal(self) -> None: ...
    def cancel(self) -> None: ...
```

现有 `SynchronousScheduler.run(iterable)` 和 `SynchronousExecutionEngine.execute()` 会消费完整输入并排空
后返回，不能直接包成 query session。应先用 characterization tests 抽出 stateful incremental machine，
再让旧同步 runner 成为兼容 adapter；不能在 gateway 复制一套 admission/routing/credit 状态机。

### 8.5 现有模块的去向

| 现有模块 | 决定 |
|---|---|
| `WorkDescriptor`、scalar packing | gateway 内部直接复用；wire 使用独立 versioned mirror，不泄漏 Python type |
| `BatchRequest` / `PayloadEnvelope` | `task_mapping.py` 包裹；仍是 scheduler 内部 model-call/work-unit 类型，不等于 semantic operator |
| `StaticAdmissionController`、endpoint routers、shared credit | 作为 `SchedulingSession` 的内部 policies 复用 |
| `PrefixAffinityEndpointRouter` | 只提供初始 locality；Kalypso-like child stickiness 仅作远期参考，当前不增加 lease table |
| `SubmissionExecutionLedger` | 在保持性测试后扩为增量 completion/cancel；不能先破坏历史 runner |
| `RaySubmissionAdapter` | 由 gateway work-unit adapter 复用；Ray 不获得 PostgreSQL plan/parser |
| PostgreSQL source/sink、Daft materializer | 保留 external/native baseline；DB-native 主路径由 ordinary child 与 transaction 供数/写回 |
| `request_gateway.py` | 继续是无队列/无准入/无路由的被动 observer，不能冒充 execution-provider gateway |
| static/shared runtime | 后续 provider profiles；首个 vertical slice 不接 SAOR/dynamic controllers |
| LOTUS compatibility | companion extension 的显式 semantic profile + recording parity fixtures；不作为 provider adapter，删除后默认 SemMap/SemFilter、gateway 和 scheduler 仍通过 |

若未来另行设计 Kalypso-like query-stage lineage，不复用 `planning.work.StageWork`：当前类型描述模型
执行资源阶段，两者变化原因不同；未来候选应使用独立类型。

## 9. 当前实施工作包、资格后验证与参考方向

工作包一至七构成当前有序实施范围。数据库资格完成后优先运行 IMLane-like batch placement 对照；
其余远期机制只有在前置条件成立、另有当前计划和实验合同时才进入实现。

### 工作包一：`REL_18_3` extension capability spike

注册 fail-closed `ai_semantic.map` marker；planner 按函数 OID 识别受限形状，用 ordinary path 包一层
`CustomPath/CustomScan`；`SemExecPump` 先连接进程内 recording adapter。首版只支持一个 unary SemMap、
稳定顺序和 forward scan，明确拒绝 rescan、parallel、mark/restore、EPQ 与 parameterized nested-loop path。

完成标准：child filter/projection、snapshot、LIMIT 0/1、early shutdown、query cancel、ERROR longjmp、
`INSERT ... SELECT` rollback、重复 payload 与资源清理都有 regression/TAP tests；EXPLAIN 显示 semantic
operator 与 ordinary child。未降低 marker 必须报错，不能回退逐行 HTTP UDF。

### 工作包二：`open/drive/close` 协议与 UDS recording gateway

实现最小 `SemanticPlanSpec/PreparedSemanticTask/CompletionRecord`、C/Python canonical digest golden
vectors、length-prefixed wire 和独立进程 recording gateway。测试 accepted prefix、正常 backpressure、
乱序、duplicate/missing completion、坏 digest、拆包/粘包、disconnect、timeout、early stop 与 late result。

完成标准：tasks/bytes/work/reorder 四类内存均有上限；相同输入在 C/Python 产生相同 digest；disconnect
后不猜测 task acceptance；所有 stop/error path 的 close 有界、无抛错且 session/socket 无泄漏。

### 工作包三：初步载体审查与条件性最小 core patch

先用反例测试 extension 对 marker identity、prepared-plan invalidation、hook coexistence、SemFilter
placement 与两个 semantic alternatives costing 的支持。future join/aggregate hooks 只记录公开能力形状，
在相应算子进入当前范围前不作为 core patch 触发条件。如果当前目标可在受限、可维护的 CustomPaths
中实现，继续 extension，不为“看起来原生”改内核。

只有当前 exact unary operator 或最小第二 path 出现已复现阻断时，才在 capability 和协议 tests 不变的
前提下增加解除阻断所需的最小 patch。若阻断只在 semantic identity/path generation，先增加
`SemanticExpr`/semantic path-generation seam，继续 lower 为 `CustomScan`；只有 executor lifecycle 也有
独立阻断时才增加 native `SemanticUnary/State`。companion extension 继续拥有 function-like SQL
marker/catalog 与 UDS provider adapter；patch 不修改 raw grammar。

完成标准：先形成初步 carrier report；最终 extension/core 判断在 exact `SemFilter` 与最小第二 path
完成或复现阻断后关闭。若选择 extension，报告证明当前目标与 lifecycle 可表达；若选择 core，
extension 与 patched carrier 对同一 recording workload 产生相同 task digest set、typed rows、错误和
lifecycle evidence，并说明每处 core 修改对应哪个已复现阻断。

### 工作包四：增量 SemLoom scheduling session

先用现有测试固定 `SynchronousScheduler` 行为，再抽出 `offer/advance/seal/cancel` state machine；旧同步
runner 通过兼容 adapter 使用它。`PreparedSemanticTask` 在 gateway 内映射为现有 work/scheduler types，
首个 profile 只接 static admission、固定/简单 router 与 fake/local Ray adapter，不接 SAOR。

完成标准：旧同步 runner 的既有 observable behavior 保持；新 session 在输入尚未 EOS 时可返回 completion；
cancel 后 late result 不发布，shared credits/ledger/actor lease 最终归零。若 scheduler 需要 SQL expression、
result parser 或 PostgreSQL Plan，说明 seam 错位并停止接入。

### 工作包五：direct HTTP 与真实 `SemMap` vertical slice

gateway 将数据库已经封闭的 canonical messages 发送到固定 OpenAI-compatible endpoint；数据库解析 raw
completion 并生成 SQL Datum。只运行小规模 capability，记录 model/protocol/endpoint identity、usage、
finish reason 与 task lifecycle，不产生调度性能结论。

### 工作包六：`SemFilter` reference path

在载体审查选定的 unary carrier 上增加严格 boolean/unknown parser、PostgreSQL 三值/NULL/error policy 与 semantic
selectivity；ordinary predicates 留在 child。数据库决定 keep/drop，provider 不能返回最终 relation rows。

完成标准：EXPLAIN 区分 ordinary 与 semantic predicate；重复/乱序输入、unknown、provider error、LIMIT
和 transaction abort 都不破坏 cardinality、tuple identity 或资源清理。

### 工作包七：最小 LOTUS/Cortex-like `SemFilter` 第二 path

先使用 deterministic fixture 或规划前已经匹配的静态 quality evidence，生成一条与 reference 分离、可在
`EXPLAIN` 辨认的 `SemFilter` physical path。`EXACT` 只生成 reference；显式 `APPROX` 保存 quality
policy、algorithm/model role、evidence epoch 与 fallback。该工作只证明 PostgreSQL 拥有 algorithm identity、
quality eligibility 和 cost，provider 不能暗换算法；不同时承诺完整 LOTUS query-time sampling、Sema AQE
或全局 Pareto planner。

完成标准：reference/alternative 的 plan digest、prepared-plan invalidation、cost、typed rows 与 provider
task roles 都可验证；evidence/policy 失配时只保留 reference。

### 数据库资格完成后优先验证：IMLane-like batch placement

在 exact `SemMap/SemFilter`、增量 provider 和 cancel/backpressure 稳定后，用完全相同
task/model/capacity 比较 `db_batch_preserved` 与 `provider_rebatch`，分别记录 child pull、organization、
submit、model、fan-in、overfetch、cancel waste 和 endpoint idle gap。该对照回答“组批和提交究竟放在
哪里”，不属于当前数据库资格的完成条件，结果出来前两种形态都保持候选。

### 仅供参考：条件满足后的 semantic 与 lineage 方向

- LOTUS-style query-time sampling/proxy-oracle cascade：需要独立质量实验合同与 reference fallback；
- Kalypso-like lineage protocol v2：只有出现真实 stage-aware admission/prefix reuse 需求后，才考虑
  operator/stage descriptors、lineage event、prefix lease 与 cache-domain stickiness；
- binary `SemJoin`、blocking aggregate/top-k/group-by、semantic fusion/prompt batching 与 AQE：分别需要
  独立 node family、关系语义和质量验证，不由当前 unary interface 预先承诺；
- 通用 PostgreSQL semantic framework/core fork：不作为默认目标，只按届时的可复现阻断重新审查。

LOTUS/Sema/IMLane 的可运行未修改 native path 始终由原系统拥有执行与调度，条件匹配时作为独立
full-system baseline；Kalypso 当前只有论文参照，不预注册 native baseline。compatibility adapter 不阻塞
以上数据库资格工作。

## 10. 查询与事务正确性

首版必须明确：

- `(query_id, operator_instance_id, task_id)` 是跨进程生命周期身份；`input_sequence` 与 child tuple binding
  只保存在 PostgreSQL executor state；
- child tuple 只来自当前 query snapshot，provider 不重新读取数据库；
- 只发送算子需要的投影列，普通关系 predicate 尽可能在 child plan 执行；
- 普通 `SELECT` 可流式返回；statement 最终失败后，已发送 DataRow 不能物理撤回，也不能视为成功查询
  结果；cancel 后不得再发布新行；
- `INSERT ... SELECT` 的结果只有 transaction commit 后可见；
- 模型调用是外部不可回滚副作用，数据库 abort 只能阻止结果提交并 best-effort 请求 provider cancel；
- 默认不自动 retry；只有明确幂等 key、attempt evidence 和结果去重合同后才能增加受控 retry；
- prompt/output 不进入普通日志，证据保存 digest、work、identity、时间和终态；
- provider crash、gateway disconnect、invalid output、parser error 和 timeout 均结构化传播到 query error。

### 10.1 必须实现的反例测试

| 反例 | 预期行为 |
|---|---|
| planner 没有降低 marker function | 执行时报明确错误，不逐行 HTTP fallback |
| provider 请求原始 SQL/表名后重新拉表 | interface 不提供这些字段，adapter 测试拒绝 |
| provider 修改 canonical messages | payload digest mismatch，query fail closed |
| completion 重复、缺失或 task ID 不属于当前 query | query 失败并保存脱敏协议证据 |
| LIMIT/用户 cancel 发生在仍有在途任务时 | 停止拉 child、调用 provider cancel、回收 session |
| backend timeout/进程崩溃 | query error；未完成 INSERT 不可见 |
| 乱序 completion + 重复输入文本 | 按 row identity 恢复，不按文本或完成顺序匹配 |
| NULL/unknown filter result | 按 plan 的三值 policy 执行，不由 provider 猜测 |
| rescan/parallel plan | 首版在计划阶段拒绝，不产生半支持执行 |
| protocol/plan/parser version 漂移 | 建立 session 或首批结果前失败 |

若以后启用 lineage protocol v2，再增加 `NO_CHILD`、prefix lease release、跨 cache-domain stickiness
与放弃 prefix reuse 的反例；它们不属于首版 mandatory test set。

## 11. 实验与 baseline 分层

新的实现资格和性能比较分开：

1. **database operator qualification**：recording/remote provider 的小规模 SQL、child plan、snapshot、
   cancel/error/result lifecycle；不做系统排名。
2. **provider matched comparison**：相同 PostgreSQL operator plan、sealed task/digest set、模型、服务、
   capacity 和 parser 下比较 direct HTTP、IMLane-like `db_batch_preserved`、SemLoom `provider_rebatch`
   两条当前候选；只有 work 与 semantic path 匹配后才能归因组批/提交机制。dependency-aware profile
   仅作远期参考，需另立实验合同。
3. **native full-system comparison**：官方路径可运行且任务可匹配时，Sema、LOTUS、IMLane、Daft、
   Ray Data 等使用自己的正式入口和 execution owner，比较完成同一用户任务的端到端经验表现；不把
   差异全部归因于调度，也不把它们强行塞入 SemLoom provider seam。Kalypso 当前仅作论文参照，未定位
   作者公开 artifact，不预注册 native comparison。

系统级计时至少区分 query release、child first tuple、provider first submit、model first/last completion、
数据库最后结果可见或 transaction commit。阶段可能重叠，不能要求分阶段 wall time 相加等于 E2E。

正式实验继续遵守 [`baseline_reference.md`](baseline_reference.md) 与证据台账。当前计划不授权新 GPU
矩阵、SAOR 参数调整或 formal run；只有前置 implementation/correctness artifact 完成后，才为具体
实验另写或更新执行合同。

## 12. 当前不能声称

- 不能说 PostgreSQL planner-visible AI 语义算子已经实现；当前只有设计计划。
- 不能在载体审查前声称 extension 必然不够或 core patch 必然需要；两者都必须以目标优化和 lifecycle
  的可复现实验为依据。
- 不能说现有 profiler/manifest、Daft/Ray/static/SAOR 结果来自数据库内算子。
- 不能说 Sema 的 DuckDB 实现可直接移植为 PostgreSQL extension。
- 不能说 `ai_semantic.map` marker function 本身就是一等算子；只有 planner/executor qualification
  通过后才能使用该表述。
- 不能说 SemLoom work-unit batching 等于 Sema prompt batching；两者是否改变语义调用结构不同。
- 不能把进程解耦、vector batch async submission、资源 Lane 或 Ray adapter 写成新颖性；IMLane 已直接覆盖。
- 不能把单 query、单 cache domain 的 stage-aware KV admission 或 virtual pinning写成新颖性；Kalypso 已直接覆盖。
- 不能说项目复用了 Kalypso 代码；截至 2026-08-28 未定位作者公开 artifact。
- 不能说 LOTUS 是核心依赖、语义所有者或项目 backend registry。
- 不能把 `lotus_compat + SemLoom provider` 称为 LOTUS native。
- 不能把一次 capability/smoke 或 CPU/fake 结果外推为调度性能贡献。

## 13. 文档替换与后续同步

本文生效后：

- 当前实现入口只指向本文；
- 旧 PostgreSQL+LOTUS 主计划和 LOTUS frontend 子计划移动到 `archive/`，文件头指向本文；
- 根总纲、快速入口、代码状态和知识库把 LOTUS 迁移从当前首任务改为可选兼容/baseline 工作；
- 历史实验结果、原始数据、旧计划中的 LOTUS v1.2.4 源码审计和 Q1–Q23 决策不删除；
- 开题正文、PPT 和图件仍含 LOTUS-centered 架构时，后续必须按本文单独做内容与视觉审计，不能只
  做字符串替换；在完成该同步前，它们是上一版架构快照。

## 14. 一手依据

- 本轮一手来源与迁移审计：
  [`../../research/sema_native_semantic_operator_architecture_reference_20260827.md`](../../research/sema_native_semantic_operator_architecture_reference_20260827.md)，
  覆盖 Sema、Cortex AISQL、IMLane、Kalypso、LOTUS 与 PostgreSQL 18 实现机制。
- Sema：Kangkang Qi et al., *Sema: A High-performance System for LLM-based Semantic Query
  Processing*，当前精读版本为 [arXiv:2603.11622v1](https://arxiv.org/abs/2603.11622)；本地精读见
  [`../../research/精读文献笔记/sema_vldb2026/sema_vldb2026.md`](../../research/精读文献笔记/sema_vldb2026/sema_vldb2026.md)。
- PostgreSQL 18 Custom Scan：<https://www.postgresql.org/docs/18/custom-scan.html>。
- PostgreSQL 18 PGXS：<https://www.postgresql.org/docs/18/extend-pgxs.html>。
- PostgreSQL 18.3 source tag：<https://github.com/postgres/postgres/tree/REL_18_3>。
- Cortex AISQL：<https://arxiv.org/abs/2511.07663>。
- IMLane：*IMLane: Composable Framework for Efficient AI Function Execution in Database Engine*，
  [PVLDB 19(12), 2026](https://www.vldb.org/pvldb/vol19/p4223-xu.pdf)；作者 artifact：
  <https://github.com/IM-DM4AI/IMLane0>。
- Kalypso：*Kalypso: Relational LLM Serving*，
  [arXiv:2607.23815v2](https://arxiv.org/abs/2607.23815)；本地精读见
  [`../../research/精读文献笔记/kalypso_arxiv2026/kalypso_arxiv2026.md`](../../research/精读文献笔记/kalypso_arxiv2026/kalypso_arxiv2026.md)。
- LOTUS：Patel et al., *LOTUS: Enabling Semantic Queries with LLMs Over Tables of Unstructured
  and Structured Data*，[PVLDB 2025](https://www.vldb.org/pvldb/vol18/p4171-patel.pdf)；现有源码与接口
  审计见 [`../../research/lotus_postgresql_execution_layer_fit_20260821.md`](../../research/lotus_postgresql_execution_layer_fit_20260821.md)。
- 当前实现事实：[`../../code/INFRA_STATUS.md`](../../code/INFRA_STATUS.md) 与
  [`../results/EXPERIMENT_EVIDENCE_REGISTRY.md`](../results/EXPERIMENT_EVIDENCE_REGISTRY.md)。
