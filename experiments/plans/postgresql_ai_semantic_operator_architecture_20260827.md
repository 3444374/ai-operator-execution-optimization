# SemLoom PostgreSQL 内置 AI 语义算子整体架构与实施计划

更新日期：2026-08-28
状态：`current / architecture-defined / implementation-not-started`
当前实现事实：现有代码是 PostgreSQL source/sink、Daft/Arrow、Ray、vLLM/CLIP、调度与观测组成的
外部物理执行基座；尚无 PostgreSQL planner-visible AI 语义算子、统一 execution-provider 协议或
Sema/LOTUS 兼容适配器。
首个实现范围：PostgreSQL 18.3、文本 `SemMap`、一个普通关系 child plan、一个 query-scoped provider
session；`SemFilter` 是第二个算子，join/aggregate/fusion/AQE 均为后续工作。
替代关系：本文取代 2026-08-21 的 PostgreSQL+LOTUS 主计划与 LOTUS 语义前端子计划；旧文档进入
`archive/`，只保留历史决策和源码审计价值。

## 1. 架构决策

项目后续采用以下主路径：

> **在 PostgreSQL 扩展中实现一等 AI 语义算子，参考 Sema 将语义操作放入 SQL、查询计划和执行
> 生命周期的做法；数据库把已经绑定并规范化的语义任务交给可替换 execution provider。SemLoom 现有
> Daft/Ray/vLLM 执行与调度代码作为其中一个 provider implementation。LOTUS 保留为相关系统、
> 行为兼容 profile 和完整系统 baseline，不再是核心运行依赖或语义所有者。**

这里的“内置”表示 PostgreSQL 拥有 SQL 表达、普通关系 child plan、snapshot、权限、语义算子计划、
结果解释以及 cancel/error/result lifecycle。模型、Ray、vLLM 和 Python runtime 仍在数据库进程外；
输入 payload 会通过数据库管理的有界通道传出，因此不能写成“数据物理上不离开数据库”。

Sema 是架构参照，不是需要逐模块复制的实现：Sema 修改 DuckDB 的 parser、planner、optimizer 和
executor；本项目坚持 PostgreSQL extension/PGXS 路径，首版不修改 PostgreSQL core grammar，也不在
尚未证明可行时承诺完整复刻 SemaSQL、operator fusion 或 Adaptive Query Execution（AQE，自适应查询执行）。

## 2. 参照系统的角色

| 对象 | 本项目中的角色 | 不承担什么 |
|---|---|---|
| Sema | 主要架构参照：semantic operator 是显式 plan node，数据库拥有优化和执行状态 | 不作为 PostgreSQL 代码依赖；论文中的 DuckDB 实现不能证明 PG extension 已可行 |
| PostgreSQL Custom Scan / planner hooks | 首版 planner-visible 物理算子的候选承载机制 | capability prototype 通过前不写成已实现事实 |
| LOTUS | 语义算子相关工作、可选 `lotus_compat` 行为 profile、未修改完整系统 baseline | 不定义核心 IR，不要求 `lotus-ai==1.2.4` 才能运行数据库算子 |
| SemLoom 现有物理执行代码 | 外部 execution provider 的物理执行和调度 implementation | 不解析 SQL，不决定 `SemFilter` 真值，不修改 prompt 或 output parser |
| Daft / Ray / vLLM / typed CLIP actor | provider 后方的数据执行、分布式运行和模型服务设施 | 不拥有 PostgreSQL snapshot、ACL 或 query lifecycle |

这个调整保留课题原来的研究目标：研究语义算子产生的外部 AI work 如何组织、提交、路由和在多 Job
间调度；改变的是数据库语义层的中心依赖和实施顺序。

## 3. 目标架构

```text
用户 SQL
  |
  v
PostgreSQL AI Semantic Extension
  |-- SQL marker + binder/type checker
  |-- SemanticOperatorPlan registry
  |-- planner-visible logical/physical operator
  |-- ordinary relational child plan
  |-- task compiler + result parser
  |-- snapshot / ACL / cancel / error / result lifecycle
  |
  |  PreparedSemanticTaskBatch
  v
AiExecutionProvider interface
  |-- recording provider            # deterministic capability/test
  |-- remote HTTP provider          # simple transport/control
  `-- SemLoom provider              # proposed physical execution
        |-- WorkDescriptor / cost estimator
        |-- work-unit organization
        |-- admission / continuous replenishment
        |-- multi-Job scheduling / endpoint routing
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

一句话：**PostgreSQL 内部实现 Sema-like AI 语义算子，SemLoom 方法通过统一 provider interface 成为其
外部物理执行与调度 implementation。**

## 4. 数据库、provider 与模型服务的所有权

| 责任 | 所有者 |
|---|---|
| SQL surface、参数绑定、类型、输出 schema、NULL/error/order policy | PostgreSQL semantic extension |
| ordinary child plan、filter/projection、snapshot、ACL、transaction | PostgreSQL |
| semantic operator kind、prompt program、result parser、quality policy | PostgreSQL semantic extension |
| task identity、query deadline、cancel、可见结果与资源清理 | PostgreSQL semantic executor |
| task 的物理分组、inflight work、Job share、endpoint route | execution provider |
| Ray task/actor 的执行与回收 | 对应 provider implementation |
| continuous batching、KV cache、token generation | vLLM native scheduler |
| LOTUS prompt/output compatibility | 可选 `lotus_compat` profile；不覆盖核心默认语义 |
| LOTUS/Daft/Ray/Sema 的原生完整路径 | 各系统自己；仅作 full-system baseline |

### 4.1 provider 明确不能做什么

provider 不接收原始 SQL 或 PostgreSQL Plan，也不能：

- 重新连接 PostgreSQL 拉取表或额外列；
- 改写 instruction、canonical messages、generation constraints 或 parser；
- 把多条独立任务融合成一个改变输出对应关系的 semantic prompt；
- 决定 `SemFilter` 的三值逻辑或 `SemExtract` 的输出列；
- 将失败任务静默改为 NULL、重试或成功；
- 在 query cancel 后继续产生数据库可见结果。

provider 可以改变任务的物理执行顺序、work-unit membership、并发、提交时机和 endpoint，只要结果
身份、语义 payload digest 和完成状态保持可验证。

## 5. 语义算子模型

### 5.1 逻辑算子注册表

| Operator kind | 输入与输出 | 首版状态 |
|---|---|---|
| `SEM_MAP` | 每个输入 tuple 生成一个 typed value，并附加为新列 | 当前唯一必做算子 |
| `SEM_FILTER` | 每个输入 tuple 生成 boolean/unknown，数据库决定是否保留 | 第二个算子，用来证明算子会改变关系 cardinality |
| `SEM_EXTRACT` | 从 tuple 生成声明式结构化字段 | `pending` |
| `SEM_JOIN` | 对候选 tuple pair 执行语义谓词 | `pending`；先有候选生成和 cardinality 合同 |
| `SEM_ORDER_BY` | 根据语义比较关系确定顺序 | `pending` |
| `SEM_AGG` | 对一组 tuple 产生聚合结果 | `pending`；需要阻塞/分层执行设计 |

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

### 5.2 `SemanticOperatorPlan`

数据库内 canonical plan 至少包含：

```text
SemanticOperatorPlan
  schema_version
  operator_kind / operator_instance_id
  input_bindings / input_schema / output_schema
  normalized_instruction
  prompt_program + prompt_program_sha256
  result_parser + parser_sha256
  null_policy / error_policy / order_policy
  quality_policy
  model_requirements / generation_constraints
  semantic_optimization_flags
  plan_sha256
```

plan 不包含 Pandas、LOTUS AST、Daft、Ray 或 vLLM 对象。每个字段要么被实现消费，要么因不支持而
在计划阶段报错；不能接受后静默丢弃。

### 5.3 `PreparedSemanticTask`

数据库从 child tuple 编译出 provider-neutral task：

```text
PreparedSemanticTask
  protocol_version
  query_id / operator_instance_id / task_id / row_identity
  operator_kind
  canonical_messages or typed model input
  semantic_payload_sha256
  expected_raw_output_kind
  generation_constraints
  estimated_work + uncertainty + calibration_signature
  deadline
```

`result_parser` 不交给 provider 执行。provider 返回 raw completion 和模型 usage；数据库按 plan 中的
parser 产生 boolean、text 或结构化值。这样换 provider 不会换语义。

### 5.4 `CompletionRecord`

```text
CompletionRecord
  query_id / operator_instance_id / task_id / row_identity
  semantic_payload_sha256 / plan_sha256
  terminal_state = completed | failed | cancelled
  raw_output or typed transport error
  prompt_tokens / output_tokens / finish_reason
  provider_evidence_digest
```

每个 task 只能出现一个数据库接受的终态。exactly-once 只描述数据库结果行和 task terminal state；
外部模型调用不可回滚，不能写成 exactly-once inference。

## 6. 唯一外部 seam：`AiExecutionProvider`

数据库从 `SemanticOperatorPlan` 派生只含 provider 所需 model/capability、generation constraints、
plan digest 和资源上限的 `AiProviderPlan`；它不包含 result parser、SQL expression 或关系 child plan。
provider 是一个 query-scoped、异步、有界的 interface：

```c
AiSession *open(const AiQueryContext *, const AiProviderPlan *);
AiSubmitResult submit(AiSession *, const PreparedSemanticTaskBatch *);
int poll(AiSession *, CompletionBatch *, int max_results, TimestampTz deadline);
void finish_input(AiSession *);
void cancel(AiSession *, const char *reason);
AiProviderEvidence close(AiSession *);
```

interface 的非类型部分同样是合同：

- `submit` 只接受数据库当前 query 已绑定的 task，返回明确的 accepted/rejected 数量；
- provider 可以乱序完成，数据库用 task/row identity fan-in；
- outstanding task、bytes 和 estimated work 都有上限；达到上限时产生 backpressure，不无限缓存；
- `finish_input` 后不得再提交；所有已接受 task 终止后 `poll` 返回 exhausted；
- `cancel` 幂等，停止接受新任务并尽快返回已接受任务的 cancelled/failed 状态；
- transport disconnect、protocol drift、digest mismatch 和未知 terminal state 均使 query fail closed；
- `close` 返回脱敏 evidence，不返回或持久化原始 prompt/output。

这是一个真实 seam，因为至少有 recording、remote HTTP 和 SemLoom 三个 adapter。首个 qualification
实现可用 Unix domain socket + versioned length-prefixed message；在 profile 证明序列化是主要成本前，
不先增加共享内存、Arrow Flight 或自定义零拷贝协议。

## 7. 两种 batching 必须分开

### 7.1 semantic batching / fusion

将多条 tuple 写入一个联合 prompt、融合两个 semantic operator、改变调用数或 parser 的优化，会改变
semantic execution path，甚至改变结果质量。它属于 PostgreSQL semantic optimizer/executor，必须：

- 生成新的显式 plan/task identity；
- 记录 tuple membership、prompt program 和 parser；
- 与 reference path 或人工 ground truth 比较质量；
- 在 local vLLM 与 remote API 下分别校准 latency/cost/quality；
- 首版默认关闭。

### 7.2 physical work organization

项目现有 token/work/prefix organization 只把多个**已经独立编译完成**的 tasks 组成 `WorkUnit`，
不修改 canonical messages、调用数、parser 或一行一结果关系。它属于 SemLoom provider。

因此：

```text
数据库 semantic batching/fusion
  = 允许改变语义调用结构，必须显式进入计划和质量验证

SemLoom work-aware organization
  = 只改变独立任务的物理提交结构，不改变语义
```

## 8. 与当前代码的衔接

建议目录：

```text
code/postgres/ai_semantic_operator/
  Makefile
  ai_semantic_operator.control
  sql/
  src/
    extension.c               # extension init / marker registration
    planner.c                 # recognize and lower supported expressions
    semantic_plan.c           # plan validation / serialization / digest
    custom_scan.c             # child-plan pull / result emission
    task_compiler.c           # tuple -> PreparedSemanticTask
    provider_client.c         # provider protocol/session/backpressure
    result_parser.c           # raw output -> typed SQL datum

code/src/operators/
  provider_protocol.py        # Python mirror of the versioned wire contract
  gateway.py                  # provider session server and lifecycle
  providers/
    recording.py              # deterministic test adapter
    remote_http.py            # simple request transport control
    semloom.py                # adapter to existing physical runtime
  compatibility/
    lotus_v124.py             # optional prompt/output compatibility profile

code/tests/operators/
code/postgres/ai_semantic_operator/sql/
code/postgres/ai_semantic_operator/expected/
```

`code/postgres/ai_semantic_operator/` 是数据库侧深 module：调用方只看 SQL/operator 行为，不需要理解
planner、task 编译、provider transport 或 parser implementation。`code/src/operators/gateway.py` 是
数据库外深 module：它隐藏 provider session、协议验证、backpressure 和 evidence；adapter 不把这些
职责重新散到 runner。

现有模块的去向：

| 现有模块 | 新架构中的位置 |
|---|---|
| `code/src/planning/work.py::WorkDescriptor` | SemLoom provider 内部，由 task work metadata/estimator 构造；不进入 SQL plan interface |
| `BatchRequest` / `PayloadEnvelope` | SemLoom adapter 的内部类型；不出现在 provider wire contract |
| `code/src/scheduling/` | 保持不感知 PostgreSQL Plan、Sema 或 LOTUS |
| `code/src/data/sources/postgres_text.py` | 外部/native baseline 与历史 profiler 使用；数据库内主路径由 child plan 供数 |
| `code/src/observability/request_gateway.py` | 可复用为 provider 后方的被动 model-request observer，不承担 provider session 语义 |
| `code/src/data/sinks/postgres.py` | 外部完整路径 baseline 使用；数据库内 `INSERT ... SELECT` 由 PostgreSQL transaction 拥有 |
| 现有 static/shared/SAOR runtime | 通过 `providers/semloom.py` 接入，不复制或迁入 extension |

删除 `lotus_v124.py` 后，默认 PostgreSQL `SemMap`、recording/HTTP/SemLoom providers 和 scheduler
都应继续工作；删除 SemLoom provider 后，PostgreSQL operator qualification 与其他 providers 仍应工作。
这两个删除测试用于防止 LOTUS 或项目调度重新成为核心耦合点。

## 9. 实施工作包与完成标准

### 工作包一：PostgreSQL planner/executor capability prototype

目标：先证明 PostgreSQL 18.3 extension 能在不修改 core 的条件下承载所需算子形态。

动作：

1. 注册 `ai_semantic.map` marker；
2. planner hook 只识别一个受限 base/child plan 和一个 map expression；
3. 生成带 ordinary child plan 的 planner-visible CustomScan candidate；
4. executor 先做 deterministic echo/uppercase transformation，不连 Python、HTTP 或 GPU；
5. `EXPLAIN (VERBOSE)` 显示 operator kind、child plan、plan digest 和脱敏配置；
6. 未识别表达式、rescan、parallel、volatile input 和未知 options 一律明确报错。

完成标准：`CREATE EXTENSION` 后 SQL 可运行；child filter/projection、snapshot、LIMIT、cancel、error、
`INSERT ... SELECT` 和资源清理均有可重复测试。若 CustomScan/planner hook 不能可靠实现这一点，停止
“PostgreSQL 内置算子”表述，记录具体限制后重新评估 extension 机制；不得退回逐行 HTTP UDF 继续
使用同一表述。

### 工作包二：中立语义 plan 与 task/result 合同

动作：实现 canonical serialization/digest、`SEM_MAP` binder、prompt program、result parser、NULL/
error/order policy，并建立 C/Python 交叉 golden vectors。

完成标准：同一输入在 C/Python 两端产生相同 plan/task digest；未知字段、schema drift、parser drift
和未消费 option 都 fail closed。测试包含空字符串、Unicode、重复 payload、乱序 row identity、NULL、
大 payload 和非法 provider result。

### 工作包三：provider protocol + recording adapter

动作：实现 query-scoped session、bounded submit/poll、finish/cancel/close、乱序 completion、断连和
evidence；用独立进程 recording provider 返回可预测结果。

完成标准：不依赖模型即可证明 task exactly-once terminal state、结果按 identity 恢复、bounded memory、
LIMIT early stop、用户 cancel、provider crash、timeout、duplicate/missing completion 和资源释放。

### 工作包四：remote HTTP `SemMap` vertical slice

动作：provider 把数据库已编译的 canonical messages 发送到一个固定 OpenAI-compatible endpoint；
数据库解析返回值并输出 SQL tuple。provider 不进行 semantic prompt rewrite。

完成标准：小规模真实 endpoint capability 通过；记录 model/endpoint/protocol identity、actual tokens、
finish reason、query/operator/task lifecycle 和脱敏 evidence。该工作只证明完整链路可运行，不产生调度
性能结论。

### 工作包五：SemLoom provider adapter

动作：`PreparedSemanticTask` 映射为现有 `WorkDescriptor → BatchRequest/PayloadEnvelope`，首先只接
legacy `project_static` frozen-static；复用现有 completion fan-in、trace 和 provider 后方 observer。

完成标准：不修改 scheduling core 即可运行；recording/HTTP/SemLoom 三个 provider 使用相同 plan、
task digest set、output parser 和 row set。若必须让 scheduler 理解 SQL/plan/parser，返回工作包二重设合同。

### 工作包六：`SemFilter` 与关系语义验证

动作：增加 boolean/unknown parser 和三值/NULL/error policy；CustomScan 从 child plan 拉取 tuple，
只向下游发出数据库判定保留的行。

完成标准：普通 relation filter 与 semantic filter 的执行位置在 EXPLAIN 和 trace 中可区分；duplicate、
out-of-order、unknown、provider error 和 cancel 不会破坏 row identity 或产生部分提交结果。

### 工作包七：代价与语义物理优化

前置：SemMap/SemFilter 的数据库生命周期和 provider seam 已稳定。

动作按证据逐项加入：operator cost estimate、predicate ordering、prompt batching、operator fusion、
micro-execution/AQE。每项都建立 reference path、quality/cost/latency 指标和独立开关。

完成标准：优化在目标模型/服务/workload 签名下超过未优化 reference，并满足预先规定的质量条件；
不能把 Sema 论文结果当成本项目已经实现或必然有效的证据。

### 工作包八：LOTUS 兼容与原生 baseline

该工作不阻塞前六个工作包。

1. `lotus_compat` 仅对受支持 `SemMap` profile 复现版本锁定的 prompt/output 行为；
2. recording LM 比较 canonical messages、generation options、output mapping 和错误行为；
3. 未修改 LOTUS 完整路径保留自己的 DataConnector/Pandas/LM/LiteLLM execution owner；
4. LOTUS native、Sema native 和其他 semantic system 作为 full-system baseline 单独报告，不注入 SemLoom
   provider、credit 或 scheduler。

完成标准：兼容 profile 的差异有逐字段证据；LOTUS 缺失或版本变化不会阻止默认算子运行。兼容测试
通过只说明特定 profile 行为一致，不说明本项目是 LOTUS 内置 backend。

## 10. 查询与事务正确性

首版必须明确：

- `(query_id, operator_instance_id, task_id, row_identity)` 是生命周期身份；
- child tuple 只来自当前 query snapshot，provider 不重新读取数据库；
- 只发送算子需要的投影列，普通关系 predicate 尽可能在 child plan 执行；
- 普通 `SELECT` 可流式返回，但 cancel 后不得继续产生用户可见行；
- `INSERT ... SELECT` 的结果只有 transaction commit 后可见；
- 模型调用是外部不可回滚副作用，数据库 abort 只能阻止结果提交并请求 provider cancel；
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

## 11. 实验与 baseline 分层

新的实现资格和性能比较分开：

1. **database operator qualification**：recording/remote provider 的小规模 SQL、child plan、snapshot、
   cancel/error/result lifecycle；不做系统排名。
2. **provider matched comparison**：相同 PostgreSQL operator plan、task set、模型、服务、资源和输出
   条件下比较 simple HTTP、SemLoom frozen-static（历史 arm 为 `project_static`）与 proposed provider；只在语义、正确性和观测合同
   一致后做机制归因。
3. **native full-system comparison**：Sema、LOTUS、Daft、Ray Data 等使用自己的正式入口和 execution
   owner，比较完成同一用户任务的端到端经验表现；不把差异全部归因于调度，也不把它们强行塞入
   SemLoom provider seam。

系统级计时至少区分 query release、child first tuple、provider first submit、model first/last completion、
数据库最后结果可见或 transaction commit。阶段可能重叠，不能要求分阶段 wall time 相加等于 E2E。

正式实验继续遵守 [`baseline_reference.md`](baseline_reference.md) 与证据台账。当前计划不授权新 GPU
矩阵、SAOR 参数调整或 formal run；只有前置 implementation/correctness artifact 完成后，才为具体
实验另写或更新执行合同。

## 12. 当前不能声称

- 不能说 PostgreSQL planner-visible AI 语义算子已经实现；当前只有设计计划。
- 不能说现有 profiler/manifest、Daft/Ray/static/SAOR 结果来自数据库内算子。
- 不能说 Sema 的 DuckDB 实现可直接移植为 PostgreSQL extension。
- 不能说 `ai_semantic.map` marker function 本身就是一等算子；只有 planner/executor qualification
  通过后才能使用该表述。
- 不能说 SemLoom work-unit batching 等于 Sema prompt batching；两者是否改变语义调用结构不同。
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
  覆盖 Sema、LOTUS、Palimpzest、DocETL、Abacus 与 PostgreSQL 扩展机制。
- Sema：Kangkang Qi et al., *Sema: A High-performance System for LLM-based Semantic Query
  Processing*，当前精读版本为 [arXiv:2603.11622v1](https://arxiv.org/abs/2603.11622)；本地精读见
  [`../../research/精读文献笔记/sema_vldb2026/sema_vldb2026.md`](../../research/精读文献笔记/sema_vldb2026/sema_vldb2026.md)。
- PostgreSQL 18 Custom Scan：<https://www.postgresql.org/docs/18/custom-scan.html>。
- PostgreSQL 18 PGXS：<https://www.postgresql.org/docs/18/extend-pgxs.html>。
- LOTUS：Patel et al., *LOTUS: Enabling Semantic Queries with LLMs Over Tables of Unstructured
  and Structured Data*，[PVLDB 2025](https://www.vldb.org/pvldb/vol18/p4171-patel.pdf)；现有源码与接口
  审计见 [`../../research/lotus_postgresql_execution_layer_fit_20260821.md`](../../research/lotus_postgresql_execution_layer_fit_20260821.md)。
- 当前实现事实：[`../../code/INFRA_STATUS.md`](../../code/INFRA_STATUS.md) 与
  [`../results/EXPERIMENT_EVIDENCE_REGISTRY.md`](../results/EXPERIMENT_EVIDENCE_REGISTRY.md)。
