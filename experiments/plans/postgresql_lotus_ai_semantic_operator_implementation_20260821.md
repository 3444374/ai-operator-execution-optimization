# PostgreSQL 内置 LOTUS AI 语义算子实现计划

更新日期：2026-08-21
状态：`design-frozen / lotus-semantic-migration-not-started / gpu-formal-forbidden`
首版范围：PostgreSQL 18.3、文本 `AI_COMPLETE`、LOTUS `sem_map`、单租户多 Job
权威关系：本文是数据库内 AI 语义算子的实施入口；
[`lotus_semantic_frontend_execution_integration_20260821.md`](lotus_semantic_frontend_execution_integration_20260821.md)
保留 LOTUS v1.2.4 源码、prompt、IR 与 native baseline 的细化合同，但不得覆盖本文的数据库所有权和计时边界。

## 1. 决策

项目采用以下主路径：

> **在 PostgreSQL 内注册 planner-visible 的 AI 语义算子，由数据库拥有 SQL、关系子计划、
> snapshot、权限、取消、错误和结果生命周期；直接复用 LOTUS v1.2.4 的 `sem_map` 语义实现，
> 通过数据库管理的流式执行通道把最小必要 row batches 交给 LOTUS/Daft/Ray/SAOR 物理后端。**

这里的“数据库内”指逻辑算子和查询生命周期属于 PostgreSQL，不表示 GPU、LOTUS Python、
Daft、Ray 或 vLLM 必须运行在 PostgreSQL backend 进程中，也不表示物理上零数据传输。
模型推理需要接收输入 payload；目标是消除用户侧 `SELECT/fetchall → Python → HTTP → INSERT`
工作流，使数据交接成为数据库执行计划拥有、可治理、可取消、可观测的内部物理步骤。

研究证据采用两条互不冒充的轨道：

1. **数据执行层性能轨**：假设 `AI_COMPLETE` 逻辑算子已存在，先以
   `emulated PostgreSQL AI-operator execution contract` 比较 LOTUS/Daft/Ray/Project/SAOR
   backend。它可在 extension 前运行，但只支撑“AI 语义算子物理数据执行”结论。
2. **数据库内集成轨**：用 PostgreSQL extension/CustomScan 小规模证明真实 SQL、
   child plan、snapshot、cancel/error/result lifecycle 闭环。它是 Cortex-like “数据库内算子”
   表述的资格门，不是调度论文的主要性能贡献。

当前首要实现不是 PostgreSQL C extension，而是把项目现有 manifest/UDF-like
语义入口替换为真实、版本锁定的 LOTUS v1.2.4 `sem_map` 语义合同。等语义与
物理 backend seam 通过后，再将同一合同接到 PostgreSQL SQL 入口。

### 1.1 2026-08-21 grilling 决策记录

下表保留用户对 Q1–Q23 的回答及后续精化，作为实现与实验的决策来源。
“已精化”表示原问题的粗略答案已被后续问题替换，不得只读旧答案。

| Q | 当前决策 | 状态 | 对实现/实验的约束 |
|---|---|---|---|
| Q1 | 同时保留未修改 LOTUS DataConnector 完整路径与 PG AI operator + LOTUS native backend | 有效 | 两者身份和归因分开 |
| Q2 | 当前性能对象是“假设 AI 语义算子存在时的数据执行层”，不等同已实现 PG 算子 | 已由 Q4/Q19 精化 | 性能轨标 emulated，集成轨单独验证 |
| Q3 | 用户正式入口是 SQL `AI_COMPLETE`；LOTUS 提供 `sem_map` 语义实现 | 有效 | LOTUS Python API 不是 proposed 对外主入口 |
| Q4 | 采用数据执行层性能轨 + PostgreSQL 内集成资格轨 | 有效 | 性能研究不被 C extension 阻塞，表述不冒充 |
| Q5 | 各系统原生 source/full path 也必须作对比 | 已由 Q6/Q8/Q9 精化 | 形成两 panel，不只比共同 stream |
| Q6 | operator-backend panel 共享数据库内/仿真 AI operator row stream | 有效 | stream adapter 不参与调度 |
| Q7 | 从 Job release/operator open 开始计时，不得提前读取/物化 | 有效 | source/handoff/service/result 分阶段报告 |
| Q8 | LOTUS/Daft/Ray/Project 原生/UDF 完整路径纳入同一 artifact | 已由 Q9 精化 | 可同表，但必须分 panel |
| Q9 | 单一 schema/artifact，`operator_backend` 与 `native_full_path` 两 panel | 有效 | 可比 E2E，不生成无边界单一排名 |
| Q10 | 相同环境下 UDF 与 AI 语义算子的性能必须可比 | 已精化 | Panel A messages 精确同义；Panel B 同任务+正确性/质量门 |
| Q11 | “相同提交”指用户发布相同逻辑工作，不要求产品 API 字面相同 | 有效 | 统一 Job controller 与 release schedule |
| Q12 | 同时做 controlled fixed-output 与 natural-EOS 两条 workload | 有效 | 机制隔离与真实性结论分开 |
| Q13 | 同时报 model-completion JCT 和 query-visible JCT | 有效 | 数据执行与完整系统边界不混合 |
| Q14 | 所有臂使用被动 observer | 有效 | 不缓存/排序/重试/限流，过 no-op overhead 门 |
| Q15 | 原生 baseline 不做性能搜索，不注入项目策略 | 已由 Q18 精化 | 只使用官方默认/示例性能值 |
| Q16 | 首轮两 Job actual-work 长期 entitlement 为 1:1，foreground 有更严 SLO | 有效 | fairness 仅在共同 backlog 窗口计算 |
| Q17 | common stream 只交付已 release rows，不给原生 backend bounded-ready 可见性 | 有效 | 原生 backend 自己决定读取/排队/提交 |
| Q18 | 原生臂只允许 PG/SQL/model/endpoint/resource/output/observation 必需适配 | 有效 | batch/concurrency/backpressure 不调参；声称 out-of-box |
| Q19 | extension 前用 server-side cursor + bounded stream 仿真 operator contract | 有效 | 必须标 emulated，不支撑已实现 PG-native 结论 |
| Q20 | 不设单一加权总分，使用 Pareto + non-inferiority | 有效 | correctness/quality 硬门，throughput/SLO/fairness/isolation 分报 |
| Q21 | 保留 LOTUS/Daft/Ray 原生 baseline；项目 Daft UDF 只 diagnostic；PG row-wise HTTP UDF 为传统下界 | 有效 | 不把项目自写代码称为 vendor native |
| Q22 | 暂定 static→SAOR 作机制对照；最佳合格 Panel-A native 作系统竞争力对照；Panel B 作 E2E 经验对照 | 暂定 | 后续可在 formal 预注册前重开，不可看 formal 结果后改 |
| Q23 | PostgreSQL row-wise HTTP UDF 必须完整实现，但当前后置 | 有效 | 当前首任务是 LOTUS `sem_map` 语义入口迁移 |

一手文献补充：Cortex 把逐行/black-box model call 当作动机，但其正式实验没有 generic
HTTP/Python UDF 臂；LOTUS 确实实测名为 `AI UDF` 的 baseline，其中部分支持 batching，
并非 PostgreSQL 逐行同步 HTTP。因此本项目使用名称
`PostgreSQL row-wise HTTP AI UDF (literature-motivated lower-bound control)`，不称 Cortex/LOTUS
原样 baseline。细节与一手链接见
[`research/lotus_postgresql_execution_layer_fit_20260821.md`](../../research/lotus_postgresql_execution_layer_fit_20260821.md)
§10。

首版不修改 PostgreSQL core source，不 fork PostgreSQL；优先使用 PostgreSQL extension、
planner hook 和 Custom Scan provider。PostgreSQL 18 的 Custom Scan 是实验性扩展接口，
因此宣称“真实 PostgreSQL 内置算子”前必须过 planner/executor capability gate，不能把设计图或
仿真 operator contract 当成已实现事实。

## 2. 路径与身份必须分开

| 路径 | 数据读取所有者 | 语义算子所有者 | 物理执行所有者 | 身份 |
|---|---|---|---|---|
| PostgreSQL 内置算子 + LOTUS native backend | PostgreSQL query executor | LOTUS `sem_map` | LOTUS LM/LiteLLM + vLLM | 数据库内算子系统 baseline |
| PostgreSQL 内置算子 + project frozen-static | PostgreSQL query executor | LOTUS `sem_map` | Daft/Ray project static + vLLM | 同栈静态参照 |
| PostgreSQL 内置算子 + SAOR | PostgreSQL query executor | LOTUS `sem_map` | Daft/Ray + SAOR + vLLM | proposed |
| 未修改 LOTUS DataConnector + native `sem_map` | LOTUS/SQLAlchemy/`pd.read_sql` | LOTUS `sem_map` | LOTUS LM/LiteLLM + vLLM | 外部 LOTUS 产品 baseline，另表 |
| 当前 manifest/profiler 路径 | 外部 harness | project manifest | Daft/Ray/project | 迁移前 diagnostic/rehearsal，不是数据库内算子 |

直接使用 LOTUS 的含义必须具体化：

1. 冻结 `lotus-ai==1.2.4`、release commit、wheel/source SHA；
2. 使用真实 `SemMapNode`、官方 prompt formatter、system prompt、suffix、generation kwargs
   和 output parser；
3. LOTUS native backend 实际调用官方 `df.sem_map(...) → LOTUS LM → LiteLLM`；
4. project backend 使用同一个 `SemMapNode` 和逐字节相同 messages，只替换物理提交 backend；
5. 主路径不调用 LOTUS `DataConnector.load_from_db()`，因为行已经由 PostgreSQL 当前查询的
   child plan 产生；再次 `pd.read_sql` 会破坏 snapshot、流式执行和查询生命周期；
6. 未修改 DataConnector 路径完整保留，用于 LOTUS native 产品级补充比较。

## 3. 目标架构与所有权

```text
Client SQL
  SELECT doc_id, ai.complete(prompt, instruction => ..., model => ...)
  FROM documents
  WHERE workload_name = ...
        |
        v
PostgreSQL parser/catalog/planner
  -> typed AI semantic expression
  -> ProjectAiCustomPath / ProjectAiCustomScan
  -> ordinary child plan owns snapshot/filter/projection
        |
        | bounded RowEnvelope stream
        v
Database-managed AI execution gateway
  -> query/operator/job identity
  -> backpressure/cancel/error propagation
  -> no user-side table export
        |
        v
LOTUS sem_map runtime (semantic owner)
  -> official SemMapNode + prompt builder + output parser
        |
        +-> LOTUS native LM/LiteLLM backend
        +-> Daft Native backend
        +-> Daft/Ray backend
        +-> Ray Data native graph backend
        +-> project frozen-static backend
        `-> project SAOR backend
        |
        v
vLLM native FCFS / continuous batching
        |
        v
result RowEnvelope stream
  -> PostgreSQL CustomScan tuple output
  -> downstream SQL operator / client result / INSERT sink
```

所有权冻结如下：

| 责任 | 所有者 |
|---|---|
| SQL 表达、catalog privilege、关系 snapshot、filter/projection、query cancel | PostgreSQL |
| `sem_map` instruction/messages/output semantics | LOTUS v1.2.4 |
| row-batch 交接、query/job identity、物理 backend 选择 | database-managed gateway |
| Daft/Ray native batching/backpressure | 对应原生 framework |
| project K/W、bounded-ready、credit、debt、SAOR | Project，仅 proposed |
| continuous batching、KV 管理、请求执行 | vLLM native scheduler |

## 4. SQL 与逻辑算子合同

### 4.1 SQL surface

首版目标语法使用 PostgreSQL extension 注册的 SQL-callable function：

```sql
SELECT
    doc_id,
    ai.complete(
        prompt,
        instruction => 'Answer the user request.',
        model => 'frozen-model-alias',
        options => '{"max_tokens":256,"temperature":0}'::jsonb
    ) AS completion
FROM documents
WHERE workload_name = 'matched_two_job';
```

函数形态与 Cortex 的 SQL surface 相似，但它不能只是逐行调用 HTTP 的黑盒 UDF。extension
必须使 planner 能识别受支持的 `ai.complete` expression，并把它降低为批量/流式执行的
CustomPath/CustomScan。`EXPLAIN (VERBOSE)` 必须显示项目 AI operator node、backend identity、
plan hash 与成本估计；若 planner 未生成该 node，查询必须 fail closed，而不是退回逐行函数执行。

首版只支持：

- 单个 `ai.complete` projection；
- 一个关系 child plan，可包含普通 scan/filter/projection；
- 稳定、非 NULL `doc_id`；
- `TEXT` prompt 输入和 `TEXT` 输出；
- deterministic generation contract；
- 单向扫描、无 rescan、无 parallel query；
- query result 或 `INSERT INTO ... SELECT ...` 两种结果语义。

首版逐项拒绝：多个 semantic operators、semantic join/filter/aggregate、correlated subquery、
planner rescan/mark-restore、parallel worker、volatile prompt expression、未知 option、
LOTUS cascade/optimizer/cache、用户自定义 callable/postprocessor。

### 4.2 Canonical plan

LOTUS 语义合同不应被 PostgreSQL 集成细节绑住。分成两个小型 canonical value：

```text
SemanticMapPlan
  schema_version
  operator_identity = lotus.sem_map@v1.2.4
  prompt_column / row_identity_column
  instruction / system_prompt / suffix
  supported_semantic_options
  generation_contract
  lotus_release / commit / wheel_or_source_sha256
  prompt_builder_sha256 / output_parser_sha256
  semantic_plan_sha256

AiOperatorInvocation
  query_id / operator_instance_id / job_id / release_epoch
  input_schema / output_schema
  source_relation_snapshot / projection / predicate digest
  semantic_plan_sha256
  backend_identity
  database_identity
  result_visibility_mode
  invocation_sha256
```

该 interface 不暴露 Pandas、LOTUS AST、Daft、Ray 或 vLLM 对象。LOTUS adapter 唯一负责
生成并核对 `SemanticMapPlan`；仿真 gateway 或 PostgreSQL extension 负责 `AiOperatorInvocation`。
两者各自对 canonical JSON 逐字节计算 SHA，这样替换仿真 source 为真实 CustomScan 时不需要
改动语义 plan、backend adapter 或 scheduler。任何字段漂移均在首行交付前失败。

## 5. PostgreSQL extension implementation

建议位置：

```text
code/postgres/ai_semantic_operator/
  Makefile
  ai_semantic_operator.control
  sql/ai_semantic_operator--0.1.sql
  src/extension.c
  src/planner.c
  src/custom_scan.c
  src/gateway_client.c
  src/plan_codec.c
  expected/
  sql/
```

### 5.1 Planner capability prototype

数据库内集成轨启动时，先做最小 C/PGXS extension，不接 LOTUS、Ray 或 GPU：

1. 注册 `ai.complete` SQL symbol；
2. planner hook 识别严格受限的目标表达式；
3. 为目标 relation 增加带 `CUSTOMPATH_SUPPORT_PROJECTION` 的 CustomPath；
4. 生成含 ordinary child plan 的 CustomScan；
5. executor 只执行 deterministic echo transformation；
6. `EXPLAIN` 显示 node、child plan、cost、plan hash；
7. 验证 filter/projection 在 child plan 内完成，而不是把整表送出；
8. 验证 query cancel、LIMIT、error、`INSERT ... SELECT` 与资源清理。

完成标准：无需 PostgreSQL core patch，`CREATE EXTENSION` 后可在 PostgreSQL 18.3 上运行；
planner 未识别、版本漂移、需要 rescan/parallel 的计划均 fail closed。若无法在 extension
范围内产生语义正确、可批量执行的 planner-visible node，则停止“已实现数据库内算子”
结论与集成性能实验；数据执行层轨可继续，但必须显式标记为 emulated operator contract。
不得退回 PL/Python/逐行 HTTP UDF 后继续声称数据库内算子已实现。

### 5.2 Executor lifecycle

CustomScan implementation 必须覆盖：

- `BeginCustomScan`：验证计划/服务身份，建立 query-scoped gateway session；
- `ExecCustomScan`：从 child plan 拉取 tuple，投影最小必要列，组成有界 batch；
- batch flush：row count、byte cap、work cap 或 child exhausted；
- result fan-in：按稳定 row identity 输出 tuple；
- PostgreSQL interrupt：传播 cancel，停止新提交并回收 session；
- `EndCustomScan`：确认无 unresolved row、关闭通道、发布 query evidence；
- `ReScanCustomScan`：首版明确报 unsupported；
- `ExplainCustomScan`：只显示脱敏身份、plan/backend/cost，不显示 prompt/DSN/token。

PostgreSQL backend 不加载 LOTUS、Ray、Daft 或 vLLM Python 环境。extension 只实现深的
database-executor interface；Python/runtime 复杂性位于 gateway 后方。

## 6. 数据交接与“数据不离开数据库”的准确合同

首版保证的是“用户不导出数据、数据库拥有交接”，不是物理零拷贝或零网络：

```text
ordinary child plan output
  -> RowEnvelope{query_id, operator_id, job_id, row_id, payload, plan_sha}
  -> bounded managed channel
  -> AI physical executor
```

要求：

- source tuple 只能来自当前 query 的 child plan，不允许 backend 重新连接 PostgreSQL；
- 不允许 `pd.read_sql`、`fetchall`、JSONL export 或预加载 manifest 成为主路径输入；
- 只发送 AI 算子需要的投影列，filter 必须先在数据库执行；
- 记录 input rows/bytes、first/last batch、channel wait 和 bounded-buffer peak；
- prompt/output 不写入通用日志，证据只保存 row ID、digest、token work 和状态；
- gateway 必须通过 Unix domain socket 或受控加密 channel，身份绑定 query/backend/runtime；
- 数据不得由 gateway 持久化；失败归档保存 digest/provenance，不保存原始敏感 payload；
- 每个 query 有独立 cancel token、deadline 和 bounded buffer；
- 物理 backend 不拥有数据库 snapshot，也不能绕过 PostgreSQL ACL 再取其他列。

### 6.1 Extension 完成前的 operator-contract 仿真

数据执行层性能轨允许一个集中、可删除的仿真 seam：

```text
Job release / emulated operator_open
  -> PostgreSQL server-side cursor under one snapshot
  -> bounded projected RowEnvelope stream
  -> the same LotusSemanticRuntime and backend interface
```

它必须满足：

- 真实数据始终位于同一 PostgreSQL 表，不使用预生成 JSONL/Arrow manifest 替代 source；
- `T0` 前不建立 cursor、不执行 source query、不预读 rows；
- 使用 server-side cursor/streaming fetch，禁止 `fetchall` 和完整 DataFrame materialization；
- 只拉取 `doc_id/job_id/prompt` 等 AI 算子必要列，记录 row/byte/batch 时间序列；
- common stream 只交付 Job release 后的 rows，不替 LOTUS/Daft/Ray 预取完整 Job、排序、
  选 Job、设置 concurrency 或维护 credit；
- 输出身份固定为 `emulated_postgresql_ai_operator_contract`，报告和图不得缩写为
  `PostgreSQL native AI operator`。

这一 seam 在真实 CustomScan 集成通过后应能从性能 runner 中删除，而不影响 LOTUS
语义 runtime、backend adapters 和 scheduler core。

## 7. LOTUS sem_map runtime

建议位置：

```text
code/src/operators/contracts.py
code/src/operators/semantic/lotus_v124.py
code/src/operators/runtime/lotus_sem_map.py
code/src/operators/runtime/postgres_gateway.py
```

唯一语义 interface：

```python
class LotusSemanticRuntime:
    def prepare(self, plan: SemanticMapPlan) -> PreparedSemanticOperator: ...

class PreparedSemanticOperator:
    def build_messages(self, rows: RowBatch) -> PreparedRequestBatch: ...
    def parse_results(self, results: CompletionBatch) -> ResultRowBatch: ...
```

implementation 必须直接复用/调用冻结 LOTUS 代码来完成：

- `SemMapNode` 字段解析；
- instruction/column substitution；
- system prompt、suffix、examples/options；
- message construction；
- output parsing/return-raw semantics。

不得复制一份“看起来相同”的 prompt template。必须用 recording LM 对拍真实
`DataFrame.sem_map`，逐行验证 message bytes、call count、generation kwargs、output 和 digest。

### 7.1 LOTUS native backend

数据库推送的 row batch 构造为临时 DataFrame，实际调用官方 `df.sem_map(...)` 和 LOTUS LM。
该 backend 不使用 LOTUS DataConnector，因为数据库 source 已由当前 query 执行；其身份写成：

> PostgreSQL AI operator + LOTUS v1.2.4 native sem_map execution

它保留 LOTUS/LiteLLM 的 batch/concurrency/retry/cache 所有权，不接 project K/W、bounded-ready
或 SAOR。真实 extension 完成前，必须在名称前加 `emulated operator contract`。

### 7.2 Project backends

Project frozen-static 与 SAOR 使用同一 PreparedSemanticOperator 产生的 messages。它们只替换
physical submission interface，不改变 LOTUS 语义。两臂共享 source、plan、gateway、Daft/Ray、
vLLM、K/W 和资源上限；唯一允许差异是冻结的 scheduling policy。

### 7.3 现有代码的迁移地图

| 现有位置 | 当前责任/问题 | 迁移后责任 |
|---|---|---|
| `code/src/serving/backends/completion.py` | `format_completion_prompts`/HTTP body 同时承担部分语义和传输 | 保留 legacy/control；新路径只消费 `PreparedRequestBatch`，prompt/messages 由 LOTUS runtime 产生 |
| `code/src/observability/profiling/manifest_guard.py` | raw prompt manifest 同时影响语义、work 和 endpoint assignment | 降为迁移 parity/provenance guard；不再是 AI 语义定义 |
| `code/src/data/sources/postgres_text.py` | `fetchall` Arrow 或 Daft `to_arrow` 全量 source | 保留 Panel B 原生/历史路径；新增有界 server-side-cursor operator stream，不在原文件叠加分支 |
| `code/src/experiments/saor/native_system_execution.py` | 五臂 external source→completion 编排 | 后续只组装两 panel 的 typed arm adapters；不在该文件实现 LOTUS compiler/runtime |
| `code/src/observability/request_gateway.py` | 共同 HTTP 观测 | 保留为被动 observer；添加 no-op overhead 门，不增加调度 |
| `code/src/data/sinks/postgres.py` | completion result sink | 保留 Panel B/`INSERT ... SELECT` 结果闭环；与 model-completion 计时分列 |

新增代码按领域拆分，不将 LOTUS/PG/Ray 合并进一个 runner：

```text
code/src/operators/
  contracts.py                       # SemanticMapPlan/AiOperatorInvocation/value types
  frontends/lotus_v124.py            # source-locked SemMapNode -> SemanticMapPlan
  semantic/lotus_runtime.py          # official messages/output semantics
  sources/postgres_operator_stream.py# emulated bounded child-plan stream
  execution/contracts.py             # PreparedRequestBatch/ResultRowBatch/backend protocol
  execution/lotus_native.py           # Panel A LOTUS native backend
  execution/project.py                # existing static/SAOR thin adapter
code/src/baselines/text/frameworks/
  lotus_native.py                     # Panel B unmodified DataConnector full path
code/postgres/ai_semantic_operator/   # later PGXS/CustomScan integration gate
```

`code/src/operators/` 不反向 import `experiments/` 或 `scripts/`；scheduler 只消费中性 request/work
contracts，不 import LOTUS/Pandas/PostgreSQL 类型。`scripts/` 仅解析配置和调用上述模块。

### 7.4 第一个 tracer bullet

第一个 PR/提交只回答“项目能否在不复制 prompt template 和不改 scheduler 的条件下使用真实
LOTUS `sem_map`”：

1. 在隔离 env 锁定 LOTUS v1.2.4；
2. 用 8–16 行 Unicode/空字符串/重复 prompt/乱序 `doc_id` 构造真实 `LazyFrame.sem_map`；
3. 用 recording LM 捕获官方 messages、kwargs、call count 和 output mapping；
4. 将同一 `SemMapNode` 编译为 `SemanticMapPlan`，用 `LotusSemanticRuntime` 产生结果；
5. 逐字节对比，再把 `PreparedRequestBatch` 交给 fake project backend；
6. 证明删除 LOTUS adapter 不需修改 scheduler/core，删除 project backend 不影响 LOTUS native baseline。

本 tracer 不连 GPU、不连服务器、不实现 SAOR 新策略、不修改 formal 合同。它通过后才接
server-side cursor 和单 GPU capability。

### 7.5 从 `AI_COMPLETE` 扩展到常见 AI 语义算子

首版只把 `AI_COMPLETE` 映射到 LOTUS `sem_map`，因为它已覆盖当前文本生成主 workload 与
多 Job 调度问题。后续只在前一算子通过数据库入口、LOTUS parity、quality 和 backend
capability 后逐个添加：

| SQL 概念算子 | LOTUS 候选语义 | 首版状态 |
|---|---|---|
| `AI_COMPLETE` / row transform | `sem_map` | **必做，当前唯一实现目标** |
| `AI_FILTER` | `sem_filter` | 后置；须冻结三值/错误/质量语义 |
| semantic join | `sem_join` | 后置；须单独设计候选对生成与 cardinality 合同 |
| semantic aggregate | `sem_agg` | 后置；不可沿用 row-in/row-out completion 证据 |
| `AI_EMBED` / `AI_CLASSIFY` | 不冒充 `sem_map` | 使用项目 typed operator，后续纳入同一 SQL/operator registry |

“数据库内 AI 语义算子”的共同标准是 typed operator identity、显式输入/输出与质量合同、
planner-visible 身份、query lifecycle 和可替换 physical backend，不是“函数名中含 AI”或在 UDF 内调 HTTP。

## 8. 可替换物理 backend interface

```python
class SemanticPhysicalBackend:
    def open(self, context: QueryExecutionContext) -> BackendSession: ...

class BackendSession:
    def submit(self, batch: PreparedRequestBatch) -> None: ...
    def poll(self) -> CompletionBatch: ...
    def cancel(self, reason: str) -> None: ...
    def close(self) -> BackendEvidence: ...
```

首版实际 adapters：

```text
lotus_native
daft_native
daft_ray
ray_data_http
project_frozen_static
project_saor
```

每个 adapter 记录 `scheduler_owner`、upstream URL/commit、adapter diff、source owner、
backend identity 和不支持指标。Daft/Ray/LOTUS baseline 保留自身 batching/backpressure；
共同 gateway 只交接 row batches 和观测结果，不替它们排序请求。

## 9. 查询正确性与事务语义

首版冻结：

- `(query_id, operator_instance_id, row_id)` 是生命周期主键；
- 输入每行恰好产生一个终态：completed/failed/cancelled；
- client retry 默认关闭；无法证明幂等时不自动重新提交模型请求；
- 模型调用是外部不可回滚副作用，但未完成查询的结果不会写入 committed sink；
- `INSERT ... SELECT` 只有 PostgreSQL transaction commit 后结果才可见；
- 普通 `SELECT` 的结果可流式返回，但 query cancel 后不得继续产生用户可见行；
- output schema、NULL/error policy、finish reason 和 token usage 固定；
- query failure、backend crash、gateway disconnect 和 vLLM timeout 均结构化落盘并脱敏；
- exactly-once 只描述数据库结果行，不把不可回滚模型调用夸大为 exactly-once inference。

## 10. 系统级比较边界

数据库内算子主矩阵统一从 SQL query/Job release 开始：

```text
T0  PostgreSQL 接受 query / Job release
T1  CustomScan 收到 child plan 第一批 tuple
T2  gateway 收到第一批 RowEnvelope
T3  第一条模型请求进入 vLLM
T4  最后一条模型请求完成
T5  最后一条结果由 SQL query 返回，或 INSERT transaction commit + readback
```

必报：

- query/system JCT：`T5-T0`；
- operator execution：`T5-T1`；
- source/startup：`T1-T0`；
- handoff/feeding：`T3-T1`；
- service interval：`T4-T3`；
- first-row、first-submit、first-result；
- input/output rows、bytes、actual tokens、correct throughput；
- per-Job JCT、solo-normalized slowdown、SLO miss；
- 共同积压窗口 actual-work share、service lag、longest no-service、Jain；
- CPU/GPU/memory/network/energy、failure、retry/cancel；
- result digest、row set、quality。

阶段可能重叠，阶段 wall 不要求相加等于 E2E。不得在 `T0` 前预读数据库、生成完整
DataFrame 或 concrete request backlog。模型/endpoint 启动、workload import 和 cache warmup
不计入 query JCT，但其生命周期必须对所有臂一致。

### 10.1 一份 artifact，两个 comparison panel

统一 CSV 必须含 `execution_class`、`source_owner`、`semantic_owner`、
`scheduler_owner` 和 `comparison_role`，报告中分成两个 panel，不生成无边界的单一总排名。

#### Panel A：`operator_backend`

该 panel 回答：相同 LOTUS-backed AI 语义算子输入下，不同数据执行 backend 如何？

| Arm | Source contract | semantic owner | physical scheduler | bounded-ready |
|---|---|---|---|---|
| LOTUS native backend under PG AI operator | common operator row stream | LOTUS | LOTUS/LiteLLM | 否 |
| Daft Native backend under PG AI operator | common operator row stream | LOTUS | Daft | 否 |
| Daft/Ray backend under PG AI operator | common operator row stream | LOTUS | Daft/Ray native path | 否 |
| Ray Data backend under PG AI operator | common operator row stream | LOTUS | Ray Data native graph | 否 |
| project frozen-static under PG AI operator | common operator row stream | LOTUS | Project static | 否 |
| SAOR under PG AI operator | common operator row stream | LOTUS | SAOR | 仅本臂 |

真实 extension 完成前，所有名称必须加 `emulated operator contract`。common stream 只转换
rows/schema/identity，不给任何原生 backend 注入 Project 的 ready window、K/W、排序或调度。

#### Panel B：`native_full_path`

该 panel 回答：完成同一用户任务时，官方/现有产品路径与 proposed 的 E2E 经验表现如何？

| Arm | Native source/entry | 身份 |
|---|---|---|
| LOTUS native v1.2.4 | `DataConnector → pd.read_sql → LazyFrame.sem_map → LM/LiteLLM` | 未修改完整产品 baseline |
| Daft built-in AI Function Native | Daft 官方 PostgreSQL/source + AI Function | 原生 framework baseline |
| Daft built-in AI Function Native/Ray | Daft 官方 PostgreSQL/source + Ray execution | 原生 framework baseline |
| Ray Data native API graph | Ray Data 官方 database/source adapter + LLM/API graph | 原生 framework baseline |
| project external frozen-static | 现有 PostgreSQL→Daft/Ray project path | 同栈静态参照 |
| project external SAOR | 现有 PostgreSQL→Daft/Ray project path | proposed 完整路径 |
| PostgreSQL row-wise HTTP UDF | 数据库用户自定义逐行模型调用 | 低效边界 control，必须完整实现，但后置 |
| project-written Daft UDF | 项目自写 | diagnostic reference，不称 Daft native |

未修改 LOTUS 产品 baseline 必须从官方 `DataConnector` 进入，实际运行
`LazyFrame.sem_map` 和 LOTUS LM/LiteLLM，不导入 project gateway、Daft/Ray adapter、K/W、
bounded-ready、credit、debt 或 SAOR。其 `upstream URL/release/commit/wheel SHA`、完整 config、
`adapter_diff=0` 和 `scheduler_owner=lotus_native` 必须进入 evidence。

Panel B 允许各系统使用自己的正式 API，“相同提交”指外部 Job controller 在相同
release epoch 发布相同表快照、行集、过滤条件、用户任务、模型、生成上限和完成条件，
不是强迫不同产品接收相同函数调用。

### 10.2 原生配置与资源合同

- 所有臂共享硬件、GPU/CPU/内存限额、模型/revision、endpoint capacity 和 workload；
- 原生 baseline 仅允许填写 PG 连接、表/SQL、模型 endpoint、资源声明、输出与观测等
  任务/环境必需参数；
- batch、concurrency、backpressure、actor pool 等性能参数保持官方默认或该官方示例的固定值，
  不进行搜索、rehearsal 追调或项目策略注入；
- 每个 baseline 记录默认/示例值的官方来源与完整 resolved config；
- 若官方默认未喂饱 GPU，如实报告为 out-of-box/vendor-native 特性，不替它修调；
- project static 与 SAOR 仍必须共享同一 K/W、ready envelope 和资源合同。

因此论文可以声称 out-of-box/vendor-native 经验差异，不能声称击败了充分调优后的
LOTUS/Daft/Ray 最优配置。

### 10.3 两条输出 workload

1. **Controlled execution**：冻结 canonical prompt、`ignore_eos=true` 与固定输出 token 长度，
   用于隔离数据执行/调度差异。只评价机制和结果身份，不声称真实输出长度可预知。
2. **Natural EOS semantics**：允许自然结束，`max_tokens` 只是安全 cap；按 actual prompt/output work、
   quality、JCT/SLO、work-estimation error 和 decision regret 评价。

Panel A 的 canonical messages 必须逐字节一致。Panel B 保留官方 prompt/semantic implementation，
但必须通过相同用户任务、output schema、correctness/quality non-inferiority 门，并报告 actual
prompt/output tokens；不得以更短或低质量输出换取更高吞吐。

### 10.4 完成、公平与归因边界

- `model_completion_jct = T4-T0` 是数据执行层主指标；
- `query_visible_jct = T5-T0` 是 UDF/产品/数据库内算子的完整系统指标；
- 两 Job 主实验暂冻结 actual-work `1:1` 长期 entitlement；foreground 只拥有更严格 SLO，
  不拥有无限优先权；
- fairness 只在两 Job 同时 backlog 窗口计算，报 actual-work share、service lag、
  longest no-service、solo-normalized slowdown、Jain、per-Job JCT/P99/SLO；
- `project static → SAOR` 是唯一 SAOR 机制因果对照；
- SAOR 与 Panel A 中所有通过 correctness/quality/fairness-evidence 门的原生 backend 逐一报告，
  再与其中 formal 正确吞吐最高者做预声明系统竞争力对照；
- Panel B 可做 E2E 经验比较，但 LOTUS DataConnector/Daft source/UDF 与 SAOR 的差额不能
  全部归因于调度；
- 不生成单一加权总分。以 correctness/quality 为硬门，以 throughput non-inferiority、
  foreground SLO、bulk protection、service lag 和 starvation 构成 Pareto 判定。精确统计门在 formal 预注册前冻结。

## 11. Observation-only request evidence

为了在不修改原生 scheduler 的情况下比较 SLO/公平，所有 backend 经过同一严格透传 observer：

- 每个 Job 使用可识别的入口身份，转发到相同 vLLM endpoint；
- observer 不缓存、不重排、不重试、不限流；
- 记录 ingress/completion epoch、endpoint usage、status 和 actual tokens；
- 请求正文与响应正文不落盘；
- observer overhead 必须通过 no-op parity gate，P95 增量超过预注册阈值即停止；
- Job release 到 completion 的等待包含 framework 未提交时间，不能只看 vLLM request latency。

若某原生 framework 只能在整表 materialize 后暴露结果，则用户可见 row completion 按官方
输出语义记录；service completion 由 observer 独立记录。二者不得混成一个 P99。

## 12. 分阶段工作包

### 工作包一：冻结 LOTUS v1.2.4 与可删除 seam

输出：隔离 driver env、release/commit/wheel/source SHA、license、关键 source-layout 只读 audit。
完成：真实 v1.2.4 通过；伪 version、漂移 `SemMapNode`/LM/runner 均 fail closed；不污染 vLLM env。
停止：无法在隔离 driver env 中安装或无法锁定官方 source identity。

### 工作包二：用 LOTUS `sem_map` 替换现有 UDF/manifest 语义入口（当前主任务）

输出：`SemanticMapPlan`、`LotusSemanticRuntime`、真实 `SemMapNode` lowering、recording LM
golden evidence、现有 manifest 仅用于迁移 parity 的 adapter。
完成：

- 用户级身份为 `AI_COMPLETE backed by lotus.sem_map@v1.2.4`，不再是项目自定义 UDF；
- 直接复用官方 instruction substitution、system prompt、suffix、supported options、
  message builder 和 output parser，不复制一套“看起来相同”的 template；
- 真实 `df.sem_map` 与 prepared runtime 的 message bytes、kwargs、output、call count、row identity 对拍；
- 所有非默认/未支持语义字段逐项 fail closed，不静默丢失。

停止：需要复制 LOTUS prompt、修改 LOTUS core、依赖未锁定私有 AST 而无 source gate，或 row identity
无法从 source 保持到 result。

### 工作包三：建立 emulated operator contract 与被动 observer

输出：PostgreSQL server-side cursor、bounded `RowEnvelope` stream、result stream、Job release、
cancel/error protocol、被动 request observer 和 redacted evidence。
完成：`T0` 前无 source read；无 `fetchall`/全表 materialization；内存随 cap 有界；
observer 不缓存/排序/重试/限流且通过 no-op P95 overhead 门；framework 未提交时间也记录。
停止：必须预生成完整 request backlog、payload 持久化、observer 改变调度，或 backend
能绕过 source contract 重新查库。

### 工作包四：Panel A operator backends

按 LOTUS native、Daft Native、Daft/Ray、Ray Data、project frozen-static、SAOR 顺序实现薄 adapter。
共同 adapter 只转换 row/schema/identity；各 framework 自己拥有 batching/backpressure/scheduling。
完成：同 `SemanticMapPlan`、message SHA set、rows、model、Job release 与 observer；每臂
owner/provenance 完整；static/SAOR 除 policy/evidence 外逐字段相同。
停止：原生 adapter 引入 project K/W、bounded-ready、credit、自写 actor/inflight 或替 framework 排序。

### 工作包五：Panel B native full paths

保留完整 LOTUS DataConnector、Daft built-in AI Function Native/Native-Ray、Ray Data API graph、当前
project static/SAOR 路径。只设置任务/环境必需参数，性能参数使用官方默认/示例值，
不做调参。统一 Job controller 发布同一逻辑任务，各产品使用自己正式 API。
完成：正确性/质量、T0 前无预读、resolved default config、scheduler owner、upstream 与
adapter diff 均可审计。

PostgreSQL row-wise HTTP UDF 必须完整实现为传统黑盒逐行调用边界，包含真实查表、
模型调用、结果返回/提交、取消与失败证据；但当前后置，不阻塞 LOTUS 语义入口迁移。

### 工作包六：PostgreSQL 内集成资格门

输出：最小 PGXS extension、`AI_COMPLETE` SQL surface、echo CustomScan、EXPLAIN、child-plan
stream、cancel/cleanup 与 `INSERT ... SELECT` 测试；随后将已通过的 `LotusSemanticRuntime` 接入。
完成：无 core patch；受支持 query 生成 planner-visible node；不支持计划 fail closed；
query lifecycle 闭环。
停止：只能逐行 UDF/PL-Python、无法消费 child plan、无法传播 cancel 或必须修改 PG core。
失败时不得声称数据库内算子已实现，但 Panel A emulation 可继续用于数据执行层研究。

### 工作包七：受控输出与自然 EOS correctness smoke

1. 16–64 行单 Job semantic/message/output parity；
2. 两 Job 64 行错峰 identity/fairness/observer smoke；
3. controlled `ignore_eos + fixed output` 与 natural EOS 各自的 correctness/quality/work evidence；
4. model-completion 与 query-visible 双 JCT；
5. failure/cancel/retry/partial-result 反例。

### 工作包八：rehearsal 与 formal

先对两 panel 分别运行 capability，再发布同一 schema 的分组 artifact。一次 rehearsal 必须通过
source/result/quality/observer/runtime/feeding 证据独立审核，才能预注册 Pareto/non-inferiority 统计门与
formal authorization。正式重复使用 `1 warm-up + 3 position-balanced formal`，不在 formal 调参。

任何 GPU 实验前仍须通过 endpoint、PostgreSQL、Ray/GPU clean、bounded feeding 和 runtime
identity gate。当前 SAOR `locked_failed_feeding` 结论不会因新 frontend 自动解除。

## 13. 必须实现的反例测试

- SQL function 存在但 planner 未生成 CustomScan；
- emulated operator contract 被报告成真实 PostgreSQL native operator；
- operator child plan 包含 rescan/parallel/correlated subquery；
- query cancel 时 backend 继续提交；
- gateway 重新连接 PostgreSQL 或调用 LOTUS DataConnector；
- filter 未下推，发送了不应离开 child plan 的行/列；
- prompt/options/source SHA 漂移；
- duplicate row ID、missing result、late result、unknown result；
- backend 失败后 result 被部分 commit；
- LOTUS native arm 出现 project K/W/bounded-ready；
- Daft/Ray baseline 出现 project scheduling import；
- common operator stream 替原生 backend 预取完整 Job、选 Job 或排序；
- native baseline 的 batch/concurrency 因 rehearsal 结果被修改，或官方默认无来源；
- Panel A/Panel B 无 `execution_class` 被混成纯 scheduler 排名；
- natural-EOS 臂使用 estimated/capped work 冒充 actual work；
- 低质量/更短输出未过 quality gate 却进入吞吐排名；
- observer 重排、重试或产生超过门槛的 latency overhead；
- T0 前发生 source fetch/concrete-ready/credit registration；
- 绝对路径、DSN、prompt、token、外部 host 或未脱敏异常进入 evidence。

## 14. 不能声称

- LOTUS 官方提供 PostgreSQL physical backend；
- PostgreSQL core 原生包含本项目 `AI_COMPLETE`；
- 模型推理在 PostgreSQL backend 进程内；
- 数据物理上从不传输；
- 当前 manifest/profiler 五臂已经是数据库内算子实验；
- emulated operator-contract 性能实验已证明 PostgreSQL extension 集成完成；
- 使用 SQL function 语法就自动获得 planner-visible semantic operator；
- LOTUS DataConnector baseline 与数据库内 backend 的差值可以全部归因于调度；
- out-of-box/vendor-native 结果代表充分调优的 LOTUS/Daft/Ray 最优性能；
- controlled `ignore_eos` 结果证明真实业务的输出 work 事先可知；
- PostgreSQL row-wise HTTP UDF 是 LOTUS/Cortex 论文实际运行的完全相同 baseline，
  除非一手论文/代码审计逐项证明；
- 新 frontend 自动修复已有 SAOR feeding-negative 结论。

## 15. 研发 Agent 完成检查单

- [ ] 先冻结 LOTUS v1.2.4 隔离 env/source identity，未过不得写 adapter；
- [ ] LOTUS v1.2.4 `sem_map` message/output parity 逐字节通过；
- [ ] 当前 project UDF/manifest 语义入口已换成 `lotus.sem_map@v1.2.4`，不复制 prompt template；
- [ ] emulated operator contract 使用 server-side cursor，`T0` 前无读取，无 `fetchall`/预导出；
- [ ] LOTUS native、Daft/Ray native、project static、SAOR 的 owner 无污染；
- [ ] 两 panel 全部有 `execution_class/source_owner/semantic_owner/scheduler_owner`；
- [ ] 原生 baseline 只配置任务/环境必需项，性能参数保持官方默认/示例值；
- [ ] query/job/row identity、cancel、error、exactly-once result 闭合；
- [ ] T0–T5 与 source/operator/service/result-visible 指标齐全；
- [ ] observer 对所有 arm 相同且 no-op overhead 过门；
- [ ] controlled/fixed-output 与 natural-EOS 两轨各自的 work/quality/evidence 闭合；
- [ ] PostgreSQL row-wise HTTP UDF 已登记为后置但必做的完整 lower-bound control；
- [ ] PostgreSQL `EXPLAIN`/CustomScan 集成门独立追踪，未过时不冒充真实数据库内算子；
- [ ] formal 默认 fail closed；
- [ ] 代码、配置、文档、测试、索引、日志和隐私扫描同步；
- [ ] 未连接 GPU 服务器，除非用户另行明确授权。

## 16. 研发 Agent 交接指令

下列文本可直接交给新研发 agent；每次只执行一个工作包，不得自行扩展：

> 当前目标不是调整 SAOR 或跑 GPU 实验，而是将项目现有 UDF/manifest-like
> `AI_COMPLETE` 语义入口迁移到真实、版本锁定的 LOTUS v1.2.4 `sem_map`。
> 必须先完整阅读本计划、LOTUS 子计划、研究审计、根 `AGENTS.md` 和 `code/AGENTS.md`。
> 第一轮只做工作包一与二：冻结隔离 LOTUS driver env/source identity；实现小型
> `SemanticMapPlan`、`LotusSemanticRuntime` 和 recording-LM parity；用 8–16 行反例数据证明
> 真实 `LazyFrame.sem_map` 与 prepared runtime 的 message bytes、kwargs、call count、output 和
> row identity 完全一致。不复制 LOTUS prompt template，不把 LOTUS/Pandas 类型泄漏进 scheduler，
> 不修改 LOTUS core，不为 future operators 预建通用框架。新路径的 operator identity 必须是
> `lotus.sem_map@v1.2.4`；现有 manifest 只作迁移 parity/provenance，不再定义语义。
> 本轮不连服务器/GPU，不运行 smoke/rehearsal/formal，不解锁 `locked_failed_feeding`，
> 不实现 PostgreSQL C extension、Daft/Ray 多 backend、row-wise UDF 或其他 LOTUS operators。
> 完成后必须报告：精确修改文件；LOTUS release/commit/wheel/source SHA；正反例测试；
> 不支持语义字段的 fail-closed 证据；与现有 scheduler 的 dependency-boundary 测试；diff check、
> compile/test 和隐私扫描。保持独立审查分支，未经用户授权不合并 `main`。

工作包一与二通过独立审核后，才给同一 agent 工作包三的 server-side-cursor/
operator-contract 任务。不要一次要求 agent 实现全部八个工作包。

## 17. 一手依据

- PostgreSQL 18 Custom Scan：<https://www.postgresql.org/docs/18/custom-scan.html>
- PostgreSQL 18 CustomPath：<https://www.postgresql.org/docs/18/custom-scan-path.html>
- PostgreSQL function planner support：<https://www.postgresql.org/docs/current/xfunc-optimization.html>
- PostgreSQL extension packaging：<https://www.postgresql.org/docs/18/extend-extensions.html>
- Cortex AISQL 论文：<https://arxiv.org/abs/2511.07663>
- LOTUS 论文：<https://www.vldb.org/pvldb/vol18/p4171-patel.pdf>
- LOTUS v1.2.4：<https://github.com/lotus-data/lotus/tree/b1a85fd7a66fabed8a1585d44d7597d592b4433f>
- LOTUS `sem_map`：<https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_map.py>
- LOTUS database connectors：<https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/data_connectors/connectors.py>
