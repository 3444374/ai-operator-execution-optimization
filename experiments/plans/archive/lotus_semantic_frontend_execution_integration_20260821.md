# LOTUS `sem_map` 语义实现与物理 backend 集成计划

> **历史状态（2026-08-27）**：本文已被
> [`../postgresql_ai_semantic_operator_architecture_20260827.md`](../postgresql_ai_semantic_operator_architecture_20260827.md)
> 取代。LOTUS v1.2.4 兼容和 native baseline 仍可复用本文的源码审计，但不再先于 PostgreSQL 中立
> 语义算子核心实现，也不再定义默认语义或总体架构。

更新日期：2026-08-21
状态：`subplan-of-postgresql-native-operator / conditional-go-pending-capability-prototype`
适用范围：文本 `AI_COMPLETE` / LOTUS `sem_map`；后续算子和多模态不在首轮实现范围

> **权威上位计划**：数据库内 SQL/operator、PostgreSQL query lifecycle、流式 row-batch
> 交接、系统计时边界和实施顺序统一见
> [`postgresql_lotus_ai_semantic_operator_implementation_20260821.md`](postgresql_lotus_ai_semantic_operator_implementation_20260821.md)。
> 本文继续负责 LOTUS v1.2.4 的直接代码复用、AST/prompt/output parity、native execution
> 和 project backend 适配细节。LOTUS 仍是 `sem_map` 语义实现所有者，不降级为仅供参考的
> syntax；变化仅在数据入口：数据库内主路径由 PostgreSQL child plan 推送 row batches，
> 不再由 LOTUS DataConnector/`pd.read_sql` 从数据库外部重新拉表。
>
> **当前交付仅限语义迁移**：先冻结 v1.2.4，证明项目使用真实
> `SemMapNode`/messages/output semantics，把现有 UDF/manifest-like `AI_COMPLETE` 换成
> `lotus.sem_map@v1.2.4`。未过这一门之前不扩展 GPU 矩阵、SAOR 策略、PostgreSQL
> C extension 或其他 semantic operators。

## 1. 决策与目标

### 1.1 决策

采用：

> **直接复用 LOTUS `sem_map` 作为数据库内 AI 算子的语义实现；PostgreSQL 拥有 SQL、
> child plan、snapshot 和 query lifecycle，Daft/Ray/SAOR 作为可替换物理 backend。**

目标架构：

```text
PostgreSQL SQL `ai.complete(...)` / Job declaration
    → PostgreSQL planner-visible AI operator + child plan（数据库所有者）
    → PostgreSQL-managed row-batch stream
    → LOTUS SemMapNode/prompt/output contract（语义实现所有者）
    → LotusSemanticPlanAdapter（版本锁定的 lowering seam）
    → Project SemanticOperatorPlan（引擎无关 IR）
    → 现有 Daft/Ray + frozen-static/SAOR（物理执行所有者）
    → vLLM native FCFS / continuous batching（模型服务所有者）
    → PostgreSQL common sink / readback
```

同时保留未修改的 LOTUS native 路径：

```text
PostgreSQL → LOTUS DataConnector/LazyFrame/SemMapNode
    → LOTUS LM + LiteLLM batch_completion
    → vLLM → common PostgreSQL sink
```

上面第二条仅是未修改 LOTUS 产品 baseline；它不得进入数据库内算子主路径。主路径禁止
LOTUS DataConnector/`pd.read_sql` 重新读取数据库，只消费 PostgreSQL 当前 child plan 产生的
row batches。物理 payload 会被数据库管理的执行通道发送到模型执行层，但用户不导出数据，
数据库仍拥有 query lifecycle。

因此后续既能回答“SAOR 相对 LOTUS native 完整系统表现如何”，也能通过
`project frozen-static → SAOR` 回答调度增量来自哪里。

### 1.2 成功标准

本计划完成的定义不是“LOTUS 能 import”，而是以下条件全部成立：

1. 数据库内/仿真主路径的 `AI_COMPLETE` 必须编译为版本锁定的 LOTUS
   `SemMapNode`/semantic plan，不把项目自写 Daft UDF 冒充语义算子；未修改 LOTUS
   产品 baseline 则必须真实运行 `LazyFrame.sem_map`。
2. LOTUS native 与项目执行臂使用相同逻辑计划、逐行消息内容、模型、生成合同和输出解析。
3. 项目执行臂保留 `job_id/doc_id/request_id` 全生命周期，且一行对应一个独立模型请求和一个结果。
4. LOTUS native 保持官方执行所有权，不接项目 K/W、bounded-ready、credit、router 或 SAOR。
5. 项目静态臂与 SAOR 臂只在冻结允许的物理策略上不同。
6. 原型、correctness gate、system smoke、rehearsal 逐级通过后，才允许申请 formal；任何阶段不得因结果不理想在线调参追正。

## 2. 一手源码事实与版本冻结

首个 capability 必须冻结：

- upstream：`https://github.com/lotus-data/lotus`
- release：`v1.2.4`
- dereferenced commit：`b1a85fd7a66fabed8a1585d44d7597d592b4433f`
- license：Apache-2.0

源码审计事实：

1. `LazyFrame.sem_map()` 构造结构化 `SemMapNode`；节点保存 instruction、system prompt、suffix、examples、strategy 与 model kwargs。
2. `SemMapNode.__call__()` 当前硬编码调用 `df.sem_map(...)`。
3. `LazyFrameRun.execute()` 当前顺序遍历私有 `_nodes` 并直接执行 `node(...)`，没有公开的 executor/backend dispatch interface。
4. eager `sem_map` 把每行转成一组 messages，再一次调用 `LM(messages)`；默认 LM 通过 LiteLLM `batch_completion(..., max_workers=max_batch_size)` 发出请求。
5. `DataConnector.load_from_db()` 使用 SQLAlchemy + `pd.read_sql` 返回完整 pandas DataFrame；它不是流式 PostgreSQL 执行器。
6. `BaseNode`、`SemMapNode`、`LazyFrameRun` 是包导出符号，但 `LazyFrame._nodes` 仍是私有实现细节。

结论：稳定版已经提供可 lowering 的结构化 AST，证明集成有现实可行性；缺失的是稳定的物理
executor seam。`BaseOptimizer` 可公开接收 node list，但只承诺返回 node list，不是 IR-export API。
首轮比较 optimizer probe 与集中只读 `_nodes` 两条实现，选择语义最清楚、可 fail-closed 的
**版本锁定外部 adapter + source-layout gate**；不直接维护大型 LOTUS fork。

## 3. 两条路线比较

| 判断维度 | LOTUS 接入项目执行器（采用） | 把项目调度移植进 LOTUS（当前不采用） |
|---|---|---|
| 调度代码 locality | SAOR、credit、trace 继续留在现有模块 | 调度状态散入 LOTUS AST/LM/runner 与项目代码 |
| LOTUS native baseline | 可使用未修改官方 release | fork 与 native 身份容易混淆 |
| Daft/Ray 数据路径 | 直接复用已验证路径 | LOTUS 当前 pandas runner 需引入/重写 Daft/Ray |
| 多模态复用 | 项目中性 work contract 可继续复用 | LOTUS 文本执行细节会进入调度核心 |
| 证据/门禁 | 复用现有 lifecycle、sink、formal evidence | 需要在 LOTUS 内重建大量项目证据代码 |
| 上游升级成本 | 一个 adapter 集中吸收变化 | fork merge 与跨仓 shotgun surgery |
| 用户界面 | 仍可使用 `LazyFrame.sem_map` | 同样可以，界面收益没有额外增加 |
| 主要风险 | optimizer 不是 IR-export hook；fallback 可能依赖私有 `_nodes` | 大 fork、baseline 污染、长期维护失控 |

### 3.1 删除测试

若删除 `LotusSemanticPlanAdapter`，LOTUS 版本检查、AST 检查、prompt parity 和 IR lowering 会重新散落到 runner、profiler 与测试中；该 module 具有真实 depth。

若把调度整体移进 LOTUS，删除 LOTUS fork 后复杂度仍完整存在于项目中，且多出跨仓同步；没有获得同等 leverage。

## 4. 模块、接口与 seam

### 4.1 项目内部 IR

新增纯类型 module：

`code/src/operators/contracts.py`

首版只暴露一个小 interface：

```python
@dataclass(frozen=True)
class SemanticMapOptions:
    examples: tuple[CanonicalSemanticExample, ...]
    strategy: str | None
    safe_mode: bool
    return_explanations: bool
    postprocessor_identity: PostprocessorIdentity | None
    residual_model_kwargs: tuple[tuple[str, CanonicalJsonValue], ...]

@dataclass(frozen=True)
class SemanticMapPlan:
    operator_id: str
    input_columns: tuple[str, ...]
    instruction: str
    system_prompt: str | None
    output_column: str
    return_raw_outputs: bool
    semantic_options: SemanticMapOptions
    generation_contract: CompletionGenerationContract
    frontend_identity: FrontendIdentity
    logical_plan_sha256: str
    prompt_builder_sha256: str
```

interface 不包含 pandas、LOTUS、Daft、Ray 或 vLLM 对象。`job_id/doc_id` 属于一次 execution
context，不写进可复用逻辑计划。首版虽然只允许 `examples=()`、`strategy=None`、`safe_mode=False`、
`return_explanations=False`、`postprocessor=None`，仍把这些默认值显式写入 canonical plan；不得因其
当前为默认值就从 IR 和 plan SHA 中省略。`model_kwargs` 中被支持的键必须逐个消费到
`generation_contract`，未消费键进入 `residual_model_kwargs` 后立即触发 fail-closed；正式可执行
plan 的 residual 必须为空。

### 4.2 LOTUS adapter

新增：

`code/src/operators/frontends/lotus_v124.py`

唯一公开 interface：

```python
class LotusSemanticPlanAdapter:
    def compile_sem_map(self, lazyframe: object) -> SemanticMapPlan: ...
```

implementation 隐藏：

- release/commit/package version 校验；
- 首先用 `BaseOptimizer` probe 核对能否无执行、无隐式副作用地取得完整 node list；
- 若不能干净导出，则集中只读 `_nodes`，并校验 `LazyFrame` 与 `_nodes` source-layout SHA；
- 仅接受 `SourceNode → SemMapNode` 首版子集；
- 解析 instruction、columns、suffix、examples、strategy、safe mode、return explanations、
  postprocessor、return-raw-output 与全部 model kwargs；
- 生成 canonical JSON 与 logical-plan SHA；
- 逐项拒绝首版不支持的非默认 options：非空 examples、非 `None` strategy、`safe_mode=True`、
  `return_explanations=True`、任意 postprocessor，以及任何未被 generation contract 消费的 model kwarg；
- 拒绝未知 node、多个 semantic nodes、cascade、GEPA/prompt rewrite、cache-on 和 callable；
- 调用冻结的 LOTUS prompt formatter 并生成逐行 request messages。

所有私有 LOTUS import 只允许存在于这个 adapter。runner、scheduler、baseline summary 与 tests
不得直接读取 `LazyFrame._nodes`。不得把 `BaseOptimizer` probe 称为 LOTUS physical-backend plugin。

### 4.3 现有 manifest adapter

将现有 project manifest 同样编译到 `SemanticMapPlan`，形成第二个真实 adapter：

`code/src/operators/frontends/project_manifest.py`

这让 semantic-plan seam 不只服务 LOTUS，也使当前 manifest 与 LOTUS frontend 可以做逐字段 parity。不要让 LOTUS 类型成为 scheduler 的必需依赖。

### 4.4 项目物理执行 adapter

新增薄 module：

`code/src/operators/execution/project_semantic_map.py`

职责只有：

1. `SemanticMapPlan + source rows + JobExecutionContext` 转换为现有 concrete request envelope；
2. 用 `job_id:request:doc_id` 建立稳定 identity；
3. 调用现有 organizer/scheduler/serving interface；
4. 按 `doc_id` fan-in 为结果行并返回证据。

它不得复制 SAOR、credit、Ray actor 或 vLLM client implementation。

### 4.5 LOTUS native baseline adapter

新增：

`code/src/baselines/text/frameworks/lotus_native.py`

该 adapter 只负责：

- 运行固定 LOTUS `DataConnector/LazyFrame/SemMapNode/LM` 路径；
- 把 common workload/endpoint/sink 配置翻译为 LOTUS 官方参数；
- 采集外部指标、结果与 provenance；
- 把结果写入共同 PostgreSQL sink。

它不得 import 项目 scheduling package。其 `scheduler_owner` 固定为
`lotus_lm_litellm_batch_completion`，`custom_scheduling_code=false`。

LOTUS 是 Pandas-like AI data-processing framework/system，而不是 PostgreSQL 数据库产品，
因此归入 `frameworks/`，不放入 `products/`。

## 5. 工作包与完成标准

### 工作包一：冻结依赖与只读源码 gate

动作：

1. 在独立可选 runtime capability 中加入 `lotus-ai==1.2.4`，保存 wheel SHA、release commit 与 Apache-2.0 license identity。
2. 新增只读 audit，核对关键模块路径、类、字段和函数体 SHA；缺失或漂移时 fail closed。
3. 保存 `python/package/commit/source-layout` JSON evidence，不访问 GPU。

完成标准：

- 正确 v1.2.4 通过；v1.1.4、伪 version、变更 `SemMapNode.__call__`、变更 runner dispatch 均失败。
- audit 只读且可在本地无 GPU 环境运行。

停止条件：LOTUS 安装要求破坏冻结 Python/vLLM 环境时，使用独立 driver env；不把 LiteLLM 依赖混进 vLLM server env。

### 工作包二：语义计划 compiler

动作：

1. 实现 `SemanticMapPlan` canonical serialization 与 SHA。
2. 先做 `BaseOptimizer` plan-probe spike；只有在无执行、无隐藏 collector/marker 合同的条件下才
   使用它做正式 lowering，否则实现集中、只读、source-hash 锁定的 `_nodes` adapter。
3. 实现 `LotusSemanticPlanAdapter`，首版只支持单个 `sem_map`。
4. 实现现有 manifest adapter。
5. 记录 unsupported-node 明确错误，不做静默 fallback。

必须测试：

- 正常 `SourceNode → SemMapNode`；
- 缺输入列、重复输出列、非法 suffix；
- 非空 examples、非 `None` strategy、`safe_mode=True`、`return_explanations=True`；
- callable 与非 callable postprocessor；
- 已支持 model kwargs 全部进入 generation contract，任一未知/残余/冲突键失败；
- `sem_filter/sem_join/sem_agg` 等未知节点；
- 两个 `sem_map`；
- optimizer 改写前后 plan SHA 不同；
- cache-on；
- pickle/AST 篡改；
- LOTUS version/source SHA 漂移。

完成标准：每个允许字段进入 canonical plan；每个未支持字段 fail closed，无丢字段的“成功编译”。

### 工作包三：逐行 prompt 与 output parity

动作：

1. 使用 recording fake LM 截获 LOTUS native 构造的每行 messages。
2. 用 adapter 对同一 DataFrame/plan 构造 messages。
3. 对 canonical request body 逐行比较 SHA；同时比较输出 postprocess、row order、raw output 和 suffix。
4. 使用包含空字符串、Unicode、长文本、重复文本、乱序 `doc_id` 的固定小数据集。

完成标准：

- 允许的每行 input message SHA 逐条相同；
- `doc_id → output` 完全相同；
- 无 prompt fusion；请求数等于行数；
- LOTUS template 或 chat protocol 漂移会失败，而不是更新 expected 值自动通过。

### 工作包四：project frozen-static backend capability

动作：

1. 把 compiled plan 接到现有 PG/Daft source、request expansion、completion backend 和 sink。
2. 首先只运行 frozen-static，不接 SAOR。
3. 请求 trace 加入 frontend/plan/prompt identity。
4. 所有实际 endpoint usage 继续按现有 work evidence 校验。

完成标准：

- 64-row CPU/fake correctness gate 通过；
- 64-row 单 GPU service capability 通过后再扩大；
- row count、digest、exactly-once、finish reason、prompt/output token 均闭合；
- 不修改现有 scheduler 核心即可运行。

停止条件：若接入需要把 LOTUS/pandas 类型传入 scheduling core，重新设计 adapter；不得用类型泄漏快速绕过。

### 工作包五：LOTUS native baseline capability

#### 原生 release 的取得与身份

可以直接使用官方 release，但“拉下来能运行”只是起点，不是正式 baseline 已成立。冻结方式二选一：

1. 从官方 GitHub checkout `v1.2.4`，核对 tag dereference 后的 commit 为
   `b1a85fd7a66fabed8a1585d44d7597d592b4433f`，从该 source 构建隔离 driver env；或
2. 安装官方 `lotus-ai==1.2.4` wheel，同时保存 wheel SHA，并用官方 tag source SHA 核对关键模块。

禁止使用 moving `main`、未锁依赖的临时环境，或把 project patch 施加到 native baseline checkout。
native checkout/package 与 project adapter 应位于两个隔离环境；前者始终可从官方 release 重新构建。

这里的“LOTUS native”准确含义是其原生 semantic-plan execution 与 LM batching：LOTUS
`LM` 使用 LiteLLM `batch_completion(max_workers=max_batch_size)`，模型服务继续由 vLLM
FCFS/continuous batching 拥有。官方 release 当前没有 SAOR 同类的多 Job entitlement/debt
scheduler，因此报告不得写“LOTUS native multi-Job fair scheduler”。

动作：

1. 使用同一个 `LazyFrame.sem_map` logical plan 和同一 PostgreSQL manifest。
2. LOTUS native 自己执行 `SemMapNode → LM → LiteLLM batch_completion`。
3. 只配置 workload/model/endpoint/PG 等任务和环境必需项；LOTUS 的
   `max_batch_size/rate_limit` 保持官方默认或冻结官方示例值，不做性能搜索；
   cache 按共同 workload 合同冻结并记录官方来源。
4. 两 Job 场景使用两个独立 LOTUS Job 进程/上下文，共享同一 vLLM；外部 orchestration 只负责 `Job@release`，不接管 Job 内请求顺序。

完成标准：

- `scheduler_owner/upstream commit/adaptation diff` 证据完整；
- native 请求数、messages SHA、模型调用和输出语义与冻结计划一致；
- 无项目 credit/inflight/router imports；
- 不可观测的 per-request 指标明确写 `unavailable`，不由粗粒度 Job wall time伪造。

### 工作包六：接入 SAOR

前置：工作包四和五全部通过，且 frozen-static 路径没有 LOTUS 语义/证据缺口。

动作：

1. 复用同一 compiled `SemanticMapPlan` 和 project execution adapter。
2. 仅把 physical policy 从 frozen-static 切为 SAOR；LOTUS plan、messages、source/sink、模型与 Job release 不变。
3. 复用现有 bounded-ready、credit、projected-debt 与 lifecycle 证据，不在 LOTUS adapter 内实现任何调度分支。

完成标准：静态与 SAOR 两臂的 logical-plan SHA、逐行 message SHA set、workload、服务签名完全相同；不同字段只能属于预注册的 scheduler policy/evidence。

### 工作包七：是否争取 LOTUS upstream seam

仅在外部 adapter 原型通过后进行：

1. 提出最小 upstream change：只读 `LazyFrame.nodes` 或可注入的 node executor dispatch。
2. 原生默认执行行为必须 bit-for-bit parity；新增 interface 不依赖 Daft/Ray/SAOR。
3. upstream 未接受时，继续使用集中、版本锁定的 adapter；不扩成长期大 fork。

完成标准：要么获得公共 seam，要么保留一处可审计 private-interface dependency。两者都优于把项目调度 implementation 移植进 LOTUS。

## 6. Tracer bullet：允许与禁止

允许先做一个最小 `Recording/ProjectLM` adapter：LOTUS native `sem_map` 生成 messages，替代 LM
把这些 messages 交给项目 completion backend。它只回答：

> LOTUS prompt/output contract 能否在不改 semantic operator 的情况下接到项目模型调用接口？

它不能作为最终架构，因为 LM interface 看不到稳定的 DataFrame `doc_id/job_id`，且 LOTUS 已先把完整 DataFrame 转成 messages，不能验证 Daft streaming/data-organization 的完整价值。

Tracer bullet 完成后必须转向 AST lowering；不得在 LM adapter 上继续叠加 identity side channel、Ray、credit 或 SAOR。

## 7. 实验矩阵与因果问题

权威矩阵改为上位计划的单一 artifact/两个 panel：`operator_backend` 包含 LOTUS、
Daft、Daft/Ray、Ray Data、project static 和 SAOR；`native_full_path` 包含未修改 LOTUS
DataConnector、Daft/Ray 官方路径、项目现有路径与后置的 PostgreSQL row-wise HTTP UDF。
本子计划只保留其中最小的语义归因 triplet：

| Arm | 语义所有者 | 物理执行所有者 | bounded-ready | 回答的问题 |
|---|---|---|---|---|
| `lotus_native_v124_full_path` | LOTUS | LOTUS LM/LiteLLM | 否 | 未修改 LOTUS 完整产品路径 |
| `lotus_semantic_project_static` | LOTUS plan | project frozen-static | 否 | 换成项目物理执行栈但无 SAOR 的表现 |
| `lotus_semantic_saor` | LOTUS plan | project SAOR | 仅该臂 | SAOR 相对同语义、同项目栈静态参照的增量 |

### 7.1 两类结论分开

系统级：

- `lotus_native_v124_full_path` vs `lotus_semantic_saor`：可以说完整系统经验表现不同；不能把全部差值归因于 SAOR。

机制级：

- `lotus_semantic_project_static` vs `lotus_semantic_saor`：在合同配平后可归因于项目动态提交策略。

### 7.2 与现有五臂的关系

现有五臂只作 `native_full_path` 迁移前证据。下一步先把项目语义入口换为真实
LOTUS `sem_map` 合同，再构建两 panel。不得在旧数据的 semantic owner、manifest、prompt、
source/result 或服务签名不一致时拼表。Daft/Ray native 仍保持各自调度所有权，
不能为了使用 LOTUS syntax 而注入项目 executor。

## 8. 共同合同与指标

### 8.1 语义/正确性

- 同一 source query、行集合、顺序与 `doc_id/job_id`；
- 同一 LOTUS logical-plan SHA、instruction、system prompt、prompt-builder SHA；
- 每行一个模型请求；无跨行 prompt fusion；
- 同一 model/tokenizer/chat-template/temperature/output cap/EOS 合同；
- cache 全部关闭或用独立 cache identity 分组，首轮统一关闭；
- 输出逐行 readback、row count、digest、exactly-once；
- 记录调用数、input/output/total token 和质量任务指标。

### 8.2 database-E2E

- 共同记录 `model_completion_jct`（Job release→最后模型完成）与
  `query_visible_jct`（Job release→最后 SQL result/commit+readback）；
- 分列 PG fetch/materialization、semantic-plan/prompt build、organize、submit、model、fan-in、sink；
- LOTUS native 的 `pd.read_sql` 全量 materialization 与项目 Daft streaming 是系统真实差异，必须报告 host memory/CPU，不把差值全部归因于调度；
- operator-backend 使用从 `T0` 开始的有界 server-side-cursor stream，不得共同预物化完整输入；
- native-full-path 保留各产品原生 source，与 operator-backend 可比 E2E，但不做纯调度归因。

### 8.3 多 Job

- group JCT/database-E2E；
- per-Job JCT、request P50/P95/P99、SLO goodput；
- throughput、model tokens/s、service lag、最长无完成；
- GPU/vLLM time series、能耗、CPU/RSS、ready/inflight/request/work/bytes；
- unavailable 指标保留 unavailable，不用推断值补齐。

## 9. Provenance 与命名

每个 run 至少保存：

- LOTUS release、commit、package/wheel SHA、license、关键 source-layout SHA；
- logical-plan canonical JSON/SHA；
- prompt-builder source SHA 与逐行 message/request digest；
- scheduler owner、executor owner、adapter diff；
- repository commit、resolved config、manifest、service signature；
- PostgreSQL/pgvector、vLLM/model/tokenizer/chat-template identity；
- common sink readback 与 raw archive SHA。

禁止简称：

- `LOTUS + SAOR native`
- `LOTUS optimized`（除非真的运行并标明具体 LOTUS optimizer）
- `PostgreSQL native AI operator`

使用：

- `LOTUS native v1.2.4 execution`
- `LOTUS semantic frontend + project frozen-static executor`
- `LOTUS semantic frontend + SAOR executor`

LOTUS 当前是 Pandas-like semantic-operator implementation，虽可读取 PostgreSQL，但不是 PostgreSQL SQL 内核算子。若未来增加 SQL syntax/compiler，应单独命名为 project SQL frontend。

## 10. 风险与停止规则

| 风险 | 证伪方式 | 处理 |
|---|---|---|
| 私有 `_nodes` 漂移 | source-layout SHA + upgrade negative test | pin v1.2.4；集中 adapter；必要时 upstream seam |
| native/project prompt 不同 | 逐行 canonical messages SHA | 不进入性能比较 |
| row identity 在 LOTUS LM 层丢失 | 乱序/重复文本/重试 parity test | 最终使用 AST lowering，不在 LM adapter 堆 side channel |
| LOTUS optimizer 改变调用数/质量 | logical plan、call count、quality evidence | 同-work 表冻结 optimizer off；optimized LOTUS 单列 quality-cost-time |
| pandas 全量 materialization OOM | scale ramp + RSS | 作为 native 系统边界；项目路径继续 Daft streaming |
| 为集成重写 scheduler | dependency/architecture test | 停止集成，重新收窄 adapter |
| LOTUS native 无 per-request 公平指标 | request instrumentation capability gate | 标 unavailable；不伪造公平结论 |
| 三臂不足 60s/未喂饱服务 | scale ramp + feeding gate | 共同扩容 workload，不只调整某一臂 |

## 11. 研发 agent 执行纪律

每次只完成一个工作包；开始前读取本计划、研究审计、`code/AGENTS.md` 与所改目录规则。

每个工作包提交前必须报告：

1. 修改的 module/interface 与未修改的 scheduler owner；
2. 新增 positive/negative tests 及精确结果；
3. 仍 blocked 的 source/runtime/GPU 证据；
4. 是否改变 logical plan、prompt、模型调用数或质量；
5. `git diff --check`、compile、目标测试和 secret scan；
6. 当前工作区/分支/commit，且不得自动合并 `main` 或启动服务器/GPU。

每个阶段的 completion criterion 必须由 artifact/test 证明；“代码看起来接上了”不算完成。

## 12. 实施顺序摘要

```text
freeze LOTUS v1.2.4 identity
    → source-layout read-only gate
    → SemanticMapPlan + LOTUS/manifest 两个 adapters
    → prompt/output/row identity parity
    → project frozen-static capability
    → LOTUS native capability
    → isolated 3-arm smoke/rehearsal
    → SAOR incremental arm
    → independent review
    → only then request formal authorization
```

## 13. 依据

- 深入审计：`../../research/lotus_postgresql_execution_layer_fit_20260821.md`
- LOTUS paper：<https://www.vldb.org/pvldb/vol18/p4171-patel.pdf>
- LOTUS release v1.2.4：<https://github.com/lotus-data/lotus/releases/tag/v1.2.4>
- LOTUS AST：<https://github.com/lotus-data/lotus/tree/v1.2.4/lotus/ast>
- LOTUS `sem_map`：<https://github.com/lotus-data/lotus/blob/v1.2.4/lotus/sem_ops/sem_map.py>
- LOTUS LM：<https://github.com/lotus-data/lotus/blob/v1.2.4/lotus/models/lm.py>
- LOTUS database connector：<https://github.com/lotus-data/lotus/blob/v1.2.4/lotus/data_connectors/connectors.py>
