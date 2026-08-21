# LOTUS 语义前端 + PostgreSQL + 项目物理执行层可行性审计

更新日期：2026-08-21

## 0. 审计结论

### 0.1 Go / No-Go

**有条件 Go。** 可以把开源 LOTUS 用作类似 Cortex AISQL 的**声明式 AI 语义算子前端**，把
`sem_map` 降低（lower）为项目自己的逻辑算子合同，再由 PostgreSQL source/sink、Daft/Ray 和
SAOR 完成物理执行。这样既避免继续把主方法表述为“自写 UDF”，又能保留 LOTUS native 作为以后
可运行、可复现的数据库 AI 系统 baseline。

但是，当前 LOTUS 稳定版并没有公开的 physical-backend/executor plugin。准确边界是：

- **Go**：首版只支持 row-preserving `sem_map`，使用固定版本的薄 plan adapter，不修改 LOTUS
  源码；LOTUS native 作为独立 baseline。
- **Conditional Go**：先用公开 `BaseOptimizer.optimize(nodes)` 验证是否能在不执行计划的前提下取得
  完整 node list；但该接口的返回值仍是 node list，不是外部 IR。若产出 Project IR 必须依赖隐式
  side effect，就不把它包装成“公共 backend seam”，而改用集中、只读且版本锁定的 `_nodes`
  adapter。两条原型路线都须锁定 LOTUS commit、AST/source-layout SHA、golden prompt 和 fail-closed
  合同；长期正确方向仍是向 LOTUS upstream 提交正式 plan visitor 或 executor hook。
- **No-Go**：现在就声称“LOTUS 官方支持 Daft/Ray/SAOR backend”，或把任意 `sem_filter`、
  `sem_join`、`sem_agg`、cascade/optimizer 全部交给项目执行而仍称 LOTUS native。

### 0.2 最终推荐分层

```text
PostgreSQL relation / Job declaration
    → LOTUS LazyFrame semantic operator（逻辑语义前端）
    → version-locked LotusPlanLowerer
    → Project SemanticOperatorIR / row request descriptors
    → Daft source + work-unit organization
    → Ray + frozen-static 或 SAOR submission control
    → vLLM native FCFS / continuous batching
    → PostgreSQL result/status sink + readback audit
```

这里 LOTUS 只拥有**算子声明和语义合同**；项目拥有**物理数据执行、请求组织与提交调度**；vLLM
拥有模型服务内部调度。三层 scheduler owner 不混写。

### 0.3 必须收紧的一句话

> 本项目采用 LOTUS `sem_map` 作为外部 DataFrame 式语义算子前端，并实现一个版本锁定的
> Daft/Ray 物理执行 backend；这不是 PostgreSQL 内核原生 SQL 算子，也不是 LOTUS 官方 backend。

LOTUS 论文明确说当前初始实现是 Pandas-like API，同时语义算子也可以加入 SQL 等其他查询语言；
因此使用 LOTUS 可以摆脱“任意 Python UDF”的定位，但在没有实现 PostgreSQL parser/planner extension
前，不能称为 Cortex 那样的**数据库内原生 SQL `AI_COMPLETE`**。

**来源类型：论文 + 源码审计 + 合理推断。** LOTUS 的语义算子是正式声明式模型；当前产品接口仍是
Pandas/LazyFrame，而不是 PostgreSQL SQL grammar。

## 1. 固定证据边界

本审计区分论文系统与当前稳定源码，不能用 2026 年新增接口倒推 2025 年论文实现。

| 对象 | 冻结身份 | 用途 |
|---|---|---|
| LOTUS 论文 | PVLDB 18(11), 2025，DOI `10.14778/3749646.3749685` | 语义算子定义、reference algorithm、论文实验 |
| LOTUS 稳定源码 | `v1.2.4`，commit `b1a85fd7a66fabed8a1585d44d7597d592b4433f` | AST、runner、LM、connector 扩展点审计 |
| LOTUS license | Apache License 2.0 | 允许外部适配或 fork，但不代表存在公共 backend API |

**来源类型：官方论文、官方 release、官方源码。** [PVLDB 论文](https://www.vldb.org/pvldb/vol18/p4171-patel.pdf)、
[v1.2.4 release](https://github.com/lotus-data/lotus/releases/tag/v1.2.4)、
[固定提交](https://github.com/lotus-data/lotus/tree/b1a85fd7a66fabed8a1585d44d7597d592b4433f)、
[Apache-2.0 license](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/LICENSE)。

## 2. LOTUS 语义是否适合充当 AI 算子前端

### 2.1 `sem_map` 确实不是普通 UDF 语义

论文把 semantic operator 定义为“由自然语言表达式参数化、作用于一个或多个数据集的声明式
变换”，并提出 model-data independence：应用逻辑与底层模型调用算法分离。论文表 1 中
`sem_map` 是自然语言 projection；其 reference algorithm 对每个 tuple 计算一次模型变换。论文还
明确区分低层 AI UDF/batched inference primitive 与更高层、可有多种物理算法的 semantic operator。

因此，将用户入口写成：

```python
plan = LazyFrame(schema={"doc_id": "int64", "prompt": "object"}).sem_map(
    "Complete the task described by {prompt}",
    suffix="completion",
)
```

比自写 `@daft.udf` 或 Python HTTP 函数更符合数据库 AI 算子定位：用户声明的是 `sem_map`
语义，物理执行器可以更换。

**来源类型：论文。** 论文 §2 定义 semantic operator 和 model-data independence；表 1 将
`sem_map` 定义为 projection；§4 说明 LOTUS 是 Pandas extension，且语义算子可以加入 SQL 等接口。
[论文全文](https://www.vldb.org/pvldb/vol18/p4171-patel.pdf)。

### 2.2 多行一起组织不违反逐行语义

论文的 `sem_filter` reference algorithm 是对全部 tuples 运行 batched LLM calls，但每次 model
invocation 只包含一个 tuple，以避免长上下文问题；`sem_map` 同样逐 tuple 计算。这里的 “batched”
是并发/物理提交方式，不是把多行融合为一个 prompt。

所以项目可以把多行放进一个 ready window、token-budget work unit 或 Ray submission group，只要：

1. 每行仍对应独立 chat request 和独立 completion；
2. 不做跨行 prompt fusion；
3. `doc_id/job_id → request_id → completion` 可逆；
4. 输出按原行 identity 写回且 exactly-once。

**来源类型：论文 + 源码 + 合理推断。** [论文 §2](https://www.vldb.org/pvldb/vol18/p4171-patel.pdf)；
当前 `sem_map` 也先逐行构造 `inputs`，再一次把 messages 列表交给 LM：
[sem_map.py L78-L118](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_map.py#L78-L118)。

## 3. 当前稳定源码到底提供了什么

### 3.1 Eager 路径

`DataFrame.sem_map(...)` 是注册在 Pandas DataFrame 上的 accessor。执行时它：

1. 从 langex 解析引用列；
2. 按 DataFrame 行序构造 multimodal records；
3. 为每行生成一套 chat messages；
4. 调用 `lotus.settings.lm(messages)`；
5. 将返回的 outputs 按位置写入 DataFrame 副本的新列。

**来源类型：固定版本源码。**
[SemMapDataframe](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_map.py#L121-L279)、
[row-to-multimodal conversion](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/templates/task_instructions.py#L340-L387)、
[map formatter](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/templates/task_instructions.py#L221-L262)。

### 3.2 LazyFrame / AST 路径

`v1.2.4` 已有结构化 LazyFrame：

- `LazyFrame.sem_map()` 构造 Pydantic `SemMapNode`；
- `BaseNode`、`SemMapNode`、`LazyFrameRun` 都从 `lotus.ast` export；
- `BaseOptimizer` 是公开 ABC，`LazyFrame.optimize()` 会把 node list 传给自定义 optimizer，因此可把
  一个自定义 optimizer 用作有界 lowering pass；
- 但是 `LazyFrame` 的 plan 存在私有字段 `_nodes`，没有公开 `nodes()`、visitor、`to_ir()` 或
  backend registry。

这意味着 LOTUS 已有很好的**逻辑计划形态**，但还没有稳定的外部 physical-backend 合同。

**来源类型：固定版本源码。**
[LazyFrame.sem_map](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/lazyframe.py#L318-L345)、
[SemMapNode](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/nodes.py#L453-L494)、
[AST exports](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/__init__.py#L1-L60)、
[BaseOptimizer](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/optimizer/base.py#L1-L40)。

### 3.3 为什么不能说已有 executor plugin

源码调用链是硬连接的：

```text
LazyFrame.run()
    → 固定构造 LazyFrameRun
LazyFrameRun.execute()
    → 顺序遍历 _nodes
    → node(current_df, resolver, ...)
SemMapNode.__call__()
    → 直接调用 df.sem_map(...)
```

没有发现把 `SemMapNode` dispatch 给用户提供的 Daft/Ray executor 的公共参数、protocol 或 registry。
可以自写 `BaseOptimizer`，在收到 node list 时校验并生成 Project IR；这能避免直接读取 `_nodes`，
但它仍是 project-defined lowering pass，不是 LOTUS 官方 executor。optimizer 本身不替换 runner，也
不提供异步 request lifecycle。

**来源类型：固定版本源码审计。**
[LazyFrame.run](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/lazyframe.py#L762-L795)、
[LazyFrameRun.execute](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/run.py#L128-L160)、
[SemMapNode.__call__](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/nodes.py#L472-L491)。

### 3.4 模型调用与 batching 的 owner

LOTUS `LM` 不是抽象 backend interface，而是一个具体 LiteLLM wrapper。默认路径调用
`litellm.batch_completion(..., max_workers=max_batch_size)`；它还拥有 cache、RPM/TPM rate
limit、usage accounting 和 batch 切分。官方示例通过 `hosted_vllm/...` 和 OpenAI-compatible
endpoint 接入 vLLM。

因此原生 LOTUS baseline 的上游 client batching owner 是 LOTUS/LiteLLM，模型服务内部 scheduler
owner 是 vLLM；不能把 LOTUS 的 LM 换成 SAOR 后仍称“LOTUS native”。

**来源类型：官方文档 + 固定版本源码。**
[LOTUS LM 文档](https://lotus-ai.readthedocs.io/en/stable/llm.html)、
[LM class](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/models/lm.py#L81-L175)、
[batch_completion call](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/models/lm.py#L261-L302)。

### 3.5 PostgreSQL connector 的边界

当前 connector 仅通过 SQLAlchemy 建 connection，再用 `pd.read_sql(query, conn)` 返回完整 Pandas
DataFrame。它不是 PostgreSQL 内部算子、不是流式/分区 Daft source，也没有配套的结果 sink、Job
lease、exactly-once 或多 Job scheduler。

所以推荐保留项目现有 PostgreSQL source/sink 合同，并把 LOTUS 作为查询之后、物理执行之前的
semantic plan；不要为了“使用 LOTUS”把成熟的 Daft source/readback 证据退化成全量 Pandas
materialization。

**来源类型：官方文档 + 固定版本源码 + 合理推断。**
[Database connector 文档](https://lotus-ai.readthedocs.io/en/stable/data_connectors.html)、
[connectors.py L10-L28](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/data_connectors/connectors.py#L10-L28)。

## 4. 三类集成边界

| 分类 | 能做什么 | 判断 |
|---|---|---|
| 无需改 LOTUS 源码、使用公共能力 | 用 `LazyFrame.sem_map` 声明语义计划；运行 LOTUS native；使用公开 node 类型做类型核对 | 立即可做 |
| 版本锁定的薄 adapter | 先测试 `BaseOptimizer` 能否干净导出计划；否则集中只读 `_nodes`。两者都只接受 `SourceNode → SemMapNode`，lower 到项目 IR，并用固定 formatter 生成 golden prompt、绑定 `doc_id/job_id` | 首版推荐，但必须 fail-closed，且不得称官方 backend hook |
| 需要 upstream hook 或维护 fork | 公共 plan visitor；可插拔 `PhysicalExecutor`；metadata-carrying LM request protocol；通用 sem_filter/join/agg/cascade 外部执行；streaming source | 不阻塞首版，阻塞“官方 backend”宣称 |

### 4.1 推荐的首版 `LotusPlanLowerer`

先实现一个最小 `LotusPlanProbe(BaseOptimizer)`，验证 LOTUS 的公开 `optimize()` 是否能把完整 node
list 交给项目代码且不触发执行。随后只接受以下计划并生成 typed Project IR：

```text
SourceNode(expected_schema=...)
    → SemMapNode(
          user_instruction,
          system_prompt,
          suffix,
          examples,
          strategy=None,
          safe_mode=False,
          model_kwargs=frozen_generation_contract
      )
```

然后生成项目自有的 typed IR：

```text
SemanticMapSpec
  operator_identity = lotus.sem_map
  lotus_version / commit / AST fingerprint
  input_schema / referenced_columns
  langex / system_prompt / examples / serialization_format
  output_column / output_schema
  model / tokenizer / chat-template / generation contract
  source query identity
```

下游每行生成：

```text
SemanticMapRequest
  matrix_instance_id / job_id / doc_id / request_id
  prompt messages + prompt digest
  estimated work + output cap
  completion / finish reason / actual work
```

如果 `BaseOptimizer` 只能通过有状态 collector 或特殊 marker 旁路带出 IR，则首版正式 adapter 改为
集中、只读访问 `_nodes`，并由版本、commit 与 source-layout SHA 锁定。业务 runner、scheduler 和
summary 不得直接访问私有字段。无论选择哪条路线，都不调用 `LazyFrame.run()/execute()`。

adapter 发现未知 node、多步 plan、optimizer 已改 prompt、cascade、cache 或无法解释的 model kwargs 时，
必须 fail closed，而不是回退到 eager LOTUS 执行。

**来源类型：源码支持的工程设计。** `BaseOptimizer` 和 `SemMapNode` 是公开 export，前者能把 node
list 暴露给自定义 optimizer；但它只承诺 `nodes → nodes`，不承诺导出外部 IR。“optimizer 产出
Project IR”因此必须先由原型验证，不能预先当作 physical-backend 协议。最终无论采用 optimizer
probe 还是只读 `_nodes` fallback，都称 project adapter，而非 upstream-supported plugin；fallback
必须单独标记 private-API dependency。

### 4.2 为什么 `ProjectLM` 只能做原型/对拍工具

也可以临时把一个 duck-typed `ProjectLM` 放进 `lotus.settings.context(lm=...)`，截获 LOTUS 已构造的
messages，再提交给 SAOR。这个办法不改 LOTUS 源码，并适合做 prompt golden/parity test；当前
`Settings.configure/context` 对 `lm` 没有运行时类型验证。

但不建议把它作为最终 physical backend：

1. 拦截发生在 DataFrame 已 materialize、LOTUS 已构造完整 messages 列表之后；
2. `LM.__call__` 输入没有 `job_id/doc_id/request_id`，只能依赖列表位置旁路关联；
3. Daft/Ray 没有真正拥有 source-to-request 的完整数据执行路径；
4. LOTUS 没有公开 `LM Protocol`，只有具体 `LM` 类，duck typing 兼容性不是稳定合同。

因此 `ProjectLM` 只用于证明“LOTUS prompt 可被项目无损消费”，正式实现使用 plan lowering + typed
request identity。

**来源类型：固定版本源码 + 工程推断。**
[Settings.context](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/settings.py#L15-L70)、
[LMOutput](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/types.py#L10-L17)。

## 5. Row identity、provenance 与正确性

LOTUS `sem_map` 当前按 DataFrame 行序建立 prompt 列表，并把 outputs 按列表位置写回 DataFrame
副本，因此单次 eager 调用能保持位置对应。但它没有把数据库主键或 Job identity 放进 LM request；
PostgreSQL connector 返回的默认 Pandas index 也不是数据库稳定 identity。

项目集成必须：

1. SQL 显式选择不可变 `doc_id`，并用 `ORDER BY doc_id` 或 manifest order；
2. 不把 Pandas RangeIndex 当外部 identity；
3. lower 后在 prompt 之外绑定 `job_id/doc_id/request_id`，避免把内部 ID 注入模型语义；
4. 验证 `len(row_ids) == len(messages) == len(completions)`；
5. 保存每行 prompt digest、output digest、finish reason 与 actual token usage；
6. sink 继续执行行数、唯一键、readback digest 和 exactly-once 门禁。

另一个固定版本风险是 `parse_cols()` 当前通过 `list(set(matches))` 生成引用列列表，多列 langex 的
列序可能跨进程变化。首版应只允许一个 prompt 列，或把最终 messages digest 作为合同并做跨进程
golden test；不能只比较原始 langex 字符串。

**来源类型：固定版本源码 + 工程推断。**
[parse_cols](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/nl_expression.py#L1-L21)、
[sem_map positional writeback](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_map.py#L234-L279)。

## 6. 如何既使用 LOTUS，又公平地与 LOTUS 比较

### 6.1 Matched semantic-execution 三臂

| 实验臂正式名称 | Semantic frontend | Physical execution / scheduler owner | bounded-ready |
|---|---|---|---|
| `LOTUS v1.2.4 native sem_map` | LOTUS | LOTUS LM/LiteLLM + vLLM | 否 |
| `LOTUS semantic frontend + project frozen-static executor` | LOTUS plan lowered to project IR | Daft/Ray project static + vLLM | 否 |
| `LOTUS semantic frontend + project SAOR executor` | LOTUS plan lowered to project IR | Daft/Ray + SAOR + vLLM | 仅 proposed 使用 |

三臂冻结相同：PostgreSQL rows/order、langex、LOTUS commit、serialization format、prompt messages
digest、model/tokenizer/chat template、generation parameters、vLLM service signature 和 PG sink。

第一臂必须是 **unmodified LOTUS native release**：正式实验使用官方 `v1.2.4` tag/commit
`b1a85fd7a66fabed8a1585d44d7597d592b4433f`，或能证明内容完全相同的 `lotus-ai==1.2.4`
wheel + artifact SHA；禁止使用 moving `main`，禁止应用项目 patch，禁止用 `ProjectLM` 替换 LOTUS
`LM`。其准确执行链是：

```text
LOTUS sem_map
    → LOTUS LM
    → LiteLLM batch_completion(max_workers=max_batch_size)
    → vLLM FCFS / continuous batching
```

它应命名为 `LOTUS native execution/batching`，不能虚构为 `LOTUS native multi-job fair scheduler`：
当前源码没有多 Job fair queue。外部共同 harness 只允许负责冻结的 Job release、共同 PostgreSQL
source/sink 适配和观测；release 后 Job 内请求顺序、并发与 batching 必须继续由 LOTUS/LiteLLM 拥有，
不得加入项目 credit、bounded-ready、inflight controller 或重排。

为了回答“相同 semantic map work 下物理执行谁更好”，此表应关闭 LOTUS cache，不调用
`LazyFrame.optimize()`，不启用 helper model/cascade，并验证每行一次模型调用。LOTUS native 不强塞
项目 K/W/ready window；它保留原生 `max_batch_size`/LiteLLM 行为。项目 static 与 SAOR 才共享项目
资源包络。

比较 database-E2E throughput/group JCT、per-Job JCT/P99/SLO、实际 calls/input-output tokens、
service lag/最长无服务、GPU/vLLM time series、错误、行数/digest/exactly-once 与任务质量。

### 6.2 Full-system LOTUS 比较必须另表

如果让 LOTUS optimizer 使用 prompt optimization、cascade、proxy、semantic search/join 等能力，它
可能改变 prompt、模型、调用数、tokens 和结果质量。这是有意义的系统级 comparison，但回答的是：

> 完整 LOTUS 系统与完整项目系统在 quality/calls/tokens/cost/JCT 上的最终 Pareto 表现如何？

它不能回答 SAOR selector 是否更好，不能把“减少模型调用”的收益归给物理调度，也不能和上面的
same-work 三臂混成一个排名。应另报 quality threshold、model calls、tokens、money cost、wall time 和
失败率。

**来源类型：论文 + 实验设计推断。** LOTUS 的核心研究杠杆本来就是在 accuracy guarantee 下选择
不同 AI algorithms/减少 cost；SAOR 的杠杆是在冻结 work 后调度 ready work。二者都值得比较，但
因果问题不同。[PVLDB 论文](https://www.vldb.org/pvldb/vol18/p4171-patel.pdf)。

### 6.3 与当前五臂矩阵的关系

这组三臂是新增的 semantic-frontend/backend attribution，不应推翻当前 Daft Native、Daft/Ray、
Ray Data、project frozen-static、SAOR 五臂 database-E2E 合同。先闭合现有实验，再新增 LOTUS
capability；否则同时更换前端语义、prompt、source materialization 和 scheduler，会失去已有证据的
连续性。

## 7. 最小原型与晋级门禁

### 7.1 本地无 GPU capability

只实现并验证：

1. pin `lotus-ai==1.2.4` 与完整 commit/source SHA；
2. 构造 `LazyFrame().sem_map(...)`；
3. 通过自定义 `BaseOptimizer` lower 只包含 `SourceNode → SemMapNode` 的计划，不直接读取 `_nodes`；
4. 生成 `SemanticMapSpec` 与逐行 typed requests；
5. 用 fake deterministic completion 验证 output order、`doc_id/job_id`、重复/缺失和 fail-closed；
6. 用 recording LM 对拍 LOTUS native messages，要求逐字节 prompt digest 一致；
7. 对未知 node、优化 plan、多列顺序漂移、source-layout SHA 漂移构造反例测试。

### 7.2 服务器 capability（不是 formal）

服务器恢复后才做一个小型 2-Job gate：

- 相同 PG source、同一 LOTUS `sem_map`、同一 vLLM、共同 sink；
- 一个短 Job、一个长 Job，错峰 release；
- 先跑 LOTUS native 和 project frozen-static，确认 prompt/call/output/correctness parity；
- 再跑 SAOR，仅验证 identity、request lifecycle、无提前 release 和机制事件闭合；
- 不在线调参，不直接进入 1+3 formal。

### 7.3 晋级条件

全部满足才进入 rehearsal：

- LOTUS 源码零修改，adapter 独立且职责有界；
- AST/source/prompt fingerprint 全部绑定 fixed commit；
- 100% row/request/completion join；
- native 与 project prompt digest、模型调用数、输入 rows 完全一致；
- 输出 schema、行数、readback digest、exactly-once 通过；
- 未知 plan fail closed；
- native scheduler owner 与 project scheduler owner 在 evidence 中明确；
- feeding/preflight 按项目现有合同通过。

任一失败都只说明当前 adapter/capability 未闭合，不能推断 LOTUS 或 SAOR 性能。

## 8. 最终判断

### 可以声称

- LOTUS 提供开源、正式定义的 semantic operator model，适合作为项目 AI 算子语义前端。
- 项目可以在不修改 LOTUS 源码的前提下，为固定 `sem_map` 子集实现版本锁定的物理 plan adapter。
- LOTUS native 与 `LOTUS frontend + project executor` 可以构成公平且有解释力的对比。
- 多行一起作为物理 work unit 不违反一行一次语义调用，只要不做 prompt fusion。

### 暂时不能声称

- PostgreSQL 已获得原生 LOTUS/Cortex 式 SQL AI operator。
- LOTUS v1.2.4 官方提供 Daft/Ray/SAOR physical backend plugin。
- 自定义 optimizer lowering 等于 LOTUS 官方认可的 external IR/backend；它仍是项目适配方式。
- fallback 使用 LOTUS 私有 `_nodes` 的 adapter 是跨版本稳定公共接口。
- `LOTUS semantic frontend + SAOR` 仍是 LOTUS native。
- 全 LOTUS optimizer 与 SAOR 的 wall-time 差异可以单独归因于调度。

### 对项目方向的影响

这条集成不会推翻项目方向，反而能把论文对象说得更准确：

> 研究的不是任意 Python UDF，而是声明式 semantic AI operator 的外部分布式物理执行；LOTUS
> 提供算子语义和可运行系统基线，项目贡献是 Daft/Ray 上的 work-unit organization 与多 Job
> state-aware submission control。

它的风险主要是工程接口稳定性，而不是研究问题冲突。首版把范围限制为 `sem_map/AI_COMPLETE`
即可；泛化到 filter/join/aggregate 应成为后续工作，不阻塞当前 SAOR 证据。

## 9. 一手来源索引

- LOTUS PVLDB 2025 论文：<https://www.vldb.org/pvldb/vol18/p4171-patel.pdf>
- LOTUS 官方仓库：<https://github.com/lotus-data/lotus>
- LOTUS v1.2.4 release：<https://github.com/lotus-data/lotus/releases/tag/v1.2.4>
- LOTUS v1.2.4 固定 commit：<https://github.com/lotus-data/lotus/tree/b1a85fd7a66fabed8a1585d44d7597d592b4433f>
- LOTUS `sem_map` 文档：<https://lotus-ai.readthedocs.io/en/stable/sem_map.html>
- LOTUS LazyFrame 文档：<https://lotus-ai.readthedocs.io/en/stable/lazyframe.html>
- LOTUS LazyFrame optimizer 文档：<https://lotus-ai.readthedocs.io/en/stable/lazyframe_optimizations.html>
- LOTUS LM/vLLM 文档：<https://lotus-ai.readthedocs.io/en/stable/llm.html>
- LOTUS database connector 文档：<https://lotus-ai.readthedocs.io/en/stable/data_connectors.html>
- 固定源码路径：
  - [`lotus/ast/lazyframe.py`](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/lazyframe.py)
  - [`lotus/ast/nodes.py`](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/nodes.py)
  - [`lotus/ast/run.py`](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/ast/run.py)
  - [`lotus/sem_ops/sem_map.py`](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/sem_ops/sem_map.py)
  - [`lotus/models/lm.py`](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/models/lm.py)
  - [`lotus/data_connectors/connectors.py`](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/data_connectors/connectors.py)
