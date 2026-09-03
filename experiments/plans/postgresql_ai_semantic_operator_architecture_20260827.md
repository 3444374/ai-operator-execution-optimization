# SemLoom PostgreSQL 内置 AI 语义算子整体架构与实施计划

更新日期：2026-09-03
状态：`current / design-revised / implementation-in-progress`

本文只维护架构决策、Module 职责、Interface、工作包依赖与完成条件。源码实际状态看
[INFRA_STATUS](../../code/INFRA_STATUS.md)，测试数字、提交身份、运行配置与失败看
[证据台账](../results/EXPERIMENT_EVIDENCE_REGISTRY.md)及结果目录；理论与文献依据看
[架构研究](../../research/sema_native_semantic_operator_architecture_reference_20260827.md)。

四 C 的值合同、PG plan、中立 open spec、wire v4/gateway 接线及受限 Filter INSERT 已完成；
受控资源与[受限真实服务检查](../results/postgresql/choice_service_20260902/README.md)均已通过。
实现与归档已合入本地 main。具体提交与验证范围由 INFRA_STATUS 和结果记录维护，
工程完成不表示模型质量、真实成本校准或优化路径已通过。
真实生成型 SemMap、增量 SchedulingSession 与自有成果向公司的移植均不因文档存在而视为完成。
四 C 的字段、预算和逐项测试只由[专项计划](completed/postgresql_choice_profile_engineering.md)维护。
四 D 的[生成型 Map 合同](postgresql_semmap_generation_contract.md)已定稿；具体 SQL、消息、输出、
版本与验收只在该文维护，仍待研发源码复核和实施，不扩大当前已支持能力。
旧串行顺序、完整资格尝试条件和历史数字保存在[历史快照](archive/postgresql_ai_semantic_operator_architecture_serial_20260901.md)，
不再作为当前执行指令；原始结果没有删除或改判。

## 1. 目标与架构决策

研究对象保持为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
自有 PG18.3 `semloom_pg` 与 SemLoom 数据执行/调度都继续完成。现在参考公司 demo 减少算子工程的
重复探索；未来可把自有算子语义、处理/优化方法及 SemLoom 执行能力移植到公司系统。
算子方法进入目标 planner/executor，执行能力通过 Adapter 接入同一核心；两者不能缩减为一次 provider 连通。

工程对比覆盖 SQL 注册、PG 接入方式、算子语义与物理策略、请求构造、取数/结果映射、生命周期和
外部执行，不限于多个算子能否同时出现。采用公司经验的判断标准是能否服务上述目标、减少重复实现
并保持可验证的职责；不以函数数量、目录相似或复用率作为完成标准。LOTUS/Cortex-like 算子优化与
SemLoom 数据执行研究都保留，公司 demo 的现有能力不是自有系统的上限。
公开扩展 pgml 补充提供 SQL 接口与模型能力封装的工程参照，采用范围与验收见
[§8.8](#pgml-engineering-reference)；不成为项目依赖，也不改变上述研究目标与工作包次序。

数据库拥有 SQL、ordinary child plan、语义计划、结果解释及查询生命周期。模型、Python、Ray、
vLLM 位于进程外；“内置”不表示 payload 不会离开数据库。直接复用 PostgreSQL 的 MVCC、事务、锁、
WAL、snapshot、ACL/RLS、executor 和错误机制，项目只适配新增资源与任务的生命周期。

载体锁定 `REL_18_3`，默认使用 extension 的 marker + CustomPath/CustomScan。针对实际目标的
plan identity、placement 或 executor lifecycle 出现可复现阻断后，才考虑解除该阻断的最小 core patch；
不为“更原生”扩 grammar、storage 或模型 runtime。PG18.4 结果不能替代目标版本的验证。

## 2. 并行研发与接入依赖

以下是可以分别推进的工程工作，不是新增三个研究内容，也不要求立即同时启动三个代码任务。
计划中的后续代码、模型运行、正式实验与公司环境操作仍按具体任务授权，计划存在本身不构成授权。

| 工作 | 近期次序 | 不再等待什么 | 仍需满足什么 |
|---|---|---|---|
| 自有 PG 语义算子 | 完整工程对照与四 C 收尾 → 四 D 真实生成型 SemMap / 必要公共整理 → 可组合执行；Filter 质量、校准、第二路径独立推进 | 真实生成型 SemMap 不等待 Filter 分类质量或先实现多算子；PG 基础检查随受影响路径开展 | 各算子的语义定义、PG18.3 生命周期与兼容性验证 |
| SemLoom 执行与调度 | 现有行为表征 → fixture/sealed-task 驱动增量 session → work organization → 有界提交、多 Job 与路由 | 独立核心不等待 Filter 质量、matched artifact、第二路径或完整 carrier audit | 现有代码行为兼容、资源上限、终态、取消、关联正确性；真实实验另有计划 |
| 公司工程参照与自有成果移植 | 自有开发前按模块参考 demo → 有实际疑点时做 deterministic spike → 按需分别移植算子方法与执行能力 | 工程参照现在进行，不等待自有全部功能完成；公司移植不阻塞主线 | 来源/权限、算子目的与语义差异、目标 planner/executor 和执行 Interface、获批环境 |

```text
PG：完整对照/四 C -> 四 D + 逐路径审查 -> 可组合执行 --+
                                                   +-> 自有 PG + SemLoom 接入验证
SemLoom：旧行为表征 -> 增量核心 -> 有界执行测试 -----+

Filter：reference 质量 -> matched cost -> 第二路径 -> 优化相关 carrier audit
公司经验 -> 当前自有实现；自有方法 -> 必要的最小 spike -> 按需向公司系统移植
```

不能把研发允许与结果可声称混在一起：

| 结果类别 | 纳入该类别前的条件 |
|---|---|
| 独立核心/fixture 结果 | 标明 producer、fake clock/adapter 或外部 workload；只证明所测状态、组织或调度行为，不证明数据库接入 |
| 自有 PG + SemLoom 功能接入 | 真实语义算子、provider、对应 schema/wire、PG18.3 snapshot/权限/取消/结果/资源检查均通过；不要求先完成无关的 Filter 第二路径 |
| PG 匹配端到端与 batch-placement 比较 | 上述接入通过；同 semantic plan/task set、模型/生成参数、服务/资源、计时和适合任务的质量要求；有预先登记的实验条件 |
| Filter reference/optimized 比较 | 该 Filter 的独立语义质量、matched reference calibration、近似授权/质量证据/fallback 及两路径 carrier 检查通过 |
| 公司接入/环境结论 | 只限实际测试的前端、Interface 与获批环境；不能由自有 PG 或一次 fixture 推广成任意数据库/生产部署支持 |

已验证的 PG reference 能力仍可按原证据描述；新增 SemLoom 集成不能借用旧结果冒充完成。
已有外部 profiler/manifest/Daft/Ray/static/SAOR 证据保持原身份；并行研发不自动恢复旧 GPU 矩阵、SAOR
调参或 formal。新的独立核心性能实验也须先写具体 workload、baseline、资源和停止条件。

## 3. 目标执行关系

```text
SQL -> PostgreSQL planner / ordinary child
       -> operator machine + PG-private pump/runtime
       -> neutral provider seam
          -> gateway execution Adapter
             -> fixed endpoint reference
             or SemLoom organization / admission / routing / scheduling
                -> replaceable Ray / model execution Adapter
       <- completion validation / parser / tuple binding
       -> downstream SQL or INSERT
```

这是目标关系；当前 recording/golden/fixed-model、待接入的 SemLoom 和未来公司前端的实际完成度只看
INFRA_STATUS。数据库决定产生什么 AI work，SemLoom 决定已定语义的 work 如何组织和执行。
provider 不接收 SQL/Plan、不重新连库拉取数据，不改 prompt/parser/关系语义，不把失败默认为 NULL 或成功。

## 4. Module 职责与复用

| Module | Interface 后隐藏的职责 | 不拥有 |
|---|---|---|
| PostgreSQL 原生设施 | ordinary child、snapshot、权限、事务、statement cancel、结果写回 | 外部模型计算的回滚 |
| planner / plan spec | marker 识别、合法 placement、版本化 semantic/physical identity、路径与估计 | 网络调用、在线模型采样、provider session |
| thin CustomScan / pump | callback、child pull、slot/Datum binding、有限 tuple context、emit/drop 驱动 | 模型 HTTP、调度策略、wire codec |
| OperatorMachine | 本算子的 task 编译、raw result parser 与关系 disposition | PG slot/Plan、FD、provider 选择、query cleanup |
| PgSemanticRuntime | query-fixed provider、lazy open、sequence、completion copy、错误映射、cleanup 与公共 EXPLAIN | transport-specific operation、Filter 真值、动态 cost 计算 |
| UDS/wire Adapter | FD、latch/interrupt wait、编码/字段验证、摘要与协议关联 | SQL、模型 HTTP、SemLoom 策略 |
| gateway / execution Adapter | 协议到执行实现的适配、模型配置与服务请求映射、返回原始完成结果 | 重新读取 PG、改写数据库语义、隐式 retry |
| SemLoom 核心 | work 画像/组织、提交、路由、多 Job 资源与执行状态 | 两套 PG 的专用类型、供应商请求字段、语义质量判定 |

PG 只增加 lazy UDS client 和新增资源清理；listener、模型连接与分布式执行在外部。复用 PG 的
MemoryContext、external-FD accounting、executor callback 和 longjmp 机制，不重写数据库资源管理器。
共享 Module 以两个真实消费者、变化原因与 Interface 测试为依据；不是按行数机械拆分。

## 5. 语义、计划与逐项数据

### 5.1 算子范围

- 一参 `ai_semantic.map(text)` / `filter(text)` 继续是 recording 兼容入口。
- Filter 的目的为按自然语言条件保留满足条件的行。当前三参 exact Filter 使用数据库保存的
  instruction、prompt/parser、model/generation，并接受 TRUE/FALSE/UNKNOWN 模型输出；SQL 只保留 TRUE。
  三值是当前 profile 的选择，不是所有 Filter 的必要条件；二值及公司行为的对照见 §8.7.2。
  模型链路可运行不等于自然语言判断质量已通过。
- 四 D 为真正的 row-preserving 生成型 SemMap 提供独立定义；不能把 recording echo 改名为 AI_COMPLETE。
- Filter 第二路径仍是必需研究工作；Join、aggregate、order、fusion 等只在各自立项后设计。

SQL marker 必须在受支持形状下 lower 为 planner-visible 算子；未 lower 时明确拒绝，不能静默退回
逐行 HTTP UDF。已支持 SQL、常量要求和实际选项值以[extension README](../../code/postgres/semloom_pg/README.md)
及源码/测试为准，本文不复制第二份完整语法手册。

### 5.2 PG-owned SemanticPlanSpec

计划保存当前算子真正消费的逻辑语义、prompt/parser/model/generation、NULL/error/order policy 与
physical algorithm/role；使用可复制、严格解码的 PG plan data。列号、TupleTableSlot 和 snapshot
是 executor 本地 binding。cost estimate 是独立 planner metadata，不伪装成 semantic identity。

planned choice profile 必须自包含并进入新 semantic digest；provider identity 与模型执行实现另行绑定。
不同 prompt/parser/UNKNOWN 含义不共用语义身份。只有出现真实 alternative 时，才加入被 planner
消费的 quality/evidence/threshold/fallback；EXACT 不得被 cost optimizer 偷换成近似语义。

### 5.3 当前中立 Interface：open spec 与 per-item task 分开

以下是当前 C seam 的分工，精确定义见[ai_provider_port.h](../../code/postgres/semloom_pg/src/ai_provider_port.h)：

| 对象 | 当前字段职责 |
|---|---|
| `AiOpenSpec`（query-fixed） | operator/value kinds、policy、schema/spec identity、algorithm/role、prompt/parser/model identity、semantic/physical digests，以及 `temperature/top_p/max_tokens/n/stream/stop`；choice 路径另持有完整 generation profile |
| `AiPreparedTask`（per-item） | `sequence`、`input`、`canonical_messages`、`semantic_payload_digest`、`is_null` |
| `AiCompletion`（per-item） | `sequence`、`output`、`response_model_id`、`finish_reason`、`prompt_tokens/output_tokens`、`is_null` |
| `AiProviderError` | caller-owned 中立分类、errno、限长脱敏详情及必要固定宽度参数 |

model 与 generation constraints 不逐行复制成 task 字段。本表列出公共 C seam；四 C 实现
已把 profile 接入 PG plan、中立 open spec 和 wire v4，fixture 执行已接通；
新 profile 已另有受控 fixture 资源和受限真实模型接入证据，不表示分类质量已合格。
`PreparedSemanticTask/CompletionRecord` 只在旧设计中作为概念名使用，不是当前两个 C struct 的别名。
wire task/completion 可携带身份摘要用于核验，不意味着这些字段全部暴露在中立 C task/completion 中。

### 5.4 生命周期身份与证据

当前 wire v2/v3/v4 依靠一个已建立的 query-scoped session connection、连接内 `sequence` 和相应摘要
关联任务。v3/v4 校验 semantic-spec、physical-algorithm、provider-execution、payload 和 completion evidence；
v4 另核对完整 generation profile 及其摘要。
这些摘要证明内容/执行身份一致，不代替单项序号。tuple binding 留在 PG，sequence 会传到 provider。

当前没有跨进程 `query_id/operator_instance_id/task_id/job_id` 组合，也没有 query-level registry。
未来多节点、多 Job 或重连场景确需时再引入 opaque identity；只有引入 retry 才讨论 attempt identity。
已集成的 choice 路径仍采用同步连接/sequence 关联，没有顺带加入上述 ID。

## 6. 同步 port 与未来增量执行

### 6.1 当前同步行为

本节描述 main 已实现的 recording、exact 和 choice 路径，它们共用同步生命周期。
接通 wire v4 前的临时执行拒绝只属于历史切片；当前支持范围见专项完成记录和 INFRA_STATUS。

当前 `AiProviderPort` 只有 `open/drive/close`，一次 `drive` 接收一项任务、返回一项 completion 或错误。
query begin 固定 Adapter/config 并注册 cleanup；首个非 NULL task 才真正 open，plain EXPLAIN、LIMIT 0、
空输入与全 NULL 输入保持无连接。NULL 不消耗 sequence；输入借用至 drive 返回，completion 存活至
下次 drive/close，PG 及时复制到明确的 tuple context。

非 OK 状态终止 session，先保存中立错误、幂等关闭，再由 PG 映射 SQLSTATE；interrupt/OOM 等
非协议错误保持 PG 原语义。没有自动重试、重连、多在途或显式 `provider.cancel`。

### 6.2 版本策略

recording wire v2 与 exact Filter wire v3 的字段集合、摘要 golden、错误和旧 SQL 行为保持不变。
choice 使用独立 schema 3 / wire v4，详见四 C；生成型 SemMap 的 schema 4 / wire v5 目标与验收见四 D 专项，尚未接线。
复用 framing、JSON primitives、session loop 与 deadline，不用“可选字段大集合”放宽旧 schema，
也不复制整套 socket/HTTP/runtime。未有数据拷贝瓶颈证据前，不新增共享内存或零拷贝传输。

### 6.3 独立 SchedulingSession 与 PG batch port 是两件事

核心的候选 `offer/advance/seal/cancel` 表示供给、推进、输入结束和停止接纳等状态动作，先在公开
sealed-task producer、fake clock/Adapter 上验证。它们不是现有 PG wire 方法，也不要求先扩 C port。
具体签名、返回值和状态表由工作包七的首个实现切片固定，不能仅因计划中有名字就判定已实现。

后续 PG batch port 在相应路径验证后，才用独立版本表达 accepted-prefix、多在途、乱序完成与有界
reorder：未接受的后缀仍归调用方，接受后保持可核验终态；任务数、payload bytes、estimated work 与
结果缓冲各有上限。零接受是正常 backpressure；输入 seal 后不再接纳新任务，仍可排空已接受项。
跨调用保留的已接受任务须有明确的 owned storage/copy 规则，并计入相应容量；不能继续引用已重置的
PG per-tuple 值或已结束 drive 的借用 slice。具体转移和释放时机先在 session 合同测试中固定。
显式 close/cancel disposition 属于该后续协议，不得反写成当前 v3/v4 的能力。

<a id="multi-session-execution"></a>

### 6.4 多节点会话与单节点多在途分开设计（待实现）

当前 gateway 的 `server.py` 在一个 session runner 返回后才服务下一个连接；session runner 等待
该连接的后续任务直至结束。多个算子各自持有长会话时，不能只放开 planner 的单算子限制：上游交出
一行后仍保持连接，下游的新会话可能等不到服务。该判断来自当前源码，不是已支持多节点路径的回归。

多节点同步执行首先需要外部 gateway 能服务多个存活会话；每个会话仍可保持单任务同步，不因此
增加 PG accepted-prefix、乱序或批协议。实施时复用成熟 I/O 设施，具体选型留在该实现切片，并满足：

- 会话/连接上限、活跃模型请求上限、排队项与 bytes 上限分别定义；空闲会话不独占模型执行名额，
  也不能占满所有服务能力而使依赖它的下游永久等待。有限线程数本身不构成无死等证明。
- 任务完成后释放本地使用权，连接可继续复用；容量不足有明确拒绝或可取消、有截止时间的等待。
  远端是否仍在计算按真实终态记录，不能仅因本地断连就视为模型容量已经释放。
- 会话内 sequence、计划身份、完成结果和错误彼此隔离；相同 payload/digest 不是同一算子实例。
  需要额外关联标识时按实际消费者设计，不提前建立通用 query registry。
- 用无模型 fixture 验证 A 空闲时 B 能完成、嵌套依赖超过服务容量不会挂死、超限/断连/取消隔离、
  单会话旧路径和结束后 FD/线程/缓冲回收。共享 Adapter 的并发访问与配置不漂移也须验证。

这项改动属于 gateway 执行服务能力；PG 仍只拥有客户端和查询清理，不新增库内 listener 或 HTTP。
本节不授权替换现有 HTTP Adapter、增加连接池、重试或真实模型运行；这些变化按实际需要单独验收。

## 7. 数据传输与 batching 的不同作用

| 对象 | 谁决定 | 语义与测试重点 |
|---|---|---|
| semantic prompt batching / fusion | PG 选择的 semantic algorithm | 可能改变调用结构，需独立语义/质量证据 |
| database execution batch / transport chunk | PG carrier 与选定 placement | 有界取数、过取量、取消、输入所有权；不自动成为不可拆分模型 batch |
| SemLoom work-unit organization | SemLoom 或实验明确指定的组织 owner | 对已定语义的独立请求按 work/locality 重组；不拆单行 prompt、不改 payload |
| continuous batching / KV 管理 | 原生模型 serving scheduler | 保持黑盒，不把 vLLM 内部策略算作自有实现 |

允许 PG 的必要取数窗口与 backpressure；避免两层复制同一组织/准入控制器。只有逐项语义允许时
才可 rebatch；不可拆分 batch 必须显式表达或拒绝。IMLane-like DB batch 与 provider rebatch 是研究
对照，等真实 PG 增量接入和匹配条件满足后运行，而不是预先认定某个位置最优。
同一查询内一元算子的基本数据依赖属于工作包六的组合检查；Kalypso-like lineage/KV、Join 与
blocking operator 的新算法仍只作另行立项后的参考。

## 8. 工程落点与维护方式

### 8.1 当前目录与目标增量

| 位置 | 工程职责 |
|---|---|
| `code/postgres/semloom_pg/` | 自有 PG carrier、plan spec、machine、pump/runtime、neutral port 与 UDS/wire |
| `code/src/execution_provider/` | 公共 gateway、版本化 codec、completion Adapter；旧 extension gateway 只保留兼容入口 |
| `code/src/planning/`、`code/src/scheduling/` | 已有 work/cost、组织、准入、路由和执行代码；增量核心先从真实调用和测试表征中抽取 |
| 未来 SemLoom gateway Adapter | 将中立 task 映射到现有核心，不能再写一套 scheduler；具体文件只在有实现消费者时创建 |
| 公司 fork | 承接自有算子语义/处理/优化方法的 planner/executor 适配，以及 SemLoom provider client 与本地 lifecycle 映射；见 §8.7 |

源码文件树与可运行命令只由代码 README/INFRA_STATUS 维护，不在本计划复制易过期的全量清单。

### 8.2 设计模式与验证

Ports & Adapters 只用于已存在的外部变化；Factory 只负责 query-fixed Adapter 选择；Strategy 区分
真实 operator/physical algorithms；状态机分别封装 PG 生命周期与增量执行状态，不合成万能执行器。
公共 Module 的测试从其 Interface 观察结果、错误、资源与终态；pure/local fixtures 先验证核心，PG 与
外部服务各自验证其 Adapter。保持旧调用兼容，新增 Interface 由真实行为驱动。

### 8.3 carrier audit 与最小 core patch

先按算子目的选择 PG 接入方式：普通本地辅助计算可用 SQL/C 函数；需要独立语义策略、AI-work cost
与执行生命周期的路径保留 planner-visible 表达；已经能由 PG/pgvector 完成的表达式、索引或 ordinary
plan 优先复用。CustomScan 是当前载体，不是所有功能都必须新增节点的理由；也不能把现有 marker
静默改成逐行 HTTP fallback 来冒充路径优化。公司三类接入方式的源码对照见 §8.7.2；
公开 SQL 函数封装与属性选择的补充参照见 [§8.8](#pgml-engineering-reference)。

审查随对应目标增量进行：同步 Map/Filter、真实生成型 Map、算子组合、batch/reorder、第二 Filter path
分别验证对象身份、SQL 函数属性、prepared invalidation、hook coexistence、placement 与 executor 生命周期。
无需等 Filter 第二路径完成才检查生成型 Map；也不能用旧同步证据替代新 batch carrier 资格。

若问题仅在 identity/path generation，最小 patch 仍 lower 到既有 CustomScan；只有 executor lifecycle
有独立阻断才引入 native node support。每处 core 修改须绑定复现反例，并比较同语义 task/result/error
与生命周期行为；不 vendor 全量 PostgreSQL，不扩 grammar/storage 或模型 runtime。

<a id="frontend-adapter-strategy"></a>

### 8.7 公司工程参照与自有成果向公司移植（工程设计，尚未移植）

两件事方向不同：**现在吸收公司 demo 的工程经验，完成自有系统；未来可把自有成果移植到公司系统。**
自有成果包括语义算子的定义与处理、reference/optimized 策略、代价/质量与回退方法，以及 SemLoom
数据组织、提交、路由和调度。公司算子调用 SemLoom 只是执行接入，不代表前述算子方法已经移植。
自有 PG18.3 主实现继续独立构建和复现；公司移植是可选应用方向，不成为主实验的私有前置依赖。

本节是公司算子工程对照与移植的唯一入口：从“算子做什么、怎样进入 PG”到“如何产生请求、恢复关系结果、
管理新增资源”，多算子组合只是其中一项。每项采用决定都回到 §1 的研究目标；公司实现更宽的 SQL
表面、已有连接池或更多函数，不自动成为自有系统必须照搬的实现方式。

<a id="own-to-company-transfer"></a>

#### 8.7.1 迁移范围与 Module 分工

| 层次 | 自有实现继续完成什么 | 未来向公司系统移植什么 / 本地保留什么 |
|---|---|---|
| 算子语义与策略 | prompt/parser/model/generation identity、关系结果处理，以及按工作包实现的 reference/optimized 策略、work/cost、质量与回退 | 移植可复用的策略计算、状态流程和合同测试；由公司 planner/executor 保存并执行相应语义与物理选择，不能交给 scheduler 暗中决策 |
| PG carrier | SQL marker、合法 placement、CustomPath/CustomScan、plan 编解码、tuple binding 与 query lifecycle | 适配目标 PG 版本及公司载体；Plan/Datum/slot、snapshot、权限、错误和事务仍由目标数据库处理，不要求 C 结构或 hook 完全相同 |
| SemLoom execution provider | work 组织、有界提交、路由、多 Job 调度、可替换 backend 与观测 | 复用同一执行核心，通过公司侧 Adapter 转换请求与完成结果；公司 fork 不另维护第二套 organizer/scheduler/router |
| 模型执行 Adapter | 已定语义到模型请求的映射、原始结果与模型证据 | 按实际模型能力适配，HTTP/认证/供应商字段留在模型 Adapter；不改写前端选定的 prompt/parser |

以上是职责划分，不要求现在创建独立策略 SDK、registry 或新目录。可独立测试的值/策略先与 PG
binding 分开；已有文件能承载时继续使用，出现真实消费者和变化原因再抽取。两类数据库前端都使用
当前查询提供的 tuple/task，gateway 不重新连库取数、不重做事务/MVCC/WAL。

```text
工程经验：公司 demo ------------------------> 自有语义算子实现
未来移植：自有算子语义 / 处理 / 优化方法 ----> 公司 planner / executor 适配
          自有 SemLoom 执行 / 调度核心 -----> 公司系统可用的同一核心与 Adapter
```

上图表达成果移植方向；运行时请求仍由公司算子经 Adapter 进入 SemLoom，再将结果返回数据库。
移植后仍由数据库选择 semantic/physical plan，SemLoom 执行已确定的 work。接口连通、算子行为等价、
优化策略等价、质量与性能分别验收；当前公司路径没有这些通过证据。

<a id="company-engineering-reference"></a>

#### 8.7.2 现在就参考：具体定位、采用范围与验证

来源：2026-09-02 对用户提供的 `x_semantic` 当前工作副本的只读源码核对；该副本有未提交改动，
不是一个已经锁定或通过 PG18.3 验证的 release。下面是源码观察与工程决定，不是模型/性能结论。
参考核对当时区分 main 与四 C 独立分支；当前集成状态见 INFRA_STATUS，原始证据不重新绑定。两边所查目录
均为 PGXS extension，未据此发现必须引入 core patch 的依据；该观察不覆盖未提供的公司数据库内核。
路径相对用户提供的参考目录；后续切片须重新确认实际版本和函数体。这里只记录最小定位与行为摘要，
不复制公司源码、prompt 原文、内部测试数据或日志；详细快照与来源证明保留在授权位置。
同一表格单元内省略目录的文件名沿用前一个文件的目录；自有 C 文件位于 `code/postgres/semloom_pg/src/`。

**PG 接入与执行语义。** 先确定函数的目的、输入输出和返回行数，再选择载体；不能把注册函数、
planner-visible 算子和外部执行请求当成同一件事。

| 公司参考对象（只读定位） | 当前观察与自有差异 | 自有决定、落点与必查用例 |
|---|---|---|
| `Makefile`、`sql/x_semantic--0.1.0.sql` | 同时注册实际执行的标量/集合函数和不能直接执行的 marker；部分函数标为 parallel safe。自有 Map/Filter 是 fail-closed marker，当前禁并行 | 保留自有语义路径的 planner 接管；辅助计算按目的选普通函数。核对 `VOLATILE/STRICT/PARALLEL/SECURITY/LEAKPROOF/COST` 的实际含义，不为解锁计划改属性。测 NULL/default、未加载 hook、权限与普通 SQL；只有独立验证后才开放并行 |
| `src/operators/sem_distance_planner.c:lower_sem_distance_marker`、`sem_distance_scan.c` | 距离 marker 改写为 pgvector 距离表达式，查询 embedding 经子查询求值，并包装已有路径；并非所有计算重写成专用执行循环 | 在后续向量/候选检索路径复用已有表达式、索引和 child plan；当前不增加新算子。若采用，测实际 path、类型/维度、空输入和 plain EXPLAIN 零模型调用；向量距离不能自动替代自然语言 Filter 真值 |
| `src/operators/sem_distance_planner.c:lookup_extension_function` | 除 schema/参数 OID 外还核验扩展成员关系；自有复核基线 `c494e1b2` 只查固定 schema/签名，身份切片已复现误接管、补校验并合入 main | 采用成员身份原则，不复制源码；验证扩展缺失、非成员/重载、删除重建与函数替换失效。仅成员 DDL 要按下方要求刷新所有相关连接，自动刷新仍 pending；不扩可迁移 schema 或改语法。状态见下方切片及 INFRA_STATUS |
| `src/operators/sem_map_op.c`、`sem_generate_op.c` 与 `src/hooks/x_semantic_hooks.c` | 普通函数可按 SQL 规则组合，但专用 Map/Generate 同层仍只允许一次，并有 shape 限制；自有也有单 marker 和 Map/Filter 互斥检查 | 从 `sem_path_common.*`、`sem_path.c`、`sem_filter_path.c` 提炼真正共用的调用识别、查询层级和绑定；各算子保留独立 placement。工作包六用实际组合验证，不仅删除 guard，也不声称公司已实现任意 SQL 组合 |
| `src/operators/sem_map_op.c:sem_map_exec` | 已有真实文本生成、输入/输出列映射和结果存储；先收集全部 child 行，再取 GUC 配置调用模型。自有 Map 仍为 recording，已有增量 pump | 四 D 复用 Map lowering、plan spec、pump/runtime，增加真实生成合同。保留计划内语义和增量取数；测行数保持、列顺序/别名、重复输入、多字节/大输出、LIMIT、INSERT 与取消。不把全量 collect 当成所有算子的公共行为 |
| `src/operators/sem_map_op.c:sem_map_rescan`、`sem_topk_op.c` | Map rescan 清空结果状态并重扫 child；自有当前明确拒绝 rescan/EPQ | 需要新 SQL 形状时先定义同参重扫复用还是重新求值、参数变化怎样重建、模型调用如何计数；再决定 tuple store 或新状态。测参数变化、重复扫、早停、错误和取消，不把 callback 存在当作语义正确；有全局状态的算子另定内存/落盘上限 |
| `test/sql/sem_map_offline.sql`、`sem_generate_offline.sql`、`llm_offline.sql` | 覆盖离线 SQL 形状、配置与空值；公司 Map 可将行内 NULL 当空串发送 | 在自有 regression/TAP 使用公开合成数据，预期由自有语义定义。分别测 NULL 不调模型、空串、RLS/权限、snapshot、prepared/invalidation、普通 SQL 非干扰；不复制私有输入或沿用公司测试资格 |

SQL 属性与扩展回调以 [PG18 CREATE FUNCTION](https://www.postgresql.org/docs/18/sql-createfunction.html)
和 [CustomScan execution](https://www.postgresql.org/docs/18/custom-scan-execution.html) 为公开依据；
实际编译与验证仍锁定 `REL_18_3`，官方文档不能代替该版本的运行证据。

**请求构造与外部执行。** 同样比较复用方式和所有权，不因公司公共 LLM 层位于 PG 进程内，就把
它的网络、分词或调度实现搬入自有 backend。

| 公司参考对象（只读定位） | 当前观察 / 值得吸收的经验 | 自有决定、落点与必查用例 |
|---|---|---|
| `src/operators/sem_map.c:sem_map_build_params`、`src/llm/llm_prompt.c:llm_build_per_row_requests` | Map 的不同入口共用构造，固定内容与逐行输入分开。自有 `sem_operator_machine.c` 的真实 task 编译目前仍使用 Filter 模板，Map 未消费生成合同 | 四 D 引入真实 Map 时把公共消息编码与算子自己的 prompt/text parser 分开；复用已有 task/Adapter，不复制 Filter 执行栈。测完整规范消息、Unicode/空串/模板样式输入、输入输出上限，以及旧 Filter bytes/digest/错误不变 |
| `src/llm/llm_protocol.{h,c}:llm_effective_request / llm_resolve_request` | 请求默认值先消解，再给编码与缓存使用；当前视图借用请求/GUC 字符串且含 PG 依赖和 endpoint 方言 | 吸收“实际参数只有一个来源”：`sem_plan_spec.c` 保存语义参数，`pg_semantic_runtime.c` 只转换到中立 `AiOpenSpec`；HTTP endpoint/方言检查归模型 Adapter。测缺省与显式参数、配置变化、身份与真实出站字段一致，不直接把该 C struct 作为跨进程合同 |
| `src/llm/llm_chat.{h,c}`、`llm_error.c` | 请求编码与响应解析分开，结果有 model、usage、finish reason 与错误分类 | 需要相关能力时补在 `code/src/execution_provider/adapters/openai_compatible_fixed.py` 等实际模型 Adapter，复用自有 framing/deadline/脱敏错误机制；测试错误模型、截断、缺失 usage、无效编码。只吸收本次需求，缺失观测不得填零冒充实际值 |
| `src/operators/sem_filter.c:sem_filter / parse_bool_from_llm / filter_cascade_check` | 当前是标量 boolean 函数：未命中缓存时调用大模型，再宽松解析 True/False；输入 NULL 返回 false，调用无结果或解析失败告警后返回 false。没有 UNKNOWN 输出类，embedding cascade 始终 disabled | 先对齐自然语言条件筛行的目的；二值是贴近公司的候选，不要求所有 Filter 都有 UNKNOWN。保留现有三值 profile 的兼容与严格解析；新增二值或容错策略须单独定合同/身份。向量粗筛不是公司已有能力，不可直接记为复用完成 |
| `src/llm/llm_batch.c:llm_batch_execute`、`llm_conn.c:llm_pool_multi_begin / llm_pool_multi_end` | 当前 backend 独有的连接池，批次内有界 serial/multi、完成后补位，结束后归还使用权但可保留连接；不是跨查询统一调度器，也没有自有路径这种 UDS gateway | 借鉴执行名额与连接寿命分开，落实在外部 gateway/SemLoom；多会话要求见 §6.4。未来公司执行接入点见 §8.7.4。测配置/容量、原行关联、逆序完成、异常清理；不因连接可复用就认定可以无限持有执行名额 |
| `src/llm/llm_batch.c`、`llm_error.c`、`llm_cache.c` | 串行路径可重试部分传输错误，prompt 层可命中完整有效请求缓存；这些都会改变实际模型调用数 | 现有自有路径继续无自动重试/缓存。将来独立指定 owner、键的语义/模型/权限范围、淘汰与资源上限、重试终态和重复结果处理；分列逻辑项、cache hit、实际尝试与 usage，不把失败变成合法空结果 |
| `src/llm/llm_tokenizer.c`、`llm_prompt.c:llm_build_per_row_requests / llm_build_merged_requests` | 检查完整消息的上下文长度，可回退估计；merged 将多行写进同一 prompt，不是只把独立请求一起发送 | work 估计与模型 Adapter 的有效请求保持一致，精确/估计明确区分；超长单项的处理由算子合同决定。prompt 合并按 §7 归语义算法，独立请求组织归 SemLoom，服务内部 batching 归 serving；不在四 D 暗加截断、拆行或 prompt 合并 |

首次完整对照按两张表逐维给出采用、保留、延期或不适用的判断；后续切片只读取其受影响项，不要求
每次通读公司仓库。四 C 核对参数身份、错误与 Filter 差异；四 D 同时核对 PG 注册/载体、生成任务、
取数和结果恢复；涉及会话或执行接入时再核对 batch/config/cancel。算子优化仍以相应论文、公开实现
与自有实验为依据。缺少参考目录只阻塞依赖私有实现的移植判断，不阻塞按公开依据完成自有算子。

**先贴近算子目的，再选择实现细节。** 公司 Filter 与自有 Filter 都判断一行是否满足自然语言条件；
真实模型 reference 都采用大模型判断，公司命中缓存时复用旧回答，不是通过 embedding 直接判真值；
自有 recording/golden 路径只是机制测试，不作为模型判断能力。
TRUE/FALSE 是可成立的二值合同；UNKNOWN 只在明确需要表达“无法判断”的 profile 中保留，不能和
SQL NULL、调用失败或解析失败混为一类。后续若以二值为目标，先确定输出词、严格 parser、NULL/error
policy 和测试预期，再增加显式版本/profile；不暗改已有 tristate 身份，也不把旧三值质量失败改判为通过。
贴近公司不要求复制其宽松搜词或失败转 false。公司 Map/Generate 的共同目的是逐行生成文本；四 D
参考这种输入/输出关系，具体 prompt、NULL 和输出上限仍由自有合同明确，不继承未声明的 GUC 默认值。

三值可继续作为自有 profile，用于区分否定与无法判断；是否更适合任务由实际需求与质量测试决定。
可移植性不要求公司 SQL 新增一个 UNKNOWN 字面返回值：在已经声明的 WHERE 过滤范围内，可以复用
自有 parser/keep-drop 规则，将 UNKNOWN 映射为不保留该行，并保留原始完成状态用于审计。该关系结果
映射不是把原始模型答案改成 FALSE，更不允许把执行错误伪装成模型判断。SELECT 投影、NOT、复合
谓词或其他 SQL 位置的等价性须另行定义和测试，不能从 WHERE 不保留推断。

#### 8.7.3 已完成部分：保留底座，按实际问题定点修改

| 已有部分 | 本轮决定 / 后续允许调整的条件 |
|---|---|
| thin scan、pump、PgSemanticRuntime、neutral port、UDS/wire、query cleanup | 继续作为自有底座；不因公司目录不同重写或合并层次。新消费者暴露真正遗漏时，先用失败用例定位再改相应 Module |
| plan-owned 语义、严格 Filter、已集成的 choice 配置 | 保留现有接口及原证据；二值 profile 或公司兼容策略可另行明确，但不静默修改已有三值身份和错误表现。不把已完成状态当作语义永远不能调整的理由 |
| prompt、有效生成参数与结果解析 | 四 D 引入真实 Map 消费者时，把当前 Filter 专用 task 编译与公共消息编码分开；原始值解析与 WHERE keep/drop 在其他 SQL 位置出现时再分别表达。行为变化独立版本化，结构重构保持旧输出与错误 |
| marker 识别、query shape 与列绑定 | 围绕实际 SQL 形状和两个算子消费者整理共同分析；新增算子不应复制整套 rewrite-tree 特判。对象身份、placement、列绑定与具体语义策略分别验证，不只删除单 marker guard |
| operator strategy 与 PG binding | 可独立表达的计算和关系 disposition 留在语义 Module；SQL/Plan/slot 操作留在 PG adapter。实际公司移植需要的局部适配可做，但不为假想数据库改造全部现有代码 |

修改已完成代码前，切片记录须写明具体耦合/反例、涉及调用方、保留行为和复验范围。没有发现问题时
记录“保留现有实现”即可。不得用“学习 demo”替代问题定义，也不能把尚未移植当成已完成代码有缺陷。

验收重构看变化是否集中：新增生成语义不修改 UDS socket 等待，新增执行 Adapter 不修改 Filter
真值，新增合法 SQL 位置不复制 provider 生命周期。缺陷修复、结构重构、语义变化分别可审阅；
同一算子差异散落多个层时应集中到拥有该语义的 Module，而不是只追求减少少量分支或提前创建通用框架。

#### 8.7.4 两个不同的移植接点

**算子方法进入公司 planner/executor。** 自有语义定义、task/result 处理与策略计算可复用；公司侧
仍需表达逻辑/物理身份、合法 placement、path 生成与选择、cost/quality/evidence/fallback 和生命周期。
公司现有生成型 CustomScan 可作为载体候选；其 Filter 标量函数不自动拥有上述能力。先选择一个已经
在自有系统实现并验证的算子/策略移植，检查计划真正消费这些字段，而不只新增 EXPLAIN 标签。
新旧 NULL、parser、失败策略不同就显式保留不同语义身份，不能静默改公司旧 SQL 或共用同一 digest。
自有 quality/calibration 证据不自动适用于不同模型、任务分布、服务或目标 PG 环境。

**执行能力通过公司 provider Adapter 接入。** `llm_batch_execute` 附近是当前候选位置，不是已经
完成的接口。现有请求构造和结果回填继续由公司调用方拥有，提供显式选择的 native 与 SemLoom 两种
执行 Implementation；原 native 可保留作兼容/对照，SemLoom 分支不再进入它的 curl 调度或重试循环。
首次只支持受约束的 per-row 文本请求；独立 deterministic Adapter 先证明映射，真实接入再使用已经
可执行的自有文本生成合同。当前 Filter wire v3 与 choice v4 不能充当任意消息/生成请求的通用协议。

开始该 Adapter 切片前确认以下对象，不只替换函数调用：

- 请求：消解模型/生成默认值，固定实际 messages 与 identity；借用值跨调用时复制到有明确上限和
  生命周期的存储。PG 类型与供应商方言不穿过中立 Interface；只在 native 实现检查其 HTTP 配置。
- 关联与输出：原始行位置与 task sequence 明确对应；重复 payload 仍是不同项。返回 raw output、
  model、usage、finish reason 与中立错误；公司适配恢复结果，缺失证据标 unavailable 或按该合同拒绝。
- 调用 owner：首个对照明确关闭或隔离缓存、传输重试和业务补发，记录实际调用数。后续恢复策略时
  分别指定 owner，native 与 SemLoom 不叠加同职责控制器，也不在失败后暗中换 backend。
- 资源：公司 Map 全量 materialization 只可用于事先限定规模的映射验证；形成增量路径时另改为有界
  child 窗口。普通传输块可按逐项语义重组，merged prompt 不得随意拆开；IMLane-like placement 仍待匹配实验。
- 取消：公司 `llm_set_cancel` 当前没有启用实现，本地 PG interrupt 与远端停止分别核对。注册/复用
  目标 PG 的清理设施，验证 no-task、早停、异常、取消、重复关闭与新查询；不承诺远端 GPU 立即停算。

公司私有差异集中在其 Adapter/载体中；只有真实双方消费者证明缺少概念时才扩展中立合同。
opaque task/job/attempt ID、异步 registry 与批协议按实际关联需求设计，不在首次移植中预建。

<a id="operator-engineering-actions"></a>

#### 8.7.5 研发顺序与完成记录

| 时机 | 具体动作 | 该步完成条件 |
|---|---|---|
| 现在：完整工程对照 | 按 §8.7.2 覆盖 SQL/PG carrier、算子目的、请求/结果、资源与外部执行，而非仅看多算子 | 每一维在当前切片登记版本/工作副本、文件/符号、源码事实、自有现状、采用/保留/延期/不适用及原因、改动位置与可观察用例；未知项显式保留，不用公司测试替代自有证据 |
| 四 C 收尾（工程验证已完成） | 保持已接通的 choice 与受限 INSERT 行为，保留专项验证记录 | 资源与受限真实 smoke 分别有证据；不把新的通用 SQL/会话重构并入该版本，旧语义与失败记录不变，当前代码已集成 |
| 四 D 的真实生成实现 | 先定生成合同，再按真实 Map/Filter 消费者整理 task 编译、结果解释与模型 Adapter 复用 | 规范消息、列绑定、NULL/空串/大输出、截断/失败及旧 Filter 行为可独立验证；真实 Map 不照搬 Filter 标签或 token 上限 |
| 对应路径的 PG 接入检查 | 工作包六逐项核对注册身份、函数属性、placement、普通 PG 复用、重扫/参数变化；多算子组合是其中一个子切片 | 先补反例，再做小范围重构/能力扩展；保留已通过的 pump/runtime。未使用的 Join、聚合等不提前实现，不以重构文件数验收 |
| Map 可执行后扩组合/多节点 | 按 §6.4 调整外部 gateway 的会话服务与活跃工作上限，再验证两个 Filter AND、Filter → Map | 空闲会话不挡住其他节点，容量不足不会永久等待，错误/取消/资源隔离通过；单节点仍可同步，不等于已完成 PG 批协议或 SemLoom 调度 |
| 有具体移植疑点且所需 Interface 可执行后 | 经授权在公司 fork 做一个算子/一类请求的最小 deterministic 验证 | 计划或请求身份、结果关联、NULL/error、取消/回收和拒绝条件可观察；未运行模型，不宣称质量或性能 |
| 相应自有方法稳定后，按需正式移植 | 算子方法移植和 SemLoom 执行接入分别提交、分别验收 | 目标 planner/executor 真正执行相应策略；执行 Adapter 复用同一核心；目标版本、兼容、资源与相同条件下的质量/成本分别验证 |

不必等待全部算子完成才参考公司经验，也不必等待公司移植才推进自有系统。完成记录放当前切片
计划/结果与现有状态入口，不新增平行复用台账；公司材料的详细版本与审计信息保留在授权位置。
只有实际变动的 Module 才进入修改清单，未采用项可明确写出理由，不要求为了“复用率”复制代码。

#### 8.7.6 代码来源与环境

| 目标 | 允许范围与条件 |
|---|---|
| 公司内网 fork | 在公司制度及授权范围内直接复用既有 plan/executor、provider/client、生命周期和测试设施，并承接获准移植的自有方法与执行核心 |
| 可公开主实现 | 公司源码、内部测试、常量或衍生实现只有获得明确外部发布授权后才能进入；否则依据公开接口/资料独立实现，记录工程参照与不照搬原因 |
| 公开 AutoDL 实验 | 自有 `semloom_pg`、可公开的 SemLoom、公开或获准模型、公开/合成数据；遵守 runtime preflight 与独立实验计划 |
| 公司 fork 实验 | 仅在公司批准的环境运行；AutoDL 未获批准时不上传 fork、二进制、容器、数据或日志，内网测试亦须在授权范围内 |

fork/修改权限不等于外部发布或部署权限；改名、翻写、打包均不改变来源要求。当前具体参考与未来
移植服务于自有完整实现和可复现研究，不增加新的研究内容，也不授予模型运行或公司代码修改权限。

<a id="pgml-engineering-reference"></a>

### 8.8 pgml 公开工程参照：模型能力封装与复用（工程设计）

本节补充 §8.7 的公司工程经验，不建立第二套架构或要求安装 pgml。目标是让新增语义算子主要增加
自身的语义、计划接入和结果解释，并复用现有模型调用、通信与清理；不是将 Python/模型加载迁入 PG。

**来源与范围。** 2026-09-02 只读核对 [PostgresML 固定提交](https://github.com/postgresml/postgresml/tree/caf2b6ccdf0d6efc2c1910cbc06725a34320181a)
`caf2b6ccdf0d6efc2c1910cbc06725a34320181a` 下的 `pgml-extension/`；
其中 [Cargo.toml](https://github.com/postgresml/postgresml/blob/caf2b6ccdf0d6efc2c1910cbc06725a34320181a/pgml-extension/Cargo.toml)
声明包版本 2.10.0、Rust/pgrx、PG12–17 features，默认 PG17 + Python。所查本地模型 binding 经 PyO3
调用 Python 模型库，与自有 PG 外执行路线不同。这里的“SQL AI 函数”不等于已经证明存在专用
SemFilter/SemJoin 计划优化器；也不能据此说普通 PG 优化器完全不能优化函数表达式。
本次未安装、构建或运行 pgml，未验证 PG18.3 兼容性、性能或所有 backend 共享模型权重。

下表左列是固定版本源码事实，右两列是自有工程决定；参考不等于直接复制代码。实际采用时在对应
切片记录确认源码版本、保留行为和用例结果，来源与取舍仍只进入工程计划或切片记录。

| 已核对的公开实现 | 自有采用方式与落点 | 时机与完成条件 |
|---|---|---|
| [api.rs：embed、transform_json/transform_string](https://github.com/postgresml/postgresml/blob/caf2b6ccdf0d6efc2c1910cbc06725a34320181a/pgml-extension/src/api.rs#L589-L708) 用扩展注册带类型的 SQL 入口，再交给模型 binding | 保留 `sql/semloom_pg--0.1.0.sql` 的函数入口、PG 类型检查和普通表达式；需要语义身份、位置或执行控制时仍由 planner/CustomScan 接管。不把未 lower 的 marker 变成普通函数模型调用 | PG 基础检查及相应算子切片：输入类型、NULL/空串、函数对象身份、合法位置与未支持形状均有明确行为；plain EXPLAIN 与无任务查询不调用模型 |
| 同一 api.rs 的多个 transform 重载归一为 task/args/inputs 后调用同一个 transformers binding | 四 D 由真实 Map/Filter 消费者整理 `sem_operator_machine.c`：共享消息编码与调用能力，各算子保留自己的 prompt/parser/关系 disposition；生成参数仍由 plan → `AiOpenSpec` 单向传递，复用 `PgSemanticRuntime` 与外部 completion Adapter | 四 D：Map 的文本输出与 Filter 的真值解析分别验证；完整规范消息、生成参数及摘要可核对，旧 Filter bytes/digest/SQLSTATE 不变。新增 Map 不复制 provider、wire、deadline 或 cleanup |
| [transformers.py 的模型/管线字典](https://github.com/postgresml/postgresml/blob/caf2b6ccdf0d6efc2c1910cbc06725a34320181a/pgml-extension/src/bindings/transformers/transformers.py#L62-L64)与 [generate 缓存未命中加载](https://github.com/postgresml/postgresml/blob/caf2b6ccdf0d6efc2c1910cbc06725a34320181a/pgml-extension/src/bindings/transformers/mod.rs#L253-L289)复用昂贵准备 | 借鉴初始化不逐项重复的原则，而非复制 PG 内模型缓存。模型实例由外部 serving 管理；如需客户端/连接复用，在 gateway 的模型 Adapter 内实现。PG 查询仍拥有独立 plan、sequence、结果缓冲与关闭动作 | 资源复用是有实际需求后的独立切片，不是四 C/四 D 的新增前置项。先写配置/认证隔离与资源上限，验证复用次数、超时/断连恢复、取消不干扰其他会话，以及 no-task 不提前连接 |
| [api.rs：embed 与 embed_batch](https://github.com/postgresml/postgresml/blob/caf2b6ccdf0d6efc2c1910cbc06725a34320181a/pgml-extension/src/api.rs#L589-L610)共用一个底层 embedding 调用；这不是自动把标量 SQL 调用合批的证据 | 保留当前同步单项 port；后续批接口复用同一单项任务语义与结果校验，由 §6.3/工作包七管理接受、关联、缓冲和完成。批量传输不改变每行 prompt，不用全表 array_agg/collect 代替有界取数 | PG 增量桥接阶段：batch size 1 与同步对照一致，重复输入/NULL/尾批/早停/取消/乱序结果按新合同验证；输入与结果缓冲都有上限。prompt 合并仍按 §7 作为另一个语义算法处理 |

**复用时保留的三项区别。**

- SQL 可组合性不等于语义优化能力：Filter placement、reference/optimized、quality/fallback 仍由自有
  planner 与算子策略实现，不能仅靠增加函数重载或改函数属性得到；也不要求所有本地辅助计算变成 CustomScan。
- 模型实例/连接复用不等于推理结果缓存。后者会改变实际调用数与复用范围，须另定语义/模型/权限身份、
  资源上限和统计口径；当前不顺带增加结果缓存、自动重试或新 registry。复用客户端也不代表空闲 session
  可以占住模型执行名额，多会话的进展与隔离继续按 §6.4 验证。
- pgml 上述入口的 `IMMUTABLE/PARALLEL SAFE` 声明不直接移植。模型生成、外部状态和查询私有资源须
  按 [PG volatility](https://www.postgresql.org/docs/18/xfunc-volatility.html)与
  [parallel safety](https://www.postgresql.org/docs/18/parallel-safety.html)分别审查；保留当前已验证的
  `VOLATILE/PARALLEL UNSAFE`，有明确语义依据及 PG18.3 用例后才另行调整，防止意外规划期求值或不安全并行。

四 C 收尾范围不变。近期只把入口/属性检查纳入既定 PG 基础检查，把任务编译与结果复用落实在四 D；
客户端复用与批量执行按上表分别推进。无需迁移到 Rust/pgrx、引入库内 Python，或预建通用模型框架。

## 9. 工作包与完成条件

工作包一至四 B 的已完成工程不在此重复提交与测试数字；实际状态见 INFRA_STATUS，原过程见历史快照。
本节保留已完成四 C 的记录入口，以及后续工作和依赖。

<a id="choice-profile-engineering"></a>

### 工作包四 C：可选 choice 生成配置

本工作包实现已选定的三值 choice profile，保留旧 SQL/schema/wire，不成为默认或质量合格的 reference；
它不规定所有 Filter 必须三值。值合同、PG plan、open spec/wire v4/gateway 和受限 Filter INSERT
已在独立分支验收并进入当前集成代码；受控 fixture 资源与 14 次受限真实请求检查均已通过。
原始结果见[收尾记录](../results/postgresql/choice_service_20260902/README.md)，字段/预算计划已归入 completed。
已移除接通前的临时拒绝；未知或不受支持的版本仍不回落 v3。
二值候选及完整算子工程重构不混入该版本收尾。
PG 保存自包含 profile，gateway 做供应商映射；新 identity 不匹配旧 calibration artifact。
[四 C 专项计划](completed/postgresql_choice_profile_engineering.md)是字段、canonical vectors、错误、累计请求预算、
资源验证和逐项完成条件的唯一入口。完成工程接入不恢复 Filter 真实校准，也不用于第二路径质量结论。

<a id="composable-operators-work-package"></a>

### 四 C 之后：完整工程对照 → 真实 Map → 可组合执行（待实现）

按用户本轮最新安排，先完成 §8.7 全链路对照与决定，以四 D 的真实 Map 整理必要公共实现，再扩
可组合执行与研究接口。两个 Filter AND 不再作为真实 Map 的前置条件；四 C 工程验证已收尾，质量未晋升。
按 [§8.8](#pgml-engineering-reference) 逐项核对注册身份、函数属性、已有 PG 能力复用与计划/结果绑定逐项补反例，只有实际问题才改已完成代码。
随后以两个 Filter AND 和 Filter → Map 为组合消费者，整理公共调用分析与外部多会话服务。
详细动作与验收分别在[改造顺序](#operator-engineering-actions)、[多会话要求](#multi-session-execution)
和[工作包六](#carrier-audit-work-package)，不在此复制第二套合同。多算子不是完整工程对照的替代品。
不等待未来所有 Join、aggregate、CASE 等形状实现，也不阻塞独立 SemLoom 核心或 Filter 质量研究。

<a id="real-semmap-work-package"></a>

### 工作包四 D：真实 SEM_MAP / AI_COMPLETE 生成纵切面（合同定稿，实施中）

这是完整工程对照之后的自有 PG 任务，为文本生成 work 提供真实数据库入口；不依赖先完成多算子
组合或 Filter 三值分类质量。先验收独立生成型 Map，Filter → Map 留给后续组合切片验证。
AI_COMPLETE 在这里是工作负载含义，不新增同名 SQL alias。SQL 重载、参数、文本输出规则和版本由
[四 D 专项合同](postgresql_semmap_generation_contract.md)唯一定义；合同已定稿，§8.0 已登记源码复核。
独立分支 `6903cf46` 只完成规范消息编译和旧路径兼容验证，见[结果记录](../results/postgresql/semmap_messages_20260903/README.md)；
完整纯值/协议、PG＋golden、真实模型与资源须继续分阶段验收，不能把本子切片写成可执行生成型 Map。

最小执行关系：SQL input/instruction/options → planner-owned SemanticPlanSpec → row-preserving
SemMap CustomScan → provider → raw text completion → PG 输出列。仍先同步单在途，再对接增量核心。

开始该工作包前按[公司工程参考表](#company-engineering-reference)核对 SQL 注册、PG 载体、Map/Generate、
prompt、取数和结果处理；按 [pgml 公开工程参照](#pgml-engineering-reference)核对模型能力封装与共享实现，
写下采用/适配/保留自有实现的决定。优先复用现有 `sem_plan_spec.c`、SemMap lowering、machine、
pump/runtime 与外部 completion Adapter；参考公司逐行文本生成、列映射和参数构造，不复制全量
materialization、执行时语义漂移或 PG 内 HTTP。已有层次仅在新消费者证明必要时定点调整。

专项合同依次管理研发复核、纯值与公共 task/result 整理、PG plan、golden 完整执行、固定模型和资源
验收；本节不复制其字段或 golden vectors。只有相应实现和验证完成，才记为四 D 工程完成。
保留现有 scan/pump/runtime/provider Seam，按 Map 暴露的真实变化原因调整；不复制 Filter 执行栈，
不让新 Map 继承三值 parser、8-token 上限或换行 stop。具体版本改变不重定义旧接口。

生成任务为输入/输出 work 与长短任务研究提供入口，不自动产生性能结论；真实比较仍须选定任务质量、
模型/服务/资源与相同输出要求。验证后的同步路径作为工作包七增量桥接的对照，组合与桥接分别验收。

### 工作包五：Filter 质量、matched cost 与第二 physical path

该工作包保留，和生成型 Map/SemLoom 核心分别推进。先确认任务、标签和统计判定，取得符合要求的
reference；再用同 semantic/model/workload/service 的真实观测及 held-out 检验校准，之后实现
LOTUS/Cortex-like proxy/oracle 两路径、显式质量授权、threshold/evidence 与 reference fallback。
reference 的二值/三值输出应服务该过滤任务，不以旧三值合同限制所有未来任务；更换合同后重新建立
对应身份、独立预期与质量证据，不沿用不匹配的校准。公司预留的 embedding cascade 不算已实现优化。

planner 生成可区分且可比较的 paths，executor 按计划执行，provider 不暗换算法。输入基数、NULL-adjusted
calls、usage、AI-work estimate 与真实成本分开；工程启发式不能冒充已校准成本。

此前格式/语义失败、held-out 未运行及未产生真实 artifact 的结论不变；完整请求前条件在结果目录及
历史快照中保留。恢复采集前须有新的当前计划，不靠本次排期调整降低旧阈值或删除失败。
该工作包不再阻塞独立 SchedulingSession 或四 D，但仍是 Filter 优化结论的必要前提。

<a id="carrier-audit-work-package"></a>

### 工作包六：按实际路径增量做 carrier audit

为四 D、算子组合、PG 增量桥接和 Filter 第二路径分别形成审查结果，记录已有 hooks 能否表达目标及反例。
完成标准是对应路径通过注册/对象身份、函数属性、plan/placement、prepared/invalidation、权限/RLS、
结果绑定与 lifecycle 检查；需要 core 时先有可重复阻断与最小 patch diff。
多个路径都要覆盖，不能把分阶段审查缩减成只验证一次同步 Map，也不把完整审查作为纯核心单测的前置项。

<a id="function-identity-slice"></a>

**函数对象身份小切片（2026-09-02，已完成并合入 main；成员 DDL 临时操作要求已确认）。** 基于 `c494e1b2`，
SQL/EXPLAIN 已复现同名非成员误接管；已检查扩展缺失、其他 schema/重载、删除重建和函数定义替换的
prepared/generic plan 失效。结果见[身份验证记录](../results/postgresql/function_identity_20260902/README.md)。
本次只读复核的公司对象为 `src/operators/sem_distance_planner.c:lookup_extension_function`，工作副本基于
`4601bf7272766d18d370ab95c588cb708d3d1d87` 且有未提交修改：该函数在名称和参数解析后检查扩展成员关系。
采用其对象身份检查原则，由自有 `extension.c` 使用 PG18.3 catalog API 实现最小校验；不复制源代码，
不增加动态 schema、对象缓存或通用 registry。先观察反例失败，再改实现。测试放独立 TAP 文件，核对
普通函数结果、不生成语义 CustomScan、真正成员仍被接管，以及同一 backend 的准备计划重建；测试不调用
真实模型。公共 runtime、SQL 属性、plan/wire 版本与 Filter 标签保持不变。四 D 的语义合同及其任务构造
整理仍是后续独立切片，不能以此次身份验证宣称已完成。
额外诊断表明，PG18.3 仅修改扩展成员关系时不自动重建已有 generic plan。按用户确认，临时操作要求
只针对函数定义未变的 `ALTER EXTENSION … ADD/DROP FUNCTION`：暂停相关查询，结束旧事务与游标，
成员 DDL 提交后让每个相关物理 backend 执行 `DISCARD PLANS` 或重连，再恢复使用；连接池必须覆盖
实际数据库连接，不能只刷新 DDL 会话。无法保证所有连接刷新时，不采用该方案。
双会话测试须保留函数 OID/定义，分别记录 ADD、DROP 刷新前计划，并验证读会话刷新后重新规划，准备
语句仍可使用。函数定义替换、删除重建的自动失效测试不能延期。仅成员变更的跨会话自动失效仍为
pending，不支持在线无感变更，不把成员移除当即时权限撤销；本次不增加跨会话 registry 或 core patch。

**可组合一元算子子切片（待实现）。** 先用两个 Filter 的 AND 组合检查独立计划、输入绑定、顺序与
计数，再在四 D 可执行后验证 Filter → 真实 Map。共同分析只管理调用位置、查询层级和依赖，算子
builder 管理自身 placement/关系语义；执行状态与 provider 关联按节点隔离。同一 gateway 的多会话
测试先满足 §6.4，不以两个独立 gateway 绕开共享入口问题。首版固定可解释的执行顺序，不加入谓词
重排、融合、异步或通用 DAG 引擎；相同输入值的不同调用仍独立关联。

该子切片覆盖 projection、LIMIT 前后位置、NULL/UNKNOWN、prepared plan、INSERT、错误/取消和
资源总上限。NOT/OR/CASE、参数化 Join、aggregate/window 等不由 AND/Map 组合推定支持；确需时先
定义值语义、条件求值、rescan/参数失效与模型调用规则。未实现形状继续明确拒绝。

### 工作包七：独立增量核心与数据库桥接分开验收

**独立核心（可先推进）。** 从[现有 SynchronousScheduler](../../code/src/scheduling/core/scheduler.py)
及其调用方补 characterization tests：固定接纳、路由、容量、completion/error 和资源释放行为。
保持旧同步 runner 可用，在真实消费点抽出增量状态机，不复制已有组织、credit 或 routing 实现。

首个核心切片用公开 sealed tasks、deterministic execution Adapter 与可控时钟，验证：

- 输入未 EOS 时可交付完成结果；offer、advance、seal、cancel 的顺序和终态明确；
- 单项只发布一次终态，重复 payload 仍有独立关联，未知/重复 completion 明确拒绝；
- tasks、payload bytes、estimated work 和完成缓冲有明确上限；未接受项所有权不转移；
- 空 offer、零接受、seal 后 offer、取消与晚到完成、执行失败、永久背压/无进展均有可终止测试；
- 取消停止新 admission，完成结果不再发布；本地 credit/队列/lease 按真实资源恢复，不承诺模型立即停算；
- 已提交但远端终止未确认的 work 单独记录，不把本地资源释放写成模型完成或空闲容量；
- 旧接口结果/错误可观察行为保持，组织与调度策略能通过同一 Interface 独立测试。

之后逐步加入 work-aware organization、accepted-prefix 语义、有界多在途与乱序、单租户多 Job admission/
routing/scheduling，再由可替换的 Ray/模型 Adapter 验证执行与观测。每一步有独立测试和证据；
fixture 或公开外部 workload 的结果继续按真实身份报告，不写成 PG 算子结果，也不自动扩旧 GPU 参数矩阵。

**PG 桥接（另行验证）。** 依赖四 D 或另一条已具备真实语义与生命周期资格的算子，以及独立核心
对应功能通过。先让中立任务进入同一个执行核心，再在有界执行确需时版本化扩 PG port/wire；
PG 保留 tuple binding、snapshot、order/result 与 cancel，gateway 负责适配而不读取 SQL。
单项与批量入口复用的参照见 [§8.8](#pgml-engineering-reference)；批量形态不重定义单项语义，
也不以全量收集输入作为接口前提。

完成条件：实际 PG18.3 plan→task→core→completion→SQL 关联通过，普通 SQL/旧算子不受影响；
新 batch 接口补充接受前缀、输入结束、乱序、早停、异常、取消隔离和全部缓冲的资源检查。
PG 新增语义/接口的审查通过后，才做本路径真实匹配 E2E；Filter 第二路径不是生成型 Map 接入的先决条件。

### IMLane-like placement 与远期工作

DB batch preserved / provider rebatch 在真实 PG 增量接入、取消/backpressure 与匹配条件满足后对照；
分别记录 child pull、组织、提交、模型、fan-in、overfetch、取消浪费和服务空闲，组织策略 owner 只有一处。
Join、aggregate、fusion/AQE、Kalypso-like lineage/KV、图像动态/HSE 和旧 SAOR formal 均不由本次
并行排期自动启动；需要实际需求、独立计划和相应资格。LOTUS compatibility/native baseline 不阻塞主实现。

## 10. 查询、取消与事务正确性

当前同步路径：LIMIT/early stop 或 statement cancel → 停止继续消费 child → 正常 End 或 query cleanup
幂等 close provider session/关闭客户端 FD。SQL cancel 仍可通过 PG longjmp；没有显式 cancel frame。
关闭后的迟到 completion 不再形成 SQL 结果，但 gateway 可能到同步 HTTP 完成/截止时间才发现断连。

HTTP deadline 不覆盖空闲 UDS 会话的任意等待；系统 DNS worker 无法强制终止，只限制未完成解析数量。
listener 属于 gateway 进程，单个查询 close 不删除监听 socket。不能把这些能力表述成远端 GPU 立即停算、
所有线程立即归零或任意情况下 graceful shutdown 都有界。具体资源反例见四 C 的验证要求及当前实现说明。

普通 SELECT 已发出的 DataRow 不能物理撤回，最终失败不算成功查询，取消后不再发布新行；
INSERT 的数据库效果由 PG transaction 决定。模型调用不可回滚；exactly-once 只描述被数据库接受的
终态/结果，不表示 exactly-once inference。retry、重连、attempt identity 只有独立设计后才允许增加。

| 反例 | 当前/目标要求 |
|---|---|
| 残留 marker、非法 SQL 形状 | 在已声明阶段明确拒绝，不回退普通 HTTP UDF |
| 修改 payload、错误版本、错误模型/摘要、重复或越序 completion | 当前同步 session/sequence 校验拒绝，错误脱敏且终止 session |
| NULL、空串、UNKNOWN、无效模型文本 | 各算子按自己的计划解释；provider 不猜真值、不修补输出 |
| no-task、LIMIT、ERROR、cancel、savepoint 与新查询 | 懒连接、本地回收、结果隔离和恢复通过；不能要求不存在的 provider.cancel |
| 同名函数、不同扩展成员、参数改变与重复扫描 | 按工作包六先验证对象身份和重扫语义；未开放 rescan/EPQ 的路径继续拒绝，不能把再次调用模型当作无影响的回调实现 |
| 后续多个算子/会话 | 每个节点的计划、计数与完成关联独立；空闲会话与容量不足的处理按 §6.4 验证，不由单节点测试推出通过 |
| 后续多在途/乱序/输入 seal | 仅在新增接口实现后验证前缀所有权、唯一终态、reorder 上限、取消和排空；不写成当前能力 |
| provider crash 或断连 | PG 明确错误；外部计算不保证回滚，未提交 INSERT 的数据库效果由 PG 回滚 |

## 11. 实验与 baseline

独立核心、PG 功能接入、provider matched comparison 与 native full-system comparison 分开记录。
原生 baseline 保留自己的 execution/scheduler owner；自写 control 不冒充 LOTUS/Sema/IMLane/Daft/Ray 原生。
同一 task/model/generation/service/capacity 与适合任务的质量要求满足后，才归因组织/提交/路由；
减少模型调用与同量 work 执行更快分开评价，阶段可能重叠，不要求分阶段 wall time 相加等于 E2E。

正式实验继续遵守[baseline reference](baseline_reference.md)、根环境规则及具体计划。公开 AutoDL
和公司环境的材料与授权按 §8.7 区分；本次修订没有运行实验或批准新模型下载。

## 12. 当前不能声称

- 四 C 的同步工程验证可外推为任意规模、多会话、任意模型或完整优化系统的验证；
- 四 D、算子组合、增量 SchedulingSession、PG batch/reorder、公司 Adapter 已因计划存在而实现；
- fixture、emulated producer 或历史外部执行结果来自新增的 PG 内置算子路径；
- choice 格式合法代表自然语言判断正确，或 raw text 可返回代表任务质量达标；
- 独立核心测试替代 PG lifecycle、资源、语义/质量或真实匹配 E2E 验证；
- extension 必然不够、core patch 必然需要，或一次兼容示例证明任意数据库/生产支持；
- 进程解耦、批量提交、原生模型 continuous batching 或已有文献机制本身是新增研究贡献。

## 13. 文档职责与维护

| 入口 | 唯一职责 |
|---|---|
| 本文 | 当前架构、分工、工作包依赖、完成条件和可声称范围 |
| [四 C 专项完成记录](completed/postgresql_choice_profile_engineering.md) | 保存 choice 字段/版本/预算/资源与当时的详细实施要求；结果看证据台账，后续工作看本主计划 |
| [四 D 生成型 Map 合同](postgresql_semmap_generation_contract.md) | 唯一定义生成型 Map 的 SQL、消息/文本语义、版本、golden vectors 与实施验收；合同定稿，不代表代码完成 |
| [INFRA_STATUS](../../code/INFRA_STATUS.md) | 实际源码结构、接线、协议版本、测试状态及未实现能力 |
| [证据台账](../results/EXPERIMENT_EVIDENCE_REGISTRY.md)及结果目录 | 提交/构建身份、测试数字、运行配置、失败、原始记录与证据包；结果目录保留请求前条件 |
| [历史快照](archive/postgresql_ai_semantic_operator_architecture_serial_20260901.md) | 旧顺序、原接口表述与完整历史合同，供溯源，不授予执行权限 |

后续具体实验条件不继续堆入本文。未运行的实施设计与实验结果分开，不覆盖既有失败证据；状态更新
只修改真实受影响的入口。旧链接 `#choice-profile-engineering` 保留为专项计划指向，避免已有引用失效。

## 14. 依据与范围

Sema/Cortex 的数据库语义所有权、LOTUS 的 reference/optimized algorithms、IMLane 的 DB/runtime
batch placement、Kalypso 的条件性 dependency/KV 参考，统一由
[架构研究与一手来源审计](../../research/sema_native_semantic_operator_architecture_reference_20260827.md)
及[知识库](../../research/knowledge_hub.md)说明。本次并行排期和文档拆分是工程决策，不是新的文献结论。
