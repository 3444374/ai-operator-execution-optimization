# SemLoom PostgreSQL 内置 AI 语义算子整体架构与实施计划

更新日期：2026-09-02
状态：`current / design-revised / implementation-in-progress`

本文只维护架构决策、Module 职责、Interface、工作包依赖与完成条件。源码实际状态看
[INFRA_STATUS](../../code/INFRA_STATUS.md)，测试数字、提交身份、运行配置与失败看
[证据台账](../results/EXPERIMENT_EVIDENCE_REGISTRY.md)及结果目录；理论与文献依据看
[架构研究](../../research/sema_native_semantic_operator_architecture_reference_20260827.md)。

四 C、真实生成型 SemMap、增量 SchedulingSession 与公司 adapter 均不因文档存在而视为完成。
四 C 的字段、预算和逐项测试只由[专项计划](postgresql_choice_profile_engineering.md)维护。
旧串行顺序、完整资格尝试条件和历史数字保存在[历史快照](archive/postgresql_ai_semantic_operator_architecture_serial_20260901.md)，
不再作为当前执行指令；原始结果没有删除或改判。

## 1. 目标与架构决策

研究对象保持为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
自有 PG18.3 `semloom_pg` 与 SemLoom 数据执行/调度都继续完成；公司 demo 是工程参考，公司 fork
通过薄 Adapter 接入同一个执行核心，不替代自有算子，也不复制调度策略。

数据库拥有 SQL、ordinary child plan、语义计划、结果解释及查询生命周期。模型、Python、Ray、
vLLM 位于进程外；“内置”不表示 payload 不会离开数据库。直接复用 PostgreSQL 的 MVCC、事务、锁、
WAL、snapshot、ACL/RLS、executor 和错误机制，项目只适配新增资源与任务的生命周期。

载体锁定 `REL_18_3`，默认使用 extension 的 marker + CustomPath/CustomScan。针对实际目标的
plan identity、placement 或 executor lifecycle 出现可复现阻断后，才考虑解除该阻断的最小 core patch；
不为“更原生”扩 grammar、storage 或模型 runtime。PG18.4 结果不能替代目标版本的验证。

## 2. 并行研发与接入依赖

以下是可以分别推进的工程工作，不是新增三个研究内容，也不要求立即同时启动三个代码任务。
代码、模型运行、正式实验与公司环境操作按具体任务分别授权，独立研发允许不自动授权外部实验。

| 工作 | 近期次序 | 不再等待什么 | 仍需满足什么 |
|---|---|---|---|
| 自有 PG 语义算子 | 四 C choice → 四 D 真实生成型 SemMap；Filter 的质量、校准、第二路径作为独立工作包五推进 | 真实生成型 SemMap 不等待 Filter 分类质量通过 | 各算子的语义定义、PG18.3 生命周期与兼容性验证 |
| SemLoom 执行与调度 | 现有行为表征 → fixture/sealed-task 驱动增量 session → work organization → 有界提交、多 Job 与路由 | 独立核心不等待 Filter 质量、matched artifact、第二路径或完整 carrier audit | 现有代码行为兼容、资源上限、终态、取消、关联正确性；真实实验另有计划 |
| 公司前端接入 | 只读映射 → 有实际疑点时做 deterministic spike → 稳定后正式 adapter 与环境验证 | 映射核对不等待自有全部功能完成 | 公司权限、可执行中立 Interface、明确差异与获批环境 |

```text
PG：四 C -> 四 D + 本路径 carrier/lifecycle 检查 ----+
                                                   +-> 自有 PG + SemLoom 接入验证
SemLoom：旧行为表征 -> 增量核心 -> 有界执行测试 ----+

Filter：reference 质量 -> matched cost -> 第二路径 -> 优化相关 carrier audit
公司：只读映射 -> 必要的最小 spike -> 获批 fork 正式接入
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
- 三参 exact Filter 使用数据库保存的 instruction、prompt/parser、model/generation 与三值语义；
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
| `AiOpenSpec`（query-fixed） | operator/value kinds、policy、schema/spec identity、algorithm/role、prompt/parser/model identity、semantic/physical digests、`temperature/top_p/max_tokens/n/stream/stop`，以及显式 presence flag 和完整 generation profile |
| `AiPreparedTask`（per-item） | `sequence`、`input`、`canonical_messages`、`semantic_payload_digest`、`is_null` |
| `AiCompletion`（per-item） | `sequence`、`output`、`response_model_id`、`finish_reason`、`prompt_tokens/output_tokens`、`is_null` |
| `AiProviderError` | caller-owned 中立分类、errno、限长脱敏详情及必要固定宽度参数 |

model、generation constraints 与 profile 不逐行复制成 task 字段；四 C 的完整 profile 已从
PG schema 3 接入 C `AiOpenSpec`、session-owned UDS spec 与 gateway v4。
`PreparedSemanticTask/CompletionRecord` 只在旧设计中作为概念名使用，不是当前两个 C struct 的别名。
wire task/completion 可携带身份摘要用于核验，不意味着这些字段全部暴露在中立 C task/completion 中。

### 5.4 生命周期身份与证据

当前 wire v2/v3/v4 依靠一个已建立的 query-scoped session connection、连接内 `sequence` 和相应摘要
关联任务。v3 校验 semantic-spec、physical-algorithm、provider-execution、payload 和 completion evidence；
这些摘要证明内容/执行身份一致，不代替单项序号。tuple binding 留在 PG，sequence 会传到 provider。

当前没有跨进程 `query_id/operator_instance_id/task_id/job_id` 组合，也没有 query-level registry。
未来多节点、多 Job 或重连场景确需时再引入 opaque identity；只有引入 retry 才讨论 attempt identity。
四 C 的 C/gateway wire v4 与 PG SELECT 已通过 fixture 接线验证；真实服务和新资源 smoke 仍待验证，
不能写成已完成生产部署，也不能顺带加入上述 ID。

## 6. 同步 port 与未来增量执行

### 6.1 当前同步行为

当前 `AiProviderPort` 只有 `open/drive/close`，一次 `drive` 接收一项任务、返回一项 completion 或错误。
query begin 固定 Adapter/config 并注册 cleanup；首个非 NULL task 才真正 open，plain EXPLAIN、LIMIT 0、
空输入与全 NULL 输入保持无连接。NULL 不消耗 sequence；输入借用至 drive 返回，completion 存活至
下次 drive/close，PG 及时复制到明确的 tuple context。

非 OK 状态终止 session，先保存中立错误、幂等关闭，再由 PG 映射 SQLSTATE；interrupt/OOM 等
非协议错误保持 PG 原语义。没有自动重试、重连、多在途或显式 `provider.cancel`。

### 6.2 版本策略

recording wire v2 与 exact Filter wire v3 的字段集合、摘要 golden、错误和旧 SQL 行为保持不变。
choice 使用独立 schema 3 / wire v4，详见四 C；生成型 SemMap 的版本选择由四 D 首个设计切片确定。
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
Kalypso-like lineage/KV、跨算子依赖、Join 与 blocking operator 仍只作另行立项后的参考。

## 8. 工程落点与维护方式

### 8.1 当前目录与目标增量

| 位置 | 工程职责 |
|---|---|
| `code/postgres/semloom_pg/` | 自有 PG carrier、plan spec、machine、pump/runtime、neutral port 与 UDS/wire |
| `code/src/execution_provider/` | 公共 gateway、版本化 codec、completion Adapter；旧 extension gateway 只保留兼容入口 |
| `code/src/planning/`、`code/src/scheduling/` | 已有 work/cost、组织、准入、路由和执行代码；增量核心先从真实调用和测试表征中抽取 |
| 未来 SemLoom gateway Adapter | 将中立 task 映射到现有核心，不能再写一套 scheduler；具体文件只在有实现消费者时创建 |
| 公司 fork | 前端差异、provider client 与本地 lifecycle 映射；见 §8.7 |

源码文件树与可运行命令只由代码 README/INFRA_STATUS 维护，不在本计划复制易过期的全量清单。

### 8.2 设计模式与验证

Ports & Adapters 只用于已存在的外部变化；Factory 只负责 query-fixed Adapter 选择；Strategy 区分
真实 operator/physical algorithms；状态机分别封装 PG 生命周期与增量执行状态，不合成万能执行器。
公共 Module 的测试从其 Interface 观察结果、错误、资源与终态；pure/local fixtures 先验证核心，PG 与
外部服务各自验证其 Adapter。保持旧调用兼容，新增 Interface 由真实行为驱动。

### 8.3 carrier audit 与最小 core patch

审查随对应目标增量进行：同步 Map/Filter、真实生成型 Map、batch/reorder、第二 Filter path 分别验证
其 marker/plan identity、prepared invalidation、hook coexistence、placement 与 executor 生命周期。
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
路径相对用户提供的参考目录；后续切片须重新确认实际版本和函数体。这里只记录最小定位与行为摘要，
不复制公司源码、prompt 原文、内部测试数据或日志；详细快照与来源证明保留在授权位置。
同一表格单元内省略目录的文件名沿用前一个文件的目录；自有 C 文件位于 `code/postgres/semloom_pg/src/`。

| 公司参考对象（只读定位） | 当前观察 / 值得吸收的经验 | 自有落点与必查用例 |
|---|---|---|
| `src/operators/sem_map_op.c`、`sem_generate_op.c` | 已有生成型 CustomScan、输入列与输出列映射；Map 保存常量 instruction。但执行先收集全部 child 行，模型配置在执行时从 GUC 获取 | 四 D 复用自有 `sem_path.c` 的 Map lowering、`sem_plan_spec.c` 和 `sem_pump.c`；参考列映射与 SQL 场景，保留增量取数和 plan-owned 语义。测普通投影、重复值、LIMIT、prepared plan、大输入与取消，不整文件替换自有 carrier |
| `src/operators/sem_map.c:sem_map_build_params`、`src/llm/llm_prompt.c:llm_build_per_row_requests` | 固定指令/模板与逐行内容分开，一个 per-row 请求对应一行 | 四 D 从 `sem_operator_machine.c` 的 task 构造和 Map machine 接入版本化 prompt；固定内容与逐行内容的编码有独立向量，Unicode、空串、输入含模板样式字符不丢失或二次解释。先支持一个明确文本生成合同，不带入全部任务枚举、few-shot/COT 功能 |
| `src/llm/llm_protocol.{h,c}:llm_effective_request / llm_resolve_request` | 请求默认值先消解，再给编码与缓存使用；当前视图借用请求/GUC 字符串且含 PG 依赖和 endpoint 方言 | 吸收“实际参数只有一个来源”：`sem_plan_spec.c` 保存语义参数，`pg_semantic_runtime.c` 只转换到中立 `AiOpenSpec`；HTTP endpoint/方言检查归模型 Adapter。测缺省与显式参数、配置变化、身份与真实出站字段一致，不直接把该 C struct 作为跨进程合同 |
| `src/llm/llm_chat.{h,c}`、`llm_error.c` | 请求编码与响应解析分开，结果有 model、usage、finish reason 与错误分类 | 需要相关能力时补在 `code/src/execution_provider/adapters/openai_compatible_fixed.py` 等实际模型 Adapter，复用自有 framing/deadline/脱敏错误机制；测试错误模型、截断、缺失 usage、无效编码。只吸收本次需求，缺失观测不得填零冒充实际值 |
| `src/operators/sem_filter.c:sem_filter / parse_bool_from_llm / filter_cascade_check` | 当前是标量 boolean 函数：未命中缓存时调用大模型，再宽松解析 True/False；输入 NULL 返回 false，调用无结果或解析失败告警后返回 false。没有 UNKNOWN 输出类，embedding cascade 始终 disabled | 先对齐自然语言条件筛行的目的；二值是贴近公司的候选，不要求所有 Filter 都有 UNKNOWN。保留现有三值 profile 的兼容与严格解析；新增二值或容错策略须单独定合同/身份。向量粗筛不是公司已有能力，不可直接记为复用完成 |
| `src/llm/llm_batch.c:llm_batch_execute`、`llm_prompt.c:llm_send_per_row` | 多种请求汇入统一批执行入口；prompt 层在它上游查缓存，batch 内有 native serial/multi 与传输重试 | 作为公司未来执行接入的候选 Seam，见 §8.7.4；不是自有 PG 新建 curl 执行器的理由。验证请求数、原行关联、出站参数、错误与清理，并显式处理缓存/重试 owner |
| `src/llm/llm_cache.c`、`llm_tokenizer.c`、`llm_prompt.c:llm_build_merged_requests` | 完整有效请求参与缓存；有本地 token 计数及估算回退；merged 会把多行写入一个 prompt | 缓存、精确/估计 work、prompt 合并分别留给实际需要的策略切片。多行合并改变模型请求，不等于通信组批；质量/身份/可拆分性单独定义。当前不搬入 PG 连接池或分词依赖 |
| `test/sql/sem_map_offline.sql`、`sem_generate_offline.sql`、`llm_offline.sql` | 可参考 NULL、空数组、配置与 SQL 拒绝条件的测试组织；公司 Map 会把行内 NULL 当空串发送 | 在自有 regression/TAP 写公开合成用例，保持自有 NULL 不调模型、空串单独处理。测试方法可以参考，预期值按自有合同制定；不复制私有输入，不把公司旧测试通过当作本路径通过 |

本表不是要求每次通读公司仓库。四 C 只核对参数身份、错误与 Filter 差异；四 D 重点读 Map/Generate、
prompt 与结果处理；公司执行接入再读 batch/config/cancel；算子优化仍以相应论文、公开实现与自有
实验为依据。缺少参考目录只阻塞依赖该私有实现的移植判断，不阻塞按公开依据完成自有算子。

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
| plan-owned 语义、严格 Filter、独立分支中的四 C 切片 | 保留现有接口及原证据；二值 profile 或公司兼容策略可另行明确，但不静默修改已有三值身份和错误表现。分支是否完成/合并以源码和各自证据为准，不把已完成状态当作语义永远不能调整的理由 |
| prompt、有效生成参数与结果解析 | 四 D 引入真实消费者时，检查是否存在重复的同义编码、默认值消解或结果判断；只有确有相同变化原因才抽取 helper。行为变化独立版本化，结构重构保持旧输出与错误 |
| operator strategy 与 PG binding | 可独立表达的计算和关系 disposition 留在语义 Module；SQL/Plan/slot 操作留在 PG adapter。实际公司移植需要的局部适配可做，但不为假想数据库改造全部现有代码 |

修改已完成代码前，切片记录须写明具体耦合/反例、涉及调用方、保留行为和复验范围。没有发现问题时
记录“保留现有实现”即可。不得用“学习 demo”替代问题定义，也不能把尚未移植当成已完成代码有缺陷。

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

#### 8.7.5 研发顺序与完成记录

| 时机 | 具体动作 | 该步完成条件 |
|---|---|---|
| 现在：自有算子开发前 | 按 §8.7.2 只读涉及的 demo 文件，并对照自有已完成部分 | 在当前切片写明来源版本/工作副本、文件/符号、观察、采用/适配/保留/延期及原因、自有落点、验证用例；源码观察、设计建议与运行证据分开 |
| 四 C / 四 D 的自有实现 | 四 C 继续自己的显式 choice 合同；四 D 吸收生成算子经验并复用自有公共层 | 按各自工作包验证，旧路径不变；不能用 demo 替换严格语义或跳过 PG18.3 验证 |
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

## 9. 工作包与完成条件

工作包一至四 B 的已完成工程不在此重复提交与测试数字；实际状态见 INFRA_STATUS，原过程见历史快照。
本节只保留待完成工作及与其他工作包的依赖。

<a id="choice-profile-engineering"></a>

### 工作包四 C：可选 choice 生成配置

目标是显式选择三值受约束生成，保留旧 SQL/schema/wire，不成为默认或质量合格的 reference。
PG 保存自包含 profile，gateway 做供应商映射；新 identity 不匹配旧 calibration artifact。
当前已完成值合同、PG plan 保存/严格解码/EXPLAIN、C port/wire v4 和固定 HTTP choice 映射；
PG choice SELECT 已进入公共 runtime，并通过 fixture/lifecycle 与旧路径兼容验证。
新资源 smoke 与真实模型验证仍待完成。此前发现的 Filter INSERT 未 lowering 已独立修复：
仅调整源查询上拉后的 carrier 识别，171 项新增测试验证真实写入、事务和拒绝形状，完整 PG18.3
TAP 919/919。结果见[INSERT 验证](../results/postgresql/semfilter_insert_20260902/README.md)，不扩大 core/runtime。
[四 C 专项计划](postgresql_choice_profile_engineering.md)是字段、canonical vectors、错误、累计请求预算、
资源验证和逐项完成条件的唯一入口。完成工程接入不恢复 Filter 真实校准，也不用于第二路径质量结论。

<a id="composable-operators-work-package"></a>

### 四 C 之后的独立切片：两个 Filter 的 AND 组合（待设计与实现）

首先完成四 C 既定资源与受限真实服务检查，再以同一表上的两个 Filter AND 谓词推动可组合计划。
目标不是逐条解除 SQL 特例，也不把 marker 改为逐行调用模型的普通函数。调用的逻辑语义、计划位置/
物理方法、具体外部执行分别拥有明确职责；保留已有 scan/pump/machine/runtime/provider 底座。

公司参照的实际区别：2026-09-02 只读 `x_semantic` 的 `4601bf7` 工作副本，相关文件有未提交修改；
`sql/x_semantic--0.1.0.sql:sem_filter` 是可执行的 C boolean 函数，而
`src/operators/sem_map_op.c` 明确限制每查询层一个调用。可借鉴表达便利性与算子经验，但普通函数可
组合不证明其专用计划支持任意组合。所给目录是 PGXS extension，没有据此推断未提供的公司 core。
自有方向和能力仍由本计划定义；未来移植包括算子方法与 SemLoom，不仅是公司 Adapter。

| 实施顺序 | 最小职责及验收 |
|---|---|
| 先固定行为与 SQL 范围 | 仅同表两个独立 Filter 的顶层 AND；不同或相同 instruction/input 都是独立调用。固定 source/call identity、规划期选择的链顺序、三值/NULL、错误、短路与任务计数预期；不承诺 SQL 文本从左到右求值，不重排/融合/缓存去重 |
| gateway 有界多会话 | 当前 `server.py` 在一个 session 返回后才 accept 下一个，不能直接支撑上下游各持连接。先验证两会话同时握手/推进；分别限制握手中、活动及等待会话，容量不足明确拒绝或有界结束，不能让已持连接的节点无限等待下一连接。每会话仍同步单在途，PG 内不加 listener |
| 公共 planner 调用分析 | 抽取 Map/Filter 真实共享的调用识别、位置合法性、输入绑定和依赖信息；每调用独立 plan spec、binding 与 state。Map/Filter 分别构造自己的 paths/关系语义，不建立通用 DAG 或让分析函数管理模型/连接 |
| 两 Filter 接线 | 单条 PG 计划中两个独立 Filter 节点；各自拥有 sequence/identity/EXPLAIN/counters/cleanup。验证输入被前一级丢弃时后一级不创建任务、NULL/错误/取消、LIMIT、prepared/invalidation、容量不足、同 backend 恢复与并发 backend 隔离 |
| 回归与交接 | PG18.3 旧 Map/Filter 与 INSERT 兼容、两会话资源采样和容量/取消清理通过；之后四 D 在同一分析机制上验证 Filter → 生成型 Map。多会话不等于多在途，不顺便扩 port/wire accepted-prefix |

同时保留三条设计不变量：Adapter 隔离数据库与外部执行；Strategy 只表达有实际消费者的执行算法；
未来 Filter 投影/NOT 先将三值解析与 WHERE 的 keep/drop 分开，不直接复用 DROP 代替布尔值。
CASE、OR/NOT、aggregate、Join 等须分别定义条件求值与位置，不能提前执行全部调用。
依据为 [Custom Scan 接口](https://www.postgresql.org/docs/18/custom-scan.html)与
[表达式求值规则](https://www.postgresql.org/docs/18/sql-expressions.html#SYNTAX-EXPRESS-EVAL)，以及知识库
关于 Sema/LOTUS/Cortex 的既有分析；这是工程接线，不宣称新的研究机制。只有可复现的 PG18.3
extension 表达障碍才讨论最小 core patch；SemLoom 独立核心仍不等待全部 SQL 组合能力。

<a id="real-semmap-work-package"></a>

### 工作包四 D：真实 SEM_MAP / AI_COMPLETE 生成纵切面（新增，待设计与实现）

这是四 C 和上述最小组合切片之后的自有 PG 任务，为文本生成 work 提供真实数据库入口，并验证
Filter → Map；不依赖 Filter 三值分类质量通过。
AI_COMPLETE 在这里是工作负载含义，不预先承诺新增同名 SQL alias；SQL 重载与具体 profile 在首个切片定案。

最小执行关系：SQL input/instruction/options → planner-owned SemanticPlanSpec → row-preserving
SemMap CustomScan → provider → raw text completion → PG 输出列。仍先同步单在途，再对接增量核心。

| 切片 | 完成条件 |
|---|---|
| 先定最小合同 | 明确 SQL 形状、plan-time constant、prompt program、text-output contract、model/generation identity、NULL/error/order、输入/输出上限与版本选择；在实现前写下拒绝条件和 golden vectors |
| deterministic 纵切面 | 每行输出关联与顺序正确，NULL 不调模型，空字符串与 SQL NULL 区分；规划/执行真正消费上述字段，普通 SQL、recording Map 与现有 Filter 保持行为 |
| 固定模型纵切面 | 复用外部 completion Adapter，记录真实 model/usage/finish reason；text encoding、长度、截断/非 stop 的处理按预定合同验证，不以能解析文本宣称内容正确 |
| PG18.3 与资源 | plain EXPLAIN/EXPLAIN ANALYZE、prepared/invalidation、snapshot/权限、INSERT/rollback、错误/取消、RSS/FD/大输入输出全部通过；证据绑定真实源码与路径 |
| 交接给 SemLoom | 使用同一 semantic task/result Interface 和已验证的同步路径作对照；增量桥接由工作包七独立验收 |

不照搬 Filter 的 8-token 上限或三值 parser，不复用不匹配的校准 artifact。新 generation/profile、schema
和 wire 的具体编号留给合同切片决定，既有版本不改变含义。生成任务更适合观察输入/输出 work 和长短
任务干扰，但不自动产生性能结论：真实比较仍须选定任务质量指标、模型/服务/资源与相同输出要求。

### 工作包五：Filter 质量、matched cost 与第二 physical path

该工作包保留，和生成型 Map/SemLoom 核心分别推进。先确认任务、标签和统计判定，取得符合要求的
reference；再用同 semantic/model/workload/service 的真实观测及 held-out 检验校准，之后实现
LOTUS/Cortex-like proxy/oracle 两路径、显式质量授权、threshold/evidence 与 reference fallback。

planner 生成可区分且可比较的 paths，executor 按计划执行，provider 不暗换算法。输入基数、NULL-adjusted
calls、usage、AI-work estimate 与真实成本分开；工程启发式不能冒充已校准成本。

此前格式/语义失败、held-out 未运行及未产生真实 artifact 的结论不变；完整请求前条件在结果目录及
历史快照中保留。恢复采集前须有新的当前计划，不靠本次排期调整降低旧阈值或删除失败。
该工作包不再阻塞独立 SchedulingSession 或四 D，但仍是 Filter 优化结论的必要前提。

### 工作包六：按实际路径增量做 carrier audit

为四 D、PG 增量桥接和 Filter 第二路径分别形成审查结果，记录已有 hooks 能否表达目标及反例。
完成标准是对应路径通过 plan/placement/lifecycle 检查；需要 core 时先有可重复阻断与最小 patch diff。
多个路径都要覆盖，不能把分阶段审查缩减成只验证一次同步 Map，也不把完整审查作为纯核心单测的前置项。

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

- 四 C、四 D、增量 SchedulingSession、PG batch/reorder 或公司 Adapter 已因计划存在而实现；
- fixture、emulated producer 或历史外部执行结果来自新增的 PG 内置算子路径；
- choice 格式合法代表自然语言判断正确，或 raw text 可返回代表任务质量达标；
- 独立核心测试替代 PG lifecycle、资源、语义/质量或真实匹配 E2E 验证；
- extension 必然不够、core patch 必然需要，或一次兼容示例证明任意数据库/生产支持；
- 进程解耦、批量提交、原生模型 continuous batching 或已有文献机制本身是新增研究贡献。

## 13. 文档职责与维护

| 入口 | 唯一职责 |
|---|---|
| 本文 | 当前架构、分工、工作包依赖、完成条件和可声称范围 |
| [四 C 专项计划](postgresql_choice_profile_engineering.md) | choice 字段/版本/预算/资源与详细实施验收；主文只保留摘要和指向 |
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
