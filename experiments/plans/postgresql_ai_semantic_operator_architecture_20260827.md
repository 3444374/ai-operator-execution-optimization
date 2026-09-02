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
| `AiOpenSpec`（query-fixed） | operator/value kinds、policy、schema/spec identity、algorithm/role、prompt/parser/model identity、semantic/physical digests，以及 `temperature/top_p/max_tokens/n/stream/stop` |
| `AiPreparedTask`（per-item） | `sequence`、`input`、`canonical_messages`、`semantic_payload_digest`、`is_null` |
| `AiCompletion`（per-item） | `sequence`、`output`、`response_model_id`、`finish_reason`、`prompt_tokens/output_tokens`、`is_null` |
| `AiProviderError` | caller-owned 中立分类、errno、限长脱敏详情及必要固定宽度参数 |

model 与 generation constraints 不逐行复制成 task 字段；四 C 拟加的 generation profile 当前尚不存在。
`PreparedSemanticTask/CompletionRecord` 只在旧设计中作为概念名使用，不是当前两个 C struct 的别名。
wire task/completion 可携带身份摘要用于核验，不意味着这些字段全部暴露在中立 C task/completion 中。

### 5.4 生命周期身份与证据

当前 wire v2/v3 依靠一个已建立的 query-scoped session connection、连接内 `sequence` 和相应摘要
关联任务。v3 校验 semantic-spec、physical-algorithm、provider-execution、payload 和 completion evidence；
这些摘要证明内容/执行身份一致，不代替单项序号。tuple binding 留在 PG，sequence 会传到 provider。

当前没有跨进程 `query_id/operator_instance_id/task_id/job_id` 组合，也没有 query-level registry。
未来多节点、多 Job 或重连场景确需时再引入 opaque identity；只有引入 retry 才讨论 attempt identity。
四 C 的 wire v4 仍是待实现的同步扩展，不能把它写成已部署协议或顺带加入上述 ID。

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

### 8.7 自有主实现与公司前端适配（决策已定，适配尚未实现）

本节维护两类数据库前端与同一个执行核心的工程分工；不是新研究内容或公司接入已经成功的证据。
自有主实现以独立构建、公开可复现为目标，公司 fork 承担工业前端的兼容接入与获批环境验证，不成为主实验的私有前置依赖。

```text
自有 PG18.3 语义算子 -> 自有 PG adapter --------+
                                               | 中立 execution-provider interface
公司语义算子 -> 公司 fork 内 adapter（待实现）--+-> 同一个 SemLoom execution provider
                                                   -> 数据组织、提交、路由、多 Job 调度
                                                   -> 可替换的 Ray / 模型执行 adapter
```

这是目标关系，不是当前接线图。现有自有路径已接到 recording/golden/fixed-model gateway；
增量 SchedulingSession 的独立核心与 PG 接入按工作包七分别推进；公司 adapter 尚未实现，尚无兼容通过证据。

#### 8.7.1 分工、复用与独立性

| Module | 必须继续完成的 Implementation | 不应承担的职责 |
|---|---|---|
| 自有 PostgreSQL 语义算子 | SQL、CustomPath/CustomScan、语义计划、prompt/parser/model/generation identity、结果关系语义、EXPLAIN，以及所需 LOTUS/Cortex-like reference/optimized paths；使用 PG 原生 snapshot、权限与 query lifecycle | 公司私有结构的兼容分支、模型 HTTP、重复实现事务/MVCC/WAL、SemLoom 调度策略 |
| SemLoom execution provider | work 画像、工作单元组织、有界提交、路由、多 Job 调度、可替换执行 backend 与统一观测；只维护一套核心 | 自有或公司 PG 的 Plan/Datum/slot、SQL 重写、隐式更换 prompt/parser 或近似语义 |
| 公司 fork 内的 Adapter | 将公司任务与完成结果映射到中立 Interface，绑定本地 identity、错误、取消和结果恢复；复用该 fork 已有查询生命周期设施 | 复制 SemLoom organizer/scheduler/router，或要求主仓库依赖公司源码才能运行 |

两类前端 Adapter 都只使用当前查询交给它的 tuple/task；gateway 不重新连接数据库读取 SQL 数据，
也不自行实现 snapshot、事务或权限判断。

公司 demo 可提供 SQL 暴露、输入组织、模型调用封装、错误恢复和测试场景的工程参照；采用具体做法前，
核对其在 PG18.3、自有计划语义和生命周期中的条件，不把 demo 可运行等同于 planner-visible 或质量正确。
保留真实且最小充分的自主算子实现，不先扩全套 join/aggregate/structured-output DSL；也不将
SemFilter 降为只有接线测试、从计划中删掉已约定的第二优化路径。后续文本数据执行仍以 AI_COMPLETE
类异构 work 为主场景，其数据库入口按真实语义独立实现；当前 recording SemMap 不因此变成真实生成算子。

采用 Ports & Adapters：变化集中在前端 Adapter 和模型执行 Adapter，公共 Module 隐藏组织、提交和
调度的 Implementation。Interface 同时写清 ownership、顺序、错误、取消和资源要求；测试从同一
Seam 验证 observable behavior。当前已有 recording/golden/fixed-model 等真实替换实现；公司 Adapter
仍是候选，不能据此宣称第二个数据库前端已验证，也不为未来接入先造 registry 或空 Module。

#### 8.7.2 现在要核对的最小映射

以现有 `AiOpenSpec → AiPreparedTask → AiCompletion` 和 `open/drive/close` 为出发点，不把 C header
直接宣布为适用于任意前端的完整 SDK。公开的中立表示表达实际任务及语义要求，PG 专用 plan 留在各自
adapter 一侧；供应商请求字段留在模型 adapter 一侧。两边不要求同 SQL、plan node、C 结构或 wire bytes。

下表是尽早开展只读接口核对的清单，**公司列是待核对问题，不是已经确认具备的能力**。每项结果应注明
可直接对应、需要显式转换或暂不支持；涉及私有实现的证据留在公司授权范围内，本计划只保存脱敏结论。

| 映射对象 | 自有路径的当前事实 | 公司接入需核对 / 不可默默补齐的内容 |
|---|---|---|
| 数据库载体 | 自有目标为 `REL_18_3`，使用受限 CustomPath/CustomScan | 实际 PG 版本、调用入口与 planner 可见性；接通 provider 不等于取得同等优化能力 |
| 算子与计划身份 | plan 保存 operator/spec/algorithm/role；中立 open spec 传递身份 | 能否从实际调用形成稳定语义身份；不得仅凭算子同名认定等价 |
| prompt、parser、model、生成配置 | exact Filter 已有对应 identity；choice 已有 schema 3 plan，port/wire 执行仍待四 C | 能否保留原始语义与有效生成参数；不同定义必须有不同 identity |
| 值表示与大小 | 当前为 text 输入及 text/tristate 结果，有 UTF-8 与长度检查 | 类型、编码与上限是否兼容；多列/图像等未支持表示不能隐式当作 text 接受 |
| 单项关联与顺序 | session 内 `uint64 sequence`，同步单在途，NULL 不占序号 | 可用什么本地关联方式恢复每项结果、处理重复 payload；不要求相同字段名 |
| NULL、错误、结果 | PG 解释 SQL NULL、严格三值 parser 与 keep/drop；错误终止查询 | 两值/三值、空串/NULL、错误/UNKNOWN 是否同义；差异显式记录或拒绝 |
| 取消与关闭 | query cleanup 关闭本地 provider，远端停止能力有限 | 何时获知取消、怎样释放自有资源；不得承诺尚未具备的远端 cancel |
| 模型证据与 work 观测 | completion 带 model、usage、finish reason；estimated work 与 actual usage 分列 | 哪些证据可真实取得；缺失要标 unavailable，不伪造 token 或把估计当实际 |
| 分组与 work hint | 目前一任务一完成；batch、accepted-prefix 与调度 work hint 尚未接入 | 是否允许逐项重组、有哪些不可拆分语义；先记录，不提前增加协议字段 |

设计材料中的 `ExecutionPlan/ExecutionItem` 及 `source_frontend/item_id/chunk_id/work_unit_id/model_batch_id`
只是说明性候选，不在本轮加入代码或正式 ABI。新增字段须由真实消费者、关联关系或错误恢复场景驱动，
遵守本计划的版本化与最小抽取规则。不同 prompt/parser/UNKNOWN 行为不能共用 semantic digest，
也不能让适配器偷偷改答案以通过同一测试；可迁移性与语义等价性分别验证。

#### 8.7.3 传输、组织与调度不能重复拥有

前端为传输效率或 query lifecycle 使用的有界取数/传输块，不自动成为不可拆分的模型 batch；只有
语义允许逐项独立时，SemLoom 才可重组为 work units。已有不可拆分 batch 要显式声明，不能为了适配
擅自拆开。transport chunk、semantic work、work unit 与 model batch 先在概念上分清，暂不为每层加 ID。

公司 fork 不再复制 SemLoom 的 work-aware organization、路由或多 Job 控制；数据库必要的取数窗口、
backpressure、生命周期管理仍保留。IMLane-like database batch placement 也仍是后续研究变量，
并非禁止数据库做任何组批；若选择数据库拥有某项策略，就明确其 owner，不能在两层各实现一套同职责控制器。

#### 8.7.4 接入时机与证据

| 时机 | 动作与完成条件 |
|---|---|
| 现在 / 近期设计核对 | 保留自有两个 Module 的主实现责任；按上表只读核对公司接口，记录未知项，不据此修改公司 fork 或宣称兼容通过 |
| 只读核对留下实际疑点、所需自有 Interface 可执行后 | 经独立授权做最小映射 spike，核对一份 plan、一项 task、一次 completion 和取消/清理路径；先用 deterministic provider，不要求完整模型或性能实验 |
| 自有主路径和 Interface 稳定后 | 在公司 fork 正式实现最小 Adapter、必要 provider client 与内部测试，接入同一 SemLoom 核心；按真实差异选文件和接入点，不预建七个空文件 |

正式适配可以后做，但第一次接口对照不能等两部分全部完成后才开始。映射发现不支持项时，优先在
公司 Adapter 局部处理；只有两个真实消费者证明中立 Interface 缺少必要概念时才版本化扩展，
不把两套 PG 的特例推进 organizer/scheduler。公司兼容不是自有公开主路径的前置验收项，也不据单个
接入示例宣称支持任意数据库或生产环境。自有算子、SemLoom 核心与公司映射可分别推进；独立核心
研发不等待 Filter 质量或第二路径，真实 PG 接入按 §2 的对应路径条件验收。

代码复用与部署分别确认权限：

| 目标 | 允许范围与条件 |
|---|---|
| 公司内网 fork | 在公司制度及授权范围内，可直接复用既有 plan/executor、provider/client、生命周期和测试设施 |
| 可公开主实现 | 公司源码、内部测试、常量或衍生实现只有获得明确外部发布授权后才能进入；否则参考工程行为，依据 PG 官方接口、论文与公开资料独立实现并记录来源 |
| 公开 AutoDL 实验 | 自有 `semloom_pg`、可公开的 SemLoom、公开或获准使用的模型、公开/合成数据；遵守 runtime preflight 与独立实验计划 |
| 公司 fork 实验 | 只在公司批准的环境运行。AutoDL 未获批准时不上传 fork、二进制、容器、数据或日志；内网 deterministic/provider 测试也须在获批范围内 |

拥有 fork 和修改权限不等于拥有外部发布或部署权限；改名、逐文件翻写、打包成镜像均不改变来源要求。
公司兼容既可服务后续实际接入，也可提供环境验证，但主系统可复现性不依赖公开公司私有材料。

## 9. 工作包与完成条件

工作包一至四 B 的已完成工程不在此重复提交与测试数字；实际状态见 INFRA_STATUS，原过程见历史快照。
本节只保留待完成工作及与其他工作包的依赖。

<a id="choice-profile-engineering"></a>

### 工作包四 C：可选 choice 生成配置

目标是显式选择三值受约束生成，保留旧 SQL/schema/wire，不成为默认或质量合格的 reference。
PG 保存自包含 profile，gateway 做供应商映射；新 identity 不匹配旧 calibration artifact。
当前已完成值合同与 PG plan 保存/严格解码/EXPLAIN，实际执行仍拒绝；port/wire/gateway 接入尚未实现。
[四 C 专项计划](postgresql_choice_profile_engineering.md)是字段、canonical vectors、错误、累计请求预算、
资源验证和逐项完成条件的唯一入口。完成工程接入不恢复 Filter 真实校准，也不用于第二路径质量结论。

<a id="real-semmap-work-package"></a>

### 工作包四 D：真实 SEM_MAP / AI_COMPLETE 生成纵切面（新增，待设计与实现）

这是四 C 之后的自有 PG 优先任务，为文本生成 work 提供真实数据库入口，不依赖 Filter 三值分类通过。
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
