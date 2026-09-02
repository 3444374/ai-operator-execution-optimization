# PostgreSQL choice profile 工程接入（工作包四 C）

更新日期：2026-09-02
状态：`in-progress / wired-and-insert-validated-on-branch / resource-and-real-model-pending`

文档角色：只定义 choice 的 SQL opt-in、版本化字段、兼容性测试、预算和完成条件。
跨工作包依赖、模块分工和公司接入由[主架构计划](postgresql_ai_semantic_operator_architecture_20260827.md)维护；
源码是否已实现看[INFRA_STATUS](../../code/INFRA_STATUS.md)，运行结果看
[证据台账](../results/EXPERIMENT_EVIDENCE_REGISTRY.md)。本次文档修订不增加实现或运行证据。

本工作包只增加 Filter 的受约束生成能力。真实生成型 SemMap 由主计划四 D 单独定义，不能照搬
Filter 的三值 parser、8-token 上限或 choice 输出集合。SemLoom 独立核心研发不以本工作包的模型质量
通过为前提；真实数据库接入与语义优化仍分别满足主计划的验收条件。

本节是本轮工程设计与验收的唯一详细入口。来源是现有源码接口、此前 choice 格式诊断及本次设计
审查；它是工程决策，不是新的算法或质量实验结论。值合同、PG plan、中立 open spec、wire v4/gateway
接线和受限 Filter INSERT 已在独立分支完成，main 尚未合并；新资源与受限真实模型验证仍待完成。
提交身份与验收范围只从 INFRA_STATUS 和证据台账读取，不把分支验收描述成 main 已可运行。

**目标与非目标。** 证明数据库能显式选择、保存、传输并验证受约束生成配置，保持现有生命周期和
旧行为；不证明 reference 语义质量、性能收益或成本精度。新配置是 opt-in、unqualified 的工程能力，
不成为默认 reference，不恢复正式校准，不用于第二路径质量比较。工作包五保留的旧资格失败、
标签、阈值和 held-out 数据不因本节而改变。“先有语义资格才能编码 choice”的旧实验执行前提，
只对本节的独立工程接入不再适用；质量采用和成本校准仍须另行验证。

## C.0 算子目的与公司工程参照

Filter 的业务目的为按自然语言条件筛选行，不要求所有模型都输出 UNKNOWN。公司当前实现未命中
缓存时直接调用大模型，解析 True/False；embedding cascade 只是预留，尚未参与执行。
本工作包实现的是此前选定的 `choice.tristate.v1`，不是全项目唯一允许的 Filter 输出模式。已有三值
接口保持兼容；贴近公司的二值输出可作为后续明确合同的候选，但不在同一个 profile ID 下删除 UNKNOWN、
放宽 parser 或把调用失败变成 false，也不因此重新解释旧质量失败。
保留三值与未来移植不矛盾：在声明的 WHERE 范围内，目标 Adapter 可以按同一 keep/drop 规则恢复
关系结果，保留原始模型输出；SQL 表面二值不要求内部丢失 UNKNOWN。其他 SQL 位置与公司原有
NULL/error 语义另行核对，不能只凭筛出的行相同就宣布全部等价。

按[主计划的具体参考表](postgresql_ai_semantic_operator_architecture_20260827.md#company-engineering-reference)
核对 Filter 的目的、有效参数与错误处理即可，不必通读整个 demo。当前吸收“请求实际参数先确定、
编码和身份共用同一来源”的经验；不将公司宽松二值解析、GUC/HTTP 配置或未启用的 cascade 搬进本切片。
若调整已完成部分，先写可观察反例与保留行为，按主计划定点修改，而不再次拆分已经验证的公共层。

完整算子工程对照、PG 接入方式选择、公共 task 编译重构、重扫语义和多算子/多会话能力由
[主计划的改造顺序](postgresql_ai_semantic_operator_architecture_20260827.md#operator-engineering-actions)
维护，不扩成本工作包的新验收项。当前收尾仍按 C.5 验证单会话资源与受限真实服务；多会话不由此通过。

## C.1 SQL 选择与版本分流

保持三参 `ai_semantic.filter(input, instruction, options)`。新 options 只比旧配置多一个字段：

```json
{
  "model": "<fixed-model-id>",
  "temperature": 0,
  "max_tokens": 8,
  "generation_profile": "semloom.generation.choice.tristate.v1"
}
```

这是独立分支已接通的 SQL 选项，main 尚未接入。早期 plan-only 切片的临时 `0A000` 拒绝已移除；
新路径复用 lazy open，LIMIT 0、zero-row 与 NULL-only 不创建 provider 会话。旧端或不受支持的版本
仍明确拒绝，不能回落旧 v3 或无约束请求。
上述 profile selector 是本节唯一名称，不增加其他别名；它由下述 profile ID 和 version 组合得到。

| SQL options | 计划与 wire | 行为 |
|---|---|---|
| 恰好原有三个字段 | 原 schema 2 / wire v3 | 原计划内容、digest、wire bytes、错误与稳定 EXPLAIN 字段不变 |
| 恰好四个字段且 profile 为上述值 | 新 schema 3 / wire v4 | 显式 choice；非默认、未通过质量验证 |
| 未知 profile、null、非字符串、缺字段或多余字段 | planning 阶段拒绝 | 不打开 provider，不发送模型请求 |

原有 instruction/options 的计划期常量、类型、长度和数值要求继续成立；schema 1 / recording wire v2
也保持不变。SQL 不接受任意 choices 列表、供应商 JSON 字段或可变 profile 定义。
`TRUE/FALSE/UNKNOWN` 是算子输出定义，可以进入生产代码；具体测试输入及其预期分类不进入生产实现。

## C.2 数据库拥有的 profile 与身份

PG 在 planning 时展开一个小型、不可变、自包含的 profile，而不是让 executor/gateway 临时查表决定
本次生成方式。plan 保存 copyObject-safe 的以下内容，并在 executor 严格解码：

| 内容 | 本轮定义 |
|---|---|
| profile ID | `semloom.generation.choice.tristate` |
| profile version | `1` |
| constraint kind | `CHOICE` |
| 有序 choice UTF-8 bytes | `["TRUE", "FALSE", "UNKNOWN"]` |
| profile digest | 对 ID、version、kind、数量、顺序及逐项 bytes 的规范编码求 SHA-256 |

使用独立 digest domain、现有固定宽度整数及长度前缀 UTF-8 编码规范；具体 canonical bytes 和跨语言
golden vectors 在实现前的合同测试中固定，不预填未经计算的哈希。完整 profile 内容纳入新 semantic
plan digest；只改 profile ID 或只依靠 schema version 变化不足以绑定实际约束。

prompt program、parser、operator 逻辑含义及 `MODEL_REFERENCE_SYNC_V1` family 不因新增表示而改名；
其内容未变的身份继续保留，完整 semantic plan digest 必须不同。`Physical Role=reference` 表示逐个
非 NULL 输入采用 reference 求值，不表示质量资格通过，也不是第二 optimized path。新 EXPLAIN 显示 profile
ID/version/digest，并说明尚未经过质量验证及成本校准；不为此建立通用资格 registry。

SQL 选择随 prepared/generic plan 保存，不能被执行时 GUC、gateway 配置或同名 profile 的新解释覆盖。
provider implementation 仍按查询固定选择；generation profile 与 provider execution identity 分开，
open/task/completion 必须相互核验，不用切换 provider 来隐式切换生成语义。

## C.3 模块职责与协议兼容

- `sem_filter_path.c` / plan spec：解析 opt-in、展开并保存 profile、计算身份，供现有 EXPLAIN 回调读取。
- `PgSemanticRuntime`：仍是 PG plan 到中立 `AiOpenSpec` 的唯一转换点；新增值保持固定宽度类型、
  bytes 与明确所有权，不把 `Datum/Jsonb/MemoryContext` 或供应商参数名带入 port。
- UDS/wire：保留同步单在途、lazy open、借用输入、session-owned completion 与幂等关闭。schema 3
  使用独立 wire v4 的字段集合、版本检查与摘要；v3 不增加可选字段或扩大为未知参数容器。
- gateway：校验抽象 `CHOICE` profile，才转换为已核验服务支持的 `structured_outputs.choice`。
  HTTP 请求不携带 PG 专用 plan 元数据；PG 代码不依赖该供应商字段名。

新旧 codec 复用已有 framing、JSON primitives、session loop、HTTP deadline 与 completion adapter
机制；只有确有第二个变化分支时才抽取公共 helper。版本独立不等于复制第二套 HTTP/socket/runtime，
也不引入通用结构化输出框架、动态 profile registry、异步队列或 capability negotiation 协议。

兼容测试覆盖旧/新查询在升级后 gateway 中分别执行，以及新查询被旧 gateway 明确拒绝；不能把
“旧路径保持可用”写成“旧端支持 choice”。C/Python 对 profile、semantic identity、open bytes、task
payload 和 completion evidence 使用同一组 golden vectors。新执行身份不得冒用旧 wire-v3 身份。

## C.4 错误、资格与校准隔离

未知 profile 在 planning/open 的相应位置明确拒绝；已知 profile 被服务拒绝时沿用现有中立错误类别
和脱敏 SQLSTATE 映射。fixture 必须证明：服务拒绝该请求时只发出一次请求，其中含 choice；没有
删除约束后重试、切回旧 profile 或修补输出。严格 PG parser 继续检查原始 completion。

“HTTP 200 + 合法标签”不能单独证明服务实际执行了 choice。首版只声明对已核验实现/版本/配置的
映射支持，检查真实出站请求并保存服务依据，不把所有 OpenAI-compatible endpoint 都列为支持对象。
若无法核验支持情况，真实 smoke 记为未验证，而不是静默退化。

新 semantic/profile identity 必须使旧 calibration artifact 无法匹配；增加在误配旧 artifact 时执行
新 profile 的 EXPLAIN 测试，验证旧系数未被采用、`AI Cost Calibration` 仍不可用。可以继续使用
明确未校准的工程估计，不生成或发布新的真实 calibration artifact。

## C.5 对照、请求预算与资源保证

新旧对照只改变 choice 约束。prompt bytes、模型文件/revision、tokenizer、chat template、服务版本与
启动配置，以及全部显式和继承生成参数一致；特别记录实际 `repetition_penalty`。自动核对两份实际
HTTP JSON：去除唯一的 choice 字段后，结构、值及其类型一致。分别保存 body digest 和脱敏运行配置，
不以 JSON 文本排版差异或只检查两个参数代替完整比较。

绝大多数验证由 deterministic/HTTP fixtures 完成。真实 smoke 复用已有模型与环境，累计最多
**100 次模型请求尝试**，是整个切片上限而非每 profile/进程的额度；预热、失败、超时、预定重复与
任何意外重试均计入。验证 runner 在唯一实际出站调用前预留并持久化额度，达到上限或无法核对累计
次数就拒绝继续；无法确认是否已发送的尝试保守计入。计数可通过测试 adapter/observer 注入，但预算
和 ledger 属于验证工具，不进入生产 gateway、PG runtime 或新的调度系统。

先完成 runtime preflight；不下载新模型、不新增服务器、不扩 GPU 矩阵、不用校准 held-out，也不为
得到更好标签而继续搜索 prompt/model。环境不满足时保存 pending 原因，不把本地 fixture 当真实结果。

取消与资源验收分开记录：

- PG statement cancel 及时终止本地查询并关闭 provider session；不声明立即终止远端 GPU 计算。
- 同步 gateway 可能到 HTTP 完成或其 deadline 才发现 UDS 断连；验证正常 DNS 下 HTTP FD、定时器与
  accepted socket 无累积，并测试返回时客户端已断开以及随后新查询恢复。
- 阻塞的系统 DNS worker 不保证在 deadline 消失；验证同一 adapter 最多保留一个未完成解析且重复
  取消不增长。HTTP deadline 不等于整个 UDS 会话的 deadline。
- listener socket 文件属于 gateway 进程，单次查询取消后应保留；测试结束时先收回本轮客户端，
  再验证 gateway 实际退出后的自有 socket 清理。当前空闲连接可能阻塞 graceful shutdown，不能借
  HTTP timeout 宣称任意情况下都能有限时间退出，也不能把新增取消协议藏在本切片中。

RSS/FD/线程采样复用现有流程，在运行前登记采样方式、预热基线与判定阈值；异常及无效采样保留，
不在观察结果后放宽阈值。故意阻塞 DNS 的测试与正常恢复测试使用各自明确的判定条件。

## C.6 按小步实现与完成条件

| 步骤 | 完成条件 |
|---|---|
| 计划与设计（已完成） | 明确工程支持与语义质量分开验收，登记 opt-in/版本/复用范围与预算；工作包整体未完成 |
| 值合同与 PG 表征（独立分支已完成） | C/Python canonical vectors、严格 profile 校验及旧行为测试通过；红测试定位非法节点与列号窄化问题并修复；原失败证据保留 |
| PG plan 接入（独立分支已完成） | 新 options、schema 3、完整 profile/摘要、copyObject、prepared/generic plan、EXPLAIN 与旧校准拒绝已验证；临时执行拒绝已在后续完整接线中移除 |
| 中立 open spec / wire / gateway 接入（独立分支已完成） | query-fixed profile 的所有权与中立映射、v4 严格校验、两侧 golden parity、已知 profile 映射与不降级证明通过；复用公共传输/生命周期，不复制执行栈 |
| 受限 SQL 功能（独立分支已完成） | SELECT 与修复后的单表 Filter INSERT、新旧配置、普通 SQL、prepared/invalidation、EXPLAIN/no-task、NULL/空串/Unicode、错误/取消及事务行为有对应 PG18.3 归档；不泛化为任意 SQL 支持 |
| 新路径资源验证（待完成） | 按 C.5 预先选定设置检查 RSS/FD/线程、no-task、取消及恢复；工具或账本实现不等于资源测试已经通过，旧路径资源证据不能重新绑定到 choice |
| 受限真实 smoke | preflight 与服务支持证据齐备；请求在总预算内，实际参数差异仅为 choice；新配置返回值通过原 parser，model/usage/finish reason/PG 计数一致；旧配置与全部失败如实记录 |
| 交付 | 记录源码/worktree identity、命令/退出码、构建身份、请求计数、失败及 manifest/SHA；未运行项目明确 pending；按实际状态同步文档，不自动合并或推送 |

新 profile 的工程完成标准不包含“九例全部分类正确”、召回/精确率达标、性能改善或 reference 晋升。
已完成项只限已登记分支与路径；本次文档修改不重跑测试或合并源码。后续仍需完成新路径资源与
受预算限制的真实 smoke；后续改动按影响范围复跑相关测试，不能用工具实现或 fixture 代替实际验证。

## C.7 暂存的质量决策（不阻塞本工程切片）

后续分别评价与固定 reference 的结果差异、独立标签下的质量及数据库程序正确性。起始候选任务为
判断一条用户文本是否请求编写、解释或调试代码；只看当前行，已明确代码意图但缺少代码正文仍可为
TRUE，无法判断对象时才为 UNKNOWN，明确非代码任务为 FALSE。SQL NULL 和执行错误单独处理。
这些是候选标注说明，不预先断言模型会判断正确。

用户暂定更重视减少漏选，同时限制误选；召回率 95%、精确率 90% 仅为待后续质量计划确认的候选目标，
不是本切片的验收要求。
标签来源、最终标注说明、验证集规模及统计判定尚未确定；本节不安排人工标注，也不以少量样例的
点估计宣称总体达标。未来须先固定独立数据及判定方法，再评价和校准；已有失败样本、旧阈值和
原始报告不因这组暂定目标而重新判为通过。
