# PostgreSQL choice profile 工程接入（工作包四 C）

更新日期：2026-09-02
状态：`in-progress / PG-plan-implemented / port-wire-integration-pending`

文档角色：只定义 choice 的 SQL opt-in、版本化字段、兼容性测试、预算和完成条件。
跨工作包依赖、模块分工和公司接入由[主架构计划](postgresql_ai_semantic_operator_architecture_20260827.md)维护；
源码是否已实现看[INFRA_STATUS](../../code/INFRA_STATUS.md)，运行结果看
[证据台账](../results/EXPERIMENT_EVIDENCE_REGISTRY.md)。文档拆分本身不构成实现或运行证据。

本工作包只增加 Filter 的受约束生成能力。真实生成型 SemMap 由主计划四 D 单独定义，不能照搬
Filter 的三值 parser、8-token 上限或 choice 输出集合。SemLoom 独立核心研发不以本工作包的模型质量
通过为前提；真实数据库接入与语义优化仍分别满足主计划的验收条件。

本节是本轮工程设计与验收的唯一详细入口。来源是现有源码接口、此前 choice 格式诊断及本次设计
审查；它是工程决策，不是新的算法或质量实验结论。首个代码切片已实现不可变 profile 值、严格
校验与独立 C/Python canonical bytes；后续 `00cc6bbf` 已接入 SQL options 和 PG plan，尚未接入
中立 open spec 或 wire v4。当前新配置可规划和 EXPLAIN，执行明确拒绝；后续运行按具体切片验证。

**目标与非目标。** 证明数据库能显式选择、保存、传输并验证受约束生成配置，保持现有生命周期和
旧行为；不证明 reference 语义质量、性能收益或成本精度。新配置是 opt-in、unqualified 的工程能力，
不成为默认 reference，不恢复正式校准，不用于第二路径质量比较。工作包五保留的旧资格失败、
标签、阈值和 held-out 数据不因本节而改变。“先有语义资格才能编码 choice”的旧实验执行前提，
只对本节的独立工程接入不再适用；质量采用和成本校准仍须另行验证。

## C.0 算子目的与本次工程参照

公司与自有 Filter 的目的都是按自然语言条件筛行。本轮保持已选定的三值 profile，UNKNOWN 与 FALSE
在当前 WHERE 范围均不保留行，但原始输出保持区分；执行错误仍终止语句。二值方案需要独立语义
与身份，不混入本版本。公司 embedding cascade 尚未启用，不作为已可复用的优化。

2026-09-02 只读核对 `x_semantic` 基于 `4601bf7` 的工作副本；所涉文件均有未提交修改，不当作固定
release。参考来源只在本计划记录，不写进生产/测试代码或注释；不复制源码、prompt、测试数据或日志。

| 参考文件/符号与观察 | 决定及自有落点 | 验证 |
|---|---|---|
| `src/llm/llm_protocol.{h,c}`：`llm_effective_request / llm_resolve_request` 先消解请求默认值，再供编码使用；视图借用 PG/GUC 字符串 | 吸收实际参数单一来源的原则，不移植私有 struct。已有 PG plan 继续拥有语义；本次 gateway 从已严格验证的 open context 生成逐项 completion request，profile 不按任务或环境变量重新选择 | 完整 profile/semantic identity 校验；出站请求去掉唯一 choice 字段后与旧请求逐字段相同；无约束降级或重试 |
| `src/llm/llm_chat.c` 的响应检查及 `llm_error.h`：截断/协议/内容失败有独立分类 | 保留自有 fixed adapter 的 model/usage/finish_reason、单次调用、deadline 和脱敏错误，不引入公司 HTTP/重试代码 | fixture 的错误模型、缺失 usage、截断、无效 JSON、HTTP 拒绝仍失败，raw output 不改写 |
| `src/operators/sem_filter.c`：`parse_bool_from_llm` 宽松搜词；无结果/解析失败返回 false；`filter_cascade_check` 固定 disabled | 保留自有严格三值 parser 与 fail-query；本轮不改算子 machine，不实现 cascade 或二值 profile | 旧 PG 测试继续通过；gateway 原样返回 UNKNOWN/非法标签，由当前或后续 PG parser 作关系判断 |

本次只先完成 gateway 侧严格 wire v4 与 golden/fixed-model 的请求映射。C 中立 open spec/codec 未接通
前，PG schema 3 继续明确拒绝执行。已有 `wire/framing.py`、session loop、HTTP deadline/resolver 和错误
路径复用；只在 v3/v4 两个实际协议消费者之间共享固定语义编解码，不建立 registry 或通用框架。
供应商 choice 字段依据 [vLLM 0.25.1 官方说明](https://docs.vllm.ai/en/v0.25.1/features/structured_outputs/)
独立实现，只验证 fixture 出站映射；不把本轮结果写成任意 endpoint 或真实模型已执行约束。
gateway 的外部 fixed config 新增可选 `choice_format="vllm_structured_outputs"`，缺省为未声明支持；
未显式设置时拒绝 choice 请求且不发 HTTP，原 v3 不受影响。这是运维对已核验服务的明确选择，
不是 gateway 自动探测能力。HTTP 只新增 `structured_outputs: {choice: [TRUE,FALSE,UNKNOWN]}`；
模型文件、prompt、原有生成字段、超时和重定向策略不变。

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

上述 options 已可用于规划和普通 EXPLAIN；实际执行仍以 `0A000` 拒绝，不是可运行的模型示例。
上述 profile selector 是本节唯一名称，不增加
讨论草案中的其他别名；它由下述 profile ID 和 version 组合得到。

| SQL options | 计划与 wire | 行为 |
|---|---|---|
| 恰好原有三个字段 | 原 schema 2 / wire v3 | 原计划内容、digest、wire bytes、错误与稳定 EXPLAIN 字段不变 |
| 恰好四个字段且 profile 为上述值 | 新 schema 3 已实现；wire v4 待实现 | 显式 choice；非默认、未通过质量验证；目前不执行 |
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

首个值合同切片固定的编码如下：domain 为 ASCII `semloom-generation-profile-v1` 加一个 NUL；
之后依次是长度前缀 UTF-8 profile ID、uint32 version、长度前缀 `CHOICE`、uint32 choice count、
三个长度前缀 choice bytes。所有整数及字节长度均为 uint32 big-endian，字符串不带终止符。
本 profile 共 114 bytes；在写实现前用独立 OpenSSL 对已展开的字节向量计算 SHA-256，结果为
`941327729217db0ad438a8d0c945750485c6047834229aa40912b254d90a24f7`。
完整字节常量和拒绝测试见
[`test_semloom_generation_profile.py`](../../code/tests/postgres/test_semloom_generation_profile.py)。
C 只输出供现有 PG SHA-256 消费的规范 bytes，不另写密码算法；当前测试独立验证这些 bytes 的摘要。
Python record 恰有 `profile_id/profile_version/constraint_kind/choices/profile_digest` 五字段；这是
已解码值的合同，不宣称已实现 JSON 重复字段检查或 wire v4 open/task/completion。

PG 的 schema 3 在原 27 个命名字段外保存一个嵌套 `generation_profile`，内含上述完整五字段，
choices 使用有序 String 节点列表。解码结果逐项复制到指定 context，不借用计划树或注册表。
新 semantic 编码的 domain 为 `semloom-semantic-spec-v3` 加一个 NUL，接 uint32 schema `3`；
其余字段按 schema 2 的原顺序直到 stop，然后追加 uint32 `114` 和完整 profile canonical bytes。
不是只哈希 selector 或 profile digest。对 instruction `Classify input.`、model `golden-model-v1`，
独立 OpenSSL 向量与 PG 实际结果均为
`3624a95a096a8a6b9e838676ec8865315b1f49c27a0e9594cf67a5440792d6c5`；对应旧 schema 2 为
`9ec789eab10d6367b60895288fde154b384edeba1ac0fb603ade0b2424ff2fb9`，physical digest 保持不变。

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
  此映射与 wire v4 在下个切片接入；当前 schema 3 不创建 runtime/provider，普通 EXPLAIN 直接读取
  已验证 plan。任何实际执行（含 LIMIT 0、zero-row、NULL-only、prepared）在初始化 child 前拒绝。
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

本次 v4 合同在 v3 严格字段集合之外：open 增加完整 `generation_profile` 五字段；opened、task、
completion 增加 `generation_profile_digest`。其他字段类型和同步 sequence 规则保持；v3 不接受这些字段。
provider execution、payload、completion 的 domain 分别升级到 `v4`，其编码字段顺序保持；semantic
digest 使用已验证的 schema 3 编码，physical algorithm digest 不变。gateway 在 task 时从 canonical
messages 提取 instruction，重算含完整 profile 的 semantic digest；不能只信任客户端提供的摘要。
error 仍严格为 type/protocol_version/sequence/code 四字段，版本必须为 4，code 使用现有 allowlist。

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
| 值合同首个切片（已实现） | C/Python 对照同一 114-byte 向量；拒绝类型/身份/顺序/内容变更和伪造摘要；C 无分配且短 buffer 不写半帧。后续 PG 切片已链接 helper，AiOpenSpec 仍未扩展 |
| 表征与红测试 | 旧 SQL/plan/digest/wire/错误/稳定 EXPLAIN 快照通过；新 profile、版本、身份与拒绝测试在旧实现上因目标能力缺失而失败；C/Python canonical vectors 已明确 |
| PG plan 接入（已实现） | 新 options、schema 3、copyObject-safe 完整 profile、摘要、prepared/generic-plan 与 invalidation；旧校准拒绝、新执行不回落 v3；旧字段和值行为不变 |
| port/wire/gateway 接入（待实现） | runtime 中立 open spec 映射、v4 严格校验、两侧 golden parity、已知 profile 映射与不降级证明；复用公共传输/生命周期，不复制执行栈 |
| 功能与资源验证 | Python/protocol/static、中立 C11、精确 PG18.3 warning-free `-O2 -Werror`、regression、完整相关 TAP、新旧配置、普通 SQL、prepared/invalidation、EXPLAIN/no-task、NULL/空串/Unicode、错误/取消/资源检查通过 |
| 受限真实 smoke | preflight 与服务支持证据齐备；请求在总预算内，实际参数差异仅为 choice；新配置返回值通过原 parser，model/usage/finish reason/PG 计数一致；旧配置与全部失败如实记录 |
| 交付 | 记录源码/worktree identity、命令/退出码、构建身份、请求计数、失败及 manifest/SHA；未运行项目明确 pending；按实际状态同步文档，不自动合并或推送 |

新 profile 的工程完成标准不包含“九例全部分类正确”、召回/精确率达标、性能改善或 reference 晋升。
代码逐项实现；当前值合同的[验证记录](../results/postgresql/choice_profile_contract_20260902/README.md)
不代替上述 SQL/plan/wire、PG lifecycle 或真实模型接入证据。

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
