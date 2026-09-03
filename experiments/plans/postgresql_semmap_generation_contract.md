# 四 D：生成型 SemMap 最小语义与实施合同

日期：2026-09-03

状态：合同已定稿；已验证消息、C/Python 纯值、Python v5 及旧 PG 路径兼容，
见[本轮记录](../results/postgresql/semmap_values_20260903/README.md)（服务器资格源码 `425d2b1c`）。
包含后续深层 JSON 修复的 `b0400944` 已合入本地 main，各次测试仍绑定原提交，不重新归属旧证据。
独立分支 `codex/semmap-pg-plan` 已完成新三参 Map 的 SQL/schema 4、PG plan 与权限初始化检查，
见 [PG plan 记录](../results/postgresql/semmap_pg_plan_20260903/README.md)（`2205ccbb`，尚未合入 main）。
只支持普通 EXPLAIN；实际执行仍以 `0A000` 拒绝。C wire 接线、完整 golden/模型/资源验收仍 pending，
不能由 plan-only 或旧路径通过代替。

生产代码对照基线：a3199bd9。本文面向研发与审查者，不是已发布功能说明。

本合同规定调用方与实现方必须一致理解的算子行为、数据表示和验收标准。它是四 D 具体 SQL、
prompt、输出、版本和测试预期的唯一入口；[主架构计划](postgresql_ai_semantic_operator_architecture_20260827.md#real-semmap-work-package)
继续拥有系统分工与工作包顺序，[INFRA_STATUS](../../code/INFRA_STATUS.md)记录实际实现。
下面的版本号、上限及失败策略是本稿的明确工程选择，不是文献结论，也不表示已经得到运行验证。
研发复核可以在首次实现前统一修订这些未发布候选并重算向量；已有 Filter/recording 身份不能改义。

## 1. 要完成什么，当前不完成什么

为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化，提供一条可验证的文本生成入口：
PG 从当前查询的普通 child plan 取得一行输入，根据计划中的指令生成一个模型请求，得到文本后填回该行。
一条成功完成的请求不增加或删除关系行；这不是 Filter 分类，也不是 embedding 相似度判断。

本工作包交付一个同步 reference：真实 Map、deterministic golden 和固定模型两种执行 Adapter、
必要的公共任务/完成结果整理，以及 PG18.3 与资源验证。不依赖 Filter 质量或真实成本校准。
本文是语义与工程验收合同，不是性能实验合同；后续数据组织/调度比较的 workload、质量、对照、
资源与统计方法由对应实验计划定义，不在本工作包提前承诺性能收益。

本稿不增加多个语义调用、Map/Filter 组合、异步/多在途、accepted-prefix、连接池、缓存/重试、
prompt 合并、流式 token 返回、工具调用、JSON-schema 输出、公司系统移植或 PostgreSQL core patch。
SemLoom 增量核心仍可按主计划独立研发；本工作包完成不等于它已经接入 PG。
本轮只形成文档，不授权安装、模型下载、真实请求或正式实验；运行仍须有目标环境和对应切片授权。

## 2. 首版决定

| 对象 | 本稿采用的行为 |
|---|---|
| SQL | 新重载 ai_semantic.map(input text, instruction text, options jsonb) RETURNS text；原 map(text) 保持 recording |
| 关系结果 | 每个到达 Map 的 child tuple 对应一个输出 tuple，只替换 Map 输出列 |
| 指令 | 查询计划拥有固定 instruction；system 消息逐字使用 instruction，user 消息逐字使用 input |
| 生成 | temperature=0、top_p=1、n=1、stream=false、stop=null；max_tokens 由 SQL 明确指定 |
| 文本 | 有效 UTF-8、无 U+0000，保留换行、空白和原始文本；空输出是空 text，不是 NULL |
| NULL | input 为 SQL NULL 时保留行、输出 NULL、零任务；空字符串仍创建一个任务 |
| 失败 | FAIL_QUERY；不重试、不切换 provider、不截断、不将失败改为 NULL 或空串 |
| 第一条物理路径 | MODEL_REFERENCE_SYNC_V1 / reference；一个节点一个 session、同步单在途 |
| 版本 | 新 plan schema 4、wire v5；旧 schema 1/2/3 与 wire v2/v3/v4 不改变 |
| 部署 | PG 固定 REL_18_3；采用 PGXS extension，不新建数据库内 listener 或 HTTP 调用 |

“完整文本”只表示模型以 stop 结束且满足输出合同，不保证回答正确、指令完成或内容可信。
temperature=0 也不保证不同硬件、模型版本或重复运行逐字相同。

## 3. SQL、参数与计划行为

### 3.1 入口与形状

以下是目标执行语法；独立 PG plan 分支已注册该重载并支持 EXPLAIN，但当前仍不能执行生成请求：

```sql
SELECT doc_id,
       ai_semantic.map(
           body,
           '用一句中文概括这段文本。',
           '{"model":"<fixed-model-id>","temperature":0,"max_tokens":256}'::jsonb
       ) AS summary
FROM ONLY documents
WHERE doc_id > 0
LIMIT 10;
```

首版沿用受限 SemMap 载体：单个非继承基表、SELECT 顶层输出或直接 INSERT ... SELECT，
允许普通谓词、其他普通输出列及 LIMIT/OFFSET。输入是 text 列或由普通 PG child 求值的 text 表达式；
不在 gateway 重新取数。Map 输出的位置、别名、其他列的值和 NULL 标志均保留。
先选择普通 child 执行路径，再在其上生成 reference Map；LIMIT/OFFSET 丢弃的行不进入模型请求。
没有 ORDER BY 的 SQL 不承诺跨次运行的输出顺序；INPUT_ORDER 只表示不重排本次 child 交付的 tuple。
lowering 不得在普通 child 求值之外额外重复执行 input 表达式。用带计数的 VOLATILE 表达式与等价
普通查询作对照，分别验证无 OFFSET、LIMIT/OFFSET 和 NULL；不把表达式求值次数等同于模型请求数。
child 表达式报错时保留原生 SQLSTATE；首项失败零连接，后续项失败关闭已有 session，停止后续请求。

ORDER BY、多个新旧 Map、Map 与任意 Filter 混用、嵌套 Map、WHERE/NOT/CASE 中的 Map、Join、
子查询/CTE、聚合/window、DISTINCT、集合操作、锁行、并行、参数化路径、rescan/EPQ 仍不在本版范围。
INSERT 不支持 RETURNING、ON CONFLICT 或 OVERRIDING。不得仅删除旧 guard 来开放这些形状。

新重载采用 LANGUAGE C、VOLATILE、PARALLEL UNSAFE、SECURITY INVOKER、CALLED ON NULL INPUT、
非 LEAKPROOF 的 marker。未被合法 lowering 时必须报错，不直接执行模型。
保留 CALLED ON NULL INPUT 是为了在规划时检查 instruction/options；NULL 输入不能掩盖非法配置。
属性含义参考 [PG CREATE FUNCTION](https://www.postgresql.org/docs/18/sql-createfunction.html)，
实际资格仍要求精确 PG18.3，不把属性声明当成执行能力。

### 3.2 options 与常量

options 恰好含 model、temperature、max_tokens 三个字段，均必填，不设隐藏缺省：

| 字段 | 接受值 | 拒绝值 |
|---|---|---|
| model | 非空 UTF-8 字符串，1–128 字节；与 endpoint 配置及响应 model 严格一致 | NULL、空串、错误类型、超长 |
| temperature | JSON 数值零；不同零的数值写法规范为 0 | 非零、字符串、boolean、NULL |
| max_tokens | 数学整数 1–4096，规范为整数 | 零、负数、非整数、超范围、字符串、boolean、NULL |

SQL jsonb 已由 PostgreSQL 处理对象键顺序和重复键；本合同检查处理后的字段集合，
不承诺检测已经被 jsonb 消解的原始重复键。wire JSON 及计划命名节点仍须拒绝重复字段。
max_tokens=128、128.0 或等值指数写法在新 Map 中得到相同计划身份；不改变旧 Filter 的解析规则。

instruction 为非 NULL 的 1–4096 字节 UTF-8 文本，不 trim 或改写。纯空白但非空的指令不被擅自替换。
instruction/options 必须是 SQL 中固定的常量，在完整执行期不变。检查分两步，均只针对已识别的新 Map：

1. 在 planner_hook 收到的已分析 Query 上、调用已有 planner_hook 或 standard_planner 之前，
   检查第 2/3 个参数，拒绝 Param、Var、SubLink 及非 IMMUTABLE 表达式；只排除 VOLATILE 不够，
   STABLE 也不能成为固定参数。遍历须覆盖本合同允许的 INSERT 来源，不改写普通 input/谓词参数。
2. 由 PG 执行普通常量折叠，再在 Map path 构造处要求两者为非 NULL、类型正确的 Const，按本节
   检查实际值。不得用估算用的求值函数放宽折叠规则，或只在这个后期入口拒绝残留 Param。

这是因为 custom plan 能将 PARAM_FLAG_CONST 参数替换为 Const；仅看最终节点无法判断其来源。
依据为 [PG18.3 常量折叠](https://github.com/postgres/postgres/blob/REL_18_3/src/backend/optimizer/util/clauses.c#L2486)。
现有 extension 只有 rel/upper path hooks；此处增加窄的前置检查并正确保留 hook 链，不改 core，
不建立通用注册系统。force_custom_plan/force_generic_plan 下同一非法参数位置均为 0A000；
SQL literal、合法 cast 和可折叠的 IMMUTABLE 常量表达式得到相同语义身份。
普通查询参数可以用于 input 或普通谓词；验证时 instruction/options 始终写在准备语句中。

同一 prepared plan 多次执行时复用语义定义，不缓存模型回答；新的执行重新拥有 session/sequence。
copyObject、计划解码、同 OID 函数替换/新 OID 重建均须验证；仅扩展成员 ADD/DROP 的管理要求继续采用
[已完成身份切片](../results/postgresql/function_identity_20260902/README.md)，不扩大自动刷新的承诺。

### 3.3 安装、权限与配置

引入新 SQL 时将 extension default_version 升为 0.2.0：保留 0.1.0 脚本，新增 0.2.0 安装脚本与
0.1.0--0.2.0 升级脚本，由 PostgreSQL 管理扩展成员关系。升级仅新增重载及必要说明，
不删除旧函数、不重建用户表、不改旧函数 OID/属性/授权。验证新装与升级得到等价的新接口。
若研发发现版本已被占用，先修订本稿的版本表，不能使用同一版本表示两套安装结果。

函数必须经现有成员身份检查识别；同名普通函数不能被接管。
新重载缺少 EXECUTE 权限时必须返回 42501，provider open 和模型请求均为零；不能降为已知限制。
PG carrier 保留原函数 OID 的私有绑定/依赖。每个生成型 Map 节点在每次 executor 初始化时，
完成必要的只读计划/函数身份校验后、初始化 ordinary child 或建立 PgSemanticRuntime 之前，
按当前有效用户执行一次 PG 原生 ACL 检查；通过后调用一次原生 function-execute hook。
检查通过才继续 child/runtime/provider 路径；不在逐行循环重复检查或重复调用 hook，结果不跨执行缓存。
函数 OID 不进入语义摘要或中立 port。lowering 移除了函数表达式，不能因此省略原来在
[PG18.3 ExecInitFunc](https://github.com/postgres/postgres/blob/REL_18_3/src/backend/executor/execExpr.c#L2539)
中的检查，也不另建授权系统。
验收直接执行、已执行并缓存计划后 REVOKE 再执行、custom/generic plan、GRANT 后恢复，以及缺权限
时的空输入/NULL-only/LIMIT 0；撤权测试须确实移除 PUBLIC 或继承角色带来的 EXECUTE 权限。
schema/列/表权限和 RLS 继续由 PG 原生机制处理：无列权限零请求，RLS 排除的行不进入 provider。
扩展成员 ADD/DROP 的手动刷新例外不适用于这里；旧路径若暴露同类缺陷，另记最小修复，不能代替新路径验收。

endpoint、认证、超时及 provider 选择继续使用已有配置方式。新真实 Map 即使 socket path 为空，
也不能被 factory 当成 recording；选择语义与执行 profile 后，首个非 NULL task 才验证路径并连接。
plain EXPLAIN、LIMIT 0、空表和 NULL-only 仍零连接。坏 instruction/options 则在规划期报错，
不因上述零任务形状而忽略。

## 4. 请求与输出的精确定义

### 4.1 规范消息

消息数组恰好两项，字段顺序为 role、content：

```json
[{"role":"system","content":"Echo the input."},{"role":"user","content":"hello"}]
```

此例编码为 81 个 UTF-8 字节，无 BOM、尾部换行或额外空格。
system.content=instruction；user.content=input。不另加 Filter 指令、Instruction 前缀、
few-shot、chat template 或字符串插值。input 中的花括号、引号、反斜线和“指令式文本”均按数据编码，
不会被当作 SQL、模板或工具调用；这不构成对模型 prompt injection 的安全保证。

公共编码器负责 JSON 转义：双引号/反斜线转义，退格/换页/换行/回车/tab 使用短转义，
其他 U+0001–U+001F 使用小写四位十六进制转义，斜线不转义，其余有效 Unicode 原样编码。
不做 Unicode normalization，不接受孤立 surrogate 或 U+0000。
规范消息编码的键顺序固定；接收端按此顺序重建字节，而不是依赖 JSON parser 的对象迭代顺序。
HTTP 外壳本身的对象键顺序不是 semantic digest 的输入。

### 4.2 生成参数和服务条件

HTTP 请求使用已选 model、上述 messages 和以下参数，不能继承 Filter 的停止词：

```json
{"temperature":0,"top_p":1,"max_tokens":128,"n":1,"stream":false,"stop":null}
```

这里只展示 max_tokens=128 的例子；它来自 SQL。stop=null 表示无自定义停止序列，
不表示空的停止字符串，也不沿用 Filter 的换行停止词或省略换行后的文本。没有 generation_profile/choice、response_format、tools、
function_call 或供应商自定义 SQL option。模型的自然 EOS 仍有效。

PG 传递已确定的中立值，HTTP 字段映射留在固定模型 Adapter。现有 Filter 的出站字节及错误保持不变。
服务端仍可能有 tokenizer/chat template、generation_config、penalty、EOS 等默认；
首次真实验证须记录模型/revision、tokenizer/chat-template、完整出站参数及这些有效默认。
服务签名改变时重新验证，不能用静态 provider execution ID 冒充完整硬件/权重/默认参数指纹。
PG/gateway 不下载模型或引入第二套 tokenizer；字节检查通过不代表满足模型 token 上下文。
gateway 发送完整未截断请求；服务因上下文超限拒绝时，按既有 MODEL_REQUEST_REJECTED 处理，
不自动裁剪 input、分拆一行、降低 max_tokens、重试或更换模型。真实服务资格须核对上下文容量与
无静默截断配置；不能承诺本地能发现任意外部服务的静默裁剪。

### 4.3 结果、上限与失败

| 条件 | PG 可观察行为 |
|---|---|
| input 为 SQL NULL | 原行保留，结果 SQL NULL；不创建 task，不占 sequence |
| input 为非 NULL 空串 | 正常请求；不能当作 NULL/no-task |
| 完整输出为空串 | 返回长度为 0 的 text，保留该行 |
| 输出为 TRUE/FALSE/UNKNOWN/NULL 或含换行/空白 | 原样文本，不作三值解析、去前后空白、去标签或拆行 |
| 合法 completion 的 finish_reason 不是 stop | 报 22000；不返回部分结果，不自动继续生成 |
| 其余表示/元数据合法的 raw_output 超过 65,536 UTF-8 字节 | 报 54000，不截断输出 |
| model/sequence/digest/evidence 不匹配、缺字段或类型非法 | 报 08P01，不交给文本解释逻辑 |
| 非 NULL input 收到 null completion/raw_output | 报 08P01，不能伪装成本地 NULL 传播 |
| output_tokens 超过计划 max_tokens | 报 08P01，不接受越过生成要求的完成值 |
| finish_reason 缺失、为空、超过 32 字节或类型非法 | 报 08P01；合法非 stop 值才按 22000 处理 |
| 响应不能表示 PG text（无效 UTF-8/U+0000） | 拒绝并清理；wire 解码路径使用现有脱敏 08P01 |
| provider/HTTP/模型失败 | 终止查询并关闭本地资源，不转成 NULL/空串 |

每项 input 上限为 163,840 字节，instruction 上限 4,096 字节；UDS frame body 上限保持 1,048,576 字节。
输出字节上限和 token 上限分别约束缓冲与生成，不能互相换算或只检查其中一个。
这些是首版保守工程值，不表示任意模型可以服务这么长的输入。
PG 在 task 编码/连接前检查 input，完整帧发送前再检查编码后大小；接收缓冲始终有上限。
raw text 必须在复制进结果 context 前检查输出上限；空输出用合法空 text Datum 表示。
新 Map 的有效 input/output 限制只来自 plan，经 AiOpenSpec 传递并进入 semantic digest。
provider 的容量只说明能否承载这些值：能力至少覆盖计划要求，更大容量不改变实际限制；不足则
拒绝 open，不静默降限或协商。新路径 preflight 使用计划值，旧路径的 provider.max_input_bytes 行为不改。

gateway 的 v5 完成校验先检查表示、model、usage 和 finish_reason 的合法形态，再检查解码后
raw_output 的 UTF-8 字节数。超限时发送仅 v5 接受的 OUTPUT_TOO_LARGE；UDS 将它映射为已有中立
AI_PROVIDER_ERROR_MESSAGE_TOO_LARGE，由 PG 返回 54000。该错误的 sequence 必须匹配当前 task。
PG 接收普通 completion 时仍在表示/关联/evidence/model/usage 校验之后防御性检查输出上限，
最后才执行 stop-only 输出 policy；多项违规按这个顺序分类，不让超长文本先被误报为非 stop。
HTTP 整体响应超限或无法解析时尚不能建立合法 completion，仍使用原有 MODEL_RESPONSE_INVALID，
不将其冒充上述文本长度错误。v3/v4 白名单和旧错误表现不变。

响应须保留 response_model_id、finish_reason、prompt_tokens、output_tokens。
response_model_id 与计划 model_id 均为有效 UTF-8、无 U+0000 的 1–128 字节字符串，两者逐字节相同。
finish_reason 为有效 UTF-8、无 U+0000 的 1–32 字节字符串，不 trim 或 normalization；不额外限制
可打印字符。wire 中表示非法时为 08P01；SQL 输入更早触发的 PG 编码错误仍保留原生 SQLSTATE。
usage 是明确提供的非负 uint64 值，零值合法但不等同缺失；不能将缺失值补零或按字符数伪造。
fixture 的合成 usage 保持测试身份，不冒充真实模型测量。
成功请求的 output_tokens 不超过计划 max_tokens，计数累加检查溢出。
非 stop 的合法完成状态属于“不满足本算子输出要求”，不是 malformed JSON；v5 不得先在通用
wire/HTTP 解析器中丢掉该状态，使 PG 无法按上述 22000 分类。旧 v3/v4 的错误分类保持原样。

新路径错误最低对照：不支持形状/常量位置为 0A000；非法参数为 22023；input/output/frame 字节超限为
54000；元数据/证据/响应表示错误为 08P01；有效但不完整输出为 22000。HTTP 超时/不可用/拒绝/内部
错误继续沿用现有中立错误到 SQLSTATE 的映射。HTTP 整体响应超过已有上限仍按现有 Adapter 错误处理。
数据库 cancel、OOM 和其他非白名单 PG 错误不能被重写成模型/协议错误。
这里的参数分类只适用于已经解析到新重载的调用；PG 更早发现的函数解析、类型或权限错误保留原生 SQLSTATE。
消息和详情不得回显指令、输入、输出、endpoint 或凭据；为新错误在首次测试切片登记稳定脱敏文案。

查询失败后，PG 负责数据库写入回滚；已经发往外部的模型请求和费用不能回滚。
SELECT 失败前客户端可能已经收到部分行，不承诺把它们撤回；取消关闭本地连接不保证远端 GPU 立即停算。

## 5. 计划、port 与 wire 的最小增量

### 5.1 身份及所有权

| 内容 | 值或规则 |
|---|---|
| semantic spec | semloom.semantic.sem_map.generate.v1，version=1 |
| prompt program | semloom.sem_map.chat.v1，version=1 |
| result parser/policy | semloom.sem_map.utf8_text.v1，version=1 |
| physical algorithm / role | MODEL_REFERENCE_SYNC_V1 / reference；同类单项模型执行可共享物理身份 |
| plan / wire | schema 4 / protocol 5；只用于本文生成型 Map |
| provider execution ID | semloom.provider.golden.uds.v5 或 semloom.provider.openai-compatible-fixed.uds.v5 |
| generation profile | absent；不是三值 choice，也不传空的 choice 对象 |
| NULL / error / order | PROPAGATE_NULL / FAIL_QUERY / INPUT_ORDER |
| input/output 类型 | text / text |
| input/output 上限 | 163840 / 65536 字节，作为新计划/中立 open spec 的明确值 |

instruction、model、生成值、prompt/parser 身份、上限和语义摘要存入可复制的 PG plan。
input/output 列号、Datum、slot、MemoryContext 与 query cleanup 仍是 PG adapter 对象，不进摘要或 wire。
中立 port 继续 open/drive/close，不新增 submit/poll/cancel/registry。stop 的 presence 与字节内容分开：
新 Map 是 absent（无字节），wire/HTTP 为 null；旧 Filter 是 present 且内容为换行。可用
has_stop + AiByteSlice 这一最小表示，但私有成员布局由研发决定；空的停止字符串不是 absence。
输出上限与上述有效 plan limits 同源，不在不同 Adapter 中再设隐含的语义缺省。
任务借用至 drive 返回；completion 由 session 持有至下一次 drive/close，PG 及时复制到 per-tuple context。

semantic digest 绑定语义，physical digest 绑定算法/role，provider digest 绑定协议、Adapter 身份和 model。
相同物理算法摘要不意味着 Map 可使用 Filter 校准。payload digest 不包含 sequence；
相同输入仍是不同任务，用连接/session 内 sequence 区分。completion evidence 必须包含 sequence 与全部摘要。
这些摘要用于一致性与关联检查，不是身份认证、模型质量证明或完整服务签名。

### 5.2 严格 v5 消息

复用已有四字节网络序长度 framing、JSON 校验、FD/latch/超时与 session loop。
下面列出 v5 字段集合；未知/重复/缺失字段拒绝，字段类型与 uint64 范围严格检查。
sequence、prompt_tokens、output_tokens 沿用现有 wire 的 JSON 十进制字符串表示：匹配
0 或 [1-9][0-9]*，最多 20 个 ASCII 字符，数值不超过 18446744073709551615；
拒绝 JSON 数字、boolean、空串、符号、小数/指数、前导零及溢出。open error 的 sequence 才为 JSON null。
中立 port 与 §10 摘要计算仍使用 uint64 数值；模型 HTTP usage 的数值由 Adapter 严格校验后再转成
wire 字符串。protocol_version、spec version、容量等字段继续使用各自规定的 JSON 数值类型。
此约定对照了现有 [session writer](../../code/src/execution_provider/adapters/semantic_session.py)
与 [C uint64 reader](../../code/postgres/semloom_pg/src/wire_semantic.c)；旧版本行为不变。

| 帧 | 必须字段 |
|---|---|
| open | type、protocol_version、semantic_spec_digest、physical_algorithm_digest、provider_execution_digest、provider_execution_id、operator_kind、semantic_spec_id、semantic_spec_version、physical_algorithm、physical_role、prompt_program_digest、result_parser_digest、model_id、generation_constraints、null_policy、error_policy、order_policy、input_type、raw_output_type、max_input_bytes、max_output_bytes |
| opened | type、protocol_version、semantic_spec_digest、physical_algorithm_digest、provider_execution_digest、max_inflight_tasks、max_frame_bytes、max_input_bytes、max_output_bytes |
| task | type、protocol_version、sequence、semantic_spec_digest、physical_algorithm_digest、provider_execution_digest、semantic_payload_digest、canonical_messages |
| completion | type、protocol_version、sequence、semantic_spec_digest、physical_algorithm_digest、provider_execution_digest、semantic_payload_digest、raw_output、response_model_id、prompt_tokens、output_tokens、finish_reason、completion_evidence_digest |
| error | type、protocol_version、sequence、code；旧白名单加仅 v5 的 OUTPUT_TOO_LARGE |

open 的 operator_kind=SEM_MAP、input_type=raw_output_type=text、protocol_version=5；
generation_constraints 恰为 §4.2 六字段。opened 的 max_input_bytes/max_output_bytes 是本 session
确认的有效计划限制，必须与 open 一致；max_frame_bytes=1048576、max_inflight_tasks=1。
provider 底层能力可以更大，但不能在 opened 中扩大实际限制；无法承载该合同则按 INVALID_OPEN 拒绝。
open 不传 SQL/Plan 或额外 instruction 副本。opened 确认固定字段、配置/容量、摘要形态及可独立
重算的物理/provider 身份；instruction 尚未到达，不能声称已独立验证完整 semantic digest。
gateway 在首项 task 到达后，从两条消息取得 instruction/input，按 §10 重算 S/PD 并与 open/task
声明核对，通过后才绑定该 session 的 instruction 并调用 completion Adapter。后续每项仍须核对
S/PD，instruction 字节与 semantic identity 都保持不变；不匹配即 INVALID_TASK、session 终止，
不产生该项模型请求。只有格式合法但内容错误的 S 时，允许 open 后在首项 task 拒绝。
无 task 不做语义执行，也不为验证 instruction 额外发送内容；PG 正常零任务路径仍不打开 session。
完成帧先验证表示、关联和 evidence，再做 plan/model/usage 与 Map 输出规则检查。
v5 的 finish_reason 为非空、最多 32 字节的字符串，合法非 stop 值交给 Map policy 拒绝。
旧端不支持 v5 时明确失败，绝不回落到 v3/v4、recording 或不受限请求。

### 5.3 代码落点：改动沿变化原因集中

| 现有 Module / 真实缺口 | 本工作包要求 |
|---|---|
| extension.c、SQL、sem_path.c 只识别 recording Map | 复用成员检查与现有 Map placement，增加三参 marker/spec 和 §3.2 窄前置检查；PG carrier 按 §3.3 保留执行权限检查；保留多 marker 与混合算子拒绝，不复制完整 planner |
| sem_plan_spec.c / .h 的真实语义只支持 Filter | 增加 schema 4 严格保存/复制/解码；先建立不接线也明确拒绝执行的测试阶段，不能把新 spec 伪装成 schema 2/3 |
| sem_operator_machine.c 的公共 task builder 使用 Filter 指令 | 各算子拥有 system 内容与结果策略，公共 writer 仅编码消息；通过 machine 的公开 task_size/write_task 路径验证，不给测试暴露内部 buffer |
| sem_map_machine.c、PgSemanticCompletion / SemloomMachineCompletion 目前只见 output bytes | 为真实 Map 最小传递必要的纯值完成元数据，或经等价的中立语义 validator 检查；不可把 PG 类型或网络细节带入 machine |
| pg_semantic_runtime.c / wire_semantic.c 提前固定 stop；payload 域按 choice 有无二选一 | v5 允许合法非 stop 元数据到达 Map policy；显式覆盖新任务摘要与上限，保留旧错误/计数语义，不能仅放宽 Filter 校验 |
| provider.c 用“不是 exact Filter”选择 recording | 改为正向识别已支持的 recording/真实 Filter/生成 Map，未知 spec 拒绝；真实 Map 缺 socket 不得变 recording |
| UDS 和 Python codec/session 对 v3/v4 二选一 | 对 v2/v3/v4/v5 明确分流，字段/摘要/结果规则按合同；共享 socket/帧/会话流程，不复制第二套 gateway 或按版本减一推断任意语义 |
| OpenAICompatibleFixedAdapter / GoldenCompletionAdapter | 复用完整请求发送和原始完成值 Interface；通用解析不作 Filter 真值判断。按支持的协议显式查询 execution ID，不继续追加 map_execution_id 特例；保留旧 public import/属性/ID、错误和 fixture 格式 |

这些落点是源码核对清单，不要求逐项新建类或文件。最小共同 Interface 可只表达两个消息内容和一份
长度受限的完成值；Map/Filter 两个真实消费者足以支撑此次整理，不预建任意 DAG/算子/供应商框架。
新 Module 名称、私有 helper 和 C 布局由研发负责；可观察语义、数据所有权和兼容要求按本文验证。
thin scan、child pump、query callback、幂等 close 不重写；必要的参数/值转发变化须最小化。

## 6. 可观察行为与成本

plain EXPLAIN 能看到生成型 Map、schema/spec、algorithm/role、prompt/parser ID、model、max_tokens、
输出上限及安全 provider 名称；不显示 instruction、输入、输出、socket 或认证。
prepared plan 固定语义；provider 仍按既有执行期规则在 query begin 固定，不新增隐含模型选择。

EXPLAIN ANALYZE 复用已有 Model Calls、Prompt Tokens、Output Tokens、Accepted Rows、Emitted Rows。
保留既有计数含义：Accepted/Emitted 是 provider 参与的成功处理行数，不包括本地 NULL 传播；
标准 plan 的 Actual Rows 才包含输出 NULL 的行。成功 Map 查询中，非 NULL 处理数、成功 completion
数和 Accepted/Emitted 一致。golden 的 usage 是测试值，必须标明；失败查询不伪造成功 EXPLAIN 统计。
gateway 的实际尝试、取消/失败与费用另记，不能从 PG 成功计数推断所有尝试。

reference 行数来自普通 child，Map 不再施加 Filter 选择率。保留现有未校准的简单 path cost 作为
单路径占位，不复制 Filter calibration loader 或伪造 token/延迟估计。四 D 不要求在无数据时拟合成本，
也不能用此代价比较第二路径；后续 Map 性能实验另定自身质量、成本与服务条件。

## 7. 工程参照：采用什么，保留什么

继续借鉴公司 demo 的算子工程经验，重点是专用算子的目的、共享请求构造与输入/输出绑定；
自有语义与实现仍由本文和公开可核对的依据确定。此次新增的私有文件/符号/版本及行为观察已保留在
本地不跟踪的来源对照记录，不随本稿提交；需要精确源码对照时，在获授权环境重查受影响部分。
公开构建与验收不依赖私有记录，不复制公司代码、prompt 原文、测试数据或日志；
移植范围与既有对照入口仍看[主计划 §8.7](postgresql_ai_semantic_operator_architecture_20260827.md#company-engineering-reference)。

| 参考定位 | 已核对行为 | 本稿决定与验证对应 |
|---|---|---|
| 自有 sem_path.c / sem_map_machine.c / sem_pump.c | recording Map 已有按需 child、输出列绑定与本地 NULL 传播 | 延用关系载体，新增生成语义；验收行值、LIMIT、NULL、错误回滚与内存，不增加数组 collect |
| pgml-extension/src/api.rs 的 transform_json / transform_string，固定 caf2b6ccdf0d6efc2c1910cbc06725a34320181a | 两个注册入口整理 task/args/inputs 后调用同一 transformers binding | 借鉴共享模型调用能力；自有模型仍在 PG 外。验收 Map/Filter 共用 completion Adapter，旧请求行为不变 |
| PG REL_18_3 clauses.c:eval_const_expressions_mutator / ece_function_is_safe | custom 参数可变为 Const；普通折叠与估算求值的允许范围不同 | 在参数替换前检查来源，之后要求普通折叠结果；验收 custom/generic 一致拒绝及合法常量身份 |
| PG REL_18_3 execExpr.c:ExecInitFunc | 表达式初始化调用原生函数 ACL 与执行 hook | lowering 后在 carrier 保留等价检查；验收撤权后 42501、零连接与零模型请求 |
| PG CustomScan callbacks | PG 提供执行状态与 Begin/Exec/End 等回调；扩展负责新增私有数据的正确使用 | 保留现有 scan/pump/runtime，使用 PG 内存与查询清理，不另建事务/锁/WAL |

公开源码：[pgml api.rs](https://github.com/postgresml/postgresml/blob/caf2b6ccdf0d6efc2c1910cbc06725a34320181a/pgml-extension/src/api.rs#L675-L708)；
生命周期依据：[PG Executing Custom Scans](https://www.postgresql.org/docs/18/custom-scan-execution.html)。
未安装或运行 pgml/公司实现，不能据此宣称其 PG18.3 兼容性或性能。
Sema/Cortex 的数据库语义所有权和 LOTUS 算法定位沿用[研究依据](../../research/sema_native_semantic_operator_architecture_reference_20260827.md)；
本工作包只建 reference，不以已有工程模式声称新增研究贡献。

未来向公司移植时，优先复用纯值 prompt/输出 policy、任务身份与执行核心；
SQL 签名/参数顺序、NULL/失败策略和 PG 生命周期由公司侧适配并重新验收。
provider 接通不等于算子策略已移植，本合同不要求现在创建跨数据库 SDK 或修改公司工作副本。

## 8. 切片顺序与验收

### 8.0 本次源码复核与首个子切片（2026-09-03）

复核基线 `63d86c0e`；未发现需要改写本合同语义或 §10 向量的源码反例。按现有接口登记如下，
未接线项不算已实现：

| 复核问题 | 源码事实与采用方式 | 验证所在切片 |
|---|---|---|
| 新旧 marker | `extension.c` 已有成员校验，`sem_path.c` 只绑定一个一参 Map；安装仍为 0.1.0 | PG 切片增加三参 OID 和 0.2.0 安装/升级，不修改旧 OID/属性；保留混用拒绝 |
| 常量与 ACL | 当前仅 rel/upper hooks，pump 初始化没有原 marker 的执行权限检查 | PG 切片在已有 planner 前加窄来源检查；保存私有函数 OID，每次初始化先做原生 ACL/hook，再 child/runtime；按 §3 反例验证 |
| stop、limits、metadata | `AiOpenSpec` 的 stop 为 slice，`PgSemanticCompletion`/machine completion 仅见 bytes/null | 接线切片显式增加 absence、计划 limits 和必要完成值；借用/复制生命周期不变，不在当前消息编译步骤提前宣称完成 |
| v5 与 Adapter | factory 仍用非 exact Filter 判 recording；codec/session 仅区分 v3/v4 | 接线时正向识别已支持语义，显式版本/执行 ID；复用 frame/session/HTTP，仅 v5 接受新错误与非 stop 元数据 |
| 公共消息编译 | `sem_operator_machine.c` 的 JSON writer 与 Filter system 内容混在一起；Python Filter 也在 codec 内组装消息 | 首个子切片只抽公共两消息编码；Filter 保留原 directive/分隔符，Map 提供原样 instruction/input。不得复制另一套 escaping |
| 不变行为 | recording 无消息体；exact/choice 共享原 Filter message bytes | 先通过 machine 公开 size/write 与 Python canonical_messages 固定旧字节，再加入 Map 的公开消息编译函数及独立 §10 消息向量 |

本次只实现规范消息编译，不接新 SQL/schema 4/port/v5，也不实现完成元数据、输出 policy 或全部摘要。
C 的 Map 消息函数属于现有 machine Module 的公开 task-size/write Interface，独立接受借用的 instruction/input；
内部 writer 不作为测试入口。等新语义 machine 完整接线时复用这些函数，不先让 recording machine 冒充生成算子。
Python 同样只增加纯消息函数；旧 public import/异常、Filter bytes/digest/SQLSTATE 的行为保留。
测试采用合同已有 machine/codec 表面及固定消息向量，增加 Unicode/NUL/边界/缓冲失败检查；不使用私有状态。
工程来源与取舍仍由 §7 和主计划 §8.7/8.8 记录，本子切片只移动自有 writer，不读取、复制或上传公司源码。
本地 C11/Python 先验；PG18.3 构建/TAP 未运行时必须标 pending，不能复用旧 1022 项当本次证明。
用户随后授权本子切片到服务器验证：只用独立 PG18.3 prefix/测试目录和合成 fixture，先做只读环境检查，
再运行 `-Werror`、旧路径完整 regression/TAP 与同一批本地合同；任一步失败保留产物并停止验收。
没有真实模型请求、资源规模测试或校准数据采集授权；本轮 PG 通过也只证明旧路径兼容，不证明新 Map SQL 可执行。

后续纯值步骤继续使用 `f7d579af` 的已验证消息作为输入，不改变已有消息字节。源码复核发现
Python 的原始 `Completion` 五字段已足够表达本合同，但定义在含 socket/session loop 的 module；
将它移到不含 I/O 的公共完成值 module，并保留原 import 别名，不为 Map 重造一种 HTTP 完成值。
新增最小 `SemanticMapPlan` 只保存 instruction/model/max_tokens，固定程序和有效 limits 由本合同确定；
按 §10 先测独立摘要向量及篡改，再编码。C 使用同一纯值语义，SHA-256 仍由调用方负责，编码器不另造密码库。
Map 完成检查与证据编码分开：表示/model/usage 先验证，合法超长输出再报长度，最后判断非 stop，
不 trim、截断、返回 NULL 或改用 Filter parser。先验证纯值、旧 public import/消息/结果，再进入 wire v5；
这些函数存在不能代表 PG 已保存新 plan 或完成元数据已跨 port 传递。

Python v5 子切片复用现有 `semantic_session` 的同步循环和 fixed HTTP Adapter，不新建 gateway。
将旧 v3/v4 的完成帧构造机械移入原 Filter codec；v5 单独拥有字段/身份/输出上限校验。
Adapter 用显式 `execution_id_for(version)` 返回已支持身份，旧属性/import 保留，旧自定义 Adapter
仅按已有 v3/v4 属性兼容，不能隐式支持 v5。新 Map golden 值必须给出 raw output/model/两项 usage/
finish reason；旧 Filter 的字符串 fixture 不变，也不能拿它给新 Map 补造完成元数据。
验证表面是 C/Python 纯值、公开 codec、socketpair 会话、独立 CLI 和 localhost HTTP fixture；
这些验证尚不是 C→Python 的 wire v5 互通或 PG Map 执行证据，后者须待计划与 C client 接线后单独验收。
独立复核补出既有 JSON 数字转换上限被 v5 继承的问题：新会话只在读帧时将该输入异常归为
INVALID_OPEN/INVALID_TASK，Adapter 自身 ValueError 仍为 GATEWAY_INTERNAL；旧 v3/v4 的会话错误不改。
CLI 尚未识别版本的首帧不可解析时只关闭该连接，后续合法会话仍可执行，不再让该输入异常结束 gateway。
首帧尚无可验证版本时不构造猜测版本的错误帧；该项是独立错误恢复加固，不改变正常 wire 字段或语义。
后续深层 JSON 反例指出上述处理尚缺 `RecursionError`。源码复核定位到同一首帧读取与 v5 读帧包装；
本次只在这两处扩展输入异常分类，不重写 framing/session，不读取或复用公司实现。
验证表面保持公开 CLI 与 session：约 20 KB、低于帧上限的深层 JSON 在首帧/已握手 task 均终止
坏连接，下一合法连接仍成功；Adapter 自身同名异常继续归为脱敏内部错误，旧 v3/v4 会话分类保留。
这是对既有输入隔离的定点修复，不宣称支持任意 JSON 深度，也不增加 PG 接线或模型运行范围。

PG 计划子切片从已集成 `340356e8` 独立推进。复核现有 `extension.c` 成员查找、`sem_path.c`
单 Map placement、`sem_plan_spec.c` 命名节点与 `sem_pump_begin` 后，继续使用这些实际调用点：
增加三参 OID/0.2.0 安装升级，在 PG 常量替换前只检查新 Map 的 instruction/options；普通 input 和
谓词参数不动。schema 4 保存完整最小语义，原函数 OID 只作 PG 私有绑定，并登记原生计划依赖。
每次节点初始化先执行原生 ACL/function-execute hook；未接 C v5 时，合法新计划仍明确 0A000，
不进入 child/runtime/provider。plain EXPLAIN 只展示已验证的计划值，不创建 provider 或算子执行状态。
这是 §3、§7 和主计划工程对照中“保留自有载体、共享接口、原生权限”的具体落点，不读取或移植
公司实现；PG REL_18_3 的 ExecInitFunc、常量折叠与 plan dependency 是实现依据。
预先确认的测试表面是 SQL 新装/升级、EXPLAIN、prepared/custom/generic、执行权限与既有测试专用
PG plan codec caller；使用公开合成输入与 §10 独立向量，不增加生产测试 GUC 或私有状态断言。
按 SQL 注册 → plan/值/复制 → 前置来源 → ACL/依赖逐项 red/green；每次保留失败与实际源码身份。
本阶段只允许独立 PG18.3 fixture 验证，不使用真实模型预算，不修改 runtime/provider/wire 或旧输出语义。

PG plan 后续复核增加整个 Map 被 SQL 函数包装的来源反例：以 `b58479f7` 为基线，核对
REL_18_3 `clauses.c:inline_function / eval_const_expressions_mutator`；前置 Query 不一定已经包含
随后内联暴露的 Map。先在同一 SQL/EXPLAIN 表面验证 instruction 参数的 custom/generic 模式与零连接，
若风险成立，只修 planner 来源识别；不全局关闭普通函数内联，不移动权限检查或改变 wire/摘要。
本补充不新增公司参考/复用或模型运行；测试先行事实与修复结果追加到原 PG plan 结果记录。
反例现已确认：custom 模式在 SQL wrapper 内联后接受参数生成的指令，generic 模式拒绝。
本版不新增 wrapper Map 入口：只允许原始 SELECT 顶层或直接 INSERT 来源中、已经检查固定参数的
显式 Map 被 lowering；规划中才由内联暴露的 Map 返回 0A000，包括常量调用 wrapper。
普通 SQL 函数的内联、直接 Map 的普通 input 表达式、旧 Map/Filter 行为保持；规划嵌套和 ERROR
必须恢复调用方的临时检查状态，不保存跨查询 registry，不依赖 SQL 文本位置推断来源。

### 8.1 开工前的研发复核

研发 agent 阅读本稿和上述实际源码后，登记“可直接实现 / 需修订及反例”，至少回答：
新旧 marker 如何区分；前置参数检查与执行期 ACL 如何接入；stop presence、有效 plan limits 与完成
元数据如何跨现有 Interface；v5 错误/ID 分流如何保持旧行为；新装/升级如何验证；旧 Filter 如何证明未变。
这一步不要求再次向用户逐项询问内部类型与函数名，也不以“文档有了”跳过源码核对。

没有反例支持的行为变更不直接进入代码。必要修订先改本文，写明原因/影响/测试，保持唯一合同。
首个代码切片开始前，SQL/输出策略/版本/§10 vectors 应是确定值，而非研发自行选择另一组默认值。

### 8.2 小步交付

| 切片 | 交付与通过条件 |
|---|---|
| 常量与纯值合同 | 先用 §10 及参数/输出边界写失败测试，再实现 Map 定义和必要公共编码；旧 Filter bytes/digest/错误不变，纯值 C11/Python 可测 |
| PG 计划接入 | 新重载/安装升级、schema 4、严格解码、EXPLAIN、copy/prepared/身份与权限；未接线时新执行明确 0A000，旧路径继续运行 |
| golden 完整执行 | 中立 open spec → v5 → gateway → PG 输出，SELECT/INSERT 真正执行；临时拒绝分支移除，NULL/空串/错误/取消和资源规则通过 |
| 固定模型与资源 | 同一个 completion Adapter 驱动真实 Map，实际出站参数与响应证据一致；真实运行与受控资源分别归档 |
| 收尾交接 | 更新实现状态与证据、保留失败，独立审查；标明尚未实现组合/增量桥接/优化路径，不借用旧 Filter 的质量或资源资格 |

新功能和行为不变的公共重构分开审查。测试以 SQL/plan、machine、port/codec 和 Adapter 的可观察
Interface 为表面，不依靠模块私有状态。旧行为测试只有在等价覆盖已迁移后才删除，失败证据不删除。

### 8.3 必测矩阵

| 组别 | 最少覆盖 |
|---|---|
| SQL/plan | literal/cast/IMMUTABLE 折叠，前置拒绝 Param/Var/SubLink/STABLE/VOLATILE，三字段集合、数值规范、非法常量、schema 篡改、copyObject、custom/generic、多次执行 |
| 关系语义 | 单行、多行、重复 input、普通列/别名/表达式；与普通 child 对照 VOLATILE 求值次数；NULL 与空串、LIMIT 0/早停/OFFSET、表达式报错、SELECT 与 INSERT 行值/回滚 |
| 零外部工作 | plain EXPLAIN、空表、NULL-only、LIMIT 0 配坏 socket/未启动 gateway 仍零连接；非法 instruction/options 仍报错 |
| 输出 | 空串、首尾空白、多行、Unicode/组合字符、TRUE/UNKNOWN 文本、65536 与 65537 字节、长度完成、缺/错 usage/model、超 max_tokens、无效 UTF-8/NUL |
| 协议 | 摘要 mutation、重复/未知/缺字段、sequence 关联/溢出/重复；v5 超限错误及跨版本拒绝；底层容量大于/小于计划、opened 限制不符；旧端拒绝、不回落、不重试 |
| PG 所有权 | §3.3 的直接/缓存/custom/generic/空输入 EXECUTE 撤权零请求及 GRANT 恢复；每次节点初始化只检查一次且不在逐行循环调用 hook；列权限/RLS、snapshot、事务/savepoint、取消与恢复 |
| 资源 | toasted input 的 detoast 与编码/复制只活到本 tuple；正常/错误/取消/早停/重复 close 后 FD 与 context 可回收 |
| 兼容 | recording Map/Filter、exact v3、choice v4 的现有 regression/TAP、协议 golden、错误/脱敏/EXPLAIN 与 public imports |
| 未支持形状 | 多个新旧 Map、Map+Filter、嵌套/WHERE/CASE、Join/CTE/aggregate/window、排序、rescan/EPQ、并行与不支持 INSERT 修饰 |

每个新错误用可识别的 payload 哨兵检查日志/异常不泄漏。SQLSTATE 和稳定脱敏文案一起断言，
不要只用“抛出了某个异常”代表通过。测试数量由真实用例决定，不预先宣称某个 TAP 总数。

### 8.4 PG18.3、资源与真实服务

使用明确的 REL_18_3 工具链做 -O2 -Werror 构建、PGXS regression、完整受影响 TAP、本地/服务器
Python 和 neutral/machine C11。源码、二进制、原始/公开日志哈希分别记录；公开副本经过处理时不能
直接冒充原始字节文件。历史 1022 项身份验收不等于新 Map 通过。

默认资源验收配置：同一已预热 backend 连续三轮纯 SELECT，各 2,000 个非 NULL 任务，
每项 input=100,000 字节、fixture raw output=65,536 字节；输出排空而不混入 INSERT 写回。
资源验收使用 [libpq single-row mode](https://www.postgresql.org/docs/18/libpq-single-row-mode.html)，
或已证明逐行读取并释放结果、保持同一条 SQL/计划/事务语义的等价驱动。每轮在发送查询后立即启用
single-row mode 并检查成功；逐项处理后 PQclear，不保留完整结果集，读到最终成功状态并排空结果后
才记该轮成功。收到若干行后仍可能报错，不能用已收行数替代最终成功状态。
默认 psql 重定向到 /dev/null 只控制输出去向，不证明 libpq 不缓存全量结果，不能单独作为本轮
流式资源验收依据。其他 psql 获取模式须另证有界读取及 SQL/计划/事务未变，不在本轮默认引入 cursor。
客户端 RSS 与读取模式也要记录，避免观察器缓存干扰；此前 psql smoke 保留其原始观察身份，不据此删除历史证据。
fixture 复用一个或很小一组 payload/output，起点前准备好；每项仍由连续 sequence 独立关联，
任务数另由输入数、gateway 记录及成功退出核对，不把观察器/fixture 存储归因于 provider。
每轮及跨三轮的 PG RSS 相对预热起点：峰值增量不超过 16 MiB、结束增量不超过 8 MiB；
UDS 所有的 FD 峰值增量不超过 2、结束增量为 0。同时记录总 FD 并区分 relation/TOAST VFD。
任何无效样本、退出竞态、失败或超限均保留；不得运行后放宽这些阈值或用斜率拟合掩盖超限。
gateway 另有以下可判定结束条件，均在运行前选定的清理时限内检查：

- 本轮存活 session、活跃请求和未交付的 completion 为零；accepted client FD 回到预热基线。
- 本轮请求的 deadline/timer（若实现存在）全部结束；长期 listener/固定缓存不要求清零，分别列出基线。
- golden 路径 HTTP 请求为零；fixture 和采样器自身存储单独记录，不能为观测而新增通用 session/timer registry。
- gateway RSS 的峰值/结束增量阈值及清理时限，依据授权的预热/小规模 fixture 检查在三轮运行前
  写入切片记录；缺少这些数值不开始资源验收，运行后不能放宽阈值。

这些检查针对本地资源，不声称断开 UDS 会立即停止远端模型计算。
本轮阈值已由用户确认并在 §8.4.1 固定，不再作为暂定值，也不保留运行后修订的例外；
不同配置或新实验须另行说明和确认，不能修改本轮阈值后继续验收。这不是既有通过证据。

真实模型选择、样例、预算、上下文容量、实际默认参数与停止条件须在该切片运行计划中确定。
先用 fake HTTP 验证全部错误，再做受限真实服务检查；本合同不继承四 C 的 100 次预算，也不授权
使用其剩余额度。每次失败尝试计入新预算，不为“得到一次成功”自动重试或更换配置。
本工作包只证明请求/文本/usage/生命周期可运行；任务质量及后续优化比较另有独立样例与预期，
不能把原模型输出回填成标签后声称正确。

### 8.4.1 本轮有限服务/资源配置（2026-09-03，固定验收值，尚未运行）

用户已确认以下固定验收值；本轮固定单 GPU。先前允许双 GPU 不改变本次运行配置或请求预算。
只读检查已发现缓存模型和所列软件版本，未下载、改环境或启动模型。执行前再检查资源归属、
可用显存、BF16 支持、实际完整模型名称/revision、模型/tokenizer/chat-template 文件哈希及有效配置。
完整 Map PG＋golden、模拟 HTTP 和 PG18.3 前置验证未通过时不执行本节；旧 SQL 路径 TAP 不能代替它们。

| 项目 | 本轮固定选择与停止要求 |
|---|---|
| 模型与软件 | 已发现的缓存名称为 Qwen2.5-7B-Instruct；运行前据实登记完整模型标识与精确 revision、权重/tokenizer/template 哈希，不能把目录名当 revision。固定 vLLM 0.25.1、torch 2.11.0、单 GPU、BF16；不下载、不自动换模型、降精度或改双 GPU；不用旧 Filter 资格 |
| 服务 | 一张 RTX 4090、BF16、max_model_len=4096、max_num_seqs=4、max_num_batched_tokens=4096、GPU memory utilization=0.8、eager；generation_config 采用 vLLM 默认而非继承模型文件，完整有效值/EOS/tokenizer/template 另记哈希 |
| 隔离 | 仅 localhost、全新 gateway/socket/服务日志与 PID；显式 PG18.3 prefix 与测试库；不覆盖任何已有服务或环境 |
| 真实尝试预算 | 本轮逻辑推理请求全程最多 32 次，每次派发前记入同一持久 ledger；主动预热、直接模型探测、失败、超时、取消均计入，结果不明不退还。重启进程/机器、换脚本或源码重跑不重置，也不借新 ledger 绕过累计；不复用四 C 余额，不自动重试或扩大预算，不必用满 |
| 样例 | 预先固定 ASCII、多行、引号/反斜线、Unicode、字面 TRUE/FALSE/UNKNOWN/NULL、花括号、非 NULL 空串、SQL NULL；每类核对 PG 值与实际 raw completion、顺序/行数/usage。NULL 必须零调用；输出质量另记观察，不回填固定标签 |
| 生成与上下文 | 原样两消息、temperature=0/top_p=1/n=1/stream=false/stop=null；常规 max_tokens=128。4096 是 prompt 与生成输出合计的上下文容量；逐例核对应用实际 chat template 后的 prompt tokens + max_tokens≤4096，给输出留出空间，不把输出额度设为 4096。不裁剪或降低参数换取通过；完整失败矩阵先由 fake HTTP 执行 |
| 运行前登记 | 在首个真实推理（含探测/预热）之前，保存完整模型/revision、实际服务配置、具体样例和顺序、token 余量、精确请求 timeout_ms 与计时范围、ledger 身份、已通过的前置源码/证据。当前 revision/具体样例/timeout_ms 仍待现场核对与登记，不能视为已填写 |
| 停止 | 非预期合同、身份/字节/计数错误、清理失败、任一资源超限或预算耗尽即停止；模型身份/revision、显存/BF16、上下文、有效默认或必填配置无法核对时不发送真实请求。保留失败及账本，不放宽阈值继续，也不用剩余预算搜索参数 |
| fixture 资源 | 按 §8.4 三轮 × 2000 任务，明确使用 fixture，禁止暗中调用模型。以同一已预热且仍存活的 gateway 进程为基线（记录 PID/启动身份），峰值 RSS 增量≤32 MiB；查询/provider session 关闭后 60 秒内结束增量≤16 MiB，同时满足会话/请求/相关 FD 等 §8.4 清理条件。不重置跨轮基线、不重启或杀掉 gateway 证明恢复；异常退出不算通过。PG 原有 RSS/FD 阈值不变 |

上下文定义已对照 [vLLM 0.25.1 的 max-model-len 说明](https://docs.vllm.ai/en/v0.25.1/cli/run-batch/#max-model-len)；
这是参数口径核对，不是本轮模型、上下文余量或 BF16 已通过预检的证据。

真实阶段仍只验证执行工程，不声称生成质量、成本校准或优化效果。服务观察、预算预留、取消后的实际
模型尝试和本地关闭分别记录；不能把已向客户端返回若干行当作 SQL 最终成功，也不能把关闭 UDS
当作远端 GPU 已停止。若单 GPU 不满足上述配置，先保留诊断并提交新配置，不自动改为双卡继续。

## 9. 完成与未完成如何表达

合同定稿、纯值实现、PG plan 接入、golden 执行、真实模型、资源验收分别标状态。
只有 §8 对应实现与验证完成，才能写“四 D 工程完成”；本稿存在只表示设计可供研发复核。
下一步的 planner 组合与 gateway 有界多会话、SemLoom 增量接入，仍按主计划单独实现和验收。

## 10. 规范编码与独立 golden vectors

本节是首个纯值切片的预期，不是从未来 C 实现反推的输出。文档编写时已用两个独立的
Node.js/Python 临时计算逐项核对；未实现 C codec、未执行 PG 或模型测试。
以后修改定义时，必须先说明语义变化，再更新版本/向量；不能只改 expected 让测试变绿。

### 10.1 基本表示和摘要公式

UTF8(s) 不含 NUL terminator；U32/U64 为无符号大端整数；B(false)=单字节 00，B(true)=01。
T(s)=U32(UTF-8 字节数)+UTF8(s)；D(s)=UTF8(s)+00；A(x) 为 64 个小写 SHA-256 hex ASCII 字节，
不把 hex 解码为 32 个二进制字节。H(x)=SHA256(x) 的小写 hex。加号表示原始字节连接。
所有输入均先通过范围/UTF-8 检查，编码器不得在生成摘要时截断、trim 或改变数字。

```text
PP = H(D("semloom-prompt-program-v1")
     + T("semloom.sem_map.chat.v1") + U32(1)
     + T("system") + T("content") + T("instruction-verbatim")
     + T("user") + T("content") + T("input-verbatim"))

RP = H(D("semloom-result-parser-v1")
     + T("semloom.sem_map.utf8_text.v1") + U32(1)
     + T("utf8-no-nul-no-trim") + T("stop-only") + U32(65536))

S = H(D("semloom-semantic-spec-v4") + U32(4)
    + T("semloom.semantic.sem_map.generate.v1") + U32(1)
    + T("SEM_MAP") + T("text") + T("text") + T(instruction)
    + T("semloom.sem_map.chat.v1") + U32(1) + T(PP)
    + T("semloom.sem_map.utf8_text.v1") + U32(1) + T(RP)
    + T("PROPAGATE_NULL") + T("FAIL_QUERY") + T("INPUT_ORDER")
    + T(model_id) + U32(0) + U32(1) + U32(max_tokens) + U32(1)
    + B(false) + B(false) + U32(163840) + U32(65536))

PH = H(D("semloom-physical-algorithm-v2")
     + T("MODEL_REFERENCE_SYNC_V1") + T("reference"))

E = H(D("semloom-provider-execution-v5") + U32(5)
    + T(provider_execution_id) + T(model_id))

PD = H(D("semloom-payload-v5") + A(S) + 00
     + U64(length(I)) + I + U64(length(M)) + M)

CE = H(D("semloom-completion-v5")
     + A(S) + A(PH) + A(E) + A(PD) + U64(sequence)
     + T(raw_output) + T(finish_reason) + T(response_model_id)
     + U64(prompt_tokens) + U64(output_tokens))
```

I 是原 input UTF-8 字节，M 是 §4.1 的规范两消息 JSON 字节。S 中两个 B(false) 分别为 stream、
has_stop；PD 中 00 是一个零字节分隔符，不是 NULL task 标志。
真正 NULL 输入不构造 I/M/PD，不占用 sequence。PH 沿用现有同步 reference 的物理身份，
S/E/PD/CE 使用明确的新域，不能用“版本号大概加一”的循环接受未知协议。

### 10.2 正例

以下均使用 model_id=response_model_id=golden-map-v1、max_tokens=128、finish_reason=stop、
provider_execution_id=semloom.provider.golden.uds.v5；其余生成值见 §4.2。
usage 数字均为独立合成测试值，不是模型 tokenizer 测量。

```text
PP = 72bbbd2abec0c7167158200281b7a88c44b94cd949f8b63f398a9101f8826afb
RP = 540ea50c27d6f2d6800146b3b26404b4a5a64c6debef02e5501e67a829caec07
PH = 558e50ae5e2716d2e699e09ddb8ffb953f772ba9a1be9dbb15379d9bfcf08d66
E  = 8c104ecf5cbf44ca11e13f71d4ef8723a362df26a4743939a5357d788b564dd2
```

仅将 ascii 用例的 provider_execution_id 改为 semloom.provider.openai-compatible-fixed.uds.v5，
其余合成参数保持不变时，S/PH/PD 不变，得到以下补充向量；它不代表调用过固定模型：

```text
E_fixed        = 6782bdee3092bc43c885e0c90e57c602bd08364f20946bacc4dc9d8283feae24
CE_ascii_fixed = 1d6007a0388e09464262e7cceabd102bbd441ffdba9bf377dc0a2c944a8b0cbb
```

下列 JSON 是计算输入向量，不是完整 wire 帧；sequence/usage 在此以数值列出，实际 wire 按 §5.2
编码为十进制字符串，摘要仍按 U64 编码。转义先解码得到 instruction/input/raw_output；再按 §4.1 编码 M。
换行和 tab 不是两个普通字符，ASCII/Unicode 也不能先做 normalization。

```json
[
  {
    "name": "ascii",
    "instruction": "Echo the input.",
    "input": "hello",
    "raw_output": "hello",
    "sequence": 0,
    "prompt_tokens": 17,
    "output_tokens": 1,
    "canonical_message_bytes": 81,
    "semantic_spec_digest": "b39cf274ee1a8c75a81995f0324cb3ab6cd18ce13ae68aaffc15fcba78e5f8ba",
    "semantic_payload_digest": "e97d97db3b315860ef5a0b39258908945f74651b94b68f4d3c319800d680266d",
    "completion_evidence_digest": "a2b9b987591ff4579565ee15a05172eb4f6ea34cd9542e874ab4e7a186102682"
  },
  {
    "name": "empty",
    "instruction": "Echo the input.",
    "input": "",
    "raw_output": "",
    "sequence": 0,
    "prompt_tokens": 8,
    "output_tokens": 0,
    "canonical_message_bytes": 76,
    "semantic_spec_digest": "b39cf274ee1a8c75a81995f0324cb3ab6cd18ce13ae68aaffc15fcba78e5f8ba",
    "semantic_payload_digest": "04e2e3e0c1a42742676ad000b8078ee01818d8bf78351ebed5b570787c477df8",
    "completion_evidence_digest": "3e6e485a1a2ac07f9f21a7d9265e471ec7c7cbbb77b785ee958e27a09321e7f2"
  },
  {
    "name": "unicode",
    "instruction": "原样返回输入。",
    "input": "甲\n\"乙\"\\丙\t{}",
    "raw_output": "甲\n\"乙\"\\丙\t{}",
    "sequence": 0,
    "prompt_tokens": 19,
    "output_tokens": 9,
    "canonical_message_bytes": 103,
    "semantic_spec_digest": "85c6173c584925bc2c400eebb78bd752e898c1971f7c8d9ecd8c4b83a43e58fd",
    "semantic_payload_digest": "ea61042f35816954d5d477f907e3667dbfb38d0dae9ac1f589e40838aeed4b32",
    "completion_evidence_digest": "4ddf117564c9ebffc55c579bfce150fd2c88b1df338b5db58f9f15a60db960c2"
  },
  {
    "name": "same-payload-sequence-1",
    "instruction": "Echo the input.",
    "input": "hello",
    "raw_output": "hello",
    "sequence": 1,
    "prompt_tokens": 17,
    "output_tokens": 1,
    "canonical_message_bytes": 81,
    "semantic_spec_digest": "b39cf274ee1a8c75a81995f0324cb3ab6cd18ce13ae68aaffc15fcba78e5f8ba",
    "semantic_payload_digest": "e97d97db3b315860ef5a0b39258908945f74651b94b68f4d3c319800d680266d",
    "completion_evidence_digest": "484506d798cbf5b9c415f7d44ce2a604a4a141600ec47f0fa7cd864287008184"
  }
]
```

### 10.3 反例和不变量

- 只改 sequence：S/PH/E/PD 不变，CE 改变；same-payload-sequence-1 是正常的下一项任务，不能因
  payload 相同而少发请求，不是重复序号反例。
- 真正重复 task 序号：0 已成功完成后再次发送 0，返回 INVALID_TASK 并终止 session，不再次调用模型。
  另测 0→2 跳号、超过 uint64、前导零；PG 对迟到/重复 completion 按关联检查拒绝，不重复输出或计数。
- 改 instruction 或 max_tokens：S 与 PD 改变；改 input：S 不变而 PD 改变。
- SQL options 的键顺序/等值数值写法、物理列号变化不改变 S。
- 改模型、输出、finish_reason 或 usage 后，原 CE 不能通过；改 provider execution ID 后，E 改变。
- 将完整多行文本沿用 Filter 换行 stop、把生成结果套三值 parser、把空串转换为 NULL，均是失败用例。
- open 的 S 格式合法但与首项 instruction 不符：opened 不等于语义验证通过，首项 task 必须零模型调用
  地失败；后续 instruction 变化同样拒绝。metadata 的无效 UTF-8/NUL，以及 wire usage 错用 JSON 数字均为反例。
- 对 U+0001、引号/反斜线构造最大 input/instruction/output，独立核对完整 frame 大小；
  编码上限不只测 ASCII。边界字节数与多字节字符跨界同时验证。
- C、Python 和文档值须三方一致；不得只让 C 与 Python 共用同一个错误预期。

本稿按字段表做离线最大转义核对：task body 为 1,008,142 字节；含最大模型/结束原因字符串及
完整 uint64 编码范围的 completion body 为 394,857 字节，均小于 1,048,576 字节。长度不含四字节帧头。
这里 sequence/两项 usage 均为最大 uint64 的 20 位十进制 JSON 字符串；若仅将 output_tokens
限制为计划最大值字符串 "4096"，后者为 394,841 字节。较大上界也覆盖先解码、随后因 token 数越过
计划而拒绝的帧。前稿的 394,853/394,837 把 usage 写成了 JSON 数字；本次补齐其两对引号和类型定义，
不改变 U64 摘要编码或已有向量哈希。
这只验证本稿编码尺寸有余量，不替代 C/Python 实现的边界测试或运行内存验收。
