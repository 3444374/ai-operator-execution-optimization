# AI 算子执行 Infra 当前状态

日期：2026-09-06（Map 真实模型纵向链路及修复后小规模资源诊断通过；正式资源资格、质量/校准和四 D 整体未完成）


2026-09-06 资源工具修复分支：已受限重写独占结果目录、阶段采集和最终聚合，新增
`resource_lifecycle.py` / `resource_phase.py`，修正归因、稳定基线、异常采样和中断证据。
测量身份为 `semloom.pg.resource.v2.1`，当前修订 `phase-lifecycle-3`；PG/provider/wire 生产路径未修改。
最终验证集合为本地119项（117通过、2项Linux专属跳过）、Linux130项通过；其中118/129项绑定
`77a123de`，另追加1项包含三种listener基线反例的测试，运行时代码相同。
`77a123de` 的隔离PG18.3真实1×100 fixture-only诊断，四场景九阶段均valid/passed，模型请求0；
同一存活backend/gateway完成清理，各阶段结束FD/线程增量0。前两次不完整/失败运行保留。
[证据与哈希](../experiments/results/postgresql/semmap_resource_lifecycle_20260906/README.md)单独登记。
正式3×2000未授权、未运行；正式资源资格和四D完成仍pending。
当前实施依据见 [资源工具生命周期修复](../experiments/plans/postgresql_semmap_generation_contract.md)。

文档角色：本文只记录源码实际模块、已接线能力、运行形态和明确未实现项；接口目标、工作包顺序与
验收标准由
[`../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`](../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md)
负责，测试/实验支持的结论由证据台账负责。

本文说明 PostgreSQL 中立语义算子 reference capability 与现有 Daft + Ray 外部物理执行基础设施
已经完成什么和实际执行流程。当前 recording `SemMap/SemFilter` compatibility paths 与三参数 exact
`SemFilter` golden/fixed-model paths 不等于第二 physical path 或完整优化系统已经实现；项目
不修改 vLLM 内部。

**已合入主线的身份加固切片**：`codex/pg-function-identity` 已复现并修复同签名非成员误接管，只改 `extension.c`
中的 PG 对象解析。测试覆盖其他 schema/重载、其他扩展成员、函数定义替换、删除重建与 prepared plan；
`934f4f61` 增加双物理连接的仅成员 ADD/DROP 手动刷新验证，函数 OID/定义不变，保留刷新前后计划。
实现与证据已随 `390f666a` 合入 main，服务器验收仍绑定测试源码 `934f4f61`。
详见[身份验证](../experiments/results/postgresql/function_identity_20260902/README.md)。
最终 PG18.3 warning-free `-O2 -Werror`、regression 1/1、TAP 1022/1022（身份 103 项）、本地/服务器
各 94/94 与中立 C11 通过。中间 990/990、生产反例与夹具失败保留；没有重跑模型或资源 smoke。
仅成员变更的跨会话自动计划失效仍 pending；受控使用须暂停查询、结束旧事务/游标，DDL 提交后刷新
所有相关物理连接（包括连接池）再恢复，不能只刷新 DDL 会话，也不能用成员移除即时撤销权限。
无法保证所有连接刷新时不使用此临时方案；函数替换/删除重建的既有自动失效测试不延期。

**尚未完成项**：仅成员变更自动刷新、四 D 生成型 Map 的资源资格、两个 Filter AND / 有界多会话与 Filter → Map；增量
SchedulingSession、PG accepted-prefix/多在途与公司 adapter。已有值合同的历史验证仍绑定 `d26e210d`。

**真实 Map 追加结果**：[`main@b19486a1` 的 2026-09-04 检查](../experiments/results/postgresql/semmap_real_model_resource_20260904/README.md)
用精确 PG18.3、wire v5、固定 Qwen2.5-7B/vLLM 0.25.1 完成累计 25/32 个有持久账本的请求。
SELECT/INSERT 各 10 个非 NULL task，NULL 零调用；空串、Unicode、取消、模型 4xx 拒绝和恢复通过，
gateway FD/线程回到基线，vLLM 正常清理。fixture 主压力又完成 3×2,000 个 100,000-byte 输入和
65,536-byte 输出，但 60 秒内至少一个固定 RSS/FD 条件失败；采集器未在断言前保存具体差值，后置
取消/断连/gateway-exit 子项未运行。因此真实纵向链路已验证，资源与四 D 整体仍未通过。
四 D 的[合同](../experiments/plans/postgresql_semmap_generation_contract.md)已定稿，源码复核已登记于 §8.0。
独立分支 `codex/semmap-message-contract` 的 `6903cf46` 只实现 C/Python Map 消息编译与公共 JSON writer，
将 Filter 的 system 内容留在 Filter machine；原 recording/Filter 行为保持。
[本轮验证](../experiments/results/postgresql/semmap_messages_20260903/README.md)：本地/服务器各 107/107，
精确 PG18.3 重新构建 `-Werror`、regression 1/1、TAP 1022/1022。该消息切片已随下述修改合入本地 main。
后续 `c338d81b` / `425d2b1c` 已实现并验证 C/Python Map 纯值摘要与完成策略、Python v5 及共享 gateway。
[本轮资格](../experiments/results/postgresql/semmap_values_20260903/README.md)绑定 `425d2b1c`：本地/服务器各
135/135，PG18.3 `-O2 -Werror`、旧路径 regression 1/1、TAP 1022/1022 重新通过；独立复核问题已修复。
后续 `a1bbdd30` 补深层 JSON 读帧隔离与同进程恢复，本地 136/136 及 C11 通过；
见[追加验证](../experiments/results/postgresql/semmap_values_20260903/README.md#json-depth-repair)。未重跑服务器 PG 或模型，旧 TAP 仍绑定 `425d2b1c`。
已从 `63d86c0e` 快进合入 `b0400944`；合并后的本地 main 复跑 136/136 与 C11 通过，
见[合并检查](../experiments/results/postgresql/semmap_values_20260903/README.md#main-integration)。该次集成未推送或部署服务器。
随后独立分支 `codex/semmap-pg-plan` 的 `2205ccbb` 已完成 schema 4 的 PG 保存、SQL 0.2.0
新三参 Map 注册/升级、固定参数来源与值检查、计划复制/严格解码、原生 EXECUTE ACL/hook 和函数依赖。
该历史 plan 切片的 EXPLAIN 为 `plan-only / not-connected`，实际执行拒绝；现已被下方接线替代。
新重载的 SELECT/直接 INSERT SELECT 规划、缓存计划跨会话撤权、函数替换/删除重建与成员手动刷新
均有测试。输入/谓词子查询反例已最小修复；仅成员变化的自动刷新仍未实现。
[PG plan 验证](../experiments/results/postgresql/semmap_pg_plan_20260903/README.md)绑定 `2205ccbb`：
PG18.3 `-O2 -Werror`、regression 1/1、TAP 1260/1260（新 plan 238 项）、本地/服务器各 136/136 与 C11。
后续 `676615fa` 复现并修复 SQL wrapper 在 custom 模式通过内联绕过来源检查；新 Map 只接管规划前
已核验的显式输出，整个 Map wrapper 明确拒绝，普通函数内联保留。临时检查状态在嵌套/ERROR 后恢复。
[追加资格](../experiments/results/postgresql/semmap_pg_plan_20260903/README.md#sql-wrapper-source-check)：
PG18.3 regression 1/1、TAP 1283/1283（Map plan 261）、两端各 136/136、`-Werror` 与 8/8 C11 通过。
随后 `5031bb50` 完成 [C v5/PG golden 资格](../experiments/results/postgresql/semmap_pg_wire_20260903/README.md)：
生成型 Map 已复用现有 runtime 逐行执行，显式 stop absence/limits 与 v5 身份贯通，原来源/ACL 检查保留。
同时修复常量/等值普通列的输出绑定和非法 UTF-8＋超长完成值的错误优先级；旧版本协议行为不变。
PG18.3 `-Werror`、regression 1/1、TAP 1741/1741（Map plan 268、execution 451）、两端各 137/137
与 8/8 C11 通过，保留该次分支验收身份；原主工作树/工具链未覆盖。
合并前复核在 `f46fe936` 收拢 v5 错误的版本/阶段校验，统一 C/Python 的单字段 usage 范围，
保留 runtime 跨任务累计溢出检查。新红测分别复现握手误报与合法 usage 被拒绝；修复后重新通过
PG18.3 `-O2 -Werror`、regression 1/1、TAP 1758/1758（Map execution 468）、两端各 139/139、C11 8/8。
源码与文档现纳入 main；详细[复核记录](../experiments/results/postgresql/semmap_pg_wire_20260903/README.md#merge-review)
分别保存失败、通过及安装/进程检查，不重新绑定旧 1741 项或历史资源证据。
后续真实 Map 请求累计 25/32 并通过受限 PG18.3 纵向链路；RSS/FD fixture 主压力虽完成全部 task，
固定资源条件仍失败，详见本文顶部追加结果。资源 pending 不能由 golden 或真实 completion 代替。
2026-09-04 静态审查后：资源观测链路已按 metric schema v2 重写（shared-tick 采样、显式 snapshot
有效性、稳定 FD-identity baseline、SO_PEERCRED 会话归因、same-tick 峰值、stress/cleanup phase
分离、异常安全落盘、真实 fault case、CLI 退出码 0/1/2/3；合同 §8.4.2 pre-run static-review
correction 已登记）。这是测量实现修复，不构成资源资格通过。同日诊断运行后二轮审查又修复了
归因失败假绿、diagnostic 负载失实（归档客户端硬编码 3×2000，参数化 `resource_client_v3.c`
取代）、缺 role tick 零值替代、cleanup 相 unknown、active session 无门、recovery 相 baseline
崩溃、数值文件名猜测残留与 persist 前致命检查等路径（合同 §8.4.2 第 9–16 条）。首轮诊断
（`semmap_resource_v2_diagnostic_20260904`，实际负载 3×2000，README 已更正）确认归因/峰值/
清理数字真实，但其负载身份与 `--diagnostic` 标记不符；修复后的小规模 diagnostic 需重跑，正式
3×2,000 qualification 仍待独立授权。
2026-09-05 三轮审查又修复了状态机组合链缺口（合同 §8.4.2 第 17–21 条）：stress 判定曾因
cleanup 延迟组合而结构性永远 inconclusive；disconnect/exit 顶层曾可产生合同外状态对（runner
现全路径经 `compose_status` 组合）；全 tickless 归因窗口与窗口外 fd 复用改标两个假绿/失真面已
关闭；case 级崩溃与非空结果根目录现输出 runner_failure（退出码 3）。补齐指令点名的
observer/CLI 测试文件。仍不构成资源资格通过；小规模 diagnostic 与 Linux 全套测试在目标环境
重验前，正式 qualification 不启动。
下方历史实现与测试记录保持原提交身份。

`00cc6bbf` 已实现第四个 SQL option 与 schema 3：完整 profile 保存为 PG 命名节点，严格解码到指定
context，支持 copyObject、prepared/generic plan 与 invalidation；C encoder 已链接 PGXS，完整规范
bytes 纳入新的 semantic digest。旧三字段 options/schema 2/digest/wire v3 不变，旧 calibration
artifact 对新身份返回 `semantic-spec-mismatch`，不使用旧系数。
该 PG plan 切片当时只支持规划与普通 EXPLAIN，执行以 `0A000` 拒绝；此临时限制现已由下方 C 接线替代。
测试修订 `134447dd` 已核对真实 provider socket 配置；完整 PG18.3 TAP 537/537、本地/服务器 Python
68/68、C11 与 warning-free `-O2 -Werror` 构建通过。详细 regression 和日志见
[PG plan 接入验证](../experiments/results/postgresql/choice_pg_plan_20260902/README.md)。
后续 `7d72d9ad` 已实现 gateway wire v4 的严格 open/task 校验、profile/semantic identity 和
golden/fixed-model 映射；共享 `wire/semantic.py` 与 `adapters/semantic_session.py`，保留 v3 import/runner。
外部 fixed config 须显式声明 `choice_format=vllm_structured_outputs`；否则 choice 在 HTTP 前拒绝。
不降级、不重试、不改写 raw output；现有模型、prompt、生成参数与 HTTP deadline 策略不变。
本地/服务器各 83/83，PG18.3 warning-free 构建、regression 1/1、TAP 537/537 通过，见
[gateway v4 验证](../experiments/results/postgresql/choice_gateway_v4_20260902/README.md)。这只增加 fixture
协议与兼容证据：该历史切片没有修改 C/SQL/TAP，也没有真实模型或新资源 smoke。
`8e7cd92d` / `8674269d` 随后把完整 profile 复制到 query-owned `AiOpenSpec` 和 session-owned UDS spec，
以 `has_generation_profile` 显式选择 v4；v3 入口拒绝带 profile 的请求。`wire_semantic.c` 共享固定 exact
编解码，v4 单独校验字段/版本/profile/evidence，并拒绝重复 JSON key。pump 的 `pending_plan` 分支已删除，
schema 3 SELECT 回到公共 runtime 与原严格 parser；无任务仍不连接 provider。
最终测试源码 `80bb7fc5` 通过 PG18.3 `-O2 -Werror`、regression 1/1、TAP 748/748、本地/服务器各
83/83 Python 与中立 C11。见 [PG choice 接线验证](../experiments/results/postgresql/choice_pg_wire_20260902/README.md)。
新增 218 项 choice 执行检查含真实 PG→golden/fake HTTP、三值/NULL、Unicode/空串、prepared/invalidation、
savepoint、错误/取消恢复、旧端拒绝和实际 HTTP 参数对照；不代表真实模型或 RSS/FD 资格。
该轮用旧 `7d72d9ad` 与 `80bb7fc5` 二进制确认三种 Filter profile 的 `INSERT ... SELECT` 均未 lowering，
执行报 `55000`，历史证据保留。后续 `8e50addf` 只修改 Filter planner 的源 FromExpr 与 INSERT 检查，
修复简单源 SELECT 上拉后未被接管的问题，未改 runtime/provider/wire/parser/cost。
最终 `39007150` 通过 PG18.3 `-O2 -Werror`、regression 1/1、TAP 919/919（新增 INSERT 171）、
本地/服务器各 83/83 和 C11；见[INSERT 验证](../experiments/results/postgresql/semfilter_insert_20260902/README.md)。
受限单表 recording/exact v3/choice v4 INSERT 已验证实际写入、三值、NULL、LIMIT、prepared、
savepoint 部分写入回滚、目标约束及 choice RLS/权限/取消恢复。该次未运行资源或真实服务检查。

`4464fe9b` 随后新增实验专用 `choice_resource_checks`、`choice_gateway_observer` 与持久化预算 ledger，
未改生产 PG/gateway。PG18.3 复用上述已验证二进制，SHA 一致；新受控资源运行的 v3/v4 各 5,164 次
fixture 调用、10 次取消与 10 次阻塞 DNS/恢复均通过预定阈值。首轮 FD 减少不符合“相等”断言、
容器 proc 权限诊断失败均保留；受控重跑只在自有表维护完成后关闭该表自动 vacuum，没有放宽阈值。
预算/HTTP seam 新增 8/8；本地既有 83/83 同时通过。真实模型请求为 0，未重跑构建/TAP，旧 919/919
仍只绑定 `39007150`。详见[资源验证](../experiments/results/postgresql/choice_resources_20260902/README.md)。
当前 gateway 仍串行服务整个会话；零任务双连接检查复现第二握手等待，不能声称已支持多算子。

四 C 的[受限真实服务检查](../experiments/results/postgresql/choice_service_20260902/README.md)随后通过。
初始工具 `87b7963b` 在首个真实响应后将 BatchEncoding 字段数误作 tokens，整轮失败；`0a1c12d3`
仅修复采集工具，离线反例与回归测试通过后在新目录完成 14 次 old/choice 请求和两个 NULL 对照。
本地/服务器各 94/94；实际 JSON 仅差 choice，PG rows/calls/usage 与原始输出一致。累计预算 15/100，
包含首轮 1 次；启动失败、主动停止和工具失败均保留。未改 PG/runtime/provider 生产代码，未重跑
TAP/构建；工程接入完成不表示三值判断质量合格、恢复校准或实现第二路径。已完成的专项计划移入
`experiments/plans/completed/`，主计划继续定义真实 Map 与可组合执行，当前集成版本已包含实现与归档。

当前 PG 可执行路径使用 schema v1/v2/v3/v4、wire v2/v3/v4/v5 与同步单在途 port；
一参 recording Map 保留兼容身份，三参生成型 Map 已通过 PG＋golden，并在 `b19486a1` 上通过受限
真实模型纵向验收；资源资格仍失败。
模型与 generation constraints 位于 query-fixed `AiOpenSpec`，不是每个 `AiPreparedTask` 的字段；
逐项 task 使用 sequence/input/canonical_messages/payload digest/is_null。当前 PG→gateway 协议没有跨进程
query/operator/task ID 组合、query registry 或显式 provider.cancel；UDS 通过连接与 sequence/摘要关联，
取消通过 close/disconnect 和 PG cleanup。既有 `SynchronousScheduler` 不等于已实现增量 session。

**固定 endpoint 与部署身份（2026-09-03，核对已集成源码至 `b0400944`，无新增接线）。**
`execution_provider/server.py` 在 golden 与 fixed 配置间二选一，串行服务整个 session；没有多 endpoint
路由。`adapters/openai_compatible_fixed.py` 的配置必填 endpoint_url/model_id/timeout_ms，可选
bearer_token_env/choice_format；timeout 为 1–300,000 ms，choice_format 用于显式 choice 能力声明。
同一个 HTTP(S) Adapter 可以指向本地或获准的第三方服务；它逐请求创建并关闭 HTTP 连接，
resolver 有独立同步保护，不等于已经完成连接池或多会话资格。token 从指定环境变量读入 gateway
内存，再用于认证头，并非只留在环境变量中；不进入 PG plan/wire 摘要或公开证据。

`wire_semantic.c` 的 v3/v4 provider digest 仅绑定 domain、协议版本、execution ID、model ID；
Python v5 也没有增加部署绑定，均不含 endpoint URL/部署实例/timeout。completion evidence 又绑定
三类身份摘要、payload、sequence、raw output/finish/model/两项 usage，不证明实际模型部署。
现有 calibration 的 workload/service signature 是外部选择与审计信息，PG 不在线探测部署。
显式 profile、部署快照与逐任务路由证据设计见
[主计划 §6.5](../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md#execution-deployment-identity)，
尚未实现；不据此更改旧摘要、原始资格或四 D v5 的用途。

独立核心研发、真实 PG 接入、Filter 质量与公司环境条件现已分开；顺序只看
[主计划](../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md)，本页不缓存第二份计划。
受限 choice 真实执行检查不改变 Filter 校准暂停与原始质量失败结论。

**公司工程参考不是已复用代码的声明**：已只读核对 SQL 函数注册、marker/CustomScan、pgvector
表达式复用、对象身份、列映射/rescan、prompt/有效请求、批执行与资源回收；完整取舍见主计划 §8.7。
公司 Filter 为大模型二值判断，embedding cascade 未启用；自有当前
三值 profile 与其 NULL/error 行为不同，不能据名称认定等价。未来移植包含算子语义/处理/优化与
SemLoom 执行两部分，目前都没有公司路径验收证据。

**当前工程限制**：生成型 Map 的 PG→C v5→golden 与固定 Qwen2.5-7B 真实链路已接通，资源压力资格
未通过；planner
限制单 marker，Map/Filter 不组合；rescan/EPQ 明确拒绝；gateway 按整个会话串行服务。函数查找已核验
扩展成员，并由上方切片验证非成员不被误接管；不据此声称发现权限绕过。后续改动
与验收要求只在主计划维护；现有 PG-private runtime、neutral port 和严格错误/结果处理继续保留。

**当前实现事实**：按
`experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md` 已完成 `REL_18_3` extension
`SemMap` 与 exact `SemFilter` 的当前受限 reference capability、PostgreSQL-private shared runtime、provider-neutral
`AiOpenSpec → AiPreparedTask → AiCompletion` `open/drive/close` 接口和同步单在途 UDS recording slice；
协议 v2 的 C/Python semantic-spec/physical-algorithm/provider-execution/payload/completion digest、
1 MiB 长度帧、adapter-owned 174,080-byte 编码前输入上限、
Unicode、escaped/raw NUL、严格整数、断连、取消与清理已验证。provider 只在首个非 NULL task 到达时打开，`PROPAGATE_NULL`
由 PostgreSQL 本地完成；`sem_scan.c` 只保留 CustomScan 回调，`sem_pump.c` 负责 child slot 流，并只转交
planner 预计算的 cost metadata，不在执行期重新计算 cost，
`PgSemanticRuntime` 统一负责 query-fixed provider、lazy open/drive/close、sequence、completion copy、
query-context cleanup、中立错误映射和公共 EXPLAIN 计数；Map/Filter machines 分别处理 emit 与
TRUE/FALSE/UNKNOWN keep/drop。`sem_plan_spec.c` 由 planner 把当前 recording operator/value/policy、
semantic spec identity、physical algorithm 与 physical role 编码为版本化、可 copyObject 的 plan 值；
executor 严格校验字段集合、类型、长度与 schema version，input column 仍是独立 executor binding。
runtime 是唯一把 PG-private plan spec 转成 `AiOpenSpec` 的位置，machine 不再构造 recording identity。
中立 header 不包含 PostgreSQL 类型，也不包含 socket/JSON/frame/response-field operation；adapter 通过
稳定错误类别、`errno`、长度和本地生成的定长脱敏详情保持原 SQLSTATE/消息。recording、
UDS 和 wire v2 各自隔离；每次 drive 使用可重置 scratch context，结果复制到 per-tuple context，UDS 从
`connect()` 前即为 nonblocking，并在 UTF8 之外 fail closed。query-context cleanup callback 在任何 lazy
资源取得前注册，返回型错误终止并关闭 session，直接 interrupt/longjmp 由同一幂等本地清理路径兜底。
公共 extension 级 PostgreSQL compatibility suite 和第二个真实消费者已经通过，未为 `SemFilter`
复制 provider lifecycle，也未为 `SemJoin` 或 blocking operator 预造通用执行器。提交 `3b2077e1`
新增三参数 `ai_semantic.filter(text,text,jsonb)`：planner 只接受常量 instruction 与严格三字段 options，
schema v2 plan 保存 prompt/parser/model/generation 与 semantic/physical digest；pump 持有 PostgreSQL
slot/Datum/MemoryContext binding，operator-machine header 已不含 PostgreSQL 类型。独立 wire v3 使用
163,840-byte 输入上限、严格 open/task/completion 字段与 C/Python digest vectors；gateway golden adapter
只按测试提供的 payload-digest fixture 返回 raw output，PostgreSQL 本地严格解析 uppercase
`TRUE/FALSE/UNKNOWN`。提交 `53cf3da8` 又增加共享 `V3SessionRunner + CompletionAdapter` 与固定
OpenAI-compatible endpoint adapter；endpoint/model/timeout/auth 从仓库外严格配置读取，每 task 一次非流式
请求且不 retry，PostgreSQL 通过 query-fixed execution profile 选择 distinct provider identity，并继续
负责 digest/model validation、parser 与 keep/drop。后续 `a4319655` 拒绝 301/302/303/307/308 而不访问
重定向目标，并以 monotonic deadline 约束慢响应；`ef314618` 进一步把 DNS 解析纳入同一调用截止时间，
因此调用会在 timeout 内返回，连接/TLS、发送、响应头和响应体仍受同一 deadline 约束。`71a8ef7d`
又拒绝显式端口 0，并让同一 adapter 的连续 DNS timeout 共享至多一个 in-flight resolver attempt；
底层系统 resolver 不能被 Python 取消，但不会再按失败调用数累积线程。提交 `47407751`
又为 exact reference path 增加独立的 `sem_filter_cost` planner metadata：ordinary predicates 重建
semantic-input rows，NULL 率调整 model calls，并显式报告 output selectivity、estimated prompt/output
tokens、model role 与 AI work estimate；actual provider calls/usage 另由 `EXPLAIN ANALYZE` 计数。
`71a8ef7d` 将模型身份改为 `semloom.exact_filter.uncalibrated.v1` 并公开 calibration unavailable。
`dcde2be5` 又新增 planner-only matched-reference calibration：离线 builder 将严格 training/held-out
观测分解为 selectivity、calls/input、tokens/call 与 fixed/call/prompt/output service-time 系数，绑定
semantic/physical/model/provider/workload/service identity；PostgreSQL 独立校验 artifact 后把匹配值复制
进 cost metadata schema v2，缺失、损坏或失配则继续执行 uncalibrated reference。runtime、provider、
wire 和 semantic digest 未改变。当前 deterministic fixture 只证明合同和 planner consumption；真实
model/workload/service artifact 及其 held-out 误差资格仍未完成，因此还不能实现第二 physical path。
当前尚缺真实 matched artifact、第二 physical path、carrier audit、
accepted-prefix、多在途和增量 SemLoom provider；实现顺序只从工程计划读取。
2026-09-01 的[首轮真实校准采集](../experiments/results/postgresql/semfilter_reference_calibration_20260901/README.md)
已运行，但没有产生 artifact：固定 Qwen2.5-1.5B-Instruct 完成 64 条预热，首个 training 查询的第 23
个响应不符合严格 tristate，PG18.3 按现有合同报 `22000`。完整 training/held-out observation 均为 0，
未拟合、未加载 artifact，没有修改生产代码、阈值或失败样本。本次 55/55 合同和 6/6 采集检查不代替成本精度。
后续复核发现旧 builder 的有限精度消元会把完全共线的残差误当非零主元；现已增加拟合前的精确有理数
设计矩阵检查。最终合成反例复核又将单主元阈值升级为整体检查：列归一化后以精确有理数形成 Gram
矩阵并求逆，奇异或无穷范数条件数 ≥`1e16` 则拒绝，本地 calibration 测试扩为 10/10。`6c111b24` 的
[独立小切片](../experiments/results/postgresql/semfilter_qualification_20260901/README.md)通过 PG18.3
`-Werror`、regression 1/1、TAP 437/437 和 Python 合同 59/59。普通 SQL 加入 MCV/dependencies 后
estimate 从 8 改为 64，与 actual=64 一致。原 generation 与独立 choice 候选的格式结果为 27/30、30/30，
但预期语义均只符合 12/27，reference 资格未通过。该历史诊断时 choice 仅有版本化实验 plan/generation
manifest，未进入生产 SQL/spec/wire；后续工程接入见本文顶部。真实 calibration artifact 仍未发布，
整轮采集继续暂停。
最终数值补充 `44f6632c` 独立通过 PG18.3 warning-free `-Werror`、regression 1/1、TAP 437/437、Python
60/60（calibration 10/10）；仅加强整体病态检查，没有重跑模型或改变上述语义资格结论。
随后完成[单一 prompt/model 对照](../experiments/results/postgresql/semfilter_prompt_qualification_20260901/README.md)：
实际 HTTP messages、服务 tokenize 与模型 chat template 一致；1.5B 新 prompt 旧/新各 5/9，
matched 7B 新 prompt 为 7/9、6/9，原 prompt 为 8/9、8/9，仍无合格配置。每例重复三次，不能把
重复响应当独立样本。中止的 7B 默认 repetition-penalty 失配尝试单列保留；修正值来自原基线。
本轮仅有实验 manifest，生产代码/SQL/plan/wire 未改，held-out 和校准继续暂停；本地/服务器
Python 60/60 复跑通过，未重跑 PostgreSQL regression/TAP。
当前源码已有受限的 `SemMap` 与 relation-level `SemFilter CustomPath/CustomScan` recording capability，
并在 `REL_18_3` 上通过 PGXS regression 与 preload/prepared/generic-plan/invalidation、RLS/权限、
snapshot/savepoint/cancel/insert 生命周期 TAP；shared runtime 已通过
中立 `AiOpenSpec/AiPreparedTask/AiCompletion` 值调用 `open/drive/close` in-process recording provider；
同步单在途 UDS provider 与分离的 semantic-spec、physical-algorithm、provider-execution digest 也已
实现，物理列号不进入 wire identity。最终结构债务提交 `e89060a7` 的精确 18.3 验收为 warning-free
`-Werror` build、regression 1/1、TAP 193/193、Python/static 20/20。仓库外资源 smoke 中，
2,000×100,000-byte SemMap 的 RSS 起始/峰值/结束为 21,340/22,172/22,172 KiB、FD 为 43/43/41；
20,000 行 SemFilter（15,000 个非 NULL task、5,000 行输出）的 RSS 为 22,172/22,204/22,204 KiB、
FD 为 43/43/41，未观察到累计 payload 近似线性增长或 FD 泄漏。该 smoke 不提供性能结论。历史
`d3a22dcf` shared-runtime、`0b9948ee` hardening 与 `d08eda38` seam/resource 证据继续保留并绑定各自提交。
提交 `868430f9` 已把 Python recording gateway 的权威实现迁到 `code/src/execution_provider/`：
`wire/framing.py` 保存有界 JSON framing，`wire/v2.py` 保存冻结 recording v2 schema/digest，
`adapters/recording.py` 保存 recording session，`server.py` 保存 UDS listener；
`code/postgres/semloom_pg/gateway/` 只保留无需额外 `PYTHONPATH` 的 import/CLI compatibility wrapper，
canonical CLI 为 `code/scripts/services/run_execution_provider_gateway.py`。该迁移没有加入 v3、HTTP 或
新 plan fields，并在精确 18.3 上通过 regression 1/1、TAP 193/193、Python/static 25/25、`-Werror`
与 Map/Filter RSS/FD smoke。其后的 `3b2077e1` 已完成工作包 4A：三参数 exact `SemFilter`、schema v2、
wire v3、deterministic golden、严格 parser 和 completion evidence 均已接线；精确 18.3 通过
warning-free `-Werror`、regression 1/1、TAP 268/268、gateway/v2/v3/static 32/32 与 neutral C11 header。
同一 backend 的原有 Map/recording Filter smoke 分别为 RSS 20,932/21,792/21,792 与
21,792/21,792/21,792 KiB，FD 27/27/25 与 27/27/25；20,000 行 exact Filter 输出 8,000 行，RSS
17,636/17,636/17,636 KiB、FD 25/25/25。数字只证明当前 workload 未观察到累计内存或 FD 增长。
后续 `359ffdf3` 完成行为不变的 4A.1 hardening：`wire_common.c` 真正拥有 framing、
socket wait/connect 与 PostgreSQL JSON primitives，v2/v3 各自保留 schema/error 解释；v3 error
frame 严格校验四字段、version、nullable/decimal sequence 和 code allowlist。query-fixed provider 在
open 前公布中立 input limit，pump 通过 runtime 在 canonical-message 扫描/分配前 fail closed，
UDS drive 仍保留防御检查。精确 18.3 复验为 warning-free `-Werror`、regression 1/1、TAP
320/320、protocol/static 33/33、gateway migration 5/5 与 neutral/machine C11 compile，并新增 exact
Unicode instruction/input、空字符串、savepoint/recovery 和严格 error-frame 覆盖。该复验不替换
`3b2077e1` 的原资源 smoke。
复核后已将最终 TAP 320/320、PostgreSQL server log、字节级一致的 regression
actual/expected、commit identity 和 SHA-256 manifest 持久化为仓库外证据包
`postgresql_semfilter_4a1_hardening_359ffdf3_20260831`。该证据包另包含干净
`359ffdf3` checkout 使用显式 PostgreSQL 18.3 `pg_config` 生成的 `-O2 -Werror`
build log、exit code 0 和 `semloom_pg.so`；临时 worktree 删除后 manifest 仍全部通过。
旧 resource-test gateway 及 socket 也已按精确进程/路径清理；这只说明本切片相关
测试资源已收回，不扩展为服务器其他工作负载的清理结论。
工作包 4B `53cf3da8` 已在相同 plan/task/result contract 后接固定 OpenAI-compatible endpoint，并把
golden/fixed 两个真实 v3 consumer 的 session loop 抽到共享 runner。固定 profile 的 model identity、
valid/invalid raw output、HTTP 4xx/5xx、timeout、returned-model mismatch、savepoint、statement cancel、
fresh-session recovery 与 no-task lazy open 已进入 TAP；精确 18.3 通过 warning-free `-Werror`、regression
1/1、TAP 404/404、Python/static 45/45 与 neutral/machine C11 compile。仓库外证据包
`postgresql_semfilter_4b_fixed_model_53cf3da8_20260831` 还保留 core/text preflight、build/installcheck、
失败尝试和小规模真实模型输出。Qwen2.5-1.5B-Instruct/vLLM 0.25.1 capability 对 `yes/no/NULL` 只返回
`yes` 对应行，并保存 raw `TRUE`、model identity、finish reason 与 usage；这不构成质量、性能或泛化结论。
工作包 4B.1 的最终提交 `ef314618`（含 `a4319655`）收紧 fixed HTTP boundary：全部常见 3xx 都返回
`MODEL_RESPONSE_INVALID`，不会访问 Location 或转发 bearer token；DNS 解析和持续小块响应均受单一
monotonic deadline 约束，超时返回 `MODEL_TIMEOUT`。服务器等价源码树通过 Python/static 48/48、
warning-free `-O2 -Werror`、regression 1/1、TAP 404/404 与 neutral/machine C11 compile。仓库外证据包
`postgresql_semfilter_4b1_http_hardening_ef314618_20260831` 保存 source/diff identity、preflight、测试、
build/installcheck、字节一致的 regression actual/expected、扩展二进制和已校验 SHA-256 manifest；
测试临时集群已停止。该加固不替换 `53cf3da8` 的真实模型 capability，也不增加质量、性能或资源结论。
exact-reference cost/cardinality 提交 `47407751` 以第三个 copyObject-safe `custom_private` 元素保存
cost model ID、reference model role、semantic-input rows、output selectivity、estimated calls/tokens 与
AI work cost，不改 schema v2 `SemanticPlanSpec` 或摘要。精确 18.3 资格为 warning-free
`-O2 -Werror`、regression 1/1、TAP 414/414、Python/static+migration 49/49 和 neutral/machine C11
compile；仓库外证据包 `postgresql_semfilter_cost_cardinality_47407751_20260831` 已校验 manifest。
复核提交 `71a8ef7d` 拒绝端口 0、把 resolver 工作限制为每 adapter 至多一个 in-flight attempt，并将
planner estimate 明确标为 uncalibrated/calibration unavailable；精确 18.3 通过 regression 1/1、TAP
415/415、Python/static+migration 49/49 与 warning-free build。证据包为
`postgresql_semfilter_gap_hardening_71a8ef7d_20260901`。planner calibration 提交 `dcde2be5` 新增严格
29-field artifact、跨 Python/PostgreSQL identity、held-out qualification、query-planning loader、稳定拒绝
原因和 cost metadata schema v2；精确 PostgreSQL 18.3 通过 clean `-O2 -Werror`、regression 1/1、TAP
437/437、Python/static/gateway 55/55 与 neutral/machine C11 compile。证据包
`postgresql_semfilter_reference_calibration_dcde2be5_20260901` 的 SHA-256 manifest 已全部校验。该结果只
证明 deterministic artifact 的生成、验证和 planner 消费；真实 model/workload/service artifact 尚未
采集，第二 physical path 仍不得实现。归档后本切片测试集群、两个临时 worktree、PGDATA 和 socket
目录已清理；该结论不覆盖服务器其他工作负载。accepted-prefix、多在途/
乱序 completion、增量 SemLoom session 和 LOTUS compatibility adapter 仍未实现。
LOTUS v1.2.4 不再是核心前置依赖。
下文图像和 SAOR 待办仍是条件性工作，未因独立核心的并行研发安排获得运行授权。

系统所有权接口开始使用 SemLoom 规范名：文本静态执行和图像 Ray/HSE 执行已提供
`SemLoom*`/`run_semloom_*` 名称；既有 `Project*` import 与 `project_static`、`project_ray` 身份保持
完全兼容。PostgreSQL source/sink、planning、scheduling 和 serving 接口继续使用领域名称。

全部机制、代码测试和正式结果目录的逐项对应见
`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`。该台账明确区分代码完成、
真实链路门禁和性能证据。

## 1. 当前端到端流程

```text
PostgreSQL
  -> DaftPostgresSource
  -> DaftOrganizer / shared Arrow policy core
  -> BatchRequest + PayloadEnvelope
  -> optional arrival replay + flush policy
  -> admission controller (K_max)
  -> optional request-cost pool router
  -> endpoint router
  -> Ray task / actor adapter
  -> vLLM-compatible endpoint
  -> request/submission/control/resource traces
  -> optional PostgreSQL JSON/pgvector sink
```

上图是已完成的**外部文本/vLLM 路径**；数据库读取与写回由外部 runner 管理，不能重标为
planner-visible 数据库内算子。2026-08-01 至 2026-08-13 的 image-first A+B 轨道已完成 CLIP 5K
画像、静态 operator-E2E、原生多 Job 观察和 observe-only 接线。下图中的
PostgreSQL→Daft→Ray CPU preprocess→typed CLIP actor 已跑通；分阶段 work/state 合同、真实 ready
broker 与 static HSE adapter 已接入 image runner，但尚未运行 HSE GPU 对照，动态 SAOR 也未接入
该路径；小规模 pgvector sink/质量验证与动态性能验证在 PostgreSQL 中立语义算子资格步骤完成后恢复：

```text
PostgreSQL image source
  -> Daft
  -> Ray CPU decode + resize + normalize
  -> StageBlockDescriptor + byte/work-bounded real ready broker
  -> typed tensor-input CLIP backend (Ray GPU actor primary)
  -> PostgreSQL + pgvector
```

边界是明确的：

- Daft 负责数据读取、分区和 dataframe 执行入口；
- Arrow table 是当前 payload boundary；
- 策略只读取 `BatchRequest` 元数据，不依赖 Daft、Arrow、Ray 或 HTTP；
- Ray task/actor 负责并发提交与收集；
- vLLM 负责模型内部 continuous batching，本项目不修改它；
- PostgreSQL/pgvector 写回是工程 baseline，不作为独立研究贡献。

## 2. Batch 数据组织部分

### 已完成

- Fixed rows。
- Sequential token-budget（当前默认）。
- Length-align × fixed rows/token-budget。
- Prefix-aware × fixed rows/token-budget。
- Classic best-fit-decreasing。
- BFD-inspired row-cap-first placement。
- Prompt-only、fixed-output-cap、trace-target-output 三种执行前代价模式及来源标签。
- Arrow 与 Daft 共用同一套纯策略函数；全局 packing 不在 Daft 分支复制实现。
- 统一记录 batch row/token 分布、packing utilization、oversized rows、
  submission count 与 per-request lifecycle。
- 新增兼容的 `WorkDescriptor`：在保留旧 `work_units` 的同时表达
  source/prepare/model/result 分阶段 demand、locality、deadline/SLO、uncertainty 和 calibration
  signature。当前只完成合同与单元测试，正式运行仍使用已冻结标量 credit。

### 本轮新增与结果

- 增加 row-cap-first placement：选箱时先减少剩余行槽，再考虑 token residual。
- 64 行真实门禁、512 行筛选/重复和 1024 行 held-out 均跑通。
- 512 行出现小幅正向信号；1024 行虽然 tokens/s `+0.82%`，但 10 秒 SLO
  violation 从 `50.39%` 升至 `88.67%`。
- 设计决定：sequential token-budget 继续默认；classic BFD 不采用；
  row-cap-first 只保留为可配置消融点。

### 当前流程

离线吞吐实验先由 organizer 根据完整输入决定 batch membership，再生成
`BatchRequest`。在线 arrival replay 为了保持到达因果关系，只允许 fixed rows
或 sequential token-budget；length/prefix/BFD 等会重排未来请求的策略被显式
拒绝，不会混入 flush 实验。

## 3. 调度与提交控制部分

### 已完成

- Static bounded inflight（`K_max`）。
- Legacy queue-adaptive baseline。
- Typed AIMD、EWMA-AIMD、PID controller。
- 非阻塞后台 vLLM metrics sampler；提交决策路径不再做网络 I/O。
- stale/missing metrics 保守 hold；control trace 记录 sample age、窗口、动作、
  原因、running/waiting/KV。
- Immediate、fixed-timeout、queue-adaptive flush，带 hard max wait。
- Arrival replay 使用 monotonic clock，保持完整行请求与到达间隔。
- Arrival replay 可选择 `submission_granularity=request`：packing/flush 仍记录
  组织边界，关批后每个完整行请求独立提交，任一完成即释放一个 admission credit。
- Scheduler 组合顺序固定：admission → pool routing → endpoint routing →
  Ray submit → bounded collection。
- 静态或 service-quantum token budget；动态策略只在静态容量曲线标定的离散
  候选中逐步移动，并在 metrics 缺失时保持当前安全值。
- per-endpoint active-work credit：按 prompt + 预测 output token 记账，
  与 request-count K 独立开关。
- least-work endpoint routing：优先预测 drain/active work 较小的 endpoint，
  与 least-queued 保持独立消融入口。
- 多 job shared credit：Ray named actor 统一持有 endpoint request/work
  capacity，使用带权 deficit round robin 和空闲容量借用；联合
  `(job_id, request_id)` 防止不同作业的 batch ID 冲突。
- `saor_bounded_priority` development path：在冻结 endpoint request/work envelope 内按
  actual-work debt recovery → ready-head reclaim barrier → SLO priority window → 原 SAOR
  fallback 的词典序释放；每 Job 只有一张 recovery lease，acquire timeout 会取消 waiter，
  不会留下幽灵队列或永久 hold。priority/SLO/window/debt cap 全部由显式 per-Job 配置给出。
- bounded-SAOR 机制审计使用 coordinator 单调序号的 lossless release-event ledger；runner 在
  采样、成功结束和失败落盘前 drain。5 ms 转换不会再被 250 ms snapshot 漏采判成失败，
  而账本缺失、为空、序号缺口或重复仍 fail closed。
- `saor_bounded_ready` development path：每 Job 用由冻结 K/W 派生的有限 ready-window 预注册
  已 concrete-ready 请求，trace 分开 ready/registered/granted/submit/service；coordinator 对
  register/grant 记录 request ID 与 epoch，跨 trace join 不完整时 fail closed。旧 single-head
  bounded-priority 保留为 observation-gap 回归对照。
- Project bounded-ready FIFO/DRR/VTC-style/strict-priority/guarded-debt matched controls 与
  single-head→bounded-ready FIFO observation bridge 已接入 readiness、runner、无损 ledger 和
  双轮汇总；这些是 Project 实现的算法 controls，不是 Daft/Ray/vLLM 原生 baseline。
- 新增 `BoundedStageWorkController` 纯策略候选：仅在离线校准的离散 work-credit
  集合内单步升降，观测 stale、stage 缺失或 calibration signature 不一致时回退
  workload-specific frozen-static。尚未接入 runner，也没有性能收益 claim。

GPU development 已完成：旧 single-head bounded-priority 两 cap 均未过 foreground 门并定位
ready-backlog observation gap；bounded-ready $0.125W_e$ 随后通过双轮开发门，$0.25W_e$ 被 bulk
guard 拒绝。同 ready-window 双轮归因中 guarded-debt 用约 4.8% 吞吐和约 5.2% bulk JCT 换取
更低 foreground tail，只形成观测非支配折中；固定顺序 n=2 且未预注册 selector non-inferiority
margin，故 `formal_authorized=false`。

截至 2026-08-14，本地五臂合同和离线汇总已经实现；2026-08-19 的两次服务器执行分别被 MFU
与 summary 检查拒绝。之后 `93271012` 已通过四阶段 readiness、五臂 correctness smoke、5/5
rehearsal cell、exactly-once 和独立 archive validation。统一 gateway 现可为五臂提供 T0--T4、
request P99/SLO、Jain、service lag 与最长无服务；单次结果只说明效率、尾延迟和公平性权衡。
五臂 formal 仍未运行，0s/5s 数据也不能替代 matched-solo isolation control。

### 当前流程

1. Flush 决定未满 batch 何时关闭；
2. Admission 决定已关闭 batch 是否允许提交；
3. Pool router 按请求代价/前缀选择逻辑池；
4. Endpoint router 在池内 round-robin、least-queued 或 prefix-affinity；
5. Ray adapter 提交 task/actor，并把完成结果恢复为原 submission 顺序；
6. lifecycle 层生成 exactly-once request/submission trace。

默认 `submission_granularity=batch` 仍按整个 submission 返回回收 credit；
显式 request 模式已经闭合逐请求 credit release。Daft packing group、Ray
submission 和 vLLM iteration batch 仍是三个不同层次。该实现有单元与真实本地
Daft→Ray task 合约证据；各策略的 GPU 证据和适用范围分别列在下方“证据边界”和实验台账中，
不能从通用 scheduler 流程本身推导统一性能收益。

### 证据边界

- 静态 `K_max=8` 的必要性已有 shared-vLLM 干扰证据。
- Queue-adaptive flush 已完成随机化变长输出、跨 arrival-rate、2048 held-out
  和 shared-vLLM 双作业：它稳定优于 fixed-25，但未优于 fixed-50；共享压力
  下约 89.4% 决策选择 50ms。当前默认采用 fixed 50ms。
- AIMD/EWMA/PID 已完成代码、单元/集成契约和单作业 512 请求真实 GPU
  矩阵。AIMD 又完成 shared-vLLM 128/512 双作业重复：0 次 decrease、窗口
  均值 15.953，相对 static K16 前台和吞吐均略差。当前没有动态反馈增量证据。
- UCB 多臂老虎机已有有限 action set 与 SLO reward 的纯控制器代码，但尚未
  接入 profiler。原因是缺少稳定的 epoch-level reward/归因边界；现在接入会把
  跨 epoch 的请求完成错误归因给当前 arm。
- 完整 SLO-aware flush 已加入 oldest-request slack、arrival/service EWMA、
  独立容量下界、hard deadline 和滞回，并完成双 4090 正式重复；相对 fixed-50
  未过 5% 晋升门槛。25–50ms 动作相对秒级 request P99 缺少一阶杠杆。
- 逐请求完成释放 credit 和持续补位已实现；此前 7B 云端 warm-up 误用
  `ray_batch_rows=1` 且仍为 batch granularity，不能作为该机制性能证据。
  后续固定 active-work 的 service-quantum 正式对照已经完成：request diagnostic 相对 whole batch
  吞吐仅 +1.75%，但能缩短 credit-held，并提供真实逐请求完成语义。该路径保留为多 Job
  credit/fairness 基础，不声称独立稳态性能增量。
- complete-row service quantum 已接入 offline/arrival replay：planning batch
  只定义组织边界，quantum 独立定义 HTTP/Ray completion 与 credit 释放边界，
  单行 prompt 永不拆分。active-work、pool shape 与 service quantum GPU
  对照均已完成。least-work routing 已接入 typed scheduler，但尚无独立 GPU 因果收益；
  shared multi-job credit/fairness 已完成 equal-workload 1/2/4-job 与 5s short/long
  guaranteed-overlap 正式对照。它证明效率—隔离—公平权衡，不证明 shared 全面胜出。

## 4. Actor pool、endpoint 与 GPU 扩展

### 已完成的框架

- `EndpointSnapshot` 显式包含 endpoint、pool、GPU、健康状态和队列指标。
- Request-cost pool 可把 short/long/prefix 请求路由到不同逻辑池。
- 池缺失或 endpoint 不健康时有确定性 fallback。
- Prefix affinity 使用 rendezvous hashing；无 prefix 时回退 least-queued。
- CLI 支持多个 endpoint URL、pool ID 和 GPU ID。
- Ray task 与 actor 走同一 typed scheduler。
- service endpoint 与 Ray actor worker 是两个维度：前者是独立 HTTP 服务地址，
  后者是面向该地址的 Ray 客户端 actor。配置并发上界为
  `endpoint_count × actor_workers_per_endpoint × ray_actor_max_concurrency`。
- HTTP worker 只向 Ray 申请 CPU，`ray_worker_num_gpus=0`；GPU 归外部模型服务。
  正式 completion 的 task retry、actor restart 和 actor task retry 均保持禁用。
- 正式/dry-run CSV 已记录 Ray 版本、解析后的 worker/resource 配置、endpoint 数、
  actor worker 数和逐 worker 提交计数；Python executor 的 `ray_version` 为空，
  actor concurrency/CPU 使用明确的非适用哨兵 0/0.0。Ray task 无 actor worker，
  记录 task 的实际 CPU 配额，actor-only concurrency 字段同样记 0。
- fake Ray task/actor 也应用相同的 CPU、零 GPU 与禁重试/重启 options；它仍只是
  debug backend，不是 HTTP 模型服务或性能证据。
- CSV 追加会校验既有 header 与当前 row keys 精确一致；空文件写 header，旧 schema
  不一致时在写入前明确失败，避免列静默错位。
- `ActorWorkerPoolSubmitter` 显式维护每 worker running/active-work/峰值/失败和
  slot-held 时间；round-robin/least-active-work 都只从有空 slot 的 worker
  选择，成功与失败都由 canonical handle 精确释放一次。
- effective per-endpoint admission 不超过
  `actor_workers_per_endpoint × ray_actor_max_concurrency`。正式 trace 记录
  worker ID/index/PID、planning/quantum identity、credit-held 和
  Ray-to-service delay；slot-held utilization 不是 GPU utilization。

### 尚未完成的验证

双 GPU per-endpoint K 功能门禁和 16K–131K active-work 扩展曲线已经完成；
65,536 是当前模型/workload 的预注册最小饱和点。Actor Pool 三形状 gate 与
正式重复均已完成；固定 work/slots/CPU 后，多 actor 未达到 5% 晋升门槛，
当前保留 1×256。complete-row service quantum 正式重复也已完成：细粒度
把 credit-held 降约 16%，但稳态吞吐增益不足 5%，固定 quantum 不晋升；
request-level completion 保留作后续动态/多 job 精确控制基础。
shared-credit 与 1/2/4-job 核心矩阵已经完成；2-job 无增量，4-job 聚合指标过
5% 但逐 repeat 波动大。5s short/long guaranteed-overlap 又完成 quota-only 控制和
static/shared A/B：shared 总吞吐 +21.03%、long JCT −18.31%，但 short JCT +4.98%、
Jain 下降。仍缺 held-out 4+ job、weighted/SLO、异构 workload、图像 phase-change、
故障迁移和异构显存容量验证，因此不能声称多 GPU 调度已经普遍完成。多个 Ray actor
worker 仍不能被当作多个 GPU endpoint。上述文本遗留项在 image-first pivot 后为
`parked-conditional`。

## 5. 观测与实验运行基础设施

### 已完成

- CSV 同时记录 PostgreSQL/pgvector 版本、tokens/s、request P50/P95/P99、
  SLO、GPU utilization/memory/power/energy、energy/1k tokens、vLLM
  running/waiting/KV、FLOP delta 与 MFU。
- vLLM-compatible completion 可选请求逐 choice token IDs，并记录真实
  per-request output tokens 与 finish reason；generic compatible server 默认
  不发送该扩展字段。
- completion prompt envelope、temperature 与数据源最大 prompt-token
  过滤均为显式配置；超长行只排除，不截断或拆分 prompt。
- request、submission、flush、control、resource trace 分文件保存。
- seeded/interleaved scenario schedule、每次运行前 idle gate、命令脱敏和
  atomic manifest。
- 本轮增加安全续跑：已完成项不重复，失败项可重试，历史 incident 标记
  recovered。
- 本轮增加失败场景剪枝：不伪造 CSV，manifest 显式记录 skipped run。
- 本轮增加 `service_metadata`：vLLM 版本、prefix cache 和 MFU 开关进入
  manifest，并参与 resume 一致性校验。（2026-07-31 起 runner 在 `main()` 额外
  校验 `prefix_caching` 与 live vLLM 进程标志一致，见 `code/src/serving/probes/vllm.py`：
  不符 fail-closed、探不到则 warn。）

### 本轮发现并修正的实验问题

仅检查 MFU metric 名称不足以证明计数有效。vLLM 0.25.1 必须使用
`--enable-mfu-metrics`，并通过真实请求验证正 FLOP delta。

此外，prefix cache 会让重复 prompt 实验出现强顺序依赖。本轮正式
512/1024 结论统一使用 `--no-enable-prefix-caching`；此前启用缓存的数据只作
事故审计，不进入性能结论。

## 6. 研究内容完成度

### 本轮新增的可复用基础设施

1. **模型一致的 workload 计数入口**：workload importer 可调用当前 vLLM 的
   `/tokenize`，按实际模型 tokenizer 记录 `prompt_tokens`。模型上下文门禁采用
   “完整行保留或排除”，不通过截断、拆分或复制 prompt 凑实验规模。更换模型时
   只需切换 tokenizer endpoint/model name，不需要改 organizer 或 scheduler。
2. **受控 prefix workload 构造器**：从同一批基础行按稳定哈希选择精确且嵌套的
   0/30/70/100% 子集，只在完整原 prompt 前增加公共指令；session、arrival、
   tenant、目标输出元数据保持不变。每条变换后的 prompt 重新计数并通过上下文
   门禁，原 workload 不被原地修改。
3. **职责单一的 prefix organizer**：仅聚合真实重复的非空 `prefix_key`；
   唯一 prefix 和重复组内部均保持原始相对顺序。Length alignment 继续由
   `length_align_*` 独立策略承担，避免一个策略名隐式叠加两个机制。实现先建立
   prefix→row positions 映射，组织复杂度为 O(n)，不为每个 prefix 重扫输入。
4. **无执行后泄漏的代价估计边界**：离线 estimator 只读取提交前可知的行数、
   prompt token、输出上限、token budget、batch 统计、K_max、flush 和 arrival
   配置。实际输出 token、实测 E2E/service、vLLM、能耗和 MFU 只作为目标或评估
   证据，不进入特征。相同配置的重复运行按组切分，避免 train/test 泄漏。

### 按研究内容划分的当前状态

- **研究内容一——数据组织**：主机制和工程链路已经闭环。Sequential
  token-budget 是当前默认；fixed rows、length-align、prefix-aware、classic
  BFD、row-cap-first 都有可运行实现和对照入口。BFD/row-cap-first 已有负向规模
  边界，prefix-only 在 cache-off 下无稳定收益；cache-on 下 prefix-aware batching
  中性；prefix-affinity routing 在 2-ep/7B 中性（prefix_affinity vs least_queued
  −0.1%，<5% 门禁），但 4-ep/1.5B prefix_affinity +5.9%（46,943 vs 44,317 tok/s，
  3 repeat 不重叠、CV≤0.9%）跨过 5% 门禁。后续 matched-KV 扫描表明 2-ep/1.5B
  在 gpu_mem_util 0.3–0.9 均中性，因此当前更支持 endpoint consolidation，而非
  单纯 per-endpoint KV 大小，是驱动；4-ep 饱和深度仍未完全隔离。文本残留已 parked。
  图像侧已有 stage descriptor、physical byte/work 和 observe-only fresh snapshot；尚未完成的是
  frame/prepare/model cost 对组织与调度决策的在线驱动和跨模态 held-out 验证。
- **研究内容二——调度与提交控制**：static K_max、arrival replay、flush、
  非阻塞 service observation、typed controller、pool/endpoint routing 和
  lifecycle trace 已形成完整流程。当前证据选择 static `K_max=8` + fixed
  50ms；queue-adaptive、AIMD/EWMA/PID 和 UCB 均未获得默认资格。尚未完成的是
  UCB 的 epoch reward 正确归因，以及真实多 endpoint/多 GPU 公平性和故障迁移。
- **两项策略联合关系**：18 单元筛选与候选重复已经完成；当前单 GPU 上联合候选
  未显著优于独立拼接，因此保留分层配置与联合搜索工具，不增加联合在线控制器。
- **多模态泛化验证**：策略接口和中性 `cost_units` 边界已具备；COCO val 5K 的
  CLIP 初始 slow-path 与当前实现边界画像均完成。tensor fast path 相对
  production-np 串行 profile 约 1.14–1.22×，但 CPU prepare 仍为 actor 的
  13.8–31.2×，因此 E2E build 动机保留。
  lazy image source、Daft built-in、Ray Data native graph 与 bounded Ray CPU→GPU operator-E2E 已完成
  provenance/correctness/matched-resource 正式证据；Daft 60K object-store 容量失败单列。原生图像
  single→four-job 40/40 runs、30 formal group 与 Project staged descriptor + observe-only 24/24 group
  已归档。Project snapshot 100% fresh、构建均值 0.141 ms，但 static/proposed-role group JCT 只差
  0.98%，因此尚未证明 state-aware 收益。官方 ResNet18 vendor-code parity、vLLM pooling 当前
  blocked，不阻塞主线；仍缺 HSE static GPU 非劣门、两级 stage controller/CE5 在线接线、统一小规模
  pgvector sink 质量闭环及跨 workload/硬件验证。
- **算子代价估计（共同使能组件）**：CE1–CE5 与 429 formal/20-context context-LOO
  已完成；CE5 pooled/macro/max regret=1.67%/2.90%/14.72%、candidate pairwise=0.808，
  只算贴线的文本配置选择可行性。它仍是离线分析器，未在线驱动 organizer/scheduler；
  独立时间段、新 workload/图像阶段、预测区间和跨硬件迁移仍未完成。

| 部分 | 代码完成度 | 真实证据 | 当前判断 |
|---|---|---|---|
| 数据读取与 Daft/Ray 主链路 | 高 | 64/512/1024 真实链路 | 已完成基础设施 |
| Fixed/token-budget batching | 高 | 多轮真实实验 | 机制成立，sequential 默认 |
| Length/prefix grouping | 高（代码） | 0/30/70/100% cache-off screen + cache-on batching/routing 消融 | cache-off 无收益；cache-on batching **regime-dependent**（2-ep 近似中性、4-ep KV 饱和分化+排名反转，见 `rc1_data_organization/`）；2-ep/7B routing 中性（−0.1%），4-ep/1.5B +5.9% 跨过 5% 门禁但混淆待隔离，方向有条件重开 |
| BFD/row-cap-first | 高 | 512 + 1024 | 负向边界明确，不默认启用 |
| Static K_max | 高 | shared-vLLM | 必要性成立 |
| VTC-compatible multi-job | 高（代码/配置） | 8-client 四臂 1+3 formal 已完成；phase-change A-only 与三档 pressure 已提前停止 | 8-client 16/16 group、12 formal 通过；动态臂相对同上限 K160 静态点没有独立增量。phase-change 未形成预注册的双端点降档压力，未运行 action/formal；不再写成整体“待部署” |
| Queue-adaptive flush | 高 | 512 变长重复 + 跨 rate + 2048 held-out + shared-vLLM | 优于 fixed-25；未优于 fixed-50 |
| SLO-aware EWMA flush | 高 | 双 4090 high/arrival-limited 各三次 formal | 相对 fixed-50 未过 5% 门槛；不默认启用 |
| Request-level continuous replenishment | 高（代码） | 双 GPU K 对照 + 固定 active-work quantum/formal | 逐请求释放与 completion 已验证；保留为 shared-credit/fairness 基础 |
| AIMD/EWMA/PID | 高（代码） | 单作业矩阵 + static K16 control + shared-vLLM 双作业 | AIMD 饱和至 K16，未保护前台；不默认启用 |
| UCB bandit | 中（纯控制器） | 无端到端实验 | 尚未接入执行路径 |
| Actor pool / endpoint routing | 高（有界 slots/trace） | 双 GPU 1×256/2×128/4×64 formal | 多 actor 未过 5% 门槛；单 job 保留 1×256，多 job 分池待测 |
| Shared-vLLM group runner | 高（代码/模板/真实 formal） | equal-workload 36/36 group + 5s short/long static/shared 6 formal | shared-credit 容量安全；4-job 仅条件性。5s A/B 证明效率—隔离—公平权衡，不称全面胜出 |
| 联合 batching × submission 搜索 | 高（本地单 GPU） | 18 单元筛选 + 4 候选重复 | 独立拼接与联合最优不可分辨 |
| 多模态复用 | 中高（native + project staged observation） | Daft built-in/Ray Data/project matched-resource formal + 原生 40/40 four-job + Project observe-only 24/24 | 静态/观测证据已闭合；待 HSE static GPU 门、stage-aware/CE5 在线决策、小规模 pgvector 质量闭环与 held-out |
| 算子代价估计 | 中（离线） | 429 formal、20 context × 4 candidate context-LOO | CE5 配置选择 marginal pass；尚未在线驱动或验证跨模态 remaining work/SLO |

## 7. 当前实现差距与工程计划入口

### PostgreSQL 中立语义算子与 provider 状态

1. `REL_18_3` extension/planner-visible `SemMap` 与 exact relation-level `SemFilter` deterministic
   recording reference paths 已验证当前受限 `SELECT` 和 direct `INSERT ... SELECT` 的 ordinary child
   plan、三值/NULL、cardinality、snapshot、取消、rollback/commit、错误恢复和结果生命周期；
   Filter direct INSERT 的修复已验证，见上方归档；rescan/EPQ/parallel、`RETURNING`、`ON CONFLICT`
   与更宽 query shapes 仍保持 fail-closed；
2. PostgreSQL-private `PgSemanticRuntime`、thin `SemloomExecPump`、独立 Map/Filter machines、provider-neutral
   `AiOpenSpec → AiPreparedTask → AiCompletion`、独立 recording/UDS adapters、协议 v2 canonical digest
   与同步单在途 `open/drive/close` 已实现；lazy open、PostgreSQL-owned `PROPAGATE_NULL`、query-context
   cleanup、per-drive scratch、per-tuple completion copy、编码前输入上限、UTF8 校验及可取消
   nonblocking connect 已通过精确 18.3 测试；escaped/raw NUL、fractional integer 与稳定 error context
   的 hardening 也已通过；
3. extension 级 PostgreSQL compatibility suite 已固定并通过：普通 SQL、RLS/权限、snapshot/事务/savepoint、
   prepared/generic plan 与 invalidation、planner-hook chaining static contract、多 backend 隔离、
   cancel/ERROR/资源清理
   和无任务不连接；该套件只在 extension 层维护，不在每个算子目录复制；
4. exact `SemFilter` 已作为第二个真实消费者证明公共层边界：PostgreSQL carrier 负责 slot/plan 与
   `LIMIT` 前 placement；`PgSemanticRuntime` 拥有 provider lifecycle/sequence/memory/error；
   `SemMapMachine` 与 `FilterMachine` 分别负责 emit 与 keep/drop/unknown 语义；
5. 工作包 4A 的 instruction、prompt program、result parser、model/generation constraints、canonical
   messages、payload/completion evidence 与 usage 字段已进入 schema v2 plan/task/result；golden 只返回
   fixture raw output，不是模型或质量 oracle；
6. 工作包 4B 已以固定 OpenAI-compatible endpoint 复用同一 schema/wire/parser，并用 query-fixed profile
   区分 provider execution identity；4B.1 又拒绝重定向、限制 DNS 等待时间，并让 resolver 工作量有界；
   真实模型只通过小规模 capability，不提供质量或性能结论；
7. 当前 planner 只生成一个 reference role；该 path 已分开 semantic-input rows、通用 output-selectivity
   estimate、NULL-adjusted calls、prompt/output-token work 和 model role，并在执行时报告实际 usage。
   planner-only calibration builder/validator/loader 已实现，匹配 artifact 时保存 calibration/workload/
   service identity 与 held-out error，失配时保留 uncalibrated reference；当前只有 deterministic fixture
   资格，真实 matched artifact、第二 path identity、quality evidence 与 fallback 尚未实现；
8. 当前 provider interface 仍是同步单任务；recording wire v2 保持冻结，Filter v3/v4 与 Map v5 的
   deterministic golden 接线、共享 fixed-endpoint adapter 已实现；Map 的真实模型验收仍待完成。accepted-prefix、多在途、乱序 completion、
   增量 SemLoom session 和 SemLoom scheduling adapter 尚未实现；
9. 以上缺口的实施顺序和完成标准见
   [`../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md`](../experiments/plans/postgresql_ai_semantic_operator_architecture_20260827.md)，
   本文不复制未来设计。

### 条件性恢复：image path-B + A+B

1. ✅ `BatchRequest`/scheduler/Ray adapter 已支持中性 work-unit；lazy image source、
   typed batch/result、CPU CLIP preprocessor 和常驻 tensor actor 已实现并有单测；
2. ✅ PG→Daft→Ray CPU preprocess→Ray CLIP GPU actor operator-E2E、exactly-once、
   fused Daft actor shape 与 ours 静态 shape 已完成；
3. ✅ host-path/matched-resource 与原生图像 single→four-job、Project observe-only 已完成，瓶颈收紧为
   CPU prepare 与 driver/Ray submission 的组合；
4. 先在冻结 project best-static 上运行 direct-dependency vs HSE static GPU 非劣门；
5. 非劣通过后，把两级 stage controller 与 CE5 接入 Project runner，保留 stale/signature mismatch
   回退，并只重跑同 manifest 的 project static/proposed；
6. 小规模接 pgvector sink 做 embedding 检索质量闭环，再补跨 workload/时间段/硬件 held-out。

5K CLIP operator-E2E 只证明静态阶段拆分优于独立校准的项目自写 fused Daft UDF；
这不代表优于 Daft/Ray Data 官方 native pipeline、完整 system-E2E，或状态感知策略
已经胜过最佳静态路径。

### 已闭环：提交控制与局部联合实验

- 自然 EOS 三组随机化重复中，fixed-50 与 queue-adaptive 相对 fixed-25
  tokens/s 分别 `+32.23% ± 3.90%` 与 `+32.09% ± 6.22%`；adaptive
  相对 fixed-50 为 `-0.10% ± 4.13%`，没有可分辨增量。
- 固定 16-token cap 的 18 单元联合筛选中，K16 虽然吞吐最高，但所有配置均
  违反 1% SLO guardrail。
- 候选重复中，独立拼接相对 fixed-25 tokens/s
  `+4.76% ± 2.29%`；联合候选相对独立拼接
  `-0.26% ± 2.07%`，没有可分辨增量。
- 相同 8192/K8 下 adaptive 相对 fixed-50 tokens/s
  `-0.75% ± 0.97%`。当前 workload 的主要收益来自 50ms coalescing
  window，而不是动态切换本身。

因此本地单 GPU 当前采用分层设计即可：sequential token-budget →
static K8 guardrail → workload-specific flush window。联合搜索保留为验证工具，
不引入联合在线控制器。

### 已完成：跨负载、2048 与受控 prefix 边界

1. 约 51.4/25.7/12.85 req/s 三档均由 fixed-50 保持最佳或与 adaptive
   等价，adaptive 未获得默认资格；
2. 2048 held-out 没有出现策略排序反转，但暴露持续积压的尾延迟放大；
3. prefix ratio `0/30/70/100%` cache-off 实验未显示 prefix-only 收益，
   并修复唯一 prefix 重排和隐式 length-align 耦合。

### 已完成：多 job shared credit/fairness 核心矩阵

- 1/2/4-job × independent/static/shared-DRR 共 36/36 group run 完成；
  共享 request/work credit 无越界并最终归零；
- 1-job 协调开销可忽略，2-job 无可分辨增量；
- 4-job shared 聚合吞吐 +9.57%、max P99 -22.52%、max JCT -15.89%，
  但三次吞吐变化为 +8.43%/-0.28%/+22.60%，保留为高竞争条件性候选；
- 5s short/long guaranteed-overlap 已完成：quota-only≈0；shared 相对 static 提高总吞吐
  21.03%、降低 long JCT 18.31%，但 short JCT 增加 4.98%、Jain 下降；因此只冻结为
  效率—隔离—公平权衡证据；weighted/SLO、held-out 4+ job 和异构 workload 仍未验证。

### 文本轨道遗留（parked-conditional）

1. 已完成 16K–131K active-work 扩展曲线，选择 65,536；
2. 已完成固定 slots/CPU 的 1×256/2×128/4×64 actor pool 对照，保留 1×256；
3. 已完成 whole-batch、service quantum 与 request diagnostic，固定 quantum
   未过 5% 门槛；
4. 已完成 SLO-aware EWMA flush 正式对照，25–50ms 动作未过 5% 门槛；
5. 已完成 bounded direct、Daft `prompt()` Native/Ray、Ray Data 官方 runtime 的
   capability、单 Job 1+3 与多 Job 观察；OceanBase 因容器部署阻塞，只保留 capability 证据，
   不再作为当前 GPU 实验前置条件；
6. 各 baseline 使用 Chat Completions、同一双 endpoint 与请求 manifest，并分别校准；现有结果
   不能用弱默认值与已调优 Project 路径比较；
7. bounded direct 容量扫描已给出 C128 最小近饱和参照；旧 transient ramp 结果只按各自 timing
   粒度说明 capacity/overload，不与 request-level E2E 混排；
8. 两作业 5s staggered 和四 Job 扩展均已完成；weighted/SLO、完整异构 offset、故障迁移与新的
   held-out 组合仍留论文阶段；
9. Prefix-aware 已在 cache-on 下评估：batching regime-dependent（2-ep 近似中性、4-ep 饱和分化，见 `rc1_data_organization/`）；routing 在 2-ep/7B 中性
   （−0.1%），4-ep/1.5B prefix_affinity +5.9% 跨过 5% 门禁但受 model×endpoint×KV
   与过饱和 regime（SLO 违约 25–31%）混淆，方向有条件重开，待隔离消融；
   per-arm 命中率待 runner 增采；
10. UCB 只在能按固定 epoch 正确归因跨 epoch 请求 reward 后接入，并保留 static
   K=8 safety fallback。

### 2026-07-29 至 2026-08-02 文本 baseline 校准历史

以下段落按当时发生顺序保留失败、修复与复测依据；其中“当前”“下一步”和“不得继续”只表示
当时的执行状态，不覆盖本文件前述当前状态。后续 capability、单 Job 1+3、多 Job 观察和 bounded
容量扫描已经完成，现状以 `experiments/results/README.md` 与证据台账为准。

截至 2026-07-29，统一 Chat Completions、不可变 manifest、固定双 endpoint
分片、bounded HTTP、vLLM Bench、Daft Native/Ray、Ray Data HTTP、
OceanBase `AI_COMPLETE` adapter、归一化结果和 fail-closed gate 均已有实现与
单元测试。首轮 64 行双 GPU core gate 已 5/5 通过：每项 64/64
exactly-once、0 incident、双 endpoint、work skew 0.0085%，最终队列归零。

该门禁仍不是远端性能 baseline。通过后的等价性审计发现 vLLM Bench 会对
custom prompt 与 openai-chat 重复套 chat template；Ray Data 的整数 concurrency
在小作业中只起一个 autoscaling actor；Daft/Ray Data 只有 shard-barrier 级
延迟，且 Daft 不返回 output usage。vLLM Bench `--skip-chat-template`、Ray Data
`(n,n)` 固定 actor pool 与 `timing_granularity/token_accounting` 已在提交
`f2e82bd` 的全新 re-gate 再次 5/5 通过。小 gate 固定创建 4 actor，但可并行
task 不足，实际只使用 1 actor，不能据此得出扩展结论。

当前最后一个 calibration 前置缺口是统一服务端工作量计数。gate runner 已增加
每个 cell、每个 endpoint 的 vLLM prompt/generation cumulative counter 前后
快照和差分，并按 adapter accounting 能力交叉核验客户端字段。Daft 以服务端
差分补齐 output-work 证据；shard-barrier P95 仍不得与 request-level P95 横比。
全新真实双 GPU service-counter gate 通过前不能启动 calibration/formal。

统一服务端计数门禁随后已通过。256 行 scale gate 的五个 core arm 为 5/5、
0 incident；vLLM Bench C32 与 bounded HTTP C32 均约 4.93K total tokens/s，
而 Daft Native 单次约 9.82K。该差异目前只说明直接客户端 C32 可能未饱和，
不能证明 Daft 提升了 vLLM 计算速度。runner 已提供 fail-closed 的
`--include-cell` 与 `--concurrency-override id=N`，下一步只用同一 manifest
校准 vLLM/bounded C64→C128，不再远端临时改配置，也不重复运行 Daft/Ray
Data。每个并发档使用全新输出目录，先过 exactly-once、服务端 counter 和空
队列门禁，再比较 JCT、generation/total tokens/s 与 3% 饱和阈值。

C64 校准中 vLLM Bench/bounded 分别达到 8,342/8,333 total tokens/s，
JCT 均约 12.02s；相对 C32 提升约 69%。vLLM Bench C128 的真实 peak
concurrency=128，达到 12,762 total tokens/s、JCT 7.849s，相对 C64 再提升
53%。bounded C128 被 httpx 默认 100-connection pool 截断，8,711
tokens/s 数据作废；`async_http.py` 已把总连接与 keepalive 容量显式绑定为
`concurrency_per_endpoint × endpoint_count`。全新 bounded-only C128 re-gate
观测到 endpoint running=124/125，得到 12,472 total tokens/s、JCT 8.048s，
与 vLLM Bench C128 只差约 2.3%，修复已通过真实双 GPU 门禁。

512 行 direct calibration 随后完成：vLLM Bench/bounded C256 分别为
15,351/14,532 total tokens/s、JCT 11.931/12.569s；C128→C256 仍提升
24.3%/33.0%。因此 8.0–8.2K 只能称为历史 project runner/arrival-replay
链路平台，C256 只能称当前 `max_num_seqs` 配置硬上限。

project profiler 现已支持 manifest 锁定的离线 request-level replenishment、
固定 endpoint routing、raw Chat/temperature=0/trace-target payload 契约、
逐行源数据核验和 `source_row_offset`。512 行模板扫描 static K32–256 与
active work 16K–98K，并在正式 CSV 记录 manifest SHA 与 validated rows。
远端持久 Ray head `127.0.0.1:6380` 已只读确认可用。

首次 64 行 project gate 在任何 HTTP 请求前 fail closed：数据库有
`target_output_tokens>256` 的行，而 official manifest 的有效输出 work 已按
请求 cap 裁为 256；project 旧路径仍使用未裁剪 trace target。统一语义已改为
`min(trace target, completion_max_tokens)`，同时修正调度 work 与 manifest
校验；guard 另行重算 exact `source_row_hash`，不会把两个不同的 above-cap
raw targets 当成同一源行。旧失败目录保留，512 校准未启动；完整测试和全新 64 行 re-gate 通过前
不得继续。

行数门禁已解除：数据库现已持有多个 2048 行 workload（sharegpt_multiturn，
doc_id 300000-302047；sharegpt_concentrated 2048 行；sharegpt_burstgpt 2048 行）
以及 lmcache_agent（851 行）等，2,048 formal 不再因行数不足或 held-out 复用被
阻塞。2,048 formal 当前唯一的前置阻塞为下文的 5% 等价性门禁（K256 vs W98K），
该门禁未达阈值前完整 calibration、2,048 formal 与新上游策略均不启动；manifest
导出改用上述独立 workload 的只读切片，不再需要向 `0..2047` 追加 `2048..2559`。

第二次 64 行 re-gate 已在 `beeee20` 通过，但随后 512 行校准首场景暴露
active-work 背压语义缺陷：调度器曾把“该请求会超过 endpoint-local work
credit”复用为 `healthy=false`。当冻结 manifest 指定的 endpoint 暂满、另一
endpoint 仍有容量时，pinned router 会误报服务不健康而不是等待。当前模型已
明确拆成长期/观测健康 `healthy` 与 request-specific `available`；容量不足是
可重试背压，真实不健康仍立即失败。preferred endpoint 也固定其 pool，不能被
pool fallback 改写。fixed-pool、multi-pool pinned 与 shared-credit oversized
边界均已测试锁定。失败校准 0/9、无该失败请求的 HTTP 提交且无 `runs.csv`，
现场保留；新提交通过全新 64 行远端门禁前仍不得恢复 512 校准。

`0c370ce` 的全新 64 行 gate 随后已通过。512 行 9-cell calibration 虽为
9/9、0 incident，但理论等价的 static K256 与 nonbinding W98K 分别只有
11,736/4,153 total tokens/s，不能用于参数选择。只读诊断确认两者 manifest、
payload、max inflight=512、endpoint work、bounded wait 和 output work 等价；
主差异是 W98K 首个 full-concurrency cell 在 HTTP/vLLM request wall 多约
28.6s，actor readiness 只贡献约 3s。

当前实现增加显式 actor-ready barrier，barrier 在 E2E timer 之前并记录
`actor_ready_s`；非流式 Chat HTTP 结果与 submission trace 记录 request
start、response headers、body complete、headers wait 和 body read。校准模板
改为同压力 warm-up + 3 repeats，并新增只包含 K256/W98K 的等价性门禁。
该门禁未达到 5% 等价阈值前，完整 calibration、2,048 formal 和新策略均不
启动。

### 2026-08-02 文本 baseline 原生性与复测合同

- 旧 `official baseline` 命名已在输出语义上拆成四类：vLLM Bench service ceiling、
  bounded direct control、Daft/Ray Data framework-native baseline、OceanBase
  product-native baseline。历史文件名为兼容保留，不再决定实验角色。
- 每个 summary/validity gate 强制记录并核验 implementation provenance、scheduler
  owner、custom scheduling、formal eligibility、upstream source；原生 arm 含项目
  调度或 provenance 缺失时 fail closed。
- `official_runtime.py` 已按框架拆到 `baselines/runtime/daft_prompt.py` 与
  `ray_data_http.py`，共享单 endpoint shard 合同，避免把不同框架逻辑继续堆在扁平文件。
- Daft public `functions.prompt` adapter 没有接线 `partition_count`，旧 calibration
  中该假扫描因子已删除；Ray Data 只扫描其官方 batch/concurrency 参数。
- 服务端 token counter 现在直接写 prompt/generation/total tokens/s，解决 Daft
  不返回 output usage 时不同 arm 的吞吐口径不一致；Daft barrier 仍不能冒充 request P99。
- 新复测按 64 行 validity → 512 行独立 calibration → 4,096 held-out、至少 60 秒、
  1 warmup + 3 interleaved repeats 执行。完整合同见
  `experiments/plans/completed/text_native_baseline_rerun_20260802.md`。后续 capability、单 Job 1+3 和多 Job
  观察矩阵均已完成；真实状态以 `experiments/results/README.md` 和 evidence registry 为准。

完整顺序与放弃条件见
`experiments/plans/reference/literature_driven_pipeline_optimization_guide.md`。

### Image-first pivot 后的多 GPU、多模态与代价估计

- 多 GPU：先部署同构、各自独立占用 GPU 的双 service endpoint，再做异构池；
  验证健康回退、队列均衡和公平性。
- 多模态：5K CLIP 画像、matched-resource operator-E2E、原生 four-job 和 Project observe-only
  均已完成；当前先过 HSE static GPU 非劣门，再接 stage controller、CE5 和小规模 pgvector 质量闭环。
- 代价估计：当前 grouped held-out 五切分平均 MAE 11.68s、MAPE 50.60%、
  R² 0.776；相对误差仍不稳定，下一步增加独立时间段/新 workload 校准和
  预测区间，不新增独立系统层。

## 8. 当前可安全采用的默认值

以下是**文本/vLLM 轨道**的历史验证默认值，不可直接复制为 image/CLIP 的最优点。
Image 静态 baseline 已完成，但动态 HSE/阶段控制尚未校准，因此仍没有可跨 workload 声称的默认
K/frame budget/actor shape。

- 数据引擎：Daft；
- 执行：Ray task/actor 按实验目的选择，不把其差异包装成贡献；
- batching：sequential token-budget；
- admission：static `K_max=8`；
- flush：离线实验 immediate；当前已验证的 accelerated-replay 负载范围使用
  fixed 50ms；更换模型、到达过程或硬件后重新校准；
- routing：单 endpoint 使用 round-robin；多 endpoint 实验前不启用复杂池路由；
- vLLM 重复 prompt 对比：明确记录 prefix cache；本轮公平比较使用 disabled；
- 任何策略晋级必须同时通过 SLO goodput，而不是只看平均吞吐或 MFU。
