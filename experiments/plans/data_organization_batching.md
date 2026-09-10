# 研究内容一：动态数据组织与批处理构造策略实验计划

## 当前实施：执行修复与数据库查询套件（2026-09-10）

受众：内部工程与验证。用户已授权以下独立工作包及通过后的提交/推送/main 集成。每包完成代码与
受控验证后即可单独确定真实运行额度，不等待其余功能；未获新额度时登记“代码及受控验证完成、
真实验证待执行”，不开放未经验证的默认能力或声称性能通过。旧 100,000 请求/2 小时额度已结束。
本节接管近期实施顺序；下方容量工作保持历史身份。

2026-09-10 用户补充审查后的顺序：A 测量/推进修复；B 数据库输入/公共查询/原生入口；C PG 总量
预算；D 基础组织；E 依赖/共享服务；F 图像类型同步 reference→阶段接入→真实模型比较。
版本核验和入口准备可独立推进。D 在 A 与对应单 Map 的真实路径及参照有效后立即开展，不等待 E/F。
真实运行预算按包确定，不以“全部代码完成”作为新的总前置条件。

修订后的实现条件：

- 反压重试只由相关存储责任释放/预算变化触发，不以任意 wake generation 变化当容量变化。
  区分永久单项不兼容；立即工作与全部排队但受阻必须成对测试，控制循环每轮处理其他连接/取消。
- PG 预算是单算子留存预算；最多一个待接纳行及转换暂存使用独立有界预算并一并报告，不能占用
  已接纳任务的结果保留。多算子查询报告各算子之和，不能把单算子 B 叫整个查询内存上限。
- SQL 采用严格按需合同：LIMIT/易抛错/不支持形状保守窗口，LIMIT 0 零模型请求；不因输出恢复
  输入序就允许后续预读错误破坏原先可成功的 LIMIT 1。安全表达式按内置身份、类型、值域白名单，
  不以 IMMUTABLE/STABLE 推断无异常，也不承诺 PG 未保证的 AND/OR 求值顺序；RLS/安全屏障
  验证包括不可访问行从未出站。
- 原生 Ray 文本明确使用现有 HTTP endpoint 的 HttpRequestProcessorConfig；read_sql 版本/分片
  单独锁定，计划构建导致的 COUNT/取样也进入查询时间。Ray/LOTUS worker 出站与重试、缓存
  一并计入额度。Q3 保留原聚合评分并补逐行真假判断审计，防止误差抵消。
- 多 Job 首版只借用计算：每 Job 存储配置足以承载目标 C，全局给出 M 份同等存储，各对照一致；
  不够时标记存储限制，不声称全面工作守恒。恢复 Job 参与后续轮转，不抢占已提交请求；入口达到
  活跃上限时沿用明确拒绝，不新增无界等待；请求释放至拒绝/完成均计时。
- 图像 real[] 为一维固定维数、无 NULL、有限 float4；NULL 零任务。模型/processor/dtype/normalize
  固定，独立 reference 数值容差先于真实运行登记。自有 Ray 后端 max_retries/max_task_retries/
  max_restarts 均为 0；取消发送、计算确认结束、ObjectRef 放弃和结果责任释放分别记录，不为单
  查询取消杀死共享 actor，也不把本地 future 异常直接当远端结束。
- D 确定实现固定行数输入序、固定工作预算、同有限窗口长度分组三个控制；工作描述、单位、
  active-work 与组织预算一起绑定。精细结果预留、批量 IPC、Core 索引仍由具体证据触发。

| 工作项 | 源码/证据与采用决定 | 完成条件 | 状态 |
|---|---|---|---|
| 失败记录与单元收尾 | `map_query_recording.py`/`map_capacity_runner.py`，基线 `1dd6d060`；源异常必须先于上下文退出记录，追加独立清理/持久化错误；复用现有 recorder/预算 | 受控 2 秒报错、12 秒清理、二次错误、准备/评价失败与两臂共同期限 | 代码及受控验证完成；真实验证待执行 |
| offer 反压 | 旧归档 `bytes-result`：24708 before_offer/512 accepted；正常长查询比例 1；复用 v6 与 producer 绑定，不把比例直接当耗时归因 | PG/provider 真实受控路径在容量不变时不反复探测；未接纳不耗序号，已接纳不重发 | 代码及受控验证完成；真实验证待执行 |
| 全局推进 | `SessionEngine.advance` 一步后仍有可派发项、generation 不变且无 immediate 字段；增加真实可推进信号和期限 | 有容量不空等，容量阻塞不忙等；取消、晚到终态及公平机会保留 | 代码及受控验证完成；真实验证待执行 |
| PG 窗口预算 | `sem_pump.c` 的 B/L 等份与分配后检查；改为总量与一个有界待接纳行，独立报告暂存/转换分配 | 同 B 的合法大行不随 L 改变合法性；宽行/超限/取消与内存责任检查 | 待实现 |
| 数据库驻留比较 | PG-source direct、实际 WHERE→Map、原始列构造消息；内存 direct 继续是诊断 | 相同源快照/不可变表、计时含读取转换；安全谓词扩大窗口，其他形状明确回退 | 待实现 |
| 公共任务与原生执行 | SemBench Movie Q3→Q1/Q2、Movie-derived Map；Ray Data SQL/processor、LOTUS 原生程序 | 原任务/evaluator 固定版本；COUNT/LIMIT/Map 各自评价；未注入 SemLoom 调度 | 待实现 |
| 依赖与多 Job | 复用 Filter→Map、查询归属和共享 Engine；计算工作守恒轮转、存储独立保留 | 2/4 查询、错峰/暂停/慢消费/失败、累计 Job 超过并发上限 | 待实现 |
| 图像 PG→Ray | CLIP encoded bytea→CPU prepare→GPU actor→real[]；先同步 reference 再增量，复用 typed image/method/stage 组件 | PG/解码/受控模型零权重集成；真实 CLIP 另验，不冒充已通过 | 待实现 |
| 文献与解释 | KEN 核心补充待精读；IMLane artifact 可用性；修正 direct 日志和非单变量对照解释 | 文献/知识库/状态/结果/日志同步，历史数据不改写 | KEN/历史解释完成；IMLane 待核查 |

工作包 A 已完成代码及受控验证，真实模型验证待新额度。记录器/runner 保存第一原因与独立清理错误；
Core 全局进展增加兼容字段；永久 Job 单项超限与暂时存储反压分开；v6 不把永久拒绝发成零接纳确认；
PG pump 等到对应 receive 释放责任后再探测。真实 PG 受控 32 行为 660→62 次 offer，提交均为32次，
无相关释放的重复探测598→0；PG回归1/1、TAP2043项通过。证据、失败与当前清理状态见
[执行修复报告](../results/postgresql/execution_repairs_20260910/README.md)。KEN 与历史解释已完成，IMLane 核查待工作包 B。

### 工作包 A 的最小真实验证清单（额度待确认）

首次额度已获用户授权并执行准备，但在首个预热gateway启动时因子进程缺绝对PYTHONPATH停止：
0模型POST，128预留未领取，204.31秒内关闭模型/两套诊断PG。已补runner源码环境并通过37项受控检查，
原失败和预留保留；下面的完整清单尚未跑成，重新运行需单独确定额度，不自动复用余额。

这份清单只准备匹配诊断，不继承旧预算、不寻找新并发平台。允许的上限为 **8,448 次模型 POST、
单张 RTX 4090、从模型准备开始最多20分钟**，须在用户单独确定额度后执行。额度含预热、不退款、不自动重试。

- 模型和服务沿用已核验的 Qwen2.5-7B-Instruct revision `a09a35458c702b33eeacc393d103063234e8bc28`、
  vLLM0.25.1、BF16、TP1、上下文4096、max_num_seqs128、max_num_batched_tokens8192、GPU利用率配置0.8、
  FCFS、chunked prefill和prefix cache；每单元确认prefix reset，重新核对软件/模型文件与实际服务配置。
- 输入固定为已有自然分布tuning清单前512行；原instruction、temperature0、max_tokens64不变。
  预热取同一清单前128行。标签只用于评价，不筛掉答错样本，不用evaluation反向调参。
- 两种执行版本：`1dd6d060` 的Core/provider/PG代码，与工作包A提交的修复代码。两者使用同一新版本
  测量/observer/runner代码；“旧执行＋测量补丁”明确登记逐文件指纹，不称为未改动的原版整体复现。
- 固定C32、核心输入128MiB、PG窗口64MiB、完整HTTP响应上限预留、producer trace开启、compact＋buffered事件、
  flush_rows64、查询期限300秒；只交叉改变L32/64及核心结果预留32/64MiB。
- 每执行版本预热128行一次，共256请求；4配置×2版本×2重复×512行，共8192请求。第一重复先旧后新，
  第二重复先新后旧；配置顺序第一轮(32,32)、(64,32)、(32,64)、(64,64)，第二轮倒序，单位为行/MiB。
- 用既有 `map_capacity.py cell` 逐单元运行，每单元全新目录与一次性预算预留；运行前写清18单元清单、
  总额及各版本源码指纹。原模型/计划身份、实际出站、producer绑定、资源或持久化检查失败即停止；
  达到请求/时间上限也停止，不补跑或在线改参。结束后关闭模型/专用gateway并核对GPU、端口与责任释放。
- 输出全部重复的JCT、offer/accepted、HTTP出站、held/HTTP峰值、质量与已采集阶段时点；512行属于短查询
  机制诊断，不套用60秒稳态资格，不登记强D0或组织算法优势。版本与配置差异分开分析。

工程来源复核：主架构 §8.7–8.8 的 `caf2b6ccdf0d6efc2c1910cbc06725a34320181a`
PostgresML `api.rs` typed embed、共享模型调用仅作公开接口参照；自有 planner/tuple binding、外部
transport 与 query cleanup 继续拥有各自职责。公司私有副本本轮未复核、未复制或上传。
窗口/推进改动是基础执行修复，不是新研究算法；相关同核心静态对照一并使用修复后的实现。

零模型验证依次覆盖 Python、PG18.3 严格构建/回归/TAP、真实 PG+受控 HTTP、共享 gateway 和
Ray CPU 阶段。真实实验入口先准备完全匹配的 L32/64×结果预算32/64MiB、可达 C32/64/128
及查询/CLIP 最小清单；不自动调用。未找到平台不能阻止独立查询接入，也不能写成已经找到强 D0。

条件性研究项：完整响应预留须有可执行的最大响应依据；批量 IPC 须先观测协议开销；Core 索引须
先 profile。方法×执行 2×2 在一个公共 reference 任务质量可评价、所需方法接口具备后选择一种
已有方法，不等待全部算子。上述条件不授权恢复旧 SAOR/HSE formal 或修改 vLLM/Ray scheduler。

> **本轮结果（2026-09-10）**：[单卡容量报告](../results/postgresql/map_capacity_20260910/README.md)保存96601次真实出站、99808次预留与全部失败。
> 五次4000行观测选出C32/L64候选；C64/128尚无同规模重复平台，强D0仍待确认。独立样本验证及服务/ACL清理已完成。
> 下方保留具体合同与先前验收历史；本轮额度已停止使用，不自动追加调用。
>
> **历史状态（2026-09-10）**：下方保存容量画像开始时的执行切片；当前实施由本文件开头独立工作包维护。
> 已有外部文本组织实验和 cache-on 双/四 endpoint 结果保留，不能移作当前 PG 路径的性能证据。
> 本轮先准备真实数据与评价，再测静态容量和有限窗口组织；多 Job 紧随其后，按实测需要复用旧策略。
> 首次尝试已使用 48/68 次请求并停止：SQuAD 小样本可用，ShareGPT 的请求意图摘要配置与输入准备未通过；
> [结果与失败审计](../results/postgresql/data_execution_pilot_20260909/README.md)保留原始原因。
> 工具修复完成本地/Linux 各 168 项相关测试、7 项真实 PG 检查和 SQuAD 新账本 10/10 次模型验收，
> 见[修复验证](../results/postgresql/data_evaluation_harness_20260910/README.md)。出站一致性与资源回收通过，
> 实现提交 `5085c6ff` 已合入 main；ShareGPT 任务质量、静态容量、组织性能及双副本验证仍待执行。
> 已完成合同见 [`completed/rc1_data_organization_rerun_20260731.md`](completed/rc1_data_organization_rerun_20260731.md)。

<a id="当前-pg-单-map-数据执行切片"></a>

## 历史 PG 单 Map 数据执行切片

### 历史工作：可复跑的单卡静态容量参照（2026-09-10）

以下是 `main@14acdc44` 两份审查后的当前工作；下方原 68 次切片和后续 10 次工具验收均已结束，
只保留历史条件与结果，不再分配额度。用户已明确授权本轮容量画像最多 **100,000 次模型请求、2 小时**。
新请求使用独立预算身份；达到目标、额度/时间上限或执行有效性失败即停止。合法完整但答案错误计入质量，
不要求扩大后的 SQuAD 每行都答对；不删除失败样本、改提示或补充额度后混入同一结果。

当前交付按依赖顺序为：独立 producer 绑定、可扩规模预算、分阶段记录、配置化 direct/PG runner、
代表性调优输入，以及使用同一服务 profile 的单卡容量画像。双副本和复杂多 Job 不是这些工作的前置项。
recorder 不拥有服务/事务/预算；runner 装配已有环境、HTTP transport、PG cluster/gateway、记录与评价；
direct 使用有界客户端和相同 HTTP transport，不使用 SessionEngine 或组织器，不命名为原生系统 baseline。

| 源码与依据 | 现状核对及本轮决定 | 必须验证 |
|---|---|---|
| `attempt_ledger.py`，`14acdc44` | v1 每次重新扫描且可追加到 64 KiB 之外；保留历史格式，创建时检查容量、追加前拒绝越界 | 超额声明提前拒绝；旧文件接近上限时失败后仍可读；原小账本兼容 |
| 新实验单元预算，SQLite 原子事务 | 工程决策：在计时前持久预留单元额度，一次性 claim 后由所属进程在内存扣减；不退款、不恢复已 claim 单元；事务不在 HTTP 发送循环中 | 100,000 次零网络扣减；跨进程互斥领取、重启/崩溃不重复授权、耗尽/过期拒绝 |
| `sem_pump.c` / `pg_semantic_runtime.c`、v6 `incremental_session.py` | 生产绑定保留；只补默认关闭的测试期 PG 行绑定观测及接纳前 task 事件，独立记录实际行 ID/stream/sequence/digest；禁止由 completion 或 attempt/offset 反推 | 逆序通过、交换序号失败、重复 payload、跨查询从零编号、缺少 producer 记录拒绝 |
| `map_query_recording.py` / Psycopg stream | 保留现有 elapsed；另记 release、首/末接收、流终止、结果持久化、stream 清理；received 与 recorded 行数分开；评价可逐行消费并单列时段 | 正常、空结果、部分错误、记录上限、评价错误与中止均有正确状态；关闭生成器后事务仍由调用方处理 |
| 请求/事件观测 | 容量模式使用有上限的后台写入与紧凑哈希事件；队列满或写盘失败不得静默丢记录；资格模式保留完整私有事件 | 无网络扰动检查及匹配模式对照；执行、消费者、评价 RSS/逻辑预留分别报告 |
| `squad_map.py` 与官方 SQuAD v1.1 dev | 保留既有短答案语义；按完整消息 token 画像生成确定性自然/分层子集，context 不跨 tuning/evaluation；不得按模型答案选样 | 原始 SHA、完整覆盖画像、原文、分组隔离、种子/样本身份、context 数及长度分布 |

已复核[知识库](../../research/knowledge_hub.md)的 Sema/服务观测及
[baseline reference §0.2](baseline_reference.md#gateway-layered-controls)的 matched direct/PG 分层比较，
沿用[主架构 §8.7–8.8](postgresql_ai_semantic_operator_architecture_20260827.md#pgml-engineering-reference)
中数据库行绑定与客户端复用的职责；未读取或分发公司材料。预算存储参考
[SQLite 原子提交](https://www.sqlite.org/atomiccommit.html)与同步持久化说明，记录消费参考
[Psycopg Cursor.stream](https://www.psycopg.org/psycopg3/docs/api/cursors.html#psycopg.Cursor.stream)。
这些是工程选型，不作为研究创新。具体服务参数、行数、单元列表与额度总和在运行前生成并保存配置，
不把资格期 `max_num_seqs=4` 当作饱和条件。先定位服务并发区间，再单独改变 PG 窗口/字节预算；
最终选点只用 tuning，evaluation 不反向用于调参。若预算不足以满足预先规定的稳定时间与重复条件，
报告候选曲线及缺项，不能把候选命名为已成立的强静态参照。

本轮运行前确定：Qwen2.5-7B-Instruct 原 revision、单 RTX 4090、TP1/BF16，vLLM 0.25.1、
FCFS、`max_model_len=4096`、`max_num_seqs=128`、`max_num_batched_tokens=8192`、
GPU memory utilization 0.8、chunked prefill 与 prefix cache 开启，使用默认编译/图执行模式并保存
实际生效配置。每个实验单元在计时前清空前缀缓存；0.25.1 的管理 API 返回空 HTTP 200，
因此还须在该次调用后的 engine 日志看到唯一 `Successfully reset prefix cache`，不能只用 HTTP 状态
推断重置成功。依据为已安装 `vllm/entrypoints/serve/dev/cache/api_router.py` 与
`vllm/v1/core/block_pool.py::reset_prefix_cache`；记录源码哈希，管理请求与模型请求分别计数。
服务在所有臂间保持同一进程及参数，首次为准备缓存管理接口而重启的实例没有模型请求。

SQuAD 全部 10,570 行先做完整消息 token 画像；种子 `20260910` 按 context 分组随机分区，
每侧分别选 5,000 行自然样本及按输入 token 三等分的均衡诊断样本。自然样本用于主扫描；
不截断、复制文本或根据答案选样。原始画像覆盖 2,067 个 context，输入 76–852 tokens，
加 64 输出预算全部符合 4096 上下文；这些是离线画像，输出工作量仍由实际 usage 观测。

预热/校验为 direct 与 PG 各两个 16 行查询，共 64 次请求。第一轮诊断固定 256 行、
PG 窗口 128、输入/结果预算各 128 MiB、PG 本地窗口 64 MiB，交错扫描 C=1/2/4/8/16/32/64/128，
共最多 4,096 次请求；这些短点不直接用于选 D0。随后按诊断范围单独扩大行数，核对至少 60 s
及相邻规模约 3% 的稳定要求，再在候选并发附近做重复、窗口与字节预算诊断。
所有后续单元须在运行前写入参数清单，并从同一 100,000 次、2 小时预算中预留；不是每轮另开额度。
最终至少五个交错配对重复；若可用不重复样本/额度不足以达到稳定时间，明确保存未资格化的候选，
不能通过串联多条短 SQL 把单查询测量改称持续单 Map，也不暗中放宽 60 s 条件。

调优中的资源拒绝（2026-09-10）：PG 本地窗口 8 MiB 在 512 行检查通过，但扩到 4,000 行后
触发行内存份额拒绝，SQL 在保存 756 行结果后失败；该批次已停止，4,000 次预留不退款。
这证明该资源配置不适合完整输入，不能登记为 D0；输入/输出语义和 60 s 条件不变。
后续是原授权内的新调优单元，恢复已通过 4,096 行的 PG 本地 64 MiB，保持 L=64、
输入预算 1 MiB、结果预算 64 MiB，比较 C=16/32；direct 使用 C=64。
每臂保留五次独立 4,000 行观测。第一组已成功的 direct 对照保留，因为它不读取 PG 本地窗口参数；
明确记录该组被资源失败中断，不能称五组均连续无中断。其余 14 个查询按新清单交错执行，
不把失败 PG 查询的部分结果纳入性能统计，也不按观测高低挑选 direct 对照。
选点后 evaluation 每臂 2,000 行只核验身份、质量和资源，不将较短查询声称为独立稳态性能重复。
计入所有失败预留后，当前全部预定单元合计最多 99,808 次，仍使用最初 100,000 次/2 小时预算。
若重复条件或选点不确定性仍不满足要求，报告候选而非补造 D0；分层样本本轮完成离线准备，
不挤占剩余额度另开分层模型实验。

### 历史合同范围：首次 68 次尝试

以下至“首次尝试后的决定”保留 2026-09-09 的当时合同和后续建议；当前授权、数据选样和运行条件
已由上方“当前工作”替代。工程依赖仍由[主设计 §9](postgresql_ai_semantic_operator_architecture_20260827.md#implementation-sequence)
维护，完成度见[状态入口](experiment_status_and_gaps.md)；旧正式矩阵没有恢复。

### 问题、顺序与停止条件

研究对象保持为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
首问是：在相同输入可见窗口、主机数据预算和服务容量下，数据组织能否改善查询完成时间，
或在相近完成时间下减少数据留存？对应研究内容一，随后扩展到研究内容二的多 Job 竞争。
这是待检验问题，不预先承诺单 Map 有吞吐增量。

| 次序 | 具体交付 | 继续条件与失败处理 |
|---|---|---|
| 真实任务准备 | 复用 SQuAD v1.1 dev 解析器和 EM/F1，生成当前 Map 的 system/user 消息、context 分组的调优/评测子集；检查重复/缺失 ID | 原始 SHA、消息和关联检查通过；先用已知答案测试 evaluator，不能当成模型质量 |
| 小样本真实检查 | 环境可用时沿用 [ShareGPT 16 行、34 请求合同](bounded_method_driver.md#sharegpt-real-data-slice)；SQuAD 另用 2 行预检＋16 行同步＋16 行增量，共最多 34 请求 | 独立请求账本；超时、身份错误、丢行、重复、资源越界即停，禁止自动重试、截断、更换模型或隐去失败；两数据集总额最多 68 请求 |
| 静态容量画像 | 同消息 direct 服务参照与 PG incremental-map；先服务并发，再 PG 窗口，再检查输入/结果预算 | 小样本通过后，先根据单次耗时另填规模、请求总额和墙钟上限才启动扫描；本轮不直接扩规模 |
| 单 Map 组织对照 | 固定行数/输入顺序、固定工作预算、同一有限窗口内长度分组 | 复用组织器，非恒定描述来自完整消息的输入 token，并记录 tokenizer/template 与估计开销；不能使用事后输出长度作在线决策 |
| 多 Job 对照 | 长短同时到达、长先短后、暂停供给后恢复；随后覆盖不同 Job 数 | 先比较固定份额与可利用空闲计算容量的简单控制，再按需求适配已有工作量公平策略；不以两 Job 外推任意规模 |

SQuAD 是有标准答案的主输入，ShareGPT 是已有的真实文本检查；若 ShareGPT 资产暂缺，
不阻塞 SQuAD 离线准备，也不下载大资产作为本轮默认动作。小样本只证明关联、质量观察及资源回收，
不足以建立强静态配置、稳定吞吐或论文性能结论。质量评价须保存每行原输出、EM/F1 和失败分母；
真实小样本结果出现质量疑问时先审查，不边改提示边汇总性能。完整 EM/F1 与标准答案规则沿用已有实现。

### 数据、消息与实际 SQL

SQuAD 原始文件必须通过既有 importer 的完整 SHA256 和 10,570 行检查；新任务身份为
`squad_v11_pg_map_v1`，不改旧 `squad_v11_dev_short_answer` 的单 user 消息含义。
按完整 context 的 SHA256 排序后，交替分配 context 组到 tuning/evaluation，组内及最终输出保留
原始数据顺序；默认每侧取 64 行，不复制或截断文本。两个子集只是本项目从官方 dev 派生的划分，
不是官方隐藏测试集。64 行用于输入准备，真实预检只消费 tuning 的前 2/16 行，evaluation 不参与调优。

system 指令固定为 `Answer the question using only the context. Return only the shortest answer span. Do not explain.`；
user 固定为 `Context:\n{context}\n\nQuestion:\n{question}\n\nAnswer:\n`。
`max_tokens=64, temperature=0`，其余参数按 PG 生效配置完整保存；模型、revision、模板、缓存和服务配置
在目标机器预检后写入仓库外运行 manifest。离线消息哈希只证明准备内容，实际出站仍须逐项核对。

把 `source_example_id,input_text` 导入独立临时表；参考答案只留在评价侧。实际测量 SQL 为
`SELECT source_example_id, ai_semantic.map(input_text, <instruction>, <options>) FROM <input_table>`。
`options` 必须采用现行 Map 合同；导入、ANALYZE 与无执行的 EXPLAIN 单列准备时间。
不在测量 SQL 中拼接字符串、筛选 split、加 LIMIT 或排序来制造多在途资格。
结果按 source ID 校验；SQL 无 ORDER BY 不承诺关系顺序，Map 的输入/结果序号通过 provider trace 核验。

PG 选择 `incremental-map`（v6），一个 Job；同步 profile 是语义参照。开始时固定窗口/HTTP 并发
为 2/2，`max_active_jobs=1`、held tasks=2；字节上限依据两项最大输入与结果预留显式列出，
PG 默认 8 MiB 仅为预检起点，不能据此声称调优完成。EXPLAIN 必须出现 SemMap 和实际窗口 2；
若退到 1，保留原因并停止多在途比较。`query-job` 的 Map 窗口仍为 1，不能替代此实验路径。

### 计时、资源与强静态配置

查询 JCT 从释放 SQL 到完整结果消费，包含执行和消费等待；导入、启动、模型加载分别记录。
主指标为 JCT、正确行数/JCT、峰值及时间序列的输入留存/已提交工作/已完成未消费结果。
PG、网关和客户端 RSS 单列；payload 预算不是 RSS 上界。记录 HTTP 真实在途、服务 running/waiting、
实际 usage、输入准备/组织/提交/完成/消费时刻及观测开销，缺失项标 `unavailable + reason`，不填零。
缓存设置保持匹配，不启用结果缓存；前缀缓存按性能服务配置固定，关闭缓存仅作后续归因实验。

容量扫描使用 tuning；服务并发候选先为 1/2/4/8，再检查 PG 窗口，避免全参数笛卡尔积。
每点先一轮诊断，稳态规模和交错重复按 runtime 及 baseline reference 确定；正式比较至少从
5 个独立配对重复起步。选择满足正确性/资源条件、达到已测峰值 97% 的最小配置；若最高档仍上升，
报告尚未达到饱和，不宣布强静态参照已建立。evaluation 只运行确定后的配置。

有限窗口组织比较同时固定可见行数、输入/结果字节和服务计算容量。输入 token 是估计单位，
不是 GPU 时间；组织批会展开成独立请求，不宣称模型内部 batching 收益。
若扩大窗口不再改善 JCT，优先检查数据留存、按序返回和消费反压；若有就绪数据且服务有余量但
提交停顿，先定位普通工程缺陷。简单组织已足够或收益落在重复波动内时，保留负结果并转多 Job，
不继续无目的调参。只有具体流水瓶颈出现后才补 Filter→Map 有界执行。

### 既有资产复用与实现记录

依据 `main@0ba12bfa` 核对 `import_squad_workload.parse_squad_dev`、
`observability.metrics.squad`、`incremental_execution.prepare_map_task/request_count_work`、
`semloom_pg/README`、`shared_credit.FairEndpointCreditCoordinator.incremental_safe`。
本轮只补数据清单/结果校验工具及 `choice_gateway_observer.record` 的单调观测时间戳，
不修改语义算子、PG、wire、Engine 或调度算法；旧请求合同保留。
parser 在 CLI 复用，评价使用现有 EM/F1；新增版本只表达 PG 双消息及分组划分，属于工程决策。
验证覆盖确定性划分、context 不跨集合、完整 Unicode 消息、ID/NULL/失败分母及内容变更可检测。
观测器回归同时验证增量事件的纳秒时间戳和已有同步/异步 POST 额度约束。

旧 shared credit、FIFO/DRR/VTC-style/SAOR 均保留；当前默认装配未注入 credit，
增量资格仅允许本地 FIFO 且关闭历史事件。多 Job 时先复用该接点，并核对计算硬上限是否先于
credit 阻止空闲容量利用；存储预算不能一并取消。公平策略所需有界候选、反馈和状态退役另验，
不改 `incremental_safe` 绕过验证。原生 Ray Data/Daft 对照随后选择一个接入，不注入项目策略。

本节依据现有知识库、baseline reference 和三份用户附件收敛近期执行，不新增研究新颖性主张。
后续组合、已有方法与图像验证继续按具体需求推进，不是本轮开始的前置条件。

### 首次尝试后的决定

2026-09-10 用户授权在恢复后的服务器验收当前工具修复，通过后提交、推送并合入 main。
验收使用从 Git 基线 `687eb8de` 建立的隔离 worktree 与可复核的待提交补丁，核对全部代码身份；
不更改服务器主工作目录。先执行相关 Linux 回归、新 CLI 的实际源文件准备与 tokenizer 检查，
再验证新 recorder 的真实 PG 成功/部分结果失败/evaluator 失败记录与连接释放。
真实模型只使用已核对 revision/文件哈希的 Qwen2.5-7B-Instruct 单 TP1 副本，沿用旧功能检查配置；
新账本 `semloom.data-evaluation.harness.20260910` 最多 10 次：SQuAD 同步预检 2 行、同步 4 行、
增量 4 行。每次请求的完整消息/生成参数必须在发送前与预期哈希清单匹配；旧 48/68 账本保持不变。
PG 单语句上限 60 s、单次模型请求 30 s、模型准备与验收总墙钟上限 15 min；超时、质量/身份错误、
丢行重复或资源未释放即停止，不自动重试或改提示。ShareGPT 仅准备与文本/上下文检查，不发送模型请求。
本次不测容量/性能或双副本，检查结果不能替代这些资格。所有原文留在仓库外，失败和部分结果保留；
结束时停止本轮 PG、gateway、模型及恢复临时 ACL。验收通过后按用户授权提交、推送并合并 main。

2026-09-10 按后续审查收敛本轮修改：先补私有输入准备、文本往返与 token ID 计数，再补执行记录与
按身份关联；本轮不使用剩余模型额度。数据源 ShareGPT 仍可继续使用，暂停的是当次“首个 human
请求的一句话意图摘要”配置。第 12 行输入 450 tokens，加 128 输出预算仍低于 4096；失败是输出
达到预算且任务偏离，不是 PG/SemLoom 静默截短输入。允许未来显式选择新任务/提示/合法预算，
不能放宽 stop 规则、暗改数据或把新配置结果并入旧实验。

本轮工具切片基于 `687eb8de`，属于测量修复而非新机制：

| 复用与新落点 | 已核对行为及决定 | 验证 |
|---|---|---|
| `baselines.common` 私有工件写入与完整值哈希 | workload 原文原样保存；公开摘要另有 schema，不可反读为 workload；旧 SQuAD schema/消息保持兼容 | 含敏感形态的合成文本、Unicode/空白逐字往返、拒绝覆盖、文件权限 |
| 新 ShareGPT 首个 human 原文入口 | 旧 `import_ai_complete_workload.first_human_prompt` 调用 normalize_text，不适合此次原样合同；保留旧 importer，新增明确身份的原文选样 | 第一 human 为空、非字符串、缺 ID/重复 ID、字节筛选、不得裁剪或规范化 |
| 统一完整 chat token 计数 | 复用 `choice_service_checks.verify_prompt_usage` 已有 return_dict=False 思路，严格提取 input_ids，验证 token 类型、完整模板与上下文 | list/dict 返回、错误结构、上下文等号与超限、原始输入不变 |
| `experiments.postgresql` 查询记录与关联 | 复用 psycopg Cursor.stream/closing；起点先持久化，完成/异常保存终点及已消费结果，随后评价；不要求模型完成有序 | 执行前失败、部分结果后异常、评价失败、取消、乱序及重复/错配拒绝 |
| 既有 `choice_gateway_observer` 与 attempt ledger | 可选完整请求值哈希白名单在 HTTP 发送前验证；实际哈希先于公开脱敏计算，旧默认入口继续有效 | 配平请求通过；改文/参数漂移/超出次数均在发送前拒绝；不取消额度记账 |

已核对主架构 §8.7–8.8：沿用 PG/语义计划/外部 transport 的职责；不涉及公司源码或其材料分发，
也不改 PG/wire/Engine。参考 [Transformers chat templates](https://huggingface.co/docs/transformers/main/en/chat_templating)
与 [psycopg stream](https://www.psycopg.org/psycopg3/docs/api/cursors.html#psycopg.Cursor.stream) 的公开接口；
私有文件和记录先后顺序是工程决策。只有使用这些工具完成实际路径验证后，才能扩大对应结论。

SemBench 明确保留为 SQuAD 执行画像之后的语义查询评测：先核验作者版本、Movie 原始查询与
evaluator，再进行小样本适配；带 LIMIT 的原始查询测完整查询完成，另立 Movie-derived 全扫描
Filter 任务测全扫描质量与执行。保留各自结果目标，不删 LIMIT 后仍称原查询。依据
[SemBench 官方入口](https://sembench.github.io/SemBench/)及 baseline reference；当前尚未构建/运行，
版本、数据和请求额度须在该阶段确定，不把它当作调度算法或要求实现其全部算子。

2026-09-09 本轮只使用一张 GPU、一个固定模型实例，属于正确性检查。两卡空闲不等于两卡已被
当前 PG 路径使用；默认装配只有一个 endpoint。后续性能实验分别保留单卡参照与双卡目标拓扑：
双卡优先采用两个同模型 TP1 副本（每副本独占一张 GPU），先按[多 endpoint 接入设计](postgresql_ai_semantic_operator_architecture_20260827.md#execution-deployment-identity)
核对实际路由、每 endpoint 请求、共享全局容量和生命周期，再纳入单 Map/多 Job 比较。
不能把单卡结果外推为双卡或分布式扩展，也不能把 TP2 与双副本混成同一扩展曲线。
这一接入是双卡实验所需的具体工程项，不恢复无关框架扩展。

近期优先继续 SQuAD 的测量修整与容量画像。已准备的 64 行前缀子集输入偏短；调优前先按完整
输入 token 分布选样、维持 context 不跨 tuning/evaluation，并把新的样本身份与额度写清。
第一次增量 SQL 的计时在检查失败前未保存，不能由事件区间补成 JCT；后续必须在验证前保存
查询起止、失败和部分结果。关联按任务序号与 payload digest 校验，不能要求完成事件按序到达。

ShareGPT 当次摘要配置维持暂停：发现任务执行代替摘要、输入指令干扰和第 12 行 `finish_reason=length`；
另有一行被临时准备脚本的证据脱敏改写，因此本轮不具备原样数据身份。后续如继续，应先将
私有 workload 准备与公开证据脱敏分开，验证源文本逐字/哈希往返；改变输入或提示时显式建立
派生任务版本。临时脚本尚不能作为可复用 runner。tokenizer 返回值须取 token ID 序列，不能
把字典字段数当 token 数；离线已纠正计数并核对本轮输入均未超上下文，不新增模型请求。
剩余 20 次额度不自动用于重试、加大输出上限或更换提示。ShareGPT 的问题不阻塞已通过的 SQuAD 路径。

## 历史设计与后续条件性扩展

以下保留 2026-07/08 的研究假设与矩阵；其中“当前”“下一步”和运行参数均按原日期解释，
不能覆盖上方当前切片或自动恢复旧实验。

整理日期：2026-07-16
对应研究内容：研究内容一
方法候选编号：A1.1-A1.6（详见 `archive/research_design_catalog.md` §3，已归档）

> **2026-07-16 方向更新**：主场景从 AI_EMBED 转向 AI_COMPLETE（生成式 LLM 推理）。具体优化方法尚未锁定——动态 batching（token-budget / length-align / prefix-aware grouping）是当前重点探索方向，但静态 batch_size 参数穷举的结果仍作为 baseline 对照保留。以下内容中的实验骨架和参数矩阵为候选方案，最终消融设计将在 vLLM baseline 建立后根据实际数据确定。详细背景见 `research/knowledge_hub.md`。

---

## 0. 前置依赖（先读这个）

**本计划中所有实验必须在 vLLM + 小 LLM baseline 建立后才能产生论文可用的最终数据：**

```
前置：vLLM + Qwen2.5-1.5B 级 LLM baseline 建立（替代手动 HTTP endpoint）
前置：COPY + deferred index 写回 baseline 建立
前置：模型 batch scaling 曲线（§4 前置实验）

当前状态：vLLM baseline、Daft 文本链路和 COPY/pgvector 工程 baseline 已建立；双 GPU
cache-on 数据组织已在 2-endpoint/4-endpoint 条件下完成复验。下方前置条件与候选矩阵是
2026-07-16 的设计快照，当前结论以文件顶部状态和 `rc1_data_organization/` 结果为准。
```

**为什么**：在 suboptimal GPU/写回 baseline 上搜出来的"最优 batch_size"会因为 GPU 端或写回端的瓶颈位置不同而偏移。论文必须用 S 级 GPU + A 级写回上的 参数组合穷举 结果。

**过渡期**：可以用当前 baseline 跑一遍 研究内容一 来验证脚本、确认趋势、调试阶段拆解——但最终数据必须来自 P0 完成后的重跑。

---

## 1. 研究问题

在"数据库触发 → 外部执行"链路中，行数据如何组织为 Arrow RecordBatch、partition 和 Ray object，才能匹配下游 AI 算子的执行特征？什么情况下需要感知 workload 类型（EMBED/FILTER/COMPLETE）来选择数据组织策略？

---

## 2. 假设（Hypotheses）

每个实验段在跑之前必须先写清楚要推翻什么。**不是盲目扫参。**

| 编号 | 假设 | 待检验 | 对应实验段 |
|---|---|---|---|
| H1.1 | 固定 batch=64 在所有 workload 和规模下已经接近最优 | 能否被推翻？| §6.1 参数组合穷举 |
| H1.2 | batch_size 的最优值与 partition_count 独立（无交互效应）| 能否被推翻？| §6.1 参数组合穷举 |
| H1.3 | 不同 workload 类型（EMBED/FILTER/COMPLETE）的最优 batch_size 相同 | 能否被推翻？| §6.2 workload 对比 |
| H1.4 | selectivity 不应影响 batch 构造策略 | 能否被推翻？| §6.3 selectivity-aware |
| H1.5 | 模型自身的 batch scaling 在 batch=64 时已达到吞吐平台期 | 能否被推翻？| §4 前置实验 |

**最可能被推翻的假设决定 研究内容一 的核心贡献**：如果 H1.3（不同 workload 的最优 batch 相同）被推翻 → 研究内容一 有独立贡献；如果 H1.3 成立但 H1.5 被推翻（模型在 >64 时继续 scaling）→ 研究内容一 的贡献移到"模型 scaling 行为驱动 batch 选择"而非"workload 感知"。

---

## 2.5 分组策略设计空间：按相似度分还是按均衡分

### 2.5.1 问题定义

Token-budget 策略确定"每个 batch 放多少 token 总量"（batch 边界），但**不决定"哪些行放入同一个 batch"**（分组策略）。分组策略的选择直接影响：
- 每个 batch 内的 prefill 时间同质性
- vLLM chunked prefill 的 prefill-decode 交错效率
- 异构 actor pool 的路由可行性
- 与 prefix-aware grouping 的兼容性

### 2.5.2 两种分组策略

| 策略 | 机制 | 示例（token budget = 4096） |
|---|---|---|
| **A: Length-Align** | 按 token 长度相似度分组，短的和短的在一起，长的和长的在一起 | Batch 1: [50, 60, 45, 55, …] × 80 行 ≈ 4000 tok；Batch 2: [3500, 4000, 3800] ≈ 11300 tok（可能超过 budget，需单独处理） |
| **B: Bin-Packing** | 混合不同长度，使每个 batch 的总 token 量尽量接近 budget | Batch 1: [50, 3500, 500] ≈ 4050 tok；Batch 2: [4000, 60] ≈ 4060 tok |

**关键区分**（来源：2026-07-20 chunked prefill 交叉分析）：
- **A 操作的是"batch 内的同质性"**——batch 之间差异大，batch 内部差异小
- **B 操作的是"batch 间的均衡性"**——batch 之间差异小，batch 内部差异大

### 2.5.3 两种策略在 vLLM Chunked Prefill 下的行为差异

vLLM chunked prefill 的调度器采用 **decode-priority** 策略（事实，来源：vLLM 官方文档 v0.4.2+）：每轮迭代优先调度 decode 请求，剩余 token 预算分配给 prefill chunk。

**方案 A（Length-Align）的行为**（推断）：
- 短 batch：所有请求 prefill 快速完成 → 全部进入 decode → decode 阶段有多请求并发
- 长 batch：所有请求 prefill 都很大 → prefill 被 chunked 分步执行 → **没有短 decode 请求可交错** → chunked prefill 的 "prefill-decode 混合" 优势减弱
- 如果短 batch 和长 batch 到达 vLLM 的时间错开，内部队列只有同类请求 → 失去混合调度的多样性

**方案 B（Bin-Packing）的行为**（推断）：
- 每个 batch 天然混合长短请求 → 提交后，短请求第一个 chunk 就完成 prefill 进入 decode，长请求继续跨 chunk prefill
- vLLM 调度器在后续 iteration 中：decode（来自短请求）+ prefill chunk（来自长请求）在同一 forward pass 中混跑
- **这正好是 chunked prefill 设计的最优场景**：compute-bound prefill 与 memory-bound decode 交错

### 2.5.4 两种策略的 Fatal Flaw

| 策略 | Fatal Flaw | 触发条件 | 验证方式 |
|---|---|---|---|
| A: Length-Align | 数据单峰分布 → 退化为随机分组 | 数据集中 > 80% 行集中在同一 token 长度区间 | 实验前画 token 长度分布直方图，确认多峰或长尾 |
| B: Bin-Packing | 极端 outlier 稀释优势 | 存在单行 token 量 > budget → 独占整个 batch，其他行与 length-align 无异 | 检查 P99/P50 token 比；如果 max > 2× budget，bin-packing 退化为 "outlier 独占 + 其余正常打包" |

### 2.5.5 与异构 Actor Pool 和 Prefix-Aware Grouping 的交互

| 交互对 | 兼容性 | 说明 |
|---|---|---|
| Length-Align × 异构 Actor Pool | ✅ **天然兼容** | 短 batch → 普通 actor，长 batch → 高容量 actor，路由清晰 |
| Bin-Packing × 异构 Actor Pool | ❌ 冲突 | 所有 batch 特征相同，无法按特征分池——分池路由失去意义 |
| Length-Align × Prefix-Aware | ✅ **可叠加** | 先按前缀分组（最大化 APC 命中率），再按长度子分组 |
| Bin-Packing × Prefix-Aware | ⚠️ 冲突 | 为均衡 token 量可能拆散同前缀的行 → 降低 APC 命中率 |

### 2.5.6 实验策略建议

**主推方案 B（Bin-Packing）作为 RC1 主策略**（推断，待实验验证）：
- 与 vLLM chunked prefill 的 prefill-decode 交错机制天然协同
- 优化目标清晰："每个 batch 的 GPU 计算量均衡"
- 文献依据：Orca 的 selective batching、vLLM 的 continuous batching 本质上都在混合不同长度的请求

**方案 A（Length-Align）保留为消融对比**：
- 在异构 actor pool 场景下（§5.3 或后续实验）：length-align + 分池路由 vs bin-packing + 统一路由
- 在 prefix-aware 联合实验中：length-align + prefix-aware 两级分组 vs bin-packing-only

**新增假设**：

| 编号 | 假设 | 待检验 | 对应实验段 |
|---|---|---|---|
| H1.6 | Bin-packing 分组在统一 actor pool + vLLM chunked prefill 下的端到端吞吐优于 length-align 分组 | 能否被推翻？| §6.1 扩展 |

### 2.5.7 语义安全边界：行内 prompt 不可拆分

**红线**（事实，来源：2026-07-20 vLLM chunked prefill deep-research 验证）：
> 将一份逻辑完整的 prompt 手动拆分成多条独立 vLLM 请求 → KV cache 隔离、上下文断裂、输出语义错误。vLLM 的 `--enable-chunked-prefill` 是引擎内部 token 级分片（数学等价），与手动 request 级拆分是完全不同的机制。

**对本实验的约束**（推断）：
- **每行数据 = 一个独立完整的 vLLM 请求**。Token-budget 策略决定的是 "多少行合并为一个 batch"，不是 "如何切割一行内的 prompt 文本"
- 如果某单行的 prompt token 量超过模型 context window（如 Qwen2.5-1.5B 的 32K），**禁止在上游 Daft/Ray 层自动切分该行的 prompt 内容为多条请求**
- 超长单行的处理方式：① 在数据准备阶段截断（truncate）到 context window 内；② 或将超长行标记为单独处理（独占一个 batch，不做拆分）；③ 或从数据集中排除

**实验前检查清单**（添加到 §12）：
- [ ] 确认数据集中每行的 prompt 是自包含的（self-contained），行间无语义依赖
- [ ] 确认所有单行的 token 量 < 模型 context window（32K for Qwen2.5-1.5B）
- [ ] 如果存在超长行：明确处理策略（truncate / 独占 batch / 排除），并记录在实验报告中

### 2.5.8 每行 token 来源与元数据记录规范

`prompt_tokens` 是每行 prompt 在目标服务模型 tokenizer 下的输入 token 数。它不是字符数、词数，也不是数据集 trace 中原始 request token 字段的无条件复用值，而是为了让上游调度策略感知模型侧计算量而附加到行上的执行元数据。

获取方式：

```python
tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, local_files_only=True)
prompt_tokens = len(tokenizer.encode(prompt, add_special_tokens=False))
```

要求：

- 使用与 vLLM 服务端模型一致的 tokenizer，例如本地 `models/Qwen2.5-1.5B-Instruct`。如果 tokenizer 与服务模型不一致，该实验不能用于 token-aware 策略结论。
- 在 workload 导入或执行前预先计算 `prompt_tokens`，写入 PostgreSQL `documents.prompt_tokens`，并随 Daft/Arrow table 一起进入 `DataOrganizer`。
- `token_budget` 组批使用的单行估计代价为 `prompt_tokens + completion_max_tokens`。其中 `completion_max_tokens` 是本次实验请求的生成上限；如果后续改用历史 P95 输出长度，需要在实验报告和 CSV 字段中显式记录。
- vLLM 返回或 Prometheus 暴露的 prompt token 指标只作为运行后校验信号，不作为执行前分组的唯一来源，因为分组决策必须在请求提交前完成。
- 如果数据集自带 request token 字段，只能作为 trace 元数据或 fallback；正式策略实验优先使用目标模型 tokenizer 重新计算后的 `prompt_tokens`。

必须记录到实验材料的字段：

| 字段 | 说明 |
|---|---|
| `tokenizer_path` / `tokenizer_name` | 计算 `prompt_tokens` 使用的 tokenizer |
| `tokenizer_add_special_tokens` | 是否在计数时加入 special tokens；当前默认 `false` |
| `prompt_tokens_min/p50/p95/p99/max` | 输入 token 分布，用于证明 fixed rows 是否是弱代理 |
| `completion_max_tokens` | 估计单行总代价时加入的输出 token 上限 |
| `max_model_len` | 过滤或标注超长行的上下文窗口约束 |
| `token_count_source` | `model_tokenizer` / `trace_metadata` / `char_proxy`，正式实验应为 `model_tokenizer` |
| `batch_tokens_p50/p95/p99/max` | 分组后每个 Ray/vLLM 请求的 token 形状 |

文档落点规则：

- token 获取、字段定义、分组公式、超长行处理写在本文件。
- 历史代码模块、抽象接口与 CSV 字段映射见
  `reference/strategy_design_implementation_reference.md`；当前接口以 `../../code/README.md` 为准。
- 具体实验命令、CSV 路径、结果解释写在对应 `experiments/results/.../README.md`。

---

## 3. 变量

| 变量 | 含义 | 取值范围 |
|---|---|---|
| `batch_size` | 每次提交到 GPU 的行数 | {8, 16, 32, 64, 128, 256, 512} |
| `partition_count` | Ray task/actor 数 | {1, 2, 4, 8} |
| `object_merge` | Arrow RecordBatch 的合并策略 | {none, coalesce_input, coalesce_output} |
| `workload_type` | AI 算子类型 | {EMBED (真实), FILTER (模拟), COMPLETE (模拟)} |
| `selectivity` (仅 FILTER) | 语义过滤的选择率 | {0.1, 0.3, 0.5, 0.8} |
| `text_length` (仅 COMPLETE) | 平均 token 数 | {short <128, medium 128-512, long >512} |
| `grouping_strategy` | 如何选择哪些行放入同一 batch | {random (baseline), length_align, bin_packing} |
| `token_budget` (仅 grouping ≠ random) | 每个 batch 的目标 token 总量上限 | {1024, 2048, 4096, 8192}（根据模型 context window 调整）|

**关于 FILTER/COMPLETE 的诚实标注**（参照 Orca 合成权重的做法）：

| Workload | 当前状态 | 论文中标注 |
|---|---|---|
| EMBED | ✅ 真实 GPU embedding（all-MiniLM-L6-v2, 384d）| 真实 workload |
| FILTER | ⚠️ 模拟——用 embedding 相似度 + 阈值模拟布尔输出，selectivity 人工控制 | "simulated AI_FILTER with known selectivity" |
| COMPLETE | ⚠️ 模拟——用随机长度处理延迟模拟 token generation | "simulated AI_COMPLETE with controlled token length distribution" |

---

## 4. 前置实验：模型 batch scaling 曲线（P0c，必须在 研究内容一 所有实验之前跑）

### 4.0 研究问题

在讨论"batch_size 如何影响端到端延迟"之前，必须先搞清楚：**GPU 模型自身的吞吐是怎么随 batch_size 变化的？** 如果模型在 batch=32 就饱和了，那讨论 batch=256 毫无意义。

### 4.0 假设

H1.5：模型自身的 batch scaling 在 batch=64 时已达到吞吐平台期。

### 4.0 方法

```
脱离数据库/Ray 链路，直接用模型推理：
  model = SentenceTransformer("all-MiniLM-L6-v2")
  texts = [random text of length ~200 chars] × N

  batch_size ∈ {1, 2, 4, 8, 16, 32, 64, 128, 256, 512}
  N = max(batch_size) × 10（确保有足够多 batch）
  
  每 batch_size: 跑 20 个 batch，忽略前 5 个（warm-up），取后 15 个的中位数
  指标: T_per_batch, T_per_row, rows/s

预计耗时: ~30 分钟（不需要数据库、不需要 Ray）
```

### 4.0 输出

- 一条 `batch_size → rows/s` 曲线（X=batch_size, Y=吞吐）
- 标注吞吐平台期的起始 batch_size
- 如果平台期在 batch=32：研究内容一 讨论区间应聚焦 8-128，256/512 只有验证价值
- 如果平台期在 batch=256：研究内容一 有更大 tuning space，batch 选择对 GPU 利用率影响更大

**这条曲线是 研究内容一 所有讨论的前提。画好了才能解释后续所有实验里 batch_size 的影响。**

---

## 5. Baseline 对照

| 编号 | 描述 | 级别 | 来源 |
|---|---|---|---|
| **A1.1** | 固定策略 Baseline（coalesced vs fine 互相对照）| 合理默认 | 已有 |
| **D1** | Fixed Partition + Fixed Batch（Daft/Spark 默认，不做 workload 感知）| B 级 | Daft 文档 + Spark SQL Tuning |

---

## 6. 实验矩阵

### 6.0 7B 双 GPU 复验隔离规则（2026-07-28）

旧双卡配置同时启用了 accelerated arrival replay、50ms flush 和 token-budget。
现场 1024 行 gate 的 packing budget utilization 仅约 13.5%，平均每批约 3 行：
大多数 batch 在达到 token budget 前已被 timeout 关闭。该配置只能研究在线
arrival/flush，不足以判断 token-budget 或 length-align 本身是否有效。

复验前先以 request-level submission 标定 per-endpoint active work 饱和区。
随后使用 `source_order=doc_id`、关闭 arrival replay，令完整 organizer 输入
可见，固定 active work、较高的 static per-endpoint K 和 endpoint routing，
仅改变 token budget：

```text
sequential_token_budget ∈ {8192, 16384, 24576, 32768, 49152, 65536}
```

这条曲线要验证的不是“更大的 budget 能装更多行”这一恒真命题，而是吞吐是否
存在甜点。budget 太小时，HTTP/Ray 调用数和固定开销增加，单次提交给 vLLM 的
可选请求不足；budget 太大时，兼容 HTTP 的列表响应形成更粗的完成屏障，
短请求要等同 submission 中最长请求返回，补位变慢，P99、completion span 和
job 间干扰可能上升。因此预期 `tokens/s` 随 budget 先升后平台或下降，而不是
单调上升；如果一直单调上升到 65536，说明搜索上界还没覆盖平台，不能声称
任何较小预算最优。

容量曲线必须先检查：

- `packing_budget_utilization_mean`、`organization_batch_rows_mean`、
  `organization_row_cap_hit_ratio`、submission batch 数和 HTTP 调用数；
- observed tokens/s、rows/s、request/service P95/P99；
- submission completion span、补位间隔和 credit idle ratio；
- 每 endpoint running/waiting、GPU/MFU 与端点流量分布。

固定 work 的曲线确定 `BEST_TESTED_TOKEN_BUDGET` 后，第二轮才在同一预算和
active work 上比较
`fixed_rows_8`、sequential、row-cap-aware 和 length-align，避免把预算大小与
membership 算法混成一个因素。如果利用率低于 50%，不进入策略胜负解释，先
诊断 256 row cap、oversized rows 或 organizer visibility。只有离线组织阶段出现
可辨认的 batch-shape 差异后，才把候选带回 arrival replay 验证在线泛化；
不能同时调 flush timeout 来“帮助”某个组织策略。

### 6.0.1 动态 token budget 的晋级实验

动态 budget 不是直接把 vLLM `running` 或 GPU utilization 映射成一个更大的
数。上游组织预算、上游 active-work admission 和 vLLM 内部
`max_num_batched_tokens` 是三个不同控制量。第一版动态组织只允许使用：

```text
pending predicted work
arrival-rate EWMA
endpoint completion/service-rate EWMA
oldest-request slack
```

控制动作是在已经由静态容量曲线标定的 `{B_min, B_mid, B_max}` 中选择下一批
的目标预算，并受 hard max-wait 约束。正式挑战 workload 分三段
`short-heavy → long-heavy → mixed/burst`。比较：

1. 每段分别使用其静态最优预算（oracle 上界，不可在线实现）；
2. 全程使用训练 workload 的最佳单一静态预算（强 baseline）；
3. 动态预算；
4. 动态预算去掉 service-rate feedback 的消融。

动态策略只有在 held-out 顺序或到达率下接近 oracle、显著优于最佳单一静态值，
并且 P99/饥饿 guardrail 不退化时才晋级。稳态单一 workload 下收敛到静态值是
合理结果，不应为制造收益而持续振荡。

### 6.1 参数组合穷举：建立静态最优 baseline

**假设**：H1.1（固定 batch=64 已最优）、H1.2（batch_size 和 partition_count 独立）。

```
batch_size      ∈ {8, 16, 32, 64, 128, 256, 512}
partition_count ∈ {1, 2, 4, 8}
object_merge    ∈ {coalesce_output}  # 当前已知最优
──────────────────────────────────────────
总组合: 7 × 4 = 28
每组合: 3 次重复（Ray 重启、warm-up 1 次不计入）
总运行: 84 次

固定条件（P0 完成后）:
  - GPU: vLLM / Ray Serve（S 级 baseline）
  - 写回: COPY + unlogged staging + deferred HNSW index（A 级 baseline）
  - 数据规模: 16384 行
  - Workload: AI_EMBED（真实）
```

**输出**：联合最优的 `(batch_size*, partition_count*)` = 研究内容一 的 A 级 baseline。同时检验 H1.2（是否存在交互效应——某些 batch_size 在特定 partition_count 下表现异常）。

### 6.2 Workload 对比

**假设**：H1.3（不同 workload 的最优 batch_size 相同）。

| Workload | batch_size | partition_count | 数据规模 | 标注 |
|---|---|---|---|---|
| EMBED | 参数组合穷举 最优 × 3 | 参数组合穷举 最优 × 3 | 1024, 4096, 16384 | ✅ 真实 |
| FILTER | selectivity ∈ {0.1, 0.5} × 参数组合穷举 | 参数组合穷举 最优 | 4096, 16384 | ⚠️ 模拟 |
| COMPLETE | text_length ∈ {short, long} × 参数组合穷举 | 参数组合穷举 最优 | 1024, 4096 | ⚠️ 模拟 |

每种组合 3 次重复，Ray 重启，warm-up 1 次不计入。

**如果 H1.3 被推翻**（不同 workload 的最优 batch 不同）→ 研究内容一 核心发现成立。
**如果 H1.3 成立**（所有 workload 下 batch=64 都最优）→ 研究内容一 的贡献变为"验证了固定策略的鲁棒性"，workload-aware 的增量价值需重新评估。

### 6.3 Selectivity-Aware 策略（当 FILTER 场景可用时）

**假设**：H1.4（selectivity 不应影响 batch 构造策略）。

| selectivity | 假设最优策略 | 为什么 |
|---|---|---|
| < 0.2 | 小 batch (32)、多 partition | 大部分行被过滤，小 batch 减少 GPU 浪费 |
| > 0.5 | 大 batch (128)、单 partition | 大部分行都过，大 batch 省 invocation 开销 |

**对照**：同 selectivity 下，固定 batch=64 作为基线。

---

## 7. 指标

| 指标 | 测量方法 | 论文参照 |
|---|---|---|
| **端到端延迟** | `time.perf_counter()` 从 DB fetch 开始到 writeback 结束 | vLLM/Orca 的端到端 serving latency |
| **阶段拆解** | DB fetch → Arrow build → GPU request wall → fan-in → writeback | TurboVecDB 的 HNSW 层级拆解思路 |
| **吞吐 (rows/s)** | `total_rows / T_e2e` | vLLM 的 requests/second |
| **Ray object 数** | `ray.objects()` 计数 | 诊断指标 |
| **GPU 利用率** (如有) | vLLM 可采集；手动 HTTP endpoint 无此指标 | vLLM 的 GPU utilization |

**关键**：不报"coalesced 比 fine 快 13.4×"这样的单点数字，而是画 `batch_size → T_e2e` 全景曲线，让 reviewer 看到全工作点。

---

## 8. 消融设计

对 A1.2（Workload-Aware Partition）的消融：

| 消融项 | 做法 | 要检验什么 |
|---|---|---|
| 规则表 vs 固定策略 | A1.2 规则表 vs A1.1 参数组合穷举 最优固定值 | 规则表在 workload 变化时是否优于固定策略？ |
| 规则表 vs 随机 | A1.2 规则表 vs 随机选配置（5 次取中位数）| 排除"随便选也能中"——规则表必须好于随机 |

---

## 9. 结果展示图

| 图号 | 内容 | 类型 | 论文参照 |
|---|---|---|---|
| Fig_RC1_0 | 模型 batch scaling 曲线：batch_size → rows/s | 折线图（前置实验）| — |
| Fig_RC1_1 | batch_size → T_e2e 曲线（不同 partition_count 各一条线）| 折线图 | vLLM 的吞吐-延迟曲线 |
| Fig_RC1_2 | 三 workload 的阶段拆解并排柱状图 | 堆叠柱状 | TurboVecDB 的层级拆解 |
| Fig_RC1_3 | selectivity → T_e2e（固定策略 vs workload-aware）| 折线图 | Orca 的多模型尺度图 |

---

## 10. 统计规范（参照 vLLM/Orca 标准）

| 要求 | 做法 |
|---|---|
| **重复次数** | 每组配置 3 次（参数组合穷举）。核心发现（被推翻的假设）额外补到 5 次 |
| **集中趋势** | 取**中位数**（不取平均值——系统实验的临时 outlier 会拉偏平均值）|
| **离散度** | 报告 IQR（四分位距），5 次以上报告标准差 |
| **Ray 状态重置** | 每次重复之间 `ray stop` → `ray start`，避免内存缓存/对象复用 |
| **数据库状态** | 每次重复之间 TRUNCATE 目标表，确保写入量一致 |
| **Warm-up** | 每组配置先跑 1 次 warm-up（不计入结果），后面 N 次计入 |
| **随机种子** | 数据生成固定 seed（`random.seed(42)`），确保不同配置跑同一批数据 |

---

## 11. "When does it NOT help?" 边界验证

每个边界条件必须对应一个**可跑的实验点**，不是空洞的自省。

| 边界条件 | 验证实验 | 期望结果 |
|---|---|---|
| workload 特征在运行前已知且不变 | 固定 1 种 workload，比较 "规则表选择" vs "固定 batch=64" | 差异 < 5% → 边界成立 |
| 数据量 < 500 行 | 256 行规模下，比较 batch_size ∈ {8, 32, 64, 256} | 各配置 T_e2e 差异 < 10% → 边界成立 |
| GPU 模型对所有 batch_size 吞吐几乎恒定 | 看 §4 前置实验的 batch scaling 曲线 | 如果平台期从 batch=8 开始 → batch_size 选择不重要 |
| 数据集中存在超过 context window 的单行 | 检查 max(token_count) 是否 > 模型 context window（32K for Qwen2.5-1.5B）| 如有 → 预处理截断或排除；**禁止在 Ray 层自动拆分单行内容为多条请求**（会导致语义断裂，参见 §2.5.7） |
| 分组策略与 chunked prefill 的交互 | length_align vs bin_packing 在 `--enable-chunked-prefill` on/off 下的对比（仅 V0；V1 强制开启）| bin_packing 在 chunked prefill on 时优势更大（prefill-decode 天然混合） |

---

## 12. 运行检查清单

- [ ] P0c: 模型 batch scaling 曲线（§4）完成
- [ ] P0a: vLLM/Ray Serve 接入完成
- [ ] P0b: COPY + deferred index 写回 baseline 确认
- [ ] P1: 参数组合穷举（batch_size × partition_count）在 P0 完成后重跑，确立 `(batch_size*, partition_count*)`
- [ ] P1: 三 workload（EMBED + FILTER/sim + COMPLETE/sim）完成
- [ ] P1: 阶段拆解数据可以画 Fig_RC1_2
- [ ] P2: selectivity-aware 策略对照（当 FILTER workload 可用时）
- [ ] §11 的边界验证实验点完成
- [ ] 所有结果 CSV 保存在 `experiments/results/rc1/`
- [ ] 每个图标注：数据来源、排除 warm-up、硬件/模型/数据库版本、重复次数、取中位数还是平均值
- [ ] **语义安全检查**（来自 §2.5.7）：每行 prompt 自包含、行间无语义依赖
- [ ] **语义安全检查**：max(token_count) < 模型 context window，超长行的处理策略已明确
- [ ] **分组策略检查**：数据集的 token 长度分布直方图已画出，确认分布特征（多峰/单峰/长尾）→ 据此决定 length_align 是否有区分度
- [ ] **Chunked Prefill 状态**：确认实验使用的 vLLM 版本及 chunked prefill 是否开启，记录在 CSV 的 `server_version` 字段中
