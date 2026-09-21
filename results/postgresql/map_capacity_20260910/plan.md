> 本文件是该次验证的运行前声明/实施规格原文，2026-09-21 从当时的主题计划（`docs/plans/data_organization_batching.md`）按“执行说明与测试材料放在一起”的原则移入。其中请求额度、停止条件和“本轮/下一步”均为当时记录；未使用的旧额度不构成今天继续运行的授权。

> **本轮结果（2026-09-10）**：[单卡容量报告](../../results/postgresql/map_capacity_20260910/README.md)保存96601次真实出站、99808次预留与全部失败。
> 五次4000行观测选出C32/L64候选；C64/128尚无同规模重复平台，强D0仍待确认。独立样本验证及服务/ACL清理已完成。
> 下方保留具体合同与先前验收历史；本轮额度已停止使用，不自动追加调用。
>
> **历史状态（2026-09-10）**：下方保存容量画像开始时的执行切片；当前实施由本文件开头独立工作包维护。
> 已有外部文本组织实验和 cache-on 双/四 endpoint 结果保留，不能移作当前 PG 路径的性能证据。
> 本轮先准备真实数据与评价，再测静态容量和有限窗口组织；多 Job 紧随其后，按实测需要复用旧策略。
> 首次尝试已使用 48/68 次请求并停止：SQuAD 小样本可用，ShareGPT 的请求意图摘要配置与输入准备未通过；
> [结果与失败审计](../../results/postgresql/data_execution_pilot_20260909/README.md)保留原始原因。
> 工具修复完成本地/Linux 各 168 项相关测试、7 项真实 PG 检查和 SQuAD 新账本 10/10 次模型验收，
> 见[修复验证](../../results/postgresql/data_evaluation_harness_20260910/README.md)。出站一致性与资源回收通过，
> 实现提交 `5085c6ff` 已合入 main；ShareGPT 任务质量、静态容量、组织性能及双副本验证仍待执行。
> 已完成合同见 [`completed/rc1_data_organization_rerun_20260731.md`](completed/rc1_data_organization_rerun_20260731.md)。

---

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

已复核[知识库](../../docs/research/knowledge_hub.md)的 Sema/服务观测及
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
