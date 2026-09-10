# 研究内容一：动态数据组织与批处理构造策略实验计划

> **当前状态（2026-09-10）**：近期执行合同由下方“当前 PG 单 Map 数据执行切片”维护。
> 已有外部文本组织实验和 cache-on 双/四 endpoint 结果保留，不能移作当前 PG 路径的性能证据。
> 本轮先准备真实数据与评价，再测静态容量和有限窗口组织；多 Job 紧随其后，按实测需要复用旧策略。
> 首次尝试已使用 48/68 次请求并停止：SQuAD 小样本可用，ShareGPT 的请求意图摘要配置与输入准备未通过；
> [结果与失败审计](../results/postgresql/data_execution_pilot_20260909/README.md)保留原始原因。
> 工具修复完成本地/Linux 各 168 项相关测试、7 项真实 PG 检查和 SQuAD 新账本 10/10 次模型验收，
> 见[修复验证](../results/postgresql/data_evaluation_harness_20260910/README.md)。出站一致性与资源回收通过，
> ShareGPT 任务质量、静态容量、组织性能及双副本验证仍待执行。
> 已完成合同见 [`completed/rc1_data_organization_rerun_20260731.md`](completed/rc1_data_organization_rerun_20260731.md)。

## 当前 PG 单 Map 数据执行切片

本节是当前数据执行实验的具体合同；工程依赖仍由[主设计 §9](postgresql_ai_semantic_operator_architecture_20260827.md#implementation-sequence)
维护，完成度见[状态入口](experiment_status_and_gaps.md)。本轮用户授权调整计划并开始小规模尝试，
先执行离线准备；真实调用须绑定实际环境和下述额度，旧正式矩阵没有因此恢复。

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
