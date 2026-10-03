# 数据库 AI 算子相关文献清单

更新日期：2026-10-03；历史选目与精读状态沿各节原日期解释。

权威 Top 15：`top15_ranked_papers.md`

PDF 索引：`reference/REFERENCE_INDEX.md`
泛读笔记：`reading_notes/`

精读笔记：`精读文献笔记/`

## 一、开题 Top 15

本轮按照“正式题录和轨道优先”重排，15/15 均为 CCF-A 正式 research paper。

| 类别 | 论文 | 出处 | 直接作用 |
|---|---|---|---|
| AI 算子/数据库 | LOTUS | PVLDB 2025 | semantic operator、准确率约束与调用优化 |
| AI 算子/数据库 | Galois | SIGMOD 2025 | SQL over LLM 的逻辑/物理算子 |
| AI 算子/数据库 | GaussML | ICDE 2024 | 原生 AI/ML 算子与 ML-aware cost |
| LLM serving | vLLM | SOSP 2023 | capacity ceiling 与 KV cache 基础 |
| LLM serving | Orca | OSDI 2022 | iteration-level continuous batching |
| LLM serving | Sarathi-Serve | OSDI 2024 | token budget 与 prefill/decode 干扰 |
| LLM serving | SGLang | NeurIPS 2024 | prefix/cache-aware 程序执行 |
| 公平调度 | VTC | OSDI 2024 | token-cost service counter |
| 动态调度 | Llumnix | OSDI 2024 | 虚拟 usage、跨实例纠偏 |
| LLM serving | DistServe | OSDI 2024 | goodput、阶段分离与 capacity planning |
| 分布式执行 | Ray | OSDI 2018 | actor/task/object store 架构载体 |
| 代价估计 | How Good Are Learned Cost Models, Really? | SIGMOD 2025 | plan-selection/ranking 评价 |
| 代价估计 | GRACEFUL | ICDE 2025 | UDF 服务成本与放置 |
| 代价估计 | COSTREAM | ICDE 2024 | operator placement、跨环境泛化 |
| 代价优化 | Abacus | PVLDB 2026 | semantic operator 多目标 Pareto 优化 |

### 当前重点精读主线

以下八篇已经完成全文精读和论文原图核对，正式写作时优先用于建立研究问题、说明已有方法并收窄本课题要继续研究的部分。它们的轨道不同，重要性来自与课题的直接关系和精读深度，不因此改变正式题录等级。

| 论文 | 正式状态 | 在开题报告中的主要作用 |
|---|---|---|
| LOTUS | PVLDB 2025 | 语义算子、质量要求和声明式优化 |
| Cortex AISQL | SIGMOD Companion 2026 | 生产 AI SQL、AI 代价参与计划选择、谓词与语义连接改写 |
| Optimizing LLM Queries in Relational Data Analytics Workloads | MLSys 2025 | 利用行、字段和关系统计重排请求，提高前缀缓存复用 |
| Ray | OSDI 2018 | 动态任务图、任务与有状态执行单元的运行基础 |
| Ray Data Streaming Batch | arXiv:2501.12407v5 | 异构流水线的动态分区、内存控制和资源调度 |
| AYO | ASPLOS 2025 | 保留应用任务、阶段依赖和数据流图信息，进行跨模块流水执行与批处理 |
| VTC | OSDI 2024 | 不依赖输出长度预测的在线服务量记账与公平调度 |
| BlendServe | ASPLOS 2026 | 离线请求重排、资源需求均衡与前缀局部性的共同考虑 |

## 二、核心补充文献

### 2.1 数据库 AI 算子与 benchmark

| 文献 | 题录状态 | 项目角色 |
|---|---|---|
| Palimpzest | CIDR 2025，非 CCF-A | 声明式计划搜索与 time/cost/quality profile；可部署系统 baseline |
| Sema | VLDB 2026 Research Track 已录用；本地全文为 arXiv:2603.11622v1 | DuckDB 原生 semantic operator、expression optimization 与运行时 AQE；正式卷期页码待发布 |
| SemBench | PVLDB 2026 正式 benchmark paper | 55 queries、多模态、质量/时间/成本/内存统一评价 |
| Database Perspective on LLM Inference Systems | PVLDB 2025 Tutorial | 推理系统地图与代价估计 open problem |
| Cortex AISQL | 按实际 Companion/工业轨道引用 | AI SQL 工业需求证据；不写成 CCF-A full paper |
| IMLane: Composable Framework for Efficient AI Function Execution in Database Engine | PVLDB 19(12): 4223–4236, 2026；DOI 10.14778/3827998.3828028 | DBEnd/ArrowLane bridge、process-level executor、database execution batch、异步提交和 Lane/resource scheduler 的直接 baseline；作者 artifact 已公开 |
| KEN: An Execution Engine for Unstructured Database Systems | PVLDB 19(5): 902–916, 2026；DOI 10.14778/3796195.3796204 | 核心补充、待全文精读。题录与摘要已核验，作者 artifact 未核验；关注模型级联随查询负载变化的执行、GPU 放置及调用调度，暂不登记为可运行 baseline |
| NeurDB | CIDR 2025，非 CCF-A | AI-native database vision 与边界对照 |
| LLM for Data Management | PVLDB 2024 | DB/LLM 研究版图 |
| Smart、SmartLite、LEADS、InferDB | 正式数据库论文 | 近数据库推理、模型选择和动态执行对照 |
| Kalypso: Relational LLM Serving | arXiv:2607.23815，2026；未标注正式 venue | 直接连接 semantic query plan 与 request-centric LLM serving，研究跨算子流水、KV prefix 生命周期和 memory-aware admission；最接近本课题接口，迫使增量收窄到 PostgreSQL query lifecycle、多 Job、多 endpoint 与不修改 vLLM 的外部控制 |
| Making Prompts First-Class Citizens for Adaptive LLM Pipelines（SPEAR） | CIDR 2026；vision/initial design 与初步实验，不算 CCF-A full research | [官方论文](https://www.vldb.org/cidrdb/papers/2026/p26-cetintemel.pdf) §2–4：版本化 prompt views/refinement，R3 评估 evidence-first 调整；缓存与 fusion 等仍多为设计机会。约束“多表示＋质量检查”的新颖性主张；后续新增的[精读笔记](精读文献笔记/spear_cidr2026/spear_cidr2026.md)与此前专项章节核验分别保留 |

2026-09-03 的[前缀/表示专项审查](semantic_prefix_reuse_design_audit_20260903.md)进一步收窄上述比较：
Kalypso 的默认 virtual pinning 不要求 explicit pin API；MLSys 2025 已含多次 LLM 查询、逐行字段序与
准确率评估；KVFlow、DLPM/D²LPM 与 SPEAR 也覆盖部分组合机制。不能仅凭“PG、流水、多表示、
质量记录、不改引擎”的名称组合推断新颖性。此增补不调整 Top 15 或既定开题题录。

KEN 的作者为 Ferdi Kossmann、Ziniu Wu、Alex Turk、Nesime Tatbul、Lei Cao 和 Samuel Madden。
题录及摘要来源为[作者单位记录](https://experts.arizona.edu/en/publications/ken-an-execution-engine-for-unstructured-database-systems/)，
核验日期为 2026-09-10。摘要描述模型级联的执行与资源代价；具体算法、实验条件和代码可运行性
仍需全文与 artifact 核查。本次只加入待精读清单，不创建完成式笔记或调整 Top 15。

IMLane artifact核查（来源：公开源码，2026-09-10）：[作者仓库](https://github.com/IM-DM4AI/IMLane0)
固定`072f8257db80569e42c9f5727d839587df529652`；README、MIT LICENSE、安装脚本和CMake已读取。
仓库提供DuckDB/OceanBase接入示例，未提供PG适配。安装脚本固定Arrow16.1.0rc1和Python3.8轮子等
依赖，与当前Python3.12/Arrow24驱动不同；需独立环境与编译验证，尚未安装或运行，不能登记为就绪baseline。
该结论只描述工程可用性，不替代论文方法比较，也不将数据库batch执行重新描述为研究空白。

### 2.2 LLM 公平调度与程序级执行

| 文献 | 题录状态 | 项目角色 |
|---|---|---|
| FairServe | arXiv 2024 | weighted service、interaction-aware throttling |
| DLPM/D2LPM | arXiv 2025 | deficit fairness + prefix locality |
| Agentix（arXiv v1 名称 Autellix） | NSDI 2026，2443–2459 | program/job-level attained service |
| Chiron | arXiv 2025 | 分层 backpressure 与 autoscaling |
| Clipper | NSDI 2017 | AIMD batching 历史来源 |
| Splitwise | ISCA 2024 | prefill/decode 分池 |
| μ-Serve | USENIX ATC 2024 | GPU frequency scaling + model multiplexing；作为功耗、能耗与 SLO attainment 参考，不归入输出长度代价估计 |
| Clockwork | OSDI 2020 | predictable serving |
| [Preble: Efficient Distributed Prompt Scheduling for LLM Serving](https://proceedings.iclr.cc/paper_files/paper/2025/hash/5bc342f48de8264779952fac378f96dc-Abstract-Conference.html) | ICLR 2025 | 缓存与负载联合实例选择；完整机制还涉及引擎调度及缓存事件 |
| [BatchLLM: Optimizing Large Batched LLM Inference with Global Prefix Sharing and Throughput-oriented Token Batching](https://proceedings.mlsys.org/paper_files/paper/2026/hash/5b7ae1758452854dee4e962207d38304-Abstract-Conference.html) | MLSys 2026 | 全局前缀组与引擎 token 调度；共享状态寿命和客户端完成分别核对 |
| [CONCUR: High-Throughput Agentic Batch Inference of LLM via Congestion-Based Concurrency Control](https://proceedings.mlr.press/v306/chen26fs.html) | ICML 2026，PMLR 306:17881–17891 | KV 占用/命中反馈控制 agent 后续生成；不能直接搬用为单轮 SQL 或远端取消机制 |
| SABER、BucketServe、Scorpio、ProServe | arXiv | 候选 admission/length/SLO/priority 机制 |
| Ray Data Streaming Batch | arXiv 2025 | 数据引擎 streaming batch 模型 |

### 2.3 代价估计扩展

| 文献 | 状态 | 项目角色 |
|---|---|---|
| CONCERTO | arXiv:2412.00749v2，2025；暂无正式 venue | execution-mechanism-aware cost features |
| Redefining Cost Estimation | arXiv 2025 | plan feature 与学习型模型综述 |
| SFS | arXiv 2026 | 动态 workload 的 service/TTFT 估计与路由 |
| TIE — Scheduling LLM Inference with Uncertainty-Aware Output Length Predictions | ICML 2026 | 用重尾输出长度分布和 Tail Inflated Expectation 替代单一点估计；支撑 work 分位数与 tail-risk 特征 |
| Past-Future Scheduler | ASPLOS 2025 | 从历史输出长度分布预测未来显存占用，以 SLA goodput 评价排队/eviction 决策 |
| JITServe | NSDI 2026 | 不精确信息下保守准入并随生成进展修正估计；支撑 remaining-work online update |
| Beyond Prediction: Tail-Aware Scheduling | ICML 2026 | 证明长度预测策略在分布漂移、burst、GPU memory pressure 下可能脆弱；作为 prediction-free/tail baseline 依据 |
| FastServe | NSDI 2026 | 用输入长度和多级反馈队列做 prediction-light 抢占调度；本项目只迁移强对照思想，不实现 serving 内部抢占 |
| Palimpzest | CIDR 2025 | 小样本 sentinel profile |
| LOTUS / Abacus | PVLDB | semantic operator cost-quality optimization |

以上新增 serving 文献不能全部当作可直接实现的系统 baseline：Past-Future、JITServe、Beyond Prediction 与 FastServe 都进入或修改 serving scheduler，而本项目固定 vLLM 为黑盒。它们在本项目中的角色是：定义输出 work 的不确定性表示、SLO goodput/tail/regret 指标，以及要求保留不依赖预测的强静态回退。可直接落地的首版仍是解析模型 + profile residual + 分位数/区间，作用点位于 Daft/Ray admission 之前。

主要来源：

- [SFS / Beyond Accuracy and Cost](https://arxiv.org/abs/2607.18253)
- [TIE / Uncertainty-Aware Output Length Predictions](https://arxiv.org/abs/2604.00499)
- [Past-Future Scheduler](https://arxiv.org/abs/2507.10150)
- [JITServe](https://www.usenix.org/conference/nsdi26/presentation/zhang-wei)
- [Beyond Prediction: Tail-Aware Scheduling](https://arxiv.org/abs/2606.18431)
- [FastServe](https://www.usenix.org/conference/nsdi26/presentation/wu-bingyang)
- [μ-Serve](https://www.usenix.org/conference/atc24/presentation/qiu)

### 2.4 数据执行的基础理论参照（2026-09-14）

以下只完成官方题录/摘要核对，未新增全文精读或调整Top15；用途与迁移条件由
[机制采用条件](研究定位.md#execution-transfer-cards)维护。

| 文献 | 核对版本与来源 | 本轮用途 |
|---|---|---|
| *A Proof for the Queuing Formula: L = λW*，John D. C. Little | Operations Research9(3):383–387，1961；[DOI](https://pubsonline.informs.org/doi/10.1287/opre.9.3.383) | 区分平均关系与有限样本面积、尾延迟 |
| *Achieving Utility-Delay-Reliability Tradeoff in Stochastic Network Optimization with Finite Buffers*，Sucha Supittayapornpong、Michael J. Neely | [arXiv:1501.03457v1](https://arxiv.org/abs/1501.03457)，2015 | 检查有限缓冲保证中的丢包、随机过程和时间平均条件 |
| *An Optimal Randomized Online Algorithm for Reordering Buffer Management*，Noa Avigdor-Elgrabli、Yuval Rabani | [arXiv:1303.3386v1](https://arxiv.org/abs/1303.3386)，2013；本轮不补推后续正式venue | 有限重排信息和竞争比较的已有理论，不将颜色切换模型等同KV缓存 |

Agentix的NSDI2026正式题录与摘要、MLSys2025关系查询重排、Ray Data arXiv v5和Kalypso v2摘要本轮复核；
BlendServe采用ASPLOS2026正式题名与摘要，不能用旧预印本题名/实验数字替代。
IMLane与DLPM复用已有精读，KEN仍待精读；这些核对不表示artifact已在本项目运行。

## 三、题录核验勘误

| 旧口径 | 核验后口径 |
|---|---|
| LOTUS 仅按 arXiv 标题引用 | 正式 PVLDB 18(11): 4171–4184, 2025；DOI 10.14778/3749646.3749685 |
| Abacus 尚未核验正式版本 | 正式 PVLDB 19(5): 1060–1073, 2026；DOI 10.14778/3796195.3796215 |
| SemBench 按预印本处理 | 正式 PVLDB 19(8): 1754–1767, 2026；DOI 10.14778/3811243.3811249 |
| Database Perspective 占 CCF-A Top 15 | 它是 PVLDB Tutorial，移入核心补充 |
| CIDR/MLSys/arXiv 统称“顶会” | 分别标为 CIDR、MLSys、预印本，不写成 CCF-A |
| Cortex AISQL 直接写成 SIGMOD CCF-A research | 按正式轨道标注；Companion/industry material 不算 full research |

## 四、代价估计专题定位

算子代价估计不是单独扩张出的第三项研究内容，而是数据组织与调度提交控制共同依赖的组件：

```text
输入/模型/硬件静态特征
  → prompt/output work 预测
  → operator service time / JCT / remaining work
  → active-work/K 初始化、组织/路由/提交决策
  → 实际 usage 与 completion trace
  → residual correction 与下一轮校准
```

首版限定为“简单解析模型 + profile 校准 + residual correction”。只有在跨时间、跨 workload 的误差与决策 regret 表明简单模型不足时，才评估 learned model。评价包括：

- work/service/JCT 的 MAE、MAPE、R² 与 prediction interval；
- 候选配置 ranking / top-k recall；
- 选定计划相对 oracle 的 JCT、吞吐和 SLO regret；
- 新 GPU、模型、长度分布和到达模式上的 held-out 泛化。

输出 work 在首版中按两档实现：固定长度/图像 workload 使用确定 work unit；自然 EOS 文本 workload 输出 `q50/q90/q95` 或等价预测区间，并记录 coverage、区间宽度和 tail underestimation。若区间过宽或检测到 OOD，必须回退到同上限静态 active-work/credit 策略；不能强制使用低置信度预测。

## 五、Baseline 对应关系

| Baseline 层 | 文献依据 | 用途 |
|---|---|---|
| direct model service | vLLM、Orca、Sarathi-Serve | 下游 serving ceiling |
| 简单上游 | vLLM official benchmark、bounded HTTP | 去除 Daft/Ray 后的强客户端对照 |
| 官方数据引擎/runtime | Ray、Ray Data Streaming Batch、Daft 官方 API | 引擎默认执行与受控并发 |
| 数据库 AI 算子系统 | LOTUS、Palimpzest、SemBench、Galois、IMLane | 算子/计划/质量-成本与内置外部执行 baseline |
| 多 job 调度 | VTC、Llumnix；补充 FairServe、DLPM、Agentix | shared credit、公平性、job-level JCT |
| 代价估计 | Learned Cost Models、GRACEFUL、COSTREAM、Abacus | 配置和路由选择依据 |

## 六、当前本地状态

- `docs/research/reading_notes/`：49 篇历史文献笔记，现按泛读库管理（不含 README 和模板）。
- `docs/research/精读文献笔记/`：精读笔记权威库，当前有十八篇主笔记、160 张论文原图裁剪件；新增 SPEAR 主笔记，未增加论文原图裁剪件。Kalypso Figure 1–12 与 IMLane Figure 1–15 已加入对应笔记。Kalypso 按 arXiv 核心补充管理，IMLane 按正式 PVLDB 2026 论文记录，SPEAR 按 CIDR 2026 vision/early-design paper 解读；三篇均未进入原十五篇速览或开题正文。
- `docs/research/reference/`：当前工作区有 7 份可解析 PDF 实体（Galois、Abacus、Palimpzest、Sema、Parrot、Kalypso、IMLane）；历史题录仍由 `reference/REFERENCE_INDEX.md` 保留。
- `docs/archive/opening/literature/top15_reading_notes/`：只保留当前 Top 15 的自包含快照。
- 目录历史中曾登记但当前工作区没有实体 PDF 的条目，不再标为“已下载”；需要时按索引重新下载。

## 上游调度与前缀复用的待核查线索

<a id="serving-literature-20261003"></a>
2026-10-03：Preble、BatchLLM 与 CONCUR 的正式题录、相关算法及部分作者源码已核验，分别登记到上表。
BatchLLM 的作者实现为[MixLLM 的 batchllm_vllm_064 分支](https://github.com/microsoft/MixLLM/tree/batchllm_vllm_064)，
与同名 HTTP 包分开；正式版还含长尾输出评价，不能沿用“未考虑长尾”的旧批评。
CONCUR 的早期笔记和 arXiv v1 PDF 使用旧题名；arXiv 摘要改名不表示存在新 v2，当前采用正式 PMLR 题名。
上述为本轮专项核验，未新建完成式精读笔记、调整 Top 15 或在本项目运行作者实现。

既有材料还提到SOLO、llm-d及OpenReview标识`VSY1nFjumI`、`R7bK9yycHp`。
这些条目在此登记为待核查线索：正式题录、资料版本、源码或全文、数据/请求重排与提交控制的
实际覆盖范围需分别核对。未经核验的收益数字与新颖性判定不进入当前结论。
PolarDB/Daft、Kalypso与代价估计的已有分析继续从研究定位和对应文献引用，避免并列维护另一份判断。

<a id="scenario-input-sources-20261003"></a>
## 多模态、具身输入与有限观察理论补充（2026-10-03）

用途为[完整流程与场景分析](优化方法依据.md#query-specific-execution-opportunities)的输入语义、工程参照与理论核查，
没有新增完成式精读笔记、Top15排名或可运行baseline。作者main/滚动文档仅按本次读取解释，运行前另固定commit。

| 一手资料与核对版本 | 核对内容及用途 |
|---|---|
| [DROID: A Large-Scale In-The-Wild Robot Manipulation Dataset，arXiv:2403.12945v2](https://arxiv.org/pdf/2403.12945v2)；[作者数据页](https://droid-dataset.github.io/) | §III-A/Appendix B的多传感器与校准，TFDS episode/steps读取入口；不概括所有公开版本的存储格式 |
| [Octo: An Open-Source Generalist Robot Policy，arXiv:2405.12213v2](https://arxiv.org/html/2405.12213v2)；[dataset.py](https://raw.githubusercontent.com/octo-models/octo/main/octo/data/dataset.py)、[traj_transforms.py](https://raw.githubusercontent.com/octo-models/octo/main/octo/data/traj_transforms.py) | history/action与mask、按权重采样和先混洗后decode；用于排除任意逐帧Map和为局部性改变训练采样 |
| [OpenVLA: An Open-Source Vision-Language-Action Model，arXiv:2406.09246v3](https://arxiv.org/html/2406.09246v3) | §3.2–3.5/§5.2/§6的输入、训练视觉编码器及控制适用范围；不从特定频率推出动作时限保证 |
| [Open X-Embodiment官方仓库](https://github.com/google-deepmind/open_x_embodiment)、[RLDS官方格式](https://github.com/google-research/rlds) | episode/step与终止标记；作为合法窗口和缺失/末步语义参照，尚无本项目接入 |
| [LeRobotDataset v3.0官方说明](https://huggingface.co/docs/lerobot/en/lerobot-dataset-v3) | Parquet状态/action、MP4视频、episode偏移与delta_timestamps；已有读取/窗口方法作为强原生参照 |
| [tf.data: A Machine Learning Data Processing Framework，arXiv:2101.12127v1](https://arxiv.org/pdf/2101.12127v1) | §2.2/§3.2–3.3.2/§5.2的采样、流水、预算和消费者速率；训练供给的已有直接机制 |
| [Little's Law as Viewed on Its 50th Anniversary，Operations Research59(3):536–549，2011；DOI10.1287/opre.1110.0940](https://pubsonline.informs.org/doi/10.1287/opre.1110.0940)；[作者原文全文](https://people.cs.umass.edu/~emery/classes/cmpsci691st/readings/OS/Littles-Law-50-Years-Later.pdf) | §2.1.2/§2.2/§3.1核对完整有限观察段的均值恒等式；不用于证明最优容量、查询完成或动作尾延迟 |

IMLane、Ray Streaming Batch、Preble与关系LLM查询仍复用上方题录及已核验原文。
MaxWeight的原始定理本次未核验，不用于这次分析的保证性结论。

<a id="heterogeneous-data-sources-20261003"></a>
## 物理需求、表示与异构资源补充（2026-10-03）

用于[异构需求分析](优化方法依据.md#heterogeneous-demand-supplement)。以下为题录和指定章节核查，
没有新增完成式精读笔记、Top15排名或已运行baseline；正式版本与实际阅读版本分别记录。

| 正式题录 / 核对资料 | 阅读范围、覆盖与迁移限制 |
|---|---|
| [Scanner: Efficient Video Analysis at Scale，ACM TOG37(4)，Article138，2018；DOI10.1145/3197517.3201394](https://graphics.stanford.edu/papers/scanner/)；[arXiv:1805.07339v1](https://arxiv.org/html/1805.07339) | §3.2/§4.1–4.3/Appendix A：增量需求反推、读取/解码/计算粒度与关键帧依赖；数据相关sequence长度变化与每帧可变长列表分开，有状态warmup不能当逐元素等价 |
| [VStore: A Data Store for Analytics on Large Videos，EuroSys2019，Article16，17页；DOI10.1145/3302424.3303971](https://thexsel.github.io/p/vstore/)；[arXiv:1810.01794v3](https://arxiv.org/html/1810.01794v3) | §2.2–2.4/§4.1–4.3：消费者保真度、存储配置与周期profile；未联合建模query cascade依赖，允许的采样/分辨率取舍不等于固定输入；[作者README](https://github.com/tiantuxu/VStore)说明配置推导等未公开，完整artifact运行待核对 |
| [cedar: Optimized and Unified Machine Learning Input Data Pipelines，PVLDB18(2):488–502，2024；DOI10.14778/3705829.3705861](https://www.vldb.org/pvldb/vol18/p488-zhao.pdf)；[arXiv:2401.08895v4](https://arxiv.org/html/2401.08895v4) | 机制按v4的§3.2/§5.1/§6.2及Table3核对：缓存/重排/融合/offload组合、随机语义、表示膨胀及ASR不缓存反例；正式PDF本次获取超时，未重新逐字比对两个版本 |
| [Plumber: Diagnosing and Removing Performance Bottlenecks in Machine Learning Data Pipelines，MLSys2022，Proceedings of Machine Learning and Systems4](https://proceedings.mlsys.org/paper_files/paper/2022/hash/d0e90e9a9310570dfa643aa3b2da6e89-Abstract.html)；[正式全文](https://proceedings.mlsys.org/paper_files/paper/2022/file/d0e90e9a9310570dfa643aa3b2da6e89-Paper.pdf) | §4.1–4.4/§5.1及Appendix：访问比例、CPU/I/O/物化画像及资源选择；网络/GPU传输观测当时列为扩展，RCNN预测高估不能当可达吞吐保证 |
| [FlexGen: High-Throughput Generative Inference of Large Language Models with a Single GPU，ICML2023，PMLR202:31094–31116](https://proceedings.mlr.press/v202/sheng23a.html)；[正式全文](https://proceedings.mlr.press/v202/sheng23a/sheng23a.pdf) | §4核对weights/KV/activations的GPU/CPU/disk放置和移动；用来区分模型内部卸载与输入/派生缓存，不纳入当前外部HTTP动作 |

工程资料单独解释为接口事实：
[Ray2.56.1 object spilling](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/doc/source/ray-core/objects/object-spilling.rst)与
[同版本Object Restore说明](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/doc/source/ray-core/internals/object-spilling.rst)
用于对象临时I/O；[DALI image decoder](https://docs.nvidia.com/deeplearning/dali/main-user-guide/docs/operations/nvidia.dali.fn.decoders.image.html)
用于格式、执行位置与已有cache的核对；
[PyTorch锁页与异步复制教程](https://docs.pytorch.org/tutorials/intermediate/pinmem_nonblock.html)用于复制开销、重叠条件与缓冲寿命；
[GPUDirect Storage Overview](https://docs.nvidia.com/gpudirect-storage/overview-guide/)§1.1–1.3用于文件字节移动和兼容模式。
DALI/PyTorch/GDS为本次读取的滚动官方资料，未固定实际安装版本，未验证本项目接入或性能。
StarPU/HetExchange、红蓝pebble原始定理与MaxWeight本次未新增核验，不用于本次保证性结论。

<a id="backend-ownership-sources-20261003"></a>
## 数据引擎职责与原生媒体接口补充（2026-10-03）

用于[执行层分工评估](优化方法依据.md#engine-ownership-assessment)，本轮只核对官方文档/源码，
不增加论文题录等级或可运行baseline。Sema与SemBench继续引用已有题录；摘要核对不代替全文。

| 一手资料 / 版本 | 核对内容及使用范围 |
|---|---|
| [Daft Architecture，滚动stable](https://docs.daft.ai/en/stable/architecture/) | Planning/Optimization/Execution：独立计划与runner，昂贵projection延后；未将全部规则归属于已测0.7.21 |
| [Daft LeRobot，滚动stable](https://docs.daft.ai/en/stable/datasets/lerobot/)；[固定v0.7.21源码](https://raw.githubusercontent.com/Eventual-Inc/Daft/v0.7.21/daft/datasets/lerobot.py) | read_episodes/load_episode_frames/read真实入口；固定版严格检查v3.0，data/**读后关联与视频解码分开；批内按shard/timestamp复用解码已存在，实际少读及筛选后解码接法待验证 |
| [Ray2.56.1 Data internals](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/doc/source/data/data-internals.rst)、[资源说明](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/doc/source/ray-core/scheduling/resources.rst) | 数据流组批/背压及Core逻辑资源；CPU线程、进程堆、对象存储与显存物理使用分别核对 |
| [Ray2.56.1公开LLM API](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/python/ray/data/llm.py)、[Serve](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/doc/source/serve/index.md)、[Train](https://raw.githubusercontent.com/ray-project/ray/ray-2.56.1/doc/source/train/train.rst) | HTTP/内嵌vLLM/Serve processor配置及服务/训练所有者分开；API当时为beta，部署路径不能用同一个框架名替代 |

上述源码证明提供何种接口与逻辑处理，不证明本项目物理I/O、decoder性能或完整查询收益。
没有在本项目运行这些新增媒体/部署对照，滚动资料在运行前另固定实际版本。

<a id="early-candidates"></a>
## 早期跨领域线索

以下仅保留原knowledge_hub.md文献地图中未在上方登记的名称，原始说明来自其2026-10-02快照。
“原标注”保留检索线索，不表示本次核验了题名、年份、轨道或性能；正式引用前从原文与官方入口核对。
已登记文献以上方题录、阅读状态及对应笔记为准，旧总数和等级统计不继续维护。

| 早期名称 | 原标注的资料来源 | 原归类用途 |
|---|---|---|
| Mooncake | FAST 2025 Best Paper | 模型服务 |
| S-LoRA | MLSys 2024 | 模型服务 |
| Nexus | SOSP 2019 | 模型服务 |
| Triton | NVIDIA | 模型服务 |
| INFaaS | ATC 2021 | 模型服务 |
| ChunkAttention | ACL 2024 | 模型服务 |
| Spark SQL | 官方文档 | 数据管线 |
| Velox | VLDB 2022 (Meta) | 数据管线 |
| Arrow DataFusion | SIGMOD 2024 | 数据管线 |
| Arrow Flight | arXiv 2022 | 数据管线 |
| Lance | arXiv 2025 | 存储与写回 |
| ColStorEval | PVLDB 2023 | 存储与写回 |
| TurboVecDB | PVLDB 2025 | 存储与写回 |
| Delta Lake | PVLDB 2020 | 存储与写回 |
| FlexPushdownDB | PVLDB 2021 | 存储与写回 |
| WiscKey | FAST 2016 | 存储与写回 |
| DiskANN | NeurIPS 2019 | 存储与写回 |
| Milvus | SIGMOD 2021 | 存储与写回 |
| Manu | VLDB 2022 | 存储与写回 |
| VBASE | OSDI 2023 | 存储与写回 |
| BigVectorBench | VLDB 2025 | 存储与写回 |
| AIDB | DEEM@SIGMOD 2024 | 存储与写回 |
| Rafiki | PVLDB 2018 | 存储与写回 |
| Trustworthy LLMs Meet Databases | VLDB 2024 Tutorial | 综述 |
| Vector DBMS Tutorial | VLDB 2024 | 综述 |
| Learned Query Optimizer | SIGMOD 2024 | 综述 |
| Learning Database Optimization | FCS 2025 | 综述 |
| Snowflake Cortex AI | `AI_EMBED`, `AI_COMPLETE`, `AI_FILTER`, `AI_CLASSIFY`, `AI_JOIN`, `AI_AGG` | 工业需求 |
| BigQuery ML/AI | `ML.GENERATE_TEXT`, `ML.GENERATE_EMBEDDING` | 工业需求 |
| Oracle AI Vector Search | `VECTOR_EMBEDDING` | 工业需求 |
| pgai | PostgreSQL + vectorizer worker + embedding endpoint + 写回 | 工业需求 |
| PostgresML | PostgreSQL 内/近数据库 ML/AI | 工业需求 |
| pgvector | PostgreSQL 向量相似度检索 | 工业需求 |
| Neo | SIGMOD 2019 | 代价估计 |

GPipe、PipeDream和Alpa另在原文作为训练流水线线索列出；具体迁移需求出现后再按原文核对，不加入当前执行安排。
