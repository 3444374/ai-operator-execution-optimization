# 数据库 AI 算子相关文献清单

更新日期：2026-08-23

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
| SemBench | PVLDB 2026 正式 benchmark paper | 55 queries、多模态、质量/时间/成本/内存统一评价 |
| Database Perspective on LLM Inference Systems | PVLDB 2025 Tutorial | 推理系统地图与代价估计 open problem |
| Cortex AISQL | 按实际 Companion/工业轨道引用 | AI SQL 工业需求证据；不写成 CCF-A full paper |
| NeurDB | CIDR 2025，非 CCF-A | AI-native database vision 与边界对照 |
| LLM for Data Management | PVLDB 2024 | DB/LLM 研究版图 |
| Smart、SmartLite、LEADS、InferDB | 正式数据库论文 | 近数据库推理、模型选择和动态执行对照 |

### 2.2 LLM 公平调度与程序级执行

| 文献 | 题录状态 | 项目角色 |
|---|---|---|
| FairServe | arXiv 2024 | weighted service、interaction-aware throttling |
| DLPM/D2LPM | arXiv 2025 | deficit fairness + prefix locality |
| Autellix | arXiv 2025 | program/job-level attained service |
| Chiron | arXiv 2025 | 分层 backpressure 与 autoscaling |
| Clipper | NSDI 2017 | AIMD batching 历史来源 |
| Splitwise | ISCA 2024 | prefill/decode 分池 |
| μ-Serve | USENIX ATC 2024 | GPU frequency scaling + model multiplexing；作为功耗、能耗与 SLO attainment 参考，不归入输出长度代价估计 |
| Clockwork | OSDI 2020 | predictable serving |
| CONCUR、SABER、BucketServe、Scorpio、ProServe | arXiv | 候选 admission/length/SLO/priority 机制 |
| Ray Data Streaming Batch | arXiv 2025 | 数据引擎 streaming batch 模型 |

### 2.3 代价估计扩展

| 文献 | 状态 | 项目角色 |
|---|---|---|
| CONCERTO | arXiv 2024 | execution-mechanism-aware cost features |
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
| 数据库 AI 算子系统 | LOTUS、Palimpzest、SemBench、Galois | 算子/计划/质量-成本 baseline |
| 多 job 调度 | VTC、Llumnix；补充 FairServe、DLPM、Autellix | shared credit、公平性、job-level JCT |
| 代价估计 | Learned Cost Models、GRACEFUL、COSTREAM、Abacus | 配置和路由选择依据 |

## 六、当前本地状态

- `research/reading_notes/`：49 篇历史文献笔记，现按泛读库管理（不含 README 和模板）。
- `research/精读文献笔记/`：精读笔记权威库，当前包含 LOTUS、Cortex AISQL、关系型 LLM 查询优化、Ray、Ray Data Streaming Batch、AYO、VTC 和 BlendServe 八篇主笔记，共 65 张论文原图裁剪件；不维护阅读状态字段。
- `research/reference/`：本轮保留 21 份可解析 PDF，其中 Top 15 的 15 份全部齐全。
- `opening/literature/top15_reading_notes/`：只保留当前 Top 15 的自包含快照。
- 目录历史中曾登记但当前工作区没有实体 PDF 的条目，不再标为“已下载”；需要时按索引重新下载。
