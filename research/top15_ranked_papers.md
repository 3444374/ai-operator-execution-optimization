# 项目最相关 Top 15 论文

更新日期：2026-07-29
选择目标：以正式、可核验的 CCF-A research paper 构成开题核心文献；高度相关但非 CCF-A 的系统、预印本、Tutorial、Companion、Demo 放入“核心补充文献”。

## 选择与核验规则

1. 先核验正式题录、轨道、卷期、页码和 DOI，再讨论排名。
2. CCF-A 正式 research paper 优先；CIDR、MLSys、arXiv 不写成 CCF-A。
3. Companion、Demo、Tutorial 即使依附 CCF-A 会议/期刊，也不计入本 Top 15。
4. 排名体现本课题的证据结构，不表示论文绝对质量。
5. 每篇必须同时有权威精读笔记和本地可解析 PDF。

## Top 15（15/15 为严格 CCF-A 正式研究论文）

### 一、AI 算子与数据库系统（3 篇）

| # | 论文 | 正式出处 | 项目角色 | 笔记 / PDF |
|---|---|---|---|---|
| 1 | **LOTUS**: Semantic Operators and Their Optimization | PVLDB 18(11), 2025；DOI 10.14778/3749646.3749685 | 声明式 semantic operators、准确率约束、调用数/质量/成本优化；官方数据库 AI 系统 baseline | `lotus_pvldb2025.md` / `lotus_pvldb2025.pdf` |
| 2 | **Galois**: Logical and Physical Optimizations for SQL over LLMs | SIGMOD 2025；DOI 10.1145/3725411 | LLM 专用逻辑/物理算子与质量感知 SQL 优化 | `galois_sigmod2025.md` / `galois_sigmod2025.pdf` |
| 3 | **GaussML**: An End-to-End In-Database Machine Learning System | ICDE 2024；DOI 10.1109/ICDE60146.2024.00391 | 数据库原生 AI/ML 算子、ML-aware cardinality/cost estimator 对照 | `gaussml_icde2024.md` / `gaussml_icde2024.pdf` |

### 二、LLM 推理与公平调度（7 篇）

| # | 论文 | 正式出处 | 项目角色 | 笔记 / PDF |
|---|---|---|---|---|
| 4 | **vLLM**: PagedAttention | SOSP 2023 | 部署平台与下游 capacity ceiling | `vllm_sosp2023.md` / `vllm_sosp2023.pdf` |
| 5 | **Orca**: Iteration-level Scheduling | OSDI 2022 | continuous batching 的基础机制与下游执行边界 | `orca_osdi2022.md` / `orca_osdi2022.pdf` |
| 6 | **Sarathi-Serve**: Chunked Prefill | OSDI 2024 | token-budget、prefill/decode 干扰与 serving capacity | `sarathi_serve_osdi2024.md` / `sarathi_serve_osdi2024.pdf` |
| 7 | **SGLang**: Structured LM Programs | NeurIPS 2024 | prefix/cache-aware 调度与结构化程序执行 | `sglang_neurips2024.md` / `sglang_neurips2024.pdf` |
| 8 | **VTC**: Fairness in Serving LLMs | OSDI 2024 | endpoint-shared service counter、公平性和 work-conserving borrowing | `vtc_osdi2024.md` / `vtc_osdi2024.pdf` |
| 9 | **Llumnix**: Dynamic Scheduling | OSDI 2024 | 多实例虚拟 usage、在线负载纠偏与优先级隔离 | `llumnix_osdi2024.md` / `llumnix_osdi2024.pdf` |
| 10 | **DistServe**: Prefill-Decode Disaggregation | OSDI 2024 | goodput、阶段干扰、capacity planning | `distserve_osdi2024.md` / `distserve_osdi2024.pdf` |

### 三、Ray 分布式执行（1 篇）

| # | 论文 | 正式出处 | 项目角色 | 笔记 / PDF |
|---|---|---|---|---|
| 11 | **Ray**: A Distributed Framework for Emerging AI Applications | OSDI 2018 | stateful actor、async task、分布式资源调度的架构载体 | `ray_osdi2018.md` / `ray_osdi2018.pdf` |

### 四、算子代价估计（4 篇）

| # | 论文 | 正式出处 | 项目角色 | 笔记 / PDF |
|---|---|---|---|---|
| 12 | **How Good Are Learned Cost Models, Really?** | SIGMOD 2025 | 代价模型必须按 plan-selection/ranking 任务评估，不能只看预测误差 | `learned_cost_models_sigmod2025.md` / `learned_cost_models_sigmod2025.pdf` |
| 13 | **GRACEFUL**: A Learned Cost Estimator for UDFs | ICDE 2025；DOI 10.1109/ICDE65448.2025.00185 | UDF/AI 算子服务时间估计与执行位置决策 | `graceful_udf_cost_icde2025.md` / `graceful_udf_cost_icde2025.pdf` |
| 14 | **COSTREAM**: Learned Cost Models for Operator Placement | ICDE 2024；DOI 10.1109/ICDE60146.2024.00015 | 跨资源环境的 operator placement 与 zero-shot cost features | `costream_icde2024.md` / `costream_icde2024.pdf` |
| 15 | **Abacus**: A Cost-Based Optimizer for Semantic Operator Systems | PVLDB 19(5), 2026；DOI 10.14778/3796195.3796215 | semantic operator 多目标代价估计、Pareto 计划选择与 profile 复用 | `abacus_pvldb2026.md` / `abacus_pvldb2026.pdf` |

## 结构覆盖

| 研究问题 | 核心证据 |
|---|---|
| 固定资源下，怎样用更少的上游压力尽快达到下游 capacity ceiling？ | vLLM、Orca、Sarathi-Serve、DistServe、Ray |
| 怎样组织数据库 AI 数据并避免无效模型调用？ | LOTUS、Galois、GaussML |
| 多 job 共享同一 vLLM 时怎样公平且 work-conserving？ | VTC、Llumnix、Ray |
| 怎样把 work/service/JCT 估计用于配置、路由和提交决策？ | Learned Cost Models、GRACEFUL、COSTREAM、Abacus |

本项目不把上述四行改写成四个独立贡献。开题仍保留“数据组织策略”和“调度与提交控制策略”两项研究内容；代价估计是贯穿二者的使能组件，多模态是泛化验证。

## 核心补充文献

| 文献 | 核验状态 | 保留角色 | 不进 Top 15 的原因 |
|---|---|---|---|
| Database Perspective on LLM Inference Systems | PVLDB 2025 Tutorial | 推理系统地图与开放问题 | Tutorial，不是 research paper |
| Palimpzest | CIDR 2025 | 声明式计划搜索、时间/成本/质量 profile；官方系统 baseline | CIDR 非 CCF-A |
| SemBench | PVLDB 19(8), 2026 | semantic query engine benchmark、workload 与指标 | benchmark 依据，方法 Top 15 席位优先给直接算法来源 |
| FairServe | arXiv 2024 | 多应用 weighted service 与 interaction-aware throttling | 预印本 |
| DLPM / D2LPM | arXiv 2025 | prefix-locality 与 deficit fairness | 预印本 |
| Agentix（arXiv v1 名称 Autellix） | NSDI 2026 | program/job-level attained service | 正式系统论文；作为核心补充，不替换当前 Top 15 |
| Chiron | arXiv 2025 | 分层 backpressure 与 autoscaling | 预印本；autoscaling 超出固定双 GPU边界 |
| Clipper | NSDI 2017 | AIMD batching 历史来源 | 现有 fixed/adaptive 实验已显示控制器不优于同上限静态策略 |
| Splitwise | ISCA 2024 | prefill/decode 分池 | 与当前不修改 vLLM 的边界较远 |
| NeurDB | CIDR 2025 | AI-native database vision | CIDR 非 CCF-A |
| Cortex AISQL | SIGMOD Companion/industry material（按实际轨道引用） | 工业 AI SQL 需求证据 | 不把 Companion/工业材料写成正式 CCF-A research paper |
| CONCUR、SABER、BucketServe、Ray Data Streaming Batch | arXiv | 候选控制/数据引擎机制 | 预印本 |

## 代价估计在项目中的定位

算子代价估计从“补充讨论”提升为两项方法共同依赖的重要组件，但不独立扩展成第三项研究内容。首版采用：

```text
简单解析模型
  + 少量真实 profile 校准
  + 运行 trace 的 residual correction
```

它服务于：

- 预测 prompt/output token work；
- 估计 operator service time 与 job completion time；
- 初始化不同 GPU、模型和 workload 的 active-work/K 上限；
- 选择数据组织、endpoint 路由和提交策略；
- 多 job 下估计 remaining work 与 SLO slack；
- 用真实 usage、completion trace 在线或跨轮次校正误差。

评价除 MAE/MAPE 外，还必须报告配置排序正确率、JCT/throughput regret、held-out workload 泛化和预测区间覆盖，避免“误差更小但选错执行方案”。

## 一致性状态

- Top 15：15/15 严格 CCF-A 正式 research paper。
- Top 15：15/15 已有权威精读笔记。
- Top 15：15/15 已有本地可解析 PDF。
- Tutorial、CIDR、arXiv 和 Companion 已从 Top 15 中移出并保留为核心补充。
