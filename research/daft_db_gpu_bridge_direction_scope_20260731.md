# 方向 scope：数据库↔GPU 经 Daft 桥接的异构算子调度

日期：2026-07-31
状态：**方向已 validate，贡献角度未锁**（冷启动 parked）——待导师/学长确认后定。
来源：学长反馈（`notes/communication_notes.md` §5）+ 工作流 `w6xclfb0g`（Daft 内部核实 + 三痛点先验 + workload fit）+ 之前 scoop（`notes/communication_notes.md` §5.5）。
不改题目；本文是 academic-pipeline Stage 1 (RESEARCH) 的 scoped 输出。

> **2026-08-01 证据边界修正**：图像 workload 已锁，但“数据搬运是主瓶颈”和
> “执行优化是结构性空白”均未被证明。PolarDB/Daft 官方已提供按算子声明资源的
> staged CPU→GPU 流水线；当前 1.296×/1.138× 只相对 fused UDF。本文的历史 scope
> 讨论保留，但实验判决以 `motivation/plans/image_host_data_path_bottleneck.md` 和
> `experiments/plans/image_clip_workload_lock_20260731.md` 的 fused/staged 矩阵为准。

---

## 1. 场景 reframe（学长最新反馈）

**不是**"vLLM 提交控制换个模态"，**是**：

> 数据库 ↔ GPU 经 **Daft 桥接**：Daft 是数据库与 GPU 之间的数据搬运/中转层；GPU 侧算子**多样**（CLIP / embedding / 自定义业务算子，不止 vLLM）；数据量**大**；执行是**流式 pipeline**。

学长指出 Daft 三个痛点：
1. **GPU 数量写死**：`@daft.cls(gpus=N)` 的 N 在装饰期静态固定，不随数据量动态（昨天 100G、今天 10000G，同一算子用同样的 N）。
2. **多算子冷启动**：4 卡跑 100 算子，不能全预加载；Daft 选 GPU → 加载模型 → 分片传数据 → 算，有冷启动。
3. **流式 pipeline 优化空间**：1 万 G 不能一次放 GPU，分批流式（100G×100 卡），流水线有调度空间。

学长原则：**先定被认可的 benchmark/workload，场景被认可以后任何正指标都被接受**；找 workload 本身是最重要的研究行为；关注数据准备过程。

## 2. Daft 实际行为核实（源码 + 官方文档一手核实）

三痛点**全部真实**，但真实程度、归属层、可占切片差异显著：

### ① gpu-static — 最硬、最干净
- `gpus` 是装饰期静态标量（源码 `daft/udf/__init__.py` L360-410，`gpus: float = 0`，per-instance placement hint；≤1.0 可分数共享多小模型共占一卡、>1.0 须整数）。`max_concurrency` 同样装饰/实例化时写死。
- 整条链路**无任何组件读输入数据量**（行数/token/frame）来调整 gpus/concurrency。
- 最接近的 Daft dynamic batching（2026-01 feature flag）**只调 batch_size 不调 gpus**；RFC #5904 / PR #5903 **只做节点级 scale-in**（Phase 2 load-based / Phase 3 heterogeneous-predictive 仍 open）。
- **杠杆**：在 `@daft.cls` 之上加 data-size/workload-aware 的 (gpus, max_concurrency) 决策器。**但 Daft 团队正在自己关闭**（RFC #5683 路线图），可占切片窄。

### ② cold-start — 真实、最可防御，但需精准定位
- **单算子**冷启动 Daft 已解（per-worker `__init__` 摊销，官方 7min/2GB）。
- 学长的"4 卡 100 算子不能全预加载"是**多算子生命周期**问题，Daft 完全不做：**无 model garden / 无 swap API / 无 LRU 驱逐**；每个 `@daft.cls` 类 = 一模型常驻 worker 生命周期，`N_op × model_size >> HBM` 时物理上无法全驻留。
- Issue #2900 暴露多 actor-pool 串联时低并发上游节流下游（子 buffer 上限 = 2×concurrency）。
- **杠杆**：固定显存预算下的多算子加载/换出调度（per-operator pool sizing + LRU/demand-predictive 驱逐 + 跨算子显存装箱）。**三条痛点里先验距离最远（medium-high）**。
- **Fatal-flaw 候选**：2×4090 + 18G 盘下，"装不下"regime 自然不可达（见 §7）。

### ③ streaming — 真实但 Daft 已部分吃掉、门槛最高
- Swordfish morsel-driven push + bounded async channel 背压 + dynamic batching 是**强 baseline**（实测与手调 ±2%）。
- VLLMPrefixCachedProvider 已把 vLLM 的 prefix + continuous-batching 吃下（128×L4 快 50.7%）。
- **真实缺口仅剩**：dynamic batching 是 **model-service-blind**（只看本 UDF batch 执行耗时，不看下游 queue/KV/running-req），对**非 vLLM 算子**（CLIP/自定义业务算子）无 service-state-aware 提交。
- 无界流 Kafka 仍无（Issue #4603 open → #5970）——"大数据流式"在 Daft 语境指批内 morsel streaming；项目若主打 DB→GPU 流式需自接 PostgreSQL CDC 喂入 bounded pipeline。
- **先验距离最近（low-medium）**，不作独立引擎贡献，宜作 Flotilla/Ray Data 之上的策略层。

### 排序（先验距离由远到近、可防御性由强到弱）
**cold-start(②) >> gpu-static(①) > streaming(③)**

## 3. 可防御界面：online vs offline 分界（核心发现）

所有 scoop 相关先验——ServerlessLLM / Llumnix / AlpaServe / Clockwork / INFaaS / Chiron / TORTA / Autellix（多模型 GPU 放置/换出）+ Daft v0.6.9 prefix router + llm-d / Preble（状态感知）——**都是 online serving**：请求随机到达，**结构性无法利用批 dataflow 管线在 query planning 阶段已知的两份 foreknowledge**：

1. **算子调用 DAG**（哪些算子、什么顺序、几次）
2. **各算子数据量**（plan 阶段可估的 token/frame/byte 体积）

这份 foreknowledge 是三痛点共同的**新颖性来源**，也是对学长"数据未到/GPU 不慢"最该举起的一点：**批管线的"数据"是 plan 阶段可估的已知体积，不是 online 随机请求流。**

> 这不是对之前 partially-scooped 判决的完全翻转。offline batch foreknowledge 仍是
> 候选差异，但 Daft/PolarDB/Ray Data 已覆盖 staged pipeline、overlap 与 backpressure；
> 只有在强 staged baseline 之后仍存在可复现的调度增量，才能把该界面写成研究空白。

## 4. 贡献空间（按先验距离由远到近）

1. **主贡献候选（痛点②）**：批 dataflow 管线中，给定已知算子 DAG + 各算子数据量 + 固定显存预算，做**冷启动感知的多异构算子放置/换出调度**——预加载/优先排程"数据量大×靠前"算子，把冷启动摊进流水线而非串行等待。多轮定向检索未发现任何论文做此精确组合。**最强防御切片，但有 fatal-flaw（§7）。**
2. **方法组件（痛点①）**：data-volume-aware right-sizing，用每算子已知 token/frame 体积反推 gpus/concurrency。可借 Pollux goodpass 思路落到批算子；Daft 团队 RFC #5683 roadmap 正在关闭，**作组件不作独立贡献**。
3. **实验平台（痛点③）**：在 Flotilla/Ray Data 之上做 model-residency-aware 流式策略层，验证 service-state-aware 对非 vLLM 算子的泛化；**不作独立引擎贡献**。

**候选 framing**：在通用 Daft-on-Ray pipeline 之上，利用批管线 DAG + 数据量
foreknowledge 做 service-state/work-aware 请求成形、共享 credit 或多算子资源决策。
该 framing 尚待 staged Daft/Ray Data baseline 和多 job 结果验证，不能预先写成
PolarDB Lakebase “结构性不覆盖”。

## 5. Workload 推荐（不依赖冷启动 regime 的优先）

| 角色 | workload | 为什么 | 来源 / 可引 |
|---|---|---|---|
| **🔴 首个 workload** | **图像 CLIP AI_EMBED（COCO/ImageNet 子集）** | JPEG decode/processor + 大 pixel tensor 让 DB/CPU/Ray/H2D/GPU 木桶效应可测；不预设哪段是瓶颈 | ImageNet/COCO + ANN-benchmarks recall@10 + BigVectorBench image 切片（VLDB'25） |
| 文本轻对照（降级） | MS MARCO 8.8M（文本） | 当前文本实测主要墙钟在 vLLM serving；作为另一 regime 对照 | MS MARCO leaderboard |
| 冷启动 regime（parked） | BigVectorBench 异构 embedding 全套（多模型 >HBM） | 三模态触发冷启动②；parked 待解封 | VLDB'25 PVLDB |
| operator 多样性参照 | SemBench 55 queries/5 workloads/4 模态 | ② 参照 | arXiv 2511.01716 |

## 6. scoop / 文献定位

- **结构性不覆盖（本项目可占）**：批 dataflow + foreknowledge（DAG + 数据量）+ 多样异构算子冷启动放置。
- **online serving 先验（不覆盖批 dataflow）**：ServerlessLLM (OSDI'24)、Llumnix (OSDI'24)、AlpaServe (OSDI'23)、Clockwork (OSDI'20)、INFaaS (ATC'21)、Chiron (OSDI'24)、Autellix (arXiv'25)、TORTA (arXiv 2507.10259, **未核验**)、Helium (arXiv 2603.16104, **未核验**)。
- **跨生态模型 swap 先验（工程层，非 Daft 调度研究）**：arXiv:2306.13835、NVIDIA Run:ai GPU memory swap（~0.5-1s 换回）、Modal snapshot。
- **批 dataflow 引擎层（竞争范式）**：Ray Data Streaming Batch (arXiv:2501.12407, block-streaming 但无 token/compute/model-load 感知)、Daft Flotilla/Swordfish。
- **Daft 自身路线图（正在关闭①）**：RFC #5683 (Phase 1 落地 PR #5903, Phase 2/3 open)、RFC #5904、dynamic batching (2026-01)。
- **可借思路**：Pollux (OSDI'21, goodpass-based right-sizing)。

## 7. 诚实风险与待决

1. **Fatal-flaw（冷启动 regime 可达性）**：2×4090 = 48G HBM、盘空闲 18G。**模型 garden ≤ 盘(18G) < HBM(48G)**——能下载的模型一定装得进显存，**自然条件下永远不触发冷启动**。两条出路：(B) 人为约束显存预算（framing 为"GPU 共享/per-tenant memory budget"，可辩护但构造性）；(A) 扩盘 + 下 ~50G 模型（真实 regime，成本高）。
2. **swap 成本**：GPU↔CPU 模型 swap ~0.5–1s（NVIDIA Run:ai 数字）；小规模小数据下可能吃掉收益，须大数据量 × 多算子切换才划算——swap_cost / inference_time 比要实测。
3. **次要先验未逐字核验**：TORTA、Sia (e-Energy'23)、Helium、E3/CARMA/token-pools——引用前需 PDF 精读确认未覆盖批 dataflow 多算子放置。
4. **Daft RFC #5683/#5904 Phase 2/3** 落地时间——若 Daft 团队先发布，痛点①切片进一步收窄；需 watch。
5. **"DB→GPU 流式"缺 CDC**——需自接 PostgreSQL 变更喂 bounded pipeline；**工程前提 vs 研究贡献要划界**。
6. **旧精读笔记需重读**：ServerlessLLM/Llumnix/Ray Data Streaming Batch/Chiron/Autellix 的"与课题连接"段按旧 vLLM-serving 方向写，需按 online-vs-offline + dataflow-foreknowledge 视角重读。
7. **题目/研究内容是否精修**：当前对外"数据库 AI 算子外部执行链路优化"没体现多算子/Daft 桥接；是否在 AGENTS.md §1 纳入"多算子冷启动放置"作研究内容——**暂缓（用户决定 4 不急）**。

## 8. 当前决定状态与当务之急（2026-07-31）

**学长原则（最高优先级）**：先定 benchmark/workload——场景先被认可，里面任何优化得到正指标都被接受；找 workload 本身就是最重要的研究行为。

因此优先级顺序（之前文档写反了，现修正）：

1. 🔴 **当务之急 = 锁 benchmark + workload**（定义一个被认可的场景），**与冷启动/机制无关**。这是当前要做的第一件事。
2. ⏸ 冷启动（痛点②，机制候选）parked——后面做，不阻塞 workload 锁定。
3. ⏸ 题目精修暂缓。

- ✅ workload validate：图像链路能暴露 CPU/GPU 阶段失衡；❌ 具体瓶颈与相对 staged
  系统的贡献尚未 validate。
- 下一步：**立刻锁一个 workload**（见 §5 推荐 + §10 当务之急行动），再谈机制。

## 10. 当务之急行动（lock workload，不依赖冷启动）

**历史候选（学长反馈，2026-07-31）**：数据路径可拆为两段——
1. 送到 vLLM（prompt→vLLM）——**做的人很多、拥挤**（vLLM + 一堆 serving 论文）。
2. **从 DB 读出来 + CPU 处理/搬到 GPU 侧**——做的人少，是机会。

当前文本实验的主要墙钟在 vLLM serving。选择图像 workload 的目的是增加可观测的
DB bytes、CPU processor、host copy/H2D 和 GPU forward stage，而不是预注册“传输必然
成为瓶颈”。

按此判据筛，**首选图像 AI_EMBED (CLIP)** 作为第一个 workload（之前误推 MS MARCO，已纠正）：

- **阶段成本可测**：每图含 JPEG bytes、CPU decode/processor、约 600KB FP32 tensor、
  host copy/H2D 和 GPU forward；具体主限制由 R0→R4 阶梯判定。
- **go/no-go 门禁**：CPU prepare/GPU forward 比只用于判断是否值得做异构 E2E；
  不能单独证明 GPU idle、PCIe 或数据搬运饱和。
- **fit 18G 盘**：COCO val 5K（smoke ~1G）/ ImageNet 子集（formal 3-8G）。
- 复用：CLIP 走独立 HTTP endpoint（非 vLLM），复用项目 Ray→HTTP 机械；`@daft.cls` Native 作 baseline。

**MS MARCO 降级为"文本轻对照"**：仍是文本，token ID 紧凑，DB 读 + 搬运轻，**不满足判据**；仅作"文本下瓶颈不显现"的边界对照，不作首选。

### 10.1 评估口径：数据库 AI 算子论文（执行优化子方向）

**锚点 field**：数据库 AI 算子论文——LOTUS（PVLDB'25）/ Cortex AISQL（SIGMOD'26）/ GaussML（ICDE'24）/ Smart（VLDB J'25）/ Galois（SIGMOD'25）/ SemBench（PVLDB'26）。**不对标** vLLM/Sarathi（模型 serving 内部，非本层，项目不改那层）。

**本项目在 field 里的位置 = 执行优化子方向**（与 LOTUS 的语义优化互补）：

| 杠杆 | 代表 | 优化什么 | 结果 |
|---|---|---|---|
| 语义/计划优化 | LOTUS / Smart / Abacus | 少调模型、重写计划 | cost↓、调用数↓（accuracy 不变） |
| **执行优化** ⭐本项目 | Daft-on-Ray / Ray Data / PolarDB Lakebase | **在已有 overlap/backpressure 之上的 work-aware / state-aware / multi-job 调度** | **JCT/SLO/fairness/资源效率改善**（quality 不变） |

同领域、不同杠杆、互补——Related Work 里明确分工："语义优化有人做了（LOTUS），执行优化是空白（本项目）。"

**性能节报什么（按数据库 AI 算子论文口径，6 项）**：

| 项 | 指标 | 角色 |
|---|---|---|
| ① accuracy（质量门禁） | recall@10 ≥ 0.95（ANN-benchmarks 协议） | 证明 embedding/写回没出错，**非卖点**（一句带过） |
| ② execution time / throughput | embeddings/s + 墙钟 | **主卖点**：执行优化让吞吐高 X× |
| ③ 时间分解（阶段拆解） | DB 读 + CPU decode + GPU embed + 搬运 + 写回 | LOTUS 式阶段表，定位瓶颈在哪 |
| ④ cost | $/M embeddings + GPU-hours + J/1k | 数据库 AI 算子论文标配 |
| ⑤ vs baseline speedup | 相对 Daft native / naive 的 X× | GaussML/Smart 式"X× faster" |
| ⑥ scaling（可选） | 数据量增长曲线 | SemBench/GaussML 式 |

**Benchmark 现状（诚实）**：
- 数据集：ImageNet-1K 子集 / COCO（公开经典，可引）。
- 质量协议：ANN-benchmarks recall@10（CCF 认可，作**质量门禁**，非主指标）。
- **执行层吞吐/搬运协议：无现成 benchmark**（survey 附录 B：厂商全闭源、无人 benchmark 上游 DB→GPU 搬运）→ 项目 §7.5 自定干净合同（feeding ≥95% bounded + 1 warmup + 3 formal + CV），**自定本身是贡献**（填补空白）。
- 可引名字：BigVectorBench（VLDB'25）image 切片 + ANN-benchmarks。

**Baseline 分两类（关键区分：直接对比 vs 只定位）**：

**A. 直接 baseline（必须跑 + 数字对比，同任务/同杠杆）**：

| Baseline | 角色 |
|---|---|
| **Daft fused `@daft.cls` Native/Ray** | 已完成的粗资源边界对照；不是 PolarDB staged 同款 |
| **Daft-on-Ray staged CPU→GPU** | ⭐ PolarDB/Daft 官方异构流水线强 baseline；必须补 |
| **OceanBase AI_EMBED**（数据库原生算子，**无 Daft/Ray**） | **产品级核心 baseline**——DB 触发外部 CLIP endpoint 再写回；完整矩阵见 `experiments/plans/database_ai_operator_baseline_matrix_20260729.md`（B1 门禁已过函数存在性 CE 4.5.0，当前 AutoDL 容器部署受阻、待可部署环境，见 `experiments/results/oceanbase_b1_gate_20260731/`） |
| Ray Data staged | 另一数据引擎的 streaming batch 强 baseline |
| naive 串行 / 固定行 pipeline | 工程默认对照 |
| bounded direct CLIP | 物理上界（绕 Daft/Ray 直连打满） |

**B. Related Work（只引用 + 定位，不比数字，不同杠杆）**：

| 系统 | 为什么不直接比 |
|---|---|
| LOTUS / Palimpzest / Abacus | 杠杆=语义/计划优化（少调模型），指标是调用数/cost，非执行吞吐 |
| Cortex AISQL / Oracle | 闭源 + 优化查询计划，非执行调度 |
| Smart / GaussML | SQL 重写 / 算子实现，非 DB↔GPU 执行调度 |
| SemBench | benchmark（给 workload），不是要比的系统 |

**审稿人问"怎么不跟 LOTUS 比"**：LOTUS 优化调用数/语义（不同杠杆），本文优化
执行调度；直接性能对照必须覆盖同杠杆的 Daft-on-Ray staged、Ray Data staged、
OceanBase/同 PostgreSQL direct control 与冻结最佳项目静态点。

**不能声称**：在 ANN-benchmarks 排行榜比向量检索（那是 Milvus/Faiss/pgvector 的事，非本层）；recall@10 是性能主指标（它是质量门禁）。

### 10.2 升级路径（benchmark 名始终 BigVectorBench）

CLIP（image 切片，当前）→ +MS MARCO（text 切片，作对照）→ +LibriSpeech audio → 三模态触发冷启动 regime（若解封）。

## 9. 证据来源（一手）

- Daft `@daft.cls` 源码：`github.com/Eventual-Inc/Daft/blob/main/daft/udf/__init__.py`（L360-410，gh api 读取）
- Daft cls 文档：`docs.daft.ai/en/stable/custom-code/cls/`
- Daft dynamic batching：`eventual.ai/blog/introducing-dynamic-batching-auto-tuning-for-daft-pipelines`
- RFC #5904：`github.com/Eventual-Inc/Daft/discussions/5904`
- Issue #2900（多 actor-pool 节流）、Issue #4603（Kafka 流式 open）
- Swordfish：`daft.ai/blog/exploring-daft-swordfish-execution-mechanism`
- 先验：Ray Data Streaming Batch (arXiv:2501.12407)、Pollux (OSDI'21)、Clockwork (OSDI'20)、Llumnix (OSDI'23)、INFaaS (ATC'21)、Chiron (OSDI'24)、Autellix (arXiv'25)、TORTA (arXiv:2507.10259)、Helium (arXiv:2603.16104)、arXiv:2306.13835、NVIDIA Run:ai GPU memory swap

> 证据分级：Daft 行为 = **本地事实**（源码 + 官方文档一手核实）；先验论文 = **文献事实**（TORTA/Helium/Sia 等标 uncertain，待 PDF 核验）；可防御界面与贡献排序 = 基于先验距离的**合理推断**；fatal-flaw regime = 基于硬件约束的**本地事实**。
