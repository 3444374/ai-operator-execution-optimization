# 知识库总汇：数据库驱动 AI 工作负载的上游执行链路优化

生成日期：2026-07-16（2026-07-17 更新：新增 §10 Daft+Ray 多模态与具身智能）
用途：集思广益入口——快速定位任何设计问题对应的参考资料、已知结论和待研究问题。
涵盖：vLLM 机制 + Ray 架构 + 57 篇文献（四个研究岛）+ 策略设计 + 实验证据 + 知识缺口 + Daft+Ray 多模态延伸

---

## 阅读指南

| 我想知道... | 跳转到 |
|---|---|
| vLLM continuous batching 怎么工作的？调度器内部是什么样？ | [§1 vLLM 机制](#1-vllm-机制详解) |
| vLLM 暴露了什么信号？怎么抓 queue depth？ | [§1.2 vLLM 可观测性](#12-vllm-可观测性) |
| vLLM APC 怎么利用？上游怎么 group 请求提高命中率？ | [§1.3 Prefix Caching](#13-vllm-prefix-caching) |
| Chunked prefill 和上游策略的安全边界？分组策略怎么选？ | [§1.4 安全边界](#14-chunked-prefill-与上游策略的安全边界) / [§1.5 分组策略](#15-分组策略设计空间length-align-vs-bin-packing) |
| Ray actor 怎么写 async loop？怎么去中心化？ | [§2.1 Ray Actor 模式](#21-ray-core-actor-模式) |
| Ray Serve batch_size_fn 支持 token 吗？ | [§2.2 Ray Serve Batching](#22-ray-serve-动态-batching) |
| Ray + vLLM 怎么集成？PrefixCacheAffinityRouter 是什么？ | [§2.3 Ray + vLLM](#23-ray--vllm-集成模式) |
| 已有文献的全景地图是什么？四个研究岛各有什么？ | [§3 文献全景地图](#3-文献全景地图) |
| 研究空白究竟在哪里？怎么证明？ | [§4 三个岛之间的空白](#4-三个岛之间的空白) |
| 从文献中提取了哪些设计原则？ | [§5 文献提取的设计原则](#5-文献提取的设计原则) |
| 预研实验有什么证据？边界在哪里？ | [§6 本项目已有实验证据](#6-本项目已有实验证据) |
| 当前策略版本是什么？实验怎么设计？ | [§7 策略设计与实验路线](#7-策略设计与实验路线) |
| Baseline 怎么分级？ | [§7.3 Baseline 分级](#73-baseline-分级) |
| 缺什么？下一步该查什么？ | [§8 知识缺口](#8-知识缺口) |
| 所有参考文件在哪里？ | [§9 文件清单](#9-文件清单) |
| Daft+Ray 多模态是什么？和本课题什么关系？ | [§10 Daft+Ray 多模态与具身智能](#10-daftray-多模态执行引擎与具身智能负载) |

---

## 1. vLLM 机制详解

**详细手册**：`research/vllm_continuous_batching_reference.md`

### 1.1 Continuous Batching 调度循环

每步 GPU forward 分三阶段：
1. **Schedule**：先给 decode 请求分配 slot，再从 waiting queue 取 prefill 请求，分配 KV cache block
2. **Forward Pass**：构造 batch（decode 各 1 token + prefill N tokens），一次 `model.forward()`
3. **Post-process**：完成的移出 running，被 preempt 的回 waiting queue

关键参数：
- `max_num_seqs`：同时 running 的最大请求数（默认 256）
- `max_num_batched_tokens`：单次 forward 最大 token 数（含 prefill + decode）
- `max_model_len`：单请求最大 context 长度
- `block_size`：KV cache 页大小，默认 16 tokens

**对我们的意义**：上游 Ray actor 提交行为直接影响这三个约束。batch 太大 → 超 `max_num_batched_tokens`；K_max 太高 → 超 `max_num_seqs` 排队。

### 1.2 vLLM 可观测性

三个核心 Prometheus 信号：

| 指标 | 含义 | 上游 actor 用法 |
|---|---|---|
| `vllm:num_requests_running` | GPU 上正在跑的请求数 | 接近 `max_num_seqs` → 暂停提交 |
| `vllm:num_requests_waiting` | 排队等待的请求数 | 持续 >0 → 降低速率 |
| `vllm:gpu_cache_usage_perc` | KV cache 使用率 | 接近 100% → 停止提交 |

获取方式：`http://<vllm_host>:8000/metrics`（Prometheus 格式），vLLM 无非 Prometheus 的 queue depth API。

### 1.3 vLLM Prefix Caching

- 16-token block → SHA-256 哈希 → 内容寻址
- 新请求从 token 0 匹配，第一个 hash miss 即停止
- 只有完整 block 可缓存，LRU 淘汰 + reference counting

**上游如何利用**：共享 system prompt 的行合并为一个请求 → APC 命中率最大化；并发提交共享 prefix 的请求 → 多请求同时命中同一批 cached blocks。

### 1.4 Chunked Prefill 与上游策略的安全边界

**详细论述**：`experiments/plans/data_organization_batching.md` §2.5.7；vLLM deep-research 验证报告（2026-07-20）。

**核心区分**（事实，来源：vLLM 官方文档 + SOSP'23 论文）：

| 操作 | 机制 | 语义影响 |
|---|---|---|
| **vLLM `--enable-chunked-prefill`** | 同一请求内部，prefill token 分多个 chunk 与 decode 交错执行；KV cache 连续累积；完整注意力 | ✅ 数学等价（贪婪解码下输出一致） |
| **手动拆分一份文档为多条请求** | 多条独立请求，KV cache 互不共享（默认），上下文隔离 | ❌ 语义断裂——后半段看不到前半段 |

**对上游策略的约束**（推断）：
- 上游 Daft/Ray 层的 token-budget 策略决定"多少行合并为一个 batch"——每行仍是独立完整的请求
- **禁止**在 Ray actor 中自动拆分单行 prompt 内容为多条 vLLM 请求（即使该行 token 量超过 budget）
- 超长单行的正确处理：预处理截断（truncate）、独占 batch、或从数据集中排除
- 正确的批量模式：多条**互不相关的独立任务**合并为一个 batch 提交（等效于 vLLM 的批量请求列表）

**与 prefix-aware grouping 的关系**：
- prefix-aware 分组是将共享 system prompt 的独立请求合并提交以利用 APC —— 这是**正确的优化**（每行仍是独立任务）
- 它不是"把一份文档拆成多段"——每行仍然是完整的独立请求，只是利用 APC 共享前缀计算
- 与 chunked prefill 的关系：prefix-aware 操作在 request 粒度（哪些请求一起提交），chunked prefill 操作在 token 粒度（单个请求内部如何计算）——两者在不同层面，互补

### 1.5 分组策略设计空间：Length-Align vs Bin-Packing

**详细论述**：`experiments/plans/data_organization_batching.md` §2.5。

两种 token-budget 驱动的分组策略（操作在"如何选择行放入同一 batch"，而非"如何切割行内文本"）：

| 策略 | 机制 | 与 vLLM chunked prefill 的协同 |
|---|---|---|
| **A: Length-Align** | 相似 token 长度的行分入同一 batch | 长 batch 内无短 decode 可交错 → chunked prefill 优势减弱 |
| **B: Bin-Packing** | 混合不同长度，使每个 batch 总 token 量均衡 | 天然混合 prefill+decode → chunked prefill 最优场景 |

推荐主推 B（Bin-Packing），A 保留为消融对比（尤其在异构 actor pool 场景下）。详见 `data_organization_batching.md` §2.5.6。

---

## 2. Ray 架构设计空间

**详细手册**：`research/ray_actor_dynamic_batching_reference.md`

### 2.1 Ray Core Actor 模式

去中心化自适应提交的核心：

```python
@ray.remote
class AdaptiveSubmitActor:
    def __init__(self, token_budget=4096, vllm_metrics_url="..."):
        self.buffer = []; self.current_tokens = 0
        self.token_budget = token_budget

    async def get_queue_depth(self):
        # 抓 vLLM Prometheus metrics, 解析 num_requests_running + waiting

    async def should_flush(self):
        running, waiting = await self.get_queue_depth()
        if running == 0 and waiting == 0: return True   # GPU 饥饿，立刻发
        if running > 200: return False                   # 接近 max_num_seqs
        if self.current_tokens >= self.token_budget: return True
        return False
```

关键机制：Stateful actor（buffer 在内存中）、Async loop（协程让出控制权）、去中心化（无需中央 scheduler）。

### 2.2 Ray Serve 动态 Batching

`@serve.batch` 参数：`max_batch_size`、`batch_wait_timeout_s`、`batch_size_fn`（2024 新增，**支持按 token 数而非请求数计算 batch size**）。

对我们：`batch_size_fn` 可直接用于 token-budget batching；`batch_wait_timeout_s` 的思路可迁移到上游 actor 攒批超时。

### 2.3 Ray + vLLM 集成模式

- Ray 2.44+：`LLMConfig` / `LLMServer` / `LLMRouter` 原生集成
- **Ray 2.49+ PrefixCacheAffinityRouter**：按 prefix hash 路由到同一 vLLM replica，TTFT 降低 60%，吞吐提升 40%+。**可作为 prefix-aware batching 的 baseline 对照。**

---

## 3. 文献全景地图

涵盖 57 篇论文 + 产业系统，分为四个研究岛。完整清单见 `opening/literature/ai_operator_literature_inventory.md`。

### 3.1 岛一：数据库 AI 算子与 DB4AI

**核心论文（CCF-A）**：

| 论文 | 出处 | 核心贡献 | 与我们的关系 |
|---|---|---|---|
| **Cortex AISQL** | SIGMOD 2026 | 六大 AI SQL 算子生产系统；AI-aware 查询优化、模型级联、语义 Join 重写 | 场景定义来源；闭源不可拆分，不能作为实验 baseline |
| **Smart** (Guo, Li et al.) | VLDB Journal 2025 | SQL+ML 谓词推理重写和成本最优执行，PostgreSQL 实现，最高 1000× | DB4AI 路线代表；优化止于数据库内核 |
| **GaussML** (Li et al.) | ICDE 2024 | 20+ ML 算子进 openGauss 查询引擎，SIMD 加速，2-6× vs MADlib | DB4AI 最强工程实现；华为+清华 |
| **NeurDB** (Zhao, Ooi et al.) | CIDR 2025 | AI 原生数据库系统蓝图 | AI×DB 融合远景 |
| **LEADS** (Zeng, Ooi et al.) | VLDB 2024 | SQL-aware 动态模型切片，PostgreSQL 实现 | MoE + DB 结合 |
| **Galois** (Satriani, Papotti et al.) | SIGMOD 2025 | LLM 作为存储层的 SQL 执行 | 挑战"算子下推最优"认知 |
| **InferDB** (Salazar-Díaz et al.) | VLDB 2024 | 用索引实现轻量数据库内推理，延迟降 2-3 个数量级 | 轻量 DB 内推理 |
| **SmartLite** (Lin, Li et al.) | VLDB 2024 | DBMS 原生 NN 算子，边缘场景 | 资源受限 DB+AI |

**关键区分**：这条路线是"把模型拉进数据库"（DB4AI），我们走的是"数据库触发后经外部系统执行 AI 再回来"——两种路线适用场景、瓶颈形态、优化方法互不相同。

### 3.2 岛二：GPU 推理服务系统

**Continuous Batching 核心技术线**：

| 论文 | 出处 | 核心机制 | 与我们的关系 |
|---|---|---|---|
| **vLLM** | SOSP 2023 Best Paper | PagedAttention + Continuous Batching，>96% KV cache 利用率 | 部署平台，不修改其内部 |
| **Orca** | OSDI 2022 | Iteration-level scheduling 开山之作，GPT-3 175B 上 36.9× | 不研究上游如何组织数据 |
| **Sarathi-Serve** | OSDI 2024 | Chunked prefill + stall-free scheduling，2.6-5.6× | 不控制请求到达粒度 |
| **FastServe** | arXiv 2023 | 抢占式 MLFQ 调度，skip-join 优先 | 调度策略参考 |
| **DistServe** | OSDI 2024 | Prefill-Decode 分离，消除阶段干扰 | 仅优化 GPU 内部 |
| **Splitwise** | ISCA 2024 | 阶段分离降功耗成本 | 仅 GPU 内部 |
| **Mooncake** | FAST 2025 Best Paper | KV cache 中心化 disaggregated 架构 | KV cache 分布架构 |
| **S-LoRA** | MLSys 2024 | 并发 LoRA adapter 服务，统一 batching | 多租户参考 |

**调度与自适应批处理**：

| 论文 | 出处 | 核心机制 | 与我们的关系 |
|---|---|---|---|
| **Clipper** | NSDI 2017 | AIMD 自适应 batching | 调度思想来源 |
| **Nexus** | SOSP 2019 | Squishy bin packing，batch-aware GPU 集群调度 | batch 作为一等调度维度 |
| **Clockwork** | OSDI 2020 | 确定性 DNN 延迟调度，弃用线程池和 OS 调度 | bounded in-flight 对照 |
| **Triton** | NVIDIA | 工业动态批处理 | 工程参考 |
| **INFaaS** | ATC 2021 | 自动化模型变体选择和资源配置 | 模型选择参考 |

**Prefix/Token-Aware 优化**：

| 论文 | 出处 | 核心机制 | 与我们的关系 |
|---|---|---|---|
| **Parrot** | OSDI 2024 | Semantic Variable 抽象，跨请求 prompt 共享 | prefix-aware 设计参考 |
| **SGLang** | NeurIPS 2024 | RadixAttention，结构化解码，prefix caching | prefix-aware 设计参考 |
| **KVFlow** | NeurIPS 2025 | 工作流感知 prefix caching | prefix 优化参考 |
| **ChunkAttention** | ACL 2024 | Prefix-aware self-attention kernel | kernel 级参考 |

**Pipeline 并行**：GPipe (NeurIPS 2019, micro-batch 拆分)、PipeDream (SOSP 2019, 1F1B 调度)、Alpa (OSDI 2022, inter/intra-operator 自动化)。

### 3.3 岛三：分布式数据管线与执行框架

| 论文/系统 | 出处 | 核心内容 |
|---|---|---|
| **Ray** | OSDI 2018 | task/actor 统一抽象、分布式调度、对象存储，AI 应用框架 |
| **Ray Data Streaming Batch** | arXiv 2025 | CPU/GPU 异构批处理管线，3-8× 吞吐 |
| **Daft** | 官方文档 | partition/batch/shuffle/join，Ray runner |
| **Spark SQL** | 官方文档 | partition tuning、coalesce、adaptive query execution |
| **Velox** | VLDB 2022 (Meta) | C++ 向量化执行引擎，Presto/Spark/PyTorch 统一执行层 |
| **DuckDB** | SIGMOD 2019 | 嵌入式分析数据库 |
| **Arrow DataFusion** | SIGMOD 2024 | Arrow-native 查询引擎 |
| **Arrow Flight** | arXiv 2022 | 高性能列式数据传输 |

**Ray 调度思想到策略变量的映射**（来自 `opening/literature/gpu_scheduler_data_placement_supplement_20260715.md`）：

| Ray 机制 | 可迁移策略 | 不可过度声称 |
|---|---|---|
| task/actor 统一 | 拆成无状态 task + 有状态 actor | 不声称重新设计 task/actor 模型 |
| local scheduler 优先 | 优先本 pool 提交，积压再换 endpoint | 不改 Ray 内部调度器 |
| resource-aware scheduling | CPU/GPU/连接数/线程资源约束 | 不写成通用集群管理 |
| data locality | 减少小 object、跨 worker fan-in | 不能只凭文献断言，需本地实验 |
| actor for stateful service | actor 表示 endpoint，维护队列 | 贡献在策略选择，不在 actor 本身 |

### 3.4 岛四：AI 数据存储与写回优化

| 论文 | 出处 | 核心内容 | 与我们的关系 |
|---|---|---|---|
| **Lance** | arXiv 2025 | AI/ML 列式存储，自适应结构编码 | AI 数据 sink 候选 |
| **ColStorEval** | PVLDB 2023 | Parquet/ORC 列式存储写入性能系统对比 | sink 格式选择量化依据 |
| **TurboVecDB** | PVLDB 2025 | 并行 I/O + 空间感知插入，HNSW 索引构建 -98.4% | 向量索引优化 |
| **Delta Lake** | PVLDB 2020 | Optimistic concurrency + 盲追加，多 worker 并行写入 | worker-direct writeback 直接参考 |
| **FlexPushdownDB** | PVLDB 2021 | Compute-vs-storage pushdown 代价决策模型 | 写回 pushdown 参考 |
| **WiscKey** | FAST 2016 | KV 分离，避免 compaction 对大 value 重写 | 写回批量参考 |
| **DiskANN** | NeurIPS 2019 | 单节点十亿级近邻搜索 | 向量检索参考 |
| **Milvus** | SIGMOD 2021 | CPU/GPU 混合查询引擎 + LSM-tree | 向量存储参考 |
| **Manu** | VLDB 2022 | 存算分离 + log-structured 写入 | worker-direct 对照 |
| **VBASE** | OSDI 2023 | Relaxed monotonicity，vector + relational 统一 | selectivity 感知队列管理 |
| **BigVectorBench** | VLDB 2025 | 向量数据库评测方法论 | 评测方法参考 |
| **AIDB** | DEEM@SIGMOD 2024 | 稀疏物化数据库 | 写回策略参考 |
| **Rafiki** | PVLDB 2018 | ML as Analytics Service | 外部执行+写回参考 |

### 3.5 综述与 Tutorial

| 论文 | 出处 | 内容 |
|---|---|---|
| **LLM for Data Management** (Zhou, Li, Zhao) | VLDB 2024 Tutorial | Data+AI 全貌，李国良组 |
| **Database Perspective on LLM Inference** (Pan, Li) | VLDB 2025 Tutorial | 推理系统 DB 视角，李国良组 |
| **Trustworthy LLMs Meet Databases** (Kim, Ailamaki) | VLDB 2024 Tutorial | LLM+DB 可信性 |
| **Vector DBMS Tutorial** (Lee et al.) | VLDB 2024 | 向量数据库全貌 |
| **Learned Query Optimizer** (Zhu et al.) | SIGMOD 2024 | 学习型优化器综述 |
| **Learning Database Optimization** (Qiao et al.) | FCS 2025 | 数据库优化技术综述 |

### 3.6 产业系统（需求证据，非论文）

| 系统 | 关键能力 | 对本课题的作用 |
|---|---|---|
| Snowflake Cortex AI | `AI_EMBED`, `AI_COMPLETE`, `AI_FILTER`, `AI_CLASSIFY`, `AI_JOIN`, `AI_AGG` | 场景定义来源 + 工业需求证据 |
| BigQuery ML/AI | `ML.GENERATE_TEXT`, `ML.GENERATE_EMBEDDING` | 工业需求证据 |
| Oracle AI Vector Search | `VECTOR_EMBEDDING` | 工业需求证据 |
| pgai (Timescale) | PostgreSQL + vectorizer worker + embedding endpoint + 写回 | 外部执行链路的工程合理性参考 |
| PostgresML | PostgreSQL 内/近数据库 ML/AI | DB4AI 对照路线 |
| pgvector | PostgreSQL 向量相似度检索 | 写回 sink 之一 |

### 3.7 CCF 等级统计

| CCF 等级 | 数量 | 主要来源 |
|---|---|---|
| CCF-A 会议/期刊 | 37 | SIGMOD×8, VLDB/PVLDB×14, ICDE×1, SOSP×1, OSDI×6, NeurIPS×2, EuroSys×1, ACM TOS×1, VLDB Journal×1, ISCA×1, FAST×2 |
| 顶会（非 CCF） | 1 | CIDR 2025 |
| 综述 | 1 | Frontiers of CS |
| 预印本/arXiv | 3 | DeepSeek-V3, Ray Data, Lance |
| 工业论文/官方文档 | 8 | Arrow Flight, Daft, Spark, Snowflake, BigQuery, Oracle, pgai, PostgresML, pgvector, vLLM |
| 自引 | 3 | 本项目 GPU-backed E2E |
| **合计** | **57** | |

---

## 4. 三个岛之间的空白

### 4.1 "三岛"模型

```
岛 1: DB4AI                   岛 2: GPU 推理服务             岛 3: AI 数据存储
Cortex AISQL (SIGMOD '26)    vLLM (SOSP '23 Best)         Lance (arXiv '25)
Smart (VLDB J '25)           Orca (OSDI '22)              pgvector
GaussML (ICDE '24)           Sarathi-Serve (OSDI '24)     Delta Lake (VLDB '20)
NeurDB (CIDR '25)            SGLang (NeurIPS '24)         TurboVecDB (VLDB '25)
LEADS (VLDB '24)             DistServe (OSDI '24)         Milvus (SIGMOD '21)
        │                            │                            │
        └────────────────────────────┼────────────────────────────┘
                                     │
                    本课题：数据库触发 → Ray 动态 Batching →
                    异构 Actor Pool + 去中心化自适应提交 →
                    vLLM Continuous Batching → 写回瓶颈判定
                    （三个岛连接处的上游执行链路优化）
```

### 4.2 空白双重确认

1. **2026-07-16 多源检索**（WebSearch × 8）：无 CCF-A 论文直接研究 pipeline batching × continuous batching 交互
2. **2026-07-16 系统性收集**（16 轮检索，28 篇论文）：空白确认

### 4.3 最接近的已有工作（需在论文中区分）

| 论文 | 研究什么 | 不研究什么 |
|---|---|---|
| Ray Data Streaming Batch (2025) | CPU/GPU 异构批处理管线 | 下游 continuous batching 反馈 |
| NeuStream (EuroSys 2025) | DNN 流管线批处理 | LLM token/prefix 需求 |
| HedraRAG (SOSP 2025) | RAG 中 CPU/GPU 协调 | 仅 RAG，非通用 AI SQL |
| Parrot (OSDI 2024) | Semantic variable prompt 共享 | 仅 GPU 侧，不涉及上游 |
| Clipper (NSDI 2017) | AIMD 自适应 batching | 不涉及 LLM、token、continuous batching |

### 4.4 不能声称的结论

1. 不能说"现有研究没有关注数据库 AI 算子"——Snowflake SIGMOD 和 Smart/GaussML/NeurDB 已充分证明
2. 不能说"外部执行一定优于数据库内 ML"——取决于场景
3. 不能说"Ray/Daft/Lance 是数据库 AI 算子的标准方案"——Snowflake 和 GaussML 用不同技术栈
4. 合理表述："在数据库触发 AI workload 后经由外部系统执行并写回的场景中，上游数据组织、调度提交与下游 continuous batching 之间的交互优化尚缺乏系统研究"

---

## 5. 文献提取的设计原则

### 5.1 从 vLLM/Orca/Sarathi 提取

- **Continuous batching 是下游给定机制**，上游目标不是替代它，而是给它最优的请求流
- **按 token 预算而非请求数做 batch**：借鉴 `max_num_batched_tokens`，上游也应以 token budget 分组
- **并发提交优于一次大 batch**：vLLM 推荐并发提交独立请求，不手动合并——验证了多 actor 独立提交架构

### 5.2 从 Clockwork/Nexus/Clipper 提取

- **确定性调度优于乐观并发**：Clockwork 的弃用线程池思路 → 上游应主动控制而非被动等待
- **Batch size 是一等调度维度**：Nexus 的 batch-aware scheduling → 上游 token-budget 不只是"参数调优"
- **AIMD 自适应**：Clipper 的加性增/乘性减 → queue-adaptive flush 的调节策略参考

### 5.3 从 Cortex AISQL/GaussML/Smart 提取

- **AI 算子不能按普通 UDF 估算**：selectivity、token length、model cost 会改变执行决策
- **AI-aware 优化的思想可迁移**：虽然它们在内核做，但"感知算子特征来选择策略"的原则适用于外部执行链路
- **写回是可选优化而非必须**：Cortex AISQL 不暴露写回阶段——说明研究空白在写回与上游的交互处

### 5.4 Ray 调度思想的策略迁移

| Ray OSDI 2018 机制 | 可迁移到本课题 |
|---|---|
| task/actor 统一 | AI 算子执行拆为无状态数据处理 task + 有状态模型服务 actor |
| local scheduler 优先 | 优先本 actor pool 提交，积压再换 endpoint |
| resource-aware scheduling | CPU/GPU/连接数/线程的资源约束 |
| data locality | 减少小 object 和跨 worker fan-in（需本地实验验证） |
| actor for stateful service | actor 表示 endpoint，维护 buffer + 队列 + 观测 |

### 5.5 从 2025 年 LLM Serving 新文献提取（2026-07-21 新增）

以下 6 篇 2025-2026 年论文为项目文献搜索发现的新增来源，与 RC1（数据组织）和 RC2（提交控制）直接相关。详细内容见 `research/ray_actor_dynamic_batching_reference.md` §6.7-§6.12。

**从 CONCUR (2025) 提取**：
- **AIMD 可迁移到 request 级**：CONCUR 控制的是"活跃 agent 数"（粗粒度），我们可以把 AIMD 用到更细的 per-actor in-flight 请求数控制
- **KV cache 作为共享资源信号**：不只是队列深度，KV cache 使用率也应作为 K_max 调节的输入信号
- **Middle-phase thrashing**：长期运行的推理 session 在内存耗尽前就会出现吞吐退化——我们的 K_max 控制应有前馈能力，不只被动反应

**从 Scorpio (2025) 提取**：
- **VBS (Virtual Batch Size) Admission Control**：用 token 量（而非请求数）投影系统负载——我们的 token-budget batching 本质上就是 VBS 的一种实现
- **Credit-based Batching**：按 SLO 松紧分配 batching 机会——可迁移到我们的异构 workload 场景（不同优先级的 SQL 查询）

**从 SABER (2025) 提取**：
- **前瞻性准入判断**：不只检查当前队列，还要预测"如果现在提交，会不会导致 in-execution 请求违反 SLA"——我们的 K_max 调节应具有预测性
- **Universal Scalability Law 建模**：`生成速度 = f(并发请求数)`——可用 vLLM 的 profiling 数据拟合此函数，作为 K_max 调节的理论上界

**从 CoLoRA (2026) 提取**：
- **Load-Aware Batch Scheduling**：实时 GPU 利用率 + 队列深度 + adapter 状态 → 自适应 batch 形成——三维信号融合是我们的 queue-adaptive flush 的参考架构
- **Unified Scheduler 的全局反馈循环**：决策模块 + 执行模块 + 指标采集模块形成闭环

**从 BucketServe (2025) 提取**：
- **按序列长度分组降低 padding 开销**：与我们的 length-aligned grouping 思路一致——验证了"按计算量相似度分组"的有效性
- **自适应 bucket split/merge**：当 workload 分布变化时动态调整分组边界——可迁移到我们的 token-budget 分组边界的自适应调节

**从 ProServe (2025) 提取**：
- **两层调度架构验证**：SlideBatching（Engine 层 token 级）+ GoRouting（Service 层 request 级）——与我们的"内部 vLLM + 外部 Ray"两层架构同构，证明分层调度在该场景下是合理设计
- **Gain-oriented dispatching**：不仅看当前负载，还要预估未来收益——actor pool 分池路由可参考此思想

### 5.6 Ray 现存机制的能力边界（2026-07-21 新增）

经过对 Ray Core/Data/Serve 各层机制的详细审查，确认以下边界（详见 `research/ray_actor_dynamic_batching_reference.md` §3.7）：

**Ray 提供的 building blocks（可直接使用）**：
| 机制 | 类型 | 适用性 |
|---|---|---|
| `ray.wait()` 手动反压 | 应用层循环 | **RC2 K_max 控制的基础实现模式** |
| `max_concurrency` | Actor 配置 | 控制单 actor 并发上限 |
| `max_tasks_in_flight` + `should_add_input()` | 二元 slot 检查 | 可作为底层执行机制，但需包装为连续决策 |
| Queue-based autoscaling (Serve) | 池大小自适应 | 架构参考（monitor→decision→execution 闭环）|

**Ray 明确不提供的（需自建）**：
| 能力 | Ray 现状 | 我们的 gap |
|---|---|---|
| K_max 动态调节 | 所有限制都是静态的 | 从 vLLM metrics → EWMA 平滑 → AIMD 调节 |
| 队列深度感知 flush | `should_add_input` 是二元开关 | 连续队列深度 → flush 时机决策 |
| Token-budget 准入控制 | 无 | token 量估算 → 准入判断 |
| 多维信号融合决策 | Serve autoscaler 只看队列长度 | vLLM waiting + running + KV cache → 融合决策 |

**重要警示**：Ray Data 的 `ConcurrencyCapBackpressurePolicy`（EWMA + deadband 自适应并发控制）已被废弃——原因是用 ~400 行复杂控制逻辑实现的策略，性能反而不如简单方案。这对我们的设计有直接含义：**自适应策略必须保持简单，避免陷入参数调优的泥潭**。

---

## 6. 本项目已有实验证据

**预研目录**：`motivation/results/gpu/`

### 6.1 AI_EMBED 预研（手动 HTTP endpoint，非 vLLM）

| 实验 | 关键发现 | 边界 |
|---|---|---|
| GPU Chain Breakdown (7/12) | 1024 行 fine vs coalesced：37.5× | PG18.4，非 PG18.3 |
| PGAI-Integrated Rerun (7/14) | batch 粒度、写回、endpoint 复测 | 手动 CUDA endpoint |
| pgvector Writeback (7/14) | pgvector 0.897s vs JSON 1.567s | sink 对比，非最终方案 |
| 双 endpoint 动机测试 | 双 endpoint 降 operator wall，写回不变 | 单 GPU 两副本 |

### 6.2 预研证明与不能证明

**证明**：阶段计时方法可行、端到端链路可观测、batch 粒度是一阶变量。
**不能证明**：动态 batching 优于静态、prefix-aware 有效、Ray 去中心化优于中央调度。这些需要 AI_COMPLETE + vLLM 平台验证。

---

## 7. 策略设计与实验路线

**主文件**：`experiments/plans/strategy_design_literature_basis.md`（策略口径）、`experiments/plans/strategy_design_implementation_reference.md`（实现拆解）

### 7.1 当前策略版本

```text
上游动态 Batching Policy（Ray actor 异构化实现）
  ├── Token-budget batching：max_tokens_per_submission
  │    借鉴 vLLM max_num_batched_tokens
  ├── Length-aligned grouping：相似 token 长度行合并
  └── Prefix-aware grouping：共享 system prompt 行合并
       利用 vLLM APC

Ray Actor 去中心化自适应提交
  ├── 每个 actor 独立观测模型服务队列深度
  ├── Queue-adaptive flush：queue 空立刻发，queue 满暂停
  └── K_max 自然形成，不设全局固定上限

耦合验证
  ├── 独立最优 batching + 独立最优 submission → 拼接
  └── 联合 grid search → 比较差异

写回瓶颈判定
  └── COPY + deferred index 工程最优 baseline
```

### 7.2 实验阶段

| 阶段 | 内容 | 核心消融 |
|---|---|---|
| 前置 | vLLM + Qwen2.5-1.5B baseline | 替代手动 HTTP endpoint |
| 第一阶段 | 动态 batching 策略消融 | 静态 batch_size vs token-budget vs length-align vs prefix-aware |
| 第二阶段 | 自适应提交策略消融 | 固定 K_max vs queue-adaptive vs actor pool 分池 |
| 第三阶段 | 耦合验证 | 独立最优拼接 vs 联合 grid search |
| 第四阶段 | 写回瓶颈判定 | COPY + deferred index vs 其他 sink |

### 7.3 Baseline 分级

| 级别 | 定义 | 示例 |
|---|---|---|
| S 级 | 文献/工业最优 | vLLM continuous batching、COPY + deferred index |
| A 级 | 有工程常识的合理默认 | coalesced batch=64 + driver fan-in |
| 诊断工具 | 仅用于理解瓶颈 | 逐行调用（fine）、无界 in-flight |

---

## 8. 知识缺口

| 缺口 | 优先级 |
|---|---|
| vLLM + Qwen2.5-1.5B 在 RTX 5070 上的实际 TTFT/TPOT/吞吐曲线 | **P0** |
| AI_COMPLETE workload 具体构造参数（token 分布、prefix ratio） | **P0** |
| token-budget 的最优范围（2048/4096/8192） | P1 |
| Ray actor queue-adaptive flush 的实际效果（本地 vLLM 实验中发现 adaptive < static，需进一步分析——见 `PROJECT_LOG.md` 2026-07-20） | P1 |
| prefix-aware grouping 在真实 APC 下的命中率 | P1 |
| 单 GPU 下异构 actor pool 是否有意义 | P1 |
| batch_size × K_max 之外的交互通道 | P2 |
| 多模态 workload 的"token 等效量"定义（frame-budget / duration-budget） | P2 |
| VLM 推理在 RTX 5070 12GB 上的实际显存和吞吐（Qwen2.5-VL 系列） | P2 |
| **2026-07-21 新增/更新**： | |
| CONCUR (2025) AIMD-based admission control 的算法细节与迁移可行性 | **P1** |
| SABER Universal Scalability Law 建模在本课题 vLLM 场景下的拟合效果 | P2 |
| Ray ConcurrencyCapBackpressurePolicy 废弃的教训如何转化为我们的设计约束 | P1 |

---

## 9. 文件清单

**2026-07-21 更新**：
- `research/ray_actor_dynamic_batching_reference.md` — 新增 §1.6-§1.8（Ray Serve 准入控制与队列自适应）、§3.7 大幅扩展（7 种反压机制详述 + ConcurrencyCap 废弃分析）、§6.7-§6.12（6 篇 2025-2026 新论文）
- `research/knowledge_hub.md` — 新增 §5.5（6 篇新论文设计原则提取）、§5.6（Ray 现存机制能力边界）、§8 知识缺口更新

**2026-07-17 新增**：
- `research/knowledge_hub.md` — 本文件，新增 §10
- `research/daft_ray_multimodal_reference.md` — Daft+Ray 多模态技术手册与具身智能连接分析

**2026-07-16 新增**：
- `research/knowledge_hub.md` — 本文件
- `research/vllm_continuous_batching_reference.md` — vLLM 技术手册
- `research/ray_actor_dynamic_batching_reference.md` — Ray 架构模式手册
- `research/inference_pipeline_interaction_literature.md` — 28 篇推理管线文献综述

**已有文献与设计文件**：
- `research/literature_and_evidence_review.md` — Ray/Daft/Lance/Snowflake 综合证据
- `research/existing_ai_operator_execution_chains.md` — 现有 AI 算子执行链路对比
- `opening/literature/ai_operator_literature_inventory.md` — 57 篇 CCF-A 文献清单
- `opening/literature/gpu_scheduler_data_placement_supplement_20260715.md` — GPU 调度补充调研 + Ray 思想映射
- `opening/literature/direction_assessment_20260715.md` — 方向评估 + 三岛模型 + 不能声称的结论
- `opening/literature/reading_list.md` — 精读/泛读文献清单

**实验计划文件**：
- `experiments/plans/strategy_design_literature_basis.md` — 策略口径与文献依据
- `experiments/plans/strategy_design_implementation_reference.md` — 实现细节与模块拆解
- `experiments/plans/archive/research_design_catalog.md` — 方案目录与评分（已归档，设计历史参考）
- `experiments/plans/baseline_reference.md` — Baseline 矩阵
- `experiments/plans/data_organization_batching.md` — 研究内容一实验计划
- `experiments/plans/service_scheduling_backpressure.md` — 研究内容二实验计划
- `experiments/plans/sink_writeback_coordination.md` — 写回验证
- `experiments/plans/cross_layer_killer_experiment.md` — 耦合验证

---

## 10. Daft+Ray 多模态执行引擎与具身智能负载

**详细手册**：`research/daft_ray_multimodal_reference.md`

### 10.1 Daft 引擎核心架构

Daft 是一个 Rust 写核心 + Python API + Arrow 列式内存的分布式 DataFrame 引擎。2025 年 10 月发布新分布式引擎 **Flotilla**。

**关键架构特征**：

| 层级 | 组件 | 关键技术 |
|------|------|---------|
| API 层 | Python DataFrame / SQL | 惰性求值，LogicalPlan |
| 优化层 | Rule-based + Cost-based optimizer | 谓词下推、列裁剪、Join 重排、UDF 分离 |
| 执行层 | Swordfish（本地）/ Flotilla（分布式） | Morsel 驱动 Push 模型、Tokio 异步、Arrow 零拷贝 |

**Swordfish 流式执行引擎**：
- Morsel（微批次）粒度：数据以小块在 CPU/GPU/网络之间异步推送，不物化整个 partition
- 内置背压：下游 GPU 推理变慢时，上游自动减缓数据加载
- 三种 Pipeline Node：SourceNode（数据摄入）、IntermediateNode（数据处理）、BlockingSinkNode（需要全量输入的操作如 Aggregate）

**Flotilla 分布式架构（2025.10）**：
- "每节点一个 Swordfish Worker"模型：一个 Worker 控制该节点所有 CPU/GPU/内存/磁盘/网络
- Ray 被降级为资源管理层：Flotilla 自己的 Rust PlanRunner/Scheduler/Dispatcher 负责调度
- Driver → Scheduler（优先级队列）→ Dispatcher（批量派发）→ 各节点 RaySwordfishActor
- Hybrid Shuffle：Ray Object Store（内存内）+ Flight Shuffle（基于 Arrow Flight，可 spill 到 NVMe）

### 10.2 GPU 推理集成：@daft.cls UDF

```python
@daft.cls(gpus=1, max_concurrency=4, use_process=True)
class MyModel:
    def __init__(self):
        self.model = load_model()  # 每个 worker 加载一次

    @daft.method.batch(return_dtype=DataType.float32(), batch_size=32)
    def predict(self, inputs):
        ...
```

关键参数：`gpus=N`（预留 GPU）、`max_concurrency=M`（全局并发上限）、`use_process=True`（绕过 GIL）。

### 10.3 Daft vs Ray Data 对比与竞争

两者都做 CPU/GPU 异构批处理管线，彼此是最直接的竞品：

| 维度 | Daft（Flotilla） | Ray Data（Streaming Batch） |
|------|-----------------|---------------------------|
| 核心论文 | SciPy 2024 Talk（无正式论文） | arXiv:2501.12407（UC Berkeley/Anyscale） |
| 执行粒度 | Morsel 级（微批次），不物化 partition | Block 级（较大 partition），fused task |
| 资源管理 | 每节点一个 Worker 管控全局资源 | 异构集群独立扩展 CPU/GPU worker |
| 优势场景 | 小实例（4 CPU/GPU）、开箱即用 | 大实例（32 CPU/GPU）、大规模集群 |
| 调度架构 | 集中式（Driver/Worker，类似 Spark） | 集中式 Adaptive Scheduler |

**Benchmark 之争**（2025 年 10 月双方分别发布）：
- Daft 声称比 Ray Data 快 2-7×（8× g6.xlarge）
- Anyscale 反驳：Ray Data 在大实例（g6.8xlarge）和高 CPU:GPU 比下反超，大规模下快 7×
- 共识：小实例 Daft 更优，大实例 Ray Data 更优。独立评测强烈建议。

### 10.4 具身智能场景：为什么 Daft+Ray 适合

**数据特征**：具身智能模型训练需要来自真实物理世界的多模态感知数据——第一人称视角视频、深度传感器数据、力反馈信号等。单个机器狗巡检每天产生数百 GB 视频。

**Daft+Ray 解决的核心问题**：
1. 多模态数据（视频/图片/音频/Tensor）作为 DataFrame 的"一等公民"列类型
2. CPU 解码 + GPU VLM 推理重叠执行，GPU 不等待 I/O
3. Morsel 流式 + 背压，处理 PB 级数据不 OOM
4. 100+ 内置多模态算子（视频抽帧、OCR、人脸模糊、音频转写等）
5. `ai_query` 函数直接嵌入 VLM 推理调用，无需数据搬移

**典型管线**（以阿里云 EMR Serverless Daft 为例）：

```text
OSS 原始视频 → read_video_frames(采样关键帧) → encode_image(JPEG)
  → ai_query(Qwen-VL, "KEEP/DROP") → 删除低质量帧 → 写入数据湖
```

**实际落地**：
- 火山引擎 + 大小机器人（机器狗巡检）：CPU 利用率 40-60% → 100%，GPU 利用率 → 90%+
- 京东云 + GR00T-N1.5：单轮训练 15h → 22min（40×）
- 字节跳动：236 亿次 LLM 查询（24T tokens），90K GPU，零崩溃

### 10.5 与本课题的关系：互补而非竞争

Daft+Ray 和本课题解决不同层面的问题：

```text
┌─────────────────────────────────────────────────────────┐
│ Daft 做的事（引擎层）                                     │
│ - 多模态数据 → Arrow → morsel 流式 → GPU UDF → 写数据湖    │
│ - 优化：CPU/GPU 重叠、内存管理、分区策略、I/O 吞吐          │
└─────────────────────────────────────────────────────────┘
                        ↓ 数据经过 Daft 组织后
┌─────────────────────────────────────────────────────────┐
│ 本课题做的事（调度策略层）                                  │
│ - PostgreSQL → Arrow RecordBatch → 按 token 量组批         │
│ - 观测 vLLM 队列状态 → 自适应 flush 时机                    │
│ - 按 prefix hash 路由到亲和 actor                          │
│ - 优化：batch 构造规则 + 提交节奏决策 + 写回瓶颈判定         │
└─────────────────────────────────────────────────────────┘
```

**关键差异**：Daft 优化的是"数据流得是否顺畅"（引擎层），本课题优化的是"什么时候发、发多少、发给谁"（策略层）。Daft 不观测 vLLM Prometheus metrics 来做反馈驱动决策，不做 token-aware grouping，不关心数据库写回瓶颈——这些恰好是本课题的核心贡献。

**与具身智能的关联**：
- Snowflake Cortex AISQL 已支持多模态 AI 算子（AI_COMPLETE/AI_EMBED/AI_CLASSIFY 处理图片/视频/音频），数据库 AI 算子已是多模态的
- 本课题的调度策略框架的泛化能力：token-budget → frame-budget/duration-budget，queue-adaptive flush 不依赖数据模态
- 在论文 Discussion (§6) 中可将具身智能多模态数据处理作为 generalization case，不作为主实验

### 10.5.1 工程决策：Daft 文本阶段直接接入（2026-07-17 更新）

**决策（2026-07-17 修订）**：Daft 从文本阶段（AI_COMPLETE + vLLM baseline 建立后）直接作为数据引擎，不再经过 Arrow 中间态。Daft 的 DataFrame API 对文本（`df["prompt"]`）和图像（`df["image"]`）是同一套接口，后续多模态实验只需替换列类型，策略层代码不动。

**理由**：

1. Daft 对文本和图像提供统一的 DataFrame API + `@daft.cls` GPU UDF，不存在"文本先用 Arrow、多模态再切 Daft"的过渡期
2. Daft 的 `into_batches`、`repartition`、`batch_size`、`max_concurrency` 等引擎级参数是优化空间的一部分——"策略级决策 + 引擎级参数调优"共同构成论文的完整优化面
3. 多模态实验进入正文（§5.3 策略泛化性验证），不是仅 Discussion。Daft 的原生多模态支持使这成为可能
4. 策略层（token-budget、queue-adaptive flush、prefix-aware routing）不依赖底层引擎选择

**优化空间三层框架**：

```
策略级（本文贡献）：          引擎级（Daft 提供，本文系统表征）：
─────────────────────        ─────────────────────────────
token-budget batching        into_batches(N) / repartition(N)
length-aligned grouping      @daft.cls batch_size
prefix-aware grouping        @daft.cls max_concurrency
queue-adaptive flush         gpus 分配 / CUDA stream 并发
K_max 动态控制               shuffle_algorithm
actor pool 分池路由          morsel size（间接）
```

**论文中完整的优化实验清单**（详见 `experiments/plans/strategy_design_implementation_reference.md` §4.7）：

| 优先级 | 实验 | 变量 | 回答的问题 |
|---|---|---|---|
| P0 | batch 粒度对比 | batch_size vs token-budget | 按计算量组批是否优于按行数组批？ |
| P0 | 分组策略对比 | random vs length-align vs prefix-aware | 相似计算量的请求放一起是否减少 straggler？ |
| P0 | 提交节奏对比 | 固定 K_max vs queue-adaptive flush | 自适应提交是否有收益？ |
| P1 | Daft 引擎参数 | into_batches × @daft.cls batch_size | 分区粒度与 GPU UDF batch size 如何匹配？ |
| P1 | 耦合验证 | RC1*+RC2* 拼接 vs joint grid search | 联合调优是否必要？ |
| P2 | 多模态泛化 | 文本 token-budget vs 图像 frame-budget | 策略抽象的模态无关性是否成立？ |
| P2 | 算子代价估计 | 预测成本 vs 实际成本（MAPE < 20%） | profile-driven 成本预测是否可用？ |

**Scope 缩减触发条件**：
- Month 1 结束前 vLLM baseline 未建立 → 多模态降为 Discussion
- 文本 RC1+RC2 消融未完成前，不启动 Daft 多模态 pipeline
- VLM 生成实验（Qwen2.5-VL-3B）始终标记为 optional

### 10.6 Snowflake Cortex 多模态 AI 算子（工业需求证据）

Snowflake 2025 年已 GA 完整的多模态 AI SQL 算子：

| 算子 | 支持模态 | 状态 |
|------|---------|------|
| AI_COMPLETE | 文本 + 图片 + 音频 + 视频 | GA (2025.11) |
| AI_EMBED | 文本 + 图片（Voyage Multimodal 3） | Public Preview |
| AI_CLASSIFY | 文本 + 图片 | GA |
| AI_FILTER | 文本 + 图片 | Public Preview |
| AI_TRANSCRIBE | 音频 + 视频 | GA |
| AI_EXTRACT | 文本 + 图片 + 文档 | GA |

这证明了"数据库 AI 算子处理多模态数据"是工业界正在推进的方向。但 Snowflake 是闭源系统，其内部数据组织、批处理构造、模型服务交互和写回之间的阶段边界不可拆分——这正是本课题对外开放的研究空间。

### 10.7 关键参考资料

| 资料 | 类型 | 用途 |
|------|------|------|
| [Daft GPU Inference with @daft.cls](https://www.daft.ai/blog/gpu-inference-with-daftcls) | 官方博客 | @daft.cls UDF 机制、GPU 分配参数 |
| [Flotilla: Daft 新分布式引擎](https://www.daft.ai/blog/introducing-flotilla-simplifying-multimodal-data-processing-at-scale) | 官方博客 | Flotilla 架构、Ray 角色变化 |
| [Exploring Daft's Swordfish Execution](https://www.daft.ai/blog/exploring-daft-swordfish-execution-mechanism) | 官方博客 | Morsel Push 模型、Tokio 异步 |
| [Ray Data Streaming Batch (arXiv:2501.12407)](https://arxiv.org/abs/2501.12407) | 论文 | Ray Data 异构执行模型，3-8× 吞吐 |
| [Benchmarking Multimodal AI: Ray Data vs Daft](https://www.anyscale.com/blog/ray-data-daft-benchmarking-multimodal-ai-workloads) | Anyscale | 双方 Benchmark 之争 |
| [EMR Serverless Daft 具身智能实践](https://developer.aliyun.com/article/1747724) | 阿里云 | 视频抽帧→VLM 推理→标注的完整管线 |
| [Snowflake Cortex Multimodal](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-multimodal) | 官方文档 | 多模态 AI SQL 算子参考 |
| [HeteroHub: 多具身 Agent 数据管理](https://ar5iv.labs.arxiv.org/html/2603.28010) | arXiv 2025 | 具身智能数据管理分层架构 |
