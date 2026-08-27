# 知识库总汇：数据库 AI 负载的上游执行链路优化

生成日期：2026-07-16（最近更新：2026-08-27，校准 PostgreSQL+LOTUS 当前顺序、既有证据与知识缺口）
用途：集思广益入口——快速定位任何设计问题对应的参考资料、已知结论和待研究问题。
涵盖：vLLM 机制 + Ray 架构 + 分级文献基线（Top 15 / 核心补充 / 工程资料）+ 策略设计 + 实验证据 + 知识缺口 + Daft+Ray 多模态延伸

本文件是项目内部的设计、证据与历史状态索引，不是可直接复制到报告或答辩中的对外综述。
抽取内容到对外材料时，必须按根文档语言规则把内部阶段代号和管理状态改写为具体条件、动作与结果。

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

涵盖 65 篇论文 + 产业系统，分为四个研究岛。完整清单见 `research/ai_operator_literature_inventory.md`。

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
| **FastServe** | NSDI 2026 | token 粒度抢占式 MLFQ，按输入长度初始化 skip-join 队列 | serving 内部 prediction-light 调度参考；本项目不修改其内部 |
| **DistServe** | OSDI 2024 | Prefill-Decode 分离，消除阶段干扰 | 仅优化 GPU 内部 |
| **Splitwise** | ISCA 2024 | 阶段分离降功耗成本 | 仅 GPU 内部 |
| **μ-Serve** | USENIX ATC 2024 | GPU 频率缩放与模型 multiplexing 协同，优化功耗并维持吞吐/延迟 SLO | 能耗/资源成本评价参考，不是输出长度或 admission cost 模型 |
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

**Ray 调度思想到策略变量的映射**（来自 `research/gpu_scheduler_data_placement_supplement_20260715.md`）：

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
| **合计** | **65** | |

### 3.8 算子代价估计与查询优化（2026-08-04 补充）

算子代价估计不再只作为附录中的孤立预测任务，而是数据组织与调度提交控制共同依赖的重要组件；它仍不单独扩张成第三项研究内容。以下论文覆盖传统 learned cost model、UDF 感知、算子放置、semantic operator 优化和推理延迟预测：

| 论文 | 出处 | 核心内容 | 与项目代价估计的关系 |
|---|---|---|---|
| **Heinrich et al. — How Good are Learned Cost Models, Really?** | SIGMOD 2025 | 7 个 SOTA LCM vs PostgreSQL 传统代价模型；精度高但计划选择未必更好；**排序>精度**；建议 hybrid 架构 | **直接方法论支撑**：排序指标（Spearman/pairwise/Top-K）的理论依据；hybrid = 传统公式 + learned correction |
| **CONCERTO (Zhang et al.)** | arXiv:2412.00749v2，2025；暂无正式 venue | DAG + GAT + TCN 三阶段架构：每算子独立资源代价 → GAT 捕获并行资源竞争 → TCN 聚合代价向量 | 多 endpoint 场景的 DAG 建模方案备选 |
| **GRACEFUL (Wehrstein et al.)** | ICDE 2025 | UDF 感知 GNN 代价估计；CFG + 查询计划联合图 → GNN + MLP 预测 runtime；zero-shot 泛化到新 UDF | 项目文档标注为 closest analog to AI-operator-aware cost estimation |
| **COSTREAM (Heinrich et al.)** | ICDE 2024 | GNN 算子放置代价模型；zero-shot 泛化到未见查询/硬件；median 21× speedup | 多 GPU/多 endpoint 场景下算子放置优化的方法参考 |
| **Abacus (Russo et al.)** | PVLDB 2026 | 在 quality/cost/latency 多目标与约束下搜索 semantic operator 的 Pareto 计划 | 连接 AI 算子 work 估计、profile 复用和计划/配置选择 |
| **LOTUS (Patel et al.)** | PVLDB 2025 | 在准确率约束下选择代理模型、cascade、join/ranking 算法 | 证明“减少无效 work”和“相同 work 执行更快”必须分开评价 |
| **Palimpzest (Liu et al.)** | CIDR 2025（非 CCF-A） | 用 sentinel plans 采样 time/cost/quality，选择 Pareto 物理计划 | 小样本 profile 与多目标选择的补充来源 |
| **Neo** | SIGMOD 2019 | 经典 learned query optimizer（端到端） | 学习型优化器奠基工作，代价估计是其子模块 |
| **Pathak & Mankodi — Redefining Cost Estimation** | arXiv 2025 | 三类特征（标量/结构/语义）+ XGBoost → MSE 0.3002；树集成在低数据量下优于深度学习 | 特征工程方法参考，印证 Ridge 在 283 行上的选择合理 |
| **Learned Query Optimizer (Zhu et al.)** | SIGMOD 2024 | 学习型优化器综述 | 未下载（ACM 付费墙），作为领域全景引用 |
| **Learning Database Optimization (Qiao et al.)** | FCS 2025 | 数据库优化技术综述 | 已下载，作为领域全景引用 |
| **SFS — Beyond Accuracy and Cost (Patel et al.)** | arXiv 2026 | 用 serving-framework token-batch simulation 估计动态 workload 下的 TTFT，并联合优化 latency/quality/cost 路由 | 最接近“提交前 what-if 估计”的对照；但需要细粒度 running prefill/decode snapshot，粗粒度 Prometheus 指标不足以完整复现 |
| **TIE — Scheduling LLM Inference with Uncertainty-Aware Output Length Predictions (Zheng et al.)** | ICML 2026 | 不把输出长度当点值，而用重尾分布与 Tail Inflated Expectation 表达长输出风险 | 直接支持输出 work 使用分布/分位数；本项目不能只用 `completion_max_tokens` 或预测均值 |
| **Past-Future Scheduler (Gong et al.)** | ASPLOS 2025 | 用历史输出长度分布估计未来各时点显存占用，在排队与 eviction 间优化 SLA goodput | 支持同时预测 remaining work、KV/memory risk 与 goodput，而不是只预测总 JCT |
| **JITServe (Zhang et al.)** | NSDI 2026 | 在请求信息不精确时保守分配 serving bandwidth，并随生成进展逐步修正估计，以最大化 service goodput | 支持“初始区间 + online residual update”，以及按 token meeting SLO 评价决策 |
| **Beyond Prediction: Tail-Aware Scheduling (Li et al.)** | ICML 2026 | 指出长度预测调度在分布漂移、burst 和 GPU memory pressure 下可能脆弱，甚至 perfect length knowledge 也不能保证尾延迟最优 | 关键反证：预测精度不是充分条件；必须报告 P90–P99、回退率、OOD 与 oracle regret，并保留 prediction-free 强静态/尾部策略 |
| **FastServe (Wu et al.)** | NSDI 2026 | 在输出长度未知的 semi-information-agnostic setting 下，用输入长度初始化多级反馈队列并在 token 粒度抢占 | serving 内部的 prediction-light 对照；本项目不修改 vLLM，不能直接实现其抢占器，但应设置“不依赖输出预测”的稳健 baseline |

**关键 insight**：以上文献反复出现三个模式——(a) 简单解析/传统公式提供一阶估计；(b) profile 或 learned component 只修正残差并提供不确定性；(c) 模型是否有用最终由 plan/config ranking 与 regret 决定，而不是只看 MAPE。

**2026-08-04 新增边界**：TIE、Past-Future 与 JITServe 表明 output work 更适合表示为分布、分位数或随执行更新的区间；Beyond Prediction 与 FastServe 则说明 prediction-driven scheduler 不是默认更强。对本项目而言，可控 Ray 队列只让 `pre-submit wait`、held work 与提交动作更清楚，**不会消除提交后自然 EOS、continuous batching、KV/cache 和共享负载造成的 service 不确定性**。因此代价模型必须和无预测的强静态策略同场比较，并以 tail/SLO/regret 决定是否晋级。

**首版边界**：继续采用“简单解析模型 + profile 校准 + residual correction”，不直接实现复杂 learned optimizer。预测对象覆盖 prompt/output token work、operator service time、JCT、remaining work 和 SLO slack；决策对象覆盖 active-work/K 初始化、数据组织、endpoint 路由和提交策略。

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

### 4.2 空白口径复审

2026-07-16 的早期检索没有发现直接研究“数据库/数据引擎上游组织与 vLLM continuous batching 协同”的正式论文。后续全文精读表明，已有工作已经覆盖了其中若干重要部分，研究空白必须进一步收窄：

1. LOTUS、Cortex AISQL、Palimpzest 和 Abacus 已覆盖 semantic operator 的质量、成本、调用数和执行计划优化；
2. *Optimizing LLM Queries in Relational Data Analytics Workloads* 已证明数据库可以利用行、字段和关系统计信息重排请求，以提高 prefix cache reuse；
3. BlendServe 已在 offline serving 内共同研究 resource balance 与 prefix locality，AYO 已利用 application primitive 和 graph topology 改善流水执行与 batching；
4. VTC、Llumnix、FairServe、DLPM 已覆盖 serving 内部或服务层的服务量记账、公平和动态调度；
5. Kalypso 已直接提出 query-plan-aware relational LLM serving，通过跨语义算子流水、parent-child prefix 生命周期和 memory-aware admission，在不改变查询语义的前提下提高 KV-cache 复用；
6. Ray Data/Daft 已提供批数据执行、异构流水线和官方 AI 接口；
7. 本项目的增量必须进一步收窄为：由 PostgreSQL 拥有 SQL、关系 child plan、snapshot 与 query lifecycle，在不修改 vLLM 的条件下，把数据库 Job、分阶段 work、尚未提交的数据和 endpoint-local 状态用于多 Job、多 endpoint 的数据组织、提交与路由；不能再把“连接 semantic query plan 与 LLM serving”本身写成空白。

因此不能再使用“没有任何已有工作研究”之类绝对表述。

### 4.3 最接近的已有工作（需在论文中区分）

| 论文 | 研究什么 | 不研究什么 |
|---|---|---|
| Kalypso: Relational LLM Serving (arXiv 2026) | 让 serving 层接收 semantic query plan，以跨 operator pipelining、依赖感知 prefix 生命周期和动态 memory budget 提高 KV-cache reuse；官方摘要报告 query completion time 最高 4.57× | 当前论文聚焦单条 semantic query 的 query completion 与一个 serving cache domain；未覆盖 PostgreSQL planner/query lifecycle、多 query/多 Job 公平、多个独立 endpoint 路由和数据库管理的流式 child-plan 交接 |
| Optimizing LLM Queries in Relational Data Analytics Workloads (MLSys 2025) | 利用完整关系数据、函数依赖和列统计重排 row/field，提高 KV prefix reuse | 主要面向离线完整输入，不建模在线到达、复杂 cache eviction、多作业干扰和实际 endpoint state |
| BlendServe (ASPLOS 2026) | offline serving 内的 resource-aware batching，同时考虑 compute/memory 需求和 prefix locality | 假定有足够大的可重排请求池，不覆盖数据库数据准备、Job 语义和上游多 endpoint 路由 |
| AYO (ASPLOS 2025) | primitive-level application graph、跨模块 parallelism/pipelining 和 topology-aware batching | 入口是应用工作流并与 backend 配合，不覆盖数据库记录统计、外部黑盒服务和数据库多 Job 提交控制 |
| Ray Data Streaming Batch (2025) | CPU/GPU 异构批处理管线 | 下游 continuous batching 反馈 |
| NeuStream (EuroSys 2025) | DNN 流管线批处理 | LLM token/prefix 需求 |
| HedraRAG (SOSP 2025) | RAG 中 CPU/GPU 协调 | 仅 RAG，非通用 AI SQL |
| Parrot (OSDI 2024) | Semantic variable prompt 共享 | 仅 GPU 侧，不涉及上游 |
| Clipper (NSDI 2017) | AIMD 自适应 batching | 不涉及 LLM、token、continuous batching |
| DRF / Pisces / DRFT | 多资源或多租户的 share guarantee、weighted fairness、work conservation、隔离与 admission control | 资源模型分别是集群 slot、KV-store/服务和事务；当前项目未证明 dominant share 或事务级 guarantee |
| Themis / Tiresias / Pollux | finish-time fairness、attained service/starvation、useful progress/goodput | 面向 gang-scheduled DL training；只迁移独占/保留份额反事实和评价，不迁移训练统计效率或 placement 机制 |
| VTC (OSDI 2024) | token-cost 公平、work-conserving service counter | 位于 serving scheduler 内，不含数据库数据组织和 Daft/Ray runtime |
| DLPM / Agentix | prefix locality 与 fairness/load balance；程序级 attained service/JCT | DLPM 是预印本且机制在 serving 层；Agentix 的程序 DAG/agent 语义不等于数据库 Job |
| Llumnix (OSDI 2024) | 多实例动态调度与 KV live migration | 依赖 serving 内部迁移，不覆盖固定 endpoint 的上游 shared credit |
| LOTUS / Palimpzest / Abacus | semantic operator 的质量、成本和物理计划优化 | 不研究固定模型服务下的最小饱和 active work 和 request-level refill |
| SemBench (PVLDB 2026) | 多系统、多模态 semantic query benchmark | 提供 workload/指标，不提出上游调度算法 |
| SFS (arXiv 2026) | 用 serving-framework simulation 做 TTFT-aware routing | 决策层是模型路由；依赖细粒度 serving snapshot，不覆盖数据库 source、Daft/Ray held queue 与 sink |
| TIE / Past-Future / JITServe | 用输出长度分布、未来显存或渐进修正优化 SLO/goodput | 机制位于 serving scheduler 内；本项目只能迁移不确定性表示与评价方法，不能把其内部调度器写成已实现 baseline |
| Beyond Prediction / FastServe | prediction-free 或 prediction-light 的尾部/抢占调度 | 证明输出长度预测不是唯一道路；其 token-level preemption 需要修改 serving runtime，超出当前边界 |

### 4.4 不能声称的结论

1. 不能说"现有研究没有关注数据库 AI 算子"——Snowflake SIGMOD 和 Smart/GaussML/NeurDB 已充分证明
2. 不能说"外部执行一定优于数据库内 ML"——取决于场景
3. 不能说"Ray/Daft/Lance 是数据库 AI 算子的标准方案"——Snowflake 和 GaussML 用不同技术栈
4. 合理表述："Kalypso 已证明 semantic query plan 可以进入 LLM serving 控制；本课题进一步研究 PostgreSQL query lifecycle 所有权、多 Job、多独立 endpoint，以及数据组织、提交和路由在不修改 vLLM 条件下如何共同工作"
5. 不能说“上游调度会加速 GPU 单次推理”；它能改善的是达到容量上限所需的压力、瞬态 ramp、可控排队、多 job 公平和端到端 JCT

---

## 5. 文献提取的设计原则

### 5.1 从 vLLM/Orca/Sarathi 提取

- **Continuous batching 是下游给定机制**，上游目标不是替代它，而是给它最优的请求流
- **按预测工作量而非仅按请求数组织 batch**：上游 token budget 是数据组织和
  submission 形状控制量；vLLM `max_num_batched_tokens` 是引擎内部单轮调度
  上限。两者可以共享“按工作量控制”的思想，但语义、时间尺度和最优值不同，
  不能把上游预算写成 vLLM 参数的直接迁移
- **并发提交优于一次大 batch**：vLLM 推荐并发提交独立请求，不手动合并——验证了多 actor 独立提交架构
- **完成即补位不应被上游整批屏障抵消**：Orca/vLLM 在服务内部按迭代移除完成
  请求并接纳 waiting 请求；本项目可迁移为 Ray 上游 request-level credit
  release/continuous replenishment，但不能写成修改或重新发明 continuous batching

### 5.2 从 Clockwork/Nexus/Clipper 提取

- **确定性调度优于乐观并发**：Clockwork 的弃用线程池思路 → 上游应主动控制而非被动等待
- **Batch size 是一等调度维度**：Nexus 的 batch-aware scheduling → 上游 token-budget 不只是"参数调优"
- **AIMD 与 delayed batching 要分开映射**：Clipper 的 AIMD 直接控制 batch
  上限，delayed batching 控制等待；本项目可借鉴 SLO guardrail、等待聚合和
  per-replica 控制，但当前 two-level flush 不是 Clipper 算法复现
- **Deadline/slack 是 flush 的硬约束候选**：从 Clockwork 的 deadline 思想迁移
  oldest-request slack；自回归服务时间不可完全预测，因此必须有静态回退

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

**从 CONCUR (2025) 提取**（历史文献笔记见 `research/reading_notes/concur_2025.md`）：
- **AIMD 可迁移到 request 级**：CONCUR 控制的是"活跃 agent 数"（粗粒度），我们可以把 AIMD 用到更细的 per-actor in-flight 请求数控制。**校正**：CONCUR **不使用 EWMA**——用瞬时 KV 使用率/命中率 + 宽死区（U_low=0.2 / U_high=0.5）+ 非对称 AIMD（α=2 增 / β=0.5 减）+ 双信号（proactive U_t + reactive H_t）
- **KV cache 作为共享资源信号**：不只是队列深度，KV cache 使用率/命中率也应作为 K_max 调节的输入信号（**CONCUR 是 KV cache 信号的正确来源，CoLoRA 不是**）
- **Middle-phase thrashing**：长期运行的推理 session 在内存耗尽前就会出现吞吐退化。**待确认**：CONCUR workload 是 agentic ReAct 多步 agent；本课题 DB operator 多为无状态单轮，middle-phase thrashing 前提可能不成立——KV cache 信号价值需在单轮场景重新验证

**从 Scorpio (2025) 提取**：
- **VBS (Virtual Batch Size) Admission Control**：SCORPIO 用按 SLO 紧迫度
  加权的 active request 数投影运行负载。它可启发 active-work admission，
  但上游 token-budget batching 控制的是尚未提交行的 membership，不是 VBS
  的实现
- **Credit-based Batching**：按 SLO 松紧分配 batching 机会——可迁移到我们的异构 workload 场景（不同优先级的 SQL 查询）

**从 SABER (2025) 提取**：
- **前瞻性准入判断**：不只检查当前队列，还要预测"如果现在提交，会不会导致 in-execution 请求违反 SLA"——我们的 K_max 调节应具有预测性
- **Universal Scalability Law 建模**：`生成速度 = f(并发请求数)`——可用 vLLM 的 profiling 数据拟合此函数。**校正（见 `saber_2025.md`）**：SABER 用 USL 做 per-request 准入预测，**不直接推导聚合 K_max**；K_max = √((1−α)/β) 上界是本课题扩展。USL 单瓶颈假设与 vLLM 双瓶颈（算力 + KV cache 非连续 preempt）存在张力，迁移前需做 out-of-sample 残差审计

**从 CoLoRA (2026, ASP-DAC, CCF-C) 提取**（历史文献笔记见 `research/reading_notes/colora_2026.md`）：
- **校正**：CoLoRA 是**多租户 LoRA** 场景调度，APS 三信号 = 排队延迟 + adapter 驻留 + SLA 紧急度（**不含 KV cache**）；LBS 实为 load + queue 两信号。本课题 flush 的"三维信号"其**信号集**应归因于 CONCUR（KV cache）+ vLLM Prometheus（running/waiting）；CoLoRA 仅贡献多信号融合的闭环**架构模式**（CCF-C，非承重证据，数字为 "up to" best-case）
- **Unified Scheduler 的全局反馈循环**：monitor → prioritize → place → batch → feedback 闭环——可作为 RC2 控制器架构骨架

**从 BucketServe (2025) 提取**：
- **按序列长度分组降低 padding 开销**：与我们的 length-aligned grouping 思路一致——验证了"按计算量相似度分组"的有效性
- **自适应 bucket split/merge**：当 workload 分布变化时动态调整分组边界——可迁移到我们的 token-budget 分组边界的自适应调节

**从 ProServe (2025) 提取**：
- **两层调度架构验证**：SlideBatching（Engine 层 token 级）+ GoRouting（Service 层 request 级）——与我们的"内部 vLLM + 外部 Ray"两层架构同构，证明分层调度在该场景下是合理设计
- **Gain-oriented dispatching**：不仅看当前负载，还要预估未来收益——actor pool 分池路由可参考此思想

**本项目新增迁移候选（2026-07-28）**：
- **Service-quantum adaptive budget**：先用静态
  `{1024,2048,4096,8192,16384,32768}` 容量曲线标定组织预算甜点。第一版只按
  arrival/service-rate EWMA 在少量离散预算中逐档选择；pending work 和 oldest
  slack 作为后续独立增量，不在首版混入。这里迁移的是 Clipper delayed
  batching、DistServe workload profiling 与 ProServe latency estimation 的
  设计模式；不是 Sarathi engine-internal iteration token budget 的复现
- **Shared endpoint-local work credit**：多个数据库 AI job 不能各自独立持有
  完整 per-endpoint K。候选服务级协调器对每 endpoint 同时维护 request cap 和
  predicted active-work cap，并以 deficit/weighted fair queue 在 job 间分配
  credit；空闲 job 的份额允许被借用。该机制优先解决共享服务的隔离、饥饿与
  work conservation，再叠加动态 flush

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

### 5.7 从代价估计与提交策略文献提取的设计模式（2026-08-04 更新）

以下从 Heinrich、CONCERTO、GRACEFUL、COSTREAM、Abacus、LOTUS、Palimpzest、VTC、Llumnix、SFS、TIE、Past-Future、JITServe、Beyond Prediction 与 FastServe 中提取可迁移的技术与设计思路。代价估计是两项研究内容的公共组件，提交策略是研究内容二；内部旧代号只用于历史实验追踪。

#### 5.7.1 代价估计（RC4）设计模式

**模式 1：Hybrid 架构（传统公式 + Learned Correction）**
- **来源**：Heinrich R4（"Don't Throw Expert Knowledge Away"）+ DACE/QPP-Net 的 PostgreSQL cost 特征实验 + Pathak & Mankodi 的 XGBoost on EXPLAIN features
- **含义**：不纯用 learned model 从零学起——先用一个简单的物理公式给出 base estimate（如 `E2E_base = total_prompt_tokens / estimated_throughput + overhead`），再用 Ridge/LightGBM 学残差 `Δ = E2E_actual - E2E_base`
- **为什么有效**：传统公式捕获了一阶效应（计算量与延迟的线性关系），learned model 只需学非线性偏差、资源竞争效应和噪声——相当于是"用小模型修正大公式的误差"，而非"用小模型从头拟合复杂的 E2E 函数"
- **落地难度**：🟢 低——加一个 `E2E_base` 特征列到当前 15 特征中即可实验
- **预期效果**：可能降低跨 seed MAPE 波动（当前 30-90%），提升 R²
- **gap**：需确定 `E2E_base` 的最优公式形式（可用 Ridge 的 coefficients 做特征重要性分析反推）

**模式 2：排序优先评估（Ranking-First Evaluation）**
- **来源**：Heinrich R2 + §4 Definition 3-4（Selected Runtime、Surpassed Plans、Spearman ρ）
- **含义**：对编排决策来说，代价模型的**排序能力**比**点估计精度**更重要。评估代价模型时，应报告 Spearman 秩相关系数、pairwise accuracy（随机抽两个配置，模型正确排序的比例）、Top-K precision（模型能否选出真正最快的 K 个配置）
- **为什么有效**：Heinrich 实验证明 Q-Error 最优的 LCM 在 Join Ordering 上反而最差——因为忽视了排序。本项目 70 个配置组的场景是"从候选配置中选最优"，与 Heinrich 的"从候选计划中选最优"完全同构
- **落地难度**：🟢 低——已计划补充，论文提供了完整的方法论依据
- **gap**：`estimate_operator_cost.py` 需增加 Spearman/pairwise/Top-K 输出

**模式 3：多粒度模型组合（Multi-Granularity Meta-Learner）**
- **来源**：Microsoft Meta-Ensemble Patent + Heinrich R1（DB-agnostic > DB-specific）
- **含义**：不只训练一个全局 Ridge——训练四个模型：(a) 每个 workload 的局部模型（精度最高，覆盖窄），(b) 每个模型 size 的模型，(c) 全局模型（全覆盖，精度低），(d) meta-learner 加权组合
- **为什么有效**：70 个配置组天然形成层级结构（按 workload × 模型 × batching strategy）——局部模型在熟悉配置上精度高，全局模型兜底。Microsoft patent 的 FastTreeRegression meta-learner 提供了具体的组合方式
- **落地难度**：🟡 中——需 283 行中的每个子集足够大才能训练局部模型；当前某些配置组只有 2-3 行，不足以训练 per-workload 模型
- **gap**：需要更多 profile 数据覆盖低频配置组，或使用 hierarchical Bayesian shrinkage（小样本时向全局模型收缩）

**模式 4：不确定性门控（Uncertainty Gating）**
- **来源**：Microsoft Patent + Heinrich R3 + OCACO（多个来源一致推荐）
- **含义**：代价模型不仅输出点估计，也输出预测区间或置信度。当置信度低时回退到保守估计（如 P95 上界），避免因过度乐观的估计选到慢得多的计划
- **为什么有效**：Heinrich 发现 LCM 的最大问题是"偶然的大误差导致选错计划"——如果模型知道"这个估计我不太确定"，至少可以保守决策
- **落地难度**：🟡 中——Ridge 本身不输出不确定性；可用 bootstrap residual 估计（训练集上残差的经验分布作为预测区间）、或换为 Bayesian Ridge / Gaussian Process
- **gap**：当前只输出点估计，无预测区间

**模式 5：解耦三阶段建模（Per-Component → Competition → Aggregation）**
- **来源**：CONCERTO（OCP → GAT → TCN）
- **含义**：不直接预测 e2e_s，而是：(Stage 1) 分别估计 DB fetch time、vLLM prefill time、vLLM decode time、writeback time；(Stage 2) 建模并发时的资源竞争（如 shared vLLM 的 KV cache 争用）；(Stage 3) 聚合为 e2e_s
- **为什么有效**：各阶段受不同因素影响（DB fetch 受 row count 影响，prefill 受 prompt token 影响，decode 受 output length 影响），分开建模更精确；并发竞争可以显式校准
- **落地难度**：🔴 中-高——当前 profile CSV 只有 e2e_s 和 model_service_s，无 per-stage breakdown。需要修改 profiler 在 CSV 中拆分阶段耗时
- **gap**：需 `postgres_ai_operator_profile.py` 增加 per-stage timing

**模式 6：Transferable Features（可迁移特征）**
- **来源**：COSTREAM + Zero-Shot (Hilprecht)
- **含义**：所有特征必须是物理量（token count、row count、timeout ms、K_max），**不能**编码特定 workload 名、模型名、硬件型号。这样模型可以 zero-shot 泛化到新模型/新 workload
- **为什么有效**：COSTREAM 在 unseen hardware 上 Q-Error 几乎无退化（1.37 → 1.59）；当前 15 个特征已经基本是 transferable 的（无 workload 名、无模型名），但 `flush_is_adaptive`/`flush_is_immediate` 是策略特定的
- **落地难度**：🟢 低——当前已基本满足，后续新特征需保持此原则
- **gap**：验证当前 Ridge 在跨模型（如 Qwen2.5-1.5B → 更大模型）上的泛化能力

**模式 7：多代价指标联合输出**
- **来源**：COSTREAM（同时预测 throughput、E2E latency、per-operator latency、backpressure、OOM）
- **含义**：不只输出 e2e_s——同时输出 tokens/s、service_p99、OOM/timeout 概率。编排决策需要多维信息：一个配置可能 e2e 快但 token/s 低（模型效率差），或者 e2e 快但 P99 高（tail latency 差）
- **落地难度**：🟡 中——多输出 Ridge 可以直接做（多个独立 Ridge，共享特征），不需要改模型架构
- **gap**：`estimate_operator_cost.py` 当前只支持单一 target

**模式 8：训练数据多样化**
- **来源**：Heinrich R3（Training Data Diversification）
- **含义**：当前 profile 数据全来自 `status=ok` 的成功运行——模型从未见过"差配置"。应该在后续 profile 中有意加入一些"已知慢"的配置变体（如 K_max 极低导致 queue 积压、batch size 极大导致 OOM 边缘），让模型学习"坏配置长什么样"
- **为什么有效**：Heinrich 证明在 access path selection 中，加入多样化训练数据（强制执行 IndexScan + SeqScan 两条路径）后 LCM 首次超越 PostgreSQL
- **落地难度**：🟡 中——需要额外运行慢配置（耗时），但不需要很多（Heinrich 只用 500 条多样化数据做 fine-tune）
- **gap**：当前 profile pipeline 只采集"合理"配置，未系统收集边界/劣化配置

**模式 9：简单模型优先（Simple Models First）**
- **来源**：Heinrich R1（FlatVector 经常排名前三）+ Pathak & Mankodi（XGBoost > LSTM in low-data）
- **含义**：在 283 行数据上，Ridge/LightGBM/XGBoost 足够了——不要过早升级到 GNN/Transformer。特征工程和数据质量比模型架构重要
- **落地难度**：🟢 低——当前 Ridge 符合此原则，保持不变即可。后续数据量增长到万级后可考虑 LightGBM
- **gap**：无

**模式 15：Output-Length 预测替代 Completion 上限**
- **来源**：SFS §3.4（output-length predictor）+ GRACEFUL §IV.C（自然 EOS 概率与 UDF 分支估计类比）+ TIE（ICML 2026）
- **含义**：当前 Ridge 以 `completion_max_tokens`（用户设定的输出上限）作为特征——但实际 E2E 时间高度依赖于真实输出 token 数（自然 EOS 位置），而非上限。第一步可用轻量模型预测实际输出；第二步必须像 TIE 一样保留输出长度的重尾分布，而非只留下一个均值。
- **为什么有效**：当前 283 行 profile 中许多请求的自然 EOS 远小于 `completion_max_tokens`，用上限代理会系统高估平均 work；反过来，只用预测均值又会低估长尾。可同时输出 `q50/q90/q95` 或期望 + tail-risk 特征，供排序和 SLO admission 使用。
- **落地难度**：🟡 中——点预测很容易，但可信分位数需要更多 per-request 样本、独立时间/workload calibration 与 coverage 审计。
- **gap**：当前只有 output cap；无执行前实际输出分布，也未评价 tail underestimation。

**模式 19：分布式 work、渐进修正与 prediction-free 回退**
- **来源**：TIE（ICML 2026）+ Past-Future（ASPLOS 2025）+ JITServe（NSDI 2026）+ Beyond Prediction（ICML 2026）+ FastServe（NSDI 2026）
- **含义**：admission-time cost 不应是静态标量，而应是 `work distribution + confidence + remaining-work update`。初始准入用保守分位数；请求开始生成后，用实际 completion/remaining work 更新 credit。若 OOD、区间过宽或分布漂移，则回退到不依赖输出预测的固定 active-work/静态策略。
- **为什么有效**：Past-Future/JITServe 说明历史分布和执行中信息可以改善 goodput；Beyond Prediction 说明即便知道真实长度，burst、memory pressure 和 tail interaction 仍可让 prediction-driven 策略失效；FastServe 说明 input-only、prediction-light 路径仍是强对照。
- **评价**：同时报告区间 coverage/width、tail underestimation、P95/P99、SLO goodput、regret、回退率与 OOD split；点预测 MAE/R² 只作诊断。
- **落地难度**：🟡 中——首版可用 bootstrap/quantile regression + completion trace 更新，不需要实现 serving 内部抢占或复杂模拟。
- **gap**：当前 credit 只在 completion 时释放，代价模型只输出单一 `e2e_s` 点值；尚无 confidence gate、remaining-work correction 或 prediction-free fallback 审计。

#### 5.7.2 提交策略（RC2）设计模式

**模式 10：SFS What-If 预演（Serving Framework Simulation）**
- **来源**：SFS（Patel et al. 2026, §4）
- **含义**：在每次 flush 决策时，用确定性模拟器预测"如果现在提交这个 pending batch，每个请求的 TTFT 是多少"。具体步骤：(1) 获取 vLLM 当前的 workload snapshot（running requests 的 prefill/decode token composition），(2) 将 pending batch 的请求加入模拟，(3) 确定性模拟后续 token batch 直到每个请求生成第一个 decode token，(4) 只提交 TTFT 预测在 SLO 内的请求
- **为什么有效**：SFS 的 TTFT MAPE <5%，sub-millisecond 开销——证明了这种"轻量模拟"对在线决策是可行的。相比当前 queue-adaptive flush 的"看 queue depth 做二元决策"，what-if 预演可以做细粒度的 per-request 准入
- **落地难度**：🔴 中-高——需要 (a) 实现 SFS 的 token-batch simulator（Python 代码，~200 行），(b) 为本地 Qwen2.5-1.5B 校准 4 个 β 参数，(c) 从 vLLM Prometheus 获取实时 workload snapshot
- **gap**：当前无 token-batch 模拟器；vLLM Prometheus 的 `vllm:running_requests` 和 `vllm:waiting_requests` 可能不够细粒度

**模式 11：LPS Queueing Model 指导 K_max 选择**
- **来源**：SFS §4.2（Average-case estimator）+ LPS (Limited Processor Sharing) model
- **含义**：公式 `W_avg = (λ/μ)^k / (μ - λ)` 给出了"给定到达率 λ、服务率 μ、并发槽位 k 下的平均等待时间"。这可以作为 K_max 选择的**解析指导**——不必纯靠实验暴力搜索 K_max
- **为什么有效**：SFS 实验显示 LPS 模型与实测 Qwen3-0.6B 等待时间高度吻合（k ≈ 25）。项目的 K=8 baseline 和 AIMD 的"漂到 K=16"可以用此公式解释：λ/μ 比率决定了什么 K 值刚好平衡队列增长
- **落地难度**：🟡 中——需要从 profile 数据估计 μ（vLLM 的请求服务率，约为 tokens/s / avg_tokens_per_request），λ 从 arrival replay 参数获取
- **gap**：当前 K_max 选择纯实验驱动，无解析公式辅助

**模式 12：Token-Batch 处理时间线性回归**
- **来源**：SFS §4.1（eq. 9，4-parameter linear regression for token-batch time）
- **含义**：不模拟低层 GPU kernel——用 4 个参数的线性回归直接从 token batch composition 预测 batch 处理时间：`T = β0 + β1·Σtok + β2·Σ(c·tok) + β3·Σ(tok·c + tok²)`。校准只需离线跑一批 token batch 并记录 composition→time 映射
- **为什么有效**：`dense computation ∝ tokens`（β1）、`attention ∝ context·decode_tokens`（β2）、`prefill attention ∝ prefill_chunk·context + prefill²`（β3）——每一项都有清晰的物理意义
- **落地难度**：🟡 中——需从 vLLM 获取 per-iteration token batch composition（可能需要改 vLLM 的 logging/metrics），或从 Prometheus 间接推断
- **gap**：当前 vLLM Prometheus 指标无 per-iteration batch composition 信息

**模式 13：轻/中/重 Workload 分类**
- **来源**：SPOS + Heinrich R3 + 项目已有计划
- **含义**：不求精确预测 E2E 秒数，而是将 pending batch 分为"轻（E2E < 10s）/ 中（10-60s）/ 重（> 60s）"三档。提交策略根据档位做粗粒度决策——轻 batch 可以激进提交（低风险），重 batch 需要等待更多请求合并（摊销 overhead）
- **为什么有效**：SPOS 证明"结构预测"比"精确长度预测"更稳健——类比到本课题，"batch 轻/中/重"比"batch E2E = 42.3s"更可靠
- **落地难度**：🟢 低——当前 Ridge 可能已有足够排序能力做分档（MAE 11.68s vs E2E 范围 ~5-300s），只需定义档位阈值并验证同档内真实 E2E 方差是否显著小于全局
- **gap**：需在 profile 数据上验证分档效果

**模式 16：USL 并发-吞吐估计（Universal Scalability Law）**
- **来源**：SABER (arXiv 2025, §IV.B Step 2) — USL 拟合 LLM 推理 per-request 速度退化曲线，R²=0.99
- **含义**：USL `σ(N) = λN / (1 + σ(N-1) + κN(N-1))` 从约 1000 个 (并发, 吞吐) 采样点拟合出完整的并发-吞吐退化曲线。峰值并发 `N* = √((1-σ)/κ)` 给出了"再多发也没用"的解析上界。与模式 11（LPS 等待时间估计）互补：LPS 建模等待时间随并发的变化，USL 建模吞吐随并发的退化——两者共同提供 K_max 选择的完整解析依据
- **为什么有效**：SABER 实验证明 USL 在 LLM 推理场景下拟合优度远超线性/Logistic 回归（R²=0.99 vs 0.97/0.91）。~1000 个采样点足以稳定估计 σ/κ。当前项目 K=8 是暴力扫参得来——如果 USL 拟合后 N* ≈ 8，经验值有理论支撑；如果偏差大，说明 vLLM 的 KV cache 抢占退化机制不服从 USL 的平滑退化假设——两种结果都有论文价值
- **落地难度**：🟡 中——需离线跑一次 concurrency sweep（K=1,2,4,8,16,32,64）采集 (L, throughput) 数据点，scipy curve_fit 即可。约一天工作量，不依赖在线信号
- **gap**：当前 K_max 选择纯实验暴力搜索；USL 在 vLLM continuous batching + KV cache 抢占下的适用性未经检验

**模式 17：双信号 Deadband 控制架构**
- **来源**：CONCUR (arXiv 2026, §4.3 Eq 1) — proactive（U_t KV cache 使用率）+ reactive（H_t 命中率）双信号，deadband 宽度 0.3
- **含义**：不用单一信号驱动自适应决策——使用两个独立信号（proactive 预警 + reactive 确认），仅在两者同时越界且变化幅度超出 deadband 时才触发动作。核心价值在于**防止控制器振荡**——单信号 + 无 deadband 会对瞬时噪声过敏，导致频繁 upshift/downshift
- **为什么直接回应项目已知问题**：项目 07-19 early adaptive 实验触发 102 次 downshift/run——正是单信号（queue depth）+ 无 deadband 的典型振荡症状。当前 queue-adaptive flush 也是看 queue depth 一个信号做 25ms/50ms 二元决策。双信号架构下可以是 (queue_depth, oldest_request_age) 或 (token_backlog, arrival/service_ratio)——两个信号同时"说该发了"才缩减 timeout，且变化量不够 deadband 就不动作
- **落地难度**：🟡 中——控制架构改动 ~50 行，需选定第二信号并调 deadband 参数（CONCUR 的经验值 0.3 可作为起点）
- **gap**：当前所有自适应控制器（AIMD/PID/EWMA/queue-adaptive flush）均使用单一信号 + 无 deadband，振荡问题在 07-19 实验中已暴露但未被架构层面解决

**模式 18：Credit-Based Admission（按 SLO 紧松度差异化准入）**
- **来源**：SCORPIO (arXiv 2025, §3.4) — TRP (Token Rate Proportional) credit accumulation + VBS (Virtual Batch Size) admission
- **含义**：不设全局 K_max——每个请求按自己的 SLO 紧松度获得不同的 credit 累积速率 TRP(r) = min S_TP / S_TP(r)（SLO 越紧 credit 越快），credit ≥ 1.0 时准入。这等价于"按 SLO 紧迫度加权的自适应并发控制"：紧 SLO 的请求更快被放行（不被大 batch 拖累），松 SLO 的请求在 credit 慢速积累中自然合并（摊销 overhead）。准入判据：VBS = Σ TRP(r)，EstimatedTPOT(VBS) ≤ min S_TP 才放行
- **为什么与现有模式不同**：模式 10（SFS what-if）和 11（LPS）都是从系统能力出发判断"能发多少"——这是 supply-side 视角。Credit-based admission 从请求的 SLO 需求出发判断"该不该发"——这是 demand-side 视角，且天然实现了请求间的公平性（不是 FIFO，不是 shortest-job-first，而是 deadline-aware proportional fairness）
- **落地难度**：🟡 中——需为每个请求定义 SLO target 和 TRP 速率。在当前批量离线场景（SLO 同质、无 per-request deadline）下退化为均匀 credit 累积（= FIFO）；只有当存在 SLO 异构性时（如混合 workload、多模态请求混跑、在线+离线混合）才体现区分度
- **gap**：当前 K_max 为全局固定值，不区分请求紧迫度；无 per-request deadline 的 tracking 基础设施

#### 5.7.3 跨场景通用模式

**模式 14：Probe Execution 数据收集策略**
- **来源**：CONCERTO §III（Runtime Tracker: probe execution mode）
- **含义**：不需要跑完整查询来收集特征——只跑"前几个 chunk"即可推断完整执行的 pipeline 结构和算子特征。本课题的类比：profile 阶段可能不需要完整的 10,000 请求 E2E——前 1,000 请求的 metrics 模式可能已经足够预测整体行为
- **落地难度**：🟡 中——需实验验证"partial execution 特征 vs full execution 代价"的相关性
- **gap**：当前 profile 全部是完整 E2E 运行

#### 5.7.4 图像异构多阶段候选（2026-08-11 偏差修正）

**模式 20：显式两级 broker + differential backpressure**

- **来源**：Tassiulas–Ephremides MaxWeight/backpressure 的 tandem-queue 迁移；Ray Core/Data
  提供显式 CPU/GPU resource、actor pool 和 streaming operator，但不替项目选择数据库 Job
  的 stage admission。
- **含义**：不再把未完成的 CPU preprocess future 直接预排入 GPU actor。显式拆成
  `pending-prepare → ready-tensor → pending-model`，分别维护 encoded/ready tensor work；
  在线只在预启动 actor pool 上选择有限安全
  `(prepare_inflight, ready_tensor_work, model_inflight)` 档位。
- **决策项**：两阶段动作相关 drift 使用
  `-(Q_prepare-Q_model)·mu_prepare - Q_model·mu_model + V·cost`。ready tensor 堆积时抑制
  继续生产，GPU 缺料且 encoded backlog 存在时增加 prepare 流量；GPU utilization 只作佐证，
  不作为阈值触发器。
- **工程边界**：actor pool 大小和线程数离线冻结；已有 16→32 CPU actor 结果显示热查询只
  增约 7.3%，但 setup/first-output 恶化，因而动态建/杀 actor 不是快控制动作。
- **状态**：engine-neutral controller 与单元测试已实现；static HSE 已接入图像 runner 的真实
  `pending-prepare → ready-block → pending-model` broker。HSE static GPU 非劣比较和由 controller
  驱动的在线动作均未运行，因此仍无动态图像性能 claim。

**模式 21：带 transform signature 的 derived-image cache / GPU preprocess 对照**

- **来源**：数据库物化结果、Arrow fixed-shape tensor、NVIDIA DALI mixed decoder/resize/
  normalize 的成熟工程模式；不是新调度算法。
- **含义**：SAOR 只能协调 pipeline，不能消灭每图 decode/resize CPU work。对重复查询可用
  `(content_hash, processor/model transform signature)` 建派生表或 lakehouse cache，优先保存
  224×224 RGB uint8（150,528 B/图）而不是 FP32 tensor（602,112 B/图）；冷 miss 保留当前
  CPU actor。另以 DALI external source + mixed/GPU preprocessing 作匹配资源 baseline。
- **评价**：必须把一次性冷扫描与热命中分开，计入 cache build/refresh/storage；固定 CLIP
  embedding 语义并报告 JCT、CPU-core-s/image、GPU-s/image、bytes/image、energy 和检索质量。
- **晋级门**：cache hit 可预测且摊销收益覆盖 refresh/storage，或 DALI 在同硬件上净降低 JCT
  且不挤占 model GPU；否则只保留为负结果/工程 baseline。

#### 5.7.5 SAOR capacity-only 负结果与可迁移教训（2026-08-11）

- **事实**：双 4090 单次四臂 development gate 中，SAOR 相对 K128 吞吐 +4.36%，但相对
  K160 +0.52%、相对 legacy threshold −1.46%，Jain 最低，故未过晋级门。K160 相对 K128
  仍有 +3.82% 吞吐与两个 Job JCT 改善，但 Job B P99 +23.93%、Jain −3.22%、KV P95
  0.826→0.997；本轮无 OOM/failure/leak。
- **算法教训**：持续高压、GPU 已饱和时，有限容量控制没有足够的 avoidable-idle 区间。当前
  runner 又只执行 aggregate capacity，没有 per-Job virtual queue/ordered release；不能把完整
  DPP 模型的公平结论外推给 capacity-only adapter。
- **建模教训**：MaxWeight backlog 项随队列增长，归一化 waiting/KV risk 在固定 `V=1` 下会
  被淹没；只学习 current arm 的 EWMA 也不是同状态反事实 service。一般 bounded prediction
  error 乘无界 backlog 后不是常数，必须走 oracle exact、可证明 `alpha`-approximate 或有界
  buffer 三条严谨路线之一。
- **系统教训**：外部 downshift 不能撤销 vLLM 已接纳请求，动作主要在末段发生时只会等待
  KV/long decode 排空。dynamic 的正确对手是强静态 Pareto 点，不是低档稻草人；后续必须先用
  recovery-gated burst 上的 offline oracle 证明有可利用动态空间，否则淘汰容量分支。
- **`saor-v0.4` 修正**：主方法收紧为固定安全 envelope 内的 SAOR-Release，用 per-Job
  unfinished work、active-set entitlement、idle borrowing/reclaim、fairness/SLO debt 和
  completion-driven Job-head release 验证多 Job；动态 K 标记为 `parked-conditional`。现有
  static/shared 对照在 2026-08-11 时尚未排除同 K global FIFO/no-project scheduler 已经足够好；
  后续 fixed-envelope formal、同 observation 的 FIFO/DRR/VTC-style 双轮归因与五臂共同观测
  rehearsal 已完成。结果仍只支持效率—tail—公平权衡，不支持 SAOR winner。Daft/Ray Data/
  产品原生 baseline 保持各自调度，不注入项目机制。详细模型与 benchmark 见
  `saor_model_scenario_audit_20260811.md`。
- **2026-08-12 工程边界**：`saor_release` 已接入共享 Ray credit runtime；它只在离线校准并
  按签名冻结的 $K^*$ 内选择 fitting Job head，不在线调 K。当前 completion 更新 fairness debt，
  `slo_weight` 强制为 0，直到 per-Job SLO virtual queue 接通，因此不能声称完整 DPP/SLO 定理
  已实现。direct `run-jobs-control` 与 project `shared_fifo` 分开：前者无 Job policy，后者是
  project-owned FIFO。真实 trace 自动审计 borrow→reclaim→reborrow 是否发生；未发生即不能抽
  动态结论。

#### 5.7.5.1 SAOR fixed-envelope formal 与 reservation 修订（2026-08-12）

- **formal 事实**：2×4090/Qwen2.5-7B、fixed K128/W65536 的六 active-set + 四 solo 共
  40/40 cell、0 incident、exactly-once。SAOR 12,393 tok/s、fg JCT/P99 57.0/50.3s、fg
  slowdown 3.45，在 credit 臂内前台最好；static 9,508 tok/s、fg JCT/P99 36.2/29.2s、
  slowdown 2.19、SLO violation 0%，仍是更强隔离 Pareto 点。
- **门禁边界**：DRR/VTC rep2 的两个 Job 近乎同时结束（绝对完成时刻差约 5.8ms/4.8ms），
  `active_set_bulk_only_post_samples=0`，使原始 validation fail-closed。它说明审计器缺 simultaneous-drain
  语义，不证明 baseline 违反工作守恒。
- **分辨率修订**：post-drain 是区间性质。若两 Job 完成间隔小于 trace 周期且该开区间内无
  样本，则证据既不能证明工作守恒，也不能证伪，正确三值语义是 `not_applicable`，而不是
  `false`。冻结 250 ms 规则后的 compact replay 将 DRR/VTC rep2 重分类，四 credit 臂 effective
  12/12；随后 `ed168d8` 在服务器完整 artifact 上运行默认 summarizer，resolution-aware v2
  validation passed、`full_formal_validation_updated=true`。长于一个周期仍无样本、新 schema 明确
  applicable 或任一前置机制失败仍保持 fail-closed。
- **第一性原理原因（经 reachability 短测修正）**：前台到达时已占用 lease 仍不可抢占，但未来
  completion 可被 lexicographic release 立即导向 foreground。两轮 strict-priority 短测达到 fg
  JCT/P99 20.04/14.27s、SLO 0%，同时保留 11,791 tok/s，说明 reservation 不是已知 foreground
  存活信号下 2-Job reachability 的必要条件。current SAOR 差的主因是 soft entitlement/fairness
  score 与 foreground tail/SLO 目标错位；hard priority 的代价是 bulk SLO 0.801 与潜在 starvation。
- **估计误差**：project credit 臂中 foreground `actual/predicted work≈1.289`，bulk≈1.064；
  前台低估百分比约是 bulk 的 4.5 倍。下一版 admission 必须比较 point estimate、q95 upper
  bound 与 actual-work oracle，不能用同一 predicted token 标量同时承担资源、安全和公平。
- **修订路线**：先把 strict priority 收紧为 lexicographic SLO priority + 有界 priority window 或
  service-lag cap；其余时间回到 DRR/SAOR。reservation $r/K$ 与 upper-bound work credit 只作未知
  到达、多 foreground 和预测误差下的鲁棒性消融。达到 static fg 非劣、吞吐相对 static≥5%、
  bulk lag/SLO 不越界才晋级；否则淘汰，不扩 4-Job。完整推导见
  `saor_model_scenario_audit_20260811.md` §11。
- **release-only upper bound 实现状态**：已实现并完成两轮 GPU 短测。前台
  Job 注册后，未来释放 credit 只给前台；已进入 vLLM 的 bulk lease 不撤销，前台 `finish_job`
  后恢复 bulk。group evidence 记录 `[bulk,foreground]=[0,1]`，独立判门要求 fg P99≤30.7s、
  fg SLO violation≤1%。实测 fg P99 14.27s、SLO 0%，但 formal repeats=0；它只诊断 release-only
  可达域，不能称 SAOR 改进或通用 hard-priority 策略胜出。

#### 5.7.6 多 Job 评价模式：不是只看 VTC 或 Jain（2026-08-13）

当前评价主体冻结为**单租户内多个 Job/workload class**：`job_id` 是应用/实验合同给出的逻辑
调度身份，request 是工作量载体。因此当前只声称 intra-tenant Job fairness/service
differentiation，不把 flat `job_id` 记账改名为 tenant fairness。多租户是兼容的后续扩展而非当前
formal blocker：外层先按稳定 `principal_id` 聚合 tenant entitlement/debt 和 buffer cap，内层再
复用现有 Job-level ready observation、priority/SLO、borrowing/reclaim；届时须另验 tenant floor、
anti-splitting、双层 debt/reclaim 和非抢占请求的恢复时间，不能自动继承当前公平性质。

文献交叉后冻结四个互补视角：DRF/Pisces/DRFT 约束“份额与隔离如何定义”，Themis/Tiresias/
Pollux 约束“Job 完成体验和未知时长如何评价”，VTC/DLPM 约束“共同积压服务差与 locality
冲突”，Sarathi-Serve/DistServe/Llumnix 约束“SLO goodput 和 tail”。任何单篇都不能单独成为
本项目判定多 Job 调度好坏的完整依据。

每个 Job 固定三种反事实：`multi/full-solo` 看总体干扰，`multi/reserved-solo` 看经验性保留份额
非劣，`policy-multi/static-multi` 看同竞争条件下 scheduler 增量。共同积压窗口另报 weighted
actual service、empirical GPS lag、最长连续无服务和 avoidable idle；用户层另报 worst-Job
JCT/P99/SLO。Jain 只表示均匀度：所有 Job 相对 static 都改善而 Jain 下降时，正确表述是
“baseline-relative empirical JCT Pareto improvement, uneven benefit”，不是单凭 Jain 判定正式
公平性质失败；高 Jain 也可能只是所有 Job 同样慢。

现有文本四 Job compact evidence 能计算三个 JCT 反事实，但不能事后还原 event-level lag 或
starvation。新 formal 必须保存无损 completion event、ready/backlogged interval、active-set/
weight 变化和 actual work；没有证明时，理论 service bound、DRF sharing incentive、Themis
finish-time fairness 均保持 unavailable。图像 CPU/GPU/bytes 在资源向量和 normalized capacity
未校准前只作 stage mechanism，不冒充 dominant-resource fairness。完整公式与晋级合同见
`evaluation_metrics_survey_20260731.md` §9.3 和
`../experiments/plans/state_aware_work_unit_evaluation_20260808.md` §5.2.10–5.2.12。

#### 5.7.6.1 Bounded-ready 正结果后的归因审核（2026-08-13）

- **开发事实**：在相同 2×4090、K128/$W_e=65,536$、long bulk→5s 后 short foreground 合同下，
  bounded-ready $H_B=0.125W_e$ 两轮达到 12,355/12,367 tok/s、foreground P99
  18.15/17.58s、foreground miss 0%、bulk 30s miss 65.8%/66.6%，通过开发门；$0.25W_e$
  因 bulk miss 75.2%/74.4% 被拒绝。旧文档的 `0.125K/0.25K` 是显示误名：实现实际计算
  `fraction × endpoint work_limit`，不是 request K。
- **归因缺口**：`saor_bounded_ready` 同时改变 concrete-ready pre-registration/execution path 与
  priority/debt selector。现有 static/old-SAOR/FIFO/DRR/VTC 使用 single-head observation，不能回答
  “简单 selector 获得同一个 ready set 后是否已经足够”。当前事实只支持 bounded-ready + guarded
  priority 组合可行，不能把约 30% 吞吐与 foreground tail 改善全部归因给 SAOR 算法。
- **最小决定性门**：在 formal 前把 ready-window 从 selector 解耦，只在 Project 路径内部用相同
  active K/W、ready bytes、arrival、服务与 cache 合同比较 bounded-ready + FIFO、DRR/WFQ、
  external VTC-style、strict-priority/EDF 和 proposed。它们是 internal controls/ablations，不是
  原生 baseline；Daft、Ray Data、vLLM 或产品路径保留原生调度。若简单策略进入同一 Pareto 前沿，论文贡献收敛为 **bounded
  ready-state exposure contract + 最小 guarded release**，删除不必要的复杂 selector；只有
  proposed 有独立增量才进入 1+3 formal。
- **公平分轨**：当前 equal-share Job/class 场景评价 weighted service lag、worst Job 和 work conservation；
  foreground/bulk 是 differentiated service，评价 foreground SLO isolation + bulk reserved-share
  JCT/max lag/longest no-service。bulk 30s 在 static 下已约 67% miss，缺外部业务依据时只作相对
  static guard，不称绝对 bulk SLO。
- **指标修正**：用户 E2E backlog 从 arrival 开始，scheduler 公平 backlog 从
  concrete-ready/registered 开始；completion 时才入账的 actual work 只能构造
  `completion-accounted empirical service lag`。Jain 需配合 max/P95 lag、min/mean、最长无服务、
  三个 JCT 反事实和 request/token SLO goodput。bounded-ready 另报 ready requests/work/bytes、host
  memory、coordinator CPU 与 registration→grant tail，避免只匹配 active envelope 却隐藏缓冲成本。
- **文献补充**：VTC/DLPM 约束共同积压服务与 prefix-locality，Themis/Pollux/PCS 支持 Job 完成
  反事实与多目标 Pareto，JITServe/SCORPIO/ProServe 支持 SLO token goodput、输出不确定性与
  priority 隔离；Agentix/BatchGen 支持把 program/job/batch 作为一等调度对象。它们多数位于
  serving 内部，只迁移指标、work accounting 与 oracle，不作为上游同层 executable baseline。

当前方向裁决为 `Accept with Revisions / attribution-gate-first`。formal 前不继续扫 cap、dynamic K、
reservation、4-Job 或图像；项目内部 matched-observation gate 通过后先完成 2-Job formal，再只选一个
不调参 held-out（reverse/simultaneous arrival、on/off burst 或 prefix-rich）。

2026-08-14 进一步把“完成 2-Job formal”拆成 fail-closed 两步。新的 Project mechanism 配置用
位置平衡 seed，三次 formal 中每个 selector 占三个不同序位；evaluation contract 预先冻结
VTC-style 主参照、foreground P99/empirical completion-lag 5% headline、throughput/bulk JCT/
class SLO/longest-no-service non-inferiority。SAOR 必须从 lossless ledger 证明 recovery grant 对应的
request 已完成，并观测 debt 从 `>=cap` 回到 `<cap`，报告 repayment P95、right-censored 与
unresolved episode；censored 不进入 repayment P95，且不能替代至少一个完整 episode。这只是
completion-granularity empirical repayment，不是理论 bound。首个最终 rehearsal 已用反例证明
“每 Job 单 recovery 在途”不能保证 repayment；修正版不是无界 drain，而是 residual-aware
projected-debt work budget：按活动集权重同时核算所有 own active work 和不可抢占 foreign
residual，显式 `finish_job` 才能 censor，离线从 raw event 重算 projection 与单 quantum
overshoot bound。**来源类型：本地 GPU rehearsal 事实。** 最终 `63d17300` 全新六臂 final rehearsal 已 passed：固定 admission output
cap=256 的 6,144-request audit 通过，15/15 repayment completed、P95 3.234s、0 unresolved，
1,108/1,108 projection 一致；单次相对 VTC-style lag P95 −13.15%、longest no-service
+0.014%，尚不判 winner。独立 raw/SHA 复核已通过。authorization 已逐字段绑定 validation SHA、
commit/root/archive/valid-rehearsal，公平 trace 错误分支已 fail closed，六臂服务延迟、资源/能耗与
pipeline 也已从封存 raw 重汇总。当前完整签名 direct ceiling 为 13,684.90 tok/s，封存 SAOR 为
12,713.03 tok/s，feeding ratio=92.898%<95%；两侧 group/manifest/运行合同/validation/archive SHA
已绑定，足以执行一次性 gate 负判决。由于 PG/Ray clean 未结构化落盘且 n=1，不能声称稳定损失
7.10%；contract 已冻结为
`locked_failed_feeding/formal_authorized=false`，停止当前 1+3 formal，不改门槛、K/W 或
$0.125W_e$。**来源类型：本地 GPU ceiling 事实。**
**来源类型：合理推断。** lag 绝对差 8,231.5 work 约等于 $1.005H_B$，说明结果与 debt-cap
作用方向一致；因 lag 是目标邻近指标，不能脱离 JCT/P99/SLO/no-service/throughput 保护写成用户收益。
frozen-static 因不经过 shared credit，其 registered-ready lag 是 N/A，只参加共同性能/SLO比较；
不能伪造 credit lifecycle，也不能用该 N/A 误杀整张矩阵。

#### 5.7.7 模式优先级矩阵

```
                    落地难度 →
收益 ↓          低（1-2轮可做）        中（需改动 pipeline）     高（需新基础设施）
高              模式1 Hybrid架构        模式5 解耦三阶段          模式10 SFS预演
                模式2 排序优先评估      模式4 不确定性门控
                模式9 简单模型优先      模式17 双信号Deadband
                模式15 Output-Length
中              模式6 Transferable      模式3 多粒度模型          模式12 Batch时间回归
                模式13 轻/中/重分类     模式7 多指标输出
                模式18 Credit-Based     模式8 数据多样化
                                       模式11 LPS模型
                                       模式14 Probe Execution
                                       模式16 USL估计
```

**建议落地顺序**：
1. **第一批**（RC4 短期）：模式 1 Hybrid + 模式 2 排序指标 + 模式 15 Output-length predictor + 模式 9 保持简单
2. **第二批**（RC4 中期）：模式 4 不确定性门控 + 模式 7 多指标输出 + 模式 8 数据多样化
3. **第三批**（RC2 探索）：模式 13 轻/中/重分类 + 模式 11 LPS K_max 选择 + 模式 16 USL 并发估计 + 模式 17 双信号 Deadband
4. **第四批**（RC2 深度）：模式 10 SFS what-if 预演 + 模式 12 token-batch 回归 + 模式 18 Credit-Based Admission
5. **远期**（多 endpoint 后）：模式 5 解耦三阶段 + 模式 3 多粒度模型 + 模式 14 Probe Execution

### 5.8 已有模式与本课题现有工作的映射

| 文献模式 | 本课题已有对应 | 成熟度 |
|---------|-------------|--------|
| Transferable features (COSTREAM) | 15 特征全是物理量（无 workload/模型名编码） | ✅ 已满足 |
| 简单模型优先 (Heinrich R1) | Ridge 161 行 | ✅ 已满足 |
| Grouped hold-out (Heinrich) | 按配置组 SHA-256 split（非按行） | ✅ 已满足 |
| Hybrid 架构 (Heinrich R4) | **缺失** `E2E_base` 公式特征 | 🔴 待补充 |
| 排序指标 (Heinrich R2) | 只报告 MAE/RMSE/R²/MAPE | 🔴 待补充 |
| 多代价指标 (COSTREAM) | 只预测 `e2e_s` | 🟡 待补充 |
| 不确定性输出 (多来源) | 只输出点估计 | 🟡 待补充 |
| SFS 预演 (SFS) | queue-adaptive flush 看 queue depth | 🟡 可替代 |
| LPS K_max 选择 (SFS) | 纯实验暴力搜索 | 🟡 可补充 |
| Output-length predictor (SFS + GRACEFUL) | 只用 `completion_max_tokens` 作为上限代理，无实际输出长度预测 | 🟡 待补充 |
| 结构特征：batch grouping 关系 (Pathak & Mankodi) | 15 特征中无 batch 间分组结构（length-align 的分组大小分布、prefix key 聚类效果） | 🟡 待补充 |
| 语义特征：token 分布特征 (Pathak & Mankodi) | 无 workload token 分布的 skewness、output length 分布等统计特征 | 🟡 待补充 |
| Probe Execution (CONCERTO) | 当前 profile 全部是完整 E2E 运行，无 partial-execution 特征推断 | 🟡 远期探索 |
| USL 并发估计 (SABER) | K_max 选择纯实验暴力搜索，无解析并发-吞吐退化曲线 | 🟡 待补充 |
| 双信号 Deadband (CONCUR) | 所有自适应控制器使用单一信号 + 无 deadband，振荡问题已在 07-19 暴露 | 🟡 待补充 |
| Credit-Based Admission (SCORPIO) | K_max 为全局固定值，不区分请求 SLO 紧松度 | 🟡 远期（需 SLO 异构场景） |

### 5.9 关于"已排除"技术的说明（2026-07-27 审计）

以下技术曾在实验中未表现出优于当前 baselines 的结果，但代码和实验记录均已保留，**不视为永久排除**。当前结论受限于单 GPU、单 workload shape、稳态到达等测试条件——在不同硬件/负载/多租户场景下可能重新体现出价值：

| 技术 | 当前结论 | 保留位置 | 重新激活条件 |
|------|---------|---------|------------|
| AIMD/EWMA-AIMD/PID 自适应准入 | 相对 static K=16 无增量（07-26 shared-vLLM 实验：vLLM waiting=0，AIMD 盯 vLLM waiting 做决策（请求在 Ray 侧排队、waiting 始终为 0） | `code/src/scheduling/submission_control/adaptive.py`、`pid.py` | 改用反映 Ray 侧积压的信号后（如逐请求 completion time 观测） |
| Two-level queue-adaptive flush | 相对 fixed-50ms 无稳定增量（89.4% 时间选 50ms，行为接近 fixed-50） | `code/src/scheduling/submission_control/flush.py` | 多 workload shape / 变长输出 / 多租户到达下重新评估 |
| GNN/Transformer 升级 | 283 行数据远未达到需要 GNN 的规模（Heinrich R1 + Pathak & Mankodi 一致结论） | 未实现（仅保留设计文档） | profile 数据增长到千级/万级行后 |

以上技术的代码路径和实验 CSV 均保持可用状态，后续重新激活时改动量预计较小（主要是接入新观测信号或切换 workload 配置）。

### 5.10 CPU–GPU 异构分阶段执行与远期待办（2026-08-11）

图像正式/诊断证据已把 feeding gap 定位为 CPU prepare、host representation conversion 与
driver/Ray submission 的组合，而不是 PostgreSQL source thread、PCIe 或 GPU forward 单点。
因此新增工作名 HSE（Heterogeneous Staged Execution）作为**执行底座候选**，不增加第三项
研究内容，也不把 Daft/Ray/DALI/Arrow/StarPU 的已有能力重写为项目创新。完整迁移审计、
数据合同、tandem-queue/DPP 模型和实验门禁见
`heterogeneous_ai_dataflow_execution_model_20260811.md`。

串联流水线有基本上界 $X\le\min_s\mu_s$。当前 project CPU16 约 1,666 image/s 与约 19K
image/s GPU-resident ceiling 的比值约 8.8%，和约 9.6% GPU busy 同量级；这支持 prepare supply
是当前木桶，也证明 ready buffer、更多 model inflight 或动态 K 本身不可能把 GPU 长期喂满。
HSE 的 flow/buffer 机制只负责逼近现有 bottleneck capacity 和控制内存；packed uint8、GPU
normalize、DALI 或 derived cache 才可能提高 prepare rate/减少 work，两类收益必须分开归因。

最小增量按顺序冻结（2026-08-12 状态）：

1. ✅ 真实 `pending-prepare → ready-block → pending-model` 队列与 lease；result 目前即时审计；
2. ✅ descriptor + FP32 NCHW block 按 physical bytes/work 预留的 static broker；packed uint8 未做；
3. ⏳ static broker GPU gate 达到冻结 project static 的非劣门后，才接 SAOR Job-head/fairness/SLO；
4. CPU fast path、DALI mixed、signed derived-image cache 分开做 work-reduction 消融；
5. Ray Data/Daft Native 继续由框架拥有调度，不能注入 project broker。

下列候选已登记但状态统一为 `parked-conditional`：

| 候选 | 最小正确性合同 | 重新激活条件 |
|---|---|---|
| prompt 变化感知 | template/segment/tokenizer revision 签名；不拆分单行 vLLM 请求 | HSE/SAOR 主门完成，细粒度签名相对 full-hash 的净决策收益 ≥5% |
| exact 结果复用 | source row/version + full input/prompt + model/processor + decoding 参数完整 cache key | 真实 exact reuse opportunity ≥10%，扣除 lookup/refresh 后 oracle 收益 ≥5% |
| semantic 结果复用 | 与 exact cache 分轨；报告 false-hit、任务质量与失效策略 | 有任务 ground truth 和可接受的质量损失合同后 |
| 数据库级增量推理 | 只对新增/变更 row version 重跑，未变结果按完整 provenance 复用 | CDC/version source 与 exactly-once sink 闭环后 |
| 模型内部增量推理 | 不把 vLLM APC 冒充任意 KV delta update；需要 engine/model 明确支持 | engine 能力、语义等价与 KV 生命周期均验证后 |

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

**主文件**：`experiments/plans/reference/strategy_design_literature_basis.md`（策略口径）、`experiments/plans/reference/strategy_design_implementation_reference.md`（历史实现拆解）

### 7.1 当前策略版本

```text
PostgreSQL planner-visible AI operator（尚未实现）
  → ordinary child plan / snapshot / query lifecycle
  → LOTUS v1.2.4 SemMapNode + prompt/output/error semantics（当前首要迁移）
  → 可替换外部物理 backend
       ├── LOTUS native product path（系统 baseline）
       ├── Daft / Ray Data native graph（框架 baseline）
       ├── project frozen-static（强静态参照）
       └── project state-aware / SAOR（条件性候选）

当前外部物理执行默认点
  → sequential token/work-budget organization
  → fixed request/work capacity + request-level replenishment
  → fixed 50 ms flush（文本已测签名）
  → endpoint/job routing 与 shared credit 只在匹配实验合同中启用

共同支撑与验证
  → 轻量算子代价估计：当前离线，不声称已驱动 SQL plan 或在线 scheduler
  → 图像 AI_EMBED/AI_CLASSIFY：静态与观测证据已完成，动态动作待验证
  → PostgreSQL + pgvector COPY/deferred index：工程 baseline
```

复杂动态策略没有稳定超过同资源上限的强静态点。当前方法研究因此保留候选池，但不把
queue-adaptive、dynamic K、多 actor 或 SAOR 写成已经胜出的默认策略。真实 LOTUS 语义和
PostgreSQL query lifecycle 未完成前，既有 manifest/profiler 结果统一标为外部物理执行证据。

### 7.2 实验阶段

| 阶段 | 当前状态 | 内容与下一步 |
|---|---|---|
| 语义入口 | **当前首要，未开始实现** | 冻结 LOTUS v1.2.4，复用真实 `SemMapNode`、messages、output parser 与错误语义，完成 native/project parity |
| 数据库资格验证 | **语义 parity 后执行** | PostgreSQL extension/planner-visible operator 的 SQL、child plan、snapshot、取消、错误与结果生命周期 |
| 文本数据组织 | **已完成主要机制实验** | fixed/token-budget/length/prefix/BFD/row-cap；结论随 KV 压力与 endpoint consolidation 变化 |
| 文本提交与多 Job | **已完成静态/共享核心证据，动态未普遍胜出** | active-work、request replenish、flush、actor pool、shared credit、1/2/4 Job 与 5s staggered；weighted/held-out/failure migration 条件性保留 |
| 图像多模态 | **静态/观测完成，动态待接** | HSE static GPU 非劣、stage/CE5 在线动作、小规模 pgvector 质量闭环与跨 workload/硬件验证 |
| 算子代价估计 | **离线可行性完成** | 429-run context-LOO 已有 marginal pass；仍需独立时间段/新 workload、预测区间和在线决策增量 |
| 联合关系与写回 | **局部联合实验完成；写回为工程 baseline** | 当前联合候选未显著优于独立拼接；COPY + deferred index 不单列研究内容 |

### 7.3 Baseline 分级

| 层级 | 定义 | 示例 |
|---|---|---|
| 服务上界 | 同模型、同请求、同 endpoint 的直接 serving capacity | vLLM Bench |
| 无 Daft/Ray 强上游 | 受控并发且独立 calibration 的最小客户端/数据库路径 | bounded HTTP、现有数据库 AI_COMPLETE |
| 官方 runtime | 现有框架的官方 AI/HTTP 执行路径 | Daft Native/Ray `prompt()`、Ray Data HTTP Processor |
| 数据库 AI 系统 | 具有 semantic operator/plan optimization 的官方实现 | LOTUS、Palimpzest；SemBench 提供 workload/指标 |
| 本项目策略 | 同 work 下的数据组织、refill、shared credit 与 cost-guided 决策 | static、token-work、fair queue |
| 诊断工具 | 只用于暴露瓶颈，不能作为论文主 baseline | 逐行串行、无界 in-flight |

---

## 8. 知识缺口

| 缺口 | 优先级 |
|---|---|
| LOTUS v1.2.4 的 source-layout/version gate、`SemMapNode` lowering、逐字节 messages、output/error parity | **当前阻断** |
| PostgreSQL 18.3 extension/planner-visible operator 是否能稳定拥有 child plan、snapshot、取消、错误与结果生命周期 | **当前阻断** |
| LOTUS native product path 与数据库管理 row stream 上的 LOTUS/project backend 如何做语义等价、身份清楚的两面板比较 | 高 |
| 图像 HSE static 是否在真实 GPU 上不劣于 direct-dependency static，以及 stage/CE5 状态能否产生受控动作增量 | 高 |
| 小规模 pgvector 写回后 embedding 检索质量是否保持（Recall@K/nDCG 等） | 高 |
| 算子代价估计在独立时间段、新 workload 和新硬件上的误差、配置排序与预测区间 | 高 |
| 代价信息进入 organizer/scheduler 或 SQL 候选计划后，相对无代价信号基线的 decision regret/JCT/SLO 增量 | 高 |
| 多 Job weighted/SLO、held-out 4+ Job、异构 workload、故障迁移和显存异构容量 | 论文阶段 |
| prefix routing 的 4-endpoint 增量如何隔离 endpoint consolidation、模型与饱和深度混淆 | 条件性 |
| Qwen2.5-VL 等多模态生成是否值得进入正文 | 可选；不阻塞主线 |

候选机制的统一发现流程、来源标签、机制卡、fatal-flaw audit、最小实验和放弃
条件见 `experiments/plans/reference/literature_driven_pipeline_optimization_guide.md`。

---

## 9. 文件清单

**2026-08-27 新增**：
- `research/精读文献笔记/kalypso_arxiv2026/kalypso_arxiv2026.md` — Kalypso 全文精读；
  作为 arXiv 核心补充登记，直接收窄 semantic query plan 与 LLM serving 接口处的研究空白；
  不进入当前 Top 15、十五篇速览或已定稿开题正文。

**2026-08-01 更新**：
- `research/existing_ai_operator_execution_chains.md` — 将数据库 AI 执行链归纳为
  in-database、SQL→remote endpoint、queue-worker、distributed data pipeline 四类；
  补 Polar_AI→EAS 与 PolarDB Daft-on-Ray 两条路线，并明确 fused Daft 不能替代
  staged CPU/GPU 强 baseline。

**2026-07-31 新增**：
- `research/evaluation_metrics_survey_20260731.md` — 评估指标体系调研：7 簇精读笔记（49 篇）+ 8 个数据库厂商/标准基准 web 调研，按 10 类归目并对照项目指标做 gap 分析。P0 缺口：TTFT 分位、ITL/TBT 分布、prefix cache hit rate（均已核实为 vLLM 已暴露但采集端未落字段，见该文件 §6）。附录 B 含 7 家厂商 AI 算子测试方法 + PolarDB Lakebase 同栈专项。
- `research/daft_db_gpu_bridge_direction_scope_20260731.md` — 方向 reframe scope：保留
  Daft 三痛点、offline-batch foreknowledge 与 workload 讨论；08-01 已撤回“数据搬运
  必然是瓶颈/执行层结构性空白”的预设，新增 staged baseline 前置条件。

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
- `research/ai_operator_literature_inventory.md` — 65 篇 CCF-A 文献清单
- `research/gpu_scheduler_data_placement_supplement_20260715.md` — GPU 调度补充调研 + Ray 思想映射
- `research/direction_assessment_20260715.md` — 方向评估 + 三岛模型 + 不能声称的结论
- `opening/literature/reading_list.md` — 精读/泛读文献清单

**实验计划文件**：
- `experiments/plans/reference/strategy_design_literature_basis.md` — 策略口径与文献依据
- `experiments/plans/reference/strategy_design_implementation_reference.md` — 历史实现细节与模块拆解
- `experiments/plans/archive/research_design_catalog.md` — 方案目录与评分（已归档，设计历史参考）
- `experiments/plans/baseline_reference.md` — Baseline 矩阵
- `experiments/plans/data_organization_batching.md` — 研究内容一实验计划
- `experiments/plans/service_scheduling_backpressure.md` — 研究内容二实验计划
- `experiments/plans/reference/sink_writeback_coordination.md` — 写回工程参考
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

### 10.5 与本课题的关系：既是底座，也是强 baseline

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

**2026-08-01 口径修正**：Daft/PolarDB 官方已经支持在同一流水线中把 CPU
download/decode/resize 与 GPU 类 UDF 分开声明资源并流式重叠，因此 stage separation、
通用 overlap 和 backpressure 本身不能作为本项目原创贡献。当前 1.296×/1.138× 只证明
项目静态阶段拆分优于校准后的 **fused** UDF，不代表优于 Daft-on-Ray staged pipeline。

本项目剩余可比较的增量是：数据库 job/workload 语义下的 token/frame work 计量、模型
服务/actor 状态感知的请求成形与准入、跨 job shared credit/idle borrowing，以及它们
相对实验开始前选定并在运行期间保持不变的最佳静态配置的 JCT/SLO/fairness 收益。Daft built-in、Ray Data native graph 与
Project static 的能力、容量和 matched-resource 证据已经完成：Daft 在 12K 后受 object-store
容量限制而单列，Ray Data 与 Project 的 120K 同资源比较显示 Project 静态结构约 13%–15%
改善。该结果只证明当前静态执行结构在该比较条件下的表现，当前缺口是 HSE static GPU 非劣、
单一在线动作和质量检查。

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

**论文中完整的历史优化实验清单**（详见 `experiments/plans/reference/strategy_design_implementation_reference.md` §4.7）：

| 优先级 | 实验 | 变量 | 回答的问题 |
|---|---|---|---|
| P0 | batch 粒度对比 | batch_size vs token-budget | 按计算量组批是否优于按行数组批？ |
| P0 | 分组策略对比 | random vs length-align vs prefix-aware | 相似计算量的请求放一起是否减少 straggler？ |
| P0 | 提交节奏对比 | 固定 K_max vs queue-adaptive flush | 自适应提交是否有收益？ |
| P1 | Daft 引擎参数 | into_batches × @daft.cls batch_size | 分区粒度与 GPU UDF batch size 如何匹配？ |
| P1 | 耦合验证 | 数据组织最优 + 提交控制最优 vs joint grid search | 联合调优是否必要？ |
| P2 | 多模态泛化 | 文本 token-budget vs 图像 frame-budget | 策略抽象的模态无关性是否成立？ |
| P1 | 算子代价估计 | 解析模型 + profile + residual；误差、ranking、regret、预测区间 | 预测是否能正确初始化容量并选择组织/路由/提交策略？ |

**Scope 缩减触发条件（历史合同与当前判定）**：
- Month 1 结束前 vLLM baseline 未建立 → 多模态降为 Discussion（未触发，vLLM baseline 已建立）；
- 文本数据组织与提交控制基础消融未完成前，不启动 Daft 多模态 pipeline（条件已满足，图像静态/
  观测证据已完成）；
- VLM 生成实验（Qwen2.5-VL-3B）始终标记为 optional（仍有效）。

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
