# 课题研究方案候选目录

生成日期：2026-07-15
用途：为三个研究内容和跨层协同优化提供可供选择的方案目录，支撑后续实验设计和代码实现决策
方法：基于 57 篇文献（`opening/literature/ai_operator_literature_inventory.md`）和 2026 年 7 月前沿检索，结合 idea-evaluator 五维评分、deep-research 证据纪律和 vibe-research-workflow 工程可行性约束

---

## 目录

1. [评估维度与方法论](#1-评估维度与方法论)
2. [当前硬件与软件约束](#2-当前硬件与软件约束)
3. [RC1：数据组织与批处理构造](#3-rc1数据组织与批处理构造)
4. [RC2：GPU 推理服务状态感知调度与反压](#4-rc2gpu-推理服务状态感知调度与反压)
5. [RC3：结果汇聚与持久化协同](#5-rc3结果汇聚与持久化协同)
6. [跨层联合优化（Cross-Layer Joint Optimization）](#6-跨层联合优化cross-layer-joint-optimization)
7. [方案组合推荐与分阶段路线图](#7-方案组合推荐与分阶段路线图)
8. [风险分析与反证条件](#8-风险分析与反证条件)
9. [Killer Experiment 矩阵](#9-killer-experiment-矩阵)

---

## 1. 评估维度与方法论

### 1.1 评估维度

每个候选方案在以下六个维度上评分（1-5，5 为最优）：

| 维度 | 含义 |
|---|---|
| **文献支撑 (Lit)** | 是否有 CCF-A 论文或工业系统直接支撑 |
| **工程可行性 (Eng)** | 代码实现是否在项目当前技术栈和开发能力内 |
| **硬件可行性 (HW)** | 是否可在单 RTX 5070 (12GB) + 64GB RAM 上验证 |
| **开源依赖 (OSS)** | 依赖的软件是否开源、文档完善、社区活跃 |
| **创新空间 (Nov)** | 与已有工作的差异化程度和新贡献潜力 |
| **实验可验证性 (Exp)** | 是否可设计清晰的消融实验和对照 |

### 1.2 方法论边界

依据 `vibe-research-workflow` 的六条行为准则：
- 方案设计来自文献分析和系统实验，核心判断由用户确认
- 所有引用标注来源类型（论文/官方文档/本地实验/合理推断/待确认）
- 不编造引用，不把未验证方案写成既定方法

依据 `AGENTS.md` §6.5 文献优先设计规则：
- 每个方案标注"从 X 借鉴 Y，组合解决 Z 问题"
- 每个 baseline 标注来源论文或系统

---

## 2. 当前硬件与软件约束

### 2.1 硬件

| 组件 | 规格 | 对方案设计的约束 |
|---|---|---|
| GPU | NVIDIA GeForce RTX 5070, 12GB VRAM | 无法运行 7B+ 模型；embedding 模型（如 all-MiniLM-L6-v2）和 1-3B LLM 可行；多模型并行受限于显存 |
| RAM | 64GB DDR5 | Arrow RecordBatch、Ray object store 有充足空间 |
| CPU | 单机，多核 | 可模拟多 worker，但不是真实多节点 |
| Disk | 本地 NVMe SSD | Lance/Parquet 本地 I/O 充足 |

**关键约束**：所有方案必须在**单机单 GPU** 上可验证。多节点分布式是论文 §8 "未来工作"。

### 2.2 软件

| 组件 | 当前版本/状态 | 备注 |
|---|---|---|
| PostgreSQL | 18.4 本地预演 | 公司平台为 18.3，当前以 18.4 为同构替身 |
| pgvector | 0.8.2 | 支持 vector(384)、HNSW 索引 |
| Ray | 已安装，task/actor 已验证 | Ray Serve、Ray Data 的 autoscaling/routing 接口可调用 |
| Daft | 已安装 | partition/shuffle/join 实验基础 |
| vLLM | 待接入 | 开源，Apache 2.0，社区活跃；作为 G1 baseline |
| Lance | 已安装 | 列式 AI 数据存储，开源 |
| HuggingFace | 本地缓存 `all-MiniLM-L6-v2` | 384 维 embedding |
| Python | `.venv` | psycopg2、pyarrow、requests 等已验证 |

---

## 3. RC1：数据组织与批处理构造

**研究问题**：数据库行数据如何被组织为合适的 batch、partition 和 object，以匹配下游 AI 算子的执行特征。

### 3.1 方案总览

| 编号 | 方案名称 | 难度 | 来源 |
|---|---|---|---|
| **A1.1** | 固定策略 Baseline（Coalesced vs Fine） | ★ | 已有实验 |
| **A1.2** | Workload-Aware Partition 策略 | ★★ | Daft 文档 + Spark SQL Tuning |
| **A1.3** | Selectivity-Aware Cascade + Partition 联动 | ★★★ | Cortex AISQL (SIGMOD 2026) |
| **A1.4** | Token-Aware Prefix Grouping | ★★★ | vLLM prefix caching + SGLang |
| **A1.5** | Learned Partition/Batch 预测 | ★★★★ | Learned Cost Models (SIGMOD 2025) + COSTREAM |
| **A1.6** | Arrow Flight 零拷贝批处理管道 | ★★★★ | Ballista PR#1318 + Spark Arrow Shuffle SPIP |

---

### A1.1：固定策略 Baseline

**方案描述**：对比固定 batch size、固定 partition 数、固定 object 合并方式的端到端表现。这是已有工作（coalesced vs fine），作为其他方案的下界 baseline。

**从何借鉴**：Daft partitioning 文档、Spark SQL Tuning Guide、本项目已有 GPU-backed E2E 实验。

**变体**：
- `batch_size ∈ {32, 64, 128, 256}`（固定 partition=1）
- `partition ∈ {1, 2, 4, 8}`（固定 batch_size=64）
- `object_merge ∈ {none, coalesce_input, coalesce_output}`

**已有实验基础**：
- `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`：coalesced vs fine = 13.4×
- `motivation/results/fake_cpu/analysis.md`：fine/coalesced e2e 比值约 4.01-4.37×

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 3/5 | 官方文档级别，不是论文 |
| Eng | 5/5 | 已有代码，只需参数化 |
| HW | 5/5 | 无额外硬件需求 |
| OSS | 5/5 | 全部已有 |
| Nov | 1/5 | 无创新，纯粹 baseline |
| Exp | 5/5 | 直接可跑 |

**定位**：论文 §7 的 **baseline**，不是方法贡献。

---

### A1.2：Workload-Aware Partition 策略

**方案描述**：根据 workload type（`AI_EMBED` / `AI_FILTER` / `AI_COMPLETE`）和输入特征（行数、文本长度、输出维度）选择 batch size 和 partition 数。

**从何借鉴**：Daft 的 `pre_shuffle_merge` + partition tuning（官方文档）；Spark adaptive query execution 的 partition coalescing。

**策略表**（待实验验证）：

| Workload | 行数 | 推荐 batch_size | 推荐 partition | 理由 |
|---|---|---|---|---|
| AI_EMBED | < 4096 | 256 | 1 | 小规模下 GPU 合并更有效 |
| AI_EMBED | > 16384 | 64 | 4 | 多 partition 并行 + bounded in-flight |
| AI_FILTER | any, selectivity < 0.1 | 32 | 2 | 大部分行被过滤，小 batch 减少无效调用 |
| AI_FILTER | any, selectivity > 0.5 | 128 | 1 | 大部分行都过，大 batch 省 invocation 数 |
| AI_COMPLETE | short (<128 tokens) | 32 | 2 | token 短、batch 可大 |
| AI_COMPLETE | long (>512 tokens) | 8 | 4 | token 长、batch 要小、partition 要多 |

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 3/5 | Daft/Spark 工程文档支撑 |
| Eng | 4/5 | 规则引擎，代码量小 |
| HW | 5/5 | 无额外需求 |
| OSS | 5/5 | 全部已有 |
| Nov | 3/5 | 规则化适配，创新有限；但可作为 A1.5 的 baseline |
| Exp | 5/5 | 规则表可直接消融 |

**定位**：RC1 的**第一个方法贡献点**，但需要与 A1.3/A1.4/A1.5 组合使用才能达到论文级贡献。

---

### A1.3：Selectivity-Aware Cascade + Partition 联动

**方案描述**（AI_FILTER/AI_CLASSIFY 专属）：将 Cortex AISQL 的 Adaptive Model Cascades 思想下推到 partition 层——先用廉价模型（或规则）做预筛选，仅不确定行升级到昂贵模型；partition 的划分与 cascade 的置信度阈值联动。

**从何借鉴**：
- Cortex AISQL (Aggarwal et al., SIGMOD 2026)：Adaptive Model Cascades，小模型处理大部分行，仅不确定行升级（2-6× 加速，90-95% 质量保持）
- Smart (Guo et al., VLDB Journal 2025)：ML 谓词的推理重写和成本最优物理优化（PostgreSQL 实现，最高 1000× 提升）

**三级 Cascade 设计**：

```
Level 1: 规则/关键词过滤（无模型调用）
  → 高置信度 PASS/FAIL → 直接标记，跳过下游模型
  → 不确定 → 进入 Level 2

Level 2: 轻量分类模型（如 DistilBERT，CPU 可跑）
  → 置信度 > 0.9 → 标记结果
  → 否则 → 进入 Level 3

Level 3: 完整 GPU 模型（最昂贵）
  → 处理约 5-15% 原始行数
```

**Partition 联动**：
- Level 1 输出 selectivity 估计 → 决定 Level 2/3 的 partition 数和 batch size
- Cascade 每层输出的行数变化 → 动态调整下游 partition

**关键实验**：
- Cascade 层数消融（1 层 / 2 层 / 3 层）
- 置信度阈值对 selectivity 和端到端延迟的影响
- 与固定策略（A1.2 无 cascade）对比

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | CCF-A 直接支撑（Cortex AISQL + Smart） |
| Eng | 3/5 | 需要接入第二模型（DistilBERT 等），模型管理复杂度 |
| HW | 4/5 | 轻量模型 CPU 可跑，RTX 5070 可跑 Level 3；但需注意显存 |
| OSS | 4/5 | HuggingFace 模型可用；需确认许可 |
| Nov | 4/5 | Cascade 已有人做，但与 partition 联动 + 数据库 AI 算子场景结合是新组合 |
| Exp | 4/5 | 层级消融 + 阈值 sweep 清晰可测 |

**定位**：RC1 的**强方法贡献候选**。论文叙事：Cortex AISQL 在数据库内部做 cascade，本课题在外部执行链路中做 cascade + partition 联动。

**风险**：如果 selectivity 恒为 1（所有行都过 AI_FILTER），cascade 无收益。需要构造 selectivity 有变化的 workload。

---

### A1.4：Token-Aware Prefix Grouping

**方案描述**（AI_COMPLETE 专属）：在 Arrow/Daft batch 构造阶段，按共享 prefix 对提示词分组，同一 prefix group 的请求合并为一个 batch 发送到 GPU——利用 vLLM 的 prefix caching（automatic prefix caching / APC）减少重复 KV cache 计算。

**从何借鉴**：
- vLLM (Kwon et al., SOSP 2023)：PagedAttention + automatic prefix caching
- SGLang (Zheng et al., NeurIPS 2024)：structured LM programs 中的 prefix sharing
- Ray Serve 2025：Custom Request Router 支持 cache-affinity routing（同 prefix → 同 replica，最高 60% 延迟降低）

**实现步骤**：
1. 在 batch 构造阶段对 prompt 提取 system prompt / shared prefix
2. 按 prefix hash 分组（而非按行顺序）
3. 同一 prefix group 达到 `batch_size` 时提交
4. 负载均衡：如果某 group 过大，拆分为子 batch；如果过小，与相邻 prefix group 合并（牺牲 cache 复用）

**关键实验**：
- 有/无 prefix grouping 的 `model_request_wall_s` 和 `tokens/s`
- 不同 prefix 共享率（0% / 30% / 70% / 100%）下的收益
- 与随机分组的对照

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | vLLM (SOSP Best) + SGLang (NeurIPS) + Ray Serve 2025 |
| Eng | 3/5 | 需要在 batch 构造层加 prefix 提取和分组逻辑；工程中等 |
| HW | 4/5 | RTX 5070 可跑 1-3B LLM（如 Qwen2.5-1.5B），prefix cache 在显存内 |
| OSS | 5/5 | vLLM 开源，Apache 2.0 |
| Nov | 4/5 | prefix caching 已有，但在"数据库→外部执行链路"的 batch 构造阶段做 prefix-aware grouping 是新场景 |
| Exp | 4/5 | 共享率 sweep 清晰可测 |

**定位**：RC1 的 **AI_COMPLETE 专属贡献**。论文叙事：现有 prefix caching 在 GPU 推理引擎内部生效，本课题在数据组织阶段主动创建 cache-friendly batch。

**风险**：需要接入真实 LLM（而非 embedding）作为 GPU endpoint；工程难度高于 embedding-only 实验。

---

### A1.5：Learned Partition/Batch 预测

**方案描述**：用轻量学习模型（XGBoost / 小型神经网络）从 workload features 预测最优 `(batch_size, partition, object_merge)` 配置，替代 A1.2 的手工规则表。

**从何借鉴**：
- Learned Cost Models (Heinrich et al., SIGMOD 2025)：对 7 种 LCM 的系统评估，发现 hybrid（学习 + 传统估计）最有效
- COSTREAM (Heinrich et al., 2024)：GNN-based operator placement，零样本泛化到未见硬件/查询
- CONCERTO (Zhang et al., 2025)：GAT + TCN 建模并行操作符间的资源竞争

**实现思路**：
1. **Offline**：grid search `(batch_size × partition × object_merge)` 在多个 workload 上的端到端耗时 → 训练数据集
2. **Features**：`row_count, avg_text_len, avg_token_len, output_dim, selectivity, prefix_share_ratio, endpoint_count`
3. **Model**：XGBoost（简单可解释）或小型 MLP
4. **Inference**：给定 workload features → 输出推荐配置

**与手工规则（A1.2）的对比**：学习模型可以捕捉非线性和交互效应（如 batch_size 和 partition 的 trade-off），而手工规则表只能做离散分段。

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | SIGMOD 2025 + COSTREAM + CONCERTO |
| Eng | 2/5 | 需要大量 grid search 数据、模型训练、特征工程；工程量大 |
| HW | 4/5 | 训练在 CPU 上完成；推理开销可忽略 |
| OSS | 5/5 | XGBoost / PyTorch 开源 |
| Nov | 5/5 | 将 learned cost model 应用于数据库 AI 算子的数据组织决策——新场景 |
| Exp | 3/5 | 需要证明 learned 优于 heuristic，且泛化到未见 workload |

**定位**：RC1 的**最强但最高成本**方案。适合作为论文 §6 的"方法升级"而非第一阶段必做。

**风险**：
- 需要足够多的 grid search 数据（估计 200-500 次 E2E 运行），时间成本高
- 如果手工规则表已经接近最优，learned model 的增量价值可能不足以支撑贡献
- **建议**: 先做 A1.2（手工规则），如果规则表在边界处表现不佳，再做 A1.5

---

### A1.6：Arrow Flight 零拷贝批处理管道

**方案描述**：在数据库→外部 worker 的数据传输中，利用 Arrow Flight gRPC streaming 替代当前的 psycopg2 fetch + 本地 Arrow build，实现端到端零拷贝列式传输。

**从何借鉴**：
- Apache Arrow Flight (arXiv:2204.03032)：列式数据传输协议
- DataFusion Ballista PR#1318（2025 年 9 月合并）：Arrow Flight shuffle 传输优化，从 61s → 11s（6× 加速）
- Spark SPARK-55609 SPIP（2026 年 2 月提出）：Arrow Flight-based shuffle 替代 Netty，预计 2-5× 提升

**工程方案**（两阶段）：
1. **Phase 1（最小可行）**：PostgreSQL → Arrow Flight server 中间层 → 外部 worker 通过 Flight client 读取 batch。替代当前 psycopg2 fetch → Arrow build 路径。
2. **Phase 2（完整）**：Ray object store 中的数据传输也走 Arrow Flight（类似 Spark SPIP 的设计），减少 object store 中的序列化/反序列化。

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 4/5 | Arrow Flight 论文 + Ballista PR + Spark SPIP |
| Eng | 1/5 | 需要部署 Arrow Flight server、改造数据读取路径；工程量大 |
| HW | 5/5 | 单机即可验证（Flight server 可在本地） |
| OSS | 5/5 | Apache Arrow Flight 开源 |
| Nov | 3/5 | 工程改进为主；数据库→外部 worker 的 Flight 集成有场景新意但不构成强方法贡献 |
| Exp | 2/5 | 对比 psycopg2 直接读 vs Flight 传输；在当前数据规模下（< 20000 rows）可能收益不明显 |

**定位**：**论文 §8 讨论/未来工作**。当前数据规模（最多 16384 行、几十 MB 级别）下，数据传输不是第一瓶颈。如果后续扩展到百万行级别，A1.6 才有可观测收益。

---

### RC1 方案汇总矩阵

| 编号 | 方案 | 难度 | Lit | Eng | HW | OSS | Nov | Exp | 综合 | 推荐阶段 |
|---|---|---|---|---|---|---|---|---|---|---|
| A1.1 | 固定策略 Baseline | ★ | 3 | 5 | 5 | 5 | 1 | 5 | **4.0** | Phase 0（已完成） |
| A1.2 | Workload-Aware Partition | ★★ | 3 | 4 | 5 | 5 | 3 | 5 | **4.2** | Phase 1 |
| A1.3 | Selectivity-Aware Cascade | ★★★ | 5 | 3 | 4 | 4 | 4 | 4 | **4.0** | Phase 2 |
| A1.4 | Token-Aware Prefix Grouping | ★★★ | 5 | 3 | 4 | 5 | 4 | 4 | **4.2** | Phase 2 |
| A1.5 | Learned Partition 预测 | ★★★★ | 5 | 2 | 4 | 5 | 5 | 3 | **4.0** | Phase 3（可选） |
| A1.6 | Arrow Flight 零拷贝管道 | ★★★★ | 4 | 1 | 5 | 5 | 3 | 2 | **3.3** | §8 讨论 |

---

## 4. RC2：GPU 推理服务状态感知调度与反压

**研究问题**：在数据组织给定 batch 和 partition 后，如何根据 GPU 推理服务的动态状态调节任务提交、路由和并发控制。

这是你最关心的部分，也是方案最丰富的部分。

### 4.1 方案总览

| 编号 | 方案名称 | 难度 | 来源 |
|---|---|---|---|
| **A2.1** | Bounded In-Flight 提交控制 | ★★ | Ray Serve backpressure + 本项已有 `bounded_wait_s` |
| **A2.2** | Workload-Aware Actor Pool + Routing | ★★★ | Ray Serve 2025 Custom Request Router |
| **A2.3** | 队列理论驱动的自适应 In-Flight | ★★★ | Queueing Theory + Multi-Bin Batching |
| **A2.4** | 反馈驱动自适应控制（MAB） | ★★★★ | PRAS (MAB-based) + OCE-CRS (Bandit) |
| **A2.5** | 两层调度（Engine + Cluster） | ★★★★★ | NexusSched (2025) |
| **A2.6** | 优先级感知抢占调度 | ★★★★ | GFS (Alibaba) + DARIS |
| **A2.7** | Cost-Model-Driven Joint Optimization | ★★★★ | FlexPushdownDB + 本课题跨层联动 |

---

### A2.1：Bounded In-Flight 提交控制

**方案描述**（最小可行 RC2 方案）：在 Ray task/actor 提交处维护 in-flight 计数器，确保同时发往 GPU endpoint 的请求数不超过上限 `K_max`。

**从何借鉴**：
- Ray Serve 的 `max_concurrent_queries` 和 backpressure 机制
- Orca (OSDI 2022)：iteration-level scheduling 的粒度控制
- 本课题已有信号：fine-grained 实验（1024 行）中 `bounded_wait_s = 10.124s`；早期 backpressure 模拟中 `queue_limit=8` 将 queue wait 从 4768ms 降到 114ms

**实现**：

```python
class BoundedSubmitController:
    def __init__(self, max_in_flight: int, endpoints: list[str]):
        self.semaphore = asyncio.Semaphore(max_in_flight)
        self.endpoints = endpoints
        self.endpoint_idx = 0  # round-robin

    async def submit(self, batch):
        await self.semaphore.acquire()
        try:
            endpoint = self.endpoints[self.endpoint_idx % len(self.endpoints)]
            self.endpoint_idx += 1
            result = await call_endpoint_async(endpoint, batch)
            return result
        finally:
            self.semaphore.release()
```

**变体**：
- `K_max` 静态配置：`{1, 2, 4, 8, 16, 32, unbounded}`
- `K_max` 动态：根据 endpoint 队列深度反馈调节（过渡到 A2.3）
- Round-Robin vs Least-Connection endpoint 选择

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 4/5 | Orca (OSDI) + Ray Serve docs |
| Eng | 5/5 | Semaphore 包装，~50 行代码 |
| HW | 5/5 | 无额外需求 |
| OSS | 5/5 | asyncio 标准库 |
| Nov | 2/5 | 工程上显而易见的优化；单独不足以成为论文贡献 |
| Exp | 5/5 | `K_max` sweep 直接可跑 |

**定位**：RC2 的 **baseline + 必要组件**。论文中不作为独立贡献，而是 A2.2/A2.3/A2.7 的底层机制。

**关键实验**：
- `K_max × endpoint_count` 对 `queue_wait_s`、`bounded_wait_s`、`operator_wall_s` 的影响
- 验证"unbounded = bad"的量化证据

---

### A2.2：Workload-Aware Actor Pool + Routing

**方案描述**：为不同 workload type 创建独立的 Ray actor pool，每个 pool 配置独立的 `max_in_flight` 和 `batch_size`；Router 根据 workload 特征分发请求到对应 pool。

**从何借鉴**：
- Ray Serve 2025 Custom Request Router：支持 cache-affinity routing、model multiplexing、latency-aware routing
- DistServe (OSDI 2024)：prefill/decode 分离到不同 GPU，与本方案的"不同 workload 分池"共享设计哲学
- Apple/Uber 的多租户 Ray GPU 调度：hierarchical resource pools + elastic borrowing

**架构设计**：

```
                    ┌──────────────────────────────────────┐
                    │         WorkloadRouter               │
                    │   read batch.workload_type field      │
                    └──┬──────────┬──────────────┬─────────┘
                       │          │              │
              ┌────────▼───┐ ┌───▼────────┐ ┌──▼──────────┐
              │ EmbedPool   │ │ FilterPool  │ │ GenPool     │
              │             │ │             │ │             │
              │ pool_size:4 │ │ pool_size:2 │ │ pool_size:1 │
              │ max_ﬂight:16│ │ max_ﬂight:8 │ │ max_ﬂight:4 │
              │ batch: 128  │ │ batch: 64   │ │ batch: 8    │
              │ endpoint:   │ │ endpoint:   │ │ endpoint:   │
              │  :8000      │ │  :8001      │ │  :8002      │
              └─────────────┘ └─────────────┘ └─────────────┘
                       │          │              │
                       └──────────┼──────────────┘
                                  │
                    ┌─────────────▼────────────────────────┐
                    │  GlobalResourceGuard                  │
                    │  total_GPU_memory / per_request_KB    │
                    │  = max_concurrent_across_all_pools    │
                    └──────────────────────────────────────┘
```

**关键设计点**：
1. **分池隔离**：AI_COMPLETE 的长尾请求不会阻塞 AI_EMBED 的高吞吐请求
2. **全局资源上限**：所有 pool 共享一个 GPU 显存预算，防止某个 pool 吃满显存
3. **跨池弹性借用**（进阶）：如果 FilterPool 空闲，EmbedPool 可以临时增加 `max_in_flight`
4. **Endpoint 亲和性**：同一 pool 绑定特定 endpoint（可配合 vLLM prefix cache 预热）

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | Ray Serve 2025 Custom Router + DistServe (OSDI) + Apple/Uber 多租户 |
| Eng | 3/5 | Ray actor pool 管理 + Router 逻辑，中等工程 |
| HW | 4/5 | RTX 5070 有限显存约束了同时可加载的模型数；需要小模型 |
| OSS | 5/5 | Ray Serve + vLLM + HuggingFace 开源 |
| Nov | 4/5 | 工作负载感知的分池路由在数据库 AI 算子场景中是新应用 |
| Exp | 4/5 | 分池 vs 共享池消融 + 混合 workload 测试 |

**定位**：RC2 的**核心方法贡献之一**。论文叙事：现有模型服务系统（vLLM/Orca）在 GPU 内部统一调度，不区分 workload 类型；本方案在外部提交层做 workload-aware 分池。

**风险**：
- 单 GPU 场景下，分池的收益可能不如多 GPU 场景明显（因为所有请求最终都在同一 GPU 上运行）
- **缓解**：通过区分 `max_in_flight` 和优先级来体现分池价值（即使物理上共享 GPU）

---

### A2.3：队列理论驱动的自适应 In-Flight

**方案描述**：将 A2.1 的静态 `K_max` 升级为基于队列状态的自适应控制——根据 GPU endpoint 的 queue depth、avg_latency 和 token backlog 动态调整 in-flight 上限。

**从何借鉴**：
- Queueing, Predictions, and LLMs (Mitzenmacher & Shahout, Stochastic Systems 2025)：排队论 + LLM 推理调度的系统综述
- Multi-Bin Batching (arXiv:2412.04504, 2024)：从排队论角度形式化 LLM batching，按预测执行时间分桶
- NexusSched LENS (2025)：GPU 物理启发的在线性能模型，`T(B,S) = τ₀ + Work(S)/Thr(B,S) + τ_B·B + τ_S·S`

**控制算法**：

```
目标：最大化 throughput 同时保持 avg_queue_wait < target_ms

每个决策周期（如每 500ms）：
1. 采集信号：
   - queue_depth: GPU endpoint 当前等待队列长度
   - avg_latency_ms: 最近 10 个请求的平均延迟
   - gpu_utilization: nvidia-smi 快照（如果可采集）
2. 调整规则：
   if queue_depth > threshold_high:
       max_in_flight = max(max_in_flight * 0.8, min_in_flight)
   elif queue_depth < threshold_low and gpu_utilization < 60%:
       max_in_flight = min(max_in_flight * 1.2, max_in_flight_cap)
   else:
       max_in_flight = max_in_flight  # 保持
```

**排队论视角**（论文 §5 理论分析素材）：
- 将 GPU endpoint 建模为 M/M/1/K 队列（Poisson arrival, exponential service, single server, finite capacity K）
- `K_max` 即为系统容量；`K_max` 过大 → queue wait 增长；`K_max` 过小 → GPU 空闲
- 最优 `K_max` = argmin(E[e2e_latency]) = f(arrival_rate, service_rate)
- 引用 Mitzenmacher & Shahout (2025) 作为理论框架

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | Mitzenmacher 综述 (Stochastic Systems 2025) + Multi-Bin Batching + NexusSched |
| Eng | 3/5 | 信号采集 + PID 控制器逻辑，中等 |
| HW | 5/5 | 无需额外硬件 |
| OSS | 5/5 | 无特殊依赖 |
| Nov | 4/5 | 排队论分析已有，但将其与 batch 构造层联动是新的 |
| Exp | 4/5 | 静态 vs 自适应消融 + 不同 target_ms 对比 |

**定位**：A2.1 的**理论升级版**。适合与 A2.2 组合使用——每个 actor pool 独立运行自适应控制器。

---

### A2.4：反馈驱动自适应控制（Multi-Armed Bandit）

**方案描述**：将 in-flight 上限和 endpoint 选择建模为 Multi-Armed Bandit 问题——每个"arm"是一个 `(K_inflight, endpoint)` 组合，在线学习最优选择。

**从何借鉴**：
- PRAS (Sun et al., 2025)：MAB-based 在线 profiling 和适配，用于边缘推理管线调度
- OCE-CRS (IEEE Trans. Services Computing, Nov-Dec 2025)：Contextual Combinatorial Bandit（CN_DUCB）用于云边 LLM 调度，支持延迟反馈
- 排队论综述 (Mitzenmacher & Shahout, 2025)：预测不可靠时的 fallback 策略

**Bandit 建模**：

```
Arms: { (K=4, endpoint=0), (K=8, endpoint=0), (K=4, endpoint=1), ... }
Context: workload_type, avg_text_len, time_of_day (if workload varies)
Reward: -e2e_latency（最小化延迟 = 最大化负延迟）
Exploration: ε-greedy 或 UCB
Feedback delay: 每次 e2e 运行后才观察到 reward（~几秒到几十秒）
```

**与 A2.3 的对比**：
- A2.3 是 reactive（观察到 queue depth 后调整）
- A2.4 是 learning-based（从历史中学习哪些配置在什么 context 下表现好）

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | PRAS + OCE-CRS + Mitzenmacher 2025 |
| Eng | 2/5 | Bandit 实现 + context 特征化 + 在线学习循环，工程复杂 |
| HW | 5/5 | 无额外需求 |
| OSS | 4/5 | 可用 sklearn/sklearn 的 Bandit 实现或手写 |
| Nov | 5/5 | 将 Bandit 应用于数据库 AI 算子的 in-flight 控制——新 |
| Exp | 3/5 | 需要大量在线运行来训练；收敛速度可能需要调优 |

**定位**：RC2 的**进阶方案**。适合放在论文 §6 "方法扩展"或作为 A2.3 的对照。**不建议作为第一阶段必做**，因为工程成本高且与 A2.3 有一定重叠。

**风险**：
- Bandit 收敛需要较多迭代（估计 100+ 次端到端运行），实验时间长
- 如果 A2.3 的 PID 控制已经接近最优，Bandit 的增量可能很小
- **建议**：先做 A2.3，如果自适应控制的效果不稳定或依赖参数调优，再引入 A2.4 做对比

---

### A2.5：两层调度（Engine + Cluster）

**方案描述**：从 NexusSched (2025) 借鉴两层架构——Engine 层做在线性能建模和 SLO-aware 自适应批处理，Cluster 层做前瞻性路由和负载均衡。

**从何借鉴**：
- NexusSched (Zhang et al., 2025)：两层调度，LENS (Engine) + PRISM (Cluster)，43% SLO 达标提升，3× 吞吐
- Ray Serve 2025 Custom Autoscaling：跨 deployment 的联合扩缩 + 外部扩缩 API

**在本课题中的适配**：

```
Engine Layer (Per-Endpoint):
  → 接入 vLLM continuous batching（替代手动 batch）
  → 实时采集：batch_execution_time, queue_depth, KV_cache_usage
  → 输出到 Cluster Layer 做路由决策

Cluster Layer (Router):
  → 基于 Engine Layer 的状态向量做前瞻性路由
  → 决策：哪个 endpoint 有最短预期完成时间
  → 调节：endpoint_count（自动扩缩到新的 Ray Serve replica）
```

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | NexusSched 直接对应 |
| Eng | 1/5 | 需要深入集成 vLLM engine、改造路由逻辑；工程量大 |
| HW | 3/5 | Engine 层需要 GPU profiling 接口（nvidia-smi 粒度可能不够） |
| OSS | 5/5 | vLLM + Ray Serve 开源 |
| Nov | 5/5 | 两层调度在 DB AI 算子场景是新的 |
| Exp | 2/5 | 需要大量系统集成；实验复杂 |

**定位**：**论文 §8 未来工作 / 展望**。硕士论文阶段不建议作为主线，但可以在 §8 中论述为"下一步扩展方向"。

**理由**：单 GPU 场景下 Engine + Cluster 两层架构的收益难以体现；需要多 GPU 集群才能观察到 Cluster 层的 routing 收益。

---

### A2.6：优先级感知抢占调度

**方案描述**：为不同 workload 分配优先级——AI_EMBED（高吞吐、可等待）低优先级，AI_COMPLETE（长尾、token 密集）中优先级，AI_FILTER 的 Level 1 快速预筛选高优先级——支持高优先级任务抢占低优先级任务的 GPU 时间片。

**从何借鉴**：
- GFS (Alibaba/SJTU, 2025)：预测性 spot 实例管理，33% 驱逐率降低，44.1% 排队延迟降低
- DARIS (IEEE, June 2025)：优先级感知实时 DNN 调度，15% 吞吐提升，高优先级 100% 期限满足
- ω-Boost policy (Yu & Scully 2024, Harlev et al. 2025)：软优先级函数，在 FCFS 和 SJF 之间插值

**在本课题中的设计**：

```
Priority Queue:
  P0 (最高): AI_FILTER Level 1 规则预筛选（几乎无 GPU 开销，先行过滤）
  P1:        AI_COMPLETE 短 prompt（< 128 tokens，快速完成）
  P2:        AI_EMBED batch（高吞吐，但单 batch 延迟不敏感）
  P3 (最低): AI_COMPLETE 长 prompt（> 512 tokens，长尾）

Preemption 策略（简化版）：
  - 不使用真正的 GPU 时间片抢占（需要 GPU driver 层面支持）
  - 而是使用 queue insertion：高优先级请求插入队头，不等低优先级任务完成后才提交
```

**简化适配**（针对本项目）：
- 不实现真正的 GPU kernel 级抢占（需要 CUDA MPS/MIG 或 eBPF GPU scheduler）
- 在提交队列层面做优先级排序：Router 维护 per-pool 优先级队列
- 高优先级请求可以"插队"但不抢占已在执行的请求

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | GFS + DARIS + ω-Boost |
| Eng | 3/5 | 优先级队列实现 + 插队逻辑，中等 |
| HW | 5/5 | 无需 GPU 级抢占支持 |
| OSS | 5/5 | asyncio.PriorityQueue 标准库 |
| Nov | 4/5 | 数据库 AI 算子场景的优先级感知调度是新组合 |
| Exp | 4/5 | 优先级队列 vs FIFO 消融，多 workload 混合测试 |

**定位**：RC2 的**可选增强**。如果 workload 混合场景下存在明显的"快速任务等长尾任务"问题，A2.6 的优先级队列可以提供额外收益。

---

### A2.7：Cost-Model-Driven Joint Optimization

**方案描述**：这是 RC2 与 RC1、RC3 的桥梁方案——不再单独调 GPU 调度参数，而是将 `(B_gpu, N_actor, K_inflight, routing_strategy)` 作为联合决策变量，通过 cost model 返回 Pareto-optimal 配置。

**从何借鉴**：
- FlexPushdownDB (Yang et al., PVLDB 2021)：代价驱动的 Compute-vs-Storage Pushdown 决策模型
- COSTREAM (Heinrich et al., 2024)：GNN-based operator placement 代价模型，21× 中位加速
- GRACEFUL (Wehrstein et al., 2025)：UDF 感知的 learned cost estimator，50× 加速

**详见 §6 端到端流程调优增强实验**。这里只说明：A2.7 是 RC2 在阶段间耦合分析中的可选增强角色——当阶段级调优后仍存在明显端到端瓶颈时，再把 GPU 调度参数与 batch size、write batch size 放入同一优化空间。

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | FlexPushdownDB (VLDB A) + COSTREAM + GRACEFUL |
| Eng | 2/5 | 需要 grid search 数据 + 模型训练 + 在线决策接口 |
| HW | 5/5 | 训练在 CPU |
| OSS | 5/5 | XGBoost / PyTorch |
| Nov | 4/5 | 可作为阶段间耦合明显时的增强贡献 |
| Exp | 3/5 | 需要先完成 RC1 和 RC2 各自的消融，再验证联合优化的增量收益 |

**定位**：**可选增强贡献**。在端到端流程调优完成后，用于分析 Independent Best 与端到端联合配置之间是否存在明显差异。

---

### RC2 方案汇总矩阵

| 编号 | 方案 | 难度 | Lit | Eng | HW | OSS | Nov | Exp | 综合 | 推荐阶段 |
|---|---|---|---|---|---|---|---|---|---|---|
| A2.1 | Bounded In-Flight | ★★ | 4 | 5 | 5 | 5 | 2 | 5 | **4.3** | Phase 1（必要 baseline） |
| A2.2 | Workload-Aware Actor Pool | ★★★ | 5 | 3 | 4 | 5 | 4 | 4 | **4.2** | Phase 2（核心） |
| A2.3 | 队列理论自适应 In-Flight | ★★★ | 5 | 3 | 5 | 5 | 4 | 4 | **4.3** | Phase 2（核心） |
| A2.4 | MAB 反馈控制 | ★★★★ | 5 | 2 | 5 | 4 | 5 | 3 | **4.0** | Phase 3（可选） |
| A2.5 | 两层调度 | ★★★★★ | 5 | 1 | 3 | 5 | 5 | 2 | **3.5** | §8 讨论 |
| A2.6 | 优先级抢占调度 | ★★★★ | 5 | 3 | 5 | 5 | 4 | 4 | **4.3** | Phase 2（可选增强） |
| A2.7 | Cost-Model Joint Opt | ★★★★ | 5 | 2 | 5 | 5 | 5 | 3 | **4.2** | Phase 3 |

---

## 5. RC3：结果汇聚与持久化协同

**研究问题**：模型调用阶段被优化后，结果如何高效汇聚和写回，避免持久化成为新的端到端瓶颈。

### 5.1 方案总览

| 编号 | 方案名称 | 难度 | 来源 |
|---|---|---|---|
| **A3.1** | Driver Fan-In 后批量写回（Baseline） | ★ | 已有实验 |
| **A3.2** | Worker-Direct Blind Append + Background Merge | ★★★ | Delta Lake (PVLDB 2020) |
| **A3.3** | Queue-Worker 解耦写回 | ★★★ | pgai Vectorizer Worker |
| **A3.4** | Staging Table + Deferred Index Build | ★★ | PostgreSQL 官方文档 + TurboVecDB |
| **A3.5** | MOR-Style Deferred Compaction | ★★★★ | Iceberg v3 Deletion Vectors (2025) |
| **A3.6** | KV-Separated Storage for Embeddings | ★★★ | WiscKey (FAST 2016) |
| **A3.7** | Tiered Sink Selection | ★★★ | Lance + pgvector + Parquet 组合 |
| **A3.8** | Joint B_gpu ↔ B_write Co-optimization | ★★★★ | 跨层联动（A2.7 的 RC3 侧） |

---

### A3.1：Driver Fan-In 后批量写回

**方案描述**：当前方式——所有 Ray task/actor 结果经 `ray.get` 汇聚到 driver 进程，再用 `psycopg2 execute_values()` 逐批 UPSERT。

**已有实验基础**：
- `motivation/results/gpu/ai_embed_chain_breakdown_20260712.md`：16384 行下 `writeback_s = 6.586s`（占总时间 50%）
- `motivation/results/gpu/pgvector_writeback_20260714.md`：JSON text (1.567s) vs pgvector(384) (0.897s) 的对比

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 2/5 | 工程 baseline，无文献支撑 |
| Eng | 5/5 | 已有代码 |
| HW | 5/5 | — |
| OSS | 5/5 | — |
| Nov | 1/5 | — |
| Exp | 5/5 | 已有结果 |

**定位**：RC3 的 **baseline**。论文中作为 BL2 的"写回默认策略"。

---

### A3.2：Worker-Direct Blind Append + Background Merge

**方案描述**：Ray worker 在完成 embedding 计算后，不经过 driver fan-in，直接写入 PostgreSQL staging table（盲追加，无锁）。后台定时任务批量合并 staging table 到主表并重建 HNSW 索引。

**从何借鉴**：
- Delta Lake (Armbrust et al., PVLDB 2020)：optimistic concurrency + blind append
- TurboVecDB (PVLDB 2025)：并行 I/O + 空间感知插入，HNSW index build 减少 98.4%
- 本课题 baseline 矩阵 W1/W2/W3

**架构**：

```
Ray worker 1 ──► 本地攒批 (B_write=256) ──► COPY to staging_table
Ray worker 2 ──► 本地攒批 (B_write=256) ──► COPY to staging_table
Ray worker N ──► 本地攒批 (B_write=256) ──► COPY to staging_table
                                                    │
                                     ┌──────────────▼────────────────┐
                                     │ Background Merge Worker       │
                                     │  INSERT INTO main_table       │
                                     │  SELECT ... FROM staging_table│
                                     │  ON CONFLICT DO UPDATE        │
                                     │  (定期执行，如每 30s 或每 N 批)│
                                     └──────────────┬────────────────┘
                                                    │
                                     ┌──────────────▼────────────────┐
                                     │ Deferred HNSW Index Build      │
                                     │  DROP INDEX → INSERT → CREATE  │
                                     │  (低频，如每 10000 行)         │
                                     └───────────────────────────────┘
```

**与 driver fan-in 的对比**：
- Driver fan-in：串行写回（一个连接），`writeback_s = 6.586s`（16384 行）
- Worker-direct：N 个 worker 并行写 staging table，然后再 merge——写回时间可以降为 max(单个 worker 写回时间) ≈ `6.586s / N_worker`

**关键实验**：
- `N_worker × B_write` 对端到端耗时的影响
- staging table 行数触发 merge 的阈值
- 与 driver fan-in（A3.1）的端到端对比

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | Delta Lake (PVLDB A) + TurboVecDB (PVLDB A) |
| Eng | 3/5 | 需要 worker-side DB connection management + merge 逻辑 |
| HW | 5/5 | 单机即可验证 |
| OSS | 5/5 | psycopg2 + PostgreSQL |
| Nov | 4/5 | 盲追加 + 后台合并是已知模式，但在 DB AI 算子 writeback 场景的应用是新组合 |
| Exp | 4/5 | worker-direct vs driver fan-in 消融清晰 |

**定位**：RC3 的**核心方法贡献之一**。论文叙事：现有 writeback 优化（COPY + 延迟索引）是工程 best practice，但不考虑与上游 GPU 调度的联动——worker-direct writeback 将写回并行化并移除 driver fan-in 瓶颈。

**风险**：
- PostgreSQL 连接数和并发写入压力：需要测试 `N_worker × concurrent COPY` 的瓶颈
- Staging table merge 本身可能成为新瓶颈
- **建议**：先从 2 worker 开始测试，逐步增加到 4-8

---

### A3.3：Queue-Worker 解耦写回

**方案描述**：借鉴 pgai vectorizer worker 的设计——模型计算结果不是由 Ray worker 直接写回，而是放入队列表；独立的 writeback worker 轮询队列并负责写回。

**从何借鉴**：
- pgai Vectorizer Worker (Timescale)：触发器→队列表→外部 worker 轮询→各自写回；`FOR UPDATE SKIP LOCKED` + advisory lock
- 本项目 baseline 矩阵 W4

**架构**：

```
Ray GPU worker ──► 写入 result_queue 表（轻量 INSERT，无索引冲突）
                         │
          ┌──────────────┴──────────────┐
          │  Writeback Worker 1          │  Writeback Worker 2
          │  SELECT ... FROM             │  SELECT ... FROM
          │  result_queue                │  result_queue
          │  FOR UPDATE SKIP LOCKED      │  FOR UPDATE SKIP LOCKED
          │  LIMIT 256                   │  LIMIT 256
          │  → UPSERT main_table         │  → UPSERT main_table
          │  → DELETE FROM result_queue  │  → DELETE FROM result_queue
          └──────────────────────────────┴──────────────┘
```

**与 A3.2 的对比**：
- A3.2：worker 直接写 staging table，后台 merge
- A3.3：worker 写队列表，独立的 writeback worker 消费
- A3.3 的写回解耦更彻底：GPU worker 不再等待任何写回操作

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 4/5 | pgai (工程) + Rafiki (PVLDB A) |
| Eng | 2/5 | 需要额外的队列表 + writeback worker 进程管理 |
| HW | 5/5 | 单机 |
| OSS | 5/5 | — |
| Nov | 3/5 | pgai 已有类似设计 |
| Exp | 4/5 | 三种写回路径（driver/worker-direct/queue）三路对比 |

**定位**：RC3 的**对照方案**。论文中与 A3.1、A3.2 形成三路对比实验。

---

### A3.4：Staging Table + Deferred Index Build

**方案描述**：最轻量的工程优化——数据先 COPY 到 unlogged staging table，积累到一定量后批量 MERGE + 重建 HNSW 索引。

**从何借鉴**：
- PostgreSQL 官方文档 §14.4（Populating a Database）
- pgvector Issues #400, #430
- TurboVecDB (PVLDB 2025)：并行 I/O + 空间感知插入

**关键配置**：
- Unlogged table（跳过 WAL，加速写入）
- `COPY ... FROM STDIN`（替代逐行 INSERT）
- `CREATE INDEX ... ON staging_table` → 然后 `ALTER INDEX ... SET LOGGED`（事后建索引远比增量插入快）
- 合并触发条件：行数阈值或时间阈值

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 3/5 | 官方文档 + pgvector Issues |
| Eng | 5/5 | SQL 层面操作，几乎无额外代码 |
| HW | 5/5 | — |
| OSS | 5/5 | — |
| Nov | 2/5 | 已知工程最佳实践 |
| Exp | 5/5 | 与直接 UPSERT 的简单对比 |

**定位**：RC3 的**工程 baseline**。论文 B 系列实验必须做——确认 COPY + deferred index 为当前最优 writeback baseline。

---

### A3.5：MOR-Style Deferred Compaction

**方案描述**：将 Iceberg v3 的 Merge-on-Read + Deletion Vectors 模式应用到 embedding 向量存储——写回时只追加新行和删除标记（不重写数据文件），读取时在内存中合并删除标记；后台 compaction 定期物理合并。

**从何借鉴**：
- Iceberg v3 Deletion Vectors (2025)：delete 操作从 3.126s → 1.407s（55% 加速），文件大小减少 73.6%
- WiscKey (FAST 2016)：KV 分离，避免 compaction 重写大 value

**在 PostgreSQL 环境中的近似实现**：

```
主表: embeddings_main (id, text, embedding, is_deleted, version)
增量表: embeddings_delta (id, text, embedding, version, op_type)

写回: INSERT INTO embeddings_delta（盲追加，快）
读:   SELECT ... FROM embeddings_main m
      LEFT JOIN embeddings_delta d ON m.id = d.id
      WHERE m.is_deleted = FALSE
      AND (d.op_type IS NULL OR d.op_type != 'DELETE')
      -- 应用层合并删除标记

Compaction: 定期将 embeddings_delta 合并到 embeddings_main，清理已删除行
```

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | Iceberg v3 (2025) + WiscKey (FAST A) |
| Eng | 2/5 | 需要实现类似 MOR 的读路径和应用层合并逻辑 |
| HW | 5/5 | — |
| OSS | 5/5 | — |
| Nov | 4/5 | 将 Iceberg MOR 思想应用到 DB embedding writeback 是新场景 |
| Exp | 3/5 | MOR vs 直接 UPSERT 对比；但实现成本高 |

**定位**：RC3 的**进阶方案**。适合放在 §6 "方法扩展"中讨论，不建议作为第一阶段。**只在写回成为主导瓶颈且其他简单优化已穷举时考虑**。

**风险**：
- 实现一个完整的 MOR 读路径在 PostgreSQL SQL 层面可能不够高效
- 如果行数不大（< 100K），直接 UPSERT 的收益可能就足够了
- Iceberg 的 compaction 是针对文件级存储设计的，PostgreSQL 的 page 级存储不完全适用

---

### A3.6：KV-Separated Storage for Embeddings

**方案描述**：将 embedding 向量从主表分离到独立的向量存储（类似 WiscKey 的 vLog），主表只存元数据和向量引用。这避免了在非向量查询中携带大向量列。

**从何借鉴**：
- WiscKey (Lu et al., FAST 2016)：LSM-tree 只存 key，大 value 存在独立 vLog
- Lance (Pace et al., arXiv 2025)：面向 AI/ML 的列式存储，自适应编码

**实现方案**（两选一）：

**方案 A：PostgreSQL + Lance 混合**
```
PostgreSQL 主表: (id, text, metadata, lance_uri)
Lance:           (id, embedding[384])
读路径: JOIN on lance_uri
```

**方案 B：PostgreSQL TOAST 优化**
```
利用 PostgreSQL 的 TOAST 机制自动将大向量列外存
设置较低的 toast_tuple_target 使向量列自动被 TOAST 外存
非向量查询时不读取 TOAST 表
```

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | WiscKey (FAST A) + Lance |
| Eng | 3/5 | 方案 B（TOAST 优化）较简单；方案 A 需要 Lance 集成 |
| HW | 5/5 | — |
| OSS | 5/5 | Lance 开源 |
| Nov | 3/5 | WiscKey 思想在 embedding 场景的应用 |
| Exp | 4/5 | KV 分离 vs 内联存储的简单对比 |

**定位**：RC3 的**可选增强**。适合作为 sink 对照实验（C 系列）的一部分。

---

### A3.7：Tiered Sink Selection

**方案描述**：根据数据热度使用不同 sink——热数据（最近写入、频繁查询）放 pgvector（支持事务和索引），温数据放 Lance（列式扫描快），冷数据放 Parquet（归档成本低）。

**从何借鉴**：
- Rafiki (Wang et al., PVLDB 2018)：ML 推理结果的多层存储管理
- Lance + pgvector + Parquet 各自的技术优势

**策略表**（示例）：

| 热度 | 定义 | Sink | 写回策略 |
|---|---|---|---|
| Hot（最近 1 小时） | 最后写入的 10000 行 | pgvector + HNSW index | 立即写入，保留索引 |
| Warm（1 小时 ~ 1 天） | 中间层 | Lance（列式） | worker-direct 批量写入 |
| Cold（> 1 天） | 归档层 | Parquet in object store | 定期批量导出 |

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 4/5 | Rafiki (PVLDB A) + Lance + ColStorEval (PVLDB A) |
| Eng | 2/5 | 需要 sink router + 数据迁移逻辑 |
| HW | 5/5 | — |
| OSS | 5/5 | — |
| Nov | 4/5 | 多层 sink 选择是已知思想，但在 DB AI 算子 writeback 场景的端到端验证是新组合 |
| Exp | 3/5 | 需要足够大的数据量和查询负载才能体现 tiered 价值 |

**定位**：RC3 的**可选增强**。当前数据规模（< 20000 rows）下 tiered storage 的收益不明显。可放在 §8 讨论。

---

### A3.8：Joint B_gpu ↔ B_write Co-optimization

**方案描述**：写回侧方案的核心闭环——`B_gpu`（GPU batch size）和 `B_write`（写回 batch size）联合优化。详见 §6。

---

### RC3 方案汇总矩阵

| 编号 | 方案 | 难度 | Lit | Eng | HW | OSS | Nov | Exp | 综合 | 推荐阶段 |
|---|---|---|---|---|---|---|---|---|---|---|
| A3.1 | Driver Fan-In Baseline | ★ | 2 | 5 | 5 | 5 | 1 | 5 | **3.8** | Phase 0（已完成） |
| A3.2 | Worker-Direct Blind Append | ★★★ | 5 | 3 | 5 | 5 | 4 | 4 | **4.3** | Phase 2（核心） |
| A3.3 | Queue-Worker 解耦 | ★★★ | 4 | 2 | 5 | 5 | 3 | 4 | **3.8** | Phase 2（对照） |
| A3.4 | Staging + Deferred Index | ★★ | 3 | 5 | 5 | 5 | 2 | 5 | **4.2** | Phase 1（必要 baseline） |
| A3.5 | MOR-Style Deferred Compaction | ★★★★ | 5 | 2 | 5 | 5 | 4 | 3 | **4.0** | Phase 3/§8 |
| A3.6 | KV-Separated Storage | ★★★ | 5 | 3 | 5 | 5 | 3 | 4 | **4.2** | Phase 2（sink 对照） |
| A3.7 | Tiered Sink Selection | ★★★ | 4 | 2 | 5 | 5 | 4 | 3 | **3.8** | §8 讨论 |
| A3.8 | Joint B_gpu ↔ B_write | ★★★★ | 5 | 2 | 5 | 5 | 5 | 3 | **4.2** | Phase 3（跨层核心） |

---

## 6. 跨层联合优化（Cross-Layer Joint Optimization）

**这是论文的核心 narrative**：数据组织（RC1）、GPU 调度（RC2）、持久化（RC3）各自独立最优不等于端到端最优。

### 6.1 方案总览

| 编号 | 方案名称 | 难度 | 来源 |
|---|---|---|---|
| **AX.1** | Joint B_gpu ↔ B_write Co-optimization | ★★★ | FlexPushdownDB + 本项目 |
| **AX.2** | Learned End-to-End Cost Model | ★★★★ | COSTREAM + CONCERTO + GRACEFUL |
| **AX.3** | Pipeline-Aware Execution Overlap | ★★★ | Ray Data Streaming Batch Model |

---

### AX.1：Joint B_gpu ↔ B_write Co-optimization

**方案描述**（跨层核心）：GPU batch size (`B_gpu`) 和写回 batch size (`B_write`) 联合搜索，证明 Independent Best < Joint Optimal。

**为什么这两个参数是耦合的**：
- `B_gpu ↑` → GPU 效率 ↑、operator time ↓；但请求粒度过大 → worker 等待 GPU 完成的时间变长 → 如果有 worker-direct writeback，worker 空闲时间变长
- `B_write ↑` → 写回吞吐 ↑、writeback time ↓；但攒批等待变长 → operator 完成到写回开始的间隙变大
- 如果 `B_gpu = B_write`：GPU 结果可以直接流式写入，pipeline 最流畅；但可能牺牲 GPU 效率或写回效率

**实验设计**：

```
Grid Search:
  B_gpu ∈ {8, 16, 32, 64, 128, 256}
  B_write ∈ {8, 16, 32, 64, 128, 256, 512}

对于每个 (B_gpu, B_write) 组合：
  e2e_s = operator_wall(B_gpu) + pipeline_gap(B_gpu, B_write) + writeback(B_write)

Independent Best:
  B_gpu* = argmin(operator_wall(B_gpu)) = 64
  B_write* = argmin(writeback(B_write)) = 256
  e2e_independent = operator_wall(64) + pipeline_gap(64, 256) + writeback(256)

Joint Optimal:
  (B_gpu**, B_write**) = argmin(e2e_s(B_gpu, B_write))
  e2e_joint = e2e_s(B_gpu**, B_write**)

Claim: e2e_joint < e2e_independent
```

**关键消融**：
- 固定 `B_write` 只改变 `B_gpu`
- 固定 `B_gpu` 只改变 `B_write`
- Joint search 全空间

**从何借鉴**：
- FlexPushdownDB (Yang et al., PVLDB 2021)：代价驱动的 compute/storage pushdown 决策，两个维度的联合优化
- 本课题 baseline 矩阵 §五

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | FlexPushdownDB (PVLDB A) |
| Eng | 3/5 | Grid search 自动化脚本，中等 |
| HW | 5/5 | 36 个组合 × 3 repeats = ~108 次 E2E 运行 |
| OSS | 5/5 | — |
| Nov | 4/5 | 增强 claim：阶段间耦合明显时，端到端联合配置可能优于 independent best |
| Exp | 4/5 | Grid search + BL1-BL6 对照矩阵 |

**定位**：**增强实验**。这是分析阶段间耦合是否存在的强对照，不作为当前开题主叙事的前置假设。

---

### AX.2：Learned End-to-End Cost Model

**方案描述**：将 AX.1 的 grid search 升级为 learned cost model——从 workload features 直接预测最优 `(B_gpu, N_actor, K_inflight, B_write, routing_strategy, writeback_mode)` 组合。

**从何借鉴**：
- COSTREAM (Heinrich et al., 2024)：GNN-based operator placement，21× 中位加速，零样本泛化
- CONCERTO (Zhang et al., 2025)：GAT + TCN 建模并行操作符资源竞争
- GRACEFUL (Wehrstein et al., 2025)：UDF 感知的 GNN 代价估计，50× 加速
- SIGMOD 2025 的 LCM 评估教训：需要 hybrid（learning + 传统估计），tail error 是关键

**Features（输入）**：

| Feature | 来源 | 类型 |
|---|---|---|
| `row_count` | workload spec | numeric |
| `avg_text_len` | workload spec | numeric |
| `output_dim` | workload spec | numeric |
| `selectivity` | workload spec（AI_FILTER） | numeric [0,1] |
| `token_length_distribution` | workload spec（AI_COMPLETE） | vector |
| `prefix_share_ratio` | workload spec（AI_COMPLETE） | numeric [0,1] |
| `endpoint_count` | system config | numeric |
| `sink_type` | system config | categorical |
| `gpu_model` | system config | categorical |

**Targets（输出）**：

| Target | 范围 |
|---|---|
| `B_gpu` | {8, 16, 32, 64, 128, 256} |
| `N_actor` | {1, 2, 4, 8} |
| `K_inflight` | {2, 4, 8, 16, 32} |
| `B_write` | {32, 64, 128, 256, 512} |
| `writeback_mode` | {driver_fanin, worker_direct, queue_worker} |

**训练数据**：从 AX.1 的 grid search 扩展——在不同 workload features 组合下做 grid search，构建训练数据集。

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 5/5 | COSTREAM + CONCERTO + GRACEFUL + LCM eval (SIGMOD 2025) |
| Eng | 1/5 | 需要大量 grid search 数据 + GNN/ML 模型训练；工程量大 |
| HW | 5/5 | 训练在 CPU |
| OSS | 5/5 | PyTorch / XGBoost |
| Nov | 5/5 | 首次将 learned cost model 应用于 DB AI 算子的全链路配置优化 |
| Exp | 2/5 | 需要大量实验数据（估计 500+ 次 E2E 运行），时间成本高 |

**定位**：**论文 §6 "方法扩展"或 §8 "未来工作"**。AX.2 的 marginal contribution 取决于 AX.1 中 Independent Best 与 Joint Optimal 之间的 gap 有多大——如果手工规则表（A1.2 + A2.3）已经接近 Joint Optimal，learned model 的增量价值有限。

**建议**：先做 AX.1（grid search），如果发现最优配置区域难以用简单规则概括（如 `B_gpu` 和 `B_write` 的交互效应复杂），再做 AX.2。

---

### AX.3：Pipeline-Aware Execution Overlap

**方案描述**：将 Ray Data Streaming Batch Model 的 pipeline 执行思想应用于本课题——GPU 计算和数据库写回不再是串行等待，而是流水线重叠：上一批 GPU 结果在写回时，下一批已经在 GPU 上执行。

**从何借鉴**：
- Ray Data Streaming Batch Model (Luan et al., arXiv 2025)：CPU/GPU 异构批处理 + pipeline 执行，3-8× 吞吐提升
- 本课题 baseline 矩阵 G4/BL4

**实现**：

```
时间线（pipeline 模式）：

Batch 1: [GPU exec][writeback                                    ]
Batch 2:          [GPU exec][writeback                           ]
Batch 3:                    [GPU exec][writeback                 ]
Batch 4:                              [GPU exec][writeback       ]

vs 串行模式：

Batch 1: [GPU exec][writeback]
Batch 2:                       [GPU exec][writeback]
Batch 3:                                              [GPU exec][writeback]
```

**关键**：需要 worker-direct writeback（A3.2）或 queue-worker writeback（A3.3）支持——driver fan-in 模式天然串行，无法 pipeline。

| 维度 | 评分 | 说明 |
|---|---|---|
| Lit | 4/5 | Ray Data Streaming Batch (arXiv 2025) |
| Eng | 3/5 | Pipeline 逻辑需要异步编程 |
| HW | 5/5 | — |
| OSS | 5/5 | asyncio |
| Nov | 3/5 | Pipeline 是已知优化模式 |
| Exp | 4/5 | Pipeline vs Serial 对比清晰 |

**定位**：RC2/RC3 的**配合方案**。作为"为什么 worker-direct writeback 比 driver fan-in 更有优势"的补充论证。BL4（Naive Pipeline）与 BL5/BL6 对照。

---

### 跨层方案汇总矩阵

| 编号 | 方案 | 难度 | Lit | Eng | HW | OSS | Nov | Exp | 综合 | 推荐阶段 |
|---|---|---|---|---|---|---|---|---|---|---|
| AX.1 | Joint B_gpu↔B_write | ★★★ | 5 | 3 | 5 | 5 | 5 | 4 | **4.5** | Phase 3（必做） |
| AX.2 | Learned E2E Cost Model | ★★★★ | 5 | 1 | 5 | 5 | 5 | 2 | **3.8** | Phase 3/§8 |
| AX.3 | Pipeline Overlap | ★★★ | 4 | 3 | 5 | 5 | 3 | 4 | **4.0** | Phase 3 |

---

## 7. 方案组合推荐与分阶段路线图

### 7.1 推荐方案组合（硕士论文可行性核心集）

基于可行性、创新空间和实验可验证性的综合权衡，推荐以下**必做方案**（= 论文最核心的方法 + baseline）：

| 研究内容 | 必做方案 | 角色 |
|---|---|---|
| **RC1** | A1.1（固定策略 Baseline）+ A1.2（Workload-Aware Partition）+ A1.3（Selectivity Cascade）或 A1.4（Prefix Grouping）| Baseline + 核心方法 |
| **RC2** | A2.1（Bounded In-Flight）+ A2.2（Actor Pool + Routing）+ A2.3（自适应 In-Flight）| Baseline + 核心方法 |
| **RC3** | A3.1（Driver Fan-In Baseline）+ A3.4（Staging + Deferred Index）+ A3.2（Worker-Direct Writeback）| Baseline + 核心方法 |
| **End-to-End** | AX.1（Joint B_gpu↔B_write）+ AX.3（Pipeline Overlap）| 可选增强：阶段间耦合分析 |

**可选增强**（如果时间和实验条件允许）：
- A2.6（优先级调度）→ 与 A2.2 组合，增强 RC2
- A3.6（KV-Separated Storage）→ 作为 sink 对照实验的一部分
- A1.5 / AX.2（Learned 方案）→ 如果规则法有局限

### 7.2 分阶段路线图

```
Phase 0（已完成，2026-07-12~14）
├── A1.1: 固定策略 Baseline（coalesced vs fine）
├── A3.1: Driver Fan-In 后批量写回
├── A2.1 部分: unbounded in-flight 观察（fine 实验中的 bounded_wait_s）
└── GPU-backed E2E 链路验证（AI_EMBED + 双 endpoint）

Phase 1（当前 ~ 2 周，2026-07-15~28）
├── RC3 工程 baseline
│   └── A3.4: COPY + unlogged staging table + deferred HNSW index
├── RC2 最小可行
│   └── A2.1: Bounded In-Flight 消融（K_max sweep × endpoint_count）
├── RC2 baseline 升级
│   └── 接入 vLLM 或 Ray Serve 作为 G1 baseline（替代手动 HTTP endpoint）
└── 交付: B 系列实验报告（确认写回 engineering best practice）

Phase 2（~ 4 周，2026-08）
├── RC1 方法
│   ├── A1.2: Workload-Aware Partition 规则表
│   └── A1.3 或 A1.4: 选一个 workload-specific 方法（根据 AI_FILTER 或 AI_COMPLETE 的工程进度）
├── RC2 核心方法
│   ├── A2.2: Workload-Aware Actor Pool + Routing
│   └── A2.3: 队列理论驱动的自适应 In-Flight
├── RC3 核心方法
│   └── A3.2: Worker-Direct Blind Append + Background Merge
├── 三类 workload 扩展
│   ├── AI_EMBED: 补 384 维 pgvector 写回
│   ├── AI_FILTER: selectivity sweep
│   └── AI_COMPLETE: token length distribution + prefix share
└── 交付: 各 RC 的单维度方法消融报告

Phase 3（~ 4 周，2026-09）
├── 跨层联合优化
│   ├── AX.1: Joint B_gpu↔B_write grid search → Independent Best vs Joint Optimal
│   └── AX.3: Pipeline Overlap（worker-direct + pipeline vs serial）
├── 完整 Killer Experiment（BL1-BL6）
├── 混合 workload 测试（30% EMBED + 30% FILTER + 40% COMPLETE）
└── 交付: C 系列实验报告 + 论文 §7 核心数据

Phase 4（~ 4 周，2026-10）
├── 论文正文撰写（§1-§8）
├── 可选增强消融（A2.6 或 A3.6）
├── 图表制作（figures/）
├── 开题答辩准备（PPT + 讲稿）
└── 交付: 完整论文初稿
```

---

## 8. 风险分析与反证条件

### 8.1 总体风险矩阵

| # | 风险 | 概率 | 影响 | 缓解策略 | 反证条件 |
|---|---|---|---|---|---|
| R1 | vLLM continuous batching 消除了外部调度的收益 | M | H | 明确边界：vLLM 优化 GPU 内部，本课题优化提交控制和跨层协同 | 如果 vLLM 下 `K_max` sweep 无效应 → RC2 范围需收紧 |
| R2 | 单 GPU 下分池/路由/优先级收益不明显 | M | M | 通过区分 `max_in_flight` 和 queue 顺序体现分池价值 | 如果 A2.2 的分池 vs 共享池消融无差异 → 合并为 simpler A2.3 |
| R3 | COPY + deferred index 已解决大部分写回问题 | H | L | 这是"好消息"不是"坏消息"——工程 baseline 的建立本身就是贡献；将 RC3 贡献移到 worker-direct writeback 和 pipeline overlap | 如果 COPY 把 writeback 从 6.5s 降到 0.5s → RC3 的边际贡献变小但仍有方法价值 |
| R4 | AI_FILTER / AI_COMPLETE 工程进度滞后 | M | M | 优先 AI_EMBED 完成三个 RC 的方法验证；AI_FILTER/AI_COMPLETE 作为 §7.3 适用性验证，不要求同等深度的实现 | 如果两类 workload 完全无法在 deadline 前实现 → 论文范围可合法收缩到 AI_EMBED + 论述性覆盖 |
| R5 | Joint Optimization 的增量收益 < 10% | L | H | 即便小，只要存在就足以证明"协同效应存在"；论文贡献不依赖"巨大收益" | 如果 Independent Best ≈ Joint Optimal → 跨层协同的 claim 不成立，需重新审视论文贡献 |
| R6 | PostgreSQL 18.3 平台不可用 | M | M | 当前所有实验在 PG18.4 本地完成；论文明确标注"本地同构预演"，§8 列出平台迁移计划 | 如果 deadline 前仍不可用 → 论文以 PG18.4 为平台，不算致命缺陷 |

### 8.2 关键反证条件

这些条件是**必须验证**的，如果验证失败，需要调整研究方向：

1. **RC2 核心反证**：如果 `K_max` sweep 在 vLLM continuous batching 下显示零效应（所有 `K_max` 的 `queue_wait_s` 相似）→ GPU 内部调度已足够好，RC2 的 external submission control 贡献需要重新定义
2. **RC3 核心反证**：如果 COPY + deferred index 后 writeback 占比降到 < 5% → RC3 的 worker-direct 和 pipeline 方法仍然有效但边际收益小，论文叙事需调整
3. **跨层核心反证**：如果 AX.1 的 Joint Optimal = Independent Best（即 `B_gpu` 和 `B_write` 的选择完全独立）→ 跨层协同的 claim 不成立，论文需重新聚焦到各 RC 的单独优化

---

## 9. Killer Experiment 矩阵

对应 `experiments/plans/baseline_reference.md` §五，核验**独立最优 vs 联合最优**：

| 编号 | 组名 | RC1 策略 | RC2 策略 | RC3 策略 | 来源 | 代表什么 |
|---|---|---|---|---|---|---|
| **BL1** | GPU-Only Optimal | 固定 coalesced batch | (B_gpu*, N_actor*, K_inflight*) = argmin operator_wall | 默认 driver writeback | vLLM/Orca/A2.3 单维度最优 | GPU 岛最优，不管写回 |
| **BL2** | Writeback-Only Optimal | 固定 coalesced batch | 固定 coalesced submit | (B_write*, mode*) = argmin writeback_s | COPY+deferred index / A3.2 | 写回岛最优，不管 GPU |
| **BL3** | Independent Best | BL1 的 RC1+RC2 | BL1 的 RC2 | BL2 的 RC3 | 组合 BL1 + BL2 | **关键对照** |
| **BL4** | Naive Pipeline | 固定策略 | 固定 K_inflight | worker-direct + pipeline overlap | Ray Data (G4) | 只做 overlap，不做 joint |
| **BL5** | Queue-Decoupled | 无优化 | 无优化 | Queue → worker 写回 | pgai (W4) | 解耦但无联合代价模型 |
| **BL6 (Ours)** | Joint Optimal | Workload-aware partition (A1.2) | Adaptive in-flight + actor pool (A2.2+A2.3) | Worker-direct + B_write co-tuned (A3.2) | **本课题** | **联合最优** |

**最低必跑**：BL1、BL2、BL4 和完整优化流程。BL3/BL6 可在阶段间耦合明显时加入，用于增强论证；不作为当前开题主线的必要条件。

---

## 附录 A：方案难度对照表

| 难度 | 含义 | 代表方案 |
|---|---|---|
| ★ | 已有代码，只需参数化 | A1.1, A3.1 |
| ★★ | 少量新代码（< 200 行） | A1.2, A2.1, A3.4 |
| ★★★ | 中等工程（200-800 行，需要理解新 API） | A1.3, A1.4, A2.2, A2.3, A2.6, A3.2, A3.3, A3.6, AX.1, AX.3 |
| ★★★★ | 大工程（800-2000 行，需要训练模型或系统集成） | A1.5, A2.4, A2.6, A2.7, A3.5, A3.7, AX.2 |
| ★★★★★ | 远超硕士论文范围 | A2.5 |

---

## 附录 B：与已有工作的差异化定位

| 已有工作 | 与本课题的关系 | 不同在哪里 |
|---|---|---|
| Cortex AISQL (SIGMOD 2026) | 场景依据 | Cortex 在闭源 DB 内做 AI-aware optimization；本课题在开源外部执行链路中做可拆分的阶段优化 |
| vLLM (SOSP 2023) | GPU baseline | vLLM 优化 GPU 内部的 memory + batching；本课题优化 GPU 外部的提交控制 + 跨层协同 |
| Orca (OSDI 2022) | 调度粒度参考 | Orca 的 iteration-level scheduling 在 GPU 引擎内部；本课题的 in-flight control 在引擎外部 |
| FlexPushdownDB (PVLDB 2021) | 跨层决策模型参考 | FlexPushdownDB 的 cost-driven pushdown 是 compute↔storage 维度；本课题的 joint optimization 是 GPU batch↔write batch 维度 |
| Ray Serve (2025) | 路由参考 | Ray Serve 的 custom routing 是通用框架；本课题的 workload-aware routing 是面向 DB AI 算子特征的专用化 |
| GaussML (ICDE 2024) + Smart (VLDB Journal 2025) | DB4AI 对照路线 | DB4AI 把模型拉进数据库；本课题把数据交出去执行 AI 再收回来 |
| pgai Vectorizer Worker | 写回形态参考 | pgai 是工程系统；本课题将其写回模式作为对照并加入跨层联合优化 |

---

## 10. Baseline 设计考量

本节为每个候选方案指定对应的 baseline 策略，确保实验对照不为 strawman。所有 baseline 来源标注到 `baseline_reference.md` 中的编号。

### 10.1 Baseline 设计原则

1. **文献优先**：优先从 CCF-A 论文或工业系统中提取最优已知策略作为 baseline，不凭空设计 strawman。
2. **公平对比**：baseline 和 proposed method 共享同一数据读取路径、同一模型 endpoint、同一硬件环境；只改变被消融的变量。
3. **多级对照**：至少包含 (a) 合理默认（有基本工程常识的第一版代码）、(b) 单维度最优（各维独立 grid search 最优值）、(c) 独立最优组合（各维最优的拼装）。
4. **来源可追溯**：论文 §7 对照表必须标注每个 baseline 的来源论文或系统。
5. **动机展示不用 strawman**：§4 动机展示用"合理默认配置"（coalesced batch=64、driver 写回），不用 row-by-row 或故意劣化配置——因为没人会那样写生产代码。行级调用和无界 in-flight 仅作为诊断工具用于理解瓶颈机制，不作为 baseline 对照。

### 10.2 各方案 Baseline 对照表

#### RC1：数据组织与批处理构造

| 方案 | Baseline 编号 | Baseline 描述 | 来源 | 为什么是公平对照 |
|---|---|---|---|---|
| A1.1 固定策略 | — | Self-baseline（coalesced vs fine 互相对照） | 本项目已有 | — |
| A1.2 Workload-Aware Partition | **D1** | Fixed Partition + Fixed Batch（不做 workload 感知） | Daft 官方文档 + Spark SQL Tuning | 同一条链路，只改变 partition/batch 选择策略 |
| A1.3 Selectivity Cascade + Partition | **D3 + D4** | 无 cascade，所有行统一过 GPU 模型（= D1 的 AI_FILTER 版） | Cortex AISQL (SIGMOD 2026) + Smart (VLDB Journal 2025) | AI-Filter workload 下 cascade vs no-cascade；需保证输出质量阈值一致 |
| A1.4 Prefix Grouping | **G1** | vLLM 默认 continuous batching + 随机分组（无 prefix-aware grouping） | vLLM (SOSP 2023) | 同一 vLLM engine，只改变 upstream 的 batch 组织方式 |
| A1.5 Learned Partition | A1.2 | 手工规则表（Heuristic） | 本项目 A1.2 | Learned vs Heuristic 在相同 feature set 上对比 |
| A1.6 Arrow Flight | — | psycopg2 fetch + 本地 Arrow build（当前方式） | 本项目当前 | 同一条数据读取路径，只改变传输层 |

**A1.3 的 baseline 特别注意事项**：
- Cortex AISQL 的 cascade 是在 Snowflake 闭源系统内部实现的。作为 baseline 时，应实现"最合理的非 cascade 对比"——即所有行都过完整 GPU 模型，用相同置信度阈值评估质量。
- 需要同时汇报端到端耗时和质量指标（precision/recall/F1），因为 cascade 可能降低质量。

**A1.4 的 baseline 特别注意事项**：
- vLLM 有 automatic prefix caching (APC)，所以即便不做 explicit prefix grouping，vLLM 内部可能已经在复用 KV cache。
- **关键假设验证**：先测试 vLLM APC 在随机分组下是否已经很好地利用了 prefix cache。如果 APC 已经不错，A1.4 的 explicit grouping 增量价值可能有限。
- Baseline 应报告：random order batch vs prefix-grouped batch，同时记录 vLLM 的 cache hit rate。

#### RC2：GPU 调度与反压

| 方案 | Baseline 编号 | Baseline 描述 | 来源 | 为什么是公平对照 |
|---|---|---|---|---|
| A2.1 Bounded In-Flight | — | Ray 默认行为（无显式 `K_max`，框架自动排队） | Ray 默认调度策略 | 同一链路，只增加显式 `K_max` 控制——证明 backpressure 的价值 |
| A2.2 Actor Pool + Routing | **G1 + G2** | vLLM continuous batching + 单一 actor pool + round-robin | vLLM (SOSP 2023) + Orca (OSDI 2022) | 同一 vLLM engine，只改变 Ray 侧 actor 组织和路由 |
| A2.3 自适应 In-Flight | **A2.1（静态）** | 固定 `K_max`（最优静态值） | 本项目 A2.1 | 自适应 vs 静态最优——证明自适应在 workload 变化时有价值 |
| A2.4 MAB 反馈控制 | **A2.3（PID）** | 队列理论驱动的 PID 自适应控制 | 本项目 A2.3 | Bandit vs PID——两种自适应策略的对比 |
| A2.6 优先级调度 | **A2.2（FIFO）** | Workload-Aware Actor Pool + FIFO（无优先级） | 本项目 A2.2 | 同一 actor pool 结构，只改变队列顺序 |
| A2.7 Cost-Model Joint | **G1 + W1** | vLLM 默认 + COPY deferred index（GPU/写回各自最优但独立） | 详见 AX.1 baseline | **跨层核心对照** |

**A2.2 的 baseline 特别注意事项**：
- G1/G2 (vLLM + Orca) 是 GPU 推理服务社区的标准 baseline。如果本课题暂未接入 vLLM，应在本实验前完成 vLLM 或 Ray Serve 的接入。
- 如果工程条件暂不具备，论文 §8 必须写"当前 GPU baseline 是简化的手动 HTTP endpoint；vLLM continuous batching 可能缩小 GPU 侧的独立优化空间"。
- **但在论文中必须承认**：vLLM 内部已经在做 iteration-level batching，A2.2 的 actor pool 是在 vLLM 外部做的**额外**控制——这两个层面的贡献需要区分清楚。

**A2.3 的关键实验设计**：
- 需要在 workload 动态变化的场景下验证自适应 vs 静态 K_max
- 静态 K_max 取 workload A 下的最优值，然后在 workload B 下测试——验证自适应能跨 workload 保持性能
- 评估指标：自适应在多大程度上避免了静态 K_max 在 unseen workload 上的退化

#### RC3：结果汇聚与持久化

| 方案 | Baseline 编号 | Baseline 描述 | 来源 | 为什么是公平对照 |
|---|---|---|---|---|
| A3.1 Driver Fan-In | — | Self-baseline | 本项目当前 | — |
| A3.2 Worker-Direct Writeback | **A3.1 + A3.4** | Driver fan-in + COPY + deferred index（当前最优 driver-side 实践） | 本项目 A3.1 + A3.4 | 关键对照：worker-direct 是否优于最优 driver 写回 |
| A3.3 Queue-Worker | **A3.2 + A3.1** | Worker-Direct + Driver Fan-In（两种当前最优） | 本项目 A3.1/A3.2 | 三路对比：driver / worker-direct / queue-worker |
| A3.4 Staging + Deferred Index | **W1** | 逐行 UPSERT + 在线索引维护（当前方式） | PostgreSQL §14.4 + pgvector Issues | 确认 COPY + deferred index = 工程最优 |
| A3.5 MOR Deferred Compaction | **W5** | Iceberg MOR with deletion vectors | Iceberg v3 (2025) | MOR vs 直接更新——写放大的量化对比 |
| A3.6 KV-Separated Storage | **W6** | WiscKey KV 分离 | WiscKey (FAST 2016) | 嵌入向量外存 vs 内联存储 |
| A3.7 Tiered Sink | **W7** | 单一 sink（pgvector） | Lance + ColStorEval (PVLDB 2023) | 多层 vs 单层；需要足够数据规模才能体现差异 |

**A3.2 的 baseline 特别注意事项**：
- 必须先跑 A3.4 确认 COPY + deferred index 的收益，然后以**最优 driver-side 写回配置**作为 A3.2 的 baseline。
- 不能拿"未优化的 driver UPSERT"当 baseline 来证明 worker-direct 好——那是 strawman。
- Worker-direct 和 driver fan-in 使用**相同的 write batch size 和 COPY 方式**，只改变"谁写、怎么写"。

**A3.4 的硬性要求**：
- 这是 B 系列实验的必做项。在进入 RC3 方法设计之前，必须跑完 B1-B3 确认：
  - B1: `execute_values()` UPSERT vs `COPY` 的量化差异
  - B2: unlogged staging table vs logged table
  - B3: 在线索引维护 vs 事后建索引
- 结果写入 `experiments/results/`，不在动机测试目录中放写回 engineering baseline 数据。

#### 跨层联合优化（AX.1-AX.3）

| 方案 | Baseline 编号 | Baseline 描述 | 来源 | 为什么是公平对照 |
|---|---|---|---|---|
| AX.1 Joint B_gpu↔B_write | **BL1 + BL2 + BL3 + BL4** | 端到端流程调优增强矩阵 | 见 §9 | 阶段间耦合分析的强对照结构 |
| AX.2 Learned E2E Cost Model | **AX.1 (Grid Search) + A1.2 (Heuristic)** | Grid search 的最优解 + Heuristic 规则表 | 本项目 AX.1 + A1.2 | Learned vs Exhaustive vs Heuristic 三路对比 |
| AX.3 Pipeline Overlap | **BL4 (Naive Pipeline) + BL3 (Independent Best, Serial)** | Ray Data naive pipeline + Serial execution | Ray Data (arXiv 2025) + 本项目 BL3 | Pipeline vs Serial；需固定其他变量不变 |

**AX.1 的 BL 对照注意事项**：

```
BL1 (GPU-Only Optimal)：
  RC1: A1.1 固定 coalesced batch
  RC2: A2.3 自适应 K_inflight（tuned for GPU throughput）
  RC3: A3.1 driver fan-in（默认）
  → 代表"只关心 GPU，不管写回"

BL2 (Writeback-Only Optimal)：
  RC1: A1.1 固定 coalesced batch
  RC2: A2.1 固定 K_inflight（不调优）
  RC3: A3.2 worker-direct + A3.4 COPY deferred index
  → 代表"只关心写回，不管 GPU"

BL3 (Independent Best)：
  RC1+RC2: 取 BL1 的配置
  RC3: 取 BL2 的配置
  → **最关键对照**：各维度独立最优的组合

BL6 (Joint Optimal = Ours)：
  全链路联合搜索得到的最优配置
  → 若 BL6 明显优于 BL3，可作为阶段间耦合的增强证据
```

### 10.3 Baseline 实现检查清单

每设计一个 baseline，逐项确认：

- [ ] 是否有文献或官方文档作为该 baseline 策略的来源？（标注编号）
- [ ] 是否与 proposed method 共享同一数据读取路径、模型 endpoint、硬件环境？
- [ ] 是否改变了**仅被消融的变量**，其他变量保持不变？
- [ ] 如果 baseline 来自工业系统（如 vLLM、COPY），是否已确认在当前实验环境下达到了该系统的预期最优配置？
- [ ] 是否避免了以"合理默认配置"作为**唯一** baseline？——论文 §7 必须与 A 级或 S 级 baseline 对比
- [ ] 是否避免了用 row-by-row 或故意劣化配置作为方法对比？——这些只能出现在 §4 诊断分析中
- [ ] 对于跨层对照（BL1-BL6），是否确保了每组的"最优"是通过同方法 grid search 找到的？
- [ ] 是否在实验报告中标注了 baseline 的来源论文/系统？

### 10.4 Baseline 分级

| 级别 | 含义 | 具体配置 | 论文中怎么用 |
|---|---|---|---|
| **S 级（文献最优）** | CCF-A 论文或工业系统报告的最优已知策略 | vLLM continuous batching、TurboVecDB io_uring 写回、DistServe prefill/decode 分离 | §7 主对照表（部分可能超出硬件范围，作为文献参照） |
| **A 级（工程最优）** | 当前硬件条件下各维度独立 grid search 出的最优配置，组合为"每个岛的最优实践" | vLLM B_gpu 最优值、COPY+deferred index 最优 B_write、grid search 最优 K_max | §7 主对照表——这是你要打败的对手 |
| **B 级（单维调优）** | 在某一维度上做了 grid search，但各维度的配置是独立选择的，没有跨层协同 | batch_size sweep 的最优值、endpoint_count sweep 的最优值、write_mode 单独最优 | 消融实验——证明"单维调优有帮助，但不能替代联合优化" |
| **—（合理默认，非 baseline 等级）** | 有基本工程常识的第一版代码：固定 batch=64、单 endpoint、driver fan-in、`execute_values()` UPSERT、无 pipeline overlap | 即当前 coalesced 模式 | **§4 动机展示**——证明"即使合理批处理，写回占 36-54%、跨层各自为政"。不进入 §7 方法对照 |

**为什么没有"C 级"**：row-by-row 调用、无界 in-flight 等故意劣化配置不是 baseline——它们是诊断工具。只用于§4 理解瓶颈机制（"如果把批处理完全拿掉会怎样""如果把 backpressure 完全拿掉会怎样"），不用于证明方法有效。

**规则**：
- §4 动机展示用"合理默认"——这是正常工程师会写的第一版代码
- §7 方法对照至少包含 A 级 baseline。S 级能跑则跑，不能跑则作为文献参照写入 §2 和 §7 讨论
- B 级用于消融——证明每个维度独立优化的边际收益
- row-by-row 和无界 in-flight 只能出现在 §4 瓶颈诊断中，且必须标注"这是诊断工具，不代表工程实践"

---

## 附录 C：Baseline 实现优先级

按 ASAP（As Soon As Possible）顺序：

| 优先级 | Baseline | 对应实验 | 原因 |
|---|---|---|---|
| **P0** | W1: COPY + deferred index | B 系列 | 必须先确认工程最优写回——否则 RC3 所有方法对比用的 baseline 都是 suboptimal |
| **P0** | G1: vLLM / Ray Serve | RC2 baseline | 必须先接入标准 GPU baseline——否则 RC2 所有方法对比用的 baseline 都是"简化的 HTTP endpoint" |
| **P1** | A1.1: 固定策略 sweep（扩展到更多参数） | RC1 baseline | 确认当前最优的静态配置 |
| **P1** | A2.1: K_max sweep（扩展 endpoint_count） | RC2 baseline | 确认静态最优 K_max |
| **P2** | BL1-BL4 | C 系列 Killer Experiment | 必须在自己的方法跑完之后才能构造完整的 BL 矩阵 |
