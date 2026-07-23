---
type: paper-note
tags:
  - deep-reading
  - paper/ray-data-streaming-batch
  - data-engine
  - heterogeneous-execution
  - daft-ray-pipeline
aliases:
  - "Ray Data Streaming Batch (arXiv 2025)"
status: 精读完成
read_date: 2026-07-23
---

# 精读笔记：The Streaming Batch Model for Efficient and Fault-Tolerant Heterogeneous Execution

---

## ▎第一层 · 基本信息

| 字段 | 内容 |
|------|------|
| **论文** | Luan, Wang, Gu, Mao, Lin, Kamsetty, Chen, Su, Veeramani, Lee, Cho, Zinzow, Liang, Stoica, Wang. *The Streaming Batch Model for Efficient and Fault-Tolerant Heterogeneous Execution.* arXiv:2501.12407v5, 2025. |
| **来源级别** | arXiv 预印本（UC Berkeley + UW + Anyscale 联合，Ray Data 系统论文）；尚未被顶会接收 |
| **链接** | https://arxiv.org/abs/2501.12407 / 本地 PDF：`ray_data_streaming_batch_2025.pdf` |
| **阅读日期** | 2026-07-23 |
| **状态** | 精读完成 |
| **相关论文组** | 分布式数据引擎（本项目基础设施层） |

### 一句话核心结论

Ray Data 提出了 **streaming batch** 执行模型——以 partition 为弹性调度单元（batch 系统优势），但允许 partition 在异构算子间动态创建和流式传输（streaming 系统优势），在异构 batch 推理和多模态训练上比 Spark/Flink 快 2.5–12×，且通过 lineage-based recovery 实现零停机故障恢复。

`#streaming-batch` `#heterogeneous-execution` `#Ray` `#data-engine` `#Daft-competitor`

---

## ▎第二层 · 论文结构分析

### 1. 问题拆解

| 问题 | 论文的回答 |
|------|-----------|
| 要解决什么痛点？ | ML 推理和训练中，CPU 数据预处理往往是瓶颈（而非 GPU 计算）。现有 batch 系统（Spark）做 stage 间全量物化导致内存压力大、无法 pipeline；streaming 系统（Flink）支持 pipeline 但资源静态绑定导致负载不均、扩缩容困难 |
| 之前的方法为什么不够？ | Batch 系统（MapReduce/Spark/Hadoop）：stage 间 barrier 同步 → 无法跨异构算子 pipeline → GPU 空闲等 CPU；静态分区 → 大 record（长视频）导致分区倾斜 → OOM。Streaming 系统（Flink/Naiad/MillWheel）：executor 绑定特定 key range → 数据倾斜无法动态纠正 → 扩缩容需全局 checkpoint/rollback |
| 论文的**核心论点** | 应该用 partition 作为弹性调度的最小单元（batch 的优势），但允许 partition 在运行时动态创建和流式传输（streaming 的优势）——两者不互斥 |
| 它的**关键假设** | UDF 是纯函数（pure）、无副作用——这是 lineage-based recovery 的前提；ML 负载以 map-style per-row transform 为主 |

### 2. 方法拆解

```
┌─────────────────────────────────────────────────────────┐
│                  Ray Data 架构（三层）                     │
│                                                         │
│  Dataset API (Python)                                   │
│  read → map → map_batches → write/iter                  │
│  │                                                      │
│  ▼ Query Planner                                        │
│  │ Logical DAG → Physical DAG                           │
│  │ Operator fusion + 初始分区数估计（1-128MB target）     │
│  │                                                      │
│  ▼ Ray Data Scheduler（集中式）                           │
│    ┌──────────────────────────────────────────┐         │
│    │ 全局状态：partition 元数据（行数/字节/位置）  │         │
│    │ + operator 输入队列（Ray object refs）     │         │
│    │ + 可用资源池（CPU/GPU/custom slots）       │         │
│    │                                          │         │
│    │ 调度循环（Algorithm 1）：                  │         │
│    │ 1. 等 task 完成 → 推 output partition     │         │
│    │ 2. 选 operator → 分配资源 → 启动 task     │         │
│    │ 3. 内存预算控制（Algorithm 2）             │         │
│    └──────────────────────────────────────────┘         │
│         │                                               │
│         ▼ Ray Workers（去中心化数据面）                    │
│           Executor 本地动态分区（128MB flush）            │
│           Generator tasks → 流式产出 partition          │
└─────────────────────────────────────────────────────────┘
```

**核心技术要点**：

1. **Streaming Batch 执行模型**（§3, Fig 2c/3c）：partition 是动态大小的 record batch，由 executor 在运行时按 target partition size（默认 128MB）和实际内存消耗动态切分。每个 task 消费一个或多个 input partition，产出零个或多个 output partition 的流。上游产出 B11 后，下游可立即消费 B11，同时上游继续产出 B12——实现跨异构算子的 memory-efficient pipelining。

2. **动态重分区**（§4.2.1, Fig 4b）：通过扩展 Ray 的 generator task 机制实现——task 不再一次性返回所有 output，而是 yield 一个 stream of partitions。每次 yield 后通过 RPC 通知 scheduler，scheduler 立即可以调度下游 task。Executor 本地按 target partition size flush buffer。对于产出远小于消费的算子（如 filter），scheduler 自动 coalesce 多个上游 partition 到一个下游 task。

3. **乐观调度策略**（§4.3, Algorithm 1+2）：两级调度——(a) 悲观层（类似 streaming backpressure）：优先调度 buffered output 最少的 operator，全局内存达上限时 stall 在途 task；(b) 乐观层（profile-guided）：用动态内存预算算法估计 pipeline 整体处理速率（bytes/s），按速率启动 source task 以保持 pipeline 满载。预算更新公式：`budget += outputPartitionSize(source) / Σ(Tᵢ/Eᵢ × Πρⱼ)`，其中 Tᵢ 是 task duration，Eᵢ 是可用执行槽数，ρⱼ 是 input:output size ratio。算法自带负反馈——高估导致 backpressure → 下游并行度下降 → 预算补充速率降低。

4. **故障恢复**（§4.2.2）：extend Ray lineage recovery 支持 generator task（未知 output 数）——首次成功执行后记录 output 数，重执行时验证一致性。与 streaming 系统的全局 checkpoint 不同：executor 故障无停机，node 故障自动缩容继续运行，node 重新加入后自动扩容（无全局 rollback）。

5. **Dataset API 与资源抽象**（§3.1, Table 2）：`map` / `map_batches` / `flat_map` / `filter` 等惰性变换 + `write` / `iter` / `iter_split` / `cache` 等消费 API。每个 transform 可声明资源需求（`num_gpus=1`）。Stateful UDF（如 GPU 模型）通过 Ray actor pool 实现——actor 持有只读模型权重但不持有系统状态，任何 stateful task 可调度到任何 actor。

### 3. 实验拆解

| 维度 | 内容 |
|------|------|
| **数据集** | RAG: TriviaQA + Llama-3-8B + vLLM；Video: Kinetics-700-2020（64,535 videos, 137.3GB, S3）+ VideoMAE；ResNet: MLPerf ImageNet；Stable Diffusion: 2B images |
| **Baseline** | Batch: Spark 3.5.1, Ray Data-staged（模拟 batch）；Stream: Flink 1.19.0, Ray Data-static（模拟 stream）；ML data loaders: tf.data, PyTorch DataLoader；Raw Ray tasks |
| **评价指标** | 吞吐（videos/s, images/s, rows/s）、作业完成时间（JCT）、训练吞吐（samples/s）、成本（GPU-hours） |
| **消融实验** | ✅ Ray Data vs Ray Data-staged（隔离 pipeline 收益）vs Ray Data-static（隔离动态调度收益）；partition size 扫描（Fig 10a: 1-210MB → 128MB 最优）；Ray Generator 开销对比（验证 generator task 无额外开销） |
| **统计显著性** | ❌ 未报告方差/误差棒；单次运行 |
| **复现条件** | 🟡 部分：Ray Data 开源（Anyscale Ray 2.40.0+），但部分 benchmark 依赖 AWS 特定实例类型（g5.2xlarge/p4de.4xlarge）和 S3 数据 |

### 4. 关键数字

| Claim | 数字 | 条件 |
|-------|------|------|
| RAG pipeline 加速 vs 单进程 staged | 1.32×（1GPU）→ 6.44×（8GPU）| Llama-3-8B + vLLM, 100K prompts, H200×8 |
| Video classification vs Flink | 2.5× | 4×A10G, Kinetics-700 |
| Video classification 达 GPU 理论最大 | 88.4% | Ray Data-dynamic |
| SD training vs PyTorch DL | 31% better throughput | 32×A100 + A10G Encoder pool, 异构 GPU 分离 |
| 集群扩展（8→32 nodes）| 1.8× vs raw Ray tasks | 5GB/node linear scaling |
| 故障恢复 | 零吞吐下降（executor failure）, 平滑缩容（node failure）| 无全局 checkpoint |
| 最优 partition size | 128MB（1-210MB 扫描）| 通用推荐值 |
| 代码规模 | ~51K Python LoC（planner 1K, scheduler 1K, executor 2K）| Ray 库级别实现 |

---

## ▎第三层 · 批判性评估

### 1. 假设检验

- **假设 1**：UDF 是纯函数（无副作用），输入输出 immutable
  - 反例 / 边界：**本项目的 GPU UDF 满足此假设**（vLLM 推理是纯函数——prompt in, tokens out）。但需注意 vLLM 内部的 KV cache 是 mutable state——Ray Data 的 lineage recovery 重执行时 KV cache 状态丢失。论文通过 stateful actor pool 处理模型权重（只读），但未讨论 KV cache 这类"运行时可变但不可序列化"的状态。
- **假设 2**：ML 负载以 map-style per-row transform 为主
  - 反例 / 边界：论文承认 group-by/sort 等 shuffle 操作"通过文献[24]技术处理"——但未给出性能数据。本项目的 batching 策略恰好在"per-row transform"和"batch transform"之间引入了一个新的组织层——每行仍是独立请求，但多行被打包成一个物理 batch。
- **假设 3**：128MB partition 是最优的通用默认值
  - 反例 / 边界：**本项目场景下该值可能不适用**。128MB 针对视频/图像大 record 调优，LLM prompt 通常远小于 1MB——partition 按 byte size 触发 flush 会导致一个 partition 包含过多行（可能数千行），超出 vLLM 单次 continuous batching 的有效范围。RC1 的 token-budget 策略恰好在纠正这一问题：用 token 量而非 byte 量作为 partition 边界。

### 2. 边界探查

- **方法适用边界**：Streaming batch 模型适用于异构 map-style pipeline（CPU→GPU→CPU）。当 pipeline 中 GPU 算子需要 continuous batching（如 vLLM）时，论文的 `map_batches` + 多 in-flight task 到同一 GPU 模型副本的设计（§4.3 "more than one outstanding task per cluster resource"）与 vLLM 的 request queue 模型有概念对应，但论文未提供 LLM serving 场景的 partition→request mapping 策略。
- **扩展性限制**：Scheduler 是集中式的（单 driver 进程）——论文声称通过 Ray 解耦控制面和数据面来避免瓶颈，但未给出 scheduler 的延迟/吞吐 profiling。在百万级 partition 场景下集中式调度器的可扩展性存疑。
- **与本项目的关系**：Ray Data 是 Daft 的直接竞品（或底层——取决于 Daft 是否使用 Ray Data vs 自己实现类似机制）。论文描述的执行模型（dynamic partitioning + optimistic scheduling）与 Daft 的 Swordfish Worker + Ray 资源层架构高度对应。理解 Ray Data 的 partition/concurrency/memory 控制机制 = 理解本项目引擎层的行为边界。

### 3. 可信度评估

| 维度 | 评价 | 依据 |
|------|------|------|
| 实验公平性 | 🟢 较公平 | 自建 Ray Data-staged/static 做 apples-to-apples 消融，对比 Spark/Flink/tf.data |
| 结果显著性 | 🟡 一般 | 效果明显（2.5-12×），但无方差报告、单次运行 |
| 开源/可复现 | 🟢 开源 | Ray Data 是 Ray 2.40.0+ 的一部分，代码公开 |
| 论文自身局限 | 🟢 诚实 | §7 Discussion 坦诚 static query planning + scheduler 是单点 + 未来需 autotuning |

### 4. 与同行工作的对比

- 比 **Daft**（SciPy 2024 + Daft Blog 2025）：Ray Data 和 Daft 在架构上高度相似（Python API → query planner → Ray 执行）。核心差异：Ray Data 用 dynamic partitioning + centralized scheduler + optimistic budget；Daft 用 Rust core + Arrow 零拷贝 + Swordfish Worker（每节点一个，Ray 降级为资源层）。Daft 更偏"数据库引擎思维"（Rust 性能 + Arrow 生态），Ray Data 更偏"Ray-native 调度思维"。
- 比 **Spark**（batch）/ **Flink**（stream）：论文的对照实验证明了 streaming batch 模型在异构 ML pipeline 上优于纯 batch 和纯 stream——这为选择 Daft/Ray Data 作为项目数据引擎提供了实验依据。
- 在 **[你的课题]** 的坐标系中：Ray Data 是**引擎层**（RC1/RC2 策略运行的平台），不是竞争对手。论文未涉及：(1) 如何按 token 量而非 byte 量组织 partition；(2) 如何感知模型服务队列状态做 admission control；(3) prefix/length-aware 分组。这三个"未涉及"恰好是本项目的三项策略贡献。

---

## ▎第四层 · 与你课题的连接

### 1. 可引用的观点（配精确位置）

> §3.2 map_batches API + "more than one outstanding task per cluster resource" for LLM continuous batching
> → **引擎接口证据**：Ray Data 已原生支持 "多个 in-flight task 到同一 GPU 模型副本"——这正是 RC2 的 actor pool + queue-adaptive flush 的引擎层基础。可在实验设置中说明"本项目在 Ray Data map_batches + stateful actor pool 基础上实现策略层"。

> §4.3 Optimistic Policy + memory budget algorithm: "the input rate must approximate the pipeline's overall throughput"
> → **调度方法论**：Ray Data 的 budget-based rate control 是 RC2 的 engine-level 参照——RC2 的 queue-adaptive flush 在"数据引擎→模型服务"边界做类似的事：按下游处理速率控制上游提交速率。但 RC2 的信号源是 vLLM 的 KV cache/utilization 而非 pipeline 内部处理速率。

> §5.1.1 RAG pipeline scaling: 8 GPU 时 CPU 成为瓶颈，6.44× speedup
> → **动机证据**：引擎级 CPU/GPU pipeline 不平衡是真实瓶颈——上游数据组织（RC1）恰好可以减轻 CPU 侧的 partition 构造开销（减少 partition 数、增大每 partition 的 token 量）。

> §5.1.3 Fault tolerance: "Ray Data has no noticeable throughput drop under executor failure"
> → **工程参考**：项目中 Ray actor 故障不影响整体 pipeline——Ray Data 的 generator task recovery 机制是 Ray 层面的保障。

> §7 Discussion + Conclusion: "future data processing systems must support more flexible APIs for sharding and reducing data copies"
> → **方向佐证**：论文自己承认 partition/sharding 策略的灵活性是未来工作——这正是本项目的空间。

### 2. ⚠️ 不能过度引用的地方

- ❌ **不声称** "Ray Data 提供了 token-aware batching"——论文的 partition 策略基于 byte size（128MB），完全不涉及 token 量或 sequence length
- ❌ **不声称** "Ray Data 的 optimistic scheduling 可以直接替代 RC2 的 queue-adaptive flush"——Ray Data 的调度作用在 pipeline 内部（CPU↔GPU 算子间），RC2 的调度作用在"数据引擎→外部模型服务"边界
- ❌ **不声称** "Ray Data 是本项目的唯一引擎选择"——Daft 使用类似的架构但不同的实现（Rust core, Swordfish Worker），项目需在两者间做实验对比
- ❌ **不声称** "Ray Data 实验的 RAG 加速比直接适用于本项目"——论文的 RAG workload（Llama-3-8B + Contriever + FAISS）与本项目的 AI_COMPLETE workload（PostgreSQL → Daft → vLLM）有差异

### 3. 对本课题的实际用途

| 用途类型 | 具体方式 | 优先级 |
|----------|----------|--------|
| ✅ 引擎层理解 | 理解 partition 动态切分、concurrency 控制、backpressure、资源分配——这些是 RC1/RC2 策略运行的引擎行为边界 | ⭐⭐⭐ |
| ✅ 实验 Baseline | Ray Data-staged（模拟 batch）和 Ray Data-static（模拟 stream）的对比方法可直接复用到本项目的消融实验设计 | ⭐⭐⭐ |
| ✅ 策略空间定义 | `map_batches` batch size、target partition size（128MB → token budget 替代）、`max_concurrency`、actor pool 配置——这些是 RC1/RC2 的引擎级参数空间 | ⭐⭐⭐ |
| ⚠️ 设计参考 | Optimistic budget algorithm 的"按下游处理速率控制上游提交速率"是 RC2 queue-adaptive flush 的同构设计——但信号源不同（pipeline 内部 vs 模型服务外部） | ⭐⭐ |
| ⚠️ 工程参考 | Generator task + lineage recovery 机制——理解 Ray actor 的故障恢复边界 | ⭐⭐ |

### 4. 不足 → 你的机会

| 论文的不足 | 你的课题可能如何填补 |
|-----------|---------------------|
| Partition 策略仅基于 byte size（128MB），不感知数据内容的计算量（token 量/frame 量）| RC1 的 token-budget batching 用 token 量替代 byte 量作为 partition 边界——本质上是"计算量感知的动态分区" |
| 调度信号全部来自 pipeline 内部（处理速率、output queue 长度），不感知外部模型服务状态 | RC2 的 queue-adaptive flush 引入外部信号（vLLM KV cache 利用率、inflight 请求数）做提交控制 |
| `map_batches` 是简单 batch 收集，不做行间 grouping/length-align/prefix-aware 组织 | RC1 在这层之上引入长度对齐、prefix 分组的策略逻辑 |
| 集中式 scheduler（单 driver）——论文自己承认是限制 | RC2 的去中心化 actor pool 路由（每个 actor 自主决策提交）是对集中式瓶颈的回应 |
| 实验仅覆盖 RAG/video/训练场景，无 LLM serving 的 inflight/queue/tail latency 分析 | 本项目以 vLLM continuous batching 为主场景，直接测量和优化这些指标 |

### 5. 可论文化的措辞

> Luan et al. [Ray Data, 2025] 提出的 streaming batch 模型以 partition 为弹性调度单元、支持异构算子间的内存高效 pipelining，是目前 Daft 和 Ray Data 等数据引擎的架构基础。然而，Ray Data 的 partition 策略基于固定 byte size（默认 128 MB），不感知数据内容的实际计算量——对于 LLM 推理场景，两段等长文本的 token 量可能差 13.9 倍。本课题在 streaming batch 引擎之上引入 token-budget-aware 数据组织策略，用计算量（token 量）而非数据量（byte 量）作为 partition 边界。

> 与 Luan et al. 将调度信号限制在 pipeline 内部（处理速率、队列长度）不同，本课题的调度与提交控制策略将信号源延伸到外部模型服务——vLLM 的 KV cache 利用率、inflight 请求数等指标反馈到上游的 queue-adaptive flush 和 K_max 动态控制。这种"跨边界的自适应"是 Ray Data 当前设计未覆盖的空白。

### 6. 后续待读

- [ ] [[clipper_nsdi2017]] — AIMD 自适应 batching，RC2 的方法祖先
- [ ] [[concur_2025]] — 双信号 AIMD admission control，RC2 参数来源
- [ ] [[vllm_sosp2023]] — Continuous batching + PagedAttention，下游平台
- [ ] **Daft Documentation** — Daft Swordfish Worker 架构（Ray Data 的竞品/对照引擎）
- [ ] **Ray Core generator task docs** — 理解 Ray Data 底层 task 机制的细节

---

## 元反思

- **精读收益**：🟢 高（本文是项目引擎层的核心参考文献——Ray Data/Daft 的执行模型直接定义了 RC1/RC2 策略的作用面和边界）
- **是否纳入核心文献库**：是
- **计划复习周期**：实现 RC1 token-budget partition 策略时重读 §4.2.1 动态重分区；实现 RC2 queue-adaptive flush 时重读 §4.3 乐观调度
- **一句话自评**：理解到位。Ray Data 为本项目提供了引擎层的"标准答案"——理解了它的 partition/concurrency/memory/scheduling 机制，才能精确说出本项目的策略层在哪里做了不同的选择（token-budget 替代 byte-budget、外部信号替代内部信号、去中心化替代集中式）。

---

## 相关笔记

- [[vllm_sosp2023]] — 下游推理平台
- [[ray_osdi2018]] — Ray actor 模型基础
- [[clipper_nsdi2017]] — AIMD 自适应 batching
- [[concur_2025]] — AIMD + 双信号 admission control
- [[文献地图]] — 文献全景
- [[top15_ranked_papers]] — Top 15 排序分析
