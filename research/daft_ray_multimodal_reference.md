# Daft+Ray 多模态执行引擎与具身智能连接分析

生成日期：2026-07-17
用途：记录 Daft+Ray 架构、Flotilla 分布式引擎、与具身智能的关联、及与本课题的关系分析。
关联：`research/knowledge_hub.md` §10

---

## 1. Daft 是什么

Daft 是一个 **Rust 写核心 + Python API + Apache Arrow 列式内存** 的分布式 DataFrame 引擎。定位是"面向多模态 AI workload 的统一数据与查询引擎"（区别于 Spark 的 JVM ↔ Python 序列化开销、Dask 的单机多线程模型）。

### 1.1 三层架构

```
Python DataFrame / SQL API
        ↓
LogicalPlan → Optimizer (Rule-based + Cost-based)
        ↓
PhysicalPlan → Runner (Local: Swordfish / Distributed: Flotilla)
```

### 1.2 关键技术特点

| 特性 | 说明 |
|------|------|
| **Rust 核心** | 所有运算 kernel（filter, aggregate, join, UDF）在 Rust 中执行，PyO3 桥接 Python |
| **Arrow 零拷贝** | Table/Series 底层是 Arrow，与 PyTorch/NumPy/Ray 交换无需序列化 |
| **惰性求值** | DataFrame 操作累积为 LogicalPlan，`.collect()` 时才触发优化和执行 |
| **多模态一等公民** | Image/Video/Audio/Tensor/Embedding 作为原生列类型，`df["img"].image.resize(32,32)` |
| **云原生** | S3/OSS/HDFS 原生支持，Apache Iceberg/Delta/Unity Catalog 集成 |

---

## 2. Swordfish 流式执行引擎（本地模式）

2025 年 9 月发布的本地执行引擎，核心思想：**Morsel 驱动的 Push 模型**。

### 2.1 Morsel 模型

```text
SourceNode → [morsel, morsel, morsel] → IntermediateNode → [morsel, ...] → SinkNode
              ↑ 通过 bounded async channel 推送                      ↑ 增量处理
```

与 Volcano 迭代器 Pull 模型的区别：下游不主动拉取，上游主动推送。好处是 I/O、CPU、GPU 可以完全重叠——第一批 morsel 在 GPU 推理时，第二批已在 CPU 解码。

### 2.2 Pipeline Node 类型

| Node | 特点 | 示例 |
|------|------|------|
| SourceNode | 数据入口 | PhysicalScan（Parquet/CSV/视频帧） |
| IntermediateNode | 流式处理 | Project, Filter, UDF |
| BlockingSinkNode | 需要全量输入 | Aggregate, Repartition, WriteSink |
| StreamingSinkNode | 透传 | Limit, Concat |

### 2.3 背压与内存管理

- Bounded async channel：下游消费慢 → 上游等待 → 天然背压
- 动态 batch size：内存紧张时自动缩小 morsel
- 单机 61GB 处理 1TB+ 数据不 OOM

---

## 3. Flotilla 分布式引擎（2025.10）

Daft 的新分布式执行引擎，替代旧的 Ray runner 架构。

### 3.1 架构哲学："每节点一个引擎"

```
Driver (Python)
  ├── PlanRunner (Rust) — LogicalPlan → PhysicalPlan DAG
  ├── Scheduler (Rust) — 优先级队列，基于数据本地性和 worker 负载
  └── Dispatcher (Rust) — 批量派发 morsel 到各节点

Worker Node (per-node):
  └── RaySwordfishActor (Python → Rust Swordfish)
       ├── CPU 线程池
       ├── GPU 管理
       ├── 网络 I/O（Tokio async）
       └── 磁盘 spill
```

### 3.2 Ray 的角色变化

在旧架构中，Ray 负责调度 + 执行 + 对象存储。在 Flotilla 中，Ray **被降级为资源管理层**：
- Daft 自己的 Scheduler 决定"哪个 task 在哪个节点执行"
- Daft 自己的 Dispatcher 决定"以什么顺序、什么 batch size 发送"
- Ray 只提供：actor 生命周期、节点发现、GPU 分配

这与本课题的用法形成鲜明对比——本课题用 Ray actor 做**去中心化自适应协调**，Daft 用 Ray 做**底层资源供给**。

### 3.3 Hybrid Shuffle

- **Ray Object Store Shuffle**：数据能放进内存时使用，最快
- **Flight Shuffle（Beta）**：基于 Apache Arrow Flight RPC，可直接 spill 到 NVMe，支持压缩

---

## 4. GPU 推理集成：@daft.cls

### 4.1 Stateful UDF 模式

```python
@daft.cls(gpus=1, max_concurrency=4, use_process=True)
class EmbeddingModel:
    def __init__(self):
        self.model = load_model()  # 每个 worker 加载一次

    @daft.method.batch(return_dtype=DataType.float32(), batch_size=32)
    def embed(self, texts):
        return self.model.encode(texts)
```

### 4.2 关键参数

| 参数 | 含义 | 调优建议 |
|------|------|---------|
| `gpus=N` | 每个实例预留 N 个 GPU | =1 用于单模型推理 |
| `max_concurrency=M` | 全局最大并发实例数 | =GPU 总数 |
| `use_process=True` | 每实例独立 OS 进程，绕 GIL | 有 CPU 预处理时必开 |
| `batch_size=B` | 每批次处理 B 行 | (concurrency/workers) × B = rows_per_task |

### 4.3 重要的"非能力"

**@daft.cls 不观测模型服务的内部状态。** 它不知道 vLLM 的 queue depth、KV cache 使用率、running request 数量。它的"背压"是纯内部的（channel 满不满），不是模型服务感知的。

这恰好是本课题的差异化切入点：**queue-adaptive flush 是根据 vLLM 的 Prometheus metrics 来做反馈驱动的提交决策**——Daft 不做这个。

---

## 5. Daft vs Ray Data 竞争全景

### 5.1 架构对比

| 维度 | Daft (Flotilla) | Ray Data (Streaming Batch) |
|------|-----------------|---------------------------|
| 论文 | 无（SciPy 2024 Talk） | arXiv:2501.12407 (UC Berkeley/Anyscale) |
| 执行模型 | Morsel Push（不物化 partition） | Block Streaming（fuse sequential ops） |
| 调度 | 集中式 Driver/Worker | 集中式 Adaptive Scheduler |
| 数据类型 | 多模态原生（Image/Video/Audio/Tensor） | 通用（需手动处理多模态） |
| API | DataFrame + SQL | Dataset API（更底层） |
| 优化器 | Rule + Cost-based | 有限 |
| 生态 | LanceDB, Iceberg, Delta | Ray Train, Ray Serve, vLLM |

### 5.2 2025 Benchmark 之争

双方在同等硬件（8× g6.xlarge, NVIDIA L4）上得出相反结论：

**Daft 的结果**（Flotilla 发布时）：
- 图片分类：4m23s vs Ray Data 23m30s（5.4×）
- 文档向量化：1m54s vs Ray Data 14m32s（7.6×）
- 视频目标检测：11m46s vs Ray Data 25m54s（2.2×）

**Anyscale 的反驳**（优化脚本后重测）：
- 小实例（g6.xlarge, 4CPU/GPU）：Daft 更快（低开销优势）
- 大实例（g6.8xlarge, 32CPU/GPU）：Ray Data 反超（更多 CPU 喂 GPU）
- 超大规模（40 GPU + 64 CPU）：Ray Data 快 7×

**对本课题的启示**：不要在论文中站队 Daft vs Ray Data。两者都可以作为本课题的数据组织层（Arrow RecordBatch 构造），且本课题关注的是它们**都不做的**调度策略决策。

---

## 6. 具身智能场景分析

### 6.1 为什么具身智能需要 Daft+Ray

具身智能的数据特征：
- 数据量大：单个机器狗每天数百 GB 视频
- 模态多样：视频帧 + 深度图 + 力反馈 + 音频 + IMU
- 噪音多：大量静止帧、模糊帧、无效空帧需要过滤
- 管线长：采集 → 解码 → 质量筛选 → VLM 标注 → 格式转换 → 写入训练集

Daft+Ray 解决的问题：
- 多模态数据作为 DataFrame 原生类型，不需要为每种格式写独立脚本
- CPU 解码 + GPU VLM 推理重叠，GPU 利用率 90%+
- Morsel 流式 + 背压，防止大规模视频处理 OOM
- `ai_query` 嵌入 VLM 调用，数据不需要搬出 pipeline

### 6.2 典型管线（阿里云 EMR Serverless Daft）

```text
OSS 原始视频（数千小时第一人称操作视频）
  → Daft read_video_frames(间隔2秒采样)
  → encode_image(JPEG 压缩)
  → FrameUploader UDF(上传到 OSS)
  → ai_query(Qwen-VL, "判断帧质量: KEEP/DROP", concurrency=32)
     ↑ VLM 直接通过签名 URL 读取图片，无需拉回计算节点
  → OSSDeleter UDF(批量删除 DROP 帧)
  → 输出：清洗后的高质量训练图片集
```

### 6.3 实际落地案例

| 案例 | 规模 | 效果 |
|------|------|------|
| 火山引擎 + 大小机器人 | 机器狗巡检视频 | CPU 100%, GPU 90%+ |
| 京东云 + GR00T-N1.5 | 千卡 GPU | 训练 15h→22min (40×) |
| 字节跳动 | 90K GPU, 236 亿次 LLM 查询 | 7 天零崩溃 |
| 阿里云 EMR Daft | 100+ 内置多模态算子 | 通义千问/Qwen-VL 深度集成 |

---

## 7. 与本课题的关系

### 7.1 核心差异：引擎层优化 vs 策略层优化

| 维度 | Daft+Ray（引擎层） | 本课题（策略层） |
|------|-------------------|-----------------|
| 优化目标 | 数据流执行效率 | 调度决策质量 |
| 关键问题 | CPU/GPU 是否重叠？内存是否 OOM？ | 按什么规则组 batch？什么节奏提交？ |
| vLLM 交互 | 单向调用（UDF） | 双向反馈（观测 → 自适应） |
| 粒度 | Morsel/Partition | Token/Request |
| 调度架构 | 集中式 Driver/Worker | 去中心化 Ray Actor |
| 写回 | 数据湖（非瓶颈） | 数据库写回（瓶颈判定） |
| 数据来源 | 对象存储（OSS/S3） | PostgreSQL 数据库表 |

### 7.2 互补关系

Daft 负责高效执行，本课题负责智能决策——两者在同一系统中可以协同：

```text
PostgreSQL → Daft/Arrow (数据组织层，利用 Daft 的多模态原生支持和 Arrow 零拷贝)
  → Ray Actor 动态 Batching (策略层：token-budget / frame-budget)
  → Queue-Adaptive Flush (策略层：观测 vLLM metrics → 自适应提交)
  → vLLM Continuous Batching (部署平台)
  → 写回 PostgreSQL/pgvector (瓶颈判定)
```

### 7.3 本课题调度策略对多模态的泛化

| 当前（文本 AI_COMPLETE） | 泛化到多模态具身智能 |
|---|---|
| `max_tokens_per_submission = 4096` | `max_frames_per_submission = 64` 或 `max_duration_ms = 30000` |
| 按 token 长度排序分组（减少 straggler） | 按视频时长排序分组（避免长视频拖慢 batch） |
| Prefix-aware：共享 system prompt 合并 | 共享 context 帧合并（同一场景连续帧共享 visual context） |
| Queue-adaptive flush：观测 vLLM queue depth | **完全复用**——VLM 同样暴露 Prometheus metrics |
| Actor pool 分池路由：按 token 长度 | 更丰富：短/长视频 actor、高/低分辨率 actor、不同 VLM 模型 actor |

### 7.4 在论文中的定位建议

- **主体实验（§3-§5）**：AI_COMPLETE（文本生成式 LLM），vLLM + Qwen2.5-1.5B
- **Discussion（§6）**：将具身智能多模态数据处理作为 generalization case
  - 论证 token-budget → frame-budget 的对应关系
  - 论证 queue-adaptive flush 的模态无关性
  - 引用 Snowflake Cortex 多模态 AI 算子作为工业需求证据
  - 引用 Daft+Ray 在具身智能中的应用作为管线可行性证据
  - 标注为 future work

### 7.5 不能声称的结论

1. 不能说"本课题解决了具身智能的数据处理问题"——没有做具身智能实验
2. 不能说"Daft+Ray 是本课题的 baseline"——Daft 优化的是引擎层，不是调度策略层
3. 不能说"本课题的方法在具身智能场景中有效"——只有在真实具身智能 workload 上验证后才能说
4. 合理表述："本文的调度策略框架的抽象层次（按计算量组织 batch、按模型服务状态调节提交）不依赖于数据模态，其泛化潜力以具身智能多模态数据处理为例进行了讨论"

---

## 8. 关键参考资料索引

| 资料 | 类型 | 关键内容 |
|------|------|---------|
| [Building Daft: Python + Rust = a better distributed query engine](https://cfp.scipy.org/2024/talk/A7CC7W/) | SciPy 2024 Talk | Daft 三层架构 |
| [Exploring Daft's Swordfish Execution](https://www.daft.ai/blog/exploring-daft-swordfish-execution-mechanism) | 官方博客 | Morsel Push 模型、Tokio 异步 |
| [Flotilla: Daft 新分布式引擎](https://www.daft.ai/blog/introducing-flotilla-simplifying-multimodal-data-processing-at-scale) | 官方博客 | Flotilla 架构、Ray 角色降级 |
| [GPU Inference with @daft.cls](https://www.daft.ai/blog/gpu-inference-with-daftcls) | 官方博客 | Stateful UDF、GPU 参数 |
| [Daft on Ray (阿里云)](https://help.aliyun.com/en/polardb/polardb-for-postgresql/what-is-daft-on-ray) | 阿里云文档 | Heterogeneous scheduling、三种模式 |
| [The Streaming Batch Model (arXiv:2501.12407)](https://arxiv.org/abs/2501.12407) | 论文 | Ray Data 异构执行模型 |
| [Ray Data vs Daft Benchmark](https://www.anyscale.com/blog/ray-data-daft-benchmarking-multimodal-ai-workloads) | Anyscale 博客 | Benchmark 竞争分析 |
| [EMR Serverless Daft 具身智能实践](https://developer.aliyun.com/article/1747724) | 阿里云技术文章 | 视频→VLM→标注完整管线 |
| [Snowflake Cortex AI Multimodal](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-multimodal) | 官方文档 | 多模态 AI SQL 算子 |
| [HeteroHub: 多具身 Agent 数据管理](https://ar5iv.labs.arxiv.org/html/2603.28010) | arXiv 2025 | 具身智能数据管理分层架构 |
| [IBM: The data gap holding back robotics](https://www.ibm.com/think/news/the-data-gap-holding-back-robotics) | IBM 技术博客 | 具身智能数据缺口 |
