---
type: paper-note
tags:
  - deep-reading
  - paper/mooncake
  - llm-inference
  - disaggregation
  - KVCache
  - prefix-cache
  - overload-scheduling
  - production-system
  - acmtos2025
aliases:
  - "Mooncake (ACM TOS 2025)"
  - "Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving"
status: 精读完成
read_date: 2026-07-23
---

# 精读笔记：Mooncake — A KVCache-centric Disaggregated Architecture for LLM Serving (ACM TOS 2025)

---

## ▎第一层 · 基本信息

| 字段 | 内容 |
|------|------|
| **论文** | Qin, Li, He, Zhang, Wu, Zheng, Xu. *Mooncake: A KVCache-centric Disaggregated Architecture for LLM Serving.* ACM Transactions on Storage, 2025（同时 arXiv:2407.00079v4, 3 Sep 2025）。 |
| **来源级别** | ACM TOS 2025 评审发表（已评审）；arXiv v4 预印本（与之同内容）。Moonshot AI（Kimi 团队）+ 清华 MadSys。Kimi 真实生产 serving 平台。 |
| **链接** | arXiv: https://arxiv.org/abs/2407.00079 / Trace + 代码: https://github.com/kvcache-ai/Mooncake / 本地 PDF: `opening/literature/reference/mooncake_acmtos2025.pdf` |
| **阅读日期** | 2026-07-23 |
| **状态** | 精读完成 |
| **相关论文组** | LLM serving disaggregation / KVCache-centric 调度 / 超大规模生产系统 / 过载调度 / Prefix cache 全局化 |

### 一句话核心结论

Mooncake 是 Kimi 的生产 LLM serving 平台，核心思想是把 **KVCache 作为一等公民**：分离 prefill/decoding 集群，并把 GPU 节点中闲置的 CPU/DRAM/SSD 聚合成一个 disaggregated global KVCache pool；围绕这个池做 cache-aware 调度 + 启发式热点迁移 + 预测式 early rejection——在长上下文场景下相对 vLLM baseline 吞吐提升最高 525%，真实负载下多承 75% 请求且满足 SLO。

`#LLM-serving` `#KVCache-centric` `#disaggregated-architecture` `#prefix-cache` `#overload-scheduling` `#early-rejection` `#production-system` `#ACM-TOS-2025`

---

## ▎第二层 · 论文结构分析

### 1. 问题拆解

| 问题 | 论文的回答 |
|------|-----------|
| 要解决什么痛点？ | 三件事：(a) 长上下文请求 TTFT 过高、单机装不下；(b) prefill 与 decoding 计算特征截然不同，耦合在一起互相干扰；(c) **超载**——MaaS 商在 GPU 供给受限下面临请求增长 >> 算力增长的常态过载，必须**拒绝部分请求**且不能浪费 prefill 算力（§1.1, §7） |
| 之前的方法为什么不够？ | vLLM 的 continuous batching + PagedAttention 最大化吞吐但耦合两阶段、且只有**本地** prefix cache（§6.1 明确指出 "the open-source version of vLLM only supports local KVCache caching"）；Splitwise/DistServe/TetriInfer 已提出分离，但没有把 KVCache 提升为**全局调度对象**；所有已有工作默认"所有请求都会被处理"，忽略过载（§1.1 末段，§7 首段） |
| 论文的**核心论点** | (1) LLM serving 的调度本质上是 **KVCache 的调度**——吞吐两条路径（KV 复用、大 batch）本质都在优化 KV；(2) 因此应把 prefill/decoding 集群分离、并把闲置 CPU/DRAM/SSD 聚合成全局 KVCache 池；(3) 围绕这个池做 cache-aware 调度 + 热点迁移 + SLO-driven early rejection；(4) 在过载场景，**基于预测的 early rejection**（而非事后丢弃）是关键 |
| 它的**关键假设** | (a) prefill compute-bound、decoding memory-bandwidth-bound，二者可被清晰拆分到不同资源池；(b) GPU 节点的 CPU/DRAM/SSD 与 RDMA 网络有足够空闲承载 KVCache 存取与跨节点传输；(c) Transformer 计算模式规律，prefill 时长可用离线数据离线拟合出误差很小的预测器（§6.1）；(d) 请求 trace 的 prefix 复用模式可被哈希分块识别；(e) Kimi 的负载特征（input 平均 7590 tokens、input/output ≈ 720）具有代表性 |

### 2. 方法拆解

```mermaid
flowchart LR
    REQ[请求到达 Conductor] --> KVH[PrefixHash 分块<br/>算 block_keys]
    KVH --> COND{Conductor<br/>全局调度}

    subgraph GP[Prefill Pool]
        direction TB
        PINST1[Prefill Instance 1<br/>GPU/VRAM + 本地 KVCache]
        PINST2[Prefill Instance 2<br/>GPU/VRAM + 本地 KVCache]
    end

    subgraph GD[Decoding Pool]
        direction TB
        DINST1[Decoding Instance 1<br/>Paged KVCache in VRAM]
        DINST2[Decoding Instance 2]
    end

    subgraph GKV[Distributed KVCache Pool<br/>CPU/DRAM/SSD + RDMA]
        KV1[(Node CPU/DRAM)]
        KV2[(Node CPU/SSD)]
        KV3[(Node CPU/DRAM)]
    end

    COND -->|cache-aware<br/>选最优 prefill| PINST1
    COND -->|load-balance<br/>选 decoding| DINST1
    KV1 -.->|Messenger<br/>(GPUDirect)RDMA| PINST1
    PINST1 -.->|layer-wise stream<br/>async transfer| KV2
    KV2 -.->|load to decoding| DINST1
    COND -.->|hot block<br/>复制迁移| KV3

    style COND fill:#2F6FEB,color:#fff
    style GKV fill:#fef3c7,color:#92400e
    style GP fill:#ffe4c4,color:#9a3412
    style GD fill:#ede9fe,color:#5b21b6
```

**核心技术要点**：

1. **KVCache-centric disaggregated 架构（§3, Fig.1）**：在 prefill/decode 集群分离之上，**额外**把每个 GPU 节点闲置的 CPU/DRAM/SSD 聚合成一个分布式 KVCache 池。KVCache 以 paged block 为单位存储，block key = Hash(自身 tokens + 前缀 hash)，从而天然支持前缀去重（§3, Fig.3）。跨 CPU↔GPU、跨节点传输由独立进程 Messenger 用 (GPUDirect) RDMA 完成。
2. **Chunked Pipeline Parallelism (CPP) for long-context prefill（§5.1）**：长上下文单请求把 tokens 切成 ≤ `prefill_chunk` 的块，**不同块在同一 pipeline group 的不同节点上并行**处理。相比 Sequence Parallelism，CPP 只在 pipeline stage 边界通信，可与计算 overlap，不与 KVCache 传输抢网络。论文称这是 CPP 在**推理阶段**的首次应用（训练阶段有 TeraPipe 先例 [24]）。
3. **Layer-wise Prefill（§5.2）**：KVCache 的 load/store 按层异步执行（launch/wait），与 attention 计算重叠。好处：prefill 实例的 VRAM 占用成本（S × T）因 T 被压缩而下降，调度时**只需考虑 DRAM 大小**而不用再担心 VRAM。
4. **Cache-aware global scheduling（§6.1, Algorithm 1）**：每个 prefill 实例都维护本地 prefix cache 索引；Conductor 对每个请求枚举所有 prefill 实例，估计 `TTFT = Tqueue + Ttransfer + Tprefill`（若 transfer 远端 KV 的收益超过其传输代价才走远端复用），选 TTFT 最小者。TTFT 超过 SLO 直接返回 HTTP 429。
5. **Heuristic hot-spot migration（§6.2）**：不精确预测未来 block 使用，而是被动迁移——当请求被调度到非最优前缀所在实例时，若"远端取 + 本地复用"比"本地从头算"更快，就顺便把热 block 复制到本地；超过阈值时主动复制到多节点，避免 fetch 拥塞。
6. **Early rejection based on prediction（§7）**：朴素 early rejection（只看当前 decoding 负载决定是否接 prefill）会造成 prefill/decoding 两池负载**反相波动**（§7.3, Fig.10a：stage1 全接 → stage2 decode 过载全拒 → stage3 decode 空闲又全接 → stage4 过载……）。论文改用**系统级预测**：假设每个请求 decoding 时长为常数 `td`，估算未来 t 时刻 decoding 池的 TBT 比例，基于此判断是否接受。Request-level（预测每请求 output length）因成本/精度问题暂未上线（§7.4）。

### 3. 实验拆解

| 维度 | 内容 |
|------|------|
| **数据集** | 公开数据 ArXiv Summarization、L-Eval；模拟数据（16k/32k/64k/128k prompt + 512 output + 50% prefix）；**真实负载 trace**：Kimi 1 小时窗口的 23,608 条请求采样（input 平均 7590 tokens、output 平均 182 tokens、input/output ≈ 720），开源在 GitHub（§4）。**注意：为保护商业信息，全部实验用 LLaMA-70B 同架构 dummy model 重放**，trace 不含真实文本，只含 timing、token 数、remapped block hash |
| **Baseline** | **vLLM**（连续 batching + PagedAttention），配置 vLLM-[4M] / vLLM-[20M] 作为同等 GPU 规模对照。是当时 SOTA 开源系统，但不是工业最强（TensorRT-LLM / DeepSpeed Inference 仅在 related work 讨论） |
| **评价指标** | P90 TTFT、P90 TBT、在 SLO 约束下的最大 RPS（goodput 思路——只有完整完成的请求才计入吞吐）、TTFT/TBT CDF。**SLO 阈值**：实验中 `TTFT_P90 = 10× baseline`、`TBT_P90 = 5× baseline`；真实部署用固定值（TTFT ≤ 30s、TBT ≤ 0.1s/token） |
| **消融实验** | (a) 调度策略三档：random / load-balancing / cache-aware / KVCache-centric（含热点迁移）—— Fig.8；(b) 早期拒绝三档：baseline / Early Rejection / Early Rejection based on Prediction —— Table 3；(c) 缓存策略 LRU/LFU/LengthAwareCache × 容量 —— Table 1；(d) 配比 [3P+1D] vs [2P+2D] —— Fig.11 |
| **统计显著性** | 论文未报告置信区间或方差，多数为单次重放的均值/P90。对生产系统这是常见短板，重放本身已含到达时间随机性 |
| **复现条件** | 硬件：每节点 8× A800-80GB + NVLINK + 800Gbps RDMA。代码与 trace 开源（Mooncake 主仓库 + kvcache-ai 主仓库），但完整生产部署涉及 Kimi 内部 Conductor、Messenger、调度器工程实现——这些组件的开源程度有限。Dummy model 可复现端到端结果 |

### 4. 关键数字

| Claim | 数字 | 条件（什么设置下） |
|-------|------|-------------------|
| 长上下文模拟场景吞吐提升 | **最高 +525%**（范围 50%–525%） | 16k–128k prompt 模拟数据，Mooncake-[3P+1D] vs vLLM-[4M]，满足 TTFT/TBT 双 SLO（§8.1.2, Fig.12） |
| 真实负载多处理的请求量 | **+75%** | 23k 真实 trace 重放，Mooncake-[10P+10D] vs vLLM-[20M]；vLLM 只有 57% 请求满足 TBT SLO，Mooncake ≈100%（§8.1.3, Fig.13） |
| 公开数据集吞吐提升 | **+20%（ArXiv Summarization）/ +40%（L-Eval）** | Mooncake-[3P+1D] vs vLLM-[4M]，满足 SLO（§8.1.1） |
| 过载场景拒绝数下降 | **4183 → 3771（朴素 ER）→ 3589（预测式 ER）** | 8P+8D Mooncake 集群，23k trace 2× 加速重放（§8.2, Table 3） |
| Trace 统计 | 平均 input **7590** tokens / output **182** tokens / input-output 比 ~720；prefix cache 上限命中率 ~50% | Kimi 1 小时窗口 23,608 条请求（§4.2） |
| Cache 命中率 vs 容量 | 容量 1000→50000 blocks，LRU 命中率 30%→50%；进一步扩容收益递减 | 单全局池假设（§4.2, Table 1） |
| Cache block 冷热不均 | **>50% block 从未被命中**，少数 block 命中数万次 | §4.2, Fig.6 |

---

## ▎第三层 · 批判性评估

### 1. 假设检验

- **假设 1：KVCache 是调度的中心对象**（§1.1 "the scheduling of KVCache is central to LLM serving scheduling"）。
  - 反例 / 边界：在**短上下文 + 低 prefix 复用**场景（如 ArXiv Summarization cache ratio ~0%），KVCache-centric 的优势主要回归到"分离避免干扰"，全局池本身贡献很小——论文实验中 +20% 主要来自 disaggregation，并非 cache 池本身。
- **假设 2：GPU 节点闲置 CPU/DRAM/SSD 足够承接 KVCache 存取与传输**（§3）。
  - 反例 / 边界：Kimi 的超大规模集群有大量异构作业（训练、其他推理）填充集群，"idle"未必成立；论文未报告 CPU/DRAM 实际占用率，也未讨论 KVCache 存取对 host CPU 的抢占效应。小集群或非 MaaS 提供商可能根本无此闲置资源。
- **假设 3：prefill 时长可被精确预测**（§6.1 "the error bound of this prediction is small as long as enough offline data is available"）。
  - 反例 / 边界：论文依赖 Transformer 计算模式规律，但 SP/CPP 并行度变化、KVCache transfer 拥塞、网络抖动都会扰动——论文承认 transfer 时间预测更难（§6.1 末段）。
- **假设 4：system-level prediction 用统一 `td` 估算 decoding 时长足够**（§7.4）。
  - 反例 / 边界：Kimi 自身 trace output 长度分布跨越 4 个数量级（Fig.5 右），统一 `td` 只能消除反相波动、无法逼近真正最优——论文承认 request-level 预测是 future work。
- **假设 5：生产 trace 用 dummy model 重放的结果可代表真实部署**（§4 开头声明）。
  - 反例 / 边界：dummy model 拟合 LLaMA-70B 的 FLOPs 与访存特征，但不包含真实推理引擎（vLLM/TensorRT）的代码路径优化，因此 525% 等数字应理解为**架构层面的相对优势**，不是 Kimi 生产环境实测。

### 2. 边界探查

- **方法适用边界**：长上下文（≥16k）+ 高 prefix 复用率 + 超载 是 Mooncake 优势最显著的三个条件；任一不满足，收益显著收窄（ArXiv Summarization 只剩 20%）。
- **扩展性限制**：全局 KVCache 池和 Conductor 的 cache-aware 调度对**集群规模**敏感——热点复制在更大集群（>1000 节点）会引发元数据膨胀，论文未讨论 Conductor 的扩展性上限。Trace 规模线性增长时调度算法本身 O(P×B)（P=prefill 实例数、B=block 数），在真实 Kimi 规模下的开销论文未量化。
- **复现难度**：trace + 代码已开源（🟢），但完整生产系统的 Conductor 调度策略、Messenger 传输实现、热点迁移启发式（含手调阈值，见 §6.2 footnote "currently adjusted manually"）的开源程度有限（🟡）。Dummy model + A800 集群才能复现端到端数字。

### 3. 可信度评估

| 维度 | 评价 | 依据 |
|------|------|------|
| 实验公平性 | 🟡 有疑点 | Baseline 是 vLLM 开源版，未对比 TensorRT-LLM / DistServe / Sarathi-Serve 等同期更强方案（§9 提到但未实验对比）。Dummy model 重放消除模型差异但也消除了真实推理引擎差异 |
| 结果显著性 | 🟢 显著 | 长上下文 +525% 与真实负载 +75% 量级足够大；过载策略拒绝数从 4183→3589 也是实质改进。部分图（Fig.12 128k）的 baseline 因被迫改为逐请求处理，对比有偏向 Mooncake 的风险 |
| 开源/可复现 | 🟡 部分 | Trace 开源（业内首个含真实 prefix 哈希的数据集，§4），主仓库开源，但生产工程实现闭源 |
| 论文自身局限 | 🟢 较诚实 | 明确说明 trace 仅代表 Kimi 特定模式（§4.2 "this is only a representative pattern and not unanimous"）、热点迁移阈值手调（§6.2 footnote）、request-level 预测未完成（§7.4）、SP-based elastic 方案未采用是工程权衡（§5.1） |

### 4. 与同行工作的对比

- 比 **DistServe (OSDI 2024)** 多了：(a) 全局 KVCache 池而非 instance 间 pull；(b) 热点复制 + 迁移机制；(c) 过载与 early rejection（DistServe 假设资源充足）。比 DistServe 少了：DistServe 的**离线 placement 搜索算法**（co-optimize GPU 分配 + parallelism）——Mooncake 的 prefill/decoding 比例是"preset"的（§8.1.1 末段承认 "the proportion ... can be preset"），动态转换是 future work。
- 比 **Splitwise (arXiv 2023)** 多了：完整生产验证 + 过载调度 + chunked pipeline parallelism；Splitwise 是概念性 disaggregation 提出。
- 比 **Sarathi-Serve / chunked prefill（§5）** 多了：彻底分离而非 inline；论文用 5.1 整节论证"为什么 chunked prefill 不够、仍需分离"——长上下文跨节点并行 + VRAM 占用成本。
- 比 **AttentionStore (arXiv 2024, 并行工作)**：均提出分层 KVCache；Mooncake 额外做了 cache-aware **调度**，AttentionStore 只做 cache 层本身（§9）。
- 比 **Preble (eScholarship 2024, [36])**：Preble 也做 prompt global scheduling；Mooncake 团队坦诚 "We corroborate many results"，但指出真实在线 trace 的复用率（≤50%）远低于开源 benchmark 虚构的复用率（§9 末段）——这是对行业的诚实纠正。
- 在**本课题的坐标系**中：Mooncake 属于 **"外部执行链路上的模型服务侧调度参考"**——它优化的是 vLLM 部署侧，不是数据库 AI 算子链路。但它的 (a) 分池路由、(b) 全局 prefix 池、(c) 预测式拒绝三件事，是上游调度设计的重要借鉴对象。

---

## ▎第四层 · 与你课题的连接

### 1. 可引用的观点（配精确位置）

1. **§1.1 第二段**："the scheduling of KVCache is central to LLM serving scheduling" + 后续给出两条吞吐优化路径（KV 复用 / 大 batch MFU）"both these throughput-oriented optimizations may lead to violations of latency-related SLOs"。
   - **对本课题的价值**：这是数据库 AI 负载调度设计的第一层理论锚点——证明"吞吐优化与 SLO 约束天然冲突"，可作为课题研究内容二（提交控制策略）的动机引文。
2. **§5（首段）关于 disaggregated prefill pool 的辩论 + 论证**：作者详细对比 chunked prefill inline vs 分离，最终结论维持分离——理由是长上下文跨节点并行需求 + VRAM 占用成本。
   - **对本课题的价值**：直接支撑研究内容二"**actor pool 分池路由**"的设计合理性。生产级系统在辩论后仍选择分池，是比纯学术论证更强的证据。
3. **§6.1（末段）+ §6.2**：明确指出 vLLM 开源版**只有本地 prefix cache**，Mooncake 做的是**全局** prefix cache 调度；并描述 cache-aware global scheduling（Algorithm 1）+ 启发式热点迁移。
   - **对本课题的价值**：研究内容一"**prefix-aware**"在有多个 vLLM 副本时的设计参考——课题代码不能依赖 vLLM 单实例的本地 prefix cache，需要在调度层维护跨副本的全局 prefix 索引。
4. **§7.3 + §7.4**：朴素 early rejection 导致 prefill/decoding 反相波动（Fig.10a 的 4-stage 周期），预测式策略（系统级 `td` 假设）解决波动（Fig.10b 全 accept）。
   - **对本课题的价值**：直接对应研究内容二的 **K_max 自适应 / admission control / queue-adaptive flush**。反相波动是 admission 设计的反面教材——简单阈值会引入振荡，需要预测式或平滑机制（与本课题 `code/AGENTS.md` §1 提到的 Ray ConcurrencyCapBackpressurePolicy EWMA + deadband 教训一致）。
5. **§4.2 + §9 末段**：真实 trace 的 prefix 复用率上限 ~50%（远低于开源 benchmark 虚构的复用率），block 冷热极度不均（>50% block 从未被命中，少数命中数万次）。
   - **对本课题的价值**：诚实的数据点，可用于开题报告中论证 prefix-aware 策略的真实收益边界，避免过度声称。

### 2. ⚠️ 不能过度引用的地方

- ❌ **不声称**："应采用 Mooncake 式的全球 KVCache 池"。课题规模（数据库 AI 算子链路、单租户/少副本）与 Kimi 超大规模（多租户、千卡级）相差多个数量级。Mooncake 的全球调度、热点复制、Conductor 调度算法是**超大规模专属问题**，不是本课题要解决的。
- ❌ **不声称**："Mooncake 的 525% 吞吐提升可以迁移到数据库 AI 负载"。该数字来自 16k–128k 长上下文 + 50% prefix 模拟数据；数据库 AI 负载通常 input 短得多（embedding 几十 tokens、AI_COMPLETE 多在 1k–4k），收益量级不可直接照搬。
- ❌ **不声称**："prefill/decoding 分池是 vLLM 的最佳部署方式"。Mooncake 自己也保留 chunked prefill inline 用于短请求（§5 首段 "A request's prefill is inlined into the decoding batch only when it can be forwarded without chunking"）；课题用 vLLM 单副本 baseline 时不必强行分池。
- ❌ **不声称**："prediction-based early rejection 是 K_max 控制的唯一解"。Mooncake 的预测依赖 Transformer 计算规律性；本课题的上游调度面对的请求异质性更高（不同 AI 算子、不同模型），prediction 模型建立成本可能不划算，简单阈值或 EWMA 未必更差（呼应 `code/AGENTS.md` §1 简单性原则）。
- ❌ **不声称**：Kimi 的 trace 统计（input 7590 tokens、input/output=720）代表数据库 AI 负载特征。两者模态不同——AI_EMBED input 极短、AI_COMPLETE 以中等长度 prompt 为主，不可类比。

### 3. 对本课题的实际用途

| 用途类型 | 具体方式 | 优先级 |
|----------|----------|--------|
| □ 动机证据 | §1.1 "KVCache 调度是 LLM serving 调度核心" + §7 "过载是 MaaS 常态"——支撑课题研究内容二的动机：上游调度必须感知模型服务侧的负载与 SLO 状态 | ⭐⭐ |
| □ Baseline | 不作为直接 baseline。课题 baseline 是 vLLM 单副本 + 静态 batching；Mooncake 是工业级参考点，不作为对比对象 | — |
| ☑ **设计参考** | (1) **全局 prefix 池跨副本复用** → 研究内容一 prefix-aware 策略的多副本扩展设计参考；(2) **prefill/decode 集群分离** → 研究内容二"异构 actor pool 分池路由"的生产级理论依据；(3) **预测式 early rejection** → K_max / admission 控制的反例与正面设计参考 | ⭐⭐⭐ |
| ☑ **对照区分** | 在开题报告/论文中说明：本课题优化的是数据库到模型服务**上游**的数据组织与提交控制，不改 vLLM 内部、不做全球调度；Mooncake 是部署侧生产系统的参考，本课题的规模与抽象层次不同 | ⭐⭐ |
| □ 空白论证 | Mooncake 解决"超大规模 MaaS 提供商"的全球调度问题；本课题填补"数据库 AI 算子链路上游调度"这一未被它覆盖的空白——两者优化对象不同 | ⭐ |

### 4. 不足 → 你的机会

| 论文的不足 / 未回答的问题 | 你的课题可能如何填补 |
|---------------------------|---------------------|
| Mooncake 只优化**模型服务侧**部署调度，不关心请求从何而来、数据如何组织 | 本课题研究**数据库 AI 算子上游链路**——数据如何被组织为请求（token-budget / length-align / prefix-aware batching）、以什么节奏提交（queue-adaptive flush / K_max）——是 Mooncake 视角看不到的上游优化空间 |
| Mooncake 假设请求到达过程是外生给定（Poisson 或 trace 重放） | 本课题的**提交控制策略**可以主动塑造请求到达模式，把"被动过载调度"前移为"主动节奏控制"——在 admission 之前就调节 inflight |
| Mooncake 的 prefix 复用是**跨请求**的 prompt 前缀哈希 | 本课题的 prefix-aware 可进一步结合**数据库 SQL 上下文**（同一 query template、同一 RAG 检索片段）做数据库语义层的 prefix 分组——这是 Mooncache 没有的模态信息 |
| Mooncake 的预测器依赖 Transformer 计算规律性、在 Kimi 同构集群离线拟合 | 本课题面对异构 AI 算子（AI_COMPLETE / AI_EMBED / AI_CLASSIFY）和异构模型，需要更鲁棒的轻量级估计——这既是挑战也是研究空间 |
| Mooncake 的 prefill/decoding 比例"preset"（§8.1.1），动态转换是 future work | 本课题的 Ray actor pool 天然支持动态分池路由——可作为对 Mooncake 静态配比限制的改进方向验证 |

### 5. 可论文化的措辞

> 正如 Qin et al. (2025) 在 Kimi 生产 serving 平台 Mooncake 中所示，LLM serving 的调度本质上是 KVCache 的调度，而吞吐优化（KV 复用、大 batch）与延迟 SLO 存在天然冲突——这一观察同样适用于数据库 AI 算子的外部执行链路：当数据库侧以高并发提交 AI 请求时，模型服务侧的 KV 复用与 batching 收益会被 SLO 违约吞噬。

> 与 Qin et al. (2025) 的全球规模 KVCache 池与 Conductor 调度不同，本课题不在模型服务部署侧做改造，而是在数据库到模型服务的**上游**链路上做数据组织与提交控制——前者是 MaaS 提供商视角的部署优化，后者是数据库 AI 算子视角的链路优化，两者层次互补。

> Qin et al. (2025) 提出的 prediction-based early rejection 解决了朴素 admission 控制的反相波动问题；本课题的 K_max 自适应与 queue-adaptive flush 在数据库 AI 负载这一更小规模、更异构的场景中，借鉴其"预测式而非纯阈值式"的设计思想，但采用更简单的 EWMA/固定步长机制（理由见 Ray ConcurrencyCapBackpressurePolicy 的废弃教训）。

> Mooncake 通过分离 prefill 与 decoding 集群（§5）证明了"按计算特征分池"的生产价值；本课题在 Ray actor pool 上采用类似分池路由思想，将异构请求（不同 AI 算子、不同长度）路由到不同 actor 子池，但规模与 Mooncake 的千卡级集群相差多个数量级。

### 6. 后续待读

- [ ] [[distserve_osdi2024]] — 已精读。Mooncake 的直接对照工作，placement 搜索算法可补 Mooncake 未做的部分。
- [ ] Splitwise (Patel et al., arXiv 2023) — disaggregation 概念提出者，Mooncake §1.1/§9 反复引用。
- [ ] Sarathi-Serve (Agrawal et al., 2024) — chunked prefill inline 方案，Mooncake §5 整节辩论对象。
- [ ] AttentionStore (Gao et al., arXiv 2024) — 并行分层 KVCache 工作，Mooncake §9 对比。
- [ ] Preble (Srivatsa et al., 2024) — prompt global scheduling 同方向工作。
- [ ] LoongServe [14] — elastic sequence parallelism，Mooncake §5.1 提到但未采用的对照路线。

---

## 元反思

- **精读收益**：🟢 高。Mooncake 是少数真实生产 LLM serving 系统论文，其"KVCache-centric"视角、过载调度的反相波动观察、分池路由的工程辩论，对本课题两项研究内容均有直接借鉴。
- **是否要纳入核心文献库**：是。作为研究内容二（提交控制策略、actor pool 分池路由）的首选生产系统参考，与研究内容一（prefix-aware）的二级参考。
- **计划复习周期**：4 周后复习（在课题进入研究内容二具体策略实现阶段时重读 §6/§7）。
- **一句话笔记自评**：把握了 Mooncake 的三层贡献（disaggregated KVCache 池 / cache-aware 调度 / 过载预测式拒绝）和它与本课题的层次差异（部署侧 vs 上游链路、超大规模 vs 数据库规模），没有过度引申其全球调度思想到本课题。最不确定的部分：§5.2 layer-wise prefill 的 overlap 实际效率上限（Fig.7 只给到 128k），以及 §7.4 的 `td` 统一假设在 output 长度极度异质时的退化程度——这两点在课题实验设计时若需精确量化，应回看原文 + 开源 trace 复算。

---

## 相关笔记

- [[tpl-文献精读-深度版]] — 本模板
- [[distserve_osdi2024]] — 同方向 disaggregation 主对照工作
- [[sarathi_serve_osdi2024]] — chunked prefill inline 路线
- [[orca_osdi2022]] — iteration-level scheduling 基础
- [[vllm_sosp2023]] — continuous batching + PagedAttention baseline
- [[文献地图]] — 文献全景
- [[ai_operator_literature_inventory]] — 完整文献清单
