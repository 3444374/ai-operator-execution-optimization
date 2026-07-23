# 推理管线交互文献综述：上游批处理与下游连续批处理的协同优化

生成日期：2026-07-16
调研方法：多源并行检索（WebSearch x 16 轮），覆盖 CCF-A 会议/期刊（SOSP, OSDI, NSDI, ISCA, NeurIPS, ICML, SIGMOD, VLDB, EuroSys, MLSys, FAST）及技术报告

---

## 研究空白确认

**确认存在研究空白**：现有文献在三类主要的批处理优化之间缺乏桥接工作：

1. **上游数据管线批处理**（data pipeline batching）：如何将数据库/数据源的行组织为 batch，控制 batch_size、partition_count、concurrency 等参数。现有工作：Ray Data Streaming Batch Model、Spark SQL tuning、Daft partitioning。
2. **推理引擎内部批处理**（inference engine batching）：如何将到达推理引擎的请求合并为 GPU 上的 micro-batch，控制 continuous batching、chunked prefill、KV cache 管理。现有工作：vLLM、Orca、Sarathi-Serve 等大量论文。
3. **推理引擎内部的 prefill-decode 协同**（prefill-decode interaction）：推理引擎内部两个阶段的批处理冲突与调度。现有工作：DistServe、Splitwise、Mooncake 等。

**没有任何已有工作研究**："上游数据管线以何种 batch_size / partition / concurrency 组织数据，对下游推理引擎的 continuous batching 效率、GPU 利用率、队列延迟和端到端吞吐有何影响，以及两者之间是否存在最优协调策略"。

这一空白正是本课题"研究内容一（数据组织）与研究内容二（模型服务调度）之间的跨层协同"的核心研究空间。

---

## 第一部分：LLM 推理服务与连续批处理

### 1. vLLM: PagedAttention + Continuous Batching (SOSP 2023)

- **作者**：Woosuk Kwon, Zhuohan Li, Siyuan Zhuang, Ying Sheng, Lianmin Zheng, Cody Hao Yu, Joseph E. Gonzalez, Hao Zhang, Ion Stoica（UC Berkeley）
- **会议**：ACM SOSP 2023（Best Paper）
- **论文**：https://arxiv.org/abs/2309.06180
- **代码**：https://github.com/vllm-project/vllm

**核心机制**：PagedAttention 受操作系统虚拟内存和分页机制启发，将 KV cache 分解为固定大小的 block（page），通过 block table 映射逻辑 block 到物理 block，实现：接近零的内部碎片（浪费 <4%）、无外部碎片、通过 copy-on-write 灵活共享 KV cache。vLLM 在此基础上实现 centralized scheduler 协调分布式 GPU worker，并支持 continuous batching —— 动态地往运行中的 batch 添加/移除请求。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：vLLM 的 continuous batching 是推理引擎内部的被动适配机制 —— 它等待上游请求到达后，在 GPU 上尽可能多地将请求合并为一个 micro-batch。但它**不控制上游请求以何种粒度、何种间隔到达**。如果上游以极细粒度（如 batch_size=1）高频提交请求，vLLM 的 continuous batching 仍有足够的合并机会；但如果上游以极大粒度（如 batch_size=512）低频提交，可能造成 GPU 空闲期和突发拥塞。vLLM 论文没有研究上游数据管线侧的 batch_size 应如何选择才能最大化 continuous batching 的收益。

**未研究的内容**：
- 上游数据管线 batch_size / partition_count 对 continuous batching 效率的因果影响
- 上游数据组织策略与下游推理引擎 KV cache 管理的协同
- token 长度分布对 continuous batching 微观效率的影响

---

### 2. Orca: Iteration-Level Scheduling (OSDI 2022)

- **作者**：Gyeong-In Yu, Joo Seong Jeong, Geon-Woo Kim, Soojeong Kim, Byung-Gon Chun（Seoul National University / FriendliAI）
- **会议**：USENIX OSDI 2022
- **论文**：https://www.usenix.org/conference/osdi22/presentation/yu

**核心机制**：Orca 首次提出 iteration-level scheduling（后被称为 continuous batching）。关键洞察是：传统 request-level batching 要求 batch 中所有请求同时完成才行，而 LLM 生成是自回归的 —— 不同请求需要不同数量的迭代。Orca 改为在每次迭代（生成一个 token）的粒度上调度，请求完成即可立即返回，新请求可随时加入 batch。同时引入 selective batching：将 Transformer 操作分为 batchable（Linear, Add, GeLU）和 non-batchable（Attention），分别处理。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：Orca 比 vLLM 更清晰地暴露了"到达模式"对 iteration-level scheduling 的影响。如果上游能控制请求的到达时间、粒度和 token 长度分布，就能影响每个 iteration 中 micro-batch 的大小和组成。但 Orca 假设请求独立到达，不研究上游如何组织数据。

**未研究的内容**：
- 上游数据管线控制请求到达粒度和时间的可能性
- 如何利用已知的 workload 特征（token 长度、shared prefix）在上游做预组织
- 上游 batch_size × 下游 iteration-level scheduling 的联合优化空间

---

### 3. Sarathi-Serve: Chunked Prefill + Stall-Free Scheduling (OSDI 2024)

- **作者**：Amey Agrawal, Nitin Kedia, Ashish Panwar, Jayashree Mohan, Nipun Kwatra, Bhargav Gulavani, Alexey Tumanov, Ramachandran Ramjee（Microsoft Research India / Georgia Tech）
- **会议**：USENIX OSDI 2024
- **论文**：https://www.usenix.org/system/files/sarathi_serve_osdi2024.pdf
- **代码**：https://github.com/microsoft/sarathi-serve

**核心机制**：Sarathi-Serve 识别了 LLM 推理的根本吞吐-延迟矛盾：prefill（输入处理）是 compute-bound 的，decode（token 生成）是 memory-bound 的。两种调度器都存在缺陷 —— prefill 优先（vLLM）导致 decode stall 数秒，decode 优先（FasterTransformer）导致 GPU 空转。Sarathi-Serve 引入：chunked-prefills（将 prefill 切分为近等大小的 chunk 分散到多个 iteration）和 stall-free scheduling（新请求可以在不暂停 decode 的情况下加入 batch）。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：Chunked prefill 的核心参数是 chunk size（如 ~512-1024 tokens），该参数应在 GPU compute 饱和点附近设定。如果上游 data pipeline 能够预先知道每条请求的 token 长度、shared prefix 情况，就可以在上游做更高效的 chunk 组织和对齐，减少下游 Sarathi 的不必要 chunk 拆分和 KV cache 碎片。**Sarathi-Serve 论文本身不研究上游如何准备和组织数据来适配 chunked prefill**。

**未研究的内容**：
- 上游 pre-batching 如何与 chunk size 对齐
- 上游按照 token 长度分组提交请求，是否能减少 chunked prefill 的内部碎片
- 上游数据组织策略对 stall-free scheduling 中 hybrid batch 构成的优化潜力

---

### 4. FastServe: Preemptive Scheduling with MLFQ (arXiv 2023)

- **作者**：Bingyang Wu, Yinmin Zhong, Zili Zhang, Shengyu Liu, Fangyue Liu, Yuanhang Sun, Gang Huang, Xuanzhe Liu, Xin Jin（Peking University）
- **论文**：https://arxiv.org/abs/2305.05920

**核心机制**：FastServe 利用 LLM 推理的自回归特性，在每次输出 token 边界实现抢占式调度。引入 Skip-Join Multi-Level Feedback Queue（MLFQ）：利用已知的输入长度（semi-information-agnostic setting）跳过不需要的高优先级队列，减少不必要的降级。同时实现主动 GPU 内存管理：在 KV cache 满时将低优先级 job 的中间状态卸载到主机内存。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：FastServe 的关键参数是 job 的输入长度（已知）和输出长度（未知但可通过输入长度预估）。如果上游 data pipeline 在组织 batch 时考虑了输入长度分布，就可以帮助 FastServe 的 MLFQ 做出更优的抢占和内存管理决策。

**未研究的内容**：上游 batch 组织与下游 MLFQ 调度的协同。

---

### 5. DistServe: Prefill-Decode Disaggregation (OSDI 2024)

- **作者**：Yinmin Zhong, Shengyu Liu, Junda Chen, Jianbo Hu, Yibo Zhu, Xuanzhe Liu, Xin Jin, Hao Zhang（Peking University / UC Berkeley / USC）
- **会议**：USENIX OSDI 2024
- **论文**：https://www.usenix.org/system/files/distserve_osdi2024.pdf

**核心机制**：DistServe 将 prefill 和 decode 阶段完全分派到不同的 GPU 实例上，消除它们之间的干扰。关键洞察：prefill 是 compute-bound（大 batch 不提升效率，一旦 GPU 饱和），decode 是 memory-bandwidth-bound（大 batch 显著提升 GPU 利用率）。因此 prefill 不需要大 batch，decode 需要大 batch。Placement searching algorithm 为每个阶段自动选择最优的模型并行策略。实现 4.4-4.9x 性能提升（保证 SLO 的前提下）。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：DistServe 最直接的 relevance 是：它将 prefill（上游处理）和 decode（下游生成）拆分为两个独立调优的阶段。但在 DistServe 中，"上游批处理"仍是推理引擎内部的 prefill 阶段，不是数据管线侧的 batch 组织。如果将 DistServe 的 disaggregation 逻辑向外推一层 —— 数据管线控制数据如何进入 prefill 节点，prefill 节点再向 decode 节点传递 KV cache —— 就形成了"数据管线 -> prefill -> decode"的三层协调问题。

**未研究的内容**：
- 数据管线侧如何根据 P/D 节点负载、KV cache 转移开销、GPU 利用率组织上游 batch
- 三层协调（数据管线 -> prefill -> decode）的联合优化

---

### 6. Splitwise: Phase Splitting for Cost/Power Efficiency (ISCA 2024)

- **作者**：Pratyush Patel, Esha Choukse, Chaojie Zhang, Aashaka Shah, Inigo Goiri, Saeed Maleki, Ricardo Bianchini（University of Washington / Microsoft）
- **会议**：ACM/IEEE ISCA 2024
- **论文**：https://arxiv.org/abs/2311.18677

**核心机制**：将 prompt computation（prefill，compute-intensive）和 token generation（decode，memory-bound）分配到不同的机器上。Prompt 机器用高算力 GPU（如 H100），Token 机器用低算力、低功耗 GPU（如 A100）。通过 layer-wise 异步传输 KV cache 来隐藏传输开销（仅 0.8% 端到端延迟）。关键是：token generation 不需要最新 GPU 的算力。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：Splitwise 的启示是上游 batch 的 token 长度分布直接影响 KV cache 传输开销和 P/D 机器之间的负载平衡。

**未研究的内容**：上游数据管线如何根据 P/D 机器配比和网络带宽组织 batch。

---

### 7. Mooncake: KV Cache-centric Disaggregation (FAST 2025 Best Paper)

- **作者**：Ruoyu Qin, Zheming Li, Weiran He et al.（Tsinghua University / Moonshot AI）
- **会议**：USENIX FAST 2025（Best Paper）
- **论文**：https://arxiv.org/abs/2407.00079
- **代码**：https://github.com/kvcache-ai/Mooncake

**核心机制**：Kimi 的生产级 LLM 服务平台。核心创新是将 GPU 集群的闲置 CPU、DRAM、SSD 和 RDMA/NIC 资源池化为分布式 KV cache 池（Mooncake Store），实现"以存储换计算"。Transfer Engine 支持 TCP、RDMA、NVMe-over-Fabric，最高 190 GB/s。Conductor 全局调度器以最大化 KV cache 复用、负载均衡和满足 SLO 为目标。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：Mooncake 的 KV cache 复用收益与请求的 prefix 共享程度直接相关。如果上游数据管线能按照 prefix 共享程度组织 batch（将共享相同 prefix 的请求一起提交），Mooncake 的 cache hit rate 和 prefill 节省将最大化。**Mooncake 的调度器在推理引擎内部做 prefix-aware 调度，但不控制上游数据如何分组到达**。

**未研究的内容**：
- 上游数据管线 prefix-aware batch 组织对 Mooncake cache hit rate 的因果影响
- 上游 batch_size 和提交速率对 KV cache 池内存压力的影响

---

## 第二部分：自适应批处理与推理服务调度

### 8. Clipper: AIMD Adaptive Batching (NSDI 2017)

- **作者**：Daniel Crankshaw, Xin Wang, Giulio Zhou, Michael J. Franklin, Joseph E. Gonzalez, Ion Stoica（UC Berkeley）
- **会议**：USENIX NSDI 2017
- **论文**：https://www.usenix.org/conference/nsdi17/technical-sessions/presentation/crankshaw

**核心机制**：通用低延迟预测服务系统，采用 AIMD（Additive-Increase-Multiplicative-Decrease）自适应批处理：在延迟 SLO 内逐步增加 batch size，一旦超过 SLO 就减少 10%。同时支持 delayed batching（在 batch 未达到最大时等待额外请求到达）。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：Clipper 的 AIMD 是纯反应式（reactive）的 —— 它不断试探延迟边界来调整 batch size。如果上游数据管线能提供 workload 特征（数据量、请求速率、token 长度分布），batch size 可以前瞻性（proactive）地设置，减少 AIMD 的探索成本。**但 Clipper 本身是通用预测服务系统，不涉及 LLM 的 continuous batching 场景**。

**未研究的内容**：
- 前瞻性（proactive）batch size 选择 vs 反应式 AIMD 的对比
- 上游 workload 特征如何指导 batch size 预设
- LLM 场景下 batch size 与 token 长度、KV cache 的耦合

---

### 9. Nexus: Batch-Aware GPU Scheduling (SOSP 2019)

- **作者**：Haichen Shen, Lequn Chen, Yuchen Jin, Liangyu Zhao, Bingyu Kong, Matthai Philipose, Arvind Krishnamurthy, Ravi Sundaram（University of Washington / Microsoft）
- **会议**：ACM SOSP 2019

**核心机制**：GPU 集群引擎，针对视频分析 DNN 推理。提出 Squishy Bin Packing：batch size 影响资源消耗和处理延迟，调度器需同时决定 GPU 数量、模型分布、batch size 和执行顺序。还支持 Prefix Batching：识别不同模型共享的子图并一起批处理。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：Squishy Bin Packing 的形式化与"上游 batch_size 选择影响下游 GPU 资源消耗"的问题非常相似。不同之处在于 Nexus 针对的是传统 DNN（非 LLM），batch size 的影响是确定性的；LLM 场景下 token 长度不同导致 batch 处理时间不确定。

**未研究的内容**：
- LLM 场景下 variable-length batch 的 bin packing 问题
- 上游数据组织如何影响 GPU 上 batch 的"可打包性"

---

### 10. Clockwork: Predictable Latency Scheduling (OSDI 2020)

- **作者**：Arpan Gujarati, Reza Karimi, Safya Alzayat, Wei Hao, Antoine Kaufmann, Ymir Vigfusson, Jonathan Mace（Max Planck / Emory University）
- **会议**：USENIX OSDI 2020

**核心机制**：基于 DNN 推理的确定性（99.99% 延迟偏差 < 0.03%），Clockwork 采取自底向上的可预测性策略：防止并发 GPU kernel 执行、消除线程调度不确定、集中所有调度决策。如果无法满足 SLO 立即拒绝，不浪费 GPU 资源。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：Clockwork 的"确定性"假设在 LLM 场景下被打破 —— LLM 推理时间依赖于 batch 中每条请求的 token 数量、KV cache hit rate 和 decode 长度，高度可变。如果上游数据管线能按 token 长度分组提交请求（使每个 batch 内的请求长度更均匀），就能恢复部分"可预测性"，但 Clockwork 本身不研究这个方向。

**未研究的内容**：
- 上游数据组织能否恢复 LLM 推理的可预测性
- 非均匀 token 长度 batch 对调度器预测准确性的影响

---

### 11. NVIDIA Triton Inference Server: Dynamic Batching

- **文档**：https://docs.nvidia.com/deeplearning/triton-inference-server/
- **类型**：工业系统文档

**核心机制**：Triton 的动态批处理（dynamic batching）允许请求在队列中等待一段时间（`max_queue_delay_microseconds`），以形成更大的 batch。支持 preferred_batch_size、priority_levels 和 queue_policy 等配置。Model Analyzer 工具自动搜索最优的 batch_size、instance_count 和 concurrency 组合。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：Triton 的动态批处理本质上是"在推理服务入口处等待以形成 batch"。如果上游已经按合适的粒度组织好了 batch，Triton 就不需要额外等待，可以直接将预组织的 batch 送入 GPU。**但 Triton 假设上游提交的是单个请求而非预组织 batch**。

**未研究的内容**：
- 上游提交预组织 batch 与下游动态批处理的协同
- 何时应该在上游组织 batch，何时应该依赖下游动态批处理
- 两端 batch 策略的联合优化空间

---

## 第三部分：数据管线与推理服务的交互

### 12. Ray Data: The Streaming Batch Model (arXiv 2025)

- **作者**：Frank Sifei Luan, Ziming Mao, Ron Yifeng Wang, Stephanie Wang, Ion Stoica et al.（UC Berkeley / Anyscale）
- **论文**：https://arxiv.org/abs/2501.12407

**核心机制**：Ray Data 提出 streaming batch model —— 批处理和流处理的混合模型，专为 CPU-GPU 异构执行设计。通过 disaggregated streaming architecture 将 CPU 预处理与 GPU 计算分离：CPU fleet 读取原始数据、解码、预处理、通过 Ray Object Store（Arrow zero-copy）流式传输到 GPU fleet。支持 dual backpressure policies（proactive 资源预算 + reactive 队列监控）。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：这是目前**最接近研究"上游数据管线与下游推理服务交互"的工作**。Ray Data 直接控制了 batch_size、concurrency、partition 等参数，并且内置 backpressure 机制来防止 GPU 被淹没。**但它不研究这些参数选择对下游推理引擎（如 vLLM）continuous batching 效率的具体影响**。Ray Data 的 GPU 阶段假设直接调用模型（不使用 continuous batching 引擎），或者将 vLLM 视为黑盒。

**未研究的内容**：
- `batch_size × concurrency` 的选择如何影响下游 vLLM/SGLang 的 continuous batching 效率
- 上游 Arrow RecordBatch 的组织方式对下游 PagedAttention KV cache 块利用率的直接影响
- 上游 backpressure 信号与下游推理引擎队列深度的联合调控
- 上游 token 长度感知的 batch 组织对下游 chunked prefill 效率的优化

---

### 13. Ray Data LLM: Native vLLM Integration (Ray 2.44+, 2025)

- **文档**：https://docs.ray.io/en/latest/llm/examples/batch/vllm-with-structural-output.html
- **类型**：工业系统文档

**核心机制**：从 Ray 2.44（2025年4月）开始，Ray Data 提供原生的 `ray.data.llm` 模块，通过 `vLLMEngineProcessorConfig` 和 `build_llm_processor` 将 vLLM 作为 batch inference 的执行后端。支持：自动 GPU/CPU 分配、placement group 管理、continuous batching with vLLM、fault tolerance（job-level checkpointing）、data parallelism（`concurrency`）+ model parallelism（`tensor_parallel_size`）组合。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：这是 Ray Data 层与 vLLM 层的**最近接触点**，但仍然将 vLLM 的 continuous batching 视为黑盒。`batch_size` 参数控制"每次调用 vLLM 时提交多少条数据"，但 vLLM 内部的 continuous batching 如何响应不同 `batch_size` 的提交模式没有被研究或暴露为可调优参数。用户反馈显示：`batch_size=16-32`、`max_concurrent_batches` 调大能 saturate vLLM，但这些是经验性调优，没有系统性研究。

**未研究的内容**（系统性的、可形式化的）：
- 上游 `batch_size` × `concurrency` 与下游 continuous batching 的排队论模型
- 最优的上游 batch 提交策略存在性证明或近似算法
- 不同 workload（token 长度分布、请求速率、prefix 共享度）下的最优上游 batch 策略

---

### 14. NeuStream: Bridging DL Serving and Stream Processing (EuroSys 2025)

- **作者**：Yuan et al.（Peking University / Microsoft Research）
- **会议**：ACM EuroSys 2025
- **论文**：https://dl.acm.org/doi/10.1145/3689031.3717489

**核心机制**：将 DNN 推理工作流分解为 stream processing pipeline 中的模块，实现 module-level fine-grained batching。使用 two-level scheduling 最大化 goodput 同时满足 SLO。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：NeuStream 的 module-level batching 提供了一种上游数据管线切分的思路 —— 不是以"请求"为单位，而是以"模块"为单位组织 batch。但 NeuStream 针对的是 DNN 推理（非 LLM，无 continuous batching），其 batching 策略不能直接套用到 LLM 场景。

**未研究的内容**：LLM 场景下的 module-level batching（在 attention 模块、FFN 模块等层级做 batch 调度）。

---

### 15. HedraRAG: Coordinating LLM and Retrieval in RAG Pipelines (SOSP 2025)

- **作者**：Hu et al.（UCSD / Rice University）
- **会议**：ACM SOSP 2025
- **论文**：https://arxiv.org/html/2507.09138

**核心机制**：解决 RAG pipeline 中检索（CPU）和生成（GPU）阶段的协调问题。使用 RAGraph（图抽象）、fine-grained sub-stage partitioning、dynamic batching、semantic-aware speculative execution 和 partial GPU index caching。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：HedraRAG 研究的是 CPU（检索）和 GPU（生成）两个异构阶段之间的协调，与本课题的"数据管线（CPU 侧 batch 组织）与推理引擎（GPU continuous batching）协调"最为接近。但 HedraRAG 的"上游"是检索阶段，"下游"是 LLM 生成阶段，两个阶段的交互方式是"检索结果作为 LLM 输入"。本课题的"上游"是数据管线（Arrow RecordBatch 组织、task/actor 分发），"下游"是推理引擎（continuous batching），交互方式是"batch 提交模式影响 continuous batching 效率"。

**未研究的内容**：
- 数据管线 batch 提交模式（batch_size、提交间隔、并发数）对 continuous batching 效率的因果影响
- Arrow RecordBatch 的物理布局如何影响推理引擎的 batch 构建效率

---

## 第四部分：Token/Prefix-Aware 优化

### 16. Parrot: Semantic Variable for Prompt Sharing (OSDI 2024)

- **作者**：Chaofan Lin, Zhenhua Han, Chengruidong Zhang, Yuqing Yang, Fan Yang, Chen Chen, Lili Qiu（SJTU / Microsoft Research）
- **会议**：USENIX OSDI 2024
- **论文**：https://www.usenix.org/conference/osdi24/presentation/lin-chaofan
- **代码**：https://github.com/microsoft/ParrotServe

**核心机制**：引入 Semantic Variable 抽象，将 LLM 应用中的 prompt 输入/输出变量标注为应用层可见的数据流。三个关键优化：将多请求 DAG 一次提交（消除 30-50% 的 network/queuing 延迟）、application-level 性能目标感知调度、共享 prompt prefix 的 KV cache 重用（观察到 94% token 重复率）。实现 11.7x 加速。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：Parrot 的 Semantic Variable 本质上是一种"上游应用语义"的表达方式，能让推理引擎知道哪些请求共享相同的 prefix、哪些请求有依赖关系。如果本课题的上游 data pipeline 能按 prefix 共享程度组织 batch，就能在不修改推理引擎的前提下获得类似 Parrot 的部分收益。

**未研究的内容**：
- 上游数据管线 prefix-aware batch 组织 vs 推理引擎内部 prefix-aware 调度的分工
- 如何在不修改推理引擎的前提下最大化上游 prefix-aware batching 的收益

---

### 17. SGLang: RadixAttention + Structured Generation (NeurIPS 2024)

- **作者**：Lianmin Zheng, Liangsheng Yin, Zhiqiang Xie et al.（Stanford / UC Berkeley / SJTU / Texas A&M）
- **会议**：NeurIPS 2024
- **论文**：https://proceedings.neurips.cc/paper_files/paper/2024/file/724be4472168f31ba1c9ac630f15dec8-Paper-Conference.pdf
- **代码**：https://github.com/sgl-project/sglang

**核心机制**：RadixAttention 使用 radix tree 自动匹配和复用 KV cache prefix，支持 LRU 驱逐、引用计数保护、cache-aware scheduling（优先调度有更长匹配 prefix 的请求，DFS 序在离线情况下达到最优 cache hit rate）。压缩有限状态机实现快速结构化解码。Chatbot Arena 部署中 LLaVA 模型 cache hit rate 52.4%，Vicuna 74.1%。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：SGLang 的 cache-aware scheduling 在推理引擎内部按"最长共享 prefix 优先"调度请求。如果上游 data pipeline 提前将请求按 prefix 共享程度分组提交，就能最大化 RadixAttention 的 cache hit rate。Dexter et al. (2025) 证明了在线调度问题是 NP-hard 并提出 k-LPM 算法，进一步说明上游预组织 batch 有理论价值。

**未研究的内容**：
- 上游 prefix-aware batch 组织 + 下游 cache-aware scheduling 的联合决策
- prefix 感知的 batch_size 选择（更大的 batch 有更多 prefix 合并机会，但也引入更多 KV cache 压力）

---

### 18. KVFlow: Workflow-Aware Prefix Caching (NeurIPS 2025)

- **论文**：https://neurips.cc/virtual/2025/loc/san-diego/poster/119883

**核心机制**：针对多 Agent 工作流中固定 prompt 被重复调用，使用 Agent Step Graph 预测未来复用并指导 KV cache 驱逐和预取。实现 1.83x（单工作流）和 2.19x（并发工作流）加速。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：KVFlow 表明：如果能提前知道 workload 结构（哪些 prompt 会被复用），就能显著提升 prefix caching 效率。本课题的 data pipeline 侧具备这一知识（知道 SQL 查询触发了哪些 AI 算子调用，数据如何分组）。

**未研究的内容**：上游数据管线将 workload 结构信息传递给推理引擎的机制。

---

### 19. ChunkAttention: Prefix-Aware Self-Attention (ACL 2024)

- **论文**：https://aclanthology.org/2024.acl-long.623/

**核心机制**：在运行时检测多租户请求的 prompt prefix 匹配，将 KV cache 组织为 prefix tree 中的 chunk，使用 two-phase partition algorithm 提升 shared-prefix attention 计算的数据局部性。3.2-4.8x self-attention kernel 加速。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：ChunkAttention 在 attention kernel 层面优化 prefix sharing。如果上游已经将共享 prefix 的请求分组提交，kernel 层面的 chunk 检测和分区开销可以进一步降低。

---

### 20. EPIC: Position-Independent Caching (ICML 2025)

- **论文**：https://proceedings.mlr.press/v267/hu25j.html

**核心机制**：突破"必须精确 prefix 匹配才能复用 KV cache"的限制，实现与位置无关的 KV cache 模块化复用。使用 LegoLink 算法处理 document 边界处的 attention sink 效应。8x TTFT 改善，7x 吞吐提升。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：EPIC 将 prefix caching 的边界从"完全相同的 prefix"扩展到"位置不同但内容相同的文本段"。如果上游数据管线按照文档/文本段的语义边界组织 batch，可最大化 EPIC 的模块化复用收益。

---

## 第五部分：Ray-Specific 推理服务模式

### 21. Ray Serve LLM + vLLM Integration (2025)

- **文档**：https://docs.ray.io/en/master/serve/llm/user-guides/configuration.html
- **类型**：工业系统文档

**核心机制**：Ray Serve LLM 提供 vLLM 之上的可编程编排层。支持标准 OpenAI-compatible serving、Wide Expert Parallelism（MoE 模型）、Disaggregated Prefill/Decode（`build_pd_openai_app`）、Multi-Model Deployment（`LLMRouter`）、LoRA Multi-Adapter Serving 和 vLLM Engine Sharing via Serve Deployments。

**与"上游批处理 x 下游连续批处理"相关的关键洞察**：Ray Serve LLM 的 `build_pd_openai_app` 将 prefill 和 decode 分离为独立可缩放的 deployment，这正是"上游（prefill）批处理"和"下游（decode）批处理"分离的架构形态。但 prefill 侧的 batch 策略和 decode 侧的 batch 策略之间的协调仍然没有被研究。

**未研究的内容**：
- Prefill deployment 的 batch_size/concurrency 与 Decode deployment 的 batch_size 之间的最优配比
- 上游 data pipeline 如何根据 P/D 负载动态调整提交策略

---

### 22. Ray Compiled Graphs for GPU Inference (2024)

- **博客**：https://www.anyscale.com/blog/announcing-compiled-graphs
- **类型**：工程发布博客（非论文）

**核心机制**：静态计算图原语，实现 50us 任务提交延迟（vs 1-2ms 标准 Ray），原生 GPU-to-GPU 通信 via NCCL（A100 NVLink 上 140x 延迟降低）。支持 Scatter-Gather（tensor-parallel 推理）、Pipeline-Parallel Chains（集成 vLLM 实现 10-15% 吞吐提升）。实现了 Zero Bubble pipeline scheduling。

**与本课题相关的洞察**：Compiled Graphs 的 Scatter-Gather 模式与本课题的"task/actor 并行度控制"直接相关。

---

## 研究空白总结

### 已确认的研究空白

| 维度 | 已有工作覆盖面 | 空白 |
|---|---|---|
| **上游数据管线批处理** | Ray Data batch_size/concurrency/partition 调优 | 上游 batch 参数（batch_size, partition_count, concurrency）如何影响下游 continuous batching 效率 |
| **下游推理引擎连续批处理** | vLLM PagedAttention, Orca iteration-level, Sarathi chunked prefill, SGLang RadixAttention | 推理引擎对外部数据组织策略的敏感度分析 |
| **推理引擎内 P/D 协同** | DistServe, Splitwise, Mooncake disaggregation, DynaServe hybrid | P/D 协同逻辑向外延伸到数据管线层 |
| **Prefix/Tag/Cache-Aware** | SGLang radix tree, Parrot semantic variable, KVFlow agent step graph, EPIC position-independent | 数据管线侧 prefix-aware 预组织对推理引擎 prefix caching 效率的影响 |
| **Pipeline × Inference 桥接** | Ray Data vLLM integration（工程集成，非研究）, HedraRAG（检索×生成协调） | 系统性的上游 batch 策略 × 下游 continuous batching 协同优化（理论模型、实验验证、跨 workload 泛化） |

### 核心空白表述

**没有任何已有工作系统性研究**：数据管线侧的 batch_size、partition_count、concurrency、token-aware 分组、prefix-aware 分组等参数选择，对推理引擎侧的 continuous batching 效率（micro-batch 大小分布、GPU 利用率、KV cache 命中率、队列等待时间、端到端延迟和吞吐）的因果影响及最优协调策略。

这个空白正是本课题"研究内容一（数据组织与批处理构造）"和"研究内容二（模型服务调度）"之间的跨层协同空间的核心研究机会。

---

## 参考文献清单

| # | 论文/文档 | 出处 | 年份 | 与本课题相关度 |
|---|---|---|---|---|
| 1 | Kwon et al. vLLM: PagedAttention. | SOSP 2023 Best Paper | 2023 | ★★★★★ |
| 2 | Yu et al. Orca: Iteration-Level Scheduling. | OSDI 2022 | 2022 | ★★★★★ |
| 3 | Agrawal et al. Sarathi-Serve: Chunked Prefill. | OSDI 2024 | 2024 | ★★★★★ |
| 4 | Wu et al. FastServe: Preemptive Scheduling. | arXiv:2305.05920 | 2023 | ★★★ |
| 5 | Sheng et al. S-LoRA: Concurrent LoRA Adapters. | MLSys 2024 | 2024 | ★★★ |
| 6 | Zhong et al. DistServe: Disaggregated Serving. | OSDI 2024 | 2024 | ★★★★★ |
| 7 | Patel et al. Splitwise: Phase Splitting. | ISCA 2024 | 2024 | ★★★★ |
| 8 | Qin et al. Mooncake: KV Cache Disaggregation. | FAST 2025 Best Paper | 2025 | ★★★★ |
| 9 | Crankshaw et al. Clipper: Adaptive Batching. | NSDI 2017 | 2017 | ★★★ |
| 10 | Shen et al. Nexus: Batch-Aware GPU Scheduling. | SOSP 2019 | 2019 | ★★★ |
| 11 | Gujarati et al. Clockwork: Predictable Serving. | OSDI 2020 | 2020 | ★★★ |
| 12 | Luan et al. Ray Data: Streaming Batch Model. | arXiv:2501.12407 | 2025 | ★★★★★ |
| 13 | Lin et al. Parrot: Semantic Variable. | OSDI 2024 | 2024 | ★★★★ |
| 14 | Zheng et al. SGLang: RadixAttention. | NeurIPS 2024 | 2024 | ★★★★★ |
| 15 | Yuan et al. NeuStream: DL + Stream Processing. | EuroSys 2025 | 2025 | ★★★ |
| 16 | Hu et al. HedraRAG: RAG Coordination. | SOSP 2025 | 2025 | ★★★★ |
| 17 | KVFlow: Workflow-Aware Prefix Caching. | NeurIPS 2025 | 2025 | ★★★ |
| 18 | ChunkAttention: Prefix-Aware Self-Attention. | ACL 2024 | 2024 | ★★★ |
| 19 | EPIC: Position-Independent Caching. | ICML 2025 | 2025 | ★★★ |
| 20 | NVIDIA Triton Inference Server Docs. | docs.nvidia.com | 2025 | ★★★ |
| 21 | Ray Serve LLM Configuration Reference. | docs.ray.io | 2025 | ★★★★ |
| 22 | BatchLLM: Global Prefix Sharing + Token Batching. | MLSys 2026 | 2026 | ★★★★ |
| 23 | PKAS: Predictive KV Cache-Aware Scheduling. | HPDC 2026 | 2026 | ★★★★ |
| 24 | PLA-Serve: Prefill-Length-Aware Serving. | MLSys 2026 | 2026 | ★★★★ |
| 25 | Load-Aware Prefill Deflection. | arXiv:2607.02043 | 2026 | ★★★ |
| 26 | PEACE: Preemptive Cluster Scheduling. | IEEE 2026 | 2026 | ★★★ |
| 27 | Tian et al. Staggered Batch Scheduling. | 2025 | 2025 | ★★★ |
| 28 | BucketServe: Bucket-Based Dynamic Batching. | IEEE 2025 | 2025 | ★★★★ |

---

## 证据层级说明

| 类型 | 说明 |
|---|---|
| CCF-A 会议/期刊论文 | vLLM (SOSP), Orca (OSDI), Sarathi-Serve (OSDI), DistServe (OSDI), Splitwise (ISCA), Clipper (NSDI), Nexus (SOSP), Clockwork (OSDI), Parrot (OSDI), SGLang (NeurIPS), HedraRAG (SOSP), Mooncake (FAST), NeuStream (EuroSys) |
| CCF-A 2026 新论文 | BatchLLM (MLSys), PKAS (HPDC), PLA-Serve (MLSys), Load-Aware Prefill Deflection, PEACE |
| 技术报告 (arXiv) | Ray Data Streaming Batch Model, FastServe, Mooncake (v1) |
| 工业系统文档 | NVIDIA Triton, Ray Serve LLM, Ray Data LLM |
| 本地实验事实 | 已有 GPU-backed E2E profile 结果（`motivation/results/gpu/`） |
