# 文本原生框架单 Job 同环境正式观察

日期：2026-08-08
状态：**通过；16/16 cells，12/12 formal 可排名**

## 1. 实验目的

在同一 ShareGPT Chat manifest、同一双 vLLM 服务与同一结果合同下，观察 bounded HTTP 强控制、Daft Native、Daft Ray 和 Ray Data 官方推荐 API graph 各自拥有执行与调度时的吞吐、JCT 与服务压力形态。实验不要求项目路径胜出，重点是回答现有原生路径是否会落入欠供给、最小饱和或过量排队等不同 regime，以及这对状态感知和有界提交设计意味着什么。

## 2. 实验设置

| 项目 | 冻结合同 |
|---|---|
| GPU / 服务 | 2× RTX 4090；2 个 Qwen2.5-7B vLLM endpoint；prefix cache ON |
| vLLM | `max_num_seqs=256`、`max_num_batched_tokens=8192`、`max_model_len=8192`、`gpu_memory_utilization=0.90` |
| workload | ShareGPT controlled-skew 2,048 行、output cap 256；manifest SHA256 `54c97a2f…3169b` |
| bounded control | 每 endpoint C128、batch=1；扫描中实测达到 C256 的 98.22%，为第一个满足预先规定 97% 选择条件的并发点 |
| Daft | `functions.prompt` Native 与 Ray runner；vendor default，不虚构未公开并发旋钮 |
| Ray Data | `HttpRequestProcessorConfig`，batch=16、concurrency=8；C4/C8/C16 单次筛选的 measured peak |
| 重复 | 每臂 1 warm-up + 3 formal，formal 交错；每个 formal ≥60 s |
| 边界 | immutable manifest → 两 endpoint 原生 graph → 完整结果 gather；不含数据库 sink |

Daft/Ray Data formal 的 `scheduler_owner` 均属于被测框架；项目只负责统一输入、endpoint、结果收集、正确性和指标采集，没有注入 project actor pool、router、inflight 或 shared credit。

## 3. 严谨性自检

- matrix `passed`，16/16 cells、12/12 formal，全部 2,048/2,048 行、0 failure、exactly-once。
- 真实 Ray worker `RLIMIT_NOFILE=(65536,65536)`；低于 65,536 会在首 cell 前 fail closed。
- 四臂正式 service tok/s CV 为 0.28%–0.60%，JCT CV 为 0.29%–0.62%。
- 四臂使用相同 immutable manifest、模型、服务、输出 cap 和双 endpoint；每臂只使用预先冻结的 vendor/default 或校准点，formal 不在线调参。
- bounded C128 的三次均值 17,800 tok/s，与独立饱和校准 17,834 tok/s 接近，证明服务容量参考可复现。

因此可报告当前合同下的原生路径观察；不能把各框架不同的公开可调空间解释成某个内部调度算法的单因素因果效果。

## 4. 实验数据

### 4.1 三次 formal 均值

| 路径 | JCT (s) | service tok/s | vs bounded | MFU | GPU util | running mean/max | waiting mean/max | KV mean/max | TTFT mean (s) | queue mean (s) | GPU energy (kJ) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| bounded C128 | 95.49 | 17,800.21 | 0.00% | 0.562 | 97.02% | 229.8/256 | 2.8/144 | 0.598/0.799 | 0.783 | 0.103 | 81.28 |
| Daft Native | 98.36 | 17,285.85 | −2.89% | 0.651 | 91.42% | 319.4/512 | 783.5/1,763 | 0.798/1.000 | 40.495 | 37.489 | 80.40 |
| Daft Ray | 101.53 | 16,747.30 | −5.92% | 0.637 | 86.23% | 300.9/512 | 741.6/1,773 | 0.750/1.000 | 40.704 | 37.640 | 78.70 |
| Ray Data HTTP | 478.66 | 3,550.90 | −80.05% | 0.112 | 94.30% | 17.3/32 | 0.0/0 | 0.059/0.129 | 0.094 | 0.00002 | 324.59 |

这里的 TTFT、queue、running/waiting 与 KV 来自 vLLM service time-series/histogram；框架 barrier JCT 是统一可比的主延迟。Daft/Ray Data adapter 没有统一的逐 request end-to-end P99 边界，因此不伪造 request P99 横向排名。

12 个单次 formal 见 [`formal_runs.csv`](formal_runs.csv)，均值、sample SD/CV 与 bounded 差值见 [`formal_summary.csv`](formal_summary.csv)。MFU 公式为 `sum(estimated_flops_per_gpu_delta) / (service_wall × 2 × 165 TFLOPS)`；能耗为两卡 mean power 之和乘 service wall，只含 GPU。

## 5. 结果解释

### 事实

1. bounded C128 稳定位于最小饱和区：running mean 约 230、waiting mean 2.8、KV mean 0.598，三次吞吐 CV 0.28%。
2. Daft Native/Ray 的吞吐仅比 bounded 低 2.89%/5.92%，但两臂都将大量请求提前送入 vLLM：waiting mean 783/742、observed max 1,763/1,773，KV observed max≈1，service queue mean≈37.5 s。
3. Ray Data HTTP 呈相反状态：running mean 17.3、waiting=0、KV mean 0.059、MFU 0.112，service tok/s 仅为 bounded 的 19.95%；即使 GPU utilization mean 为 94.3%，也没有达到 work/MFU 饱和。
4. 三种状态在三次 formal 中都稳定，吞吐与 JCT CV 均小于 0.7%。

### 推断

- 现有原生 graph 可以完成相同任务，但默认/公开校准点会把同一服务置于明显不同的压力区：Daft 两臂偏向过量提前提交，Ray Data 当前路径偏向供给不足。单一静态并发或 GPU utilization 无法统一描述这些状态。
- 上游控制至少需要同时观测完成速率/MFU、running、waiting、KV 与 tail，并以最小饱和区为目标；这为状态感知和 bounded dynamic admission 提供了直接动机。
- 高 waiting 并不等同于吞吐灾难：Daft Native 仍距 bounded 仅 2.89%。因此动态控制的目标不是简单把 waiting 清零，而是减少无收益排队、保护 tail/多 job，同时保持 work-conserving feeding。

### 不能声称

- 不能声称项目方法已经优于 Daft/Ray Data；本实验没有 project dynamic arm。
- 不能把 Ray Data 结果归因给“Ray scheduler 很慢”，也不能把 Daft 高 waiting 归因给某个未观测的内部算法；这里只报告当前官方 API graph 与冻结合同的外部现象。
- 不能直接用本实验证明多 job idle borrowing 或公平调度有效；需要下一组错峰两 job 观察和项目 static/shared 同上限 A/B。
- 不能把 C128、C8/B16 或 Daft vendor default 外推到其他 workload、机器或模型。

## 6. 对四条开题证据链的含义

- **Work Unit / 数据组织**：同为 2,048 行并不产生同等 active work；请求和 token work 必须进入中性描述，row 只保留 correctness 身份。
- **状态感知**：running/waiting/KV、MFU/完成速率和 tail 能区分“GPU utilization 都很高”背后的欠供给、最小饱和与过量排队。
- **动态调度**：有界控制需要在 Ray Data 式欠供给时补充 work、在 Daft 式过量排队时收紧 credit，并以同上限 frozen static 做可证伪 A/B；本实验只证明问题，不证明 proposed 已解决。
- **算子代价估计**：admission 应消费 predicted/remaining work 与 uncertainty，而不是并发数；但代价模型本身的选择质量仍由独立 429-formal context-LOO 结果支撑。

## 7. 后续实验与停止规则

1. 不重跑或扩大本单 job 矩阵，不扫 Daft 隐含参数，不为了追求排名更换 workload。
2. 运行 short/long 两 job 错峰原生观察，确认独立 job 竞争时上述压力是否转化为 job-level JCT/隔离问题。
3. 运行项目 `static_partition` vs `shared_work_credit` 同上限 A/B，只回答 idle borrowing、work conservation 和每 job 指标；阴性结果也停止。

## 8. 后续图清单（本轮不绘制）

本结果只需要一张双面板图：左侧画四臂 JCT/service tok/s（误差线为三次 formal SD）；右侧画 running、waiting、KV、MFU 的标准化状态指纹，直接标注 underfeed / minimum-saturation / overqueue。不要只画吞吐柱状图，也不要把 12 个重复点无解释地堆在主图上。

## 9. 原始证据与服务器保留

- 原始目录：`/root/autodl-tmp/experiment-artifacts/opening_text_native_single_job_formal_20260808_saturated_c128_v2/`（38 MB）
- 独立归档：`/root/autodl-tmp/experiment-artifacts/archives/opening_text_native_single_job_formal_20260808_saturated_c128_v2.tar.gz`（12 MB）
- 归档 SHA256：`d2d59e7428df3254ef877e32f89cca3dff45dd5be13dbf67c8c2bc7f006b7da7`
- 低 nofile 失败、错误标签中止和早期 underfed C32 正式片段均保存在各自独立目录与 `opening_text_native_partial_incidents_20260808.tar.gz`，不覆盖、不混入本结果。
