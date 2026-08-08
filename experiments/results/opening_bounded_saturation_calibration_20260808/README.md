# ShareGPT bounded HTTP 饱和校准

日期：2026-08-08
状态：**通过；正式对照冻结为每 endpoint 并发 128**

## 1. 实验目的

在运行 Daft Native、Daft Ray、Ray Data 与 bounded HTTP 的同环境正式矩阵前，先回答 bounded client 是否真正喂饱当前双 vLLM 服务。该校准也复核此前 database-E2E ShareGPT 对照中每 endpoint 并发 32 的 serving regime。

## 2. 实验设置

| 项目 | 合同 |
|---|---|
| GPU / 服务 | 2× RTX 4090；2 个 Qwen2.5-7B vLLM endpoint；prefix cache ON |
| vLLM | `max_num_seqs=256`、`max_num_batched_tokens=8192`、`max_model_len=8192`、`gpu_memory_utilization=0.90` |
| workload | ShareGPT controlled-skew，2,048 行，output cap 256；manifest SHA256 `54c97a2f…3169b` |
| 扫描变量 | bounded HTTP 每 endpoint 并发 32、64、128、256；batch=1，其余完全一致 |
| 重复 | 每点 1 warm-up + 1 formal，交错顺序；所有 formal 时长 ≥60 s |
| 选择规则 | 以 formal C256 为已测峰值，选择达到其 97% 的最小并发点 |

本实验是容量校准，不承担框架性能排名，也不把已测峰值解释为硬件理论峰值。

## 3. 合规性自检

- 8/8 cells 通过，4/4 formal 可排名；每 cell 2,048/2,048 行、0 failure、exactly-once。
- 同一 immutable manifest、同一两 endpoint、同协议与输出合同。
- formal C128/C256 的双卡平均 GPU utilization 为 97.79%/97.85%；MFU 为 0.571/0.686。
- C128 达 formal C256 吞吐的 98.22%，满足预注册 97% 最小饱和选择规则。
- C256 的 waiting mean/max=116.8/369，KV mean/max=0.846/0.9996；继续增压已经形成明显排队和 KV 顶格。

## 4. 实验数据

### 4.1 Formal headline

| 每 endpoint 并发 | service tok/s | 相对 C256 | wall (s) | MFU | running mean/max | waiting mean/max | KV mean/max | TTFT mean (s) | queue mean (s) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 32 | 9,454.88 | 52.07% | 179.77 | 0.303 | 61.3/64 | 0.0/1 | 0.163/0.240 | 0.153 | 0.0003 |
| 64 | 14,057.93 | 77.42% | 120.95 | 0.450 | 120.2/128 | 0.3/34 | 0.317/0.433 | 0.318 | 0.0085 |
| **128** | **17,834.14** | **98.22%** | **95.31** | **0.571** | **231.8/256** | **3.7/136** | **0.601/0.803** | **0.829** | **0.1323** |
| 256 | 18,158.19 | 100.00% | 93.64 | 0.686 | 339.3/512 | 116.8/369 | 0.846/1.000 | 6.181 | 4.2459 |

C32→C64→C128 的吞吐分别增加 48.7% 和 26.9%；C128→C256 仅再增加 1.82%，但 TTFT mean 从 0.829 s 增至 6.181 s，queue mean 从 0.132 s 增至 4.246 s。

完整 8-cell 数据见 [`runs_summary.csv`](runs_summary.csv)。MFU 按两 endpoint `estimated_flops_per_gpu_delta / (wall × 2 × 165 TFLOPS)` 计算；能耗为两卡 mean power × service wall，仅含 GPU。

## 5. 结果解释

### 事实

1. C32 的 GPU utilization 虽接近 98%，但 service throughput 仅为已测峰值的 52.07%，running 仅约 61；高 GPU utilization 单独不能证明 vLLM 已被喂饱。
2. C128 是达到已测峰值 97% 的最小并发点，且没有 C256 的持续 waiting/KV 顶格现象，因此冻结为正式 bounded 对照。
3. C256 提高 MFU，但只换来 1.82% 吞吐增量，并把队列和 TTFT 大幅推高；它是过量提交压力点，不是后续公平比较的默认点。

### 纠正此前口径

此前 `opening_database_e2e_text_refeed_20260808/` 的 ShareGPT direct/DuckDB 均使用 C32。该点现在被证实为**未饱和协议控制**，因此 project/direct=1.5457 不能再作为“喂饱后的项目方法收益”或强静态性能比较；它主要反映并发/执行结构差异。原始数据仍有效地支持 exactly-once、database-E2E、产品 cap 语义和可观测性结论。

### 不能声称

- 不能称 C128 是跨 workload、模型或机器的通用最优并发。
- 不能用单次 formal 做统计显著性结论；该扫描只用于冻结 1+3 正式矩阵的容量点。
- 不能只凭 GPU utilization 判断 feeding；必须联合 service throughput frontier、running/waiting、KV、MFU 和尾延迟。

## 6. 对课题的含义

- **状态感知**：同一服务在 C32/C128/C256 下呈现欠供给、平台和过量排队三种状态；至少需要观测 running、waiting、KV、MFU/完成速率，而不是只看静态并发或 GPU utilization。
- **动态提交**：控制目标应是在 workload 变化时维持最小饱和区，而非持续把队列推向最大；动态方法仍必须通过与 C128 同上限的正式 A/B 验证。
- **Work Unit 与代价估计**：并发数不是 work；相同 2,048 行下的 active token work 与完成速率才是容量选择和 remaining-work 估计的输入。

## 7. 后续图清单（本轮不绘制）

只保留一张双面板候选图：左侧为并发—service tok/s/MFU，标出 C128 97% 最小饱和点；右侧为并发—waiting/KV/TTFT，显示 C256 的过量排队。正式原生矩阵完成后再决定它作为主图还是状态感知动机图的子面板。

## 8. 原始证据与服务器保留

- 原始目录：`/root/autodl-tmp/experiment-artifacts/opening_bounded_saturation_calibration_20260808/`
- 独立归档：`/root/autodl-tmp/experiment-artifacts/archives/opening_bounded_saturation_calibration_20260808.tar.gz`
- 归档 SHA256：`d416c71762515172e177d6483f6156645d949c25a40fbfe117e89b3fc86139d0`
- matrix index SHA256：`8f065a1f033ab1e09b7b0d01a87f871631ff67744305a96f3e1f7649cf0b8e4c`

服务器同时保留 capability scan、Ray Data C4/C8/C16 扫描、低 nofile 失败诊断和被中止的错误标签运行；它们均以状态区分，不删除、不混入正式结论。
