# Image CLIP R0-R2 transfer/compute ceiling profile, 2026-08-03

> 性质：R0→R4 木桶效应筛查的 **R0-R2 层**（GPU compute ceiling / pinned H2D / pageable host-copy）。**Synthetic 预生成 tensor、单卡、诊断性质**——不含 DB read / JPEG decode / Ray 调度 / fan-in（那是 R3/R4）。bandwidth 口径是 logical bytes / CUDA event，**非硬件 PCIe counter**。

## 1. 实验设置

- 服务器：AutoDL 2×RTX 4090，本实验单卡 `CUDA_VISIBLE_DEVICES=0`。
- 模型：CLIP ViT-B/32，float16 forward。
- 代码：commit `29b256b`；脚本 `code/scripts/profiling/profile_clip_transfer_ceiling.py`。
- 矩阵：batch {16, 64, 256} × mode {R0, R1, R2} × 5 warmup + 30 formal repeats = **270 行**（`raw.csv`）。
- GPU：active-device util mean 41% / **peak 99%**、power 191W mean / 244W peak、PCIe gen4×16（拓扑 GPU0/1 跨 NUMA SYS）。

## 2. 实验设计（R0→R2 表示阶梯，逐层加传输成本）

- **R0 `gpu_resident`**：GPU-resident tensor → forward。GPU/模型计算上限。
- **R1 `pinned_fp16`**：pinned FP16 host tensor → nonblocking H2D → forward。PCIe H2D 成本。
- **R2 `pageable_fp32`**：pageable FP32 → ownership copy + FP32→FP16 dtype 转换 → H2D → forward。Ray object-store / host-copy / dtype 边际成本。

> R3（内存 JPEG → decode/preprocess → GPU）≈ `image_clip_bottleneck_profile_20260801`（CPU preprocess ~5ms/img）；R4（PG → Daft/Ray → GPU operator E2E）≈ `image_project_static_60k_x2_20260803` 矩阵。本报告只覆盖 R0-R2。

## 3. 严谨性自检

- 计时：CUDA events + synchronized wall；model setup 排除。
- 30 formal repeats，报 p50（不用单次峰值）。
- bandwidth 语义：`logical_host_bytes_over_cuda_event_not_hardware_counter`（诚实标注，非硬件 PCIe counter）。
- GPU active-device util **peak 99%** 证明 R0 真打满 GPU compute（对比项目矩阵的 0–19% = 之前是 CPU-prep-bound，不是 GPU 慢）。
- 数值等价：三 mode 的 `output_sum` 全行 = -24.953、`output_norm_error=0` → R0/R1/R2 产出相同 embedding（差异纯在传输路径，不在模型输出）。

## 4. 实验数据（per mode × batch，p50）

| mode | batch | wall ms | H2D ms | forward ms | host-copy ms | img/s | logical H2D GB/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| R0 gpu_resident | 16 | 3.33 | — | 3.30 | — | 4805 | — |
| R0 | 64 | 6.51 | — | 6.48 | — | **9824** | — |
| R0 | 256 | 26.64 | — | 26.60 | — | 9611 | — |
| R1 pinned_fp16 | 16 | 3.58 | 0.21 | 3.31 | — | 4474 | 22.6 |
| R1 | 64 | 7.34 | 0.80 | 6.48 | — | 8720 | 24.1 |
| R1 | 256 | 29.75 | 3.11 | 26.57 | — | 8605 | 24.8 |
| R2 pageable_fp32 | 16 | 5.07 | 1.09 | 3.39 | 0.51 | 3157 | 8.8 |
| R2 | 64 | 32.71 | 4.19 | 6.48 | **21.97** | 1957 | 9.2 |
| R2 | 256 | 131.13 | 16.48 | 26.57 | **87.89** | 1952 | 9.4 |

## 5. 结果解释

**事实**：
1. **R0（GPU compute）天花板 ~9.6–9.8K img/s**——batch 64 已饱和（9824），256 不再升（9611），util peak 99%。**GPU forward 本身不是瓶颈**。
2. **R1（pinned H2D）~8.6–8.7K img/s**，比 R0 低 ~10%。pinned H2D 仅 0.2–3 ms、logical ~24 GB/s（近 PCIe gen4×16 实际带宽）。**PCIe/H2D 路径不是 binding 瓶颈**（R0→R1 降幅约 10%，H2D 占 wall 比例小）。
3. **R2（pageable FP32 ownership-copy + dtype 转换 + H2D）崩到 ~2K img/s**（batch 64/256）。**host ownership-copy（pageable→pinned + FP32→FP16）在 batch 64 占 22 ms、batch 256 占 88 ms，远超 forward（6.5/26.6 ms）和 H2D（4.2/16.5 ms）**。R2 比 R0 慢约 **5×**。

**木桶判决（R0→R2）**：binding 顺序为 **host ownership-copy + dtype 转换（R2）≫ GPU compute（R0）≈ pinned H2D（R1）**。即"数据搬运"瓶颈具体是 **pageable FP32 的 ownership copy + FP32→FP16 dtype 转换**，**不是 PCIe H2D 本身、也不是 GPU forward**。

**推断**：项目 pipeline（pageable numpy / Ray object → FP16 GPU）的 host-side copy + dtype 转换是当前主要传输侧成本。优化方向（属工程优化，非策略）：pinned FP16 直接（跳过 dtype 转换）、或 GPU-side decode/preprocess（避开 host copy）。但 R0-R2 是隔离诊断；真实全链路还含 JPEG decode/preprocess（R3 ~5ms/img）+ Ray 调度/fan-in（R4），R3/R4 才定全链路 binding。

**不能声称**：
- bandwidth 是 logical（cuda event），**非硬件 PCIe counter**；GB/s 是推导值。
- R0-R2 用 synthetic 预生成 tensor，**不含 DB read / JPEG decode / Ray 调度 / fan-in**——不能单独声称全链路瓶颈，只定位 compute/H2D/host-copy 三层。
- 不能据此声称任何项目策略收益；这是动机/归因诊断。
- nvidia-smi util mean 41% 含 R2 host-copy 等待的拉低，不能当 MFU；peak 99% 只证 R0 能打满。

## 6. 对课题含义

- R0-R2 把"数据搬运瓶颈"**细化**了：不是 PCIe（R1 pinned 没问题，~24 GB/s）、不是 GPU compute（R0 ~9.7K 够），**是 pageable FP32 的 ownership-copy + FP32→FP16 dtype 转换（R2 host-copy）**。比之前"CPU preprocess ~5ms 主导"更精确——host-copy + dtype 是传输侧的具体大头。
- 与 R3（CPU decode/preprocess）+ R4（operator E2E）互补：R2 host-copy + R3 preprocess 共同构成 CPU/主机侧成本，R4（60K×2 矩阵）验证全链路。
- 优化暗示：pinned FP16 / GPU-side preprocess 能砍 R2 host-copy；属工程优化。项目策略（调度/组织/状态感知）仍须在 R4 真实 pipeline 上证收益。

## 7. 下一步

1. **R3/R4 拼全链路**：R3 ≈ 已有 `image_clip_bottleneck_profile_20260801`（CPU preprocess ~5ms/img）；R4 ≈ step-1 `image_project_static_60k_x2` 矩阵（operator E2E）。把 R0-R2 + R3 + R4 合成完整木桶判决（哪段 binding 全链路）。
2. 按 plan step 4-5：原生 baseline（Daft built-in / Ray Data）独立校准 + formal 排名（统一 L2-normalized contract，归一化计入各臂 E2E）。
3. system-E2E（加 pgvector 写回）补完整数据库作业时间。

## 原始数据

- `raw.csv`（270 行 R0-R2 数据）
- 远端 manifest：`/root/autodl-tmp/experiment-artifacts/image_clip_transfer_ceiling_20260803_115013/manifest.json`
- 复现：见 `deploy/autodl/image_serving.md` §5.5
