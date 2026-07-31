# Image CLIP AI_EMBED 分阶段瓶颈画像 + go/no-go 门禁, 2026-08-01

## Question

image AI_EMBED (CLIP) 锁定为首个 workload 后、建 runner 前，先回答一个 fatal-flaw 门禁问题（`image_clip_workload_lock_20260731.md` §6）：

> 在我们的设置下，CPU 侧数据准备相对 GPU CLIP forward 有多重？`ratio = cpu_total_per_img / gpu_embed_per_img` 是否 > 0.3——即异构调度（CPU 准备 vs GPU 推理）是否有真实舞台？

判据：> 0.3 → **GO**（建 path-B runner）；< 0.1 → NO-GO；中间 → BORDERLINE。

> ⚠️ **首跑规模**：1024 张 COCO val2017 × 50 iters/batch（~75s，「试试」量级）。`§6` 规范为 **5K val + 更长测量**；**5K COCO val + 加长 redo（`--limit 5000 --iters 200`，~5min）待跑**，拿 publishable 级稳定数字。当前结论（GO、CPU preprocess 主导）稳健，redo 预期不翻转。

这是文本侧 `ai_embed_chain_breakdown_20260712` 画像的**图像模态对应**：把 "PostgreSQL bytea → 512-d embedding" 链路拆成分阶段 per-img 成本，定位瓶颈在哪段。**单进程**（隔离每段成本，不引 Ray/Daft/scheduler 噪声），**走 PG**（读 `image_documents.image` bytea，DB 在路径里）。

## Setup

| Item | Value |
|---|---|
| Database | PostgreSQL 18.4 (Ubuntu 18.4-1.pgdg22.04+1) local rehearsal |
| pgvector | 0.8.5 |
| Model | `openai/clip-vit-base-patch32`（本地，transformers 5.14.1） |
| Embedding dim | 512（`.pooler_output`） |
| GPU | AutoDL 2×4090（本实验单卡 `cuda`） |
| Dataset | COCO val2017，1024 张，bytea 存 PG `image_documents.image` |
| torch | 2.11.0+cu130 |
| 运行方式 | 单进程；warmup 3 + 50 measured batches / batch-size；batch-size ∈ {1,16,32,64,128} |
| 脚本 | `code/scripts/profile_image_clip_bottleneck.py` |

数据流（分阶段计时）：

```text
PostgreSQL image_documents.image (bytea)
  -> pg_read        bulk-fetch bytea（一次查 1024 行，摊到 /img）
  -> pil_decode     JPEG decode + convert RGB               [CPU]
  -> cpu_preprocess CLIPProcessor resize(224) + normalize   [CPU]
  -> transfer       CPU tensor -> GPU (.to(cuda))           [H2D]
  -> gpu_embed      CLIP forward, .pooler_output            [GPU]
```

GPU 阶段用 `torch.cuda.synchronize()` 包住，wall clock 反映真实设备工作而非异步流返回。

## 实验数据（per-image p50, ms；全量 CSV 见同名 .csv）

| B | pg_read | pil_decode | cpu_preprocess | transfer | gpu_embed | cpu_total | **ratio** |
|---|---|---|---|---|---|---|---|
| 1 | 0.831 | 0.096 | 5.593 | 0.186 | 4.684 | 5.690 | **1.215** |
| 16 | 0.831 | 0.046 | 5.369 | 0.071 | 0.411 | 5.415 | **13.175** |
| 32 | 0.831 | 0.042 | 5.269 | 0.074 | 0.318 | 5.312 | **16.716** |
| 64 | 0.831 | 0.042 | 5.221 | 0.128 | 0.307 | 5.263 | **17.165** |
| 128 | 0.831 | 0.042 | 5.120 | 0.120 | 0.296 | 5.162 | **17.422** |

`ratio = (pil_decode + cpu_preprocess) / gpu_embed`（CPU 总准备 / GPU embed）。

## 结果解释

**事实**：
- **VERDICT: GO**。实用 batch（≥16）ratio 13–17，远超 0.3 门禁。
- 瓶颈是 **CLIPProcessor 的 resize+normalize（`cpu_preprocess` ~5.1–5.6 ms/img）**——它随 batch 基本不下降（per-image CPU 工作），而 `gpu_embed` 随 batch 摊销急降（4.68 → 0.30 ms/img）。
- **不是** JPEG decode（0.04–0.10 ms/img，可忽略）、**不是** CPU→GPU transfer（0.07–0.19 ms/img）、**不是** PG-read（0.83 ms/img bulk 摊销）。
- B=128 时单 batch：`cpu_preprocess` 5.12×128 ≈ 655 ms vs `gpu_embed` 0.30×128 ≈ 38 ms。若在同一 worker 串行（先 decode+preprocess 再 embed），**GPU 忙约 5.5%、空转约 94.5%**。

**推断**：
- "数据搬运瓶颈"更精确是 **CPU 预处理（resize+normalize）瓶颈**——仍是 CPU-vs-GPU 异构性的体现，但根因在 CLIPProcessor 的 CPU 变换，不在 JPEG decode 或 H2D 搬运。
- 这正好印证 path-B 架构（`image_clip_workload_lock_20260731.md` §2）：把 CPU decode+preprocess 放上游 Ray worker、CLIP 跑独立 endpoint，让 **CPU 预处理与 GPU embed overlap**——对照 Daft `@daft.cls` Native（选项 A）在同一 GPU worker 串行做 preprocess+embed、GPU 在 preprocess 期间空转。**本 profile 量化了为什么 path-B 必须分离 CPU 预处理**。
- `gpu_embed` 随 batch 摊销（B=1 4.68 ms → B=128 0.30 ms）证实 CLIP 无 KV / 无生成、纯 forward 高度可批——frame-budget 越大 GPU 越划算；但 CPU preprocess 不随之下降，故 batch 越大 CPU/GPU 失衡越严重（ratio 1.2 → 17.4）。

**不能声称**：
- 这是**单进程、单卡、单模型（CLIP ViT-B/32）画像**，没有调度、没有 overlap、没有写回——不能声称任何策略收益，只回答"瓶颈在哪、有多重"。
- `cpu_preprocess` ~5 ms/img 是 CLIPProcessor 当前实现的实测值（resize BICUBIC + to_tensor + normalize），**版本/实现相关**（transformers 5.14.1）。换更快的预处理（GPU-side resize、torchvision JIT 等）会改变绝对值；但"CPU 预处理主导"的结构在 CPU 预处理路径下成立。
- 仅 COCO val2017 单数据集；不同图像尺寸分布会改变 decode/preprocess 绝对值。

## 对课题含义

- **§6 go/no-go 门禁通过（GO）**——image AI_EMBED (CLIP) 作为首个 workload 成立：CPU 数据准备是真实、重、可量化的瓶颈（~5 ms/img，是 GPU embed 的 13–17×），异构调度（CPU 预处理 vs GPU embed 的 overlap）有明确舞台。
- **path-B 架构决策获得量化动机**：CPU preprocess 必须与 GPU embed 分离并 overlap，否则 GPU 空转 ~94%。这把 `image_clip_workload_lock` §2 的 A vs B 决定从"设计选择"升级为"有动机数据支撑"。
- **A（状态感知请求成形/提交）+ B（代价估计）方向**在此 workload 上有实际可优化空间：观测 CLIP endpoint 状态、按 frame-budget + CPU-prep 节奏组织提交，目标正是压低这 ~94% GPU 空转。

## 下一步

1. 解除 `image_clip_workload_lock_20260731.md` §0 的 "build 暂停"（§6 门禁已过，方向已锁 A+B）。
2. 建 path-B runner：PG → Daft → Ray CPU decode+preprocess → CLIP endpoint → pgvector，复用本脚本的 `load_clip()` / `pil_decode()` / `cpu_preprocess()` / `clip_encode()`（代码已按 code/AGENTS.md 代码质量总则写成可复用 stage 函数）。
3. `image_clip_workload_lock` §7 对照臂：bounded direct CLIP / **Daft `@daft.cls` Native（A，关键强 baseline）** / Ray Data / naive / **ours（B + A 状态感知调度）**——claim 门槛：ours 相对 Daft Native 的 images/s 或 SLO-goodput **>+5% 且 SLO 违约 <1%** 才晋级。

## 原始数据

```text
motivation/results/gpu/image_clip_bottleneck_profile_20260801.csv
```

脚本：`code/scripts/profile_image_clip_bottleneck.py`（复现：`source ai-operator-runtime.env && python code/scripts/profile_image_clip_bottleneck.py --pg-dsn "$DATABASE_URL"`）。
