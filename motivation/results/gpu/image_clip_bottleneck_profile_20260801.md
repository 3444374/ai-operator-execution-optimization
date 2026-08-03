# Image CLIP AI_EMBED 分阶段瓶颈画像 + go/no-go 门禁, 2026-08-01

## Question

image AI_EMBED (CLIP) 锁定为首个 workload 后、建 runner 前，先回答一个 fatal-flaw 门禁问题（`image_clip_workload_lock_20260731.md` §6）：

> 在我们的设置下，CPU 侧数据准备相对 GPU CLIP forward 有多重？`ratio = cpu_total_per_img / gpu_embed_per_img` 是否 > 0.3——即异构调度（CPU 准备 vs GPU 推理）是否有真实舞台？

判据：> 0.3 → **GO**（建 path-B runner）；< 0.1 → NO-GO；中间 → BORDERLINE。

> ✅ **本报告为 5K 稳定画像**（5000 张 COCO val2017 作为采样池，100
> iters/batch，约 5min）：p95 紧贴 p50，足以通过“是否继续建设 E2E runner”的
> 动机门禁；它不是端到端方法实验，不能单独作为论文性能结论。

## Setup

| Item | Value |
|---|---|
| Database | PostgreSQL 18.4 (Ubuntu 18.4-1.pgdg22.04+1) local rehearsal |
| pgvector | 0.8.5 |
| Model | `openai/clip-vit-base-patch32`（本地，transformers 5.14.1） |
| Embedding dim | 512（`.pooler_output`） |
| GPU | AutoDL 2×4090（本实验单卡 `cuda`） |
| Dataset | **COCO val2017，5000 张**（bytea 存 PG `image_documents.image`，doc_id 0..4999；载入 815 MB / 33.8 s via `code/scripts/data/import_coco_images.py`） |
| torch | 2.11.0+cu130 |
| 运行方式 | 单进程；warmup 5 + 100 measured batches / batch-size；batch-size ∈ {1,16,32,64,128,256} |
| 脚本 | `code/scripts/profiling/profile_image_clip_bottleneck.py` |

数据流（分阶段计时）：

```text
PostgreSQL image_documents.image (bytea)
  -> pg_read        bulk-fetch bytea（一次查 5000 行，摊到 /img）
  -> pil_decode     JPEG decode + convert RGB               [CPU]
  -> cpu_preprocess CLIPProcessor slow preprocessing path   [CPU]
  -> transfer       CPU tensor -> GPU (.to(cuda))           [H2D]
  -> gpu_embed      CLIP forward, .pooler_output            [GPU]
```

GPU 阶段用 `torch.cuda.synchronize()` 包住，wall clock 反映真实设备工作而非异步流返回。

## 实验数据（per-image p50, ms；全量 CSV 见同名 .csv）

| B | pg_read | pil_decode | cpu_preprocess | transfer | gpu_embed | cpu_total | **ratio** |
|---|---|---|---|---|---|---|---|
| 1 | 0.755 | 0.096 | 5.679 | 0.185 | 3.904 | 5.775 | **1.479** |
| 16 | 0.755 | 0.049 | 5.668 | 0.085 | 0.415 | 5.717 | **13.791** |
| 32 | 0.755 | 0.045 | 5.487 | 0.087 | 0.317 | 5.532 | **17.455** |
| 64 | 0.755 | 0.044 | 5.364 | 0.126 | 0.307 | 5.407 | **17.615** |
| 128 | 0.755 | 0.041 | 5.209 | 0.119 | 0.297 | 5.250 | **17.689** |
| 256 | 0.755 | 0.041 | 5.186 | 0.096 | 0.286 | 5.227 | **18.283** |

`ratio = (pil_decode + cpu_preprocess) / gpu_embed`。p95 紧贴 p50（如 B=256：preproc p50 5.19 / p95 5.50，差 ~6%），100 iters 下稳定。

## 结果解释

**事实**：
- **VERDICT: GO**。实用 batch（≥16）ratio 13.8–18.3，远超 0.3 门禁。
- 该配置下的瓶颈是 **CLIPProcessor slow preprocessing path**
  （`cpu_preprocess` ~5.2–5.7 ms/img）——随 batch 基本不下降，而
  `gpu_embed` 随 batch 摊销急降（3.90 → 0.29 ms/img）。子阶段补测只直接归因出
  resize ~1.3ms；其余大部分仍是未细分的 processor overhead。
- **不是** JPEG decode（0.04–0.10 ms/img）、**不是** CPU→GPU transfer（0.08–0.19 ms/img）、**不是** PG-read（0.755 ms/img bulk 摊销）。
- B=128 单 batch：`cpu_preprocess` 5.21×128 ≈ 667 ms vs `gpu_embed`
  0.297×128 ≈ 38 ms。若二者在同一 worker 严格串行且忽略其他开销，GPU forward
  时间只占这两段总时间约 5.3%；这是由阶段计时推导的**理论串行占比**，不是
  `nvidia-smi` 时间序列实测的“GPU 空转 94.7%”。
- ratio 随 batch **渐近**（13.8→17.5→17.6→17.7→18.3），B≥32 后基本饱和在 ~18：preproc 扁平、embed 在 B=256 已近地板（0.286 ms），再加大 batch ratio 不会显著上升。

**推断**：
- “数据搬运瓶颈”更精确是当前实现的 **CPU 预处理路径瓶颈**——仍是
  CPU-vs-GPU 异构性的体现，但当前证据不支持把全部 residual 归因到某个具体
  resize、转换或 tensor 操作。
- 该比例支持继续验证 path-B 架构（`image_clip_workload_lock_20260731.md` §2）：
  CPU decode+preprocess 放上游 Ray worker、CLIP 跑独立 actor，以尝试 overlap。
  但 profile 只证明“存在可重叠工作”，没有证明分离后一定提升 E2E；Ray 调度、
  object transfer、actor queue 和写回开销可能抵消收益，必须由端到端对照决定。
- `gpu_embed` 随 batch 摊销（B=1 3.90 ms → B=256 0.29 ms）证实 CLIP 无 KV / 无生成、纯 forward 高度可批；但 CPU preprocess 不随之下降，故 batch 越大 CPU/GPU 失衡越严重（ratio 1.5 → 18.3）。

**不能声称**：
- 这是**单进程、单卡、单模型（CLIP ViT-B/32）画像**，没有调度、没有 overlap、没有写回——不能声称任何策略收益，只回答"瓶颈在哪、有多重"。
- `cpu_preprocess` ~5 ms/img 是 transformers 5.14.1 的
  `CLIPProcessor(..., return_tensors="pt")` slow path 实测值，不能外推到
  `return_tensors="np"`、`CLIPImageProcessorFast`、GPU-side preprocess 或其他
  processor 版本。子步拆分只确认 resize ~1.3 ms；旧脚本得到的约 3.8 ms
  residual 是“未被四个 method wrapper 覆盖的时间”，不是对其内部组成的实测分解。
- pg_read 0.755 ms/img 是 **bulk 摊销**（一次 SELECT 5000 行）；真实流式/分批读会更大（path-B runner 范畴，本画像未测）。

## 对课题含义

- **§6 go/no-go 门禁通过（GO）**——image AI_EMBED (CLIP) 作为首个 workload 成立：CPU 数据准备是真实、重、可量化的瓶颈（~5 ms/img，是 GPU embed 的 14–18×），异构调度（CPU 预处理 vs GPU embed 的 overlap）有明确舞台。
- **path-B 获得了继续实现和验证的量化动机**：CPU preprocess 与 GPU embed
  存在明显阶段失衡；是否应分离、能否形成有效 overlap，仍由 E2E gate 判定。
- **A（状态感知请求成形/提交）+ B（代价估计）方向**在此 workload 上有实际可优化空间：观测 CLIP endpoint 状态、按 frame-budget + CPU-prep 节奏组织提交，目标正是压低这 ~95% GPU 空转。

## 下一步

1. 建 path-B runner：PG → Daft → Ray CPU decode+preprocess → CLIP actor →
   pgvector。生产路径复用 `code/src/image/` 的 typed contracts、preprocessor 和
   actor；不得从 profiling 脚本反向 import 实现。
2. `image_clip_workload_lock` §7 对照臂：bounded direct CLIP / fused Daft /
   **Daft-on-Ray staged** / **Ray Data staged** / naive / ours。当前 profile 只决定是否
   值得进入 E2E；架构 claim 需先赢独立校准的 staged baseline，策略 claim 再比较冻结
   最佳项目静态点，且至少改善约 5%、重复同向、质量/SLO 不退化。
3. （可选）若日后要测**流式/分批 pg_read** 的真实成本，在 path-B runner 里按 chunked SELECT 计时，本单进程画像不含该口径。

## 附：preproc 子阶段拆分（2026-08-01 补测）

把 `cpu_preprocess`（整 ~5.2 ms/img）按 CLIPImageProcessor 自身方法拆分——
包住 `resize`/`center_crop`/`rescale`/`normalize` 计时，外加
whole-`processor()` 总时间与未归因时间。下表是**旧版脚本**以“总时间 p50 −
各阶段 p50 之和”计算的近似值（5000 图采样池、100 iters；原始
`clip_preproc_stages_20260801.csv`）；合并后的脚本已改为逐 iteration 求差再计算
p50/p95，需在新代码路径复测后生成新 CSV，不能覆盖这份历史证据：

| B | resize | crop | rescale | normalize | 子步求和 | total preproc | **未归因（近似）** |
|---|---|---|---|---|---|---|---|
| 1 | 1.49 | 0.025 | 0 | 0.062 | 1.58 | 5.72 | **4.15** |
| 32 | 1.29 | 0.006 | 0 | 0.043 | 1.34 | 5.11 | **3.77** |
| 128 | 1.26 | 0.003 | 0 | 0.023 | 1.29 | 5.13 | **3.85** |

**能确定的修正**：曾凭印象估“BICUBIC resize 占大头（~3–4 ms）”，实测不对；
resize 约 1.3 ms/img（约总时间 25%）。四个 wrapper 没覆盖约 3.8 ms/img，但当前
instrumentation 没有继续分解它，所以不能把 PIL→NumPy、rescale、tensor stacking
或 Python 循环中的任一项写成主因。

**含义**：当前 slow processor 路径整体较重，值得把
`CLIPImageProcessorFast`、当前 `return_tensors="np"` 边界和 GPU-side preprocess
作为受控对照；不能在没有数据时声称它们“一定大幅降低”成本。正式实验也不能
故意保留较慢实现来制造优化空间：ours 与 baseline 必须冻结相同的模型、processor
语义和输出质量，slow/fast 只能作为显式实验因子。

上述实现边界复测现已完成：`image_clip_preprocess_variants_20260801/` 保存
production-np、legacy-pt、torchvision+PIL/tensor-decode 四臂 720 条 raw repeats。
tensor fast path 提升约 1.14–1.22×，但 CPU prepare 仍为 actor 的 13.8–31.2×；
embedding parity 通过。该结果保留 E2E build 动机，但仍不证明 path-B 方法收益。

## 原始数据

```text
motivation/results/gpu/image_clip_bottleneck_profile_20260801.csv
```

脚本：`code/scripts/profiling/profile_image_clip_bottleneck.py`（复现：`source ai-operator-runtime.env && python code/scripts/profiling/profile_image_clip_bottleneck.py --pg-dsn "$DATABASE_URL" --limit 5000 --iters 100 --batch-sizes 1,16,32,64,128,256`）。数据装载：`code/scripts/data/import_coco_images.py`。
