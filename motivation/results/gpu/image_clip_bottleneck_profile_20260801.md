# Image CLIP AI_EMBED 分阶段瓶颈画像 + go/no-go 门禁, 2026-08-01

## Question

image AI_EMBED (CLIP) 锁定为首个 workload 后、建 runner 前，先回答一个 fatal-flaw 门禁问题（`image_clip_workload_lock_20260731.md` §6）：

> 在我们的设置下，CPU 侧数据准备相对 GPU CLIP forward 有多重？`ratio = cpu_total_per_img / gpu_embed_per_img` 是否 > 0.3——即异构调度（CPU 准备 vs GPU 推理）是否有真实舞台？

判据：> 0.3 → **GO**（建 path-B runner）；< 0.1 → NO-GO；中间 → BORDERLINE。

> ✅ **本报告为 5K 规范跑**（5000 张 COCO val2017 × 100 iters/batch，~5min）——publishable 级稳定数字（p95 紧贴 p50），VERDICT: GO。

## Setup

| Item | Value |
|---|---|
| Database | PostgreSQL 18.4 (Ubuntu 18.4-1.pgdg22.04+1) local rehearsal |
| pgvector | 0.8.5 |
| Model | `openai/clip-vit-base-patch32`（本地，transformers 5.14.1） |
| Embedding dim | 512（`.pooler_output`） |
| GPU | AutoDL 2×4090（本实验单卡 `cuda`） |
| Dataset | **COCO val2017，5000 张**（bytea 存 PG `image_documents.image`，doc_id 0..4999；载入 815 MB / 33.8 s via `code/scripts/import_coco_images.py`） |
| torch | 2.11.0+cu130 |
| 运行方式 | 单进程；warmup 5 + 100 measured batches / batch-size；batch-size ∈ {1,16,32,64,128,256} |
| 脚本 | `code/scripts/profile_image_clip_bottleneck.py` |

数据流（分阶段计时）：

```text
PostgreSQL image_documents.image (bytea)
  -> pg_read        bulk-fetch bytea（一次查 5000 行，摊到 /img）
  -> pil_decode     JPEG decode + convert RGB               [CPU]
  -> cpu_preprocess CLIPProcessor resize(224) + normalize   [CPU]
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
- 瓶颈是 **CLIPProcessor resize+normalize（`cpu_preprocess` ~5.2–5.7 ms/img）**——随 batch 基本不下降（per-image CPU 工作），而 `gpu_embed` 随 batch 摊销急降（3.90 → 0.29 ms/img）。
- **不是** JPEG decode（0.04–0.10 ms/img）、**不是** CPU→GPU transfer（0.08–0.19 ms/img）、**不是** PG-read（0.755 ms/img bulk 摊销）。
- B=128 单 batch：`cpu_preprocess` 5.21×128 ≈ 667 ms vs `gpu_embed` 0.297×128 ≈ 38 ms。同一 worker 串行（先 preprocess 再 embed）时 **GPU 忙约 5.3%、空转约 94.7%**；B=256 空转约 94.8%（几乎不变——preproc 主导且固定）。
- ratio 随 batch **渐近**（13.8→17.5→17.6→17.7→18.3），B≥32 后基本饱和在 ~18：preproc 扁平、embed 在 B=256 已近地板（0.286 ms），再加大 batch ratio 不会显著上升。

**推断**：
- "数据搬运瓶颈"更精确是 **CPU 预处理（resize+normalize）计算瓶颈**——仍是 CPU-vs-GPU 异构性的体现，但根因在 CLIPProcessor 的 CPU 变换，不在 JPEG decode 或 H2D 搬运。
- 印证 path-B 架构（`image_clip_workload_lock_20260731.md` §2）：CPU decode+preprocess 放上游 Ray worker、CLIP 跑独立 endpoint，让 **CPU 预处理与 GPU embed overlap**——对照 Daft `@daft.cls` Native（选项 A）在同一 GPU worker 串行做 preprocess+embed、GPU 在 preprocess 期间空转。**本 profile 量化了为什么 path-B 必须分离 CPU 预处理**（否则 GPU 空转 ~95%）。
- `gpu_embed` 随 batch 摊销（B=1 3.90 ms → B=256 0.29 ms）证实 CLIP 无 KV / 无生成、纯 forward 高度可批；但 CPU preprocess 不随之下降，故 batch 越大 CPU/GPU 失衡越严重（ratio 1.5 → 18.3）。

**不能声称**：
- 这是**单进程、单卡、单模型（CLIP ViT-B/32）画像**，没有调度、没有 overlap、没有写回——不能声称任何策略收益，只回答"瓶颈在哪、有多重"。
- `cpu_preprocess` ~5 ms/img 是 CLIPProcessor 当前实现的实测值（resize BICUBIC + to_tensor + normalize），**版本/实现相关**（transformers 5.14.1）。换更快的预处理（GPU-side resize、torchvision JIT 等）会改变绝对值；但"CPU 预处理主导"的结构在 CPU 预处理路径下成立。子步拆分见文末「附：preproc 子阶段拆分」——resize 只 ~1.3 ms（~25%），residual（PIL→numpy→tensor 转换 + 逐图循环）~3.8 ms（~74%）才是大头。
- pg_read 0.755 ms/img 是 **bulk 摊销**（一次 SELECT 5000 行）；真实流式/分批读会更大（path-B runner 范畴，本画像未测）。

## 对课题含义

- **§6 go/no-go 门禁通过（GO）**——image AI_EMBED (CLIP) 作为首个 workload 成立：CPU 数据准备是真实、重、可量化的瓶颈（~5 ms/img，是 GPU embed 的 14–18×），异构调度（CPU 预处理 vs GPU embed 的 overlap）有明确舞台。
- **path-B 架构决策获得量化动机**：CPU preprocess 必须与 GPU embed 分离并 overlap，否则 GPU 空转 ~95%。这把 `image_clip_workload_lock` §2 的 A vs B 决定从"设计选择"升级为"有动机数据支撑"。
- **A（状态感知请求成形/提交）+ B（代价估计）方向**在此 workload 上有实际可优化空间：观测 CLIP endpoint 状态、按 frame-budget + CPU-prep 节奏组织提交，目标正是压低这 ~95% GPU 空转。

## 下一步

1. 建 path-B runner：PG → Daft → Ray CPU decode+preprocess → CLIP endpoint → pgvector，复用本脚本的 `load_clip()` / `pil_decode()` / `cpu_preprocess()` / `clip_encode()`（已按 code/AGENTS.md 代码质量总则写成可复用 stage 函数）。
2. `image_clip_workload_lock` §7 对照臂：bounded direct CLIP / **Daft `@daft.cls` Native（A，关键强 baseline）** / Ray Data / naive / **ours（B + A 状态感知调度）**——claim 门槛：ours 相对 Daft Native 的 images/s 或 SLO-goodput **>+5% 且 SLO 违约 <1%** 才晋级。
3. （可选）若日后要测**流式/分批 pg_read** 的真实成本，在 path-B runner 里按 chunked SELECT 计时，本单进程画像不含该口径。

## 附：preproc 子阶段拆分（2026-08-01 补测）

把 `cpu_preprocess`（整 ~5.2 ms/img）按 CLIPImageProcessor 自身方法拆分——包住 `resize`/`center_crop`/`rescale`/`normalize` 计时，外加 whole-`processor()` 总时间与 residual（5K × 100 iters，脚本 `code/scripts/profile_clip_preproc_stages.py`，原始 `clip_preproc_stages_20260801.csv`）：

| B | resize | crop | rescale | normalize | 子步求和 | total preproc | **residual** |
|---|---|---|---|---|---|---|---|
| 1 | 1.49 | 0.025 | 0 | 0.062 | 1.58 | 5.72 | **4.15** |
| 32 | 1.29 | 0.006 | 0 | 0.043 | 1.34 | 5.11 | **3.77** |
| 128 | 1.26 | 0.003 | 0 | 0.023 | 1.29 | 5.13 | **3.85** |

**修正一处估算**：曾凭印象估"BICUBIC resize 占大头（~3-4 ms）"——实测不对。resize 只 ~1.3 ms/img（~25%）；crop/rescale/normalize 可忽略（rescale 在 transformers 5.14.1 折叠进 normalize、不单独触发）。**真正占大头的是 residual ~3.8 ms/img（~74%）** = CLIPImageProcessor「slow」路径里未被命名方法覆盖的部分：PIL→numpy 转换、被折叠的 rescale、numpy→tensor + 批堆叠、逐图 Python 循环开销。

**含义**：preproc 瓶颈不是"resize 算法重"，而是 **slow CLIPImageProcessor 逐图 CPU 转换路径整体重**——这反而更强地支撑"用 `CLIPImageProcessorFast`（torchvision 后端）/ GPU-side preprocess 能大幅压低 preproc"（大头是转换开销，不是 resize 本身）。本项目故意保留 CPU slow 路径以制造异构舞台。子步细节**不改变 headline**（CPU preproc 5 ms >> GPU embed 0.3 ms；ratio ~18；GPU 空转 ~95%），只把"5 ms 花在哪"讲清楚。residual 还可进一步拆（np.array / torch.from_numpy / stack / 循环），需更深桩，按需补。

## 原始数据

```text
motivation/results/gpu/image_clip_bottleneck_profile_20260801.csv
```

脚本：`code/scripts/profile_image_clip_bottleneck.py`（复现：`source ai-operator-runtime.env && python code/scripts/profile_image_clip_bottleneck.py --pg-dsn "$DATABASE_URL" --limit 5000 --iters 100 --batch-sizes 1,16,32,64,128,256`）。数据装载：`code/scripts/import_coco_images.py`。
