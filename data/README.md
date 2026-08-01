# Data

This directory is for local dataset payloads and derived workload tables.

Raw downloaded files live under `data/raw/` and are ignored by git. Keep only
metadata, download commands, and preprocessing scripts in the repository.

## Current Raw Files

| Dataset | Local file | Size | Use |
|---|---:|---:|---|
| ShareGPT Vicuna unfiltered | `data/raw/sharegpt_vicuna/ShareGPT_V3_unfiltered_cleaned_split.json` | 672,837,942 bytes | Real prompt text for `AI_COMPLETE` data-organization experiments |
| BurstGPT | `data/raw/burstgpt/BurstGPT_1.csv` | 52,283,111 bytes | Real LLM serving trace with timestamp and token fields for scheduling experiments |

## Sources (exact)

Raw files are gitignored — **every environment (local machine, server, cloud) must download them fresh**. Use these exact URLs:

| Dataset | Exact URL | Repo |
|---|---|---|
| ShareGPT | `https://huggingface.co/datasets/anon8231489123/ShareGPT_Vicuna_unfiltered/resolve/main/ShareGPT_V3_unfiltered_cleaned_split.json` | HF dataset `anon8231489123/ShareGPT_Vicuna_unfiltered` |
| BurstGPT | `https://github.com/HPMLL/BurstGPT/releases/download/v2.0/BurstGPT_1.csv` | GitHub release `HPMLL/BurstGPT` tag `v2.0`(asset,**不是**仓库 `data/` 树里的文件) |
| **COCO 2017 val（图像 smoke，首个 workload）** | `http://images.cocodataset.org/zips/val2017.zip` | COCO 官方（5000 图，~777 MB；图像 AI_EMBED 首选 workload 的 smoke 集） |
| COCO 2017 train（正式，按需） | `http://images.cocodataset.org/zips/train2017.zip` | COCO 官方（118k 图，~19 GB；正式规模，受磁盘约束需精选子集） |
| **CLIP ViT-B/32（首个 workload 的 embedding 模型）** | `https://huggingface.co/openai/clip-vit-base-patch32` | HF model `openai/clip-vit-base-patch32`（512d，~600 MB） |
| CLIP ViT-B/16（对照） | `https://huggingface.co/openai/clip-vit-base-patch16` | HF model `openai/clip-vit-base-patch16`（512d，~600 MB） |
| MS MARCO Passage（文本轻对照，降级） | `https://huggingface.co/datasets/microsoft/msmarco-passage` | HF dataset `microsoft/msmarco-passage`（~8.8M 段，~5.7 GB；BigVectorBench text 切片，仅作"文本下瓶颈不显现"的边界对照） |
| BGE-base-en-v1.5（MS MARCO 的 embedding 模型） | `https://huggingface.co/BAAI/bge-base-en-v1.5` | HF model `BAAI/bge-base-en-v1.5`（1024d，~440 MB） |
| BGE-large-en-v1.5（重 GPU 对照） | `https://huggingface.co/BAAI/bge-large-en-v1.5` | HF model `BAAI/bge-large-en-v1.5`（1024d，~1.3 GB） |

同仓库的其他文件(ShareGPT 的 `_no_imsorry` 变体;BurstGPT 的 `BurstGPT_2.csv` / `_3.csv` / `BurstGPT_without_fails_*.csv`)可下但**当前 workload 导入不需要**。

## Fetch on a fresh environment (server / cloud)

Raw 被 gitignore,所以换环境就要重下。AutoDL(或任何国内带宽服务器)上**必须**开学术加速 + 禁 Xet,否则 HF/modelscope/hf-mirror 全部 8 kB/s ~ 700 kB/s 或 stall(10 小时 ETA)。完整流程见 `deploy/autodl/README.md` §5 与 §7;最小命令:

```bash
source /etc/network_turbo >/dev/null 2>&1   # AutoDL 学术加速(github/hf);非 AutoDL 跳过
export HF_HUB_DISABLE_XET=1                  # 避免 cas-server.xethub.hf.co 401
cd <repo root>
mkdir -p data/raw/sharegpt_vicuna data/raw/burstgpt
wget -c --tries=10 --timeout=30 \
  "https://huggingface.co/datasets/anon8231489123/ShareGPT_Vicuna_unfiltered/resolve/main/ShareGPT_V3_unfiltered_cleaned_split.json" \
  -O data/raw/sharegpt_vicuna/ShareGPT_V3_unfiltered_cleaned_split.json
wget -c --tries=10 --timeout=30 \
  "https://github.com/HPMLL/BurstGPT/releases/download/v2.0/BurstGPT_1.csv" \
  -O data/raw/burstgpt/BurstGPT_1.csv
```

开 turbo 后:ShareGPT(~641 MB)~10 MB/s、BurstGPT(~50 MB)~7 MB/s,各一分钟左右。不开 turbo 基本下不动。

## Text workload (MS MARCO, 文本轻对照 — 降级)

**降级为"文本轻对照"**（2026-07-31 校正）。学长判据：数据搬运瓶颈要在 DB 读 + CPU→GPU 搬运段显现，需每行 payload 重。MS MARCO 仍是文本，token ID 紧凑（~1KB/行），搬运轻，**瓶颈不显现**——仅作"文本下不显现"的边界对照，不作首选（首选改图像 CLIP，见下节）。设计见 `experiments/plans/msmarco_embedding_workload_20260731.md`。

AutoDL 上 fetch（HF 需 turbo + 禁 Xet）：

```bash
source /etc/network_turbo >/dev/null 2>&1   # AutoDL 学术加速
export HF_HUB_DISABLE_XET=1
cd <repo root 或 autodl-tmp 数据目录>
# MS MARCO Passage（~5.7 GB，8.8M 段）
huggingface-cli download microsoft/msmarco-passage --repo-type dataset \
  --local-dir data/raw/msmarco_passage
# BGE-base-en-v1.5（默认 embedding，~440 MB）
huggingface-cli download BAAI/bge-base-en-v1.5 \
  --local-dir models/bge-base-en-v1.5
```

AutoDL 现空闲 18G（清缓存后），MS MARCO + BGE ≈ 6 GB 放得下。

## Image workload (CLIP, 首个 workload — 当务之急)

**首选 workload**（2026-07-31 校正回升）。学长判据要求每行 payload 重——图像每行 CPU→GPU 搬运 ~600KB（文本 ~600×）+ JPEG decode/resize 重，让 **DB 读 + CPU→GPU 数据搬运瓶颈显现**。设计 + go/no-go 门禁见 `experiments/plans/image_clip_workload_lock_20260731.md`；benchmark 三层（数据集 + ANN-benchmarks recall@10 + §7.5 自定吞吐协议）见 `research/daft_db_gpu_bridge_direction_scope_20260731.md` §10.1。

图像 AI_EMBED workload，用于异构资源调度（CPU decode vs GPU embed）+ 多模态泛化验证。锁定方案见 `experiments/plans/image_clip_workload_lock_20260731.md`。

AutoDL 上 fetch（COCO 不需要 turbo，HF 模型需要）：

```bash
source /etc/network_turbo >/dev/null 2>&1   # HF 模型需要；COCO 直连 images.cocodataset.org 不需要
export HF_HUB_DISABLE_XET=1
cd <repo root 或 autodl-tmp 数据目录>
# COCO 2017 val（smoke，~777 MB，5000 图）
mkdir -p data/raw/coco_val2017
wget -c --tries=10 --timeout=30 http://images.cocodataset.org/zips/val2017.zip \
  -O data/raw/coco_val2017/val2017.zip
# COCO 2017 train（正式 workload；运行前先用 df -h 核对空间）
mkdir -p data/raw/coco_train2017
wget -c --tries=10 --timeout=30 http://images.cocodataset.org/zips/train2017.zip \
  -O data/raw/coco_train2017/train2017.zip
# 可直接从 ZIP 向 PostgreSQL 流式导入，不同时保留解压副本。
python code/scripts/import_coco_images.py \
  --zip data/raw/coco_train2017/train2017.zip --limit 60000 \
  --pg-dsn "$DATABASE_URL" --workload coco_train2017_60k
# CLIP ViT-B/32（默认 embedding 模型，~600 MB）
python -c "from huggingface_hub import snapshot_download; snapshot_download('openai/clip-vit-base-patch32', cache_dir='models')" 2>/dev/null \
  || huggingface-cli download openai/clip-vit-base-patch32 \
       --local-dir models/clip-vit-base-patch32
```

COCO train 压缩包约 19 GB，导入 PostgreSQL 后还会再占一份 BYTEA/索引/WAL 空间。
不要依赖文档中的历史剩余容量；每次下载前用 `df -h /root/autodl-tmp` 实测。导入器
支持直接读取 ZIP，避免额外保留完整解压目录，但仍需为数据库与 WAL 留安全余量。

## Boundary

Do not use the earlier synthetic `documents` seed as the final comparison
workload. The comparable baseline and optimized runs should be generated from
the same normalized ShareGPT/BurstGPT workload table.

## Local Import

The current local PostgreSQL rehearsal database has a normalized
`sharegpt_multiturn` workload imported into `documents`:

```text
rows=2048
doc_id range: 300000..302047
prompt_tokens range: 3..1486
target_output_tokens range: 1..256
```

`prompt_tokens` are counted with the local Qwen2.5-1.5B-Instruct tokenizer
when `--tokenizer-path models\Qwen2.5-1.5B-Instruct` is passed to
`code/scripts/import_ai_complete_workload.py`.

Use `--source-workload-name sharegpt_multiturn` in
`code/scripts/postgres_ai_operator_profile.py` to read only this workload.

Legacy: the older `sharegpt_burstgpt` workload (previously rows=1024, doc_id
starting at 1000000, target_output_tokens up to 2048, filtered by
`prompt_tokens + completion_max_tokens <= 2048`) is retained in the database
for 0725-0728 experiment reproduction but is no longer the main workload.
