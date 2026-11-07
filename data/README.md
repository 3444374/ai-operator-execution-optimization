# Data

This directory is for local dataset payloads and derived workload tables.

Raw downloaded files live under `data/raw/` and are ignored by git. Keep only
metadata, download commands, and preprocessing scripts in the repository.

## Expected local assets

The paths below are the canonical local destinations, not a claim that every checkout currently
contains the files. Verify them with `manage_environment.py check` before importing or running an
experiment.

| Dataset | Local file | Size | Use |
|---|---:|---:|---|
| ShareGPT Vicuna unfiltered | `data/raw/sharegpt_vicuna/ShareGPT_V3_unfiltered_cleaned_split.json` | 672,837,942 bytes | Real prompt text for `AI_COMPLETE` data-organization experiments |
| BurstGPT | `data/raw/burstgpt/BurstGPT_1.csv` | 52,283,111 bytes | Real LLM serving trace with timestamp and token fields for scheduling experiments |

## Sources (exact)

SQuAD v1.1 dev 的来源为 [官方文件](https://rajpurkar.github.io/SQuAD-explorer/dataset/dev-v1.1.json)，
共 10,570 个问题、4,854,279 bytes，SHA256 为
`95aa6a52d5d6a735563366753ca50492a658031da74f301ac5238b03966972c9`。
[历史导入器](../code/scripts/data/import_squad_workload.py)保留原问答标识、多参考答案、提示和内容摘要，仍服务旧工作负载导入与纯数据核对。
普通记录单查询不通过该脚本取数；先前登记的[PostgreSQL Map脚本](../experiments/reproduction/native_baselines/historical_sources/7bf1d92b/code/scripts/baselines/squad_pg_map_pilot.py)
仅保留为准确历史来源，不列作当前入口。完整文件和派生输入留在机器自己的仓库外数据目录，
单台机器已有文件不能证明其他机器已下载，不同机器需分别按来源、摘要与行数核对。

Raw files are gitignored — **every environment (local machine, server, cloud) must download them fresh**. Use these exact URLs:

跨机器时优先使用 `deploy/runtime/assets.json` 与
`code/scripts/environment/manage_environment.py` 检查/下载；本节保留人工核验来源和
应急命令。资产下载完成不代表 PostgreSQL workload 已导入，仍需执行本目录对应 importer
并通过 runner 的 schema、行数与唯一性检查。

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

**降级为文本轻对照**（2026-07-31 校正，现已归档）。MS MARCO 仍是文本，token ID
紧凑（约 1KB/行），不能代表图像 host-data-path 条件。历史设计见
`experiments/plans/archive/文本嵌入对照_20260731.md`。

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

MS MARCO + BGE 预计约占 6 GB；这是资产估算，不是当前机器剩余容量。下载前必须按 runtime
preflight 和 `df -h` 的现场结果决定是否执行。

## Image workload (CLIP, current multimodal workload)

当前图像输入使用 JPEG 文件目录与内容清单；CLIP decode、resize、模型执行和结果处理各自记录。
2026-07-31 的 PG 图像实验及后续历史方案保留原身份，恢复入口见
[PG 图像原件](../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md#pg-image-data-tools)，不作为当前输入准备步骤。

图像 AI_EMBED workload，用于异构资源调度（CPU decode vs GPU embed）与多模态泛化验证。

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
# CLIP ViT-B/32（默认 embedding 模型，~600 MB）
python -c "from huggingface_hub import snapshot_download; snapshot_download('openai/clip-vit-base-patch32', cache_dir='models')" 2>/dev/null \
  || huggingface-cli download openai/clip-vit-base-patch32 \
       --local-dir models/clip-vit-base-patch32
```

COCO train 压缩包约 19 GB。下载前按 runtime 指引实测目标磁盘空间，并为解压后的选定文件留出空间。
对已准备好的 JPEG 目录制作一次清单，查询时按清单延迟读取，不把 ZIP 存在当作文件输入已经可用：

```bash
python code/scripts/data/build_image_file_manifest.py \
  --image-root data/raw/coco_val2017/val2017 --workload-name coco_val2017 \
  --output tmp/coco_val2017.files.json
```

清单保留 COCO 数字 ID、相对路径、大小和内容摘要。不同 split 分别声明 workload，不能通过数字偏移伪造原始 ID。
旧 PG 主键迁移、ZIP 导入事务及专测随原件归档；已有原始 ZIP、图像、结果和失败记录没有移动或删除。

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
`code/scripts/data/import_ai_complete_workload.py`.

`--controlled-prefix-source-workload` 的 `--dry-run` 只读取现有 workload 并打印概要，
不创建或修改表，也不插入数据；正常导入仍先准备表结构，再读取与写入。

Use `--source-workload-name sharegpt_multiturn` in
`code/scripts/profiling/postgres_ai_operator_profile.py` to read only this workload.

Legacy: the older `sharegpt_burstgpt` workload (previously rows=1024, doc_id
starting at 1000000, target_output_tokens up to 2048, filtered by
`prompt_tokens + completion_max_tokens <= 2048`) is retained in the database
for 0725-0728 experiment reproduction but is no longer the main workload.
