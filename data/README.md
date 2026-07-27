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

## Boundary

Do not use the earlier synthetic `documents` seed as the final comparison
workload. The comparable baseline and optimized runs should be generated from
the same normalized ShareGPT/BurstGPT workload table.

## Local Import

The current local PostgreSQL rehearsal database has a normalized
`sharegpt_burstgpt` workload imported into `documents`:

```text
rows=1024
doc_id range starts at 1000000
prompt_tokens range: 1..1851
target_output_tokens range: 2..2048
categories: short/medium/long x ChatGPT/GPT-4
```

`prompt_tokens` are counted with the local Qwen2.5-1.5B-Instruct tokenizer
when `--tokenizer-path models\Qwen2.5-1.5B-Instruct` is passed to
`code/scripts/import_ai_complete_workload.py`. The current import filtered rows
with `prompt_tokens + completion_max_tokens <= 2048` for the local vLLM server.

Use `--source-workload-name sharegpt_burstgpt` in
`code/scripts/postgres_ai_operator_profile.py` to read only this workload.
