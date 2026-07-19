# Data

This directory is for local dataset payloads and derived workload tables.

Raw downloaded files live under `data/raw/` and are ignored by git. Keep only
metadata, download commands, and preprocessing scripts in the repository.

## Current Raw Files

| Dataset | Local file | Size | Use |
|---|---:|---:|---|
| ShareGPT Vicuna unfiltered | `data/raw/sharegpt_vicuna/ShareGPT_V3_unfiltered_cleaned_split.json` | 672,837,942 bytes | Real prompt text for `AI_COMPLETE` data-organization experiments |
| BurstGPT | `data/raw/burstgpt/BurstGPT_1.csv` | 50,853,373 bytes | Real LLM serving trace with timestamp and token fields for scheduling experiments |

## Sources

- ShareGPT Vicuna unfiltered: Hugging Face dataset `anon8231489123/ShareGPT_Vicuna_unfiltered`, file `ShareGPT_V3_unfiltered_cleaned_split.json`.
- BurstGPT: GitHub repository `HPMLL/BurstGPT`, file `data/BurstGPT_1.csv`.

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
