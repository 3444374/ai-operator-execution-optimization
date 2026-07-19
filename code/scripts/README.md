# PostgreSQL AI 算子画像脚本

## 文件定位

当前连接与测试流程集中在：

```text
code/scripts/postgres_ai_operator_profile.py
```

pgai SQL trigger-surface profile entry:

```text
code/scripts/pgai_sql_operator_profile.py
```

Daft text DataOrganizer smoke entry:

```text
code/scripts/daft_text_organizer_smoke.py
```

Shared-vLLM K_max interference runner:

```text
code/scripts/run_kmax_interference_experiment.py
```

它既是当前 Phase 1 的实验驱动脚本，也是后续拆分正式 worker 之前的最小端到端实现。当前没有另一份隐藏的连接代码。

本目录只放实验主体、服务启动、数据采集和 profiling 入口。绘图、图表复现和素材筛选脚本统一放在 `figures/scripts/`。

## 流程与函数映射

```text
PostgreSQL documents/job table
  -> DataSource (arrow_postgres or daft_postgres)
  -> ArrowOrganizer / DaftOrganizer
  -> model backend (fake, compatible_http, or ollama)
  -> submit_with_backpressure
  -> sink.write / finish_job
  -> metrics append
```

| 环节 | 函数/对象 | 作用 |
|---|---|---|
| 数据库连接 | `connect` | 使用 psycopg 和 `--database-url` 建立连接 |
| 平台识别 | `database_metadata` | 读取真实 PG 和 pgvector 版本并写入 CSV |
| 建表 | `setup_schema` / `SCHEMA_SQL` | 创建 documents、jobs、embeddings、completions 表 |
| 任务触发替身 | `create_job` | 用 job table 模拟数据库 AI 算子触发 |
| 数据读取 | `PostgresArrowSource` / `DaftPostgresSource` | 从 PG 基线路径或 Daft SQL 入口读取并返回 Arrow Table |
| 批划分 | `ArrowOrganizer` / `DaftOrganizer` | 按策略决定 actor 输入粒度；Daft 后端通过 `code/src/organizers.py` 接入 |
| AI 算子 | `FakeEmbeddingActor` / `CompatibleHTTPEmbeddingActor` / `FakeCompletionActor` / `CompatibleHTTPCompletionActor` / `OllamaCompletionActor` | `fake` 只用于离线 smoke 和控制变量；`compatible_http` 用于 vLLM-compatible embedding 或 completion endpoint；`ollama` 用于本地 Ollama `/api/generate` completion smoke |
| 并发与反压 | `submit_with_backpressure` | 控制 in-flight、等待和 fan-in |
| 数据写回 | `code/src/sinks.py::write_embeddings` / `write_completions` | embedding 支持 `none`、JSON 文本和 pgvector；completion 支持 `none` 和 JSON 文本 |
| 指标输出 | `code/src/metrics.py::append_metrics` | 追加写入实验 CSV |

## 当前本地运行

```bash
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/ai_operator" \
.venv/bin/python code/scripts/postgres_ai_operator_profile.py \
  --setup --seed-rows 256 --total-rows 256 \
  --db-fetch-rows 128 --ray-batch-rows 64 \
  --model-workers 2 --max-inflight 4 \
  --strategy coalesced \
  --output feasibility/results/pg18_4_connection_smoke_256_rows.csv
```

## 结果位置

- 原始数据：`feasibility/results/pg18_4_connection_smoke_256_rows.csv`
- 设置、过程、表核对、严谨性与结论：
  `feasibility/results/pg18_4_connection_validation.md`
- PostgreSQL 18.4 + pgvector 数据库部署：`deploy/postgres18.4/README.md`
- pgai SQL 算子触发面预演：`deploy/pgai/README.md`

## Daft text organizer smoke

`daft_text_organizer_smoke.py` is the smallest script-level entry for the
organizer abstraction in `code/src/organizers.py`. It does not connect to
PostgreSQL or vLLM; it verifies that text rows can pass through either
`ArrowOrganizer` or `DaftOrganizer` and return downstream Arrow batches. Use
`--runner ray` when checking Daft `into_partitions` or `repartition`;
NativeRunner reports these partition operations as no-op. The default output is
under `tmp/` because this is a local smoke result, not a formal experiment
result.

```powershell
.conda\pg-ai-profile\python.exe code\scripts\daft_text_organizer_smoke.py `
  --organizer arrow --rows 256 --batch-size 64 `
  --output tmp\daft_text_organizer_smoke.csv

.conda\pg-ai-profile\python.exe code\scripts\daft_text_organizer_smoke.py `
  --organizer daft --runner ray --rows 32 --batch-size 8 `
  --partition-mode into_partitions --partitions 4 `
  --output tmp\daft_text_organizer_smoke.csv
```

当前结果只证明 PostgreSQL 18.4 同构链路连通，不是公司 PostgreSQL 18.3
平台结果，也不是性能优化结论。

## 正式对照实验

2026-07-11 已为 `postgres_ai_operator_profile.py` 增加可重复对照实验参数：

- `--executor python|ray_task|ray_actor`
- `--data-source arrow_postgres|daft_postgres`
- `--source-order doc_id|arrival_time`
- `--batching-policy fixed_rows|token_budget|length_align_fixed_rows|length_align_token_budget|prefix_aware_fixed_rows|prefix_aware_token_budget`
- `--token-budget`
- `--scheduling-policy static|queue_adaptive`
- `--adaptive-min-inflight`
- `--adaptive-max-inflight`
- `--adaptive-queue-threshold`
- `--adaptive-running-threshold`
- `--adaptive-kv-threshold`
- `--adaptive-poll-interval-s`
- `--operator ai_embed|ai_complete`
- `--organizer arrow|daft`
- `--organizer-partition-mode none|into_partitions|repartition`
- `--organizer-partitions`
- `--daft-runner native|ray`
- `--model-backend fake|compatible_http|http_openai|ollama`
- `--embedding-endpoint-url`
- `--embedding-model`
- `--embedding-api-key`
- `--completion-endpoint-url`
- `--completion-model`
- `--completion-api-key`
- `--completion-max-tokens`
- `--model-metrics-url`
- `--writeback-mode none|json_text|pgvector`
- `--write-batch-rows`
- `--warmup-runs`
- `--repeats`
- `--experiment-id`

`--source-order doc_id` is the offline throughput mode: PostgreSQL already
contains the workload rows, and the profile scans them in stable document-id
order before Daft organization. `--source-order arrival_time` is the
arrival-aware service mode: rows are read by `arrival_time_s NULLS LAST, doc_id`
before Daft organization and Ray submission, which is the right setup for
K_max, queue-adaptive flush, and backpressure experiments.

`--batching-policy fixed_rows` preserves the original row-count batching path.
`--batching-policy token_budget --token-budget N` greedily forms upstream
submission batches using `prompt_tokens + completion_max_tokens` as the
estimated model cost. This only changes the Ray/vLLM submission units; it does
not modify vLLM continuous batching or Ray's internal scheduler. CSV rows record
`batching_policy`, `token_budget`, and `model_request_timeout_s`.

Length- and prefix-aware variants reorder rows before creating the upstream
submission batches:

```text
length_align_fixed_rows
length_align_token_budget
prefix_aware_fixed_rows
prefix_aware_token_budget
```

The length-aware variants sort by `prompt_tokens`. The prefix-aware variants
sort by `prefix_key`, then `prompt_tokens`. CSV rows record
`organization_policy_family`, `batch_prompt_token_spread_mean`, and
`prefix_group_ratio`. These are organization signals only; prefix-cache benefit
still requires APC/cache metrics or a controlled prefix-share workload.

`--scheduling-policy static` uses the configured `--max-inflight` as a fixed
admission window. `--scheduling-policy queue_adaptive` polls the vLLM metrics
endpoint and switches between `--adaptive-min-inflight` and
`--adaptive-max-inflight` according to queue/running/KV thresholds. CSV rows
record `adaptive_downshifts`, `adaptive_upshifts`, and
`adaptive_limit_mean`.

`run_kmax_interference_experiment.py` is a small orchestration wrapper around
`postgres_ai_operator_profile.py`. It starts a background bulk `AI_COMPLETE`
job and then starts a foreground small job against the same vLLM endpoint. Use
it when testing the admission-control motivation for `K_max`: bounded
background inflight versus unbounded background inflight under shared service.
It supports static background `K_max` sweeps and an optional first
`queue_adaptive` scenario. Its default outputs are:

```text
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_small_20260719.csv
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_bulk_20260719.csv
```

脚本现在会拆分 `db_fetch_s` 与 `arrow_build_s`，支持普通 Python baseline，并且只在
`--executor ray_actor` 或 `--executor ray_task` 时按需导入 Ray。2026-07-12 增加了
OpenAI-compatible endpoint 后端，用于后续连接本地 vLLM、Ray Serve 或其他
GPU-backed model service。推荐参数名是 `compatible_http`，旧的 `http_openai`
只作为兼容别名保留。`fake` 仍是默认值，只能作为脚本调试、PG18.4 同构预演或
历史对照，不能写成 GPU-backed 结论。`AI_COMPLETE` 当前使用
`--completion-endpoint-url` 指向 vLLM-compatible `/v1/completions`，并将 JSON 文本写回
`document_completions`；也支持 `--model-backend ollama` 连接本地 Ollama
`/api/generate` 做 completion smoke。它还不是 token-aware/prefix-aware 策略实现。

示例命令：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4096 --total-rows 4096 `
  --db-fetch-rows 512 --ray-batch-rows 256 `
  --embedding-dim 128 --model-workers 2 --max-inflight 8 `
  --executor ray_actor --strategy coalesced `
  --warmup-runs 1 --repeats 3 `
  --experiment-id pg18_4_fake_4096 `
  --output motivation\results\pg18_4_fake\system_profile.csv
```

完整矩阵、CSV 位置与结果解释：

```text
motivation/results/pg18_4_fake/system_profile.md
```

GPU-backed embedding endpoint 配置检查示例：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run `
  --executor ray_actor `
  --model-backend compatible_http `
  --embedding-endpoint-url http://localhost:8000/v1/embeddings `
  --embedding-model local-embedding `
  --experiment-id gpu_ai_embed_config_check `
  --output feasibility\results\gpu_ai_embed_config_dry_run.csv
```

AI_COMPLETE vLLM-compatible completion endpoint 配置检查示例：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run `
  --operator ai_complete `
  --executor ray_actor `
  --model-backend compatible_http `
  --completion-endpoint-url http://localhost:8000/v1/completions `
  --completion-model local-llm `
  --completion-max-tokens 128 `
  --experiment-id ai_complete_config_check `
  --output feasibility\results\ai_complete_config_dry_run.csv
```

Local vLLM + Qwen2.5-1.5B startup on the current Windows/WSL Docker machine:

```powershell
docker run -d --name ai-operator-vllm-qwen --gpus all `
  -p 8000:8000 `
  --ipc=host `
  -e VLLM_WSL2_ENABLE_PIN_MEMORY=1 `
  -e VLLM_USE_V2_MODEL_RUNNER=0 `
  -v D:\Code\ai-operator-execution-optimization\models\Qwen2.5-1.5B-Instruct:/models/qwen:ro `
  vllm/vllm-openai:v0.25.1-cu129-ubuntu2404 `
  --model /models/qwen `
  --served-model-name qwen2.5-1.5b `
  --dtype auto `
  --max-model-len 2048 `
  --gpu-memory-utilization 0.75 `
  --enforce-eager
```

Minimal `AI_COMPLETE + Daft + vLLM` smoke:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4 --total-rows 2 `
  --db-fetch-rows 2 --ray-batch-rows 1 `
  --operator ai_complete `
  --executor python `
  --model-backend compatible_http `
  --completion-endpoint-url http://localhost:8000/v1/completions `
  --completion-model qwen2.5-1.5b `
  --completion-max-tokens 8 `
  --data-source daft_postgres --organizer daft `
  --writeback-mode json_text `
  --experiment-id vllm_local_qwen15b_daft_ai_complete_smoke `
  --output tmp\vllm_local_qwen15b_ai_complete_smoke.csv
```

Controlled `AI_COMPLETE + Daft + Ray + vLLM` baseline workload:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\import_ai_complete_workload.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --workload-name sharegpt_burstgpt `
  --start-doc-id 1000000 `
  --max-rows 1024 `
  --batch-rows 500 `
  --tokenizer-path models\Qwen2.5-1.5B-Instruct `
  --max-model-len 2048 `
  --completion-max-tokens 16
```

Then profile the imported workload:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup `
  --total-rows 128 `
  --db-fetch-rows 128 --ray-batch-rows 8 `
  --operator ai_complete `
  --executor ray_task `
  --model-backend compatible_http `
  --completion-endpoint-url http://localhost:8000/v1/completions `
  --completion-model qwen2.5-1.5b `
  --completion-max-tokens 32 `
  --model-metrics-url http://localhost:8000/metrics `
  --source-workload-name sharegpt_burstgpt `
  --source-order doc_id `
  --data-source daft_postgres --organizer daft `
  --writeback-mode none `
  --experiment-id vllm_qwen15b_sharegpt_burstgpt_ray_task_batch_8 `
  --output experiments\results\local_vllm_qwen15b_baseline\sharegpt_burstgpt_ray_baseline.csv
```

AI_COMPLETE Ollama native completion smoke 示例：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4 --total-rows 2 `
  --db-fetch-rows 2 --ray-batch-rows 1 `
  --operator ai_complete `
  --executor python `
  --model-backend ollama `
  --completion-endpoint-url http://localhost:11434 `
  --completion-model qwen2.5:1.5b `
  --completion-max-tokens 16 `
  --data-source daft_postgres --organizer daft `
  --writeback-mode json_text `
  --experiment-id ollama_daft_ai_complete_smoke `
  --output tmp\ollama_ai_complete_smoke.csv
```

正式 GPU-backed 结果应输出到：

```text
motivation/results/gpu/ai_embed_profile.csv
```

只有在 `--model-backend compatible_http` 连接到真实 GPU-backed endpoint 时，结果才可放入
`motivation/results/gpu/`。

本地真实模型 endpoint 可用 `local_embedding_server.py` 启动：

```powershell
$env:HF_HOME="D:\Code\ai-operator-execution-optimization\.cache\huggingface"
$env:HF_HUB_CACHE="D:\Code\ai-operator-execution-optimization\.cache\huggingface\hub"
$env:TRANSFORMERS_CACHE=$env:HF_HUB_CACHE
$env:TORCH_HOME="D:\Code\ai-operator-execution-optimization\.cache\torch"

.conda\pg-ai-profile\python.exe code\scripts\local_embedding_server.py `
  --model .cache\models\all-MiniLM-L6-v2 `
  --device cuda `
  --batch-size 64 `
  --port 8000
```

该服务提供 OpenAI-compatible `/v1/embeddings` 接口，供
`postgres_ai_operator_profile.py --model-backend compatible_http` 调用。
2026-07-12 的首轮 GPU-backed profile 中，该 endpoint 是用户手动启动的。

## 2026-07-14 GPU key rerun

Latest GPU-backed key rerun after pgai SQL trigger-surface validation:

```text
motivation/results/gpu/pgai_integrated_key_rerun_20260714.md
motivation/results/gpu/ai_embed_pgai_integrated_key_20260714.csv
```

This rerun uses `local_embedding_server.py` on ports 8000 and 8001 with
`--device cuda`. It keeps pgai SQL surface validation separate from the
job-table GPU timing profile.

## 2026-07-14 pgvector(384) writeback support

`postgres_ai_operator_profile.py --setup --embedding-dim 384` now creates
`document_embeddings.embedding_vector` as `vector(384)`. If an old
`embedding_vector` column has a different dimension, the script drops and
recreates that column only; it does not delete Docker volumes or the
documents/job tables.

Latest GPU-backed sink comparison:

```text
motivation/results/gpu/pgvector_writeback_20260714.md
motivation/results/gpu/ai_embed_pgvector_writeback_20260714.csv
```
