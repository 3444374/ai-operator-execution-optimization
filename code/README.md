# Project Code

本目录存放可以迁移到正式课题工程的代码。一次性 benchmark 仍放在 `feasibility/benchmarks/` 或 `motivation/benchmarks/`。

绘图、图表复现和素材筛选脚本统一放在 `figures/scripts/`；本目录优先保留实验主体代码、服务入口和 profiling 驱动。

## 目录结构

```
code/
├── scripts/
│   ├── postgres_ai_operator_profile.py   ← PostgreSQL AI 算子链路画像（Ray actor + GPU endpoint + writeback）
│   ├── pgai_sql_operator_profile.py      ← pgai SQL 触发面画像（ai.ollama_embed via pgai 扩展）
│   ├── local_embedding_server.py         ← 本地 OpenAI 兼容 embedding 服务（Ollama）
│   └── daft_text_organizer_smoke.py      ← Daft 文本 DataFrame / into_batches / Ray runner smoke
├── configs/                              ← 后续工程配置文件（当前为空）
├── src/
│   ├── sources.py                        ← PostgreSQL/Daft 数据入口后端
│   ├── organizers.py                     ← ArrowOrganizer / DaftOrganizer 数据组织后端
│   ├── model_backends.py                 ← fake / compatible HTTP embedding and completion backend
│   ├── sinks.py                          ← none/json_text/pgvector embedding 写回 + completion JSON 写回
│   ├── metrics.py                        ← timing / GPU snapshot / CSV metrics helper
│   └── workloads.py                      ← 内置 synthetic / controlled workload seed
├── tests/
│   ├── test_sources.py                   ← 数据入口后端最小单元测试
│   ├── test_organizers.py                ← 数据组织后端最小单元测试
│   ├── test_model_backends.py            ← 模型后端最小单元测试
│   ├── test_sinks.py                     ← 写回后端最小单元测试
│   ├── test_workloads.py                 ← 内置 workload seed 单元测试
│   └── test_import_ai_complete_workload.py ← ShareGPT/BurstGPT importer 单元测试
└── requirements.txt                      ← Python 依赖（numpy, pyarrow<25, ray, psycopg, daft, torch, transformers）
```

安装依赖：

```bash
pip install -r code/requirements.txt
```

## PostgreSQL data source backends

`code/src/sources.py` defines the data entry boundary used by
`code/scripts/postgres_ai_operator_profile.py`:

- `arrow_postgres`: baseline psycopg read plus Arrow table construction.
- `daft_postgres`: Daft `read_sql` PostgreSQL entry; it requires `sqlglot` and
  `connectorx`.

Data entry can use either the baseline psycopg path or Daft:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run --data-source daft_postgres --organizer daft `
  --output tmp\postgres_profile_dry_run.csv
```

Current writeback remains `none`, `json_text`, or `pgvector`. Lance is a future
optional sink backend and is not part of this implementation.

## Runtime code boundaries

The main profiling script should stay as orchestration code. Reusable behavior
now lives under `code/src/`:

- `sources.py`: PostgreSQL/Daft data entry.
- `organizers.py`: Arrow/Daft batch organization.
- `model_backends.py`: `fake` debug backend, `compatible_http` embedding/completion backend, and Ollama native completion backend.
- `sinks.py`: existing PostgreSQL embedding writeback modes plus `document_completions` JSON-text writeback.
- `metrics.py`: timers, GPU snapshot, and CSV append helper.
- `workloads.py`: small built-in seed workloads for smoke/dev only.

`fake` is retained only as a local control backend for offline smoke tests and
pipeline debugging. It is not a model-service result source. For vLLM-compatible
experiments, use `--model-backend compatible_http`; the older `http_openai`
name is accepted only as a compatibility alias. `AI_COMPLETE` is selected with
`--operator ai_complete` and expects a vLLM-compatible `/v1/completions` URL
through `--completion-endpoint-url`. For local Ollama smoke runs, use
`--model-backend ollama --completion-endpoint-url http://localhost:11434`.

## AI_COMPLETE workload import

Final comparable AI_COMPLETE baselines should use the normalized
ShareGPT/BurstGPT workload instead of the legacy synthetic seed. Raw payloads
live under `data/raw/` and are ignored by git; see `data/README.md`.

Import 1024 local rows without clearing existing `documents` rows:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\import_ai_complete_workload.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --workload-name sharegpt_burstgpt `
  --start-doc-id 1000000 `
  --max-rows 1024 `
  --batch-rows 500
```

Run the profiling script against this workload with:

```powershell
--source-workload-name sharegpt_burstgpt
```

## Daft 文本组织 smoke

当前 Daft 接入的项目代码在 `code/src/organizers.py`：`ArrowOrganizer` 是 baseline 后端，`DaftOrganizer` 是文本阶段 Daft DataFrame 后端。独立 smoke 入口只负责验证 `rows -> Arrow Table -> organizer -> batches`，并可显式切换 Ray runner 验证 `into_partitions`。这不是正式性能实验，不写入 `motivation/results/gpu/`。

```powershell
.conda\pg-ai-profile\python.exe code\scripts\daft_text_organizer_smoke.py `
  --organizer daft --runner ray --rows 32 --batch-size 8 `
  --partition-mode into_partitions --partitions 4 `
  --output tmp\daft_text_organizer_smoke.csv
```

```powershell
.conda\pg-ai-profile\python.exe code\tests\test_organizers.py
```

## PostgreSQL AI 算子链路画像

当前新增入口：

```text
code/scripts/postgres_ai_operator_profile.py
```

目标是采集 PostgreSQL 18 触发 AI 算子后的外部执行链路画像；当前本地运行
18.4，最终目标平台为公司内部 18.3：

```text
PostgreSQL documents/job table
  -> external worker
  -> ArrowOrganizer / DaftOrganizer
  -> Ray actor fake or compatible HTTP AI_EMBED / AI_COMPLETE
  -> bounded backpressure
  -> fan-in
  -> writeback document_embeddings / document_completions
```

当前本机已通过 Docker 运行 PostgreSQL 18.4 + pgvector 0.8.2 同构预演实例，
数据库、扩展和向量查询已经验证；WSL `.venv` 已安装 Ray、PyArrow、NumPy
和 psycopg，并完成 256 行 PostgreSQL -> Arrow -> Ray actor -> fake embedding
-> PostgreSQL 写回冒烟运行。CSV 位于
`feasibility/results/pg18_4_connection_smoke_256_rows.csv`（及 `pg18_4_connection_smoke_runs.csv`），完整记录见
`feasibility/results/pg18_4_connection_validation.md`。
脚本内部连接、读取、Ray 执行和写回函数的对应关系见 `scripts/README.md`。

最小 dry-run：

```bash
.venv/bin/python code/scripts/postgres_ai_operator_profile.py \
  --dry-run \
  --output feasibility/results/postgres_ai_operator_profile_dry_run.csv
```

连接当前本地同构 PostgreSQL 实例：

```bash
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/ai_operator" \
.venv/bin/python code/scripts/postgres_ai_operator_profile.py \
  --setup \
  --seed-rows 10000 \
  --total-rows 10000 \
  --db-fetch-rows 1024 \
  --ray-batch-rows 512 \
  --model-workers 2 \
  --max-inflight 8 \
  --strategy coalesced \
  --organizer arrow \
  --output feasibility/results/pg18_4_connection_smoke_256_rows.csv
```

Daft organizer dry-run:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\postgres_ai_operator_profile.py `
  --dry-run --executor python `
  --organizer daft --daft-runner native `
  --output tmp\postgres_profile_dry_run.csv
```

真实报告必须说明数据库平台类型：

- PostgreSQL 18.3 内部验证平台；
- 普通 PostgreSQL + pgvector 同构预演替身；
- 其他开源数据库 AI 算子平台。
