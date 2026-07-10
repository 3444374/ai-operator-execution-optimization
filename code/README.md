# Project Code

本目录存放可以迁移到正式课题工程的代码。一次性 benchmark 仍放在 `validation/benchmarks/` 或 `motivation/`。

## PostgreSQL AI 算子链路画像

当前新增入口：

```text
code/scripts/postgres_ai_operator_profile.py
```

目标是采集 PostgreSQL 18.3 触发 AI 算子后的外部执行链路画像：

```text
PostgreSQL documents/job table
  -> external worker
  -> Arrow RecordBatch
  -> Ray actor fake/local AI_EMBED
  -> bounded backpressure
  -> fan-in
  -> writeback document_embeddings
```

当前机器没有 `psql/pg_config`，Python 环境也还没有 `psycopg`，Docker daemon 未启动，因此暂时不能在本机直接跑真实数据库链路。脚本已支持 dry-run 和后续真实连接。

最小 dry-run：

```bash
.venv/bin/python code/scripts/postgres_ai_operator_profile.py \
  --dry-run \
  --output validation/results/postgres_ai_operator_profile_dry_run.csv
```

拿到 PostgreSQL 18.3 或同构 PostgreSQL 实例后：

```bash
DATABASE_URL="postgresql://USER:PASSWORD@HOST:PORT/DB" \
.venv/bin/python code/scripts/postgres_ai_operator_profile.py \
  --setup \
  --seed-rows 10000 \
  --total-rows 10000 \
  --db-fetch-rows 1024 \
  --ray-batch-rows 512 \
  --model-workers 2 \
  --max-inflight 8 \
  --strategy coalesced \
  --output validation/results/postgres_ai_operator_profile.csv
```

真实报告必须说明数据库平台类型：

- PostgreSQL 18.3 内部验证平台；
- 普通 PostgreSQL + pgvector 同构预演替身；
- 其他开源数据库 AI 算子平台。
