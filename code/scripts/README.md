# PostgreSQL AI 算子画像脚本

## 文件定位

当前连接与测试流程集中在：

```text
code/scripts/postgres_ai_operator_profile.py
```

它既是当前 Phase 1 的实验驱动脚本，也是后续拆分正式 worker 之前的最小端到端实现。当前没有另一份隐藏的连接代码。

## 流程与函数映射

```text
PostgreSQL documents/job table
  -> connect / create_job
  -> fetch_record_batch
  -> split_batch
  -> FakeEmbeddingActor.embed
  -> submit_with_backpressure
  -> write_embeddings / finish_job
  -> append_metrics
```

| 环节 | 函数/对象 | 作用 |
|---|---|---|
| 数据库连接 | `connect` | 使用 psycopg 和 `--database-url` 建立连接 |
| 平台识别 | `database_metadata` | 读取真实 PG 和 pgvector 版本并写入 CSV |
| 建表 | `setup_schema` / `SCHEMA_SQL` | 创建 documents、jobs、embeddings 三张表 |
| 任务触发替身 | `create_job` | 用 job table 模拟数据库 AI 算子触发 |
| 数据读取 | `fetch_record_batch` | 从 PG 读取并构造 Arrow RecordBatch |
| 批划分 | `split_batch` | 按策略决定 actor 输入粒度 |
| AI 算子 | `FakeEmbeddingActor.embed` | 当前 fake embedding；后续替换真实模型 |
| 并发与反压 | `submit_with_backpressure` | 控制 in-flight、等待和 fan-in |
| 数据写回 | `write_embeddings` | 当前以 JSON 文本写回 PostgreSQL |
| 指标输出 | `append_metrics` | 追加写入实验 CSV |

## 当前本地运行

```bash
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/ai_operator" \
.venv/bin/python code/scripts/postgres_ai_operator_profile.py \
  --setup --seed-rows 256 --total-rows 256 \
  --db-fetch-rows 128 --ray-batch-rows 64 \
  --model-workers 2 --max-inflight 4 \
  --strategy coalesced \
  --output validation/results/pg18_4_connection_smoke_256_rows.csv
```

## 结果位置

- 原始数据：`validation/results/pg18_4_connection_smoke_256_rows.csv`
- 设置、过程、表核对、严谨性与结论：
  `validation/results/pg18_4_connection_validation.md`
- 数据库部署：`deploy/postgres18.4/README.md`

当前结果只证明 PostgreSQL 18.4 同构链路连通，不是公司 PostgreSQL 18.3
平台结果，也不是性能优化结论。

## 正式对照实验

2026-07-11 已为 `postgres_ai_operator_profile.py` 增加可重复对照实验参数：

- `--executor python|ray_task|ray_actor`
- `--writeback-mode json_text|pgvector`
- `--write-batch-rows`
- `--warmup-runs`
- `--repeats`
- `--experiment-id`

脚本现在会拆分 `db_fetch_s` 与 `arrow_build_s`，支持普通 Python baseline，并且只在
`--executor ray_actor` 时按需导入 Ray。

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
  --output motivation\results\pg18_4_system_profile_fake_ai_embed.csv
```

完整矩阵、CSV 位置与结果解释：

```text
motivation/results/pg18_4_system_profile_fake_ai_embed.md
```
