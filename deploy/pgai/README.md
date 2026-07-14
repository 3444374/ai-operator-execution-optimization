# pgai SQL 算子预演环境

本目录新增一个独立 Docker Compose 环境，用于验证 pgai / 等价 PostgreSQL
AI SQL surface。它和现有 `deploy/postgres18.4/` 并行，不复用现有
`ai-operator-postgres18` 容器和 named volume。

## 为什么单独建环境

当前 `deploy/postgres18.4/` 使用 `pgvector/pgvector:0.8.2-pg18-bookworm`，
重点是 PostgreSQL 18.4 + pgvector 本地同构预演。该镜像通常不自带 pgai 的
`ai` 扩展。

pgai 官方 extension quick start 使用 Docker 启动一个已安装 pgai extension
的 PostgreSQL 实例，并通过 Ollama 暴露 SQL 模型调用函数，例如：

```sql
CREATE EXTENSION IF NOT EXISTS ai CASCADE;
SELECT ai.ollama_embed('all-minilm', 'hello database ai operator');
```

因此本目录先复现“真实 SQL 触发 AI embedding”的最小链路，再决定是否把这种
SQL surface 迁移或等价接入 PostgreSQL 18.4 / PostgreSQL 18.3 平台。

## 重要边界

- 本环境基于 `timescale/timescaledb-ha:pg17`，不是 PostgreSQL 18.4。
- 它只能证明 pgai SQL 触发面和 Ollama embedding 调用可用，不能作为 PG18.4
  或 PG18.3 性能结果。
- 它不是论文主线本身；主线仍是数据库驱动 AI workload 背后的 batch、Ray
  task/actor、模型服务队列、fan-in、backpressure 和 writeback 调优。
- pgai 仓库页面显示该项目已在 2026-05-27 归档；后续用于正式实验前，需要把
  兼容性和维护状态写进风险说明。

## 启动

首次启动时 `init.sql` 自动执行 `CREATE EXTENSION IF NOT EXISTS vector; CREATE EXTENSION IF NOT EXISTS ai CASCADE;`。

```powershell
docker compose -f deploy/pgai/compose.yaml up -d
```

当前固定镜像标签：

```text
timescale/timescaledb-ha:pg17
ollama/ollama:0.31.2
```

数据库连接地址：

```text
postgres://postgres:postgres@localhost:5433/postgres
```

检查扩展：

```powershell
docker exec ai-operator-pgai-db psql -U postgres -d postgres -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('ai','vector');"
```

## 拉取 Ollama embedding 模型

这一步会下载模型文件，需要网络和磁盘空间：

```powershell
docker compose -f deploy/pgai/compose.yaml exec ollama ollama pull all-minilm
```

## SQL 冒烟验证

执行最小 `AI_EMBED` 等价链路：

```powershell
docker exec -i ai-operator-pgai-db psql -U postgres -d postgres -f /dev/stdin < deploy/pgai/smoke_ai_embed.sql
```

预期结果是 3 行数据，每行 `embedding_dims = 384`。这说明 SQL 侧调用
`ai.ollama_embed(...)` 生成 embedding，并写入了 pgvector `vector(384)` 列。

2026-07-14 已完成一次本地冒烟验证：

```text
PostgreSQL 17.10
ai 0.11.2
vector 0.8.4
all-minilm:latest 45 MB
embedding_dims = 384 for 3/3 smoke rows
```

验证报告见：

```text
feasibility/results/pgai_sql_smoke_20260714.md
```

## 停止

停止但保留数据：

```powershell
docker compose -f deploy/pgai/compose.yaml down
```

不要随意添加 `--volumes`。删除 named volume 会清掉数据库和 Ollama 模型缓存。

## 和现有画像脚本的关系

当前 `code/scripts/postgres_ai_operator_profile.py` 仍然是：

```text
PostgreSQL documents table
  -> Python/Ray 外部执行
  -> fake 或 HTTP embedding endpoint
  -> PostgreSQL 写回
```

本目录跑通后，下一步可以给画像脚本增加新的触发面，例如：

```text
--operator-surface job_table|pgai_sql
```

其中 `pgai_sql` 使用 SQL 调用 `ai.ollama_embed(...)` 或等价函数，作为真实
数据库 AI 算子触发面的 baseline。不要直接把 pgai 预演结果写成 PG18.4 结果。
