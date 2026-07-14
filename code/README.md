# Project Code

本目录存放可以迁移到正式课题工程的代码。一次性 benchmark 仍放在 `feasibility/benchmarks/` 或 `motivation/benchmarks/`。

绘图、图表复现和素材筛选脚本统一放在 `figures/scripts/`；本目录优先保留实验主体代码、服务入口和 profiling 驱动。

## 目录结构

```
code/
├── scripts/
│   ├── postgres_ai_operator_profile.py   ← PostgreSQL AI 算子链路画像（Ray actor + GPU endpoint + writeback）
│   ├── pgai_sql_operator_profile.py      ← pgai SQL 触发面画像（ai.ollama_embed via pgai 扩展）
│   └── local_embedding_server.py         ← 本地 OpenAI 兼容 embedding 服务（Ollama）
├── configs/                              ← 后续工程配置文件（当前为空）
├── src/                                  ← 后续可复用库代码（当前为空）
├── tests/                                ← 后续测试代码（当前为空）
└── requirements.txt                      ← Python 依赖（numpy, pyarrow, ray, psycopg, torch, transformers）
```

安装依赖：

```bash
pip install -r code/requirements.txt
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
  -> Arrow RecordBatch
  -> Ray actor fake/local AI_EMBED
  -> bounded backpressure
  -> fan-in
  -> writeback document_embeddings
```

当前本机已通过 Docker 运行 PostgreSQL 18.4 + pgvector 0.8.2 同构预演实例，
数据库、扩展和向量查询已经验证；WSL `.venv` 已安装 Ray、PyArrow、NumPy
和 psycopg，并完成 256 行 PostgreSQL -> Arrow -> Ray actor -> fake embedding
-> PostgreSQL 写回冒烟运行。CSV 位于
`feasibility/results/postgres_ai_operator_profile.csv`，完整记录见
`feasibility/results/postgres18_local_environment_validation.md`。
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
  --output feasibility/results/postgres_ai_operator_profile.csv
```

真实报告必须说明数据库平台类型：

- PostgreSQL 18.3 内部验证平台；
- 普通 PostgreSQL + pgvector 同构预演替身；
- 其他开源数据库 AI 算子平台。
