# PostgreSQL 18.4 本地同构预演环境

本目录运行 PostgreSQL 18.4 和 pgvector 0.8.2，用于预演数据库 AI 算子
外部执行链路。它是公司内部 PostgreSQL 18.3 验证平台的本地接口替身，
不是 PostgreSQL 18.3 本身；本地结果不能写成内部平台测量结果。

当前镜像通过 DaoCloud 代理下载后保存在 Docker 本地，Compose 使用标准
镜像名 `pgvector/pgvector:0.8.2-pg18-bookworm`。

## 启动与验证

```powershell
docker compose -f deploy/postgres18.4/compose.yaml up -d
docker exec ai-operator-postgres18 psql -U postgres -d ai_operator -c "SELECT version();"
docker exec ai-operator-postgres18 psql -U postgres -d ai_operator -c "SELECT extname, extversion FROM pg_extension WHERE extname = 'vector';"
```

## 项目连接地址

```text
postgresql://postgres:postgres@localhost:5432/ai_operator
```

项目画像入口：

```text
code/scripts/postgres_ai_operator_profile.py
```

首次真实连接已于 2026-07-11 完成，结果写入
`validation/results/postgres_ai_operator_profile.csv`。数据库中的 `documents`
和 `document_embeddings` 均为 256 行，最新 job 2 状态为 `finished`。完整设置、
数据和严谨性边界见
`validation/results/postgres18_local_environment_validation.md`。

## 停止环境

停止容器但保留数据：

```powershell
docker compose -f deploy/postgres18.4/compose.yaml down
```

`down` 会保留 named volume。除非明确要删除数据库内容，否则不要添加
`--volumes`。
