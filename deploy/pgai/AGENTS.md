# deploy/pgai/AGENTS.md

本目录维护 pgai SQL 算子预演环境，用于验证真实 SQL 触发的 PostgreSQL AI
模型调用形态。

## 定位

- 这是 pgai / 等价 PostgreSQL AI SQL surface 的隔离预演环境。
- 它用于验证 `CREATE EXTENSION ai`、`ai.ollama_embed(...)` 等 SQL 入口能否跑通。
- 它不替代 `deploy/postgres18.4/` 的 PostgreSQL 18.4 + pgvector 同构预演环境。
- 该环境当前基于 pgai 官方 Docker 示例所用的 Timescale PG17 镜像；不能写成
  PostgreSQL 18.4 或 PostgreSQL 18.3 结果。

## 边界

- 本目录只放 pgai Docker 部署、初始化 SQL、冒烟验证 SQL 和运行说明。
- 不放正式性能结果；连接验证结果归 `feasibility/results/`，系统画像结果归
  `motivation/results/`。
- pgai 只作为真实 SQL 触发面的参考，不把论文主线改成 pgai 产品集成。

## 运行规则

- 使用独立容器名、端口和 named volume，避免影响现有 `ai-operator-postgres18`。
- 不使用 `latest` 作为 PostgreSQL/pgai 数据库镜像标签。
- 普通 `down` 保留 named volume；未经用户明确同意，不运行 `down --volumes`。
- Ollama 模型下载体积较大，运行 `ollama pull` 前应向用户说明网络和磁盘影响。

## 验证要求

环境变更后至少验证：

1. 数据库容器 health 为 `healthy`。
2. `CREATE EXTENSION ai` 成功。
3. `vector` 扩展可用。
4. Ollama `all-minilm` 模型可调用。
5. `smoke_ai_embed.sql` 能通过 SQL 生成 embedding 并写入向量列。
