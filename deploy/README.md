# deploy/

本目录存放部署配置与指南。`pgai/`、`postgres18.4/` 是本机 Docker Compose 部署；`autodl/` 是 AutoDL 云服务器（2× GPU + vLLM + PostgreSQL + Ray/Daft 完整栈）的部署 runbook。环境启动后，连接验证和 smoke test 结果记录在 `feasibility/results/`。

## 子目录

| 目录 | 用途 | 组件 |
|---|---|---|
| `pgai/` | PostgreSQL + pgai AI 算子集成环境 | PostgreSQL、pgai 扩展、pgvector |
| `postgres18.4/` | PostgreSQL 18.4 本地同构预演 | PostgreSQL 18.4、pgvector |
| `autodl/` | AutoDL 云服务器部署指南与配置化脚本 | 2× GPU、vLLM、PostgreSQL18.4+pgvector、Ray/Daft |

## 使用

```bash
# 启动 pgai 环境
cd deploy/pgai
docker compose up -d

# 启动 PostgreSQL 18.4 同构预演
cd deploy/postgres18.4
docker compose up -d
```

每个子目录有独立的 `AGENTS.md`（规则）、`README.md`（详细说明）和 `compose.yaml`。
AutoDL 使用 `autodl.env.example`、`download_model.sh` 和
`start_endpoints.sh`，不使用 Docker Compose。

## 与其他目录的关系

- 实验脚本：`motivation/benchmarks/`、`code/scripts/`
- 连接验证结果：`feasibility/results/`
- 集成计划：`motivation/plans/integration.md`

## 注意

`postgres18.4/` 是本地同构预演环境，不等同于公司内部 PostgreSQL 18.3 统一验证平台。涉及 PG18.4 的结果必须标注平台边界。
