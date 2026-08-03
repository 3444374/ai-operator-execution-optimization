# deploy/

本目录存放部署配置与指南。`pgai/`、`postgres18.4/` 是本机 Docker Compose 部署；`autodl/` 是 AutoDL 云服务器（2× GPU + 文本 vLLM + 图像 Ray CLIP actor/vLLM pooling baseline + PostgreSQL + Ray/Daft）的部署 runbook。环境启动后，连接验证和 smoke test 结果记录在 `feasibility/results/`。

## 子目录

| 目录 | 用途 | 组件 |
|---|---|---|
| `pgai/` | PostgreSQL + pgai AI 算子集成环境 | PostgreSQL、pgai 扩展、pgvector |
| `postgres18.4/` | PostgreSQL 18.4 本地同构预演 | PostgreSQL 18.4、pgvector |
| `autodl/` | AutoDL 云服务器部署指南与配置化脚本 | 2× GPU、vLLM、Ray CLIP actor、PostgreSQL18.4+pgvector、Ray/Daft |

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

AutoDL 新对话的唯一入口是 `autodl/README.md` 顶部“新对话 / 新 agent 的
唯一操作入口”。其中分别给出全新实例环境准备、每次开机恢复、64 行 gate、
正式后台启动与 `--resume` 恢复流程。具体实验顺序以
`../PROJECT_OUTLINE.md` 和
`../experiments/plans/experiment_status_and_gaps.md` 为准，不从旧聊天推断，
也不把多个因果问题合成一个大矩阵。

## 与其他目录的关系

- 实验脚本：`motivation/benchmarks/`、`code/scripts/`
- 连接验证结果：`feasibility/results/`
- 集成计划：`motivation/plans/integration.md`

## 注意

`postgres18.4/` 是本地同构预演环境，不等同于公司内部 PostgreSQL 18.3 统一验证平台。涉及 PG18.4 的结果必须标注平台边界。
