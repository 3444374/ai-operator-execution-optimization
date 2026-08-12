# deploy/

本目录存放部署配置与指南。`pgai/`、`postgres18.4/` 是本机 Docker Compose 部署；`autodl/` 是 AutoDL 云服务器（2× GPU + 文本 vLLM + 图像 Ray CLIP actor/vLLM pooling baseline + PostgreSQL + Ray/Daft）的部署 runbook。环境启动后，连接验证和 smoke test 结果记录在 `feasibility/results/`。

## 子目录

| 目录 | 用途 | 组件 |
|---|---|---|
| `pgai/` | PostgreSQL + pgai AI 算子集成环境 | PostgreSQL、pgai 扩展、pgvector |
| `postgres18.4/` | PostgreSQL 18.4 本地同构预演 | PostgreSQL 18.4、pgvector |
| `runtime/` | 跨机器 profile、软件能力组、模型/数据资产清单与只读 preflight | AutoDL、单 5070、其他 Linux/NVIDIA 环境 |
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

在任意机器开始或恢复实验前先读 `runtime/README.md` 并保存 `preflight.json`；硬件 profile
默认自动选择，再进入对应平台 runbook。
`runtime/` 只检查和补齐明确选择的能力/资产，不会自动修改驱动、CUDA 或正式实验参数。

AutoDL 新对话的唯一入口是 `autodl/README.md` 顶部“新对话 / 新 agent 的
唯一操作入口”。其中分别给出全新实例环境准备、每次开机恢复、64 行 gate、
正式后台启动与 `--resume` 恢复流程。具体实验顺序以
`../PROJECT_OUTLINE.md` 和
`../experiments/plans/experiment_status_and_gaps.md` 为准，不从旧聊天推断，
也不把多个因果问题合成一个大矩阵。

SAOR 有界优先级开发模板为 `autodl/saor_bounded_priority.example.json`：它冻结 static、
原 SAOR 与两档 bulk fairness-debt cap，并要求用 lossless release-event ledger 审计机制。
服务器恢复前不得跳过 `runtime/README.md` 的只读 preflight；当前服务器关闭时不执行远端
rehearsal，也不把静态 readiness 当作实验结果。

## 与其他目录的关系

- 实验脚本：`motivation/benchmarks/`、`code/scripts/`
- 连接验证结果：`feasibility/results/`
- 集成计划：`motivation/plans/integration.md`

## 注意

`postgres18.4/` 是本地同构预演环境，不等同于公司内部 PostgreSQL 18.3 统一验证平台。涉及 PG18.4 的结果必须标注平台边界。
