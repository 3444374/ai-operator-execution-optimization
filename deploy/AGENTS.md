# deploy/AGENTS.md

本目录存放数据库和 AI 算子服务的 Docker Compose 部署配置。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 为 `motivation/` 和 `experiments/` 提供可复现的数据库和 AI 算子服务环境。
- `pgai/`：PostgreSQL + pgai 扩展 + pgvector 的 AI 算子集成环境。
- `postgres18.4/`：PostgreSQL 18.4 + pgvector 的本地同构预演环境（非公司内部 PG18.3 平台）。

## 边界

- 部署配置只保证环境可启动、可连接、可跑通 smoke test。
- 不在这里放实验脚本、实验结果或性能分析。
- GPU 模型服务（Ollama、vLLM 等）的部署配置暂不放在这里。
- 连接验证结果放 `feasibility/results/`，不放在 deploy/。

## 规则

- 每次修改 compose 或 init SQL 后，必须跑通对应目录的 smoke test 验证。
- 镜像版本、端口、挂载路径要明确写清楚，不依赖隐式默认值。
- `postgres18.4/` 的结果只能标注为 PG18.4 本地预演，不能写成 PostgreSQL 18.3 内部平台结论。
