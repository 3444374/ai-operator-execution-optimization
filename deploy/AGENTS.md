# deploy/AGENTS.md

本文件继承根 `AGENTS.md`，只增加部署配置、容器和平台 runbook 规则。当前组件、版本和命令见
`README.md`、`runtime/README.md` 与目标平台 README，不写入本规则文件。

## 作用

- 为 `motivation/` 和 `experiments/` 提供可复现的数据库和 AI 算子服务环境。
- `pgai/`：PostgreSQL + pgai 扩展 + pgvector 的 AI 算子集成环境。
- `postgres18.4/`：PostgreSQL 18.4 + pgvector 的本地同构预演环境（非公司内部 PG18.3 平台）。
- `autodl/`：AutoDL 云服务器部署 runbook（2× GPU + 文本 vLLM 多 endpoint + 图像 Ray CLIP actor/vLLM pooling baseline + PostgreSQL + Ray/Daft + workload 数据），服务"多 endpoint / 多 GPU 真实验证"。

## 边界

- 部署配置/指南只保证环境可启动、可连接、可跑通 smoke test。
- 不在这里放实验脚本、实验结果或性能分析。
- 本地 Docker 化的 **单** GPU 模型服务（Ollama 等）配置不放在这里；但**云服务器的完整栈部署指南**（AutoDL：vLLM + PG + Ray/Daft 一条龙）放 `deploy/autodl/`。
- 连接验证结果放 `feasibility/results/`，不放在 deploy/。

## 规则

- 每次修改 compose 或 init SQL 后，必须跑通对应目录的 smoke test 验证。
- 镜像版本、端口、挂载路径要明确写清楚，不依赖隐式默认值。
- `postgres18.4/` 的结果只能标注为 PG18.4 本地预演，不能写成 PostgreSQL 18.3 内部平台结论。
- 平台指南中的 vLLM、torch、transformers、Pillow、Ray、Daft、PostgreSQL 和 pgvector 版本必须与
  runtime profile/service manifest 一致或显式记录差异；改动版本时同步相关 runbook 与锁定配置。
  driver/Ray actor 与 vLLM 保持独立 Python 环境，不能为图像 baseline 覆盖 driver torch。
