# deploy/AGENTS.md

继承根规则。组件与平台入口见 [README.md](README.md)，实际环境操作按根 §5 加载 runtime 和目标平台要求。

- 本目录保存部署配置与 runbook；连接验证结果放 `results/feasibility/`，画像放 `results/motivation/`，方法实验放 `results/`。
- 本地单 GPU 模型 Docker 配置不放本目录；AutoDL 的完整云服务栈由 `autodl/` 维护。
- compose/init SQL 的运行行为变化后，执行受影响的 smoke；注释、说明和格式修订只检查对应内容。
- 镜像、端口和挂载路径显式配置；版本以 runtime profile/service manifest 为准，存在差异时注明，
  变更版本时同步相关 runbook 和锁定配置。
- driver/Ray actor 与 vLLM 使用独立 Python 环境，图像 baseline 不覆盖 driver torch。
- `postgres18.4/` 只提供 PG18.4 本地预演，其结果不能作为 PG18.3 内部平台结论。
