# deploy/pgai/AGENTS.md

继承上级规则。pgai SQL 预演的启动与 smoke 命令见 [README.md](README.md)。

- 结果记录实际镜像、PostgreSQL 与扩展版本，不能代替 `deploy/postgres18.4/` 预演或 PG18.3 内部平台验证。
  pgai 仅作真实 SQL 触发面的工程参考。
- 容器名、端口和 named volume 与已有环境隔离；数据库镜像使用固定标签。普通 `down` 保留数据，
  `down --volumes` 仍需用户明确同意。
- 模型下载前说明网络和磁盘影响，并遵守根 §5 的环境与模型运行要求。
- 新建环境或改变运行行为后，核对容器 health、`CREATE EXTENSION ai`、`vector`，以及 README 指定模型
  经 `smoke_ai_embed.sql` 生成 embedding 并写入向量列；局部说明修订无需启动服务。
- 验证结果放 `feasibility/results/`；系统画像和方法结论进入各自目录。
