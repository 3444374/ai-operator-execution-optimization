# deploy/postgres18.4/AGENTS.md

继承上级规则。PG18.4 + pgvector 本地预演的运行命令见 [README.md](README.md)。

- 实际版本从 compose 和运行查询记录；结果只代表 PG18.4 本地环境，不能作为 PG18.3 平台验证。
- 镜像用固定标签，不用 `latest` 或浮动大版本；普通 `down` 保留 named volume，删除 volume 或
  执行 `down --volumes` 仍需用户明确同意。
- 改变 PG 大版本、扩展版本或数据目录前说明兼容性与迁移影响；模型服务与 Ray 等作为外部 worker/service 部署。
- 新建环境或改变运行行为后，检查容器 health、PG/扩展版本、最小向量距离查询，以及画像脚本的建表、
  读取和写回；局部说明修订只检查对应文字与引用。
- 连接验证放 `feasibility/results/`，系统画像放 `motivation/results/`，本目录不保存实验结果。
