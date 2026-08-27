# deploy/postgres18.4/AGENTS.md

本文件继承根 `AGENTS.md` 与 `deploy/AGENTS.md`，只增加 PostgreSQL 18.4 + pgvector 本地同构预演规则。

## 定位

- 本目录目标是 PostgreSQL 18.4 + pgvector；实际镜像/扩展版本从 compose、README 和运行时查询记录。
- 它用于预演数据库触发、外部 worker、AI 算子和写回链路。
- PostgreSQL 18.3 目标平台才承担最终平台资格验证。
- 本目录产生的本地数据和性能结果不得写成 PostgreSQL 18.3 平台结果。

## 边界

- 放本地 PostgreSQL 18.4 + pgvector 部署、连接和环境验证说明。
- 不放正式系统画像结果、性能结论或论文分析。
- 连接验证归 `feasibility/results/`；系统画像和瓶颈定位归 `motivation/results/`。

## 运行规则

- 使用固定镜像标签，不使用 `latest` 或浮动 `18` 标签。
- 数据保存在 Docker named volume 中；普通 `down` 不删除数据。
- 未经用户明确同意，不运行 `down --volumes`，不删除 named volume。
- 修改 PostgreSQL 大版本、扩展版本或数据目录前，先说明兼容性与迁移影响。
- pgai/Ray/模型服务优先作为独立 worker/service 部署，不默认塞进数据库容器。

## 验证要求

环境变更后至少验证：

1. 容器 health 为 `healthy`。
2. `postgres --version`。
3. `vector` 扩展版本。
4. 一个最小向量距离查询。
5. 项目画像脚本能通过数据库 URL 建表、读数据并写回。

详细部署说明见 `README.md`。
