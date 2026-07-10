# deploy/postgres18.4/AGENTS.md

本目录维护本地 PostgreSQL 18 同构预演环境。进入本目录前先读根目录
`AGENTS.md`。

## 定位

- 当前容器实际运行 PostgreSQL 18.4 + pgvector 0.8.2。
- 它用于预演数据库触发、外部 worker、AI 算子和写回链路。
- 公司内部 PostgreSQL 18.3 才是最终真实验证平台。
- 本目录产生的数据和性能结果不得写成 PostgreSQL 18.3 平台结果。

## 运行规则

- 使用固定镜像标签，不使用 `latest` 或浮动的 `18` 标签。
- 数据保存在 Docker named volume 中；普通 `down` 不删除数据。
- 未经用户明确同意，不运行 `down --volumes` 或删除 named volume。
- 修改 PostgreSQL 大版本、扩展版本或数据目录前，先说明兼容性与迁移影响。
- pgai/Ray/模型服务优先作为独立 worker/service 部署，不默认塞进数据库容器。

## 验证要求

环境变更后至少验证：

1. 容器 health 为 `healthy`；
2. `postgres --version`；
3. `vector` 扩展版本；
4. 一个最小向量距离查询；
5. 项目画像脚本能通过数据库 URL 建表、读数据并写回。
