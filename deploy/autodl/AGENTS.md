# deploy/autodl/AGENTS.md

本文件继承根 `AGENTS.md`、`deploy/AGENTS.md` 与 `deploy/runtime/AGENTS.md`，只增加 AutoDL
的网络和持久存储规则。具体命令、路径与迁移校验见本目录 `README.md`。

## 网络

- AutoDL 上通过 HTTP(S) 访问 GitHub 或 Hugging Face 前，若 `/etc/network_turbo` 存在，
  必须在执行下载命令的**同一 shell 会话**中加载它。依赖 turbo 的 Git 操作使用 HTTPS
  remote；不假定 SSH remote 会经过 HTTP(S) 代理。
- turbo 只用于平台明确支持的 GitHub/Hugging Face 下载。`pip` 和其他站点使用
  runbook 指定的镜像或直连路径。

## 持久存储

- 初始化、导入或迁移前，先确认系统盘/数据盘的实际挂载点、可用空间与目标路径。
- 系统盘只保留操作系统、软件二进制、服务配置和有界且轮转的小日志。大体量或持续增长的
  PGDATA（默认包含 WAL）、模型、原始数据集、虚拟环境、缓存、服务/实验日志、实验产物和本地迁移暂存副本放数据盘。
- PostgreSQL 软件与配置留在系统盘，`data_directory` 指向数据盘。`postgres` 必须能穿越
  所有父目录，PGDATA 由 `postgres` 拥有并保持 `0700`；使用最小 execute-only ACL，不放宽 `/root` 整体权限。
- AutoDL 数据盘的关机保留不等于备份，也不随系统镜像保存。重置、换实例或迁移前必须在独立存储上保存可恢复备份；同数据盘副本只用于迁移回滚。
- PostgreSQL 迁移按失败关闭流程执行：干净停库 → 完整复制 → checksum/文件清单一致
  → 切换 `data_directory` → 启动、连接、行数与表/索引一致性检查。验证通过前保留旧副本；只迁移存储路径时不改数据库连接串。

## 记录边界

- 单台服务器的容量、主机身份和迁移校验记录保存在仓库外 artifact 目录。只有可复用部署约定变化才更新项目 runbook 和 `PROJECT_LOG.md`。
