# deploy/autodl/AGENTS.md

本文件继承根 `AGENTS.md`、`deploy/AGENTS.md` 与 `deploy/runtime/AGENTS.md`，只增加 AutoDL
的网络和持久存储规则。具体命令、路径与迁移校验见本目录 `README.md`。

## 网络

- 是否使用 turbo 按**最终访问域名**判断，不按 `git`、`pip`、`wget` 或 Python API 等命令名判断。
  AutoDL 官方当前列出的内置加速域名是 `github.com`、`githubusercontent.com`、
  `githubassets.com` 和 `huggingface.co`；访问这些域名前，若 `/etc/network_turbo` 存在，
  必须在执行命令的**同一 shell 会话**中加载它。
- 依赖 turbo 的 Git 操作使用 HTTPS remote；不假定 SSH remote 会经过 HTTP(S) 代理。
  `pip` 若直接下载上述域名上的资源，同样使用 turbo；访问 PyPI、Conda 或其他未列入官方
  支持清单的站点前取消代理，改用 runbook 指定的镜像或直连路径。不要硬编码 turbo 代理地址。

## 磁盘分工

本节适用于安装、下载、导入、构建、服务运行、实验输出和数据迁移，不只适用于 PostgreSQL。
执行任何可能产生大文件或持续写入的操作前，先确认系统盘与数据盘的实际挂载点、可用空间和目标路径。

| 存储位置 | 应放内容 | 不应作为 |
|---|---|---|
| 系统盘 `/` | 操作系统、apt/系统级软件二进制、平台基础 Conda、`/etc` 下的服务配置，以及有上限并轮转的小型系统日志 | 数据库数据、模型、数据集、项目虚拟环境、缓存、运行日志或实验产物的落盘位置 |
| 数据盘 `/root/autodl-tmp` | 项目仓库与仓库外 runtime env、PGDATA/WAL/tablespace、模型、原始数据集、项目虚拟环境、下载/包/模型/JIT/构建缓存、服务与 runner 日志、实验产物、下载中的临时文件和迁移暂存副本 | 唯一的灾难恢复备份 |
| 独立备份位置 | 重置、换实例或迁移前保存的可恢复备份 | 与源数据位于同一数据盘的普通副本 |

- AutoDL 数据盘在普通关机后保留，但不随系统镜像保存；“关机后还在”不等于已备份。
- 新增路径时按内容的体量、增长性和恢复责任归类，不因安装工具的默认路径在系统盘就继续使用默认值。

### PostgreSQL 对磁盘规则的应用

- PostgreSQL 软件与配置留在系统盘，`data_directory` 指向数据盘。`postgres` 必须能穿越
  所有父目录，PGDATA 由 `postgres` 拥有并保持 `0700`；使用最小 execute-only ACL，不放宽 `/root` 整体权限。
- PostgreSQL 迁移按失败关闭流程执行：干净停库 → 完整复制 → checksum/文件清单一致
  → 切换 `data_directory` → 启动、连接、行数与表/索引一致性检查。验证通过前保留旧副本；只迁移存储路径时不改数据库连接串。

## 记录边界

- 单台服务器的容量、主机身份和迁移校验记录保存在仓库外 artifact 目录。只有可复用部署约定变化才更新项目 runbook 和 `PROJECT_LOG.md`。
