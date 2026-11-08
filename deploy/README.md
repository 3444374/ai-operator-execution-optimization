# 部署与运行环境

[runtime](runtime/README.md)声明模型／数据能力和独立环境；[AutoDL](autodl/README.md)保留服务器网络、
磁盘、缓存与现有服务的运行约定。当前实验入口见[SemBench 脚本](../code/scripts/README.md)。
每次运行须绑定实际源码、输入、模型、版本、资源所有权与清理回执；模板不代表已测最优配置。

PG／pgai 的旧 Compose、SQL 与实验部署源已从当前入口归档，未来仍可恢复，见
[准确来源](../experiments/reproduction/paused_pg_and_retired_http_20261105/README.md)。
本次仅归档 Git 源码，不启动、停止或修改任何数据库、容器、数据目录或持久卷。
历史实验配置只在其冻结树中解释；不继续用旧 HTTP 对照脚本装配新的 SemBench。
