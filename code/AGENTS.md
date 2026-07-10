# code/AGENTS.md

本目录用于后续正式课题工程代码。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 存放可复用的系统实现，而不是一次性 benchmark。
- 未来可承载 fake/real `AI_EMBED` pipeline、PostgreSQL connector、Ray/Daft/Lance 集成、优化实现和测试。

## 建议结构

- `src/`：可复用实现代码。
- `tests/`：单元测试和小型集成测试。
- `scripts/`：运行实验、生成数据、启动服务的脚本。
- `configs/`：实验配置。

## 工程规则

- 先把动机实验跑通，再迁移稳定代码到本目录。
- 不在这里堆一次性 notebook 或临时 CSV。
- 所有代码应有可运行入口、配置、结果输出和最小测试。
- 不提前引入复杂框架；只有当重复逻辑稳定出现时才抽象。

## 当前状态

已新增 PostgreSQL AI 算子外部执行链路画像入口：

- `scripts/postgres_ai_operator_profile.py`

该脚本兼容 PostgreSQL 18，用于公司 PostgreSQL 18.3 内部验证平台或本地
PostgreSQL 18.4 同构预演链路，采集数据库触发、外部 worker、Arrow
RecordBatch、Ray actor、AI operator invocation、fan-in、writeback 和 bounded
backpressure 指标。每条真实运行 CSV 必须记录实际 `server_version` 和
`pgvector_version`，不能靠目录名推断实验平台。

当前本机已通过 Docker 运行 PostgreSQL 18.4 + pgvector 0.8.2 同构预演实例，
连接地址和操作说明见 `deploy/postgres18.4/README.md`。数据库基础连通性、
向量查询和 256 行项目画像链路均已验证，正式 CSV 与三张表核对结果见
`validation/results/postgres18_local_environment_validation.md`。这仍是一次
fake embedding 冒烟运行，最终必须在公司 PostgreSQL 18.3 内部平台复验。

代码入口、函数映射、运行命令和结果位置见 `scripts/README.md`。
