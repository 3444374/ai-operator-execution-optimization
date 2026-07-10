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

该脚本用于 PostgreSQL 18.3 内部验证平台或同构预演链路，采集数据库触发、外部 worker、Arrow RecordBatch、Ray actor、AI operator invocation、fan-in、writeback 和 bounded backpressure 指标。

当前机器缺少 PostgreSQL/psycopg/Docker daemon，暂时只能做 dry-run 和语法验证；拿到 PostgreSQL 18.3 或同构实例后再运行真实画像。
