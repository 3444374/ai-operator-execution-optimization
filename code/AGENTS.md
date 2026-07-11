# code/AGENTS.md

本目录用于可复用工程代码和实验驱动脚本。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 存放可复用系统实现，而不是一次性 notebook 或临时 CSV。
- 承载 PostgreSQL connector、fake/real `AI_EMBED` pipeline、Ray/Daft/Lance 类外部执行链路、优化实现和测试入口。

## 当前入口

当前已有：

```text
code/scripts/postgres_ai_operator_profile.py
```

该脚本兼容 PostgreSQL 18.3 内部验证平台或本地 PostgreSQL 18.4 同构预演链路，用于采集：

- 数据库触发；
- 外部 worker；
- Arrow RecordBatch；
- Python baseline；
- Ray actor；
- AI operator invocation；
- bounded in-flight / queue wait；
- fan-in；
- writeback；
- `server_version` 与 `pgvector_version`。

脚本已支持：

- `--executor python|ray_actor`
- `--strategy fine|coalesced`
- `--warmup-runs`
- `--repeats`
- `--experiment-id`

## 结果位置

- 连接验证：`validation/results/pg18_4_connection_validation.md`
- 连接冒烟 CSV：`validation/results/pg18_4_connection_smoke_256_rows.csv`
- 系统画像：`motivation/results/pg18_4_system_profile_fake_ai_embed.md`
- 系统画像 CSV：`motivation/results/pg18_4_system_profile_fake_ai_embed.csv`

## 工程规则

- 所有代码应有可运行入口、配置、结果输出和最小验证。
- 不提前引入复杂框架；只有当重复逻辑稳定出现时才抽象。
- 不在 `code/` 堆临时 CSV 或一次性 notebook。
- 每条真实运行 CSV 必须记录实际 `server_version` 和 `pgvector_version`，不能靠目录名推断平台。
- Python baseline、Ray task、Ray actor、模型服务等 baseline 要尽量共享同一数据读取和写回路径，避免 baseline 不可比。
