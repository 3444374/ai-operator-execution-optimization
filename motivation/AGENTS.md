# motivation/AGENTS.md

本文件继承根 `AGENTS.md`，只增加动机场景与系统画像规则。当前状态和优先结果见 `README.md` 与
`results/README.md`，不写入规则文件。

## 1. 职责

- 回答“数据库 AI workload 的哪类执行现象值得研究”，连接真实用户、SQL/AI operator、外部模型
  服务、数据组织、提交、fan-in 和 writeback。
- `benchmarks/` 保存动机实验脚本，`plans/` 保存场景/画像计划，`results/` 保存端到端画像与解释。
- 新画像可以产生研究假设，但方法有效性、策略排名和正式消融进入 `experiments/`。

## 2. 证据边界

- GPU-backed database-E2E 是主动机的主要证据；CPU、fake、PG18.4 fake 只按对应子目录规则作为
  对照、预演或历史假设来源。
- 每个场景先写明真实用户、operator 输入输出、为什么需要批处理/分布式/外部服务、候选瓶颈和
  会推翻该方向的结果。
- 不为证明某个既定优化点构造 toy workload；候选瓶颈必须通过阶段计时、资源与质量数据比较。
- 数据库触发、模型执行和写回的实际所有者必须写清；普通 PostgreSQL/pgvector 预演不能写成
  PostgreSQL 18.3 平台或数据库内模型执行证据。
- 动机结果说明“现象存在/值得继续研究”，不写成 proposed 方法已经有效。

## 3. 运行与报告

- 新实验写明问题、命令、配置签名、数据、CSV/raw、阶段边界、质量、资源、结论和不能声称内容。
- 至少区分 DB/source、Arrow/serialization、submit/put、queue/model service、fan-in 和 writeback；
  warm-up、失败与重试必须标记。
- 正式结果只写入 `motivation/results/`；连接验证和纯组件 microbenchmark 分别进入
  `feasibility/results/` 与 `feasibility/benchmarks/`。
- 画像改变课题主线时，同步 `PROJECT_OUTLINE.md`、实验证据台账、相关学习/开题材料和
  `PROJECT_LOG.md`；只新增背景时不机械修改所有入口。
