# motivation/AGENTS.md

继承根规则。场景与结果入口见 [README.md](README.md)。

- 本目录回答数据库 AI workload 的哪些执行现象值得研究；方法有效性、策略排名和正式消融进入
  `docs/plans/` 与 `results/`，连接验证和组件 microbenchmark 进入 `feasibility/`。
- 新场景说明真实用户、算子输入输出、外部执行需求、候选瓶颈和可推翻假设的观察；用阶段计时、
  资源与质量数据比较候选原因，不为既定优化点构造 toy workload。
- 主动机以 GPU-backed database-E2E 为主要证据；CPU/fake 只作对照、预演或假设来源。
  写清数据库触发、模型执行和写回的实际所有者，不能把普通 PG/pgvector 预演写成 PG18.3 或库内模型证据。
- 新实验记录问题、命令、配置签名、数据、CSV/raw、阶段起止、质量、资源、结论和不能声称内容。
  阶段至少区分 DB/source、Arrow/serialization、submit/put、queue/model service、fan-in、writeback；
  标记 warm-up、失败与重试。结果只能证明现象存在或值得研究，不能代替方法有效性验证。
- 结果保存在根 `results/motivation/`。画像改变课题主线时，按根 §7 同步总纲、证据台账、相关学习/开题
  材料和日志；仅增加背景资料时更新实际受影响的入口。
