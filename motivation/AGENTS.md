# motivation/AGENTS.md

本目录维护数据库 AI 算子场景、端到端动机测试、系统画像、瓶颈定位和可优化点分析。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 定义为什么这个课题值得做。
- 把组件 microbenchmark 连接到数据库 AI 算子、批量 embedding、RAG 数据准备、模型服务队列和写回链路。
- 记录 PG18.4 / PG18.3 真实数据库触发链路中的系统画像和瓶颈定位。
- 在具体优化方向未定时，优先寻找最适合用户 AI infra / inference infra 目标的 AI 算子场景。

## 实验原则

- 动机实验必须从真实或合理近似的 AI 算子 workload 出发，不能只为了证明某个系统优化点构造 toy workload。
- 端到端动机结果、系统画像结果和瓶颈定位结果放在 `motivation/results/`。
- 只证明环境可连接的结果不放这里，放 `validation/results/`。
- 每个正式动机实验都要输出 CSV、运行命令、参数解释、严谨性自检、结果解释和不能声称的结论。
- 不预设最终优化方向。Object transfer、fan-in、shuffle、batching、partition、task/actor、模型服务调用、backpressure、vector writeback、scan/filter/pushdown 都必须由实验收敛。
- “粒度控制”只是入口，不应成为全部课题。后续必须验证 AI 算子特征是否能驱动 task/actor 并行度、CPU/GPU 或 CPU/model-service 资源配比、模型服务路由、数据局部性和 backpressure 决策。

## 当前结果

正式动机结果位于：

- `motivation/results/fake_ai_embed_pipeline.csv`
- `motivation/results/ai_operator_scenario_motivation.csv`
- `motivation/results/ai_operator_granularity_attribution.csv`
- `motivation/results/ai_operator_backpressure.csv`
- `motivation/results/pg18_4_system_profile_fake_ai_embed.csv`
- `motivation/results/pg18_4_system_profile_fake_ai_embed.md`
- `motivation/results/motivation_test_results_analysis.md`

PG18.4 相关结果分工：

- `validation/results/pg18_4_connection_validation.md`：只证明系统能连接 PG18.4、读写表和跑通冒烟链路。
- `motivation/results/pg18_4_system_profile_fake_ai_embed.md`：记录 4096 行真实数据库触发链路系统画像，用于分析瓶颈和可优化点。

## 当前判断

当前更稳妥的技术路线表述是：

> 基于 Ray/Daft/Lance 类外部执行链路的数据库 AI 算子批处理系统调优。

不能写成：

- 默认达梦已经采用 Daft+Ray+Lance；
- 论文主线是“Ray vs 非 Ray”；
- PG18.4 fake-model 结果已经证明真实 GPU/PG18.3 平台收益。

Ray / 非 Ray 对比应该作为 baseline 和消融，主线仍是系统瓶颈定位与调优。

## 下一步

优先补 baseline：

1. Python batched worker。
2. Ray task baseline。
3. Ray actor batch size / actor 数。
4. pgvector `vector(128)` 写回。
5. 真实 CPU embedding 小模型。
6. GPU / Ray Serve / vLLM。

只有当场景动机、系统瓶颈、PG18.3 或同构真实链路画像数据和用户 AI infra 学习目标同时对齐时，才把某个优化点写成最终主线。
