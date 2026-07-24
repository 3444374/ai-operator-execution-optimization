# motivation/AGENTS.md

本目录维护动机场景、AI 算子形态和端到端动机测试计划。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 定义为什么这个课题值得做。
- 把系统 microbenchmark 连接到数据库 AI 算子、批量 embedding、RAG 数据准备等业务场景。
- 规划 fake AI 算子到 PostgreSQL 18.3 内部验证平台的真实画像路径；普通 PostgreSQL + pgvector 只作为平台暂不可用时的同构预演替身。
- 在具体优化方向未定时，优先通过实验数据比较后再收敛优化方向。

## 实验原则

- fake `AI_EMBED(text)` 只用于隔离系统瓶颈的早期验证。
- 后续必须接 PostgreSQL 18.3 真实数据库形态或真实 AI 算子接口验证，不长期停留在模拟脚本。
- 每个动机实验都要输出端到端指标：rows/s、object_count、batch size、fan-in time、write time。
- 动机实验正式结果放在 `motivation/results/`（唯一来源），不再同步到 `feasibility/results/`。
- 如果端到端链路不再显示 object/fan-in 瓶颈，要及时回退并调整课题。
- 不预设最终优化方向。Object Transfer、fan-in、Shuffle、batching、partition、模型服务调用、批量推理前后处理、向量写回、scan/filter/pushdown 都必须通过实验数据比较后再收敛。
- 动机测试必须从真实或合理近似的 AI 算子 workload 出发，不能只为了证明某个系统优化点而构造 toy workload。
- 对每个候选场景，先写清楚：真实用户是谁、AI 算子是什么、输入输出是什么、为什么需要分布式/批处理、可能瓶颈在哪里、什么结果会推翻该方向。
- 对外部建议和生成文本保持批判性使用：可以吸收问题定义、反证条件和实验建议，但不能把其中未验证判断写成事实。尤其是 PostgreSQL / 开源数据库出发的离线 LLM 场景，必须先核查具体系统、接口和维护状态，再决定是否作为主场景。
- “粒度控制”只是入口，不应成为全部课题。后续动机实验应逐步验证 AI 算子特征是否能驱动 task/actor 并行度、CPU/GPU 或 CPU/model-service 资源配比、模型服务路由、数据局部性和 backpressure 决策。

## 当前状态

真实 GPU-backed E2E 主动机已完成（`motivation/results/gpu/`），已说明"为什么值得做"：

- AI_EMBED 真实链路：1024 行下 fine/coalesced 端到端约 `13.4x`；16384 行下 operator 与 writeback 均为大块成本（`ai_embed_chain_breakdown_20260712.md`）。
- 双 endpoint 下 Ray task/actor 体现并发 routing 价值，端到端收益仍受 writeback 约束（`multi_endpoint_ray_motivation_20260712.md`）。
- 方向已收敛（2026-07-16）到上游调度优化：数据组织 + 提交控制（详见根 `AGENTS.md` §1、`PROJECT_OUTLINE.md`）。

早期 fake `AI_EMBED(text)` 链路（`motivation/results/fake_cpu/`）仅作历史预研参考，不再是当前动机依据。

动机测试正式结果位于：
- `motivation/results/gpu/`：真实 GPU-backed E2E 主动机结果
- `motivation/results/cpu/`：CPU baseline 对照
- `motivation/results/pg18_4_fake/`：PG18.4 同构预演
- `motivation/results/fake_cpu/`：CPU/fake 历史预研

## 当前下一步

主动机已完成，"为什么值得做"已回答。方法有效性验证转入 `experiments/`（见 `experiments/plans/experiment_status_and_gaps.md`）。motivation 侧仅按需补充：

1. 若 `experiments/` 需要新的 GPU-backed 动机信号（如新场景瓶颈画像），在 `motivation/results/gpu/` 补充。
2. 进入 PostgreSQL 18.3 内部平台后，补充真实平台画像，确认数据库触发→外部 worker→AI 算子→Ray/Arrow→fan-in/writeback/backpressure 指标如何采集。
3. 不再围绕 fake/CPU 链路或"方向未定"展开新动机实验。
