# 项目图资产规划

本文件记录 learning、开题报告、开题 PPT、中期汇报和毕业论文共同复用的核心图。正式图文件统一放在根目录 `figures/`，不再分散维护在 `opening/assets/` 或 `learning/figures/`。

## 必须优先维护的图

| 图 | 位置 | 类型 | 核心结论 | 当前状态 |
|---|---|---|---|---|
| 课题总体研究框架图 | 报告第 3 节、PPT 技术路线/研究内容页、learning 总览、中期和论文绪论 | solution overview / system architecture | 数据库 AI 负载 进入 Daft/Arrow、Ray、GPU model service 和 sink 后形成可观测、可消融的数据执行与存储协同链路 | 已维护于 `figures/architecture/` |
| 研究方案图 | 报告第 4 节、PPT 研究方案页、论文方法章节开头 | research plan / method overview | 三类 AI workload 先做分阶段性能剖析，再通过计划层数据组织、运行层提交路由和服务端动态批处理形成三层上游执行策略，并用写回纳入端到端评价 | 已维护于 `figures/architecture/cross_layer_method_framework.*` |
| 运行时策略闭环图 | 报告第 4 节、PPT 策略机制页、论文方法章节 | mechanism figure / running example | 用一个 `AI_EMBED` 查询解释观测信号、`K_max`、`routing policy`、服务端 `micro-batch` 和写回 guardrail 如何协同，不重切数据库侧已物化 batch | 已维护于 `figures/architecture/runtime_strategy_control_loop.*` |
| 数据库到 GPU 再到写回的链路阶段时延图 | 报告第 4 节、PPT 可行性页、飞书实验依据、论文动机实验 | staged latency breakdown | 真实 GPU-backed 链路中，GPU request、fan-in 和 sink writeback 都会影响端到端时间 | 已维护于 `figures/data/report_main/02_gpu_stage_latency_stack.*` |
| 调用粒度与 workload 场景对比图 | 报告第 4 节、PPT 动机页、答辩追问 | motivation evidence | fine/coalesced、batch、endpoint routing 和 writeback 对 AI workload 的端到端表现有实测影响 | 主图在 `figures/data/report_main/`，补充图在 `figures/data/backup/` |

## 可后续补充的图

| 图 | 适用位置 | 使用条件 |
|---|---|---|
| 三类 AI 算子 workload 对比图 | 研究背景、研究内容或 PPT 备份页 | 当需要解释 `AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE` 为什么共享同一执行链路问题时使用 |
| 写回形态对照图 | 技术路线、后续实验计划、中期汇报 | 当补充 driver fan-in、worker-side writeback、queue worker 写回实验设计时使用 |
| 策略选择矩阵图 | 预期成果、论文方法章节、答辩页 | 当三类 workload 的指标和边界进一步稳定后使用 |

## 设计规则

- 大图先服务研究问题，不先追求复杂视觉效果。
- 每张图必须有一句核心结论，图题或图注直接说明它证明或解释什么。
- 报告和 DOCX 优先使用 SVG，PPT 和飞书预览优先使用 PNG。
- 实验结果图必须来自真实 CSV、明确的数据表或报告中已列出的正式均值。
- 不用低清截图作为正式图。
- 总体框架图和技术路线图应保持术语一致：少用口语化的“外部链路”，优先使用“执行过程”“执行路径”“分阶段性能剖析”“模型服务感知批处理执行”“写回瓶颈判定”等正式表述。
