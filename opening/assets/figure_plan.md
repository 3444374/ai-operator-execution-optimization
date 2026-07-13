# 开题核心图规划

本文件记录开题报告、PPT 和飞书文档需要维护的核心图。图的目标是说明研究问题、系统边界、方法框架和实验依据，不用于堆砌装饰。

## 必须优先完成的图

| 图 | 位置 | 类型 | 核心结论 | 当前状态 |
|---|---|---|---|---|
| 课题总体研究框架图 | 报告第 3 节、PPT 技术路线/研究内容页、飞书开题文档 | solution overview / schematic-led composite | 三类数据库 AI 算子共享同一条可观测批处理执行过程，三项研究内容分别作用在模型服务感知调度、写回协同和策略边界上 | 已在 `opening/report/opening_report.md` 以 Mermaid 源稿加入 |
| 端到端执行路径与阶段画像图 | 报告第 4 节、PPT 可行性页、飞书实验依据 | motivated example / system pipeline | 当前已经跑通 PostgreSQL -> batch -> Ray/Python -> GPU endpoint -> fan-in -> writeback 的实验链路 | 已有 Mermaid 草图，后续转 SVG/PNG |
| 可行性实验关键结果图组 | 报告第 4 节、PPT 实验依据页 | experimental results | batch 粒度、endpoint routing 和 writeback 都会影响端到端性能 | 已有 Mermaid / xychart 草图，后续用真实数据生成 SVG/PNG |

## 可后续补充的图

| 图 | 适用位置 | 使用条件 |
|---|---|---|
| 三类 AI 算子 workload 对比图 | 研究背景或研究内容 | 当需要解释 `AI_EMBED`、`AI_FILTER/AI_CLASSIFY`、`AI_COMPLETE` 为什么不是三个独立方向时使用 |
| 写回形态对照图 | 技术路线或后续实验计划 | 当补 driver fan-in、worker-side writeback、queue worker 写回实验设计时使用 |
| 策略选择矩阵图 | 预期成果或答辩页 | 当三类 workload 的指标和边界进一步稳定后使用 |

## 设计规则

- 大图先服务研究问题，不先追求复杂视觉效果。
- 每张图必须有一句核心结论，图题第一句直接说明这张图证明或解释什么。
- 报告 / DOCX 优先使用 SVG；PPT 优先使用 PNG 或 PPT 原生形状。
- 实验结果图必须来自真实 CSV、明确的数据表或报告中已列出的正式均值。
- 不用低清截图作为正式图。
- 总体框架图和技术路线图应保持术语一致：少用“外部链路”，优先使用“执行过程”“执行路径”“阶段画像”“模型服务感知批处理执行”“写回协同”等正式表述。
