# Runtime Strategy Control Loop Figure Audit

生成日期：2026-07-15

图文件：

```text
figures/architecture/runtime_strategy_control_loop.png
figures/architecture/runtime_strategy_control_loop.svg
figures/architecture/runtime_strategy_rule_table.png
figures/architecture/runtime_strategy_rule_table.svg
figures/scripts/generate_runtime_strategy_control_loop.py
```

## 图的角色

类型：策略机制图，拆成两张图使用。

- `runtime_strategy_control_loop.*`：用一个 `AI_EMBED` 查询例子说明运行时观测信号、上游执行动作和端到端反馈闭环。
- `runtime_strategy_rule_table.*`：单独说明“观测信号 -> 候选动作 -> 保护约束”的待验证策略逻辑。

这组图回答“策略具体怎么运行”，不承担完整研究路线说明。完整研究路线仍由 `cross_layer_method_framework.*` 承担。

2026-07-15 更新：`runtime_strategy_control_loop.*` 已补入开题报告 §4.2 作为图 4-2，用于替代原 Mermaid 链路示意，重点解释三层策略在一次数据库 AI 算子执行中的观测、动作和端到端反馈。

## 设计依据

依据：

- `figures/audit/local_reference_figure_reading_notes.md`
- `figures/audit/strategy_figure_micro_design_points.md`
- `experiments/plans/strategy_design_literature_basis.md`

吸收的图形经验：

- 用一个具体运行例子锚定机制，不把所有模块平铺。
- 每个执行阶段用“观测量 / 调节项 / 判定项 / 约束项 / 评价项”等正式标签说明信号作用，避免画成字段列表。
- 将策略动作贴近实际执行位置，例如批量/分区、`K_max`、路由策略、服务端 `micro-batch` 和写回占比；其中批量/分区作为执行前计划层配置，动态 batch 放在模型服务侧请求队列，不重切数据库侧已物化批次。
- 将数据流、控制流和反馈流分开表达。
- 规则表独立成图，避免主图过大和内容割裂。
- 规则表只作为候选策略说明，不写成已由实验验证的最终策略。

## 版式检查

| 检查项 | 结果 | 说明 |
|---|---|---|
| 中文化 | Pass | 可见标签已尽量中文化，仅保留 `AI_EMBED`、`SQL`、`GPU`、`K_max`、`P99`、`token` 等必要技术记号 |
| 一个具体例子 | Pass | 使用 `AI_EMBED` 查询贯穿闭环 |
| 框内内容作用 | Pass | 主流程框均采用“观测量 / 调节项 / 判定项 / 约束项 / 评价项”标签，说明这些信号如何驱动策略 |
| 策略动作贴近执行位置 | Pass | 批量/分区、`K_max`、路由策略、`micro-batch` 和写回占比分别贴在对应阶段下方；动态 batch 位于模型服务侧 |
| 数据流与反馈流区分 | Pass | 实线表示数据批次流动，虚线主要表示门控、路由和反压参数更新 |
| 规则表拆分 | Pass | 规则表独立为 `runtime_strategy_rule_table.*`，主图不再承载大表格；标题已标注为候选策略 |
| 写回定位 | Pass | 写回作为保护约束和端到端收益检查，不写成当前唯一主贡献 |
| 禁用术语 | Pass | SVG 中未出现 `RC`、`BL`、`边界确认`、`联合决策面`、`Workload 入口`、`图形借鉴`、未解释 `vs` 和旧英文标签 |
| 边框遮挡 | Pass | 脚本自检显示所有核心卡片和表格单元格边框均在画布内且可见 |
| 箭头越界 | Pass | 主数据流箭头均位于相邻卡片之间；反馈箭头已接到策略控制器右边缘，配置箭头未压住卡片边框 |
| PNG 人工预览 | Pass | 两张 PNG 预览中未见文字、箭头、边框明显遮挡 |

## 与旧图关系

| 图 | 当前定位 | 建议 |
|---|---|---|
| `cross_layer_method_framework.*` | 研究方案总览图 | 保留，用于回答“我要做什么” |
| `upstream_strategy_design.*` | 策略设计过渡图 | 可作 PPT 过渡页或备份页 |
| `optimization_strategy_logic.*` | 旧规则表草图 | 信息密度偏高，降级为内部备忘 |
| `runtime_strategy_control_loop.*` | 策略机制主图 | 建议作为策略设计主图之一 |
| `runtime_strategy_rule_table.*` | 候选策略规则表 | 仅用于解释待验证的触发逻辑，不作为已证明结论 |

## 后续可优化点

- 如果后续模型服务调度数据成为主贡献，可单独画 `K_max -> 模型服务队列 -> GPU 服务` 的反压小图。
- 如果写回实验显示写回占比长期较高，可单独画写回路径对比图。
- 如果 `AI_FILTER` 或 `AI_COMPLETE` 数据跑通，可扩展规则表，但闭环主图仍建议保留 `AI_EMBED` running example。
