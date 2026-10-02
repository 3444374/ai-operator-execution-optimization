# 开题素材目录

本目录存放开题 PPT 和报告的模板、说明和非图表素材。项目级图资产已经统一迁移到根目录 `figures/`，供 opening、learning、中期汇报和毕业论文共同复用。

核心图规划见：

```text
figures/audit/figure_plan.md
```

正式图表、图表选择说明、审计记录和绘图脚本见：

```text
figures/README.md
figures/data/selected_motivation_figures.md
figures/audit/experiment_charts_audit.md
figures/scripts/README.md
```

规则：

- 架构图、路线图和流程图优先使用 SVG 或 PPT 原生形状。
- 实验结果图优先从真实 CSV 生成，并保留生成脚本或来源说明。
- 不保存低清截图作为正式图。
- 图中只放关键词、坐标轴、图例和必要标签，解释文字放报告正文或 PPT 备注。
- 开题报告、飞书文档和 PPT 不应把所有实验图都堆进主线；主线优先使用系统架构图和真实 GPU-backed 实验图，PG18.4 fake、fake/CPU 和 feasibility 图主要作为附录、答辩备用或补充说明。
