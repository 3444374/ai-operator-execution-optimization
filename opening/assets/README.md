# 开题素材目录

本目录存放开题 PPT 和报告可复用素材，包括 SVG 图、流程图、实验结果图、表格和模板说明。

核心图规划见：

```text
opening/assets/figure_plan.md
```

规则：

- 架构图、路线图和流程图优先用 SVG 或 PPT 原生形状。
- 实验结果图优先从真实 CSV 生成，并保留生成脚本或来源说明。
- 不保存低清截图作为正式图。
- 图中只放关键词、坐标、图例和必要标签，解释文字放报告或 PPT 备注。

## ECharts 图表

实验结果图可以使用 ECharts 作为可复现图表生成器。推荐流程是：

```text
ECharts option -> SSR SVG -> sharp 转 PNG -> PPT/Word 引用图表文件
```

详细规则见：

```text
opening/assets/echarts_rules.md
```

约定输出目录：

```text
opening/assets/charts/
```

PPT 优先使用 PNG，Word / 报告优先使用 SVG。
