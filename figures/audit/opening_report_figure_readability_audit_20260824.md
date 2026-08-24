# 开题报告图件可读性专项审计（2026-08-24）

## 1. 范围与结论

本轮处理报告图 1、图 6、图 9、图 10、图 13、图 14，并同步检查
`opening/report/opening_report.md`；未修改 PPT。结论如下：

- 图 1 的对象改为“AI 语义算子的外部物理执行”，不再把外部物理链路称为“外部 AI 算子”，图内不出现具体语义运行时名称。
- 图 6 将 `running`、`waiting`、`formal`、`graph→gather` 等内部措辞改为自然中文，数据、点位与坐标范围不变。
- 图 9 将共享容量、请求位置 / 工作量空间、多作业队列、文本 / 图像执行、请求补位、完成释放和运行状态字段中文化；结构、连线和方法边界不变。
- 图 10 将左图标题由“结果可直接比较”改为“SQuAD 质量可核对，性能暂不排名”，并在图内说明项目路径的计时还包含指标采集和记录处理；柱形、误差线和实验数值未改变。
- 代价估计原合成图保留为备用，并新增报告图 13a 和图 13b：预测时间与实测时间、配置排序与错误选择后的额外耗时。
- 图 14 保留 `baseline`，把 `setup-dominated`、`operator JCT`、`formal cell` 和“规模门禁”等未在前文解释的简写改为自然中文；数据、分组和数值未改变。

## 2. 权威源与生成链

| 报告图号 | 报告 PNG | 权威可编辑源 | 生成 / 导出方式 | 本轮处理 |
|---:|---|---|---|---|
| 图 1 | `opening/report/figures/fig01_external_ai_operator_assumptions.png` | `figures/architecture/editable/opening_background_20260812/07_traditional_vs_external_ai_operator.drawio` 与同名 SVG | Draw.io 为结构源；本轮同步修改 SVG，并用本机无头 Chrome 从 SVG 重导 1600×900 PNG | 改术语并同步开题图集副本 |
| 图 6 | `opening/report/figures/fig07c_state_fingerprint.png` | `figures/data/report_main/opening_native_single_job_state_fingerprint.{png,svg}` | `figures/scripts/generate_opening_story_figures_20260808.py --figures F` | 改轴名与底部说明，不改数据 |
| 图 9 | `opening/report/figures/fig04_state_aware_scheduling.png` | `figures/architecture/editable/04_state_aware_scheduling.{drawio,svg,png}` | Draw.io / SVG 可编辑源；审计见同目录 `04_state_aware_scheduling.audit.md`；无头 Chrome 重导 PNG | 可见框标题、说明和状态字段中文化 |
| 图 10 | `opening/report/figures/fig06_text_baseline_boundaries.png` | `figures/scripts/generate_opening_story_figures_20260808.py` 中 `figure_text_baseline_evidence_map` | `--figures T` 生成 `opening_text_baseline_evidence_map.{png,svg,pdf}` | 收紧左图比较说明，不改数据 |
| 图 13 | `opening/report/figures/fig14a_cost_prediction_time.png`、`fig14b_cost_ranking_decision_loss.png` | `figures/scripts/generate_opening_story_figures_20260808.py` 中 `figure_cost_decision_v4` | `--figures E` 生成原合成图以及两张拆分图 | 原合成图备用，正文使用图 13a / 13b |
| 图 14 | `opening/report/figures/fig12_image_baseline_boundaries.png` | 同一脚本中的 `figure_image_baseline_evidence_map` | `--figures I` 生成 `opening_image_baseline_evidence_map.{png,svg}` | 只改可见文字，不改数据 |

`opening_target_architecture_status` 与 `fig05_system_architecture.png` 只保留为历史 / 内部候选资产，当前报告不引用，也不纳入本轮报告图输出检查。

## 3. 图 13 拆分与 A4 字号

原合成图 `opening_cost_model_decision_quality_v4.{png,svg,pdf}` 完整保留。新增：

1. `opening_cost_prediction_time_report.{png,svg,pdf}`：六种方法各自的实测时间、预测时间与逐候选差值；画布 11.0×7.4 英寸，图内最小文字 14 pt。
2. `opening_cost_ranking_decision_loss_report.{png,svg,pdf}`：四种工作量上限的两两排序准确率和错误选择后的额外耗时；画布 11.5×6.6 英寸，图内最小文字 14 pt。

按 A4 正文约 165 mm（6.50 英寸）宽度插入时，两图横向缩放比例分别约为 0.59 与 0.57；14 pt 文字对应约 8.3 pt 与 8.0 pt，达到文档插入后的 8 pt 可读下限。报告建议先引用图 13a 解释“时间预测误差”，再引用图 13b 解释“配置排序与决策后果”；原合成图适合整页概览或附录，不再强行缩进同一正文栏。

## 4. 输出尺寸与 SHA-256

| 文件 | 尺寸 | SHA-256 |
|---|---:|---|
| `fig01_external_ai_operator_assumptions.png` | 1600×900 | `342522839f525d6f4f78bea4996d41f489e4d1ef51dd7c9a3d2ae7288960a380` |
| `fig07c_state_fingerprint.png` | 3938×2212 | `5c1f26a7208e1b750ab323f0064156c008822daf12382333ff5af72b30c783aa` |
| `fig04_state_aware_scheduling.png` | 1600×900 | `f717915201afa655773929f0bb77ae3c47b3d43f8ec9b3b96a5f36801c33388a` |
| `fig06_text_baseline_boundaries.png` | 3874×1521 | `7a18a5ea4705be62fca667deba2622b69a7f612e8ff5ff4ac73f6a9ee4b5b3a9` |
| `fig14_cost_decision_quality.png` | 4475×1766 | `12cbce5ac57f925756d21d89fbeac416d7523832236aabce2f4457fc7d3fd419` |
| `fig14a_cost_prediction_time.png` | 3030×2146 | `92217a5c1f26439e46b935b9c70324ec1dbadbbe00d4f28d011010a6c2cda582` |
| `fig14b_cost_ranking_decision_loss.png` | 3395×1921 | `223cca4854d0319693c1197356cd2ade8c9850cbce24ac4d636c75ff85558223` |
| `fig12_image_baseline_boundaries.png` | 3814×1646 | `3ed217e0b8a50f02da94b7df73b79312ed10e73e7de2e97f97b1ade42647dd03` |

拆分图的权威 SVG SHA-256：

- `opening_cost_model_decision_quality_v4.svg`：`3166f3efb79f7e13f51a382f7b26f4e4f1a5b057981ac73a2f4aca6751d58e25`
- `opening_cost_prediction_time_report.svg`：`23dbb4fbab912ce9fa32682abb4cc5f8c209ccb39ca4e46f83b66632b535198f`
- `opening_cost_ranking_decision_loss_report.svg`：`3372bf8c5684bd8fae7dc113d5c390c27458f01226e5b0cf4ceea4fe26092502`
- `opening_native_single_job_state_fingerprint.svg`：`2b17ada131422b89447921ab789692268ce29dbc994b806203db0d0e9fcdbc99`
- `04_state_aware_scheduling.svg`：`cdf268ee0e2958ce01b69edff369a3a969f093e4cc2b86afef941d69d3b9d0ff`
- `opening_text_baseline_evidence_map.svg`：`25d5d1067f95828f05ca9f5ad83893148803e2337c6fad93eda6d7fd76a8a975`
- `opening_image_baseline_evidence_map.svg`：`de0f35f746d728f43bf5efd327c66d8fe05405f74b72dcf1e5fa09ccd74174cc`

## 5. 视觉与结构检查

- 使用 `view_image(detail=original)` 打开图 1、图 6、图 9、代价估计原合成图、图 13a、图 13b 和图 14。
- 图 1：主标题、右侧分区名和底部结论均完整显示；长术语未换行、未碰边；箭头与图标未变化。
- 图 6：六个 small multiples 的标题、中文轴名、点值和底部范围说明仍可读；底部不再出现 `formal` 或 `graph→gather`。
- 图 9：1600×900 下中文模块标题、正文、五类连线和底部图例无裁切或遮挡；该图已满足不低于 18 px 的源图字号合同，图内不再出现 `Shared Credit`、`Fair Queue`、`typed Ray actor`、`Runtime Snapshot`、`ready/running/queued` 或 `Job`。
- 图 10：左图标题和左下说明完整显示，三条柱形及误差数字没有移动；图内不再声称 SQuAD 三条路径可以直接作细微性能排名。
- 图 13a：六个统一坐标小图、误差摘要和底部读图说明无重叠；横纵轴标题完整。
- 代价估计原合成图与图 13b：散点区域不再放置混合模型中位数、平均值、最差值数字或摘要框，也不绘制平均值三角；图外六项图例明确说明单个留出情境、20 个情境中位数、最差情境、混合模型蓝色和 5% / 15% 参考线。分图标题、坐标、图例和底部读图说明分区清楚，无数据点—文字重叠。
- 图 14：`baseline` 保留；自然中文标题、横轴和红色异常说明无裁切，所有数值与原图一致。
- Draw.io 与关键 SVG 均通过 XML 解析；图 6、图 9 未命中上述内部英文标签，图 15 SVG 未命中 `setup-dominated`、`operator JCT` 或 `formal cell`。

最终状态：通过。报告正文已同步本轮术语和实验口径，PPT 未修改。
