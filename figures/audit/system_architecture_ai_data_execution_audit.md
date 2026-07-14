# 系统架构图质检记录

图文件：

- `figures/architecture/system_architecture_ai_data_execution.svg`
- `figures/architecture/system_architecture_ai_data_execution.png`

## 图类型

Solution Overview / System Architecture。

该图用于解释开题报告、开题 PPT、learning、中期汇报和毕业论文中的总体研究框架：数据库提供 AI workload 入口和结果落点，Daft/Arrow、Ray execution、GPU model service 和 AI data sink 共同构成可观测、可消融的数据执行与存储协同链路。

## 本轮修改

- 将主链路统一放在水平通道中，箭头只连接相邻组件，避免遮挡下方研究内容框。
- 将每个阶段框内的子内容改为规整的小标签，标题居中，子标签等宽排列。
- 使用编号 `1`、`2`、`3` 对应三项研究内容，减少跨区域长箭头。
- 将“观测与策略层”与下方四个执行阶段对齐，四根下指箭头落在对应阶段中心线。
- 调整阶段编号位置，避免圆形编号越出边框或遮挡 `GPU model service` 等较长标题。

## 质检结果

- Vector format：通过。已生成 SVG，可用于报告和后续 Word/PPT 转写。
- PNG preview：通过。已生成 PNG，便于 PPT 和飞书预览。
- Font size：通过。标题、模块标题、子标签和图注在 PNG 预览中可读。
- Alignment：通过。主组件、观测层、指标条和研究卡片使用统一网格对齐。
- Arrow clarity：通过。主数据流箭头和观测层下行箭头不遮挡文字或卡片内容。
- Label spacing：通过。观测层标签等距排列，研究卡片编号与标题分离。
- Badge placement：通过。阶段编号位于框内，不越出边框，也不遮挡主标题文字。
- Colour-blind safety：基本通过。使用蓝、绿、橙、紫和灰，并通过位置、标题和编号双重编码。
- Self-contained caption：通过。图注说明研究对象是数据库驱动 AI workload 进入外部数据执行与存储链路后的系统结构。
- Chartjunk：通过。无 3D、阴影、装饰性渐变和无意义背景元素。

## 后续可改进

- 用于最终 PPT 时，可根据模板版心微调画布比例，并把图注改为 PPT 页脚或讲稿备注。
- 用于 Word 正式报告时，可优先插入 SVG，并在正文图题中解释 `Database source`、`Daft + Arrow`、`Ray execution`、`GPU model service` 和 `AI data sink` 的关系。
- 如果后续 Lance 实验结果成为重点，可将 `AI data sink` 模块拆分为 Lance 与 PostgreSQL / pgvector 两个并列落点。
