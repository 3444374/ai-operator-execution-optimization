# 系统架构图质检记录

图文件：

- `figures/architecture/system_architecture_ai_data_execution.svg`
- `figures/architecture/system_architecture_ai_data_execution.png`

## 图类型

Solution Overview / System Architecture。

该图用于解释开题报告、开题 PPT、learning、中期汇报和毕业论文中的总体研究框架：数据库提供 AI workload 入口和结果落点，Daft/Arrow、Ray execution、GPU model service 和 AI data sink 共同构成可观测、可消融的数据执行与存储协同链路。

## 本轮修改（2026-07-14）

- 将主链路统一放在水平通道中，箭头只连接相邻组件，避免遮挡下方研究内容框。
- 将每个阶段框内的子内容改为规整的小标签，标题居中，子标签等宽排列。
- 使用编号 `1`、`2`、`3` 对应三项研究内容，减少跨区域长箭头。
- 将”观测与策略层”与下方四个执行阶段对齐，四根下指箭头落在对应阶段中心线。
- 调整阶段编号位置，避免圆形编号越出边框或遮挡 `GPU model service` 等较长标题。

## 本轮修改（2026-07-15，同步三层策略设计）

开题报告将策略设计收敛为三层上游执行策略：计划层确定数据库侧数据批量与分区，运行层控制 Ray 提交门控、路由和反压，服务端利用请求队列形成动态 `micro-batch`。写回阶段用于判断上游调优收益是否被持久化成本吞噬，不写成当前独立主贡献。架构图同步更新：

- 主链路阶段标题尽量中文化，仅保留 `PostgreSQL`、`Daft + Arrow`、`Ray`、`GPU`、`Lance`、`pgvector` 等必要技术名。
- 观测与策略层新增 `micro-batch`，并将 `stage time / queue wait` 等泛化标签改为中文指标。
- 研究内容 2 明确包含 `K_max` 门控、`endpoint routing` 和服务端 dynamic `micro-batch`。
- 研究内容 3 标题改为 `写回瓶颈判定与轻量优化`，子项改为 `writeback ratio 判定`、`driver / worker / queue 写回`、`Lance / pgvector / PostgreSQL`。
- 图注更新：明确研究内容 1、2 分别关注计划层数据组织、运行层入口调度与服务端动态批处理；研究内容 3 用于判定写回是否吞噬上游收益。

## 本轮修改（2026-07-15 第二次，修正颜色语义）

- 底部三个研究内容卡片统一改为中性色，避免读者误以为它们和上方蓝/绿/橙/紫系统阶段存在一一颜色映射。
- 研究内容 2 标题从 `GPU 服务感知 Ray 调度` 改为 `运行层调度与服务端批处理`，更准确表达当前方案横跨 Ray 入口调度、endpoint routing 和模型服务侧 `micro-batch`。
- 保留上方系统阶段的颜色编码：蓝色表示数据层，绿色表示 Ray 执行层，橙色表示 GPU 模型服务，紫色表示结果存储。

## 本轮修改（2026-07-17，同步课题方向重构）

课题方向明确收敛到上游调度优化（数据组织策略 + 调度与提交控制策略），主场景切换为 AI_COMPLETE，vLLM 为部署平台。同步更新：

- 标题去除"协同"：`...数据执行与存储架构`。
- GPU 模型服务阶段：子项从 `embedding / classify / LLM / micro-batch` 改为 `LLM 推理 / continuous batching / PagedAttention / prefix caching`，反映主场景 AI_COMPLETE。
- 修复 GPU 模型服务与 Ray 执行层的编号重复 bug：阶段编号改为 1-2-3-4 顺序。
- 观测层标签 `micro-batch` 改为 `服务队列`。
- 研究内容一标题：`数据组织与批处理构造` → `数据组织策略`；子项改为 token-budget 动态批量 / length-aligned 分组 / prefix-aware 分组。
- 研究内容二标题：`运行层调度与服务端批处理` → `运行层调度与提交控制策略`；子项改为 queue-adaptive flush / K_max 动态控制 / actor pool 分池路由。
- 研究内容三标题：`写回瓶颈判定与轻量优化` → `写回瓶颈判定`（去掉"轻量优化"）。
- 图注更新：去掉"计划层""服务端动态批处理"，使用当前策略术语。

## 质检结果

- Vector format：通过。已生成 SVG，可用于报告和后续 Word/PPT 转写。
- PNG preview：通过。已生成 PNG，便于 PPT 和飞书预览。
- Font size：通过。标题、模块标题、子标签和图注在 PNG 预览中可读。
- Alignment：通过。主组件、观测层、指标条和研究卡片使用统一网格对齐。
- Arrow clarity：通过。主数据流箭头和观测层下行箭头不遮挡文字或卡片内容。
- Label spacing：通过。观测层标签等距排列，研究卡片编号与标题分离。
- Badge placement：通过。阶段编号位于框内，不越出边框，也不遮挡主标题文字。
- Colour-blind safety：基本通过。使用蓝、绿、橙、紫和灰，并通过位置、标题和编号双重编码。
- Self-contained caption：通过。图注说明研究对象是数据库 AI 负载 进入外部数据执行与存储链路后的系统结构。
- Chartjunk：通过。无 3D、阴影、装饰性渐变和无意义背景元素。

## 后续可改进

- 用于最终 PPT 时，可根据模板版心微调画布比例，并把图注改为 PPT 页脚或讲稿备注。
- 用于 Word 正式报告时，可优先插入 SVG，并在正文图题中解释数据库入口、`Daft + Arrow` 数据层、`Ray` 执行层、`GPU` 模型服务和 AI 结果存储的关系。
- 如果后续 Lance 实验结果成为重点，可将 `AI data sink` 模块拆分为 Lance 与 PostgreSQL / pgvector 两个并列落点。
