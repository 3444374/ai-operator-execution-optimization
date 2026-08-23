# Cortex AISQL 精读笔记论文原图选择与裁剪审计

日期：2026-08-22

## 范围与证据边界

- 使用位置：`research/精读文献笔记/cortex_aisql_sigmod2026/cortex_aisql_sigmod2026.md`。
- 权威来源：用户本地文献目录中的 `cortex_aisql_sigmod2026.pdf`，题名 *Cortex AISQL: A Production SQL Engine for Unstructured Data*，SIGMOD Companion ’26 正式版。
- 正式 PDF：13 页，SHA256 `DE533CC29FD9B6B8F573E66B26878BD4943ECDFE67AF51A2DE2C6941D4EC6059`；已通过 `%PDF` 签名、题名和页数解析检查。
- 源文件没有复制进 `research/reference/`，因此不改变项目参考 PDF 子集及其计数。
- 这些 PNG 是论文原图的局部裁剪，只服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 1 | 3 | §4.1.2 | 一图定位 Cloud Services、Query Processing、Cortex Platform、内部推理引擎和 partner endpoint，是后续讨论外部执行链路与 scheduler 边界的系统地图。 |
| Figure 7 | 7 | §4.4.1.1 | 用完整计划树显示传统 join-cost 计划的 110,000 次调用如何变成 AI-aware 计划的 330 次调用，文字难以替代其算子位置关系。 |
| Figure 9 | 9 | §5.1.1 | 展示 cheap predicate selectivity 与 AI-aware reordering 收益的连续关系，避免只记住“最高 7×”单点。 |
| Figure 10 | 9 | §5.1.2 | 直接呈现 pull-up 与 push-down 的交叉边界，以及 AI-aware placement 在 measured range 内的稳定表现。 |
| Figure 11 | 10 | §5.2.3 | 同时保留六个数据集的执行时间与 F1，突出 model cascade 的 workload-dependent 速度—质量权衡。 |
| Figure 12 | 11 | §5.3.3 | 同时显示 semantic join rewrite 的数量级加速与各数据集 F1 差异，既支撑主收益也保留 recall/quality 边界。 |

未加入 Figure 2–5：statement 类型、执行时间、credit 和 table 数量分布已经逐项转写为文字或 Markdown 表格；未加入 Figure 6：schema 与用户查询条件已在 §4.4.1 用字段列表和流程重建；未加入 Figure 8：4×6 示例及 24→4 次调用已完整转写。Algorithm 1 和 Table 1–4 同样保留为可检索文字或表格，不重复截图。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `research/精读文献笔记/cortex_aisql_sigmod2026/figures/fig1_cortex_platform_architecture.png` | 1155×562 | `680FD7E9CB5CA937F735D402863116D1CD32BE18344E127CA0FC2A58DD68B2A6` |
| `research/精读文献笔记/cortex_aisql_sigmod2026/figures/fig7_ai_aware_execution_plans.png` | 1134×726 | `A4302F4060FF534F5B1225961556AD7EA9A09C63CECC37584C80A35F98433A3B` |
| `research/精读文献笔记/cortex_aisql_sigmod2026/figures/fig9_predicate_reordering.png` | 1145×695 | `AC475945A49BAA548EFC30B1AB9AB58976979C5174BB330CFAF0D21820DA8814` |
| `research/精读文献笔记/cortex_aisql_sigmod2026/figures/fig10_ai_predicate_join_placement.png` | 1134×695 | `800CA4E020C452CFE9921DA967927CC6995082564C59E5735C199B9051DA7145` |
| `research/精读文献笔记/cortex_aisql_sigmod2026/figures/fig11_adaptive_model_cascades.png` | 2341×808 | `55DD4156134DBF7997FB740B503736D47E4B563A5F4CECCA5348E0902E3AD300` |
| `research/精读文献笔记/cortex_aisql_sigmod2026/figures/fig12_semantic_join_rewrite.png` | 2341×859 | `B46A0270B349FA8DF87E7B927DD1235FD3986D91B2DB21BE8AFA49B204316087` |

提取方式：使用 `pypdfium2` 对正式 PDF 对应页面作 4.5× raster render，再按原图图形边界裁剪。没有重绘、锐化、替换颜色、修改坐标或删除图内元素；论文英文 caption 不进入裁剪件，由精读正文的中文 alt text 和来源行承担说明。

## 视觉与论证 QA

- 6 张图已按原始分辨率预览；组件名、计划节点、调用数、图例、坐标轴、单位、数据集名和数值标注完整可读，没有混入相邻正文或残缺 caption。
- Figure 7 以节点形状、位置和调用数表达差异；Figure 9–12 除颜色外还使用系列位置、线形或数值标签。Figure 1 的厂商标识仅是论文原图内容，不代表项目背书。
- 坐标范围、对数轴、图例和图内数值保持论文原样，没有截轴或视觉夸大。
- 6 张图分别承担架构、计划机制、两个 optimizer 决策边界和两类核心实验，没有收集装饰性图片。
- PNG 仅用于精读笔记显示。若将来进入正式论文或开题材料，应从正式 PDF 取得矢量版本或按投稿规范重新导出。

审计结论：6 张图适合进入当前 Cortex AISQL 精读笔记；0 个 critical、0 个 major、0 个视觉阻断项。
