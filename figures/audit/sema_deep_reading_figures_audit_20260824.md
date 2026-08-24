# Sema 精读笔记配图审计

日期：2026-08-24

## 1. 来源与版本边界

- 来源文件：`research/reference/sema_vldb2026.pdf`
- 题名：*Sema: A High-performance System for LLM-based Semantic Query Processing*
- 本地版本：arXiv:2603.11622v1，2026-03-12，27 页
- PDF SHA256：`b57530d5f873dd57eeb39aeed244749fb9a499fd2d18f52b3d269b4fdc83bda1`
- 版本边界：本地 PDF 首页、页眉和元数据均未给出正式会议、期刊、卷期或 DOI，因此图号、查询数和分项实验仍按 arXiv v1 解读。VLDB 2026 官方程序已经独立确认该工作被 Research Track 录用；截至 2026-08-24，PVLDB 卷期、页码和 DOI 尚未发布。
- 主笔记：`research/精读文献笔记/sema_vldb2026/sema_vldb2026.md`

## 2. 选图原则与结果

正文 Figure 1–8 分别承担系统结构、查询语法、端到端流程、总体延迟、质量对比、执行优化、AQE 消融和 AQE case study，均能帮助理解正文中的不同论点，因此全部加入。所有图片均从本地 PDF 直接裁切，没有重绘、改色、拼接数据或修改图中文字。

| 图 | PDF 页 | 输出文件 | 插入位置 | 独立作用 |
|---|---:|---|---|---|
| Figure 1 | 2 | `fig1_system_architecture.png` | §5.1 | 定位 Sema 在 DuckDB 各层的扩展点 |
| Figure 2 | 4 | `fig2_running_query.png` | §5.4 | 区分普通 SQL 条件与 NL expression |
| Figure 3 | 5 | `fig3_system_workflow.png` | §5.5 | 串联 parser/planner、optimizer 与 AQE executor |
| Figure 4 | 9 | `fig4_overall_latency.png` | §9.2.2 | 比较四个系统在 Q1–Q20 上的端到端延迟 |
| Figure 5 | 9 | `fig5_query_quality.png` | §9.2.5 | 展示 Q15–Q20 的多指标质量轮廓 |
| Figure 6 | 10 | `fig6_execution_optimizations.png` | §9.3 | 对比 local/remote 下 batching 与 fusion 的影响 |
| Figure 7 | 11 | `fig7_aqe_breakdown.png` | §9.5 | 对比 local/remote 下 AQE 目标与执行组合 |
| Figure 8 | 11 | `fig8_q6_case_study.png` | §7.7 | 对照 Q6 的三个语义谓词与 case-study 统计 |

## 3. 未加入的内容

- Table 1：五类 semantic operators 已在 §4.2 逐项转写，重复截图不会增加信息。
- Algorithm 1：输入、三个阶段、最终结果组成及伪代码歧义已在 §7.3 完整整理，保留文字比缩小后的伪代码截图更易读。
- Appendix Figure 9–40：主要是 20 个 benchmark query 的 SemaSQL/系统写法，笔记 Appendix A 已逐查询映射；整批加入会明显拉长笔记，但不会增加方法或实验机制说明。
- 论文表格：精确数值已优先转写为 Markdown 表格，避免从截图反向读数。

## 4. 裁剪与文件校验

裁剪按原始 PDF 坐标进行，主图以约 4.5× 页面坐标分辨率输出；Figure 7–8 的最终边界另由 324 dpi 页面渲染裁切，以去除页眉并完整保留图注。只移除页面正文、页眉和相邻表格，没有修改图内像素内容。

| 文件 | 分辨率 | SHA256 |
|---|---:|---|
| `fig1_system_architecture.png` | 1081×1126 | `c5ac81ef1930fc1edc9d793d2f13481131c4e6615f44e33b971a6ba86d82dcc4` |
| `fig2_running_query.png` | 1171×954 | `c6521cf22548c66b7e8a9dd1bed6b7ebac942f88705c3c87aacd59e0623481c5` |
| `fig3_system_workflow.png` | 2295×1080 | `fad9c6a2f9f6fd180407009ebf302172104380e7fb523212b576d91030a26f97` |
| `fig4_overall_latency.png` | 2318×675 | `fca806d031c7c3ac724978ff0476df7cff211d8df0c4368ac616fa19cdbd61e5` |
| `fig5_query_quality.png` | 2318×518 | `d73a6ff0caa8fae2cc69d31048a3d99adc503c0d3d2d396ecf18c0cdfb13139c` |
| `fig6_execution_optimizations.png` | 1148×1531 | `bb56e56fcf01c5344a9d65655589fef3a6bb436a368e0e90fb06a180f864a45a` |
| `fig7_aqe_breakdown.png` | 2317×999 | `32e4e8ab43697140bc7a7b819b4a7e32422a2b1b2d9dbb397002c786d0849b6a` |
| `fig8_q6_case_study.png` | 1170×729 | `425931d9276bc8c20def8b076bed9ef5cb31d87321b145895b779b8b9a9ea82a` |

## 5. 视觉 QA 与读图边界

- 8/8 图片均已按原始分辨率目视检查：图号、轴标题、图例、查询文本和图注可见，无相邻正文混入。
- Figure 4 使用对数纵轴，且没有柱顶精确值；笔记只使用作者正文给出的范围，不进行像素估值。
- Figure 5 各雷达图的轴含义不同，不跨 query 比较面积。
- Figure 6–7 必须区分 local vLLM 与 remote API；不把某一种配置的局部结果写成普适排序。
- Figure 7 的 F1 是相对 reference path 的一致性，不是人工 ground truth accuracy。
- Figure 8 只展示原始查询，AQE 的路径选择仍由 Phase 1 统计和 Table 5 的 micro-execution 共同解释。
