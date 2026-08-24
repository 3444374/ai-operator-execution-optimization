# IMBridge 精读笔记论文原图选择与裁剪审计

日期：2026-08-24

## 范围与证据边界

- 使用位置：`research/精读文献笔记/IMBridge_sigmod2024/IMBridge_sigmod2024.md`。
- 权威来源：用户本地文献目录中的 `IMBridge_2026.pdf`，题名 *IMBridge: Impedance Mismatch Mitigation between Database Engine and Prediction Query Execution*。虽然本地文件名含 `2026`，论文首页 ACM Reference Format、页眉和 DOI 均明确对应 SIGMOD-Companion ’24（2024）；本次不由文件名推断发表年份。
- 本地 PDF：4 页，SHA256 `4E4E358BC0E842858C59F93BBA2329C1D6433FB59C36C07B56C7B90C48BA6B45`；已通过 `%PDF-1.5` 签名、首页题名、作者、ACM Reference Format、DOI `10.1145/3626246.3654754` 和页数解析检查。
- 源文件没有复制进 `research/reference/`，因此不改变项目参考 PDF 子集及其计数。
- 这些 PNG 是论文原图的局部裁剪，只服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。IMBridge 面向 OceanBase 内的 Python prediction UDF、函数生命周期改写和独立推理算子，不覆盖本项目的外部分布式模型服务、多 Job 公平或多 endpoint 路由。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 1 | 1 | §2.1 | 同时展示 prediction function 的定义语句、setup/preprocess/inference 三段代码和关系查询中的调用位置，定义论文所讨论的用户接口与数据路径。 |
| Figure 2 | 2 | §4.1 | 把 Prediction Function Rewriter 与 Decoupled Prediction Operator 分别定位到计划改写和执行计划生成路径，是两项机制分工的总览图。 |
| Figure 3 | 2 | §2.4 | 两个 panel 分别呈现重复 context setup 与 evaluation batch size 偏离 desirable size，直接对应论文的两类 impedance mismatch。 |
| Figure 4 | 2 | §4.2.2 | 用 `__init__`/`__call__` 和 planning-time/runtime 两个区域解释 context 的生命周期提升，比笔记伪代码更直观地呈现复用边界。 |
| Figure 5 | 4 | §4.2.7 | 是 Function Rewriter 唯一的演示证据，同时给出原始/改写代码、逐轮函数时间和 244.15 s → 116.26 s 的总耗时。 |
| Figure 6 | 4 | §4.3.6 | 是 Decoupled Prediction Operator 唯一的演示证据，在同一界面展示独立 PREDICT OP、batch-size 搜索、throughput 与 244.15 s → 24.16 s 的总耗时。 |

论文一共只有 Figure 1–6，六幅均承担独立论证角色，因此全部进入精读笔记。原文没有编号 Table 或 Algorithm；automatic hoisting、buffer/slice、AIMD tuner 和演示数据均已由正文转写，不为增加图量重复截取正文或自制表格。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `research/精读文献笔记/IMBridge_sigmod2024/figures/fig1_prediction_query_user_language.png` | 1179×468 | `C395A80DF7AF31A0B9443E84D16EE7A637BED18F4775D48D0344E28B3741A00F` |
| `research/精读文献笔记/IMBridge_sigmod2024/figures/fig2_system_architecture.png` | 1053×833 | `876CD51ED41F206B9A6F5D656CD3AB60717039E38C19287540A79B5EA0F621DB` |
| `research/精读文献笔记/IMBridge_sigmod2024/figures/fig3_impedance_mismatch.png` | 1116×693 | `37EB423A53A34B3F57A95DE6D88D5FC8DE034635C390027537597DD1F9941A94` |
| `research/精读文献笔记/IMBridge_sigmod2024/figures/fig4_prediction_function_rewrite.png` | 1116×527 | `9313B970B19BBD5EC00DA9544E3F6636CCFC7A4C04FE75CEDD9D4C5F277500BC` |
| `research/精读文献笔记/IMBridge_sigmod2024/figures/fig5_function_rewriter_demo.png` | 1098×1157 | `A07636BE1F7CB330C88433358D338B29B3451F1C72F83C4B7ECF39A1BD3BD349` |
| `research/精读文献笔记/IMBridge_sigmod2024/figures/fig6_decoupled_prediction_operator_demo.png` | 1094×1634 | `49EBEC5ED24C238E4730C7B84C6FDB81F894C057734FF260A29DE5ED09D2EB5E` |

提取方式：使用 PyMuPDF 对本地 PDF 对应页面作 4.5× raster render，再按每幅 Figure 与英文 caption 的完整边界裁剪。没有重绘、锐化、替换颜色、修改坐标、删除图内元素或拼接不同页面；Figure 3 的两个 panel、Figure 5/6 的完整演示界面均保持整体。

## 视觉与论证 QA

- 6 张裁剪件已按整页、contact sheet 和 Figure 5/6 原始分辨率分别预览；SQL/UDF 代码、架构节点、箭头、panel 标签、图例、坐标轴、单位、计划节点、表格数值、柱状图和英文 caption 均完整可读，没有混入相邻正文。
- Figure 1/2/4 主要依赖文字、布局、形状与箭头；Figure 3 使用颜色之外还有 marker、位置、panel 标签和数值刻度；Figure 5/6 同时保留数值、标签和空间分区，不依赖颜色作为唯一编码。
- Figure 3 右图只按 Q2 的采样结果解读；Figure 5/6 只按演示截图解读，不当作带重复次数、方差和完整环境设置的统计实验，也不据此把 18.2× 外推为普遍收益。
- PNG 仅用于精读笔记显示。若将来进入正式论文或开题材料，应从正式出版 PDF 取得矢量版本或按投稿规范重新导出。

审计结论：论文全部 6 个 Figure 的 6 个裁剪件适合进入当前 IMBridge 精读笔记；0 个 critical、0 个 major、0 个视觉阻断项。
