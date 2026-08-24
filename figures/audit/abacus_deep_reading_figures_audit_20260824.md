# Abacus 精读笔记论文原图选择与引用审计

日期：2026-08-24

## 范围与证据来源

- 使用位置：`research/精读文献笔记/abacus_pvldb2026/abacus_pvldb2026.md`。
- 权威来源：`research/reference/abacus_pvldb2026.pdf`，题名 *Abacus: A Cost-Based Optimizer for Semantic Operator Systems*，PVLDB 19(5): 1060–1073，2026，DOI `10.14778/3796195.3796215`。
- 本地正式版 PDF 共 14 页，SHA256 `DFD6F4A2588C25A9CE30335F86ED32DCFD449F11BA81321B667751BAC46866FB`；已核对首页 PVLDB Reference Format、题名、作者、卷期页码、DOI、正文图号和 caption。
- 新增精读目录中原有 8 个 PNG 均为该 PDF Figure 的干净裁剪。此次未修改像素内容，只修复 Markdown 中错误的 `assets/` 引用，改为实际存在的 `figures/` 路径，并增加来源、读图方法与外推边界。
- PNG 只服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 1 | 2 | §2.2 | 同一文献检索程序在 Maximize Quality 与 MaxQuality@Cost<$1 下产生不同物理计划，是理解多目标全局选择的总动机图。 |
| Figure 2 | 4 | §4.3 | 串起输入、逻辑计划、物理搜索空间、per-operator 估计与最终计划，是 Abacus 编译期优化链路的系统总览。 |
| Figure 3 | 6 | §5.1 | 直观展示 Cascades group tree、logical expressions 与 physical expressions，为后续 Pareto-Cascades 修改建立结构基础。 |
| Figure 4 | 8 | §6.2 | 对照 BioDEX 的多级 Map/Top-K 与 CUAD 的单个宽 Map，解释两个文本 benchmark 的逻辑计划差异。 |
| Figure 5 | 8 | §6.2 | 展示 MMQA 文本、表格、图像三分支及最终关系 Join，是全文唯一完整多模态 DAG。 |
| Figure 6 | 10 | §6.5 | 四个面板同时比较 sample budget、prior 类型、工作负载和有/无成本约束，是 prior 有效性的主要实验证据。 |
| Figure 7 | 11 | §6.6 | 展示成本约束从 `$1` 放宽到 `None` 时计划质量的总体变化，并暴露有限采样下非严格单调的边界。 |
| Figure 8 | 11 | §6.7 | 同时消融 priors、MAB sampling 与 Pareto-Cascades，区分目标指标收益和经验 constraint satisfaction。 |

论文正文共有 Figure 1–8，八图分别承担动机、架构、算法结构、benchmark 计划或独立实验问题，没有只重复已转录 Table/Algorithm 的 Figure，因此全部保留。Algorithm 1–5、Table 1–3 和 Equation 1 已在正文按行或按字段转写，不再截图。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `research/精读文献笔记/abacus_pvldb2026/figures/fig1.png` | 1306×663 | `6F4B163DBC69D2CCCA83B4950FD5D72A09BB57FD4A79F38E5DB867A4EB2C6288` |
| `research/精读文献笔记/abacus_pvldb2026/figures/fig2.png` | 1306×405 | `7650C05501127C44213F377B322780FD08FE4BD416B3320C60682F3CFBFC0809` |
| `research/精读文献笔记/abacus_pvldb2026/figures/fig3.png` | 1306×401 | `1D4E07F3602A75BAD05DA9EF86B975EDF4DD0BCB0F12AE139D44D0D2637CA23C` |
| `research/精读文献笔记/abacus_pvldb2026/figures/fig4.png` | 643×370 | `C0E934A48D913FD10CE1FA32A9FE68902A80391C9826019C821AA4E936661BD3` |
| `research/精读文献笔记/abacus_pvldb2026/figures/fig5.png` | 668×501 | `CEEBBBB5706FD7A3BD1D46BC0DDC18DF5D99E482FA124A43349FEED49702B08B` |
| `research/精读文献笔记/abacus_pvldb2026/figures/fig6.png` | 1306×430 | `0FBF0FD49A423CF5E74E53F2158898E728CEF5682BC0E626EE348FA724EFD431` |
| `research/精读文献笔记/abacus_pvldb2026/figures/fig7.png` | 651×433 | `9DD0A52B51D436B8F434989814AA82217B9510F4D86CCC1A9A0EA2DFC559C148` |
| `research/精读文献笔记/abacus_pvldb2026/figures/fig8.png` | 648×575 | `FDD45627FAC2E08BC0B23343C95846243CBB498616BC4FF3C4C17B148CC02ABA` |

裁剪方式的原始生成工具未随新增目录提供，因此不推测具体软件或缩放倍数。此次使用 Poppler 将 PDF p.2、4、6、8、10、11 重新渲染作视觉对照，确认 PNG 的节点、标签、坐标、图例和数值与正式版 Figure 一致；没有对 PNG 重绘、锐化、改色或拼接。

## 视觉与论证 QA

- 8 张 PNG 已按原始分辨率逐张预览；Figure 1–3、6 的标签完整清晰，Figure 4–5、7–8 分辨率较低但正文阅读所需的节点、图例、坐标轴和柱形仍可辨认，没有裁掉 Figure 主体。
- Figure 1 的数值按动机示例而非 benchmark 结果解读；Figure 2–3 按优化期架构和传统 Cascades 结构解读，不外推为运行时自适应机制。
- Figure 4–5 只说明 benchmark 逻辑计划；尤其 MMQA 实验预筛了 ground-truth 相关 data items，不能由 Figure 5 推断完整语料检索的扩展性。
- Figure 6 比较的是最终计划质量而非预测误差，论文未明确说明 sample-based prior 的完整构造成本如何计入；Figure 7 不严格单调；Figure 8 的 constraint satisfaction 是重复实验中的经验频率，不是 hard guarantee。
- 当前 PNG 适合 Markdown 精读。如果进入开题报告、PPT 或论文正文，应从正式 PDF 重新导出更高分辨率或矢量版本，特别是 Figure 4、5、7、8。

审计结论：8 张论文原图均适合进入当前 Abacus 精读笔记；错误图片路径已修复，0 个 critical、0 个 major、0 个视觉阻断项。
