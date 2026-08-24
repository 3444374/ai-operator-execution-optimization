# Galois 精读笔记论文原图选择与裁剪审计

日期：2026-08-24

## 范围与证据边界

- 使用位置：`research/精读文献笔记/galois_sigmod2025/galois_sigmod2025.md`。
- 权威来源：`research/reference/galois_sigmod2025.pdf`，题名 *Logical and Physical Optimizations for SQL Query Execution over Large Language Models*，Proceedings of the ACM on Management of Data 3(3)，SIGMOD 2025，Article 181，DOI `10.1145/3725411`。
- 本地 PDF 共 28 页，SHA256 `5C3C57A047AC88633B2BED155528DAE9F070839F8E5D5581D697C1A8E30D3398`；已核对 PDF metadata、首页题名、作者、页数和正文图号。
- 这些 PNG 是论文原图的局部裁剪，只服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 1 | 3 | §3.1 | 将自然语言直接问答、直接 SQL prompt 与 Galois 的 DB-first 执行并列，是建立全文心智模型的总览图。 |
| Figure 2 | 5 | §6.1 | 用同一查询对比 no push、push all、push selective 和 push confident，直接显示 predicate pushdown 如何同时改变 recall 与错误 tuple。 |
| Figure 3 | 5 | §7–8 | 把 Table-Scan 的整表取回与 Key-Scan 的 key discovery + per-key fan-out 放在同一张图中，是物理算子质量—调用开销权衡的关键机制图。 |
| Figure 4 | 7 | §12 | 展示两表查询中 LLMScan / Filter-LLMScan 组合如何形成 8 个 logical plans，补充公式不容易表达的 plan-tree 空间关系。 |
| Figure 7 | 18 | §36 | 展示提高输出 logprob 过滤阈值时 precision 上升、recall 下降的直接质量权衡。 |
| Figure 8 | 19 | §39.1 | 比较六类 SQL 查询上的 AVG-Score，支撑“关系 operator 交给 DB 后仍受底层 LLMScan 完整性限制”的实验解读。 |
| Figure 9 | 19 | §41 | 与 Figure 8 的质量趋势对照，显示 Galois A/F 为较高质量支付更高 token cost，并保留 GaloisWO 未入图的量级说明。 |
| Figure 10 | 20 | §43 | 展示 GPT-4o Mini、LLaMA 8B 和 LLaMA 70B 在 Geo-Test 上的模型特定 `τ` 校准，是理解 Table-Scan / Key-Scan selector 的必要图。 |
| Figure 11 | 21 | §44 | 将 Galois A/F 与逐查询枚举所有可行计划得到的 oracle optimum 并列，直接暴露当前 plan selection 距离最优的差距。 |

未加入 Figure 5–6：两图是 Table-Scan 和 Key-Scan 的 prompt syntax，已在精读正文 §14.1、§16–17 完整转写为可搜索文字与流程，重复截图不增加新的空间或实验信息。Table 1–10 和 Algorithm 1–2 同样已由正文转写，不重复截图。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `research/精读文献笔记/galois_sigmod2025/figures/fig1_overview.png` | 1783×774 | `58E38D961D9637E845910CFCFE28AF40931A12460A6BFDB8A9E2D88F9B193EBE` |
| `research/精读文献笔记/galois_sigmod2025/figures/fig2_logical_pushdown.png` | 1783×1359 | `B826E5BFE45B0B8FD903AB07F918C1CD822A38AA50E4AB5D3209662F0DA2B627` |
| `research/精读文献笔记/galois_sigmod2025/figures/fig3_table_vs_key_scan.png` | 1783×779 | `6320EAD4E77CC07DF143AD2C061286D7812767934C11FCD20AE4CA72C933CE99` |
| `research/精读文献笔记/galois_sigmod2025/figures/fig4_logical_plan_enumeration.png` | 1783×1130 | `D6E2E9B2DC54F612D4AFF728BAC28F0DCC69EF8275CDE6A17B83A05589116CA4` |
| `research/精读文献笔记/galois_sigmod2025/figures/fig7_logprob_precision_recall.png` | 1783×527 | `813157E384C98D548BB4DD4929CD66C08A37BB4B75BB9BEFA60188404F958A1D` |
| `research/精读文献笔记/galois_sigmod2025/figures/fig8_quality_vs_query_complexity.png` | 887×617 | `4DC967B33034ECF28731938F5BD3E7E6A8FE31805589116E001392CF5A59A8DB` |
| `research/精读文献笔记/galois_sigmod2025/figures/fig9_cost_vs_query_complexity.png` | 905×617 | `E9ACB394A8A54FB1F7B60BF4FF6AB52E48720473D24BFE2F3E14C01CD7AEB2F0` |
| `research/精读文献笔记/galois_sigmod2025/figures/fig10_tau_selection.png` | 1603×603 | `8F9CD725B6874671A1AAB30C17F1649402CF81532714C67E4AB5482DAD8BB301` |
| `research/精读文献笔记/galois_sigmod2025/figures/fig11_optimizer_vs_optimal.png` | 1783×729 | `B9AFC42B632D67A7124A648C0D4638D6F6A97F48682D06517672FB28626A7FFE` |

提取方式：使用 PyMuPDF 对对应 PDF 页面作 4.5× raster render，再按每幅 Figure 与英文 caption 的完整边界裁剪。没有重绘、锐化、替换颜色、修改坐标、删除图内元素或拼接不同页面。

## 视觉与论证 QA

- 9 张裁剪件已按原始分辨率逐张预览；operator 名称、tuple 值、plan 节点、调用编号、图例、坐标轴、单位、数值标注与英文 caption 完整可读，没有混入相邻表格、页眉或正文。
- Figure 2 的绿/橙色同时有 tuple 内容、位置和 empty result 冗余编码；Figure 7–10 使用不同 marker、线型/位置与文本标注；Figure 11 在柱内直标数值，不依赖颜色作为唯一编码。
- Figure 8–9 的横轴是查询类别而非连续数值，Figure 9 的 token 轴以百万为单位，Figure 10 的阈值从 1 向 0 递减，均已在精读正文来源行显式提醒。
- Figure 1–2 的 motivation-example 边界、Figure 7 的质量控制角色、Figure 10 的校准范围和 Figure 11 的 oracle 上界性质均已在正文中说明。
- PNG 仅用于精读笔记显示。若将来进入正式论文、开题报告或 PPT，应从正式 PDF 取得矢量版本或按材料规范重新导出。

审计结论：9 张论文原图裁剪件适合进入当前 Galois 精读笔记；0 个 critical、0 个 major、0 个视觉阻断项。
