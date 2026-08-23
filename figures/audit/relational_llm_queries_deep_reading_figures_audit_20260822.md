# Relational LLM Queries 精读笔记论文原图选择与裁剪审计

日期：2026-08-22

## 范围与证据边界

- 使用位置：`research/精读文献笔记/relational_llm_queries_mlsys2025/relational_llm_queries_mlsys2025.md`。
- 权威来源：用户本地文献目录中的 `Relational_LLM_Queries_mlsys2025.pdf`，题名 *Optimizing LLM Queries in Relational Data Analytics Workloads*，按论文与精读笔记记录为 MLSys 2025 论文。
- 本地 PDF：15 页，SHA256 `25E3F3B855A4ACBD12082990E2D76AE958D9776D7523203BDF68A5D91FDCE92B`；已通过 `%PDF` 签名、PDF metadata/首页题名、作者和页数解析检查。
- 源文件没有复制进 `research/reference/`，因此不改变项目参考 PDF 子集及其计数。
- 这些 PNG 是论文原图的局部裁剪，只服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。论文优化的是已知 batch relational workload 的 row/field organization，不修改 vLLM 内部 scheduler、continuous batching 或 KV-cache algorithm。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 1 | 4 | §6.3 | 两个构造同时说明 unique first field 如何使后续重复值全部 miss，以及为什么 per-row field ordering 比全局 fixed ordering 多一层物理执行自由度。 |
| Figure 2 | 5 | §9.5 | 用黄色 selected group、绿色 remaining rows 和蓝色 selected rows/remaining fields 展示 GGR 的一次递归拆分，是 Algorithm 1 之外最清楚的机制图。 |
| Figure 3 | 8 | §20 | 同图比较 No Cache、Cache (Original) 和 Cache (GGR) 在 Filter、Projection、RAG 上的端到端 runtime，是五类 benchmark 中三个 query type 的主结果。 |
| Figure 4 | 8 | §24 | 单独覆盖 sequential Multi-LLM Invocation 与 LLM output 后接 relational Aggregation，避免用 Figure 3 的单次调用结果代替复杂 query chain。 |
| Figure 5 | 9 | §27 | 在 8×L4、Llama-3-70B Filter queries 上复现实验趋势，承担跨模型规模验证；范围不扩展到其他 query type 或硬件。 |
| Figure 6 | 10 | §29.5 | 三个模型的 10,000-run bootstrap exact-match 分布直接暴露 field ordering 的质量影响、Llama-3-8B FEVER +14.2% 现象及 Beer −6% 反例，是性能优化不可缺少的 correctness 图。 |

论文一共只有 Figure 1–6，六幅均承担独立论证角色，因此全部进入精读笔记。Algorithm 1 已逐行转写，Figure 2 只补充递归空间关系；Table 1–7 的 dataset、PHR、真实/估算成本、solver time、OPHR/GGR 小样本差距和 1B 结果均已转写为 Markdown，不重复截图。Appendix 中没有额外 Figure。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `research/精读文献笔记/relational_llm_queries_mlsys2025/figures/fig1_fixed_vs_per_row_field_ordering.png` | 2205×837 | `99624B83E2CEB48DAC7A15566E4433816A44201403003377C83B49BC0332E7CF` |
| `research/精读文献笔记/relational_llm_queries_mlsys2025/figures/fig2_ggr_recursive_split.png` | 1071×729 | `9A79F570328EF1ACC81D535F208D61ECD589904EF1F3BD3AAD7382DDF7E8C725` |
| `research/精读文献笔记/relational_llm_queries_mlsys2025/figures/fig3_filter_projection_rag_results.png` | 2205×671 | `938DF5D3D75CE8C2B7A12651B277D2009906176746840F58B7562EEB2DAA8948` |
| `research/精读文献笔记/relational_llm_queries_mlsys2025/figures/fig4_multi_llm_aggregation_results.png` | 1071×644 | `760AD597686EA48AEDCB6353D5AC607A08834F2DD6C82AF0B104DA71B0D95A26` |
| `research/精读文献笔记/relational_llm_queries_mlsys2025/figures/fig5_llama70b_results.png` | 1071×585 | `F5E5808E25F721228E295BAFDEC544BE8788278564A5E81F3E60473A5044849D` |
| `research/精读文献笔记/relational_llm_queries_mlsys2025/figures/fig6_accuracy_impact.png` | 2205×558 | `CAB3D8D7FF36785D8031D00F2D1E1BD717A42AEB3B582C45DF3AC5893DDDF0A4` |

提取方式：使用 PyMuPDF 对本地 PDF 对应页面作 4.5× raster render，再按每幅 Figure 与英文 caption 的完整边界裁剪。没有重绘、锐化、替换颜色、修改坐标、删除图内元素或拼接不同页面；所有 multi-panel Figure 均保持整体。

## 视觉与论证 QA

- 6 张裁剪件已按原始分辨率和 contact sheet 两轮预览；field/group 标签、PHC 公式、箭头、图例、坐标轴、秒/%单位、speedup 标注、箱线图、panel 标签和英文 caption 完整可读，没有混入相邻 Table 或正文。
- Figure 1 虽使用红/绿表示 miss/hit，但同时有 Fixed/A Better Ordering 标题、位置、PHC 数值和 group 标签；Figure 2 同时使用空间拆分、实/虚线边界与颜色；Figure 3–6 使用颜色加 hatch、位置或 panel 分区，不依赖颜色作为唯一编码。
- Figure 3–5 的 runtime 轴从 0 开始；Figure 6 的 accuracy 轴展示约 60%–100% 的有效分布区间，并保留完整刻度和 bootstrap 分布，没有删除异常方向的结果。
- Figure 1 的 worst-case 构造、Figure 5 的 Filter-only 范围、Figure 6 的任务范围和 Beer 数值冲突均在精读正文来源行中显式说明。
- PNG 仅用于精读笔记显示。若将来进入正式论文或开题材料，应从正式 PDF 取得矢量版本或按投稿规范重新导出。

审计结论：论文全部 6 个 Figure 的 6 个裁剪件适合进入当前 Relational LLM Queries 精读笔记；0 个 critical、0 个 major、0 个视觉阻断项。
