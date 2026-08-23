# LOTUS 精读笔记论文原图选择与裁剪审计

日期：2026-08-21

## 范围与证据边界

- 使用位置：`research/精读文献笔记/lotus_pvldb2025/lotus_pvldb2025.md`。
- 权威来源：`research/reference/lotus_pvldb2025.pdf`，SHA256 `865382441D7CA488C4DA4670F233FD1E9B78532D9A4F12CC018D4A58D40F6298`。
- 这些图片是 LOTUS 论文原图的局部裁剪，仅服务精读讲解；不是项目自制图，也不是本项目实验结果。
- PDF 是图中文字和数据的权威来源。PNG 只用于 Markdown 显示，没有改写数据、坐标、图例或论文图注。

## 选择结果

| 论文图 | PDF / PVLDB 页码 | 精读正文位置 | 选择理由 |
|---|---|---|---|
| Figure 1 | 2 / 4172 | §10 Figure 1 | 直接展示 `sem_search → sem_filter → sem_agg` 的可组合程序，是 operator abstraction 最紧凑的 running example。 |
| Figure 4 | 9 / 4179 | §12 Figure 4 | 用散点明确展示 proxy、oracle 与 approximation 的 accuracy–execution-time operating points，文字数字不能替代其前沿形状。 |
| Figure 6 | 11 / 4181 | §18 Figure 6 | 展示同一 accuracy-target 机制迁移到 group-by classification 后的 cost–quality trade-off，支撑跨 operator 解释。 |
| Figure 7 | 12 / 4182 | §19 Figure 7 | 四联图同时验证 target recall/precision、Oracle 调用量和 failure probability，是论文“统计保证”核心主张的直接证据。 |

未加入 Figure 2、Figure 3：它们只是短 API 示例，精读正文已逐行转写。未加入 Figure 5：五个发现标签已完整列出，原图没有额外机制或定量结构。Tables 1–7 继续使用正文表格和数字讲解，不以截图重复。

## 输出与完整性

| 文件 | SHA256 |
|---|---|
| `research/精读文献笔记/lotus_pvldb2025/figures/lotus_fig1_semantic_operator_program.png` | `5340FEE18170833EDE78A83725E39368934601D936D04AAC3E9F329E22DC987F` |
| `research/精读文献笔记/lotus_pvldb2025/figures/lotus_fig4_fact_checking_tradeoff.png` | `8F38681DFE4370B671F1B803923655EE5AFF8F090D3C49FAC828BE417B918A8F` |
| `research/精读文献笔记/lotus_pvldb2025/figures/lotus_fig6_groupby_tradeoff.png` | `FB0F3720D73EB0FBAEB4B28CCDEB29C267EE75C99ED41AEB5F641D7C30D35960` |
| `research/精读文献笔记/lotus_pvldb2025/figures/lotus_fig7_accuracy_guarantees.png` | `A163072A5D1321CC66636C4C80F3CE75C420987AECA7F3C5D8F63E6DF4A4B890` |

提取方式：使用 `pypdfium2` 从正式 PDF 对对应页面作 4× raster render 后按原图边界裁剪，保留论文原始 caption。没有重绘、锐化、颜色替换或内容删除。

## 视觉与论证 QA

- 四张图已按原始分辨率预览；代码、坐标轴、图例、panel 标签和论文 caption 均可读，没有混入相邻正文。
- Figure 4/6 使用颜色与 marker 双重编码；Figure 7 使用颜色、线型和 panel 标题，不只依赖颜色。
- Figure 4/6 的坐标范围和 Figure 7 的四个 panel 均保持论文原样，不作视觉夸大。
- Figure 1 是 supporting running example；Figure 4、6、7 是实验结果图。四图各自承担不同解释任务，没有用同一信息重复占位。
- Markdown 另加中文 alt text 和来源行，明确论文 Figure 编号、PDF 页码与 PVLDB 页码。

审计结论：4 张图适合进入当前精读笔记；0 个 critical、0 个 major、0 个 minor 视觉阻断项。
