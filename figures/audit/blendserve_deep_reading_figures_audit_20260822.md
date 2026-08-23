# BlendServe 精读笔记论文原图选择与裁剪审计

日期：2026-08-22

## 范围与证据边界

- 使用位置：`research/精读文献笔记/blendserve_asplos2026/blendserve_asplos2026.md`。
- 权威来源：用户本地文献目录中的 `blendserve_asplos26.pdf`，题名 *BlendServe: Optimizing Offline Inference with Resource-Aware Batching*，ASPLOS ’26 正式版。
- 正式 PDF：19 页，SHA256 `4BD27DE86137747BA0AC42C2D71AD369AD36BFB004EFC728895A8D0AA4A2CACC`；已通过 `%PDF` 签名、题名元数据和页数解析检查。
- 源文件没有复制进 `research/reference/`，因此不改变项目参考 PDF 子集及其计数。
- 这些 PNG 是论文原图的局部裁剪，只服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 1 | 2 | §2 | 用两个时间线直接表达顺序 batching 的资源空洞与 resource-aware blending 的互补，是全文最简洁的动机图。 |
| Figure 2 | 3 | §5.1 | 六组 histogram 保留均值之外的 input/output 长度分布形态和数量级差异，Markdown 汇总表无法替代。 |
| Figure 3 | 4 | §5.2 | `T_comp/T_mem` 的 log-scale 时间线直接展示顺序 workload 的 phase imbalance 与重排后的稳定性，是 request ordering 动机的测量证据。 |
| Figure 4 | 7 | §6.3 | 把 output length、input length、workload 点位和 density=1 boundary 放在同一坐标系，是理解 compute-density 模型最有效的图。 |
| Figure 5 | 8 | §7.1 | 一图串联 prefix tree construction、output sampling、sorting/splitting 和 dual scanning，是 BlendServe 的权威系统地图。 |
| Figure 6 | 8 | §7.4 | 将 density target 具体转化为 80GB GPU 上的左右 KV-cache 分区，补足公式难以表达的空间关系。 |
| Figure 7 | 10 | §8.3 | 同时保留 8B 单卡和 70B 八卡主结果、全部 baseline 与 practical upperbound，是论文主要端到端证据。 |
| Figure 9 | 12 | §8.4 | 直接验证 BlendServe 在 resource-aware reordering 后仍保持接近 DFS optimum 的 prefix sharing，承担 locality 机制证据。 |
| Figure 10 | 12 | §8.4 | 三条完整执行时间线对照 BlendServe、NanoFlow-DFS 和 NanoFlow-Balance，承担 resource balance 机制证据。 |
| Figure 11 | 12 | §8.5 | 以 density × prefix-sharing 网格展示 65 个 synthetic workload 的 sensitivity，同时明确属于 simulated backend 证据。 |

未加入 Figure 8：专门比较 P/D disaggregation 的配置结论与适用边界已在 §8.3 精确转写，Figure 7 已承担主端到端结果；未加入 Figure 12：other-model generality 主要来自 simulator，模型列表、平均/最高提升和证据边界已完整记录；未加入 Appendix Figure 13–15：三组扩展 synthetic trace 的数值与 output-length variance 局限已在 §9.2 转写，不承担新的独立机制。Table 1–4 已转成 Markdown 表格，Algorithm 1–3 的步骤和公式已转写为可检索文本，因此不重复截图。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `research/精读文献笔记/blendserve_asplos2026/figures/fig1_resource_aware_batching.png` | 1103×572 | `25F50EB814CF73A32844024C657934E379C4EB4A415E28409C55A0A6ECEBB8C4` |
| `research/精读文献笔记/blendserve_asplos2026/figures/fig2_trace_length_density.png` | 1126×756 | `51196050FA8F037EC08BF8DB92D1F5CDB45EB97EAE8F0C25D983F679E79C6BDA` |
| `research/精读文献笔记/blendserve_asplos2026/figures/fig3_resource_balance_motivation.png` | 1126×482 | `94F71B8503DA7325C2BF61FD2E2427B52241F5B46F052CFFE3A44AC70F1575FE` |
| `research/精读文献笔记/blendserve_asplos2026/figures/fig4_compute_density.png` | 1103×608 | `0E73F2597B90066F1E6EC3F7CBB1CB40C29BCE4C6F0893D2623E95670281A3AB` |
| `research/精读文献笔记/blendserve_asplos2026/figures/fig5_blendserve_overview.png` | 1949×635 | `0CA8FF70B74C31D471F780C465FECF6C03803D2D603882451AA4EF09F8058B5A` |
| `research/精读文献笔记/blendserve_asplos2026/figures/fig6_dual_scanner_memory_partition.png` | 914×486 | `BE5A668CE356E47B33A040AD27CAD4872372FB2FD238630262145530A6F8696E` |
| `research/精读文献笔记/blendserve_asplos2026/figures/fig7_end_to_end_throughput.png` | 1126×873 | `295C161C6D5F4F5418CFD75866A41CFD98118E11A43D04D14373EF1B45E36132` |
| `research/精读文献笔记/blendserve_asplos2026/figures/fig9_prefix_sharing_ratio.png` | 1126×406 | `18581B572C041B00D2E1584A02D937745DA7CD2A8AF3E8697AE0F2C800CBC256` |
| `research/精读文献笔记/blendserve_asplos2026/figures/fig10_resource_usage_over_time.png` | 1126×892 | `535A95FCFE9328A97BCEE79DAF34C617FCD400FB9D362721B4F8FDCCDA63F6A1` |
| `research/精读文献笔记/blendserve_asplos2026/figures/fig11_sensitivity_heatmap.png` | 1058×806 | `416B7E55E0313F2B8DA33E57649A9FE92BB1D54ECC286CFBF319E51BFA7A86A5` |

提取方式：使用 PyMuPDF 对正式 PDF 对应页面作 4.5× raster render，再按原图边界裁剪。所有 multi-panel Figure 保持为一个整体；没有重绘、锐化、替换颜色、修改坐标或删除图内元素。论文英文 caption 不进入裁剪件，由精读正文的中文 alt text、来源行和证据边界承担说明。

## 视觉与论证 QA

- 10 张裁剪件已按原始分辨率预览；图例、坐标轴、单位、科学计数法、log-scale 标签、步骤号、node 类型和 panel 标题完整可读，没有混入相邻正文或残缺 caption。
- Figure 1/5/6 主要依赖布局、箭头、形状、文字和颜色共同编码；Figure 2–4/7/9–11 保留原论文的图例、线型、series 位置、数值或 panel 分区，不只依赖颜色。
- Figure 3 的 log y-axis、Figure 7 两面板的不同数量级、Figure 10 三面板的不同 y-range，以及 Figure 11 的 simulation 边界均在正文来源行显式提醒。
- 10 个裁剪件分别承担动机、workload 分布、代价模型、架构、调度实例、端到端主结果与两类机制消融，没有收集装饰性图片。
- PNG 仅用于精读笔记显示。若将来进入正式论文或开题材料，应从正式 PDF 取得矢量版本或按投稿规范重新导出。

审计结论：来自 10 个论文 Figure 的 10 个裁剪件适合进入当前 BlendServe 精读笔记；0 个 critical、0 个 major、0 个视觉阻断项。
