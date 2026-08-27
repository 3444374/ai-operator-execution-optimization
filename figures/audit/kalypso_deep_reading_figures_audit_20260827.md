# Kalypso 精读笔记论文原图选择与裁剪审计

日期：2026-08-27

## 范围与来源

- 使用位置：`research/精读文献笔记/kalypso_arxiv2026/kalypso_arxiv2026.md`。
- 来源文件：`research/reference/kalypso_arxiv2026.pdf`，题名 *Kalypso: Relational LLM Serving*，arXiv:2607.23815v2，2026-08-14。
- 本地 PDF 共 14 页，SHA256 `C9A8BCEBAF43C771F3C083A77DE523D09AE95AAA815CDE06D174F347859232A7`；首页与 PDF metadata 的题名、作者、版本和日期已经核对。该版本未标注正式会议或期刊，继续按 arXiv 核心补充管理。
- 使用 Poppler 以 324 DPI 渲染原页，再按 Figure 边界裁切并保留原论文 caption；没有重绘、改色、锐化或修改图内数值。
- PNG 仅服务 Markdown 精读讲解，不是项目实验结果。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 独立作用 |
|---|---:|---|---|
| Figure 1 | 3 | §2.3 | 直接标出 filter/map 共享的 System Prompt + Tuple token prefix，是跨 operator reuse 的最小例子。 |
| Figure 2 | 3 | §2.4 | 展示输入超过约 70-tuple KV 容量后第二个 operator 重新 prefill 的动机现象。 |
| Figure 3 | 3 | §4.2.1 | 定义 Query API、Kalypso 控制层与底层 LLM engine 的系统职责。 |
| Figure 4 | 5 | §4.4.1 | 将 left-deep query plan 展开为 pipeline、stage、task、Cartesian Product 和 blocking operator。 |
| Figure 5 | 6 | §4.4.3 | 说明 parallel depth-first 固定预算为何使 downstream stage starving。 |
| Figure 6 | 7 | §4.4.3 | 说明 parallel breadth-first 固定预算为何使 pending children 堆积并使 downstream stage saturated。 |
| Figure 7 | 10 | §6.3 | 四个 workload 的系统端到端主结果，也是 4.57× 最大值的来源。 |
| Figure 8 | 11 | §6.4.5 | 比较 vLLM memory utilization 从 0.9 降至 0.5 时三套系统的变化。 |
| Figure 9 | 11 | §6.4.1 | 对照 Kalypso blocking variant 与默认 pipelined execution。 |
| Figure 10 | 11 | §6.4.2 | 同时扫描两种 memory setting、两个 workload 和五个固定 stage ratios，并与 adaptive 结果比较。 |
| Figure 11 | 12 | §6.4.3 | 比较无需修改 vLLM 的 virtual pinning 与 explicit pinning。 |
| Figure 12 | 12 | §6.4.4 | 展示固定 output-token budget 过大或过小时的延迟代价，并对照在线估计。 |

正文共有 Figure 1–12。十二张图分别承担 prompt/容量动机、系统和执行机制、两类失败状态、系统主结果或独立消融，没有只重复 Table 1–2 或 Algorithm 1 的图，因此全部保留。表格和算法已经在笔记中按字段与步骤转写，不再截图。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `fig1_filter_map_prompt_structure.png` | 1170×598 | `C546FA0045CEBB08F96BF037786C587091E19D4B5492C68DFFB8F048B73D39F2` |
| `fig2_kv_cache_capacity_motivation.png` | 1170×841 | `8F004F1DFC37A083B1D9937513ECF13A67516529C8F537252A85C745897E9721` |
| `fig3_kalypso_architecture.png` | 1147×810 | `0C644B1A3AD427E944734FDAEF950C261AEC0D2CAC6C0209AC34EEDE6DD5C74C` |
| `fig4_query_plan_pipelining.png` | 2340×720 | `9ABA1E47FD2ECAB79DBF53FBEC653A2B71F073D68DBB8B1DA490FB9384C1C2D9` |
| `fig5_depth_first_starvation.png` | 1193×832 | `40BB9E27B1978E90BF26584841FF9991EA6F3D09BBD6F7E4010140CE9F6A4858` |
| `fig6_breadth_first_saturation.png` | 1147×900 | `8D3D2C7794C1666C40B5DDB18D9FA30D4F441447C1AC808EFA23CEF8849F1AC7` |
| `fig7_end_to_end_latency.png` | 2025×630 | `FBC2BD4556DB9692C3DE39F59A1AA6ECC27C79A4FC1D78FC55A3C6C2E268AECC` |
| `fig8_memory_utilization_sensitivity.png` | 2340×764 | `5384C6071BA0DBE78F017D080A753F04F9398589179A2AB1D9AF983ABFD5AD23` |
| `fig9_blocking_vs_pipelined.png` | 1193×585 | `87CE61F8F2943548AE7A6424EEE206E54AE3B5BA288560CFAA46E14391A23DF5` |
| `fig10_stage_budget_allocation.png` | 1147×922 | `B286AF23A224512041B038C53374467EEBEB0F263913D93C8C366457E4902DBC` |
| `fig11_virtual_vs_explicit_pinning.png` | 1170×855 | `678058069E774AFCB404FC419A9502919476E343D13A21987AAFB6EF5BA44EBA` |
| `fig12_token_budget_sensitivity.png` | 1147×801 | `247067A1A8217CF0624E719FC53FE7C721AEBFC9D981ED278D9F71228CF95280` |

所有输出位于 `research/精读文献笔记/kalypso_arxiv2026/figures/`。正文有 12 个本地图片引用，逐一对应同名 PNG。

## 视觉与论证检查

- 12 张 PNG 已按原始分辨率逐张预览；图例、坐标轴、stage/task 标签、数值和原论文 caption 均完整，没有混入页眉、相邻正文或下一张图。
- Figure 1、3–6 按机制或状态示意图讲解，不作为性能结果；Figure 2 只按单 A16、Llama-3.2-3B、750-token padding 的受控动机实验解读。
- Figure 7 的四个 panel 使用不同纵轴，缺失 baseline 表示系统不支持相应 workload；论文说明每项取三次运行平均值，但 Figure 7–12 均无误差条。
- Figure 9 同时改变流水重叠、跨 operator reuse 与 pinning，不能单独归因；Figure 11 不能证明 virtual/explicit 统计等价；Figure 12 仅使用 MEDEC。
- Figure 10 的 0.6-memory ContractNLI 图值为 static 1:9 = 1,165 s、adaptive = 1,185 s，图值不支持正文“adaptive 在该设置仍最佳”的说法，笔记已显式保留该不一致。

审计结论：正文 Figure 1–12 均有独立讲解价值并已插入对应段落；0 个 critical、0 个 major、0 个视觉阻断项。
