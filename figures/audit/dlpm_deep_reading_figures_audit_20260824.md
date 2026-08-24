# DLPM 精读笔记论文原图选择与裁剪审计

日期：2026-08-24

## 范围与版本边界

- 使用位置：`research/精读文献笔记/dlpm_2025/dlpm_2025.md`。
- 权威全文：用户本地文献目录中的 `dlpm_arxiv2025.pdf`，题名 *Locality-aware Fair Scheduling in LLM Serving*。
- 本地 PDF：17 页，SHA256 `440269188E4163E15FECCA730A2C91D4E8078F61118FC2DF3D60B5A191187A2B`；已通过 `%PDF-1.5` 签名、首页题名、作者和页数解析检查。
- 版本核验：PDF 左侧标注 arXiv:2501.14312v1，官方 arXiv 页面截至 2026-08-24 也只有 v1（2025-01-24），并给出 arXiv DataCite DOI `10.48550/arXiv.2501.14312`；未检索到正式会议/期刊 proceedings，因此继续按 2025 arXiv preprint 记录，不根据目录名推断 venue。
- 源文件没有复制进 `research/reference/`，因此不改变项目参考 PDF 子集及其计数。
- 这些 PNG 是论文原图的局部裁剪，只服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。论文的公平保证针对同一模型副本集中的 client token service，并依赖 worker-local fair scheduler，不能直接外推为本项目黑盒 vLLM endpoint 上的端到端数据库 Job 公平保证。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 1 | 1 | §4.5 | 单 A10 上将 Qᵘ 的多个点与 LPM、VTC 放进 throughput–Jain fairness 平面，是 quantum 作为显式权衡旋钮的直接证据。 |
| Figure 2 | 3 | §2.2 | 三个 panel 用同一 prefix-sharing 场景对比 LPM starvation、VTC 打散 prefix 与 DLPM bounded eligibility，是论文最清楚的动机机制图。 |
| Figure 3 | 3 | §2.2 | 把 DLPM 的 locality–fairness 冲突和 D²LPM 的 locality–load-balancing 冲突分成两个层次，承担问题空间总览。 |
| Figure 4 | 6 | §5.1 | 同时分解 data-parallelism 增长时的 synchronization/prefix-match 开销，并扫描 global queue size，是作者放弃 centralized strawman 的直接依据。 |
| Figure 5 | 6 | §5.2 | 显示 global per-client-per-worker deficit、Global Radix Tree、异步 eviction 与 worker-local deficit 的两层 D²LPM 架构。 |
| Figure 6 | 7 | §6.1 | 四个 execution graph 说明 Long-Context QA、LLM-as-a-Judge、Tree-of-Thoughts 与 Multi-Turn 的依赖结构，避免只按平均 token 长度理解 workload。 |
| Figure 7 | 9 | §6.2 | 以六行四列覆盖三个 synthetic workload、两类 misbehavior、1–8 GPUs 的 Service、Jain index、P50/P99，是论文主结果与 Long-Context 反例的共同载体。 |
| Figure 8 | 10 | §6.3 | 在真实 multi-turn trace 重放中并排展示 D²LPM、VTC、RR+LPM 的 response time 与 actual service，承担恢复型 client 的时序证据。 |
| Figure 9 | 10 | §7.1 | 三行四列同时展示 response time、actual service 与 client-visible service，直接解释“资源公平但逻辑 token 服务量可不同”。 |
| Figure 10 | 11 | §7.2 | 补充 Qʷ 对分布式 throughput 与 Jain index 的影响，与 Figure 1 的 Qᵘ 单 worker 权衡分工不同。 |
| Figure 11 | 11 | §7.3 | 固定总 request rate、将 client 数从 5 增至 50，承担有限规模 client-scaling 消融。 |
| Figure 12 | 12 | §7.4 | 四种 client workload 混合时的 response time、actual service 和 client-visible service，覆盖单一 workload 主实验未表达的异质组合。 |

论文正文一共 Figure 1–12，十二幅均承担独立论证角色，因此全部进入精读笔记。Figure 1 在原 Word 转换稿中被嵌入两次，本次只在 §4.5 保留一次，§7.2 文字回指。Table 1、Algorithm 1、Algorithm 2 及 Table 2–3 已逐项转写为可检索的 Markdown 表格或步骤，不重复截图。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `research/精读文献笔记/dlpm_2025/figures/fig1_locality_fairness_pareto.png` | 1094×891 | `7B881684234651B3130DA1D7FFCA14739A8EAE072F62E858BAB6F61E52E4134B` |
| `research/精读文献笔记/dlpm_2025/figures/fig2_lpm_vtc_dlpm_conflict.png` | 1099×999 | `04DED9BE9D9A7524942E6CB21B56C4C156E7874A6AB5A52E02FE4C2ED1A40DB5` |
| `research/精读文献笔记/dlpm_2025/figures/fig3_problem_space.png` | 1094×594 | `5DE539AD27B7A63446A1F58668B4C48721053526E7D47ACB60D2347D3FB834EC` |
| `research/精读文献笔记/dlpm_2025/figures/fig4_centralized_scheduler_overhead.png` | 1099×792 | `51C1525FCE8C3918085122DFD326698664E28FACD32496103AA9E9A7881E2525` |
| `research/精读文献笔记/dlpm_2025/figures/fig5_d2lpm_overview.png` | 1094×1071 | `F75615F3030F456A4EC394F55FE28722BF7DCAC726A3659D161555E678674D6D` |
| `research/精读文献笔记/dlpm_2025/figures/fig6_workload_execution_graphs.png` | 1094×532 | `F954E55BDD40F19F12B2F2C254100591EE2E8B651F65C72F5473FF7792035D59` |
| `research/精读文献笔记/dlpm_2025/figures/fig7_synthetic_main_results.png` | 2282×2061 | `4AA26871FD50C2A7F1DC318B082E901E6C456037B6BD60B63E42A89123AE6438` |
| `research/精读文献笔记/dlpm_2025/figures/fig8_real_multiturn_trace.png` | 1099×792 | `A2B701691A7C9DA511A13157ECB48D94090009FF6BA30E5F56C4735798A63A50` |
| `research/精读文献笔记/dlpm_2025/figures/fig9_fairness_properties.png` | 1094×1098 | `578AEC145D6F7281A50D81851C2493627A8A170F05B65B3572999CBB8689BEBB` |
| `research/精读文献笔记/dlpm_2025/figures/fig10_qw_sensitivity.png` | 1099×837 | `23CC59054E5A69A43E32C4A2D87FAE418266D2C9E385E328600492120BA6AEBC` |
| `research/精读文献笔记/dlpm_2025/figures/fig11_client_scaling.png` | 1094×486 | `58ADC6937982DDD42D3C10EC9E8B63546ADFEE50FFA28078AC74296C3D95E0D6` |
| `research/精读文献笔记/dlpm_2025/figures/fig12_mixed_workloads.png` | 1099×950 | `1B737AA7B3205FCC0B811E10EA7D3C858C8933C6F0DA40E6F0462956B07B5B33` |

提取方式：使用 PyMuPDF 对本地 PDF 对应页面作 4.5× raster render，再按每幅 Figure 与英文 caption 的完整边界裁剪。没有重绘、锐化、替换颜色、修改坐标、删除图内元素或拼接不同页面；所有 multi-panel Figure 均保持整体。

## 视觉与论证 QA

- 12 张裁剪件已按整页、contact sheet 及 Figure 7/9/12 原始分辨率预览；legend、marker、线型、坐标轴、单位、panel 标签、缺失数据 `N/A`、架构箭头、execution graph 和英文 caption 完整可读，没有混入相邻正文。
- Figure 1、4、7–12 除颜色外还使用 marker、线型、柱形位置、panel 分区和文字标签；Figure 2/3/5/6 使用形状、箭头、布局和实体名称，不以颜色作为唯一编码。
- Figure 1 的单配置范围、Figure 4 的固定 queue/batch 条件、Figure 7 缺失点与 Long-Context worst-case、Figure 8 的 trace 重放边界、Figure 9 不同横轴终点、Figure 11 的 5–50 client 范围均随图写入正文。
- PNG 仅用于精读笔记显示。若将来进入正式论文或开题材料，应从 arXiv 源文件取得矢量版本或按投稿规范重新导出。

审计结论：论文全部 12 个 Figure 的 12 个裁剪件适合进入当前 DLPM 精读笔记；0 个 critical、0 个 major、0 个视觉阻断项。
