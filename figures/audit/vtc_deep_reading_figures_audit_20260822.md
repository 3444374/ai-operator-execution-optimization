# VTC 精读笔记论文原图选择与裁剪审计

日期：2026-08-22

## 范围与证据边界

- 使用位置：`research/精读文献笔记/vtc_osdi2024/vtc_osdi2024.md`。
- 权威来源：用户本地文献目录中的 `vtc_osdi2024.pdf`，题名 *Fairness in Serving Large Language Models*。该文件是 arXiv v2（2024-06-05），对应 18th USENIX Symposium on Operating Systems Design and Implementation（OSDI ’24）正式论文；精读笔记按正式发表信息记录，但不把本地文件伪称为 USENIX 版式 PDF。
- 本地 PDF：24 页，SHA256 `2FA74F1E7FF787BDF4CE7702AC0C28BE0A4D627ECD08DAB19DF5B65F530060DA`；已通过 `%PDF` 签名、首页题名和页数解析检查。
- 源文件没有复制进 `research/reference/`，因此不改变项目参考 PDF 子集及其计数。
- 这些 PNG 是论文原图的局部裁剪，只服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。VTC 是引擎内多 client 公平调度相关工作，本项目的 external VTC-style 实现仍只能作为 Project internal control，不能冒充原生 VTC baseline。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 1 | 2 | §6.5 | 显示 VTC 位于 waiting queue 与 LLM execution engine 之间，并把 minimum-counter selection、token-level counter update 和 request finish/rejoin 放进同一执行路径。 |
| Figure 2 | 4 | §4.2 | 直观说明长 sequence 同时增加 decode token 时间、降低可并行 request 数和 effective throughput；论文明确说长度只作示意，不按精确比例解读。 |
| Figure 3 | 10 | §27.1 | 左 panel 对比 VTC/FCFS 的累积 service difference，右 panel 展示 VTC 下相近的 60 秒窗口 service rate，是 bounded service difference 的经验示意。 |
| Figure 4 | 10 | §28 | 低负载 client 及时获得服务、overloaded client 借用剩余容量，service rate 与 response time 两个 panel 共同展示 work-conservation。 |
| Figure 6 | 11 | §30 | Client 1 在 OFF 阶段停止新请求但仍然 backlogged，直接区分 arrival pattern 与 fairness 定义中的 backlog 状态。 |
| Figure 8 | 11 | §32 | 两个 client 分别偏 decode-heavy 与 prefill-heavy，验证不同 input/output cost 和随机到达下应按 service cost 而非 request count 记账。 |
| Figure 9 | 12 | §33 | aggressive client 增压时低负载 client 的 response time 保持稳定，是 isolation 性质的直观证据；同时保留题注到达率表述不一致的警告。 |
| Figure 10 | 12 | §34 | 三阶段 workload 对比 VTC 与不做 Counter Lift 的 LCF，直接说明 leave/rejoin 和 distribution shift 下 Counter Lift 的必要性。 |
| Figure 12 | 12 | §36 | 在 LMSYS Chatbot Arena trace 上并排给出 FCFS 与 VTC 的 selected-client response time，承担真实 workload isolation 证据。 |
| Figure 15 | 14 | §39.1–§40 | 两个 panel 分别改变 KV memory pool 和 request length，把实验中的 service discrepancy 量级与 Theorem 4.4 对 $M$、request cost upper bound 的依赖对应起来。 |
| Figure 16 | 20 | §41 | standard VTC 产生近似等份额，weighted VTC 按 1:2:3:4 分配 service，是 differentiated service 的最直接验证。 |
| Figure 19 | 22 | §43 | 对两 client 和八 client 展示标准 VTC、±50% length prediction 与 oracle 的 service difference；只支持 practical discrepancy 改善，不改变 worst-case bound。 |

未加入 Figure 5：与 Figure 4/6 的 ON/OFF、work-conserving 和 backlog 语义重复，且正文已保留其 workload 定义与文字不一致边界；未加入 Figure 7：variable length 机制被更强的 Figure 8 input/output cost 对照覆盖；未加入 Figure 11：真实 trace 的 arrival-rate 条件已转写，Figure 12 才承担调度结果；未加入 Figure 13/14：RPM threshold 的 latency/utilization 权衡已由正文和 Table 2 精确记录；未加入 Figure 17/18：profiled cost function、公式和 real-trace 结果已转写，且不改变 VTC 主机制；未加入 Figure 20：长度分布范围、均值和 workload 条件已转写。Table 1–6 与 Algorithm 1–4 均已有可检索文字、公式或 Markdown 表格，不重复截图。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `research/精读文献笔记/vtc_osdi2024/figures/fig1_vtc_architecture.png` | 1094×1306 | `EF7CF84D7928188C4B07D2943DDD2DEE03BE30A9A0ABB2E34047F832AF1B2F0A` |
| `research/精读文献笔记/vtc_osdi2024/figures/fig2_length_cost_capacity.png` | 1094×599 | `578DC4088E07713CE37A419510CE4CC07686A0CBEF45281FAF08CB1DFA92DD94` |
| `research/精读文献笔记/vtc_osdi2024/figures/fig3_backlogged_fairness.png` | 1094×1080 | `354D77EEA3DFEC97C7DA8869607048D4EF7C73FE16478DB4A0C96516C0B345EB` |
| `research/精读文献笔记/vtc_osdi2024/figures/fig4_work_conservation.png` | 1094×775 | `DF4D597F898322146D13C37CA40EB6315BFCC6CC932946356225AD3FA513FBE7` |
| `research/精读文献笔记/vtc_osdi2024/figures/fig6_off_but_backlogged.png` | 1099×783 | `941F1EB85F9815B7C63EB9708FF4FB45361E41C4FC290C8A5B08C8F3C02AE926` |
| `research/精读文献笔记/vtc_osdi2024/figures/fig8_input_output_cost.png` | 1094×968 | `E205AA48952D9FA847903C1B57D9540D856747BAF1B1CC47C88F9B2B6B8A033B` |
| `research/精读文献笔记/vtc_osdi2024/figures/fig9_isolation.png` | 1099×833 | `AA2E3A8DE1066A5E86CFC74DE5E4F561E40996278E2DC17B449B63A01EB01404` |
| `research/精读文献笔记/vtc_osdi2024/figures/fig10_counter_lift_ablation.png` | 1099×1107 | `AA0D8515FFF3DFFB6F3098F0B83F542EEB156BCD1B40929073C8ECA1EFCADC79` |
| `research/精读文献笔记/vtc_osdi2024/figures/fig12_real_trace_isolation.png` | 1094×698 | `D311AB2A20A8C95B5EF83E39AA113A7EA9323058541DE954A886F69406D0E41B` |
| `research/精读文献笔记/vtc_osdi2024/figures/fig15_bound_sensitivity.png` | 1099×824 | `A86919EABD42135043D9D9B90E53100BBC7B4D7B6B569C38BBD49244BCDD161E` |
| `research/精读文献笔记/vtc_osdi2024/figures/fig16_weighted_vtc.png` | 1094×909 | `6768BADDE7B7E8E72A6DAF4214FF15FCDCCAC6D45D81CE289F9F81295D952335` |
| `research/精读文献笔记/vtc_osdi2024/figures/fig19_length_prediction.png` | 1094×972 | `C280561A1069F99E020675EC3853FF4BB3171021693D59D220D9E560E1C242CC` |

提取方式：使用 PyMuPDF 对本地 PDF 对应页面作 4.5× raster render，再按原图与英文 caption 的完整边界裁剪。没有重绘、锐化、替换颜色、修改坐标、删除图内元素或拼接不同页面；所有 multi-panel Figure 均保持整体。

## 视觉与论证 QA

- 12 张裁剪件已按原始分辨率和 contact sheet 两轮预览；架构节点、箭头、client 标识、图例、坐标轴、单位、科学计数法、panel 标签和英文 caption 完整可读，没有混入相邻 Figure。
- Figure 1/2 依靠布局、箭头、形状和文字标签表达；实验图除颜色外还使用固定 panel、marker、线型、图例、坐标位置和数值刻度，不以颜色作为唯一编码。
- Figure 2 的非精确示意边界、Figure 3 的“经验示意而非证明”、Figure 9 的题注文字冲突、Figure 12 的断线含义和 Figure 19 不改变 worst-case bound 均在精读正文来源行中显式说明。
- 12 个裁剪件分别承担调度位置、动态 cost/capacity、公平与 work-conservation、backlog、异构 token cost、isolation、Counter Lift、真实 trace、bound sensitivity、weighted fairness 和 prediction 扩展，没有收集装饰性图片。
- PNG 仅用于精读笔记显示。若将来进入正式论文或开题材料，应从正式出版 PDF 取得矢量版本或按投稿规范重新导出。

审计结论：来自 12 个论文 Figure 的 12 个裁剪件适合进入当前 VTC 精读笔记；0 个 critical、0 个 major、0 个视觉阻断项。
