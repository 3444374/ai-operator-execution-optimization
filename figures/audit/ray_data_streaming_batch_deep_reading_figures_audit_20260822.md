# Ray Data Streaming Batch 精读笔记论文原图选择与裁剪审计

日期：2026-08-22

## 范围与证据边界

- 使用位置：`research/精读文献笔记/ray_data_streaming_batch_nsdi2027/ray_data_streaming_batch_nsdi2027.md`。
- 权威来源：用户本地文献目录中的 `ray_data_streaming_batch_nsdi2027.pdf`，题名 *The Streaming Batch Model for Efficient and Fault-Tolerant Heterogeneous Execution*，arXiv:2501.12407v5（2025-10-22）。PDF 首页没有正式 venue，审计不根据文件名把它写成 NSDI 2027。
- PDF：19 页，SHA256 `2F720B1A040C89DC1E5469DF3A1C77D8ECC8A2C143E7E7B5876F13EA11AE4FD0`；已通过 `%PDF` 签名、题名和页数解析检查。
- 源文件没有复制进 `research/reference/`，因此不改变项目参考 PDF 子集及其计数。
- 这些 PNG 是论文原图的局部裁剪，只服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 2 | 3 | §2.5 | 在同一输入上并列 batch、streaming 与 streaming batch 的 partition/resource 绑定方式，是全文执行模型差异的最短视觉定义。 |
| Figure 4 | 4 | §3 Challenge 1 | 直接展示 static 与 dynamic repartition 对流水启动和峰值内存的影响，支撑动态 partition 不只是负载均衡技巧。 |
| Figure 5 | 4 | §3 Challenge 2 | 用资源气泡和内存曲线解释 pessimistic policy 为何安全但空闲、optimistic policy 为何能提前启动上游。 |
| Figure 6 | 5 | §3.2 | 把 lazy Dataset、logical/physical DAG、Ray Data scheduler、worker 与 object store 置于同一架构边界中。 |
| Figure 7 | 9 | §5.1 | 三个子图覆盖 RAG JCT、视频分类吞吐和故障恢复，是 full-system inference 证据的完整总览。 |
| Figure 9 | 11 | §5.3.1 | 同时比较系统和 `-Part.`/`-Adapt.` 两项消融在 6–16 GB 下的 JCT/OOM，直接验证两项机制各自不可省略。 |

未加入 Figure 1：两条异构 dataflow 已在 §2.2 用 ASCII 流程完整转述；未加入 Figure 3：时间线含义与 Figure 2 的结构对比重复，正文已明确说明 stage barrier 和 compute bubble；未加入 Figure 8：ResNet 与 Stable Diffusion 的 throughput、资源、run time 和 cost 已逐项转写；未加入 Figure 10/11：partition overhead、scalability 与 fractional parallelism 是次级边界，正文已保留准确数值和解释。Table 1/2 与 Algorithm 1/2 已转写为可检索文字，不重复截图。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `research/精读文献笔记/ray_data_streaming_batch_nsdi2027/figures/fig2_execution_model_comparison.png` | 2382×664 | `5C3B38B75F5D7984878CECD46782AD9ACF10B71080AB74A885F8BC41E93A4F5C` |
| `research/精读文献笔记/ray_data_streaming_batch_nsdi2027/figures/fig4_dynamic_repartition.png` | 1063×573 | `F48FBC2CCE28B470629AD1C33E226DAD2ECFABF5E723269310FEF3DEDDDEF77E` |
| `research/精读文献笔记/ray_data_streaming_batch_nsdi2027/figures/fig5_memory_aware_scheduling.png` | 1084×460 | `5749BC5B0C6BB42AB746BD4128CD6D2032FBD719738A3EB3FCC91B3B432E1DF9` |
| `research/精读文献笔记/ray_data_streaming_batch_nsdi2027/figures/fig6_ray_data_architecture.png` | 1084×501 | `F67782AEA5EB84F93C44954293D651F13BF1FE4C8B76E1F41E450F4767E6873D` |
| `research/精读文献笔记/ray_data_streaming_batch_nsdi2027/figures/fig7_end_to_end_evaluation.png` | 2402×746 | `20E45E4CF8EB78E820CD1133BD0FBC4D7BC62B9F188C61C91B4BF1B64F4BE8BC` |
| `research/精读文献笔记/ray_data_streaming_batch_nsdi2027/figures/fig9_memory_aware_pipelining_ablation.png` | 1206×541 | `D442BA65506678FC2D376BEFAE32CC00B1EFB6DB7FB0FAF5208015E41498DA6C` |

提取方式：使用 `pypdfium2` 对 PDF 对应页面作 4.5× raster render，再按原图图形边界裁剪。没有重绘、锐化、替换颜色、修改坐标或删除图内元素；论文英文 caption 不进入裁剪件，由精读正文的中文 alt text 和来源行承担说明。

## 视觉与论证 QA

- 6 张图已按原始分辨率预览；panel 标签、partition ID、资源标签、箭头、时间/JCT/吞吐坐标轴、图例、热力图数值和 OOM 灰格完整可读，没有混入相邻正文或残缺 caption。
- Figure 2/4/5/6 主要靠布局、形状、标签和箭头表达机制；Figure 7 使用颜色、线型和 panel 分区；Figure 9 每格有数值且灰色代表 OOM，不只依赖红绿颜色。
- 坐标范围、图例和图内数值保持论文原样，没有截轴、重排系列或视觉夸大。
- 6 张图分别承担执行模型、两项核心机制、系统架构、full-system 结果与机制消融，没有收集装饰性图片。
- PNG 仅用于精读笔记显示。若将来进入正式论文或开题材料，应从源 PDF 取得矢量版本或按投稿规范重新导出。

审计结论：6 张图适合进入当前 Ray Data 精读笔记；0 个 critical、0 个 major、0 个视觉阻断项。
