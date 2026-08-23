# AYO 精读笔记论文原图选择与裁剪审计

日期：2026-08-22

## 范围与证据边界

- 使用位置：`research/精读文献笔记/ayo_asplos2025/ayo_asplos2025.md`。
- 权威来源：用户提供的 ASPLOS ’25 正式版 `ayo_asplos25.pdf`，题名 *Towards End-to-End Optimization of LLM-based Applications with Ayo*，DOI `10.1145/3676641.3716278`。
- 正式 PDF：15 页，SHA256 `98C93EC0804FCA7D549A1EF7430AC77BF71849884CD41BF764CF62FEA181AF7B`；已通过 `%PDF` 签名、题名和页数解析检查。
- 源文件位于用户的本地文献目录，本轮没有复制进 `research/reference/`，因此不改变项目参考 PDF 子集及其计数。
- 这些 PNG 是 AYO 正式论文原图的局部裁剪，仅服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 1 | 2 | §1.1 | 模块级 latency breakdown 直接支撑“non-LLM component 不可忽略”的动机，堆叠比例关系不适合只靠文字复述。 |
| Figure 3 | 4 | §3.2 | 同图展示 module-level workflow、primitive-level dataflow graph 和 optimized e-graph，是 Ayo 表示转换的核心证据。 |
| Figure 4 | 4 | §4.1–§4.2 | 两个 running example 同时解释 request correlation 与 dependency，直观说明 request-level batching 为何不等于 application-level optimization。 |
| Figure 5 | 5 | §5.1 | 把离线 registry/profile/template、Graph Optimizer、两级 runtime scheduler 和 backend engines 放到一张架构图中。 |
| Figure 6 | 7 | §8 | 在 Advanced RAG e-graph 上同时标出四个 optimization pass，是理解各 pass 如何叠加的必要机制图。 |
| Figure 7 | 8 | §10.3 | 用两个 query 的 depth 与队列顺序对比 blind batching 和 topology-aware batching，展示 graph progress heuristic。 |
| Figure 8 | 10 | §15 | 4×4 子图完整覆盖四类应用、不同模型、数据集和请求速率，是论文端到端主结果的总览。 |
| Figure 9 | 11 | §16 | 单独验证 naive/advanced RAG 共置时的性能关系，承担多应用共享基础设施这一独立结论。 |
| Figure 10 | 12 | §17.1 | 分离 parallelization 与 pipelining 的 graph optimization 收益，是四个 pass 不是空壳的直接消融证据。 |
| Figure 11 | 12 | §17.2 | 单独验证 topology-aware batching 的收益，避免把 runtime scheduling 效果混入 graph optimizer。 |
| Figure 12 | 12 | §18.4 | 展示 request rate 增长后 queuing 占比上升，以及 GraphOpt / communication 相对较小的 overhead 结构。 |

未加入 Figure 2：五类 workflow 已在正文用独立 ASCII 流程完整转述。未加入 Algorithm 1、Algorithm 2、Table 2、Table 3 截图：步骤、字段与数值已转写为可检索文字或 Markdown 表格。上述未附项的原占位符均已改成明确原因，不留下伪完成占位。

## 输出与完整性

| 文件 | SHA256 |
|---|---|
| `research/精读文献笔记/ayo_asplos2025/figures/fig1_latency_breakdown.png` | `B7B9ED98FAB6F2B295C74B77923AEE0A21DC19EF76B1E7B856863DA481FE6A1A` |
| `research/精读文献笔记/ayo_asplos2025/figures/fig3_workflow_to_optimized_graph.png` | `70AC82B0550D6C68BBE9948853FD82734F19E85394CAEBC35284039D307FCAC0` |
| `research/精读文献笔记/ayo_asplos2025/figures/fig4_application_aware_batching.png` | `A42A7DE34C402907F03AF63F1433ADD5E24F6EDE477BD9E480A3E06474BDBBE7` |
| `research/精读文献笔记/ayo_asplos2025/figures/fig5_system_overview.png` | `17AA424AB01AAA9950A0C3D188ECF64FC49FB0D6F5E90AD2D12A03F03A633B4B` |
| `research/精读文献笔记/ayo_asplos2025/figures/fig6_advanced_rag_optimized_egraph.png` | `4E7A2DD0DAEE1DA08BB8BAB31FFA65B616EFC58DD14286C5544D02A6917B4F72` |
| `research/精读文献笔记/ayo_asplos2025/figures/fig7_topology_aware_batching.png` | `5E296C87D775BDBCF54DF3B5C20D2A1687858608209272CC9F2657CDD86256AB` |
| `research/精读文献笔记/ayo_asplos2025/figures/fig8_end_to_end_performance.png` | `BFA1845CB5EB6C16A8F36E73E8EAC62D81939C53BF14EF251E4C19E6416A8E77` |
| `research/精读文献笔记/ayo_asplos2025/figures/fig9_colocated_applications.png` | `410E2AFD5ADE0A581D60D6D9436AD583BCAF9DE10D475EBA631BBB1AAD63C395` |
| `research/精读文献笔记/ayo_asplos2025/figures/fig10_graph_optimization_ablation.png` | `8B3017FCB557C989D8C8E93B7817E1CF2E6AAABB6B9AB9CCB741E8F136F11CB0` |
| `research/精读文献笔记/ayo_asplos2025/figures/fig11_topology_batching_ablation.png` | `398F1DF6041844CA3E7B0C0688F413B484CF317E9A63B2F74BC8986D14061246` |
| `research/精读文献笔记/ayo_asplos2025/figures/fig12_latency_breakdown.png` | `DE7E08AB60869021E3BEAA2481A7A1582AC7DE8933BFFE8991D454E1680F9964` |

提取方式：使用 `pypdfium2` 从正式 PDF 对对应页面作高分辨率 raster render，再按原图图形边界裁剪。Figure 1/3/4/5/6/7/8 使用 4×，Figure 9 使用 5×，Figure 10/11 使用 6×，Figure 12 因原图较小使用 7×。没有重绘、锐化、替换颜色、修改坐标或删除图内元素；论文英文 caption 不进入裁剪件，改由精读正文的中文 alt text 和来源行承担说明。

## 视觉与论证 QA

- 11 张图已按原始分辨率预览；图例、节点、箭头、batch size、depth、panel 标签、坐标轴、单位和时间标注可读，没有混入相邻正文或残缺英文 caption。
- Figure 1 使用颜色与纹理；Figure 3–8、10、11 同时使用节点形状、边框、marker、线型或文字，不只依赖颜色。
- Figure 9 与 Figure 12 的论文原图主要依赖颜色区分系列，分别由固定方法顺序 / 数值标注和固定堆叠顺序缓解；精读裁剪保持原图，不擅自重绘。这是 2 个 minor 可访问性提示，不构成当前笔记的阻断项。
- 所有坐标范围、图例和图内数值保持论文原样，没有截轴或视觉夸大。
- 11 张图分别承担动机、表示、架构、机制、主实验、共置实验、两类消融和 overhead 解释，没有收集装饰性图片。
- PNG 仅用于精读笔记显示。若将来进入正式论文或开题材料，应从正式 PDF 取得矢量或按投稿规范重新导出，不直接把这些 Markdown PNG 当作投稿图。

审计结论：11 张图适合进入当前 AYO 精读笔记；0 个 critical、0 个 major、2 个 minor 原图可访问性提示，无视觉阻断项。
