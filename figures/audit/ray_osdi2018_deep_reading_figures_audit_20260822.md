# Ray OSDI 2018 精读笔记论文原图选择与裁剪审计

日期：2026-08-22

## 范围与证据边界

- 使用位置：`research/精读文献笔记/ray_osdi2018/ray_osdi2018.md`。
- 权威来源：用户本地文献目录中的 `ray_osdi2018.pdf`，题名 *Ray: A Distributed Framework for Emerging AI Applications*，13th USENIX Symposium on Operating Systems Design and Implementation（OSDI ’18）正式版。
- 正式 PDF：18 页，SHA256 `066FECEE9604CA232B5FBAEAA7DD260C88149A1BE6DD4357EF16705986B99290`；已通过 `%PDF` 签名、首页题名和页数解析检查。
- 源文件没有复制进 `research/reference/`，因此不改变项目参考 PDF 子集及其计数。
- 这些 PNG 是论文原图的局部裁剪，只服务 Markdown 精读讲解；不是项目自制图，也不是本项目实验结果。

## 选择结果

| 论文图 | PDF 页码 | 精读正文位置 | 选择理由 |
|---|---:|---|---|
| Figure 4 | 6 | §4.6 | 以 data/control/stateful 三类 edge 把 stateless task、actor method、object 与 nested invocation 放进同一 dynamic task graph，是 Task/Actor 统一语义的核心图。 |
| Figure 5 | 7 | §4.7 | 一图给出 application/system 两层、per-node object store/local scheduler、GCS、global scheduler 与调试工具，是全文架构地图。 |
| Figure 6 | 8 | §4.12–§4.15 | 箭头粗细直观说明大多数 task 走本地路径，只有必要时才进入 global scheduler，文字难以替代其层次和请求率关系。 |
| Figure 7 | 9 | §4.18–§4.19 | 同图追踪 remote task 与 `ray.get()`，并区分 control plane 和 data plane，是理解 scheduling、metadata callback 和 object replication 完整路径的必要图。 |
| Figure 8 | 9 | §5.1.1–§5.1.2 | 两个子图分别验证 locality-aware placement 和 empty-task architecture scalability，同时保留“1.8M tasks/s 不是普遍 workload 吞吐”的上下文。 |
| Figure 10 | 10 | §5.1.4–§5.1.5 | 上下两个实验分别承担 GCS reconfiguration 延迟与 lineage flushing 内存上界，因此拆成 Figure 10a/10b 两个裁剪件放到对应小节。 |
| Figure 11 | 11 | §5.1.6–§5.1.7 | 同时展示 task lineage reconstruction 与 actor checkpoint recovery，直接支撑 Ray 的统一容错模型及 actor 额外 checkpoint 边界。 |
| Figure 12 | 11 | §5.1.8–§5.1.9 | 同图保留 small-object OpenMPI 反例和 scheduler latency 消融，避免把 Ray allreduce 写成无条件更优。 |
| Figure 14 | 13 | §5.3 | ES 与 PPO 是论文完整 RL application 结果，展示编程模型如何通过 aggregation tree 和异构资源布局转化为应用级表现。 |

未加入 Figure 1：RL simulation/training/serving 关系已在 §2.2 用 ASCII 流程完整展开；未加入 Figure 2/3：pseudocode 与 Ray Python 示例已经转写为可检索代码和步骤；未加入 Figure 9：object store 的 IOPS/GB/s、线程数和边界已在 §5.1.3 精确记录；未加入 Figure 13：training building-block 的相对结论已在正文保留，且不承担 Ray 核心架构或当前课题最关键的独立机制。Table 1–4 均已转写为文字或 Markdown 表格，不重复截图。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `research/精读文献笔记/ray_osdi2018/figures/fig4_dynamic_task_graph.png` | 1145×961 | `91500185613D20200A5E759BF8AB2D43DA43871BCCBF9636EDC35863A5D04AC8` |
| `research/精读文献笔记/ray_osdi2018/figures/fig5_ray_architecture.png` | 1156×675 | `AA5C82277FB0BBBE2947B969E1227FAA33C159E92AAEF41A796091AC4610BEE7` |
| `research/精读文献笔记/ray_osdi2018/figures/fig6_bottom_up_scheduler.png` | 1156×685 | `2A942EF1CEE5F18F819855D61A2A9CD656E346B89ACC1AC7523DC3E25D155220` |
| `research/精读文献笔记/ray_osdi2018/figures/fig7_end_to_end_execution.png` | 1156×1268 | `2C63C01686CD91217528FC0E5AA422D33B28236FCE2D8876F1FA12AEB7E3B5A2` |
| `research/精读文献笔记/ray_osdi2018/figures/fig8_locality_and_scalability.png` | 1165×583 | `E74DC0325500BAD7B0B0E25F073919C3F665577F4546548CB16F6F18E7EE4024` |
| `research/精读文献笔记/ray_osdi2018/figures/fig10a_gcs_reconfiguration.png` | 1155×614 | `444A97CAF74FDECE6F611207C540E89FF5DC63A6B622503C783CAC57DBBF487B` |
| `research/精读文献笔记/ray_osdi2018/figures/fig10b_gcs_flushing.png` | 1155×593 | `CA1749647F1CB3D16B8959DE8DBFBA16F2AB956D52EC3D93A3C1E7936C4C1396` |
| `research/精读文献笔记/ray_osdi2018/figures/fig11_fault_tolerance.png` | 1176×1340 | `CB9718A537B9F0C2234790474D7E33C658E68BB8C2D44CD972003A749A501D7E` |
| `research/精读文献笔记/ray_osdi2018/figures/fig12_allreduce_scheduler_ablation.png` | 1175×573 | `A73C2675F980A76BE4E5810A3BE0851DF676B6CFB59CB99C0491002ACC3FC34A` |
| `research/精读文献笔记/ray_osdi2018/figures/fig14_rl_applications.png` | 1175×634 | `34CD2996C97DB077C698982138C2C12254214AFFDF6DDB5261EDA6D63C657148` |

提取方式：使用 `pypdfium2` 对正式 PDF 对应页面作 4.5× raster render，再按原图图形边界裁剪。Figure 10 的两个 panel 因垂直分布且对应两个独立正文小节，分别输出；其余 multi-panel Figure 保持为一个整体。没有重绘、锐化、替换颜色、修改坐标或删除图内元素；论文英文 caption 不进入裁剪件，由精读正文的中文 alt text 和来源行承担说明。

## 视觉与论证 QA

- 10 张裁剪件已按原始分辨率预览；节点名、edge 类型、层次标签、箭头、步骤号、图例、坐标轴、单位、对数轴、error bar 和红叉完整可读，没有混入相邻正文或残缺 caption。
- Figure 4/5 主要依赖布局、形状、线型和标签；Figure 6/7 同时用箭头方向、粗细、实虚线和颜色；实验图除颜色外还使用固定系列位置、线型、数值/刻度或 panel 分区。
- Figure 10a 和 Figure 12a 的对数轴保持原样；所有坐标范围、图例和图内数值均未修改，没有截轴或视觉夸大。
- 10 个裁剪件分别承担编程模型、架构、调度、执行路径、性能与容错机制、调度消融和完整应用结果，没有收集装饰性图片。
- PNG 仅用于精读笔记显示。若将来进入正式论文或开题材料，应从正式 PDF 取得矢量版本或按投稿规范重新导出。

审计结论：来自 9 个论文 Figure 的 10 个裁剪件适合进入当前 Ray OSDI 2018 精读笔记；0 个 critical、0 个 major、0 个视觉阻断项。
