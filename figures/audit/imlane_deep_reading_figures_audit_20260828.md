# IMLane 精读笔记论文原图选择与裁剪审计

日期：2026-08-28

## 范围与来源

- 使用位置：`research/精读文献笔记/imlane_pvldb2026/imlane_pvldb2026.md`。
- 来源文件：`research/reference/IMLane_PVLDB2026.pdf`，题名 *IMLane: Composable Framework for Efficient AI Function Execution in Database Engine*，PVLDB 19(12): 4223–4236，2026，DOI `10.14778/3827998.3828028`。
- 本地 PDF 共 14 页，SHA256 `C2E5334D532385384F5DF85033F302BFC7F1493B272757998110621AEB248A8A`；首页题名、作者、PVLDB reference format、页码和 DOI 已核对。
- 使用 Poppler 以 324 DPI 渲染原页，再按 Figure 边缘裁切并保留原论文 caption；没有重绘、改色、锐化或修改图内数值。
- Figures 9–10 在论文中同排且由同一段落联合解释，因此保留为一个联合裁剪件；其余 Figure 各使用一个 PNG。PNG 只服务 Markdown 精读讲解，不是项目实验结果。

## 选择结果

| 论文图 | PDF 页码 | 印刷页码 | 精读正文位置 | 独立作用 |
|---|---:|---:|---|---|
| Figure 1 | 1 | 4223 | §1.1 | 说明 Data Agent 如何用 SQL 与 Python AI Function 组合完成数据检索和欺诈识别。 |
| Figure 2 | 3 | 4225 | §1.3 | 展示 AI Function 附着于选择算子，以及 Join 作为 pipeline breaker 的物理计划切分。 |
| Figure 3 | 3 | 4225 | §1.4 | 区分纯 Python 调用和 native call，说明共享 GIL 下的线程竞争路径。 |
| Figure 4 | 4 | 4226 | §1.5 | 说明 AI Function 的并行度和执行时机如何继承数据库 partition-wise task scheduling。 |
| Figure 5 | 5 | 4227 | §3.1 | 展示关系算子留在数据库线程、完整 AI Function 移入独立 executor process 的设计。 |
| Figure 6 | 7 | 4229 | §4.1 | 用两个增量 partition 和三个 CPU core 解释粗粒度任务造成的负载不均。 |
| Figure 7 | 7 | 4229 | §4.1 | 用时间线说明同步执行时 host CPU 与异构资源交替空闲。 |
| Figure 8 | 7 | 4229 | §4.2 | 展示 scheduling request、Lane selection、handler return 与资源恢复四步流程。 |
| Figure 9 | 8 | 4230 | §4.5 | 说明 batch-wise 单元如何继续填充超出原 partition 数量的 CPU/Lane。 |
| Figure 10 | 8 | 4230 | §4.5 | 说明不同 batch 的 host CPU 阶段与异构计算阶段如何在时间上重叠。 |
| Figure 11 | 9 | 4231 | §5.1 | 定义 DBEnd Library、Coordinator、Lane 与 Backend Executors 的组件关系。 |
| Figure 12 | 10 | 4232 | §6.3 | 汇总 OceanBase/DuckDB 的端到端时间、IPC 开销、调度 variant 与 Ray backend。 |
| Figure 13 | 10 | 4232 | §6.4 | 比较 host CPU、本地 GPU、远程 CPU 和远程 GPU 的平均利用率。 |
| Figure 14 | 11 | 4233 | §6.5 | 扫描 CPU、GPU memory、远程 worker 与远程 GPU 数量，展示资源扩展趋势。 |
| Figure 15 | 12 | 4234 | §6.7 | 比较 IMLane、pandas、SparkSQL 与 Ray.data 的端到端执行时间。 |

正文 Figure 1–15 分别承担应用动机、执行瓶颈、机制设计、系统架构或独立实验结论，均能补充现有文字。Listing 1–2、Algorithm 1–2 和 Table 1 已在笔记中按接口、步骤或字段转写，不再重复截图。

## 输出与完整性

| 文件 | 像素尺寸 | SHA256 |
|---|---:|---|
| `fig1_ai_driven_workflow.png` | 1165×915 | `58C0AC61D35FA14E1B314345EF3CFB92D04DE656A20AE4E088B2DDFA09AA6A53` |
| `fig2_ai_function_physical_plan.png` | 1110×580 | `1A628ACA26018730E2FE414F248C93B2362F84B0DDA910BB77BB1DC9FACAF76A` |
| `fig3_gil_thread_parallelism.png` | 1175×850 | `274873357EFF44DA8C8203D81B924FD383D078D11397CEF384EF08F54079DD40` |
| `fig4_coupled_scheduling.png` | 1145×590 | `B57EFDEEDF2DCF30E4668ED0B7FCE706B50C933255256537F6629580F3A38034` |
| `fig5_process_parallelism.png` | 1175×910 | `EA8EE8E167C26216A7960BA88B18D040C7A2108A8BB662B2C9D7FFCF84739955` |
| `fig6_partition_load_imbalance.png` | 640×515 | `C28DC93743564D98D0F8443745AA04116C9277D0432331117CBA88E4A7667ECB` |
| `fig7_sync_heterogeneous_idle.png` | 475×515 | `8FF7D5FBD3E53072E143A39861806EE5A15E94DBA98554D9F8EB7C24D8924D7C` |
| `fig8_resource_aware_scheduler.png` | 1130×525 | `AC933926E634C716C02707B141A18106AD436321A48C36AE69A98876A7D57F69` |
| `fig9_10_batchwise_async_scheduling.png` | 1135×580 | `191AF40066DA957F3191CAE70C77B8E227484BDBC9A7E385072EA5D82E13A196` |
| `fig11_imlane_architecture.png` | 1200×930 | `D946458D1F361FAF2FDC56612DA257F6EAB4BD1056DB35B91BD52A66A77203FB` |
| `fig12_end_to_end_and_ipc.png` | 1285×1020 | `A5FB2360B115DB01CEE8FDAFCF2BBD145E1E133312047FBCA76CFF091D281418` |
| `fig13_resource_utilization.png` | 1035×1020 | `F239D5A5CAD55E3F73B7F0E7B6631A729DCE3668220EE14502152297DE227AB2` |
| `fig14_resource_scalability.png` | 2325×1145 | `AE035B8285F01B1F129F1EBB7E6EA440134A78755757EF6BA767553CE919B605` |
| `fig15_external_system_comparison.png` | 1120×515 | `5228B0D4FE9066A5A0A40105A42A69875BF8EEACEE9431AB41802FBB2C8B0339` |

所有输出位于 `research/精读文献笔记/imlane_pvldb2026/figures/`。正文有 14 个本地图片引用，覆盖论文 15 个 Figure；Figures 9–10 共用一个联合裁剪件。

## 视觉与论证检查

- 14 张 PNG 已按原始分辨率逐张预览；图例、坐标轴、组件标签、数值、原论文 caption 均完整，没有混入页眉、相邻正文或下一张图。
- Figures 1–11 中除 Figures 12–15 外均按应用、瓶颈、机制或架构示意讲解，不作为性能结果。Figure 5 的故障隔离优势没有对应故障注入实验，正文已经说明这一限制。
- Figure 12 使用对数纵轴；柱底点状部分表示 IPC 开销。图中不同 query 的绝对时间跨度很大，笔记只在同一 query 和数据库内比较 variant。
- Figure 13 是 AI Function 执行期间的平均资源利用率，不是完整查询的连续时间线；Q5、Q6、Q7 分别使用本地 GPU、远程 CPU 与远程 GPU。
- Figure 14 同时包含执行时间柱和 speedup 虚线，而且各 query 的资源横轴、纵轴量级不同；笔记不从图中估算正文未报告的精确值。
- Figure 15 的比较包含 pandas、SparkSQL、Ray.data 从 OceanBase 拉取数据的完整路径开销；Q3 的 pandas 结果超过 1 小时，图中以 `>1h` 标记，不把该图外推为任意数据源上的通用框架排名。

审计结论：正文 Figure 1–15 均已插入对应讲解位置；15 个 Figure 使用 14 个裁剪件，0 个 critical、0 个 major、0 个视觉阻断项。
