# 开题报告专用图片

本目录保存 `opening/report/opening_report.md` 实际引用的图片及历史候选图片副本。数据图来自已经审计的开题专用图集；新架构图的权威源位于 `figures/architecture/`。本目录只服务 Markdown 与后续 Word/WPS 排版，不作为实验数据或绘图脚本的事实来源。

| 报告图号 | 文件 | 权威源 | 正文作用 |
|---:|---|---|---|
| 图 1 | `fig07_work_state_capacity.png` | `P07_动机证据_工作量运行状态与容量边界.png` | 区分记录数、配置上限与实际在途工作 |
| 图 2 | `target_architecture_status.png` | `figures/architecture/opening_target_architecture_status.png` | 区分目标数据库内算子路径与当前可运行证据链 |
| 图 3 | `fig06_text_baseline_boundaries.png` | `P06_文本基线_执行路径与可比边界.png` | 说明两类文本证据只能在各自计时范围内解释 |
| 图 4 | `fig08_work_organization_regime.png` | `P13_数据组织_服务压力与局部性权衡.png` | 说明数据组织排名依赖完整服务配置 |
| 图 5 | `fig10_shared_credit_tradeoff.png` | `P15_共享调度_效率隔离与公平权衡.png` | 说明共享未用份额的效率收益与 Job 保护取舍 |
| 图 6 | `fig11_image_stage_evidence.png` | `P08_图像阶段_准备传输与GPU执行失配.png` | 支撑图像工作量的分阶段描述与提交范围选择 |

其余 `fig01` 至 `fig14` 文件是 2026-08-20 版本使用过的历史副本或配套候选图。当前正文只引用上表六张图，避免把可行性分析写成实验结果汇编。旧文件暂不删除，以便核对早期版本；恢复引用前必须重新检查图号、术语和正文主张。

正式排版时优先使用这些 PNG 副本保证 Markdown 和 Word/WPS 兼容。若图中数据、术语或结论需要修改，应回到权威源及对应绘图脚本重新生成，再同步覆盖本目录，不能直接在 PNG 上改字或改数值。

2026-08-22 根据报告实验口径复核结果重新同步图 7～图 11：图 7 明确两组数据同时改变了服务实例数、每实例显存比例和运行压力；图 8 将“active work、endpoint、formal”等图内简写改为在途工作量、模型服务实例和统计运行；图 9 的静态与共享完成进度统一使用“作业独占完整资源”作为归一化参照，Jain 指数为 0.988 与 0.876；图 10 改为直接说明排序准确率和决策损失参考值，并明确留出的是输入条件组合而非新硬件或新模型；图 11 将“瓶颈不是 PCIe”的过强判断改为“输入表示与主机端数据组织会改变阶段效率”。图中数据未更换。

2026-08-23 再次同步图 10。新版本为六种估计方法分别绘制 80 组候选均值的真实时间和预测时间，空心点与实心点之间的竖线表示逐候选相差的秒数；同页保留四种上限的两两排序和错误选择后的额外耗时。正文同步说明解析模型的中位绝对相对偏差较小、岭回归的候选均值平均绝对误差较小，避免把混合模型的排序结果误写成单点时间预测最准确。

2026-08-23 同步修正当时报告使用的图 2、图 3、图 5 和图 6。完整审计见 `figures/audit/opening_report_minimal_figure_corrections_audit_20260823.md`。

2026-08-24 报告重构后只保留六张正文图。新增图 2，明确 PostgreSQL child plan、快照和查询生命周期仍待实现，当前外部读取链只提供有界算子接口的性能证据。生成脚本为 `figures/scripts/generate_opening_target_architecture_status.py`，图件审计见 `figures/audit/opening_target_architecture_status_audit_20260824.md`。
