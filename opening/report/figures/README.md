# 开题报告专用图片

本目录保存 `opening/report/opening_report.md` 实际引用的图片及少量配套候选图片副本。图片均来自项目已经冻结并通过审计的开题专用图集，权威源仍位于 `figures/opening_figure_set/`，本目录不作为实验数据或绘图脚本的事实来源。

| 报告图号 | 文件 | 权威源 | 正文作用 |
|---:|---|---|---|
| 图 1 | `fig01_external_ai_operator_assumptions.png` | `P03_背景_传统算子与外部AI执行假设.png` | 解释传统数据库算子与外部 AI 算子的执行假设差异 |
| 图 2 | `fig02_ai_data_execution_gap.png` | `P05_研究空白_AI数据执行层.png` | 界定数据库与模型服务之间的研究对象 |
| 图 3 | `fig03_work_unit_organization.png` | `P12_研究内容一_WorkUnit与数据组织.png` | 说明分阶段工作量描述与数据组织候选机制 |
| 图 4 | `fig04_state_aware_scheduling.png` | `P14_研究内容二_状态感知提交与多作业调度.png` | 说明固定容量下的提交、路由和多作业调度 |
| 图 5 | `fig05_system_architecture.png` | `P11_系统架构_数据组织与状态调度闭环.png` | 展示总体技术路线及数据流、控制流和状态反馈 |
| 图 6 | `fig06_text_baseline_boundaries.png` | `P06_文本基线_执行路径与可比边界.png` | 区分数据库端到端产品轨和框架原生执行轨 |
| 图 7 | `fig08_work_organization_regime.png` | `P13_数据组织_服务压力与局部性权衡.png` | 说明数据组织效果受缓存压力和运行条件影响 |
| 图 8 | `fig07_work_state_capacity.png` | `P07_动机证据_工作量运行状态与容量边界.png` | 支撑工作量描述、运行状态观测和容量标定动机 |
| 图 9 | `fig10_shared_credit_tradeoff.png` | `P15_共享调度_效率隔离与公平权衡.png` | 说明共享工作量额度的效率收益和公平代价 |
| 图 10 | `fig14_cost_decision_quality.png` | `P16_代价估计_配置选择与决策质量.png` | 说明代价估计需按配置排序和决策损失评价 |
| 图 11 | `fig11_image_stage_evidence.png` | `P08_图像阶段_准备传输与GPU执行失配.png` | 支撑图像工作量的分阶段描述与有界提交动机 |

未插入正文的配套候选图片为：`fig09_text_native_multijob.png`、`fig12_image_baseline_boundaries.png` 和 `fig13_image_multijob.png`。相关事实已经由正文与图 6、图 9、图 11 说明，暂不继续增加图量，以免可行性分析重新变成实验结果汇编。

正式排版时优先使用这些 PNG 副本保证 Markdown 和 Word/WPS 兼容。若图中数据、术语或结论需要修改，应回到权威源及对应绘图脚本重新生成，再同步覆盖本目录，不能直接在 PNG 上改字或改数值。
