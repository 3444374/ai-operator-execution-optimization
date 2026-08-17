# 开题专用图集

这是开题 PPT 和开题报告的统一选图入口。原始图仍保留在
`figures/architecture/editable/` 与 `figures/data/report_main/`；本目录只保存按当前
20 页答辩大纲筛选、按页码重命名的副本，不替代可复现源数据和原始审计。

## 从哪里拿图

| 目录 | 内容 | 使用建议 |
|---|---|---|
| `main_png/` | 19 张 PPT 主讲候选图的 PNG 副本 | 快速预览、兼容性优先的 PPT/Word |
| `main_svg/` | 同一 19 张 PPT 主讲候选图的 SVG 副本 | PPT、Word 和报告优先使用，缩放不失真 |
| `editable_drawio/` | 8 张概念图的 Draw.io 编辑源 | 修改文字、卡片、箭头或 icon 时使用 |
| `backup_png/` | 2 张答辩备份数据图的 PNG | 回答单 Job 排队与状态问题时使用 |
| `backup_svg/` | 同一 2 张备份图的 SVG | 需要放大或进入文档时使用 |

数据图不使用 Draw.io：它们由冻结结果和
`figures/scripts/generate_opening_story_figures_20260808.py` 可复现生成。

## 主讲最小集

| 页码 | 可读文件名 | 页面任务 | 权威源 |
|---:|---|---|---|
| 02 | `P02_背景_数据库AI算子外部执行链路` | 展示数据库 AI 算子、外部执行层、模型服务状态和写回闭环 | `architecture/editable/opening_background_20260812/06_ai_native_execution_architecture` |
| 03 | `P03_背景_传统算子与外部AI执行假设` | 对照传统成本字段与通用外部 AI 算子的多阶段执行链路 | `architecture/editable/opening_background_20260812/07_traditional_vs_external_ai_operator` |
| 04 | `P04_相关工作_跨层执行闭环` | 按层归纳代表工作，指出数据库任务语义与服务运行信息衔接不足 | `architecture/editable/opening_background_20260812/08_related_work_landscape` |
| 05 | `P05_研究空白_AI数据执行层` | 归纳工作量表达、作业—服务状态对应和多作业约束三类研究空白 | `architecture/editable/01_research_gap` |
| 06 | `P06_文本基线_执行路径与可比边界` | 分开说明 database-E2E 与官方 Chat graph 的可比范围 | `data/report_main/opening_text_baseline_evidence_map` |
| 07A | `P07A_动机证据_记录数与模型工作量` | 原 P07 panel a：说明相同行数仍可能对应 14.3× 工作量差异 | `data/report_main/opening_motivation_work_state_part1_work` |
| 07B | `P07B_动机证据_运行状态与容量边界` | 说明静态上限、运行状态与近饱和边界不是同一概念；65K→98K 吞吐仅 +2.3%，P99 +8.9% | `data/report_main/opening_motivation_work_state_part2_state_capacity` |
| 08A | `P08A_图像阶段_准备阶段失衡` | 原 P08 panel a：说明图像也是分阶段工作量，张数描述不了阶段压力（prepare/GPU 13.9–31.0×） | `data/report_main/opening_image_stage_aware_evidence_part1_prepare` |
| 08B | `P08B_图像阶段_传输形态与提交窗口` | 原 P08 panels b–c：说明传输形态和 active-window 会改变执行表现 | `data/report_main/opening_image_stage_aware_evidence_part2_transfer_window` |
| 09 | `P09_文本多作业_原生路径并发干扰` | 证明共享模型服务下的多 Job 干扰是现实现象 | `data/report_main/opening_native_fourjob_normalized_impact` |
| 11 | `P11_系统架构_数据组织与状态调度闭环` | 展示 work、state、bounded control 与 sink 的总体闭环 | `architecture/editable/02_system_architecture` |
| 12 | `P12_研究内容一_WorkUnit与数据组织` | 展示分阶段 work、候选 packing 和同 budget 评价 | `architecture/editable/03_work_unit` |
| 13 | `P13_数据组织_服务压力与局部性权衡` | 用数据说明组织策略具有 regime dependency | `data/report_main/opening_work_organization_regime_v2` |
| 14 | `P14_研究内容二_状态感知提交与多作业调度` | 展示安全准入、共享额度、公平队列、路由与释放 | `architecture/editable/04_state_aware_scheduling` |
| 15 | `P15_共享调度_效率隔离与公平权衡` | 展示同上限静态/共享 A/B 的条件性收益与代价 | `data/report_main/opening_multijob_interference_tradeoff` |
| 16 | `P16_代价估计_配置选择与决策质量` | 说明代价估计要改善配置排序与 decision regret | `data/report_main/opening_cost_model_decision_quality_v2` |
| 17 | `P17_图像基线_执行路径与可比边界` | 区分 12K 诊断和 120K matched-resource 正式对照 | `data/report_main/opening_image_baseline_evidence_map` |
| 18 | `P18_图像多作业_并发干扰` | 展示图像路径内部的 four-job/isolated slowdown | `data/report_main/opening_image_fourjob_normalized_impact` |
| 19 | `P19_研究基础与后续工作计划` | 概括已完成基础、后续工作方向与评价维度 | `architecture/editable/05_evidence_gate` |

第 10 页承担研究问题归纳，第 20 页承担总结与答辩收束，当前不需要另画独立大图。开题报告正文
可从 P05–P19 的拆分候选中按版面选图；P02–P04 是 PPT 背景与相关工作专用图。本轮只更新图资产，未重新生成 PPT。

## 答辩备份

| 文件名 | 用途 | 权威源 |
|---|---|---|
| `B01_文本单作业_请求延迟分解` | 解释相近 Job JCT 为什么可能掩盖 request queue 与 TTFT | `data/report_main/opening_native_single_job_request_latency` |
| `B02_文本单作业_服务状态指纹` | 区分欠供给、最小饱和与过量排队 | `data/report_main/opening_native_single_job_state_fingerprint` |

## 维护规则

1. PPT/Word 只看图时先进入 `main_png/`；正式排版优先替换为 `main_svg/` 同名文件。
2. 修改概念图时编辑 `editable_drawio/` 对应文件，并同步更新权威源、SVG、PNG 与审计；不要在
   PNG 上覆盖文字或边框。
3. 修改数据图时从冻结结果重新运行绘图脚本，不手工改图中数值。
4. 权威源更新后必须同步覆盖本目录同名副本，避免图集与正式结果分叉；概念 SVG 覆盖后运行
   `python3 figures/scripts/embed_svg_assets.py <SVG文件>` 将相对 icon 重新内嵌，保证单文件可移植。
5. 不把 static–dynamic 示意曲线、DuckDB 多 Job、Daft 60K×2 容量边界图、ShareGPT
   database-E2E 三臂性能图或跨框架绝对 short JCT 放入本图集；这些内容目前缺少正式结果或
   不满足可比合同。

选择与质检记录见 `figures/audit/opening_figure_set_manifest_20260811.md`。
