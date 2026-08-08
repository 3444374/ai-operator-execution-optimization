# 2026-08-08 开题叙事图：证据与视觉审计

## 共同边界

- 目标：让动机中的现象、挑战和后续设计逐项对应，而不是用最终算法结果倒推动机。
- 输出：`generate_opening_story_figures_20260808.py` 同时生成 PNG 与 SVG；PNG 用于报告与答辩内容大纲，SVG 保留可编辑文字。当前不制作 PPT 成品。
- 统计：只读取项目内正式 CSV/JSON 或冻结画像数据，不手填结果，不混入 warm-up。
- 证据层级：动机图证明问题与研究必要性；组织、图像和代价图证明已有机制/可行性信号；均不证明待研究的状态感知动态策略已经优于同上限静态基线。
- 视觉规则：一张图一个主句；无未解释散点；标签不压数据、标题或图例；颜色之外同时保留位置、形状或文字标签。2026-08-08 已统一为中文主标题/轴/直接标注，保留 WorkDescriptor、MFU、token 等必要技术词。

## 1. Work、状态与提交压力动机

文件：`data/report_main/opening_motivation_work_state.{png,svg}`。

- panel a 来源：固定 16 行批次的 work 最小/最大中位数 474/6,793 token，差 14.3×。
- panel b 来源：相同配置 W65K 下，高 offered load 的 observed active work 为 100%、MFU 约 35%；arrival-limited 状态分别约 29% 和 7%。
- panel c 来源：active-work 八档正式曲线；65K/endpoint 已达已测峰值 97.8%，P99 从 65K 的 36.8 s 上升到 98K 的 40.0 s。
- 支持：行数不是可靠 work proxy；配置上限不是运行状态；控制器应把实际 work 压力保持在安全区。
- 不支持：MFU 35% 是硬件极限；65K 对其他机器/模型仍最优；动态控制已胜静态。
- 渲染 QA：三 panel 坐标/单位完整，14.3×、状态差异、safe band 与尾延迟标签均不遮挡数据；无单点 GPU utilization 冒充 time-series 均值。

## 2. 研究边界与共同使能部件

文件：`architecture/opening_ai_data_execution_boundary.{png,svg}`。

- 类型：solution overview，不含性能数字。
- 核心边界：Database AI operator → AI Data Execution Layer → vLLM/typed GPU actor → Database/vector sink；不修改模型、kernel 或 serving scheduler。
- 中间层只有两项研究内容：work-unit construction/organization；state-aware admission/routing/multi-job。Cost estimator 位于二者下方，向两者提供 stage/service/remaining work、SLO slack 与 uncertainty。
- 不支持：Daft/Ray/vLLM 是贡献；cost 是第三项研究内容；图中方法已经全部实现或胜出。
- 渲染 QA：两项研究卡片同层等大；cost 共用卡片有两条明确输入箭头；数据流与 sink 箭头不穿过文字。

## 3. WorkDescriptor 到调度决策

文件：`architecture/opening_work_to_schedule_overview.{png,svg}`。

- 类型：solution overview，不含性能数字。
- 核心链路：数据库行 → work organization → state-aware admission/routing/credit/fair queue → 模型或 typed GPU executor → sink。共同代价估计器向两项研究内容提供 stage/service/remaining work、SLO slack、uncertainty 与 residual correction，并落入 staged WorkDescriptor；fresh runtime state 回馈调度。
- WorkDescriptor 至少包括 source/prepare/model/result、locality、deadline/SLO、uncertainty 和 calibration signature。
- 不支持：图中候选控制规则已经集成到正式 runner 或已经通过性能门。
- 渲染 QA：所有连接均有明确起止组件；反馈线不穿过文字；卡片与标签不越界。

## 4. 数据组织的 regime dependency

文件：`data/report_main/opening_work_organization_regime_v2.{png,svg}`。

- 来源：prefix-cache ON 的 2-endpoint 大 KV 对照与 4-endpoint 小 KV 饱和正式重复中位数。
- 支持：低压时多种组织方法接近；KV 饱和时 locality 成为稀缺资源，重排序方法 cache hit 降到 0.06–0.07，并伴随吞吐下降。
- 不支持：sequential 或 fixed rows 跨 workload 全局最优；组织策略必然带来收益。
- 设计改动：移除难解释的散点，把 throughput 与 cache-hit 数值直接并列，减少答辩解释成本。
- 渲染 QA：所有 bar 有值；cache hit 只在相关 regime 显示；风险标注位于数据区之外，不覆盖 top bar。

## 5. 图像 staged work 证据

文件：`data/report_main/opening_image_stage_aware_evidence.{png,svg}`。

- panel a 来源：`image_clip_preprocess_variants_20260801/raw_repeats.csv` exact tensor path；batch 16/64/256 的 CPU prepare/GPU actor 比为 13.8×/31.2×/29.5×。
- panel b 来源：`image_ai_embed_operator_formal_20260803/summary.csv`；Ray Data official native API graph 与 project frozen-static 在 matched CPU/GPU 下的 operator JCT。
- 支持：图像 workload 不能只用 frame 数描述，CPU prepare 是独立 stage work；显式阶段结构存在约 13–15% preliminary signal。
- 不支持：图像 proposed 已完成；动态、Daft built-in 或 system-E2E 已胜；旧 45.7% 口径有效。
- 渲染 QA：误差棒、单位、lower-is-better 和 baseline provenance 可见；标题明确使用 preliminary。

## 6. 代价估计的决策价值

文件：`data/report_main/opening_cost_model_decision_quality_v2.{png,svg}`。

- 来源：20-context leave-one-context-out 结果 `ce_context_loo_rerun_20260807.json`。
- 编码：竖线=median，菱形=macro mean，圆点=max；横线连接 typical 到 worst case。
- 支持：Hybrid 同时低于 median/macro 5% 与 max 15% 门，max=14.72%，属于 marginal pass。
- 不支持：模型成熟、跨 workload 泛化、worst-case 风险已解决。
- 渲染 QA：两条门线标签位于图内空白，不覆盖标题；图例与 Hybrid 数值不重叠。

六张现有图已在中文重绘后逐张打开复核：无缺字方框、无裁切、无标题/图例/数值重叠；所有 marker 的含义在图内图例或文字中直接解释。字体链以 PingFang SC 为首选，英文技术词回退 Arial/DejaVu Sans。

## 7. Replacement 文本三臂（待数据门禁通过后生成）

计划文件：`data/report_main/opening_database_e2e_replacement.{png,svg}`。

- 输入只能来自 `experiments/results/opening_database_e2e_text_refeed_20260808/` 的审计通过汇总；旧 `opening_database_e2e_text_20260807` 数据只作 failed-feeding 诊断，不得回填。
- 两个 workload 分面分别比较 bounded direct static-sharded、DuckDB AI static-sharded 与 project frozen-static。主轴使用 correct rows/s；P99、time-series GPU util、MFU 与 energy 以直接标注或紧凑副轴呈现，不把 service tokens/s 冒充 database correct throughput。
- feeding、correctness、failure、manifest/PG identity 与三次 formal 稳定性任一未过，绘图命令必须 fail-closed，图文件不得生成。
- 支持：开题统一 source/sink/质量/资源合同下的强静态基线表现。
- 不支持：project dynamic 已胜；DuckDB AI 的 product scheduler 可以由项目调参；某个 workload 的 K 可跨 workload 或机器复用。
- 渲染 QA：待生成后补充；必须核对每个 arm 的均值、离散、P99、MFU、energy、quality 和 failure 文字与汇总 JSON 一致。

## 报告与答辩内容大纲使用顺序

1. 先用动机三联图导出“表示—感知—控制”三项挑战。
2. 用 overview 说明数据组织如何把数据库行变成可调度的 work。
3. 用组织、图像、cost 三图展示已有先验证据与清晰边界。
4. 最后才给出同资源、同最大 K/W 的 frozen-static vs dynamic 后续实验计划。

该顺序避免把 preliminary signal 写成最终方法胜出，也避免让答辩内容变成实验目录罗列。
