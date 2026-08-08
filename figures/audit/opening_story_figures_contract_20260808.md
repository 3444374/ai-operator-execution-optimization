# 2026-08-08 开题叙事图：证据与视觉审计

## 共同边界

- 目标：让动机中的现象、挑战和后续设计逐项对应，而不是用最终算法结果倒推动机。
- 输出：`generate_opening_story_figures_20260808.py` 同时生成 PNG 与 SVG；PNG 用于报告与答辩内容大纲，SVG 保留可编辑文字。当前不制作 PPT 成品。
- 统计：只读取项目内正式 CSV/JSON 或冻结画像数据，不手填结果，不混入 warm-up。
- 证据层级：动机图证明问题与研究必要性；组织、图像和代价图证明已有机制/可行性信号；均不证明待研究的状态感知动态策略已经优于同上限静态基线。
- 视觉规则：一张图一个主句；无未解释散点；标签不压数据、标题或图例；颜色之外同时保留位置、形状或文字标签。2026-08-08 已统一为中文主标题/轴/直接标注，保留 WorkDescriptor、MFU、token 等必要技术词。

## 2026-08-09 数据就绪状态

本轮只整理数据与绘图合同，**未运行绘图脚本，未新建或覆盖 PNG/SVG，未修改 PPT**。

| 编号 | 内容 | 当前状态 | 备注 |
|---|---|---|---|
| A | work 与运行状态动机 | `ready-existing` | 已有正式图与可追溯数据 |
| B | 研究边界与共同使能 | `ready-existing` | 已有 solution overview |
| C | 数据组织的 regime dependency | `ready-existing` | 已有正式图 |
| D | 图像 staged work | `ready-existing` | 已有正式图 |
| E | 代价估计的决策质量 | `ready-existing` | 已有正式图，结论为 marginal pass |
| F | 原生文本单 Job 状态指纹 | `data-ready-not-generated` | 12 formal 已通过门禁 |
| G | 同上限 static–dynamic phase change | `plan-only-no-result` | 只能作实验设计示意，不得填结果数值 |
| H | 两 Job 前台干扰与共享权衡 | `data-ready-not-generated` | 5 s guaranteed-overlap 已通过门禁 |
| Appendix | database-E2E correctness/语义表 | `appendix-table-only` | 不生成正文性能排名图 |

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

## 7. Replacement 文本三臂（附录 correctness/语义表）

数据：`experiments/results/opening_database_e2e_text_refeed_20260808/summary/formal_summary.csv`
与 `summary/audit.json`。旧 `opening_database_e2e_text_20260807` 只作 failed-feeding 诊断，
不得回填。

- 门禁已通过：24/24 cells、18 formal，source/sink、identity、exactly-once、manifest
  和稳定性合同一致，0 infrastructure failure。
- SQuAD 可作静态地基：direct/DuckDB/project correct rows/s 为
  136.63/136.68/137.77，在该 workload 下近似中性。
- DuckDB ShareGPT 有 4,921/6,144 行 cap 语义失败，correct rows/s 为 2.26；
  它只用于产品语义边界，不与 Chat 轨混排。
- ShareGPT C32 direct 后续被独立扫描证实只达已测峰值的 52.07%，因此
  project/direct 1.5457× 被并发与执行结构混淆，必须显式标为 `not rankable`。
- 决策：不再生成 `opening_database_e2e_replacement.{png,svg}` 作正文性能图。
  开题只保留一张附录表，展示 correctness、sink、语义失败与可排名性。

## 8. F：原生文本单 Job 状态指纹

计划文件：`data/report_main/opening_native_single_job_state_fingerprint.{png,svg}`。

- 类型：experimental results；数据为
  `experiments/results/opening_text_native_single_job_formal_20260808/formal_summary.csv`，
  4 arms × 3 formal，warm-up 不进统计。
- 可用字段：`wall_s_mean/sd`、`tokens_per_s_mean/sd`、`running_mean`、
  `waiting_mean`、`kv_mean`、`mfu_mean`、`gpu_util_mean_pct`、`ttft_mean_s`、
  `queue_mean_s`。原生 adapter 无统一 request P99，不得补算或伪造。
- 画法：左侧用两个对齐的点图/误差线展示 JCT 与 service tok/s；右侧用
  running、waiting、KV、MFU 四个原单位 small multiples。避免雷达图、双 y 轴和
  隐藏单位的统一归一化。
- 主句：同一 ShareGPT 任务下，bounded control 处于最小饱和参照，
  Daft Native/Ray 出现 high-running/high-waiting/KV-near-full，Ray Data 当前路径
  low-running/no-waiting/low-MFU。
- 不支持：不归因框架内部算法，不称某框架普遍更快，不将单 Job
  短 cell 外推为长时间容量排名。

## 9. H：两 Job 前台干扰与共享权衡

计划文件：`data/report_main/opening_multijob_interference_tradeoff.{png,svg}`。

- 数据：`experiments/results/opening_multijob_interference_20260809/data/combined/summary.csv`、
  `data/combined/comparisons.csv`、`data/project/scenario_summary.csv` 与
  `data/project/pairwise_comparison.csv`。30 formal rows、10 summary rows、6 comparisons，
  所有 two-job arms 实际 overlap 大于 0。
- panel a 是“后到 Job 是否伤害前台”：只画各系统内 `single short → short+long`
  的 short JCT 变化和误差，并直接标注实际 overlap。项目 full/half-pool
  匹配控制标为 `causal`，Daft Native/Ray/Ray Data 标为
  `observational:overlap_present`；不作系统间绝对 JCT 排名。
- panel b 是“效率—隔离—公平权衡”：用四个对齐 small multiples 比较
  project static/shared 的 aggregate tok/s、long JCT、short JCT 和 Jain fairness；
  禁止双 y 轴和雷达图。
- 备份时间线可分 pre-long/overlap/drain 三段展示 running、waiting、KV、GPU util
  与 completed-work rate。当前无 interval FLOPs counter，MFU 只能报 group aggregate，
  不得画成 interval MFU。
- 原 15 s Daft Native 无 overlap 数据不进入干扰结论；它只说明该到达间隔下
  short 先完成。开题结论以统一 5 s guaranteed-overlap 数据为准。
- 支持：原生三路 short JCT 各自上升 82.42%/104.84%/32.76%；project
  shared 相对 static 总吞吐 +21.03%、long JCT −18.31%，但 short JCT +4.98%、
  Jain 0.759→0.707。它证明权衡存在，不证明 shared/dynamic 全面胜出。
- 不支持：原生 request P99、系统间绝对性能排名、4+ Job、weighted/SLO、
  图像多 Job 或最终 state-aware controller 效果。

## 10. G：同上限 static–dynamic phase change

当前只有论文实验设计，**无开题结果数据**。如果在开题中保留，只能画方法/评估
示意：low→high/high→low 或 easy→heavy 的 workload phase，在完全相同的最大
K、active-work、buffer bytes、CPU/GPU 和 actor 数下比较 frozen-static、
observe-only 和最小动作候选。图内不得出现虚构吞吐、延迟、MFU 或改善百分比。

## 报告与答辩内容大纲使用顺序

1. 先用 A 动机三联图导出“表示—感知—控制”三项挑战。
2. 用 B 说明数据组织如何把数据库行变成可调度的 work，以及代价估计如何共同使能组织与调度。
3. 用 C–E 展示组织、图像和 cost 的已有证据与边界。
4. 用 F 说明不同原生 graph 的外部状态形态，再用 H 说明后到 Job 干扰和效率—隔离—公平权衡。
5. G 只作同上限 static–dynamic 评估设计，database-E2E 三臂只进附录 correctness/语义表。

该顺序避免把 preliminary signal 写成最终方法胜出，也避免让答辩内容变成实验目录罗列。
