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
| A | work 与运行状态动机 | `data-ready-label-fix-pending` | 数据完整；下一次渲染必须把 active work 标为运行内峰值，并移除未经定义的“安全区/过载区”色带 |
| B | 研究边界与共同使能 | `ready-existing` | 已有 solution overview |
| C | 数据组织的 regime dependency | `data-ready-label-fix-pending` | 数据完整；下一次渲染改为“大 KV 池差异约 12% / 小 KV 池饱和分化”，不写“近似中性” |
| D | 图像 staged work | `ready-existing` | 已有正式图 |
| E | 代价估计的决策质量 | `ready-existing` | 已有正式图，结论为 marginal pass |
| F | 原生文本单 Job 状态指纹 | `data-ready-not-generated` | 12 formal 已通过门禁 |
| G | 同上限 static–dynamic phase change | `do-not-draw-no-result` | 保留实验合同即可；开题不生成结果图或带虚构数值的示意图 |
| H | 两 Job 前台干扰、idle borrowing 与 arrival regime | `data-ready-not-generated` | online/eager 5 s guaranteed-overlap 与 matched full/half control 已通过门禁 |
| Appendix | database-E2E correctness/语义表 | `appendix-table-only` | 不生成正文性能排名图 |

### 数据冻结回读（2026-08-09）

以下 SHA256 前 16 位和行数来自本地权威文件回读；它们用于防止下一次绘图时静默换源。
所有 headline 都由相应文件重新计算通过，本轮没有运行绘图入口。

| 图 | 权威输入 | 回读规模 | SHA256 前 16 位 | 已复算的关键值 |
|---|---|---:|---|---|
| A | `local_vllm_qwen15b_baseline/sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv` | 28 行；21 formal；fixed16 为 3 formal | `94d8f72ec1f5fef6` | 474 / 6,793 token，14.3× |
| A | `dual_gpu_slo_ewma_flush_formal_20260729/formal_summary.csv` | 6 个汇总 cell | `92d938121500f4f4` | fixed-50 high/near 的峰值 active-work 比为 100%/29.25%，MFU 35.01%/7.07% |
| A | `dual_gpu_active_work_saturation_20260729/formal_summary.csv` | 8 个 active-work 点，各 n=3 | `27ac604737237900` | 65K 为已测峰值 97.797%；98K P99=40.048 s |
| C | `rc1_data_organization/dataorg_2ep_1.5b_cacheON_20260731/raw/runs.csv` | 20 行；15 formal；每策略 3 formal | `89f6b7d44e10ea61` | 50.3–56.3k tok/s，KV max 7%–10% |
| C | `rc1_data_organization/dataorg_4ep_1.5b_cacheON_20260731/raw/runs.csv` | 20 行；15 formal；每策略 3 formal | `6115c11df2375475` | 39.4–50.0k tok/s；重排序命中 0.06–0.07 |
| D | `image_clip_preprocess_variants_20260801/raw_repeats.csv` | 720 formal；目标 batch 每个 30 repeat | `8851447336c86e9e` | prepare/actor=13.84×/31.24×/29.48× |
| D | `image_ai_embed_operator_formal_20260803/summary.csv` | 4 cell，各 n=3 formal | `63ccdefdef015d5f` | CPU8/16 的 JCT 改善 12.82%/15.12% |
| E | `operator_cost_profile_dual4090_formal_v2_cache_on_20260807/ce_context_loo_rerun_20260807.json` | 20 context，6 estimator | `bbb2f2f8c5c1c07f` | CE5 macro/max regret=2.897%/14.715% |
| F | `opening_text_native_single_job_formal_20260808/formal_summary.csv` | 4 arm × 3 formal | `bd0fd0fa502f50a6` | bounded/Daft Native/Daft Ray/Ray Data=17,800/17,286/16,747/3,551 tok/s |
| H | `opening_multijob_interference_20260809/data/combined/summary.csv` | 10 汇总行，源自 30 formal | `3622732cf88b4fee` | 所有 two-job overlap>0；原生 short JCT +82.42%/+104.84%/+32.76% |
| H | `opening_multijob_interference_20260809/data/combined/comparisons.csv` | 6 个预注册对比 | `33701106d0f8bda8` | project shared vs static：吞吐 +21.03%，short JCT +4.98% |
| H | `opening_multijob_interference_20260809/data/eager_project/scenario_summary.csv` | 4场景×3 formal | `16c4bbcb637c0263` | quota-only short JCT +59.00%；matched static/shared competition +58.77%/+28.90% |
| H | `opening_multijob_interference_20260809/data/eager_project/phase_state_summary.csv` | 2策略×3阶段 | `0d9b3ef1478de403` | pre-long running总和 static/shared=120.6/230.1 |
| H | `opening_multijob_interference_20260809/data/eager_project/cross_system_short_impact.csv` | 5个系统内normalized对比 | `5df6a348efe95a93` | 只画within-track影响；禁止跨轨绝对JCT排名 |
| Appendix | `opening_database_e2e_text_refeed_20260808/summary/formal_summary.csv` | 6 cell × 3 formal | `6e9d731dff3c5bde` | SQuAD correct rows/s=136.63/136.68/137.77；ShareGPT DuckDB cap failure=4,921 |

完整数据门禁另由各结果目录的 `audit.json` 和 README 承担；本表不是替代审计器。
E 的 SHA 于 2026-08-09 随 6 处 `§6` 字符 UTF-8 规范化更新；JSON 字段、数值与图 E
语义均未改变，旧 SHA `1600360e920c405d` 仅对应不可被标准 UTF-8 Python 解析的字节版本。

### Figure Designer 完整性审计

| 类型 | 图 | 范式与判定 |
|---|---|---|
| Motivated Example | A | “真实运行现象 + 现有表达失败”三联图；不画 proposed 胜出 teaser，符合当前尚无动态胜出结果的证据状态 |
| Solution Overview | B、WorkDescriptor 总览 | system boundary + multi-layer feedback；输入、两项研究内容、共同 cost enabler、执行后端和 sink 均有真实名称 |
| Experimental Results | C、D、E、未来 F/H | C/D 为 aligned small multiples，E 为 decision-regret interval，F/H 继续使用原单位 small multiples；禁止 radar、双 y 轴和系统间绝对抗干扰排名 |
| 排除项 | G | 无结果，不占用 Experimental Results 图位 |

通用规则审计：现有输出同时有 SVG/PNG、无 3D/阴影/渐变、坐标有单位、正式重复有
误差编码、颜色外同时使用位置/形状/文字。当前为 **0 CRITICAL、2 MAJOR、0 MINOR**：
两项 MAJOR 正是 A 的峰值/区间标签和 C 的“近似中性”标签，均已在生成脚本中预置修订，
但按用户要求尚未重新渲染。F/H 首次生成后仍需检查最终字号、黑白可辨性和自包含图注。

## 1. Work、状态与提交压力动机

文件：`data/report_main/opening_motivation_work_state.{png,svg}`。

- panel a 来源：固定 16 行批次的 work 最小/最大中位数 474/6,793 token，差 14.3×。
- panel b 来源：相同配置 W65K 下，`max_active_work_seen_mean/W` 在 high/arrival-limited
  为 100%/29.25%，MFU 为 35.01%/7.07%。该字段是运行内峰值，不是时间平均 active work。
- panel c 来源：active-work 八档正式曲线；65K/endpoint 已达已测峰值 97.8%，P99 从 65K 的 36.8 s 上升到 98K 的 40.0 s。
- 支持：行数不是可靠 work proxy；配置上限不是运行状态；提交控制需要先标定最小
  近饱和点，并在增加 work 时同时观察吞吐边际收益与 tail cost。
- 不支持：MFU 35% 是硬件极限；65K 对其他机器/模型仍最优；动态控制已胜静态。
- 下一次渲染修订：panel b 横轴写“运行内峰值 active work / W65K”；panel c 删除人为
  划分的“安全工作区/过载尾部”色带，改为“低供给段—最小近饱和点—边际收益递减”。
  现有图布局可读，但在完成这两项语义修订前不视为最终冻结图。

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

- 来源：prefix-cache ON 的 2-endpoint 大 KV 池与 4-endpoint 小 KV 池正式重复中位数。
- 支持：2-endpoint KV max 仅 7%–10% 时五种策略仍有约 12% 范围，但 locality 破坏未
  放大；4-endpoint KV max 98%–100% 时分化约 27%，长度/装箱重排 cache hit 降到
  0.06–0.07，并伴随吞吐和 tail 退化。
- 不支持：sequential 或 fixed rows 跨 workload 全局最优；组织策略必然带来收益。
- 设计改动：移除难解释的散点，把 throughput 与 cache-hit 数值直接并列，减少答辩解释成本。
- 证据边界：2-endpoint 相对 bounded ceiling 仅达 63%–71%，严格 feeding-saturation
  门未过；该图只能说明当前 W65K 准入合同下的 regime/locality 机制，不能作容量排名。
- 下一次渲染修订：标题改为“大 KV 池（KV max 7%–10%）：策略差异约 12%”与
  “小 KV 池（KV max 98%–100%）：局部性主导”，删除“近似中性”和无数据定义的红色阈值底色。

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

六张现有图已逐张打开复核：均无缺字方框、裁切或文字重叠。B、WorkDescriptor 总览、
D、E 可保持；A、C 的视觉布局可读但存在上述语义标签修订，因此状态不是最终冻结。
字体链以 PingFang SC 为首选，英文技术词回退 Arial/DejaVu Sans。

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
  在线输入使用`data/combined/comparisons.csv`；eager主输入使用
  `data/eager_project/{scenario_summary,comparisons,phase_state_summary,cross_system_short_impact}.csv`。
  online有30 formal；Project eager有12 formal，所有two-job arm实际overlap大于0。
- 到达方向固定为 `Short@0s → Long@5s`。panel a 是“后到 long 是否伤害已运行 short”：
  只画各系统内 `single short → short+long`
  的 short JCT 变化和误差，并直接标注实际 overlap。项目 full/half-pool
  匹配控制标为`causal`，Daft Native/Ray/Ray Data标为`observational:overlap_present`；
  panel a只用`cross_system_short_impact.csv`的within-track normalized delta，禁止画
  71.24s vs11.06s或其他跨轨绝对JCT柱图。
- panel b按online/eager分面，用四个对齐small multiples比较project static/shared的
  aggregate tok/s、long JCT、short JCT和Jain；禁止把两个regime均值混合，也禁止双y轴/雷达图。
- 备份时间线可分 pre-long/overlap/drain 三段展示 running、waiting、KV、GPU util
  与 completed-work rate。当前无 interval FLOPs counter，MFU 只能报 group aggregate，
  不得画成 interval MFU。
- 原 15 s Daft Native 无 overlap 数据不进入干扰结论；它只说明该到达间隔下
  short 先完成。开题结论以统一 5 s guaranteed-overlap 数据为准。
- 支持：原生三路short JCT各自+82.42%/+104.84%/+32.76%；Project online下shared提高
  aggregate但伤short/Jain，eager下quota-only +59.00%、shared相对static short JCT−48.94%、
  aggregate+31.85%、Jain 0.894→0.972。它证明arrival-regime dependence与idle borrowing，
  不证明shared/dynamic全面胜出。
- 不支持：原生 request P99、系统间绝对性能排名、4+ Job、weighted/SLO、
  图像多 Job 或最终 state-aware controller 效果。

## 10. G：同上限 static–dynamic phase change

当前只有论文实验设计，**无开题结果数据**。开题阶段不画 G；只在实验计划中保留
low→high/high→low 或 easy→heavy 的合同。等同最大 K、active-work、buffer bytes、
CPU/GPU 和 actor 数的 frozen-static、observe-only 与最小动作候选真正完成后，再决定
是否生成结果图。禁止用示意曲线、虚构吞吐、延迟、MFU 或改善百分比占据结果图位置。

## 报告与答辩内容大纲使用顺序

1. 先用 A 动机三联图导出“表示—感知—控制”三项挑战。
2. 用 B 说明数据组织如何把数据库行变成可调度的 work，以及代价估计如何共同使能组织与调度。
3. 用 C–E 展示组织、图像和 cost 的已有证据与边界。
4. 用 F 说明不同原生 graph 的外部状态形态，再用 H 说明后到 Job 干扰和效率—隔离—公平权衡。
5. 下一次获准绘图时只需：A/C 标签修订、生成 F/H；B、WorkDescriptor 总览、D、E
   不重画。G 不画，database-E2E 三臂只进附录 correctness/语义表。

该顺序避免把 preliminary signal 写成最终方法胜出，也避免让答辩内容变成实验目录罗列。
