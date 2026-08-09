# 2026-08-08 开题叙事图：证据与视觉审计

## 共同边界

- 目标：让动机中的现象、挑战和后续设计逐项对应，而不是用最终算法结果倒推动机。
- 输出：`generate_opening_story_figures_20260808.py` 同时生成 PNG 与 SVG；PNG 用于报告与答辩内容大纲，SVG 保留可编辑文字。当前不制作 PPT 成品。
- 统计：只读取项目内正式 CSV/JSON 或冻结画像数据，不手填结果，不混入 warm-up。
- 证据层级：动机图证明问题与研究必要性；组织、图像和代价图证明已有机制/可行性信号；均不证明待研究的状态感知动态策略已经优于同上限静态基线。
- 视觉规则：一张图一个主句；无未解释散点；标签不压数据、标题或图例；颜色之外同时保留位置、形状或文字标签。2026-08-08 已统一为中文主标题/轴/直接标注，保留 WorkDescriptor、MFU、token 等必要技术词。

## 2026-08-10 第一性原理图集状态

选图理由与正文/备份/不画边界见 `opening_required_data_figures_20260810.md`。本轮已统一
重建 A/T/N/C/H/D/E 七张正文数据图与 F 备份图，**未生成 G，未修改 PPT**。

| 编号 | 内容 | 当前状态 | 备注 |
|---|---|---|---|
| A | work 与运行状态动机 | `rendered-qa-pass` | active work 已明确标为运行内峰值；已移除未经定义的“安全区/过载区”色带 |
| T | 文本 baseline 分轨 | `rendered-qa-pass` | SQuAD 产品轨与 ShareGPT Chat graph 轨分开；DuckDB、Daft Native/Ray、Ray Data 均被呈现但不跨轨排名 |
| N | 原生四 Job 归一化干扰 | `rendered-qa-pass` | 三条原生轨分别画 four-job/isolated-single；Short 与 3 个 Long 的 3 次 formal 点均可见 |
| B | 研究边界与共同使能 | `ready-existing` | 已有 solution overview |
| C | 数据组织的 regime dependency | `rendered-qa-pass` | 已改为“2-endpoint 低 KV 压力差异约 12% / 4-endpoint 高 KV 压力下局部性主导”，不再把运行状态写成池大小 |
| D | 图像 staged work | `rendered-qa-pass` | 已统一图注与统计边界；仍不声称动态胜出 |
| E | 代价估计的决策质量 | `rendered-qa-pass` | 已统一图注；结论仍为 marginal pass |
| F | 原生文本单 Job 状态指纹 | `rendered-qa-pass` | 12 formal；6 个原单位 small multiples，均值 ± SD |
| G | 同上限 static–dynamic phase change | `do-not-draw-no-result` | 保留实验合同即可；开题不生成结果图或带虚构数值的示意图 |
| H | 四 Job quota、竞争与共享权衡 | `rendered-qa-pass` | 1 short + 3 long；full/quarter/static/shared、总效率、MFU、公平与 long spread 同图 |
| Appendix | database-E2E correctness/语义表 | `appendix-table-only` | 不生成正文性能排名图 |

### 数据冻结回读（2026-08-09）

以下 SHA256 前 16 位和行数来自本地权威文件回读；它们用于防止下一次绘图时静默换源。
所有 headline 都由相应文件重新计算通过，本轮没有运行绘图入口。

| 图 | 权威输入 | 回读规模 | SHA256 前 16 位 | 已复算的关键值 |
|---|---|---:|---|---|
| A | `local_vllm_qwen15b_baseline/sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv` | 28 行；21 formal；fixed16 为 3 formal | `94d8f72ec1f5fef6` | 474 / 6,793 token，14.3× |
| A | `dual_gpu_slo_ewma_flush_formal_20260729/formal_summary.csv` | 6 个汇总 cell | `92d938121500f4f4` | fixed-50 high/near 的峰值 active-work 比为 100%/29.25%，MFU 35.01%/7.07% |
| A | `dual_gpu_active_work_saturation_20260729/formal_summary.csv` | 8 个 active-work 点，各 n=3 | `27ac604737237900` | 65K 为已测峰值 97.797%；98K P99=40.048 s |
| T | `opening_database_e2e_text_refeed_20260808/summary/formal_summary.csv` | 6 cell × 3 formal | `6e9d731dff3c5bde` | SQuAD correct rows/s=136.63/136.68/137.77；DuckDB ShareGPT cap failure=4,921 |
| T | `opening_text_native_single_job_formal_20260808/formal_summary.csv` | 4 arm × 3 formal | `bd0fd0fa502f50a6` | bounded/Daft Native/Daft Ray/Ray Data=17.8/17.3/16.7/3.6k tok/s |
| C | `rc1_data_organization/dataorg_2ep_1.5b_cacheON_20260731/raw/runs.csv` | 20 行；15 formal；每策略 3 formal | `89f6b7d44e10ea61` | 50.3–56.3k tok/s，KV max 7%–10% |
| C | `rc1_data_organization/dataorg_4ep_1.5b_cacheON_20260731/raw/runs.csv` | 20 行；15 formal；每策略 3 formal | `6115c11df2375475` | 39.4–50.0k tok/s；重排序命中 0.06–0.07 |
| D | `image_clip_preprocess_variants_20260801/raw_repeats.csv` | 720 formal；目标 batch 每个 30 repeat | `8851447336c86e9e` | prepare/actor=13.84×/31.24×/29.48× |
| D | `image_ai_embed_operator_formal_20260803/raw/runs_3arm_12k_consistency_20260804.csv` | 3 arm × (1 warmup + 3 formal) | `15ac62548887093c` | Daft built-in/Ray Data/Project JCT=65.2/17.8/15.9s；fast arms 未达稳态 |
| D | `image_ai_embed_operator_formal_20260803/raw/runs_matched_resource_schemav12_20260804.csv` | 4 cell × (1 warmup + 3 formal) | `df5ca0c872eca585` | 120K CPU8/16 下 Project 对 Ray Data JCT 低约10%/17%；仅此 panel 可排名 |
| E | `operator_cost_profile_dual4090_formal_v2_cache_on_20260807/ce_context_loo_rerun_20260807.json` | 20 context，6 estimator | `bbb2f2f8c5c1c07f` | CE5 macro/max regret=2.897%/14.715% |
| F | `opening_text_native_single_job_formal_20260808/formal_summary.csv` | 4 arm × 3 formal | `bd0fd0fa502f50a6` | bounded/Daft Native/Daft Ray/Ray Data=17,800/17,286/16,747/3,551 tok/s |
| F | `opening_text_native_single_job_formal_20260808/formal_runs.csv` | 12 formal | `1384ab9dc4abf003` | JCT/running/waiting/KV/MFU 的逐次输入；图中误差线=SD |
| H | `opening_fourjob_interference_20260809/data/combined/job_formal_runs.csv` | 120 条逐 Job formal | `d7fa2417361fcd7b` | Project short full/quarter/static/shared=13.07/36.65/58.79/16.33 s |
| N | `opening_fourjob_interference_20260809/data/combined/job_formal_runs.csv` | 120 条逐 Job formal | `d7fa2417361fcd7b` | Daft Native/Ray/Ray Data 的 short slowdown=1.67×/1.25×/1.68×；三个 long 也均退化 |
| H | `opening_fourjob_interference_20260809/data/combined/group_formal_runs.csv` | 75 条组级 formal | `b415c8a68e5d1139` | Project shared vs static：tok/s +8.68%，group JCT −7.97%，MFU +8.56pp |
| H | `opening_fourjob_interference_20260809/data/combined/isolated_normalized_fairness.csv` | 6 个系统内对照 | `0d325fc6303d5a42` | matched-static/shared Jain=0.998/0.876 |
| H | `opening_fourjob_interference_20260809/data/combined/long_job_spread.csv` | 15 个 formal group | `4252937ec7f81d57` | Project long JCT spread：static 1.1 s，shared 61.1 s |
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
| Experimental Results | T、N、C、H、D、E、F | T/D 为分轨 baseline，N 为轨内归一化点区间，C/F 为 aligned small multiples，H 为因果分解，E 为 decision-regret interval；禁止 radar、双 y 轴和系统间绝对抗干扰排名 |
| 排除项 | G | 无结果，不占用 Experimental Results 图位 |

通用规则审计：A/T/N/C/H/D/E/F 同时有 SVG/PNG；本地 QA 另生成 PDF 与灰度预览但不纳入 Git。
无 3D、阴影、渐变或双 y 轴；坐标带单位；正式重复以均值 ± SD 编码；颜色之外同时使用
位置、marker 形状和文字。SciPilot `check_figure.py --strict` 对八张 300-DPI PNG 全部
PASS；PDF 均为单页矢量且字体嵌入、无 Type 3。八张彩色与灰度预览均逐张人工检查，
无缺字、裁切、标题/图例/数据重叠。最终为 **0 CRITICAL、0 MAJOR、0 MINOR**。

## 1. Work、状态与提交压力动机

文件：`data/report_main/opening_motivation_work_state.{png,svg}`。

- panel a 来源：固定 16 行批次的 work 最小/最大中位数 474/6,793 token，差 14.3×。
- panel b 来源：相同配置 W65K 下，`max_active_work_seen_mean/W` 在 high/arrival-limited
  为 100%/29.25%，MFU 为 35.01%/7.07%。该字段是运行内峰值，不是时间平均 active work。
- panel c 来源：active-work 八档正式曲线；65K/endpoint 已达已测峰值 97.8%，P99 从 65K 的 36.8 s 上升到 98K 的 40.0 s。
- 支持：行数不是可靠 work proxy；配置上限不是运行状态；提交控制需要先标定最小
  近饱和点，并在增加 work 时同时观察吞吐边际收益与 tail cost。
- 不支持：MFU 35% 是硬件极限；65K 对其他机器/模型仍最优；动态控制已胜静态。
- 2026-08-10 渲染修订已完成：panel b 横轴写“运行内峰值 active work / W65K”；
  panel c 使用“低供给段—最小近饱和点—边际收益递减”，没有人为色带。

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

- 来源：相同双卡硬件、prefix-cache ON 的 2-endpoint 低 KV 压力与 4-endpoint
  consolidation 高 KV 压力正式重复中位数；变化的是 serving 拓扑与运行压力，不是硬件。
- 支持：2-endpoint KV max 仅 7%–10% 时五种策略仍有约 12% 范围，但 locality 破坏未
  放大；4-endpoint KV max 98%–100% 时分化约 27%，长度/装箱重排 cache hit 降到
  0.06–0.07，并伴随吞吐和 tail 退化。
- 不支持：sequential 或 fixed rows 跨 workload 全局最优；组织策略必然带来收益。
- 设计改动：移除难解释的散点，把 throughput 与 cache-hit 数值直接并列，减少答辩解释成本。
- 证据边界：2-endpoint 相对 bounded ceiling 仅达 63%–71%，严格 feeding-saturation
  门未过；该图只能说明当前 W65K 准入合同下的 regime/locality 机制，不能作容量排名。
- 2026-08-10 渲染修订已完成：标题为“低 KV 压力（2 endpoint，KV max 7%–10%）”
  与“高 KV 压力（4 endpoint，KV max 98%–100%）”，已删除“近似中性”、池大小误导和
  无数据定义的阈值底色。

## 5. 图像 staged work 与 baseline 边界

文件：`data/report_main/opening_image_stage_aware_evidence.{png,svg}`。

- panel a 来源：`image_clip_preprocess_variants_20260801/raw_repeats.csv` exact tensor path；batch 16/64/256 的 CPU prepare/GPU actor 比为 13.8×/31.2×/29.5×。
- panel b 是 12K 同语义 Daft built-in、Ray Data、Project 三臂；三者 exactly-once，但
  fast arms setup-dominated，且 Daft 20K 已触发 OutOfDisk，因此只作结构/扩展边界诊断。
- panel c 是 120K matched-resource 的 Ray Data official native API graph 与 project
  frozen-static；两档 CPU 都是 3 次 formal，只有此 panel 可作性能排名。
- 支持：图像 workload 不能只用 frame 数描述，CPU prepare 是独立 stage work；materialize
  与 streaming 结构决定可扩展性；matched-resource 下静态结构存在 preliminary signal。
- 不支持：12K 三臂稳态排名、图像动态已胜、Daft 的 12K JCT 可外推到 120K。
- 渲染 QA：误差棒、单位、lower-is-better、规模和可排名边界均直接可见。

## 6. 代价估计的决策价值

文件：`data/report_main/opening_cost_model_decision_quality_v2.{png,svg}`。

- 来源：20-context leave-one-context-out 结果 `ce_context_loo_rerun_20260807.json`。
- 编码：竖线=median，菱形=macro mean，圆点=max；横线连接 typical 到 worst case。
- 支持：Hybrid 同时低于 median/macro 5% 与 max 15% 门，max=14.72%，属于 marginal pass。
- 不支持：模型成熟、跨 workload 泛化、worst-case 风险已解决。
- 渲染 QA：两条门线标签位于图内空白，不覆盖标题；图例与 Hybrid 数值不重叠。

当前叙事图中，本轮统一重建 A/T/N/C/H/D/E/F；B 与 WorkDescriptor 总览保持现有架构图。
A/T/N/C/H/D/E/F 已逐张打开复核，均无缺字方框、裁切或文字重叠；灰度下仍可由
位置、marker 与标签区分。字体链以 PingFang SC 为首选，英文技术词回退 Arial/DejaVu Sans。

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

## 8. T：文本 baseline 分轨证据

文件：`data/report_main/opening_text_baseline_evidence_map.{png,svg}`。

- panel a 使用统一 PostgreSQL source/sink 的 SQuAD database-E2E 产品轨，比较 Direct
  static、DuckDB AI 与 Project frozen-static 的 correct rows/s；三臂在该合同下可排名。
- panel b 使用同一 ShareGPT Chat manifest，比较 bounded HTTP、Daft Native、Daft Ray、
  Ray Data 的 service tokens/s；Daft/Ray Data 保持 vendor scheduler ownership。
- DuckDB 的 ShareGPT 4,921/6,144 cap 语义失败只作产品边界注释，不进入 Chat 图的性能排名。
- 支持：现有路径在正确吞吐、语义和服务供给状态上各有边界，研究不能只看 raw rows/s；
  这导出 neutral WorkDescriptor、correctness-aware evidence 和状态感知提交。
- 不支持：两个 panel 跨轨排名、Project 普遍胜出或 DuckDB/Daft/Ray 的内部算法归因。

## 9. F：原生文本单 Job 状态指纹

文件：`data/report_main/opening_native_single_job_state_fingerprint.{png,svg}`。

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

## 10. N：原生四 Job 归一化干扰

文件：`data/report_main/opening_native_fourjob_normalized_impact.{png,svg}`。

- 数据：`opening_fourjob_interference_20260809/data/combined/job_formal_runs.csv`；
  `Short@0s → 3×Long@5s`，每条轨均有四个 isolated-single 与 concurrent-four-job。
- 画法：Daft Native、Daft Ray、Ray Data 分面；横轴只用 `four-job JCT / 本 Job 的
  isolated-single JCT 均值`，白点展示三次 formal，菱形与误差线为均值 ± SD。
- 支持：Short 与三个 Long 在三条原生轨中都受到共享服务竞争影响；多 Job 管理不能只
  观察前台 Short，也要观察全部 Long 的进度和离散。
- 不支持：跨框架绝对 JCT 排名、框架内部调度算法归因、项目方法胜出。

## 11. H：Project 四 Job 配额、竞争与共享权衡

文件：`data/report_main/opening_multijob_interference_tradeoff.{png,svg}`。

- 数据：同一四 Job 结果的 `job_formal_runs.csv`、`group_formal_runs.csv`、
  `isolated_normalized_fairness.csv` 与 `long_job_spread.csv`。
- panel a 分离 full→quarter 配额损失、quarter→static 真实竞争与 static→shared 调度效果；
  panel b 报组吞吐/JCT/MFU；panel c 同时报 isolated-normalized progress、Jain 与 long spread。
- 支持：shared credit 提高 work conservation，并显著帮助 Short，但当前 equal-weight 点的
  Jain 与 Long 间离散仍需 fairness/SLO guard；因此动态调度目标必须同时包含效率和隔离。
- 不支持：shared/dynamic 普遍胜出、weighted/SLO 已验证、图像多 Job 已完成。

## 12. G：同上限 static–dynamic phase change

当前只有论文实验设计，**无开题结果数据**。开题阶段不画 G；只在实验计划中保留
low→high/high→low 或 easy→heavy 的合同。等同最大 K、active-work、buffer bytes、
CPU/GPU 和 actor 数的 frozen-static、observe-only 与最小动作候选真正完成后，再决定
是否生成结果图。禁止用示意曲线、虚构吞吐、延迟、MFU 或改善百分比占据结果图位置。

## 报告与答辩内容大纲使用顺序

1. 先用 A 动机三联图导出“表示—感知—控制”三项挑战。
2. 用 B 说明数据组织如何把数据库行变成可调度的 work，以及代价估计如何共同使能组织与调度。
3. 用 N 证明多 Job 竞争是原生路径中的共同外部现象，再用 H 解释 Project 的配额、竞争、
   work conservation 与公平权衡。
4. 用 C、D、E 展示组织、跨模态阶段与 cost 的已有证据；F 作为状态解释备份。
5. G 不画，database-E2E 三臂只进附录 correctness/语义表。

该顺序避免把 preliminary signal 写成最终方法胜出，也避免让答辩内容变成实验目录罗列。
