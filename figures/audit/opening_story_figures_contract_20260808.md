# 2026-08-08 开题叙事图：证据与视觉审计

## 共同边界

- 目标：让动机中的现象、挑战和后续设计逐项对应，而不是用最终算法结果倒推动机。
- 输出：`generate_opening_story_figures_20260808.py` 同时生成 PNG 与 SVG；PNG 用于报告与答辩内容大纲，SVG 保留可编辑文字。当前不制作 PPT 成品。
- 统计：只读取项目内正式 CSV/JSON 或冻结画像数据，不手填结果，不混入 warm-up。
- 证据层级：动机图证明问题与研究必要性；组织、图像和代价图证明已有机制/可行性信号；均不证明待研究的状态感知动态策略已经优于同上限静态基线。
- 视觉规则：一张图一个主句；无未解释散点或连线；标签不压数据、标题或图例；颜色之外同时保留位置、形状或文字标签。2026-08-10 的出版风格复核进一步冻结：红色只表示真实失败、静态退化或预注册门槛；形状表示方法/场景时必须有图例，表示统计量时必须写明统计口径；线段必须明确是 SD、范围、门槛还是同一对象的成对变化。

## 2026-08-10 第一性原理图集状态

选图理由与正文/备份/不画边界见 `opening_required_data_figures_20260810.md`。本轮已统一
重建 A/T/N/C/H/D/I/J/E 九张正文数据图、新增单 Job 任务—请求主图并保留 F 状态备份图，
**未生成 G，未修改 PPT**。

| 编号 | 内容 | 当前状态 | 备注 |
|---|---|---|---|
| A | work 与运行状态动机 | `rendered-qa-pass` | active work 已明确标为运行内峰值；已移除未经定义的“安全区/过载区”色带 |
| T | 文本 baseline 分轨 | `rendered-qa-pass` | SQuAD 产品轨与 ShareGPT Chat graph 轨分开；DuckDB、Daft Native/Ray、Ray Data 均被呈现但不跨轨排名 |
| N | 原生四 Job 归一化干扰 | `rendered-qa-pass` | 三条原生轨组成 3×4 slowdown 矩阵；格内直标 four-job/isolated-single 倍率与 JCT 增幅，不再使用误差线 |
| B | 研究边界与共同使能 | `ready-existing` | 已有 solution overview |
| C | 数据组织的 regime dependency | `rendered-qa-pass` | 吞吐与prefix-cache命中率双轨迹；每条线连接同一策略的低→高压力中位数，无误差线，不再把运行状态写成池大小 |
| D | 图像 staged-work 动机 | `rendered-qa-pass` | prepare/model、R0/R1/R2 传输形态与 active-window 分开呈现；仍不声称动态胜出 |
| I | 图像 baseline 数据对照 | `rendered-qa-pass` | 图内只保留 12K 诊断与 120K matched-resource 数据；路径能力/角色改用报告表格；仅 120K Ray Data/Project 可排名 |
| J | 图像四 Job 归一化干扰 | `rendered-qa-pass` | 4×4 路径/策略×Job slowdown 矩阵；与文本四 Job 图统一；Project shared 状态仅观测 |
| E | 代价估计的决策质量 | `rendered-qa-pass` | ranking单列；20-context regret完整点云展示真实分布、均值与最坏值；Ridge MAE反例直标；结论仍为marginal pass |
| F-main | 原生文本单 Job 任务—请求对照 | `rendered-qa-pass` | 12 formal；JCT/waiting/queue time/TTFT 四项原单位对齐，均值直标 |
| F-state | JCT 的服务与资源状态补充 | `rendered-qa-pass` | tok/s/running/waiting/KV/MFU/GPU utilization 六项原单位对齐 |
| G | 同上限 static–dynamic phase change | `do-not-draw-no-result` | 保留实验合同即可；开题不生成结果图或带虚构数值的示意图 |
| H | 四 Job 干扰幅度与共享权衡 | `rendered-qa-pass` | 每条线固定为同一Job，连接独立→1/4配额→Static→Shared；效率表+Static→Shared进度折线；连线表示受控场景顺序 |
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
| D | `image_clip_transfer_ceiling_20260803/raw.csv` | 3 mode × 3 batch × 30 repeat | `f6d908143c5063d2` | batch64 R0/R1/R2=9.82K/8.72K/1.96K img/s；host ownership-copy 为传输侧主损失 |
| D | `image_host_path_screening_20260802/summary.csv` | active-window 5 点单次 screening | `fce52c936a79cd4b` | active4→32 setup后吞吐 0.50→1.02K img/s；active64 回退且 wait P50=1.44s |
| I | `image_ai_embed_operator_formal_20260803/raw/runs_3arm_12k_consistency_20260804.csv` | 3 arm × (1 warmup + 3 formal) | `15ac62548887093c` | Daft built-in/Ray Data/Project JCT=65.2/17.8/15.9s；fast arms 未达稳态 |
| I | `image_ai_embed_operator_formal_20260803/raw/runs_matched_resource_schemav12_20260804.csv` | 4 cell × (1 warmup + 3 formal) | `df5ca0c872eca585` | 120K CPU8/16 下 Project 对 Ray Data JCT 低约10%/17%；仅此 panel 可排名 |
| I | `vllm_clip_pooling_gate_20260804/summary.csv` | 2 个 600s capability gate | `d5b2480bca5287ca` | 两次均 timeout 且无 embedding；不得生成性能值 |
| J | `opening_image_native_fourjob_formal_20260810/data/slowdown_summary.csv` | 8 个 system×job 汇总 cell，各 n=3 | `14bec9ba1324d575` | Daft Built-in=1.02×/2.13×--3.19×；Ray Data=1.06×--1.64× |
| J | `opening_image_project_fourjob_observe_only_formal_20260810/data/job_summary.csv` | 12 个 scenario×job 汇总 cell，各 n=3 | `c587cf74b5590c1d` | static=1.74×--1.81×；shared-credit=1.12×--1.78×；状态只观测 |
| E | `operator_cost_profile_dual4090_formal_v2_cache_on_20260807/ce_context_loo_rerun_20260807.json` | 20 context，6 estimator | `bbb2f2f8c5c1c07f` | CE5 macro/max regret=2.897%/14.715% |
| F | `opening_text_native_single_job_formal_20260808/formal_summary.csv` | 4 arm × 3 formal | `bd0fd0fa502f50a6` | direct/Daft Native/Daft Ray/Ray Data 的 JCT=95.5/98.4/101.5/478.7s；queue=0.10/37.49/37.64/≈0s |
| F | `opening_text_native_single_job_formal_20260808/formal_runs.csv` | 12 formal | `1384ab9dc4abf003` | waiting/TTFT 与 tok/s/running/KV/MFU/GPU util 的逐次输入；SD 保留在数据中、主图不绘制 |
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
| Experimental Results | T、N、C、H、D、I、J、E、F | T/I 为分轨 baseline，D 为图像阶段/传输/窗口动机，N/J 为文本/图像轨内归一化干扰，C 为 regime 对照，F-main 为任务—请求主图、F-state 为状态补充，H 为因果分解，E 为 decision-regret；禁止 radar、双 y 轴和系统间绝对抗干扰排名 |
| 排除项 | G | 无结果，不占用 Experimental Results 图位 |

通用规则审计：A/T/N/C/H/D/I/J/E/F-main/F-state 同时有 SVG/PNG；本地 QA 另生成 PDF 与灰度预览但不纳入 Git。
无 3D、阴影、渐变或双 y 轴；坐标带单位；正式重复以均值 ± SD 编码；颜色之外同时使用
位置、marker 形状和文字。A/N/F 的统计 marker、H 的场景 marker 与成对连线、E 的
median/macro/max/range 均在图内图例或页脚明确定义。SciPilot `check_figure.py --strict` 对十一张 300-DPI PNG 全部
PASS；PDF 均为单页矢量且字体嵌入、无 Type 3。十一张彩色与灰度预览均逐张人工检查，
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
- 设计改动：移除误差线，吞吐与cache-hit各用一个slope panel；每条线只连接同一策略的
  低→高压力3次formal中位数，右端直接标策略与前后数值。实线圆点表示保序，虚线方点
  表示重排/装箱，使策略随压力变化的趋势可直接读取。吞吐纵轴与直接标签统一使用
  `k token/s`，不再使用中文“千token/s”。
- 证据边界：2-endpoint 相对 bounded ceiling 仅达 63%–71%，严格 feeding-saturation
  门未过；该图只能说明当前 W65K 准入合同下的 regime/locality 机制，不能作容量排名。
- 2026-08-10 渲染修订已完成：总标题、共享策略图例和两个panel标题分为三层；panel标题
  固定为“a 端到端吞吐”和“b Prefix cache命中率”，低/高压力合同只出现在各自x轴刻度。
  图例不再重复“颜色对应组织策略”标题，顶部无文字重叠；同时已删除“近似中性”、池大小
  误导和无数据定义的阈值底色。

## 5. 图像 staged-work 动机

文件：`data/report_main/opening_image_stage_aware_evidence.{png,svg}`。

- panel a 来源：`image_clip_preprocess_variants_20260801/raw_repeats.csv` exact tensor path；
  batch 16/64/256 的 prepare/actor 比中位数为 13.9×/31.0×/29.5×，误差线为 IQR（n=30）。
- panel b 来源：R0/R1/R2 transfer ceiling 的 batch64 各 30 次重复；GPU-resident、pinned FP16、
  pageable FP32 分别约 9.82K/8.72K/1.96K img/s，说明 PCIe 不是主损失，host ownership-copy 才是。
- panel c 来源：5K active-window 单次 screening；active4→32 增加供给，active64 时吞吐回退且
  unattributed wait P50 增至 1.44s。该 panel 明确标为 diagnostic，不是策略正式结果。
- 支持：图像 work 需要显式区分 prepare/model/tensor-transfer stage；准入窗口存在欠供给、平台与
  过量排队区间，因此需要阶段状态观测和有界提交。
- 不支持：active32 可迁移到其他规模/机器；图像动态策略已胜；R0-R2 microprofile 可替代 operator E2E。

## 5.1 图像 baseline 纯数据图

文件：`data/report_main/opening_image_baseline_evidence_map.{png,svg}`。

- 路径角色、能力门禁和是否可排名改由报告独立表格呈现；数据图不再包含结构卡片或角色矩阵。
- panel a 以横条展示 12K 三臂均值，并在条末直接写均值±SD；三臂同语义且 exactly-once，但 fast arms
  setup-dominated，Daft 20K 已 OutOfDisk，因此不作稳态排名。删去重复散点、浮动说明和图例，
  避免把小样本诊断视觉包装成正式排名。
- panel b 以每个 CPU 档两条直接标名的横条展示 120K matched-resource Ray Data 与 Project；
  CPU8/16 各 3 formal，横条为均值，条末数字为均值±SD。横轴从 0 开始，不再叠加重复点、均值大点、
  配对线与图例。panel 内显式写明 Daft Built-in 在 20K 已 OutOfDisk、未形成 120K formal cell，
  避免单独截取 panel 时被误解为漏画。只有此 panel 可作性能排名。
- 不支持：给 vLLM pooling 补虚构吞吐；把 Direct ceiling、12K diagnostic 与 120K operator JCT
  混成总排行榜；把 Project Static 称为第三个原生 baseline。

## 5.2 图像四 Job 归一化干扰

文件：`data/report_main/opening_image_fourjob_normalized_impact.{png,svg}`。

- 数据：原生路径读取 `opening_image_native_fourjob_formal_20260810/data/slowdown_summary.csv`；
  Project读取 `opening_image_project_fourjob_observe_only_formal_20260810/data/job_summary.csv`。
- 画法：与文本 N 图统一为 4×4 slowdown 矩阵；列为 Short、Long 1--3，行为 Daft
  Built-in、Ray Data、Project static、Project shared。每格同时标 `four-job/isolated-single`
  倍率与 JCT 增幅，连续蓝色色阶只编码影响强度。
- 关系：Project static/shared 是互斥实验臂，不是同时开启的机制；shared 行的状态 trace
  仅用于 observe-only 诊断，不驱动该次实验动作。
  两份audit均passed、共享同一manifest SHA、每个cell为3次formal且exactly-once。
- 支持：图像多Job干扰会随Job与执行图变化，不能只观测Short，也不能只按图片数平均分配；
  需要per-Job staged work、ready/active/remaining state、隔离与公平约束。
- 不支持：Project状态感知动态收益。RuntimeStateSnapshot未驱动credit或routing，shared/static
  group JCT只差0.98%。原生adapter没有统一prepare/H2D/forward timing，因此本图不补阶段分解。
- 统计边界：本地紧凑归档只有汇总与CV，主图明确使用3次formal均值比；重复离散保留在结果表，
  不用虚构raw点或拥挤误差线代替缺失的逐次本地归档。

## 6. 代价估计的决策价值

文件：`data/report_main/opening_cost_model_decision_quality_v2.{png,svg}`。

- 来源：20-context leave-one-context-out 结果 `ce_context_loo_rerun_20260807.json`。
- 编码：左 panel 报告 estimator 级 candidate pairwise；右 panel 从每个 estimator 的20个
  `folds[].selection.decision_regret_pct` 逐点读取并完整展开。小点是单个 context，纵向抖动
  仅用于避免相同 regret 重叠；小菱形是20个 context 的中位数，同尺寸深色点标记
  最坏 context。浅绿
  0--5% 与浅灰5--15%区间分别辅助读取平均/最坏门槛，不把点云误称为置信区间。
- 点数与数值回读：CE0--CE5 每行严格20点，共120点；CE3 Ridge、CE4 LightGBM、CE5 Hybrid
  各有11/20个精确 `0.0%`，所以旧图中median=0是实际命中oracle所致，不是缺失或舍入。
  三者macro mean为3.88%/3.33%/2.90%，max为22.71%/26.89%/14.72%。绘图入口显式校验每个
  estimator恰好20 contexts，并用逐点数据重算mean/max对齐JSON summary。
- 支持：Hybrid 同时低于 median/macro 5% 与 max 15% 门，max=14.72%，属于 marginal pass。
- 关键反例：Ridge逐行MAE 3.23s低于Hybrid 3.98s，但max regret为22.71%而失败；因此逐行
  预测误差不能替代候选ranking与decision regret。
- 不支持：模型成熟、跨 workload 泛化、worst-case 风险已解决。
- 渲染 QA：总标题、统一五项图例与两个panel标题分层；平均/最坏门槛说明从panel标题下方
  移入图例。非Hybrid最坏context与普通context保持相同尺寸且不加描边，仅改为深灰色；
  中位数使用略大于原始点、但小于旧版汇总标记的小菱形。平均值不以点型重复编码，正式macro mean数值保留
  在图注与报告中。增加画布高度并在六个estimator行之间
  加入浅灰水平分隔线，防止上下点云串行误读。regret横轴在0左侧留白，使零值点云完整显示。
  图注明确抖动无数据含义，图例说明单context/均值/最坏值，标签与门线、标题均不重叠。

当前叙事图中，本轮统一重建 A/T/N/C/H/D/I/J/E/F-main/F-state；B 与 WorkDescriptor 总览保持现有架构图。
A/T/N/C/H/D/I/J/E/F-main/F-state 已逐张打开复核，均无缺字方框、裁切或文字重叠；灰度下仍可由
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
- panel b 使用同一 ShareGPT Chat manifest，比较直接调用（容量参照）、Daft Native、Daft Ray、
  Ray Data 的 service tokens/s；Daft/Ray Data 保持 vendor scheduler ownership。
- DuckDB 的 ShareGPT 4,921/6,144 cap 语义失败只作产品边界注释，不进入 Chat 图的性能排名。
- 两个 panel 的均值±SD 口径只在整图页脚说明一次；删除原先压在最上方柱条上的重复说明，
  条末数字与柱条保持独立，不再发生文字—数据重叠。
- 支持：现有路径在正确吞吐、语义和服务供给状态上各有边界，研究不能只看 raw rows/s；
  这导出 neutral WorkDescriptor、correctness-aware evidence 和状态感知提交。
- 不支持：两个 panel 跨轨排名、Project 普遍胜出或 DuckDB/Daft/Ray 的内部算法归因。

## 9. F：原生文本单 Job 主结果与状态补充

文件：

- `data/report_main/opening_native_single_job_request_latency.{png,svg}`；
- `data/report_main/opening_native_single_job_state_fingerprint.{png,svg}`。

- 类型：experimental results；数据为
  `experiments/results/opening_text_native_single_job_formal_20260808/formal_summary.csv`，
  4 arms × 3 formal，warm-up 不进统计。
- F-main 按相同行顺序展示 Job JCT、vLLM waiting、单请求 queue time 与 TTFT；它承担
  “相近 makespan 掩盖请求级排队”的主结论。Job JCT 明确采用
  `min(submitted_at_s) → max(completed_at_s)`：包含执行路径内部的上游准入、请求提交、vLLM
  排队与推理，直到全部结果 gather；不包含 manifest 准备和数据库 source/sink。
- F-state 移除重复的 JCT，改为六个补充 panel：service tok/s、running、waiting、KV、MFU 与
  GPU utilization。它解释相近 JCT 背后的 overqueue、minimum-saturation 与 underfeed，不承担
  第二个独立性能结论。GPU utilization 采用 0–100% 轴，避免放大 86%–97% 的小差异。
- 两图均使用无描边实心圆表示 3 次 formal 均值，数值直接标在点旁；颜色与 y 位置共同映射
  执行路径。Ray Data 行加浅灰底并写明欠供给，避免把零 waiting 误读为调度更优。重复间 SD
  保留在 `formal_summary.csv` 与审计数据中；主图不比较重复波动，故不叠加误差线和端帽。
- 所有多行 y 轴标签均将完整文本块锚定在轴边缘，同时在文本块内部居中各行；“直接调用”
  与“（容量参照）”因此共享同一视觉中心，不再出现短行被推向右侧的阶梯感。同一规则也用于
  图像四 Job 矩阵的 Project shared 多行标签。
- 主句：前三条已饱和路径的 Job JCT 只差 6.0s，但 Daft Native/Ray 的单请求平均 queue time
  约 37.5s、TTFT 约 40.5s；任务级 makespan 会掩盖请求级排队。Ray Data 的零 waiting 与
  478.7s JCT、3.6k tok/s、MFU 0.11 共同构成欠供给诊断。
- 不支持：不归因框架内部算法，不称某框架普遍更快，不将单 Job
  短 cell 外推为长时间容量排名。

## 10. N：原生四 Job 归一化干扰

文件：`data/report_main/opening_native_fourjob_normalized_impact.{png,svg}`。

- 数据：`opening_fourjob_interference_20260809/data/combined/job_formal_runs.csv`；
  `Short@0s → 3×Long@5s`，每条轨均有四个 isolated-single 与 concurrent-four-job。
- 画法：行是 Daft Native、Daft Ray、Ray Data，列是 Short 与三个 Long；格子颜色编码
  `four-job JCT / 本 Job isolated-single JCT 均值`，格内直接写 slowdown 倍率与 JCT
  百分比增幅。`1.0×` 明确表示无影响；三次 formal 的 SD 保留在正式数据与附录，不再用
  十二组误差线占据主视觉。
- 支持：Short 与三个 Long 在三条原生轨中都受到共享服务竞争影响；多 Job 管理不能只
  观察前台 Short，也要观察全部 Long 的进度和离散。
- 不支持：跨框架绝对 JCT 排名、框架内部调度算法归因、项目方法胜出。
- 叙事角色：这是“现有原生框架也存在多 Job 干扰”的动机证据，放在研究方法之前；不与
  H 合并，因为 H 回答的是项目内部控制臂的因果机制和效率—隔离—公平代价。

## 11. H：Project 四 Job 配额、竞争与共享权衡

文件：`data/report_main/opening_multijob_interference_tradeoff.{png,svg}`。

- 数据：同一四 Job 结果的 `job_formal_runs.csv`、`group_formal_runs.csv`、
  `isolated_normalized_fairness.csv` 与 `long_job_spread.csv`。
- panel a 的每条线固定代表一个Job，依次连接独立Full、独立1/4配额、四Job Static和
  四Job Shared的JCT归一化均值，以趋势分离配额损失、真实竞争与共享策略效果；panel b
  用无边框表直标Static/Shared的组吞吐、group JCT、MFU与变化；panel c按同一Job连接
  Static→Shared的isolated-normalized progress，并直接报告Jain与long spread。
  连线表示预注册受控场景顺序，不是时间序列；三次formal的SD保留在CSV/附录。
- 视觉编码：panel a纵轴改为“归一化JCT（独立运行=1）”，panel c改为“归一化完成进度
  （独立运行=1）”；两条数值轴不再在每个点旁重复标同一数值，准确值直接由刻度读取。
  只有无数值坐标轴的panel b保留表内精确数字，减少冗余和标签遮挡。panel b 的变化列
  统一使用相对变化：MFU从38.2%增至46.8%，表内写相对`+22.41%`，不再单独使用
  `+8.56pp`；Static/Shared原值均为中性深灰，变化上涨为红、下跌为绿，并以正负号冗余编码。
- 支持：shared credit 提高 work conservation，并显著帮助 Short，但当前 equal-weight 点的
  Jain 与 Long 间离散仍需 fairness/SLO guard；因此动态调度目标必须同时包含效率和隔离。
- 不支持：shared/dynamic 普遍胜出、weighted/SLO 已验证、图像动态策略已胜出。
- 叙事角色：这是研究内容二的 Project 机制 A/B 证据，放在方案之后；N 与 H 可以引用同一
  冻结结果目录，但因比较对象、分母和证明义务不同，不是两张重复的性能排名图。

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
4. 用 C、D、I、J、E 展示组织、跨模态阶段、图像baseline/多Job与cost的已有证据；F作为状态解释备份。
5. G 不画，database-E2E 三臂只进附录 correctness/语义表。

该顺序避免把 preliminary signal 写成最终方法胜出，也避免让答辩内容变成实验目录罗列。
