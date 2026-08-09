# 开题叙事与 Claim Matrix

初版冻结日期：2026-08-07
第一性原理复审：2026-08-09（ShareGPT bounded C128、原生单 job 与 5s guaranteed-overlap 多 job 证据已冻结）

用途：本文件是开题阶段研究叙事、证据等级和新增实验停止规则的内部判定表。报告、答辩内容大纲、问答和实验计划若与本表冲突，先回到原始结果核对，再更新本表和相关材料。不得为了得到更好看的结果改变研究问题。当前暂停 PPT 成品制作。

## 1. 冻结题目与系统抽象

题目保持为：

> 数据库 AI 负载的执行优化与调度研究

统一研究对象是数据库触发后的 AI 数据执行层：

```text
Database
  -> AI Data Execution Layer
       -> work-unit construction
       -> cost estimation
       -> admission and routing
       -> resource-aware scheduling
       -> multi-job coordination
  -> Model Service / GPU Executor
  -> Database / Vector Sink
```

研究内容一是 AI workload 感知的数据组织与 work-unit 构造。研究内容二是容量感知的提交、路由与多 job 调度。算子代价估计是两项研究内容共用的使能组件，不单列为第三项研究内容。文本 `AI_COMPLETE` 与图像 `AI_EMBED/AI_CLASSIFY` 用于检验抽象能否跨模态复用。

Daft、Ray、vLLM、CLIP 和 PostgreSQL + pgvector 是实现与验证平台，不是课题贡献本身。课题不修改 vLLM continuous batching、Ray 调度器、模型结构或 GPU kernel。

## 2. Claim Matrix

证据等级：

- `已证明`：可进入开题主线，必须保留适用条件和数据来源。
- `条件性`：可作为初步信号，必须同时写明混淆、门槛或外推边界。
- `待验证`：只能写成研究方案或后续实验，不能使用完成时表述。
- `不能声称`：现有数据不支持，或比较合同不成立。

| Claim | 等级 | 当前证据 | 开题口径 | 仍缺什么 |
|---|---|---|---|---|
| 固定行数不是可靠的 AI work 代理 | 已证明 | `experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv`：固定 16 行时 batch token min/max=474/6,793（14.3×），固定 128 行时 token P95≈26,677 | 行数相同不代表模型计算量相同，work-unit 需要 token/frame 等工作量表征 | 无开题 blocker；跨模态精度属于论文阶段 |
| 同一静态上限下的运行状态会随 offered load 改变 | 已证明 | `experiments/results/dual_gpu_slo_ewma_flush_formal_20260729/README.md`：同为 W65K，high 约 169–172 running、MFU≈35%，arrival-limited 约 19 running、MFU≈7% | 运行状态并非常量，因此需要可观测状态与安全回退；该现象不等于动态策略已经胜出 | 仍缺同上限 static vs dynamic 的 phase-change/burst 正式对照 |
| 固定配置下存在最小饱和 active work | 已证明 | `experiments/results/dual_gpu_active_work_saturation_20260729/README.md`：65,536 work/endpoint 达最大已测吞吐的 97.80%，下一档仅增 0.92% | 上游提交应先标定最小饱和点，再比较策略 | 该数值仅绑定当前机器、模型、协议和 workload |
| 继续增加 offered work 会进入平台并恶化尾延迟 | 已证明 | 同上：65K 到 98K 吞吐仅增 2.25%，P99 36.78s 到 40.05s；`experiments/results/multicard_scale_ramp_formal_20260806/README.md` 显示多路径在大规模共同塌陷 | 上游调度的目标不是无限增压，而是在正确 serving regime 内维持有效供给 | scale-ramp 的跨路径 per-row 时延合同尚未统一 |
| 复杂动态策略不天然优于强静态点 | 已证明 | AIMD/PID/EWMA、adaptive flush、service quantum 和多 actor 的正式对照多数未过约 5% 晋级门槛 | 动态策略只有在 workload 或服务状态变化产生可观测增量时才晋级 | state-aware 新方法仍待后续验证 |
| 数据组织策略排名受 serving regime 影响 | 已证明 | `experiments/results/rc1_data_organization/README.md`：2 endpoint 无 KV 压力时 50k 到 56k，4 endpoint KV 饱和时 39k 到 50k 且排名反转 | work balance 与 locality 存在冲突，不能脱离 endpoint/KV 条件宣称某策略普遍最优 | 单 workload、单模型；严格 feeding-saturation 口径有边界 |
| 项目图像静态执行结构有可重复收益 | 已证明（静态结构信号） | `experiments/results/image_ai_embed_operator_formal_20260803/README.md`：matched CPU8/16 两轮正式实验方向一致，主报告冻结约13%–15% operator-JCT改善 | 在相同资源和输出合同下，显式阶段组织与提交结构优于Ray Data native graph的冻结点 | 不能使用旧45.7%；仍缺状态感知增量、跨workload外推与小规模sink质量闭环；sink不是性能排名blocker |
| 图像 work 是分阶段的，固定图片数不足以描述当前瓶颈 | 已证明（画像） | `motivation/results/gpu/image_clip_preprocess_variants_20260801/README.md`：实用 batch 下 CPU prepare 为 GPU actor 的 13.8–31.2 倍 | 跨模态公共 work-unit 需表达 prepare/model 等阶段需求，不能只把 token 字段改名为 frame | 仍需按 prepare/model work 组织与动态控制的正式消融 |
| 算子代价估计已表现出配置选择价值 | 条件性 | `experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/README.md`：429 formal，CE5 pooled regret 1.67%、macro 2.90%、max 14.72%、candidate pairwise 0.808 | 可写为第一份选择质量可行性证据；CE5 只是 marginal pass | 更多 context、时间段、workload 和硬件 held-out |
| 简单代价 proxy 不足以支持配置选择 | 已证明（文本） | 同上 20 contexts；各 context 四候选 E2E spread 12.0%–86.5%。CE0 mean macro regret 37.2%/pairwise 0.50，CE1 analytical 17.8%/0.53；逐行 MAE 更低的 Ridge 选择反而更差 | 代价估计必须按 ranking/regret 验收，首版保留物理解析结构并只学习 residual | 图像阶段代价的 held-out 选择证据仍缺 |
| 三条静态路径在 SQuAD 均匀控制组已有统一 database-E2E 对照 | 已证明（静态地基） | `experiments/results/opening_database_e2e_text_refeed_20260808/README.md`：K128 replacement 24/24 单元、18 formal 全门禁通过；direct/DuckDB/project service tok/s=40,920.72/40,955.99/41,277.95，correct rows/s=136.63/136.68/137.77 | 喂饱后均匀短输出下三条静态路径近似中性；强静态点必须保留为后续动态对照 | 无 database-E2E blocker；不能从近似中性外推异质负载 |
| 两类 workload 的完整三臂统一 database-E2E 护栏 | 已证明（correctness 地基） | `experiments/results/opening_database_e2e_text_refeed_20260808/`：24/24 cells 的 source/sink、identity、exactly-once 和 CV 门通过；后续饱和扫描证明 ShareGPT C32 direct 只有已测峰值的 52.07% | 可用于 database-E2E 与产品语义边界；ShareGPT 三臂不作 matched-saturation 性能排名 | 原生单 job 正式矩阵改用 bounded C128 |
| ShareGPT C32→C256 呈现欠供给、平台与过量排队区间 | 已证明（容量动机） | `experiments/results/opening_bounded_saturation_calibration_20260808/README.md`：formal tok/s=9,455/14,058/17,834/18,158；C128 达峰值 98.22%，C256 waiting mean=116.8、KV max=0.9996、TTFT mean=6.18s | GPU utilization 高不等于喂饱；状态感知需联合完成速率、running/waiting、KV、MFU 与 tail，控制目标是最小饱和区 | 数值只绑定当前机器、模型、协议、workload；动态收益仍待 A/B |
| 异质文本 workload 会拉开冻结静态路径差距 | 条件性（降级） | ShareGPT project/C32-direct service tok/s=14,568.91/9,425.25、DB-E2E=116.70/180.33 s；C32 后续证实欠供给，且两臂并发/执行结构不同。DuckDB 4,921/6,144 行 cap 语义失败 | 只说旧静态配置在异质 workload 下暴露容量校准和产品语义问题；不能称项目方法收益 | 同 manifest、独立冻结点的原生单 job 矩阵；项目方法增量需同上限 A/B |
| 后到 Job 会影响已存在前台，且共享额度的价值依赖 arrival regime | 已证明（受控文本） | `experiments/results/opening_multijob_interference_20260809/README.md`：在线 replay 下 quota-only≈0，shared 提高总吞吐但 short/Jain 回退；统一 eager 后 full→half quota-only 已使 short JCT +59.00%，matched half→static+long 又 +58.77%，matched full→shared+long +28.90%。eager shared 相对 static 的 short JCT −48.94%、总吞吐 +31.85%、long JCT −25.75%、Jain 0.894→0.972 | 多 Job 管理必须显式表征 per-job work、arrival/active/drain 状态；需要 work-conserving idle borrowing，并同时保留 foreground SLO/fairness guard。online/eager 方向相反，证明策略不能固定跨 regime 复用 | 两 Job 是最小因果点；equal weight、文本。Project eager 与原生仅比较各轨内部 normalized impact，跨轨 T0/绝对 JCT 不排名；weighted/SLO、Long→Short、图像 phase-change 属论文阶段 |
| 四 Job 下 quota、真实竞争和共享调度可以被分离，且效率与公平并非同一目标 | 已证明（受控文本） | `experiments/results/opening_fourjob_interference_20260809/README.md`：Project full→quarter short JCT +180.38%，quarter→static 真实竞争再 +60.40%；shared 相对 static short/long1/2/3 JCT −72.23%/−8.28%/−20.24%/−52.66%，group tok/s +8.68%、MFU +8.56pp，但 Jain 0.960→0.923。Daft Native/Ray、Ray Data 的 short 与三个long相对各自single均退化 | 动态调度需同时实现 idle borrowing、per-job floor/cap、work-fair deficit 与 SLO/fairness guard；必须观测每个 Job 的 remaining/completed work 和 endpoint 状态 | 当前只有一个四Job workload/offset、equal weight；shared long收益和CV不均，不能称最终算法或跨框架胜出；weighted、held-out、故障迁移属论文阶段 |
| state-aware 请求成形/提交优于冻结强静态策略 | 待验证 | 尚无与同上限 frozen static 的正式对照 | 只能写成拟研究方法，不得写成已有贡献 | 开题后 proposed 主实验 |
| 文本原生路径在同环境下呈现稳定但不同的服务压力形态 | 已证明（外部现象） | `experiments/results/opening_text_native_single_job_formal_20260808/README.md`：16/16 cells、12 formal；bounded/Daft Native/Daft Ray/Ray Data tok/s=17,800/17,286/16,747/3,551，CV<0.6%。Daft waiting mean=783/742、KV max≈1；Ray Data running=17.3、MFU=0.112 | 同一任务可落入最小饱和、过量排队或欠供给；状态感知需联合 work rate/MFU、running/waiting、KV 与 tail | 只证明当前官方 graph/冻结点的外部现象；不能归因内部算法或称项目方法胜出 |
| 现有原生路径在多 job 共享服务时呈现前台干扰与不同压力形态 | 已证明（外部现象） | 两 Job见 `opening_multijob_interference_20260809/`；四 Job见 `opening_fourjob_interference_20260809/`。四Job下 Daft Native/Ray/Ray Data short JCT 相对各自single +66.62%/+25.48%/+67.57%，三个long分别退化约178%–200%/132%–158%/118%–123%；Daft两臂high waiting/KV，Ray Data low running/no waiting/low MFU | 同一 Job 数可落入不同服务状态且short/long均受影响，因此需要全局 work/state 观测；不归因框架内部算法，也不把 normalized delta 当跨框架性能排名 | 原生 adapter 无 request P99；Project 与原生完整T0边界不同，短single不作≥60s容量或绝对排名，不称项目优于框架 |
| 项目、Daft、Ray Data 或DuckDB存在跨workload普遍性能优胜关系 | 不能声称 | SQuAD三臂近似中性；ShareGPT三臂并发/语义合同不匹配；原生框架仅有当前冻结点的外部状态观察 | 只能在各自成立的source/语义/timing/feeding合同内报告条件性结果 | 需同任务语义、同完整T0、各系统独立饱和点和正式重复；不作为开题blocker |
| state-aware/shared/dynamic已经普遍优于强静态点 | 不能声称 | 多项动态候选未过约5%门；online/eager多Job方向相反；尚无phase-change同上限主实验 | 写成拟研究方法、已实现的局部机制及可证伪评价计划 | 开题后frozen-static vs state-aware同上限消融 |
| sequential、prefix-aware或65K是全局最优策略/通用容量 | 不能声称 | 组织策略排名随endpoint/KV regime反转；65K只绑定当前机器/模型/协议/workload签名 | 只报告当前签名下的最小近饱和点与机制边界 | 新签名必须重新gate与校准 |
| Project 71.24s与Daft Native 11.06s构成6.4倍系统性能差距 | 不能声称 | Project online含66.875s arrival span；对齐T3后为11.354/11.059s，service throughput/MFU仅差约2.5%–2.7%；两轨完整T0仍不同 | 只用于解释arrival合同与模型请求路径，不作跨轨绝对排名 | 如论文需要绝对容量排名，另做统一完整T0、同语义、≥60s矩阵 |

## 3. 已完成的统一实验组与可排名边界

### 均匀控制组

- workload：SQuAD short-answer，output cap=64。
- arms：`direct_static_sharded`、`duckdb_ai_static_sharded`、`project_frozen_static`。
- 合同：同 PostgreSQL source、同 immutable manifest、2 endpoints、同 Qwen、同 prefix-cache 状态、temperature=0、同数据库 sink、外部统一 database-E2E，1 warmup + 3 formal。
- headline：correct rows/s，同时报告 database-E2E、EM、F1、failure、truncation、service tokens/s、GPU/MFU 和 energy/correct row。

### 异质实验组

- workload：冻结一个 ShareGPT controlled-skew workload，明确 short/medium/long 构成并保存 prompt/output-work histogram。
- arms、source/sink、模型和重复合同与均匀控制组相同。
- 追加报告 work CV、token P50/P95/P99、estimated service work、endpoint work imbalance、TTFT/JCT/tail、cache/locality、active work 和 serving pressure。

两组replacement均已完成。SQuAD通过correctness、feeding与稳定性门，可作三条静态路径的均匀控制地基；ShareGPT通过correctness、sink、identity与稳定性门，但C32 direct后续证实只达已测峰值52.07%，且DuckDB fixed-cap语义失败，因此只作容量校准/产品语义护栏，不作三臂性能排名。原生单job已用bounded C128完成1+3，稳定观察到Daft两臂过量排队与Ray Data当前路径欠供给。5s两Job与1-short+3-long四Job均已于2026-08-09闭环；开题前停止增加offset、weight、Job数量或框架臂，转入数据整理和待画图合同。

## 4. 新实验准入问题

任何新增实验在执行前都必须回答：

1. 它支持开题中的哪一句话？
2. 如果不运行，哪项核心 claim 会缺少可信证据？
3. 为什么现有正式结果不能回答？

无法同时回答三个问题时，不启动实验。本轮不做第二数据库产品、TPC-H cost planning、完整 scale x concurrency grid、weighted/SLO 全矩阵或多模型参数搜索。

## 5. 重构后的 headline evidence

1. Motivation / requirements：固定行隐藏 token work、同一静态上限下 high 与 arrival-limited 状态不同、active-work 存在欠供给/平台/过载区间；分别导出 work 描述、状态感知和有界动态控制的必要性。
2. Work-aware 组织的必要性与局限：固定行 token tail，加 2 endpoint/4 endpoint 数据组织 regime 对照；简化为读者无需猜测散点含义的 small multiples。
3. 图像 stage-aware 泛化：CPU prepare/GPU actor 阶段失衡与 matched-resource 静态结构收益；headline 保持约 13% 到 15%，不使用 45.7%，不称动态 proposed。
4. Cost-model decision quality：正文只显示 Hybrid 的选择质量与边界，六 estimator 全量比较移到备份页；max regret 14.72% 明确为 marginal pass。

算子代价估计必须在主方案图中作为两项研究内容的共同使能部件出现：它向 WorkDescriptor/Organizer 提供 stage work 与不确定区间，也向 admission/routing/multi-job 提供 service/remaining work 和 SLO slack。单独结果页只展示 decision-regret 可行性，不把 cost 写成第三项研究内容。

图和答辩内容合同以 `first_principles_reassessment_20260808.md` 与 `opening_defense_outline_20260808.md` 为准；旧四图与 28 页 v6 仅作历史底稿，不再制作新 PPT 成品。

## 6. 答辩约束

- vLLM 优化模型服务内部，本课题研究数据库数据进入服务前的 work-unit、准入、路由和多 job 协调。
- Ray 是可控执行机制，Daft 是数据引擎和系统 baseline，二者都不是贡献名称。
- 现有负结果说明 strong static 必须作为默认基线，不能用“动态”替代实证。
- 文本和图像用于验证统一 work/credit 抽象的模态边界，不是两个彼此无关的题目。
- 最终创新点是 workload-aware 与 runtime-state-aware 的上游执行优化；其中 state-aware 的性能增量仍待开题后验证。

## 7. 开题材料冻结 readiness

本表区分“证据已经冻结”和“发布产物已经冻结”。只有全部必需发布产物通过视觉与口径
审计后，才能把开题材料整体标为 `frozen`；数据就绪不能替代图、PPT 或发布面验收。

| 材料/证据 | 当前状态 | 权威依据 | 恢复工作后的唯一动作 |
|---|---|---|---|
| 题目、系统抽象、两项研究内容与共同使能组件 | `content-frozen` | 本文件 §1–§2、`opening/report/opening_report.md` | 仅在导师明确改变题目或研究边界时重开 |
| 四级 Claim Matrix 与不能声称边界 | `content-frozen` | 本文件 §2、`opening/qa_bank.md` | 最终答辩一致性审计，不新增实验追正 |
| SQuAD/ShareGPT database-E2E replacement | `evidence-frozen-with-boundary` | `experiments/results/opening_database_e2e_text_refeed_20260808/` | SQuAD 可作静态地基；ShareGPT 只进 correctness/语义附录表 |
| serving capacity、数据组织、图像 matched-resource、cost decision quality | `evidence-frozen` | 本文件 §2、§5 与图合同中的输入哈希 | 不重跑现有实验；只按冻结输入生成材料 |
| 原生单 Job 与两/四 Job 干扰/项目 matched A/B | `evidence-frozen` | `experiments/results/opening_text_native_single_job_formal_20260808/`、`opening_multijob_interference_20260809/`、`opening_fourjob_interference_20260809/` | 只画轨内状态/归一化干扰，不作跨框架绝对排名 |
| 服务器 raw、失败 incident 与扫描归档 | `verified-and-preserved` | 各结果 README 的 archive SHA256 与服务器路径 | 不删除、不覆盖；论文阶段按需回读 |
| 图 B、WorkDescriptor 总览 | `retain-existing` | `figures/audit/opening_story_figures_contract_20260808.md` | 架构图保持现有版本，只做最终版式检查 |
| 七张正文数据图 A/T/N/C/H/D/E | `rendered-qa-pass` | `figures/audit/opening_required_data_figures_20260810.md` 与叙事图合同；八张 PNG/SVG（含备份 F）完成 300-DPI、矢量、灰度和视觉审计 | T/D 分轨呈现文本与图像 baseline；只允许版式级微调，不因追求更漂亮结论更换数据源 |
| 图 F 原生单 Job 状态备份 | `rendered-qa-pass` | 12 formal 的 JCT/tok/s/running/waiting/KV/MFU 原单位 small multiples | 报告正文或答辩追问使用；不作框架通用排名 |
| 图 G | `plan-only-no-result` | phase-change 尚无同上限正式结果 | 开题不画；论文阶段实验通过后再决定 |
| database-E2E 展示 | `appendix-table-only` | SQuAD 可排名、ShareGPT 不可作性能排名 | 不生成正文性能图 |
| 本地开题报告、答辩大纲、QA | `content-with-figures-publication-pending` | `opening/report/opening_report.md`、`opening/opening_defense_outline_20260808.md`、`opening/qa_bank.md` | PPT恢复前再做一次引用、数字和措辞总审计 |
| PPTX | `historical-not-current-paused` | `opening/slides/opening_defense_20260807_v6.pptx` | 用户恢复后基于权威大纲和新图重构、实际渲染检查 |
| 飞书云文档 | `historical-not-current-paused` | revision 289 落后于本地报告 | 用户恢复后从本地权威报告重新生成同步源并差异审计 |
| Wiki | `explicitly-exempt` | 用户明确要求不同步 Wiki | 不执行 |
| 开题材料整体 | `not-yet-frozen` | 数据图已验收；PPT 和云发布面仍暂停/未验收 | 后续仅完成获准的发布工作与最终一致性审计；不得据此新增 baseline |

停止规则：当前不存在需要通过新增开题实验才能解除的 readiness 阻塞。图渲染、PPT、云文档
和最终审计恢复后只消费现有冻结数据；不得为了填满页面、改善叙事或得到更好看的方向而重跑
offset、weight、更多 Job 数、第二数据库、文本全框架矩阵或大规模参数扫描。

## 8. 本地内容一致性审计（2026-08-09）

审计对象为 `PROJECT_OUTLINE.md`、本地开题报告、答辩大纲、QA、第一性原理复审和本
Claim Matrix；数字直接回到冻结 CSV/JSON，不以文档之间相互一致代替数据复核。

| 审计项 | 权威数据复核 | 结论 |
|---|---|---|
| database-E2E | `summary/formal_summary.csv` 共 6 组，每组 3 formal；SQuAD correct rows/s=136.63/136.68/137.77，ShareGPT DuckDB cap failures=4,921/6,144 | 报告数字一致；ShareGPT 不作性能排名 |
| bounded capacity | `runs_summary.csv` 的 formal C32/C64/C128/C256=9,454.88/14,057.93/17,834.14/18,158.19 tok/s | C128=98.22% 已测峰值，最小饱和口径一致 |
| 原生单 Job | `opening_text_native_single_job_formal_20260808/formal_summary.csv` 四臂与报告的 17,800/17,286/16,747/3,551 tok/s 一致 | 只作外部状态指纹，不归因内部算法 |
| 多 Job | 两Job `opening_multijob_interference_20260809/data/eager_project/cross_system_short_impact.csv`；四Job `opening_fourjob_interference_20260809/data/combined/{job_slowdown_comparisons,group_summary}.csv` | 两Job复核arrival-regime dependence；四Job复核quota/competition/shared、公平性和三条原生轨内退化；跨轨绝对JCT不排名 |
| 图像 matched-resource | schema-v12 正式表复核 cpu8/cpu16 两档均同向；主报告继续使用跨 campaign 冻结的约 13%–15%，不用旧 45.7% | 静态结构信号成立；动态增量仍待验证 |
| cost decision quality | UTF-8 合法的 `ce_context_loo_rerun_20260807.json` 复核 429 formal、20 context；CE5 pooled/macro/max=1.6657%/2.8973%/14.7152%，candidate pairwise=0.8083 | 文档四舍五入一致，保持 marginal pass |
| 禁止外推 | 扫描旧 45.7%、15 s、6.4×、ShareGPT 154.57%、K512 与“动态胜出”等关键词 | 仅出现在反驳、历史说明或不能声称条目；未发现主线越界 |

审计发现并修复三类证据卫生问题：代价报告标题补充 429-formal 合并评估边界；估计器
范围由误写的 CE0–CE6 更正为实际 CE0–CE5，并删除“研究内容四”的矛盾表述；合并 LOO
JSON 中 6 处误编码的 `§6` 已规范为 UTF-8。上述修正均不改变实验字段、数值或 Claim 等级。

## 9. 总目标完成条件审计（2026-08-09）

本表按当前 `/goal` 的完整交付条件验收，不以“文件已经存在”代替内容、门禁、渲染或发布
验收。用户明确暂停的工作保持未完成，不能据此把总目标提前标为完成。

| 目标要求 | 当前判定 | 可验证证据 | 剩余动作 |
|---|---|---|---|
| 冻结论文叙事与 Claim Matrix | `complete` | 本文件 §1–§4；四级 Claim、实验准入与停止规则已冻结 | 仅导师改变题目时重开 |
| SQuAD 统一 database-E2E 三臂 1+3 | `complete` | replacement 12 cells/9 formal；source/sink、correctness、feeding、stability 通过 | 无 |
| ShareGPT 统一 database-E2E 三臂 1+3 | `complete-with-ranking-boundary` | replacement 12 cells/9 formal；correctness/sink/stability 通过；C32 欠供给与 DuckDB cap 语义失败已单列 | 不作三臂性能排名 |
| 完成 P0 后停止新增开题 baseline | `complete` | §4、§7 停止规则；后续只补支持新用户问题的最小诊断，不扩第二数据库或全矩阵 | 保持停止 |
| serving capacity 核心证据 | `complete-figure` | active-work、bounded C32–C256 与图 A 数据合同完整，图已通过 QA | 后续只在统一版式时无损复用 |
| work-aware 组织及局限核心证据 | `complete-figure` | 2/4 endpoint regime 数据与图 C 合同完整，图已通过 QA | 保留 feeding 边界，不转述为容量排名 |
| 图像 staged/baseline 核心证据 | `complete-figure` | 图 D：CPU prepare画像、12K Daft/Ray Data/Project 结构诊断、120K Ray Data/Project matched-resource 正式重复 | 只在120K matched panel排名；12K三臂不外推 |
| cost-model decision quality 核心证据 | `complete-existing-figure` | 图 E；429 formal/20 context，CE5 marginal pass；UTF-8 JSON 与新 SHA 已验证 | 最终版式检查 |
| PROJECT_OUTLINE 与本地开题报告重构 | `content-complete` | 总纲、报告、答辩大纲、QA 与 §8 数据复算一致 | 新图插入后做引用总审计 |
| 四级 Claim 与答辩攻击面审计 | `complete` | 本文件 §2、`opening/qa_bank.md` 的攻击面、不能声称与回答模板 | PPT 完成后再做逐页口径检查 |
| 原生状态与多 Job 新增动机证据 | `data-complete-render-paused` | 两Job与四Job输入/门禁完整；四Job120条逐Job、75条组级和18条阶段formal已归档 | 获准后首次生成 F/H；不新增实验 |
| PPT 重构与实际渲染验收 | `incomplete-paused` | 当前 v6 仅为历史底稿，不能代表新叙事 | 用户恢复后增量重构并在 PowerPoint/WPS 实查 |
| 飞书云文档发布面 | `incomplete-paused` | revision 289 已标历史，未用当前报告覆盖 | 用户恢复后从本地权威报告生成并差异审计 |
| Wiki | `explicitly-exempt` | 用户明确要求不同步 Wiki | 无 |
| 本地/GitHub/实验服务器同步与 raw 保留 | `complete-current-commit` | Git 三端一致；服务器 archive SHA 与空间已复核，未跟踪 raw 未清理 | 每次后续提交继续同步 |
| 开题材料整体冻结 | `incomplete` | A/C/F/H、PPT、云发布和最终逐页/逐图审计未全部完成 | 只完成发布层工作后再执行最终 completion audit |

当前没有实验层 blocker；剩余项全部属于用户暂停的渲染与发布层。恢复后不得借“最终材料尚未
冻结”重新打开已经关闭的 baseline、offset、weight、更多 Job 数、K512 或第二数据库实验。
