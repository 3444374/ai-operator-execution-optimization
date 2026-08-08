# 开题叙事与 Claim Matrix

初版冻结日期：2026-08-07
第一性原理复审：2026-08-08（feeding 纠正与叙事重构中，尚未最终冻结）

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
| 项目图像静态执行结构有可重复收益 | 已证明 | `experiments/results/image_ai_embed_operator_formal_20260803/README.md`：matched CPU8/16 两轮正式实验方向一致，主报告冻结约 13% 到 15% operator-JCT 改善 | 在相同资源和输出合同下，显式阶段组织与提交结构优于 Ray Data native graph 的冻结点 | 不能使用旧 45.7%；仍缺当前 commit 的统一 operator-E2E 排名与状态感知增量；sink 不是性能排名 blocker |
| 图像 work 是分阶段的，固定图片数不足以描述当前瓶颈 | 已证明（画像） | `motivation/results/gpu/image_clip_preprocess_variants_20260801/README.md`：实用 batch 下 CPU prepare 为 GPU actor 的 13.8–31.2 倍 | 跨模态公共 work-unit 需表达 prepare/model 等阶段需求，不能只把 token 字段改名为 frame | 仍需按 prepare/model work 组织与动态控制的正式消融 |
| 算子代价估计已表现出配置选择价值 | 条件性 | `experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/README.md`：429 formal，CE5 pooled regret 1.67%、macro 2.90%、max 14.72%、candidate pairwise 0.808 | 可写为第一份选择质量可行性证据；CE5 只是 marginal pass | 更多 context、时间段、workload 和硬件 held-out |
| 简单代价 proxy 不足以支持配置选择 | 已证明（文本） | 同上 20 contexts；各 context 四候选 E2E spread 12.0%–86.5%。CE0 mean macro regret 37.2%/pairwise 0.50，CE1 analytical 17.8%/0.53；逐行 MAE 更低的 Ridge 选择反而更差 | 代价估计必须按 ranking/regret 验收，首版保留物理解析结构并只学习 residual | 图像阶段代价的 held-out 选择证据仍缺 |
| 三条静态路径在 SQuAD 均匀控制组已有统一 database-E2E 对照 | 条件性（failed-feeding 诊断） | 首轮统一 source/sink 与三次 formal 完整，但 project feeding ratio 89.9% 未过 95% 门；纠正校准已选择 K128 | 首轮只说明比较合同和瓶颈诊断可执行，不能进入最终性能排名 | 以 K128 的合格整体替换重跑 |
| 两类 workload 的完整三臂统一 database-E2E 结论 | 待验证（替换重跑中） | 首轮 24/24 单元与 correctness 完整，但 SQuAD/ShareGPT project feeding 89.93%/91.38% 均未过门 | 在两份 workload-specific 校准合同通过后整体重跑；只引用新矩阵 | ShareGPT K32/64/128/256 选择与两组完整 replacement formal |
| 异质文本 workload 会稳定拉开三臂差距 | 已证明（否定） | ShareGPT direct/project correct rows/s 为 11.34/10.36，service feeding 91.38%；差距没有反转为项目优势。DuckDB AI service 吞吐≈direct，但 4,936/6,144 行 cap 语义失败主导 correct throughput | 异质性本身不足以制造调度增量，且产品语义必须进入正确吞吐 | 不补新 baseline；状态变化与方法增量留开题后验证 |
| state-aware 请求成形/提交优于冻结强静态策略 | 待验证 | 尚无与同上限 frozen static 的正式对照 | 只能写成拟研究方法，不得写成已有贡献 | 开题后 proposed 主实验 |
| Daft/Ray Data 文本原生路径已有同环境正式排名 | 待验证 | 当前只有 64/256-row validity/scale gate，没有至少 60 s、1+3 交错 formal | 只能说原生入口可执行且 scheduler owner 可审计，不得说哪个框架更快 | 同 ShareGPT Chat manifest 的 bounded、Daft Native/Ray、Ray Data 独立冻结点 1+3 formal |
| 现有原生框架在多 job 共享服务时已暴露全局提交/感知缺口 | 待验证 | 已有项目 1/2/4-job 结果不包含 Daft built-in/Ray Data，且同步等量启动不能证明 idle borrowing | 不预设现有框架一定表现差；只观察独立 job 竞争、全局压力和可观测性 | Daft Native/Ray、Ray Data short/long staggered 原生观察 + 项目 static/shared 同上限 A/B |

## 3. 待最终冻结的统一实验组

### 均匀控制组

- workload：SQuAD short-answer，output cap=64。
- arms：`direct_static_sharded`、`duckdb_ai_static_sharded`、`project_frozen_static`。
- 合同：同 PostgreSQL source、同 immutable manifest、2 endpoints、同 Qwen、同 prefix-cache 状态、temperature=0、同数据库 sink、外部统一 database-E2E，1 warmup + 3 formal。
- headline：correct rows/s，同时报告 database-E2E、EM、F1、failure、truncation、service tokens/s、GPU/MFU 和 energy/correct row。

### 异质实验组

- workload：冻结一个 ShareGPT controlled-skew workload，明确 short/medium/long 构成并保存 prompt/output-work histogram。
- arms、source/sink、模型和重复合同与均匀控制组相同。
- 追加报告 work CV、token P50/P95/P99、estimated service work、endpoint work imbalance、TTFT/JCT/tail、cache/locality、active work 和 serving pressure。

首轮两组实验因项目臂未喂饱而只作诊断。当前先按 workload 独立选择最小饱和 K，再整体替换。为闭合“现有系统不足→设计”因果链，替换矩阵后只再补两类不可替代证据：一个 ShareGPT Chat 原生框架单 job 矩阵，以及 Daft/Ray Data 原生两 job 观察 + 项目 static/shared 因果 A/B。差异不足 5% 或为负同样有效；不换 workload、模型、数据库，不扫更多 offset/weight 追正。

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
