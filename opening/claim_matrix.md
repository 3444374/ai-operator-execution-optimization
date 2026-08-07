# 开题叙事与 Claim Matrix

冻结日期：2026-08-07

用途：本文件是开题阶段研究叙事、证据等级和新增实验停止规则的内部判定表。报告、PPT、答辩问答和实验计划若与本表冲突，先回到原始结果核对，再更新本表和相关材料。不得为了得到更好看的结果改变研究问题。

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
| 固定行数不是可靠的 AI work 代理 | 已证明 | `experiments/results/local_vllm_qwen15b_baseline/README.md`：固定行 batch=8 时 token 跨度 13.9x，batch=128 时 token P95=26,678 | 行数相同不代表模型计算量相同，work-unit 需要 token/frame 等工作量表征 | 无开题 blocker；跨模态精度属于论文阶段 |
| 固定配置下存在最小饱和 active work | 已证明 | `experiments/results/dual_gpu_active_work_saturation_20260729/README.md`：65,536 work/endpoint 达最大已测吞吐的 97.80%，下一档仅增 0.92% | 上游提交应先标定最小饱和点，再比较策略 | 该数值仅绑定当前机器、模型、协议和 workload |
| 继续增加 offered work 会进入平台并恶化尾延迟 | 已证明 | 同上：65K 到 98K 吞吐仅增 2.25%，P99 36.78s 到 40.05s；`experiments/results/multicard_scale_ramp_formal_20260806/README.md` 显示多路径在大规模共同塌陷 | 上游调度的目标不是无限增压，而是在正确 serving regime 内维持有效供给 | scale-ramp 的跨路径 per-row 时延合同尚未统一 |
| 复杂动态策略不天然优于强静态点 | 已证明 | AIMD/PID/EWMA、adaptive flush、service quantum 和多 actor 的正式对照多数未过约 5% 晋级门槛 | 动态策略只有在 workload 或服务状态变化产生可观测增量时才晋级 | state-aware 新方法仍待后续验证 |
| 数据组织策略排名受 serving regime 影响 | 已证明 | `experiments/results/rc1_data_organization/README.md`：2 endpoint 无 KV 压力时 50k 到 56k，4 endpoint KV 饱和时 39k 到 50k 且排名反转 | work balance 与 locality 存在冲突，不能脱离 endpoint/KV 条件宣称某策略普遍最优 | 单 workload、单模型；严格 feeding-saturation 口径有边界 |
| 项目图像静态执行结构有可重复收益 | 已证明 | `experiments/results/image_ai_embed_operator_formal_20260803/README.md`：matched CPU8/16 两轮正式实验方向一致，主报告冻结约 13% 到 15% operator-JCT 改善 | 在相同资源和输出合同下，显式阶段组织与提交结构优于 Ray Data native graph 的冻结点 | 不能使用旧 45.7%；system database-E2E 与状态感知增量未完成 |
| 算子代价估计已表现出配置选择价值 | 条件性 | `experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/README.md`：429 formal，CE5 pooled regret 1.67%、macro 2.90%、max 14.72%、candidate pairwise 0.808 | 可写为第一份选择质量可行性证据；CE5 只是 marginal pass | 更多 context、时间段、workload 和硬件 held-out |
| 数据库产品、直接控制与项目静态路径已有统一 database-E2E 排名 | 不能声称 | 现有 scale-ramp 的 request 与 query-barrier timing granularity 不一致，且未统一质量与 sink | 现有结果只用于 serving capacity 与失败边界，不做完整三臂 per-row 排名 | P0-1 SQuAD 三臂统一合同 |
| 异质文本 workload 会稳定拉开三臂差距 | 待验证 | 既有 RC1/cost 数据证明 heterogeneity 和 regime 信号存在，但不是目标三臂统一 database-E2E 对照 | 作为研究动机问题，不预设结果方向 | P0-2 冻结异质 workload 三臂实验 |
| state-aware 请求成形/提交优于冻结强静态策略 | 待验证 | 尚无与同上限 frozen static 的正式对照 | 只能写成拟研究方法，不得写成已有贡献 | 开题后 proposed 主实验 |

## 3. 开题前仅允许的新增数据

### P0-1：均匀控制组

- workload：SQuAD short-answer，output cap=64。
- arms：`direct_static_sharded`、`duckdb_ai_static_sharded`、`project_frozen_static`。
- 合同：同 PostgreSQL source、同 immutable manifest、2 endpoints、同 Qwen、同 prefix-cache 状态、temperature=0、同数据库 sink、外部统一 database-E2E，1 warmup + 3 formal。
- headline：correct rows/s，同时报告 database-E2E、EM、F1、failure、truncation、service tokens/s、GPU/MFU 和 energy/correct row。

### P0-2：异质实验组

- workload：冻结一个 ShareGPT controlled-skew workload，明确 short/medium/long 构成并保存 prompt/output-work histogram。
- arms、source/sink、模型和重复合同与 P0-1 相同。
- 追加报告 work CV、token P50/P95/P99、estimated service work、endpoint work imbalance、TTFT/JCT/tail、cache/locality、active work 和 serving pressure。

两组实验完成后停止增加开题 baseline。差异不足 5% 不触发换 workload、换模型、换数据库或扩大参数扫描。结果接近同样是有效结论，它说明当前 serving 层吸收了上游差异，state-aware 方法需要更明确的竞争 regime。

## 4. 新实验准入问题

任何新增实验在执行前都必须回答：

1. 它支持开题中的哪一句话？
2. 如果不运行，哪项核心 claim 会缺少可信证据？
3. 为什么现有正式结果不能回答？

无法同时回答三个问题时，不启动实验。开题前不做第二数据库产品、文本 Daft/Ray Data 全矩阵、multi-job 五 baseline、TPC-H cost planning 或完整 scale x concurrency grid。

## 5. 四组 headline evidence

1. Serving capacity / overload：多路径 scale-ramp，只说明容量平台和过载塌陷，不做不同 timing granularity 的 per-row 时延横比。
2. Work-aware 组织的必要性与局限：固定行 token tail，加 2 endpoint/4 endpoint 数据组织 regime 对照。
3. 图像 matched-resource：使用 Ray Data vs project 的 CPU8/CPU16 正式结果，headline 保持约 13% 到 15%，不使用 45.7%。
4. Cost-model decision quality：展示 CE0 到 CE5 selection regret，明确 CE5 max regret 14.72% 是 marginal pass。

## 6. 答辩约束

- vLLM 优化模型服务内部，本课题研究数据库数据进入服务前的 work-unit、准入、路由和多 job 协调。
- Ray 是可控执行机制，Daft 是数据引擎和系统 baseline，二者都不是贡献名称。
- 现有负结果说明 strong static 必须作为默认基线，不能用“动态”替代实证。
- 文本和图像用于验证统一 work/credit 抽象的模态边界，不是两个彼此无关的题目。
- 最终创新点是 workload-aware 与 runtime-state-aware 的上游执行优化；其中 state-aware 的性能增量仍待开题后验证。
