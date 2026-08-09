# 项目大纲

更新时间：2026-08-09

本文件是项目方向、研究内容、证据等级和近期执行顺序的权威总纲。实验细节以对应结果目录的 README/CSV/JSON 为准；文献入口见 `research/knowledge_hub.md`；开题材料必须服从 `opening/claim_matrix.md`。

## 1. 题目与研究对象

题目冻结为：

> 数据库 AI 负载的执行优化与调度研究

统一研究对象是数据库触发后的 AI 数据执行层：

```text
Database
  -> AI Data Execution Layer
       -> research content 1: work-unit construction and organization
       -> research content 2: state-aware admission, routing and multi-job
       -> shared cost estimator
            -> stage/service/remaining work
            -> SLO slack and uncertainty
  -> Model Service / GPU Executor
  -> Database / Vector Sink
```

对外口径：数据库内置 AI 算子的外部分布式数据处理执行链路优化。

Daft、Ray、vLLM、PostgreSQL、pgvector 和 CLIP 是实现与验证平台，不是贡献名称。项目不修改数据库内核、vLLM continuous batching、Ray 调度器、模型结构或 GPU kernel，也不回到传统 GPU 查询算子。

## 2. 研究内容

### 2.1 研究内容一：workload-aware work-unit 构造

研究数据库记录如何组成发送给模型服务的 staged work unit。核心接口是 `WorkDescriptor`：source/prepare/model/result work、locality key、deadline/SLO、不确定区间和 calibration signature，而不是固定行数或把 token 机械改名为 frame。候选机制包括 sequential budget、length alignment、prefix-aware grouping 和受控 best-fit。重点刻画两个冲突：

- work balance：减少 batch 与 endpoint 之间的计算量偏差；
- locality：保留 prefix、frame 或数据局部性，避免因重排序破坏缓存与流水线效率。

评价 packing、work skew、prefix group ratio、cache hit、吞吐、TTFT、尾延迟、能耗与任务质量。策略排序必须绑定机器、模型、endpoint/KV regime 和 workload，不宣称全局最优。

### 2.2 研究内容二：容量感知的提交、路由与多作业调度

以 endpoint-shared request/work credit 表达在途工作量，在 completion 时精确释放并连续补位；在固定资源和上限下研究：

- 最小饱和 active work 与过载边界；
- request-level replenishment；
- endpoint routing、idle borrowing 与故障迁移；
- 多 job fair queue、JCT、tail、SLO 和公平性。

固定静态 credit 是默认强 baseline。动态 K、queue-adaptive flush、状态感知 routing 或多作业控制只有显著优于同资源、同上限静态点时才晋级；吞吐接近时继续评价 tail/SLO/fairness，均无改善则记录失效边界。

### 2.3 共同使能组件：算子代价估计

首版采用解析 work 特征、profile 校准和 residual correction，预测文本/图像的 stage work、operator service、JCT、remaining work 与 SLO slack。它同时服务于 active-work 初始化、`WorkDescriptor`/组织预算、路由、提交与多 job，不单列为第三项研究内容。

评价 MAE/MAPE 之外的候选配置 ranking、pairwise accuracy、selection regret、最坏 context 与预测区间；平均误差好不能替代决策质量。

### 2.4 多模态泛化

文本 `AI_COMPLETE` 是主要方法场景；图像 `AI_EMBED/AI_CLASSIFY` 是正文泛化验证。公共策略只消费 staged estimated work、credit、fresh state 和 completion event：文本 adapter 输出 source/tokenize/prompt-output/result work，图像 adapter 输出 encoded/prepare/tensor-model/result work；Organizer、Scheduler、Tracing 和配置逻辑保持一致。不适用某模态的能力必须显式声明。

## 3. 系统与实验边界

```text
PostgreSQL source
  -> Daft DataFrame / Arrow
  -> Shared Cost Estimator + WorkDescriptor + Organizer
  -> Ray actor admission / shared credit / routing
  -> text: vLLM generation
     image: typed CLIP GPU actor
  -> unified PostgreSQL / pgvector sink
```

- 写回采用 PostgreSQL + pgvector、COPY + deferred index，属于统一 correctness/E2E guardrail，不是独立研究内容。
- 正式 baseline 必须由被测系统拥有执行与调度；项目只做 source、sink、质量审计和指标适配。
- 自写 actor pool、credit、inflight/backpressure 或 Daft UDF 只能按清晰 provenance 标为项目方法或 diagnostic reference。
- 模型/数据下载不等于数据库 workload 已导入；必须继续执行 importer 和 schema/行数/exactly-once 门禁。
- 性能参数绑定“机器 + 模型/服务配置 + 协议 + workload 分布/规模”签名，签名变化重新校准。

## 4. 研究问题与因果设计

四个研究问题：

1. 固定资源下达到近饱和吞吐所需的最小 active work 是多少，过载怎样影响 tail 与能耗？
2. 相同 work 下怎样组织记录，balance 与 locality 何时冲突？
3. arrival、work mix 或阶段瓶颈突变时，state-aware admission/routing 能否在同一最大 K/work 上限下比 frozen-static 更快回到标定的近饱和包络？
4. 多 job 共享 endpoint pool 时，shared credit、routing 和公平队列能否改善 JCT/tail/fairness？

两项策略先独立搜索冻结静态点并分别消融，再把独立最优拼接，与小规模联合 grid 对比。联合显著优于拼接说明需要联合调优；两者接近说明可分层优化。任何结果都不改变研究对象，但会改变方法适用边界。

正式实验统一要求：immutable manifest、相同 source、相同完整结果语义、相同服务 flags、固定随机种子、warmup 与交错 formal repeats；结果保存请求、submission、资源时序和版本。调度主实验以完整结果 gather 的 correct throughput/JCT 为 headline；仅 database-E2E 护栏要求相同 sink 与 sink readback，另报告 service throughput、质量、failure 类型和资源门禁。

## 5. 当前证据等级

### 5.1 已证明

- 固定行数不是稳定 work 代理：固定 16 行批次的 work 最小/最大中位数为 474/6,793 token，相差 14.3 倍。
- 同一静态上限不是运行状态：W65K 下 high offered load 的运行内峰值 active work/MFU 约为 100%/35%，arrival-limited 约为 29%/7%；前者不是时间平均 active work。
- 当前双 4090/Qwen/vLLM 签名下，65,536 active work/endpoint 达最大已测吞吐均值的 97.80%；下一档只增 0.92%，继续增压会恶化 P99。
- 复杂动态控制不天然优于强静态点：AIMD/PID/EWMA、adaptive flush、service quantum 与多 actor 多数未过约 5% 晋级门槛。
- 数据组织策略排名受 serving regime 影响：双 endpoint 大 KV 池下策略范围约 12%；四 endpoint 小 KV 池饱和时分化约 27% 且排名反转，重排序可使 prefix hit 降至 0.06–0.07。
- 图像 matched-resource 静态执行结构有可重复收益：主报告冻结约 13%–15% operator-JCT 改善；旧 45.7% 资源不匹配，不再使用。
- 多 Job 干扰已从两作业扩展到受控 `short@0s → 3 long@5s`：Project full/quarter
  single 将 quota 与竞争分离；三条原生路径全部 Job 均出现轨内退化。Project shared 相对
  static 总吞吐 +8.68%、short JCT −72.23%，但 Jain 0.960→0.923且 long 收益/稳定性不均，
  支撑 idle borrowing 与 fairness guard 同时存在的研究问题，不证明 dynamic 普遍胜出。

### 5.2 条件性

- Hybrid 代价估计在 429 个 formal 观测、20 context × 4 candidate 的 context-LOO 中取得 pooled regret 1.67%、macro 2.90%、pairwise 0.808、max regret 14.72%。最大 regret 距 15% 门仅 0.28 个百分点，属于 marginal pass。
- prefix-affinity routing 在四 endpoint/小 KV 池条件下出现约 5.9% 增量，但 endpoint consolidation 与饱和深度尚未完全隔离，不外推为普遍有效。

### 5.3 待验证

- runtime-state-aware 请求成形、提交或路由能否超过同上限 frozen-static；
- phase-change、burst、mixed-cost 下 dynamic 的响应时间、SLO goodput 与 tail；
- 多 job 的 5s 两作业与 1-short+3-long 四作业均已完成；仍待新 workload held-out、
  加权/SLO、公平 guard、Long→Short 与故障迁移；
- 代价模型跨时间段、新 workload 和硬件的稳定性；
- 图像Daft built-in、Ray Data native与project frozen-static的operator-E2E/provenance证据已完成；仍待状态感知增量、跨workload外推与小规模sink质量闭环，sink不是性能排名blocker。
- 图像 short→3×long 多作业与 DuckDB bounded-output 四作业目前只完成 immutable
  manifest/config/runner/指标准备，尚未启动正式实验；图像 proposed 角色已与具体算法名
  解耦，后续状态感知/动态调度调整只需版本化并重跑 project static/proposed。

### 5.4 不能声称

- 项目路径普遍优于 direct、DuckDB AI、Daft、Ray Data 或 vLLM 官方路径；
- sequential、length-align 或 prefix-aware 是全局最优 organizer；
- 65,536 是 vLLM 通用容量或最佳并发；
- 动态策略已经胜出；
- 图像路径提升 45.7%；
- 代价模型已经稳健解决。

## 6. 开题前统一文本 database-E2E

2026-08-07 首轮三臂因 project feeding 仅为 direct 的 89.9%/91.38%，保留为 failed-feeding 历史诊断。2026-08-08 K128 replacement 的 24/24 单元、18 formal 通过 correctness、sink、identity 与稳定性门禁；但随后 ShareGPT bounded C32–C256 扫描证明 C32 只有已测峰值的 52.07%，故 ShareGPT 三臂性能排名降级，正式原生矩阵改用达到峰值 98.22% 的最小点 C128。

开题静态地基先完成 SQuAD short-answer 均匀控制组与 ShareGPT controlled-skew 异质组。两组均比较：

- `direct_static_sharded`；
- `duckdb_ai_static_sharded`；
- `project_frozen_static`。

统一合同：PostgreSQL source、immutable equal-row manifest、双 Qwen2.5-7B vLLM endpoint、prefix cache ON、统一 PostgreSQL sink、外部 database-E2E、质量与资源指标、1 warmup + 3 formal。

SQuAD replacement 三次 formal 均值：direct、DuckDB AI、project 的 correct rows/s 为 136.63、136.68、137.77，service tokens/s 为 40,920.72、40,955.99、41,277.95；三臂 EM/F1 接近，project/direct service ratio 为 1.0087，均匀短输出下近似中性。

ShareGPT replacement 三次 formal 均值：direct、DuckDB AI、project 的 correct rows/s 为 11.36、2.26、17.55，service tokens/s 为 9,425.25、9,421.31、14,568.91。后续 bounded C32/C64/C128/C256 扫描为 9,454.88/14,057.93/17,834.14/18,158.19 tok/s，C128 是达到已测峰值 97% 的最小点；C256 仅增 1.82%，却使 waiting mean=116.8、KV max=0.9996、TTFT mean=6.18s。旧 project/C32-direct=1.5457 因对照欠供给而不作方法排名。DuckDB fixed-cap 产品语义失败 4,921/6,144 行的结论仍有效。

同一 ShareGPT Chat manifest 的原生单 job 1+3 已完成：bounded C128、Daft Native、Daft Ray、Ray Data 的 service tok/s 为 17,800/17,286/16,747/3,551，四臂 CV<0.6%。Daft 两臂 waiting mean 为 783/742、KV max≈1，呈现过量提前提交；Ray Data running mean=17.3、MFU=0.112，呈现供给不足；bounded C128 位于最小饱和区。该结果只证明官方 graph/冻结点的外部压力形态，不证明项目方法胜出或某个框架内部算法有缺陷。

5s guaranteed-overlap 对照已完成：Daft Native/Ray、Ray Data 的 short JCT 相对各自 single 增加82.42%/104.84%/32.76%，只作外部观察。项目在线 replay 下 quota-only≈0，static/shared 加入 long 后 short JCT增加3.79%/8.95%；shared 提高总吞吐但 short/Jain回退。统一 eager Project 12 formal 又显示 full→half quota-only 已使short JCT+59.00%，matched half→static+long再+58.77%，matched full→shared+long+28.90%；eager shared 相对static使short JCT−48.94%、总吞吐+31.85%、long JCT−25.75%、Jain 0.894→0.972。两种到达regime方向相反，冻结为“多Job管理必须感知arrival/active/drain状态、支持idle borrowing并保留SLO/fairness guard”的证据，不称动态普遍胜出；开题前不再扫offset/weight/更多Job追正。

Project all-at-t0 single-short 诊断已补齐统一 T0–T4 计时：T0 profiler E2E14.957s，T3 earliest model submit→latest completion11.354s，service14,361tok/s、MFU42.93%；Daft Native同一short T3为11.059s、14,727tok/s、MFU44.04%，差异仅约2.5%–2.7%。Daft缺准备前T0，因此完整E2E仍不排名；该结果排除了“Project模型请求路径慢6.4×”。随后Project eager多Job只补full single、half single、static+long、shared+long，不重跑原生三臂；arrival span均为66.76µs、12/12 formal通过。逐阶段显示matched static竞争使short service mean/P99 +50.34%/+78.62%，shared为+14.63%/+28.70%；submit→service仍约2ms。在线replay与eager结论分轨保留。

## 7. 开题叙事图

1. `opening_motivation_work_state`：固定行隐藏 work、静态上限不是状态、提交压力存在最小近饱和点与边际收益递减区，分别导出 WorkDescriptor、感知和有界控制。
2. `opening_ai_data_execution_boundary`：两项研究内容并列，算子代价估计作为共同使能部件。
3. `opening_work_to_schedule_overview`：组织输出 work/locality/deadline，调度结合 fresh state 消费。
4. `opening_work_organization_regime_v2`：work-aware 组织的必要性与 regime 局限。
5. `opening_image_stage_aware_evidence`：图像阶段失衡与 matched-resource preliminary signal。
6. `opening_cost_model_decision_quality_v2`：代价模型 selection regret 与最坏风险。

权威输出位于 `figures/data/report_main/` 与 `figures/architecture/`，生成脚本为 `figures/scripts/generate_opening_story_figures_20260808.py`，claim 与视觉审计见 `figures/audit/opening_story_figures_contract_20260808.md`。现有六张叙事资产均可读；A/C 数据已冻结但仍需标签级重绘，F/H 为 `data-ready-not-generated`，G 为 `do-not-draw-no-result`。当前只冻结内容大纲、紧凑实验数据和待画图合同，不生成新图或新的 PPT 成品。

## 8. 当前执行顺序

1. 第一性原理 framing、Claim Matrix、staged WorkDescriptor/状态合同与共同 cost enabler 已完成；六张现有图已做视觉回读，但 A/C 标签修订、F/H 首次生成仍待用户恢复绘图。这里的“合同完成”只表示类型/纯策略基础可执行：production descriptor builder、fresh stage snapshot 正式接线和 CE5 在线驱动仍待验证。
2. K128 replacement database-E2E 已通过并归档；旧 failed-feeding 结果只作历史诊断，不再进入当前数字口径。
3. 权威内容入口改为 `opening/opening_defense_outline_20260808.md`；当前只更新实验报告、紧凑数据和待画图合同，不生成新图，也不生成、覆盖或同步新的 PPT/云文档。
4. 文本原生单 job、5s 两 job 与四 job 矩阵均已完成。图像 Daft built-in/Ray Data/project
   四作业和 DuckDB bounded-output 四作业只完成冻结准备，不跑 formal；图像使用 0.5s
   offset 并要求实际 overlap。后续先完成 capability/rehearsal，再跑 native 一次；项目
   感知/动态实现调整后只重跑同 manifest 的 project static/proposed。
5. 用户已明确不需要 Wiki 同步；当前也暂停普通飞书云文档覆盖，只完成本地材料与 Git 发布。

## 9. 结果解释与写作规则

每个正式实验按以下顺序记录：目的、设置、合规自检、设计、全组件数据、解释、对课题含义、下一步。解释明确区分事实、推断、待确认和不能声称。

GPU 利用率优先使用 time-series mean/p50/p95/max；KV usage 按 0–1 分数读取。feeding-saturation 以同协议 bounded direct 为参照；未过门的臂不抽策略性能结论。raw rows/s、correct rows/s 和 service tokens/s 不得互相替代，语义失败必须保留在总行数分母。

正式报告、论文、PPT 和图表不使用内部实验缩写。PG18.4 AutoDL 结果必须标为 rehearsal，不能冒充目标 PostgreSQL 18.3 平台结论。

## 10. 同步入口

- 开题报告：`opening/report/opening_report.md`
- 开题 Claim Matrix：`opening/claim_matrix.md`
- 当前答辩内容合同：`opening/opening_defense_outline_20260808.md`；历史PPT v6设计
  `opening/slides/opening_defense_v6_design.md`已被取代且禁止作为生成输入
- 答辩问答：`opening/qa_bank.md`
- 当前方向速览：`overview/current_direction_and_plan.md`
- 实验状态：`experiments/plans/experiment_status_and_gaps.md`
- 文献与知识：`research/knowledge_hub.md`
- 变更日志：`PROJECT_LOG.md`

影响方向、实验结论或关键入口的修改必须同步 `PROJECT_LOG.md`、`PROJECT_INDEX.md`、根 README 和受影响目录 README。修改知识文件后按 `research/knowledge_sync_guide.md` 同步平级 Obsidian Wiki。
