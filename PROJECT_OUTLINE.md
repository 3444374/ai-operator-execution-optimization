# 项目大纲

更新时间：2026-08-07

本文件是项目方向、研究内容、证据等级和近期执行顺序的权威总纲。实验细节以对应结果目录的 README/CSV/JSON 为准；文献入口见 `research/knowledge_hub.md`；开题材料必须服从 `opening/claim_matrix.md`。

## 1. 题目与研究对象

题目冻结为：

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

对外口径：数据库内置 AI 算子的外部分布式数据处理执行链路优化。

Daft、Ray、vLLM、PostgreSQL、pgvector 和 CLIP 是实现与验证平台，不是贡献名称。项目不修改数据库内核、vLLM continuous batching、Ray 调度器、模型结构或 GPU kernel，也不回到传统 GPU 查询算子。

## 2. 研究内容

### 2.1 研究内容一：workload-aware work-unit 构造

研究数据库记录如何组成发送给模型服务的 work unit。核心变量是 token/frame work，而不是固定行数；候选机制包括 sequential budget、length alignment、prefix-aware grouping 和受控 best-fit。重点刻画两个冲突：

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

首版采用解析 work 特征、profile 校准和 residual correction，预测 prompt/output work、operator service、JCT、remaining work 与 SLO slack。它服务于 active-work 初始化、work-unit 构造、路由和提交选择，不单列为第三项研究内容。

评价 MAE/MAPE 之外的候选配置 ranking、pairwise accuracy、selection regret、最坏 context 与预测区间；平均误差好不能替代决策质量。

### 2.4 多模态泛化

文本 `AI_COMPLETE` 是主要方法场景；图像 `AI_EMBED/AI_CLASSIFY` 是正文泛化验证。公共策略只消费 estimated work、credit、queue 和 completion event：文本 adapter 输出 token work，图像 adapter 输出 frame/pixel/preprocess work；Organizer、Scheduler、Tracing 和配置逻辑保持一致。不适用某模态的能力必须显式声明。

## 3. 系统与实验边界

```text
PostgreSQL source
  -> Daft DataFrame / Arrow
  -> Cost Adapter + Organizer
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

三个研究问题：

1. 固定资源下达到近饱和吞吐所需的最小 active work 是多少，过载怎样影响 tail 与能耗？
2. 相同 work 下怎样组织记录，balance 与 locality 何时冲突？
3. 多 job 共享 endpoint pool 时，shared credit、routing 和公平队列能否改善 JCT/tail/fairness？

两项策略先独立搜索冻结静态点并分别消融，再把独立最优拼接，与小规模联合 grid 对比。联合显著优于拼接说明需要联合调优；两者接近说明可分层优化。任何结果都不改变研究对象，但会改变方法适用边界。

正式实验统一要求：immutable manifest、相同 source/sink、相同服务 flags、固定随机种子、warmup 与交错 formal repeats；结果同时保存请求、submission、资源时序、版本和 sink readback。headline 优先使用 correct throughput 与 database-E2E，并报告 service throughput、质量、failure 类型和资源门禁。

## 5. 当前证据等级

### 5.1 已证明

- 固定行数不是稳定 work 代理：同一行数下 token 分布跨度可达 13.9 倍。
- 当前双 4090/Qwen/vLLM 签名下，65,536 active work/endpoint 达最大已测吞吐均值的 97.80%；下一档只增 0.92%，继续增压会恶化 P99。
- 复杂动态控制不天然优于强静态点：AIMD/PID/EWMA、adaptive flush、service quantum 与多 actor 多数未过约 5% 晋级门槛。
- 数据组织策略排名受 serving regime 影响：双 endpoint 大 KV 池近似中性；四 endpoint 小 KV 池饱和时吞吐分化且排名反转，重排序可使 prefix hit 降至 0.06–0.07。
- 图像 matched-resource 静态执行结构有可重复收益：主报告冻结约 13%–15% operator-JCT 改善；旧 45.7% 资源不匹配，不再使用。

### 5.2 条件性

- Hybrid 代价估计在 429 个 formal 观测、20 context × 4 candidate 的 context-LOO 中取得 pooled regret 1.67%、macro 2.90%、pairwise 0.808、max regret 14.72%。最大 regret 距 15% 门仅 0.28 个百分点，属于 marginal pass。
- prefix-affinity routing 在四 endpoint/小 KV 池条件下出现约 5.9% 增量，但 endpoint consolidation 与饱和深度尚未完全隔离，不外推为普遍有效。

### 5.3 待验证

- runtime-state-aware 请求成形、提交或路由能否超过同上限 frozen-static；
- 多 job held-out、错峰、加权公平性与故障迁移；
- 代价模型跨时间段、新 workload 和硬件的稳定性；
- 图像 system database-E2E 与状态感知增量。

### 5.4 不能声称

- 项目路径普遍优于 direct、DuckDB AI、Daft、Ray Data 或 vLLM 官方路径；
- sequential、length-align 或 prefix-aware 是全局最优 organizer；
- 65,536 是 vLLM 通用容量或最佳并发；
- 动态策略已经胜出；
- 图像路径提升 45.7%；
- 代价模型已经稳健解决。

## 6. 开题前统一文本 database-E2E

开题前仅允许 SQuAD short-answer 均匀控制组与 ShareGPT controlled-skew 异质组。两组均比较：

- `direct_static_sharded`；
- `duckdb_ai_static_sharded`；
- `project_frozen_static`。

统一合同：PostgreSQL source、immutable equal-row manifest、双 Qwen2.5-7B vLLM endpoint、prefix cache ON、统一 PostgreSQL sink、外部 database-E2E、质量与资源指标、1 warmup + 3 formal。

SQuAD 三次 formal 均值：direct 129.85、DuckDB AI 135.71、project 116.88 correct rows/s。三臂 EM/F1 接近；项目臂 service tokens/s 只有 direct 的约 89.9%，未过 95% feeding-saturation 门，因此只作为负结果与瓶颈诊断，不支持项目策略性能 claim。DuckDB AI 每次 1 行 cap 语义失败，保留在分母。

ShareGPT 三次 formal 均值：direct 11.34、DuckDB AI 2.23、project 10.36 correct rows/s；对应 service tokens/s 为 9,412.74、9,411.76、8,601.29。项目臂只有 direct 的 91.38%，再次未过 95% feeding-saturation 门。DuckDB AI 的模型服务吞吐与 direct 接近，但固定 256-token cap 下三次 formal 共 4,936/6,144 行出现产品层 cap 语义失败，因此 correct throughput 显著降低；基础设施失败仍为 0。异质 workload 没有使项目冻结静态路径获得优势。

两组完成后停止新增开题 baseline。差异不足 5%、项目不占优或产品语义不兼容均不触发换模型、换数据库、换 workload 或扩大扫描。

## 7. 开题四张核心证据图

1. `opening_serving_capacity_frontier`：最小饱和 active work 与过载边界。
2. `opening_work_organization_regime`：work-aware 组织的必要性与 regime 局限。
3. `opening_image_matched_resource`：图像 matched-resource 可重复证据。
4. `opening_cost_model_decision_quality`：代价模型 selection regret 与最坏风险。

权威输出位于 `figures/data/report_main/`，生成脚本为 `figures/scripts/generate_opening_core_evidence_figures.py`，claim 与视觉审计见 `figures/audit/opening_core_evidence_figures_contract_20260807.md`。开题报告和 PPT 不再堆叠大量参数扫描。

## 8. 当前执行顺序

1. 两组统一文本三臂 formal、完整性审计、结果归档和开题 baseline 停止规则已完成。
2. Claim Matrix、报告、问答库和飞书本地源稿已按最终汇总同步；四张核心图与 28 页 v6
   PPTX 已完成程序化 QA，并于 2026-08-08 通过 Microsoft PowerPoint 真实打开检查。
3. 飞书用户授权已于 2026-08-08 恢复；线上正文已覆盖到 revision 289，八章目录、三项关键
   数字、结论边界和四个带 caption 的图片块均回读通过。
4. 平级 Obsidian wiki 目录在本机不存在，不能执行镜像脚本；恢复该目录后完成最后镜像并标记
   发布冻结。开题后再进入图像 system database-E2E 与 state-aware 方法实验。

## 9. 结果解释与写作规则

每个正式实验按以下顺序记录：目的、设置、合规自检、设计、全组件数据、解释、对课题含义、下一步。解释明确区分事实、推断、待确认和不能声称。

GPU 利用率优先使用 time-series mean/p50/p95/max；KV usage 按 0–1 分数读取。feeding-saturation 以同协议 bounded direct 为参照；未过门的臂不抽策略性能结论。raw rows/s、correct rows/s 和 service tokens/s 不得互相替代，语义失败必须保留在总行数分母。

正式报告、论文、PPT 和图表不使用内部实验缩写。PG18.4 AutoDL 结果必须标为 rehearsal，不能冒充目标 PostgreSQL 18.3 平台结论。

## 10. 同步入口

- 开题报告：`opening/report/opening_report.md`
- 开题 Claim Matrix：`opening/claim_matrix.md`
- 开题 PPT 冻结设计：`opening/slides/opening_defense_v6_design.md`
- 答辩问答：`opening/qa_bank.md`
- 当前方向速览：`overview/current_direction_and_plan.md`
- 实验状态：`experiments/plans/experiment_status_and_gaps.md`
- 文献与知识：`research/knowledge_hub.md`
- 变更日志：`PROJECT_LOG.md`

影响方向、实验结论或关键入口的修改必须同步 `PROJECT_LOG.md`、`PROJECT_INDEX.md`、根 README 和受影响目录 README。修改知识文件后按 `research/knowledge_sync_guide.md` 同步平级 Obsidian Wiki。
