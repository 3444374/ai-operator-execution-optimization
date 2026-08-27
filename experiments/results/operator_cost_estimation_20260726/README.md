# AI 算子端到端代价估计：当前权威结果（2026-08-04）

本目录的当前结论只来自 `phase=formal` 的 204 条真实 AI_COMPLETE profile。2026-08-04
审计发现旧 loader 将 warmup 混入训练、候选 repeat 均值和 selection metrics；旧 283-row
报告及证据已整体移至 `archive/allphases_pre_20260804/`，只用于追溯，不能继续引用为正式
性能结果。

## 1. 实验设置

- 数据：204 条 `status=ok, phase=formal` 记录，17 个 decision contexts；其中 13 个
  context 至少有两个候选，可以评价计划选择。
- 任务：只使用执行前特征预测 `e2e_s`，并在未见过的完整 decision context 上选择候选。
- 方法：CE0 mean、CE1 analytical、CE2 lookup、CE3 Ridge、CE4 LightGBM、CE5
  analytical + residual hybrid；CE6 oracle 只作上界。
- 当前实现使用 23 个执行前特征，并在 context identity 中隔离机器、数据库和 serving
  协议；实际输出、vLLM 指标、能耗、MFU 和真实 E2E 不进入输入特征。

## 2. 实验设计

采用 leave-one-decision-context-out：每轮完整留出一个 context 的全部候选和 formal
repeats，用其余 formal 行训练。候选内先合并 repeats，再计算 candidate pairwise、Top-K、
pick 和 regret。精确预测 tie 固定选择字典序最小 candidate ID，不依赖 CSV 行顺序。

## 3. 严谨性自检

- loader 对 warmup 和缺失 `phase` 的历史行 fail-closed；
- 每个 fold 断言 held-out context 不进入训练集；
- 当前压缩 JSON 保存源 CSV SHA256、代码 SHA256、逐 context 候选/重复、真实与预测均值；
- 13 个 multi-candidate contexts 很少，且候选数不均衡，不能把本轮当成跨机器、跨模型
  泛化结论；
- 双 4090 pilot 是独立机器轨道，不与旧单 5070 的 204 行静默合并。

## 4. 实验数据

| estimator | MAE(s) | 行级 pairwise | 候选 pairwise | 候选 Top-K | pick | macro regret | median | max | pooled | 晋级 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| CE0 mean | 68.27 | 0.500 | 0.500 | 0.108 | 0.077 | 42.79% | 9.95% | 381.42% | 13.23% | 否 |
| CE1 analytical | 13.39 | 0.533 | 0.558 | 0.367 | 0.231 | 22.45% | 1.74% | 168.67% | 4.82% | 否 |
| CE2 lookup | 44.67 | 0.446 | 0.347 | 0.108 | 0.077 | 42.79% | 9.95% | 381.42% | 13.23% | 否 |
| CE3 ridge | 1212.77 | 0.500 | 0.517 | 0.279 | 0.154 | 6.90% | 1.74% | 42.02% | 2.14% | 否 |
| CE4 LightGBM | 37.91 | 0.473 | 0.524 | 0.418 | 0.308 | 4.98% | 1.74% | 21.02% | 1.84% | 否 |
| **CE5 hybrid** | **7.91** | **0.684** | **0.800** | **0.744** | **0.538** | **4.58%** | **0.00%** | **26.23%** | **0.62%** | **否** |

详细逐 fold 数据和口径见 `ce_context_loo_20260804.md`。机器可复算证据为
`ce_context_loo_formal_only_23feature_20260804.json.gz`，SHA256：
`59f1f2baec71657583a4f2536f47c2b749105befda4d80ef12c82058d59397cf`。

## 5. 结果解释

### 实验事实

CE5 是当前最强候选之一：MAE 最低，candidate pairwise 为 0.800，macro/pooled regret
为 4.58%/0.62%。但它只选对 7/13 context，最差 context regret 为 26.23%，行级
pairwise 0.684 也未达到既有 0.75 门槛，因此仍不晋级。

### 合理推断

执行前特征对粗粒度 E2E 和候选排序有信号，但当前数据覆盖不足以让模型安全接管计划选择。
candidate 聚合指标比把 repeats 当独立计划更符合优化器语义；这只说明下一轮应使用该口径，
不允许事后替换旧门槛来宣布成功。

### 不能声称

- 不能说 CE5 已经是可靠的在线调度器或严格 SLO 预测器；
- 不能用低 pooled regret 掩盖 26.23% 的最差 context 回退；
- 不能把 warmup 混入的 283-row 历史数字继续写进当前 headline；
- 不能把双 4090 pilot 的 n=1 候选吞吐并入本数据集或据此选择最优配置。

## 6. 对课题的含义

代价估计继续定位为数据组织、active-work 初始化和计划选择的共同使能组件，而非独立研究
内容。现有证据支持继续采样和比较 CE0–CE5，但不支持让任何 estimator 接管执行决策。

## 7. 下一步

双 4090 四候选 pilot 已通过，见 `../operator_cost_profile_pilot_20260804/`。独立 20-context
formal 已在 `../../plans/completed/operator_cost_profile_dual4090_formal_20260804.md` 预注册；按用户
要求当前暂缓，由远端 agent 后续从 `main` 执行。新结果必须 formal-only、按机器隔离，
先通过完整性门禁，再评价 CE0–CE5 和是否进入 state-aware correction。

## 文件说明

- `ce_context_loo_20260804.md`：当前详细七步报告；
- `ce_context_loo_formal_only_23feature_20260804.json.gz`：当前可复算证据；
- `archive/allphases_pre_20260804/`：旧 all-phase 报告、JSON 和模型文件，审计用途；
- `../operator_cost_profile_pilot_20260804/`：双 4090 采样基础设施门禁。
