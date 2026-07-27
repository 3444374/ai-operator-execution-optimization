# AI 算子端到端代价估计（2026-07-26）

## 复现入口

单个 grouped held-out split 使用：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\estimate_operator_cost.py `
  --input-csv <合并后的真实 runs.csv> `
  --output experiments\results\operator_cost_estimation_20260726\e2e_cost_model.json `
  --target e2e_s `
  --test-fraction 0.25 `
  --seed 20260726 `
  --alpha 1.0
```

五个 seed 的 JSON 是独立重复执行得到的稳健性审计。输入只能使用真实 profile CSV；合并和过滤后必须保持 283 行、70 个配置组，并由脚本按配置组切分，不能先按行随机切分。

## 目标

基于已经采集的真实 profile 数据，验证仅使用执行前可知特征能否估计
AI_COMPLETE 算子的端到端时间。该模型用于后续编排决策的补充信息，不作为
独立研究贡献，也不声称带来调度加速。

**两个预期用途**：

1. **数据库优化编排**（主要用途）。为查询优化器提供 AI 算子代价估计，
   辅助选择执行计划：这个 AI_COMPLETE 调用大概需要多少时间？两个
   候选计划的代价排序是否正确？是否需要分配更多并行资源？

2. **提交策略辅助**（探索性用途）。如果排序能力足够强，可在提交侧提供
   粗粒度工作量预估——pending batch 预计计算量是轻还是重？当前 inflight
   的总估计 token-work 是否过高？作为对 vLLM Prometheus 信号（
   running/waiting/KV）的补充，但不能替代文献中的 Orca 式持续供给和
   vLLM 反馈驱动提交机制。

## 数据与特征

输入为 `experiments/results/` 下 29 个 `runs.csv`：

- 原始 `status=ok` 行 285；
- 2 行缺少完整 packing 特征而排除；
- 最终 283 行、70 个唯一配置组。

配置组由模型、workload、batching、输出上限、token budget、K_max、flush、
arrival replay 等执行前配置签名定义。同一配置的 warm-up/formal/repeat 不会
同时进入训练和测试，避免重复运行泄漏。

特征共 15 个：

- 行数、prompt token 总量、completion 上限；
- token budget、组织后的 batch 数；
- batch 估计代价 P50/P95/max；
- K_max；
- flush timeout/max wait；
- arrival scale、是否 arrival replay；
- adaptive/immediate flush 指示变量。

实际输出 token、实测 E2E/service、vLLM 指标、能耗和 MFU 均不作为特征。
目标为 `e2e_s`。

## 模型与指标

模型为标准化特征上的 log1p ridge regression（alpha=1），对照为训练集目标
均值。主切分使用 seed 20260726、25% 配置组留出：

| 模型 | 测试行 | MAE (s) | MAPE | RMSE (s) | R² |
|---|---:|---:|---:|---:|---:|
| 训练均值 baseline | 87 | 29.427 | 255.35% | 55.805 | -0.004 |
| ridge cost model | 87 | 9.728 | 32.58% | 34.302 | 0.620 |

为避免只报告有利切分，又对连续 5 个固定 seed 做同样的分组留出审计：

| seed | 测试行 | MAE (s) | MAPE | RMSE (s) | R² |
|---:|---:|---:|---:|---:|---:|
| 20260726 | 87 | 9.728 | 32.58% | 34.302 | 0.620 |
| 20260727 | 58 | 15.058 | 68.16% | 29.682 | 0.788 |
| 20260728 | 85 | 8.957 | 30.76% | 16.905 | 0.833 |
| 20260729 | 39 | 13.578 | 90.60% | 29.045 | 0.858 |
| 20260730 | 85 | 11.091 | 30.92% | 19.502 | 0.781 |
| 均值 | — | 11.682 | 50.60% | 25.887 | 0.776 |

五个切分的 ridge MAE 均优于对应 mean baseline；mean baseline MAE 范围为
27.621–34.651s。

## 结果解释

**事实**：执行前特征对 E2E 具有可用但不稳定的解释力。主切分 R² 为 0.620，
跨 seed 范围为 0.620–0.858；MAPE 范围为 30.76%–90.60%，说明小目标和留出
配置组合会显著影响相对误差。

**事实**：五个切分上 ridge 均降低 MAE，平均 MAE 从 mean baseline 的
29.890s 降至 11.682s。

**推断**：当前模型适合做粗粒度容量/编排提示，不适合作为严格 SLO 预测器。
最大的不可观测因素是自然 EOS 下的实际输出长度；completion 上限只是执行前
代理变量。

**不能声称**：

- 不能把当前 MAPE 写成跨模型泛化精度；
- 不能使用测试集选择 alpha 或特征后再报告同一测试集；
- 不能把预测相关性等同于在线调度收益；
- 不能在没有模型/模态重训或校准时直接用于多模态。

## 工程产物

- `code/src/cost_estimation.py`：分组切分、ridge estimator、回归指标；
- `code/scripts/estimate_operator_cost.py`：从 profile CSV 构造可复现模型；
- `e2e_cost_model.json`：主切分完整特征 schema、系数、均值/尺度和指标；
- `e2e_cost_model_seed_*.json`：五个稳健性切分。

## 待补充：排序能力分析

当前评估使用 MAE/RMSE/R²/MAPE 等回归指标，回答的是"预测值离真实值
差多少秒"。但对编排和提交策略来说，更关键的指标是**排序质量**——模型
能否正确区分"便宜配置"和"昂贵配置"：

| 指标 | 含义 | 对编排的意义 | 对提交策略的意义 |
|---|---|---|---|
| Spearman 秩相关系数 | 预测排名与真实排名的相关性 | 优化器选择 A 还是 B 的正确率 | 判断 pending batch 是"轻"是"重"的可靠性 |
| Pairwise accuracy | 随机抽两个配置，模型正确排序的比例 | 两计划对比的置信度 | inflight estimate vs 新请求的轻重判断 |
| Top-K precision | 预测最便宜的 K 个中，实际最便宜的比例 | 优化器挑最优候选的命中率 | 选择"安全提交窗口"的准确性 |

R² 0.776 暗示排序能力大概率不错（回归好通常排序也好），但不能替代
显式计算。需要补充计算后更新本节。

## 后续工作

以下按优先级排列，每项标注对应的文献设计模式（详见
`research/knowledge_hub.md` §5.7-§5.9）。完整的 15 个设计模式总览
和优先级矩阵见该文档。

### 第一批（短期，1-2 轮即可落地）

1. **补充排序指标**（模式 2：排序优先评估 | Heinrich SIGMOD 2025 R2）。
   在 `estimate_operator_cost.py` 中增加 Spearman 秩相关系数、
   pairwise accuracy、Top-K precision 输出。这是 Heinrich 论文的
   核心论点——对编排决策来说排序比点估计精度重要。已有最强文献支撑。

2. **Hybrid 架构实验**（模式 1：传统公式 + Learned Correction |
   Heinrich R4, Pathak & Mankodi）。
   增加一个 `E2E_base` 特征列：`E2E_base = total_prompt_tokens /
   estimated_throughput + fixed_overhead`，作为第 16 个特征输入
   Ridge。预期效果：降低跨 seed MAPE 波动（当前 30-90%），改善 R²。
   本质是让 Ridge 学"传统公式无法解释的偏差"而非从头学整个 E2E 函数。

3. **Output-Length 预测器**（模式 15：Output-Length Predictor |
   SFS §3.4, GRACEFUL §IV.C）。
   当前 Ridge 以 `completion_max_tokens`（用户设定的输出上限）作为
   特征——但实际 E2E 高度依赖于真实输出 token 数（自然 EOS 位置），
   而非上限。SFS 证明用 LightGBM 从 prompt 特征预测实际输出长度是
   可行的（MAPE <5% on output length）。实现方式：离线训练一个
   LightGBM（输入 prompt 特征→输出实际 output tokens），推理时作为
   第 17 个特征输入 Ridge。已有 283 行 profile 包含每行的实际输出
   token 数作为 ground truth，无需重新 profile。

4. **轻/中/重分档验证**（模式 13：Workload 分类 | SPOS, Heinrich R3）。
   用已有 283 行 profile 评估：按预测 E2E 将配置分为轻/中/重三档，
   同档内真实 E2E 方差是否显著小于全局？这决定了代价估计能否作为
   提交侧的粗粒度 workload 分类器（不追求精确点估计，而是可靠的
   档位判断）。

### 第二批（中期，需改动 profile pipeline）

5. **预测区间**（模式 4：不确定性门控 | Microsoft Patent, Heinrich R3）。
   编排决策不应只靠点估计，需要输出保守上界或预测区间。可用
   bootstrap residual 估计（训练集残差经验分布作为预测区间）实现。

6. **多代价指标联合输出**（模式 7：多指标输出 | COSTREAM）。
   同时预测 `tokens/s` 和 `service_p99`（当前只预测 `e2e_s`）。
   实现方式：多个独立 Ridge 共享 15 特征，不改模型架构。

7. **训练数据多样化**（模式 8：数据多样化 | Heinrich R3）。
   后续 profile 中有意加入"已知慢"的配置变体（K_max 极低、
   batch size 极大等），让模型学习边界和劣化配置的代价特征。
   Heinrich 用 500 条多样化数据 fine-tune 后 LCM 首次超越 PG。

8. **独立 workload/时间段留出验证**（模式 6：Transferable Features |
   COSTREAM）。用新模型或新 workload 做外推验证，评估排序退化程度。
   当前特征已是 transferable 的（物理量），理论上可泛化——需实验确认。

### 第三批（远期，多 endpoint/多 GPU 后）

9. **解耦三阶段建模**（模式 5：Per-Component 建模 | CONCERTO）。
   分别估计 DB fetch、vLLM prefill/decode、writeback 时间后进行
   聚合。需要 `postgres_ai_operator_profile.py` 增加 per-stage
   timing 列。

10. **多粒度模型组合**（模式 3：Meta-Learner | Microsoft Patent）。
    训练 per-workload 局部模型 + 全局模型 + meta-learner 加权。
    当前 283 行不足以训练可靠的局部模型（某些配置组仅 2-3 行），
    需更多 profile 数据积累后才值得做。

11. **Probe Execution 数据收集**（模式 14：Probe Execution |
    CONCERTO §III）。验证"部分执行特征 vs 完整 E2E 代价"的相关性
    ——即是否能从前 N 个请求的 metrics 模式推断完整运行的 E2E 行为。
    如果成立，profile 阶段可以用更少的请求量（如 200 行替代完整
    512/2048 行）快速收集训练数据。需实验确认"partial trace"与
    "full run"的排序一致性。

### 不纳入短期计划

- **SFS What-If 预演**和 **LPS K_max 选择**（模式 10/11）属于
  提交策略（RC2）范畴，不是代价估计（RC4）的直接工作。列入
  `experiments/plans/experiment_status_and_gaps.md` 的 RC2 缺口。
- **Token-Batch 处理时间回归**（模式 12）依赖 per-iteration vLLM
  batch composition 信号，当前 vLLM Prometheus 粒度不足以支持，列入
  RC2 远期探索。
- **GNN/Transformer 升级**：6 篇文献一致确认简单模型（Ridge/
  XGBoost）在小数据上足够——当前 283 行远未达到需要 GNN 的规模。

### 关于"已排除"技术的说明

以下技术曾在实验中未表现出优于当前 baselines 的结果，但代码和实验
记录均已保留，**不视为永久排除**。当前结论受限于单 GPU、单 workload
shape、稳态到达等测试条件——在不同硬件/负载/多租户场景下可能重新体现
出价值：

| 技术 | 当前结论 | 保留位置 | 重新激活条件 |
|------|---------|---------|------------|
| AIMD/EWMA-AIMD/PID 自适应准入 | 相对 static K=16 无增量（vLLM waiting=0，AIMD 盲视 Ray 侧软拥塞） | `code/src/adaptive_admission.py` | 解决软拥塞信号盲区后（逐请求 completion time 观测） |
| Two-level queue-adaptive flush | 相对 fixed-50ms 无稳定增量（89.4% 时间选 50ms） | `code/src/queue_adaptive_flush.py` | 多 workload shape / 变长输出 / 多租户到达下重新评估 |
| GNN/Transformer 升级 | 283 行数据远未达到需要 GNN 的规模 | 未实现（仅保留设计文档） | profile 数据增长到千级/万级行后 |

以上技术的代码路径和实验 CSV 均保持可用，后续重新激活时改动量较小。

### 文献支撑索引

| 后续工作项 | 对应文献模式 | 核心论文 |
|-----------|------------|---------|
| 排序指标补充 | 模式 2 | Heinrich SIGMOD 2025 |
| Hybrid 架构 | 模式 1 | Heinrich R4, Pathak & Mankodi |
| Output-Length 预测器 | 模式 15 | SFS §3.4, GRACEFUL §IV.C |
| 轻/中/重分档 | 模式 13 | SPOS, Heinrich R3 |
| 预测区间 | 模式 4 | Microsoft Patent, Heinrich R3 |
| 多指标输出 | 模式 7 | COSTREAM ICDE 2024 |
| 数据多样化 | 模式 8 | Heinrich R3 |
| 跨 workload 留出 | 模式 6 | COSTREAM |
| 解耦三阶段 | 模式 5 | CONCERTO |
| 多粒度模型 | 模式 3 | Microsoft Patent |
| Probe Execution | 模式 14 | CONCERTO §III |
| 简单模型优先 | 模式 9 | Heinrich R1, Pathak & Mankodi |
