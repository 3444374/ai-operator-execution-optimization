# 算子代价估计 decision-context LOO 审计（2026-08-04）

本报告回答一个限定问题：当一个完整的数据库 AI 算子决策场景没有出现在训练集时，
代价估计器能否预测其候选配置并选出较快配置。它不评价运行时状态修正，也不直接证明
估计器已经可以接管调度。

## 1. 实验设置

- 输入：现有 AI_COMPLETE profile 的 283 条有效记录；目标为 `e2e_s`。
- 覆盖：17 个 decision context，其中 13 个至少有 2 个候选，可进入 LOO。
- estimator：CE0 mean、CE1 analytical、CE2 lookup、CE3 ridge、CE4 LightGBM、
  CE5 analytical + residual hybrid；CE6 oracle 仅是上界，不参与拟合。
- 服务器复算环境：独立 analysis venv，LightGBM 4.7.0、NumPy 2.5.1；不使用 GPU。
- 可复算证据：`ce_context_loo_20260804.json.gz`。其中保存源 CSV SHA256、代码 SHA256、
  每个 context 的候选、repeat 数、真实/预测均值、逐 fold 指标和汇总指标。

decision context 固定模型、workload、行数、输出上限和 arrival 条件；候选配置改变 batching、
token/work budget、inflight、actor workers 和 flush。这里测试的是 **unseen decision-context**，
并不保证每一个 candidate signature 也从未在其他 context 出现，因此不能写成笼统的
“unseen-config generalization”。

## 2. 实验设计

对 13 个 multi-candidate context 逐一执行：

1. 留出该 context 的全部候选和全部 formal repeats；
2. 用其余 282 行左右的数据拟合 estimator；
3. 在被留出的完整候选集合上预测；
4. 行级 MAE/pairwise 保留旧报告口径；
5. 先按 candidate 合并 repeats，再计算 within-context pairwise/Top-K，作为更贴近优化器
   选计划语义的新诊断口径；
6. 同时报 context 等权的 macro mean/median/range，以及按 oracle runtime 总量汇总的
   pooled regret。

晋级仍沿用此前预注册合同：`median decision regret <= 5%` **且**旧行级 pairwise
`>= 0.75`。候选聚合 pairwise 是本次审计新增指标，不能事后替代旧指标宣布晋级；它应在
下一轮新数据采集前预注册。

## 3. 严谨性自检

- 脚本对每 fold 断言 held-out context 不出现在训练集，避免 context leakage。
- repeats 在候选 ranking 前取均值；不再把同一候选的重复运行当成不同计划。
- JSON 保存每 fold 原始候选均值，而不是只保存最终表格。
- 13 个 context 仍很少，且候选数极不均衡：
  `19,13,7,5,3,3,3,3,2,2,2,2,2,1,1,1,1`。
- CE0/lookup 等 estimator 存在 predicted-best tie；现有 `selection_metrics` 使用旧的
  first-minimum 顺序消解，JSON 显式记录 tie count。含 tie 的 pick/regret 只能谨慎读取。
- scenario-group split 与 context-LOO 回答不同问题：前者偏向已见相近 context 的配置插值，
  后者测试新 context。不能因 LOO 数字更好就声称前者“低估”CE5。

## 4. 实验数据

下表均为 13 folds 的 context 等权平均；`pooled regret` 为所有 context 的 selected/oracle
runtime 分别求和后再计算比例。

| estimator | MAE(s) | 行级 pairwise | 候选 pairwise | 候选 Top-K | pick | regret mean | regret median | regret max | pooled regret | 晋级 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| CE0 mean | 67.03 | 0.500 | 0.500 | 0.454 | 0.385 | 111.82% | 1.55% | 1210.52% | 7.71% | 否 |
| CE1 analytical | 13.55 | 0.522 | 0.541 | 0.341 | 0.385 | 8.87% | 1.55% | 44.71% | 3.75% | 否 |
| CE2 lookup | 43.47 | 0.445 | 0.352 | 0.454 | 0.231 | 98.12% | 1.74% | 1210.52% | 6.25% | 否 |
| CE3 ridge | 1819.17 | 0.535 | 0.504 | 0.341 | 0.231 | 104.89% | 1.74% | 1210.52% | 9.69% | 否 |
| CE4 LightGBM | 43.28 | 0.513 | 0.486 | 0.433 | 0.308 | 10.68% | 1.55% | 69.71% | 2.37% | 否 |
| **CE5 hybrid** | **7.69** | **0.705** | **0.828** | **0.821** | **0.692** | **2.14%** | **0.00%** | **16.69%** | **0.31%** | **否** |

CE5 选对 9/13 context。`2.14%` 是 13 个 context regret 的简单平均，不是唯一可报数字；
pooled regret 为 `0.31%`，而最差 context 为 `16.69%`。三个数字共同说明平均总体损失小，
但仍存在明显的单场景回退。

## 5. 结果解释

### 实验事实

1. CE5 在本轮的 MAE、候选聚合 pairwise、候选 Top-K、pick 和 regret 上均是最强正信号。
2. CE5 的旧行级 pairwise 为 0.705，未达到 0.75；因此不满足现有双条件晋级合同。
3. CE1 的 selected-rank mean 为 2.31，略好于 CE5 的 2.38；所以“CE5 每项指标都支配”
   仍不是准确表述。
4. CE3 的 MAE 和 regret mean 被至少一个极端 fold 放大；当前数据能证明存在严重的
   unseen-context 外推不稳定，不能仅凭均值推断所有 context 都失败。

### 合理推断

解析基底加 residual correction 可能比纯 Ridge/树模型更能保持跨 context 的结构信息；
但这只是 13 个不均衡 context 上的候选解释，不是已经验证的普遍规律。

### 待确认

- 候选聚合 pairwise 作为下一轮主 ranking 指标后，CE5 是否仍能超过 0.75；
- 独立时间段、新 workload 和自然 EOS 输出分布下是否保持低 regret；
- state-aware correction 是否改善实际调度决策，而不恶化 P95/P99、SLO 和公平性。

### 不能声称

- 不能声称 CE5 已经接管调度或达到正式晋级；
- 不能把 2.14% 单独写成所有 workload 的稳定 regret；
- 不能把 context-LOO 改称完全未见 candidate/config；
- 不能把 CE3 的 1819s MAE 直接归因为实现 bug或普遍 OOD 灾难，除非逐 fold 机制复核。

## 6. 对课题的含义

CE5 仍是下一轮最值得保留的项目候选，但当前定位是“共同使能组件的强正信号”，不是独立
研究贡献或可部署优化器。LOO 的价值在于修正评价方法：预测秒数准确、候选排序正确和最终
决策安全是三层不同问题，必须同时报告。

## 7. 下一步

当前 17 个 context 若全部补到至少 4 candidates，需要 26 个配置单元；再新增 3 个
context × 4 candidates，需要 12 个，共 **38 个新配置单元**。按每单元
`1 warmup + 3 formal` 是 **152 runs**，不是原先估计的约 24 个单元。

正式启动前先做 4-cell pilot，实测单 run 时间、GPU/端点重启成本和失败率，再给出总耗时；
没有 pilot 不能声称 30–45 分钟可完成。新矩阵需在运行前冻结：

1. 候选聚合 within-context pairwise/Top-K 的主口径和 tie policy；
2. 20 个 context 的 workload/rows/output/arrival 覆盖；
3. 每个 context 的 4–6 个 candidate，避免只给两个大 context 堆大量候选；
4. 独立时间段/workload holdout；
5. CE5 相对 CE1/CE2/CE4 与强静态 fallback 的决策 regret、回退率和 estimator 开销。
