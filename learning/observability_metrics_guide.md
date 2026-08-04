# 下一轮实验会记录哪些指标

## 为什么补这些指标

同一个 `tokens/s` 可能对应完全不同的系统状态：请求可能首 token 很慢、逐 token
间隔很抖、prefix cache 没命中，或者策略只是把更多超时请求塞进了系统。下一轮
实验因此同时记录“产出多少”“用户等多久”“资源是否有效工作”“策略为什么产生
这个结果”和“质量是否保持”。这些字段只增加观测，不修改调度策略。

## AI_COMPLETE 原始运行字段

- `vllm_time_to_first_token_{mean,p50,p95,p99}_s`：请求进入 vLLM 后，到首 token
  出现的分布。数据来自 vLLM Prometheus histogram 的运行前后 bucket delta。
- `vllm_inter_token_latency_{mean,p50,p95,p99}_s`：相邻输出 token 的时间间隔，
  用于区分“首 token 慢”和“生成过程卡顿”。
- `vllm_prefix_cache_{queries,hits}_delta` 与 `hit_rate`：prefix 策略是否真的减少
  prefill 重算的直接机制证据。
- `request_slo_{input,output,total}_tokens_goodput_per_s`：只统计 E2E SLO 内完成的
  token。它与普通 `tokens_per_s` 的区别是：迟到的工作不算有效产出。
- `observed_p99_slo_scale`：配置 `--ttft-slo-ms`/`--itl-slo-ms` 后，报告观测 P99
  是目标的多少倍。小于等于 1 表示该次运行满足这两个聚合 P99 目标；它不是逐请求
  联合 SLO attainment。
- `token_cost_*`：只有显式传入 input/output 的每百万 token 单价才计算。自建 GPU
  不应伪装成云 API 单价；未配置时状态为 `unavailable:prices_not_configured`。

## 数据组织与调度解释字段

- `packing_padding_waste_ratio`：每个组织 batch 按最长 prompt 补齐时，浪费 token
  slot 占总 padded slot 的比例。arrival-replay 若未保留逐行长度，状态明确不可用。
- `scheduling_control_overhead_{s,pct}`：只计 `organizer_plan_s + submit_s`，再除以
  `operator_wall_s`。`organizer_collect_s` 可能包含真实数据物化，不冒充纯调度开销。
- shared-vLLM 输出每 job 的实际 token work、SLO token goodput、最终累计服务差和
  job 活跃重叠期间的最大服务差。当前实现同时写
  `service_disparity_bound_status=unavailable:not_proven...`，因此这些数是描述性公平
  证据，不冒充 DRR/VTC 的理论上界。

## 正式重复与代价模型

正式 CSV 完成后运行：

```bash
python code/scripts/analysis/summarize_formal_repeats.py \
  --input-csv NEW_RESULT/runs.csv \
  --output NEW_RESULT/repeat_statistics.json \
  --baseline-scenario-id frozen_static \
  --regression-tolerance-pct 5
```

它输出 sample std、CV、Student-t 95% CI，以及按 `repeat_index` 配对的回退次数。
少于两次 formal 会明确标记没有区间，不能伪造误差条。

`estimate_operator_cost.py` 现在除 MAE/MAPE/RMSE/R² 外，还输出 Q-error
P50/P90/P95/P99/max、Spearman ρ，以及同一 workload 多候选配置中的 pick rate、
selected runtime、oracle runtime、regret、selected-plan rank 和 surpassed plans。
没有至少两个候选配置的 decision context 会被排除，不计作“选对”。

## AI_EMBED 质量门禁

Recall@K/nDCG/MRR 必须有显式相关性真值。先用 gate/capture 运行保存逐行向量，再执行：

```bash
python code/scripts/analysis/evaluate_embedding_retrieval.py \
  --embeddings gate_embeddings.npz \
  --relevance-csv relevance.csv \
  --k 1,5,10 \
  --output retrieval_quality.json
```

该脚本做精确 cosine 排序并排除 query 自身。它验证 embedding 的下游任务质量，
不计入性能 E2E；当前也不等于 pgvector ANN 写回后的 recall-vs-QPS 实验，后者仍需
索引、exact scan 真值和 disjoint query set。

## 运行边界

本次 profiler CSV 字段发生变化，下一轮必须使用新的结果目录，不能向旧 header
追加。vLLM 版本若不暴露 histogram bucket，TTFT/ITL 分位状态会是 unavailable；
价格、检索真值或多候选 decision context 缺失时同样 fail-closed。这样的“缺失”是
实验事实，不能用 0、checksum 或估计值替代。
