# 双 4090 算子代价 profile 4-cell pilot（2026-08-04）

## 问题

在扩展算子代价估计数据前，先回答三件事：当前双 4090 文本链路能否稳定生成一个
完整的四候选 decision context；每个 run 实际需要多久；152-run 全矩阵的时间和失败
风险是否可接受。本 pilot 不用于宣布 estimator 晋级，也不与旧单 5070 的 283 行
直接合并。

## 固定项与唯一变量

- 固定：Qwen2.5-7B、2 个 vLLM Completions endpoint、512 行
  `sharegpt_burstgpt`、输出上限 256、async transport、request submission、
  token budget 8192、1×256 actor/endpoint、fixed 50 ms flush、cache off。
- 唯一变量：每 endpoint active-work credit = 32,768 / 49,152 / 65,536 / 98,304。
- 编排：每臂 1 warmup + 1 formal，固定 seed 交错顺序，共 8 runs。

配置入口：`deploy/autodl/dual_gpu_cost_profile_pilot.example.json`。

## 通过门槛

1. 8/8 runs `status=ok`，没有 incident；512 unique requests、exactly-once；
2. 两个 endpoint 均有请求，vLLM counters 可用，服务 metadata 与配置一致；
3. 每个 formal 保存 summary、request/submission/resource/flush trace；
4. 四个 candidate 的 23 项 pre-execution feature 不相同，且 decision-context ID 相同；
5. 以实际 8-run 墙钟、每 run JCT 和端点空闲等待计算正式矩阵预计耗时，不沿用
   “30–45 分钟”的旧估计。

若任一门槛失败，停止，不扩成 152 runs。n=1 formal 只能校验运行合同和粗略耗时，不能
报告性能差异或 CV/CI。

## 正式扩展的预注册指标

新数据在任何 estimator 结果出现前冻结以下三层指标：

- 预测：MAE、RMSE、Q-error P50/P90/P95/P99/max、区间 coverage/width；
- 排序：candidate-repeat 聚合后的 within-context Spearman、pairwise、Top-K；
- 决策：pick、selected/oracle runtime、regret median/mean/max/pooled、selected rank、
  fallback rate 和 estimator overhead。

旧 row-level pairwise 只保留为历史兼容字段。下一轮新数据的主 ranking 口径是候选先聚合
repeat 后的 pairwise；tie policy 必须在 formal 前固定，不能看到结果后更换。

## 环境隔离

旧 283 行来自单 5070。新双 4090 profile 单独保存，并携带 GPU model/per-GPU memory、
数据库版本、serving protocol/transport 与 endpoint candidate identity。只有在明确做
cross-hardware transfer 实验时才合并两个数据源；普通 LOO 不允许静默混合。
