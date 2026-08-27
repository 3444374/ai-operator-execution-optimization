# 双 4090 算子代价 profile 4-cell pilot（2026-08-04/05）

> **归档状态（2026-08-27）**：pilot 已完成，其作用是验证采样合同和正式运行预算，不能用于
> 配置排名。结果与 raw trace 见
> [`../../results/operator_cost_profile_pilot_20260804/`](../../results/operator_cost_profile_pilot_20260804/)。
> 下文的“当前入口”是执行当时的历史表述。

> **历史执行入口：v3 cache-on gate。** 2026-08-04 的 v1/v2 是 cache-off 历史采样合同
> 门禁；其冻结配置保存在结果 raw manifest 中。按项目真实部署口径，当前
> `dual_gpu_cost_profile_pilot.example.json` 已升级为 cache-on v3。cache-off 只保留为
> 独立消融，不再承担正式性能 baseline。

## 问题

在扩展算子代价估计数据前，先回答三件事：当前双 4090 文本链路能否稳定生成一个
完整的四候选 decision context；每个 run 实际需要多久；152-run 全矩阵的时间和失败
风险是否可接受。本 pilot 不用于宣布 estimator 晋级，也不与旧单 5070 的 283 行
直接合并。

### v1 诊断轮与 v2 修订

第一轮 `dual_gpu_cost_profile_pilot_20260804` 完成 8/8 runs，但预注册门禁错误地
要求 fixed-timeout、非 arrival-replay 路径也输出 flush trace。当前实现只有 arrival
replay 存在显式关批决策循环；本轮离线请求已经由 organizer 完成组织，固定 flush 没有
可记录的逐次决策事件。因此 v1 只作为运行合同诊断证据，不据此启动正式扩展。

v2 在看到任何新结果前把 trace 合同修正为：所有 formal 必须保存 request、submission、
resource trace；只有 `arrival_replay=true` 时才强制 flush trace。非 replay 的
`flush_trace_path=""`、`flush_trace_events=0` 必须明确记录为 `not_applicable`，不能伪造
空事件。v2 仍使用完全相同的 workload 和四个 candidate，重新采集独立证据。

## 固定项与唯一变量

- 固定：Qwen2.5-7B、2 个 vLLM Completions endpoint、512 行
  `sharegpt_multiturn`、输出上限 256、`httpx_async` transport、request submission、
  token budget 8192、1×256 actor/endpoint、fixed 50 ms flush、prefix cache on。
- 唯一变量：每 endpoint active-work credit = 32,768 / 49,152 / 65,536 / 98,304。
- 编排：每臂 1 warmup + 1 formal，固定 seed 交错顺序，共 8 runs。

配置入口：`deploy/autodl/dual_gpu_cost_profile_pilot.example.json`。

## 通过门槛

1. 8/8 runs `status=ok`，没有 incident；512 unique requests、exactly-once；
2. 两个 endpoint 均有请求，vLLM counters 可用；端点启动命令和 live probe 均证明
   prefix cache on、模型、端口、batched-token 和 max-seqs 与声明一致；每行 CSV
   `service_prefix_caching=enabled`；
3. 每个 formal 保存 summary、request/submission/resource trace；arrival replay 才要求
   flush trace，非 replay 必须显式记录为不适用；
4. 四个 candidate 的 23 项 pre-execution feature 不相同，且 decision-context ID 相同；
5. 以实际 8-run 墙钟、每 run JCT 和端点空闲等待计算正式矩阵预计耗时，不沿用
   “30–45 分钟”的旧估计。
6. cache query/hit delta 非负、hits 不超过 queries、hit rate 位于 [0,1]；这些是机制
   解释字段，不可作为执行后泄漏特征输入 pre-execution cost estimator。

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
