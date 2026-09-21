# Request Lifecycle Infrastructure Gate

## 1. 实验设置

本门禁验证逐请求 lifecycle 与 seeded scenario runner 是否能在真实组件链路

```text
PostgreSQL 18.4 + pgvector 0.8.2
  -> Daft PostgreSQL source
  -> Arrow batch boundary
  -> Ray task
  -> vLLM 0.25.1 / Qwen2.5-1.5B / RTX 5070 12GB
```

上正确工作。输入为 `sharegpt_burstgpt` workload 的 64 个完整 prompt，按
`arrival_time` 排序并以 `0.0001` 倍时间尺度回放。公共参数为 token budget
6144、静态 `K_max=8`、25ms fixed window、50ms adaptive maximum window、
16-token 输出上限、无写回。`scenario_config.json` 使用 seed `20260725`，
两种场景各运行一次，无 warm-up。

1 秒 SLO 仅是观测链路门禁阈值，不代表真实业务承诺。正式执行没有使用 fake
backend。

## 2. 实验设计

- Baseline：`fixed_timeout` flush。
- Candidate：`queue_adaptive` 双窗口 flush。
- runner 在每次 profiler 子进程前要求 vLLM health=200 且
  running=waiting=0。
- 每次运行分别输出 run、request、submission、flush、resource、stdout 和
  stderr；manifest 每成功一次原子更新。
- 请求 trace 以完整 prompt 为单位；同一上游 submission 内的 prompt 共享
  submission completion timestamp。

第一次 preflight 使用了不存在的 `--model-request-timeout-s` 参数，runner
以 exit code 2 立即停止，且没有向 vLLM 发送请求。该 incident 保留在
`preflight_invalid_timeout_flag_manifest.json`；修正为
`--completion-request-timeout-s` 后重新执行。

## 3. 严谨性自检

最终门禁逐项重算并通过：

- 两个 run 均为 `ok`，vLLM success delta 均为 64；
- 每个场景均有 64 个 request rows、64 个唯一 request ID 和 doc ID；
- 每个 request 的 `submission_id` 均在 schema 2 `submissions.csv` 中显式存在；
- arrival ≤ flush ≤ submit ≤ completion，所有必需时间有限且 E2E 非负；
- run CSV 的 request P50/P95/P99 与 request rows 使用同一 nearest-rank
  口径重算，误差不超过 `1e-6`；
- 所有 CSV 行均记录真实 server/pgvector 版本；
- manifest schedule 与 completed runs 一致，结束后 vLLM
  running=waiting=0。

## 4. 实验数据

| 指标 | fixed timeout | queue adaptive | adaptive vs fixed |
|---|---:|---:|---:|
| Run E2E (s) | 3.776086 | 3.652046 | -3.285% |
| Rows/s | 16.949 | 17.524 | +3.393% |
| Observed tokens/s | 2972.920 | 3073.893 | +3.396% |
| Submissions | 22 | 19 | -13.636% |
| Mean batch rows | 2.909091 | 3.368421 | +15.789% |
| Batch service P99 (s) | 0.386654 | 0.361418 | -6.527% |
| Request E2E P50 (s) | 2.312145 | 2.224274 | -3.800% |
| Request E2E P95 (s) | 2.440645 | 2.308910 | -5.398% |
| Request E2E P99 (s) | 2.473097 | 2.351755 | -4.906% |
| 1s SLO violation ratio | 1.000 | 1.000 | 0 |
| 1s SLO goodput (requests/s) | 0 | 0 | 0 |

两次运行的服务端实际增量相同：10202 prompt tokens、1024 generation tokens、
64 successful sequences。

## 5. 结果解释

**事实**：本次真实链路中，adaptive 形成了更少的 submission 和更大的平均
batch；逐请求 client-observed E2E 指标、SLO 字段和跨 trace 外键可以一致地
落盘并重算。

**推断**：当前 1 秒阈值低于该 64 行链路的全部请求 E2E，因此它适合作为
“violation 路径被触发”的 instrumentation 检查，不适合作为本 workload 的
可行 SLO。

**待确认**：adaptive 的正向差值是否来自策略本身，仍需多轮 seeded
interleaving、输出长度变化和更大规模验证。

**不能声称**：本门禁只有每策略一次，且顺序为 fixed 后 adaptive；它不能证明
adaptive 稳定优于 fixed，不能提供显著性结论，也不能说明多 endpoint、多 GPU
或 2048 行行为。

## 6. 对课题的含义

request/submission/run 三层观测基础已能在正式
PostgreSQL→Daft→Arrow→Ray→vLLM 路径工作。后续 admission controller、
UCB reward、联合搜索和代价估计可以直接消费真实 request E2E/SLO 数据，不再用
batch service latency 冒充逐请求端到端延迟。

## 7. 下一步

1. 用 seeded runner 执行 fixed/adaptive 的多轮交错重复，报告置信区间。
2. 对 SLO 阈值做明确的实验定义或使用 latency–throughput 曲线，而不是沿用本门禁
   的 1 秒 instrumentation 阈值。
3. 在不改变本门禁结论的前提下，继续 output-aware packing、controller reward
   epoch 和 batching×submission 联合搜索。

## 复现入口

- 配置：`scenario_config.json`
- 调度与失败审计：`manifest.json`
- 运行级指标：`runs.csv`
- 逐请求数据：`*.requests.csv`
- submission 外键与服务时间：`*.submissions.csv`
- flush/resource 时序：`*.flush.csv`、`*.resources.csv`
