# Adaptive flush 双窗口改进实验（2026-07-25）

## 1. 实验设置

本实验回答：修正 queue-adaptive flush 的窗口选择和事件时间吸收语义后，它是否
能够真正形成多行 batch，并在真实单 GPU 链路中至少守住 fixed-timeout 的尾延迟
边界。

真实链路为：

```text
PostgreSQL 18.4 + pgvector 0.8.2
  -> Daft 0.7.20
  -> Arrow
  -> Ray 2.56 task
  -> vLLM 0.25.1 / Qwen2.5-1.5B
  -> NVIDIA GeForce RTX 5070 12 GB
```

未使用 fake backend。workload 为 PostgreSQL 中按 `arrival_time` 排序的
`sharegpt_burstgpt`。共同参数为 token budget 6144、静态 `K_max=8`、生成上限
16 tokens、低负载窗口 25 ms、压力窗口 50 ms、无写回。

复现命令以 `code/scripts/postgres_ai_operator_profile.py` 为入口，核心参数为：

```text
--total-rows {64|512|1024}
--data-source daft_postgres
--source-workload-name sharegpt_burstgpt
--source-order arrival_time
--operator ai_complete
--executor ray_task
--model-backend compatible_http
--completion-endpoint-url http://localhost:8000/v1/completions
--completion-model qwen2.5-1.5b
--completion-max-tokens 16
--batching-policy token_budget
--token-budget 6144
--max-inflight 8
--arrival-replay
--arrival-time-scale {0.0001|0.0005}
--flush-policy {immediate|fixed_timeout|queue_adaptive}
--flush-timeout-ms 25
--flush-max-wait-ms 50
```

完整环境、门禁和结论边界见 `manifest.json`。

## 2. 实验设计

代码修正包括：

1. 低负载或指标缺失时不再立即 flush，而是退化为 25 ms fixed-timeout；
2. waiting、KV 或 running 压力下扩展为 50 ms；
3. running 压力阈值直接使用本次运行的 `K_max`；
4. pending batch 打开时只选择一次不可变窗口；
5. 即使下游 Ray 背压导致墙钟延迟，event deadline 不晚于窗口 deadline 的行仍会
   加入当前 batch；
6. flush trace 记录 `selected_wait_s` 和 `window_reason`。

实验按三级执行：

- 64 行真实门禁：fixed-timeout 与 adaptive 各一次；
- 1024 行行为探针：immediate、fixed-timeout、adaptive 各一次；
- 512 行正式重复：每策略 1 次预热 + 5 次正式重复，预热不计统计。

正式策略组顺序为 adaptive、immediate、fixed，未按 repeat 随机化。

## 3. 严谨性自检

- 全部 18 条正式组运行记录（3 条预热 + 15 条正式）状态均为 `ok`；
- 每个正式 repeat 都覆盖 512 行、512 个唯一文档 ID，无遗漏或重复；
- 64 和 1024 门禁同样通过 exactly-once 审计；
- 每条 run、flush、submission、resource trace 都含 PostgreSQL 与 pgvector 版本；
- tokens/s 使用 vLLM Prometheus 的实际 prompt + generation token 增量；
- 正式统计使用样本标准差和 95% t 置信区间半宽（n=5，t=2.776）；
- Windows Ray shutdown 会向 stderr 输出 access-violation 日志。immediate 组的
  PowerShell 包装因此返回非零，但 1 条预热和 5 条正式 CSV 均完整、均为 `ok`，
  vLLM 每轮成功增量均为 512，服务最终 running=waiting=0。

## 4. 实验数据

### 64 行真实门禁

| 策略 | submissions | 平均 batch rows | batch service P99 (s) |
|---|---:|---:|---:|
| fixed timeout | 22 | 2.909 | 0.7198 |
| queue adaptive | 19 | 3.368 | 0.4561 |

两组均成功处理 64/64 请求，adaptive submissions 不超过 fixed，门禁通过。

### 1024 行单次行为探针

| 策略 | E2E (s) | tokens/s | submissions | 平均 batch rows | batch service P99 (s) |
|---|---:|---:|---:|---:|---:|
| immediate | 48.400 | 2893.3 | 1024 | 1.000 | 0.3336 |
| fixed timeout | 29.713 | 4713.0 | 342 | 2.994 | 0.4547 |
| queue adaptive | 27.819 | 5034.3 | 244 | 4.197 | 0.4978 |

adaptive P99 为 0.4978s，低于预设的 fixed P99 × 110% 上限 0.5002s。该余量很小，
因此只视为进入重复实验的门禁，不作为显著性结论。

### 512 行正式重复

均值 ± 95% CI 半宽：

| 策略 | E2E (s) | rows/s | observed tokens/s | submissions | 平均 batch rows | batch service P99 (s) |
|---|---:|---:|---:|---:|---:|---:|
| immediate | 29.895 ± 0.580 | 17.130 ± 0.328 | 2361.3 ± 45.3 | 512.0 ± 0.0 | 1.000 ± 0.000 | 0.3131 ± 0.0212 |
| fixed timeout | 21.413 ± 0.572 | 23.920 ± 0.636 | 3297.1 ± 87.6 | 200.0 ± 0.0 | 2.560 ± 0.000 | 0.3641 ± 0.0530 |
| queue adaptive | 20.647 ± 0.027 | 24.798 ± 0.033 | 3418.2 ± 4.6 | 153.0 ± 2.2 | 3.347 ± 0.046 | 0.3349 ± 0.0118 |

相对 fixed timeout，adaptive：

- E2E 降低 3.579%；
- observed tokens/s 提升 3.671%；
- submissions 减少 23.500%；
- 平均 batch rows 提升 30.732%；
- batch service P99 降低 8.010%。

adaptive 的 5 次正式运行共选择 409 个 25ms 低负载窗口、351 个 50ms
running-pressure 窗口和 5 个指标缺失 fallback 窗口，说明策略实际发生动态切换，
不是固定 50ms 的别名。

## 5. 结果解释

**事实**：旧版 adaptive 的平均 batch rows 为 1.0；新版在同一 512 行设置下达到
3.347，并比新版 fixed-timeout 少 23.5% submissions。

**事实**：本轮 5 次重复中，adaptive 的 observed tokens/s 均高于 fixed-timeout
组的 5 次重复范围，同时 batch service P99 均值没有恶化。

**推断**：双窗口和 event-time catch-up 把服务压力转化成了实际 coalescing，
减少 Ray/vLLM 提交开销；这是当前吞吐提升的主要候选解释。

**待确认**：组顺序、服务热状态和 16-token 固定生成上限可能影响组间差异。仍需在
随机化 repeat 顺序、变长输出和 held-out 规模下复验。

**不能声称**：

- 不能从本轮单 GPU 结果推广到多 endpoint 或多 GPU；
- 不能把 batch service P99 当成 per-request E2E P99；
- 不能声称 adaptive 在所有到达率和模型上都优于 fixed timeout；
- 不能据此跳过 batching × submission 联合搜索。

## 6. 对课题的含义

本轮已经修复“adaptive 不形成 batch”的致命实现问题，并给出研究内容二的首个正向
候选证据。queue-adaptive flush 可以进入下一阶段的随机化复验和联合搜索候选池，
但尚未达到最终论文结论强度。

## 7. 下一步

1. 在 512/1024 行上按 repeat 随机化策略顺序，补 per-request arrival、submit、
   completion 时间戳和 E2E P95/P99；
2. 加入自然 EOS/变长输出，对固定 16-token 设置做混淆变量消融；
3. 上述 guardrail 仍成立后再运行 2048 行 held-out；
4. 进入 token budget × K_max × flush 的联合搜索，不直接合并 `main`。

## 数据文件

- `gate_runs.csv`：64 行真实门禁；
- `probe_1024_runs.csv`：1024 行单次行为探针；
- `formal_512_runs.csv`：3 条预热 + 15 条正式逐运行原始指标；
- `formal_512_metric_summary.csv`：均值、样本标准差与 95% CI；
- `*_flush_trace.csv`：窗口选择与 flush 事件；
- `*_submission_trace.csv`：batch 组成、文档 ID 与服务时间；
- `*_resource_trace.csv`：GPU 与 vLLM 时序采样；
- `*.stdout.log` / `*.stderr.log`：正式组原始运行日志。
