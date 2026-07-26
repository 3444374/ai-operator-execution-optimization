# Queue-adaptive flush 跨到达率筛选（2026-07-26）

## 实验问题

在当前 512 请求、自然 EOS、单 GPU 链路中，最佳静态 flush 等待窗口是否会随
回放负载变化，以及 queue-adaptive 25/50ms 是否能优于各档最优静态窗口。

真实链路为 PostgreSQL 18.4 + pgvector 0.8.2 → Daft 0.7.20 → Arrow →
Ray 2.56 task → vLLM 0.25.1 / Qwen2.5-1.5B → RTX 5070 12GB。未使用
fake backend。共同参数为 token budget 6144、静态 admission `K_max=8`、
ChatML、temperature 0、max output 512、`prompt_tokens <= 1500`、不写回。

## 负载档位

筛选使用过滤后按 `arrival_time_s` 排序的前 512 行。原始到达跨度为
39760s：

| replay scale | 压缩后跨度 | 平均回放强度 |
|---:|---:|---:|
| 0.00025 | 9.94s | 51.41 req/s |
| 0.0005（既有锚点） | 19.88s | 25.70 req/s |
| 0.001 | 39.76s | 12.85 req/s |

这些数值仅描述加速回放强度，不代表生产 QPS。既有 0.0005 档已有三次正式
重复；本轮对新增的快、慢两档各做一次筛选。只有策略排序改变时才增加重复。

## 严谨性自检

- manifest 状态为 `completed`，6/6 场景完成，0 incident；
- 每个场景均有 512 条 request trace、512 个唯一 request id；
- 每个场景均观测到 512 个实际输出 token 计数和 512 个 finish reason；
- 所有场景 SLO violation ratio 为 0；
- 所有场景 `vllm_metrics_status=ok`、`mfu_status=ok`，MFU 与 GPU 能耗均为正；
- 本轮是策略边界筛选，不提供显著性结论。

## 实验数据

| 回放档位 | 策略 | E2E (s) | tokens/s | request P99 (s) | submissions | energy/1k tokens (J) | MFU |
|---|---|---:|---:|---:|---:|---:|---:|
| 快 | fixed 25ms | 100.869 | 2264.572 | 89.123 | 137 | 58.738 | 0.11268 |
| 快 | fixed 50ms | 82.467 | 2774.049 | 70.983 | 96 | 50.851 | 0.13838 |
| 快 | adaptive 25/50ms | 83.293 | 2757.114 | 71.440 | 100 | 50.929 | 0.13755 |
| 慢 | fixed 25ms | 169.914 | 1356.731 | 126.861 | 276 | 84.573 | 0.06732 |
| 慢 | fixed 50ms | 134.671 | 1706.712 | 93.390 | 200 | 71.629 | 0.08477 |
| 慢 | adaptive 25/50ms | 135.823 | 1684.107 | 93.971 | 203 | 72.226 | 0.08364 |

相对变化：

| 对比 | 快档 tokens/s | 快档 E2E | 慢档 tokens/s | 慢档 E2E |
|---|---:|---:|---:|---:|
| fixed 50 vs fixed 25 | +22.50% | -18.24% | +25.80% | -20.74% |
| adaptive vs fixed 25 | +21.75% | -17.43% | +24.13% | -20.06% |
| adaptive vs fixed 50 | -0.61% | +1.00% | -1.32% | +0.86% |

## 结果解释

**事实**：快、当前、慢三个回放档位中，fixed 50ms 都没有被 fixed 25ms
反超。新增两档中，adaptive 与 fixed 50ms 的吞吐差为 -0.61% 和 -1.32%，
E2E 高 1.00% 和 0.86%，并分别多 4.17% 和 1.50% submissions。

**事实**：adaptive trace 在快档有 58 次、慢档有 181 次
`running_pressure` flush；控制器大部分时间选择 50ms 压力窗口。

**推断**：当前负载范围内，adaptive 的主要作用仍是近似选择 fixed 50ms，
没有显示出动态切换本身的增量价值。单 GPU 默认继续采用更简单的 fixed 50ms。

**不能声称**：一次筛选不能证明 fixed 50ms 在任意到达过程、模型或 GPU 上
都最优；也不能据此否定更宽窗口集合、连续窗口控制或其他观测信号。

## 决策

不为这两个新增档位追加正式重复，因为策略排序没有改变，且既有 0.0005 档
三次重复已经显示 adaptive 与 fixed 50ms 不可分辨。后续 2048 行留出实验以
fixed 50ms 为主策略，adaptive 仅作为复杂策略对照，不作为默认候选。

原始入口：

- `scenario_config.json`
- `screen/manifest.json`
- `screen/runs.csv`
- `screen/*.requests.csv`
- `screen/*.submissions.csv`
- `screen/*.flush.csv`
- `screen/*.resources.csv`
